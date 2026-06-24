use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};
use sha1::{Digest, Sha1};
use tracing::{debug, warn};

use crate::manifest::{ManifestEntry, ManifestId};
use crate::{Error, Result};

use super::persist::{persist_resolutions, persist_resolved_refs};
use super::{
    AnalyzerProgress, AnalyzerProgressObserver, WorkspaceAnalyzer, WorkspaceFile,
    all_workspace_analyzers,
};

// Timeout is a hang detector, not a total work cap. T3 measured nlohmann's
// C++ pass advancing through 47.4k definition sites with zero request errors
// beyond the old 600s wall-clock cap, so only stop when the analyzer-side
// progress beacon itself stalls.
pub(crate) const ANALYZER_STALL_TIMEOUT: Duration = Duration::from_secs(300);
const ANALYZER_STALL_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
const ANALYZER_STALL_LSP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Run registered workspace analyzers over a manifest and persist
/// facts that can be mapped back to existing CAS rows.
///
/// This is best-effort. Analyzer failures are recorded in
/// `workspace_analysis_runs` and do not fail repo registration.
///
/// # Errors
/// Returns SQLite or filesystem errors encountered while recording
/// run status or persisting successful facts.
pub fn run_registered_workspace_analyzers(
    conn: &mut Connection,
    repo_root: &Path,
    manifest_id: ManifestId,
    entries: &[ManifestEntry],
    now_ns: i64,
) -> Result<usize> {
    run_workspace_analyzers_with_timeout(
        conn,
        repo_root,
        manifest_id,
        entries,
        now_ns,
        all_workspace_analyzers(),
        ANALYZER_STALL_TIMEOUT,
    )
}

#[cfg(test)]
pub(super) fn run_workspace_analyzers(
    conn: &mut Connection,
    repo_root: &Path,
    manifest_id: ManifestId,
    entries: &[ManifestEntry],
    now_ns: i64,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
) -> Result<usize> {
    run_workspace_analyzers_with_timeout(
        conn,
        repo_root,
        manifest_id,
        entries,
        now_ns,
        analyzers,
        ANALYZER_STALL_TIMEOUT,
    )
}

pub(super) fn run_workspace_analyzers_with_timeout(
    conn: &mut Connection,
    repo_root: &Path,
    manifest_id: ManifestId,
    entries: &[ManifestEntry],
    now_ns: i64,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
    analyzer_stall_timeout: Duration,
) -> Result<usize> {
    let mut inserted = 0;

    for analyzer in analyzers {
        let analyzer_id = analyzer.id();
        let analyzer_revision = analyzer.revision();
        let config_hash = config_hash(repo_root, analyzer.config_paths());
        mark_run(
            conn,
            RunRecord {
                manifest_id,
                analyzer_id,
                analyzer_revision,
                config_hash: &config_hash,
                status: RunStatus::Queued,
                started_at_ns: now_ns,
                finished_at_ns: now_ns,
                error: None,
                job_id: None,
            },
        )?;
        let outcome = run_one_workspace_analyzer_with_timeout(
            conn,
            AnalyzerRunRequest {
                analyzer,
                repo_root,
                manifest_id,
                entries,
                now_ns,
                analyzer_stall_timeout,
                job_id: None,
                progress_observer: None,
            },
        )?;
        inserted += outcome.inserted_refs;
    }

    Ok(inserted)
}

pub(crate) struct AnalyzerRunRequest<'a> {
    pub(crate) analyzer: Box<dyn WorkspaceAnalyzer>,
    pub(crate) repo_root: &'a Path,
    pub(crate) manifest_id: ManifestId,
    pub(crate) entries: &'a [ManifestEntry],
    pub(crate) now_ns: i64,
    pub(crate) analyzer_stall_timeout: Duration,
    pub(crate) job_id: Option<i64>,
    pub(crate) progress_observer: Option<AnalyzerProgressObserver>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnalyzerExecution {
    pub status: RunStatus,
    pub inserted_refs: usize,
    pub error: Option<String>,
}

pub(crate) fn run_one_workspace_analyzer_with_timeout(
    conn: &mut Connection,
    request: AnalyzerRunRequest<'_>,
) -> Result<AnalyzerExecution> {
    let AnalyzerRunRequest {
        analyzer,
        repo_root,
        manifest_id,
        entries,
        now_ns,
        analyzer_stall_timeout,
        job_id,
        progress_observer,
    } = request;
    let analyzer_id = analyzer.id();
    let analyzer_revision = analyzer.revision();
    let parser_id = analyzer.parser_id();
    let tier_prefix = analyzer.tier_prefix();
    let config_hash = config_hash(repo_root, analyzer.config_paths());
    let files = workspace_files_for(conn, parser_id, repo_root, entries)?;
    if files.is_empty() {
        mark_run(
            conn,
            RunRecord {
                manifest_id,
                analyzer_id,
                analyzer_revision,
                config_hash: &config_hash,
                status: RunStatus::Skipped,
                started_at_ns: now_ns,
                finished_at_ns: now_ns,
                error: Some("no matching files"),
                job_id,
            },
        )?;
        return Ok(AnalyzerExecution {
            status: RunStatus::Skipped,
            inserted_refs: 0,
            error: Some("no matching files".into()),
        });
    }

    mark_run(
        conn,
        RunRecord {
            manifest_id,
            analyzer_id,
            analyzer_revision,
            config_hash: &config_hash,
            status: RunStatus::Running,
            started_at_ns: now_ns,
            finished_at_ns: now_ns,
            error: None,
            job_id,
        },
    )?;

    match analyze_workspace_with_timeout(
        analyzer,
        repo_root,
        manifest_id,
        &files,
        analyzer_stall_timeout,
        progress_observer,
    ) {
        AnalyzerRun::Completed(Ok(facts)) => {
            let inserted_refs = persist_resolved_refs(
                conn,
                manifest_id,
                analyzer_id,
                tier_prefix,
                parser_id,
                &facts,
            )?;
            persist_resolutions(
                conn,
                manifest_id,
                analyzer_id,
                tier_prefix,
                parser_id,
                &facts,
            )?;
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id,
                    analyzer_revision,
                    config_hash: &config_hash,
                    status: RunStatus::Succeeded,
                    started_at_ns: now_ns,
                    finished_at_ns: now_ns,
                    error: None,
                    job_id,
                },
            )?;
            Ok(AnalyzerExecution {
                status: RunStatus::Succeeded,
                inserted_refs,
                error: None,
            })
        }
        AnalyzerRun::Completed(Err(err)) => {
            let message = err.to_string();
            let status = if is_content_modified_error(&err) {
                debug!(
                    analyzer_id,
                    error = %message,
                    "transient: LSP content-modified during run"
                );
                RunStatus::Skipped
            } else if is_analyzer_unavailable_error(&err) {
                RunStatus::Skipped
            } else {
                warn!(
                    analyzer_id,
                    error = %message,
                    "workspace analyzer failed"
                );
                RunStatus::Failed
            };
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id,
                    analyzer_revision,
                    config_hash: &config_hash,
                    status,
                    started_at_ns: now_ns,
                    finished_at_ns: now_ns,
                    error: Some(&message),
                    job_id,
                },
            )?;
            Ok(AnalyzerExecution {
                status,
                inserted_refs: 0,
                error: Some(message),
            })
        }
        AnalyzerRun::TimedOut { timeout } => {
            let message = format!("analyzer stalled: no progress for {}s", timeout.as_secs());
            warn!(
                analyzer_id,
                timeout_secs = timeout.as_secs(),
                "workspace analyzer stalled"
            );
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id,
                    analyzer_revision,
                    config_hash: &config_hash,
                    status: RunStatus::TimedOut,
                    started_at_ns: now_ns,
                    finished_at_ns: now_ns,
                    error: Some(&message),
                    job_id,
                },
            )?;
            Ok(AnalyzerExecution {
                status: RunStatus::TimedOut,
                inserted_refs: 0,
                error: Some(message),
            })
        }
    }
}

enum AnalyzerRun {
    Completed(Result<super::WorkspaceFacts>),
    TimedOut { timeout: Duration },
}

fn analyze_workspace_with_timeout(
    analyzer: Box<dyn WorkspaceAnalyzer>,
    repo_root: &Path,
    manifest_id: ManifestId,
    files: &[WorkspaceFile],
    timeout: Duration,
    progress_observer: Option<AnalyzerProgressObserver>,
) -> AnalyzerRun {
    let repo_root = repo_root.to_path_buf();
    let files = files.to_vec();
    let (tx, rx) = mpsc::channel();
    let progress = progress_observer
        .map(AnalyzerProgress::with_observer)
        .unwrap_or_default();
    let worker_progress = progress.clone();
    let worker = std::thread::spawn(move || {
        let result = analyzer.analyze_workspace(&repo_root, manifest_id, &files, &worker_progress);
        let _ = tx.send(result);
    });

    let mut last_progress = progress.snapshot();
    loop {
        match rx.recv_timeout(timeout) {
            Ok(result) => return AnalyzerRun::Completed(result),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let current_progress = progress.snapshot();
                if current_progress != last_progress {
                    debug!(
                        progress = current_progress,
                        previous_progress = last_progress,
                        stall_window_secs = timeout.as_secs(),
                        "workspace analyzer still making progress"
                    );
                    last_progress = current_progress;
                    continue;
                }
                progress.cancel();
                let _ = cleanup_stalled_analyzer_worker(worker, &rx);
                cleanup_stalled_analyzer_resources();
                return AnalyzerRun::TimedOut { timeout };
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return AnalyzerRun::Completed(Err(Error::InvalidArgument(
                    "workspace analyzer worker disconnected".to_string(),
                )));
            }
        }
    }
}

fn cleanup_stalled_analyzer_worker(
    worker: std::thread::JoinHandle<()>,
    rx: &mpsc::Receiver<Result<super::WorkspaceFacts>>,
) -> std::thread::Result<()> {
    match rx.recv_timeout(ANALYZER_STALL_JOIN_TIMEOUT) {
        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => worker.join(),
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(()),
    }
}

fn cleanup_stalled_analyzer_resources() {
    test_observe_stalled_analyzer_cleanup();
    if let Err(err) =
        crate::lsp::pool::force_shutdown_global_if_initialized(ANALYZER_STALL_LSP_SHUTDOWN_TIMEOUT)
    {
        warn!(error = %err, "failed to clean up stalled analyzer LSP pool");
    }
}

#[cfg(test)]
fn test_observe_stalled_analyzer_cleanup() {
    if let Some(observer) = STALLED_ANALYZER_CLEANUP_OBSERVER
        .lock()
        .expect("stalled analyzer cleanup observer poisoned")
        .as_ref()
    {
        observer();
    }
}

#[cfg(not(test))]
fn test_observe_stalled_analyzer_cleanup() {}

#[cfg(test)]
static STALLED_ANALYZER_CLEANUP_OBSERVER: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

fn is_content_modified_error(err: &Error) -> bool {
    matches!(err, Error::Lsp(lsp_err) if lsp_err.is_content_modified())
}

fn is_analyzer_unavailable_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Lsp(crate::lsp::Error::BinaryMissing(_))
            | Error::Lsp(crate::lsp::Error::WorkspaceUnsuitable(_))
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Skipped,
    TimedOut,
    Cancelled,
}

impl RunStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            "timed_out" => Some(Self::TimedOut),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::TimedOut | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RunRecord<'a> {
    pub(crate) manifest_id: ManifestId,
    pub(crate) analyzer_id: &'a str,
    pub(crate) analyzer_revision: u32,
    pub(crate) config_hash: &'a str,
    pub(crate) status: RunStatus,
    pub(crate) started_at_ns: i64,
    pub(crate) finished_at_ns: i64,
    pub(crate) error: Option<&'a str>,
    pub(crate) job_id: Option<i64>,
}

pub(crate) fn mark_run(conn: &Connection, run: RunRecord<'_>) -> Result<()> {
    let finished = match run.status {
        RunStatus::Queued | RunStatus::Running => None,
        RunStatus::Succeeded
        | RunStatus::Failed
        | RunStatus::Skipped
        | RunStatus::TimedOut
        | RunStatus::Cancelled => Some(run.finished_at_ns),
    };
    conn.execute(
        "INSERT INTO workspace_analysis_runs
           (manifest_id, analyzer_id, analyzer_revision, config_hash,
            status, started_at_ns, finished_at_ns, error, job_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(manifest_id, analyzer_id) DO UPDATE SET
            analyzer_revision = excluded.analyzer_revision,
            config_hash = excluded.config_hash,
            status = excluded.status,
            started_at_ns = excluded.started_at_ns,
            finished_at_ns = excluded.finished_at_ns,
            error = excluded.error,
            job_id = COALESCE(excluded.job_id, workspace_analysis_runs.job_id)",
        params![
            run.manifest_id.0,
            run.analyzer_id,
            run.analyzer_revision,
            run.config_hash,
            run.status.as_str(),
            run.started_at_ns,
            finished,
            run.error,
            run.job_id,
        ],
    )?;
    Ok(())
}

/// Select the manifest entries this analyzer should see: those whose
/// blob was indexed under the analyzer's Tier-1 parser. This reuses
/// the indexer's backend dispatch (extension and shebang detection)
/// instead of maintaining a parallel extension table here.
pub(crate) fn workspace_files_for(
    conn: &Connection,
    parser_id: &str,
    repo_root: &Path,
    entries: &[ManifestEntry],
) -> Result<Vec<WorkspaceFile>> {
    let mut stmt =
        conn.prepare("SELECT 1 FROM blobs WHERE blob_sha = ?1 AND parser_id = ?2 LIMIT 1")?;
    let mut files = Vec::new();
    for entry in entries {
        let indexed: Option<i64> = stmt
            .query_row(params![entry.blob_sha, parser_id], |r| r.get(0))
            .optional()?;
        if indexed.is_none() {
            continue;
        }
        let worktree_path = repo_root.join(&entry.path);
        files.push(WorkspaceFile {
            path: entry.path.clone(),
            blob_sha: entry.blob_sha.clone(),
            worktree_path: worktree_path.exists().then_some(worktree_path),
        });
    }
    Ok(files)
}

pub(crate) fn config_hash(repo_root: &Path, config_paths: &[&str]) -> String {
    let mut hasher = Sha1::new();
    for rel in expanded_config_paths(repo_root, config_paths) {
        let path = repo_root.join(&rel);
        if let Ok(bytes) = std::fs::read(&path) {
            hasher.update(rel.as_bytes());
            hasher.update([0]);
            hasher.update(bytes);
            hasher.update([0]);
        }
    }
    hex::encode(hasher.finalize())
}

fn expanded_config_paths(repo_root: &Path, config_paths: &[&str]) -> Vec<String> {
    let mut expanded = Vec::new();
    for rel in config_paths {
        if has_glob_meta(rel) {
            // Project-shaped LSPs often key their state off discovered files
            // such as `*.csproj`; hashing the literal pattern would miss added
            // projects and leave stale Tier-3 facts behind.
            expanded.extend(expand_config_glob(repo_root, rel));
        } else {
            expanded.push((*rel).to_string());
        }
    }
    expanded
}

fn expand_config_glob(repo_root: &Path, rel: &str) -> Vec<String> {
    let pattern_path = Path::new(rel);
    let Some(file_pattern) = pattern_path.file_name().and_then(|name| name.to_str()) else {
        return Vec::new();
    };
    let parent = pattern_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(""));
    if has_glob_meta(&parent.to_string_lossy()) {
        return Vec::new();
    }

    let dir = repo_root.join(parent);
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut matches = entries
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| {
            let file_name = entry.file_name();
            let file_name = file_name.to_str()?;
            wildcard_matches(file_pattern, file_name).then(|| {
                let path = PathBuf::from(parent).join(file_name);
                path.to_string_lossy().replace('\\', "/")
            })
        })
        .collect::<Vec<_>>();
    matches.sort();
    matches
}

fn has_glob_meta(pattern: &str) -> bool {
    pattern.contains(['*', '?'])
}

fn wildcard_matches(pattern: &str, candidate: &str) -> bool {
    wildcard_matches_bytes(pattern.as_bytes(), candidate.as_bytes())
}

fn wildcard_matches_bytes(pattern: &[u8], candidate: &[u8]) -> bool {
    match pattern.split_first() {
        None => candidate.is_empty(),
        Some((&b'*', rest)) => {
            wildcard_matches_bytes(rest, candidate)
                || candidate.split_first().is_some_and(|(_, candidate_rest)| {
                    wildcard_matches_bytes(pattern, candidate_rest)
                })
        }
        Some((&b'?', rest)) => candidate
            .split_first()
            .is_some_and(|(_, candidate_rest)| wildcard_matches_bytes(rest, candidate_rest)),
        Some((&expected, rest)) => {
            candidate
                .split_first()
                .is_some_and(|(&actual, candidate_rest)| {
                    expected == actual && wildcard_matches_bytes(rest, candidate_rest)
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_analyzer::WorkspaceFacts;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    struct CancelAwareAnalyzer;

    impl WorkspaceAnalyzer for CancelAwareAnalyzer {
        fn id(&self) -> &'static str {
            "cancel-aware"
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            "fake"
        }

        fn parser_id(&self) -> &'static str {
            "fake-parser"
        }

        fn analyze_workspace(
            &self,
            _repo_root: &Path,
            _manifest_id: ManifestId,
            _files: &[WorkspaceFile],
            progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            while !progress.is_cancelled() {
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(WorkspaceFacts::default())
        }
    }

    #[test]
    fn config_hash_changes_when_globbed_config_file_is_added() {
        let tmp = tempfile::tempdir().unwrap();
        let before = config_hash(tmp.path(), &["*.csproj"]);

        fs::write(tmp.path().join("App.csproj"), "<Project />\n").unwrap();
        let after = config_hash(tmp.path(), &["*.csproj"]);

        assert_ne!(before, after);
    }

    #[test]
    fn expanded_config_paths_sorts_glob_matches() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("B.csproj"), "<Project />\n").unwrap();
        fs::write(tmp.path().join("A.csproj"), "<Project />\n").unwrap();

        let paths = expanded_config_paths(tmp.path(), &["*.csproj"]);

        assert_eq!(paths, vec!["A.csproj", "B.csproj"]);
    }

    #[test]
    fn stalled_analyzer_cancels_worker_and_cleans_lsp_pool() {
        let cleanup_called = Arc::new(AtomicBool::new(false));
        {
            let cleanup_called = cleanup_called.clone();
            *STALLED_ANALYZER_CLEANUP_OBSERVER
                .lock()
                .expect("stalled cleanup observer poisoned") = Some(Box::new(move || {
                cleanup_called.store(true, Ordering::SeqCst);
            }));
        }

        let started = Instant::now();
        let run = analyze_workspace_with_timeout(
            Box::new(CancelAwareAnalyzer),
            Path::new("/tmp/repo"),
            ManifestId(1),
            &[WorkspaceFile {
                path: "src/lib.rs".into(),
                blob_sha: "sha".into(),
                worktree_path: None,
            }],
            Duration::from_millis(10),
            None,
        );

        *STALLED_ANALYZER_CLEANUP_OBSERVER
            .lock()
            .expect("stalled cleanup observer poisoned") = None;

        assert!(matches!(
            run,
            AnalyzerRun::TimedOut {
                timeout
            } if timeout == Duration::from_millis(10)
        ));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "stalled analyzer cleanup took {:?}",
            started.elapsed()
        );
        assert!(cleanup_called.load(Ordering::SeqCst));
    }
}
