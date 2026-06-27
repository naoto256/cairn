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

    // Input completeness gate (v0.7.0 D PR): for analyzers that
    // need the runner to read inputs on their behalf, materialize
    // bytes here so that a missing or unreadable workspace file
    // forces a `Failed` run *before* the analyzer is called. Without
    // this, the kotlin/swift/csharp/python/javascript/ruby/php
    // Tier-2.5 analyzers would silently return empty facts on a
    // transiently-inaccessible worktree, and `persist_resolutions`
    // would commit the DELETE half of its delete-then-insert with no
    // INSERTs to balance it, deleting prior `tier25-*` rows under a
    // `Succeeded` status mark. Partial unreadable is treated as a
    // full failure: an incomplete cross-file graph produces wrong
    // fallbacks instead of correct partial truth.
    let files = if analyzer.requires_materialized_files() {
        let selected_count = files.len();
        let materialized = materialize_workspace_files(files);
        if !materialized.unreadable.is_empty() {
            let error = format_unreadable_error(&materialized.unreadable, selected_count);
            warn!(
                analyzer_id,
                manifest_id = manifest_id.0,
                unreadable = materialized.unreadable.len(),
                selected = selected_count,
                "input materialization failed; marking run failed and preserving prior rows"
            );
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id,
                    analyzer_revision,
                    config_hash: &config_hash,
                    status: RunStatus::Failed,
                    started_at_ns: now_ns,
                    finished_at_ns: now_ns,
                    error: Some(&error),
                    job_id,
                },
            )?;
            return Ok(AnalyzerExecution {
                status: RunStatus::Failed,
                inserted_refs: 0,
                error: Some(error),
            });
        }
        materialized.files
    } else {
        files
    };

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
            source_bytes: None,
        });
    }
    Ok(files)
}

/// One file that the runner could not read from disk while
/// materializing inputs for a Tier-2.5 analyzer.
///
/// `path` is repo-relative (the column already on the manifest and
/// the surface the operator sees in `cairn ctl daemon doctor`).
/// `worktree_path` is kept around for debug logging; it is *not*
/// rendered into operator-facing error text so absolute paths don't
/// leak into routine logs. `error_kind` is `Option` because a missing
/// `worktree_path` is reported with `kind = None` and a synthetic
/// message rather than fabricating an `ErrorKind::NotFound`.
#[derive(Debug, Clone)]
pub(crate) struct WorkspaceFileReadError {
    pub(crate) path: String,
    pub(crate) worktree_path: Option<PathBuf>,
    pub(crate) error_kind: Option<std::io::ErrorKind>,
    pub(crate) message: String,
}

/// Outcome of attempting to attach `source_bytes` to each
/// [`WorkspaceFile`] before invoking a Tier-2.5 analyzer.
///
/// `files` only contains entries whose bytes were attached
/// successfully. `unreadable` is non-empty iff at least one selected
/// file could not be read; the runner treats *any* unreadable entry
/// as a run-level failure rather than handing the analyzer a partial
/// snapshot (an incomplete cross-file graph would silently produce
/// wrong fallbacks and `DELETE` prior `tier25-*` rows under a
/// `Succeeded` mark).
#[derive(Debug)]
pub(crate) struct MaterializedWorkspaceFiles {
    pub(crate) files: Vec<WorkspaceFile>,
    pub(crate) unreadable: Vec<WorkspaceFileReadError>,
}

/// Read every workspace file's bytes and attach them to
/// [`WorkspaceFile::source_bytes`]. Files with no `worktree_path` or
/// whose `std::fs::read` fails are collected into `unreadable` and
/// excluded from the materialized list.
///
/// Read once, share via `Arc<[u8]>`: analyzers iterate through the
/// returned files and clone the `Arc` if they want to keep the slice
/// alive past the call (the kotlin/swift/csharp/python/javascript/
/// ruby/php Tier-2.5 paths today only need a `&[u8]` view, so the
/// clone is essentially free).
pub(crate) fn materialize_workspace_files(files: Vec<WorkspaceFile>) -> MaterializedWorkspaceFiles {
    let mut out_files = Vec::with_capacity(files.len());
    let mut unreadable = Vec::new();
    for mut file in files {
        let Some(worktree_path) = file.worktree_path.clone() else {
            let err = WorkspaceFileReadError {
                path: file.path.clone(),
                worktree_path: None,
                error_kind: None,
                message: "worktree path missing (file was indexed but is not present on disk)"
                    .to_string(),
            };
            debug!(
                path = %err.path,
                error_kind = ?err.error_kind,
                "materialize: worktree path missing"
            );
            unreadable.push(err);
            continue;
        };
        match std::fs::read(&worktree_path) {
            Ok(bytes) => {
                file.source_bytes = Some(std::sync::Arc::from(bytes.into_boxed_slice()));
                out_files.push(file);
            }
            Err(err) => {
                let read_err = WorkspaceFileReadError {
                    path: file.path.clone(),
                    worktree_path: Some(worktree_path),
                    error_kind: Some(err.kind()),
                    message: err.to_string(),
                };
                debug!(
                    path = %read_err.path,
                    worktree_path = ?read_err.worktree_path,
                    error_kind = ?read_err.error_kind,
                    message = %read_err.message,
                    "materialize: workspace file unreadable"
                );
                unreadable.push(read_err);
            }
        }
    }
    MaterializedWorkspaceFiles {
        files: out_files,
        unreadable,
    }
}

/// Render the operator-facing error message that goes into the
/// `workspace_analysis_runs.error` column when materialization
/// fails. Lists up to three repo-relative paths with their OS error
/// messages, then a `(showing first 3 of N)` tail when there are
/// more — enough for `cairn ctl daemon doctor` to point the operator
/// at the affected files without flooding the log.
pub(crate) fn format_unreadable_error(
    unreadable: &[WorkspaceFileReadError],
    selected_count: usize,
) -> String {
    const PREVIEW: usize = 3;
    let total = unreadable.len();
    let head = unreadable
        .iter()
        .take(PREVIEW)
        .map(|err| format!("{}: {}", err.path, err.message))
        .collect::<Vec<_>>()
        .join(", ");
    let tail = if total > PREVIEW {
        format!(" (showing first {PREVIEW} of {total})")
    } else if total > 1 {
        format!(" (showing all {total})")
    } else {
        String::new()
    };
    format!("{total} of {selected_count} workspace files unreadable: {head}{tail}",)
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
                source_bytes: None,
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

    // ───────────────────────────────────────────────────────────────
    // D PR (v0.7.0) — input materialization gate tests
    // ───────────────────────────────────────────────────────────────
    //
    // These pin the framework-level invariant that protects every
    // `requires_materialized_files() == true` analyzer (the seven
    // Tier-2.5 crates today) against silent data loss when the
    // worktree is transiently inaccessible.

    fn ws_file(path: &str, worktree_path: Option<PathBuf>) -> WorkspaceFile {
        WorkspaceFile {
            path: path.into(),
            blob_sha: format!("blob-{path}"),
            worktree_path,
            source_bytes: None,
        }
    }

    /// Test #1 (all missing) — every selected file has `worktree_path
    /// = None`, so the materializer emits an `unreadable` entry per
    /// file and the runner-side gate (exercised through
    /// `materialize_workspace_files` here) refuses to hand the
    /// analyzer a partial snapshot.
    #[test]
    fn materialize_all_missing_reports_every_file_as_unreadable() {
        let files = vec![
            ws_file("a.txt", None),
            ws_file("b.txt", None),
            ws_file("c.txt", None),
        ];
        let materialized = materialize_workspace_files(files);
        assert!(materialized.files.is_empty());
        assert_eq!(materialized.unreadable.len(), 3);
        let paths: Vec<_> = materialized
            .unreadable
            .iter()
            .map(|e| e.path.as_str())
            .collect();
        assert_eq!(paths, ["a.txt", "b.txt", "c.txt"]);
        // None of the entries has a `worktree_path` to surface — the
        // origin signal is "indexed but absent on disk" rather than
        // "present at a path that failed to read".
        for err in &materialized.unreadable {
            assert!(err.worktree_path.is_none());
            assert!(err.error_kind.is_none());
            assert!(err.message.contains("worktree path missing"));
        }
    }

    /// Test #2 (partial missing) — when even one selected file is
    /// unreadable, the materializer still returns the readable ones
    /// in `files` *but* surfaces the unreadable ones separately. The
    /// runner gate above this turns any non-empty `unreadable` into a
    /// run-level `Failed`, so partial is treated as total failure.
    #[test]
    fn materialize_partial_missing_separates_readable_and_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let good = tmp.path().join("good.txt");
        fs::write(&good, b"hello").unwrap();

        let files = vec![
            ws_file("good.txt", Some(good.clone())),
            ws_file("missing.txt", None),
        ];
        let materialized = materialize_workspace_files(files);
        assert_eq!(materialized.files.len(), 1);
        assert_eq!(
            materialized.files[0].source_bytes.as_deref(),
            Some(&b"hello"[..])
        );
        assert_eq!(materialized.unreadable.len(), 1);
        assert_eq!(materialized.unreadable[0].path, "missing.txt");
    }

    /// Test #3 (read error) — `worktree_path` exists, but the actual
    /// `std::fs::read` fails (e.g. a directory at that path). The
    /// failure carries the underlying `ErrorKind` and message so
    /// `doctor` can route the operator to the right remediation.
    #[test]
    fn materialize_read_error_carries_os_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir_path = tmp.path().join("a_directory");
        fs::create_dir(&dir_path).unwrap();

        let files = vec![ws_file("a_directory", Some(dir_path.clone()))];
        let materialized = materialize_workspace_files(files);
        assert!(materialized.files.is_empty());
        assert_eq!(materialized.unreadable.len(), 1);
        let err = &materialized.unreadable[0];
        assert_eq!(err.path, "a_directory");
        assert_eq!(err.worktree_path, Some(dir_path));
        // The exact ErrorKind varies by platform (`IsADirectory` on
        // Linux, `Other` on older macOS, `InvalidInput` elsewhere) —
        // assert it is present so the operator surface can render it,
        // not the specific variant.
        assert!(err.error_kind.is_some());
        assert!(!err.message.is_empty());
    }

    /// Test #4 (legitimate empty facts) — when every file is
    /// readable, `materialize_workspace_files` produces no
    /// `unreadable` entries even if the analyzer subsequently chooses
    /// to return an empty `WorkspaceFacts`. This is the explicit
    /// signal that the runner is *allowed* to call `persist_resolutions`
    /// (which will DELETE prior rows) — analyzers that legitimately
    /// produce empty facts (e.g. a future analyzer-improvement run
    /// that removes false positives) must clear stale rows. The
    /// materializer cannot conflate "input was unreadable" with
    /// "analyzer chose to emit nothing."
    #[test]
    fn materialize_all_readable_yields_no_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        for name in ["a.txt", "b.txt", "c.txt"] {
            fs::write(tmp.path().join(name), name.as_bytes()).unwrap();
        }
        let files = vec![
            ws_file("a.txt", Some(tmp.path().join("a.txt"))),
            ws_file("b.txt", Some(tmp.path().join("b.txt"))),
            ws_file("c.txt", Some(tmp.path().join("c.txt"))),
        ];
        let materialized = materialize_workspace_files(files);
        assert_eq!(materialized.files.len(), 3);
        assert!(materialized.unreadable.is_empty());
        // source_bytes is attached, ready for the analyzer's
        // `file.source_bytes.as_deref()` migration.
        for f in &materialized.files {
            assert!(f.source_bytes.is_some());
        }
    }

    /// Test #5 (happy path) — round-trip a small file through the
    /// materializer and confirm the byte content matches.
    #[test]
    fn materialize_happy_path_attaches_exact_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("foo.kt");
        fs::write(&path, b"class Foo {}").unwrap();

        let materialized = materialize_workspace_files(vec![ws_file("foo.kt", Some(path))]);
        assert_eq!(materialized.files.len(), 1);
        assert_eq!(
            materialized.files[0].source_bytes.as_deref(),
            Some(&b"class Foo {}"[..])
        );
        assert!(materialized.unreadable.is_empty());
    }

    /// Test #6 (Skipped boundary preserved) — `files.is_empty()`
    /// remains a `Skipped` outcome (legit non-applicability, e.g.
    /// running a Kotlin analyzer over a Ruby-only repo) and must NOT
    /// be promoted to `Failed` by the new gate. Exercised by calling
    /// the materializer with an empty slice — it returns empty +
    /// empty, and the gate up in `run_one_workspace_analyzer_with_timeout`
    /// short-circuits to `Skipped` before materialization runs.
    #[test]
    fn materialize_empty_files_is_a_noop_not_a_failure() {
        let materialized = materialize_workspace_files(Vec::new());
        assert!(materialized.files.is_empty());
        assert!(materialized.unreadable.is_empty());
    }

    /// Test #7 (LSP / default analyzer perf regression pin) — the
    /// default `requires_materialized_files()` is `false` so the
    /// runner's gate does NOT call `materialize_workspace_files` for
    /// LSP-class analyzers. Pin the default explicitly so a future
    /// refactor cannot silently flip it and start pre-reading every
    /// file on a 50k-blob monorepo.
    #[test]
    fn requires_materialized_files_default_is_false() {
        struct DefaultAnalyzer;
        impl WorkspaceAnalyzer for DefaultAnalyzer {
            fn id(&self) -> &'static str {
                "default-analyzer"
            }
            fn revision(&self) -> u32 {
                1
            }
            fn language(&self) -> &'static str {
                "x"
            }
            fn parser_id(&self) -> &'static str {
                "x"
            }
            fn analyze_workspace(
                &self,
                _: &Path,
                _: ManifestId,
                _: &[WorkspaceFile],
                _: &AnalyzerProgress,
            ) -> Result<WorkspaceFacts> {
                Ok(WorkspaceFacts::default())
            }
        }
        assert!(!DefaultAnalyzer.requires_materialized_files());
    }

    /// Test #8 (error text contains path + OS error) — the
    /// human-facing string the runner stamps into
    /// `workspace_analysis_runs.error` must list at least one
    /// affected repo-relative path so `doctor` can route the operator
    /// directly to it. Absolute paths must NOT leak (they belong in
    /// debug logs only).
    #[test]
    fn format_unreadable_error_renders_paths_and_count() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("subdir");
        fs::create_dir(&dir).unwrap();
        let files = vec![
            ws_file("examples/Foo.kt", None),
            ws_file("examples/Bar.kt", None),
            ws_file("subdir", Some(dir.clone())),
            ws_file("missing.kt", None),
            ws_file("also_missing.kt", None),
        ];
        let materialized = materialize_workspace_files(files);
        let msg = format_unreadable_error(&materialized.unreadable, 5);
        assert!(msg.contains("examples/Foo.kt"));
        assert!(msg.contains("worktree path missing"));
        assert!(msg.contains("of 5 workspace files unreadable"));
        assert!(msg.contains("showing first 3 of 5"));
        // Absolute path must not appear in the operator-facing string.
        assert!(
            !msg.contains(tmp.path().to_string_lossy().as_ref()),
            "absolute path leaked into error text: {msg}"
        );
    }

    /// Test #9 (Skipped vs Failed boundary) — concrete fixtures pin
    /// the two paths produced by the runner gate side-by-side:
    ///
    ///   - empty file selection (= no matching parser, e.g. Kotlin
    ///     analyzer on a Ruby-only repo) → `Skipped`
    ///   - non-empty selection with at least one unreadable →
    ///     `Failed`
    ///
    /// A future refactor that conflates them (e.g. promotes both to
    /// `Failed`) would break the analyzer-revision-drift scanner,
    /// which treats `Skipped at current rev` as quiescence and
    /// `Failed at current rev` as a doctor-surfaced error.
    #[test]
    fn materialize_skipped_vs_failed_boundary() {
        // Empty selection: the runner gate above never reaches
        // `materialize_workspace_files`. Confirm the helper is a
        // no-op on that input so a hypothetical future caller cannot
        // turn the legit-empty case into a Failed one.
        let empty = materialize_workspace_files(Vec::new());
        assert!(empty.files.is_empty());
        assert!(empty.unreadable.is_empty());

        // Non-empty selection with a missing file: at least one
        // entry on the unreadable list, which the gate translates
        // into `Failed`.
        let with_missing = materialize_workspace_files(vec![ws_file("missing.kt", None)]);
        assert!(with_missing.files.is_empty());
        assert_eq!(with_missing.unreadable.len(), 1);
    }
}
