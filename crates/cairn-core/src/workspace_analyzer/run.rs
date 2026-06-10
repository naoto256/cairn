use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};
use sha1::{Digest, Sha1};
use tracing::{debug, warn};

use crate::manifest::{ManifestEntry, ManifestId};
use crate::{Error, Result};

use super::persist::persist_resolved_refs;
use super::{WorkspaceAnalyzer, WorkspaceFile, all_workspace_analyzers};

// Large LSP-backed workspaces can spend minutes on cold-start indexing. Bound
// each backend so a stuck analyzer is recorded and the synchronous CLI returns.
const WORKSPACE_ANALYZER_TIMEOUT: Duration = Duration::from_secs(600);

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
        WORKSPACE_ANALYZER_TIMEOUT,
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
        WORKSPACE_ANALYZER_TIMEOUT,
    )
}

pub(super) fn run_workspace_analyzers_with_timeout(
    conn: &mut Connection,
    repo_root: &Path,
    manifest_id: ManifestId,
    entries: &[ManifestEntry],
    now_ns: i64,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
    analyzer_timeout: Duration,
) -> Result<usize> {
    let mut inserted = 0;

    for analyzer in analyzers {
        let analyzer_id = analyzer.id();
        let analyzer_revision = analyzer.revision();
        let parser_id = analyzer.parser_id();
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
                },
            )?;
            continue;
        }

        mark_run(
            conn,
            RunRecord {
                manifest_id,
                analyzer_id,
                analyzer_revision,
                config_hash: &config_hash,
                status: RunStatus::Pending,
                started_at_ns: now_ns,
                finished_at_ns: now_ns,
                error: None,
            },
        )?;
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
            },
        )?;

        match analyze_workspace_with_timeout(
            analyzer,
            repo_root,
            manifest_id,
            &files,
            analyzer_timeout,
        ) {
            AnalyzerRun::Completed(Ok(facts)) => {
                let n = persist_resolved_refs(conn, manifest_id, analyzer_id, parser_id, &facts)?;
                inserted += n;
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
                    },
                )?;
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
                } else if is_binary_missing_error(&err) {
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
                    },
                )?;
            }
            AnalyzerRun::TimedOut { timeout } => {
                let message = format!("analyzer timed out after {}s", timeout.as_secs());
                warn!(
                    analyzer_id,
                    timeout_secs = timeout.as_secs(),
                    "workspace analyzer timed out"
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
                        error: Some(&message),
                    },
                )?;
            }
        }
    }

    Ok(inserted)
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
) -> AnalyzerRun {
    let repo_root = repo_root.to_path_buf();
    let files = files.to_vec();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = analyzer.analyze_workspace(&repo_root, manifest_id, &files);
        let _ = tx.send(result);
    });

    match rx.recv_timeout(timeout) {
        Ok(result) => AnalyzerRun::Completed(result),
        Err(mpsc::RecvTimeoutError::Timeout) => AnalyzerRun::TimedOut { timeout },
        Err(mpsc::RecvTimeoutError::Disconnected) => AnalyzerRun::Completed(Err(
            Error::InvalidArgument("workspace analyzer worker disconnected".to_string()),
        )),
    }
}

fn is_content_modified_error(err: &Error) -> bool {
    matches!(err, Error::Lsp(lsp_err) if lsp_err.is_content_modified())
}

fn is_binary_missing_error(err: &Error) -> bool {
    matches!(err, Error::Lsp(crate::lsp::Error::BinaryMissing(_)))
}

#[derive(Debug, Clone, Copy)]
enum RunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

impl RunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RunRecord<'a> {
    manifest_id: ManifestId,
    analyzer_id: &'a str,
    analyzer_revision: u32,
    config_hash: &'a str,
    status: RunStatus,
    started_at_ns: i64,
    finished_at_ns: i64,
    error: Option<&'a str>,
}

fn mark_run(conn: &Connection, run: RunRecord<'_>) -> Result<()> {
    let finished = match run.status {
        RunStatus::Pending | RunStatus::Running => None,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Skipped => Some(run.finished_at_ns),
    };
    conn.execute(
        "INSERT INTO workspace_analysis_runs
           (manifest_id, analyzer_id, analyzer_revision, config_hash,
            status, started_at_ns, finished_at_ns, error)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(manifest_id, analyzer_id) DO UPDATE SET
            analyzer_revision = excluded.analyzer_revision,
            config_hash = excluded.config_hash,
            status = excluded.status,
            started_at_ns = excluded.started_at_ns,
            finished_at_ns = excluded.finished_at_ns,
            error = excluded.error",
        params![
            run.manifest_id.0,
            run.analyzer_id,
            run.analyzer_revision,
            run.config_hash,
            run.status.as_str(),
            run.started_at_ns,
            finished,
            run.error,
        ],
    )?;
    Ok(())
}

/// Select the manifest entries this analyzer should see: those whose
/// blob was indexed under the analyzer's Tier-1 parser. This reuses
/// the indexer's backend dispatch (extension and shebang detection)
/// instead of maintaining a parallel extension table here.
fn workspace_files_for(
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

fn config_hash(repo_root: &Path, config_paths: &[&str]) -> String {
    let mut hasher = Sha1::new();
    for rel in config_paths {
        let path = repo_root.join(rel);
        if let Ok(bytes) = std::fs::read(&path) {
            hasher.update(rel.as_bytes());
            hasher.update([0]);
            hasher.update(bytes);
            hasher.update([0]);
        }
    }
    hex::encode(hasher.finalize())
}
