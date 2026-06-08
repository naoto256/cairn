use std::path::Path;

use rusqlite::{Connection, params};
use sha1::{Digest, Sha1};
use tracing::{debug, warn};

use crate::manifest::{ManifestEntry, ManifestId};
use crate::{Error, Result};

use super::persist::persist_resolved_refs;
use super::{WorkspaceAnalyzer, WorkspaceFile, all_workspace_analyzers};

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
    run_workspace_analyzers(
        conn,
        repo_root,
        manifest_id,
        entries,
        now_ns,
        all_workspace_analyzers(),
    )
}

pub(super) fn run_workspace_analyzers(
    conn: &mut Connection,
    repo_root: &Path,
    manifest_id: ManifestId,
    entries: &[ManifestEntry],
    now_ns: i64,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
) -> Result<usize> {
    let mut inserted = 0;
    let config_hash = config_hash(repo_root);

    for analyzer in analyzers {
        let files = workspace_files_for(analyzer.language(), repo_root, entries);
        if files.is_empty() {
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id: analyzer.id(),
                    analyzer_revision: analyzer.revision(),
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
                analyzer_id: analyzer.id(),
                analyzer_revision: analyzer.revision(),
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
                analyzer_id: analyzer.id(),
                analyzer_revision: analyzer.revision(),
                config_hash: &config_hash,
                status: RunStatus::Running,
                started_at_ns: now_ns,
                finished_at_ns: now_ns,
                error: None,
            },
        )?;

        match analyzer.analyze_workspace(repo_root, manifest_id, &files) {
            Ok(facts) => {
                let n = persist_resolved_refs(
                    conn,
                    manifest_id,
                    analyzer.id(),
                    analyzer.parser_id(),
                    &facts,
                )?;
                inserted += n;
                mark_run(
                    conn,
                    RunRecord {
                        manifest_id,
                        analyzer_id: analyzer.id(),
                        analyzer_revision: analyzer.revision(),
                        config_hash: &config_hash,
                        status: RunStatus::Succeeded,
                        started_at_ns: now_ns,
                        finished_at_ns: now_ns,
                        error: None,
                    },
                )?;
            }
            Err(err) => {
                let message = err.to_string();
                let status = if is_content_modified_error(&err) {
                    debug!(
                        analyzer_id = analyzer.id(),
                        error = %message,
                        "transient: rust-analyzer content-modified during run"
                    );
                    RunStatus::Skipped
                } else if message.contains("LSP binary not available") {
                    RunStatus::Skipped
                } else {
                    warn!(
                        analyzer_id = analyzer.id(),
                        error = %message,
                        "workspace analyzer failed"
                    );
                    RunStatus::Failed
                };
                mark_run(
                    conn,
                    RunRecord {
                        manifest_id,
                        analyzer_id: analyzer.id(),
                        analyzer_revision: analyzer.revision(),
                        config_hash: &config_hash,
                        status,
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

fn is_content_modified_error(err: &Error) -> bool {
    matches!(err, Error::Lsp(lsp_err) if lsp_err.is_content_modified())
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

fn workspace_files_for(
    language: &str,
    repo_root: &Path,
    entries: &[ManifestEntry],
) -> Vec<WorkspaceFile> {
    entries
        .iter()
        .filter(|entry| file_matches_language(&entry.path, language))
        .map(|entry| {
            let worktree_path = repo_root.join(&entry.path);
            WorkspaceFile {
                path: entry.path.clone(),
                blob_sha: entry.blob_sha.clone(),
                worktree_path: worktree_path.exists().then_some(worktree_path),
            }
        })
        .collect()
}

fn file_matches_language(path: &str, language: &str) -> bool {
    match language {
        "rust" => path.ends_with(".rs"),
        "python" => path.ends_with(".py"),
        "typescript" => path.ends_with(".ts") || path.ends_with(".tsx"),
        "go" => path.ends_with(".go"),
        "markdown" => path.ends_with(".md") || path.ends_with(".markdown"),
        _ => false,
    }
}

fn config_hash(repo_root: &Path) -> String {
    let mut hasher = Sha1::new();
    for rel in ["Cargo.toml", "rust-toolchain.toml", "rust-toolchain"] {
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
