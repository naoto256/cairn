//! Workspace-level analyzer boundary.
//!
//! Per-language [`cairn_lang_api::Analyzer`] implementations operate
//! on one source blob at a time. LSP-class analyzers such as
//! rust-analyzer need a wider view: a repository root, a manifest, and
//! the set of files visible in that snapshot. This module defines that
//! boundary and persists facts emitted by registered workspace analyzers.

use std::path::{Path, PathBuf};

use cairn_proto::RefKind;
use linkme::distributed_slice;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tracing::{debug, warn};

use crate::cas::kind_conv::ref_kind_to_str;
use crate::lsp::{Location, Position};
use crate::manifest::{ManifestEntry, ManifestId};
use crate::{Error, Result};

/// Linker-time registry of workspace analyzers.
///
/// Future analyzer crates or modules contribute constructors with
/// `#[distributed_slice(WORKSPACE_ANALYZERS)]`, mirroring the language
/// backend and JSON-RPC method registries.
#[allow(unsafe_code)]
#[distributed_slice]
pub static WORKSPACE_ANALYZERS: [fn() -> Box<dyn WorkspaceAnalyzer>] = [..];

/// Collect every registered workspace analyzer.
#[must_use]
pub fn all_workspace_analyzers() -> Vec<Box<dyn WorkspaceAnalyzer>> {
    WORKSPACE_ANALYZERS.iter().map(|ctor| ctor()).collect()
}

/// Analyzer that can derive facts from a repository snapshot.
pub trait WorkspaceAnalyzer: Send + Sync {
    /// Stable analyzer identifier, e.g. `"rust-analyzer-lsp"`.
    fn id(&self) -> &'static str;

    /// Monotonic revision for this analyzer's output.
    fn revision(&self) -> u32;

    /// Short language tag this analyzer enriches, e.g. `"rust"`.
    fn language(&self) -> &'static str;

    /// Parser id whose Tier-1 symbols/refs this analyzer enriches.
    /// Keeping this on the analyzer makes the persistence boundary
    /// explicit instead of guessing from language strings.
    fn parser_id(&self) -> &'static str;

    /// Analyze one manifest worth of files rooted at `repo_root`.
    fn analyze_workspace(
        &self,
        repo_root: &Path,
        manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts>;
}

/// One file visible to a [`WorkspaceAnalyzer`] within a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceFile {
    /// Path relative to the registered repository root.
    pub path: String,
    /// Blob SHA recorded by the manifest for this path.
    pub blob_sha: String,
    /// Absolute path when the file is materialized in the worktree.
    pub worktree_path: Option<PathBuf>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceFacts {
    pub resolved_refs: Vec<ResolvedRef>,
}

/// A reference resolved by a workspace analyzer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedRef {
    /// Source path relative to the registered repository root.
    pub source_path: String,
    /// LSP position of the source method identifier, zero-based.
    pub source_position: Position,
    /// Byte range of the source method identifier.
    pub source_byte_range: std::ops::Range<usize>,
    /// Definition target returned by the analyzer.
    pub target: Location,
    /// Target path relative to the repository root when the analyzer
    /// can map the LSP URI back to a local file.
    pub target_path: Option<String>,
}

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

fn run_workspace_analyzers(
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

fn persist_resolved_refs(
    conn: &mut Connection,
    manifest_id: ManifestId,
    analyzer_id: &str,
    parser_id: &str,
    facts: &WorkspaceFacts,
) -> Result<usize> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM refs
          WHERE source = ?1
            AND blob_sha IN (
                SELECT blob_sha FROM manifest_entries WHERE manifest_id = ?2
            )",
        params![ref_source(analyzer_id), manifest_id.0],
    )?;

    let mut inserted = 0;
    for r in &facts.resolved_refs {
        let Some(source_blob) = blob_for_path(&tx, manifest_id, &r.source_path)? else {
            continue;
        };
        let Some(parser_id) = parser_for_blob(&tx, &source_blob, parser_id)? else {
            continue;
        };
        let Some((target_qualified, target_name)) = target_symbol_for_location(
            &tx,
            manifest_id,
            &parser_id,
            r.target_path.as_deref(),
            &r.target,
        )?
        else {
            continue;
        };
        let enclosing_id = enclosing_symbol_for_ref(
            &tx,
            &source_blob,
            &parser_id,
            r.source_byte_range.start,
            r.source_byte_range.end,
        )?;
        let byte_start = i64::try_from(r.source_byte_range.start).unwrap_or(i64::MAX);
        let byte_end = i64::try_from(r.source_byte_range.end).unwrap_or(i64::MAX);
        let line = i64::from(r.source_position.line.saturating_add(1));
        tx.execute(
            "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified,
                kind, type_role, byte_start, byte_end, line, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, ?10)",
            params![
                source_blob,
                parser_id,
                enclosing_id,
                target_name,
                target_qualified,
                ref_kind_to_str(RefKind::Call),
                byte_start,
                byte_end,
                line,
                ref_source(analyzer_id),
            ],
        )?;
        inserted += 1;
    }

    tx.commit()?;
    Ok(inserted)
}

fn ref_source(analyzer_id: &str) -> String {
    if analyzer_id == "rust-analyzer-lsp" {
        return "tier3-rust-analyzer".to_string();
    }
    format!("tier3-{analyzer_id}")
}

fn blob_for_path(conn: &Connection, manifest_id: ManifestId, path: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT blob_sha FROM manifest_entries
             WHERE manifest_id = ?1 AND path = ?2",
            params![manifest_id.0, path],
            |r| r.get(0),
        )
        .optional()?)
}

fn parser_for_blob(conn: &Connection, blob_sha: &str, parser_id: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT parser_id FROM blobs
             WHERE blob_sha = ?1 AND parser_id = ?2
             LIMIT 1",
            params![blob_sha, parser_id],
            |r| r.get(0),
        )
        .optional()?)
}

fn target_symbol_for_location(
    conn: &Connection,
    manifest_id: ManifestId,
    parser_id: &str,
    target_path: Option<&str>,
    location: &Location,
) -> Result<Option<(String, String)>> {
    let Some(path) = target_path
        .map(str::to_string)
        .or_else(|| file_uri_to_manifest_path(location.uri.as_str()))
    else {
        return Ok(None);
    };
    let Some(blob_sha) = blob_for_path(conn, manifest_id, &path)? else {
        return Ok(None);
    };
    let line = i64::from(location.range.start.line.saturating_add(1));
    Ok(conn
        .query_row(
            "SELECT qualified, name FROM symbols
             WHERE blob_sha = ?1
               AND parser_id = ?2
               AND line_start <= ?3 AND line_end >= ?3
               AND kind IN ('function', 'method', 'test')
             ORDER BY (line_end - line_start) ASC, line_start DESC
             LIMIT 1",
            params![blob_sha, parser_id, line],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?)
}

fn enclosing_symbol_for_ref(
    conn: &Connection,
    blob_sha: &str,
    parser_id: &str,
    byte_start: usize,
    byte_end: usize,
) -> Result<Option<i64>> {
    let start = i64::try_from(byte_start).unwrap_or(i64::MAX);
    let end = i64::try_from(byte_end).unwrap_or(i64::MAX);
    Ok(conn
        .query_row(
            "SELECT id FROM symbols
             WHERE blob_sha = ?1
               AND parser_id = ?2
               AND byte_start <= ?3 AND byte_end >= ?4
               AND kind IN ('function', 'method', 'test')
             ORDER BY (byte_end - byte_start) ASC
             LIMIT 1",
            params![blob_sha, parser_id, start, end],
            |r| r.get(0),
        )
        .optional()?)
}

fn file_uri_to_manifest_path(uri: &str) -> Option<String> {
    let path = uri.strip_prefix("file://")?;
    let decoded = percent_decode(path)?;
    let marker = "/src/";
    if let Some(idx) = decoded.find(marker) {
        return Some(decoded[idx + 1..].to_string());
    }
    decoded.rsplit_once('/').map(|(_, name)| name.to_string())
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = *bytes.get(i + 1)?;
            let lo = *bytes.get(i + 2)?;
            out.push(hex_value(hi)? * 16 + hex_value(lo)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeWorkspaceAnalyzer;

    impl WorkspaceAnalyzer for FakeWorkspaceAnalyzer {
        fn id(&self) -> &'static str {
            "fake-workspace"
        }

        fn revision(&self) -> u32 {
            7
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
        ) -> Result<WorkspaceFacts> {
            Ok(WorkspaceFacts::default())
        }
    }

    #[allow(unsafe_code)]
    #[distributed_slice(WORKSPACE_ANALYZERS)]
    static FAKE_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
        || Box::new(FakeWorkspaceAnalyzer);

    struct SuccessfulRustAnalyzer {
        facts: WorkspaceFacts,
    }

    impl WorkspaceAnalyzer for SuccessfulRustAnalyzer {
        fn id(&self) -> &'static str {
            "rust-analyzer-lsp"
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            "rust"
        }

        fn parser_id(&self) -> &'static str {
            "tree-sitter-rust"
        }

        fn analyze_workspace(
            &self,
            _repo_root: &Path,
            _manifest_id: ManifestId,
            _files: &[WorkspaceFile],
        ) -> Result<WorkspaceFacts> {
            Ok(self.facts.clone())
        }
    }

    struct ContentModifiedRustAnalyzer;

    impl WorkspaceAnalyzer for ContentModifiedRustAnalyzer {
        fn id(&self) -> &'static str {
            "rust-analyzer-lsp"
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            "rust"
        }

        fn parser_id(&self) -> &'static str {
            "tree-sitter-rust"
        }

        fn analyze_workspace(
            &self,
            _repo_root: &Path,
            _manifest_id: ManifestId,
            _files: &[WorkspaceFile],
        ) -> Result<WorkspaceFacts> {
            Err(Error::Lsp(crate::lsp::Error::ResponseError {
                code: crate::lsp::CONTENT_MODIFIED_ERROR_CODE,
                message: "content modified".into(),
            }))
        }
    }

    #[test]
    fn discovers_registered_workspace_analyzer() {
        let analyzers = all_workspace_analyzers();
        let fake = analyzers
            .iter()
            .find(|a| a.id() == "fake-workspace")
            .expect("fake workspace analyzer should be registered");

        assert_eq!(fake.revision(), 7);
        assert_eq!(fake.language(), "fake");
    }

    #[test]
    fn workspace_analyzer_boundary_accepts_manifest_context() {
        let analyzer = FakeWorkspaceAnalyzer;
        let files = [WorkspaceFile {
            path: "src/lib.rs".into(),
            blob_sha: "sha1".into(),
            worktree_path: Some(PathBuf::from("/tmp/repo/src/lib.rs")),
        }];

        let facts = analyzer
            .analyze_workspace(Path::new("/tmp/repo"), ManifestId(42), &files)
            .unwrap();

        assert_eq!(facts, WorkspaceFacts::default());
    }

    #[test]
    fn persist_resolved_refs_maps_lsp_locations_to_refs() {
        let tmp = tempfile::tempdir().unwrap();
        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "source-sha";
        let target_sha = "target-sha";

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/main.rs', ?1), (1, 'src/lib.rs', ?2)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-rust', 1, 0), (?2, 'tree-sitter-rust', 1, 0)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (?1, 'tree-sitter-rust', 'main', 'crate::main', 'function',
                0, 200, 1, 10, 'syn'),
               (?2, 'tree-sitter-rust', 'bar', 'crate::Foo::bar', 'method',
                20, 80, 3, 5, 'syn')",
            params![source_sha, target_sha],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: vec![ResolvedRef {
                source_path: "src/main.rs".to_string(),
                source_position: Position {
                    line: 6,
                    character: 8,
                },
                source_byte_range: 40..43,
                target: Location {
                    uri: crate::lsp::Url::from("file:///repo/src/lib.rs"),
                    range: crate::lsp::Range {
                        start: Position {
                            line: 2,
                            character: 7,
                        },
                        end: Position {
                            line: 2,
                            character: 10,
                        },
                    },
                },
                target_path: Some("src/lib.rs".to_string()),
            }],
        };

        let inserted = persist_resolved_refs(
            &mut conn,
            ManifestId(1),
            "rust-analyzer-lsp",
            "tree-sitter-rust",
            &facts,
        )
        .unwrap();

        assert_eq!(inserted, 1);
        let row: (String, String, String, Option<i64>, i64) = conn
            .query_row(
                "SELECT target_name, target_qualified, source, enclosing_id, line
                 FROM refs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(row.0, "bar");
        assert_eq!(row.1, "crate::Foo::bar");
        assert_eq!(row.2, "tier3-rust-analyzer");
        assert!(row.3.is_some());
        assert_eq!(row.4, 7);
    }

    #[test]
    fn persist_resolved_refs_uses_analyzer_parser_id_for_python_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "shared-source-sha";
        let target_sha = "shared-target-sha";

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'pkg/b.py', ?1), (1, 'pkg/a.py', ?2)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES
               (?1, 'tree-sitter-go', 1, 0),
               (?1, 'tree-sitter-python', 1, 0),
               (?2, 'tree-sitter-python', 1, 0),
               (?2, 'tree-sitter-rust', 1, 0)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (?1, 'tree-sitter-python', 'run', 'run', 'function',
                0, 200, 1, 10, 'python'),
               (?2, 'tree-sitter-rust', 'wrong', 'rust::wrong', 'method',
                0, 10, 2, 2, 'syn'),
               (?2, 'tree-sitter-python', 'm', 'A.m', 'method',
                0, 30, 2, 3, 'python')",
            params![source_sha, target_sha],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: vec![ResolvedRef {
                source_path: "pkg/b.py".to_string(),
                source_position: Position {
                    line: 5,
                    character: 6,
                },
                source_byte_range: 42..43,
                target: Location {
                    uri: crate::lsp::Url::from("file:///repo/pkg/a.py"),
                    range: crate::lsp::Range {
                        start: Position {
                            line: 1,
                            character: 8,
                        },
                        end: Position {
                            line: 1,
                            character: 9,
                        },
                    },
                },
                target_path: Some("pkg/a.py".to_string()),
            }],
        };

        let inserted = persist_resolved_refs(
            &mut conn,
            ManifestId(1),
            "pyright-lsp",
            "tree-sitter-python",
            &facts,
        )
        .unwrap();

        assert_eq!(inserted, 1);
        let row: (String, String, String) = conn
            .query_row(
                "SELECT parser_id, target_name, target_qualified FROM refs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row.0, "tree-sitter-python");
        assert_eq!(row.1, "m");
        assert_eq!(row.2, "A.m");
    }

    #[test]
    fn content_modified_run_is_skipped_without_deleting_prior_refs() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_root = tmp.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src/main.rs"), "fn main() { foo(); }\n").unwrap();
        std::fs::write(repo_root.join("src/lib.rs"), "pub fn foo() {}\n").unwrap();

        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "source-sha";
        let target_sha = "target-sha";
        let manifest_id = ManifestId(1);
        let entries = vec![
            ManifestEntry {
                path: "src/main.rs".into(),
                blob_sha: source_sha.into(),
            },
            ManifestEntry {
                path: "src/lib.rs".into(),
                blob_sha: target_sha.into(),
            },
        ];

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/main.rs', ?1), (1, 'src/lib.rs', ?2)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-rust', 1, 0), (?2, 'tree-sitter-rust', 1, 0)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (?1, 'tree-sitter-rust', 'main', 'crate::main', 'function',
                0, 200, 1, 10, 'syn'),
               (?2, 'tree-sitter-rust', 'foo', 'crate::foo', 'function',
                0, 20, 1, 1, 'syn')",
            params![source_sha, target_sha],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: vec![ResolvedRef {
                source_path: "src/main.rs".to_string(),
                source_position: Position {
                    line: 0,
                    character: 12,
                },
                source_byte_range: 12..15,
                target: Location {
                    uri: crate::lsp::Url::from("file:///repo/src/lib.rs"),
                    range: crate::lsp::Range {
                        start: Position {
                            line: 0,
                            character: 7,
                        },
                        end: Position {
                            line: 0,
                            character: 10,
                        },
                    },
                },
                target_path: Some("src/lib.rs".to_string()),
            }],
        };

        let inserted = run_workspace_analyzers(
            &mut conn,
            &repo_root,
            manifest_id,
            &entries,
            10,
            vec![Box::new(SuccessfulRustAnalyzer { facts })],
        )
        .unwrap();
        assert_eq!(inserted, 1);
        assert_eq!(tier3_ref_count(&conn), 1);

        let inserted = run_workspace_analyzers(
            &mut conn,
            &repo_root,
            manifest_id,
            &entries,
            20,
            vec![Box::new(ContentModifiedRustAnalyzer)],
        )
        .unwrap();

        assert_eq!(inserted, 0);
        assert_eq!(tier3_ref_count(&conn), 1);
        let status: String = conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = 1 AND analyzer_id = 'rust-analyzer-lsp'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "skipped");
    }

    fn tier3_ref_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM refs WHERE source = 'tier3-rust-analyzer'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }
}
