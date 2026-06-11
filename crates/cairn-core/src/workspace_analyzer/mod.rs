//! Workspace-level analyzer boundary.
//!
//! Per-language [`cairn_lang_api::Analyzer`] implementations operate
//! on one source blob at a time. LSP-class analyzers such as
//! rust-analyzer need a wider view: a repository root, a manifest, and
//! the set of files visible in that snapshot. This module defines that
//! boundary and persists facts emitted by registered workspace analyzers.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Re-exported so language crates can declare their pass's ref kind
// without depending on cairn-proto directly.
pub use cairn_proto::RefKind;
use linkme::distributed_slice;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::Error;
use crate::Result;
use crate::lsp::{Location, Position};
#[cfg(test)]
use crate::manifest::ManifestEntry;
use crate::manifest::ManifestId;
#[cfg(test)]
use crate::workspace_analyzer::persist::persist_resolved_refs;
#[cfg(test)]
use crate::workspace_analyzer::run::{
    run_workspace_analyzers, run_workspace_analyzers_with_timeout,
};

mod lsp_pass;
mod path;
mod persist;
mod run;

pub use lsp_pass::{
    DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, run_lsp_definition_pass,
};
pub use run::run_registered_workspace_analyzers;
pub(crate) use run::{
    ANALYZER_STALL_TIMEOUT, AnalyzerRunRequest, RunRecord, RunStatus, config_hash, mark_run,
    run_one_workspace_analyzer_with_timeout,
};

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

/// Shared progress beacon for one analyzer run. The runner watches it to
/// distinguish "still working" from "hung": the analyzer-side pass touches it
/// as work completes, and the stall detector only fires when it stops
/// advancing.
#[derive(Debug, Clone, Default)]
pub struct AnalyzerProgress(Arc<AtomicU64>);

impl AnalyzerProgress {
    pub fn tick(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
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
    /// explicit instead of guessing from language strings. The runner
    /// also selects this analyzer's input files by it: a manifest
    /// entry participates iff its blob was indexed under this parser,
    /// reusing the Tier-1 dispatch decision (extension and shebang)
    /// instead of re-deriving file patterns.
    fn parser_id(&self) -> &'static str;

    /// Repo-root-relative config files whose content feeds this
    /// analyzer's run staleness hash (`config_hash` on
    /// `workspace_analysis_runs`). Defaults to none, meaning only the
    /// analyzer revision invalidates prior runs.
    fn config_paths(&self) -> &'static [&'static str] {
        &[]
    }

    /// Analyzers that share one pooled LSP process return the same group id so
    /// the job scheduler never runs two of them concurrently. The shared pool
    /// serializes them anyway, and the waiter's analyzer timeout must not tick
    /// while it waits.
    fn pool_group(&self) -> Option<&'static str> {
        None
    }

    /// Analyze one manifest worth of files rooted at `repo_root`.
    fn analyze_workspace(
        &self,
        repo_root: &Path,
        manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
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
    /// How the source site uses the target symbol.
    pub kind: RefKind,
    /// Definition target returned by the analyzer.
    pub target: Location,
    /// Target path relative to the repository root when the analyzer
    /// can map the LSP URI back to a local file.
    pub target_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use rusqlite::{Connection, params};

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
            _progress: &AnalyzerProgress,
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
            _progress: &AnalyzerProgress,
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
            _progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            Err(Error::Lsp(crate::lsp::Error::ResponseError {
                code: crate::lsp::CONTENT_MODIFIED_ERROR_CODE,
                message: "content modified".into(),
            }))
        }
    }

    struct WorkspaceUnsuitableRustAnalyzer;

    impl WorkspaceAnalyzer for WorkspaceUnsuitableRustAnalyzer {
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
            _progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            Err(Error::Lsp(crate::lsp::Error::WorkspaceUnsuitable(
                "Gemfile without Gemfile.lock; run bundle install to enable ruby-lsp".into(),
            )))
        }
    }

    struct SlowRustAnalyzer;

    impl WorkspaceAnalyzer for SlowRustAnalyzer {
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
            _progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            std::thread::sleep(std::time::Duration::from_millis(200));
            Ok(WorkspaceFacts::default())
        }
    }

    struct ProgressingSlowRustAnalyzer;

    impl WorkspaceAnalyzer for ProgressingSlowRustAnalyzer {
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
            progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            for _ in 0..6 {
                progress.tick();
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Ok(WorkspaceFacts::default())
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
            .analyze_workspace(
                Path::new("/tmp/repo"),
                ManifestId(42),
                &files,
                &AnalyzerProgress::default(),
            )
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
                kind: RefKind::Call,
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
        assert_eq!(row.2, "tier3-rust-analyzer-lsp");
        assert!(row.3.is_some());
        assert_eq!(row.4, 7);
    }

    #[test]
    fn persist_resolved_refs_skips_out_of_repo_targets_and_clears_legacy_source() {
        let tmp = tempfile::tempdir().unwrap();
        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "source-sha";

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/main.rs', ?1)",
            params![source_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-rust', 1, 0)",
            params![source_sha],
        )
        .unwrap();
        // A leftover 0.1.x row written under the legacy source alias.
        conn.execute(
            "INSERT INTO refs
               (blob_sha, parser_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES (?1, 'tree-sitter-rust', 'old', 'crate::old', 'call',
                     0, 3, 1, 'tier3-rust-analyzer')",
            params![source_sha],
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
                kind: RefKind::Call,
                target: Location {
                    uri: crate::lsp::Url::from(
                        "file:///home/u/.cargo/registry/src/index/dep-1.0/src/lib.rs",
                    ),
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
                // Out-of-repo definition target: no manifest path.
                target_path: None,
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

        assert_eq!(inserted, 0);
        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM refs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 0, "legacy-source row should be cleared");
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
                kind: RefKind::Call,
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
                kind: RefKind::Call,
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

    #[test]
    fn workspace_unsuitable_run_is_skipped_with_reason() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_root = tmp.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src/main.rs"), "fn main() {}\n").unwrap();

        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "source-sha";
        let manifest_id = ManifestId(1);
        let entries = vec![ManifestEntry {
            path: "src/main.rs".into(),
            blob_sha: source_sha.into(),
        }];

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/main.rs', ?1)",
            params![source_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-rust', 1, 0)",
            params![source_sha],
        )
        .unwrap();

        let inserted = run_workspace_analyzers(
            &mut conn,
            &repo_root,
            manifest_id,
            &entries,
            10,
            vec![Box::new(WorkspaceUnsuitableRustAnalyzer)],
        )
        .unwrap();

        assert_eq!(inserted, 0);
        let (status, error): (String, String) = conn
            .query_row(
                "SELECT status, error FROM workspace_analysis_runs
                 WHERE manifest_id = 1 AND analyzer_id = 'rust-analyzer-lsp'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "skipped");
        assert!(error.contains("Gemfile without Gemfile.lock"));
        assert!(error.contains("run bundle install to enable ruby-lsp"));
    }

    #[test]
    fn analyzer_stall_records_timed_out_run_and_continues() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_root = tmp.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src/main.rs"), "fn main() {}\n").unwrap();

        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "source-sha";
        let manifest_id = ManifestId(1);
        let entries = vec![ManifestEntry {
            path: "src/main.rs".into(),
            blob_sha: source_sha.into(),
        }];

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/main.rs', ?1)",
            params![source_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-rust', 1, 0)",
            params![source_sha],
        )
        .unwrap();

        let inserted = run_workspace_analyzers_with_timeout(
            &mut conn,
            &repo_root,
            manifest_id,
            &entries,
            10,
            vec![Box::new(SlowRustAnalyzer)],
            std::time::Duration::from_millis(10),
        )
        .unwrap();

        assert_eq!(inserted, 0);
        let row: (String, Option<String>) = conn
            .query_row(
                "SELECT status, error FROM workspace_analysis_runs
                 WHERE manifest_id = 1 AND analyzer_id = 'rust-analyzer-lsp'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row.0, "timed_out");
        assert!(
            row.1
                .as_deref()
                .is_some_and(|error| error.contains("analyzer stalled"))
        );
    }

    #[test]
    fn analyzer_that_keeps_ticking_past_stall_window_completes() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_root = tmp.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src/main.rs"), "fn main() {}\n").unwrap();

        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "source-sha";
        let manifest_id = ManifestId(1);
        let entries = vec![ManifestEntry {
            path: "src/main.rs".into(),
            blob_sha: source_sha.into(),
        }];

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/main.rs', ?1)",
            params![source_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-rust', 1, 0)",
            params![source_sha],
        )
        .unwrap();

        let inserted = run_workspace_analyzers_with_timeout(
            &mut conn,
            &repo_root,
            manifest_id,
            &entries,
            10,
            vec![Box::new(ProgressingSlowRustAnalyzer)],
            std::time::Duration::from_millis(100),
        )
        .unwrap();

        assert_eq!(inserted, 0);
        let row: (String, Option<String>) = conn
            .query_row(
                "SELECT status, error FROM workspace_analysis_runs
                 WHERE manifest_id = 1 AND analyzer_id = 'rust-analyzer-lsp'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row.0, "succeeded");
        assert_eq!(row.1, None);
    }

    fn tier3_ref_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM refs WHERE source = 'tier3-rust-analyzer-lsp'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }
}
