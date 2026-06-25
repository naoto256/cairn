//! Workspace-level analyzer boundary.
//!
//! Per-language [`cairn_lang_api::Analyzer`] implementations operate
//! on one source blob at a time. Workspace-scoped analyzers need a
//! wider view: a repository root, a manifest, and the set of files
//! visible in that snapshot. This module defines that boundary and
//! persists facts emitted by registered workspace analyzers.
//!
//! Concrete backends span the spectrum from a tree-sitter-driven
//! cross-file pass that runs entirely in-process to an LSP-class
//! analyzer such as rust-analyzer that talks to a long-lived language
//! server; both implement the same [`WorkspaceAnalyzer`] trait.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// Re-exported so language crates can declare their pass's ref kind
// without depending on cairn-proto directly.
pub use cairn_proto::RefKind;
use linkme::distributed_slice;
use serde::{Deserialize, Serialize};

pub use crate::resolution::{ResolutionKind, SemanticKind};

#[cfg(test)]
use crate::Error;
use crate::Result;
use crate::lsp::{Location, Position};
#[cfg(test)]
use crate::manifest::ManifestEntry;
use crate::manifest::ManifestId;
#[cfg(test)]
use crate::workspace_analyzer::persist::{persist_resolutions, persist_resolved_refs};
#[cfg(test)]
use crate::workspace_analyzer::run::{
    run_workspace_analyzers, run_workspace_analyzers_with_timeout,
};

mod expected;
mod lsp_pass;
mod path;
mod persist;
mod run;

pub use expected::expected_analyzers_for_manifest;
pub(crate) use expected::manifest_parser_ids;
pub use lsp_pass::{
    DefinitionRetryPolicy, DefinitionSite, LspDefinitionCollector, LspDefinitionPass,
    LspMultiKindDefinitionPass, run_lsp_definition_pass, run_lsp_multi_kind_definition_pass,
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
#[derive(Debug, Default)]
struct AnalyzerProgressState {
    ticks: AtomicU64,
    cancelled: AtomicBool,
}

/// Cloneable progress and cancellation handle shared between the runner and
/// one workspace analyzer invocation. Ticks are monotonic liveness beacons, not
/// a file count contract.
#[derive(Clone, Default)]
pub struct AnalyzerProgress {
    state: Arc<AnalyzerProgressState>,
    observer: Option<AnalyzerProgressObserver>,
}

pub(crate) type AnalyzerProgressObserver = Arc<dyn Fn(u64) + Send + Sync + 'static>;

impl std::fmt::Debug for AnalyzerProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyzerProgress")
            .field("ticks", &self.snapshot())
            .field("cancelled", &self.is_cancelled())
            .field("has_observer", &self.observer.is_some())
            .finish()
    }
}

impl AnalyzerProgress {
    #[must_use]
    pub(crate) fn with_observer(observer: AnalyzerProgressObserver) -> Self {
        Self {
            state: Arc::default(),
            observer: Some(observer),
        }
    }

    /// Marks forward progress for stall detection. An analyzer should call this
    /// after bounded units of work such as a file, request, or retry completes.
    pub fn tick(&self) {
        let ticks = self.state.ticks.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(observer) = &self.observer {
            observer(ticks);
        }
    }

    #[must_use]
    /// Returns the current liveness tick value observed by the runner.
    pub fn snapshot(&self) -> u64 {
        self.state.ticks.load(Ordering::Relaxed)
    }

    pub(crate) fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Relaxed);
    }

    #[must_use]
    /// Reports whether the runner has asked this invocation to stop work.
    /// Implementations should check this between expensive operations and
    /// return promptly when it becomes true.
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Relaxed)
    }
}

/// Stable provenance prefixes recorded in `refs.source` for facts emitted by
/// registered workspace analyzers.
///
/// The first character of `refs.source` selects the tier: `tier3-<analyzer_id>`
/// for LSP-class analyzers, `tier25-<analyzer_id>` once the tree-sitter-based
/// cross-file pass lands, and so on. Listing the known prefixes here lets SQL
/// helpers ([`source_rank_case_sql`], [`source_is_workspace_tier_sql`]) and
/// dedup/noise-suppression code stay tier-agnostic — a new tier only needs to
/// (a) extend this constant and (b) point its analyzer at the new prefix via
/// [`WorkspaceAnalyzer::tier_prefix`].
pub const WORKSPACE_TIER_PREFIXES: &[&str] = &["tier3", "tier25"];

/// Source string written by tree-sitter Tier-1 passes for backends that ship
/// a single-file semantic enricher (`'rust-syn'` etc.). Workspace-analyzer
/// output ranks ahead of this; we hardcode the known one for now and can lift
/// it into the same tier-prefix table once a Tier-2.5 analyzer registers.
const TIER2_NATIVE_SOURCES: &[&str] = &["rust-syn"];

/// Builds an SQL `CASE` expression that ranks provenance strings from
/// most authoritative (lowest number) to least.
///
/// Rank order:
/// 0. `tier3-*` — LSP / full resolver output (most authoritative).
/// 1. `tier25-*` — tree-sitter-based cross-file Tier-2.5 pass.
/// 2. `tier2-direct-*` — Tier-2 backends emitting resolutions where the
///    grammar shape unambiguously implies the semantic kind
///    (Phase 3 of the Tier-2.5 prep work).
/// 3. `rust-syn` and other [`TIER2_NATIVE_SOURCES`] — Tier-1 / Tier-2
///    enrichers.
/// 4. Everything else, including fact-layer fallbacks read from
///    `implementations.kind` without a resolution row.
///
/// `column` is interpolated as-is into the SQL; only pass a static identifier
/// (e.g. `"r.source"`), never user input.
#[must_use]
pub fn source_rank_case_sql(column: &str) -> String {
    let mut sql = String::from("CASE");
    // Tier-3 (LSP-class) is most authoritative; Tier-2.5 (in-process cross-file)
    // ranks below it. We list tier25 first so its LIKE matches before the
    // generic tier3 prefix loop below, then exclude tier25 from that loop.
    sql.push_str(&format!(" WHEN {column} LIKE 'tier25-%' THEN 1"));
    for prefix in WORKSPACE_TIER_PREFIXES {
        if *prefix == "tier25" {
            continue;
        }
        sql.push_str(&format!(" WHEN {column} LIKE '{prefix}-%' THEN 0"));
    }
    sql.push_str(&format!(" WHEN {column} LIKE 'tier2-direct-%' THEN 2"));
    for source in TIER2_NATIVE_SOURCES {
        sql.push_str(&format!(" WHEN {column} = '{source}' THEN 3"));
    }
    sql.push_str(" ELSE 4 END");
    sql
}

/// Builds an SQL predicate matching any workspace-tier provenance prefix.
///
/// `column` is interpolated as-is; only pass a static identifier.
#[must_use]
pub fn source_is_workspace_tier_sql(column: &str) -> String {
    WORKSPACE_TIER_PREFIXES
        .iter()
        .map(|prefix| format!("{column} LIKE '{prefix}-%'"))
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Analyzer that can derive facts from a repository snapshot.
pub trait WorkspaceAnalyzer: Send + Sync {
    /// Stable analyzer identifier, e.g. `"rust-analyzer-lsp"`.
    /// This value keys run records, pool groups, and persisted provenance, so
    /// it must only change when the old output should be abandoned.
    fn id(&self) -> &'static str;

    /// Tier prefix used when persisting facts emitted by this analyzer.
    /// Defaults to `"tier3"` so existing LSP-class analyzers keep writing
    /// `tier3-<analyzer_id>` into `refs.source`. A future tree-sitter-based
    /// cross-file pass overrides this with `"tier25"` (or similar) and gets
    /// listed in [`WORKSPACE_TIER_PREFIXES`].
    fn tier_prefix(&self) -> &'static str {
        "tier3"
    }

    /// Monotonic revision for this analyzer's output.
    /// Bump it when persisted facts need to be recomputed even if inputs and
    /// config files have not changed.
    fn revision(&self) -> u32;

    /// Short language tag this analyzer enriches, e.g. `"rust"`.
    /// The tag is user-facing metadata; input selection is governed by
    /// [`Self::parser_id`].
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
    /// The runner calls this once per selected manifest/analyzer pair and only
    /// persists facts returned in `Ok`; errors leave prior successful output
    /// untouched for that run key.
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
/// Facts emitted by a workspace analyzer for later CAS persistence.
/// Fields should stay optional-by-absence: an empty vector means the analyzer
/// found no facts of that kind, not that downstream persistence should infer
/// defaults.
pub struct WorkspaceFacts {
    pub resolved_refs: Vec<ResolvedRef>,
    /// Resolution-layer rows emitted by Tier-2.5+ analyzers. Persisted into the
    /// `resolutions` table by [`crate::workspace_analyzer::persist`]. Empty by
    /// default so existing LSP-class analyzers (Tier-3) keep returning facts
    /// unchanged.
    #[serde(default)]
    pub resolutions: Vec<WorkspaceResolution>,
}

/// One persistence-shaped resolution emitted by a workspace analyzer.
///
/// Two orthogonal axes carry the resolved target:
///
/// - [`Self::target_path`] — source of truth for "which workspace file the
///   target lives in". Set whenever the analyzer resolved to a
///   workspace-internal target, regardless of whether the target is a symbol
///   (class, function, method) or a module / file (`require_relative './db'`,
///   `import './foo'`). Flows into `resolutions.target_path` after the
///   persist layer validates the path against the manifest (analyzer bugs
///   that emit a phantom path are dropped to NULL with a `debug!` log).
/// - [`Self::target_qualified`] — optional best-effort symbol qualified name.
///   Persist layer feeds it into a `(blob_sha, parser_id, qualified)` symbols
///   lookup (with a cross-parser-id uniqueness-checked fallback) to populate
///   `resolutions.target_symbol_id`. May be `None` even when `target_path` is
///   `Some` — import edges intentionally leave it `None` because the target
///   is a file, not a symbol.
///
/// The two columns persist independently. Three wire-observable shapes
/// result:
///
/// - `target_path = Some, target_symbol_id = Some` — workspace file *and*
///   symbol pinned. Same-parser type/call edges, and cross-parser type/call
///   edges where the `(blob_sha, qualified)` pair (or the manifest-wide
///   `qualified`) had exactly one match.
/// - `target_path = Some, target_symbol_id = None` — workspace file pinned,
///   no symbol-level identity. Canonical for import edges (target is a file,
///   not a symbol) and for type/call edges where the symbol lookup hit
///   nothing or hit ambiguously.
/// - `target_path = None, target_symbol_id = None` — site observed, target
///   unresolved. Bare specifiers, external dependencies, stdlib, and the
///   `PathOrigin::PhantomDropped` case where the analyzer emitted a path
///   that does not exist in the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceResolution {
    /// Source path relative to the registered repository root.
    pub source_path: String,
    /// Byte range of the site token inside `source_path`'s blob.
    pub site_byte_range: std::ops::Range<u32>,
    /// Site class.
    pub kind: ResolutionKind,
    /// Inheritance / conformance flavour; only meaningful when
    /// [`Self::kind`] is [`ResolutionKind::Type`].
    pub semantic_kind: Option<SemanticKind>,
    /// Repo-root-relative path of the file containing the resolved target.
    /// `None` when unresolved. See struct doc for the orthogonal relationship
    /// with [`Self::target_qualified`].
    pub target_path: Option<String>,
    /// Optional symbol qualified name (looked up against `symbols.qualified`
    /// to populate `target_symbol_id`). Leave `None` for import edges and
    /// any other target whose primary identity is a file path rather than a
    /// symbol. See struct doc.
    pub target_qualified: Option<String>,
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
                std::thread::sleep(std::time::Duration::from_millis(200));
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
            resolutions: Vec::new(),
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
            "tier3",
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
            resolutions: Vec::new(),
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
                        "file:///tmp/.cargo/registry/src/index/dep-1.0/src/lib.rs",
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
            "tier3",
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
            resolutions: Vec::new(),
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
            "tier3",
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
            resolutions: Vec::new(),
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
            std::time::Duration::from_secs(1),
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

    #[test]
    fn persist_resolutions_roundtrips_resolved_and_unresolved_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        let source_sha = "site-sha";
        let target_sha = "target-sha";

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'main.rb', ?1), (1, 'foo.rb', ?2)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'tree-sitter-ruby', 1, 0), (?2, 'tree-sitter-ruby', 1, 0)",
            params![source_sha, target_sha],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES (?1, 'tree-sitter-ruby', 'Foo', 'Foo', 'class',
                     0, 20, 1, 3, 'ruby')",
            params![target_sha],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![
                // Resolved type reference: Foo on main.rb resolves to Foo in foo.rb.
                WorkspaceResolution {
                    source_path: "main.rb".into(),
                    site_byte_range: 10..13,
                    kind: ResolutionKind::Type,
                    semantic_kind: None,
                    target_path: Some("foo.rb".into()),
                    target_qualified: Some("Foo".into()),
                },
                // Unresolved: target absent.
                WorkspaceResolution {
                    source_path: "main.rb".into(),
                    site_byte_range: 20..24,
                    kind: ResolutionKind::Call,
                    semantic_kind: None,
                    target_path: None,
                    target_qualified: None,
                },
            ],
        };

        let inserted = persist_resolutions(
            &mut conn,
            ManifestId(1),
            "ruby-resolver",
            "tier25",
            "tree-sitter-ruby",
            &facts,
        )
        .unwrap();
        assert_eq!(inserted, 2);

        let rows: Vec<(i64, i64, String, Option<i64>, String)> = conn
            .prepare(
                "SELECT site_byte_start, site_byte_end, kind, target_symbol_id, source
                 FROM resolutions ORDER BY site_byte_start",
            )
            .unwrap()
            .query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].2, "type");
        assert!(rows[0].3.is_some(), "Foo should resolve to symbol id");
        assert_eq!(rows[0].4, "tier25-ruby-resolver");
        assert_eq!(rows[1].2, "call");
        assert!(
            rows[1].3.is_none(),
            "unresolved row keeps target_symbol_id NULL"
        );

        // Re-running with one fewer row should delete the existing rows for
        // this source and reinsert the new set.
        let facts2 = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "main.rb".into(),
                site_byte_range: 30..33,
                kind: ResolutionKind::Type,
                semantic_kind: Some(SemanticKind::Inherit),
                target_path: Some("foo.rb".into()),
                target_qualified: Some("Foo".into()),
            }],
        };
        let inserted = persist_resolutions(
            &mut conn,
            ManifestId(1),
            "ruby-resolver",
            "tier25",
            "tree-sitter-ruby",
            &facts2,
        )
        .unwrap();
        assert_eq!(inserted, 1);
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resolutions WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        let semantic: Option<String> = conn
            .query_row(
                "SELECT semantic_kind FROM resolutions WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(semantic.as_deref(), Some("inherit"));
    }

    fn tier3_ref_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM refs WHERE source = 'tier3-rust-analyzer-lsp'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    // ──── Schema v10 / Phase 1: target_path persistence ───────────────────

    /// Set up a one-file manifest with one blob in an isolated tempdir.
    /// Caller keeps the `TempDir` alive for the duration of the test.
    fn one_file_db(
        file_path: &str,
        blob: &str,
        parser_id: &str,
    ) -> (rusqlite::Connection, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = crate::cas::store::open(&tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns) VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha) VALUES (1, ?1, ?2)",
            params![file_path, blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![blob, parser_id],
        )
        .unwrap();
        (conn, tmp)
    }

    #[test]
    fn persist_resolutions_roundtrips_target_path_for_import() {
        // Import edge: `target_qualified = None` (post-Ruby-hack-fix), but
        // `target_path` resolves to a workspace file. Expect:
        //   - resolutions.target_path = Some("lib/db.rb")
        //   - resolutions.target_symbol_id = NULL
        let site_blob = "site-blob";
        let target_blob = "target-blob";
        let parser_id = "tree-sitter-ruby";

        let (mut conn, _tmp) = one_file_db("Rakefile", site_blob, parser_id);
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha) VALUES (1, ?1, ?2)",
            params!["lib/db.rb", target_blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![target_blob, parser_id],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "Rakefile".into(),
                site_byte_range: 10..20,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: Some("lib/db.rb".into()),
                target_qualified: None,
            }],
        };
        let inserted = persist_resolutions(
            &mut conn,
            ManifestId(1),
            "ruby-resolver",
            "tier25",
            parser_id,
            &facts,
        )
        .unwrap();
        assert_eq!(inserted, 1);
        let (target_path, target_symbol_id): (Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT target_path, target_symbol_id FROM resolutions
                 WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(target_path.as_deref(), Some("lib/db.rb"));
        assert!(target_symbol_id.is_none());
    }

    #[test]
    fn persist_resolutions_cross_parser_unique_hit() {
        // Kotlin call site resolves to Java symbol — single matching symbol
        // exists across parser_ids. Expect cross-parser fallback to adopt
        // its id.
        let site_blob = "kt-blob";
        let target_blob = "java-blob";
        let kt_parser = "tree-sitter-kotlin-ng";
        let java_parser = "tree-sitter-java";

        let (mut conn, _tmp) = one_file_db("src/main/kotlin/X.kt", site_blob, kt_parser);
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha) VALUES (1, ?1, ?2)",
            params!["src/main/java/JsonAdapter.java", target_blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![target_blob, java_parser],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (?1, ?2, 'JsonAdapter', 'com.x.JsonAdapter', 'class',
                0, 100, 1, 10, 'syn')",
            params![target_blob, java_parser],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "src/main/kotlin/X.kt".into(),
                site_byte_range: 30..40,
                kind: ResolutionKind::Type,
                semantic_kind: Some(SemanticKind::Inherit),
                target_path: Some("src/main/java/JsonAdapter.java".into()),
                target_qualified: Some("com.x.JsonAdapter".into()),
            }],
        };
        persist_resolutions(
            &mut conn,
            ManifestId(1),
            "kotlin-resolver",
            "tier25",
            kt_parser,
            &facts,
        )
        .unwrap();
        let target_symbol_id: Option<i64> = conn
            .query_row(
                "SELECT target_symbol_id FROM resolutions
                 WHERE source = 'tier25-kotlin-resolver'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            target_symbol_id.is_some(),
            "cross-parser fallback should adopt the unique sibling-parser symbol id"
        );
    }

    #[test]
    fn persist_resolutions_cross_parser_uniqueness_check() {
        // Two symbols share the same `(blob_sha, qualified)` across two
        // parser_ids — the fallback must refuse to pick arbitrarily and
        // leave target_symbol_id NULL. target_path is still persisted.
        let site_blob = "kt-blob";
        let target_blob = "shared-blob";
        let kt_parser = "tree-sitter-kotlin-ng";

        let (mut conn, _tmp) = one_file_db("src/main/kotlin/X.kt", site_blob, kt_parser);
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha) VALUES (1, ?1, ?2)",
            params!["src/shared.both", target_blob],
        )
        .unwrap();
        // Same blob_sha indexed by two parsers (synthetic but allowed by schema).
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, 'parser-a', 1, 0), (?1, 'parser-b', 1, 0)",
            params![target_blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (?1, 'parser-a', 'F', 'pkg.F', 'class', 0, 50, 1, 5, 'syn'),
               (?1, 'parser-b', 'F', 'pkg.F', 'class', 0, 50, 1, 5, 'syn')",
            params![target_blob],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "src/main/kotlin/X.kt".into(),
                site_byte_range: 0..5,
                kind: ResolutionKind::Type,
                semantic_kind: Some(SemanticKind::Inherit),
                target_path: Some("src/shared.both".into()),
                target_qualified: Some("pkg.F".into()),
            }],
        };
        persist_resolutions(
            &mut conn,
            ManifestId(1),
            "kotlin-resolver",
            "tier25",
            kt_parser,
            &facts,
        )
        .unwrap();
        let (target_path, target_symbol_id): (Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT target_path, target_symbol_id FROM resolutions
                 WHERE source = 'tier25-kotlin-resolver'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_path.as_deref(),
            Some("src/shared.both"),
            "target_path is preserved regardless of ambiguous symbol lookup"
        );
        assert!(
            target_symbol_id.is_none(),
            "ambiguous cross-parser fallback must return None"
        );
    }

    #[test]
    fn persist_resolutions_overwrites_legacy_rows_on_revision_bump() {
        // R2 Phase 1 upgrade-path pin: simulate an existing-DB state
        // where the manifest already has a `workspace_analysis_runs`
        // row at the old revision and a resolutions row written by
        // that old analyzer with `target_path = NULL` (pre-v10
        // shape, post-migration NULL). When the analyzer re-runs at a
        // bumped revision (the scheduler does not pick this up
        // automatically; users invoke `cairn ctl repo reindex` per
        // the upgrade notes in CHANGELOG.md) and emits a fresh
        // WorkspaceResolution with `target_path = Some(...)`,
        // `persist_resolutions` must:
        //   1. DELETE the legacy resolutions row (same source key).
        //   2. INSERT the new row carrying target_path.
        // This test pins just the persist behaviour; the scheduler
        // / re-enqueue path is documented as manual today and tracked
        // for a follow-up release.
        let site_blob = "site-blob";
        let target_blob = "target-blob";
        let parser_id = "tree-sitter-ruby";
        let (conn, _tmp) = one_file_db("Rakefile", site_blob, parser_id);
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha) VALUES (1, ?1, ?2)",
            params!["lib/db.rb", target_blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![target_blob, parser_id],
        )
        .unwrap();

        // Legacy row: same source key, target_path NULL — what an
        // older analyzer wrote before the v10 column existed.
        // Hardcoded revision 99 (legacy) / 100 (bumped) decouples
        // this test from the real analyzer revisions on disk.
        let legacy_revision: u32 = 99;
        let bumped_revision: u32 = 100;
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
                started_at_ns, finished_at_ns, error)
             VALUES (1, 'ruby-resolver', ?1, 'cfg', 'succeeded', 0, 1, NULL)",
            params![legacy_revision],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES (?1, ?2, 10, 20, 'import', NULL, NULL, NULL, 'tier25-ruby-resolver')",
            params![site_blob, parser_id],
        )
        .unwrap();

        // Sanity: legacy row exists with target_path NULL.
        let (count, before_target_path): (i64, Option<String>) = conn
            .query_row(
                "SELECT COUNT(*), MAX(target_path) FROM resolutions
                 WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert!(before_target_path.is_none());

        // Bumped analyzer re-runs with the same source string but a
        // new revision; emits a WorkspaceResolution with target_path
        // pointing at a workspace file.
        // (The persist layer does not consult analyzer_revision in
        // its DELETE/INSERT; it scopes by `source` only. Bumping the
        // run table row separately is what the scheduler does on
        // re-run, mirrored here so the legacy state is realistic.)
        let mut conn = conn;
        conn.execute(
            "UPDATE workspace_analysis_runs
                SET analyzer_revision = ?1, status = 'queued'
              WHERE manifest_id = 1 AND analyzer_id = 'ruby-resolver'",
            params![bumped_revision],
        )
        .unwrap();
        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "Rakefile".into(),
                site_byte_range: 10..20,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: Some("lib/db.rb".into()),
                target_qualified: None,
            }],
        };
        persist_resolutions(
            &mut conn,
            ManifestId(1),
            "ruby-resolver",
            "tier25",
            parser_id,
            &facts,
        )
        .unwrap();

        // Legacy row must be deleted, single new row with target_path.
        let (count, after_target_path): (i64, Option<String>) = conn
            .query_row(
                "SELECT COUNT(*), MAX(target_path) FROM resolutions
                 WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1, "legacy row should be replaced, not duplicated");
        assert_eq!(after_target_path.as_deref(), Some("lib/db.rb"));
    }

    #[test]
    fn persist_resolutions_skips_nonexistent_target_path() {
        // Analyzer bug guard: target_path that does not exist in the
        // manifest should be dropped to NULL (and debug-logged) rather
        // than propagated to the wire.
        let site_blob = "site-blob";
        let parser_id = "tree-sitter-ruby";
        let (mut conn, _tmp) = one_file_db("main.rb", site_blob, parser_id);

        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "main.rb".into(),
                site_byte_range: 0..5,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: Some("lib/phantom_not_in_manifest.rb".into()),
                target_qualified: None,
            }],
        };
        persist_resolutions(
            &mut conn,
            ManifestId(1),
            "ruby-resolver",
            "tier25",
            parser_id,
            &facts,
        )
        .unwrap();
        let target_path: Option<String> = conn
            .query_row(
                "SELECT target_path FROM resolutions WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            target_path.is_none(),
            "phantom target_path must be sanitized to NULL on the resolutions row"
        );
    }

    #[test]
    fn persist_resolutions_phantom_target_path_does_not_escape_via_qualified_fallback() {
        // R2 B3 regression pin: a resolution whose `target_path` is
        // *phantom* (analyzer emitted a non-existent path) plus a
        // `target_qualified` that happens to match a unique
        // workspace symbol must NOT silently adopt that symbol's
        // file via the manifest-wide qualified-only fallback. The
        // analyzer-bug signal (`target_path = NULL`,
        // `target_symbol_id = NULL`) must be preserved.
        let site_blob = "site-blob";
        let unrelated_blob = "unrelated-blob";
        let parser_id = "tree-sitter-ruby";

        let (mut conn, _tmp) = one_file_db("Rakefile", site_blob, parser_id);
        // A different workspace file holds a symbol whose qualified
        // would match the analyzer's target_qualified. Without the
        // phantom guard the qualified-only fallback would adopt
        // this symbol and re-point target_path to its file.
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha) VALUES (1, ?1, ?2)",
            params!["lib/unrelated.rb", unrelated_blob],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![unrelated_blob, parser_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (?1, ?2, 'TempFile', 'rake/clean', 'class',
                0, 100, 1, 10, 'syn')",
            params![unrelated_blob, parser_id],
        )
        .unwrap();

        let facts = WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions: vec![WorkspaceResolution {
                source_path: "Rakefile".into(),
                site_byte_range: 0..5,
                kind: ResolutionKind::Type,
                semantic_kind: Some(SemanticKind::Inherit),
                // Phantom path (does not exist in this manifest).
                target_path: Some("lib/phantom_not_in_manifest.rb".into()),
                // Qualified that *would* match the unrelated symbol
                // above via manifest-wide fallback if not gated.
                target_qualified: Some("rake/clean".into()),
            }],
        };
        persist_resolutions(
            &mut conn,
            ManifestId(1),
            "ruby-resolver",
            "tier25",
            parser_id,
            &facts,
        )
        .unwrap();
        let (target_path, target_symbol_id): (Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT target_path, target_symbol_id FROM resolutions
                 WHERE source = 'tier25-ruby-resolver'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(
            target_path.is_none(),
            "phantom target_path must stay NULL — not re-pointed to lib/unrelated.rb"
        );
        assert!(
            target_symbol_id.is_none(),
            "qualified-only fallback must not run for PhantomDropped paths"
        );
    }
}
