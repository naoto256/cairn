//! Method payloads — the protocol-neutral data shapes that ride inside
//! a JSON-RPC `params` / `result`.
//!
//! These types are the contract between the daemon (which produces them)
//! and any consumer (cairn's MCP front-end, future LSP front-end,
//! cairn-graph, cairn-audit, IDE plugins). They are intentionally
//! divorced from any specific RPC envelope: an MCP `tools/call(get_outline)`
//! and a plain `{"method":"get_outline"}` JSON-RPC both deserialize into
//! [`OutlineArgs`] and return [`OutlineResult`].

use serde::{Deserialize, Serialize};

use crate::common::{
    Completeness, Diagnostic, Hint, LanguageEnrichment, RefKind, SourceTier, SymbolKind,
    TierRepoStatus, TierStatus, Timing,
};

// ─── shared argument fragments ─────────────────────────────────────────────

fn is_false(value: &bool) -> bool {
    !*value
}

/// Repository filter shared by query methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoScope {
    /// Repository alias. `None` searches every registered repo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

/// Snapshot selector shared by methods that can address a branch or
/// explicit anchor. Flattening keeps the public wire shape stable:
/// callers still send `repo`, `branch`, and `anchor` at the top level.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotScope {
    /// Repository alias. `None` searches every registered repo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Bare branch name to query. Ignored when `anchor` is supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
    /// `tentative/<id>`). Takes priority over `branch` when set;
    /// supplying only the bare branch name still works via `branch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
}

/// Optional result cap shared by list-like methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PaginationArgs {
    /// Maximum number of items to return. `None` uses the daemon default;
    /// reaching the effective limit marks the result as partial with `cap`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Optional Tier-3 diagnostic verbosity shared by query methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tier3Args {
    /// Include full repo-wide Tier-3 readiness alongside the query-relevant
    /// readiness. Defaults to false to keep unrelated analyzer failures out
    /// of ordinary query confidence checks.
    #[serde(default, skip_serializing_if = "is_false")]
    pub verbose_tier3: bool,
}

// ─── list_repos ─────────────────────────────────────────────────────────────

/// Arguments to `list_repos`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListReposArgs {
    /// Optional substring filter matched against alias and root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Maximum number of repositories to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Result of `list_repos`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListReposResult {
    /// Registered repository summaries visible to the daemon.
    pub repos: Vec<RepoListEntry>,
    /// Completeness of the inventory. `Partial` when `limit` truncated rows.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// One repository returned by `list_repos`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoListEntry {
    /// Short alias used by query arguments.
    pub alias: String,
    /// Registered repository root path.
    pub root: String,
    /// Distinct language tags present in current snapshots.
    pub languages: Vec<String>,
    /// Aggregate repository readiness.
    pub status: RepoAggregateStatus,
    /// Number of snapshot manifests reachable through anchors.
    pub snapshot_count: u32,
    /// File count across the current repo view.
    pub current_file_count: u64,
    /// Symbol count across the current repo view.
    pub current_symbol_count: u64,
}

/// Aggregate repository status used by inventory views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepoAggregateStatus {
    Ready,
    Indexing,
    Partial,
    Error,
}

/// Arguments to `repo_status`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoStatusArgs {
    /// Repository alias. Exactly one of `repo` or `path` is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Filesystem path under a registered repository. Exclusive with `repo`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Include per-snapshot detail. Defaults to false.
    #[serde(default)]
    pub include_snapshots: bool,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `repo_status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatusResult {
    pub repo: RepoStatusEntry,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// Detailed status for one repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatusEntry {
    pub alias: String,
    pub root: String,
    pub languages: Vec<String>,
    pub summary: RepoStatusSummary,
    pub current: RepoStatusCurrent,
    pub tier3_status: TierRepoStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<RepoSnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatusSummary {
    pub snapshot_count: u32,
    pub ready_snapshot_count: u32,
    pub stale_snapshot_count: u32,
    pub current_file_count: u64,
    pub current_symbol_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatusCurrent {
    pub anchor: String,
    pub status: String,
}

/// Snapshot entry returned by `repo_status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSnapshotEntry {
    /// User-facing anchor labels pointing at this manifest. `branch/<name>`
    /// anchors are rendered as `<name>`; `HEAD` and `tentative/<id>` remain
    /// explicit.
    pub branches: Vec<String>,
    /// Snapshot readiness string reported by the daemon.
    pub status: String,
    /// Per-language analyzer tier matrix for this snapshot.
    pub enrichment: Vec<LanguageEnrichment>,
    /// Last anchor access/update timestamp formatted by the daemon. `None`
    /// means no timestamp was recorded for the snapshot.
    pub last_accessed: Option<String>,
    /// Number of files in the snapshot manifest.
    pub file_count: u64,
    /// Number of symbols indexed for the snapshot.
    pub symbol_count: u64,
}

impl RepoSnapshotEntry {
    /// First branch label in `branches` ordering (`HEAD` if present).
    #[must_use]
    pub fn primary_label(&self) -> Option<&str> {
        self.branches.first().map(String::as_str)
    }

    /// Whether `HEAD` points at this snapshot's manifest.
    #[must_use]
    pub fn has_head(&self) -> bool {
        self.branches.iter().any(|b| b == "HEAD")
    }
}

#[cfg(test)]
mod list_repos_tests {
    use super::*;

    #[test]
    fn repo_snapshot_entry_serializes_enrichment_matrix() {
        let entry = RepoSnapshotEntry {
            branches: vec!["HEAD".into(), "main".into()],
            status: "ready".into(),
            enrichment: vec![LanguageEnrichment {
                language: "rust".into(),
                tier: SourceTier::Semantic,
                has_analyzer: true,
            }],
            last_accessed: Some("2026-06-03T00:00:00Z".into()),
            file_count: 1,
            symbol_count: 2,
        };
        let v = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "branches": ["HEAD", "main"],
                "status": "ready",
                "enrichment": [{
                    "language": "rust",
                    "tier": "semantic",
                    "has_analyzer": true
                }],
                "last_accessed": "2026-06-03T00:00:00Z",
                "file_count": 1,
                "symbol_count": 2
            })
        );
        let back: RepoSnapshotEntry = serde_json::from_value(v).unwrap();
        assert_eq!(back.enrichment[0].language, "rust");
        assert_eq!(back.primary_label(), Some("HEAD"));
        assert!(back.has_head());
    }

    #[test]
    fn list_repos_default_omits_snapshots_and_jobs() {
        let result = ListReposResult {
            repos: vec![RepoListEntry {
                alias: "cairn".into(),
                root: "/tmp/cairn".into(),
                languages: vec!["markdown".into(), "rust".into()],
                status: RepoAggregateStatus::Ready,
                snapshot_count: 1,
                current_file_count: 2,
                current_symbol_count: 3,
            }],
            completeness: Completeness::complete(),
            timing: Timing::default(),
        };
        let value = serde_json::to_value(&result).unwrap();
        let repo = &value["repos"][0];
        assert!(repo.get("snapshots").is_none());
        assert!(repo.get("jobs").is_none());
    }

    #[test]
    fn repo_status_requires_exactly_one_of_repo_or_path() {
        let neither = RepoStatusArgs::default();
        assert!(neither.repo.is_none() && neither.path.is_none());

        let both = RepoStatusArgs {
            repo: Some("cairn".into()),
            path: Some("/tmp/cairn".into()),
            ..RepoStatusArgs::default()
        };
        assert!(both.repo.is_some() && both.path.is_some());
    }

    #[test]
    fn repo_status_result_serializes_diagnostics_and_hints_when_present() {
        let result = RepoStatusResult {
            repo: RepoStatusEntry {
                alias: "demo".into(),
                root: "/tmp/demo".into(),
                languages: vec!["rust".into()],
                summary: RepoStatusSummary {
                    snapshot_count: 1,
                    ready_snapshot_count: 0,
                    stale_snapshot_count: 1,
                    current_file_count: 1,
                    current_symbol_count: 0,
                },
                current: RepoStatusCurrent {
                    anchor: "HEAD".into(),
                    status: "stale".into(),
                },
                tier3_status: TierRepoStatus {
                    this_repo: crate::TierStatusBody::ready(),
                    repo_wide: None,
                },
                snapshots: Vec::new(),
            },
            diagnostics: vec![Diagnostic {
                code: crate::DiagnosticCode::AnalyzerStale,
                severity: crate::DiagnosticSeverity::Info,
                message: "stale".into(),
                language: Some("rust".into()),
                analyzer_id: None,
                repo: Some("demo".into()),
                file: None,
                details: None,
            }],
            hints: vec![Hint {
                code: crate::HintCode::SnapshotStale,
                message: "Current snapshot is stale.".into(),
                action: None,
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: Some("demo".into()),
            }],
            timing: Timing::default(),
        };

        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["diagnostics"][0]["code"], "analyzer_stale");
        assert_eq!(value["hints"][0]["code"], "snapshot_stale");
    }

    #[test]
    fn repo_status_result_omits_envelope_optional_fields_on_happy_path() {
        let result = RepoStatusResult {
            repo: RepoStatusEntry {
                alias: "demo".into(),
                root: "/tmp/demo".into(),
                languages: vec!["rust".into()],
                summary: RepoStatusSummary {
                    snapshot_count: 1,
                    ready_snapshot_count: 1,
                    stale_snapshot_count: 0,
                    current_file_count: 1,
                    current_symbol_count: 1,
                },
                current: RepoStatusCurrent {
                    anchor: "HEAD".into(),
                    status: "ready".into(),
                },
                tier3_status: TierRepoStatus {
                    this_repo: crate::TierStatusBody::ready(),
                    repo_wide: None,
                },
                snapshots: Vec::new(),
            },
            diagnostics: Vec::new(),
            hints: Vec::new(),
            timing: Timing::default(),
        };

        let value = serde_json::to_value(result).unwrap();
        assert!(value.get("diagnostics").is_none());
        assert!(value.get("hints").is_none());
    }
}

// ─── list_jobs ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListJobsArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default)]
    pub include_terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListJobsResult {
    pub jobs: Vec<JobEntry>,
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEntry {
    pub job_id: i64,
    pub alias: String,
    pub analyzer_id: String,
    pub state: String,
    pub scheduler_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_group: Option<String>,
    pub queued_ms: u64,
    pub pool_wait_ms: u64,
    pub run_ms: u64,
    pub progress_ticks: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<f64>,
}

// ─── get_outline ────────────────────────────────────────────────────────────

/// Arguments to `get_outline`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlineArgs {
    /// Repository filter. `None` searches every registered repo and
    /// returns matching outlines from each; identical-named paths
    /// across repos are distinguished by the `file` field on each
    /// returned item.
    #[serde(flatten)]
    pub scope: RepoScope,
    /// Single file to outline, relative to the repo root. Required when
    /// `path` is absent; omitted in directory mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// File-path string prefix relative to repo root. This is a
    /// byte-level prefix filter, matching `find_symbols.path`
    /// semantics: include a trailing slash to scope to a directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Restrict items to a single symbol kind. Mirrors
    /// `find_symbols.kind`. Useful for "show me only the types under
    /// this directory" without paging through hundreds of method
    /// entries first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<SymbolKind>,
    /// Directory-depth cap relative to `path`, counted by `/`
    /// separators after the prefix. `max_depth = 1` keeps items from
    /// files directly under the prefix and omits anything nested
    /// deeper — the canonical "module-level summary" shape for a
    /// crate or package root. Ignored in single-file mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_depth: Option<u32>,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `get_outline`. Empty `doc`/`signature` fields are omitted on the
/// wire to keep token usage tight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlineResult {
    /// Outline entries. Empty means the file/path matched no indexed symbols,
    /// or the selected anchor was not indexed.
    pub items: Vec<OutlineItem>,
    /// Completeness of this outline. `Partial` when the file's
    /// Tier-2 enrichment (doc overrides, semantic signatures) had
    /// not finished, or when a large file timed out mid-extraction.
    /// `#[serde(default)]` keeps older clients that omit the field
    /// readable; absence is treated as `Complete`.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// One symbol-like entry returned by `get_outline`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlineItem {
    /// Present in directory mode to attribute each item to a file.
    /// Omitted in single-file mode because the request already names
    /// the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Kind tag for the outlined item.
    pub kind: SymbolKind,
    /// Display name as indexed by the backend.
    pub name: String,
    /// Qualified name suitable for follow-up `get_symbol_source` calls.
    pub qualified: String,
    /// Signature text when the backend captured one. `None` is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    /// 1-based source line for the item.
    pub line: u32,
    /// Docstring or heading text captured for the item. `None` is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Index tier that produced this item.
    pub source: SourceTier,
}

// ─── find_symbols ───────────────────────────────────────────────────────────

/// Arguments to `find_symbols`.
///
/// Every filter is optional — but **at least one** of `query`, `kind`,
/// `container`, or `path` must be supplied (otherwise the call would
/// dump every indexed symbol, which is rarely what an agent wants).
/// The filters AND together.
///
/// `repo = None` searches every registered repo. Omitting both
/// `branch` and `anchor` resolves to the registered worktree's
/// `tentative/<id>` snapshot (= committed HEAD plus uncommitted
/// edits the daemon's file watcher has picked up), falling back to
/// `HEAD` when no tentative snapshot exists yet. Pass them explicitly
/// to opt in to committed-only state (`anchor = "HEAD"`) or a
/// specific branch (`branch = "<name>"`). Hits always name the
/// repo / branch they came from.
///
/// `query` was required in 0.2.0 and is now optional so that
/// `{kind: "class"}` alone enumerates classes, `{container: "Foo"}`
/// alone enumerates Foo's members, etc.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FindSymbolArgs {
    /// Text matched against `name` / `qualified` (exact when `fuzzy`
    /// is false, FTS5 over name + qualified + doc when true). In
    /// fuzzy mode, whitespace between bare tokens is FTS5 AND, quoted
    /// text is an exact-order phrase, and prefix matching is only
    /// applied when the caller writes `*` (for example `Authent*`).
    /// Optional from 0.2.1; pair with `kind` / `container` / `path`
    /// instead when you want a structural enumeration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// Restrict matches to one symbol kind. `None` allows all kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<SymbolKind>,
    /// Qualified-prefix scope. `container = "Foo"` returns symbols
    /// whose qualified name starts with `Foo::` (Rust) or `Foo.`
    /// (Python) — i.e. Foo's members. Combine with `kind = "method"`
    /// to get just the methods. When `include_inherited` is set, the
    /// scope is walked up the `implementations` table so inherited
    /// members are union-ed in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// Walk the `implementations` table from `container` upward and
    /// include members from every base type. Ignored when `container`
    /// is absent. Requires Tier-2 enrichment; on a syntactic-only
    /// snapshot the response is reported as `partial`.
    #[serde(default)]
    pub include_inherited: bool,
    /// File-path **string** prefix (relative to the repo root). Only
    /// symbols whose `file.path` starts with this string are returned;
    /// the match is byte-level, not directory-aware.
    ///
    /// To scope to a directory, include the trailing `/`:
    ///
    /// - `path = "crates/foo/"` → only files under `crates/foo/`.
    /// - `path = "crates/foo"` → also matches `crates/foo_bar/...`
    ///   (sibling directories with a shared prefix).
    ///
    /// To match a single file or a file-name prefix, omit the slash:
    ///
    /// - `path = "crates/foo/src/lib"` → matches `lib.rs` and
    ///   `lib_helper.rs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Use SQLite FTS5 over `name`, `qualified`, and `doc` instead of
    /// exact `name` / `qualified` matching. Bare tokens separated by
    /// spaces are AND-ed by FTS5, quoted strings require exact token
    /// order, and prefix matching requires an explicit trailing `*`.
    #[serde(default)]
    pub fuzzy: bool,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
    /// When true, hits omit the `signature` field. Use for broad
    /// enumerations (e.g. `kind = "function"` over a directory) where
    /// the signature dominates wire/context cost. Named for
    /// consistency with `GetSymbolSourceArgs.signature_only`; the
    /// other navigation fields (`id`, `qualified`, `name`, `kind`,
    /// `repo`, `branch`, `location`) are always returned.
    #[serde(default)]
    pub signature_only: bool,
}

/// Result of `find_symbols`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSymbolResult {
    /// Matching symbols, sorted for stable scanning across languages/files.
    pub items: Vec<FindSymbolHit>,
    /// `Partial` when the FTS index or a snapshot DB had not yet
    /// reached steady state — a name that exists on disk may not
    /// appear in `items` even though the file was discovered.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// One symbol returned by `find_symbols`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSymbolHit {
    /// Internal symbol row id in the selected snapshot database.
    pub id: i64,
    /// Qualified symbol name, used by `get_symbol_source`.
    pub qualified: String,
    /// Display name or last segment of the symbol.
    pub name: String,
    /// Symbol kind tag.
    pub kind: SymbolKind,
    /// Repository alias the hit came from. Lets a cross-repo query
    /// distinguish identically-qualified symbols across registered
    /// repos. The wire format always emits this; older clients that
    /// passed `repo` explicitly can ignore it.
    pub repo: String,
    /// Anchor label the hit was extracted from — `HEAD`,
    /// `branch/<n>`, `tag/<n>`, or `tentative/<id>`. Carried
    /// alongside `location` so a query that scans multiple snapshots
    /// (or whose default resolved to the worktree's tentative
    /// snapshot) can distinguish identically-located symbols across
    /// those snapshots.
    pub branch: String,
    /// `repo:branch:file:line` string, clickable in Claude Code UI.
    pub location: String,
    /// Repo-relative path, split out so agents do not need to parse
    /// `location` before calling `get_symbol_source(file=...)`.
    pub file: String,
    /// 1-based line number, split out alongside the human-facing
    /// `location` string.
    pub line: u32,
    /// Short language tag (`rust`, `python`, ...), when a semantic
    /// analyzer has enriched the blob. Omitted for syntactic-only hits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Signature text when present and `signature_only` did not request
    /// omission. `None` is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    /// Index tier that produced this hit.
    pub source: SourceTier,
}

// ─── type-relation + import queries (semantic enrichment surface) ──────────
//
// These return facts the Tier-2 analyzer populates: type-relation
// edges (`impl Trait for Foo`, `class Dog extends Animal`,
// `class Foo implements Bar`, ECMAScript mixins) flattened into the
// `implementations` table, and `use` / `import` statements flattened
// into the imports table. Same envelope shape as `find_symbols` —
// `repo` is optional where cross-repo search is supported; omitting
// both `branch` and `anchor` resolves to the registered worktree's
// `tentative/<id>` snapshot (= committed HEAD plus uncommitted edits
// the watcher has picked up), falling back to `HEAD` when no
// tentative snapshot exists yet. Hits carry their originating branch.

/// Arguments to `find_subtypes`. Asks "who implements / extends /
/// mixes in `name`?" — every type that names `name` on the
/// interface/base side of a type-relation edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSubtypesArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// The base type or trait/interface. Match returns every type
    /// that implements / extends / mixes in this name.
    pub name: String,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Arguments to `find_supertypes`. Asks "what does `name` extend /
/// implement / mix in?" — every type that appears on the
/// interface/base side of an edge originating at `name`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSupertypesArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// The subtype. Match returns every base type / trait / interface
    /// / mixin this name implements or inherits from.
    pub name: String,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `find_subtypes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSubtypesResult {
    /// Matching type-relation edges. Empty means no edge matched or the
    /// semantic tier has not produced any matching facts yet.
    pub items: Vec<ImplHit>,
    /// `Partial` when the Tier-2 analyzer had not finished on every
    /// file. Missing tier is `Semantic`. Items already extracted are
    /// still valid; new ones may arrive once indexing settles.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// Result of `find_supertypes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSupertypesResult {
    /// Matching type-relation edges. Empty means no edge matched or the
    /// semantic tier has not produced any matching facts yet.
    pub items: Vec<ImplHit>,
    /// `Partial` when the Tier-2 analyzer had not finished on every
    /// file. Missing tier is `Semantic`. Items already extracted are
    /// still valid; new ones may arrive once indexing settles.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// Serde default for [`ImplHit::kind_source`] — matches the
/// `KIND_SOURCE_FACT` constant in `cairn-core::query::find_impls`.
/// Kept here (rather than referencing the core constant) so the proto
/// crate stays dependency-free.
fn default_kind_source() -> String {
    "tier2-fact".into()
}

/// One type-relation edge — the data shape behind both
/// `find_subtypes` and `find_supertypes`. The two methods walk the
/// same `implementations` table from opposite directions, so the
/// hit shape is the same regardless of which side the caller pinned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImplHit {
    /// Qualified name (or token form) of the subtype the edge is on
    /// — the `Foo` in `impl Trait for Foo`, the `Dog` in `class Dog
    /// extends Animal`, the `Mixed` in `class Mixed extends mixin(...)`.
    pub type_qualified: String,
    /// Qualified name of the supertype the edge points at — the
    /// `Trait` in `impl Trait for Foo`, the `Animal` in `class Dog
    /// extends Animal`. `null` for an inherent impl (`impl Foo {}`)
    /// where there is no supertype side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_qualified: Option<String>,
    /// Edge kind — `"trait"` for Rust trait impls, `"inherent"` for
    /// Rust inherent impls, `"inherit"` for TypeScript `extends` /
    /// Python base classes, `"implements"` for TypeScript `implements`,
    /// `"mixin"` for ECMAScript mixin patterns.
    pub kind: String,
    /// Provenance for [`Self::kind`]. Either a resolution-layer
    /// `source` string (e.g. `"tier2-direct-java"`,
    /// `"tier25-py-resolver"`, `"tier3-pyright-lsp"`) when the kind
    /// came through the `resolutions` table, or `"tier2-fact"` when
    /// the Tier-2 `implementations.kind` was used as fallback. Older
    /// daemons that predate Phase 4 of the Tier-2.5 prep work emit no
    /// resolutions and so always report `"tier2-fact"`; the default
    /// here keeps deserialization of those payloads green.
    #[serde(default = "default_kind_source")]
    pub kind_source: String,
    /// Anchor label the edge came from.
    pub branch: String,
    /// `repo:branch:file:line` pointing at the subtype-side symbol
    /// (the impl block or the `class … extends …` declaration).
    pub location: String,
}

/// Arguments to `find_imports`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportsArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// File whose imports to list (path relative to repo root). When
    /// omitted, every import in the (filtered) snapshot is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `find_imports`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportsResult {
    /// Matching import edges.
    pub items: Vec<ImportHit>,
    /// `Partial` when the Tier-2 analyzer had not finished — `use`
    /// edges come exclusively from semantic enrichment today.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// One import edge returned by `find_imports`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportHit {
    /// Path of the file the `use` lives in, relative to repo root.
    pub file: String,
    /// Dotted module path on the left of the final `::`.
    pub to_module: String,
    /// Name imported from `to_module`; `*` for a glob import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported: Option<String>,
    /// `as` rename, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// True when this is a `pub use` re-export.
    pub is_reexport: bool,
    /// Anchor label the import came from.
    pub branch: String,
    /// `repo:branch:file:line` pointing at the import statement.
    #[serde(default)]
    pub location: String,
    /// 1-based line number of the import statement.
    pub line: u32,
}

// ─── find_references ───────────────────────────────────────────────────────
//
// "Who calls / uses this symbol?" Driven by the Tier-2 `refs` table
// populated by the syn body-visitor (calls + method calls today; more
// kinds when the analyzer learns them). Same envelope shape as
// `find_symbols` / `find_subtypes`: omitting both `branch` and `anchor`
// resolves to the registered worktree's `tentative/<id>` snapshot,
// falling back to `HEAD` when no tentative snapshot exists yet.

/// Direction of a reference query. Symmetric primitives let an agent
/// ask both "who calls X?" and "what does X call?" with the same tool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceDirection {
    /// Caller / use-site lookup (default). Matches `refs.target_*`
    /// against `symbol`. Answer: "who calls / uses X?".
    #[default]
    Incoming,
    /// Callee / outgoing lookup. Matches `refs.enclosing_qualified`
    /// against `symbol`. Answer: "what does X call / use?".
    Outgoing,
}

/// Arguments to `find_references`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindReferencesArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// Name or qualified path of the **anchor** symbol. In the default
    /// `direction = Incoming` this is the *target* (who calls X);
    /// with `Outgoing` it is the *enclosing* container (what does X
    /// call). A token containing `::` matches the qualified form
    /// first; a bare name falls back to the looser name index.
    pub symbol: String,
    /// Restrict to a specific reference kind. For outgoing queries,
    /// the default still returns only resolved call refs unless
    /// `include_noise` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<RefKind>,
    /// `Incoming` (default) = "who references `symbol`"; `Outgoing`
    /// = "what does `symbol` reference" (callees + type uses inside
    /// `symbol`'s body). 0.2.1 surface completion.
    #[serde(default)]
    pub direction: ReferenceDirection,
    /// Outgoing queries default to resolved call refs only
    /// (`kind = call` and non-empty `target_qualified`) so consumers
    /// can map a function's call graph without type refs,
    /// annotations, or unresolved receiver-method noise. Set this to
    /// true to return the legacy full ref set for debugging.
    #[serde(default)]
    pub include_noise: bool,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `find_references`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindReferencesResult {
    /// Matching reference sites.
    pub items: Vec<FindReferenceHit>,
    /// `Partial` carries two separate concerns for references:
    /// missing tiers (Tier-2 not yet run on every file) **and**
    /// resolution precision (method-call receivers can't be
    /// resolved without rust-analyzer-class Tier-3). The `reason`
    /// tag distinguishes the two for consumers that want to know.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// One reference site returned by `find_references`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindReferenceHit {
    /// Bare target token recorded at the reference site.
    pub target_name: String,
    /// Resolved target qualified name. `None` means the analyzer captured
    /// the token but could not resolve it to a symbol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_qualified: Option<String>,
    /// Reference kind.
    pub kind: RefKind,
    /// Qualified name of the function / impl block the reference sits
    /// inside. `None` for top-level expressions (rare in Rust).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_qualified: Option<String>,
    /// Anchor label the reference came from.
    pub branch: String,
    /// `repo:branch:file:line` string, clickable in editors that
    /// recognise the format.
    pub location: String,
    /// Single source line at `line` (trailing newline stripped). The
    /// caller would otherwise round-trip to `get_symbol_source` just
    /// to see what each reference looks like; carrying the line here
    /// is cheap (~80 chars per hit) and gives `find_references` the
    /// "what does this call site look like" answer in one call.
    /// `None` when the indexed blob can't be materialised (e.g. a
    /// worktree file that disappeared under a tentative anchor).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

// ─── find_callers / find_callees ───────────────────────────────────────────
//
// Thin shortcuts over `find_references` for the two most-asked questions
// on the call graph: "who calls X?" and "what does X call?". Both
// narrow to `kind = call` with a resolved `target_qualified`, mirroring
// the default `find_references` outgoing semantics — the legacy
// noise toggle is not exposed here on purpose; for unresolved method
// calls or type refs reach for `find_references` directly.

/// Arguments to `find_callers`. "Who calls `name`?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCallersArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// Callee symbol. Matches `refs.target_qualified` first when the
    /// name carries `::`, falling back to the bare last segment;
    /// bare names go straight to the name index.
    pub name: String,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Arguments to `find_callees`. "What does `name` call?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCalleesArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// Caller (enclosing) symbol. Matches `symbols.qualified` via
    /// the enclosing FK on each ref row.
    pub name: String,
    /// Optional result cap.
    #[serde(flatten)]
    pub pagination: PaginationArgs,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `find_callers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCallersResult {
    /// Matching call edges where the queried symbol is the callee.
    pub items: Vec<CallHit>,
    /// `Partial` when the Tier-2 refs table was not ready or the result was
    /// capped. Items already returned are still valid.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// Result of `find_callees`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCalleesResult {
    /// Matching call edges where the queried symbol is the caller.
    pub items: Vec<CallHit>,
    /// `Partial` when the Tier-2 refs table was not ready or the result was
    /// capped. Items already returned are still valid.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

/// One call edge — shared by `find_callers` and `find_callees`.
///
/// - For `find_callers`, the queried symbol is the callee:
///   `target_qualified` mirrors the query, `enclosing_qualified` is
///   the caller (the function that issues the call), and `location`
///   points at the call site inside that caller.
/// - For `find_callees`, the queried symbol is the caller:
///   `enclosing_qualified` mirrors the query, `target_qualified` is
///   the callee, and `location` points at the call site inside the
///   caller's body.
///
/// The shape is symmetric on purpose: both directions surface the
/// same `caller → callee at file:line` edge, only the side the caller
/// pinned changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallHit {
    /// Bare callee token recorded at the call site.
    pub target_name: String,
    /// Resolved callee qualified name. `None` means the analyzer captured
    /// the call token but could not resolve it to a symbol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_qualified: Option<String>,
    /// Qualified name of the enclosing function — the caller side of
    /// the edge. `None` for top-level expressions (rare in Rust).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_qualified: Option<String>,
    /// Anchor label the call came from.
    pub branch: String,
    /// `repo:branch:file:line` of the call site itself.
    pub location: String,
    /// Single source line at the call site (trailing newline
    /// stripped), so the caller can read the call without a follow-up
    /// `get_symbol_source` round trip. `None` when the indexed blob
    /// can't be materialised.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

// ─── get_symbol_source ─────────────────────────────────────────────────────
//
// Return the indexed source of a symbol — signature plus body for
// functions / impls, the full declaration for structs / enums / consts.
// Resolved by `qualified` because that is what `find_symbols` /
// `get_outline` already hand back; the daemon reads the file from
// disk using the byte range recorded at index time.

/// Arguments to `get_symbol_source`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSymbolSourceArgs {
    /// Repository + snapshot scope.
    #[serde(flatten)]
    pub scope: SnapshotScope,
    /// Qualified name of the symbol (matches `qualified` in the
    /// `symbols` table). Use `find_symbols` first if you only have a
    /// bare name and the repo has more than one match.
    pub qualified: String,
    /// Path (relative to repo root) the symbol lives in. Optional
    /// disambiguator when the same qualified name exists in multiple
    /// files (rare; useful for cross-module test fixtures).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Return only the signature + doc string, not the body. Cheap
    /// way to "peek" at an API surface (parameters / return type /
    /// docstring) without paying for the full implementation text.
    /// 0.2.1 surface completion.
    #[serde(default)]
    pub signature_only: bool,
    /// Tier-3 status verbosity.
    #[serde(flatten)]
    pub tier3: Tier3Args,
}

/// Result of `get_symbol_source`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSymbolSourceResult {
    /// Qualified name of the symbol that was found.
    pub qualified: String,
    /// Display name or last segment of the symbol.
    pub name: String,
    /// Symbol kind tag.
    pub kind: SymbolKind,
    /// Anchor label the source came from.
    pub branch: String,
    /// `repo:branch:file:line` pointing at the first line of the
    /// returned source.
    pub location: String,
    /// 1-based first line covered by [`Self::source`].
    pub line_start: u32,
    /// 1-based last line covered by [`Self::source`].
    pub line_end: u32,
    /// Source text of the symbol, exactly as it appears in the file
    /// (no re-indentation or stripping).
    pub source: String,
    /// Signature text when the backend captured one. In `signature_only`
    /// mode this remains populated while [`Self::source`] may be empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    /// Docstring or documentation comment captured for the symbol. `None`
    /// is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Index tier that produced the returned source metadata.
    pub source_tier: SourceTier,
    /// Tier-3 analyzer readiness for snapshots touched by this query.
    #[serde(default = "TierStatus::ready")]
    pub tier3_status: TierStatus,
    /// Structured facts about partial confidence or analyzer state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    /// Machine-readable next-step options for empty or incomplete results.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// Server-side wall time spent producing this response.
    #[serde(default)]
    pub timing: Timing,
}

// ─── register_repo / reindex ────────────────────────────────────────────────
//
// These live here for the MCP front-end's convenience even though the
// daemon's data RPC does not handle them: register / reindex are admin
// verbs and go to `control.sock`. `cairn serve` translates the matching
// MCP tools into [`crate::control::ControlRequest`] messages.

/// Arguments to `register_repo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRepoArgs {
    /// Absolute path to the repository root.
    pub path: String,
    /// Short name the agent will use in subsequent queries.
    pub alias: String,
}

/// Arguments to `reindex`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexArgs {
    /// Repository alias to reindex.
    pub alias: String,
}

/// Result of `register_repo` / `reindex`. Carries the headline stats so
/// the agent can confirm the index produced something.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResult {
    /// Repository alias that was indexed.
    pub alias: String,
    /// Branch or anchor label indexed by the operation.
    pub branch: String,
    /// Number of files parsed or refreshed.
    pub files_indexed: u64,
    /// Number of files skipped because they were ignored or unchanged.
    pub files_skipped: u64,
    /// Number of symbols inserted into the snapshot index.
    pub symbols_inserted: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::common::default_tier;

    #[test]
    fn find_symbol_hit_round_trips_language_some() {
        let value = json!({
            "id": 1,
            "qualified": "demo::f",
            "name": "f",
            "kind": "function",
            "repo": "demo",
            "branch": "HEAD",
            "location": "demo:HEAD:src/lib.rs:1",
            "file": "src/lib.rs",
            "line": 1,
            "language": "rust",
            "source": "semantic"
        });
        let hit: FindSymbolHit = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(hit.language.as_deref(), Some("rust"));
        assert_eq!(hit.file, "src/lib.rs");
        assert_eq!(hit.line, 1);
        assert_eq!(serde_json::to_value(hit).unwrap()["language"], "rust");
    }

    #[test]
    fn find_symbol_hit_omits_language_none() {
        let value = json!({
            "id": 1,
            "qualified": "Intro",
            "name": "Intro",
            "kind": "section",
            "repo": "demo",
            "branch": "HEAD",
            "location": "demo:HEAD:README.md:1",
            "file": "README.md",
            "line": 1,
            "source": "syntactic"
        });
        let hit: FindSymbolHit = serde_json::from_value(value).unwrap();
        assert_eq!(hit.language, None);
        assert!(serde_json::to_value(hit).unwrap().get("language").is_none());
    }

    #[test]
    fn outline_args_round_trips_directory_path() {
        let value = json!({
            "repo": "demo",
            "path": "src/",
            "limit": 50
        });
        let args: OutlineArgs = serde_json::from_value(value).unwrap();
        assert_eq!(args.scope.repo.as_deref(), Some("demo"));
        assert_eq!(args.file, None);
        assert_eq!(args.path.as_deref(), Some("src/"));
        assert_eq!(args.pagination.limit, Some(50));

        let serialized = serde_json::to_value(args).unwrap();
        assert!(serialized.get("file").is_none());
        assert_eq!(serialized["repo"], "demo");
        assert_eq!(serialized["path"], "src/");
        assert_eq!(serialized["limit"], 50);
        assert!(serialized.get("scope").is_none());
        assert!(serialized.get("pagination").is_none());
    }

    #[test]
    fn snapshot_scope_and_pagination_stay_flat_on_wire() {
        let value = json!({
            "repo": "demo",
            "branch": "main",
            "anchor": "HEAD",
            "query": "parse_args",
            "limit": 25
        });

        let args: FindSymbolArgs = serde_json::from_value(value).unwrap();
        assert_eq!(args.scope.repo.as_deref(), Some("demo"));
        assert_eq!(args.scope.branch.as_deref(), Some("main"));
        assert_eq!(args.scope.anchor.as_deref(), Some("HEAD"));
        assert_eq!(args.query.as_deref(), Some("parse_args"));
        assert_eq!(args.pagination.limit, Some(25));

        let serialized = serde_json::to_value(args).unwrap();
        assert_eq!(
            serialized,
            json!({
                "query": "parse_args",
                "repo": "demo",
                "branch": "main",
                "anchor": "HEAD",
                "limit": 25,
                "include_inherited": false,
                "fuzzy": false,
                "signature_only": false
            })
        );
    }

    #[test]
    fn tier3_status_this_query_field_explicit_in_wire() {
        let status = TierStatus::ready();
        let serialized = serde_json::to_value(status).unwrap();
        assert_eq!(
            serialized,
            json!({
                "this_query": {
                    "ready": true,
                    "analyzers": []
                }
            })
        );

        let parsed: TierStatus = serde_json::from_value(json!({
            "this_query": {
                "ready": false,
                "analyzers": [
                    {"id": "rust-analyzer-lsp", "language": "rust", "state": "running"}
                ]
            }
        }))
        .unwrap();
        assert!(!parsed.this_query.ready);
        assert_eq!(parsed.this_query.analyzers.len(), 1);
        assert!(parsed.repo_wide.is_none());
    }

    #[test]
    fn tier3_status_default_omits_repo_wide() {
        let status = TierStatus::ready();
        let serialized = serde_json::to_value(status).unwrap();
        assert!(serialized.get("repo_wide").is_none());
    }

    #[test]
    fn tier3_status_verbose_includes_repo_wide() {
        let status = TierStatus::from_body(crate::TierStatusBody {
            ready: true,
            analyzers: Vec::new(),
        })
        .with_repo_wide(crate::TierStatusBody {
            ready: false,
            analyzers: vec![crate::TierAnalyzerStatus {
                id: Some("sourcekit-lsp".into()),
                language: "swift".into(),
                tier: default_tier(),
                state: crate::AnalyzerState::Missing,
                reason_code: Some(crate::ReasonCode::BinaryNotFound),
                reason: Some("sourcekit-lsp binary not found in PATH".into()),
            }],
        });

        let serialized = serde_json::to_value(status).unwrap();
        assert_eq!(serialized["this_query"]["ready"], true);
        assert_eq!(serialized["repo_wide"]["ready"], false);
        assert_eq!(
            serialized["repo_wide"]["analyzers"][0]["id"],
            "sourcekit-lsp"
        );
    }

    #[test]
    fn analyzer_state_enum_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(crate::AnalyzerState::NotApplicable).unwrap(),
            json!("not_applicable")
        );
        assert_eq!(
            serde_json::to_value(crate::ReasonCode::BinaryNotFound).unwrap(),
            json!("binary_not_found")
        );
    }

    #[test]
    fn reason_code_includes_not_scheduled() {
        assert_eq!(
            serde_json::to_value(crate::ReasonCode::NotScheduled).unwrap(),
            json!("not_scheduled")
        );
    }

    #[test]
    fn tier3_repo_status_serializes_this_repo() {
        let status = crate::TierRepoStatus {
            this_repo: crate::TierStatusBody::from_analyzers(vec![crate::TierAnalyzerStatus {
                id: Some("rust-analyzer-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: crate::AnalyzerState::Ready,
                reason_code: None,
                reason: None,
            }]),
            repo_wide: None,
        };
        let serialized = serde_json::to_value(status).unwrap();
        assert_eq!(serialized["this_repo"]["ready"], true);
        assert!(serialized.get("repo_wide").is_none());
    }

    #[test]
    fn job_entry_canonical_shape() {
        let entry = JobEntry {
            job_id: 7,
            alias: "cairn".into(),
            analyzer_id: "rust-analyzer-lsp".into(),
            state: "running".into(),
            scheduler_state: "running".into(),
            pool_group: None,
            queued_ms: 1,
            pool_wait_ms: 0,
            run_ms: 2,
            progress_ticks: 3,
            rate: Some(4.0),
        };
        let serialized = serde_json::to_value(entry).unwrap();
        assert_eq!(serialized["job_id"], 7);
        assert_eq!(serialized["state"], "running");
        assert_eq!(serialized["scheduler_state"], "running");
        assert!(serialized.get("pool_group").is_none());
    }

    #[test]
    fn job_entry_state_and_scheduler_state_can_differ_on_wire() {
        let entry = JobEntry {
            job_id: 8,
            alias: "cairn".into(),
            analyzer_id: "rust-analyzer-lsp".into(),
            state: "running".into(),
            scheduler_state: "pool_waiting".into(),
            pool_group: Some("rust-analyzer-lsp".into()),
            queued_ms: 10,
            pool_wait_ms: 5,
            run_ms: 0,
            progress_ticks: 0,
            rate: None,
        };
        let serialized = serde_json::to_value(entry).unwrap();
        assert_eq!(serialized["state"], "running");
        assert_eq!(serialized["scheduler_state"], "pool_waiting");
    }

    #[test]
    fn outline_item_file_is_optional_on_wire() {
        let with_file = OutlineItem {
            file: Some("src/lib.rs".into()),
            kind: SymbolKind::Function,
            name: "demo".into(),
            qualified: "demo".into(),
            signature: None,
            line: 1,
            doc: None,
            source: SourceTier::Syntactic,
        };
        assert_eq!(
            serde_json::to_value(&with_file).unwrap()["file"],
            "src/lib.rs"
        );

        let without_file = OutlineItem {
            file: None,
            kind: SymbolKind::Function,
            name: "demo".into(),
            qualified: "demo".into(),
            signature: None,
            line: 1,
            doc: None,
            source: SourceTier::Syntactic,
        };
        assert!(
            serde_json::to_value(without_file)
                .unwrap()
                .get("file")
                .is_none()
        );
    }

    #[test]
    fn query_results_round_trip_diagnostics_and_hints_when_present() {
        fn assert_query_envelope<T>(value: serde_json::Value)
        where
            T: serde::de::DeserializeOwned + serde::Serialize,
        {
            let parsed: T = serde_json::from_value(value).unwrap();
            let serialized = serde_json::to_value(parsed).unwrap();
            assert_eq!(
                serialized["diagnostics"][0]["code"],
                "analyzer_not_scheduled"
            );
            assert_eq!(serialized["hints"][0]["code"], "empty_result_widen_scope");
        }

        let envelope = json!({
            "diagnostics": [{
                "code": "analyzer_not_scheduled",
                "severity": "warning",
                "message": "expected analyzer was not scheduled"
            }],
            "hints": [{
                "code": "empty_result_widen_scope",
                "message": "drop repo scope",
                "action": "widen_scope",
                "drop_params": ["repo"]
            }]
        });
        let with_items = |mut value: serde_json::Value| {
            let object = value.as_object_mut().unwrap();
            object.insert("items".into(), json!([]));
            value
        };

        assert_query_envelope::<OutlineResult>(with_items(envelope.clone()));
        assert_query_envelope::<FindSubtypesResult>(with_items(envelope.clone()));
        assert_query_envelope::<FindSupertypesResult>(with_items(envelope.clone()));
        assert_query_envelope::<ImportsResult>(with_items(envelope.clone()));
        assert_query_envelope::<FindReferencesResult>(with_items(envelope.clone()));
        assert_query_envelope::<FindCallersResult>(with_items(envelope.clone()));
        assert_query_envelope::<FindCalleesResult>(with_items(envelope.clone()));

        let mut source = envelope;
        let object = source.as_object_mut().unwrap();
        object.extend([
            ("qualified".into(), json!("demo::target")),
            ("name".into(), json!("target")),
            ("kind".into(), json!("function")),
            ("branch".into(), json!("HEAD")),
            ("location".into(), json!("demo:HEAD:src/lib.rs:1")),
            ("line_start".into(), json!(1)),
            ("line_end".into(), json!(1)),
            ("source".into(), json!("pub fn target() {}")),
            ("source_tier".into(), json!("syntactic")),
        ]);
        assert_query_envelope::<GetSymbolSourceResult>(source);
    }
}
