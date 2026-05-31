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

use crate::common::{Completeness, RefKind, SourceTier, SymbolKind};

// ─── list_repos ─────────────────────────────────────────────────────────────

/// Result of `list_repos`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListReposResult {
    pub repos: Vec<RepoEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoEntry {
    pub alias: String,
    pub root: String,
    pub languages: Vec<String>,
    pub snapshots: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub branch: String,
    pub status: String,
    pub enrichment: SourceTier,
    pub last_accessed: Option<String>,
    pub file_count: u64,
    pub symbol_count: u64,
}

// ─── get_outline ────────────────────────────────────────────────────────────

/// Arguments to `get_outline`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlineArgs {
    pub repo: String,
    pub file: String,
}

/// Result of `get_outline`. Empty `doc`/`signature` fields are omitted on the
/// wire to keep token usage tight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlineResult {
    pub items: Vec<OutlineItem>,
    /// Completeness of this outline. `Partial` when the file's
    /// Tier-2 enrichment (doc overrides, semantic signatures) had
    /// not finished, or when a large file timed out mid-extraction.
    /// `#[serde(default)]` keeps older clients that omit the field
    /// readable; absence is treated as `Complete`.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlineItem {
    pub kind: SymbolKind,
    pub name: String,
    pub qualified: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub line: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
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
/// `repo = None` searches every registered repo. `branch = None`
/// searches every snapshot owned by the (matching) repo. Pass them to
/// narrow. Hits always name the repo / branch they came from.
///
/// `query` was required in 0.2.0 and is now optional so that
/// `{kind: "class"}` alone enumerates classes, `{container: "Foo"}`
/// alone enumerates Foo's members, etc.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FindSymbolArgs {
    /// Text matched against `name` / `qualified` (exact when `fuzzy`
    /// is false, FTS5 over name + qualified + doc when true). Optional
    /// from 0.2.1; pair with `kind` / `container` / `path` instead
    /// when you want a structural enumeration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Repository alias. `None` searches every registered repo (the
    /// "which repo has parse_args?" case); pass a concrete alias to
    /// restrict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
    /// `tentative/<id>`). Takes priority over `branch` when set;
    /// supplying only the bare branch name still works via `branch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
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
    /// File-path prefix (relative to the repo root). Only symbols
    /// whose `file.path` starts with this prefix are returned. Useful
    /// for "show me the classes in `crates/foo/`".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub fuzzy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSymbolResult {
    pub items: Vec<FindSymbolHit>,
    /// `Partial` when the FTS index or a snapshot DB had not yet
    /// reached steady state — a name that exists on disk may not
    /// appear in `items` even though the file was discovered.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindSymbolHit {
    pub id: i64,
    pub qualified: String,
    pub name: String,
    pub kind: SymbolKind,
    /// Repository alias the hit came from. Lets a cross-repo query
    /// distinguish identically-qualified symbols across registered
    /// repos. The wire format always emits this; older clients that
    /// passed `repo` explicitly can ignore it.
    pub repo: String,
    /// Snapshot the hit came from. Carried alongside `location` so a
    /// cross-branch query (the default) can distinguish identically
    /// located symbols that exist on multiple branches.
    pub branch: String,
    /// `repo:branch:file:line` string, clickable in Claude Code UI.
    pub location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub source: SourceTier,
}

// ─── impls / imports (semantic enrichment query surface) ──────────────────
//
// These return facts the Tier-2 analyzer populates: trait/impl edges
// from `syn` for Rust, and `use` statements flattened to one row per
// imported name. Same envelope shape as `find_symbols` — `repo` is
// required, `branch` defaults to cross-branch, hits carry their
// originating branch.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImplsArgs {
    pub repo: String,
    /// Match impl edges whose **trait** side equals this name. Use
    /// when answering "what implements `Display`?". Either `trait_`
    /// or `type_` must be set.
    #[serde(default, rename = "trait", skip_serializing_if = "Option::is_none")]
    pub trait_: Option<String>,
    /// Match impl edges whose **type** side equals this name. Use
    /// when answering "what traits does `Foo` implement?".
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
    /// `tentative/<id>`). Takes priority over `branch` when set;
    /// supplying only the bare branch name still works via `branch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImplsResult {
    pub items: Vec<ImplHit>,
    /// `Partial` when the Tier-2 analyzer (syn for Rust) had not
    /// finished on every file. Missing tier is `Semantic`. Items
    /// already extracted are still valid; new ones may arrive once
    /// indexing settles.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImplHit {
    /// Qualified name (or token form) of the type the impl is on.
    pub type_qualified: String,
    /// Qualified name of the trait the impl implements; `null` for
    /// an inherent impl.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_qualified: Option<String>,
    /// `"trait"` for trait impls, `"inherent"` for inherent impls.
    pub kind: String,
    pub branch: String,
    /// `repo:branch:file:line` pointing at the type-side symbol.
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportsArgs {
    pub repo: String,
    /// File whose imports to list (path relative to repo root). When
    /// omitted, every import in the (filtered) snapshot is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
    /// `tentative/<id>`). Takes priority over `branch` when set;
    /// supplying only the bare branch name still works via `branch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportsResult {
    pub items: Vec<ImportHit>,
    /// `Partial` when the Tier-2 analyzer had not finished — `use`
    /// edges come exclusively from semantic enrichment today.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
}

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
    pub branch: String,
    pub line: u32,
}

// ─── find_references ───────────────────────────────────────────────────────
//
// "Who calls / uses this symbol?" Driven by the Tier-2 `refs` table
// populated by the syn body-visitor (calls + method calls today; more
// kinds when the analyzer learns them). Cross-branch by default, same
// envelope shape as `find_symbols` / `find_impls`.

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
    pub repo: String,
    /// Name or qualified path of the **anchor** symbol. In the default
    /// `direction = Incoming` this is the *target* (who calls X);
    /// with `Outgoing` it is the *enclosing* container (what does X
    /// call). A token containing `::` matches the qualified form
    /// first; a bare name falls back to the looser name index.
    pub symbol: String,
    /// Restrict to a specific reference kind. When omitted every kind
    /// is returned (calls, method calls, type uses, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<RefKind>,
    /// `Incoming` (default) = "who references `symbol`"; `Outgoing`
    /// = "what does `symbol` reference" (callees + type uses inside
    /// `symbol`'s body). 0.2.1 surface completion.
    #[serde(default)]
    pub direction: ReferenceDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
    /// `tentative/<id>`). Takes priority over `branch` when set;
    /// supplying only the bare branch name still works via `branch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindReferencesResult {
    pub items: Vec<FindReferenceHit>,
    /// `Partial` carries two separate concerns for references:
    /// missing tiers (Tier-2 not yet run on every file) **and**
    /// resolution precision (method-call receivers can't be
    /// resolved without rust-analyzer-class Tier-3). The `reason`
    /// tag distinguishes the two for consumers that want to know.
    #[serde(default = "Completeness::complete")]
    pub completeness: Completeness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindReferenceHit {
    pub target_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_qualified: Option<String>,
    pub kind: RefKind,
    /// Qualified name of the function / impl block the reference sits
    /// inside. `None` for top-level expressions (rare in Rust).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_qualified: Option<String>,
    pub branch: String,
    /// `repo:branch:file:line` string, clickable in editors that
    /// recognise the format.
    pub location: String,
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
    pub repo: String,
    /// Qualified name of the symbol (matches `qualified` in the
    /// `symbols` table). Use `find_symbols` first if you only have a
    /// bare name and the repo has more than one match.
    pub qualified: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
    /// `tentative/<id>`). Takes priority over `branch` when set;
    /// supplying only the bare branch name still works via `branch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSymbolSourceResult {
    pub qualified: String,
    pub name: String,
    pub kind: SymbolKind,
    pub branch: String,
    /// `repo:branch:file:line` pointing at the first line of the
    /// returned source.
    pub location: String,
    pub line_start: u32,
    pub line_end: u32,
    /// Source text of the symbol, exactly as it appears in the file
    /// (no re-indentation or stripping).
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    pub source_tier: SourceTier,
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
    pub alias: String,
}

/// Result of `register_repo` / `reindex`. Carries the headline stats so
/// the agent can confirm the index produced something.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResult {
    pub alias: String,
    pub branch: String,
    pub files_indexed: u64,
    pub files_skipped: u64,
    pub symbols_inserted: u64,
}
