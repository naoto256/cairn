//! Concrete MCP tools.
//!
//! Each sub-module owns one tool end-to-end: its [`super::ToolSpec`]
//! (name + description + JSON schema), its argument-parsing rules,
//! and a `#[distributed_slice]` entry that drops it into
//! [`super::MCP_TOOLS`] at link time. Adding a new tool is a
//! single-file change; the dispatcher picks it up without any local
//! edits.

mod find_impls;
mod find_imports;
mod find_references;
mod find_symbols;
mod get_outline;
mod get_symbol_source;
mod list_repos;
mod register_repo;
mod reindex_repo;

pub(super) const BRANCH_PARAM_DESC: &str = "Restrict to a single snapshot by bare branch name (for example `main` or `release/0.1.0`). Do not pass `HEAD`, `tag/<v>`, or `tentative/<id>` here; those are anchor names. Omit both `branch` and `anchor` to default to `HEAD`.";

pub(super) const ANCHOR_PARAM_DESC: &str = "Raw anchor name: `HEAD` (default), `branch/<n>`, `tag/<n>`, or `tentative/<id>`. Takes priority over `branch`. Use this to target HEAD or a tag/tentative snapshot; use `branch` for a bare branch name.";

pub(super) const COMPLETENESS_REASON_DESC: &str = "Results may carry `completeness: partial` with one of: `cap` — limit reached; raise `limit` to see more. `tier2_warming` — Tier-2 semantic analyzer is still indexing this snapshot. `tier3_warming` — Tier-3 workspace analyzer is still indexing. `tier3_unavailable` — Tier-3 binary was not found; Tier-1 / Tier-2 facts only. `analyzer_failed` — analyzer crashed on this snapshot; check `cairn ctl status`.";
