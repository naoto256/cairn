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
