//! Concrete data-RPC methods.
//!
//! Each sub-module owns one JSON-RPC method end-to-end: the arg
//! struct (re-exported from [`cairn_proto::methods`]), the SQL it
//! runs against the snapshot DB, and a `#[distributed_slice]` entry
//! that drops it into [`super::DATA_METHODS`] at link time. Adding
//! a new method is a single-file change; the dispatcher in
//! [`super::DataRpc`] picks it up without any local edits.

mod find_impls;
mod find_imports;
mod find_references;
mod find_symbols;
mod get_outline;
mod get_symbol_source;
mod list_repos;
