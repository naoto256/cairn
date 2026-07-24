//! Content-addressed storage layer.
//!
//! Owns the per-repo SQLite store that holds parsed-data keyed by
//! `(blob_sha, parser_id)`. The manifest and anchor layers (sibling
//! modules) build on the same connection to map paths and refs onto
//! blob_shas.

pub mod blob;
pub mod hash;
pub mod kind_conv;
pub mod parse;
// Exception to the per-repo scope above: `registry` owns the
// daemon-global `index.db` (repository identity, aliases, durable
// reconcile state), a physically separate database from the per-repo
// stores under `repos/`.
pub mod registry;
pub mod schema;
pub mod store;

pub use blob::{BlobMeta, ParsedData};
pub use hash::git_blob_sha;
pub use parse::parse;
