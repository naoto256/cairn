//! `cairn-core` — daemon internals.
//!
//! Owns the per-repo CAS store (blob → parsed_data, manifest, anchor),
//! the JSON-RPC / control dispatch over the daemon sockets, and the
//! orchestration that registers a repo and keeps its index current.

// `linkme`'s `#[distributed_slice]` expansion emits a static with
// `link_section`, which rustc flags under the `unsafe_code` lint
// group. We write no unsafe code ourselves; `deny` keeps the door
// shut against accidental `unsafe` blocks while allowing explicit
// `#[allow(unsafe_code)]` on the linker-section attributes the
// distributed-slice machinery requires. Sibling crates that only
// host the slice DECLARATION (cairn-lang-api) can stay on `forbid`;
// crates with the ENTRIES (this one) cannot.
#![deny(unsafe_code)]

pub mod anchor;
pub mod cas;
pub mod ctl;
pub mod daemon;
pub mod data_rpc;
pub(crate) mod enrichment;
pub mod manifest;
pub mod migration;
pub mod paths;
pub mod query;
pub mod register;
pub mod sockets;
#[cfg(test)]
pub(crate) mod testutil;
pub mod timefmt;

pub use sockets::SocketPaths;

/// Crate-level error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("schema corruption: {0}")]
    SchemaCorruption(String),
}

pub type Result<T> = std::result::Result<T, Error>;
