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
pub(crate) mod jsonrpc_errors;
pub mod manifest;
pub mod migration;
pub mod paths;
pub mod query;
pub mod register;
pub mod sockets;
#[cfg(test)]
pub(crate) mod testutil;
pub mod timefmt;
pub mod workspace_analyzer;

pub use sockets::SocketPaths;

/// Crate-level error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// JSON-RPC parameter parse / validation failure.
    #[error("invalid params: {0}")]
    InvalidParams(String),
    /// User-facing repo alias is not registered.
    #[error("unknown repo alias: `{alias}`")]
    RepoNotFound { alias: String },
    /// Requested snapshot anchor is not present in the store.
    #[error("anchor not found: `{name}`")]
    AnchorNotFound { name: String },
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("schema corruption: {0}")]
    SchemaCorruption(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn typed_error_display_formats_wire_messages() {
        assert_eq!(
            Error::InvalidParams("missing field `repo`".into()).to_string(),
            "invalid params: missing field `repo`"
        );
        assert_eq!(
            Error::RepoNotFound {
                alias: "demo".into()
            }
            .to_string(),
            "unknown repo alias: `demo`"
        );
        assert_eq!(
            Error::AnchorNotFound {
                name: "HEAD".into()
            }
            .to_string(),
            "anchor not found: `HEAD`"
        );
    }
}
