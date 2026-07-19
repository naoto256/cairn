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
pub mod jobs;
pub(crate) mod jsonrpc_dispatch;
pub(crate) mod jsonrpc_errors;
pub mod lsp;
pub mod lsp_discovery;
pub mod manifest;
pub mod migration;
pub mod paths;
pub mod query;
pub mod reconcile;
pub mod register;
pub mod resolution;
pub mod sockets;
#[cfg(test)]
pub(crate) mod testutil;
pub mod timefmt;
pub mod watcher;
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
    /// A canonical repository exists in the registry but is not accepting
    /// new activity while registration or removal is in progress.
    #[error("repository `{repo_hash}` is unavailable ({state})")]
    RepositoryUnavailable {
        repo_hash: String,
        state: &'static str,
    },
    /// A no-create store open found no existing SQLite database.
    #[error("repository store not found: `{path}`")]
    StoreNotFound { path: String },
    /// Requested snapshot anchor is not present in the store.
    #[error("anchor not found: `{name}`")]
    AnchorNotFound { name: String },
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("job manager is shutting down")]
    JobManagerShuttingDown,
    #[error("daemon shutdown deadline exceeded after {timeout_ms}ms")]
    ShutdownDeadlineExceeded { timeout_ms: u64 },
    #[error(transparent)]
    Lsp(#[from] lsp::Error),
    #[error("schema corruption: {0}")]
    SchemaCorruption(String),
}

impl Error {
    /// Convert a blocking task join failure into a server-side error.
    ///
    /// The detail is logged for diagnosis, but callers only see the sanitized
    /// JSON-RPC `Internal` message so panic payloads cannot leak repo paths or
    /// in-memory state over the wire.
    pub fn internal_task_panic(context: impl Into<String>, err: tokio::task::JoinError) -> Self {
        let context = context.into();
        tracing::error!(context = %context, error = %err, "blocking task failed to join");
        Self::Internal(format!("{context} task panicked: {err}"))
    }
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
        assert_eq!(
            Error::Internal("task panicked: secret-path".into()).to_string(),
            "internal error: task panicked: secret-path"
        );
        assert_eq!(
            Error::JobManagerShuttingDown.to_string(),
            "job manager is shutting down"
        );
        assert_eq!(
            Error::ShutdownDeadlineExceeded { timeout_ms: 10_000 }.to_string(),
            "daemon shutdown deadline exceeded after 10000ms"
        );
    }

    #[tokio::test]
    async fn blocking_task_panic_maps_to_internal() {
        let err = tokio::task::spawn_blocking(|| panic!("secret panic payload"))
            .await
            .unwrap_err();
        let err = Error::internal_task_panic("test", err);

        assert!(matches!(err, Error::Internal(message) if message.contains("test task panicked")));
    }
}
