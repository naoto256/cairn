//! `cairn-core` — daemon internals.
//!
//! Owns the per-repo CAS store (blob → parsed_data, manifest, anchor),
//! the JSON-RPC / control dispatch over the daemon sockets, and the
//! orchestration that registers a repo and keeps its index current.
//!
//! The crate-facing orchestration surface mostly returns [`Error`]
//! and the crate's `Result` alias so callers can `?`-chain; some
//! sub-modules (notably `lsp` and parts of `cas`) still expose
//! module-local error types or raw `rusqlite`/`std` results at
//! their inner boundaries. The mapping from [`Error`] to JSON-RPC
//! responses — including the sanitization rule for
//! [`Error::Internal`] — lives in `jsonrpc_errors`.

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
pub(crate) mod freshness;
pub mod jobs;
pub(crate) mod jsonrpc_dispatch;
pub(crate) mod jsonrpc_errors;
pub mod lifecycle;
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
pub mod startup;
#[cfg(test)]
pub(crate) mod testutil;
pub mod timefmt;
pub mod watcher;
pub mod workspace_analyzer;

/// Convenience re-export so callers can build the daemon socket
/// bundle without importing [`sockets`] directly.
pub use sockets::SocketPaths;

/// Crate-level error type.
///
/// The crate-facing orchestration surface converges on this enum so
/// callers can `?`-chain; inner modules may still raise other error
/// types before they reach this boundary. Variants split by their
/// wire mapping:
///
/// - Variants with explicit / non-default JSON-RPC mappings
///   (audience varies): [`Error::InvalidParams`],
///   [`Error::InvalidArgument`], [`Error::RepoNotFound`],
///   [`Error::AnchorNotFound`], [`Error::FileNotIndexed`],
///   [`Error::SnapshotStale`], [`Error::AmbiguousSource`],
///   [`Error::DaemonInitializing`].
/// - Variants that fall through to the default `INTERNAL_ERROR`
///   mapping: [`Error::Io`], [`Error::Sqlite`], [`Error::Scan`],
///   [`Error::Internal`], [`Error::Lsp`],
///   [`Error::JobManagerShuttingDown`],
///   [`Error::ShutdownDeadlineExceeded`],
///   [`Error::RepositoryUnavailable`], [`Error::StoreNotFound`],
///   [`Error::SchemaCorruption`].
///
/// The wire translation lives in `jsonrpc_errors::error_from`;
/// only [`Error::Internal`] has its message body sanitized before
/// crossing the boundary.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Filesystem, process, or socket I/O failure bubbled from a
    /// `?` boundary. Covers many layers — socket paths, git spawn,
    /// lifecycle removal — so callers should not branch on this
    /// variant alone. LSP transport failures surface as
    /// [`Error::Lsp`] rather than here. Maps to a JSON-RPC internal
    /// error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Storage-layer failure from `rusqlite`. Bubbles from every
    /// SQLite user in the crate — CAS store open, migrations,
    /// registry, manifest, anchor, query, and reconcile paths.
    /// Maps to a JSON-RPC internal error.
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Repository walk during manifest publication reported at
    /// least one error. Only bubbled from
    /// `cairn_watch::scan::walk_repo(..).into_entries()` inside
    /// [`manifest`]. Maps to a JSON-RPC internal error.
    #[error("scan: {0}")]
    Scan(#[from] cairn_watch::scan::ScanFailure),
    /// JSON-RPC parameter parse / validation failure.
    ///
    /// Raised by the envelope layer where `serde_json::from_value`
    /// fails to shape `params` into the typed argument struct, plus
    /// a handful of methods for structurally malformed inputs that
    /// survived shape validation. Maps to `INVALID_PARAMS`; the
    /// message body is preserved on the wire.
    #[error("invalid params: {0}")]
    InvalidParams(String),
    /// User-facing repo alias is not registered.
    ///
    /// Resolved against the registry from a caller's `params.repo`.
    /// Maps to `REPO_NOT_FOUND` and attaches a `repo_not_registered`
    /// hint so the client can prompt the user to call
    /// `register_repo`.
    #[error("unknown repo alias: `{alias}`")]
    RepoNotFound { alias: String },
    /// A canonical repository exists in the registry but is not accepting
    /// new activity while registration or removal is in progress.
    ///
    /// Raised by [`lifecycle::RepoLifecycleManager`] gate helpers
    /// (`acquire`, `acquire_active`, `set_active`,
    /// `ensure_publishable`, `begin_removal`) when the current
    /// [`lifecycle::RepoActivityState`] disallows the requested
    /// operation. Gate helpers emit `"registering"`, `"removing"`,
    /// or `"removed"`; lifecycle guard paths additionally emit
    /// `"cleanup_pending"` and `"shutting_down"`. Removing/removed
    /// paths are the common cause. Falls through to the default
    /// JSON-RPC `INTERNAL_ERROR` mapping.
    #[error("repository `{repo_hash}` is unavailable ({state})")]
    RepositoryUnavailable {
        repo_hash: String,
        state: &'static str,
    },
    /// A no-create store open found no existing SQLite database.
    ///
    /// Raised by [`cas::store`] `open_existing`, which opens without
    /// `SQLITE_OPEN_CREATE` and then disambiguates the failure via
    /// `std::fs::metadata`. Only surfaced when the file is genuinely
    /// absent — other open failures propagate as [`Error::Sqlite`]
    /// or [`Error::Io`].
    #[error("repository store not found: `{path}`")]
    StoreNotFound { path: String },
    /// Requested snapshot anchor is not present in the store.
    ///
    /// Raised by query helpers that call `anchor::resolve` or
    /// `anchor::get` and receive `None`. Maps to `INVALID_PARAMS`
    /// so the caller can retry with a different anchor rather than
    /// treat it as a server-side fault.
    #[error("anchor not found: `{name}`")]
    AnchorNotFound { name: String },
    /// A file-target query cannot distinguish a genuinely empty result from
    /// a file missing in, or a stale publication of, the selected snapshot.
    ///
    /// Only raised by `data_rpc::methods::get_symbol_source::execute`
    /// today — either when the snapshot-freshness synthesis flags a
    /// mismatch, or when the CAS blob loader detects that the
    /// on-disk file drifted from the indexed blob. Maps to
    /// `FILE_NOT_INDEXED` with a `wait_for_index` hint that points
    /// callers at `cairn ctl repo reindex <alias>`.
    #[error("file `{file}` is not indexed in a freshness-verified snapshot{repo_suffix}: {reason}", repo_suffix = repo.as_ref().map(|repo| format!(" for repo `{repo}`")).unwrap_or_default())]
    FileNotIndexed {
        repo: Option<String>,
        file: String,
        reason: String,
    },
    /// A non-file-targeted lookup cannot distinguish a genuine miss from a
    /// stale current snapshot.
    ///
    /// Only raised by `data_rpc::methods::get_symbol_source::execute`
    /// today, on the non-file-targeted branch of the same freshness
    /// check that produces [`Error::FileNotIndexed`]. Maps to
    /// `SNAPSHOT_STALE` with a `wait_for_index` hint.
    #[error("current snapshot freshness could not be proven{repo_suffix}: {reason}", repo_suffix = repo.as_ref().map(|repo| format!(" for repo `{repo}`")).unwrap_or_default())]
    SnapshotStale {
        repo: Option<String>,
        reason: String,
    },
    /// The daemon socket is available, but startup has not atomically
    /// published the resource bundle required by this method.
    ///
    /// Constructed by `startup::StartupControlHandler` for any
    /// control-plane method other than `status`/`shutdown` while
    /// initializing, and by `startup::initializing_error` for every
    /// data-plane request that arrives before the ready bundle is
    /// published. Maps to `DAEMON_INITIALIZING` with a progress
    /// snapshot (`phase`, `completed_phases`, `total_phases`) the
    /// client can display or poll on.
    #[error(
        "daemon is initializing ({completed}/{total}: {phase:?})",
        completed = initialization.completed_phases,
        total = initialization.total_phases,
        phase = initialization.phase
    )]
    DaemonInitializing {
        initialization: cairn_proto::control::DaemonInitializationStatus,
    },
    /// A source lookup matched multiple physical declarations and requires a
    /// narrower repo, file, or line selector.
    ///
    /// Only raised by `data_rpc::methods::get_symbol_source`, which
    /// collects candidates across the resolved repo set and returns
    /// this once more than one physical declaration remains. Maps
    /// to `AMBIGUOUS_SOURCE`. Resolve it by narrowing the query
    /// (add `repo`, `file`, or `line`); `candidates_truncated`
    /// signals that the daemon capped the list, so the returned
    /// set is not exhaustive.
    #[error(
        "multiple source declarations match qualified name `{qualified}`; add `repo`, `file`, or `line`"
    )]
    AmbiguousSource {
        qualified: String,
        candidates: Vec<cairn_proto::methods::SymbolSourceCandidate>,
        candidates_truncated: bool,
    },
    /// Legacy catch-all for both semantic validation failures inside
    /// business logic (the JSON-RPC envelope shape was accepted, but
    /// the argument contents were rejected — reconcile, registration,
    /// socket-path validation, job / analyzer id lookup, typed
    /// data-RPC methods) and various operational failures that do
    /// not fit another variant (path canonicalization, data-directory
    /// setup, clock queries, git spawn, parser lookup, watcher arm).
    /// Maps to `INVALID_PARAMS`; the message body is preserved on
    /// the wire.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Server-side fault that should not be attributed to the
    /// caller. The wire mapping in `jsonrpc_errors::error_from`
    /// collapses the message to a fixed `"internal error"` string
    /// so panic payloads, repo paths, or in-memory state cannot
    /// leak. Use [`Error::internal_task_panic`] for blocking-task
    /// join failures.
    #[error("internal error: {0}")]
    Internal(String),
    /// Job queue refused admission because [`jobs::JobManager`] has
    /// entered shutdown. Raised only by the queue-admission path in
    /// [`jobs`]; callers should treat it as terminal and abandon
    /// the enqueue. Maps to a JSON-RPC internal error.
    #[error("job manager is shutting down")]
    JobManagerShuttingDown,
    /// Daemon shutdown supervisor's `tokio::time::timeout` around
    /// teardown elapsed. Raised only by [`daemon`] and typically
    /// observable only from process-exit paths — sockets are being
    /// closed, so a client is unlikely to receive it over the wire.
    #[error("daemon shutdown deadline exceeded after {timeout_ms}ms")]
    ShutdownDeadlineExceeded { timeout_ms: u64 },
    /// Language-server / workspace-analyzer failure surfaced from
    /// [`lsp::Error`]. Raised primarily by
    /// [`workspace_analyzer`] around LSP acquire, request, and
    /// file-URL construction. Maps to a JSON-RPC internal error;
    /// the underlying `lsp::Error` display is preserved in the
    /// message (via `#[error(transparent)]`).
    #[error(transparent)]
    Lsp(#[from] lsp::Error),
    /// Reserved for schema-invariant violations detected at read
    /// time. Currently unused — no construction site exists — but
    /// kept so future migrations can surface a distinct code
    /// without reshaping the enum.
    #[error("schema corruption: {0}")]
    SchemaCorruption(String),
}

impl Error {
    /// Primary SQLite result code, when this error came from SQLite.
    pub(crate) fn sqlite_error_code(&self) -> Option<rusqlite::ffi::ErrorCode> {
        match self {
            Self::Sqlite(error) => error.sqlite_error_code(),
            _ => None,
        }
    }

    /// Extended SQLite result code, when this error came from SQLite.
    pub(crate) fn sqlite_extended_code(&self) -> Option<i32> {
        match self {
            Self::Sqlite(error) => error.sqlite_error().map(|code| code.extended_code),
            _ => None,
        }
    }

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

/// Ergonomic alias so `crate::Result<T>` binds [`Error`] by default.
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
        assert_eq!(
            Error::DaemonInitializing {
                initialization: cairn_proto::control::DaemonInitializationStatus::initializing(
                    cairn_proto::control::DaemonInitializationPhase::WatcherBarrier,
                    Some(
                        cairn_proto::control::DaemonInitializationDetail::ArmingRegisteredWatchers,
                    ),
                ),
            }
            .to_string(),
            "daemon is initializing (4/7: WatcherBarrier)"
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
