//! Error taxonomy for the LSP client and pool.
//!
//! Rough grouping of the variants:
//!
//! - availability / startup: `BinaryMissing`, `WorkspaceUnsuitable`,
//!   `Spawn`, `Handshake`, `ReadinessTimeout`
//! - per-request: `RequestTimeout`, `Protocol`, `ResponseError`
//! - child death: `ServerExited`, `ServerExitedWithStderr`
//! - pool lifecycle: `PoolAtCapacity`, `PoolDraining`,
//!   `PoolPoisoned`, `PoolStopped`
//! - termination proof: `ChildTerminationFailed`,
//!   `OperationWithCleanupFailure`
//!
//! The `is_*` helpers below encode the retry / fail-closed
//! decisions callers depend on; keep them in sync when adding
//! variants.
use std::path::PathBuf;
use std::process::ExitStatus;

/// JSON-RPC error code `ContentModified` defined by the LSP spec:
/// the server dropped the request because the underlying document
/// state changed. Transient by nature; see
/// [`Error::is_content_modified`].
pub const CONTENT_MODIFIED_ERROR_CODE: i64 = -32801;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server binary was not found, or its availability probe
    /// (e.g. `--version`) exited unsuccessfully.
    #[error("LSP binary not available: {0}")]
    BinaryMissing(PathBuf),
    /// An analyzer-specific precondition on the workspace failed.
    /// Constructed by tier-3 language integrations rather than by
    /// this module; `workspace_analyzer::run` groups it with
    /// `BinaryMissing` as "analyzer unavailable" (the pass is
    /// skipped, not failed).
    #[error("LSP workspace unsuitable: {0}")]
    WorkspaceUnsuitable(String),
    /// OS-level failure spawning a child process. The availability
    /// probe maps `NotFound` to `BinaryMissing` first; the real
    /// server spawn reports every io error here (its binary was
    /// already probed successfully).
    #[error("failed to spawn LSP server: {0}")]
    Spawn(std::io::Error),
    /// The `initialize` exchange failed, or the freshly spawned
    /// child was missing a stdio pipe. Carries a flattened message,
    /// possibly with a captured stderr excerpt appended.
    #[error("LSP handshake failed: {0}")]
    Handshake(String),
    /// `wait_for_workspace_load` elapsed before the server's
    /// `$/progress` activity settled.
    #[error("LSP readiness timed out")]
    ReadinessTimeout,
    /// A request operation, notification write, or availability
    /// probe exceeded its timeout.
    #[error("LSP request timed out")]
    RequestTimeout,
    /// The child is gone or unreachable: stdout EOF / read error,
    /// no installed writer, or a reply channel dropped during
    /// teardown. The exit status is included only when one was
    /// actually observed.
    #[error("LSP server exited{0}")]
    ServerExited(ExitStatusDetail),
    /// [`Error::ServerExited`] with the captured stderr head/tail
    /// attached for diagnosis.
    #[error("LSP server exited{status}; stderr: {stderr}")]
    ServerExitedWithStderr {
        status: ExitStatusDetail,
        stderr: String,
    },
    /// Wire-level violations: framing, JSON, or unexpected response
    /// shapes. Also the lossy textual replica fanned out to pending
    /// requests when the reader loop dies with a non-`ServerExited`
    /// error, since `Error` is not `Clone`.
    #[error("LSP protocol error: {0}")]
    Protocol(String),
    /// The server answered a request with a JSON-RPC `error`
    /// object. `code` is preserved so callers can special-case
    /// specific codes (e.g. `ContentModified`).
    #[error("LSP protocol error: {message}")]
    ResponseError { code: i64, message: String },
    /// The pool is at capacity and no idle entry could be evicted
    /// to make room for a new spawn. Two conditions unblock the
    /// caller:
    ///
    /// - **A lease is released.** The record becomes idle and
    ///   eligible for LRU eviction; the slot itself is only
    ///   actually freed after the evicted entry's shutdown
    ///   completes.
    /// - **A currently in-flight eviction completes.** If every
    ///   slot is either leased or held by an `Evicting`
    ///   placeholder, no new key can spawn until at least one
    ///   placeholder is removed (which happens when its victim's
    ///   shutdown returns termination-proven).
    ///
    /// The operator can also trigger a manual reindex — cairn
    /// does not auto-retry a `Failed` job.
    #[error(
        "LSP pool at capacity ({capacity}); retry after a lease is released or the current eviction completes, or run cairn ctl repo reindex <alias>"
    )]
    PoolAtCapacity { capacity: usize },
    /// A cleanup / drain is in flight — either the whole pool
    /// (`force_shutdown_all` transitioned to `Draining`) or the
    /// specific key being acquired (LRU eviction victim in the
    /// `Evicting` placeholder state). New acquisitions of the
    /// affected key are rejected until the cleanup completes;
    /// blocking would risk spawning a replacement child while the
    /// old child is still being terminated.
    #[error("LSP pool is draining; retry after the current cleanup completes")]
    PoolDraining,
    /// An internal pool ownership or accounting invariant failed. New
    /// acquisitions are rejected until daemon restart so custody cannot be
    /// silently discarded. OS cleanup residuals use
    /// [`Error::ChildTerminationFailed`] and do not produce this state.
    #[error(
        "LSP pool halted after an internal ownership invariant failed; restart the daemon to recover"
    )]
    PoolPoisoned,
    /// The pool has been finally shut down (daemon-level
    /// `shutdown_all`). No further acquisitions are permitted.
    #[error("LSP pool has been shut down")]
    PoolStopped,
    /// The child process could not be reaped via `wait()` after
    /// `kill()`. The initiating caller receives this truthful residual while
    /// the pool releases terminal custody and continues admission.
    #[error("LSP child termination could not be proven: {0}")]
    ChildTerminationFailed(String),
    /// A cleanup step ran after the original operation failed. Both
    /// errors are preserved so tests / diagnostics can inspect
    /// either. `original` is the caller-visible failure; `cleanup`
    /// is the secondary termination / wait failure that must not
    /// mask it.
    #[error("{original}; cleanup: {cleanup}")]
    OperationWithCleanupFailure {
        original: Box<Error>,
        cleanup: Box<Error>,
    },
}

impl Error {
    /// Duplicate a typed error only for private cleanup-receipt observers.
    /// This deliberately does not make the public error taxonomy `Clone`.
    pub(crate) fn clone_for_cleanup(&self) -> Self {
        match self {
            Self::BinaryMissing(path) => Self::BinaryMissing(path.clone()),
            Self::WorkspaceUnsuitable(message) => Self::WorkspaceUnsuitable(message.clone()),
            Self::Spawn(error) => Self::Spawn(match error.raw_os_error() {
                Some(code) => std::io::Error::from_raw_os_error(code),
                None => std::io::Error::new(error.kind(), error.to_string()),
            }),
            Self::Handshake(message) => Self::Handshake(message.clone()),
            Self::ReadinessTimeout => Self::ReadinessTimeout,
            Self::RequestTimeout => Self::RequestTimeout,
            Self::ServerExited(status) => Self::ServerExited(ExitStatusDetail(status.0)),
            Self::ServerExitedWithStderr { status, stderr } => Self::ServerExitedWithStderr {
                status: ExitStatusDetail(status.0),
                stderr: stderr.clone(),
            },
            Self::Protocol(message) => Self::Protocol(message.clone()),
            Self::ResponseError { code, message } => Self::ResponseError {
                code: *code,
                message: message.clone(),
            },
            Self::PoolAtCapacity { capacity } => Self::PoolAtCapacity {
                capacity: *capacity,
            },
            Self::PoolDraining => Self::PoolDraining,
            Self::PoolPoisoned => Self::PoolPoisoned,
            Self::PoolStopped => Self::PoolStopped,
            Self::ChildTerminationFailed(message) => Self::ChildTerminationFailed(message.clone()),
            Self::OperationWithCleanupFailure { original, cleanup } => {
                Self::OperationWithCleanupFailure {
                    original: Box::new(original.clone_for_cleanup()),
                    cleanup: Box::new(cleanup.clone_for_cleanup()),
                }
            }
        }
    }
}

impl Error {
    /// True for a server response error carrying the LSP
    /// `ContentModified` code. Callers treat this as transient;
    /// `lsp_pass` retries such a definition request once.
    #[must_use]
    pub fn is_content_modified(&self) -> bool {
        matches!(
            self,
            Self::ResponseError {
                code: CONTENT_MODIFIED_ERROR_CODE,
                ..
            }
        )
    }

    /// True for both timeout shapes: readiness (workspace load) and
    /// per-request. Top-level only — it does not look inside
    /// `OperationWithCleanupFailure`.
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::ReadinessTimeout | Self::RequestTimeout)
    }

    /// True when this error is or wraps a `ChildTerminationFailed`
    /// signal. This classifies caller-visible OS/process evidence only; it is
    /// deliberately not a pool-mode decision. `LspClientPool` decides
    /// admission from its private typed cleanup disposition: OS residuals
    /// release exact custody, while ownership/accounting invariants halt it.
    #[must_use]
    pub fn is_termination_unproven(&self) -> bool {
        match self {
            Self::ChildTerminationFailed(_) => true,
            Self::OperationWithCleanupFailure { cleanup, .. } => cleanup.is_termination_unproven(),
            _ => false,
        }
    }
}

/// Optional child exit status formatted for the `ServerExited*`
/// messages: renders as `": <status>"` when the status was observed
/// and as an empty string otherwise, so the surrounding message
/// reads naturally in both cases.
#[derive(Debug)]
pub struct ExitStatusDetail(Option<ExitStatus>);

impl std::fmt::Display for ExitStatusDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(status) => write!(f, ": {status}"),
            None => Ok(()),
        }
    }
}

impl From<Option<ExitStatus>> for ExitStatusDetail {
    fn from(status: Option<ExitStatus>) -> Self {
        Self(status)
    }
}
