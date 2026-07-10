use std::path::PathBuf;
use std::process::ExitStatus;

pub const CONTENT_MODIFIED_ERROR_CODE: i64 = -32801;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LSP binary not available: {0}")]
    BinaryMissing(PathBuf),
    #[error("LSP workspace unsuitable: {0}")]
    WorkspaceUnsuitable(String),
    #[error("failed to spawn LSP server: {0}")]
    Spawn(std::io::Error),
    #[error("LSP handshake failed: {0}")]
    Handshake(String),
    #[error("LSP readiness timed out")]
    ReadinessTimeout,
    #[error("LSP request timed out")]
    RequestTimeout,
    #[error("LSP server exited{0}")]
    ServerExited(ExitStatusDetail),
    #[error("LSP server exited{status}; stderr: {stderr}")]
    ServerExitedWithStderr {
        status: ExitStatusDetail,
        stderr: String,
    },
    #[error("LSP protocol error: {0}")]
    Protocol(String),
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
    /// The pool is poisoned because a prior child cleanup could
    /// not prove that the child terminated. This can originate
    /// from `force_shutdown_all` (outer timeout or an entry that
    /// returned termination-unproven), an LRU eviction whose
    /// victim shutdown returned termination-unproven, or any
    /// termination-unproven error surfaced through the central
    /// `LspClientPool::with_lsp` exit point. New acquisitions are
    /// permanently rejected until the daemon restarts, so the
    /// daemon cannot silently create a replacement child alongside
    /// a possibly-still-live orphan.
    #[error(
        "LSP pool is poisoned by a prior child cleanup that could not prove termination; restart the daemon to recover"
    )]
    PoolPoisoned,
    /// The pool has been finally shut down (daemon-level
    /// `shutdown_all`). No further acquisitions are permitted.
    #[error("LSP pool has been shut down")]
    PoolStopped,
    /// The child process could not be reaped via `wait()` after
    /// `kill()`. We cannot prove the child terminated, so callers
    /// must treat this as fail-closed (poison the pool rather than
    /// spawn a replacement child alongside a possibly-still-live
    /// orphan).
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

    #[must_use]
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::ReadinessTimeout | Self::RequestTimeout)
    }

    /// True when this error is or wraps a `ChildTerminationFailed`
    /// signal. Callers that manage child lifecycle must treat this
    /// as fail-closed: the child could not be proven dead, so no
    /// replacement may be spawned in its slot until the daemon
    /// restarts. Currently used by `LspClientPool` to poison itself
    /// on an LRU eviction whose victim shutdown left an unproven
    /// child, and by `force_shutdown_all` to poison on the same
    /// signal in addition to the outer timeout.
    #[must_use]
    pub fn is_termination_unproven(&self) -> bool {
        match self {
            Self::ChildTerminationFailed(_) => true,
            Self::OperationWithCleanupFailure { cleanup, .. } => cleanup.is_termination_unproven(),
            _ => false,
        }
    }
}

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
