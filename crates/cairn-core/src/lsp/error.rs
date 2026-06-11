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
