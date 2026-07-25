//! Daemon/client version guard shared by CLI and MCP front-ends.
//!
//! Compares the running daemon's `daemon_version` (via the control
//! socket `status` RPC) against this binary's `CARGO_PKG_VERSION`
//! using [`pre_one_zero_compat`], then decides per
//! [`VersionGuardMode`] whether to continue silently, warn on
//! stderr, or return an error.
//!
//! The compatibility table (defined in
//! [`cairn_proto::version::pre_one_zero_compat`]):
//!
//! - `SamePatch` / `PatchMismatch`: silent, continue.
//! - `MinorMismatch`: warn on stderr, continue in both modes.
//! - `MajorMismatch`: fatal for [`VersionGuardMode::Cli`], warn
//!   (do not fail) for [`VersionGuardMode::Mcp`] so `initialize`
//!   can still complete and the host surfaces the diagnostic.
//! - `Unparseable`: warn on stderr, continue — treated as
//!   "unknown compatibility" rather than an outright failure.
//!
//! If the status RPC itself fails (socket missing, daemon down,
//! transport error) the guard logs a warning and returns `Ok` so
//! downstream commands can surface the real failure with their
//! own richer error path.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use cairn_proto::control::StatusReport;
use cairn_proto::jsonrpc::Response;
use cairn_proto::version::{VersionCompatibility, pre_one_zero_compat};
use serde_json::Value;

use super::rpc_client;

/// Version string this binary reports to the guard — the same
/// value clap surfaces through `cairn --version`.
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionGuardMode {
    /// Interactive CLI commands may abort when the daemon is from an
    /// incompatible major version.
    Cli,
    /// MCP clients expect initialize to complete; warn on stderr and keep
    /// serving so the host can surface the diagnostic without breaking JSON-RPC.
    Mcp,
}

/// Run the version guard against the daemon reachable at
/// `socket_path`. Returns `Err` only in the CLI-fatal case
/// (`MajorMismatch` under [`VersionGuardMode::Cli`]); every other
/// outcome (mismatch warning, unparseable versions, transport
/// failure) is downgraded to a stderr warning and `Ok`.
pub(crate) async fn check_daemon_version(socket_path: &Path, mode: VersionGuardMode) -> Result<()> {
    let daemon_version = match daemon_version(socket_path).await {
        Ok(version) => version,
        Err(err) => {
            eprintln!("warning: could not verify cairn daemon version: {err}");
            return Ok(());
        }
    };

    match pre_one_zero_compat(&daemon_version, CLIENT_VERSION) {
        VersionCompatibility::SamePatch | VersionCompatibility::PatchMismatch => Ok(()),
        VersionCompatibility::MinorMismatch => {
            eprintln!("{}", version_warning(&daemon_version));
            Ok(())
        }
        VersionCompatibility::MajorMismatch if mode == VersionGuardMode::Mcp => {
            eprintln!("{}", version_warning(&daemon_version));
            Ok(())
        }
        VersionCompatibility::MajorMismatch => Err(anyhow!(version_error(&daemon_version))),
        VersionCompatibility::Unparseable => {
            eprintln!(
                "warning: could not compare cairn daemon version {daemon_version:?} with CLI version {CLIENT_VERSION:?}"
            );
            Ok(())
        }
    }
}

fn version_warning(daemon_version: &str) -> String {
    format!(
        "warning: cairn daemon is {daemon_version}, CLI is {CLIENT_VERSION}; restart the daemon with 'brew services restart cairn' or use 'cairn ctl daemon shutdown' then 'cairn daemon' (shutdown bypasses this guard) to pick up your installed CLI"
    )
}

fn version_error(daemon_version: &str) -> String {
    format!(
        "cairn daemon is {daemon_version}, CLI is {CLIENT_VERSION}; incompatible major versions, restart the daemon with the installed CLI before continuing"
    )
}

/// Extract just the `daemon_version` field from a control-socket
/// `status` reply. Reports transport failure, RPC-level `error`
/// objects, missing `result`, and decode failures as distinct
/// `anyhow` messages so the caller's warning text can point at the
/// specific step that broke.
async fn daemon_version(socket_path: &Path) -> Result<String> {
    let resp = control_status(socket_path).await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("status returned error: {}", err.message));
    }
    let value = resp
        .result
        .ok_or_else(|| anyhow!("status returned no result"))?;
    let report: StatusReport = serde_json::from_value(value).context("decoding status result")?;
    Ok(report.daemon_version)
}

/// Issue a single newline-delimited `status` JSON-RPC round trip
/// over the control socket via [`rpc_client::round_trip`].
async fn control_status(socket_path: &Path) -> Result<Response> {
    rpc_client::round_trip(socket_path, "status", Value::Null)
        .await
        .context("requesting daemon status")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_proto::control::StatusReport;
    use cairn_proto::jsonrpc::{RequestId, ok_response};
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn cli_guard_passes_matching_daemon_version() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("control.sock");
        let _server = spawn_status_server(socket.clone(), CLIENT_VERSION);

        check_daemon_version(&socket, VersionGuardMode::Cli)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cli_guard_warns_but_continues_on_minor_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("control.sock");
        let _server = spawn_status_server(socket.clone(), "0.3.0");

        check_daemon_version(&socket, VersionGuardMode::Cli)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cli_guard_aborts_on_major_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("control.sock");
        let _server = spawn_status_server(socket.clone(), "1.0.0");

        let err = check_daemon_version(&socket, VersionGuardMode::Cli)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("incompatible major versions"));
    }

    #[tokio::test]
    async fn version_guard_precedes_initialization_admission() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("control.sock");
        let initialization = cairn_proto::control::DaemonInitializationStatus::initializing(
            cairn_proto::control::DaemonInitializationPhase::WatcherBarrier,
            Some(cairn_proto::control::DaemonInitializationDetail::ArmingRegisteredWatchers),
        );
        let _server =
            spawn_status_server_with_initialization(socket.clone(), "1.0.0", initialization);

        let err = check_daemon_version(&socket, VersionGuardMode::Cli)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("incompatible major versions"));
        assert!(!err.to_string().contains("initializing"));
    }

    #[tokio::test]
    async fn mcp_guard_warns_but_continues_on_major_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("control.sock");
        let _server = spawn_status_server(socket.clone(), "1.0.0");

        check_daemon_version(&socket, VersionGuardMode::Mcp)
            .await
            .unwrap();
    }

    fn spawn_status_server(
        socket: std::path::PathBuf,
        version: &'static str,
    ) -> tokio::task::JoinHandle<()> {
        spawn_status_server_with_initialization(
            socket,
            version,
            cairn_proto::control::DaemonInitializationStatus::ready(),
        )
    }

    fn spawn_status_server_with_initialization(
        socket: std::path::PathBuf,
        version: &'static str,
        initialization: cairn_proto::control::DaemonInitializationStatus,
    ) -> tokio::task::JoinHandle<()> {
        let listener = UnixListener::bind(socket).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            assert!(request.contains("\"method\":\"status\""));
            let report = StatusReport {
                daemon_version: version.into(),
                uptime_secs: 1,
                initialization,
                repos: Vec::new(),
            };
            let response = ok_response(RequestId::Number(1), json!(report));
            let mut line = serde_json::to_string(&response).unwrap();
            line.push('\n');
            write.write_all(line.as_bytes()).await.unwrap();
            write.flush().await.unwrap();
        })
    }
}
