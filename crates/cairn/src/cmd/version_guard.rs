//! Daemon/client version guard shared by CLI and MCP front-ends.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use cairn_proto::control::StatusReport;
use cairn_proto::jsonrpc::Response;
use cairn_proto::version::{VersionCompatibility, pre_one_zero_compat};
use serde_json::Value;

use super::rpc_client;

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
