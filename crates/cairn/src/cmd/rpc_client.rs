//! Short-lived JSON-RPC client for Cairn daemon sockets.
//!
//! CLI and MCP front-ends all send one newline-delimited JSON-RPC
//! request and wait for one newline-delimited response. Keeping that
//! transport in one helper prevents subtle drift in EOF and parse-error
//! handling while leaving each command responsible for rendering.
//!
//! Wire shape: the client writes one line terminated by `\n` and
//! reads one line from the reply, matching the server-side framing
//! owned by `cairn-core`'s daemon accept loop and
//! `cairn_core::jsonrpc_dispatch`. `read_line` returns whatever it
//! got up to the first newline or EOF, so a reply that lacks the
//! terminating newline still parses if the daemon closed the
//! connection after writing it. A zero-length read is surfaced as
//! an explicit "daemon closed the connection without responding"
//! error rather than silent success. Each call opens a fresh UDS
//! connection and drops it when the response has been read — the
//! transport is intentionally connection-per-request.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use cairn_proto::control::DaemonInitializationStatus;
use cairn_proto::jsonrpc::{
    JsonRpcVersion, Request, RequestId, Response, ResponseError, error_code,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Convenience wrapper that builds a JSON-RPC 2.0 request with a
/// fixed numeric id of 1 and delegates to `send_request`. Since
/// each call uses its own connection there is no id collision
/// risk; callers that need a specific id should construct the
/// [`Request`] themselves. Notifications are not supported —
/// [`Request::id`] is a required field on this envelope.
pub(crate) async fn round_trip(
    socket_path: &Path,
    method: &str,
    params: Value,
) -> Result<Response> {
    let req = Request {
        jsonrpc: JsonRpcVersion::V2,
        id: RequestId::Number(1),
        method: method.into(),
        params: Some(params),
    };
    send_request(socket_path, &req).await
}

/// Send `req` over a fresh UDS connection and return the parsed
/// [`Response`]. Connect, write/flush, and read I/O errors are
/// propagated directly; a zero-length read is surfaced as an
/// explicit "daemon closed the connection without responding"
/// error; decode failure is a `serde_json` error on the response
/// line with the offending line attached. The returned
/// [`Response`] may itself carry a JSON-RPC `error` object;
/// callers decide how to render that (see [`render_error`]).
pub(crate) async fn send_request(socket_path: &Path, req: &Request) -> Result<Response> {
    let stream = UnixStream::connect(socket_path).await?;
    let (read, mut write) = stream.into_split();
    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    write.write_all(line.as_bytes()).await?;
    write.flush().await?;

    let mut reader = BufReader::new(read);
    let mut buf = String::new();
    let n = reader.read_line(&mut buf).await?;
    if n == 0 {
        return Err(anyhow!("daemon closed the connection without responding"));
    }
    serde_json::from_str(buf.trim()).with_context(|| format!("parsing response: {}", buf.trim()))
}

/// Render a JSON-RPC `error` object to stderr. Emits an `error:`
/// line and, for the `DAEMON_INITIALIZING` code, an extra
/// `status:` progress line and an optional `hint:` line pulled
/// from the error's `data` payload — matching the fields the
/// daemon attaches while the [`StartupGate`](cairn_core::startup)
/// still has phases outstanding.
pub(crate) fn render_error(error: &ResponseError) {
    for line in error_lines(error) {
        eprintln!("{line}");
    }
}

/// Format a JSON-RPC error into 1–3 lines: the base `error:` line
/// is always present. For `DAEMON_INITIALIZING` payloads a
/// `status:` line is added when the `initialization` field decodes,
/// and a `hint:` line is added when the first `hints` entry has a
/// string `message` field — the two lines are independent, so
/// either may appear alone. Any missing or unparseable field is
/// silently omitted rather than producing a partial line, so an
/// unknown data shape degrades cleanly to the base error line.
fn error_lines(error: &ResponseError) -> Vec<String> {
    let mut lines = vec![format!("error: {}", error.message)];
    if error.code != error_code::DAEMON_INITIALIZING {
        return lines;
    }
    let Some(data) = &error.data else {
        return lines;
    };
    if let Ok(status) = serde_json::from_value::<DaemonInitializationStatus>(
        data.get("initialization").cloned().unwrap_or(Value::Null),
    ) {
        let detail = status
            .detail
            .map(|detail| format!(" ({})", detail.label()))
            .unwrap_or_default();
        lines.push(format!(
            "status: initializing {}/{}: {}{}",
            status.completed_phases,
            status.total_phases,
            status.phase.label(),
            detail
        ));
    }
    if let Some(message) = data
        .get("hints")
        .and_then(Value::as_array)
        .and_then(|hints| hints.first())
        .and_then(|hint| hint.get("message"))
        .and_then(Value::as_str)
    {
        lines.push(format!("hint: {message}"));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_proto::jsonrpc::{error_code, error_response, ok_response};
    use serde_json::json;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn round_trip_writes_request_and_reads_response() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            assert!(request.contains("\"method\":\"status\""));
            let mut line =
                serde_json::to_string(&ok_response(RequestId::Number(1), json!({"ok": true})))
                    .unwrap();
            line.push('\n');
            write.write_all(line.as_bytes()).await.unwrap();
            write.flush().await.unwrap();
        });

        let response = round_trip(&socket, "status", Value::Null).await.unwrap();

        server.await.unwrap();
        assert_eq!(response.result, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn send_request_preserves_error_response() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("rpc.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (_read, mut write) = stream.into_split();
            let mut line = serde_json::to_string(&error_response(
                RequestId::Number(1),
                error_code::METHOD_NOT_FOUND,
                "unknown method",
            ))
            .unwrap();
            line.push('\n');
            write.write_all(line.as_bytes()).await.unwrap();
            write.flush().await.unwrap();
        });
        let req = Request {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(1),
            method: "missing".into(),
            params: Some(Value::Null),
        };

        let response = send_request(&socket, &req).await.unwrap();

        server.await.unwrap();
        assert_eq!(response.error.unwrap().code, error_code::METHOD_NOT_FOUND);
    }

    #[test]
    fn initializing_error_renders_closed_progress_and_one_shot_hint() {
        let error = ResponseError {
            code: error_code::DAEMON_INITIALIZING,
            message: "daemon is initializing".into(),
            data: Some(json!({
                "initialization": {
                    "state": "initializing",
                    "phase": "watcher_barrier",
                    "completed_phases": 4,
                    "total_phases": 7,
                    "detail": "arming_registered_watchers"
                },
                "hints": [{"message": "Retry after initialization completes."}]
            })),
        };

        assert_eq!(
            error_lines(&error),
            vec![
                "error: daemon is initializing",
                "status: initializing 4/7: watcher barrier (arming registered watchers)",
                "hint: Retry after initialization completes.",
            ]
        );
    }
}
