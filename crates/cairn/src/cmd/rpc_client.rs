//! Short-lived JSON-RPC client for Cairn daemon sockets.
//!
//! CLI and MCP front-ends all send one newline-delimited JSON-RPC
//! request and wait for one newline-delimited response. Keeping that
//! transport in one helper prevents subtle drift in EOF and parse-error
//! handling while leaving each command responsible for rendering.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use cairn_proto::jsonrpc::{JsonRpcVersion, Request, RequestId, Response};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

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
}
