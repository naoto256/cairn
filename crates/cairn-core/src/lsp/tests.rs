use super::client::{StderrTail, parse_definition_result};
use super::reader::{ProgressState, WorkspaceLoadComplete, response_result};
use super::transport::{MAX_BODY_SIZE, read_lsp_message, write_lsp_message};
use super::*;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncWrite, AsyncWriteExt, DuplexStream, split};
use tokio::time::timeout;

mod client;
mod protocol;
mod reader;

#[test]
fn timeout_variants_have_distinct_operator_messages() {
    assert_eq!(
        Error::ReadinessTimeout.to_string(),
        "LSP readiness timed out"
    );
    assert_eq!(Error::RequestTimeout.to_string(), "LSP request timed out");
    assert!(Error::ReadinessTimeout.is_timeout());
    assert!(Error::RequestTimeout.is_timeout());
    assert!(!Error::Handshake("boom".to_string()).is_timeout());
}

enum FakeMode {
    Normal,
    DefinitionTimeout,
    CrashAfterInitialize,
    ProgressCompletes,
    ProgressNeverEnds,
    ProgressEndWithoutBegin,
    ServerStatusQuiescent,
    RequireServerStatusOptIn,
    RequireDidOpen,
    RequireProgressCreateResponse,
    RecordDocumentSync(tokio::sync::mpsc::UnboundedSender<Value>),
}

async fn fake_server(stream: DuplexStream, mode: FakeMode) {
    let (mut reader, mut writer) = split(stream);
    let mut did_open = false;
    let mut progress_create_response = false;
    let mut pending_definition = None;
    while let Some(message) = read_lsp_message(&mut reader).await.unwrap() {
        let method = message.get("method").and_then(Value::as_str);
        let id = message.get("id").and_then(Value::as_u64);
        match (method, id) {
            (None, Some(9001)) if matches!(mode, FakeMode::RequireProgressCreateResponse) => {
                progress_create_response = true;
                if let Some(id) = pending_definition.take() {
                    write_definition_result(&mut writer, id).await;
                }
            }
            (Some("initialize"), Some(id)) => {
                if matches!(mode, FakeMode::RequireServerStatusOptIn) {
                    let enabled = message
                        .get("params")
                        .and_then(|params| params.get("initializationOptions"))
                        .and_then(|options| options.get("experimental"))
                        .and_then(|experimental| experimental.get("serverStatusNotification"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    assert!(
                        enabled,
                        "initialize did not opt into rust-analyzer/serverStatus: {message}"
                    );
                }
                write_lsp_message(
                    &mut writer,
                    &json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "capabilities": {
                                "definitionProvider": true
                            }
                        }
                    }),
                )
                .await
                .unwrap();
            }
            (Some("initialized"), None) => {
                if matches!(mode, FakeMode::CrashAfterInitialize) {
                    return;
                }
                if matches!(mode, FakeMode::ServerStatusQuiescent) {
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "method": "rust-analyzer/serverStatus",
                            "params": {
                                "health": "ok",
                                "quiescent": true,
                                "message": null
                            }
                        }),
                    )
                    .await
                    .unwrap();
                }
                if matches!(mode, FakeMode::ProgressEndWithoutBegin) {
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "method": "$/progress",
                            "params": {
                                "token": "cairn-progress",
                                "value": { "kind": "end", "message": "ready" }
                            }
                        }),
                    )
                    .await
                    .unwrap();
                }
                if matches!(mode, FakeMode::RequireProgressCreateResponse) {
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "id": 9001,
                            "method": "window/workDoneProgress/create",
                            "params": { "token": "cairn-progress" }
                        }),
                    )
                    .await
                    .unwrap();
                }
                if matches!(
                    mode,
                    FakeMode::ProgressCompletes | FakeMode::ProgressNeverEnds
                ) {
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "id": 9001,
                            "method": "window/workDoneProgress/create",
                            "params": { "token": "cairn-progress" }
                        }),
                    )
                    .await
                    .unwrap();
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "method": "$/progress",
                            "params": {
                                "token": "cairn-progress",
                                "value": { "kind": "begin", "title": "loading workspace" }
                            }
                        }),
                    )
                    .await
                    .unwrap();
                    if matches!(mode, FakeMode::ProgressCompletes) {
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "method": "$/progress",
                                "params": {
                                    "token": "cairn-progress",
                                    "value": { "kind": "end", "message": "ready" }
                                }
                            }),
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            (
                Some("textDocument/didOpen" | "textDocument/didChange" | "textDocument/didClose"),
                None,
            ) => {
                if matches!(method, Some("textDocument/didOpen")) {
                    did_open = true;
                }
                if let FakeMode::RecordDocumentSync(tx) = &mode {
                    tx.send(message).unwrap();
                }
            }
            (Some("textDocument/definition"), Some(id)) => {
                if matches!(mode, FakeMode::DefinitionTimeout) {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                } else if matches!(mode, FakeMode::RequireProgressCreateResponse)
                    && !progress_create_response
                {
                    pending_definition = Some(id);
                } else if matches!(mode, FakeMode::RequireDidOpen) && !did_open {
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": -32602,
                                "message": "file not found"
                            }
                        }),
                    )
                    .await
                    .unwrap();
                } else {
                    write_definition_result(&mut writer, id).await;
                }
            }
            (Some("shutdown"), Some(id)) => {
                write_lsp_message(
                    &mut writer,
                    &json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": Value::Null
                    }),
                )
                .await
                .unwrap();
            }
            (Some("exit"), None) => return,
            _ => {}
        }
    }
}

async fn write_definition_result<W>(writer: &mut W, id: u64)
where
    W: AsyncWrite + Unpin,
{
    write_lsp_message(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": [
                {
                    "uri": "file:///tmp/cairn%20fake/src/lib.rs",
                    "range": {
                        "start": { "line": 2, "character": 8 },
                        "end": { "line": 2, "character": 14 }
                    }
                }
            ]
        }),
    )
    .await
    .unwrap();
}

fn progress_message(token: &str, kind: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "$/progress",
        "params": {
            "token": token,
            "value": { "kind": kind }
        }
    })
}
