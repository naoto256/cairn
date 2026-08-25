use super::*;
use tokio::sync::oneshot;

#[tokio::test]
async fn stale_request_snapshot_does_not_write_to_replacement_transport() {
    let client = Arc::new(LspClient::configured(
        Path::new("/unused/fake-lsp"),
        Vec::new(),
        Vec::new(),
        Path::new("/tmp/cairn"),
        Value::Null,
        Duration::from_secs(1),
    ));
    let (old_client_io, old_server_io) = tokio::io::duplex(4096);
    let (old_reader, old_writer) = split(old_client_io);
    client.install_transport(old_reader, old_writer).await;
    let old_generation = client.transport_generation();

    let snapshot_seen = Arc::new(tokio::sync::Notify::new());
    let resume_request = Arc::new(tokio::sync::Notify::new());
    let snapshot_wait = snapshot_seen.notified();
    tokio::pin!(snapshot_wait);
    snapshot_wait.as_mut().enable();
    let stale_client = Arc::clone(&client);
    let stale_snapshot_seen = Arc::clone(&snapshot_seen);
    let stale_resume = Arc::clone(&resume_request);
    let stale = tokio::spawn(async move {
        stale_client
            .request_with_snapshot_hook::<Value, _, _>(
                "stale/request",
                Value::Null,
                move || async move {
                    stale_snapshot_seen.notify_one();
                    stale_resume.notified().await;
                },
            )
            .await
    });
    snapshot_wait.await;

    drop(old_server_io);
    timeout(Duration::from_millis(100), async {
        while client.transport_generation() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old reader did not publish transport exit");

    let (new_client_io, mut new_server_io) = tokio::io::duplex(4096);
    let (new_reader, new_writer) = split(new_client_io);
    client.install_transport(new_reader, new_writer).await;
    assert!(
        !client
            .force_terminate_generation_for_test(old_generation)
            .await
            .unwrap(),
        "stale generation cleanup must not clear replacement transport state"
    );
    resume_request.notify_one();

    let stale_err = timeout(Duration::from_millis(100), stale)
        .await
        .expect("stale request did not fail promptly")
        .unwrap()
        .unwrap_err();
    assert!(matches!(stale_err, Error::ServerExited(_)));
    assert!(
        timeout(
            Duration::from_millis(20),
            read_lsp_message(&mut new_server_io)
        )
        .await
        .is_err(),
        "stale request was written to the replacement transport"
    );

    let replacement_client = Arc::clone(&client);
    let replacement = tokio::spawn(async move {
        replacement_client
            .request_with_snapshot_hook::<Value, _, _>("replacement/request", Value::Null, || {
                std::future::ready(())
            })
            .await
    });
    let request = timeout(
        Duration::from_millis(100),
        read_lsp_message(&mut new_server_io),
    )
    .await
    .expect("replacement request was not written")
    .unwrap()
    .unwrap();
    assert_eq!(request["method"], "replacement/request");
    let id = request["id"].as_u64().unwrap();
    write_lsp_message(
        &mut new_server_io,
        &json!({"jsonrpc": "2.0", "id": id, "result": {"ok": true}}),
    )
    .await
    .unwrap();
    assert_eq!(
        replacement.await.unwrap().unwrap(),
        json!({"ok": true}),
        "replacement transport must remain usable"
    );
}

#[tokio::test]
async fn initialize_definition_and_shutdown_roundtrip() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let server = tokio::spawn(fake_server(server_io, FakeMode::Normal));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn fake"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let locations = client
        .definition(
            &Url::from("file:///tmp/cairn%20fake/src/lib.rs"),
            Position {
                line: 10,
                character: 4,
            },
        )
        .await
        .unwrap();

    assert_eq!(locations.len(), 1);
    assert_eq!(
        locations[0].uri.as_str(),
        "file:///tmp/cairn%20fake/src/lib.rs"
    );
    assert_eq!(locations[0].range.start.line, 2);

    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn initialize_opts_into_rust_analyzer_server_status() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let server = tokio::spawn(fake_server(server_io, FakeMode::RequireServerStatusOptIn));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn document_sync_notifications_use_full_text_payloads() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(fake_server(server_io, FakeMode::RecordDocumentSync(tx)));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let uri = Url::from("file:///tmp/cairn/src/lib.rs");

    client
        .did_open(&uri, "rust", 1, "fn main() {}\n")
        .await
        .unwrap();
    client
        .did_change(&uri, 2, "fn main() { println!(\"hi\"); }\n")
        .await
        .unwrap();
    client.did_close(&uri).await.unwrap();

    let open = rx.recv().await.unwrap();
    assert_eq!(
        open.get("method").and_then(Value::as_str),
        Some("textDocument/didOpen")
    );
    let open_doc = &open["params"]["textDocument"];
    assert_eq!(open_doc["uri"], uri.as_str());
    assert_eq!(open_doc["languageId"], "rust");
    assert_eq!(open_doc["version"], 1);
    assert_eq!(open_doc["text"], "fn main() {}\n");

    let change = rx.recv().await.unwrap();
    assert_eq!(
        change.get("method").and_then(Value::as_str),
        Some("textDocument/didChange")
    );
    assert_eq!(change["params"]["textDocument"]["uri"], uri.as_str());
    assert_eq!(change["params"]["textDocument"]["version"], 2);
    assert_eq!(
        change["params"]["contentChanges"][0]["text"],
        "fn main() { println!(\"hi\"); }\n"
    );

    let close = rx.recv().await.unwrap();
    assert_eq!(
        close.get("method").and_then(Value::as_str),
        Some("textDocument/didClose")
    );
    assert_eq!(close["params"]["textDocument"]["uri"], uri.as_str());

    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn definition_times_out_when_server_never_replies() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let _server = tokio::spawn(fake_server(server_io, FakeMode::DefinitionTimeout));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_millis(20),
    )
    .await
    .unwrap();

    let err = client
        .definition(
            &Url::from("file:///tmp/cairn/src/lib.rs"),
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, Error::RequestTimeout));
}

#[tokio::test]
async fn workspace_load_waits_for_progress_quiescence() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let server = tokio::spawn(fake_server(server_io, FakeMode::ProgressCompletes));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    client
        .wait_for_workspace_load_with_quiescence(Duration::from_secs(1), Duration::from_millis(20))
        .await
        .unwrap();

    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn semantic_workspace_load_stop_outranks_readiness_deadlines() {
    let client = Arc::new(LspClient::configured(
        Path::new("/unused/fake-lsp"),
        Vec::new(),
        Vec::new(),
        Path::new("/tmp/cairn"),
        Value::Null,
        Duration::from_secs(1),
    ));
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (client_reader, client_writer) = split(client_io);
    client.install_transport(client_reader, client_writer).await;

    let waiter = {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            client
                .wait_for_workspace_load_bounded(Duration::from_secs(120), Duration::from_secs(90))
                .await
        })
    };
    tokio::task::yield_now().await;
    client.process_control().stop_and_terminate().await.unwrap();

    assert!(matches!(waiter.await.unwrap(), Err(Error::PoolStopped)));
    drop(server_io);
}

#[tokio::test]
async fn workspace_load_ignores_server_status_without_progress() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let _server = tokio::spawn(fake_server(server_io, FakeMode::ServerStatusQuiescent));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let err = client
        .wait_for_workspace_load_with_quiescence(
            Duration::from_millis(20),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();

    assert!(matches!(err, Error::ReadinessTimeout));
}

#[tokio::test]
async fn workspace_load_does_not_finish_on_progress_end_without_begin() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let _server = tokio::spawn(fake_server(server_io, FakeMode::ProgressEndWithoutBegin));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let err = client
        .wait_for_workspace_load(Duration::from_millis(20))
        .await
        .unwrap_err();

    assert!(matches!(err, Error::ReadinessTimeout));
}

#[tokio::test]
async fn workspace_load_times_out_without_progress_end() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let _server = tokio::spawn(fake_server(server_io, FakeMode::ProgressNeverEnds));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let err = client
        .wait_for_workspace_load(Duration::from_millis(20))
        .await
        .unwrap_err();

    assert!(matches!(err, Error::ReadinessTimeout));
}

#[tokio::test]
async fn did_open_notifies_server_before_definition() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let server = tokio::spawn(fake_server(server_io, FakeMode::RequireDidOpen));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn fake"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let uri = Url::from("file:///tmp/cairn%20fake/src/lib.rs");

    client
        .did_open(&uri, "rust", 1, "fn main() {}\n")
        .await
        .unwrap();
    let locations = client
        .definition(
            &uri,
            Position {
                line: 0,
                character: 3,
            },
        )
        .await
        .unwrap();

    assert_eq!(locations.len(), 1);
    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn early_server_exit_surfaces_as_server_exited() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let _server = tokio::spawn(fake_server(server_io, FakeMode::CrashAfterInitialize));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let err = client
        .definition(
            &Url::from("file:///tmp/cairn/src/lib.rs"),
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap_err();

    // Tightened after `reader::fail_pending` was fixed to
    // preserve the `ServerExited` variant instead of falling
    // back to `Protocol` text — the pool's `ServerExited`
    // cleanup branch depends on this variant being surfaced
    // upward. Any regression that silently converts to Protocol
    // must be caught here.
    assert!(
        matches!(err, Error::ServerExited(_)),
        "unexpected error variant: {err:?}"
    );
}

#[tokio::test]
async fn handshake_failure_includes_stderr_tail() {
    let Some(python) = python3() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("stderr_lsp.py");
    std::fs::write(
        &script,
        r#"
import sys
import time

sys.stderr.write("mock startup failure\n")
sys.stderr.flush()
time.sleep(0.05)
"#,
    )
    .unwrap();

    let err = match LspClient::start_configured(
        &python,
        vec![script.to_string_lossy().to_string()],
        Vec::new(),
        tmp.path(),
        json!({}),
        Duration::from_secs(1),
    )
    .await
    {
        Ok(_) => panic!("mock LSP unexpectedly initialized"),
        Err(err) => err,
    };

    let message = err.to_string();
    assert!(message.contains("LSP handshake failed"), "{message}");
    assert!(message.contains("LSP server exited"), "{message}");
    assert!(
        message.contains("stderr: mock startup failure"),
        "{message}"
    );
}

#[tokio::test]
async fn start_configured_passes_env_to_spawned_server() {
    let Some(python) = python3() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let script = tmp.path().join("env_lsp.py");
    let env_file = tmp.path().join("env.txt");
    std::fs::write(
        &script,
        r#"
import json
import os
import sys

env_file = sys.argv[1]
with open(env_file, "w", encoding="utf-8") as f:
    f.write(os.environ.get("CAIRN_TEST_LSP_ENV", ""))

while True:
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            sys.exit(0)
        if line == b"\r\n":
            break
        key, value = line.decode("ascii").rstrip("\r\n").split(": ", 1)
        headers[key.lower()] = value
    body = sys.stdin.buffer.read(int(headers["content-length"]))
    message = json.loads(body)
    method = message.get("method")
    if method == "initialize":
        response = {
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {"capabilities": {}},
        }
        encoded = json.dumps(response).encode("utf-8")
        sys.stdout.buffer.write(
            b"Content-Length: " + str(len(encoded)).encode("ascii") + b"\r\n\r\n" + encoded
        )
        sys.stdout.buffer.flush()
    elif method == "shutdown":
        response = {"jsonrpc": "2.0", "id": message["id"], "result": None}
        encoded = json.dumps(response).encode("utf-8")
        sys.stdout.buffer.write(
            b"Content-Length: " + str(len(encoded)).encode("ascii") + b"\r\n\r\n" + encoded
        )
        sys.stdout.buffer.flush()
    elif method == "exit":
        sys.exit(0)
"#,
    )
    .unwrap();

    let client = LspClient::start_configured(
        &python,
        vec![
            script.to_string_lossy().to_string(),
            env_file.to_string_lossy().to_string(),
        ],
        vec![("CAIRN_TEST_LSP_ENV".to_string(), "expected".to_string())],
        tmp.path(),
        json!({}),
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read_to_string(&env_file).unwrap(), "expected");
    client.shutdown().await.unwrap();
}

#[test]
fn stderr_tail_keeps_short_output_without_marker() {
    let mut stderr = StderrTail::default();
    stderr.push(b"line 1\nline 2\nline 3\n");

    assert_eq!(stderr.text(), "line 1\nline 2\nline 3");
}

#[test]
fn stderr_tail_keeps_head_marker_and_tail_for_long_output() {
    let mut stderr = StderrTail::default();
    let lines = (1..=20)
        .map(|line| format!("line {line}: details"))
        .collect::<Vec<_>>()
        .join("\n");

    stderr.push(lines.as_bytes());

    let text = stderr.text();
    assert!(text.contains("line 1: details"), "{text}");
    assert!(text.contains("line 5: details"), "{text}");
    assert!(text.contains(" ... "), "{text}");
    assert!(text.contains("line 16: details"), "{text}");
    assert!(text.contains("line 20: details"), "{text}");
    assert!(!text.contains("line 6: details"), "{text}");
    assert!(!text.contains("line 15: details"), "{text}");
}

#[tokio::test]
async fn server_work_done_progress_request_is_answered_before_definition() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let server = tokio::spawn(fake_server(
        server_io,
        FakeMode::RequireProgressCreateResponse,
    ));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn fake"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let uri = Url::from("file:///tmp/cairn%20fake/src/lib.rs");

    client
        .did_open(&uri, "go", 1, "package main\n")
        .await
        .unwrap();
    let locations = client
        .definition(
            &uri,
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(locations.len(), 1);
    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn unknown_server_request_receives_method_not_found_response() {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let server = tokio::spawn(fake_server(
        server_io,
        FakeMode::RequireUnknownRequestResponse,
    ));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn fake"),
        "cfg",
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let locations = client
        .definition(
            &Url::from("file:///tmp/cairn%20fake/src/lib.rs"),
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(locations.len(), 1);
    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn pending_map_is_cleared_on_timeout() {
    // A server that never replies must not leak pending request
    // entries. Drive a definition call against the
    // `DefinitionTimeout` fake and assert the map is empty after
    // the timeout error returns.
    let (client_io, server_io) = tokio::io::duplex(8192);
    let _server = tokio::spawn(fake_server(server_io, FakeMode::DefinitionTimeout));
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_millis(20),
    )
    .await
    .unwrap();

    let err = client
        .definition(
            &Url::from("file:///tmp/cairn/src/lib.rs"),
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RequestTimeout));

    // Pending must be empty after the timed-out request returns.
    assert!(
        client.pending.lock().await.is_empty(),
        "pending map leaked entries on timeout"
    );
}

#[tokio::test]
async fn request_timeout_covers_backpressured_stdin_write() {
    let (client, server) = backpressured_client().await;
    let large_uri = Url::from(format!("file:///tmp/cairn/{}", "x".repeat(64 * 1024)));

    let err = client
        .definition(
            &large_uri,
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, Error::RequestTimeout));
    assert!(
        client.pending.lock().await.is_empty(),
        "timed-out write must reclaim its pending request"
    );
    let retry = client
        .definition(
            &Url::from("file:///tmp/cairn/src/lib.rs"),
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(retry, Error::ServerExited(_)));
    server.abort();
}

#[tokio::test]
async fn notification_timeout_covers_backpressured_stdin_write() {
    let (client, server) = backpressured_client().await;
    let uri = Url::from("file:///tmp/cairn/src/large.rs");

    let err = client
        .did_open(&uri, "rust", 1, &"x".repeat(64 * 1024))
        .await
        .unwrap_err();

    assert!(matches!(err, Error::RequestTimeout));
    let retry = client
        .definition(
            &Url::from("file:///tmp/cairn/src/lib.rs"),
            Position {
                line: 0,
                character: 0,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(retry, Error::ServerExited(_)));
    server.abort();
}

async fn backpressured_client() -> (LspClient, tokio::task::JoinHandle<()>) {
    let (client_io, server_io) = tokio::io::duplex(256);
    let (ready_tx, ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut reader, mut writer) = split(server_io);
        let initialize = read_lsp_message(&mut reader)
            .await
            .unwrap()
            .expect("initialize request");
        let id = initialize["id"].as_u64().unwrap();
        write_lsp_message(
            &mut writer,
            &json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "capabilities": {} },
            }),
        )
        .await
        .unwrap();
        let initialized = read_lsp_message(&mut reader)
            .await
            .unwrap()
            .expect("initialized notification");
        assert_eq!(initialized["method"], "initialized");
        ready_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let (client_reader, client_writer) = split(client_io);
    let client = LspClient::start_with_io(
        client_reader,
        client_writer,
        Path::new("/tmp/cairn"),
        "cfg",
        Duration::from_millis(100),
    )
    .await
    .unwrap();
    ready_rx.await.unwrap();
    (client, server)
}

fn python3() -> Option<std::path::PathBuf> {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|_| std::path::PathBuf::from("python3"))
}
