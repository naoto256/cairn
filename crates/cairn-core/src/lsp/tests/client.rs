use super::*;

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

    assert!(
        matches!(err, Error::ServerExited(_)) || matches!(err, Error::Protocol(_)),
        "unexpected error: {err:?}"
    );
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
