use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

#[tokio::test]
async fn delayed_reader_exit_does_not_invalidate_replacement_transport() {
    let (old_client, mut old_server) = tokio::io::duplex(1024);
    let (old_reader, old_writer) = split(old_client);
    let (new_client, mut new_server) = tokio::io::duplex(1024);
    let (new_reader, new_writer) = split(new_client);
    let pending = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let current_generation = Arc::new(AtomicU64::new(2));
    let progress = Arc::new(ProgressState::default());
    progress.reset_for_generation(2).await;
    let old_writer: SharedWriter = Arc::new(tokio::sync::Mutex::new(Box::new(old_writer)));
    let new_writer: SharedWriter = Arc::new(tokio::sync::Mutex::new(Box::new(new_writer)));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    pending.lock().await.insert(
        7,
        PendingRequest {
            generation: 2,
            sender,
        },
    );

    write_lsp_message(
        &mut old_server,
        &progress_message("old-generation", "begin"),
    )
    .await
    .unwrap();
    write_lsp_message(&mut old_server, &progress_message("old-generation", "end"))
        .await
        .unwrap();
    write_lsp_message(
        &mut old_server,
        &json!({
            "jsonrpc": "2.0",
            "id": 9001,
            "method": "old/unknownRequest",
            "params": {}
        }),
    )
    .await
    .unwrap();
    let old_reader = tokio::spawn(reader_loop(
        old_reader,
        1,
        Arc::clone(&pending),
        Arc::clone(&current_generation),
        old_writer,
        Arc::clone(&progress),
    ));
    let old_response = timeout(
        Duration::from_millis(100),
        read_lsp_message(&mut old_server),
    )
    .await
    .expect("old transport did not receive its server-request response")
    .unwrap()
    .unwrap();
    assert_eq!(old_response["id"], 9001);
    assert_eq!(old_response["error"]["code"], -32601);
    drop(old_server);
    old_reader.await.unwrap();

    assert_eq!(current_generation.load(Ordering::SeqCst), 2);
    assert!(
        pending.lock().await.contains_key(&7),
        "old reader must not drain a replacement-generation request"
    );
    assert!(
        timeout(
            Duration::from_millis(20),
            progress.wait_for_quiescence(Duration::from_millis(1))
        )
        .await
        .is_err(),
        "old progress must not make the replacement generation ready"
    );

    let replacement_reader = tokio::spawn(reader_loop(
        new_reader,
        2,
        Arc::clone(&pending),
        Arc::clone(&current_generation),
        new_writer,
        progress,
    ));
    write_lsp_message(
        &mut new_server,
        &json!({"jsonrpc": "2.0", "id": 7, "result": {"ok": true}}),
    )
    .await
    .unwrap();

    assert_eq!(
        timeout(Duration::from_millis(100), receiver)
            .await
            .expect("replacement response timed out")
            .expect("replacement reader dropped pending response")
            .expect("replacement returned an error"),
        json!({"ok": true})
    );
    drop(new_server);
    replacement_reader.await.unwrap();
}

#[tokio::test]
async fn workspace_load_resets_quiet_timer_when_new_progress_arrives() {
    let progress = Arc::new(ProgressState::default());
    let waiter = {
        let progress = Arc::clone(&progress);
        tokio::spawn(async move {
            progress
                .wait_for_quiescence(Duration::from_millis(50))
                .await
        })
    };

    progress.record(&progress_message("phase-1", "begin")).await;
    progress.record(&progress_message("phase-1", "end")).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    progress.record(&progress_message("phase-2", "begin")).await;
    tokio::time::sleep(Duration::from_millis(35)).await;

    assert!(
        !waiter.is_finished(),
        "quiet timer should reset when new progress begins"
    );
    progress.record(&progress_message("phase-2", "end")).await;
    let completed = timeout(Duration::from_millis(100), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed, WorkspaceLoadComplete::ProgressQuiescence);
}

#[tokio::test]
async fn workspace_load_observes_activity_completed_after_snapshot() {
    let progress = Arc::new(ProgressState::default());
    let snapshot_seen = Arc::new(Notify::new());
    let release_snapshot = Arc::new(Notify::new());
    let first_snapshot = Arc::new(AtomicBool::new(true));

    let snapshot_wait = snapshot_seen.notified();
    tokio::pin!(snapshot_wait);
    snapshot_wait.as_mut().enable();

    let waiter = {
        let progress = Arc::clone(&progress);
        let snapshot_seen = Arc::clone(&snapshot_seen);
        let release_snapshot = Arc::clone(&release_snapshot);
        let first_snapshot = Arc::clone(&first_snapshot);
        tokio::spawn(async move {
            progress
                .wait_for_quiescence_with_snapshot_hook(Duration::from_millis(10), move || {
                    let snapshot_seen = Arc::clone(&snapshot_seen);
                    let release_snapshot = Arc::clone(&release_snapshot);
                    let first_snapshot = Arc::clone(&first_snapshot);
                    async move {
                        if first_snapshot.swap(false, Ordering::SeqCst) {
                            snapshot_seen.notify_one();
                            release_snapshot.notified().await;
                        }
                    }
                })
                .await
        })
    };

    snapshot_wait.await;
    progress.record(&progress_message("phase-1", "begin")).await;
    progress.record(&progress_message("phase-1", "end")).await;
    release_snapshot.notify_one();

    let completed = timeout(Duration::from_millis(100), waiter)
        .await
        .expect("pre-armed activity notification must wake the waiter")
        .unwrap();
    assert_eq!(completed, WorkspaceLoadComplete::ProgressQuiescence);
}

#[tokio::test]
async fn progress_state_reset_clears_saw_begin_from_prior_session() {
    // Prior-session `saw_begin` must not persist across a
    // `spawn_process` respawn. Without reset,
    // `wait_for_quiescence` on the new child would satisfy
    // immediately from the old server's begin+end.
    let progress = Arc::new(ProgressState::default());
    progress.record(&progress_message("phase-1", "begin")).await;
    progress.record(&progress_message("phase-1", "end")).await;
    let pre_reset = timeout(
        Duration::from_millis(200),
        progress.wait_for_quiescence(Duration::from_millis(50)),
    )
    .await
    .expect("pre-reset quiescence should complete");
    assert_eq!(pre_reset, WorkspaceLoadComplete::ProgressQuiescence);
    progress.reset().await;
    // Post-reset: no new `begin` observed → wait blocks
    // indefinitely; the outer timeout is expected to elapse.
    let post_reset = timeout(
        Duration::from_millis(150),
        progress.wait_for_quiescence(Duration::from_millis(50)),
    )
    .await;
    assert!(
        post_reset.is_err(),
        "post-reset wait_for_quiescence must not carry over prior saw_begin"
    );
}

#[tokio::test]
async fn progress_state_reset_clears_active_tokens_from_prior_session() {
    // The other half of the reset contract:
    // `active_tokens` must be cleared too. If a prior session
    // left tokens active (begin without end — e.g. the server
    // crashed mid-load), the new session's `wait_for_quiescence`
    // would otherwise block on those ghost tokens forever.
    let progress = Arc::new(ProgressState::default());
    progress.record(&progress_message("ghost", "begin")).await;
    // Prior session has an active token AND saw_begin; drop it.
    progress.reset().await;
    // Now simulate a clean new-session load: begin + end.
    progress.record(&progress_message("phase-1", "begin")).await;
    progress.record(&progress_message("phase-1", "end")).await;
    // Quiescence must complete — if `active_tokens` were not
    // cleared, the "ghost" token from the prior session would
    // still be considered active and the wait would block until
    // the outer timeout.
    let outcome = timeout(
        Duration::from_millis(200),
        progress.wait_for_quiescence(Duration::from_millis(50)),
    )
    .await
    .expect("post-reset new-session quiescence must complete");
    assert_eq!(outcome, WorkspaceLoadComplete::ProgressQuiescence);
}

#[test]
fn response_result_preserves_lsp_error_code() {
    let (_, result) = response_result(&json!({
        "jsonrpc": "2.0",
        "id": 7,
        "error": {
            "code": CONTENT_MODIFIED_ERROR_CODE,
            "message": "content modified"
        }
    }))
    .unwrap();

    let err = result.unwrap_err();
    assert!(err.is_content_modified());
    assert_eq!(err.to_string(), "LSP protocol error: content modified");
}

#[test]
fn response_result_ignores_server_requests() {
    assert!(
        response_result(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "window/workDoneProgress/create",
            "params": { "token": "index" }
        }))
        .is_none()
    );
}
