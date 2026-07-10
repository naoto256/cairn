use super::*;

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
