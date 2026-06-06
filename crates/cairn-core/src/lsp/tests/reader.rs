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
