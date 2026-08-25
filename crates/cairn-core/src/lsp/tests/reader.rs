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

fn semantic_progress_message(token: &str, kind: &str, payload: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "$/progress",
        "params": {
            "token": token,
            "value": { "kind": kind, "message": payload }
        }
    })
}

#[test]
fn semantic_progress_reducer_accepts_only_material_active_token_changes() {
    let mut active = HashMap::new();
    let mut saw_begin = false;
    let begin = json!({"kind": "begin", "title": "workspace"});
    let duplicate_begin = json!({"kind": "begin", "title": "duplicate"});
    let report_one = json!({"kind": "report", "message": "1/2"});
    let report_two = json!({"kind": "report", "message": "2/2"});
    let end = json!({"kind": "end", "message": "done"});

    assert!(!reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "inactive".into(),
        "report",
        &report_one,
    ));
    assert!(reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "begin",
        &begin,
    ));
    assert!(saw_begin);
    assert_eq!(active.get("load"), Some(&begin));
    assert!(!reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "begin",
        &duplicate_begin,
    ));
    assert_eq!(active.get("load"), Some(&begin));
    assert!(reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "report",
        &report_one,
    ));
    assert!(!reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "report",
        &report_one,
    ));
    assert!(reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "report",
        &report_two,
    ));
    assert!(reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "end",
        &end,
    ));
    assert!(!reduce_semantic_progress(
        &mut active,
        &mut saw_begin,
        "load".into(),
        "end",
        &end,
    ));
    assert!(active.is_empty());
}

#[tokio::test(start_paused = true)]
async fn semantic_quiet_ignores_duplicate_progress_and_server_status_noise() {
    let progress = ProgressState::default();
    progress
        .record(&semantic_progress_message("load", "begin", "workspace"))
        .await;
    progress
        .record(&semantic_progress_message("load", "end", "done"))
        .await;
    let started = tokio::time::Instant::now();
    let wait = progress.wait_for_semantic_quiescence(
        started,
        started + Duration::from_millis(120),
        Duration::from_millis(90),
        Duration::from_millis(5),
    );
    tokio::pin!(wait);

    tokio::select! {
        biased;
        outcome = &mut wait => panic!("quiet completed early: {outcome:?}"),
        () = tokio::time::advance(Duration::from_micros(4_900)) => {}
    }
    progress
        .record(&semantic_progress_message("load", "end", "duplicate"))
        .await;
    progress
        .record(&semantic_progress_message("inactive", "report", "noise"))
        .await;
    progress
        .record_server_status(&json!({
            "params": {"health": "ok", "quiescent": true}
        }))
        .await;
    progress
        .record_server_status(&json!({
            "params": {"health": "ok", "quiescent": true}
        }))
        .await;
    tokio::time::advance(Duration::from_micros(100)).await;

    assert_eq!(
        wait.await,
        WorkspaceLoadWaitOutcome::Complete(WorkspaceLoadComplete::ProgressQuiescence)
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_millis(5),
        "duplicate noise must not refresh the semantic quiet deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn legacy_raw_quiet_still_refreshes_on_duplicate_progress() {
    let progress = ProgressState::default();
    progress.record(&progress_message("load", "begin")).await;
    progress.record(&progress_message("load", "end")).await;
    let started = tokio::time::Instant::now();
    let wait = progress.wait_for_quiescence(Duration::from_millis(5));
    tokio::pin!(wait);

    tokio::select! {
        biased;
        outcome = &mut wait => panic!("raw quiet completed early: {outcome:?}"),
        () = tokio::time::advance(Duration::from_micros(4_900)) => {}
    }
    progress.record(&progress_message("load", "end")).await;
    tokio::select! {
        biased;
        outcome = &mut wait => panic!("duplicate raw progress did not refresh quiet: {outcome:?}"),
        () = tokio::time::advance(Duration::from_micros(4_900)) => {}
    }
    tokio::time::advance(Duration::from_micros(100)).await;
    assert_eq!(wait.await, WorkspaceLoadComplete::ProgressQuiescence);
    let elapsed = tokio::time::Instant::now() - started;
    assert!(
        elapsed >= Duration::from_micros(9_900) && elapsed <= Duration::from_millis(11),
        "legacy relative sleep should restart after duplicate progress: {elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn semantic_stall_deadline_has_exact_ninety_second_boundary() {
    let progress = ProgressState::default();
    progress
        .record(&semantic_progress_message("load", "begin", "workspace"))
        .await;
    let started = tokio::time::Instant::now();
    let wait = progress.wait_for_semantic_quiescence(
        started,
        started + Duration::from_millis(200),
        Duration::from_millis(90),
        Duration::from_millis(5),
    );
    tokio::pin!(wait);

    tokio::select! {
        biased;
        outcome = &mut wait => panic!("stall completed before 89.9: {outcome:?}"),
        () = tokio::time::advance(Duration::from_micros(89_900)) => {}
    }
    tokio::time::advance(Duration::from_micros(200)).await;
    assert_eq!(
        wait.await,
        WorkspaceLoadWaitOutcome::Deadline(WorkspaceLoadDeadline::Stall)
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_micros(90_100),
        "stall deadline must be scheduled independently of the hard cap"
    );
}

#[tokio::test(start_paused = true)]
async fn semantic_hard_deadline_caps_endless_unique_progress_at_one_twenty() {
    let progress = ProgressState::default();
    progress
        .record(&semantic_progress_message("load", "begin", "workspace"))
        .await;
    let started = tokio::time::Instant::now();
    let wait = progress.wait_for_semantic_quiescence(
        started,
        started + Duration::from_millis(120),
        Duration::from_millis(90),
        Duration::from_millis(5),
    );
    tokio::pin!(wait);

    for (step, elapsed) in [("one", 30), ("two", 30), ("three", 30)] {
        tokio::select! {
            biased;
            outcome = &mut wait => panic!("hard deadline completed early: {outcome:?}"),
            () = tokio::time::advance(Duration::from_millis(elapsed)) => {}
        }
        progress
            .record(&semantic_progress_message("load", "report", step))
            .await;
    }
    tokio::select! {
        biased;
        outcome = &mut wait => panic!("hard deadline completed before 119.9: {outcome:?}"),
        () = tokio::time::advance(Duration::from_micros(29_900)) => {}
    }
    tokio::time::advance(Duration::from_micros(200)).await;
    assert_eq!(
        wait.await,
        WorkspaceLoadWaitOutcome::Deadline(WorkspaceLoadDeadline::Hard)
    );
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_micros(120_100),
        "hard deadline must be scheduled independently of semantic progress"
    );
}

#[tokio::test(start_paused = true)]
async fn semantic_lock_trace_accepts_sixty_and_stalls_one_twenty_and_one_fifty() {
    let progress = ProgressState::default();

    progress
        .record(&semantic_progress_message("load-60", "begin", "workspace"))
        .await;
    let started = tokio::time::Instant::now();
    let wait_60 = progress.wait_for_semantic_quiescence(
        started,
        started + Duration::from_millis(120),
        Duration::from_millis(90),
        Duration::from_millis(5),
    );
    tokio::pin!(wait_60);
    tokio::select! {
        biased;
        outcome = &mut wait_60 => panic!("60-second trace completed early: {outcome:?}"),
        () = tokio::time::advance(Duration::from_millis(60)) => {}
    }
    progress
        .record(&semantic_progress_message(
            "load-60",
            "report",
            "lock released",
        ))
        .await;
    progress
        .record(&semantic_progress_message("load-60", "end", "ready"))
        .await;
    tokio::time::advance(Duration::from_millis(5)).await;
    assert!(matches!(
        wait_60.await,
        WorkspaceLoadWaitOutcome::Complete(_)
    ));
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_millis(65)
    );

    for label in ["load-120", "load-150"] {
        progress.reset().await;
        progress
            .record(&semantic_progress_message(label, "begin", "workspace"))
            .await;
        let started = tokio::time::Instant::now();
        let wait = progress.wait_for_semantic_quiescence(
            started,
            started + Duration::from_millis(120),
            Duration::from_millis(90),
            Duration::from_millis(5),
        );
        tokio::pin!(wait);
        tokio::time::advance(Duration::from_millis(90)).await;
        assert_eq!(
            wait.await,
            WorkspaceLoadWaitOutcome::Deadline(WorkspaceLoadDeadline::Stall),
            "{label} must stop at the semantic-stall deadline"
        );
    }
}

#[tokio::test]
async fn semantic_progress_is_scoped_to_transport_generation_and_resets_payloads() {
    let progress = ProgressState::default();
    progress.reset_for_generation(7).await;
    progress
        .record_for_generation(7, &semantic_progress_message("old", "begin", "workspace"))
        .await;
    assert_eq!(progress.semantic_snapshot().await, (7, 1, true, 1));

    progress.reset_for_generation(8).await;
    progress
        .record_for_generation(7, &semantic_progress_message("old", "report", "late"))
        .await;
    progress
        .record_for_generation(7, &semantic_progress_message("old", "end", "late"))
        .await;
    assert_eq!(progress.semantic_snapshot().await, (8, 0, false, 0));

    progress
        .record_for_generation(8, &semantic_progress_message("new", "begin", "workspace"))
        .await;
    progress
        .record_for_generation(8, &semantic_progress_message("new", "end", "ready"))
        .await;
    assert_eq!(progress.semantic_snapshot().await, (8, 0, true, 2));
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
