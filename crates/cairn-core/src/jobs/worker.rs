use super::*;

impl JobManager {
    /// One worker task. All workers compete for dispatches on the
    /// shared receiver (serialized by the async mutex); the loop
    /// ends when the dispatch channel closes during shutdown. A
    /// failed run is logged and the loop continues — and
    /// `notify_worker_finished` is sent on success *and* failure so
    /// the scheduler frees the worker slot, pool-group slot, and
    /// tracked de-dup key in either outcome (the notify itself is
    /// best-effort and may be dropped during shutdown).
    pub(super) async fn worker_loop(self: Arc<Self>) {
        loop {
            let dispatch = {
                let mut receiver = self.worker_receiver.lock().await;
                receiver.recv().await
            };
            let Some(dispatch) = dispatch else {
                break;
            };
            let job_id = dispatch.job.id;
            let pool_group = dispatch.pool_group;
            let key = dispatch.key.clone();
            if let Err(err) = self.run_job(dispatch).await {
                warn!(
                    error = %err,
                    sqlite_code = ?err.sqlite_error_code(),
                    sqlite_extended_code = ?err.sqlite_extended_code(),
                    "analyzer job failed"
                );
            }
            self.notify_worker_finished(job_id, pool_group, key);
        }
    }

    async fn run_job(&self, dispatch: DispatchJob) -> Result<()> {
        // Hold a lifecycle lease (when wired) for the whole run so
        // repository removal cannot proceed underneath the analyzer;
        // the lease moves into the blocking task and is dropped when
        // the run returns.
        let lease = self
            .lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.acquire_by_repo_hash(&dispatch.job.repo_hash))
            .transpose()?;
        let runtime_metrics = self.runtime_metrics.clone();
        let job_id = dispatch.job.id;
        #[cfg(test)]
        let terminal_identity = (
            dispatch.job.repo_hash.clone(),
            dispatch.job.store_path.clone(),
            dispatch.job.manifest_id,
            dispatch.job.analyzer_id.clone(),
        );
        let progress_metrics = runtime_metrics.clone();
        let progress =
            crate::workspace_analyzer::AnalyzerProgress::with_observer(Arc::new(move |ticks| {
                progress_metrics.mark_progress(job_id, ticks);
            }));
        // Register the progress handle before the blocking run starts
        // so `begin_shutdown` / `cancel` can reach this job; if
        // admission is already closed, registration cancels the
        // handle immediately and the run below observes it at its
        // first cancellation check.
        self.register_active_progress(job_id, progress.clone());
        let joined = tokio::task::spawn_blocking(move || {
            // Enter running state only after the blocking pool grants
            // an execution slot. The terminal-state guard inside
            // `mark_running` keeps this transition monotonic.
            runtime_metrics.mark_running(job_id);
            run_job_blocking(dispatch.job, runtime_metrics, progress, lease)
        })
        .await;
        self.unregister_active_progress(job_id);
        #[cfg(not(test))]
        return finalize_joined_run(&self.runtime_metrics, job_id, joined);
        #[cfg(test)]
        {
            let result = finalize_joined_run(&self.runtime_metrics, job_id, joined);
            record_terminal_after_worker(terminal_identity, job_id, result.as_ref().err()).await;
            result
        }
    }

    /// Report a finished dispatch back to the scheduler so it frees
    /// the worker slot / pool group and releases the tracked key.
    /// Best-effort: during shutdown the sender may already be taken
    /// or the scheduler gone, and the send is dropped silently.
    fn notify_worker_finished(&self, job_id: JobId, pool_group: Option<&'static str>, key: JobKey) {
        let sender = self
            .scheduler_sender
            .lock()
            .expect("job scheduler sender lock poisoned");
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(SchedulerMsg::WorkerFinished {
                job_id,
                pool_group,
                key,
            });
        }
    }

    /// Tell the scheduler a queued job was cancelled so it is
    /// dropped from the pending lanes before dispatch. Best-effort,
    /// same as `notify_worker_finished`.
    pub(super) fn notify_cancelled_job(&self, job_id: JobId) {
        let sender = self
            .scheduler_sender
            .lock()
            .expect("job scheduler sender lock poisoned");
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(SchedulerMsg::Cancel(job_id));
        }
    }
}

#[cfg(test)]
async fn record_terminal_after_worker(
    identity: (
        String,
        std::path::PathBuf,
        crate::manifest::ManifestId,
        String,
    ),
    job_id: JobId,
    worker_error: Option<&Error>,
) {
    record_terminal_after_worker_with_probe(identity, job_id, worker_error.is_some(), || {}).await;
}

#[cfg(test)]
async fn record_terminal_after_worker_with_probe<F>(
    identity: (
        String,
        std::path::PathBuf,
        crate::manifest::ManifestId,
        String,
    ),
    job_id: JobId,
    worker_error: bool,
    probe: F,
) where
    F: FnOnce() + Send + 'static,
{
    let failure_identity = identity.clone();
    let durable = tokio::task::spawn_blocking(move || {
        probe();
        let (_, store_path, manifest_id, analyzer_id) = identity;
        cas_store::open_existing(&store_path).and_then(|conn| {
            conn.query_row(
                "SELECT status, error IS NOT NULL
             FROM workspace_analysis_runs
             WHERE job_id = ?1 AND manifest_id = ?2 AND analyzer_id = ?3",
                params![job_id, manifest_id.0, analyzer_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
            )
            .map_err(Error::from)
        })
    })
    .await;
    let (repo_hash, _, _, analyzer_id) = failure_identity;
    match durable {
        Ok(Ok((status, has_error))) => crate::churn_recorder::record_terminal(
            &repo_hash,
            &analyzer_id,
            job_id,
            &status,
            has_error.then_some("analyzer_failed"),
        ),
        Ok(Err(_)) | Err(_) => crate::churn_recorder::record_observation_failure(
            &repo_hash,
            crate::churn_recorder::ObservationFailureKind::TerminalRowUnavailable,
            None,
            Some(&analyzer_id),
            Some(job_id),
        ),
    }
    if worker_error {
        crate::churn_recorder::record_terminal(
            &repo_hash,
            &analyzer_id,
            job_id,
            "worker_error",
            Some("worker_error"),
        );
    }
}

/// Collapse the blocking worker's two failure channels into the
/// runtime-metrics terminal transition.
///
/// `run_job_blocking` reports ordinary failures through its inner
/// [`Result`], while panics surface as a [`tokio::task::JoinError`].
/// Both mark an unfinished run failed; a concurrent terminal state
/// such as cancellation remains authoritative.
fn finalize_joined_run(
    runtime_metrics: &JobRuntimeMetricsStore,
    job_id: JobId,
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match joined {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            runtime_metrics.mark_failed_if_unfinished(job_id);
            Err(err)
        }
        Err(err) => {
            runtime_metrics.mark_failed_if_unfinished(job_id);
            Err(Error::internal_task_panic("analyzer job", err))
        }
    }
}

/// Synchronous body of one analyzer run, executed on the blocking
/// pool. Re-reads the durable row before doing anything: the state
/// at dispatch time wins over the (possibly stale) memory-queue
/// entry. `_lease` pins the repository's lifecycle for the whole
/// run and is released on return.
fn run_job_blocking(
    job: Job,
    runtime_metrics: JobRuntimeMetricsStore,
    progress: crate::workspace_analyzer::AnalyzerProgress,
    _lease: Option<crate::lifecycle::RepoLease>,
) -> Result<()> {
    let mut conn = cas_store::open_existing(&job.store_path)?;
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT status, cancel_requested
             FROM workspace_analysis_runs
             WHERE job_id = ?1 AND manifest_id = ?2 AND analyzer_id = ?3",
            params![job.id, job.manifest_id.0, job.analyzer_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    // The lookup is keyed by (job_id, manifest, analyzer). If no row
    // matches (e.g. the run row was rewritten or removed since this
    // job was dispatched), the job is a no-op.
    let Some((state, cancel_requested)) = row else {
        return Ok(());
    };
    // Three cancellation sources collapse here: the in-process
    // progress handle (shutdown, or cancel of a job this process is
    // handling), a row already flipped to `cancelled`, and a queued
    // row whose `cancel_requested` flag was set while it waited.
    if progress.is_cancelled()
        || state == RunStatus::Cancelled.as_str()
        || (state == RunStatus::Queued.as_str() && cancel_requested != 0)
    {
        conn.execute(
            "UPDATE workspace_analysis_runs
             SET status = 'cancelled', finished_at_ns = ?1
             WHERE job_id = ?2",
            params![now_ns(), job.id],
        )?;
        runtime_metrics.mark_finished(job.id, RunStatus::Cancelled.as_str());
        return Ok(());
    }
    // Any other terminal state means the run already concluded
    // elsewhere; nothing to do.
    if state != RunStatus::Queued.as_str() && state != RunStatus::Running.as_str() {
        return Ok(());
    }

    let analyzer = all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == job.analyzer_id)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown analyzer: {}", job.analyzer_id)))?;
    let entries = manifest::get_entries(&conn, job.manifest_id)?;
    let now = now_ns();
    debug!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        "analyzer job started"
    );
    // ANALYZER_STALL_TIMEOUT bounds progress *silence*, not total
    // run time: a long run that keeps ticking progress is allowed
    // (see the constant's rationale in workspace_analyzer::run).
    let outcome = run_one_workspace_analyzer_with_timeout(
        &mut conn,
        AnalyzerRunRequest {
            analyzer,
            repo_root: &job.repo_root,
            manifest_id: job.manifest_id,
            entries: &entries,
            now_ns: now,
            analyzer_stall_timeout: ANALYZER_STALL_TIMEOUT,
            job_id: Some(job.id),
            progress: Some(progress),
        },
    )?;
    runtime_metrics.mark_finished(job.id, outcome.status.as_str());
    debug!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        status = %outcome.status.as_str(),
        inserted_refs = outcome.inserted_refs,
        "analyzer job finished"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::tests::job;

    fn scheduler_state(metrics: &JobRuntimeMetricsStore, job_id: JobId) -> String {
        let mut snapshot = job(job_id, "pyright-lsp", "failed");
        metrics.decorate(&mut snapshot, now_ns());
        snapshot.scheduler_state.unwrap()
    }

    fn running_metrics(job_id: JobId) -> JobRuntimeMetricsStore {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(job_id, None, 1);
        metrics.mark_running(job_id);
        metrics
    }

    #[test]
    fn inner_worker_error_marks_unfinished_metrics_failed() {
        let metrics = running_metrics(1);

        let result = finalize_joined_run(
            &metrics,
            1,
            Ok(Err(Error::InvalidArgument("worker failed".into()))),
        );

        assert!(result.is_err());
        assert_eq!(scheduler_state(&metrics, 1), RunStatus::Failed.as_str());
    }

    #[tokio::test]
    async fn worker_panic_marks_unfinished_metrics_failed() {
        let metrics = running_metrics(2);
        let join_error = tokio::spawn(async {
            panic!("worker panic");
        })
        .await
        .unwrap_err();

        let result = finalize_joined_run(&metrics, 2, Err(join_error));

        assert!(result.is_err());
        assert_eq!(scheduler_state(&metrics, 2), RunStatus::Failed.as_str());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_observation_uses_blocking_pool_and_reports_missing_or_unreadable_row() {
        let tmp = tempfile::tempdir().unwrap();
        let (recorder, _guard) = crate::churn_recorder::install("repo", tmp.path());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let store_path = tmp.path().join("store.db");
        cas_store::open(&store_path).unwrap();
        let task = tokio::spawn(record_terminal_after_worker_with_probe(
            (
                "repo".into(),
                store_path,
                crate::manifest::ManifestId(7),
                "test-analyzer".into(),
            ),
            41,
            false,
            move || {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
            },
        ));

        started_rx.await.unwrap();
        // Reaching this point while the blocking probe is still held is the
        // responsiveness evidence; the counter below is only a sentinel that
        // the yield returned.
        let mut heartbeat = 0;
        tokio::task::yield_now().await;
        heartbeat += 1;
        assert_eq!(
            heartbeat, 1,
            "current-thread runtime must remain responsive"
        );
        release_tx.send(()).unwrap();
        task.await.unwrap();
        record_terminal_after_worker(
            (
                "repo".into(),
                tmp.path().join("unreadable.db"),
                crate::manifest::ManifestId(8),
                "test-analyzer".into(),
            ),
            42,
            None,
        )
        .await;

        assert_eq!(
            recorder.snapshot().observation_failures,
            [
                crate::churn_recorder::ObservationFailure {
                    kind: crate::churn_recorder::ObservationFailureKind::TerminalRowUnavailable,
                    repo_hash: "repo".into(),
                    generation: None,
                    analyzer_id: Some("test-analyzer".into()),
                    job_id: Some(41),
                },
                crate::churn_recorder::ObservationFailure {
                    kind: crate::churn_recorder::ObservationFailureKind::TerminalRowUnavailable,
                    repo_hash: "repo".into(),
                    generation: None,
                    analyzer_id: Some("test-analyzer".into()),
                    job_id: Some(42),
                },
            ]
        );
    }

    #[test]
    fn late_worker_failure_preserves_concurrent_cancellation() {
        let metrics = running_metrics(3);
        metrics.mark_finished(3, RunStatus::Cancelled.as_str());

        let result = finalize_joined_run(
            &metrics,
            3,
            Ok(Err(Error::InvalidArgument("late failure".into()))),
        );

        assert!(result.is_err());
        assert_eq!(scheduler_state(&metrics, 3), RunStatus::Cancelled.as_str());
    }
}
