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
            run_job_blocking(dispatch.job, runtime_metrics, progress, lease)
        })
        .await;
        self.unregister_active_progress(job_id);
        // A panic inside the blocking task surfaces as a JoinError
        // and is converted to an internal error instead of unwinding
        // through the worker loop.
        joined.map_err(|e| Error::internal_task_panic("analyzer job", e))?
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
    info!(
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
    info!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        status = %outcome.status.as_str(),
        inserted_refs = outcome.inserted_refs,
        "analyzer job finished"
    );
    Ok(())
}
