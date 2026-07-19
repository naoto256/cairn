use super::*;

impl JobManager {
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
                warn!(error = %err, "analyzer job failed");
            }
            self.notify_worker_finished(job_id, pool_group, key);
        }
    }

    async fn run_job(&self, dispatch: DispatchJob) -> Result<()> {
        let runtime_metrics = self.runtime_metrics.clone();
        tokio::task::spawn_blocking(move || run_job_blocking(dispatch.job, runtime_metrics))
            .await
            .map_err(|e| Error::internal_task_panic("analyzer job", e))?
    }

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

fn run_job_blocking(job: Job, runtime_metrics: JobRuntimeMetricsStore) -> Result<()> {
    let mut conn = cas_store::open(&job.store_path)?;
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT status, cancel_requested
             FROM workspace_analysis_runs
             WHERE job_id = ?1 AND manifest_id = ?2 AND analyzer_id = ?3",
            params![job.id, job.manifest_id.0, job.analyzer_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((state, cancel_requested)) = row else {
        return Ok(());
    };
    if state == RunStatus::Cancelled.as_str()
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
    if state != RunStatus::Queued.as_str() && state != RunStatus::Running.as_str() {
        return Ok(());
    }

    let analyzer = all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == job.analyzer_id)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown analyzer: {}", job.analyzer_id)))?;
    let entries = manifest::get_entries(&conn, job.manifest_id)?;
    let now = now_ns();
    let progress_metrics = runtime_metrics.clone();
    let job_id = job.id;
    let progress_observer: crate::workspace_analyzer::AnalyzerProgressObserver =
        Arc::new(move |ticks| {
            progress_metrics.mark_progress(job_id, ticks);
        });
    info!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        "analyzer job started"
    );
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
            progress_observer: Some(progress_observer),
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
