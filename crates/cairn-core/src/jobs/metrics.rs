use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct JobRuntimeMetricsStore {
    inner: Arc<Mutex<HashMap<JobId, JobRuntimeMetrics>>>,
}

// Runtime-only scheduler metrics make active jobs diagnosable without a CAS
// schema bump. Historical rows after daemon restart simply omit these optional
// fields on the wire.
#[derive(Debug, Clone)]
struct JobRuntimeMetrics {
    enqueued_at_ns: i64,
    pool_group: Option<String>,
    scheduler_state: String,
    pool_wait_started_at_ns: Option<i64>,
    pool_wait_ms: u64,
    run_started_at_ns: Option<i64>,
    finished_at_ns: Option<i64>,
    progress_ticks: u64,
    last_progress_at_ns: Option<i64>,
}

impl JobRuntimeMetricsStore {
    pub(super) fn mark_enqueued(
        &self,
        job_id: JobId,
        pool_group: Option<&'static str>,
        enqueued_at_ns: i64,
    ) {
        self.inner
            .lock()
            .expect("job metrics lock poisoned")
            .insert(
                job_id,
                JobRuntimeMetrics {
                    enqueued_at_ns,
                    pool_group: pool_group.map(str::to_string),
                    scheduler_state: "queued".into(),
                    pool_wait_started_at_ns: None,
                    pool_wait_ms: 0,
                    run_started_at_ns: None,
                    finished_at_ns: None,
                    progress_ticks: 0,
                    last_progress_at_ns: None,
                },
            );
    }

    pub(super) fn mark_waiting_pool_group(&self, job_id: JobId, pool_group: &'static str) {
        let now = now_ns();
        let mut metrics = self.inner.lock().expect("job metrics lock poisoned");
        let entry = metrics.entry(job_id).or_insert_with(|| JobRuntimeMetrics {
            enqueued_at_ns: now,
            pool_group: Some(pool_group.to_string()),
            scheduler_state: "queued".into(),
            pool_wait_started_at_ns: None,
            pool_wait_ms: 0,
            run_started_at_ns: None,
            finished_at_ns: None,
            progress_ticks: 0,
            last_progress_at_ns: None,
        });
        entry.pool_group = Some(pool_group.to_string());
        entry.scheduler_state = "waiting_pool_group".into();
        entry.pool_wait_started_at_ns.get_or_insert(now);
    }

    pub(super) fn mark_running(&self, job_id: JobId) {
        let now = now_ns();
        let mut metrics = self.inner.lock().expect("job metrics lock poisoned");
        if let Some(entry) = metrics.get_mut(&job_id) {
            if let Some(started) = entry.pool_wait_started_at_ns.take() {
                entry.pool_wait_ms = entry
                    .pool_wait_ms
                    .saturating_add(duration_ms(started, now).unwrap_or(0));
            }
            entry.scheduler_state = "running".into();
            entry.run_started_at_ns.get_or_insert(now);
        }
    }

    pub(super) fn mark_progress(&self, job_id: JobId, ticks: u64) {
        let now = now_ns();
        if let Some(entry) = self
            .inner
            .lock()
            .expect("job metrics lock poisoned")
            .get_mut(&job_id)
        {
            entry.progress_ticks = ticks;
            entry.last_progress_at_ns = Some(now);
        }
    }

    pub(super) fn mark_finished(&self, job_id: JobId, state: &str) {
        let now = now_ns();
        if let Some(entry) = self
            .inner
            .lock()
            .expect("job metrics lock poisoned")
            .get_mut(&job_id)
        {
            if let Some(started) = entry.pool_wait_started_at_ns.take() {
                entry.pool_wait_ms = entry
                    .pool_wait_ms
                    .saturating_add(duration_ms(started, now).unwrap_or(0));
            }
            entry.scheduler_state = state.to_string();
            entry.finished_at_ns = Some(now);
        }
    }

    pub(super) fn decorate(&self, snapshot: &mut JobSnapshot, observed_at_ns: i64) {
        let metrics = self.inner.lock().expect("job metrics lock poisoned");
        let Some(entry) = metrics.get(&snapshot.job_id) else {
            return;
        };
        snapshot.pool_group = entry.pool_group.clone();
        snapshot.scheduler_state = Some(entry.scheduler_state.clone());
        snapshot.enqueued_at = Some(entry.enqueued_at_ns);
        snapshot.run_started_at = entry.run_started_at_ns;
        snapshot.queued_ms = entry
            .run_started_at_ns
            .or(entry.finished_at_ns)
            .or_else(|| matches!(snapshot.state.as_str(), "queued").then_some(observed_at_ns))
            .and_then(|end| duration_ms(entry.enqueued_at_ns, end));
        let pool_wait_ms = entry.pool_wait_ms.saturating_add(
            entry
                .pool_wait_started_at_ns
                .and_then(|started| duration_ms(started, observed_at_ns))
                .unwrap_or(0),
        );
        snapshot.pool_wait_ms =
            (entry.pool_group.is_some() || pool_wait_ms > 0).then_some(pool_wait_ms);
        snapshot.run_ms = entry.run_started_at_ns.and_then(|started| {
            duration_ms(started, entry.finished_at_ns.unwrap_or(observed_at_ns))
        });
        snapshot.progress_ticks = Some(entry.progress_ticks);
        snapshot.last_progress_at = entry.last_progress_at_ns;
        snapshot.progress_per_minute = match (snapshot.run_ms, entry.progress_ticks) {
            (Some(run_ms), ticks) if run_ms > 0 && ticks > 0 => {
                Some((ticks as f64) * 60_000.0 / (run_ms as f64))
            }
            _ => None,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::tests::*;

    #[test]
    fn runtime_metrics_decorate_pool_waiting_job() {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(42, Some("clangd-lsp"), 1_000_000_000);
        metrics.mark_waiting_pool_group(42, "clangd-lsp");

        let mut snapshot = job(42, "clangd-cpp-lsp", "queued");
        metrics.decorate(&mut snapshot, 2_500_000_000);

        assert_eq!(snapshot.pool_group.as_deref(), Some("clangd-lsp"));
        assert_eq!(
            snapshot.scheduler_state.as_deref(),
            Some("waiting_pool_group")
        );
        assert_eq!(snapshot.enqueued_at, Some(1_000_000_000));
        assert!(snapshot.queued_ms.is_some_and(|ms| ms >= 1500));
        assert!(snapshot.pool_wait_ms.is_some());
    }

    #[test]
    fn runtime_metrics_decorate_running_progress_rate() {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(7, None, 1_000_000_000);
        metrics.mark_running(7);
        metrics.mark_progress(7, 120);
        std::thread::sleep(Duration::from_millis(2));

        let mut snapshot = job(7, "pyright-lsp", "running");
        metrics.decorate(&mut snapshot, now_ns());

        assert_eq!(snapshot.scheduler_state.as_deref(), Some("running"));
        assert_eq!(snapshot.progress_ticks, Some(120));
        assert!(snapshot.last_progress_at.is_some());
        assert!(snapshot.progress_per_minute.is_some());
    }
}
