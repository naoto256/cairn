//! Process-local runtime metrics for analyzer jobs.
//!
//! The CAS store persists only coarse job state; everything here —
//! scheduler state, queue / pool-wait / run timings, progress
//! ticks — lives in one in-memory map keyed by `JobId` and dies
//! with the daemon. `decorate` merges an entry into a
//! `JobSnapshot` at read time; a job with no entry (any row that
//! predates the last restart and was not re-enqueued by
//! `restore_from_db`) keeps all runtime-only snapshot fields
//! `None`, meaning "not tracked by this process".
//!
//! Entries are inserted at enqueue time and are not removed when a
//! job finishes: terminal entries stay readable for `jobs list`
//! decoration until the daemon exits.
use super::*;

/// Shared handle to the per-job runtime metrics map; `Clone`
/// copies the handle, not the map.
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
    /// "queued" -> ("waiting_pool_group" ->) "running" -> a
    /// terminal `RunStatus` string once `mark_finished` runs.
    scheduler_state: String,
    /// Start of the currently open pool-wait window, if the job is
    /// waiting behind its active pool group; folded into
    /// `pool_wait_ms` when the window closes (run start / finish).
    pool_wait_started_at_ns: Option<i64>,
    /// Pool-wait time from already-closed windows, in
    /// milliseconds. `decorate` adds any still-open window on top.
    pool_wait_ms: u64,
    run_started_at_ns: Option<i64>,
    finished_at_ns: Option<i64>,
    /// Latest cumulative tick count reported by the analyzer — an
    /// absolute counter, not a delta.
    progress_ticks: u64,
    last_progress_at_ns: Option<i64>,
}

impl JobRuntimeMetricsStore {
    /// Create the job's runtime record at enqueue time. An insert
    /// for an id that already has an entry would reset it
    /// wholesale, but job ids are allocator-unique, so in practice
    /// each id is created here exactly once.
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

    /// Record that the scheduler observed the job blocked behind
    /// its already-active pool group. Only the scheduler's enqueue
    /// path calls this, so a job whose group becomes busy *after*
    /// its own enqueue stays in state "queued" and accrues no
    /// pool-wait time. The `or_insert_with` arm is defensive;
    /// `mark_enqueued` normally created the entry already.
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

    /// Dispatch-time transition: close any open pool-wait window
    /// into `pool_wait_ms` and stamp `run_started_at_ns` once.
    /// "running" here means handed to the worker channel — the
    /// analyzer process itself may start slightly later.
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

    /// Overwrite the cumulative tick counter and its timestamp.
    /// `ticks` is the analyzer's absolute progress counter, not a
    /// delta.
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

    /// Terminal transition: `state` is the final `RunStatus`
    /// string. Also closes any still-open pool-wait window — a job
    /// cancelled while waiting for its group never passed through
    /// `mark_running`.
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

    /// Merge this store's entry into a snapshot. No-op when the
    /// job was never tracked by this process — the runtime-only
    /// snapshot fields then stay `None`.
    ///
    /// Derived timings (each yields `None` via `duration_ms` if
    /// the clock went backwards across the interval):
    /// * `queued_ms` — enqueue until run start; until finish for
    ///   jobs that never ran (e.g. cancelled while queued); or
    ///   until `observed_at_ns` while the row still says "queued".
    /// * `pool_wait_ms` — closed windows plus the open window up
    ///   to `observed_at_ns`; reported whenever the job has a pool
    ///   group, so an uncontended pooled job shows `Some(0)`.
    /// * `run_ms` — run start until finish, or until
    ///   `observed_at_ns` while still running.
    /// * `progress_per_minute` — rate over `run_ms`; `None` until
    ///   at least one tick and a positive `run_ms` exist.
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
