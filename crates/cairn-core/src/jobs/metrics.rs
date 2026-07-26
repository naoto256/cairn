//! Process-local runtime metrics for analyzer jobs.
//!
//! The CAS store persists only coarse job state; everything here —
//! scheduler state, queue / pool-wait / run timings, progress
//! ticks — lives in one in-memory state and dies with the daemon.
//! The `entries` map is the metrics source of truth; a separate
//! `terminal_order` index orders terminal entries for bounded
//! eviction. `decorate` merges an entry into a
//! `JobSnapshot` at read time; a job with no entry (any row that
//! predates the last restart and was not re-enqueued by
//! `restore_from_db`) keeps all runtime-only snapshot fields
//! `None`, meaning "not tracked by this process".
//!
//! The map has a soft cap of 1,000 entries. Active jobs are never
//! evicted; terminal jobs are retained for retrospective decoration
//! while they fit, then the oldest terminal entries are discarded.
//! If active jobs alone exceed the cap, all of them remain tracked.
use super::*;
use std::collections::BTreeSet;

const MAX_RUNTIME_METRICS_ENTRIES: usize = 1_000;

/// Shared handle to the per-job entry map and terminal-order
/// eviction index; `Clone` copies the handle, not the state.
#[derive(Debug, Clone, Default)]
pub(super) struct JobRuntimeMetricsStore {
    inner: Arc<Mutex<JobRuntimeMetricsState>>,
}

#[derive(Debug, Default)]
struct JobRuntimeMetricsState {
    /// Runtime metrics keyed by job id. This is the source of truth;
    /// `terminal_order` contains only eviction keys into this map.
    entries: HashMap<JobId, JobRuntimeMetrics>,
    /// Terminal entries ordered from oldest to newest. Active jobs
    /// never appear here, and each terminal job has at most its
    /// current `(finished_at_ns, JobId)` key.
    terminal_order: BTreeSet<(i64, JobId)>,
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
    /// Enforce the soft total-entry cap without evicting active jobs.
    ///
    /// The `JobId` tiebreaker makes eviction deterministic when
    /// multiple terminal transitions receive the same timestamp.
    fn evict_oldest_terminal_over_cap(state: &mut JobRuntimeMetricsState) {
        while state.entries.len() > MAX_RUNTIME_METRICS_ENTRIES {
            let Some((finished_at, job_id)) = state.terminal_order.pop_first() else {
                // Active jobs are protected even when they alone
                // exceed the configured soft cap.
                break;
            };
            if state
                .entries
                .get(&job_id)
                .is_some_and(|entry| entry.finished_at_ns == Some(finished_at))
            {
                state.entries.remove(&job_id);
            }
        }
    }

    /// Create the job's runtime record at enqueue time. An insert
    /// for an id that already has an entry resets it wholesale and
    /// removes its prior terminal-order key. Job ids are normally
    /// allocator-unique, so reuse is defensive. The soft cap is
    /// enforced after insertion without evicting active entries.
    pub(super) fn mark_enqueued(
        &self,
        job_id: JobId,
        pool_group: Option<&'static str>,
        enqueued_at_ns: i64,
    ) {
        let mut state = self.inner.lock().expect("job metrics lock poisoned");
        if let Some(previous) = state.entries.insert(
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
        ) && let Some(finished_at) = previous.finished_at_ns
        {
            state.terminal_order.remove(&(finished_at, job_id));
        }
        Self::evict_oldest_terminal_over_cap(&mut state);
    }

    /// Record that the scheduler observed the job blocked behind
    /// its already-active pool group. Only the scheduler's enqueue
    /// path calls this, so a job whose group becomes busy *after*
    /// its own enqueue stays in state "queued" and accrues no
    /// pool-wait time. The `or_insert_with` arm is defensive;
    /// `mark_enqueued` normally created the entry already.
    pub(super) fn mark_waiting_pool_group(&self, job_id: JobId, pool_group: &'static str) {
        let now = now_ns();
        let mut state = self.inner.lock().expect("job metrics lock poisoned");
        let entry = state
            .entries
            .entry(job_id)
            .or_insert_with(|| JobRuntimeMetrics {
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
        Self::evict_oldest_terminal_over_cap(&mut state);
    }

    /// Worker-start transition: close any open pool-wait window into
    /// `pool_wait_ms` and stamp `run_started_at_ns` once.
    ///
    /// Terminal state is monotonic: a late or duplicate start signal
    /// cannot overwrite a completion already recorded by the worker.
    pub(super) fn mark_running(&self, job_id: JobId) {
        let now = now_ns();
        let mut state = self.inner.lock().expect("job metrics lock poisoned");
        if let Some(entry) = state.entries.get_mut(&job_id) {
            if entry.finished_at_ns.is_some() {
                return;
            }
            if let Some(started) = entry.pool_wait_started_at_ns.take() {
                entry.pool_wait_ms = entry
                    .pool_wait_ms
                    .saturating_add(duration_ms(started, now).unwrap_or(0));
            }
            entry.scheduler_state = "running".into();
            entry.run_started_at_ns.get_or_insert(now);
        }
    }

    /// Overwrite the cumulative tick counter and its timestamp while
    /// the job is active. Late progress after a terminal transition
    /// is ignored so retrospective metrics remain stable. `ticks` is
    /// the analyzer's absolute progress counter, not a delta.
    pub(super) fn mark_progress(&self, job_id: JobId, ticks: u64) {
        let now = now_ns();
        if let Some(entry) = self
            .inner
            .lock()
            .expect("job metrics lock poisoned")
            .entries
            .get_mut(&job_id)
        {
            if entry.finished_at_ns.is_some() {
                return;
            }
            entry.progress_ticks = ticks;
            entry.last_progress_at_ns = Some(now);
        }
    }

    /// Terminal transition: preserve final metrics for retrospective
    /// decoration, then evict the oldest terminal entry if the soft
    /// total-entry cap is exceeded. Also closes any still-open
    /// pool-wait window — a job cancelled while waiting for its group
    /// never passed through `mark_running`.
    pub(super) fn mark_finished(&self, job_id: JobId, terminal_state: &str) {
        let now = now_ns();
        let mut state = self.inner.lock().expect("job metrics lock poisoned");
        if let Some(previous) = state
            .entries
            .get(&job_id)
            .and_then(|entry| entry.finished_at_ns)
        {
            state.terminal_order.remove(&(previous, job_id));
        }
        let mut finished_key = None;
        if let Some(entry) = state.entries.get_mut(&job_id) {
            if let Some(started) = entry.pool_wait_started_at_ns.take() {
                entry.pool_wait_ms = entry
                    .pool_wait_ms
                    .saturating_add(duration_ms(started, now).unwrap_or(0));
            }
            entry.scheduler_state = terminal_state.to_string();
            entry.finished_at_ns = Some(now);
            finished_key = Some((now, job_id));
        }
        if let Some(key) = finished_key {
            state.terminal_order.insert(key);
        }
        Self::evict_oldest_terminal_over_cap(&mut state);
    }

    /// Fail a worker run only when no concurrent terminal transition
    /// has already won. In particular, a late error must not replace
    /// a cancellation recorded while the blocking worker was active.
    pub(super) fn mark_failed_if_unfinished(&self, job_id: JobId) {
        let now = now_ns();
        let mut state = self.inner.lock().expect("job metrics lock poisoned");
        let mut finished_key = None;
        if let Some(entry) = state.entries.get_mut(&job_id) {
            if entry.finished_at_ns.is_some() {
                return;
            }
            if let Some(started) = entry.pool_wait_started_at_ns.take() {
                entry.pool_wait_ms = entry
                    .pool_wait_ms
                    .saturating_add(duration_ms(started, now).unwrap_or(0));
            }
            entry.scheduler_state = RunStatus::Failed.as_str().to_string();
            entry.finished_at_ns = Some(now);
            finished_key = Some((now, job_id));
        }
        if let Some(key) = finished_key {
            state.terminal_order.insert(key);
        }
        Self::evict_oldest_terminal_over_cap(&mut state);
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
        let state = self.inner.lock().expect("job metrics lock poisoned");
        let Some(entry) = state.entries.get(&snapshot.job_id) else {
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

    fn normalize_terminal_time(state: &mut JobRuntimeMetricsState, job_id: JobId, fixed_ns: i64) {
        let current_ns = state
            .entries
            .get(&job_id)
            .and_then(|entry| entry.finished_at_ns)
            .expect("test job must be terminal");
        assert!(state.terminal_order.remove(&(current_ns, job_id)));
        state.entries.get_mut(&job_id).unwrap().finished_at_ns = Some(fixed_ns);
        assert!(state.terminal_order.insert((fixed_ns, job_id)));
    }

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

    #[test]
    fn terminal_metrics_are_retained_below_cap_and_decorate_snapshots() {
        let metrics = JobRuntimeMetricsStore::default();
        for job_id in 1..=3 {
            metrics.mark_enqueued(job_id, None, 1_000_000_000);
            metrics.mark_running(job_id);
            metrics.mark_progress(job_id, job_id as u64);
            metrics.mark_finished(job_id, RunStatus::Succeeded.as_str());
        }

        assert_eq!(metrics.inner.lock().unwrap().entries.len(), 3);
        for job_id in 1..=3 {
            let mut snapshot = job(job_id, "pyright-lsp", "succeeded");
            metrics.decorate(&mut snapshot, now_ns());
            assert_eq!(
                snapshot.scheduler_state.as_deref(),
                Some(RunStatus::Succeeded.as_str())
            );
            assert_eq!(snapshot.progress_ticks, Some(job_id as u64));
            assert!(snapshot.run_ms.is_some());
        }
    }

    #[test]
    fn cap_evicts_oldest_terminal_entry_and_protects_active_jobs() {
        let metrics = JobRuntimeMetricsStore::default();
        for job_id in 1..=(MAX_RUNTIME_METRICS_ENTRIES as i64 - 2) {
            metrics.mark_enqueued(job_id, None, 1_000_000_000);
        }
        let newer_terminal = 2_000;
        let older_terminal = 2_001;
        metrics.mark_enqueued(newer_terminal, None, 1_000_000_000);
        metrics.mark_finished(newer_terminal, RunStatus::Succeeded.as_str());
        metrics.mark_enqueued(older_terminal, None, 1_000_000_000);
        metrics.mark_finished(older_terminal, RunStatus::Succeeded.as_str());
        {
            let mut stored = metrics.inner.lock().unwrap();
            normalize_terminal_time(&mut stored, newer_terminal, 20);
            normalize_terminal_time(&mut stored, older_terminal, 10);
        }

        let newest_active = 3_000;
        metrics.mark_enqueued(newest_active, None, 1_000_000_000);

        let stored = metrics.inner.lock().unwrap();
        assert_eq!(stored.entries.len(), MAX_RUNTIME_METRICS_ENTRIES);
        assert!(!stored.entries.contains_key(&older_terminal));
        assert!(stored.entries.contains_key(&newer_terminal));
        assert!(stored.entries.contains_key(&newest_active));
        assert!(
            (1..=(MAX_RUNTIME_METRICS_ENTRIES as i64 - 2))
                .all(|job_id| stored.entries.contains_key(&job_id))
        );
    }

    #[test]
    fn active_jobs_may_exceed_soft_cap_without_eviction() {
        let metrics = JobRuntimeMetricsStore::default();
        for job_id in 1..=(MAX_RUNTIME_METRICS_ENTRIES as i64 + 1) {
            metrics.mark_enqueued(job_id, None, 1_000_000_000);
        }

        let stored = metrics.inner.lock().unwrap();
        assert_eq!(stored.entries.len(), MAX_RUNTIME_METRICS_ENTRIES + 1);
        assert!(
            stored
                .entries
                .values()
                .all(|entry| entry.finished_at_ns.is_none())
        );
        assert!(stored.terminal_order.is_empty());
    }

    #[test]
    fn terminal_metrics_cannot_regress_to_running() {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(9, None, 1_000_000_000);
        metrics.mark_finished(9, RunStatus::Cancelled.as_str());
        let finished_at = metrics
            .inner
            .lock()
            .unwrap()
            .entries
            .get(&9)
            .unwrap()
            .finished_at_ns;

        metrics.mark_running(9);

        let stored = metrics.inner.lock().unwrap();
        let entry = stored.entries.get(&9).unwrap();
        assert_eq!(entry.scheduler_state, RunStatus::Cancelled.as_str());
        assert_eq!(entry.finished_at_ns, finished_at);
        assert_eq!(entry.run_started_at_ns, None);
    }

    #[test]
    fn terminal_metrics_ignore_late_progress() {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(10, None, now_ns());
        metrics.mark_running(10);
        metrics.mark_progress(10, 7);
        metrics.mark_finished(10, RunStatus::Succeeded.as_str());

        let before_entry = metrics
            .inner
            .lock()
            .unwrap()
            .entries
            .get(&10)
            .unwrap()
            .clone();
        let observed_at = now_ns();
        let mut before_snapshot = job(10, "pyright-lsp", "succeeded");
        metrics.decorate(&mut before_snapshot, observed_at);

        metrics.mark_progress(10, 99);

        let after_entry = metrics
            .inner
            .lock()
            .unwrap()
            .entries
            .get(&10)
            .unwrap()
            .clone();
        let mut after_snapshot = job(10, "pyright-lsp", "succeeded");
        metrics.decorate(&mut after_snapshot, observed_at);

        assert_eq!(after_entry.progress_ticks, before_entry.progress_ticks);
        assert_eq!(
            after_entry.last_progress_at_ns,
            before_entry.last_progress_at_ns
        );
        assert_eq!(after_entry.scheduler_state, before_entry.scheduler_state);
        assert_eq!(after_entry.finished_at_ns, before_entry.finished_at_ns);
        assert_eq!(
            after_snapshot.progress_ticks,
            before_snapshot.progress_ticks
        );
        assert_eq!(
            after_snapshot.last_progress_at,
            before_snapshot.last_progress_at
        );
        assert_eq!(
            after_snapshot.progress_per_minute,
            before_snapshot.progress_per_minute
        );
        assert_eq!(
            after_snapshot.scheduler_state,
            before_snapshot.scheduler_state
        );
    }

    #[test]
    fn job_id_reuse_removes_prior_terminal_index() {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(11, None, 1_000_000_000);
        metrics.mark_finished(11, RunStatus::Succeeded.as_str());
        let old_key = *metrics
            .inner
            .lock()
            .unwrap()
            .terminal_order
            .iter()
            .find(|(_, job_id)| *job_id == 11)
            .unwrap();

        metrics.mark_enqueued(11, Some("clangd-lsp"), 2_000_000_000);

        let stored = metrics.inner.lock().unwrap();
        let entry = stored.entries.get(&11).unwrap();
        assert_eq!(entry.scheduler_state, "queued");
        assert_eq!(entry.finished_at_ns, None);
        assert_eq!(entry.progress_ticks, 0);
        assert!(!stored.terminal_order.contains(&old_key));
        assert!(
            stored
                .terminal_order
                .iter()
                .all(|(_, job_id)| *job_id != 11)
        );
    }

    #[test]
    fn duplicate_terminal_transition_replaces_index_key() {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(12, None, 1_000_000_000);
        metrics.mark_finished(12, RunStatus::Failed.as_str());
        {
            let mut stored = metrics.inner.lock().unwrap();
            normalize_terminal_time(&mut stored, 12, 30);
        }
        metrics.mark_finished(12, RunStatus::Cancelled.as_str());
        {
            let mut stored = metrics.inner.lock().unwrap();
            normalize_terminal_time(&mut stored, 12, 20);
        }

        let stored = metrics.inner.lock().unwrap();
        let entry = stored.entries.get(&12).unwrap();
        let keys: Vec<_> = stored
            .terminal_order
            .iter()
            .filter(|(_, job_id)| *job_id == 12)
            .copied()
            .collect();
        assert_eq!(keys, vec![(20, 12)]);
        assert!(!stored.terminal_order.contains(&(30, 12)));
        assert!(stored.terminal_order.contains(&(20, 12)));
        assert_eq!(entry.finished_at_ns, Some(20));
        assert_eq!(entry.scheduler_state, RunStatus::Cancelled.as_str());
    }

    #[test]
    fn reuse_and_duplicate_leave_eviction_order_consistent() {
        let metrics = JobRuntimeMetricsStore::default();

        let reused_active = 1;
        metrics.mark_enqueued(reused_active, None, 1_000_000_000);
        metrics.mark_finished(reused_active, RunStatus::Succeeded.as_str());
        metrics.mark_enqueued(reused_active, None, 2_000_000_000);

        let newer_terminal = 2;
        metrics.mark_enqueued(newer_terminal, None, 1_000_000_000);
        metrics.mark_finished(newer_terminal, RunStatus::Failed.as_str());
        metrics.mark_finished(newer_terminal, RunStatus::Succeeded.as_str());

        let oldest_terminal = 3;
        metrics.mark_enqueued(oldest_terminal, None, 1_000_000_000);
        metrics.mark_finished(oldest_terminal, RunStatus::Succeeded.as_str());

        {
            let mut stored = metrics.inner.lock().unwrap();
            normalize_terminal_time(&mut stored, newer_terminal, 20);
            normalize_terminal_time(&mut stored, oldest_terminal, 10);
        }

        for job_id in 4..=MAX_RUNTIME_METRICS_ENTRIES as i64 {
            metrics.mark_enqueued(job_id, None, 1_000_000_000);
        }
        let newest_active = MAX_RUNTIME_METRICS_ENTRIES as i64 + 1;
        metrics.mark_enqueued(newest_active, None, 1_000_000_000);

        let stored = metrics.inner.lock().unwrap();
        assert_eq!(stored.entries.len(), MAX_RUNTIME_METRICS_ENTRIES);
        assert!(!stored.entries.contains_key(&oldest_terminal));
        assert!(stored.entries.contains_key(&newer_terminal));
        assert!(stored.entries.contains_key(&reused_active));
        assert!(stored.entries.contains_key(&newest_active));
        assert!(
            stored
                .terminal_order
                .iter()
                .all(|(_, job_id)| *job_id != reused_active)
        );
        assert_eq!(
            stored
                .terminal_order
                .iter()
                .filter(|(_, job_id)| *job_id == newer_terminal)
                .count(),
            1
        );
    }
}
