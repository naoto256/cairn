//! Background Tier-3 workspace analyzer jobs.
//!
//! The CAS store already keeps one current `workspace_analysis_runs`
//! row per `(manifest, analyzer)`. This module makes those rows the
//! daemon-visible job state and keeps the expensive LSP-backed work out
//! of short-lived control requests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::cas::{registry as cas_registry, store as cas_store};
use crate::manifest::{self, ManifestEntry, ManifestId};
use crate::paths::CasDataDir;
use crate::workspace_analyzer::{
    ANALYZER_STALL_TIMEOUT, AnalyzerRunRequest, RunRecord, RunStatus, all_workspace_analyzers,
    config_hash, expected_analyzers_for_manifest, mark_run,
    run_one_workspace_analyzer_with_timeout,
};
use crate::{Error, Result};

pub type JobId = i64;

const MAX_WORKER_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueuedAnalyzerJob {
    pub job_id: JobId,
    pub analyzer_id: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobSnapshot {
    pub job_id: JobId,
    pub alias: String,
    pub analyzer_id: String,
    pub state: String,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub error: Option<String>,
    pub pool_group: Option<String>,
    pub scheduler_state: Option<String>,
    pub enqueued_at: Option<i64>,
    pub run_started_at: Option<i64>,
    pub queued_ms: Option<u64>,
    pub pool_wait_ms: Option<u64>,
    pub run_ms: Option<u64>,
    pub progress_ticks: Option<u64>,
    pub last_progress_at: Option<i64>,
    pub progress_per_minute: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct JobListOptions {
    pub(crate) include_all: bool,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CancelResult {
    pub cancelled: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobsPruneSummary {
    pub repos: Vec<JobsPruneRepoSummary>,
    pub total_deleted_runs: u64,
    pub total_deleted_index_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobsPruneRepoSummary {
    pub alias: String,
    pub deleted_runs_count: u64,
    pub deleted_index_entries_count: u64,
}

#[derive(Debug, Clone)]
struct Job {
    id: JobId,
    alias: String,
    repo_hash: String,
    store_path: PathBuf,
    repo_root: PathBuf,
    manifest_id: ManifestId,
    analyzer_id: String,
}

struct QueueAnalyzerRun<'a> {
    conn: &'a mut Connection,
    alias: &'a str,
    repo_hash: &'a str,
    repo_root: &'a Path,
    manifest_id: ManifestId,
    now_ns: i64,
    job_id: JobId,
    analyzer_id: &'a str,
    analyzer_revision: u32,
    config_hash: &'a str,
    pool_group: Option<&'static str>,
}

#[derive(Debug)]
enum SchedulerMsg {
    Enqueue(Job),
    WorkerFinished {
        job_id: JobId,
        pool_group: Option<&'static str>,
        key: JobKey,
    },
    Cancel(JobId),
    Shutdown,
}

/// A single row observed during `restore_from_db`, tagged with the
/// store it lives in and whether it is dispatch-active (queued or a
/// running row that was flipped back to queued). Used to build the
/// cross-store `job_id -> Vec<RestoreRow>` collision map before the
/// recycle rewrite in phase 3. `is_active == false` rows are
/// non-active (any terminal status) — they must still participate
/// in collision detection because `JobId` is an external identifier
/// that clients can still hold, but they are never advertised to
/// the memory queue in phase 4.
#[derive(Debug, Clone)]
struct RestoreRow {
    alias: String,
    repo_hash: String,
    store_path: PathBuf,
    repo_root: PathBuf,
    manifest_id: ManifestId,
    analyzer_id: String,
    is_active: bool,
}

#[derive(Debug)]
struct DispatchJob {
    job: Job,
    pool_group: Option<&'static str>,
    key: JobKey,
}

// `JobKey` carries `repo_hash` so per-store dedup / tracked-key
// reservation cannot collide across stores that happen to share
// `(manifest_id, analyzer_id)` — `manifest_id` is a per-store
// `INTEGER PRIMARY KEY`, so the (manifest_id=2, "rust-analyzer-lsp")
// pair is normal to see in multiple stores. Without `repo_hash`,
// `TrackedJobKeys::reserve_after` and `restore_from_db`'s
// `reserve_existing` would silently coalesce a second store's job
// onto the first, leaving that store's Tier-2.5 / Tier-3 stale.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct JobKey {
    repo_hash: String,
    manifest_id: ManifestId,
    analyzer_id: String,
}

impl JobKey {
    fn from_job(job: &Job) -> Self {
        Self {
            repo_hash: job.repo_hash.clone(),
            manifest_id: job.manifest_id,
            analyzer_id: job.analyzer_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum GroupLane {
    Pooled(&'static str),
    Unpooled,
}

#[derive(Debug, Clone)]
struct JobLocator {
    alias: String,
    repo_hash: String,
}

#[derive(Debug, Clone, Default)]
struct JobIndex {
    inner: Arc<Mutex<HashMap<JobId, JobLocator>>>,
}

impl JobIndex {
    fn insert(&self, job_id: JobId, alias: &str, repo_hash: &str) {
        self.inner.lock().expect("job index lock poisoned").insert(
            job_id,
            JobLocator {
                alias: alias.to_string(),
                repo_hash: repo_hash.to_string(),
            },
        );
    }

    fn get(&self, job_id: JobId) -> Option<JobLocator> {
        self.inner
            .lock()
            .expect("job index lock poisoned")
            .get(&job_id)
            .cloned()
    }

    fn remove(&self, job_id: JobId) -> bool {
        self.inner
            .lock()
            .expect("job index lock poisoned")
            .remove(&job_id)
            .is_some()
    }

    fn remove_many(&self, job_ids: &[JobId]) -> u64 {
        let mut index = self.inner.lock().expect("job index lock poisoned");
        job_ids
            .iter()
            .filter(|job_id| index.remove(job_id).is_some())
            .count() as u64
    }

    fn count_present(&self, job_ids: &[JobId]) -> u64 {
        let index = self.inner.lock().expect("job index lock poisoned");
        job_ids
            .iter()
            .filter(|job_id| index.contains_key(job_id))
            .count() as u64
    }
}

#[derive(Debug, Clone, Default)]
struct TrackedJobKeys {
    inner: Arc<Mutex<HashSet<JobKey>>>,
}

impl TrackedJobKeys {
    fn reserve_after(
        &self,
        key: JobKey,
        write_current_row: impl FnOnce() -> Result<()>,
    ) -> Result<bool> {
        let mut keys = self.inner.lock().expect("tracked job key lock poisoned");
        if keys.contains(&key) {
            return Ok(false);
        }
        write_current_row()?;
        keys.insert(key);
        Ok(true)
    }

    fn reserve_existing(&self, key: JobKey) -> bool {
        self.inner
            .lock()
            .expect("tracked job key lock poisoned")
            .insert(key)
    }

    fn release(&self, key: &JobKey) {
        self.inner
            .lock()
            .expect("tracked job key lock poisoned")
            .remove(key);
    }
}

#[derive(Debug, Clone, Default)]
struct JobRuntimeMetricsStore {
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
    fn mark_enqueued(&self, job_id: JobId, pool_group: Option<&'static str>, enqueued_at_ns: i64) {
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

    fn mark_waiting_pool_group(&self, job_id: JobId, pool_group: &'static str) {
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

    fn mark_running(&self, job_id: JobId) {
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

    fn mark_progress(&self, job_id: JobId, ticks: u64) {
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

    fn mark_finished(&self, job_id: JobId, state: &str) {
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

    fn decorate(&self, snapshot: &mut JobSnapshot, observed_at_ns: i64) {
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

pub struct JobManager {
    cas_data_dir: Arc<CasDataDir>,
    scheduler_sender: Mutex<Option<mpsc::UnboundedSender<SchedulerMsg>>>,
    scheduler_receiver: Mutex<Option<mpsc::UnboundedReceiver<SchedulerMsg>>>,
    scheduler: Mutex<Option<JoinHandle<()>>>,
    worker_sender: Mutex<Option<mpsc::UnboundedSender<DispatchJob>>>,
    worker_receiver: Arc<AsyncMutex<mpsc::UnboundedReceiver<DispatchJob>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    runtime_metrics: JobRuntimeMetricsStore,
    job_index: JobIndex,
    tracked_keys: TrackedJobKeys,
    // Daemon-global monotonic `JobId` allocator. `restore_from_db`
    // seeds this above the whole-store historical max and any
    // tombstoned id, so post-restart enqueues cannot collide with
    // any active or retired row across all stores — a per-store
    // `MAX(job_id)+1` would happily reissue.
    next_job_id: Arc<AtomicI64>,
}

pub struct EnqueueReindex<'a> {
    pub conn: &'a mut Connection,
    pub alias: &'a str,
    pub repo_hash: &'a str,
    pub repo_root: &'a Path,
    pub manifest_id: ManifestId,
    pub entries: &'a [ManifestEntry],
    pub now_ns: i64,
}

/// Why a full-repo reindex is being enqueued.
///
/// Recorded on every [`FullReindexEnqueueOutcome`] so call sites
/// (manual operator request vs. auto-recovery from drift detection)
/// can be told apart in logs and metrics. The variant set is open
/// for future drivers (e.g. a watcher heuristic that escalates to
/// full reindex) — add a new entry rather than overloading an
/// existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexReason {
    /// Operator invoked `cairn ctl repo reindex <alias>`.
    Manual,
    /// Daemon-startup scanner detected a `parser_revision` drift
    /// (the linked-in backend's `parser_revision()` no longer
    /// matches the `blobs.parser_revision` persisted for an
    /// expected parse unit, or the row is missing entirely).
    ParserRevisionDrift,
}

/// Outcome of a successful [`JobManager::enqueue_full_repo_reindex`]
/// call.
///
/// The shape is wider than `Result<()>` so callers can distinguish:
///   * `jobs_enqueued == 0` (analyzer registry produced no jobs for
///     this manifest — e.g. a `.gitignore`-only repo)
///   * `skip_analyzers_for_unchanged_manifest == true` (the
///     register dedup gate decided the tentative manifest is
///     byte-identical and reused it; analyzer pass skipped)
///   * `blobs_parsed == 0` paired with the dedup skip (no work
///     was actually needed — should never co-occur with
///     `ReindexReason::ParserRevisionDrift`, see the test in
///     `staleness::e2e`)
///
/// `coalesced: bool` is intentionally absent. A single
/// `enqueue_full_repo_reindex` request can drive *multiple* analyzer
/// rows; collapsing the per-row coalesce decisions to one boolean
/// would lose detail. `jobs_enqueued` already reports the resulting
/// row count.
#[derive(Debug, Clone)]
pub struct FullReindexEnqueueOutcome {
    pub alias: String,
    pub repo_hash: String,
    pub reason: ReindexReason,
    pub manifest_id: ManifestId,
    pub blobs_parsed: usize,
    pub jobs_enqueued: usize,
    pub skip_analyzers_for_unchanged_manifest: bool,
}

/// Public enqueue request for one specific (manifest, analyzer) pair.
///
/// Caller supplies the routing identity (`alias`, `repo_hash`,
/// `repo_root`, `manifest_id`, `analyzer_id`) and a clock
/// (`now_ns`). The run-row stamp — `analyzer_revision`, `config_hash`,
/// `pool_group`, `job_id` — is derived **inside**
/// [`JobManager::enqueue_analyzer_run`] from the linked-in analyzer
/// registry, so a buggy caller cannot stamp a stale revision or a
/// wrong pool group. If the linked-in registry has no analyzer with
/// the requested `analyzer_id` for this manifest, the enqueue
/// returns `Ok(None)` (typed skip) instead of synthesizing a row.
///
/// De-dup against running/queued jobs and the in-memory tracked-keys
/// set still applies; concurrent identical requests collapse to one
/// row, with the loser observing `Ok(None)`.
pub struct EnqueueAnalyzerRun<'a> {
    pub conn: &'a mut Connection,
    pub alias: &'a str,
    pub repo_hash: &'a str,
    pub repo_root: &'a Path,
    pub manifest_id: ManifestId,
    pub analyzer_id: &'a str,
    pub now_ns: i64,
}

impl JobManager {
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Arc<Self> {
        let (scheduler_sender, scheduler_receiver) = mpsc::unbounded_channel();
        let (worker_sender, worker_receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            cas_data_dir,
            scheduler_sender: Mutex::new(Some(scheduler_sender)),
            scheduler_receiver: Mutex::new(Some(scheduler_receiver)),
            scheduler: Mutex::new(None),
            worker_sender: Mutex::new(Some(worker_sender)),
            worker_receiver: Arc::new(AsyncMutex::new(worker_receiver)),
            workers: Mutex::new(Vec::new()),
            runtime_metrics: JobRuntimeMetricsStore::default(),
            job_index: JobIndex::default(),
            tracked_keys: TrackedJobKeys::default(),
            // Fresh manager starts at 1 so tests that assert
            // `first.job_id == 1` after a bare `JobManager::new` keep
            // working; in production `restore_from_db()` is called
            // right after `new()` and re-seeds to
            // `max(observed_max, now_ns())`, which is the value the
            // cross-store correctness invariant actually depends on.
            // `seed_job_id_allocator_at_least` uses a compare-and-swap
            // bump so an already-issued monotonic value never
            // regresses.
            next_job_id: Arc::new(AtomicI64::new(1)),
        })
    }

    /// Allocate a fresh, daemon-globally unique `JobId`. All new
    /// enqueues route through this or its `_at_least` variants
    /// instead of the old per-store `next_job_id(conn)` (which was
    /// `MAX(job_id)+1` scoped to a single store and produced
    /// cross-store collisions).
    ///
    /// Fails closed on counter overflow — the allocator is the
    /// uniqueness invariant, so silently wrapping past `i64::MAX`
    /// would hand out a colliding id.
    fn allocate_job_id(&self) -> Result<JobId> {
        let mut current = self.next_job_id.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| Error::Internal("job id allocator overflowed i64".into()))?;
            match self.next_job_id.compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed,
            }
        }
    }

    /// Allocate a fresh id that is guaranteed to be `>= floor`. The
    /// allocator's internal counter is advanced past the returned
    /// value so subsequent allocations cannot reissue it — this is
    /// the invariant `allocate_job_id().max(floor)` breaks: that
    /// pattern returns `floor` but leaves the counter at
    /// `current + 1`, so a second call with the same floor returns
    /// the same value. Fails closed on counter overflow.
    fn allocate_job_id_at_least(&self, floor: JobId) -> Result<JobId> {
        let mut current = self.next_job_id.load(Ordering::Relaxed);
        loop {
            let start = current.max(floor);
            let next = start
                .checked_add(1)
                .ok_or_else(|| Error::Internal("job id allocator overflowed i64".into()))?;
            match self.next_job_id.compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(start),
                Err(observed) => current = observed,
            }
        }
    }

    /// Reserve a contiguous block of `count` ids whose first element
    /// is `>= floor`. Same floor-advance invariant as
    /// `allocate_job_id_at_least`. Overflow on the trailing end of
    /// the block is a fail-closed error.
    fn allocate_job_id_range_at_least(&self, count: usize, floor: JobId) -> Result<JobId> {
        let count_i64 = i64::try_from(count)
            .map_err(|_| Error::Internal("job id range too large for i64".into()))?;
        let mut current = self.next_job_id.load(Ordering::Relaxed);
        loop {
            let start = current.max(floor);
            let next = start
                .checked_add(count_i64)
                .ok_or_else(|| Error::Internal("job id allocator overflowed i64".into()))?;
            match self.next_job_id.compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(start),
                Err(observed) => current = observed,
            }
        }
    }

    /// Advance the allocator so the next `allocate_job_id` returns at
    /// least `at_least + 1`. Called during `restore_from_db` after
    /// scanning every store's active rows so post-restart enqueues
    /// cannot reissue an id that is still active anywhere. Overflow
    /// of `at_least + 1` fails closed.
    fn seed_job_id_allocator_at_least(&self, at_least: JobId) -> Result<()> {
        let bump = at_least
            .checked_add(1)
            .ok_or_else(|| Error::Internal("job id allocator overflowed i64".into()))?;
        let mut current = self.next_job_id.load(Ordering::Relaxed);
        loop {
            let target = current.max(bump);
            if target == current {
                return Ok(());
            }
            match self.next_job_id.compare_exchange(
                current,
                target,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    pub fn start_workers(self: &Arc<Self>) {
        let workers = worker_concurrency();
        for _ in 0..workers {
            let manager = self.clone();
            let handle = tokio::spawn(async move {
                manager.worker_loop().await;
            });
            self.workers
                .lock()
                .expect("job worker lock poisoned")
                .push(handle);
        }
        let scheduler_receiver = self
            .scheduler_receiver
            .lock()
            .expect("job scheduler receiver lock poisoned")
            .take();
        if let Some(receiver) = scheduler_receiver {
            let worker_sender = self
                .worker_sender
                .lock()
                .expect("job worker sender lock poisoned")
                .as_ref()
                .cloned();
            if let Some(worker_sender) = worker_sender {
                let runtime_metrics = self.runtime_metrics.clone();
                let tracked_keys = self.tracked_keys.clone();
                let handle = tokio::spawn(async move {
                    scheduler_loop(
                        receiver,
                        worker_sender,
                        workers,
                        runtime_metrics,
                        tracked_keys,
                    )
                    .await;
                });
                *self.scheduler.lock().expect("job scheduler lock poisoned") = Some(handle);
            }
        }
        info!(workers, "job scheduler and workers started");
    }

    pub fn restore_from_db(&self) -> Result<()> {
        let index_db_path = self.cas_data_dir.index_db_path();
        let mut index = cas_registry::open(&index_db_path)?;
        let entries = cas_registry::list_all(&index)?;
        // `list_all` returns *alias* rows, not stores. Two aliases
        // pointing at the same `repo_hash` would otherwise cause
        // every phase below to open the same store twice, doubling
        // every queued row into `active_by_id` and triggering a
        // false cross-store collision on rows that are actually the
        // same DB row. Dedupe by `repo_hash` up front and keep the
        // lexicographically-smallest alias as the representative —
        // deterministic because `list_all` is already `ORDER BY
        // alias`. Phase 4's memory-queue Job then carries a
        // canonical alias per store.
        let unique_entries: Vec<_> = {
            let mut seen: HashSet<String> = HashSet::new();
            entries
                .iter()
                .filter(|e| seen.insert(e.repo_hash.clone()))
                .cloned()
                .collect()
        };
        // Load every id previously retired as ambiguous. Any surviving
        // row that still carries one of these ids must be recycled
        // even if no sibling row is present this time — a prior
        // restart partially rewrote a collision group before crashing
        // and we would otherwise resolve `cancel(old_id)` to that last
        // row, silently targeting a still-live sibling of the
        // (already-rewritten) intended job.
        let existing_tombstones = cas_registry::all_ambiguous_job_ids(&index)?;

        // Phase 1: status recovery only. Flip `running` back to
        // `queued` (a `running` row can only have been produced by a
        // now-dead daemon). All `job_id` assignment — including
        // NULL-fill — happens in phase 2 through the daemon-global
        // allocator, strictly after `observed_max` seeding, so no
        // per-store id is ever written to disk.
        for entry in &unique_entries {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET status = 'queued', finished_at_ns = NULL, error = NULL
                 WHERE status = 'running'",
                [],
            )?;
        }

        // Phase 2: collect every row across every store — all
        // statuses, not just `queued`. `JobId` is an external
        // identifier (`jobs.list` / `jobs.cancel`), so a terminal
        // row that shares an id with a queued row in a sibling store
        // is a collision even though the terminal row is never
        // re-dispatched: a stale client's `cancel(old_id)` would
        // otherwise silently target the queued sibling. Rows with a
        // concrete `job_id` are grouped by id for collision
        // detection; rows with `job_id IS NULL` (queued only,
        // realistically) get fresh globally-unique ids assigned from
        // the allocator *after* seeding. `observed_max` spans every
        // historical row so the allocator floor clears the whole
        // history.
        let mut all_by_id: HashMap<JobId, Vec<RestoreRow>> = HashMap::new();
        let mut missing_id_rows: Vec<RestoreRow> = Vec::new();
        let mut observed_max: JobId = 0;
        // Tombstoned ids are permanently retired. Historical store
        // rows alone don't enforce that: a prune sweep or a
        // wall-clock rollback could drop the store's `MAX(job_id)`
        // below a tombstoned id, and the allocator would re-issue
        // it. Include the tombstone max in the floor as well.
        if let Some(&max_tomb) = existing_tombstones.iter().max() {
            observed_max = observed_max.max(max_tomb);
        }
        for entry in &unique_entries {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            // Whole-store historical max — includes terminal rows.
            let store_max: Option<i64> = conn
                .query_row(
                    "SELECT MAX(job_id) FROM workspace_analysis_runs
                     WHERE job_id IS NOT NULL",
                    [],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            if let Some(m) = store_max {
                observed_max = observed_max.max(m);
            }
            let mut stmt = conn.prepare(
                "SELECT job_id, manifest_id, analyzer_id, status
                 FROM workspace_analysis_runs
                 ORDER BY started_at_ns ASC, analyzer_id ASC",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, Option<i64>>(0)?,
                        ManifestId(r.get::<_, i64>(1)?),
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (maybe_id, manifest_id, analyzer_id, status) in rows {
                let is_active = status == "queued";
                let row = RestoreRow {
                    alias: entry.alias.clone(),
                    repo_hash: entry.repo_hash.clone(),
                    store_path: store_path.clone(),
                    repo_root: PathBuf::from(&entry.root_path),
                    manifest_id,
                    analyzer_id,
                    is_active,
                };
                match maybe_id {
                    Some(id) => all_by_id.entry(id).or_default().push(row),
                    None => {
                        // A NULL `job_id` on a terminal row is
                        // meaningless (it was never externally
                        // named) — skip it. Only queued rows realistically
                        // land here.
                        if is_active {
                            missing_id_rows.push(row);
                        }
                    }
                }
            }
        }

        // Seed the daemon-global allocator so any post-restart
        // enqueue (including the NULL-fill and recycles below)
        // starts strictly after every observed active id and after
        // wall-clock `now_ns()` (preserving the monotonic-ish
        // property across restarts).
        self.seed_job_id_allocator_at_least(observed_max.max(now_ns()))?;

        // Assign fresh globally-unique ids to rows whose `job_id`
        // was persisted as NULL. This happens *after* the allocator
        // seed so the new ids clear every historical / tombstoned
        // id, and it uses `allocate_job_id()?` so an allocator at
        // `i64::MAX` fails closed rather than silently reissuing.
        for row in missing_id_rows {
            let new_id = self.allocate_job_id()?;
            let conn = cas_store::open(&row.store_path)?;
            // Identity-only: assign the new `job_id` without
            // touching `cancel_requested`. A queued row whose
            // cancel had already been requested must remain
            // scheduled for cancellation.
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET job_id = ?1
                 WHERE manifest_id = ?2 AND analyzer_id = ?3",
                params![new_id, row.manifest_id.0, row.analyzer_id],
            )?;
            all_by_id.entry(new_id).or_default().push(row);
        }

        // Phase 3: rewrite collision groups. A group needs recycling
        // if its `old_id` was ambiguous *now* (`rows.len() > 1`) OR
        // *previously* (present in `existing_tombstones` from an
        // earlier partial rewrite). Every row in such a group gets a
        // fresh globally-unique `JobId`; truly-unique groups
        // (`rows.len() == 1` and not tombstoned) keep their original
        // id. Critically the pre-restart ambiguous id is **not**
        // reused by any row and is **not** inserted into `JobIndex`,
        // so a stale client holding it will hit the fallback scan
        // and receive `unknown job id`. Preserving one arbitrary row
        // in a collision group would be unsafe — it would resolve
        // the ambiguous id to that row and silently cancel the
        // wrong job.
        //
        // The per-store UPDATE loop is not cross-store atomic: if
        // store B's UPDATE fails after store A's succeeded, store A
        // holds the new id and store B still holds `old_id`, so
        // `cancel(old_id)` would silently target store B's row. The
        // fix is a durable tombstone in `index.db::ambiguous_job_ids`
        // committed **before** any per-store UPDATE. On partial
        // failure the tombstone survives, `cancel(old_id)` returns
        // unknown, and the next restart sees the surviving row's id
        // in `existing_tombstones` and recycles it.
        let mut new_ambiguous_ids: Vec<JobId> = Vec::new();
        for (old_id, rows) in &all_by_id {
            if rows.len() > 1 && !existing_tombstones.contains(old_id) {
                new_ambiguous_ids.push(*old_id);
            }
        }
        if !new_ambiguous_ids.is_empty() {
            let retired_at = now_ns();
            let tx = index.transaction()?;
            cas_registry::insert_ambiguous_ids(&tx, &new_ambiguous_ids, retired_at)?;
            tx.commit()?;
        }
        let mut recycled: Vec<(JobId, JobId)> = Vec::new();
        let mut rows_after_recycle: Vec<(JobId, RestoreRow)> = Vec::new();
        for (old_id, rows) in all_by_id {
            let must_recycle = rows.len() > 1 || existing_tombstones.contains(&old_id);
            if !must_recycle {
                let row = rows.into_iter().next().expect("len checked");
                rows_after_recycle.push((old_id, row));
                continue;
            }
            for row in rows {
                let new_id = self.allocate_job_id()?;
                let conn = cas_store::open(&row.store_path)?;
                // Identity-only rewrite: `cancel_requested` is a
                // scheduling flag, not part of the identity, so it
                // must survive the recycle. Otherwise a queued row
                // whose cancel was requested pre-restart would be
                // silently re-armed, and a terminal row's
                // historical `cancel_requested` value would be lost.
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET job_id = ?1
                     WHERE manifest_id = ?2 AND analyzer_id = ?3",
                    params![new_id, row.manifest_id.0, row.analyzer_id],
                )?;
                recycled.push((old_id, new_id));
                rows_after_recycle.push((new_id, row));
            }
        }
        if !recycled.is_empty() {
            debug!(
                collisions = recycled.len(),
                "restore: recycled ambiguous job ids to fresh globally-unique values",
            );
        }

        // Phase 4: advertise every *active* row to the memory queue.
        // Terminal rows are still part of `rows_after_recycle` for
        // collision purposes but must not be dispatched — they are
        // filtered out here by the `is_active` flag. `JobKey` carries
        // `repo_hash`, so `reserve_existing` cannot coalesce two
        // stores that happen to share `(manifest_id, analyzer_id)`.
        for (id, row) in rows_after_recycle {
            if !row.is_active {
                continue;
            }
            let key = JobKey {
                repo_hash: row.repo_hash.clone(),
                manifest_id: row.manifest_id,
                analyzer_id: row.analyzer_id.clone(),
            };
            if !self.tracked_keys.reserve_existing(key.clone()) {
                continue;
            }
            self.runtime_metrics.mark_enqueued(
                id,
                pool_group_for_analyzer_id(&row.analyzer_id).ok().flatten(),
                now_ns(),
            );
            let enqueued = self.enqueue_memory(Job {
                id,
                alias: row.alias.clone(),
                repo_hash: row.repo_hash.clone(),
                store_path: row.store_path.clone(),
                repo_root: row.repo_root.clone(),
                manifest_id: row.manifest_id,
                analyzer_id: row.analyzer_id.clone(),
            });
            if !enqueued {
                self.tracked_keys.release(&key);
            }
        }
        Ok(())
    }

    /// Enqueue one `(manifest_id, analyzer_id)` rerun. The job stamp
    /// (`analyzer_revision`, `config_hash`, `pool_group`, `job_id`) is
    /// computed **inside** this method from the linked-in analyzer
    /// registry — callers do not (and must not) supply those values.
    /// This is the single source of truth for what "an analyzer rerun
    /// at this manifest" hashes to: a buggy caller cannot stamp a
    /// stale revision or a wrong pool group.
    ///
    /// Returns:
    /// * `Ok(Some(job))` when a fresh row is queued.
    /// * `Ok(None)` when the analyzer is not in
    ///   [`expected_analyzers_for_manifest`] for this manifest (the
    ///   linked-in registry has no producer for that id — typed skip,
    ///   not a failure), OR when the de-dup / tracked-keys gate
    ///   coalesces the request away.
    ///
    /// # Errors
    /// SQLite errors from the underlying write or
    /// `expected_analyzers_for_manifest` query.
    pub fn enqueue_analyzer_run(
        &self,
        request: EnqueueAnalyzerRun<'_>,
    ) -> Result<Option<QueuedAnalyzerJob>> {
        let EnqueueAnalyzerRun {
            conn,
            alias,
            repo_hash,
            repo_root,
            manifest_id,
            analyzer_id,
            now_ns,
        } = request;

        // Single source of truth: find the linked-in analyzer for this
        // manifest by id. If absent (e.g. an obsolete analyzer_id from
        // a build the persisted row remembers but this build has
        // dropped), skip cleanly rather than stamping a synthesized
        // row.
        let analyzers = expected_analyzers_for_manifest(conn, manifest_id)?;
        let Some(analyzer) = analyzers.iter().find(|a| a.id() == analyzer_id) else {
            return Ok(None);
        };
        let (analyzer_revision, cfg, pool_group) =
            crate::workspace_analyzer::compute_analyzer_run_inputs(analyzer.as_ref(), repo_root);
        // Daemon-global monotonic allocator so ids never collide
        // across stores. `_at_least` folds the `now_ns` floor into
        // the allocator's CAS so the counter advances past the
        // returned value even when the floor exceeded the current
        // counter — a plain `allocate_job_id().max(floor)` would
        // return `floor` twice on back-to-back calls.
        let job_id = self.allocate_job_id_at_least(now_ns)?;

        self.queue_analyzer_run(QueueAnalyzerRun {
            conn,
            alias,
            repo_hash,
            repo_root,
            manifest_id,
            now_ns,
            job_id,
            analyzer_id,
            analyzer_revision,
            config_hash: &cfg,
            pool_group,
        })
    }

    pub fn enqueue_reindex(&self, request: EnqueueReindex<'_>) -> Result<Vec<QueuedAnalyzerJob>> {
        let EnqueueReindex {
            conn,
            alias,
            repo_hash,
            repo_root,
            manifest_id,
            entries,
            now_ns,
        } = request;
        // Reserve a contiguous run of ids from the daemon-global
        // allocator, one per expected analyzer. Same rationale as
        // `enqueue_analyzer_run`: per-store allocation would reissue
        // ids already active in other stores. `_at_least` folds the
        // `now_ns` floor into the CAS so the allocator's counter
        // always exceeds the ids we just handed out.
        let analyzers = expected_analyzers_for_manifest(conn, manifest_id)?;
        let mut jobs = Vec::new();
        let first_id = self.allocate_job_id_range_at_least(analyzers.len(), now_ns)?;
        for (job_id, analyzer) in (first_id..).zip(analyzers) {
            let analyzer_id = analyzer.id();
            let analyzer_revision = analyzer.revision();
            let pool_group = analyzer.pool_group();
            let cfg = config_hash(repo_root, analyzer.config_paths());
            if let Some(job) = self.queue_analyzer_run(QueueAnalyzerRun {
                conn,
                alias,
                repo_hash,
                repo_root,
                manifest_id,
                now_ns,
                job_id,
                analyzer_id,
                analyzer_revision,
                config_hash: &cfg,
                pool_group,
            })? {
                jobs.push(job);
            }
        }

        if entries.is_empty() {
            return Ok(jobs);
        }
        Ok(jobs)
    }

    /// **Compat helper — not a production entry point.** Since
    /// PR3 Phase 3, repo-level reindex intent (watcher events,
    /// manual reindex, parser-revision drift) records durable
    /// state through [`crate::reconcile::RepoReconcileManager`]
    /// and the worker executes the register hot path
    /// asynchronously. This method still exists so
    /// [`crate::workspace_analyzer::staleness::check_revision_staleness_and_enqueue`]
    /// can fall back inline when it is called without a reconcile
    /// driver (test / degraded startup only). Production wiring
    /// always passes `Some(reconcile)`, so this method's only
    /// live caller path is the `reconcile.is_none()` gate.
    ///
    /// Original behaviour: look the alias up in the registry,
    /// open its CAS store, walk the register hot path with the
    /// dedup gate **off** (= analyzers always re-queued), return
    /// a structured outcome. All routing identity (`repo_hash`,
    /// `repo_root`, `manifest_id`, `entries`, `now_ns`) is derived
    /// inside this method from the registry + clock; a buggy
    /// caller cannot hand-stamp the wrong store path or a stale
    /// timestamp.
    ///
    /// # Errors
    /// * [`Error::RepoNotFound`] if `alias` is not registered.
    /// * SQLite / IO errors from the registry, store open, git
    ///   invocation, or the analyzer enqueue path bubble up
    ///   unchanged.
    pub(crate) fn enqueue_full_repo_reindex(
        &self,
        alias: &str,
        reason: ReindexReason,
    ) -> Result<FullReindexEnqueueOutcome> {
        let entry = {
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            cas_registry::lookup_by_alias(&index, alias)?
        };
        let entry = entry.ok_or_else(|| Error::RepoNotFound {
            alias: alias.to_string(),
        })?;

        let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
        let mut conn = cas_store::open(&store_path)?;
        let repo_root = PathBuf::from(&entry.root_path);

        let outcome = crate::register::register_repo_force_analyzers_enqueue(
            &mut conn,
            &entry.alias,
            &entry.repo_hash,
            &repo_root,
            now_ns(),
            self,
        )?;

        Ok(FullReindexEnqueueOutcome {
            alias: entry.alias.clone(),
            repo_hash: entry.repo_hash.clone(),
            reason,
            manifest_id: outcome.tentative_manifest,
            blobs_parsed: outcome.blobs_parsed,
            jobs_enqueued: outcome.analyzer_jobs.len(),
            skip_analyzers_for_unchanged_manifest: outcome.skip_analyzers_for_unchanged_manifest,
        })
    }

    fn queue_analyzer_run(
        &self,
        request: QueueAnalyzerRun<'_>,
    ) -> Result<Option<QueuedAnalyzerJob>> {
        let QueueAnalyzerRun {
            conn,
            alias,
            repo_hash,
            repo_root,
            manifest_id,
            now_ns,
            job_id,
            analyzer_id,
            analyzer_revision,
            config_hash,
            pool_group,
        } = request;
        let key = JobKey {
            repo_hash: repo_hash.to_string(),
            manifest_id,
            analyzer_id: analyzer_id.to_string(),
        };
        let queued = self.tracked_keys.reserve_after(key.clone(), || {
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id,
                    analyzer_revision,
                    config_hash,
                    status: RunStatus::Queued,
                    started_at_ns: now_ns,
                    finished_at_ns: now_ns,
                    error: None,
                    job_id: Some(job_id),
                },
            )?;
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET cancel_requested = 0
                 WHERE manifest_id = ?1 AND analyzer_id = ?2",
                params![manifest_id.0, analyzer_id],
            )?;
            Ok(())
        })?;
        if !queued {
            debug!(
                analyzer_id,
                job_id,
                manifest_id = manifest_id.0,
                "coalesced duplicate analyzer job before updating current run row"
            );
            return Ok(None);
        }

        self.runtime_metrics
            .mark_enqueued(job_id, pool_group, now_ns);
        let enqueued = self.enqueue_memory(Job {
            id: job_id,
            alias: alias.to_string(),
            repo_hash: repo_hash.to_string(),
            store_path: self.cas_data_dir.store_db_path(repo_hash),
            repo_root: repo_root.to_path_buf(),
            manifest_id,
            analyzer_id: analyzer_id.to_string(),
        });
        if !enqueued {
            self.tracked_keys.release(&key);
        }
        Ok(enqueued.then(|| QueuedAnalyzerJob {
            job_id,
            analyzer_id: analyzer_id.to_string(),
            state: RunStatus::Queued.as_str().to_string(),
        }))
    }

    pub(crate) fn jobs(
        &self,
        alias_filter: Option<&str>,
        state_filter: Option<RunStatus>,
        options: JobListOptions,
    ) -> Result<Vec<JobSnapshot>> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let mut out = Vec::new();
        for entry in cas_registry::list_all(&index)? {
            if let Some(alias) = alias_filter
                && entry.alias != alias
            {
                continue;
            }
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            let mut rows = if options.include_all {
                collect_all_job_rows(&conn, &entry.alias)?
            } else {
                collect_current_job_rows(&conn, &entry.alias)?
            };
            let observed_at_ns = now_ns();
            for row in &mut rows {
                self.runtime_metrics.decorate(row, observed_at_ns);
            }
            if !options.include_all {
                rows = latest_default_job_rows(rows);
            }
            out.extend(rows.into_iter().filter(|row| {
                state_filter
                    .map(|filter| row.state == filter.as_str())
                    .unwrap_or(true)
            }));
        }
        out.sort_by_key(|job| std::cmp::Reverse(job.job_id));
        if let Some(limit) = options.limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    pub fn prune_jobs(&self, repo_filter: Option<&str>, dry_run: bool) -> Result<JobsPruneSummary> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let entries = match repo_filter {
            Some(alias) => {
                let entry = cas_registry::lookup_by_alias(&index, alias)?.ok_or_else(|| {
                    Error::RepoNotFound {
                        alias: alias.into(),
                    }
                })?;
                vec![entry]
            }
            None => cas_registry::list_all(&index)?,
        };

        let mut repos = Vec::with_capacity(entries.len());
        let mut total_deleted_runs = 0_u64;
        let mut total_deleted_index_entries = 0_u64;
        for entry in entries {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let mut conn = cas_store::open(&store_path)?;
            let active_orphans = count_orphan_active_runs(&conn)?;
            if active_orphans > 0 {
                warn!(
                    alias = %entry.alias,
                    active_orphans,
                    "jobs prune preserved active jobs for manifests no current anchor references"
                );
            }
            let (deleted_runs_count, deleted_index_entries_count) =
                prune_jobs_in_store(&mut conn, &self.job_index, dry_run)?;
            total_deleted_runs = total_deleted_runs.saturating_add(deleted_runs_count);
            total_deleted_index_entries =
                total_deleted_index_entries.saturating_add(deleted_index_entries_count);
            repos.push(JobsPruneRepoSummary {
                alias: entry.alias,
                deleted_runs_count,
                deleted_index_entries_count,
            });
        }
        Ok(JobsPruneSummary {
            repos,
            total_deleted_runs,
            total_deleted_index_entries,
        })
    }

    pub fn cancel(&self, job_id: JobId) -> Result<CancelResult> {
        // Check the durable ambiguous-id tombstone *first*. Callers
        // holding a `JobId` that was retired by a prior
        // `restore_from_db` collision-recycle must be rejected
        // before any `JobIndex` or per-store scan, otherwise a
        // surviving row from a partial rewrite would silently
        // satisfy the cancel and target the wrong job. The tombstone
        // is authoritative — ambiguous ids never get reissued.
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        if cas_registry::is_ambiguous_job_id(&index, job_id)? {
            return Err(Error::InvalidArgument(format!("unknown job id: {job_id}")));
        }
        if let Some(locator) = self.job_index.get(job_id) {
            let store_path = self.cas_data_dir.store_db_path(&locator.repo_hash);
            let conn = cas_store::open(&store_path)?;
            if let Some(result) = self.cancel_in_store(&conn, job_id)? {
                return Ok(result);
            }
            self.job_index.remove(job_id);
            debug!(
                alias = %locator.alias,
                job_id,
                "job index entry was stale; falling back to scan"
            );
        }
        for entry in cas_registry::list_all(&index)? {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            if let Some(result) = self.cancel_in_store(&conn, job_id)? {
                self.job_index
                    .insert(job_id, &entry.alias, &entry.repo_hash);
                return Ok(result);
            }
        }
        Err(Error::InvalidArgument(format!("unknown job id: {job_id}")))
    }

    fn cancel_in_store(&self, conn: &Connection, job_id: JobId) -> Result<Option<CancelResult>> {
        let row: Option<(String, i64, String)> = conn
            .query_row(
                "SELECT status, manifest_id, analyzer_id
                 FROM workspace_analysis_runs WHERE job_id = ?1",
                [job_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((state, manifest_id, analyzer_id)) = row else {
            return Ok(None);
        };
        let result = match RunStatus::from_str(&state) {
            Some(RunStatus::Queued) => {
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET status = 'cancelled', cancel_requested = 1, finished_at_ns = ?1
                     WHERE job_id = ?2",
                    params![now_ns(), job_id],
                )?;
                self.runtime_metrics
                    .mark_finished(job_id, RunStatus::Cancelled.as_str());
                self.notify_cancelled_job(job_id);
                CancelResult {
                    cancelled: true,
                    reason: "queued job cancelled".into(),
                }
            }
            Some(RunStatus::Running) => {
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET cancel_requested = 1
                     WHERE manifest_id = ?1 AND analyzer_id = ?2",
                    params![manifest_id, analyzer_id],
                )?;
                CancelResult {
                    cancelled: false,
                    reason:
                        "running job marked for cancellation; analyzer will finish current request"
                            .into(),
                }
            }
            Some(state) if state.is_terminal() => CancelResult {
                cancelled: false,
                reason: format!("job already {}", state.as_str()),
            },
            _ => {
                return Ok(None);
            }
        };
        Ok(Some(result))
    }

    pub async fn shutdown(&self, drain_timeout: Duration) {
        test_observe_job_manager_shutdown();
        {
            let mut sender = self
                .scheduler_sender
                .lock()
                .expect("job scheduler sender lock poisoned");
            if let Some(sender) = sender.take() {
                let _ = sender.send(SchedulerMsg::Shutdown);
            }
        }
        let scheduler = self
            .scheduler
            .lock()
            .expect("job scheduler lock poisoned")
            .take();
        if let Some(handle) = scheduler {
            let _ = tokio::time::timeout(drain_timeout, handle).await;
        }
        {
            let mut sender = self
                .worker_sender
                .lock()
                .expect("job worker sender lock poisoned");
            sender.take();
        }
        let handles = {
            let mut workers = self.workers.lock().expect("job worker lock poisoned");
            std::mem::take(&mut *workers)
        };
        let _ = tokio::time::timeout(drain_timeout, async {
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
    }

    fn enqueue_memory(&self, job: Job) -> bool {
        let sender = self
            .scheduler_sender
            .lock()
            .expect("job scheduler sender lock poisoned");
        match sender.as_ref() {
            Some(sender) => {
                let job_id = job.id;
                let alias = job.alias.clone();
                let repo_hash = job.repo_hash.clone();
                if let Err(err) = sender.send(SchedulerMsg::Enqueue(job)) {
                    warn!(error = %err, "failed to enqueue analyzer job");
                    false
                } else {
                    self.job_index.insert(job_id, &alias, &repo_hash);
                    true
                }
            }
            None => {
                warn!("job manager is shutting down; analyzer job was not enqueued");
                false
            }
        }
    }

    async fn worker_loop(self: Arc<Self>) {
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

    fn notify_cancelled_job(&self, job_id: JobId) {
        let sender = self
            .scheduler_sender
            .lock()
            .expect("job scheduler sender lock poisoned");
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(SchedulerMsg::Cancel(job_id));
        }
    }
}

async fn scheduler_loop(
    mut receiver: mpsc::UnboundedReceiver<SchedulerMsg>,
    worker_tx: mpsc::UnboundedSender<DispatchJob>,
    worker_capacity: usize,
    runtime_metrics: JobRuntimeMetricsStore,
    tracked_keys: TrackedJobKeys,
) {
    let mut scheduler =
        JobScheduler::new(worker_tx, worker_capacity, runtime_metrics, tracked_keys);
    while let Some(message) = receiver.recv().await {
        match message {
            SchedulerMsg::Enqueue(job) => scheduler.enqueue(job),
            SchedulerMsg::WorkerFinished {
                job_id,
                pool_group,
                key,
            } => scheduler.worker_finished(job_id, pool_group, key),
            SchedulerMsg::Cancel(job_id) => scheduler.cancel(job_id),
            SchedulerMsg::Shutdown => break,
        }
        scheduler.drain_runnable();
    }
}

struct JobScheduler {
    worker_tx: mpsc::UnboundedSender<DispatchJob>,
    worker_capacity: usize,
    runtime_metrics: JobRuntimeMetricsStore,
    lanes: HashMap<GroupLane, VecDeque<Job>>,
    ready_order: VecDeque<GroupLane>,
    active_groups: HashSet<&'static str>,
    active_workers: usize,
    tracked_keys: TrackedJobKeys,
    cancelled_jobs: HashSet<JobId>,
}

impl JobScheduler {
    fn new(
        worker_tx: mpsc::UnboundedSender<DispatchJob>,
        worker_capacity: usize,
        runtime_metrics: JobRuntimeMetricsStore,
        tracked_keys: TrackedJobKeys,
    ) -> Self {
        Self {
            worker_tx,
            worker_capacity,
            runtime_metrics,
            lanes: HashMap::new(),
            ready_order: VecDeque::new(),
            active_groups: HashSet::new(),
            active_workers: 0,
            tracked_keys,
            cancelled_jobs: HashSet::new(),
        }
    }

    fn enqueue(&mut self, job: Job) {
        let lane = self.lane_for(&job);
        if let GroupLane::Pooled(group) = lane
            && self.active_groups.contains(group)
        {
            self.runtime_metrics.mark_waiting_pool_group(job.id, group);
        }
        let was_empty = self.lanes.get(&lane).is_none_or(VecDeque::is_empty);
        self.lanes.entry(lane).or_default().push_back(job);
        if was_empty && !self.ready_order.contains(&lane) {
            self.ready_order.push_back(lane);
        }
    }

    fn cancel(&mut self, job_id: JobId) {
        self.cancelled_jobs.insert(job_id);
        self.remove_pending_job_id(job_id);
    }

    fn worker_finished(&mut self, _job_id: JobId, pool_group: Option<&'static str>, _key: JobKey) {
        self.active_workers = self.active_workers.saturating_sub(1);
        if let Some(group) = pool_group {
            self.active_groups.remove(group);
            let lane = GroupLane::Pooled(group);
            if self.lanes.get(&lane).is_some_and(|jobs| !jobs.is_empty())
                && !self.ready_order.contains(&lane)
            {
                self.ready_order.push_back(lane);
            }
        }
        self.cancelled_jobs.remove(&_job_id);
        self.tracked_keys.release(&_key);
    }

    fn drain_runnable(&mut self) {
        while self.active_workers < self.worker_capacity {
            let Some(lane) = self.ready_order.pop_front() else {
                break;
            };
            if matches!(lane, GroupLane::Pooled(group) if self.active_groups.contains(group)) {
                self.ready_order.push_back(lane);
                if self.ready_order.iter().all(|candidate| {
                    matches!(candidate, GroupLane::Pooled(group) if self.active_groups.contains(group))
                }) {
                    break;
                }
                continue;
            }
            let Some(job) = self.pop_next_job(lane) else {
                continue;
            };
            let pool_group = match lane {
                GroupLane::Pooled(group) => {
                    self.active_groups.insert(group);
                    Some(group)
                }
                GroupLane::Unpooled => None,
            };
            let key = JobKey::from_job(&job);
            self.runtime_metrics.mark_running(job.id);
            if self
                .worker_tx
                .send(DispatchJob {
                    job,
                    pool_group,
                    key: key.clone(),
                })
                .is_err()
            {
                if let Some(group) = pool_group {
                    self.active_groups.remove(group);
                }
                self.tracked_keys.release(&key);
                break;
            }
            self.active_workers += 1;
            if self.lanes.get(&lane).is_some_and(|jobs| !jobs.is_empty())
                && !self.ready_order.contains(&lane)
            {
                self.ready_order.push_back(lane);
            }
        }
    }

    fn pop_next_job(&mut self, lane: GroupLane) -> Option<Job> {
        loop {
            let jobs = self.lanes.get_mut(&lane)?;
            let job = jobs.pop_front()?;
            if self.cancelled_jobs.remove(&job.id) {
                self.tracked_keys.release(&JobKey::from_job(&job));
                continue;
            }
            return Some(job);
        }
    }

    fn remove_pending_job_id(&mut self, job_id: JobId) {
        for jobs in self.lanes.values_mut() {
            if let Some(index) = jobs.iter().position(|job| job.id == job_id) {
                if let Some(job) = jobs.remove(index) {
                    self.tracked_keys.release(&JobKey::from_job(&job));
                }
                return;
            }
        }
    }

    fn lane_for(&self, job: &Job) -> GroupLane {
        #[cfg(test)]
        if let Some(group) = test_pool_group_for_analyzer_id(&job.analyzer_id) {
            return group.map_or(GroupLane::Unpooled, GroupLane::Pooled);
        }

        match pool_group_for_analyzer_id(&job.analyzer_id) {
            Ok(Some(group)) => GroupLane::Pooled(group),
            Ok(None) => GroupLane::Unpooled,
            Err(err) => {
                warn!(
                    analyzer_id = %job.analyzer_id,
                    error = %err,
                    "unknown analyzer in scheduler; dispatching without pool group"
                );
                GroupLane::Unpooled
            }
        }
    }
}

#[cfg(test)]
fn test_pool_group_for_analyzer_id(analyzer_id: &str) -> Option<Option<&'static str>> {
    match analyzer_id {
        "clangd-c-lsp" | "clangd-cpp-lsp" | "clangd-objc-lsp" => Some(Some("clangd-lsp")),
        "typescript-language-server-ts-lsp"
        | "typescript-language-server-js-lsp"
        | "typescript-language-server-tsx-lsp" => Some(Some("typescript-language-server-lsp")),
        "pyright-lsp" | "gopls-lsp" | "ruby-lsp" => Some(None),
        _ => None,
    }
}

fn collect_all_job_rows(conn: &Connection, alias: &str) -> Result<Vec<JobSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns, error
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
         ORDER BY job_id DESC",
    )?;
    collect_job_rows(&mut stmt, alias)
}

fn collect_current_job_rows(conn: &Connection, alias: &str) -> Result<Vec<JobSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns, error
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
           AND manifest_id IN (SELECT DISTINCT manifest_id FROM anchors)
         ORDER BY job_id DESC",
    )?;
    collect_job_rows(&mut stmt, alias)
}

const ORPHAN_TERMINAL_RUNS_WHERE: &str = "
    manifest_id NOT IN (SELECT DISTINCT manifest_id FROM anchors)
    AND status IN ('succeeded', 'failed', 'cancelled', 'skipped', 'timed_out')
";

fn prune_jobs_in_store(
    conn: &mut Connection,
    job_index: &JobIndex,
    dry_run: bool,
) -> Result<(u64, u64)> {
    let job_ids = orphan_terminal_job_ids(conn)?;
    let deleted_runs_count = count_orphan_terminal_runs(conn)?;
    if dry_run {
        return Ok((deleted_runs_count, job_index.count_present(&job_ids)));
    }

    let tx = conn.transaction()?;
    let deleted = tx.execute(
        &format!("DELETE FROM workspace_analysis_runs WHERE {ORPHAN_TERMINAL_RUNS_WHERE}"),
        [],
    )?;
    tx.commit()?;

    let deleted_runs_count = u64::try_from(deleted).unwrap_or(u64::MAX);
    let deleted_index_entries_count = job_index.remove_many(&job_ids);
    Ok((deleted_runs_count, deleted_index_entries_count))
}

fn count_orphan_terminal_runs(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM workspace_analysis_runs WHERE {ORPHAN_TERMINAL_RUNS_WHERE}"),
        [],
        |r| r.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(0))
}

fn orphan_terminal_job_ids(conn: &Connection) -> Result<Vec<JobId>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT job_id FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL AND {ORPHAN_TERMINAL_RUNS_WHERE}"
    ))?;
    let job_ids = stmt
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(job_ids)
}

fn count_orphan_active_runs(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM workspace_analysis_runs
         WHERE manifest_id NOT IN (SELECT DISTINCT manifest_id FROM anchors)
           AND status IN ('queued', 'running')",
        [],
        |r| r.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(0))
}

fn collect_job_rows(stmt: &mut rusqlite::Statement<'_>, alias: &str) -> Result<Vec<JobSnapshot>> {
    let rows = stmt
        .query_map([], |r| {
            Ok(JobSnapshot {
                job_id: r.get(0)?,
                alias: alias.to_string(),
                analyzer_id: r.get(1)?,
                state: r.get(2)?,
                created_at: r.get(3)?,
                started_at: None,
                finished_at: r.get(4)?,
                error: r.get(5)?,
                pool_group: None,
                scheduler_state: None,
                enqueued_at: None,
                run_started_at: None,
                queued_ms: None,
                pool_wait_ms: None,
                run_ms: None,
                progress_ticks: None,
                last_progress_at: None,
                progress_per_minute: None,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn latest_default_job_rows(rows: Vec<JobSnapshot>) -> Vec<JobSnapshot> {
    let mut seen_terminal = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        match RunStatus::from_str(&row.state) {
            Some(RunStatus::Queued | RunStatus::Running) => out.push(row),
            Some(state) if state.is_terminal() => {
                if seen_terminal.insert(row.analyzer_id.clone()) {
                    out.push(row);
                }
            }
            _ => out.push(row),
        }
    }
    out
}

#[cfg(test)]
fn test_observe_job_manager_shutdown() {
    if let Some(observer) = JOB_MANAGER_SHUTDOWN_OBSERVER
        .lock()
        .expect("job manager shutdown observer poisoned")
        .as_ref()
    {
        observer();
    }
}

#[cfg(not(test))]
fn test_observe_job_manager_shutdown() {}

#[cfg(test)]
pub(crate) static JOB_MANAGER_SHUTDOWN_OBSERVER: Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    Mutex::new(None);

fn pool_group_for_analyzer_id(analyzer_id: &str) -> Result<Option<&'static str>> {
    all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == analyzer_id)
        .map(|a| a.pool_group())
        .ok_or_else(|| Error::InvalidArgument(format!("unknown analyzer: {analyzer_id}")))
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

impl JobManager {
    /// Borrow the [`CasDataDir`] this manager was constructed with.
    /// The revision-staleness scanner needs it to enumerate stores
    /// without re-threading the path through Daemon::run.
    #[must_use]
    pub fn cas_data_dir(&self) -> &Arc<CasDataDir> {
        &self.cas_data_dir
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    )
    .unwrap_or(i64::MAX)
}

fn duration_ms(start_ns: i64, end_ns: i64) -> Option<u64> {
    let delta = end_ns.checked_sub(start_ns)?;
    u64::try_from(delta / 1_000_000).ok()
}

fn worker_concurrency() -> usize {
    let env_value = std::env::var("CAIRN_WORKER_CONCURRENCY").ok();
    worker_concurrency_from_env_value(env_value.as_deref(), fallback_worker_concurrency)
}

fn worker_concurrency_from_env_value(
    env_value: Option<&str>,
    fallback: impl FnOnce() -> usize,
) -> usize {
    if let Some(value) = env_value {
        match value.parse::<usize>() {
            Ok(parsed) if parsed > 0 => {
                // Keep env overrides within the same ceiling as automatic sizing to bound worker fan-out.
                let clamped = parsed.clamp(1, MAX_WORKER_CONCURRENCY);
                if clamped != parsed {
                    warn!(
                        requested = parsed,
                        clamped, "CAIRN_WORKER_CONCURRENCY clamped"
                    );
                }
                return clamped;
            }
            _ => {}
        }
    }
    fallback()
}

fn fallback_worker_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .map(|n| (n / 2).clamp(1, MAX_WORKER_CONCURRENCY))
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::subscriber::Interest;
    use tracing::{Event, Level, Metadata, Subscriber};

    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::manifest::ManifestId;
    use crate::paths::CasDataDir;
    use crate::workspace_analyzer::RunStatus;
    use crate::workspace_analyzer::expected_analyzers_for_manifest;

    use super::{
        DispatchJob, Job, JobId, JobIndex, JobListOptions, JobManager, JobRuntimeMetricsStore,
        JobScheduler, JobSnapshot, MAX_WORKER_CONCURRENCY, QueueAnalyzerRun, TrackedJobKeys,
        latest_default_job_rows, now_ns, worker_concurrency_from_env_value,
    };

    #[derive(Default)]
    struct WarnCapture {
        fields: Mutex<Vec<Vec<(String, String)>>>,
    }

    impl WarnCapture {
        fn fields(&self) -> Vec<Vec<(String, String)>> {
            self.fields.lock().expect("warn capture poisoned").clone()
        }
    }

    struct WarnSubscriber(Arc<WarnCapture>);

    impl Subscriber for WarnSubscriber {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= &Level::WARN
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            if event.metadata().level() != &Level::WARN {
                return;
            }
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            self.0
                .fields
                .lock()
                .expect("warn capture poisoned")
                .push(visitor.fields);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}

        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if self.enabled(metadata) {
                Interest::always()
            } else {
                Interest::never()
            }
        }
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: Vec<(String, String)>,
    }

    impl FieldVisitor {
        fn push(&mut self, field: &Field, value: impl ToString) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.push(field, format!("{value:?}"));
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.push(field, value);
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.push(field, value);
        }
    }

    fn assert_no_worker_concurrency_warn(capture: &WarnCapture) {
        assert!(
            capture.fields().is_empty(),
            "unexpected worker concurrency warn"
        );
    }

    fn assert_worker_concurrency_warn(capture: &WarnCapture, requested: usize, clamped: usize) {
        let events = capture.fields();
        assert_eq!(events.len(), 1, "expected one worker concurrency warn");
        assert!(
            events[0]
                .iter()
                .any(|(name, value)| name == "requested" && value == &requested.to_string()),
            "warn did not include requested={requested}: {:?}",
            events[0]
        );
        assert!(
            events[0]
                .iter()
                .any(|(name, value)| name == "clamped" && value == &clamped.to_string()),
            "warn did not include clamped={clamped}: {:?}",
            events[0]
        );
    }

    fn with_warn_capture(f: impl FnOnce(&WarnCapture)) {
        static DISPATCH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        let _guard = DISPATCH_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("tracing dispatch lock poisoned");
        let capture = Arc::new(WarnCapture::default());
        tracing::dispatcher::with_default(
            &tracing::Dispatch::new(WarnSubscriber(Arc::clone(&capture))),
            || {
                f(&capture);
            },
        );
    }

    #[test]
    fn worker_concurrency_uses_env_value_within_bounds() {
        with_warn_capture(|capture| {
            assert_eq!(worker_concurrency_from_env_value(Some("4"), || 2), 4);
            assert_no_worker_concurrency_warn(capture);
        });
    }

    #[test]
    fn worker_concurrency_clamps_env_value_above_ceiling_and_warns() {
        with_warn_capture(|capture| {
            assert_eq!(
                worker_concurrency_from_env_value(Some("128"), || 2),
                MAX_WORKER_CONCURRENCY
            );
            assert_worker_concurrency_warn(capture, 128, MAX_WORKER_CONCURRENCY);
        });
    }

    #[test]
    fn worker_concurrency_falls_back_for_zero_negative_or_invalid_env() {
        for value in ["0", "-4", "not-a-number"] {
            with_warn_capture(|capture| {
                assert_eq!(worker_concurrency_from_env_value(Some(value), || 3), 3);
                assert_no_worker_concurrency_warn(capture);
            });
        }
    }

    #[test]
    fn default_job_rows_keep_active_and_latest_terminal_per_analyzer() {
        let rows = vec![
            job(7, "rust-analyzer", "running"),
            job(6, "pyright", "succeeded"),
            job(5, "pyright", "failed"),
            job(4, "gopls", "queued"),
            job(3, "gopls", "succeeded"),
            job(2, "rust-analyzer", "succeeded"),
            job(1, "unknown", "mystery"),
        ];

        let filtered = latest_default_job_rows(rows);
        let ids = filtered.iter().map(|job| job.job_id).collect::<Vec<_>>();

        assert_eq!(ids, vec![7, 6, 4, 3, 2, 1]);
    }

    #[test]
    fn scheduler_serializes_same_pool_group() {
        let (mut scheduler, mut rx) = test_scheduler(2);
        scheduler.enqueue(test_job(1, 1, "clangd-c-lsp"));
        scheduler.enqueue(test_job(2, 2, "clangd-cpp-lsp"));
        scheduler.drain_runnable();

        let first = rx.try_recv().expect("first same-group job dispatched");
        assert_eq!(first.job.id, 1);
        assert!(rx.try_recv().is_err(), "second same-group job ran early");
        assert!(scheduler.active_groups.contains("clangd-lsp"));
    }

    #[test]
    fn scheduler_dispatches_different_group_while_one_waits() {
        let (mut scheduler, mut rx) = test_scheduler(2);
        scheduler.enqueue(test_job(1, 1, "clangd-c-lsp"));
        scheduler.enqueue(test_job(2, 2, "clangd-cpp-lsp"));
        scheduler.enqueue(test_job(3, 3, "typescript-language-server-ts-lsp"));
        scheduler.drain_runnable();

        let dispatched = drain_dispatched(&mut rx);
        let ids = dispatched.iter().map(|job| job.job.id).collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn scheduler_parallelizes_unpooled_jobs_up_to_capacity() {
        let (mut scheduler, mut rx) = test_scheduler(2);
        scheduler.enqueue(test_job(1, 1, "pyright-lsp"));
        scheduler.enqueue(test_job(2, 2, "ruby-lsp"));
        scheduler.enqueue(test_job(3, 3, "gopls"));
        scheduler.drain_runnable();

        let dispatched = drain_dispatched(&mut rx);
        assert_eq!(dispatched.len(), 2);
        assert_eq!(scheduler.active_workers, 2);
        assert!(scheduler.active_workers <= 2);
    }

    #[test]
    fn scheduler_dispatches_waiting_group_after_completion() {
        let (mut scheduler, mut rx) = test_scheduler(1);
        scheduler.enqueue(test_job(1, 1, "clangd-c-lsp"));
        scheduler.enqueue(test_job(2, 2, "clangd-cpp-lsp"));
        scheduler.drain_runnable();
        let first = rx.try_recv().expect("first job dispatched");

        scheduler.worker_finished(first.job.id, first.pool_group, first.key);
        scheduler.drain_runnable();

        let second = rx
            .try_recv()
            .expect("same group job dispatched after release");
        assert_eq!(second.job.id, 2);
    }

    #[test]
    fn scheduler_cancel_drops_pending_job_before_dispatch() {
        let (mut scheduler, mut rx) = test_scheduler(1);
        scheduler.enqueue(test_job(1, 1, "pyright-lsp"));
        scheduler.cancel(1);
        scheduler.drain_runnable();

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn duplicate_enqueue_does_not_overwrite_current_job_row() {
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(Arc::clone(&cas_data_dir));
        let repo_hash = "repo-hash";
        let manifest_id = ManifestId(1);
        {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, "repo", repo.path().to_str().unwrap(), repo_hash, 1).unwrap();
            tx.commit().unwrap();
        }
        let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (?1, 'tentative', 0)",
            [manifest_id.0],
        )
        .unwrap();

        let first = manager
            .queue_analyzer_run(test_queue_request(
                &mut conn,
                repo.path(),
                repo_hash,
                manifest_id,
                1,
            ))
            .unwrap()
            .expect("first job should queue");
        assert_eq!(first.job_id, 1);

        let queued_duplicate = manager
            .queue_analyzer_run(test_queue_request(
                &mut conn,
                repo.path(),
                repo_hash,
                manifest_id,
                2,
            ))
            .unwrap();
        assert!(
            queued_duplicate.is_none(),
            "queued duplicate should coalesce before DB write"
        );
        let queued_row: (i64, String) = conn
            .query_row(
                "SELECT job_id, status
                 FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                [manifest_id.0],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(queued_row, (1, "queued".into()));

        conn.execute(
            "UPDATE workspace_analysis_runs
             SET status = 'running'
             WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
            [manifest_id.0],
        )
        .unwrap();

        let duplicate = manager
            .queue_analyzer_run(test_queue_request(
                &mut conn,
                repo.path(),
                repo_hash,
                manifest_id,
                3,
            ))
            .unwrap();
        assert!(duplicate.is_none(), "active duplicate should coalesce");
        let row: (i64, String, i64) = conn
            .query_row(
                "SELECT job_id, status, cancel_requested
                 FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                [manifest_id.0],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, (1, "running".into(), 0));

        let listed = manager
            .jobs(
                Some("repo"),
                None,
                JobListOptions {
                    include_all: true,
                    limit: None,
                },
            )
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].job_id, 1);

        let cancelled = manager.cancel(1).unwrap();
        assert!(!cancelled.cancelled);
        assert!(cancelled.reason.contains("running job marked"));
        let cancel_requested: i64 = conn
            .query_row(
                "SELECT cancel_requested FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                [manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cancel_requested, 1);
        assert!(
            manager.cancel(2).is_err(),
            "coalesced queued job id must not exist"
        );
        assert!(
            manager.cancel(3).is_err(),
            "coalesced active job id must not exist"
        );
    }

    #[test]
    fn enqueue_reindex_queues_only_expected_manifest_analyzers() {
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(Arc::clone(&cas_data_dir));
        let repo_hash = "repo-hash";
        let manifest_id = ManifestId(1);
        let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
        insert_manifest(&conn, manifest_id.0);
        insert_manifest_parser(&conn, manifest_id, "src/fake.rs", "fake-sha", "fake-parser");
        insert_manifest_parser(
            &conn,
            manifest_id,
            "src/unknown.rs",
            "unknown-sha",
            "unknown-parser",
        );

        let expected_ids = expected_analyzers_for_manifest(&conn, manifest_id)
            .unwrap()
            .into_iter()
            .map(|analyzer| analyzer.id().to_string())
            .collect::<Vec<_>>();

        let jobs = manager
            .enqueue_reindex(super::EnqueueReindex {
                conn: &mut conn,
                alias: "repo",
                repo_hash,
                repo_root: repo.path(),
                manifest_id,
                entries: &[],
                now_ns: 1,
            })
            .unwrap();

        let queued_ids = jobs
            .iter()
            .map(|job| job.analyzer_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(queued_ids, expected_ids);
        assert_eq!(queued_ids, vec!["fake-workspace"]);

        let db_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM workspace_analysis_runs", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            db_count, 1,
            "reindex now creates jobs only for expected analyzers instead of recording no-match skips"
        );
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
    fn job_index_round_trips_and_removes_locator() {
        let index = JobIndex::default();
        index.insert(99, "repo", "hash");

        let locator = index.get(99).expect("job locator missing");
        assert_eq!(locator.alias, "repo");
        assert_eq!(locator.repo_hash, "hash");

        index.remove(99);
        assert!(index.get(99).is_none());
    }

    #[test]
    fn prune_jobs_removes_orphan_terminal_rows_and_job_index_entries() {
        let (_data, _repo, manager, conn) = prune_test_manager();
        insert_manifest(&conn, 1);
        insert_manifest(&conn, 2);
        insert_anchor(&conn, "HEAD", 1);
        insert_job_run(&conn, 1, "current-lsp", "succeeded", Some(12));
        insert_job_run(&conn, 2, "orphan-terminal-lsp", "succeeded", Some(10));
        insert_job_run(&conn, 2, "orphan-active-lsp", "running", Some(11));
        manager.job_index.insert(10, "repo", "repo-hash");
        manager.job_index.insert(11, "repo", "repo-hash");
        manager.job_index.insert(12, "repo", "repo-hash");

        let result = manager.prune_jobs(Some("repo"), false).unwrap();

        assert_eq!(result.total_deleted_runs, 1);
        assert_eq!(result.total_deleted_index_entries, 1);
        assert_eq!(result.repos[0].deleted_runs_count, 1);
        assert_eq!(result.repos[0].deleted_index_entries_count, 1);
        assert_eq!(
            count_runs(&conn, 2, "orphan-terminal-lsp"),
            0,
            "orphan terminal row should be pruned"
        );
        assert_eq!(
            count_runs(&conn, 2, "orphan-active-lsp"),
            1,
            "active orphan rows stay visible instead of being hidden by GC"
        );
        assert_eq!(
            count_runs(&conn, 1, "current-lsp"),
            1,
            "current-anchor terminal rows are retained"
        );
        assert!(manager.job_index.get(10).is_none());
        assert!(manager.job_index.get(11).is_some());
        assert!(manager.job_index.get(12).is_some());
    }

    #[test]
    fn prune_jobs_dry_run_counts_without_deleting_rows_or_index_entries() {
        let (_data, _repo, manager, conn) = prune_test_manager();
        insert_manifest(&conn, 1);
        insert_manifest(&conn, 2);
        insert_anchor(&conn, "HEAD", 1);
        insert_job_run(&conn, 2, "orphan-terminal-lsp", "failed", Some(20));
        manager.job_index.insert(20, "repo", "repo-hash");

        let result = manager.prune_jobs(None, true).unwrap();

        assert_eq!(result.total_deleted_runs, 1);
        assert_eq!(result.total_deleted_index_entries, 1);
        assert_eq!(count_runs(&conn, 2, "orphan-terminal-lsp"), 1);
        assert!(manager.job_index.get(20).is_some());
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

    fn job(job_id: i64, analyzer_id: &str, state: &str) -> JobSnapshot {
        JobSnapshot {
            job_id,
            alias: "repo".into(),
            analyzer_id: analyzer_id.into(),
            state: state.into(),
            created_at: job_id,
            started_at: None,
            finished_at: None,
            error: None,
            pool_group: None,
            scheduler_state: None,
            enqueued_at: None,
            run_started_at: None,
            queued_ms: None,
            pool_wait_ms: None,
            run_ms: None,
            progress_ticks: None,
            last_progress_at: None,
            progress_per_minute: None,
        }
    }

    fn test_scheduler(
        worker_capacity: usize,
    ) -> (JobScheduler, mpsc::UnboundedReceiver<DispatchJob>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            JobScheduler::new(
                tx,
                worker_capacity,
                JobRuntimeMetricsStore::default(),
                TrackedJobKeys::default(),
            ),
            rx,
        )
    }

    fn drain_dispatched(rx: &mut mpsc::UnboundedReceiver<DispatchJob>) -> Vec<DispatchJob> {
        let mut out = Vec::new();
        while let Ok(job) = rx.try_recv() {
            out.push(job);
        }
        out
    }

    fn test_job(id: JobId, manifest_id: i64, analyzer_id: &str) -> Job {
        Job {
            id,
            alias: "repo".into(),
            repo_hash: "repo-hash".into(),
            store_path: PathBuf::from("/tmp/store.db"),
            repo_root: PathBuf::from("/tmp/repo"),
            manifest_id: ManifestId(manifest_id),
            analyzer_id: analyzer_id.into(),
        }
    }

    /// `enqueue_analyzer_run` is the only public surface for a
    /// **targeted** single-`(manifest, analyzer)` rerun. The stamp
    /// (`analyzer_revision`, `config_hash`, `pool_group`, `job_id`) must
    /// be derived inside JobManager — callers do not supply it — and
    /// an unknown `analyzer_id` must return `Ok(None)` instead of
    /// stamping a synthesized row. The de-dup gate must also hold:
    /// a second call for the same `(manifest, analyzer)` while the
    /// first is still `queued` coalesces.
    #[test]
    fn enqueue_analyzer_run_stamps_from_registry_and_dedupes() {
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(Arc::clone(&cas_data_dir));
        let repo_hash = "repo-hash";
        let manifest_id = ManifestId(1);
        let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
        insert_manifest(&conn, manifest_id.0);
        insert_manifest_parser(&conn, manifest_id, "src/fake.rs", "fake-sha", "fake-parser");

        // Unknown analyzer id → typed skip, no row written.
        let skip = manager
            .enqueue_analyzer_run(super::EnqueueAnalyzerRun {
                conn: &mut conn,
                alias: "repo",
                repo_hash,
                repo_root: repo.path(),
                manifest_id,
                analyzer_id: "not-in-registry",
                now_ns: 1,
            })
            .unwrap();
        assert!(
            skip.is_none(),
            "unknown analyzer id must skip cleanly without stamping a row"
        );
        let unknown_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workspace_analysis_runs
                 WHERE analyzer_id = 'not-in-registry'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(unknown_rows, 0);

        // Known analyzer id (fake-workspace, registered via the
        // distributed_slice in workspace_analyzer/mod.rs test helpers)
        // → enqueue with the stamp derived from the analyzer instance,
        // not caller-supplied.
        let queued = manager
            .enqueue_analyzer_run(super::EnqueueAnalyzerRun {
                conn: &mut conn,
                alias: "repo",
                repo_hash,
                repo_root: repo.path(),
                manifest_id,
                analyzer_id: "fake-workspace",
                now_ns: 1,
            })
            .unwrap()
            .expect("first enqueue should queue");
        assert_eq!(queued.analyzer_id, "fake-workspace");

        // The stamped revision matches the linked-in registry value (7),
        // proving the API computed it internally rather than echoing a
        // caller value.
        let stamped_rev: i64 = conn
            .query_row(
                "SELECT analyzer_revision FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'fake-workspace'",
                [manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stamped_rev, 7);

        // Second call for the same (manifest, analyzer) coalesces while
        // the first is still queued — only one row total.
        let dup = manager
            .enqueue_analyzer_run(super::EnqueueAnalyzerRun {
                conn: &mut conn,
                alias: "repo",
                repo_hash,
                repo_root: repo.path(),
                manifest_id,
                analyzer_id: "fake-workspace",
                now_ns: 2,
            })
            .unwrap();
        assert!(
            dup.is_none(),
            "duplicate enqueue while queued should coalesce"
        );
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'fake-workspace'",
                [manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1);
    }

    fn test_queue_request<'a>(
        conn: &'a mut rusqlite::Connection,
        repo_root: &'a Path,
        repo_hash: &'a str,
        manifest_id: ManifestId,
        job_id: JobId,
    ) -> QueueAnalyzerRun<'a> {
        QueueAnalyzerRun {
            conn,
            alias: "repo",
            repo_hash,
            repo_root,
            manifest_id,
            now_ns: job_id,
            job_id,
            analyzer_id: "pyright-lsp",
            analyzer_revision: 1,
            config_hash: "cfg",
            pool_group: None,
        }
    }

    fn prune_test_manager() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        Arc<JobManager>,
        rusqlite::Connection,
    ) {
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, "repo", repo.path().to_str().unwrap(), "repo-hash", 1)
                .unwrap();
            tx.commit().unwrap();
        }
        let conn = cas_store::open(&cas_data_dir.store_db_path("repo-hash")).unwrap();
        let manager = JobManager::new(cas_data_dir);
        (data, repo, manager, conn)
    }

    fn insert_manifest(conn: &rusqlite::Connection, manifest_id: i64) {
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (?1, 'tentative', 0)",
            [manifest_id],
        )
        .unwrap();
    }

    fn insert_anchor(conn: &rusqlite::Connection, anchor_name: &str, manifest_id: i64) {
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES (?1, ?2, 0)",
            rusqlite::params![anchor_name, manifest_id],
        )
        .unwrap();
    }

    fn insert_manifest_parser(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        path: &str,
        blob_sha: &str,
        parser_id: &str,
    ) {
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            rusqlite::params![blob_sha, parser_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![manifest_id.0, path, blob_sha],
        )
        .unwrap();
    }

    fn insert_job_run(
        conn: &rusqlite::Connection,
        manifest_id: i64,
        analyzer_id: &str,
        status: &str,
        job_id: Option<i64>,
    ) {
        insert_job_run_with_cancel(conn, manifest_id, analyzer_id, status, job_id, 0);
    }

    fn insert_job_run_with_cancel(
        conn: &rusqlite::Connection,
        manifest_id: i64,
        analyzer_id: &str,
        status: &str,
        job_id: Option<i64>,
        cancel_requested: i64,
    ) {
        let finished_at_ns = RunStatus::from_str(status)
            .filter(|state| state.is_terminal())
            .map(|_| 20_i64);
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
                started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, ?2, 1, 'cfg', ?3, 10, ?4, NULL, ?5, ?6)",
            rusqlite::params![
                manifest_id,
                analyzer_id,
                status,
                finished_at_ns,
                job_id,
                cancel_requested
            ],
        )
        .unwrap();
    }

    fn persisted_cancel_requested(
        conn: &rusqlite::Connection,
        manifest_id: i64,
        analyzer_id: &str,
    ) -> i64 {
        conn.query_row(
            "SELECT cancel_requested FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = ?2",
            rusqlite::params![manifest_id, analyzer_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn count_runs(conn: &rusqlite::Connection, manifest_id: i64, analyzer_id: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = ?2",
            rusqlite::params![manifest_id, analyzer_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    // ─── Cross-store JobKey / JobId identity tests ───────────────
    //
    // These tests pin the invariant that daemon-global job
    // scheduling does not conflate two stores that happen to share
    // a `(manifest_id, analyzer_id)` pair. Without `repo_hash` in
    // `JobKey`, a second store's rerun would be silently coalesced
    // on top of the first store's queued job; without a
    // daemon-global allocator, two stores could hand out the same
    // `JobId` and `cancel` would route to whichever store was
    // scanned first.

    /// Test helper: build a two-store fixture with two aliases
    /// (`repo-a` -> `hash-a`, `repo-b` -> `hash-b`), both carrying a
    /// tentative manifest at `manifest_id`. Returns the temp dirs
    /// (kept alive by the caller) plus the JobManager and both
    /// store connections.
    #[allow(clippy::type_complexity)]
    fn two_store_fixture(
        manifest_id: ManifestId,
    ) -> (
        tempfile::TempDir,
        tempfile::TempDir,
        tempfile::TempDir,
        Arc<JobManager>,
        rusqlite::Connection,
        rusqlite::Connection,
    ) {
        let data = tempfile::tempdir().unwrap();
        let repo_a = tempfile::tempdir().unwrap();
        let repo_b = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, "repo-a", repo_a.path().to_str().unwrap(), "hash-a", 1)
                .unwrap();
            cas_registry::upsert(&tx, "repo-b", repo_b.path().to_str().unwrap(), "hash-b", 1)
                .unwrap();
            tx.commit().unwrap();
        }
        let conn_a = cas_store::open(&cas_data_dir.store_db_path("hash-a")).unwrap();
        let conn_b = cas_store::open(&cas_data_dir.store_db_path("hash-b")).unwrap();
        insert_manifest(&conn_a, manifest_id.0);
        insert_manifest(&conn_b, manifest_id.0);
        let manager = JobManager::new(cas_data_dir);
        (data, repo_a, repo_b, manager, conn_a, conn_b)
    }

    /// Read the `job_id` currently persisted for `(manifest_id,
    /// analyzer_id)` in the given store.
    fn persisted_job_id(conn: &rusqlite::Connection, manifest_id: i64, analyzer_id: &str) -> JobId {
        conn.query_row(
            "SELECT job_id FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = ?2",
            rusqlite::params![manifest_id, analyzer_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn restore_recycles_all_rows_in_collision_group_preserves_unique() {
        // Cross-store collision: hash-a and hash-b both persisted
        // job_id=1 for (manifest_id=1, pyright-lsp). A third row on
        // hash-a at (manifest_id=1, ruby-lsp) has a unique job_id=2.
        // After `restore_from_db`, the collision group must have
        // *every* row rewritten (no row keeps job_id=1) and the
        // unique group must be preserved (job_id=2 unchanged). The
        // ambiguous id 1 must not appear on any surviving row.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_a, manifest_id.0, "ruby-lsp", "queued", Some(2));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        let a_pyright = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let b_pyright = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        let a_ruby = persisted_job_id(&conn_a, manifest_id.0, "ruby-lsp");
        assert_ne!(a_pyright, 1, "hash-a's colliding row must be recycled");
        assert_ne!(b_pyright, 1, "hash-b's colliding row must be recycled");
        assert_ne!(
            a_pyright, b_pyright,
            "both recycled ids must be unique across stores"
        );
        assert_eq!(a_ruby, 2, "unique-group row must keep its original id");
    }

    #[test]
    fn cancel_with_ambiguous_pre_restore_job_id_returns_not_found() {
        // A client that held the ambiguous pre-restart job_id=1
        // must not be able to cancel *any* store, because the id no
        // longer identifies a unique job. `cancel(1)` must return
        // `unknown job id` instead of silently cancelling whichever
        // store was scanned first.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        // Both stores now hold fresh ids != 1.
        assert_ne!(persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp"), 1);
        assert_ne!(persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp"), 1);

        // A client cancel with the stale ambiguous id must fail
        // rather than silently target one of the stores.
        let err = manager.cancel(1).unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("unknown job id"),
            "cancel(1) must return unknown job id after collision recycle, got {message}"
        );
    }

    #[test]
    fn two_stores_same_manifest_id_same_analyzer_id_both_enqueue() {
        // Both stores share `(manifest_id=1, pyright-lsp)` but have
        // distinct job_ids (so there is no collision to recycle).
        // `restore_from_db` must load *both* rows into the memory
        // queue — before `JobKey` carried `repo_hash`,
        // `reserve_existing` would have silently coalesced the
        // second.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(2));

        manager.restore_from_db().unwrap();

        // Both rows must be advertised to the memory queue; the
        // JobIndex is the observable proxy for that.
        assert!(
            manager.job_index.get(1).is_some(),
            "hash-a's job_id=1 must be enqueued"
        );
        assert!(
            manager.job_index.get(2).is_some(),
            "hash-b's job_id=2 must be enqueued"
        );
    }

    #[test]
    fn cancel_targets_only_correct_store() {
        // After restore, each store owns a unique job_id. `cancel`
        // must route via `JobIndex.get(job_id).repo_hash` to the
        // matching store and must not touch the sibling store's row.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(2));

        manager.restore_from_db().unwrap();

        manager.cancel(1).unwrap();

        let status_a: String = conn_a
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        let status_b: String = conn_b
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status_a, "cancelled", "hash-a's job must be cancelled");
        assert_eq!(
            status_b, "queued",
            "hash-b's job must be untouched by cancel(1)"
        );
    }

    #[test]
    fn job_index_no_collision_across_stores_after_restore() {
        // Two stores + two unique job_ids => JobIndex holds two
        // locators pointing to different repo_hashes. Neither should
        // overwrite the other.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(11));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(22));

        manager.restore_from_db().unwrap();

        let loc_a = manager.job_index.get(11).expect("job 11 registered");
        let loc_b = manager.job_index.get(22).expect("job 22 registered");
        assert_eq!(loc_a.repo_hash, "hash-a");
        assert_eq!(loc_b.repo_hash, "hash-b");
    }

    #[test]
    fn same_next_job_id_across_stores_does_not_collide_on_active_id() {
        // Both stores end up with `job_id=1` (the shape that
        // per-store `MAX(job_id)+1` produced). After restore, no
        // active row anywhere may still carry that ambiguous id, and
        // the allocator must be seeded above every observed id so
        // subsequent enqueues cannot reissue it.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        let id_a = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let id_b = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(id_a, id_b);
        assert!(id_a != 1 && id_b != 1);

        // A new allocation must land above every observed active id.
        let next = manager.allocate_job_id().unwrap();
        assert!(
            next > id_a && next > id_b,
            "post-restore allocation ({next}) must exceed recycled ids ({id_a}, {id_b})"
        );
    }

    #[test]
    fn allocator_floor_is_reflected_in_next_allocation() {
        // When a floor exceeds the allocator's current counter, the
        // counter must advance past the returned value so the next
        // allocation cannot reissue it. A plain
        // `allocate_job_id().max(floor)` returned `floor` but left
        // the counter at `current + 1`, so a second call with the
        // same floor would return the same id.
        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);
        let floor = 1_000_000i64;
        let first = manager.allocate_job_id_at_least(floor).unwrap();
        let second = manager.allocate_job_id_at_least(floor).unwrap();
        assert_eq!(first, floor, "first allocation must equal the floor");
        assert!(
            second > first,
            "second allocation with same floor must exceed first ({first}), got {second}"
        );
    }

    #[test]
    fn range_floor_advances_allocator_past_entire_range() {
        // Same invariant for the range variant: after handing out
        // `[first, first + count)`, the next single allocation must
        // start past the tail of the range.
        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);
        let floor = 500_000i64;
        let first = manager.allocate_job_id_range_at_least(4, floor).unwrap();
        assert_eq!(first, floor);
        let next = manager.allocate_job_id().unwrap();
        assert!(
            next >= first + 4,
            "single allocation ({next}) must exceed range tail (first={first}, count=4)"
        );
    }

    #[test]
    fn concurrent_allocations_with_same_floor_are_unique() {
        // The CAS loop in `allocate_job_id_at_least` must produce
        // distinct ids under contention, even when every thread
        // supplies the same floor.
        use std::collections::HashSet;
        use std::sync::Arc as StdArc;
        use std::thread;

        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);
        let floor = 100i64;
        let threads = 8;
        let per_thread = 200;
        let mgr = StdArc::new(manager);
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let m = StdArc::clone(&mgr);
                thread::spawn(move || {
                    let mut ids = Vec::with_capacity(per_thread);
                    for _ in 0..per_thread {
                        ids.push(m.allocate_job_id_at_least(floor).unwrap());
                    }
                    ids
                })
            })
            .collect();
        let mut all: HashSet<JobId> = HashSet::new();
        for h in handles {
            for id in h.join().unwrap() {
                assert!(id >= floor, "every id must respect the floor");
                assert!(all.insert(id), "id {id} was handed out twice");
            }
        }
        assert_eq!(all.len(), threads * per_thread);
    }

    #[test]
    fn restore_loads_both_stores_into_memory_queue() {
        // End-to-end: after `restore_from_db`, both stores' active
        // rows are visible in the JobIndex (proxy for the in-memory
        // queue's dispatch set). Without `repo_hash` in `JobKey`,
        // `reserve_existing` would have coalesced the second row and
        // left it stuck in DB.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(100));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(200));

        manager.restore_from_db().unwrap();

        assert!(manager.job_index.get(100).is_some());
        assert!(manager.job_index.get(200).is_some());
    }

    // ─── Durable ambiguous-id tombstone tests ────────────────────
    //
    // The four tests below pin the crash-safety invariant that the
    // collision-recycle in `restore_from_db` remains correct even if
    // per-store UPDATEs fail partway. Without the tombstone the
    // surviving row would keep the old ambiguous id and `cancel`
    // would silently target it.

    #[test]
    fn restore_writes_tombstone_for_collision_group() {
        // The ambiguous old id must appear in `ambiguous_job_ids`
        // *after* restore. This is the durable half of the
        // "cancel-of-ambiguous returns unknown" guarantee — the
        // in-memory `JobIndex` alone is not enough because it is
        // dropped on daemon restart.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        let index_path = data.path().join("index.db");
        let index = cas_registry::open(&index_path).unwrap();
        assert!(
            cas_registry::is_ambiguous_job_id(&index, 1).unwrap(),
            "ambiguous job_id=1 must be tombstoned"
        );
    }

    #[test]
    fn partial_rewrite_recovery_recycles_surviving_row() {
        // Simulate a partial-rewrite crash: a prior restart
        // tombstoned old_id=1 and rewrote store-a's row to a fresh
        // id, but crashed before store-b's UPDATE landed. The
        // next restart sees only ONE row carrying job_id=1 (no
        // in-restart collision), but the tombstone says "this id was
        // ambiguous once — recycle any survivor." The row must be
        // rewritten to a fresh id.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, _conn_a, conn_b) = two_store_fixture(manifest_id);
        // Only store-b holds the ambiguous id now (store-a was
        // already rewritten by a hypothetical earlier restart).
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));
        // Seed the tombstone as if a prior restart had committed it.
        {
            let mut index = cas_registry::open(&data.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[1], 12345).unwrap();
            tx.commit().unwrap();
        }

        manager.restore_from_db().unwrap();

        let id_b = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(
            id_b, 1,
            "tombstoned surviving row must be recycled to a fresh id, got {id_b}"
        );
    }

    #[test]
    fn cancel_of_tombstoned_id_returns_unknown_without_scan() {
        // Even if a store still holds a row with the tombstoned id
        // (e.g. a mid-restart crash left it there), `cancel(old_id)`
        // must be rejected as `unknown job id` before the per-store
        // scan can hit that row. The tombstone check is
        // authoritative.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, _conn_a, conn_b) = two_store_fixture(manifest_id);
        // Manually seed both a tombstone and a still-live row that
        // carries the tombstoned id — simulating the partial-rewrite
        // window before the next restore_from_db runs.
        {
            let mut index = cas_registry::open(&data.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[42], 12345).unwrap();
            tx.commit().unwrap();
        }
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(42));

        // Bypass restore so the row still carries the tombstoned id.
        let err = manager.cancel(42).unwrap_err();
        assert!(
            format!("{err}").contains("unknown job id"),
            "cancel of tombstoned id must return unknown, got {err}"
        );
        // And the store row must be untouched.
        let status: String = conn_b
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "queued");
    }

    #[test]
    fn restore_seeds_allocator_above_terminal_job_ids() {
        // The allocator floor must clear every historical row's
        // job_id, not just currently-queued ones. `JobId` is an
        // external identifier (surfaced by `jobs list` etc.) so
        // re-issuing an id that already named a terminal run would
        // break identifier stability. Seed a `succeeded` row with a
        // job_id far above any queued row's and confirm the
        // post-restore allocation exceeds it.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        // Active queued row at a small id.
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(100));
        // Historical terminal row at a large id — must not be
        // re-issued. (Different analyzer_id so it doesn't collide
        // with the queued row's PK.)
        insert_job_run(&conn_b, manifest_id.0, "ruby-lsp", "succeeded", Some(5000));

        manager.restore_from_db().unwrap();

        let next = manager.allocate_job_id().unwrap();
        assert!(
            next > 5000,
            "allocator ({next}) must clear historical terminal job_id 5000"
        );
    }

    #[test]
    fn same_repo_hash_multiple_aliases_is_scanned_once() {
        // Two aliases pointing at the same repo_hash must be treated
        // as ONE store during restore. Without dedupe the loop
        // scanned the store twice, doubled every queued row into
        // `active_by_id`, and triggered a false cross-store
        // collision on rows that were actually a single DB row —
        // producing a spurious tombstone and a job_id / DB drift.
        let manifest_id = ManifestId(1);
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            // Both aliases point at the same physical store hash-a.
            cas_registry::upsert(&tx, "alias-a", repo.path().to_str().unwrap(), "hash-a", 1)
                .unwrap();
            cas_registry::upsert(&tx, "alias-b", repo.path().to_str().unwrap(), "hash-a", 2)
                .unwrap();
            tx.commit().unwrap();
        }
        let conn = cas_store::open(&cas_data_dir.store_db_path("hash-a")).unwrap();
        insert_manifest(&conn, manifest_id.0);
        insert_job_run(&conn, manifest_id.0, "pyright-lsp", "queued", Some(42));
        let manager = JobManager::new(cas_data_dir);

        manager.restore_from_db().unwrap();

        // The single physical row must keep its job_id.
        assert_eq!(persisted_job_id(&conn, manifest_id.0, "pyright-lsp"), 42);
        // No tombstone was written — this was not an ambiguous id.
        let index = cas_registry::open(&data.path().join("index.db")).unwrap();
        assert!(!cas_registry::is_ambiguous_job_id(&index, 42).unwrap());
        // Exactly one JobIndex entry (not double-registered).
        assert!(manager.job_index.get(42).is_some());
    }

    #[test]
    fn restore_seeds_allocator_above_tombstoned_ids() {
        // Tombstoned ids are permanently retired. Even if the store
        // row that carried one has since been pruned (so it no
        // longer appears in the whole-store `MAX(job_id)`), the
        // allocator floor must still clear it — otherwise a clock
        // rollback + prune sequence could re-issue a retired
        // ambiguous id.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, _conn_a, _conn_b) = two_store_fixture(manifest_id);
        // Seed a tombstone far above any store row (the fixture
        // stores are empty). No queued rows exist, so historical max
        // is 0. Only the tombstone should push the floor up.
        {
            let mut index = cas_registry::open(&data.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[9_999_999], 12345).unwrap();
            tx.commit().unwrap();
        }

        manager.restore_from_db().unwrap();

        let next = manager.allocate_job_id().unwrap();
        assert!(
            next > 9_999_999,
            "allocator ({next}) must clear tombstoned id 9_999_999"
        );
    }

    #[test]
    fn allocator_overflow_fails_closed() {
        // The allocator is the uniqueness invariant, so overflow
        // past `i64::MAX` must fail closed rather than silently
        // saturate or wrap. A `saturating_add(1)` at `i64::MAX`
        // returns `MAX` unchanged, the CAS commits `next == current`,
        // the returned value is `MAX`, and the very next call
        // returns `MAX` again — a duplicate id.
        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);

        // Seed the counter to `MAX - 1` — the next single allocation
        // returns `MAX - 1` (advancing the counter to `MAX`), and any
        // further single allocation must error rather than reissue.
        manager
            .seed_job_id_allocator_at_least(JobId::MAX - 2)
            .unwrap();
        let last = manager.allocate_job_id().unwrap();
        assert_eq!(last, JobId::MAX - 1);
        assert!(
            manager.allocate_job_id().is_err(),
            "counter at MAX must fail closed on further single allocation"
        );
        assert!(
            manager.allocate_job_id_at_least(0).is_err(),
            "counter at MAX must fail closed on floored allocation too"
        );

        // A range that would trail past `i64::MAX` must fail closed
        // even from a fresh allocator.
        let data2 = tempfile::tempdir().unwrap();
        let m2 = JobManager::new(Arc::new(CasDataDir::with_root(data2.path().to_path_buf())));
        assert!(
            m2.allocate_job_id_range_at_least(2, JobId::MAX - 1)
                .is_err(),
            "range whose tail overflows must fail closed"
        );

        // Seeding at `i64::MAX` itself (which needs to bump to
        // `MAX + 1`) must also fail closed.
        assert!(
            m2.seed_job_id_allocator_at_least(JobId::MAX).is_err(),
            "seed at i64::MAX must fail closed"
        );
    }

    #[test]
    fn restore_assigns_global_unique_ids_to_null_jobs_across_stores() {
        // Rows whose `job_id` was persisted as NULL must be filled
        // from the daemon-global allocator, not per-store
        // `MAX(job_id)+1`. If two stores both had a NULL row, a
        // per-store filler would hand them the same id and rely on
        // the collision-repair path to fix it — but a Phase 2 error
        // in between would leave the per-store ids persisted. All
        // id assignment goes through the global allocator so no
        // per-store id ever hits disk.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        // Two stores, each with a queued row whose job_id is NULL.
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", None);
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", None);

        manager.restore_from_db().unwrap();

        let id_a = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let id_b = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(id_a, id_b, "the two NULL rows must get distinct ids");
        assert!(manager.job_index.get(id_a).is_some());
        assert!(manager.job_index.get(id_b).is_some());
    }

    #[test]
    fn restore_null_assignment_fails_closed_at_allocator_max() {
        // If the allocator is already at `i64::MAX`, filling a NULL
        // `job_id` row must propagate the allocator's fail-closed
        // error rather than silently reusing an id or writing a
        // bogus value.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, _conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", None);
        // Also seed a tombstone at `i64::MAX - 1` so the Phase 2
        // seed drives the allocator into overflow territory
        // (`observed_max = MAX - 1` -> seed bump to `MAX`, and the
        // subsequent single allocation must fail).
        let data_dir = manager.cas_data_dir();
        let mut index = cas_registry::open(&data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::insert_ambiguous_ids(&tx, &[JobId::MAX - 1], 1).unwrap();
        tx.commit().unwrap();

        let err = manager.restore_from_db().unwrap_err();
        assert!(
            format!("{err}").contains("job id allocator overflowed"),
            "restore must fail closed when allocator overflows, got {err}"
        );
    }

    #[test]
    fn restore_recycles_terminal_plus_queued_collision_and_cancel_cannot_target_active_with_old_id()
    {
        // Cross-store collision where one side is terminal and the
        // other is queued. `JobId` is an external identifier, so a
        // stale client that recorded "job 42 succeeded" for store A
        // must not be able to cancel store B's still-queued job 42
        // just because A's row is not on the dispatch path. Restore
        // must rewrite BOTH the terminal and the queued row to fresh
        // globally-unique ids, tombstone 42, and reject `cancel(42)`.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "succeeded", Some(42));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(42));

        manager.restore_from_db().unwrap();

        let a_id = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let b_id = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(a_id, 42, "terminal side must be recycled");
        assert_ne!(b_id, 42, "queued side must be recycled");
        assert_ne!(a_id, b_id, "both sides must land on distinct ids");

        // `cancel(42)` must return unknown, not silently target B.
        let err = manager.cancel(42).unwrap_err();
        assert!(format!("{err}").contains("unknown job id"));
        let status_b: String = conn_b
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status_b, "queued", "B's queued row must be untouched");
    }

    #[test]
    fn restore_recycles_terminal_terminal_collision_for_globally_unique_job_list() {
        // Two terminal rows sharing a `JobId` also constitute a
        // collision from the `jobs.list` / external-identifier
        // perspective: two distinct historical runs cannot share the
        // same id. Restore must rewrite both to fresh ids and record
        // the tombstone for the shared old id.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "succeeded", Some(99));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "failed", Some(99));

        manager.restore_from_db().unwrap();

        let a_id = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let b_id = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(a_id, 99);
        assert_ne!(b_id, 99);
        assert_ne!(a_id, b_id);
        let index = cas_registry::open(&data.path().join("index.db")).unwrap();
        assert!(
            cas_registry::is_ambiguous_job_id(&index, 99).unwrap(),
            "the shared terminal id must be tombstoned"
        );
        // Neither terminal row is dispatchable — JobIndex must NOT
        // list them (Phase 4 filters by is_active).
        assert!(manager.job_index.get(a_id).is_none());
        assert!(manager.job_index.get(b_id).is_none());
    }

    #[test]
    fn pruning_terminal_collision_cannot_remove_other_store_active_job_index() {
        // Composed invariant: even when a terminal row and an active
        // row shared a `JobId` at persist time, a later `prune_jobs`
        // must not touch the active sibling's `JobIndex` entry.
        // Restore's terminal recycling is what makes this safe —
        // after rewrite the terminal side's id no longer matches
        // the active side's, so `prune_jobs_in_store`'s
        // `remove_many(&orphan_terminal_ids)` on store A cannot
        // touch store B's entry.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        // Terminal orphan in store A (no anchor references
        // manifest_id=1, so it satisfies the prune predicate).
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "succeeded", Some(7));
        // Queued row in store B with the same id.
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(7));

        manager.restore_from_db().unwrap();

        let b_id = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(b_id, 7);
        assert!(
            manager.job_index.get(b_id).is_some(),
            "B's active row must be registered under its fresh id"
        );

        // Prune terminal orphans across both stores.
        manager.prune_jobs(None, false).unwrap();

        // B's active JobIndex entry must survive — its id is fresh
        // and unrelated to A's terminal orphan.
        assert!(
            manager.job_index.get(b_id).is_some(),
            "prune must not evict B's active locator via the shared old id"
        );
    }

    #[test]
    fn collision_recycle_preserves_cancel_requested_on_queued_row() {
        // Identity-only rewrite: a queued row whose cancel was
        // requested pre-restart must NOT be silently re-armed by
        // the collision-recycle SQL. `cancel_requested = 1` must
        // survive the id rewrite so the worker still treats the
        // job as cancelled once it drains from the memory queue.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run_with_cancel(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(3), 1);
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(3));

        manager.restore_from_db().unwrap();

        // A's row was recycled to a fresh id but its cancel flag
        // must be intact.
        assert_ne!(persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp"), 3);
        assert_eq!(
            persisted_cancel_requested(&conn_a, manifest_id.0, "pyright-lsp"),
            1,
            "collision recycle must preserve cancel_requested = 1"
        );
        // B's row was untouched by the cancel flag.
        assert_eq!(
            persisted_cancel_requested(&conn_b, manifest_id.0, "pyright-lsp"),
            0
        );
    }

    #[test]
    fn collision_recycle_preserves_cancel_requested_on_terminal_row() {
        // Terminal rows also carry `cancel_requested` as historical
        // record. The recycle must not clobber it.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run_with_cancel(
            &conn_a,
            manifest_id.0,
            "pyright-lsp",
            "cancelled",
            Some(8),
            1,
        );
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(8));

        manager.restore_from_db().unwrap();

        assert_ne!(persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp"), 8);
        assert_eq!(
            persisted_cancel_requested(&conn_a, manifest_id.0, "pyright-lsp"),
            1,
            "terminal recycle must preserve cancel_requested = 1"
        );
    }

    #[test]
    fn tombstone_survives_across_manager_reinit() {
        // The tombstone must be durable across daemon restarts —
        // it lives in `index.db`, not in `JobManager` in-memory
        // state. A second `JobManager` opened against the same data
        // dir must still reject `cancel(old_id)`.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));
        manager.restore_from_db().unwrap();
        drop(manager);

        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager2 = JobManager::new(cas_data_dir);
        // No restore call on manager2 — the tombstone alone must
        // suffice to reject the stale cancel.
        let err = manager2.cancel(1).unwrap_err();
        assert!(format!("{err}").contains("unknown job id"));
    }
}
