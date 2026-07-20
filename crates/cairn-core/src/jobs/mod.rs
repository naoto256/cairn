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
    ANALYZER_STALL_TIMEOUT, AnalyzerProgress, AnalyzerRunRequest, RunRecord, RunStatus,
    all_workspace_analyzers, config_hash, expected_analyzers_for_manifest, mark_run,
    run_one_workspace_analyzer_with_timeout,
};
use crate::{Error, Result};

mod index;
mod list;
mod metrics;
mod restore;
mod scheduler;
mod worker;

use index::{JobIndex, TrackedJobKeys};
use metrics::JobRuntimeMetricsStore;
use scheduler::{SchedulerMsg, scheduler_loop};

#[cfg(test)]
use scheduler::JobScheduler;

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

struct JobAdmissionState {
    accepting: bool,
    active_progress: HashMap<JobId, AnalyzerProgress>,
}

impl Default for JobAdmissionState {
    fn default() -> Self {
        Self {
            accepting: true,
            active_progress: HashMap::new(),
        }
    }
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

pub struct JobManager {
    cas_data_dir: Arc<CasDataDir>,
    lifecycle: Option<Arc<crate::lifecycle::RepoLifecycleManager>>,
    scheduler_sender: Mutex<Option<mpsc::UnboundedSender<SchedulerMsg>>>,
    scheduler_receiver: Mutex<Option<mpsc::UnboundedReceiver<SchedulerMsg>>>,
    scheduler: Mutex<Option<JoinHandle<()>>>,
    worker_sender: Mutex<Option<mpsc::UnboundedSender<DispatchJob>>>,
    worker_receiver: Arc<AsyncMutex<mpsc::UnboundedReceiver<DispatchJob>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    runtime_metrics: JobRuntimeMetricsStore,
    job_index: JobIndex,
    tracked_keys: TrackedJobKeys,
    /// Admission and active cancellation handles share one lock so
    /// `begin_shutdown` is a linearization point: after it returns, no
    /// enqueue can commit a durable row and every run that registered before
    /// it has observed cancellation.
    admission: Mutex<JobAdmissionState>,
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
    pub(crate) fn acquire_repository_lease(
        &self,
        repo_hash: &str,
    ) -> Result<Option<crate::lifecycle::RepoLease>> {
        self.lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.acquire_by_repo_hash(repo_hash))
            .transpose()
    }

    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Arc<Self> {
        Self::build(cas_data_dir, None)
    }

    #[must_use]
    pub fn with_lifecycle(
        cas_data_dir: Arc<CasDataDir>,
        lifecycle: Arc<crate::lifecycle::RepoLifecycleManager>,
    ) -> Arc<Self> {
        Self::build(cas_data_dir, Some(lifecycle))
    }

    fn build(
        cas_data_dir: Arc<CasDataDir>,
        lifecycle: Option<Arc<crate::lifecycle::RepoLifecycleManager>>,
    ) -> Arc<Self> {
        let (scheduler_sender, scheduler_receiver) = mpsc::unbounded_channel();
        let (worker_sender, worker_receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            cas_data_dir,
            lifecycle,
            scheduler_sender: Mutex::new(Some(scheduler_sender)),
            scheduler_receiver: Mutex::new(Some(scheduler_receiver)),
            scheduler: Mutex::new(None),
            worker_sender: Mutex::new(Some(worker_sender)),
            worker_receiver: Arc::new(AsyncMutex::new(worker_receiver)),
            workers: Mutex::new(Vec::new()),
            runtime_metrics: JobRuntimeMetricsStore::default(),
            job_index: JobIndex::default(),
            tracked_keys: TrackedJobKeys::default(),
            admission: Mutex::new(JobAdmissionState::default()),
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
        let _lease = self
            .lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.acquire_by_repo_hash(&entry.repo_hash))
            .transpose()?;

        let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
        let mut conn = cas_store::open_existing(&store_path)?;
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
        let admission = self.admission.lock().expect("job admission lock poisoned");
        if !admission.accepting {
            return Err(Error::JobManagerShuttingDown);
        }
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
        drop(admission);
        Ok(enqueued.then(|| QueuedAnalyzerJob {
            job_id,
            analyzer_id: analyzer_id.to_string(),
            state: RunStatus::Queued.as_str().to_string(),
        }))
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
            let _lease = self
                .lifecycle
                .as_ref()
                .map(|lifecycle| lifecycle.acquire_by_repo_hash(&locator.repo_hash))
                .transpose()?;
            let store_path = self.cas_data_dir.store_db_path(&locator.repo_hash);
            let conn = cas_store::open_existing(&store_path)?;
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
            let _lease = self
                .lifecycle
                .as_ref()
                .map(|lifecycle| lifecycle.acquire_for_enumeration(&entry.repo_hash))
                .transpose()?
                .flatten();
            if self.lifecycle.is_some() && _lease.is_none() {
                continue;
            }
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open_existing(&store_path)?;
            if let Some(result) = self.cancel_in_store(&conn, job_id)? {
                self.job_index
                    .insert(job_id, &entry.alias, &entry.repo_hash);
                return Ok(result);
            }
        }
        Err(Error::InvalidArgument(format!("unknown job id: {job_id}")))
    }

    /// Cancel every queued/running analyzer job owned by one canonical
    /// repository. The lifecycle gate has already linearized Removing, so no
    /// new enqueue can become runnable after this scan.
    pub fn cancel_repository(&self, repo_hash: &str) -> Result<usize> {
        let store_path = self.cas_data_dir.store_db_path(repo_hash);
        let conn = match cas_store::open_existing(&store_path) {
            Ok(conn) => conn,
            Err(Error::StoreNotFound { .. }) => return Ok(0),
            Err(err) => return Err(err),
        };
        let mut stmt = conn.prepare(
            "SELECT job_id, status FROM workspace_analysis_runs
             WHERE job_id IS NOT NULL AND status IN ('queued', 'running')",
        )?;
        let rows: Vec<(JobId, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        for (job_id, status) in &rows {
            if status == RunStatus::Queued.as_str() {
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET status = 'cancelled', cancel_requested = 1, finished_at_ns = ?1
                     WHERE job_id = ?2",
                    params![now_ns(), job_id],
                )?;
                self.notify_cancelled_job(*job_id);
            } else {
                conn.execute(
                    "UPDATE workspace_analysis_runs SET cancel_requested = 1 WHERE job_id = ?1",
                    params![job_id],
                )?;
                self.cancel_active_progress(*job_id);
            }
        }
        Ok(rows.len())
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
                self.cancel_active_progress(job_id);
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
        self.begin_shutdown();
        test_observe_job_manager_drain();
        if tokio::time::timeout(drain_timeout, async {
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
                let _ = handle.await;
            }
            self.worker_sender
                .lock()
                .expect("job worker sender lock poisoned")
                .take();
            let handles = {
                let mut workers = self.workers.lock().expect("job worker lock poisoned");
                std::mem::take(&mut *workers)
            };
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await
        .is_err()
        {
            warn!(
                timeout_ms = drain_timeout.as_millis(),
                "job manager shutdown drain timed out"
            );
        }
    }

    /// Close analyzer-job admission and request cancellation of every active
    /// run. This is idempotent and is the shutdown linearization point.
    pub fn begin_shutdown(&self) {
        let mut admission = self.admission.lock().expect("job admission lock poisoned");
        let first_transition = admission.accepting;
        admission.accepting = false;
        for progress in admission.active_progress.values() {
            progress.cancel();
        }
        drop(admission);
        if first_transition {
            test_observe_job_manager_shutdown();
        }
    }

    fn register_active_progress(&self, job_id: JobId, progress: AnalyzerProgress) {
        let mut admission = self.admission.lock().expect("job admission lock poisoned");
        if !admission.accepting {
            progress.cancel();
        }
        admission.active_progress.insert(job_id, progress);
    }

    fn cancel_active_progress(&self, job_id: JobId) {
        if let Some(progress) = self
            .admission
            .lock()
            .expect("job admission lock poisoned")
            .active_progress
            .get(&job_id)
        {
            progress.cancel();
        }
    }

    fn unregister_active_progress(&self, job_id: JobId) {
        self.admission
            .lock()
            .expect("job admission lock poisoned")
            .active_progress
            .remove(&job_id);
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
fn test_observe_job_manager_drain() {
    if let Some(observer) = JOB_MANAGER_DRAIN_OBSERVER
        .lock()
        .expect("job manager drain observer poisoned")
        .as_ref()
    {
        observer();
    }
}

#[cfg(not(test))]
fn test_observe_job_manager_drain() {}

#[cfg(test)]
pub(crate) static JOB_MANAGER_SHUTDOWN_OBSERVER: Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    Mutex::new(None);

#[cfg(test)]
pub(crate) static JOB_MANAGER_DRAIN_OBSERVER: Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    Mutex::new(None);

fn pool_group_for_analyzer_id(analyzer_id: &str) -> Result<Option<&'static str>> {
    all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == analyzer_id)
        .map(|a| a.pool_group())
        .ok_or_else(|| Error::InvalidArgument(format!("unknown analyzer: {analyzer_id}")))
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
mod tests;
