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

#[derive(Debug)]
struct DispatchJob {
    job: Job,
    pool_group: Option<&'static str>,
    key: JobKey,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct JobKey {
    manifest_id: ManifestId,
    analyzer_id: String,
}

impl JobKey {
    fn from_job(job: &Job) -> Self {
        Self {
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
        })
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
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        for entry in cas_registry::list_all(&index)? {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET status = 'queued', finished_at_ns = NULL, error = NULL
                 WHERE status = 'running'",
                [],
            )?;
            let first_id = next_job_id(&conn)?.max(now_ns());
            let missing_job_ids = conn
                .prepare(
                    "SELECT manifest_id, analyzer_id
                     FROM workspace_analysis_runs
                     WHERE status = 'queued' AND job_id IS NULL
                     ORDER BY started_at_ns ASC, analyzer_id ASC",
                )?
                .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (next_id, (manifest_id, analyzer_id)) in (first_id..).zip(missing_job_ids) {
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET job_id = ?1, cancel_requested = 0
                     WHERE manifest_id = ?2 AND analyzer_id = ?3",
                    params![next_id, manifest_id, analyzer_id],
                )?;
                self.runtime_metrics.mark_enqueued(
                    next_id,
                    pool_group_for_analyzer_id(&analyzer_id).ok().flatten(),
                    now_ns(),
                );
            }
            let mut stmt = conn.prepare(
                "SELECT job_id, manifest_id, analyzer_id
                 FROM workspace_analysis_runs
                 WHERE status = 'queued' AND job_id IS NOT NULL
                 ORDER BY started_at_ns ASC, job_id ASC",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        ManifestId(r.get::<_, i64>(1)?),
                        r.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (id, manifest_id, analyzer_id) in rows {
                let analyzer_id_for_key = analyzer_id.clone();
                let key = JobKey {
                    manifest_id,
                    analyzer_id: analyzer_id_for_key.clone(),
                };
                if !self.tracked_keys.reserve_existing(key) {
                    continue;
                }
                self.runtime_metrics.mark_enqueued(
                    id,
                    pool_group_for_analyzer_id(&analyzer_id).ok().flatten(),
                    now_ns(),
                );
                let enqueued = self.enqueue_memory(Job {
                    id,
                    alias: entry.alias.clone(),
                    repo_hash: entry.repo_hash.clone(),
                    store_path: store_path.clone(),
                    repo_root: PathBuf::from(&entry.root_path),
                    manifest_id,
                    analyzer_id,
                });
                if !enqueued {
                    self.tracked_keys.release(&JobKey {
                        manifest_id,
                        analyzer_id: analyzer_id_for_key,
                    });
                }
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
        let job_id = next_job_id(conn)?.max(now_ns);

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
        let first_id = next_job_id(conn)?.max(now_ns);
        let mut jobs = Vec::new();
        for (job_id, analyzer) in
            (first_id..).zip(expected_analyzers_for_manifest(conn, manifest_id)?)
        {
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

    /// High-level "reindex this whole repo right now" entry point.
    ///
    /// Looks the alias up in the registry, opens its CAS store,
    /// walks the register hot path with the dedup gate **off** (=
    /// analyzers are always re-queued), and returns a structured
    /// outcome.
    ///
    /// # Why the surface is intentionally narrow
    ///
    /// Callers (operator-driven reindex, parser-revision drift
    /// recovery, future watcher heuristics) supply only `alias` and
    /// `reason`. All the routing identity (`repo_hash`, `repo_root`,
    /// `manifest_id`, `entries`, `now_ns`) is derived inside this
    /// method from the registry + clock. A buggy caller cannot
    /// hand-stamp the wrong store path or a stale timestamp.
    ///
    /// # Errors
    /// * [`Error::RepoNotFound`] if `alias` is not registered.
    /// * SQLite / IO errors from the registry, store open, git
    ///   invocation, or the analyzer enqueue path bubble up
    ///   unchanged.
    pub fn enqueue_full_repo_reindex(
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
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
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

fn next_job_id(conn: &Connection) -> Result<JobId> {
    let max: Option<i64> =
        conn.query_row("SELECT MAX(job_id) FROM workspace_analysis_runs", [], |r| {
            r.get(0)
        })?;
    Ok(max.unwrap_or_else(now_ns) + 1)
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
        let finished_at_ns = RunStatus::from_str(status)
            .filter(|state| state.is_terminal())
            .map(|_| 20_i64);
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
                started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, ?2, 1, 'cfg', ?3, 10, ?4, NULL, ?5, 0)",
            rusqlite::params![manifest_id, analyzer_id, status, finished_at_ns, job_id],
        )
        .unwrap();
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
}
