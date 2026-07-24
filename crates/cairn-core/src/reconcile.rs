//! `RepoReconcileManager` — durable, repo_hash-owned driver of the
//! reindex state machine.
//!
//! Every reindex intent (watcher event, manual reindex, startup
//! recovery, retry) first bumps `repo_reconcile_state.desired_generation`
//! (or `force_generation`) durably, and only then wakes an
//! in-process worker to execute the attempt. A crash between the
//! bump and the attempt leaves the gap `desired > applied` durable,
//! so the next startup can resume via
//! `cas::registry::recover_interrupted_attempts` + immediate wake.
//!
//! # Concurrency contract
//!
//! - At most one attempt per `repo_hash` runs at a time. The
//!   in-process guarantee is the `worker_running` flag on the
//!   per-repo runtime; the on-disk guarantee is Phase 1's
//!   `mark_attempt_start` WHERE clause
//!   (`attempt_generation IS NULL` + affected-rows == 1). A racing
//!   request that loses the mutex is a no-op wake — the running
//!   worker sees the newer `desired_generation` and re-loops.
//! - At most `CAIRN_RECONCILE_MAX_CONCURRENCY` attempts run across
//!   all repositories. Workers acquire the global permit before
//!   the first attempt write and release it after finalization.
//! - The runtime map is a coalesce aid only; the DB is the source
//!   of truth. Dropping and rebuilding the map on restart is safe.
//!
//! # Not in scope for Phase 2
//!
//! - Watcher handle ownership (still `WatchManager`).
//! - Wire / status / doctor protocol changes (Phase 3+).
//! - New DB schema.
//! - Per-file incremental indexing.

use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, Semaphore};
use tracing::{debug, info, warn};

use crate::cas::registry::{self as cas_registry, WatcherState};
use crate::cas::store as cas_store;
use crate::jobs::JobManager;
use crate::lifecycle::{RemovalIntent, RepoLifecycleManager};
use crate::paths::CasDataDir;
use crate::register::{
    ReconcilePublicationReceipt, ReconcileRegistration, register_repo_reconcile,
    register_repo_reconcile_enqueue_analyzers,
};
use crate::{Error, Result};

const RECONCILE_MAX_CONCURRENCY_ENV: &str = "CAIRN_RECONCILE_MAX_CONCURRENCY";
const DEFAULT_RECONCILE_MAX_CONCURRENCY: usize = 8;
const MAX_RECONCILE_MAX_CONCURRENCY: usize = 64;

/// Parse a raw `CAIRN_RECONCILE_MAX_CONCURRENCY` value into a
/// permit count. Anything outside `1..=MAX_RECONCILE_MAX_CONCURRENCY`
/// (including non-numeric or overflowing input) falls back to the
/// default with a `warn!` that classifies the rejection. Split
/// from the env read so tests can drive it with arbitrary input.
fn reconcile_max_concurrency_from_env_value(raw: Option<&str>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_RECONCILE_MAX_CONCURRENCY;
    };
    let trimmed = raw.trim();
    match trimmed.parse::<usize>() {
        Ok(value @ 1..=MAX_RECONCILE_MAX_CONCURRENCY) => value,
        _ => {
            let classification = match trimmed.parse::<usize>() {
                Ok(_) => "out_of_range",
                Err(_)
                    if !trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit()) =>
                {
                    "overflow"
                }
                Err(_) => "invalid",
            };
            warn!(
                env = RECONCILE_MAX_CONCURRENCY_ENV,
                default = DEFAULT_RECONCILE_MAX_CONCURRENCY,
                classification,
                "invalid reconcile concurrency; using default"
            );
            DEFAULT_RECONCILE_MAX_CONCURRENCY
        }
    }
}

/// Size of the global attempt semaphore, read from the
/// environment once at manager construction (never resized).
fn reconcile_max_concurrency() -> usize {
    reconcile_max_concurrency_from_env_value(
        std::env::var(RECONCILE_MAX_CONCURRENCY_ENV).ok().as_deref(),
    )
}

/// Why the reconcile driver was woken. Recorded for logs and
/// bookkeeping; does not alter the state-machine transition rules
/// beyond the force/normal choice already carried by
/// `force_generation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileTrigger {
    /// Filesystem watcher event.
    WatchEvent,
    /// Explicit user request via `cairn ctl repo reindex`.
    ManualReindex,
    /// Daemon startup found `desired > applied` (or an
    /// interrupted attempt that recovery cleared).
    StartupRecovery,
    /// Catch-up generation recorded atomically with registration
    /// publication after the watcher was armed.
    RegistrationCatchUp,
    /// Full reconcile requested for every active repository after daemon
    /// startup has attempted to arm all watchers.
    StartupFullReconcile,
    /// Low-frequency full reconcile used to recover a silently missed
    /// filesystem event.
    PeriodicFullReconcile,
    /// Internal retry after a prior failed attempt.
    Retry,
    /// Daemon-startup revision-staleness scanner detected
    /// `parser_revision` drift on a registered repo — the
    /// tentative manifest's persisted parser revision is
    /// behind (or missing against) what the linked-in backend
    /// reports. Triggers a `force_generation` bump so the
    /// worker performs a full reparse rather than a
    /// dedupe-eligible refresh.
    ParserRevisionDrift,
}

/// Return value from a request into the manager — mostly for
/// tests and callers that want to log the durable generation
/// they just recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileRequestOutcome {
    pub repo_hash: String,
    /// Value of `desired_generation` after the durable bump.
    pub generation: i64,
    /// True if `force_generation` was also bumped (manual
    /// reindex path).
    pub forced: bool,
    /// True if the caller either spawned a new worker or woke an
    /// existing one. False if the caller intentionally did not
    /// wake (e.g. bulk startup priming will wake separately).
    pub scheduled: bool,
}

/// Result of the two-step startup sequence: interrupted-attempt
/// recovery followed by all-repository priming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupPrimeOutcome {
    /// Repo hashes whose interrupted attempt was cleared by
    /// [`RepoReconcileManager::recover_interrupted_attempts_without_wake`].
    pub recovered: Vec<String>,
    /// `(repo_hash, generation)` pairs primed for an immediate
    /// full reconcile.
    pub primed: Vec<(String, i64)>,
}

/// Cadence and staleness thresholds for the low-frequency full
/// reconcile that recovers silently missed filesystem events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeriodicReconcilePolicy {
    /// How often the scheduler scans all repositories.
    pub poll_interval: Duration,
    /// Oldest a clean snapshot may get before a periodic
    /// reconcile must refresh it.
    pub max_clean_age: Duration,
    /// Lead time before `max_clean_age`; zero preserves the legacy threshold.
    pub due_margin: Duration,
}

impl PeriodicReconcilePolicy {
    /// Validate the policy and derive the clean-age threshold at
    /// which a repository becomes due. `due_margin` pulls the
    /// threshold ahead of `max_clean_age` so a poll landing just
    /// before expiry still refreshes in time. `Err(InvalidArgument)`
    /// rejects zero durations, a margin smaller than the poll
    /// interval (it could never fire between polls), and a margin
    /// that would more than halve the effective clean window.
    fn due_age(&self) -> Result<Duration> {
        if self.poll_interval.is_zero() || self.max_clean_age.is_zero() {
            return Err(Error::InvalidArgument(
                "periodic reconcile durations must be non-zero".into(),
            ));
        }
        if self.due_margin.is_zero() {
            return Ok(self.max_clean_age);
        }
        if self.due_margin < self.poll_interval {
            return Err(Error::InvalidArgument(
                "periodic reconcile due margin must be zero or at least the poll interval".into(),
            ));
        }
        if self.due_margin >= self.max_clean_age {
            return Err(Error::InvalidArgument(
                "periodic reconcile due margin must be less than max clean age".into(),
            ));
        }
        let due_age = self.max_clean_age - self.due_margin;
        if due_age < self.max_clean_age / 2 {
            return Err(Error::InvalidArgument(
                "periodic reconcile due margin must leave at least half the max clean age".into(),
            ));
        }
        Ok(due_age)
    }
}

impl Default for PeriodicReconcilePolicy {
    fn default() -> Self {
        let poll_interval = Duration::from_secs(5 * 60);
        Self {
            poll_interval,
            max_clean_age: crate::freshness::MAX_CURRENT_SNAPSHOT_AGE,
            due_margin: poll_interval,
        }
    }
}

/// Backoff schedule for failed reconcile attempts.
///
/// `base * 2^min(consecutive_failures - 1, 20)`, capped at `max`.
/// The 2^20 cap prevents the shift from overflowing `Duration`
/// under an astronomical failure streak.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(60 * 10),
        }
    }
}

impl RetryPolicy {
    /// `next_retry_at_ns` for the (n+1)-th attempt given
    /// `n = consecutive_failures` at the moment of failure.
    /// Uses `saturating_add` so a huge failure streak caps at
    /// `i64::MAX` rather than wrapping.
    fn next_retry_at_ns(&self, now_ns: i64, consecutive_failures: i64) -> i64 {
        // First retry (`consecutive_failures == 1`) uses shift 0
        // = one `base_delay`. Documented formula:
        // `base * 2^min(consecutive_failures - 1, 20)`.
        let shift = consecutive_failures.saturating_sub(1).clamp(0, 20) as u32;
        let scaled = self.base_delay.saturating_mul(1u32 << shift);
        let capped = scaled.min(self.max_delay);
        let delay_ns = i64::try_from(capped.as_nanos()).unwrap_or(i64::MAX);
        now_ns.saturating_add(delay_ns)
    }
}

/// Injectable wall-clock for tests. Production wires
/// [`SystemClock`]; tests wire a manual clock so they can pin
/// `now_ns` and advance retry deadlines deterministically.
pub trait Clock: Send + Sync + 'static {
    fn now_ns(&self) -> i64;
}

/// Real wall-clock. Clamps at `i64::MAX` so a distant future
/// SystemTime cannot overflow the `i64` reconcile state schema.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX)
    }
}

/// Per-repo runtime state used to coalesce concurrent requests
/// and to guarantee at most one live worker per `repo_hash`.
/// The DB, not this map, is the source of truth for durable
/// state.
struct RepoRuntime {
    notify: Arc<Notify>,
    worker_running: bool,
    /// Alias last used to trigger a request. The worker picks
    /// this alias for the register/enqueue call if it still
    /// resolves to `repo_hash`; else it falls back to the
    /// lexicographically first alias in `aliases_for_repo`.
    preferred_alias: Option<String>,
    /// Bumped on every request; the worker checks it before
    /// deciding to exit so a request landing in the tiny window
    /// between the worker's "no work" DB read and the "clear
    /// worker_running" mutex acquisition is not lost.
    request_seq: u64,
}

impl RepoRuntime {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            worker_running: false,
            preferred_alias: None,
            request_seq: 0,
        }
    }
}

/// Injectable register-executor for tests. When set, the worker
/// calls the hook instead of running the real
/// `register_repo_*_enqueue` path. Tests use this to inject
/// success/failure, block for in-flight assertions, and count
/// invocations without needing a real git worktree.
#[cfg(test)]
pub type TestRegisterHookFn =
    std::sync::Arc<dyn Fn(&str, &str, i64, bool) -> Result<()> + Send + Sync + 'static>;

/// Injectable pre-open hook for tests that need to observe or fail
/// `load_state` before it opens `index.db`.
#[cfg(test)]
pub type TestLoadStateHookFn = std::sync::Arc<dyn Fn(&str) -> Result<()> + Send + Sync + 'static>;

/// Central reindex state-machine driver — one instance per
/// daemon. See the module-level docs for the concurrency
/// contract.
pub struct RepoReconcileManager {
    cas_data_dir: Arc<CasDataDir>,
    job_manager: Option<Arc<JobManager>>,
    lifecycle: Option<Arc<RepoLifecycleManager>>,
    clock: Arc<dyn Clock>,
    retry: RetryPolicy,
    attempt_permits: Arc<Semaphore>,
    runtimes: Mutex<HashMap<String, RepoRuntime>>,
    shutdown: Arc<Notify>,
    shutting_down: std::sync::atomic::AtomicBool,
    live_workers: AtomicUsize,
    workers_idle: Arc<Notify>,
    periodic_shutdown: Arc<Notify>,
    periodic_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Monotonic counter for tests: how many attempts (start OR
    /// failure) have been driven through the manager. Used to
    /// synchronise with tokio task scheduling in MF tests.
    #[cfg(test)]
    test_attempts_started: AtomicI64,
    /// Test-only register injector. When `Some`, the worker
    /// invokes it in place of the real `register_repo_*_enqueue`
    /// call — the injector receives
    /// `(repo_hash, alias, generation, forced)` and returns
    /// `Result<()>`. Left `None` in production.
    #[cfg(test)]
    test_register_hook: std::sync::Mutex<Option<TestRegisterHookFn>>,
    /// Test-only state-load injector. Runs after the global permit is
    /// acquired but before `index.db` is opened.
    #[cfg(test)]
    test_load_state_hook: std::sync::Mutex<Option<TestLoadStateHookFn>>,
}

impl RepoReconcileManager {
    /// Build a manager wired to a system clock and the default
    /// retry policy. `job_manager = None` disables analyzer
    /// enqueue — attempts still update the register / anchor
    /// tables but no analyzer jobs are queued. This is the shape
    /// used by the historical no-jobs code path; production
    /// callers pass `Some(job_manager)`.
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>, job_manager: Option<Arc<JobManager>>) -> Arc<Self> {
        Self::with_config_and_lifecycle(
            cas_data_dir,
            job_manager,
            None,
            Arc::new(SystemClock),
            RetryPolicy::default(),
        )
    }

    /// Production constructor when a lifecycle manager owns
    /// repository admission and removal — requests and attempts
    /// then take lifecycle leases before touching durable state.
    #[must_use]
    pub fn new_with_lifecycle(
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Option<Arc<JobManager>>,
        lifecycle: Arc<RepoLifecycleManager>,
    ) -> Arc<Self> {
        Self::with_config_and_lifecycle(
            cas_data_dir,
            job_manager,
            Some(lifecycle),
            Arc::new(SystemClock),
            RetryPolicy::default(),
        )
    }

    /// Full constructor exposed for tests. Production uses
    /// [`Self::new`].
    #[must_use]
    pub fn with_config(
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Option<Arc<JobManager>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
    ) -> Arc<Self> {
        Self::with_config_and_lifecycle(cas_data_dir, job_manager, None, clock, retry)
    }

    /// Fully injectable constructor (clock, retry policy,
    /// optional lifecycle). Attempt concurrency still comes from
    /// the environment; tests that need a fixed permit count use
    /// the private `..._and_attempt_concurrency` variant.
    #[must_use]
    pub fn with_config_and_lifecycle(
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Option<Arc<JobManager>>,
        lifecycle: Option<Arc<RepoLifecycleManager>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
    ) -> Arc<Self> {
        Self::with_config_and_lifecycle_and_attempt_concurrency(
            cas_data_dir,
            job_manager,
            lifecycle,
            clock,
            retry,
            reconcile_max_concurrency(),
        )
    }

    /// Innermost constructor — every public constructor funnels
    /// here. `attempt_concurrency` sizes the global attempt
    /// semaphore once for the manager's lifetime.
    fn with_config_and_lifecycle_and_attempt_concurrency(
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Option<Arc<JobManager>>,
        lifecycle: Option<Arc<RepoLifecycleManager>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
        attempt_concurrency: usize,
    ) -> Arc<Self> {
        debug_assert!((1..=MAX_RECONCILE_MAX_CONCURRENCY).contains(&attempt_concurrency));
        Arc::new(Self {
            cas_data_dir,
            job_manager,
            lifecycle,
            clock,
            retry,
            attempt_permits: Arc::new(Semaphore::new(attempt_concurrency)),
            runtimes: Mutex::new(HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
            workers_idle: Arc::new(Notify::new()),
            periodic_shutdown: Arc::new(Notify::new()),
            periodic_task: Mutex::new(None),
            #[cfg(test)]
            test_attempts_started: AtomicI64::new(0),
            #[cfg(test)]
            test_register_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            test_load_state_hook: std::sync::Mutex::new(None),
        })
    }

    #[cfg(test)]
    fn with_config_and_attempt_concurrency(
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Option<Arc<JobManager>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
        attempt_concurrency: usize,
    ) -> Arc<Self> {
        Self::with_config_and_lifecycle_and_attempt_concurrency(
            cas_data_dir,
            job_manager,
            None,
            clock,
            retry,
            attempt_concurrency,
        )
    }

    /// Install a test-only register injector. Returns the
    /// previously installed hook, if any.
    #[cfg(test)]
    pub fn set_test_register_hook(&self, hook: TestRegisterHookFn) -> Option<TestRegisterHookFn> {
        let mut guard = self.test_register_hook.lock().unwrap();
        guard.replace(hook)
    }

    #[cfg(test)]
    fn take_test_register_hook_snapshot(&self) -> Option<TestRegisterHookFn> {
        self.test_register_hook.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub fn set_test_load_state_hook(
        &self,
        hook: TestLoadStateHookFn,
    ) -> Option<TestLoadStateHookFn> {
        let mut guard = self.test_load_state_hook.lock().unwrap();
        guard.replace(hook)
    }

    #[cfg(test)]
    fn take_test_load_state_hook_snapshot(&self) -> Option<TestLoadStateHookFn> {
        self.test_load_state_hook.lock().unwrap().clone()
    }

    /// Record durable dirty intent for `repo_hash` and wake the
    /// worker. The `repo_hash` MUST already exist in
    /// `repositories` — this API does not create rows.
    pub async fn request_dirty_by_repo_hash(
        self: &Arc<Self>,
        repo_hash: String,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
        self.request_dirty(repo_hash, None, trigger).await
    }

    /// Convenience wrapper: resolve `alias → repo_hash`, then
    /// bump `desired_generation`.
    pub async fn request_dirty_by_alias(
        self: &Arc<Self>,
        alias: String,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
        let repo_hash = self.resolve_alias(&alias).await?;
        self.request_dirty(repo_hash, Some(alias), trigger).await
    }

    /// Manual reindex path: bump `force_generation` (and
    /// therefore `desired_generation`).
    pub async fn request_force_by_alias(
        self: &Arc<Self>,
        alias: String,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
        let repo_hash = self.resolve_alias(&alias).await?;
        self.request_force(repo_hash, Some(alias), trigger).await
    }

    /// Resolve `alias -> repo_hash` on a blocking thread.
    /// `Error::RepoNotFound` when the alias has no registry
    /// entry.
    async fn resolve_alias(&self, alias: &str) -> Result<String> {
        let cas_data_dir = self.cas_data_dir.clone();
        let alias_owned = alias.to_string();
        tokio::task::spawn_blocking(move || -> Result<String> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &alias_owned)?.ok_or_else(|| {
                Error::RepoNotFound {
                    alias: alias_owned.clone(),
                }
            })?;
            Ok(entry.repo_hash)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile resolve_alias", e))?
    }

    /// Shared normal-priority path: durably bump
    /// `desired_generation`, commit, and only then wake or spawn
    /// the worker — a crash between commit and wake still leaves
    /// the `desired > applied` gap for startup recovery. When a
    /// lifecycle manager is wired, the lease is held across the
    /// bump so a repository pending removal rejects the request
    /// instead of recording work for a row about to vanish.
    async fn request_dirty(
        self: &Arc<Self>,
        repo_hash: String,
        alias: Option<String>,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
        let _lease = match &self.lifecycle {
            Some(lifecycle) => Some(lifecycle.acquire_active_by_repo_hash(&repo_hash)?),
            None => None,
        };
        let cas_data_dir = self.cas_data_dir.clone();
        let now_ns = self.clock.now_ns();
        let repo_hash_task = repo_hash.clone();
        let generation = tokio::task::spawn_blocking(move || -> Result<i64> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let g = cas_registry::increment_desired_generation(&tx, &repo_hash_task, now_ns)?;
            tx.commit()?;
            Ok(g)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile increment_desired", e))??;

        debug!(
            repo_hash = %repo_hash,
            generation,
            ?trigger,
            "reconcile dirty request recorded"
        );
        let scheduled = self.wake_or_spawn(&repo_hash, alias.clone());
        Ok(ReconcileRequestOutcome {
            repo_hash,
            generation,
            forced: false,
            scheduled,
        })
    }

    /// Force path: durably bump `force_generation` together with
    /// `desired_generation`. The registry helper also clears any
    /// pending retry deadline, so the operator's request runs
    /// immediately and the worker performs a forced (full
    /// reparse) attempt. Same lease and commit-before-wake
    /// ordering as [`Self::request_dirty`].
    async fn request_force(
        self: &Arc<Self>,
        repo_hash: String,
        alias: Option<String>,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
        let _lease = match &self.lifecycle {
            Some(lifecycle) => Some(lifecycle.acquire_active_by_repo_hash(&repo_hash)?),
            None => None,
        };
        let cas_data_dir = self.cas_data_dir.clone();
        let now_ns = self.clock.now_ns();
        let repo_hash_task = repo_hash.clone();
        let generation = tokio::task::spawn_blocking(move || -> Result<i64> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let g = cas_registry::increment_force_generation(&tx, &repo_hash_task, now_ns)?;
            tx.commit()?;
            Ok(g)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile increment_force", e))??;

        info!(
            repo_hash = %repo_hash,
            generation,
            ?trigger,
            "reconcile force request recorded"
        );
        let scheduled = self.wake_or_spawn(&repo_hash, alias.clone());
        Ok(ReconcileRequestOutcome {
            repo_hash,
            generation,
            forced: true,
            scheduled,
        })
    }

    /// Clear every interrupted attempt without waking a worker. Daemon startup
    /// calls this before arming watchers, then passes the returned hashes to
    /// [`Self::prime_startup_reconcile`] after the watcher barrier.
    pub async fn recover_interrupted_attempts_without_wake(&self) -> Result<Vec<String>> {
        let cas_data_dir = self.cas_data_dir.clone();
        let hashes = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let hashes = cas_registry::recover_interrupted_attempts(&tx)?;
            tx.commit()?;
            Ok(hashes)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile recover_interrupted", e))??;
        Ok(hashes)
    }

    /// Durably request one immediate full reconcile for every active canonical
    /// repository, then wake workers only after the all-or-nothing transaction
    /// commits.
    pub async fn prime_startup_reconcile(
        self: &Arc<Self>,
        recovered: Vec<String>,
    ) -> Result<StartupPrimeOutcome> {
        let cas_data_dir = self.cas_data_dir.clone();
        let now_ns = self.clock.now_ns();
        let primed = tokio::task::spawn_blocking(move || -> Result<Vec<(String, i64)>> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let primed = cas_registry::prime_startup_generations(&tx, now_ns)?;
            tx.commit()?;
            Ok(primed)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile startup prime", e))??;
        for (repo_hash, generation) in &primed {
            debug!(
                repo_hash = %repo_hash,
                generation,
                trigger = ?ReconcileTrigger::StartupFullReconcile,
                "startup reconcile generation recorded"
            );
            self.wake_or_spawn(repo_hash, None);
        }
        Ok(StartupPrimeOutcome { recovered, primed })
    }

    /// Wake a generation that another owner recorded in the same transaction
    /// as its own state publication. No database mutation occurs here.
    pub fn wake_recorded_generation(
        self: &Arc<Self>,
        repo_hash: &str,
        preferred_alias: Option<String>,
    ) -> bool {
        self.wake_or_spawn(repo_hash, preferred_alias)
    }

    /// Start the low-frequency reconcile scheduler. The first poll is delayed;
    /// startup priming provides the immediate full reconcile.
    pub fn start_periodic_reconcile(
        self: &Arc<Self>,
        policy: PeriodicReconcilePolicy,
    ) -> Result<()> {
        let due_age = policy.due_age()?;
        let mut slot = self
            .periodic_task
            .lock()
            .map_err(|_| Error::Internal("periodic reconcile task mutex poisoned".into()))?;
        if slot.is_some() {
            return Err(Error::InvalidArgument(
                "periodic reconcile scheduler already started".into(),
            ));
        }
        // Weak reference: the scheduler task must not keep the
        // manager alive; once the daemon drops its Arc the task
        // exits on its next tick.
        let weak = Arc::downgrade(self);
        let shutdown = self.periodic_shutdown.clone();
        *slot = Some(tokio::spawn(async move {
            let start = tokio::time::Instant::now() + policy.poll_interval;
            let mut interval = tokio::time::interval_at(start, policy.poll_interval);
            // Skip missed ticks instead of bursting: a long stall
            // should yield one catch-up cycle, not a backlog.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.notified() => return,
                    _ = interval.tick() => {
                        let Some(manager) = weak.upgrade() else {
                            return;
                        };
                        manager.run_periodic_cycle(due_age).await;
                    }
                }
            }
        }));
        Ok(())
    }

    /// One scheduler tick: list every repository not marked for
    /// removal, then record a periodic generation for each one
    /// whose clean snapshot is older than `due_age`. Per-repo
    /// errors are logged and skipped so one bad repository cannot
    /// starve the rest; the loop aborts once shutdown is flagged.
    async fn run_periodic_cycle(self: &Arc<Self>, due_age: Duration) {
        let cas_data_dir = self.cas_data_dir.clone();
        let repos = match tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            Ok(cas_registry::list_repositories(&index)?
                .into_iter()
                .filter(|repo| repo.removal_request.is_none())
                .map(|repo| repo.repo_hash)
                .collect())
        })
        .await
        {
            Ok(Ok(repos)) => repos,
            Ok(Err(err)) => {
                warn!(error = %err, "periodic reconcile failed to list repositories");
                return;
            }
            Err(err) => {
                warn!(error = %err, "periodic reconcile repository task failed");
                return;
            }
        };

        for repo_hash in repos {
            if self.shutting_down.load(Ordering::SeqCst) {
                return;
            }
            match self
                .request_periodic_if_due(repo_hash.clone(), due_age)
                .await
            {
                Ok(Some(generation)) => info!(
                    repo_hash = %repo_hash,
                    generation,
                    trigger = ?ReconcileTrigger::PeriodicFullReconcile,
                    "periodic reconcile generation recorded"
                ),
                Ok(None) => {}
                Err(Error::RepositoryUnavailable { .. }) => {
                    debug!(repo_hash = %repo_hash, "periodic reconcile skipped unavailable repository");
                }
                Err(err) => warn!(
                    repo_hash = %repo_hash,
                    error = %err,
                    "periodic reconcile request failed"
                ),
            }
        }
    }

    /// `Ok(None)` means not due — the repository is dirty, has an
    /// attempt in flight, was scanned recently, or is being
    /// removed. The due-predicate and the generation bump run in
    /// one transaction so a racing watcher request can neither
    /// lose nor duplicate work. `Ok(Some(generation))` is the
    /// freshly recorded generation, with the worker already
    /// woken.
    async fn request_periodic_if_due(
        self: &Arc<Self>,
        repo_hash: String,
        due_age: Duration,
    ) -> Result<Option<i64>> {
        let _lease = match &self.lifecycle {
            Some(lifecycle) => Some(lifecycle.acquire_active_by_repo_hash(&repo_hash)?),
            None => None,
        };
        let due_age_ns = i64::try_from(due_age.as_nanos()).unwrap_or(i64::MAX);
        let now_ns = self.clock.now_ns();
        let cas_data_dir = self.cas_data_dir.clone();
        let repo_hash_task = repo_hash.clone();
        let generation = tokio::task::spawn_blocking(move || -> Result<Option<i64>> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let generation = cas_registry::increment_periodic_generation_if_due(
                &tx,
                &repo_hash_task,
                now_ns,
                due_age_ns,
            )?;
            tx.commit()?;
            Ok(generation)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile periodic request", e))??;
        if generation.is_some() {
            self.wake_or_spawn(&repo_hash, None);
        }
        Ok(generation)
    }

    /// Wake workers for every repo the DB says still has
    /// `desired > applied`. Startup uses the stronger all-repository priming
    /// barrier; this remains a support primitive for callers that only need to
    /// resume already-recorded work.
    pub async fn wake_dirty_repositories(self: &Arc<Self>) -> Result<Vec<String>> {
        let cas_data_dir = self.cas_data_dir.clone();
        let dirty = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let repos = cas_registry::list_repositories(&index)?;
            let mut out = Vec::new();
            for r in repos {
                if let Some(state) = cas_registry::get_reconcile_state(&index, &r.repo_hash)?
                    && state.desired_generation > state.applied_generation
                {
                    out.push(r.repo_hash);
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile wake_dirty", e))??;
        for h in &dirty {
            self.wake_or_spawn(h, None);
        }
        Ok(dirty)
    }

    /// Persist watcher lifecycle state. Fails closed unless exactly one
    /// reconcile-state row exists for `repo_hash`.
    pub(crate) fn set_watcher_state_immediate(
        &self,
        repo_hash: &str,
        state: WatcherState,
        error: Option<&str>,
    ) -> Result<()> {
        persist_watcher_state(&self.cas_data_dir, repo_hash, state, error)
    }

    /// Async wrapper for callers that must not perform SQLite I/O on the
    /// runtime worker thread.
    pub async fn set_watcher_state_by_repo_hash(
        &self,
        repo_hash: String,
        state: WatcherState,
        error: Option<String>,
    ) -> Result<()> {
        let cas_data_dir = self.cas_data_dir.clone();
        tokio::task::spawn_blocking(move || {
            persist_watcher_state(&cas_data_dir, &repo_hash, state, error.as_deref())
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile set_watcher_state", e))??;
        Ok(())
    }

    /// Signal every worker to exit and wait up to `timeout` for
    /// them to drain. Returns even on timeout; callers that
    /// require a clean drain should surface the timeout as a
    /// shutdown-degraded log.
    pub async fn shutdown(&self, timeout: Duration) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.shutdown.notify_waiters();
        self.periodic_shutdown.notify_one();
        let deadline = tokio::time::Instant::now() + timeout;
        let periodic = self
            .periodic_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = periodic {
            let _ = tokio::time::timeout_at(deadline, task).await;
        }
        loop {
            if self.live_workers.load(Ordering::SeqCst) == 0 {
                return;
            }
            // Arm the idle notification BEFORE re-reading the
            // counter so a worker draining between the check and
            // the await cannot strand us until the timeout.
            let notified = self.workers_idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.live_workers.load(Ordering::SeqCst) == 0 {
                return;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                warn!(
                    live = self.live_workers.load(Ordering::SeqCst),
                    "reconcile manager shutdown timed out; leaving workers behind"
                );
                return;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Prevent future in-process wakes for a repository. The lifecycle
    /// gate blocks new requests while its lease drain waits for any
    /// already-running attempt to leave the store.
    pub fn quiesce_repository(&self, repo_hash: &str) {
        // Dropping the runtime entry makes a live worker's next
        // `try_finalize_exit` observe a missing entry and exit;
        // the wake below breaks it out of any retry or notify
        // sleep so that check happens promptly.
        let mut runtimes = self.lock_runtimes();
        if let Some(runtime) = runtimes.remove(repo_hash) {
            runtime.notify.notify_waiters();
        }
    }

    /// `true` if a live worker was already running (we notified
    /// it) or we spawned a new one. `false` only if we're
    /// shutting down.
    fn wake_or_spawn(self: &Arc<Self>, repo_hash: &str, preferred_alias: Option<String>) -> bool {
        if self.shutting_down.load(Ordering::SeqCst) {
            debug!(
                repo_hash = %repo_hash,
                "reconcile shutting down; skipping wake"
            );
            return false;
        }
        let mut runtimes = self.lock_runtimes();
        let runtime = runtimes
            .entry(repo_hash.to_string())
            .or_insert_with(RepoRuntime::new);
        runtime.request_seq = runtime.request_seq.wrapping_add(1);
        if let Some(alias) = preferred_alias {
            runtime.preferred_alias = Some(alias);
        }
        // Holding the runtimes mutex makes the running/not-running
        // decision atomic with the request-seq bump: an exiting
        // worker must take the same mutex in `try_finalize_exit`,
        // so it either sees our bump and loops again, or we see
        // `worker_running == false` here and spawn a fresh worker.
        if runtime.worker_running {
            runtime.notify.notify_one();
            true
        } else {
            runtime.worker_running = true;
            let notify = runtime.notify.clone();
            drop(runtimes);
            // Increment BEFORE re-checking the shutdown flag so a
            // concurrent `shutdown()` cannot observe
            // `live_workers == 0` after flipping `shutting_down`
            // between our earlier check and this bump. If the flag
            // was flipped in the window, undo the bump and clear
            // the worker_running flag so a later request can
            // respawn cleanly under a new manager. This closes the
            // "shutdown returns drained while a worker is still
            // about to spawn" race.
            self.live_workers.fetch_add(1, Ordering::SeqCst);
            if self.shutting_down.load(Ordering::SeqCst) {
                self.live_workers.fetch_sub(1, Ordering::SeqCst);
                let mut runtimes = self.lock_runtimes();
                if let Some(rt) = runtimes.get_mut(repo_hash) {
                    rt.worker_running = false;
                }
                debug!(
                    repo_hash = %repo_hash,
                    "reconcile shutting down mid-spawn; aborting new worker"
                );
                return false;
            }
            let mgr = self.clone();
            let hash = repo_hash.to_string();
            tokio::spawn(async move {
                worker_loop(mgr.clone(), hash, notify).await;
                if mgr.live_workers.fetch_sub(1, Ordering::SeqCst) == 1 {
                    mgr.workers_idle.notify_waiters();
                }
            });
            true
        }
    }

    fn lock_runtimes(&self) -> MutexGuard<'_, HashMap<String, RepoRuntime>> {
        self.runtimes.lock().unwrap_or_else(|poisoned| {
            warn!("reconcile manager mutex poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// Returns `(has_work, exit_now)`. `exit_now` transitions
    /// the runtime out of `worker_running` under the mutex so a
    /// concurrent request can respawn cleanly.
    fn try_finalize_exit(&self, repo_hash: &str, observed_seq: u64) -> bool {
        let mut runtimes = self.lock_runtimes();
        let Some(rt) = runtimes.get_mut(repo_hash) else {
            // Runtime entry gone — `quiesce_repository` removed
            // it; nothing to clear, just exit.
            return true;
        };
        if rt.request_seq != observed_seq {
            // A request landed while we were reading DB / holding
            // no mutex — do not exit; loop again.
            return false;
        }
        rt.worker_running = false;
        true
    }
}

/// Blocking helper shared by both watcher-state entry points.
/// The affected-rows == 1 fail-closed contract lives in
/// `cas_registry::set_watcher_state`.
fn persist_watcher_state(
    cas_data_dir: &CasDataDir,
    repo_hash: &str,
    state: WatcherState,
    error: Option<&str>,
) -> Result<()> {
    let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
    let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    cas_registry::set_watcher_state(&tx, repo_hash, state, error)?;
    tx.commit()?;
    Ok(())
}

/// The per-repo worker loop. Runs until `desired <= applied`
/// (and no request landed in the exit-check race window) or
/// the manager shutdown fires.
async fn worker_loop(mgr: Arc<RepoReconcileManager>, repo_hash: String, notify: Arc<Notify>) {
    debug!(repo_hash = %repo_hash, "reconcile worker started");
    loop {
        // Invariant: every exit path clears `worker_running` under
        // the runtimes mutex. A path that returned without doing
        // so would leave future requests "waking" a worker that no
        // longer exists, wedging the repo until daemon restart.
        if mgr.shutting_down.load(Ordering::SeqCst) {
            let mut runtimes = mgr.lock_runtimes();
            if let Some(rt) = runtimes.get_mut(&repo_hash) {
                rt.worker_running = false;
            }
            return;
        }

        // Arm the notify future BEFORE reading state so we
        // never miss a wake fired between "no work" observation
        // and the exit-check mutex.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Bound state inspection as well as the attempt itself: opening one
        // index connection per worker before acquiring this permit can exhaust
        // the daemon's file-descriptor headroom during startup fan-out.
        let shutdown = mgr.shutdown.notified();
        tokio::pin!(shutdown);
        shutdown.as_mut().enable();
        // Re-check the flag after arming the shutdown future:
        // `notify_waiters` only reaches futures that are already
        // registered, so a notification fired before `enable()`
        // would otherwise be missed.
        if mgr.shutting_down.load(Ordering::SeqCst) {
            let mut runtimes = mgr.lock_runtimes();
            if let Some(rt) = runtimes.get_mut(&repo_hash) {
                rt.worker_running = false;
            }
            return;
        }
        let permit = tokio::select! {
            permit = mgr.attempt_permits.clone().acquire_owned() => {
                match permit {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(
                            repo_hash = %repo_hash,
                            "reconcile attempt semaphore closed; worker exiting"
                        );
                        let mut runtimes = mgr.lock_runtimes();
                        if let Some(rt) = runtimes.get_mut(&repo_hash) {
                            rt.worker_running = false;
                        }
                        return;
                    }
                }
            }
            _ = shutdown.as_mut() => {
                let mut runtimes = mgr.lock_runtimes();
                if let Some(rt) = runtimes.get_mut(&repo_hash) {
                    rt.worker_running = false;
                }
                return;
            }
        };
        if mgr.shutting_down.load(Ordering::SeqCst) {
            drop(permit);
            let mut runtimes = mgr.lock_runtimes();
            if let Some(rt) = runtimes.get_mut(&repo_hash) {
                rt.worker_running = false;
            }
            return;
        }

        // Snapshot the request counter BEFORE reading state: a
        // request from here on either bumps the seq (defeating
        // the idle-exit below) or committed its durable bump
        // early enough for the state read to observe it.
        let observed_seq = {
            let rt = mgr.lock_runtimes();
            rt.get(&repo_hash).map(|r| r.request_seq).unwrap_or(0)
        };

        let state_result = load_state(&mgr, &repo_hash).await;
        if mgr.shutting_down.load(Ordering::SeqCst) {
            drop(permit);
            let mut runtimes = mgr.lock_runtimes();
            if let Some(rt) = runtimes.get_mut(&repo_hash) {
                rt.worker_running = false;
            }
            return;
        }
        let state = match state_result {
            Ok(Some(state)) => state,
            Ok(None) => {
                drop(permit);
                debug!(
                    repo_hash = %repo_hash,
                    "reconcile worker: repo row gone; exiting"
                );
                let mut runtimes = mgr.lock_runtimes();
                if let Some(rt) = runtimes.get_mut(&repo_hash) {
                    rt.worker_running = false;
                }
                return;
            }
            Err(err) => {
                drop(permit);
                warn!(
                    repo_hash = %repo_hash,
                    error = %err,
                    "reconcile worker: failed to load state; retrying after delay"
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    _ = shutdown.as_mut() => {
                        let mut runtimes = mgr.lock_runtimes();
                        if let Some(rt) = runtimes.get_mut(&repo_hash) {
                            rt.worker_running = false;
                        }
                        return;
                    }
                }
            }
        };

        // Interrupted attempt survived from a prior run — the
        // startup-recovery path clears it before spawning
        // workers, but during normal steady-state a concurrent
        // process should never leave `attempt_generation` set.
        // Log loudly and re-notify so recovery can pick it up.
        if state.attempt_generation.is_some() {
            drop(permit);
            warn!(
                repo_hash = %repo_hash,
                attempt = ?state.attempt_generation,
                "reconcile worker: in-flight attempt observed at loop head; \
                 not starting a new one until recovery clears it"
            );
            tokio::select! {
                _ = notified => continue,
                _ = shutdown.as_mut() => {
                    let mut runtimes = mgr.lock_runtimes();
                    if let Some(rt) = runtimes.get_mut(&repo_hash) {
                        rt.worker_running = false;
                    }
                    return;
                }
            }
        }

        // `force_generation` is always a value that some
        // `desired_generation` bump also carried, so comparing it
        // against `applied_generation` detects a not-yet-applied
        // force request without tracking which desired value it
        // rode in on.
        let force_pending = state.force_generation > state.applied_generation;
        if state.desired_generation <= state.applied_generation {
            drop(permit);
            // No work. Try to exit; if a request landed in the
            // race window, loop back.
            if mgr.try_finalize_exit(&repo_hash, observed_seq) {
                debug!(repo_hash = %repo_hash, "reconcile worker idle-exit");
                return;
            }
            continue;
        }

        // A manual force request clears any pre-existing retry deadline when
        // it records the new generation, bypassing backoff exactly once. A
        // failed forced attempt installs a fresh deadline, and that deadline
        // remains authoritative while the same force generation is pending.
        //
        // The permit is released for the wait so a backing-off
        // repo does not occupy a global attempt slot; a notify
        // wake (e.g. a force request that cleared the deadline)
        // re-loops immediately to re-read state.
        if let Some(retry_at) = state.next_retry_at_ns {
            let now = mgr.clock.now_ns();
            if retry_at > now {
                let sleep_ns = retry_at.saturating_sub(now);
                let sleep = Duration::from_nanos(u64::try_from(sleep_ns).unwrap_or(u64::MAX));
                drop(permit);
                tokio::select! {
                    _ = tokio::time::sleep(sleep) => continue,
                    _ = notified => continue,
                    _ = shutdown.as_mut() => {
                        let mut runtimes = mgr.lock_runtimes();
                        if let Some(rt) = runtimes.get_mut(&repo_hash) {
                            rt.worker_running = false;
                        }
                        return;
                    }
                }
            }
        }

        // Attempt the newest desired generation — intermediate
        // generations are coalesced, and `mark_attempt_success`
        // advances `applied` with MAX, so this single attempt
        // settles every request recorded so far. The permit stays
        // held through `run_attempt`, which includes Phase C
        // finalization — the "release after finalization" half of
        // the module contract.
        let generation = state.desired_generation;
        let forced = force_pending;
        let attempt_res = run_attempt(&mgr, &repo_hash, generation, forced).await;
        drop(permit);
        #[cfg(test)]
        mgr.test_attempts_started.fetch_add(1, Ordering::SeqCst);
        match attempt_res {
            Ok(AttemptOutcome::Completed) => info!(
                repo_hash = %repo_hash,
                generation,
                forced,
                "reconcile attempt succeeded"
            ),
            // The lifecycle owner now drives removal of this
            // repository; the worker exits rather than racing the
            // delete with further attempts.
            Ok(AttemptOutcome::RemovalRequested) => {
                info!(
                    repo_hash = %repo_hash,
                    generation,
                    "reconcile worker handed missing root to lifecycle owner; exiting"
                );
                let mut runtimes = mgr.lock_runtimes();
                if let Some(runtime) = runtimes.get_mut(&repo_hash) {
                    runtime.worker_running = false;
                }
                return;
            }
            Err(err) => warn!(
                repo_hash = %repo_hash,
                generation,
                forced,
                error = %err,
                sqlite_code = ?err.sqlite_error_code(),
                sqlite_extended_code = ?err.sqlite_extended_code(),
                "reconcile attempt failed"
            ),
        }
        // Loop back — the DB might have `desired > applied`
        // again if an event landed mid-attempt, or the failure
        // path might have set `next_retry_at_ns`.
    }
}

/// How a single non-erroring attempt concluded (failures travel
/// in the `Result` instead).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptOutcome {
    /// The attempt ran to a durable `mark_attempt_success`.
    Completed,
    /// An ephemeral repo's root vanished; removal was handed to
    /// the lifecycle owner and no attempt was started.
    RemovalRequested,
}

/// Read the durable reconcile row on a blocking thread.
/// `Ok(None)` means the repository (or at least its reconcile
/// row) no longer exists — the worker treats that as "repo
/// removed" and exits.
async fn load_state(
    mgr: &Arc<RepoReconcileManager>,
    repo_hash: &str,
) -> Result<Option<cas_registry::RepoReconcileState>> {
    let cas_data_dir = mgr.cas_data_dir.clone();
    let hash = repo_hash.to_string();
    #[cfg(test)]
    let test_hook = mgr.take_test_load_state_hook_snapshot();
    tokio::task::spawn_blocking(move || -> Result<_> {
        #[cfg(test)]
        if let Some(hook) = test_hook {
            hook(&hash)?;
        }
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        cas_registry::get_reconcile_state(&index, &hash)
    })
    .await
    .map_err(|e| Error::internal_task_panic("reconcile load_state", e))?
}

/// Run one attempt end-to-end. On success, `mark_attempt_success`
/// runs in the same tx as the register work is committed against
/// the store DB — the `index.db` transition is a separate tx
/// (different DB file), but both are fail-closed on error and
/// the durable dirty gap is preserved on failure.
async fn run_attempt(
    mgr: &Arc<RepoReconcileManager>,
    repo_hash: &str,
    generation: i64,
    forced: bool,
) -> Result<AttemptOutcome> {
    // Held for the whole attempt: lifecycle removal drains
    // outstanding leases before touching the store, so the store
    // files stay in place until this attempt finalizes.
    let _lease = match &mgr.lifecycle {
        Some(lifecycle) => Some(lifecycle.acquire_active_by_repo_hash(repo_hash)?),
        None => None,
    };
    // Ephemeral-root check: a non-persistent repository whose
    // root directory vanished is handed to the lifecycle owner
    // for removal instead of burning an attempt that could only
    // fail. Runs before Phase A so the durable attempt slot
    // (`attempt_generation`) is never claimed for it — the global
    // concurrency permit, though, is already held at this point.
    if let Some(lifecycle) = &mgr.lifecycle {
        let cas_data_dir = mgr.cas_data_dir.clone();
        let hash = repo_hash.to_string();
        let missing_ephemeral = tokio::task::spawn_blocking(move || -> Result<bool> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let Some(repo) = cas_registry::lookup_repository(&index, &hash)? else {
                return Ok(false);
            };
            if repo.persistent {
                return Ok(false);
            }
            match std::fs::metadata(&repo.root_path) {
                Ok(_) => Ok(false),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
                Err(err) => Err(err.into()),
            }
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile root lifecycle check", e))??;
        if missing_ephemeral {
            lifecycle.request_removal(RemovalIntent::MissingRoot {
                repo_hash: repo_hash.to_string(),
            })?;
            return Ok(AttemptOutcome::RemovalRequested);
        }
    }
    let now_ns = mgr.clock.now_ns();
    let cas_data_dir = mgr.cas_data_dir.clone();
    let hash = repo_hash.to_string();
    let preferred_alias = {
        let runtimes = mgr.lock_runtimes();
        runtimes
            .get(repo_hash)
            .and_then(|r| r.preferred_alias.clone())
    };
    let job_manager = mgr.job_manager.clone();

    // Phase A: mark_attempt_start on index.db (blocking).
    // This is the on-disk half of the at-most-one-attempt
    // contract: the WHERE clause requires a free attempt slot and
    // `applied < generation <= desired`, and affected-rows != 1
    // rejects the write fail-closed.
    let start_ok = {
        let cas_data_dir = cas_data_dir.clone();
        let hash = hash.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::mark_attempt_start(&tx, &hash, generation, now_ns)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| Error::internal_task_panic("reconcile mark_attempt_start", e))?
    };
    if let Err(err) = start_ok {
        warn!(
            repo_hash = %repo_hash,
            generation,
            error = %err,
            sqlite_code = ?err.sqlite_error_code(),
            sqlite_extended_code = ?err.sqlite_extended_code(),
            "reconcile: mark_attempt_start rejected; another worker or stale state — skipping"
        );
        return Err(err);
    }

    // Phase B: pick alias + run the register/enqueue work.
    #[cfg(test)]
    let test_hook = mgr.take_test_register_hook_snapshot();
    #[cfg(not(test))]
    let test_hook: Option<()> = None;
    let register_result = if let Some(_h) = &test_hook {
        #[cfg(test)]
        {
            let cas_data_dir_hook = cas_data_dir.clone();
            let hash_hook = hash.clone();
            let alias_hook = preferred_alias.clone();
            let hook = test_hook.clone().unwrap();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let index = cas_registry::open(&cas_data_dir_hook.index_db_path())?;
                let aliases = cas_registry::aliases_for_repo(&index, &hash_hook)?;
                let alias =
                    pick_alias(&alias_hook, &aliases).ok_or_else(|| Error::RepoNotFound {
                        alias: format!("no aliases for repo_hash={hash_hook}"),
                    })?;
                (hook)(&hash_hook, &alias, generation, forced)
            })
            .await
            .map_err(|e| Error::internal_task_panic("reconcile test hook", e))?
        }
        #[cfg(not(test))]
        {
            unreachable!()
        }
    } else {
        run_register_work(
            cas_data_dir.clone(),
            hash.clone(),
            preferred_alias,
            generation,
            forced,
            job_manager,
            now_ns,
        )
        .await
        .map(|_| ())
    };

    // Phase C: commit success or failure to index.db.
    let policy = mgr.retry;
    let finalize_res = tokio::task::spawn_blocking({
        let cas_data_dir = cas_data_dir.clone();
        let hash = hash.clone();
        // Only the error text crosses into the blocking closure;
        // the typed `register_result` is kept outside so the
        // caller can propagate it after finalization.
        let register_result = register_result
            .as_ref()
            .map(|_| ())
            .map_err(|e| e.to_string());
        let now_ns_finalize = mgr.clock.now_ns();
        move || -> Result<()> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            match register_result {
                Ok(()) => {
                    cas_registry::mark_attempt_success(&tx, &hash, generation, now_ns_finalize)?;
                }
                Err(err_str) => {
                    // Compute next retry from the pre-update
                    // failure counter — we're bumping it by 1
                    // and the policy exponent uses that value.
                    let current = cas_registry::get_reconcile_state(&tx, &hash)?
                        .map(|s| s.consecutive_failures)
                        .unwrap_or(0);
                    let next_failures = current.saturating_add(1);
                    let next_retry_at = policy.next_retry_at_ns(now_ns_finalize, next_failures);
                    cas_registry::mark_attempt_failure(
                        &tx,
                        &hash,
                        generation,
                        &err_str,
                        next_retry_at,
                    )?;
                }
            }
            tx.commit()?;
            Ok(())
        }
    })
    .await
    .map_err(|e| Error::internal_task_panic("reconcile finalize", e))?;
    // Finalize errors take precedence: if the success/failure
    // transition itself could not be committed, the attempt slot
    // stays claimed and startup recovery later clears it as an
    // interrupted attempt.
    finalize_res?;
    register_result.map(|_| AttemptOutcome::Completed)
}

/// Phase B body: resolve the repo row and alias, open the
/// per-repo store DB, run the register/enqueue pipeline, and
/// verify the publication receipt against what is durably
/// stored. Runs entirely on a blocking thread — this is the
/// long-running part of an attempt.
async fn run_register_work(
    cas_data_dir: Arc<CasDataDir>,
    repo_hash: String,
    preferred_alias: Option<String>,
    generation: i64,
    forced: bool,
    job_manager: Option<Arc<JobManager>>,
    now_ns: i64,
) -> Result<ReconcilePublicationReceipt> {
    tokio::task::spawn_blocking(move || -> Result<ReconcilePublicationReceipt> {
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        let repo = cas_registry::lookup_repository(&index, &repo_hash)?.ok_or_else(|| {
            Error::RepoNotFound {
                alias: repo_hash.clone(),
            }
        })?;
        let aliases = cas_registry::aliases_for_repo(&index, &repo_hash)?;
        let alias = pick_alias(&preferred_alias, &aliases).ok_or_else(|| Error::RepoNotFound {
            alias: format!("no aliases for repo_hash={repo_hash}"),
        })?;
        let store_path = cas_data_dir.store_db_path(&repo_hash);
        let mut conn = cas_store::open_existing(&store_path)?;
        let root = PathBuf::from(&repo.root_path);
        let receipt = run_register(
            &mut conn,
            ReconcileRegistration {
                alias: &alias,
                repo_hash: &repo_hash,
                worktree_path: &root,
                now_ns,
                generation,
                forced,
            },
            &job_manager,
        )?;
        verify_publication_receipt(&conn, &receipt)?;
        Ok(receipt)
    })
    .await
    .map_err(|e| Error::internal_task_panic("reconcile register work", e))?
}

/// Dispatch to the analyzer-enqueue variant when a `JobManager`
/// is wired, else the register-only path. A register that
/// completes without a publication receipt is an internal error:
/// Phase C would otherwise mark success for an attempt that
/// published nothing.
fn run_register(
    conn: &mut rusqlite::Connection,
    request: ReconcileRegistration<'_>,
    job_manager: &Option<Arc<JobManager>>,
) -> Result<ReconcilePublicationReceipt> {
    let outcome = match job_manager.as_deref() {
        Some(jm) => register_repo_reconcile_enqueue_analyzers(conn, request, jm)?,
        None => register_repo_reconcile(
            conn,
            request.worktree_path,
            request.now_ns,
            request.generation,
            request.forced,
        )?,
    };
    outcome.publication.ok_or_else(|| {
        Error::Internal("reconcile register completed without a publication receipt".into())
    })
}

/// Re-read the anchor the receipt claims to have published and
/// require the durable manifest id and reconcile generation to
/// match. A one-shot check just before Phase C: it rejects a
/// receipt that is already missing or mismatched at verification
/// time (it cannot rule out clobbering after the check).
fn verify_publication_receipt(
    conn: &rusqlite::Connection,
    receipt: &ReconcilePublicationReceipt,
) -> Result<()> {
    let durable = crate::anchor::get(conn, &receipt.anchor)?.ok_or_else(|| {
        Error::Internal(format!(
            "reconcile publication anchor `{}` disappeared before finalize",
            receipt.anchor.as_str()
        ))
    })?;
    if durable.manifest_id != receipt.manifest_id
        || durable.reconcile_generation != Some(receipt.generation)
    {
        return Err(Error::Internal(format!(
            "reconcile publication receipt mismatch for `{}`: expected manifest={} generation={}, got manifest={} generation={:?}",
            receipt.anchor.as_str(),
            receipt.manifest_id.0,
            receipt.generation,
            durable.manifest_id.0,
            durable.reconcile_generation
        )));
    }
    Ok(())
}

/// Prefer the alias that triggered the request if it still maps
/// to this repo; otherwise fall back to the first alias in the
/// (already sorted) `aliases_for_repo` result. `None` only when
/// the repo has no aliases at all.
fn pick_alias(preferred: &Option<String>, aliases: &[String]) -> Option<String> {
    if let Some(p) = preferred
        && aliases.iter().any(|a| a == p)
    {
        return Some(p.clone());
    }
    aliases.first().cloned()
}

#[cfg(test)]
impl RepoReconcileManager {
    /// Test hook: monotonic count of attempts driven through
    /// the worker loop.
    pub fn test_attempts_started(&self) -> i64 {
        self.test_attempts_started.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests;
