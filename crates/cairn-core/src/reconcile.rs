//! `RepoReconcileManager` — durable, repo_hash-owned driver of the
//! reindex state machine.
//!
//! Every reindex intent (watcher event, manual reindex, startup
//! recovery, retry) first bumps `repo_reconcile_state.desired_generation`
//! (or `force_generation`) durably, and only then wakes an
//! in-process worker to execute the attempt. A crash between the
//! bump and the attempt leaves the gap `desired > applied` durable,
//! so the next startup can resume via
//! [`cas::registry::recover_interrupted_attempts`] + immediate wake.
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
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::cas::registry::{self as cas_registry, WatcherState};
use crate::cas::store as cas_store;
use crate::jobs::JobManager;
use crate::paths::CasDataDir;
use crate::register::{register_repo_enqueue_analyzers, register_repo_force_analyzers_enqueue};
use crate::{Error, Result};

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

/// Central reindex state-machine driver — one instance per
/// daemon. See the module-level docs for the concurrency
/// contract.
pub struct RepoReconcileManager {
    cas_data_dir: Arc<CasDataDir>,
    job_manager: Option<Arc<JobManager>>,
    clock: Arc<dyn Clock>,
    retry: RetryPolicy,
    runtimes: Mutex<HashMap<String, RepoRuntime>>,
    shutdown: Arc<Notify>,
    shutting_down: std::sync::atomic::AtomicBool,
    live_workers: AtomicUsize,
    workers_idle: Arc<Notify>,
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
        Self::with_config(
            cas_data_dir,
            job_manager,
            Arc::new(SystemClock),
            RetryPolicy::default(),
        )
    }

    /// Full constructor exposed for tests. Production uses
    /// [`new`].
    #[must_use]
    pub fn with_config(
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Option<Arc<JobManager>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            cas_data_dir,
            job_manager,
            clock,
            retry,
            runtimes: Mutex::new(HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            live_workers: AtomicUsize::new(0),
            workers_idle: Arc::new(Notify::new()),
            #[cfg(test)]
            test_attempts_started: AtomicI64::new(0),
            #[cfg(test)]
            test_register_hook: std::sync::Mutex::new(None),
        })
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

    async fn request_dirty(
        self: &Arc<Self>,
        repo_hash: String,
        alias: Option<String>,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
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

    async fn request_force(
        self: &Arc<Self>,
        repo_hash: String,
        alias: Option<String>,
        trigger: ReconcileTrigger,
    ) -> Result<ReconcileRequestOutcome> {
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

    /// On startup: clear every non-NULL `attempt_generation` and
    /// annotate `last_error`, then wake the workers for the
    /// affected repos so they retry immediately.
    pub async fn recover_interrupted_attempts(self: &Arc<Self>) -> Result<Vec<String>> {
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
        for h in &hashes {
            self.wake_or_spawn(h, None);
        }
        Ok(hashes)
    }

    /// Wake workers for every repo the DB says still has
    /// `desired > applied`. Called from startup after
    /// [`recover_interrupted_attempts`]; safe to call at any
    /// time.
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

    /// Persist watcher lifecycle state. Fails-closed if
    /// `repo_hash` has no reconcile state row (Phase 1
    /// affected-rows == 1 contract).
    pub async fn set_watcher_state_by_repo_hash(
        &self,
        repo_hash: String,
        state: WatcherState,
        error: Option<String>,
    ) -> Result<()> {
        let cas_data_dir = self.cas_data_dir.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::set_watcher_state(&tx, &repo_hash, state, error.as_deref())?;
            tx.commit()?;
            Ok(())
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
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.live_workers.load(Ordering::SeqCst) == 0 {
                return;
            }
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

/// The per-repo worker loop. Runs until `desired <= applied`
/// (and no request landed in the exit-check race window) or
/// the manager shutdown fires.
async fn worker_loop(mgr: Arc<RepoReconcileManager>, repo_hash: String, notify: Arc<Notify>) {
    debug!(repo_hash = %repo_hash, "reconcile worker started");
    loop {
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

        let observed_seq = {
            let rt = mgr.lock_runtimes();
            rt.get(&repo_hash).map(|r| r.request_seq).unwrap_or(0)
        };

        let state = match load_state(&mgr, &repo_hash).await {
            Ok(Some(state)) => state,
            Ok(None) => {
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
                warn!(
                    repo_hash = %repo_hash,
                    error = %err,
                    "reconcile worker: failed to load state; retrying after delay"
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    _ = mgr.shutdown.notified() => {
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
            warn!(
                repo_hash = %repo_hash,
                attempt = ?state.attempt_generation,
                "reconcile worker: in-flight attempt observed at loop head; \
                 not starting a new one until recovery clears it"
            );
            tokio::select! {
                _ = notified => continue,
                _ = mgr.shutdown.notified() => {
                    let mut runtimes = mgr.lock_runtimes();
                    if let Some(rt) = runtimes.get_mut(&repo_hash) {
                        rt.worker_running = false;
                    }
                    return;
                }
            }
        }

        let force_pending = state.force_generation > state.applied_generation;
        if state.desired_generation <= state.applied_generation {
            // No work. Try to exit; if a request landed in the
            // race window, loop back.
            if mgr.try_finalize_exit(&repo_hash, observed_seq) {
                debug!(repo_hash = %repo_hash, "reconcile worker idle-exit");
                return;
            }
            continue;
        }

        // Retry backoff — honoured only if no force is pending.
        // A manual reindex should not wait for the retry timer.
        if !force_pending && let Some(retry_at) = state.next_retry_at_ns {
            let now = mgr.clock.now_ns();
            if retry_at > now {
                let sleep_ns = retry_at.saturating_sub(now);
                let sleep = Duration::from_nanos(u64::try_from(sleep_ns).unwrap_or(u64::MAX));
                tokio::select! {
                    _ = tokio::time::sleep(sleep) => continue,
                    _ = notified => continue,
                    _ = mgr.shutdown.notified() => {
                        let mut runtimes = mgr.lock_runtimes();
                        if let Some(rt) = runtimes.get_mut(&repo_hash) {
                            rt.worker_running = false;
                        }
                        return;
                    }
                }
            }
        }

        let generation = state.desired_generation;
        let forced = force_pending;
        let attempt_res = run_attempt(&mgr, &repo_hash, generation, forced).await;
        #[cfg(test)]
        mgr.test_attempts_started.fetch_add(1, Ordering::SeqCst);
        match attempt_res {
            Ok(()) => info!(
                repo_hash = %repo_hash,
                generation,
                forced,
                "reconcile attempt succeeded"
            ),
            Err(err) => warn!(
                repo_hash = %repo_hash,
                generation,
                forced,
                error = %err,
                "reconcile attempt failed"
            ),
        }
        // Loop back — the DB might have `desired > applied`
        // again if an event landed mid-attempt, or the failure
        // path might have set `next_retry_at_ns`.
    }
}

async fn load_state(
    mgr: &Arc<RepoReconcileManager>,
    repo_hash: &str,
) -> Result<Option<cas_registry::RepoReconcileState>> {
    let cas_data_dir = mgr.cas_data_dir.clone();
    let hash = repo_hash.to_string();
    tokio::task::spawn_blocking(move || -> Result<_> {
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
) -> Result<()> {
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
            forced,
            job_manager,
            now_ns,
        )
        .await
    };

    // Phase C: commit success or failure to index.db.
    let policy = mgr.retry;
    let finalize_res = tokio::task::spawn_blocking({
        let cas_data_dir = cas_data_dir.clone();
        let hash = hash.clone();
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
    finalize_res?;
    register_result.map(|_| ())
}

async fn run_register_work(
    cas_data_dir: Arc<CasDataDir>,
    repo_hash: String,
    preferred_alias: Option<String>,
    forced: bool,
    job_manager: Option<Arc<JobManager>>,
    now_ns: i64,
) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
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
        let mut conn = cas_store::open(&store_path)?;
        let root = PathBuf::from(&repo.root_path);
        run_register(
            &mut conn,
            &alias,
            &repo_hash,
            &root,
            forced,
            &job_manager,
            now_ns,
        )
    })
    .await
    .map_err(|e| Error::internal_task_panic("reconcile register work", e))?
}

fn run_register(
    conn: &mut rusqlite::Connection,
    alias: &str,
    repo_hash: &str,
    worktree_path: &Path,
    forced: bool,
    job_manager: &Option<Arc<JobManager>>,
    now_ns: i64,
) -> Result<()> {
    match job_manager.as_deref() {
        Some(jm) => {
            if forced {
                register_repo_force_analyzers_enqueue(
                    conn,
                    alias,
                    repo_hash,
                    worktree_path,
                    now_ns,
                    jm,
                )?;
            } else {
                register_repo_enqueue_analyzers(conn, alias, repo_hash, worktree_path, now_ns, jm)?;
            }
        }
        None => {
            crate::register::register_repo(conn, worktree_path, now_ns)?;
        }
    }
    Ok(())
}

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
