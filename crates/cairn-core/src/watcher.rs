//! Daemon-owned filesystem watchers, keyed by canonical
//! `repo_hash`.
//!
//! The control socket owns registration; this module owns
//! watcher lifetime. Each watched repository keeps one
//! `cairn-watch` handle alive and feeds every raw event into
//! [`RepoReconcileManager::request_dirty_by_repo_hash`] before
//! the debounce sleep — so the durable dirty gap
//! (`desired > applied`) is recorded even if the daemon dies
//! before the reindex actually runs.
//!
//! The internal handle map is keyed by `repo_hash`, not by
//! alias. Two aliases pointing to the same on-disk repo share
//! one OS watcher and one reconcile driver worker.

use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use cairn_watch::{WatchBackend, WatchEvent, WatcherHandle, watch_repo_with_backend};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::cas::registry::{self as cas_registry, WatcherState};
use crate::paths::CasDataDir;
use crate::reconcile::{ReconcileTrigger, RepoReconcileManager};
use crate::{Error, Result};

/// Debounce interval handed to the `cairn-watch` backend: raw OS
/// notifications are batched for this long before they surface on
/// the event channel.
const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
/// Event channel capacity. One slot gives edge-triggered semantics:
/// `cairn-watch` sends with `try_send` and treats a full channel as
/// an already-pending edge, so a slow dispatcher never builds a
/// backlog — any queued event only means "the repo is dirty".
const WATCH_EDGE_CAPACITY: usize = 1;
/// Fixed (non-sliding) coalescing window. After the first event is
/// received, everything arriving before the deadline collapses into
/// a single dirty request — so a continuous event stream still
/// yields one request per window instead of being deferred forever.
const WATCH_COALESCE_WINDOW: Duration = Duration::from_millis(500);
/// Initial backoff for retrying a failed
/// `request_dirty_by_repo_hash` (e.g. the repo row does not exist
/// yet). Doubles per attempt up to [`WATCH_REQUEST_RETRY_MAX`].
const WATCH_REQUEST_RETRY_INITIAL: Duration = Duration::from_millis(25);
/// Cap for the dirty-request retry backoff.
const WATCH_REQUEST_RETRY_MAX: Duration = Duration::from_secs(1);

/// Keeps one live watcher per registered `repo_hash`.
pub struct WatchManager {
    cas_data_dir: Arc<CasDataDir>,
    /// `None` means log-only mode: events are coalesced and logged
    /// but never recorded as durable dirty generations.
    reconcile: Option<Arc<RepoReconcileManager>>,
    backend: WatchBackend,
    #[cfg(test)]
    fail_watcher_start: bool,
    #[cfg(test)]
    dropped_watchers: Arc<AtomicUsize>,
    /// Monotonic arm-id source; ids are unique within this watcher
    /// registry and never reused (see [`Self::allocate_arm_id`]).
    next_arm_id: AtomicU64,
    /// Live watchers keyed by canonical `repo_hash`, never alias.
    watchers: Mutex<HashMap<String, RepoWatcher>>,
}

/// One live watch: the OS watcher handle plus the tokio task that
/// drains its event channel. Dropping this stops both.
struct RepoWatcher {
    /// Identity token; [`WatchArmReceipt`] rollback removes a
    /// watcher only when this id matches the receipt.
    arm_id: u64,
    /// Held only to keep the OS watcher alive. Dropping it stops
    /// event delivery and closes the dispatcher's channel.
    _handle: WatcherHandle,
    task: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    drop_counter: Arc<AtomicUsize>,
}

/// Outcome of an arm request against the watcher registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchArmDisposition {
    /// A watcher for the repo already existed; nothing was created
    /// and rolling back the receipt is a no-op.
    AlreadyArmed,
    /// A new watcher was created and published for the repo.
    Armed,
}

/// Opaque ownership receipt for a watcher arm operation. Rollback only
/// removes the exact watcher created by this receipt, never a replacement.
#[derive(Debug)]
pub struct WatchArmReceipt {
    repo_hash: String,
    arm_id: u64,
    disposition: WatchArmDisposition,
}

impl WatchArmReceipt {
    #[must_use]
    pub fn disposition(&self) -> WatchArmDisposition {
        self.disposition
    }
}

/// One repo whose watcher could not be armed during startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchStartFailure {
    pub repo_hash: String,
    pub error: String,
}

/// Per-repo outcome of [`WatchManager::start_registered`]. A failed
/// entry is already persisted as `WatcherState::Failed`; the report
/// exists so the daemon can log/act without re-reading the DB.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WatchStartupReport {
    pub armed: Vec<String>,
    pub failed: Vec<WatchStartFailure>,
}

impl Drop for RepoWatcher {
    fn drop(&mut self) {
        #[cfg(test)]
        self.drop_counter.fetch_add(1, Ordering::SeqCst);
        // Abort promptly instead of waiting for the channel-closed
        // path: the dispatcher may be sleeping inside its retry
        // backoff, and it must not outlive the watcher entry.
        self.task.abort();
    }
}

impl WatchManager {
    /// Build a manager with no reconcile driver. Watcher events
    /// are logged but not routed anywhere — useful in tests that
    /// only care about handle lifetime.
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Self {
        Self::build(cas_data_dir, WatchBackend::Recommended, None)
    }

    /// Production constructor: events are routed into
    /// `reconcile.request_dirty_by_repo_hash`.
    #[must_use]
    pub fn with_reconcile(
        cas_data_dir: Arc<CasDataDir>,
        reconcile: Arc<RepoReconcileManager>,
    ) -> Self {
        Self::build(cas_data_dir, WatchBackend::Recommended, Some(reconcile))
    }

    #[must_use]
    pub fn with_backend(cas_data_dir: Arc<CasDataDir>, backend: WatchBackend) -> Self {
        Self::build(cas_data_dir, backend, None)
    }

    #[must_use]
    pub fn with_backend_and_reconcile(
        cas_data_dir: Arc<CasDataDir>,
        backend: WatchBackend,
        reconcile: Arc<RepoReconcileManager>,
    ) -> Self {
        Self::build(cas_data_dir, backend, Some(reconcile))
    }

    fn build(
        cas_data_dir: Arc<CasDataDir>,
        backend: WatchBackend,
        reconcile: Option<Arc<RepoReconcileManager>>,
    ) -> Self {
        Self {
            cas_data_dir,
            reconcile,
            backend,
            #[cfg(test)]
            fail_watcher_start: false,
            #[cfg(test)]
            dropped_watchers: Arc::new(AtomicUsize::new(0)),
            next_arm_id: AtomicU64::new(1),
            watchers: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_failing_watcher(cas_data_dir: Arc<CasDataDir>) -> Self {
        Self {
            cas_data_dir,
            backend: WatchBackend::Poll,
            reconcile: None,
            fail_watcher_start: true,
            dropped_watchers: Arc::new(AtomicUsize::new(0)),
            next_arm_id: AtomicU64::new(1),
            watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Start watchers for every canonical repository. Iterates
    /// `list_repositories`, not `list_all`, so two aliases for
    /// one repo do not open two OS watchers.
    ///
    /// Individual watcher setup failures are logged and the
    /// daemon keeps starting; per-repo failures are persisted
    /// via [`RepoReconcileManager::set_watcher_state_by_repo_hash`]
    /// so `repo_status` / doctor can observe them.
    pub fn start_registered(&self) -> Result<WatchStartupReport> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let mut report = WatchStartupReport::default();
        for repo in cas_registry::list_repositories(&index)? {
            // A repo with a persisted removal intent must not be
            // re-armed; the lifecycle owner is tearing it down.
            if repo.removal_request.is_some() {
                continue;
            }
            match self
                .arm_repository_if_absent(repo.repo_hash.clone(), PathBuf::from(repo.root_path))
            {
                Ok(_) => {
                    self.persist_watcher_state(&repo.repo_hash, WatcherState::Active, None)?;
                    report.armed.push(repo.repo_hash)
                }
                Err(err) => {
                    self.persist_watcher_state(
                        &repo.repo_hash,
                        WatcherState::Failed,
                        Some(&err.to_string()),
                    )?;
                    warn!(
                        repo_hash = %repo.repo_hash,
                        error = %err,
                        "failed to start repo watcher"
                    );
                    report.failed.push(WatchStartFailure {
                        repo_hash: repo.repo_hash,
                        error: err.to_string(),
                    });
                }
            }
        }
        Ok(report)
    }

    /// Arm a repository only when no watcher is already published for its
    /// canonical owner. The check and insertion are linearized under the
    /// watcher registry mutex so concurrent registrations cannot replace one
    /// another accidentally.
    pub fn arm_repository_if_absent(
        &self,
        repo_hash: String,
        root_path: PathBuf,
    ) -> Result<WatchArmReceipt> {
        let mut watchers = self.lock_recovering();
        if let Some(existing) = watchers.get(&repo_hash) {
            return Ok(WatchArmReceipt {
                repo_hash,
                arm_id: existing.arm_id,
                disposition: WatchArmDisposition::AlreadyArmed,
            });
        }
        let arm_id = self.allocate_arm_id()?;
        let watcher = self.start_watcher(&repo_hash, &root_path, arm_id)?;
        watchers.insert(repo_hash.clone(), watcher);
        drop(watchers);
        info!(
            repo_hash = %repo_hash,
            path = %root_path.display(),
            "repo watcher armed"
        );
        Ok(WatchArmReceipt {
            repo_hash,
            arm_id,
            disposition: WatchArmDisposition::Armed,
        })
    }

    /// Roll back only the watcher created by `receipt`. A stale receipt is a
    /// no-op after another operation replaced the watcher.
    pub fn rollback_arm(&self, receipt: WatchArmReceipt) {
        if receipt.disposition != WatchArmDisposition::Armed {
            return;
        }
        let mut watchers = self.lock_recovering();
        let matches = watchers
            .get(&receipt.repo_hash)
            .is_some_and(|watcher| watcher.arm_id == receipt.arm_id);
        if matches {
            watchers.remove(&receipt.repo_hash);
            info!(repo_hash = %receipt.repo_hash, "repo watcher arm rolled back");
        }
    }

    /// Start or replace the watcher for one canonical repo.
    ///
    /// Persists `WatcherState::Active` on success and
    /// `WatcherState::Failed` on error via the reconcile driver
    /// (if one is wired). Failure of the new watcher does not
    /// drop the existing one — an operator-observable failed
    /// state is preferable to a silently blinded repo.
    pub fn watch_repository(&self, repo_hash: String, root_path: PathBuf) -> Result<()> {
        let arm_id = self.allocate_arm_id()?;
        let watcher = self
            .start_watcher(&repo_hash, &root_path, arm_id)
            .inspect_err(|err| {
                self.record_watcher_failed(&repo_hash, &err.to_string());
            })?;

        self.persist_watcher_state(&repo_hash, WatcherState::Active, None)?;

        // Only swap after setup succeeds; a start failure must not
        // blind an existing watched repo.
        self.lock_recovering().insert(repo_hash.clone(), watcher);
        info!(
            repo_hash = %repo_hash,
            path = %root_path.display(),
            "repo watcher started"
        );
        Ok(())
    }

    /// Create the OS watcher and spawn its dispatcher task. Does
    /// not touch the registry map — callers decide insert versus
    /// replace semantics after the start has already succeeded.
    fn start_watcher(
        &self,
        repo_hash: &str,
        root_path: &std::path::Path,
        arm_id: u64,
    ) -> Result<RepoWatcher> {
        #[cfg(test)]
        if self.fail_watcher_start {
            return Err(Error::InvalidArgument(
                "injected watcher start failure".into(),
            ));
        }

        let (tx, rx) = mpsc::channel(WATCH_EDGE_CAPACITY);
        let handle =
            watch_repo_with_backend(root_path, WATCH_DEBOUNCE, tx, self.backend).map_err(|e| {
                Error::InvalidArgument(format!("watch_repo {}: {e}", root_path.display()))
            })?;
        let task = tokio::spawn(dispatch_events(
            self.reconcile.clone(),
            repo_hash.to_string(),
            rx,
        ));
        Ok(RepoWatcher {
            arm_id,
            _handle: handle,
            task,
            #[cfg(test)]
            drop_counter: self.dropped_watchers.clone(),
        })
    }

    /// Allocate a monotonically increasing arm id, unique within
    /// this manager's registry. Ids are never reused: on
    /// (practically unreachable) u64 exhaustion this errors instead
    /// of wrapping, because a wrapped id could let a stale receipt
    /// remove a newer watcher.
    fn allocate_arm_id(&self) -> Result<u64> {
        self.next_arm_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::Internal("watcher arm id overflow".into()))
    }

    /// Compatibility wrapper: resolves alias → repo_hash and
    /// forwards to [`watch_repository`]. Kept because existing
    /// control-plane code still speaks in alias terms.
    pub fn watch_alias(&self, alias: String, root_path: PathBuf) -> Result<()> {
        let repo_hash = {
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            match cas_registry::lookup_by_alias(&index, &alias)? {
                Some(entry) => entry.repo_hash,
                None => {
                    return Err(Error::RepoNotFound { alias });
                }
            }
        };
        self.watch_repository(repo_hash, root_path)
    }

    /// Drop the watcher for `repo_hash`, stopping the OS watcher
    /// and aborting its dispatcher task. Idempotent: unknown hashes
    /// are a silent no-op.
    pub fn unwatch_repository(&self, repo_hash: &str) {
        if self.lock_recovering().remove(repo_hash).is_some() {
            info!(repo_hash = %repo_hash, "repo watcher stopped");
        }
    }

    /// Compatibility wrapper: resolves alias → repo_hash and
    /// forwards to [`unwatch_repository`].
    pub fn unwatch_alias(&self, alias: &str) {
        let repo_hash = {
            let Ok(index) = cas_registry::open(&self.cas_data_dir.index_db_path()) else {
                return;
            };
            match cas_registry::lookup_by_alias(&index, alias) {
                Ok(Some(entry)) => entry.repo_hash,
                _ => return,
            }
        };
        self.unwatch_repository(&repo_hash);
    }

    pub fn is_watching_repository(&self, repo_hash: &str) -> bool {
        self.lock_recovering().contains_key(repo_hash)
    }

    /// Compatibility wrapper — resolves alias → repo_hash.
    pub fn is_watching_alias(&self, alias: &str) -> bool {
        let repo_hash = {
            let Ok(index) = cas_registry::open(&self.cas_data_dir.index_db_path()) else {
                return false;
            };
            match cas_registry::lookup_by_alias(&index, alias) {
                Ok(Some(entry)) => entry.repo_hash,
                _ => return false,
            }
        };
        self.is_watching_repository(&repo_hash)
    }

    #[cfg(test)]
    pub fn set_fail_watcher_start(&mut self, fail: bool) {
        self.fail_watcher_start = fail;
    }

    #[cfg(test)]
    fn dropped_watcher_count(&self) -> usize {
        self.dropped_watchers.load(Ordering::SeqCst)
    }

    /// Record the watcher state on the durable reconcile row.
    /// No-op `Ok(())` when no reconcile driver is wired (log-only
    /// managers have nowhere durable to write).
    fn persist_watcher_state(
        &self,
        repo_hash: &str,
        state: WatcherState,
        error: Option<&str>,
    ) -> Result<()> {
        self.reconcile.as_ref().map_or(Ok(()), |reconcile| {
            reconcile.set_watcher_state_immediate(repo_hash, state, error)
        })
    }

    /// Best-effort failure path: persist `WatcherState::Failed`,
    /// then bump a dirty generation so the durable gap covers
    /// whatever changes the blinded repo can no longer observe.
    /// Errors are logged at debug level only — the repo may simply
    /// not be registered yet.
    fn record_watcher_failed(&self, repo_hash: &str, msg: &str) {
        if let Err(err) = self.persist_watcher_state(repo_hash, WatcherState::Failed, Some(msg)) {
            debug!(
                repo_hash = %repo_hash,
                error = %err,
                "failed to persist watcher failed state (repo may not be registered)"
            );
            return;
        }
        let Some(reconcile) = self.reconcile.clone() else {
            return;
        };
        let hash = repo_hash.to_string();
        tokio::spawn(async move {
            if let Err(err) = reconcile
                .request_dirty_by_repo_hash(hash.clone(), ReconcileTrigger::WatchEvent)
                .await
            {
                debug!(
                    repo_hash = %hash,
                    error = %err,
                    "failed to wake reconcile after watcher failure"
                );
            }
        });
    }

    /// Lock the watcher registry, recovering from mutex poison.
    /// Safe because every critical section performs a single
    /// insert/remove/lookup — a panic cannot leave the map in a
    /// half-updated state worth discarding.
    fn lock_recovering(&self) -> MutexGuard<'_, HashMap<String, RepoWatcher>> {
        self.watchers.lock().unwrap_or_else(|poisoned| {
            warn!("watch manager mutex poisoned; recovering watcher registry");
            poisoned.into_inner()
        })
    }
}

/// Consume a bounded edge signal and record at most one durable
/// dirty generation per fixed coalescing window. The pending edge
/// is not forgotten until `request_dirty` succeeds.
async fn dispatch_events(
    reconcile: Option<Arc<RepoReconcileManager>>,
    repo_hash: String,
    mut rx: mpsc::Receiver<WatchEvent>,
) {
    while let Some(first_event) = rx.recv().await {
        let mut event_count = 1_u64;
        let mut channel_closed = false;
        debug!(repo_hash = %repo_hash, ?first_event, "repo watcher event edge");
        // Fixed deadline anchored to the *first* event: later events
        // are absorbed but never extend the window, so a continuous
        // stream cannot postpone the dirty request indefinitely.
        let deadline = tokio::time::Instant::now() + WATCH_COALESCE_WINDOW;
        loop {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => break,
                event = rx.recv() => match event {
                    Some(event) => {
                        event_count = event_count.saturating_add(1);
                        debug!(repo_hash = %repo_hash, ?event, "coalesced repo watcher event");
                    }
                    None => {
                        channel_closed = true;
                        break;
                    }
                }
            }
        }

        // Log-only mode (no reconcile driver): the edge was observed
        // and logged, nothing durable to record.
        let Some(reconcile) = reconcile.clone() else {
            if channel_closed {
                break;
            }
            continue;
        };

        // The coalesced edge must not be lost: retry with capped
        // exponential backoff until the dirty bump lands. This
        // covers the registration race where the repo row does not
        // exist yet when the first filesystem event arrives.
        let mut retry_delay = WATCH_REQUEST_RETRY_INITIAL;
        loop {
            match reconcile
                .request_dirty_by_repo_hash(repo_hash.clone(), ReconcileTrigger::WatchEvent)
                .await
            {
                Ok(outcome) => {
                    debug!(
                        repo_hash = %repo_hash,
                        generation = outcome.generation,
                        events = event_count,
                        "watcher request recorded"
                    );
                    break;
                }
                Err(err) => {
                    warn!(
                        repo_hash = %repo_hash,
                        error = %err,
                        retry_ms = retry_delay.as_millis(),
                        "watcher failed to record dirty request; retaining pending edge"
                    );
                    // Sender gone means the watcher is being torn
                    // down (or the daemon is stopping); stop
                    // retrying rather than leak the task. Startup
                    // reconcile priming re-covers any edge dropped
                    // here on the next daemon run.
                    if channel_closed || rx.is_closed() {
                        return;
                    }
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(WATCH_REQUEST_RETRY_MAX);
                }
            }
        }
        if channel_closed {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{RegistrationReconcilePolicy, RepoLifecycleManager};

    fn test_event() -> WatchEvent {
        WatchEvent::File {
            path: PathBuf::from("src/lib.rs"),
            change: cairn_watch::FileChange::Touched,
        }
    }

    fn desired_generation(cas: &CasDataDir, repo_hash: &str) -> Option<i64> {
        let index = cas_registry::open(&cas.index_db_path()).unwrap();
        cas_registry::get_reconcile_state(&index, repo_hash)
            .unwrap()
            .map(|state| state.desired_generation)
    }

    async fn wait_for_desired(cas: &CasDataDir, repo_hash: &str, expected: i64) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if desired_generation(cas, repo_hash).is_some_and(|value| value >= expected) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("desired generation did not reach {expected}"));
    }

    fn seed_repo(cas: &CasDataDir, repo_hash: &str, root: &std::path::Path) {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "demo", &root.to_string_lossy(), repo_hash, 1).unwrap();
        tx.commit().unwrap();
    }

    #[tokio::test]
    async fn watch_repository_replaces_existing_watcher_after_new_start_succeeds() {
        let old_root = tempfile::tempdir().unwrap();
        let new_root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", old_root.path());
        let manager = WatchManager::with_backend(cas, WatchBackend::Poll);

        manager
            .watch_repository("h".into(), old_root.path().to_path_buf())
            .unwrap();
        assert!(manager.is_watching_repository("h"));
        assert_eq!(manager.dropped_watcher_count(), 0);

        manager
            .watch_repository("h".into(), new_root.path().to_path_buf())
            .unwrap();

        assert!(manager.is_watching_repository("h"));
        assert_eq!(manager.dropped_watcher_count(), 1);
    }

    #[tokio::test]
    async fn watch_repository_keeps_existing_watcher_when_new_start_fails() {
        let old_root = tempfile::tempdir().unwrap();
        let new_root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", old_root.path());
        let mut manager = WatchManager::with_backend(cas, WatchBackend::Poll);

        manager
            .watch_repository("h".into(), old_root.path().to_path_buf())
            .unwrap();
        manager.set_fail_watcher_start(true);

        let err = manager
            .watch_repository("h".into(), new_root.path().to_path_buf())
            .unwrap_err();

        assert!(err.to_string().contains("injected watcher start failure"));
        assert!(manager.is_watching_repository("h"));
        assert_eq!(manager.dropped_watcher_count(), 0);
    }

    #[tokio::test]
    async fn arm_if_absent_reuses_existing_watcher_without_replacement() {
        let root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", root.path());
        let manager = WatchManager::with_backend(cas, WatchBackend::Poll);

        let first = manager
            .arm_repository_if_absent("h".into(), root.path().to_path_buf())
            .unwrap();
        let second = manager
            .arm_repository_if_absent("h".into(), root.path().to_path_buf())
            .unwrap();

        assert_eq!(first.disposition(), WatchArmDisposition::Armed);
        assert_eq!(second.disposition(), WatchArmDisposition::AlreadyArmed);
        assert_eq!(manager.dropped_watcher_count(), 0);
        manager.rollback_arm(second);
        assert!(manager.is_watching_repository("h"));
    }

    #[tokio::test]
    async fn rollback_arm_only_removes_the_matching_arm_id() {
        let first_root = tempfile::tempdir().unwrap();
        let replacement_root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", first_root.path());
        let manager = WatchManager::with_backend(cas, WatchBackend::Poll);

        let stale = manager
            .arm_repository_if_absent("h".into(), first_root.path().to_path_buf())
            .unwrap();
        manager
            .watch_repository("h".into(), replacement_root.path().to_path_buf())
            .unwrap();
        assert_eq!(manager.dropped_watcher_count(), 1);

        manager.rollback_arm(stale);
        assert!(manager.is_watching_repository("h"));
        assert_eq!(
            manager.dropped_watcher_count(),
            1,
            "a stale receipt must not remove the replacement"
        );

        manager.unwatch_repository("h");
        assert_eq!(manager.dropped_watcher_count(), 2);
    }

    #[tokio::test]
    async fn matching_arm_receipt_can_roll_back_its_watcher() {
        let root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", root.path());
        let manager = WatchManager::with_backend(cas, WatchBackend::Poll);

        let receipt = manager
            .arm_repository_if_absent("h".into(), root.path().to_path_buf())
            .unwrap();
        manager.rollback_arm(receipt);

        assert!(!manager.is_watching_repository("h"));
        assert_eq!(manager.dropped_watcher_count(), 1);
    }

    #[tokio::test]
    async fn start_registered_reports_each_success_and_failure() {
        let good_root = tempfile::tempdir().unwrap();
        let missing_parent = tempfile::tempdir().unwrap();
        let missing_root = missing_parent.path().join("gone");
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        {
            let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert_repository(&tx, "good", &good_root.path().to_string_lossy(), 1)
                .unwrap();
            cas_registry::upsert_repository(&tx, "missing", &missing_root.to_string_lossy(), 1)
                .unwrap();
            tx.commit().unwrap();
        }
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        reconcile.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        let manager = WatchManager::with_backend_and_reconcile(
            cas.clone(),
            WatchBackend::Poll,
            reconcile.clone(),
        );

        let report = manager.start_registered().unwrap();

        assert_eq!(report.armed, vec!["good"]);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].repo_hash, "missing");
        assert!(manager.is_watching_repository("good"));
        assert!(!manager.is_watching_repository("missing"));
        let index = cas_registry::open(&cas.index_db_path()).unwrap();
        let good = cas_registry::get_reconcile_state(&index, "good")
            .unwrap()
            .unwrap();
        let missing = cas_registry::get_reconcile_state(&index, "missing")
            .unwrap()
            .unwrap();
        assert_eq!(good.watcher_state, WatcherState::Active);
        assert_eq!(missing.watcher_state, WatcherState::Failed);
        assert!(missing.watcher_error.is_some());

        let primed = reconcile.prime_startup_reconcile(Vec::new()).await.unwrap();
        assert_eq!(
            primed.primed,
            vec![("good".to_string(), 1), ("missing".to_string(), 1)]
        );
        wait_for_desired(&cas, "good", 1).await;
        wait_for_desired(&cas, "missing", 1).await;
        reconcile.shutdown(Duration::from_secs(1)).await;
    }

    #[test]
    fn watcher_registry_recovers_after_mutex_poison() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = WatchManager::with_backend(
            Arc::new(CasDataDir::with_root(tmp.path().to_path_buf())),
            WatchBackend::Poll,
        );

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = manager.watchers.lock().unwrap();
            panic!("poison watcher registry");
        }));

        assert!(!manager.is_watching_repository("missing"));
    }

    #[tokio::test]
    async fn dispatch_coalesces_events_by_fixed_window() {
        let root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", root.path());
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        reconcile.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        let (tx, rx) = mpsc::channel(WATCH_EDGE_CAPACITY);
        let task = tokio::spawn(dispatch_events(Some(reconcile), "h".into(), rx));

        tx.send(test_event()).await.unwrap();
        tx.send(test_event()).await.unwrap();
        tx.send(test_event()).await.unwrap();
        wait_for_desired(&cas, "h", 1).await;
        assert_eq!(desired_generation(&cas, "h"), Some(1));

        tokio::time::sleep(WATCH_COALESCE_WINDOW + Duration::from_millis(50)).await;
        tx.send(test_event()).await.unwrap();
        wait_for_desired(&cas, "h", 2).await;
        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn continuous_events_do_not_starve_dirty_generation() {
        let root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        seed_repo(&cas, "h", root.path());
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        reconcile.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        let (tx, rx) = mpsc::channel(WATCH_EDGE_CAPACITY);
        let task = tokio::spawn(dispatch_events(Some(reconcile), "h".into(), rx));
        let producer = tokio::spawn(async move {
            for _ in 0..30 {
                tx.send(test_event()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        wait_for_desired(&cas, "h", 1).await;
        assert!(
            !producer.is_finished(),
            "generation advanced only after the producer stopped"
        );
        producer.await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn failed_dirty_request_retains_edge_until_repository_exists() {
        let root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        reconcile.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        let (tx, rx) = mpsc::channel(WATCH_EDGE_CAPACITY);
        let task = tokio::spawn(dispatch_events(Some(reconcile), "h".into(), rx));

        tx.send(test_event()).await.unwrap();
        tokio::time::sleep(WATCH_COALESCE_WINDOW + Duration::from_millis(100)).await;
        assert_eq!(desired_generation(&cas, "h"), None);
        seed_repo(&cas, "h", root.path());
        wait_for_desired(&cas, "h", 1).await;

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn registering_edge_is_retained_until_publication_activates_gate() {
        let root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data_root.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let reconcile =
            RepoReconcileManager::new_with_lifecycle(cas.clone(), None, lifecycle.clone());
        reconcile.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        let permit = lifecycle
            .begin_registration("h".into(), root.path().to_path_buf(), 1)
            .unwrap();
        let (tx, rx) = mpsc::channel(WATCH_EDGE_CAPACITY);
        let task = tokio::spawn(dispatch_events(Some(reconcile.clone()), "h".into(), rx));

        tx.send(test_event()).await.unwrap();
        tokio::time::sleep(WATCH_COALESCE_WINDOW + Duration::from_millis(100)).await;
        assert_eq!(
            desired_generation(&cas, "h"),
            Some(0),
            "the Registering lifecycle gate rejects the first dirty attempt"
        );

        let publication = lifecycle
            .publish_registration(
                permit,
                "demo",
                None,
                2,
                RegistrationReconcilePolicy::ImmediateCatchUp,
            )
            .unwrap();
        reconcile.wake_recorded_generation(&publication.repo_hash, Some("demo".into()));
        wait_for_desired(&cas, "h", 2).await;

        drop(tx);
        task.await.unwrap();
        reconcile.shutdown(Duration::from_secs(1)).await;
    }
}
