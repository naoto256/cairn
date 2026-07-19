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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use cairn_watch::{WatchBackend, WatchEvent, WatcherHandle, watch_repo_with_backend};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::cas::registry::{self as cas_registry, WatcherState};
use crate::paths::CasDataDir;
use crate::reconcile::{ReconcileTrigger, RepoReconcileManager};
use crate::{Error, Result};

const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
const WATCH_EDGE_CAPACITY: usize = 1;
const WATCH_COALESCE_WINDOW: Duration = Duration::from_millis(500);
const WATCH_REQUEST_RETRY_INITIAL: Duration = Duration::from_millis(25);
const WATCH_REQUEST_RETRY_MAX: Duration = Duration::from_secs(1);

/// Keeps one live watcher per registered `repo_hash`.
pub struct WatchManager {
    cas_data_dir: Arc<CasDataDir>,
    reconcile: Option<Arc<RepoReconcileManager>>,
    backend: WatchBackend,
    #[cfg(test)]
    fail_watcher_start: bool,
    #[cfg(test)]
    dropped_watchers: Arc<AtomicUsize>,
    watchers: Mutex<HashMap<String, RepoWatcher>>,
}

struct RepoWatcher {
    _handle: WatcherHandle,
    task: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    drop_counter: Arc<AtomicUsize>,
}

impl Drop for RepoWatcher {
    fn drop(&mut self) {
        #[cfg(test)]
        self.drop_counter.fetch_add(1, Ordering::SeqCst);
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
    pub fn start_registered(&self) -> Result<()> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        for repo in cas_registry::list_repositories(&index)? {
            if let Err(err) =
                self.watch_repository(repo.repo_hash.clone(), PathBuf::from(repo.root_path))
            {
                warn!(
                    repo_hash = %repo.repo_hash,
                    error = %err,
                    "failed to start repo watcher"
                );
            }
        }
        Ok(())
    }

    /// Start or replace the watcher for one canonical repo.
    ///
    /// Persists `WatcherState::Active` on success and
    /// `WatcherState::Failed` on error via the reconcile driver
    /// (if one is wired). Failure of the new watcher does not
    /// drop the existing one — an operator-observable failed
    /// state is preferable to a silently blinded repo.
    pub fn watch_repository(&self, repo_hash: String, root_path: PathBuf) -> Result<()> {
        #[cfg(test)]
        if self.fail_watcher_start {
            self.record_watcher_failed(&repo_hash, "injected watcher start failure");
            return Err(Error::InvalidArgument(
                "injected watcher start failure".into(),
            ));
        }

        let (tx, rx) = mpsc::channel(WATCH_EDGE_CAPACITY);
        let handle = watch_repo_with_backend(&root_path, WATCH_DEBOUNCE, tx, self.backend)
            .map_err(|e| {
                let msg = format!("watch_repo {}: {e}", root_path.display());
                self.record_watcher_failed(&repo_hash, &msg);
                Error::InvalidArgument(msg)
            })?;

        let reconcile = self.reconcile.clone();
        let hash_for_task = repo_hash.clone();
        let task = tokio::spawn(dispatch_events(reconcile, hash_for_task, rx));

        // Only swap after setup succeeds; a start failure must not
        // blind an existing watched repo.
        self.lock_recovering().insert(
            repo_hash.clone(),
            RepoWatcher {
                _handle: handle,
                task,
                #[cfg(test)]
                drop_counter: self.dropped_watchers.clone(),
            },
        );
        info!(
            repo_hash = %repo_hash,
            path = %root_path.display(),
            "repo watcher started"
        );
        self.record_watcher_active(&repo_hash);
        Ok(())
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

    fn record_watcher_active(&self, repo_hash: &str) {
        let Some(reconcile) = self.reconcile.clone() else {
            return;
        };
        let hash = repo_hash.to_string();
        tokio::spawn(async move {
            if let Err(err) = reconcile
                .set_watcher_state_by_repo_hash(hash.clone(), WatcherState::Active, None)
                .await
            {
                warn!(
                    repo_hash = %hash,
                    error = %err,
                    "failed to persist watcher active state"
                );
            }
        });
    }

    fn record_watcher_failed(&self, repo_hash: &str, msg: &str) {
        let Some(reconcile) = self.reconcile.clone() else {
            return;
        };
        let hash = repo_hash.to_string();
        let msg = msg.to_string();
        tokio::spawn(async move {
            if let Err(err) = reconcile
                .set_watcher_state_by_repo_hash(hash.clone(), WatcherState::Failed, Some(msg))
                .await
            {
                debug!(
                    repo_hash = %hash,
                    error = %err,
                    "failed to persist watcher failed state (repo may not be registered)"
                );
                return;
            }
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

        let Some(reconcile) = reconcile.clone() else {
            if channel_closed {
                break;
            }
            continue;
        };

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
}
