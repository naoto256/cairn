//! Daemon-owned filesystem watchers for registered repositories.
//!
//! The control socket owns registration, but the daemon owns watcher
//! lifetime. Each watched alias keeps a `cairn-watch` handle alive and
//! funnels debounced file/git events back into the same register flow
//! used by explicit `reindex_repo`.

use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cairn_watch::{WatchBackend, WatchEvent, WatcherHandle, watch_repo_with_backend};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::cas::{registry as cas_registry, store as cas_store};
use crate::jobs::JobManager;
use crate::paths::CasDataDir;
use crate::register::{register_repo as cas_register, register_repo_enqueue_analyzers};
use crate::{Error, Result};

const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
const REINDEX_DEBOUNCE: Duration = Duration::from_millis(500);

/// Keeps one live watcher per registered alias.
pub struct WatchManager {
    cas_data_dir: Arc<CasDataDir>,
    job_manager: Option<Arc<JobManager>>,
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
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Self {
        Self::with_backend_and_jobs(cas_data_dir, WatchBackend::Recommended, None)
    }

    #[must_use]
    pub fn with_jobs(cas_data_dir: Arc<CasDataDir>, job_manager: Arc<JobManager>) -> Self {
        Self::with_backend_and_jobs(cas_data_dir, WatchBackend::Recommended, Some(job_manager))
    }

    #[must_use]
    pub fn with_backend(cas_data_dir: Arc<CasDataDir>, backend: WatchBackend) -> Self {
        Self::with_backend_and_jobs(cas_data_dir, backend, None)
    }

    #[must_use]
    pub fn with_backend_and_jobs(
        cas_data_dir: Arc<CasDataDir>,
        backend: WatchBackend,
        job_manager: Option<Arc<JobManager>>,
    ) -> Self {
        Self {
            cas_data_dir,
            job_manager,
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
            job_manager: None,
            fail_watcher_start: true,
            dropped_watchers: Arc::new(AtomicUsize::new(0)),
            watchers: Mutex::new(HashMap::new()),
        }
    }

    /// Start watchers for every alias currently present in the CAS
    /// registry. Individual watcher setup failures are logged and the
    /// daemon keeps starting; runtime registration surfaces setup
    /// failures to the caller via [`Self::watch_alias`].
    pub fn start_registered(&self) -> Result<()> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        for entry in cas_registry::list_all(&index)? {
            if let Err(err) = self.watch_alias(entry.alias.clone(), PathBuf::from(entry.root_path))
            {
                warn!(
                    alias = %entry.alias,
                    error = %err,
                    "failed to start repo watcher"
                );
            }
        }
        Ok(())
    }

    /// Start or replace the watcher for one alias.
    pub fn watch_alias(&self, alias: String, root_path: PathBuf) -> Result<()> {
        #[cfg(test)]
        if self.fail_watcher_start {
            return Err(Error::InvalidArgument(
                "injected watcher start failure".into(),
            ));
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let handle = watch_repo_with_backend(&root_path, WATCH_DEBOUNCE, tx, self.backend)
            .map_err(|e| {
                Error::InvalidArgument(format!("watch_repo {}: {e}", root_path.display()))
            })?;

        let task = tokio::spawn(reindex_on_events(
            self.cas_data_dir.clone(),
            self.job_manager.clone(),
            alias.clone(),
            rx,
        ));

        // Only swap after setup succeeds; a start failure must not blind an existing alias.
        self.lock_recovering().insert(
            alias.clone(),
            RepoWatcher {
                _handle: handle,
                task,
                #[cfg(test)]
                drop_counter: self.dropped_watchers.clone(),
            },
        );
        info!(alias = %alias, path = %root_path.display(), "repo watcher started");
        Ok(())
    }

    pub fn unwatch_alias(&self, alias: &str) {
        if self.lock_recovering().remove(alias).is_some() {
            info!(alias = %alias, "repo watcher stopped");
        }
    }

    pub fn is_watching_alias(&self, alias: &str) -> bool {
        self.lock_recovering().contains_key(alias)
    }

    #[cfg(test)]
    pub fn set_fail_watcher_start(&mut self, fail: bool) {
        self.fail_watcher_start = fail;
    }

    #[cfg(test)]
    fn dropped_watcher_count(&self) -> usize {
        self.dropped_watchers.load(Ordering::SeqCst)
    }

    fn lock_recovering(&self) -> MutexGuard<'_, HashMap<String, RepoWatcher>> {
        self.watchers.lock().unwrap_or_else(|poisoned| {
            warn!("watch manager mutex poisoned; recovering watcher registry");
            poisoned.into_inner()
        })
    }
}

async fn reindex_on_events(
    cas_data_dir: Arc<CasDataDir>,
    job_manager: Option<Arc<JobManager>>,
    alias: String,
    mut rx: mpsc::UnboundedReceiver<WatchEvent>,
) {
    while let Some(event) = rx.recv().await {
        debug!(alias = %alias, ?event, "repo watcher event");
        tokio::time::sleep(REINDEX_DEBOUNCE).await;
        while let Ok(event) = rx.try_recv() {
            debug!(alias = %alias, ?event, "coalesced repo watcher event");
        }
        match reindex_alias(cas_data_dir.clone(), job_manager.clone(), alias.clone()).await {
            Ok(()) => info!(alias = %alias, "repo watcher reindex complete"),
            Err(err) => warn!(alias = %alias, error = %err, "repo watcher reindex failed"),
        }
    }
}

async fn reindex_alias(
    cas_data_dir: Arc<CasDataDir>,
    job_manager: Option<Arc<JobManager>>,
    alias: String,
) -> Result<()> {
    let now_ns = now_ns()?;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        let entry =
            cas_registry::lookup_by_alias(&index, &alias)?.ok_or_else(|| Error::RepoNotFound {
                alias: alias.clone(),
            })?;
        let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
        let mut conn = cas_store::open(&store_path)?;
        match job_manager.as_deref() {
            Some(manager) => {
                register_repo_enqueue_analyzers(
                    &mut conn,
                    &alias,
                    &entry.repo_hash,
                    &PathBuf::from(entry.root_path),
                    now_ns,
                    manager,
                )?;
            }
            None => {
                cas_register(&mut conn, &PathBuf::from(entry.root_path), now_ns)?;
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| Error::InvalidArgument(format!("watcher reindex task panicked: {e}")))?
}

fn now_ns() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::InvalidArgument(format!("clock: {e}")))?
            .as_nanos(),
    )
    .unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn watch_alias_replaces_existing_watcher_after_new_start_succeeds() {
        let old_root = tempfile::tempdir().unwrap();
        let new_root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let manager = WatchManager::with_backend(
            Arc::new(CasDataDir::with_root(data_root.path().to_path_buf())),
            WatchBackend::Poll,
        );

        manager
            .watch_alias("demo".into(), old_root.path().to_path_buf())
            .unwrap();
        assert!(manager.is_watching_alias("demo"));
        assert_eq!(manager.dropped_watcher_count(), 0);

        manager
            .watch_alias("demo".into(), new_root.path().to_path_buf())
            .unwrap();

        assert!(manager.is_watching_alias("demo"));
        assert_eq!(manager.dropped_watcher_count(), 1);
    }

    #[tokio::test]
    async fn watch_alias_keeps_existing_watcher_when_new_start_fails() {
        let old_root = tempfile::tempdir().unwrap();
        let new_root = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let mut manager = WatchManager::with_backend(
            Arc::new(CasDataDir::with_root(data_root.path().to_path_buf())),
            WatchBackend::Poll,
        );

        manager
            .watch_alias("demo".into(), old_root.path().to_path_buf())
            .unwrap();
        manager.set_fail_watcher_start(true);

        let err = manager
            .watch_alias("demo".into(), new_root.path().to_path_buf())
            .unwrap_err();

        assert!(err.to_string().contains("injected watcher start failure"));
        assert!(manager.is_watching_alias("demo"));
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

        assert!(!manager.is_watching_alias("missing"));
    }
}
