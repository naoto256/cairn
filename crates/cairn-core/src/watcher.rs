//! Daemon-owned filesystem watchers for registered repositories.
//!
//! The control socket owns registration, but the daemon owns watcher
//! lifetime. Each watched alias keeps a `cairn-watch` handle alive and
//! funnels debounced file/git events back into the same register flow
//! used by explicit `reindex_repo`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cairn_watch::{WatchBackend, WatchEvent, WatcherHandle, watch_repo_with_backend};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::cas::{registry as cas_registry, store as cas_store};
use crate::paths::CasDataDir;
use crate::register::register_repo as cas_register;
use crate::{Error, Result};

const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
const REINDEX_DEBOUNCE: Duration = Duration::from_millis(500);

/// Keeps one live watcher per registered alias.
pub struct WatchManager {
    cas_data_dir: Arc<CasDataDir>,
    backend: WatchBackend,
    #[cfg(test)]
    fail_watcher_start: bool,
    watchers: Mutex<HashMap<String, RepoWatcher>>,
}

struct RepoWatcher {
    _handle: WatcherHandle,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RepoWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WatchManager {
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Self {
        Self::with_backend(cas_data_dir, WatchBackend::Recommended)
    }

    #[must_use]
    pub fn with_backend(cas_data_dir: Arc<CasDataDir>, backend: WatchBackend) -> Self {
        Self {
            cas_data_dir,
            backend,
            #[cfg(test)]
            fail_watcher_start: false,
            watchers: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_failing_watcher(cas_data_dir: Arc<CasDataDir>) -> Self {
        Self {
            cas_data_dir,
            backend: WatchBackend::Poll,
            fail_watcher_start: true,
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
        self.unwatch_alias(&alias);
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
            alias.clone(),
            rx,
        ));

        self.watchers
            .lock()
            .expect("watch manager mutex poisoned")
            .insert(
                alias.clone(),
                RepoWatcher {
                    _handle: handle,
                    task,
                },
            );
        info!(alias = %alias, path = %root_path.display(), "repo watcher started");
        Ok(())
    }

    pub fn unwatch_alias(&self, alias: &str) {
        if self
            .watchers
            .lock()
            .expect("watch manager mutex poisoned")
            .remove(alias)
            .is_some()
        {
            info!(alias = %alias, "repo watcher stopped");
        }
    }

    #[cfg(test)]
    pub fn is_watching_alias(&self, alias: &str) -> bool {
        self.watchers
            .lock()
            .expect("watch manager mutex poisoned")
            .contains_key(alias)
    }
}

async fn reindex_on_events(
    cas_data_dir: Arc<CasDataDir>,
    alias: String,
    mut rx: mpsc::UnboundedReceiver<WatchEvent>,
) {
    while let Some(event) = rx.recv().await {
        debug!(alias = %alias, ?event, "repo watcher event");
        tokio::time::sleep(REINDEX_DEBOUNCE).await;
        while let Ok(event) = rx.try_recv() {
            debug!(alias = %alias, ?event, "coalesced repo watcher event");
        }
        match reindex_alias(cas_data_dir.clone(), alias.clone()).await {
            Ok(()) => info!(alias = %alias, "repo watcher reindex complete"),
            Err(err) => warn!(alias = %alias, error = %err, "repo watcher reindex failed"),
        }
    }
}

async fn reindex_alias(cas_data_dir: Arc<CasDataDir>, alias: String) -> Result<()> {
    let now_ns = now_ns()?;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        let entry =
            cas_registry::lookup_by_alias(&index, &alias)?.ok_or_else(|| Error::RepoNotFound {
                alias: alias.clone(),
            })?;
        let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
        let mut conn = cas_store::open(&store_path)?;
        cas_register(&mut conn, &PathBuf::from(entry.root_path), now_ns)?;
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
