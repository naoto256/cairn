//! Shared storage facade.
//!
//! Owns the always-open registry connection and opens per-snapshot
//! data DBs on demand. rusqlite is synchronous; this facade serialises
//! access through a tokio `Mutex` and lifts every SQL call onto a
//! blocking task so async callers (the daemon, the MCP server) never
//! block the runtime.

use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::paths::DataDir;
use crate::{Result, registry_db};

/// Composite handle the daemon hands to indexer / MCP / control
/// dispatch.
pub struct Storage {
    pub data_dir: DataDir,
    registry: Arc<Mutex<Connection>>,
}

impl Storage {
    /// Open (or create) the registry under `data_dir` and return a
    /// handle.
    ///
    /// # Errors
    /// Filesystem or SQLite failures.
    pub fn open(data_dir: DataDir) -> Result<Self> {
        data_dir.ensure()?;
        let conn = registry_db::open(&data_dir.registry_db_path())?;
        Ok(Self {
            data_dir,
            registry: Arc::new(Mutex::new(conn)),
        })
    }

    /// Run `f` against the registry connection on a blocking thread.
    /// All caller-side mutation should funnel through here so the
    /// async layer never holds a rusqlite handle across await points.
    pub async fn with_registry<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Connection) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let registry = self.registry.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = registry.blocking_lock();
            f(&mut guard)
        })
        .await
        .map_err(|e| crate::Error::InvalidArgument(format!("registry task panicked: {e}")))?
    }

    /// Open a data DB at `path` on a blocking thread. The returned
    /// connection is owned by the caller; close by dropping.
    pub async fn open_data_db(&self, path: PathBuf) -> Result<Connection> {
        tokio::task::spawn_blocking(move || crate::data_db::open(&path))
            .await
            .map_err(|e| crate::Error::InvalidArgument(format!("open task panicked: {e}")))?
    }
}
