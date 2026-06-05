//! Long-lived LSP client pool for workspace analyzers.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use super::{LspClient, Position, Result, Url};

type ClientWork<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

/// Registry key for one long-lived rust-analyzer process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub canonical_repo_root: PathBuf,
    pub analyzer_id: String,
    pub binary: PathBuf,
    pub config_hash: String,
}

impl PoolKey {
    /// Build a key from the repo root and launch configuration.
    ///
    /// # Errors
    /// Returns an LSP protocol error when the repo root cannot be
    /// canonicalized.
    pub fn rust_analyzer(
        repo_root: &Path,
        analyzer_id: &str,
        binary: &Path,
        config_hash: &str,
    ) -> Result<Self> {
        let canonical_repo_root = std::fs::canonicalize(repo_root).map_err(|e| {
            super::Error::Protocol(format!("canonicalize {}: {e}", repo_root.display()))
        })?;
        Ok(Self {
            canonical_repo_root,
            analyzer_id: analyzer_id.to_string(),
            binary: binary.to_path_buf(),
            config_hash: config_hash.to_string(),
        })
    }

    /// Build a pyright key from the repo root and launch configuration.
    ///
    /// # Errors
    /// Returns an LSP protocol error when the repo root cannot be
    /// canonicalized.
    pub fn pyright(
        repo_root: &Path,
        analyzer_id: &str,
        binary: &Path,
        config_hash: &str,
    ) -> Result<Self> {
        let canonical_repo_root = std::fs::canonicalize(repo_root).map_err(|e| {
            super::Error::Protocol(format!("canonicalize {}: {e}", repo_root.display()))
        })?;
        Ok(Self {
            canonical_repo_root,
            analyzer_id: analyzer_id.to_string(),
            binary: binary.to_path_buf(),
            config_hash: config_hash.to_string(),
        })
    }
}

/// Launch and readiness settings for a pooled rust-analyzer client.
#[derive(Debug, Clone)]
pub struct RustAnalyzerSpawnSpec {
    pub binary: PathBuf,
    pub workspace_root: PathBuf,
    pub config_hash: String,
    pub request_timeout: Duration,
    pub workspace_load_timeout: Duration,
}

/// Launch and readiness settings for a pooled pyright-langserver client.
#[derive(Debug, Clone)]
pub struct PyrightSpawnSpec {
    pub binary: PathBuf,
    pub workspace_root: PathBuf,
    pub config_hash: String,
    pub request_timeout: Duration,
}

/// Borrowed pooled client plus document synchronization state.
pub struct PooledRustAnalyzer<'a> {
    client: &'a LspClient,
    opened_documents: &'a mut HashMap<String, i32>,
}

impl PooledRustAnalyzer<'_> {
    /// Open or fully replace a document in rust-analyzer.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP client.
    pub async fn sync_document(&mut self, uri: &Url, language_id: &str, text: &str) -> Result<()> {
        if let Some(version) = self.opened_documents.get_mut(uri.as_str()) {
            *version = version.saturating_add(1);
            self.client.did_change(uri, *version, text).await
        } else {
            self.opened_documents.insert(uri.as_str().to_string(), 1);
            self.client.did_open(uri, language_id, 1, text).await
        }
    }

    /// Resolve the definition at `uri` + `position`.
    ///
    /// # Errors
    /// Returns timeout/protocol/server errors from the underlying LSP
    /// request.
    pub async fn definition(&self, uri: &Url, position: Position) -> Result<Vec<super::Location>> {
        self.client.definition(uri, position).await
    }
}

/// Borrowed pooled pyright client plus document synchronization state.
pub struct PooledPyright<'a> {
    client: &'a LspClient,
    opened_documents: &'a mut HashMap<String, i32>,
}

impl PooledPyright<'_> {
    /// Open or fully replace a document in pyright.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP client.
    pub async fn sync_document(&mut self, uri: &Url, language_id: &str, text: &str) -> Result<()> {
        if let Some(version) = self.opened_documents.get_mut(uri.as_str()) {
            *version = version.saturating_add(1);
            self.client.did_change(uri, *version, text).await
        } else {
            self.opened_documents.insert(uri.as_str().to_string(), 1);
            self.client.did_open(uri, language_id, 1, text).await
        }
    }

    /// Resolve the definition at `uri` + `position`.
    ///
    /// # Errors
    /// Returns timeout/protocol/server errors from the underlying LSP
    /// request.
    pub async fn definition(&self, uri: &Url, position: Position) -> Result<Vec<super::Location>> {
        self.client.definition(uri, position).await
    }
}

/// Daemon-scoped pool of long-lived LSP clients.
pub struct LspClientPool {
    runtime: Runtime,
    entries: RwLock<HashMap<PoolKey, Arc<PoolEntry>>>,
}

impl LspClientPool {
    /// Create an empty pool.
    ///
    /// # Errors
    /// Returns an LSP protocol error if the dedicated Tokio runtime
    /// cannot be created.
    pub fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("cairn-lsp-pool")
            .build()
            .map_err(|e| super::Error::Protocol(format!("lsp pool runtime: {e}")))?;
        Ok(Self {
            runtime,
            entries: RwLock::new(HashMap::new()),
        })
    }

    /// Borrow a long-lived rust-analyzer client for `key`, lazily
    /// spawning it when needed.
    ///
    /// # Errors
    /// Returns LSP spawn/readiness/protocol errors from the pooled
    /// client.
    pub fn with_rust_analyzer<T, F>(
        &self,
        key: PoolKey,
        spawn_spec: RustAnalyzerSpawnSpec,
        work: F,
    ) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledRustAnalyzer<'a>) -> ClientWork<'a, T>,
    {
        let entry = self.entry(key)?;
        self.runtime
            .block_on(async move { entry.with_client(spawn_spec, work).await })
    }

    /// Borrow a long-lived pyright client for `key`, lazily spawning
    /// it when needed.
    ///
    /// # Errors
    /// Returns LSP spawn/readiness/protocol errors from the pooled
    /// client.
    pub fn with_pyright<T, F>(
        &self,
        key: PoolKey,
        spawn_spec: PyrightSpawnSpec,
        work: F,
    ) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledPyright<'a>) -> ClientWork<'a, T>,
    {
        let entry = self.entry(key)?;
        self.runtime
            .block_on(async move { entry.with_pyright_client(spawn_spec, work).await })
    }

    /// Gracefully stop all live clients and clear the registry.
    ///
    /// # Errors
    /// Returns the first LSP shutdown error observed.
    pub fn shutdown_all(&self) -> Result<()> {
        let entries = {
            let mut registry = self
                .entries
                .write()
                .map_err(|_| super::Error::Protocol("lsp pool registry poisoned".into()))?;
            registry.drain().map(|(_, entry)| entry).collect::<Vec<_>>()
        };
        self.runtime.block_on(async move {
            for entry in entries {
                entry.shutdown().await?;
            }
            Ok(())
        })
    }

    fn entry(&self, key: PoolKey) -> Result<Arc<PoolEntry>> {
        {
            let registry = self
                .entries
                .read()
                .map_err(|_| super::Error::Protocol("lsp pool registry poisoned".into()))?;
            if let Some(entry) = registry.get(&key) {
                return Ok(Arc::clone(entry));
            }
        }

        let mut registry = self
            .entries
            .write()
            .map_err(|_| super::Error::Protocol("lsp pool registry poisoned".into()))?;
        Ok(Arc::clone(
            registry
                .entry(key)
                .or_insert_with(|| Arc::new(PoolEntry::default())),
        ))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }
}

#[derive(Default)]
struct PoolEntry {
    state: Mutex<PoolEntryState>,
}

#[derive(Default)]
struct PoolEntryState {
    client: Option<LspClient>,
    opened_documents: HashMap<String, i32>,
}

impl PoolEntry {
    async fn with_client<T, F>(&self, spec: RustAnalyzerSpawnSpec, work: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledRustAnalyzer<'a>) -> ClientWork<'a, T>,
    {
        let mut state = self.state.lock().await;
        if state.client.is_none() {
            let client = LspClient::start_with_timeout(
                &spec.binary,
                &spec.workspace_root,
                &spec.config_hash,
                spec.request_timeout,
            )
            .await?;
            client
                .wait_for_workspace_load(spec.workspace_load_timeout)
                .await?;
            state.client = Some(client);
            state.opened_documents.clear();
        }

        let PoolEntryState {
            client,
            opened_documents,
        } = &mut *state;
        let client = client
            .as_ref()
            .ok_or_else(|| super::Error::ServerExited(None.into()))?;
        let mut pooled = PooledRustAnalyzer {
            client,
            opened_documents,
        };
        let result = work(&mut pooled).await;
        if matches!(result, Err(super::Error::ServerExited(_))) {
            let client = state.client.take();
            state.opened_documents.clear();
            if let Some(client) = client {
                let _ = client.shutdown().await;
            }
        }
        result
    }

    async fn with_pyright_client<T, F>(&self, spec: PyrightSpawnSpec, work: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledPyright<'a>) -> ClientWork<'a, T>,
    {
        let mut state = self.state.lock().await;
        if state.client.is_none() {
            let client = LspClient::start_pyright_with_timeout(
                &spec.binary,
                &spec.workspace_root,
                &spec.config_hash,
                spec.request_timeout,
            )
            .await?;
            // Pyright does not emit rust-analyzer's `$/progress`
            // workspace-load notifications; the initialize response is
            // the readiness gate for didOpen + definition requests.
            state.client = Some(client);
            state.opened_documents.clear();
        }

        let PoolEntryState {
            client,
            opened_documents,
        } = &mut *state;
        let client = client
            .as_ref()
            .ok_or_else(|| super::Error::ServerExited(None.into()))?;
        let mut pooled = PooledPyright {
            client,
            opened_documents,
        };
        let result = work(&mut pooled).await;
        if matches!(result, Err(super::Error::ServerExited(_))) {
            let client = state.client.take();
            state.opened_documents.clear();
            if let Some(client) = client {
                let _ = client.shutdown().await;
            }
        }
        result
    }

    async fn shutdown(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.opened_documents.clear();
        if let Some(client) = state.client.take() {
            client.shutdown().await?;
        }
        Ok(())
    }
}

static GLOBAL_POOL: OnceLock<LspClientPool> = OnceLock::new();

/// Return the daemon-global LSP pool.
///
/// # Errors
/// Returns an LSP protocol error if the pool runtime cannot be
/// initialized.
pub fn global() -> Result<&'static LspClientPool> {
    if let Some(pool) = GLOBAL_POOL.get() {
        return Ok(pool);
    }
    let pool = LspClientPool::new()?;
    Ok(GLOBAL_POOL.get_or_init(|| pool))
}

/// Shut down the daemon-global pool if it was initialized.
///
/// # Errors
/// Returns the first LSP shutdown error observed.
pub async fn shutdown_global_if_initialized() -> Result<()> {
    if let Some(pool) = GLOBAL_POOL.get() {
        tokio::task::spawn_blocking(move || pool.shutdown_all())
            .await
            .map_err(|e| super::Error::Protocol(format!("lsp pool shutdown task: {e}")))??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_key_uses_launch_configuration() {
        let repo = tempfile::tempdir().unwrap();
        let key_a =
            PoolKey::rust_analyzer(repo.path(), "rust-analyzer-lsp", Path::new("ra"), "cfg-a")
                .unwrap();
        let key_b =
            PoolKey::rust_analyzer(repo.path(), "rust-analyzer-lsp", Path::new("ra"), "cfg-b")
                .unwrap();

        assert_eq!(
            key_a.canonical_repo_root,
            std::fs::canonicalize(repo.path()).unwrap()
        );
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn empty_pool_shutdown_is_noop() {
        let pool = LspClientPool::new().unwrap();
        assert_eq!(pool.len(), 0);
        pool.shutdown_all().unwrap();
        assert_eq!(pool.len(), 0);
    }
}
