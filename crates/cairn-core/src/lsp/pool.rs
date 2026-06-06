//! Long-lived LSP client pool for workspace analyzers.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use serde_json::Value;
use tokio::process::Command;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::time::timeout;

use super::{LspClient, Position, Result, Url};

type ClientWork<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

/// Registry key for one long-lived LSP server process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub canonical_repo_root: PathBuf,
    pub language: String,
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
    pub fn lsp(
        language: &str,
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
            language: language.to_string(),
            analyzer_id: analyzer_id.to_string(),
            binary: binary.to_path_buf(),
            config_hash: config_hash.to_string(),
        })
    }
}

/// Strategy used to verify an LSP binary before spawning it.
#[derive(Debug, Clone)]
pub enum AvailabilityStrategy {
    /// `<binary> --version` returns exit 0.
    VersionFlag,
    /// `<binary> version` returns exit 0.
    VersionNoFlag,
    /// Path resolves to an executable file.
    PathExistsExecutable,
}

/// Strategy used to decide when an initialized LSP is ready for work.
#[derive(Debug, Clone)]
pub enum ReadinessStrategy {
    /// Wait for `$/progress` workspace-load quiescence.
    ProgressQuiescence { timeout: Duration },
    /// The initialize response is the readiness gate.
    InitializeResponseOnly,
}

/// Launch and readiness settings for a pooled LSP client.
#[derive(Debug, Clone)]
pub struct LspSpawnSpec {
    pub binary: PathBuf,
    pub workspace_root: PathBuf,
    pub config_hash: String,
    pub request_timeout: Duration,
    pub availability: AvailabilityStrategy,
    pub readiness: ReadinessStrategy,
    pub language_id: &'static str,
    pub launch_args: Vec<String>,
    pub initialization_options: Value,
}

/// Borrowed pooled client plus document synchronization state.
pub struct PooledLsp<'a> {
    client: &'a LspClient,
    opened_documents: &'a mut HashMap<String, i32>,
    language_id: &'static str,
}

impl PooledLsp<'_> {
    /// Open or fully replace a document.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP client.
    pub async fn sync_document(&mut self, uri: &Url, text: &str) -> Result<()> {
        if let Some(version) = self.opened_documents.get_mut(uri.as_str()) {
            *version = version.saturating_add(1);
            self.client.did_change(uri, *version, text).await
        } else {
            self.opened_documents.insert(uri.as_str().to_string(), 1);
            self.client.did_open(uri, self.language_id, 1, text).await
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

    /// Borrow a long-lived LSP client for `key`, lazily spawning it
    /// when needed according to `spawn_spec`.
    ///
    /// # Errors
    /// Returns LSP spawn/readiness/protocol errors from the pooled
    /// client.
    pub fn with_lsp<T, F>(&self, key: PoolKey, spawn_spec: LspSpawnSpec, work: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledLsp<'a>) -> ClientWork<'a, T>,
    {
        let entry = self.entry(key)?;
        self.runtime
            .block_on(async move { entry.with_lsp_client(spawn_spec, work).await })
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

async fn check_lsp_available(
    binary_path: &Path,
    strategy: &AvailabilityStrategy,
    request_timeout: Duration,
) -> Result<()> {
    match strategy {
        AvailabilityStrategy::VersionFlag | AvailabilityStrategy::VersionNoFlag => {
            check_version_command(
                binary_path,
                availability_probe_args(strategy).unwrap_or(&[]),
                request_timeout,
            )
            .await
        }
        AvailabilityStrategy::PathExistsExecutable => check_path_exists_executable(binary_path),
    }
}

fn availability_probe_args(strategy: &AvailabilityStrategy) -> Option<&'static [&'static str]> {
    match strategy {
        AvailabilityStrategy::VersionFlag => Some(&["--version"]),
        AvailabilityStrategy::VersionNoFlag => Some(&["version"]),
        AvailabilityStrategy::PathExistsExecutable => None,
    }
}

async fn check_version_command(
    binary_path: &Path,
    args: &[&str],
    request_timeout: Duration,
) -> Result<()> {
    let output = timeout(
        request_timeout,
        Command::new(binary_path).args(args).output(),
    )
    .await
    .map_err(|_| super::Error::Timeout)?
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            super::Error::BinaryMissing(binary_path.to_path_buf())
        } else {
            super::Error::Spawn(e)
        }
    })?;
    if output.status.success() {
        Ok(())
    } else {
        Err(super::Error::BinaryMissing(binary_path.to_path_buf()))
    }
}

fn check_path_exists_executable(binary_path: &Path) -> Result<()> {
    let resolved = resolve_executable(binary_path)
        .ok_or_else(|| super::Error::BinaryMissing(binary_path.to_path_buf()))?;
    if is_executable(&resolved) {
        Ok(())
    } else {
        Err(super::Error::BinaryMissing(binary_path.to_path_buf()))
    }
}

fn resolve_executable(binary_path: &Path) -> Option<PathBuf> {
    if has_path_separator(binary_path) {
        return binary_path.exists().then(|| binary_path.to_path_buf());
    }
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join(binary_path))
            .find(|candidate| candidate.exists())
    })
}

fn has_path_separator(path: &Path) -> bool {
    path.components().count() > 1
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

impl PoolEntry {
    async fn with_lsp_client<T, F>(&self, spec: LspSpawnSpec, work: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledLsp<'a>) -> ClientWork<'a, T>,
    {
        let mut state = self.state.lock().await;
        if state.client.is_none() {
            check_lsp_available(&spec.binary, &spec.availability, spec.request_timeout).await?;
            let client = LspClient::start_configured(
                &spec.binary,
                spec.launch_args.clone(),
                &spec.workspace_root,
                spec.initialization_options.clone(),
                spec.request_timeout,
            )
            .await?;
            dispatch_readiness(&spec.readiness, |timeout| {
                client.wait_for_workspace_load(timeout)
            })
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
        let mut pooled = PooledLsp {
            client,
            opened_documents,
            language_id: spec.language_id,
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

async fn dispatch_readiness<F, Fut>(
    readiness: &ReadinessStrategy,
    wait_for_workspace_load: F,
) -> Result<()>
where
    F: FnOnce(Duration) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    match readiness {
        ReadinessStrategy::ProgressQuiescence { timeout } => {
            wait_for_workspace_load(*timeout).await
        }
        ReadinessStrategy::InitializeResponseOnly => Ok(()),
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
    use crate::lsp::Error;

    #[cfg(unix)]
    struct FakeProbeBinary {
        _dir: tempfile::TempDir,
        path: PathBuf,
    }

    #[cfg(unix)]
    impl FakeProbeBinary {
        fn path(&self) -> &Path {
            &self.path
        }
    }

    #[cfg(unix)]
    fn fake_probe_binary(expected_arg: &'static str) -> FakeProbeBinary {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake-lsp");
        fs::write(
            &path,
            format!(
                "#!/bin/sh\nif [ \"$#\" -eq 1 ] && [ \"$1\" = \"{expected_arg}\" ]; then exit 0; fi\nexit 1"
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        FakeProbeBinary { _dir: dir, path }
    }

    #[test]
    fn pool_key_uses_launch_configuration() {
        let repo = tempfile::tempdir().unwrap();
        let key_a = PoolKey::lsp(
            "rust",
            repo.path(),
            "rust-analyzer-lsp",
            Path::new("ra"),
            "cfg-a",
        )
        .unwrap();
        let key_b = PoolKey::lsp(
            "rust",
            repo.path(),
            "rust-analyzer-lsp",
            Path::new("ra"),
            "cfg-b",
        )
        .unwrap();
        let key_go =
            PoolKey::lsp("go", repo.path(), "gopls-lsp", Path::new("gopls"), "cfg-a").unwrap();

        assert_eq!(
            key_a.canonical_repo_root,
            std::fs::canonicalize(repo.path()).unwrap()
        );
        assert_ne!(key_a, key_b);
        assert_eq!(key_a.language, "rust");
        assert_eq!(key_go.analyzer_id, "gopls-lsp");
    }

    #[cfg(unix)]
    #[test]
    fn lsp_pool_runs_version_flag_availability_probe() {
        let binary = fake_probe_binary("--version");
        let runtime = Runtime::new().unwrap();

        runtime
            .block_on(check_lsp_available(
                binary.path(),
                &AvailabilityStrategy::VersionFlag,
                Duration::from_secs(1),
            ))
            .unwrap();
        assert!(matches!(
            runtime.block_on(check_lsp_available(
                binary.path(),
                &AvailabilityStrategy::VersionNoFlag,
                Duration::from_secs(1),
            )),
            Err(Error::BinaryMissing(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn lsp_pool_runs_version_no_flag_availability_probe() {
        let binary = fake_probe_binary("version");
        let runtime = Runtime::new().unwrap();

        runtime
            .block_on(check_lsp_available(
                binary.path(),
                &AvailabilityStrategy::VersionNoFlag,
                Duration::from_secs(1),
            ))
            .unwrap();
        assert!(matches!(
            runtime.block_on(check_lsp_available(
                binary.path(),
                &AvailabilityStrategy::VersionFlag,
                Duration::from_secs(1),
            )),
            Err(Error::BinaryMissing(_))
        ));
    }

    #[test]
    fn lsp_pool_checks_path_exists_executable_availability_without_spawning() {
        let binary = tempfile::NamedTempFile::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut perms = binary.as_file().metadata().unwrap().permissions();
            perms.set_mode(0o755);
            binary.as_file().set_permissions(perms).unwrap();
        }
        let runtime = Runtime::new().unwrap();

        runtime
            .block_on(check_lsp_available(
                binary.path(),
                &AvailabilityStrategy::PathExistsExecutable,
                Duration::from_secs(1),
            ))
            .unwrap();

        let missing = binary.path().with_file_name("missing-lsp");
        assert!(matches!(
            runtime.block_on(check_lsp_available(
                &missing,
                &AvailabilityStrategy::PathExistsExecutable,
                Duration::from_secs(1),
            )),
            Err(Error::BinaryMissing(_))
        ));
    }

    #[test]
    fn lsp_pool_dispatches_availability_strategy_per_server() {
        assert_eq!(
            availability_probe_args(&AvailabilityStrategy::VersionFlag),
            Some(&["--version"][..])
        );
        assert_eq!(
            availability_probe_args(&AvailabilityStrategy::VersionNoFlag),
            Some(&["version"][..])
        );
        assert_eq!(
            availability_probe_args(&AvailabilityStrategy::PathExistsExecutable),
            None
        );
    }

    #[test]
    fn lsp_pool_dispatches_progress_quiescence_readiness_to_wait_hook() {
        let runtime = Runtime::new().unwrap();
        let timeout = Duration::from_secs(2);
        let mut waited = None;

        runtime
            .block_on(dispatch_readiness(
                &ReadinessStrategy::ProgressQuiescence { timeout },
                |timeout| {
                    waited = Some(timeout);
                    async { Ok(()) }
                },
            ))
            .unwrap();

        assert_eq!(waited, Some(timeout));
    }

    #[test]
    fn lsp_pool_skips_wait_hook_for_initialize_response_readiness() {
        let runtime = Runtime::new().unwrap();
        let mut waited = false;

        runtime
            .block_on(dispatch_readiness(
                &ReadinessStrategy::InitializeResponseOnly,
                |timeout| {
                    let _ = timeout;
                    waited = true;
                    async { Ok(()) }
                },
            ))
            .unwrap();

        assert!(!waited);
    }

    #[test]
    fn lsp_pool_dispatches_readiness_strategy_per_server() {
        let rust = LspSpawnSpec {
            binary: PathBuf::from("rust-analyzer"),
            workspace_root: PathBuf::from("/tmp/repo"),
            config_hash: "cfg".into(),
            request_timeout: Duration::from_secs(1),
            availability: AvailabilityStrategy::VersionFlag,
            readiness: ReadinessStrategy::ProgressQuiescence {
                timeout: Duration::from_secs(2),
            },
            language_id: "rust",
            launch_args: Vec::new(),
            initialization_options: serde_json::json!({
                "experimental": {
                    "serverStatusNotification": true
                }
            }),
        };
        let pyright = LspSpawnSpec {
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "python",
            launch_args: vec!["--stdio".to_string()],
            initialization_options: serde_json::json!({}),
            ..rust.clone()
        };

        assert!(matches!(
            rust.readiness,
            ReadinessStrategy::ProgressQuiescence { .. }
        ));
        assert!(matches!(
            pyright.readiness,
            ReadinessStrategy::InitializeResponseOnly
        ));
        assert_eq!(
            rust.initialization_options["experimental"]["serverStatusNotification"],
            true
        );
        assert_eq!(pyright.launch_args, vec!["--stdio"]);
        assert_eq!(pyright.initialization_options, serde_json::json!({}));
    }

    #[test]
    fn empty_pool_shutdown_is_noop() {
        let pool = LspClientPool::new().unwrap();
        assert_eq!(pool.len(), 0);
        pool.shutdown_all().unwrap();
        assert_eq!(pool.len(), 0);
    }
}
