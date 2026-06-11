use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;
use tracing::info;

use super::error::{Error, Result};
use super::reader::{ProgressState, reader_loop};
use super::transport::write_lsp_message;
use super::types::{Location, LocationLink, Position, Url};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const WORKSPACE_LOAD_QUIET_PERIOD: Duration = Duration::from_secs(5);
const MAX_RESTARTS: usize = 3;
const STDERR_TAIL_BYTES: usize = 2048;
const STDERR_TAIL_LINES: usize = 10;

pub struct LspClient {
    binary_path: Option<PathBuf>,
    args: Vec<String>,
    workspace_root: PathBuf,
    initialization_options: Value,
    timeout: Duration,
    max_restarts: usize,
    restarts: AtomicUsize,
    next_id: AtomicU64,
    alive: Arc<AtomicBool>,
    writer: Arc<Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>>,
    child: Mutex<Option<Child>>,
    pub(super) pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    progress: Arc<ProgressState>,
    stderr_tail: Arc<Mutex<StderrTail>>,
}

impl LspClient {
    /// Start a rust-analyzer subprocess using the default timeout.
    ///
    /// # Errors
    /// Returns [`Error::BinaryMissing`] when `binary_path --version`
    /// cannot run successfully.
    pub async fn start(
        binary_path: &Path,
        workspace_root: &Path,
        config_hash: &str,
    ) -> Result<Self> {
        Self::start_with_timeout(binary_path, workspace_root, config_hash, DEFAULT_TIMEOUT).await
    }

    /// Start a rust-analyzer subprocess using a custom request timeout.
    ///
    /// # Errors
    /// See [`Self::start`].
    pub async fn start_with_timeout(
        binary_path: &Path,
        workspace_root: &Path,
        config_hash: &str,
        request_timeout: Duration,
    ) -> Result<Self> {
        check_binary_available(binary_path, request_timeout).await?;
        let client = Self::new(
            Some(binary_path.to_path_buf()),
            Vec::new(),
            workspace_root.to_path_buf(),
            rust_analyzer_initialization_options(config_hash),
            request_timeout,
            MAX_RESTARTS,
        );
        client.spawn_process().await?;
        Ok(client)
    }

    /// Start an LSP subprocess after the caller has performed any
    /// server-specific availability probe.
    ///
    /// # Errors
    /// Returns spawn/handshake/protocol errors from the LSP server.
    pub async fn start_configured(
        binary_path: &Path,
        args: Vec<String>,
        workspace_root: &Path,
        initialization_options: Value,
        request_timeout: Duration,
    ) -> Result<Self> {
        let client = Self::new(
            Some(binary_path.to_path_buf()),
            args,
            workspace_root.to_path_buf(),
            initialization_options,
            request_timeout,
            MAX_RESTARTS,
        );
        client.spawn_process().await?;
        Ok(client)
    }

    fn new(
        binary_path: Option<PathBuf>,
        args: Vec<String>,
        workspace_root: PathBuf,
        initialization_options: Value,
        request_timeout: Duration,
        max_restarts: usize,
    ) -> Self {
        Self {
            binary_path,
            args,
            workspace_root,
            initialization_options,
            timeout: request_timeout,
            max_restarts,
            restarts: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            alive: Arc::new(AtomicBool::new(false)),
            writer: Arc::new(Mutex::new(None)),
            child: Mutex::new(None),
            pending: Arc::new(Mutex::new(HashMap::new())),
            progress: Arc::new(ProgressState::default()),
            stderr_tail: Arc::new(Mutex::new(StderrTail::default())),
        }
    }

    #[cfg(test)]
    pub(super) async fn start_with_io<R, W>(
        reader: R,
        writer: W,
        workspace_root: &Path,
        config_hash: &str,
        request_timeout: Duration,
    ) -> Result<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let client = Self::new(
            None,
            Vec::new(),
            workspace_root.to_path_buf(),
            rust_analyzer_initialization_options(config_hash),
            request_timeout,
            0,
        );
        client.install_transport(reader, writer).await;
        client.initialize().await?;
        Ok(client)
    }

    async fn spawn_process(&self) -> Result<()> {
        let Some(binary_path) = &self.binary_path else {
            return Err(Error::ServerExited(None.into()));
        };

        {
            let mut child_slot = self.child.lock().await;
            if let Some(child) = child_slot.as_mut() {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
            *child_slot = None;
        }

        let mut child = Command::new(binary_path)
            .args(&self.args)
            .current_dir(&self.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(Error::Spawn)?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Handshake("missing child stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Handshake("missing child stdout".into()))?;
        self.stderr_tail.lock().await.clear();
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(capture_stderr(stderr, Arc::clone(&self.stderr_tail)));
        }

        self.install_transport(stdout, stdin).await;
        *self.child.lock().await = Some(child);
        if let Err(err) = self.initialize().await {
            return Err(self.with_stderr_context(err).await);
        }
        Ok(())
    }

    async fn install_transport<R, W>(&self, reader: R, writer: W)
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        self.alive.store(true, Ordering::SeqCst);
        *self.writer.lock().await = Some(Box::new(writer));
        let pending = Arc::clone(&self.pending);
        let alive = Arc::clone(&self.alive);
        let writer = Arc::clone(&self.writer);
        let progress = Arc::clone(&self.progress);
        tokio::spawn(async move {
            reader_loop(reader, pending, alive, writer, progress).await;
        });
    }

    async fn initialize(&self) -> Result<()> {
        let root_uri = Url::from_file_path(&self.workspace_root)?;
        let root_path = self.workspace_root.to_string_lossy();
        let workspace_name = self
            .workspace_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("workspace");
        let _: Value = self
            .request(
                "initialize",
                json!({
                    "processId": Value::Null,
                    "rootPath": root_path,
                    "rootUri": root_uri.as_str(),
                    "workspaceFolders": [
                        {
                            "uri": root_uri.as_str(),
                            "name": workspace_name,
                        }
                    ],
                    "capabilities": {
                        "window": {
                            "workDoneProgress": true
                        },
                        "textDocument": {
                            "definition": {
                                "linkSupport": true
                            }
                        }
                    },
                    "initializationOptions": self.initialization_options.clone(),
                }),
            )
            .await
            .map_err(|e| Error::Handshake(e.to_string()))?;
        self.notify("initialized", json!({})).await?;
        Ok(())
    }

    /// Resolve the definition at `uri` + `position`.
    ///
    /// # Errors
    /// Returns timeout/protocol/server errors from the underlying LSP
    /// request.
    pub async fn definition(&self, uri: &Url, position: Position) -> Result<Vec<Location>> {
        self.ensure_running().await?;
        let value: Value = self
            .request(
                "textDocument/definition",
                json!({
                    "textDocument": { "uri": uri.as_str() },
                    "position": position,
                }),
            )
            .await?;
        parse_definition_result(value)
    }

    /// Wait until the server reports that workspace loading has
    /// completed via LSP `$/progress` notifications.
    ///
    /// # Errors
    /// Returns [`Error::ReadinessTimeout`] when no completed progress sequence
    /// is observed before `wait_timeout` elapses.
    pub async fn wait_for_workspace_load(&self, wait_timeout: Duration) -> Result<()> {
        self.wait_for_workspace_load_with_quiescence(wait_timeout, WORKSPACE_LOAD_QUIET_PERIOD)
            .await
    }

    pub(super) async fn wait_for_workspace_load_with_quiescence(
        &self,
        wait_timeout: Duration,
        quiet_period: Duration,
    ) -> Result<()> {
        self.ensure_running().await?;
        let completed_via = timeout(
            wait_timeout,
            self.progress.wait_for_quiescence(quiet_period),
        )
        .await
        .map_err(|_| Error::ReadinessTimeout)?;
        info!(?completed_via, "workspace load complete");
        Ok(())
    }

    /// Open a text document in the server using full-text synchronization.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP transport.
    pub async fn did_open(
        &self,
        uri: &Url,
        language_id: &str,
        version: i32,
        text: &str,
    ) -> Result<()> {
        self.ensure_running().await?;
        self.notify(
            "textDocument/didOpen",
            json!({
                "textDocument": {
                    "uri": uri.as_str(),
                    "languageId": language_id,
                    "version": version,
                    "text": text,
                }
            }),
        )
        .await
    }

    /// Replace a text document using full-text synchronization.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP transport.
    pub async fn did_change(&self, uri: &Url, version: i32, text: &str) -> Result<()> {
        self.ensure_running().await?;
        self.notify(
            "textDocument/didChange",
            json!({
                "textDocument": {
                    "uri": uri.as_str(),
                    "version": version,
                },
                "contentChanges": [
                    {
                        "text": text,
                    }
                ],
            }),
        )
        .await
    }

    /// Close a text document in the server.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP transport.
    pub async fn did_close(&self, uri: &Url) -> Result<()> {
        self.ensure_running().await?;
        self.notify(
            "textDocument/didClose",
            json!({
                "textDocument": {
                    "uri": uri.as_str(),
                }
            }),
        )
        .await
    }

    /// Gracefully stop the server.
    ///
    /// # Errors
    /// Returns protocol errors from the `shutdown` request. The final
    /// kill fallback is best-effort.
    pub async fn shutdown(self) -> Result<()> {
        if self.alive.load(Ordering::SeqCst) {
            let _: Value = self.request("shutdown", Value::Null).await?;
            self.notify("exit", Value::Null).await?;
        }
        self.alive.store(false, Ordering::SeqCst);
        *self.writer.lock().await = None;

        let mut child_slot = self.child.lock().await;
        if let Some(child) = child_slot.as_mut() {
            match timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(Error::Protocol(format!("wait failed: {e}"))),
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                }
            }
        }
        *child_slot = None;
        Ok(())
    }

    async fn ensure_running(&self) -> Result<()> {
        if self.alive.load(Ordering::SeqCst) {
            return Ok(());
        }
        let attempt = self.restarts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt > self.max_restarts {
            return Err(Error::ServerExited(None.into()));
        }
        self.spawn_process().await
    }

    async fn request<T>(&self, method: &str, params: Value) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        if let Err(err) = self.write_message(&message).await {
            self.pending.lock().await.remove(&id);
            return Err(self.with_stderr_context(err).await);
        }

        // Ensure the pending slot is reclaimed on every exit path —
        // including a Timeout — so a never-replying server cannot leak
        // entries unboundedly across repeated `request` calls.
        let response = match timeout(self.timeout, rx).await {
            Ok(received) => received,
            Err(_) => {
                self.pending.lock().await.remove(&id);
                return Err(Error::RequestTimeout);
            }
        };
        let response = match response {
            Ok(received) => received,
            Err(_) => {
                return Err(self
                    .with_stderr_context(Error::ServerExited(None.into()))
                    .await);
            }
        };
        let response = match response {
            Ok(value) => value,
            Err(err) => return Err(self.with_stderr_context(err).await),
        };
        serde_json::from_value(response).map_err(|e| Error::Protocol(e.to_string()))
    }

    async fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn write_message(&self, message: &Value) -> Result<()> {
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(Error::ServerExited(None.into()));
        };
        write_lsp_message(writer, message).await
    }

    async fn with_stderr_context(&self, err: Error) -> Error {
        let stderr = self.stderr_tail.lock().await.text();
        if stderr.is_empty() {
            return err;
        }
        match err {
            Error::Handshake(message) => Error::Handshake(format!("{message}; stderr: {stderr}")),
            Error::ServerExited(status) => Error::ServerExitedWithStderr { status, stderr },
            other => other,
        }
    }
}

fn rust_analyzer_initialization_options(config_hash: &str) -> Value {
    json!({
        "cairnConfigHash": config_hash,
        "experimental": {
            "serverStatusNotification": true
        },
    })
}

async fn check_binary_available(binary_path: &Path, request_timeout: Duration) -> Result<()> {
    let output = timeout(
        request_timeout,
        Command::new(binary_path).arg("--version").output(),
    )
    .await
    .map_err(|_| Error::RequestTimeout)?
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::BinaryMissing(binary_path.to_path_buf())
        } else {
            Error::Spawn(e)
        }
    })?;
    if output.status.success() {
        Ok(())
    } else {
        Err(Error::BinaryMissing(binary_path.to_path_buf()))
    }
}

#[derive(Default)]
struct StderrTail {
    text: String,
}

impl StderrTail {
    fn clear(&mut self) {
        self.text.clear();
    }

    fn push(&mut self, chunk: &[u8]) {
        self.text.push_str(&String::from_utf8_lossy(chunk));
        trim_to_last_bytes(&mut self.text, STDERR_TAIL_BYTES);
        trim_to_last_lines(&mut self.text, STDERR_TAIL_LINES);
    }

    fn text(&self) -> String {
        self.text.trim().to_string()
    }
}

async fn capture_stderr<R>(mut stderr: R, tail: Arc<Mutex<StderrTail>>)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut buf = [0_u8; 1024];
    loop {
        match stderr.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => tail.lock().await.push(&buf[..n]),
        }
    }
}

fn trim_to_last_bytes(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut start = text.len() - max_bytes;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text.drain(..start);
}

fn trim_to_last_lines(text: &mut String, max_lines: usize) {
    let line_count = text.lines().count();
    if line_count <= max_lines {
        return;
    }
    let mut drop_lines = line_count - max_lines;
    let mut start = 0;
    for (idx, ch) in text.char_indices() {
        if ch == '\n' {
            drop_lines -= 1;
            if drop_lines == 0 {
                start = idx + 1;
                break;
            }
        }
    }
    text.drain(..start);
}

pub(super) fn parse_definition_result(value: Value) -> Result<Vec<Location>> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    if let Ok(location) = serde_json::from_value::<Location>(value.clone()) {
        return Ok(vec![location]);
    }
    if let Ok(locations) = serde_json::from_value::<Vec<Location>>(value.clone()) {
        return Ok(locations);
    }
    let links: Vec<LocationLink> =
        serde_json::from_value(value).map_err(|e| Error::Protocol(e.to_string()))?;
    Ok(links
        .into_iter()
        .map(|link| Location {
            uri: link.target_uri,
            range: link.target_selection_range.unwrap_or(link.target_range),
        })
        .collect())
}
