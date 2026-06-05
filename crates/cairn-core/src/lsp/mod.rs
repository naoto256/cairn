//! Minimal LSP subprocess client for workspace analyzers.
//!
//! PR2 keeps this deliberately small: enough JSON-RPC framing,
//! lifecycle, timeout, and `textDocument/definition` support for the
//! rust-analyzer integration planned in PR3, without pulling in the
//! full `lsp-types` surface yet.

pub mod pool;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::time::{sleep, timeout};
use tracing::{debug, info};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const WORKSPACE_LOAD_QUIET_PERIOD: Duration = Duration::from_secs(5);
const MAX_RESTARTS: usize = 3;
pub const CONTENT_MODIFIED_ERROR_CODE: i64 = -32801;
/// Cap on a single LSP message body. rust-analyzer's largest legitimate
/// responses (workspace symbols on huge crates) stay well under this; a
/// `Content-Length` above the cap is treated as a malicious or runaway
/// subprocess and refused before allocation, preventing local DoS.
const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LSP binary not available: {0}")]
    BinaryMissing(PathBuf),
    #[error("failed to spawn LSP server: {0}")]
    Spawn(std::io::Error),
    #[error("LSP handshake failed: {0}")]
    Handshake(String),
    #[error("LSP request timed out")]
    Timeout,
    #[error("LSP server exited{0}")]
    ServerExited(ExitStatusDetail),
    #[error("LSP protocol error: {0}")]
    Protocol(String),
    #[error("LSP protocol error: {message}")]
    ResponseError { code: i64, message: String },
}

impl Error {
    #[must_use]
    pub fn is_content_modified(&self) -> bool {
        matches!(
            self,
            Self::ResponseError {
                code: CONTENT_MODIFIED_ERROR_CODE,
                ..
            }
        )
    }
}

#[derive(Debug)]
pub struct ExitStatusDetail(Option<ExitStatus>);

impl std::fmt::Display for ExitStatusDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(status) => write!(f, ": {status}"),
            None => Ok(()),
        }
    }
}

impl From<Option<ExitStatus>> for ExitStatusDetail {
    fn from(status: Option<ExitStatus>) -> Self {
        Self(status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Url(String);

impl Url {
    /// Build a `file://` URI from an absolute UTF-8 path.
    ///
    /// # Errors
    /// Returns [`Error::Protocol`] for relative or non-UTF-8 paths.
    pub fn from_file_path(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(Error::Protocol(format!(
                "file URI path must be absolute: {}",
                path.display()
            )));
        }
        let raw = path
            .to_str()
            .ok_or_else(|| Error::Protocol(format!("non-utf8 path: {}", path.display())))?;
        Ok(Self(format!("file://{}", percent_encode_path(raw))))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Url {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Url {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Zero-based LSP position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub line: u32,
    pub character: u32,
}

/// Zero-based, half-open LSP range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub uri: Url,
    pub range: Range,
}

pub struct LspClient {
    binary_path: Option<PathBuf>,
    workspace_root: PathBuf,
    config_hash: String,
    timeout: Duration,
    max_restarts: usize,
    restarts: AtomicUsize,
    next_id: AtomicU64,
    alive: Arc<AtomicBool>,
    writer: Arc<Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>>,
    child: Mutex<Option<Child>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    progress: Arc<ProgressState>,
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
            workspace_root.to_path_buf(),
            config_hash.to_string(),
            request_timeout,
            MAX_RESTARTS,
        );
        client.spawn_process().await?;
        Ok(client)
    }

    fn new(
        binary_path: Option<PathBuf>,
        workspace_root: PathBuf,
        config_hash: String,
        request_timeout: Duration,
        max_restarts: usize,
    ) -> Self {
        Self {
            binary_path,
            workspace_root,
            config_hash,
            timeout: request_timeout,
            max_restarts,
            restarts: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            alive: Arc::new(AtomicBool::new(false)),
            writer: Arc::new(Mutex::new(None)),
            child: Mutex::new(None),
            pending: Arc::new(Mutex::new(HashMap::new())),
            progress: Arc::new(ProgressState::default()),
        }
    }

    #[cfg(test)]
    async fn start_with_io<R, W>(
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
            workspace_root.to_path_buf(),
            config_hash.to_string(),
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
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(drain_stderr(stderr));
        }

        self.install_transport(stdout, stdin).await;
        *self.child.lock().await = Some(child);
        self.initialize().await?;
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
        let _: Value = self
            .request(
                "initialize",
                json!({
                    "processId": Value::Null,
                    "rootUri": root_uri.as_str(),
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
                    "initializationOptions": {
                        "cairnConfigHash": self.config_hash,
                        "experimental": {
                            "serverStatusNotification": true
                        },
                    },
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
    /// Returns [`Error::Timeout`] when no completed progress sequence
    /// is observed before `wait_timeout` elapses.
    pub async fn wait_for_workspace_load(&self, wait_timeout: Duration) -> Result<()> {
        self.wait_for_workspace_load_with_quiescence(wait_timeout, WORKSPACE_LOAD_QUIET_PERIOD)
            .await
    }

    async fn wait_for_workspace_load_with_quiescence(
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
        .map_err(|_| Error::Timeout)?;
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
            return Err(err);
        }

        // Ensure the pending slot is reclaimed on every exit path —
        // including a Timeout — so a never-replying server cannot leak
        // entries unboundedly across repeated `request` calls.
        let response = match timeout(self.timeout, rx).await {
            Ok(received) => received,
            Err(_) => {
                self.pending.lock().await.remove(&id);
                return Err(Error::Timeout);
            }
        };
        let response = response.map_err(|_| Error::ServerExited(None.into()))??;
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
}

async fn check_binary_available(binary_path: &Path, request_timeout: Duration) -> Result<()> {
    let output = timeout(
        request_timeout,
        Command::new(binary_path).arg("--version").output(),
    )
    .await
    .map_err(|_| Error::Timeout)?
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

async fn reader_loop<R>(
    mut reader: R,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    alive: Arc<AtomicBool>,
    writer: Arc<Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>>,
    progress: Arc<ProgressState>,
) where
    R: AsyncRead + Send + Unpin + 'static,
{
    loop {
        match read_lsp_message(&mut reader).await {
            Ok(Some(message)) => {
                log_notification(&message);
                if let Some((id, result)) = response_result(&message) {
                    let tx = pending.lock().await.remove(&id);
                    if let Some(tx) = tx {
                        let _ = tx.send(result);
                    }
                } else if is_progress_notification(&message) {
                    progress.record(&message).await;
                } else if is_server_status_notification(&message) {
                    progress.record_server_status(&message).await;
                } else if let Some(response) = server_request_response(&message) {
                    if let Some(writer) = writer.lock().await.as_mut() {
                        let _ = write_lsp_message(writer, &response).await;
                    }
                }
            }
            Ok(None) => {
                alive.store(false, Ordering::SeqCst);
                fail_pending(&pending, Error::ServerExited(None.into())).await;
                break;
            }
            Err(err) => {
                alive.store(false, Ordering::SeqCst);
                fail_pending(&pending, err).await;
                break;
            }
        }
    }
}

fn log_notification(message: &Value) {
    if message.get("id").is_some() {
        return;
    }
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    debug!(method, "lsp_server_notification");
}

#[derive(Default)]
struct ProgressState {
    inner: Mutex<ProgressSnapshot>,
    notify: Notify,
}

#[derive(Default)]
struct ProgressSnapshot {
    active_tokens: HashSet<String>,
    saw_begin: bool,
    change_seq: u64,
}

impl ProgressState {
    async fn record(&self, message: &Value) {
        let Some(params) = message.get("params") else {
            return;
        };
        let token = params
            .get("token")
            .map(progress_token)
            .unwrap_or_else(|| "<missing>".to_string());
        let Some(kind) = params
            .get("value")
            .and_then(|v| v.get("kind"))
            .and_then(Value::as_str)
        else {
            return;
        };
        let title = params
            .get("value")
            .and_then(|v| v.get("title"))
            .and_then(Value::as_str);
        debug!(
            method = "$/progress",
            token = %token,
            kind,
            title,
            "lsp_progress"
        );

        let mut inner = self.inner.lock().await;
        inner.change_seq = inner.change_seq.saturating_add(1);
        match kind {
            "begin" => {
                inner.active_tokens.insert(token);
                inner.saw_begin = true;
            }
            "end" => {
                inner.active_tokens.remove(&token);
            }
            _ => {}
        }
        drop(inner);
        self.notify.notify_waiters();
    }

    async fn record_server_status(&self, message: &Value) {
        let quiescent = message
            .get("params")
            .and_then(|params| params.get("quiescent"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let health = message
            .get("params")
            .and_then(|params| params.get("health"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        debug!(
            method = "rust-analyzer/serverStatus",
            health, quiescent, "lsp_server_status"
        );
        if !quiescent {
            return;
        }

        info!(health, "rust-analyzer workspace is quiescent");
    }

    async fn wait_for_quiescence(&self, quiet_period: Duration) -> WorkspaceLoadComplete {
        loop {
            let ready_seq = {
                let inner = self.inner.lock().await;
                if inner.saw_begin && inner.active_tokens.is_empty() {
                    Some(inner.change_seq)
                } else {
                    None
                }
            };

            if let Some(seq) = ready_seq {
                tokio::select! {
                    () = sleep(quiet_period) => {
                        let inner = self.inner.lock().await;
                        if inner.saw_begin
                            && inner.active_tokens.is_empty()
                            && inner.change_seq == seq
                        {
                            return WorkspaceLoadComplete::ProgressQuiescence;
                        }
                    }
                    () = self.notify.notified() => {}
                }
            } else {
                self.notify.notified().await;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkspaceLoadComplete {
    ProgressQuiescence,
}

fn progress_token(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}

fn is_progress_notification(message: &Value) -> bool {
    message.get("id").is_none()
        && message.get("method").and_then(Value::as_str) == Some("$/progress")
}

fn is_server_status_notification(message: &Value) -> bool {
    message.get("id").is_none()
        && message.get("method").and_then(Value::as_str) == Some("rust-analyzer/serverStatus")
}

fn server_request_response(message: &Value) -> Option<Value> {
    let id = message.get("id")?;
    let method = message.get("method")?.as_str()?;
    if method != "window/workDoneProgress/create" {
        return None;
    }
    Some(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": Value::Null,
    }))
}

fn response_result(message: &Value) -> Option<(u64, Result<Value>)> {
    let id = message.get("id")?.as_u64()?;
    if let Some(error) = message.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown LSP error")
            .to_string();
        if let Some(code) = error.get("code").and_then(Value::as_i64) {
            return Some((id, Err(Error::ResponseError { code, message })));
        }
        return Some((id, Err(Error::Protocol(message))));
    }
    Some((
        id,
        Ok(message.get("result").cloned().unwrap_or(Value::Null)),
    ))
}

async fn fail_pending(pending: &Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>, err: Error) {
    let mut pending = pending.lock().await;
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(Error::Protocol(err.to_string())));
    }
}

async fn drain_stderr<R>(mut stderr: R)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut buf = [0_u8; 1024];
    while matches!(stderr.read(&mut buf).await, Ok(n) if n > 0) {}
}

async fn read_lsp_message<R>(reader: &mut R) -> Result<Option<Value>>
where
    R: AsyncRead + Unpin,
{
    let mut header = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        match reader.read_exact(&mut byte).await {
            Ok(_) => {
                header.push(byte[0]);
                if header.ends_with(b"\r\n\r\n") || header.ends_with(b"\n\n") {
                    break;
                }
                if header.len() > 8192 {
                    return Err(Error::Protocol("LSP header too large".into()));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && header.is_empty() => {
                return Ok(None);
            }
            Err(e) => return Err(Error::Protocol(format!("read header: {e}"))),
        }
    }

    let header_text =
        std::str::from_utf8(&header).map_err(|e| Error::Protocol(format!("header utf8: {e}")))?;
    let len = content_length(header_text)?;
    if len > MAX_BODY_SIZE {
        return Err(Error::Protocol(format!(
            "LSP message body exceeds {MAX_BODY_SIZE} bytes ({len})"
        )));
    }
    let mut body = vec![0_u8; len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| Error::Protocol(format!("read body: {e}")))?;
    serde_json::from_slice(&body).map_err(|e| Error::Protocol(format!("body json: {e}")))
}

fn content_length(header: &str) -> Result<usize> {
    for line in header.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|e| Error::Protocol(format!("invalid content-length: {e}")));
        }
    }
    Err(Error::Protocol("missing content-length".into()))
}

async fn write_lsp_message<W>(writer: &mut W, message: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let body = serde_json::to_vec(message).map_err(|e| Error::Protocol(e.to_string()))?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer
        .write_all(header.as_bytes())
        .await
        .map_err(|e| Error::Protocol(format!("write header: {e}")))?;
    writer
        .write_all(&body)
        .await
        .map_err(|e| Error::Protocol(format!("write body: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| Error::Protocol(format!("flush: {e}")))?;
    Ok(())
}

fn parse_definition_result(value: Value) -> Result<Vec<Location>> {
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocationLink {
    target_uri: Url,
    target_range: Range,
    target_selection_range: Option<Range>,
}

fn percent_encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for b in path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(char::from(b));
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, split};

    #[tokio::test]
    async fn initialize_definition_and_shutdown_roundtrip() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let server = tokio::spawn(fake_server(server_io, FakeMode::Normal));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn fake"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let locations = client
            .definition(
                &Url::from("file:///tmp/cairn%20fake/src/lib.rs"),
                Position {
                    line: 10,
                    character: 4,
                },
            )
            .await
            .unwrap();

        assert_eq!(locations.len(), 1);
        assert_eq!(
            locations[0].uri.as_str(),
            "file:///tmp/cairn%20fake/src/lib.rs"
        );
        assert_eq!(locations[0].range.start.line, 2);

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn initialize_opts_into_rust_analyzer_server_status() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let server = tokio::spawn(fake_server(server_io, FakeMode::RequireServerStatusOptIn));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn document_sync_notifications_use_full_text_payloads() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(fake_server(server_io, FakeMode::RecordDocumentSync(tx)));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let uri = Url::from("file:///tmp/cairn/src/lib.rs");

        client
            .did_open(&uri, "rust", 1, "fn main() {}\n")
            .await
            .unwrap();
        client
            .did_change(&uri, 2, "fn main() { println!(\"hi\"); }\n")
            .await
            .unwrap();
        client.did_close(&uri).await.unwrap();

        let open = rx.recv().await.unwrap();
        assert_eq!(
            open.get("method").and_then(Value::as_str),
            Some("textDocument/didOpen")
        );
        let open_doc = &open["params"]["textDocument"];
        assert_eq!(open_doc["uri"], uri.as_str());
        assert_eq!(open_doc["languageId"], "rust");
        assert_eq!(open_doc["version"], 1);
        assert_eq!(open_doc["text"], "fn main() {}\n");

        let change = rx.recv().await.unwrap();
        assert_eq!(
            change.get("method").and_then(Value::as_str),
            Some("textDocument/didChange")
        );
        assert_eq!(change["params"]["textDocument"]["uri"], uri.as_str());
        assert_eq!(change["params"]["textDocument"]["version"], 2);
        assert_eq!(
            change["params"]["contentChanges"][0]["text"],
            "fn main() { println!(\"hi\"); }\n"
        );

        let close = rx.recv().await.unwrap();
        assert_eq!(
            close.get("method").and_then(Value::as_str),
            Some("textDocument/didClose")
        );
        assert_eq!(close["params"]["textDocument"]["uri"], uri.as_str());

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn definition_times_out_when_server_never_replies() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let _server = tokio::spawn(fake_server(server_io, FakeMode::DefinitionTimeout));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_millis(20),
        )
        .await
        .unwrap();

        let err = client
            .definition(
                &Url::from("file:///tmp/cairn/src/lib.rs"),
                Position {
                    line: 0,
                    character: 0,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Timeout));
    }

    #[tokio::test]
    async fn workspace_load_waits_for_progress_quiescence() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let server = tokio::spawn(fake_server(server_io, FakeMode::ProgressCompletes));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        client
            .wait_for_workspace_load_with_quiescence(
                Duration::from_secs(1),
                Duration::from_millis(20),
            )
            .await
            .unwrap();

        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn workspace_load_ignores_server_status_without_progress() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let _server = tokio::spawn(fake_server(server_io, FakeMode::ServerStatusQuiescent));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let err = client
            .wait_for_workspace_load_with_quiescence(
                Duration::from_millis(20),
                Duration::from_millis(5),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Timeout));
    }

    #[tokio::test]
    async fn workspace_load_resets_quiet_timer_when_new_progress_arrives() {
        let progress = Arc::new(ProgressState::default());
        let waiter = {
            let progress = Arc::clone(&progress);
            tokio::spawn(async move {
                progress
                    .wait_for_quiescence(Duration::from_millis(50))
                    .await
            })
        };

        progress.record(&progress_message("phase-1", "begin")).await;
        progress.record(&progress_message("phase-1", "end")).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        progress.record(&progress_message("phase-2", "begin")).await;
        tokio::time::sleep(Duration::from_millis(35)).await;

        assert!(
            !waiter.is_finished(),
            "quiet timer should reset when new progress begins"
        );
        progress.record(&progress_message("phase-2", "end")).await;
        let completed = timeout(Duration::from_millis(100), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed, WorkspaceLoadComplete::ProgressQuiescence);
    }

    #[tokio::test]
    async fn workspace_load_does_not_finish_on_progress_end_without_begin() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let _server = tokio::spawn(fake_server(server_io, FakeMode::ProgressEndWithoutBegin));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let err = client
            .wait_for_workspace_load(Duration::from_millis(20))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Timeout));
    }

    #[tokio::test]
    async fn workspace_load_times_out_without_progress_end() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let _server = tokio::spawn(fake_server(server_io, FakeMode::ProgressNeverEnds));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let err = client
            .wait_for_workspace_load(Duration::from_millis(20))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Timeout));
    }

    #[tokio::test]
    async fn did_open_notifies_server_before_definition() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let server = tokio::spawn(fake_server(server_io, FakeMode::RequireDidOpen));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn fake"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let uri = Url::from("file:///tmp/cairn%20fake/src/lib.rs");

        client
            .did_open(&uri, "rust", 1, "fn main() {}\n")
            .await
            .unwrap();
        let locations = client
            .definition(
                &uri,
                Position {
                    line: 0,
                    character: 3,
                },
            )
            .await
            .unwrap();

        assert_eq!(locations.len(), 1);
        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn early_server_exit_surfaces_as_server_exited() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let _server = tokio::spawn(fake_server(server_io, FakeMode::CrashAfterInitialize));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let err = client
            .definition(
                &Url::from("file:///tmp/cairn/src/lib.rs"),
                Position {
                    line: 0,
                    character: 0,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::ServerExited(_)) || matches!(err, Error::Protocol(_)),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn file_url_percent_encodes_spaces() {
        let url = Url::from_file_path(Path::new("/tmp/cairn fake/src/lib.rs")).unwrap();
        assert_eq!(url.as_str(), "file:///tmp/cairn%20fake/src/lib.rs");
    }

    #[test]
    fn parses_location_link_definition_result() {
        let value = json!([
            {
                "targetUri": "file:///tmp/lib.rs",
                "targetRange": {
                    "start": { "line": 1, "character": 0 },
                    "end": { "line": 1, "character": 8 }
                },
                "targetSelectionRange": {
                    "start": { "line": 1, "character": 4 },
                    "end": { "line": 1, "character": 8 }
                }
            }
        ]);

        let locations = parse_definition_result(value).unwrap();
        assert_eq!(locations[0].range.start.character, 4);
    }

    #[test]
    fn response_result_preserves_lsp_error_code() {
        let (_, result) = response_result(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "error": {
                "code": CONTENT_MODIFIED_ERROR_CODE,
                "message": "content modified"
            }
        }))
        .unwrap();

        let err = result.unwrap_err();
        assert!(err.is_content_modified());
        assert_eq!(err.to_string(), "LSP protocol error: content modified");
    }

    #[tokio::test]
    async fn oversized_content_length_is_rejected_before_allocation() {
        // A `Content-Length` above MAX_BODY_SIZE must be refused before
        // any allocation — a buggy or malicious subprocess must not be
        // able to force arbitrary-sized buffer allocation. Build a
        // header by hand against a duplex pipe and confirm
        // `read_lsp_message` errors out without ever reading the body.
        let (mut a, mut b) = tokio::io::duplex(256);
        let oversized = MAX_BODY_SIZE + 1;
        let header = format!("Content-Length: {oversized}\r\n\r\n");
        a.write_all(header.as_bytes()).await.unwrap();
        // Intentionally do NOT supply the body — a buggy implementation
        // would block here trying to read `oversized` bytes; the fixed
        // version must return Error::Protocol from the header check.
        let err = read_lsp_message(&mut b).await.unwrap_err();
        assert!(
            matches!(err, Error::Protocol(ref msg) if msg.contains("body exceeds")),
            "unexpected: {err:?}"
        );
    }

    #[tokio::test]
    async fn pending_map_is_cleared_on_timeout() {
        // A server that never replies must not leak pending request
        // entries. Drive a definition call against the
        // `DefinitionTimeout` fake and assert the map is empty after
        // the timeout error returns.
        let (client_io, server_io) = tokio::io::duplex(8192);
        let _server = tokio::spawn(fake_server(server_io, FakeMode::DefinitionTimeout));
        let (client_reader, client_writer) = split(client_io);
        let client = LspClient::start_with_io(
            client_reader,
            client_writer,
            Path::new("/tmp/cairn"),
            "cfg",
            Duration::from_millis(20),
        )
        .await
        .unwrap();

        let err = client
            .definition(
                &Url::from("file:///tmp/cairn/src/lib.rs"),
                Position {
                    line: 0,
                    character: 0,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Timeout));

        // Pending must be empty after the timed-out request returns.
        assert!(
            client.pending.lock().await.is_empty(),
            "pending map leaked entries on timeout"
        );
    }

    enum FakeMode {
        Normal,
        DefinitionTimeout,
        CrashAfterInitialize,
        ProgressCompletes,
        ProgressNeverEnds,
        ProgressEndWithoutBegin,
        ServerStatusQuiescent,
        RequireServerStatusOptIn,
        RequireDidOpen,
        RecordDocumentSync(tokio::sync::mpsc::UnboundedSender<Value>),
    }

    async fn fake_server(stream: DuplexStream, mode: FakeMode) {
        let (mut reader, mut writer) = split(stream);
        let mut did_open = false;
        while let Some(message) = read_lsp_message(&mut reader).await.unwrap() {
            let method = message.get("method").and_then(Value::as_str);
            let id = message.get("id").and_then(Value::as_u64);
            match (method, id) {
                (Some("initialize"), Some(id)) => {
                    if matches!(mode, FakeMode::RequireServerStatusOptIn) {
                        let enabled = message
                            .get("params")
                            .and_then(|params| params.get("initializationOptions"))
                            .and_then(|options| options.get("experimental"))
                            .and_then(|experimental| experimental.get("serverStatusNotification"))
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        assert!(
                            enabled,
                            "initialize did not opt into rust-analyzer/serverStatus: {message}"
                        );
                    }
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "capabilities": {
                                    "definitionProvider": true
                                }
                            }
                        }),
                    )
                    .await
                    .unwrap();
                }
                (Some("initialized"), None) => {
                    if matches!(mode, FakeMode::CrashAfterInitialize) {
                        return;
                    }
                    if matches!(mode, FakeMode::ServerStatusQuiescent) {
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "method": "rust-analyzer/serverStatus",
                                "params": {
                                    "health": "ok",
                                    "quiescent": true,
                                    "message": null
                                }
                            }),
                        )
                        .await
                        .unwrap();
                    }
                    if matches!(mode, FakeMode::ProgressEndWithoutBegin) {
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "method": "$/progress",
                                "params": {
                                    "token": "cairn-progress",
                                    "value": { "kind": "end", "message": "ready" }
                                }
                            }),
                        )
                        .await
                        .unwrap();
                    }
                    if matches!(
                        mode,
                        FakeMode::ProgressCompletes | FakeMode::ProgressNeverEnds
                    ) {
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "id": 9001,
                                "method": "window/workDoneProgress/create",
                                "params": { "token": "cairn-progress" }
                            }),
                        )
                        .await
                        .unwrap();
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "method": "$/progress",
                                "params": {
                                    "token": "cairn-progress",
                                    "value": { "kind": "begin", "title": "loading workspace" }
                                }
                            }),
                        )
                        .await
                        .unwrap();
                        if matches!(mode, FakeMode::ProgressCompletes) {
                            write_lsp_message(
                                &mut writer,
                                &json!({
                                    "jsonrpc": "2.0",
                                    "method": "$/progress",
                                    "params": {
                                        "token": "cairn-progress",
                                        "value": { "kind": "end", "message": "ready" }
                                    }
                                }),
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
                (
                    Some(
                        "textDocument/didOpen" | "textDocument/didChange" | "textDocument/didClose",
                    ),
                    None,
                ) => {
                    if matches!(method, Some("textDocument/didOpen")) {
                        did_open = true;
                    }
                    if let FakeMode::RecordDocumentSync(tx) = &mode {
                        tx.send(message).unwrap();
                    }
                }
                (Some("textDocument/definition"), Some(id)) => {
                    if matches!(mode, FakeMode::DefinitionTimeout) {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    } else if matches!(mode, FakeMode::RequireDidOpen) && !did_open {
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {
                                    "code": -32602,
                                    "message": "file not found"
                                }
                            }),
                        )
                        .await
                        .unwrap();
                    } else {
                        write_lsp_message(
                            &mut writer,
                            &json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": [
                                    {
                                        "uri": "file:///tmp/cairn%20fake/src/lib.rs",
                                        "range": {
                                            "start": { "line": 2, "character": 8 },
                                            "end": { "line": 2, "character": 14 }
                                        }
                                    }
                                ]
                            }),
                        )
                        .await
                        .unwrap();
                    }
                }
                (Some("shutdown"), Some(id)) => {
                    write_lsp_message(
                        &mut writer,
                        &json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": Value::Null
                        }),
                    )
                    .await
                    .unwrap();
                }
                (Some("exit"), None) => return,
                _ => {}
            }
        }
    }

    fn progress_message(token: &str, kind: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "$/progress",
            "params": {
                "token": token,
                "value": { "kind": kind }
            }
        })
    }
}
