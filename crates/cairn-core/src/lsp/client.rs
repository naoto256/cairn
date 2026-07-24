//! Single LSP child process: spawning, JSON-RPC request/response
//! plumbing, readiness, bounded restarts, and shutdown.
//!
//! One background reader task (`reader::reader_loop`) owns stdout
//! and resolves responses into per-request oneshot channels
//! registered in `pending`; stdin sits behind a writer mutex.
//! Forced teardown of an installed child is centralized in
//! `force_terminate`, the fail-closed path when termination cannot
//! be proven. Clean graceful shutdown reaps inline and delegates
//! its forced fallback to `force_terminate`; availability probes
//! and missing-stdio failures own and reap their local `Child`
//! directly.
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

// Default per-request timeout (also bounds the `--version`
// availability probe in `start`).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
// Shutdown is short by design: after the graceful request times out,
// the client still sends `exit` and lets process cleanup finish.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
// Wait for an LSP server's initial index chatter to quiet down before
// treating startup as ready; this avoids racing early definition calls.
const WORKSPACE_LOAD_QUIET_PERIOD: Duration = Duration::from_secs(5);
// Bound automatic restarts so a crashing server cannot loop forever in
// a daemon process.
const MAX_RESTARTS: usize = 3;
// Keep enough stderr to diagnose startup failures without surfacing an
// unbounded server log in user-facing errors.
const STDERR_SECTION_BYTES: usize = 1024;
// Once truncated, keep the first HEAD and last TAIL lines (each
// section further capped at STDERR_SECTION_BYTES) joined by the
// omission marker.
const STDERR_HEAD_LINES: usize = 5;
const STDERR_TAIL_LINES: usize = 5;
const STDERR_OMISSION_MARKER: &str = " ... ";

/// Handle to one LSP server subprocess speaking JSON-RPC over
/// stdio.
///
/// Shared state lives behind `Arc`s so the background reader task
/// and `LspProcessControl` clones stay valid independently of this
/// handle. `shutdown(self)` is the graceful exit. Dropping the
/// client alone does not kill the child: the `Child` sits behind a
/// shared `Arc<Mutex<..>>`, so `kill_on_drop(true)` fires only
/// when the last owner (client or `LspProcessControl` clone)
/// drops it, and that path skips the `shutdown`/`exit` handshake.
pub struct LspClient {
    binary_path: Option<PathBuf>,
    args: Vec<String>,
    env: Vec<(String, String)>,
    workspace_root: PathBuf,
    initialization_options: Value,
    timeout: Duration,
    max_restarts: usize,
    // Lifetime respawn-attempt count; never reset, so a repeatedly
    // crashing server stops being restarted once `max_restarts` is
    // exhausted.
    restarts: AtomicUsize,
    // Monotonic JSON-RPC request id source. Ids are not reused
    // before u64 wrap-around, so a late reply to a timed-out
    // request finds no `pending` entry and is dropped rather than
    // resolving a newer request.
    next_id: AtomicU64,
    // True while a transport is installed; cleared by the reader
    // task on stdout EOF / read error and by `force_terminate`.
    alive: Arc<AtomicBool>,
    // One-way latch: once set (shutdown or pool stop), respawns are
    // refused with `PoolStopped`.
    stopping: Arc<AtomicBool>,
    writer: Arc<Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>>,
    child: Arc<Mutex<Option<Child>>>,
    // In-flight requests by id. The reader task resolves entries;
    // every termination path drains the map so callers cannot hang.
    pub(super) pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
    progress: Arc<ProgressState>,
    stderr_tail: Arc<Mutex<StderrTail>>,
}

/// Cloneable control-plane handle for one LSP child process.
///
/// This deliberately contains no document or request-operation state. A pool
/// shutdown must be able to stop and reap the child while a normal analyzer
/// pass holds the entry's data-plane mutex for the duration of its work.
#[derive(Clone)]
pub(crate) struct LspProcessControl {
    alive: Arc<AtomicBool>,
    stopping: Arc<AtomicBool>,
    writer: Arc<Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>>,
    child: Arc<Mutex<Option<Child>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>,
}

impl LspProcessControl {
    /// Permanently prevent this client from spawning a replacement child, then
    /// terminate and reap the current child without taking the pool entry's
    /// data-plane mutex.
    pub(crate) async fn stop_and_terminate(&self) -> Result<()> {
        self.stopping.store(true, Ordering::SeqCst);
        self.force_terminate().await
    }

    async fn force_terminate(&self) -> Result<()> {
        self.alive.store(false, Ordering::SeqCst);
        // Kill first. A wedged server can backpressure a pipe write while the
        // writer mutex is held; waiting for that mutex before kill would make
        // the process-control plane depend on the data plane it must unblock.
        let mut child_slot = self.child.lock().await;
        let termination_err = if let Some(child) = child_slot.as_mut() {
            let _ = child.kill().await;
            child
                .wait()
                .await
                .err()
                .map(|err| Error::ChildTerminationFailed(format!("wait() after kill: {err}")))
        } else {
            None
        };
        *child_slot = None;
        drop(child_slot);
        {
            let mut writer = self.writer.lock().await;
            *writer = None;
        }
        // Dropping the pending senders without a reply wakes every
        // in-flight `request` with a channel-closed error, which the
        // request path reports as `ServerExited`.
        {
            let mut pending = self.pending.lock().await;
            pending.clear();
        }
        match termination_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
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
        env: Vec<(String, String)>,
        workspace_root: &Path,
        initialization_options: Value,
        request_timeout: Duration,
    ) -> Result<Self> {
        let client = Self::configured(
            binary_path,
            args,
            env,
            workspace_root,
            initialization_options,
            request_timeout,
        );
        client.start_process().await?;
        Ok(client)
    }

    pub(super) fn configured(
        binary_path: &Path,
        args: Vec<String>,
        env: Vec<(String, String)>,
        workspace_root: &Path,
        initialization_options: Value,
        request_timeout: Duration,
    ) -> Self {
        Self::new(
            Some(binary_path.to_path_buf()),
            args,
            env,
            workspace_root.to_path_buf(),
            initialization_options,
            request_timeout,
            MAX_RESTARTS,
        )
    }

    pub(super) async fn start_process(&self) -> Result<()> {
        self.spawn_process().await
    }

    fn new(
        binary_path: Option<PathBuf>,
        args: Vec<String>,
        env: Vec<(String, String)>,
        workspace_root: PathBuf,
        initialization_options: Value,
        request_timeout: Duration,
        max_restarts: usize,
    ) -> Self {
        Self {
            binary_path,
            args,
            env,
            workspace_root,
            initialization_options,
            timeout: request_timeout,
            max_restarts,
            restarts: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            alive: Arc::new(AtomicBool::new(false)),
            stopping: Arc::new(AtomicBool::new(false)),
            writer: Arc::new(Mutex::new(None)),
            child: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            progress: Arc::new(ProgressState::default()),
            stderr_tail: Arc::new(Mutex::new(StderrTail::default())),
        }
    }

    /// Test-only constructor over in-memory pipes: no child process
    /// exists, and `max_restarts` is 0 so a dead transport fails
    /// fast instead of attempting a respawn.
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
        if self.stopping.load(Ordering::SeqCst) {
            return Err(Error::PoolStopped);
        }
        // `binary_path` is `None` only for the test-only in-memory
        // transport, which has no process to (re)spawn; report the
        // transport as gone.
        let Some(binary_path) = &self.binary_path else {
            return Err(Error::ServerExited(None.into()));
        };

        // Fail closed if the prior child's termination cannot be
        // proven — spawning a fresh child alongside a
        // possibly-still-live orphan would violate the "no two
        // instances per key" invariant callers rely on.
        self.force_terminate().await?;

        // `kill_on_drop(true)` is the last-resort backstop: an
        // unexpected drop of the `LspClient` (panic in caller,
        // future cancellation) still SIGKILLs the child.
        // `force_terminate` is preferred on every explicit failure
        // path because it also reaps the child via `wait()`.
        let mut child = Command::new(binary_path)
            .args(&self.args)
            .envs(self.env.iter().map(|(key, value)| (key, value)))
            .current_dir(&self.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(Error::Spawn)?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => return Err(reap_local_child(&mut child, "missing child stdin").await),
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => return Err(reap_local_child(&mut child, "missing child stdout").await),
        };
        self.stderr_tail.lock().await.clear();
        // Clear readiness state before installing the new
        // transport — otherwise a respawn inherits `saw_begin`
        // from the prior server and `wait_for_workspace_load`
        // returns before the new server has actually announced
        // any `$/progress` begin.
        self.progress.reset().await;
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(capture_stderr(stderr, Arc::clone(&self.stderr_tail)));
        }

        // Publish the child under the lock, re-checking `stopping`
        // there: a stop that ran before the child became visible in
        // `self.child` could not have killed it, so this path must
        // reap it locally instead.
        {
            let mut child_slot = self.child.lock().await;
            if self.stopping.load(Ordering::SeqCst) {
                drop(child_slot);
                return Err(reap_local_child(&mut child, "client is stopping").await);
            }
            *child_slot = Some(child);
        }
        self.install_transport(stdout, stdin).await;
        // `install_transport` re-arms `alive` and the writer. If a
        // stop raced in after the child was published, undo that via
        // `force_terminate` (idempotent) and report `PoolStopped`.
        if self.stopping.load(Ordering::SeqCst) {
            return Err(match self.force_terminate().await {
                Ok(()) => Error::PoolStopped,
                Err(cleanup) => Error::OperationWithCleanupFailure {
                    original: Box::new(Error::PoolStopped),
                    cleanup: Box::new(cleanup),
                },
            });
        }
        if let Err(err) = self.initialize().await {
            let contextual = self.with_stderr_context(err).await;
            return Err(match self.force_terminate().await {
                Ok(()) => contextual,
                Err(cleanup_err) => Error::OperationWithCleanupFailure {
                    original: Box::new(contextual),
                    cleanup: Box::new(cleanup_err),
                },
            });
        }
        Ok(())
    }

    /// Terminate the child and reap it via `wait()`. Returns
    /// `Ok(())` when either the child was successfully reaped, or
    /// there was no child slot to begin with. Returns
    /// [`Error::ChildTerminationFailed`] when `wait()` errors after
    /// the kill attempt — that is our termination-proof signal, and
    /// callers must fail-closed rather than spawn a replacement.
    ///
    /// `kill()`'s own return value is ignored on purpose: a
    /// concurrently-exited child returns an error from `kill()`,
    /// but the subsequent `wait()` still succeeds and provides the
    /// termination proof.
    ///
    /// The narrow ownership contract this helper enforces is:
    /// "when we are abandoning a `Child` that we (or another
    /// caller) placed in `self.child`, `force_terminate` is the
    /// canonical path — it drops the writer, clears pending
    /// oneshots, kills the child, and reaps it via `wait()`."
    ///
    /// Not every failure path in this module routes through it:
    /// missing-stdio uses an inline `reap_local_child` shape
    /// because the `Child` isn't in `self.child` yet; the
    /// availability probes are standalone `Command::spawn` +
    /// `wait()` outside `LspClient` entirely; the graceful
    /// `shutdown(self)` calls `force_terminate` only when its
    /// bounded graceful wait fails or the child slot needs kill +
    /// reap. Those variants are documented at their call sites.
    pub(crate) async fn force_terminate(&self) -> Result<()> {
        self.process_control().force_terminate().await
    }

    /// Snapshot the control-plane `Arc`s into a cloneable handle
    /// usable without holding the pool entry's data-plane mutex.
    pub(crate) fn process_control(&self) -> LspProcessControl {
        LspProcessControl {
            alive: Arc::clone(&self.alive),
            stopping: Arc::clone(&self.stopping),
            writer: Arc::clone(&self.writer),
            child: Arc::clone(&self.child),
            pending: Arc::clone(&self.pending),
        }
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

    /// Run the LSP lifecycle handshake: the `initialize` request
    /// followed by the `initialized` notification, which must
    /// complete before other requests per the LSP spec.
    /// Capabilities advertise only what this client relies on:
    /// `workDoneProgress` (readiness tracking) and definition
    /// `linkSupport`. An `initialize` failure is flattened into
    /// [`Error::Handshake`]; a failed `initialized` notification
    /// surfaces as the underlying transport error instead.
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

    /// Readiness means: at least one `$/progress` `begin` has been
    /// observed, no progress tokens remain active, and that state
    /// held unchanged for `quiet_period`. The quiet period guards
    /// against servers that end one startup progress sequence and
    /// immediately begin another.
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

    /// Gracefully stop the server. Always terminates and reaps the
    /// child before returning: after a bounded graceful window
    /// (`SHUTDOWN_TIMEOUT`), any remaining child is force-terminated
    /// via [`Self::force_terminate`]. Graceful protocol errors and
    /// termination-unproven errors are surfaced *distinctly*:
    ///
    /// - Both clean → `Ok(())`
    /// - Graceful protocol failed, cleanup OK → `Err(protocol)`
    /// - Graceful OK, cleanup failed → `Err(ChildTerminationFailed)`
    /// - Both failed → `Err(OperationWithCleanupFailure)` wrapping
    ///   the original protocol error and the termination signal.
    ///
    /// # Errors
    /// See the mapping above.
    pub async fn shutdown(self) -> Result<()> {
        self.stopping.store(true, Ordering::SeqCst);
        let mut protocol_err: Option<Error> = None;
        if self.alive.load(Ordering::SeqCst) {
            match self.request::<Value>("shutdown", Value::Null).await {
                Ok(_) => {
                    if let Err(e) = self.notify("exit", Value::Null).await {
                        protocol_err = Some(e);
                    }
                }
                Err(e) => protocol_err = Some(e),
            }
        }
        // Give the child a bounded grace period to exit on its own,
        // then delegate to `force_terminate` for any remaining
        // cleanup. If the graceful wait succeeds we drop the child
        // handle immediately so `force_terminate` has nothing to
        // do; if it errored or timed out we let `force_terminate`
        // handle kill + wait uniformly.
        let graceful_reaped = {
            let mut child_slot = self.child.lock().await;
            match child_slot.as_mut() {
                None => true,
                Some(child) => match timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
                    Ok(Ok(_)) => {
                        *child_slot = None;
                        true
                    }
                    Ok(Err(e)) => {
                        if protocol_err.is_none() {
                            protocol_err = Some(Error::Protocol(format!("wait failed: {e}")));
                        }
                        false
                    }
                    Err(_) => false,
                },
            }
        };
        let cleanup = if graceful_reaped {
            // Still clear alive/writer/pending so the state is
            // consistent even without a live child.
            self.alive.store(false, Ordering::SeqCst);
            {
                let mut writer = self.writer.lock().await;
                *writer = None;
            }
            {
                let mut pending = self.pending.lock().await;
                pending.clear();
            }
            Ok(())
        } else {
            self.force_terminate().await
        };
        match (protocol_err, cleanup) {
            (None, Ok(())) => Ok(()),
            (Some(e), Ok(())) => Err(e),
            (None, Err(e)) => Err(e),
            (Some(orig), Err(cleanup)) => Err(Error::OperationWithCleanupFailure {
                original: Box::new(orig),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    /// Liveness gate called before every operation. When the reader
    /// task has marked the transport dead, transparently respawn the
    /// server — at most `max_restarts` times over the client's
    /// lifetime — before giving up with `ServerExited`. The counter
    /// increments per respawn *attempt*, so failing spawns also
    /// consume the budget.
    async fn ensure_running(&self) -> Result<()> {
        if self.stopping.load(Ordering::SeqCst) {
            return Err(Error::PoolStopped);
        }
        if self.alive.load(Ordering::SeqCst) {
            return Ok(());
        }
        let attempt = self.restarts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt > self.max_restarts {
            return Err(Error::ServerExited(None.into()));
        }
        self.spawn_process().await
    }

    /// Send a JSON-RPC request and await its matching response.
    ///
    /// The oneshot receiver is registered in `pending` before the
    /// message is written, so a fast reply cannot race the
    /// registration. Failure shapes:
    /// - write error or timeout: the pending entry is removed here;
    /// - channel closed without a reply (map drained by a
    ///   termination path): reported as `ServerExited`;
    /// - `Err` delivered by the reader (server `error` object, or a
    ///   fan-out replica when the reader loop dies): passed through.
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

    /// Serialize and frame one message onto stdin. A `None` writer
    /// means the transport was torn down; reported as
    /// `ServerExited` rather than a protocol error.
    async fn write_message(&self, message: &Value) -> Result<()> {
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(Error::ServerExited(None.into()));
        };
        write_lsp_message(writer, message).await
    }

    /// Attach captured stderr to the error shapes where it aids
    /// diagnosis (handshake failures and server exits). Other
    /// errors pass through unchanged so stderr noise cannot obscure
    /// e.g. a timeout classification.
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

/// rust-analyzer-specific `initializationOptions`.
/// `cairnConfigHash` is not interpreted by the server; it is
/// carried as an opaque marker of the cairn config the session was
/// started with. `serverStatusNotification` opts in to
/// rust-analyzer's experimental status notification, which is
/// currently only logged — readiness is decided via `$/progress`
/// quiescence instead.
fn rust_analyzer_initialization_options(config_hash: &str) -> Value {
    json!({
        "cairnConfigHash": config_hash,
        "experimental": {
            "serverStatusNotification": true
        },
    })
}

/// Kill + reap a `Child` that was just spawned but never handed
/// off to `self.child`. Used by the missing-stdio paths: the local
/// `child` isn't visible to `force_terminate` yet, so we do the
/// same kill + wait shape inline and surface both the handshake
/// error and any termination-unproven signal.
async fn reap_local_child(child: &mut Child, original: &str) -> Error {
    let _ = child.kill().await;
    match child.wait().await {
        Ok(_) => Error::Handshake(original.into()),
        Err(e) => Error::OperationWithCleanupFailure {
            original: Box::new(Error::Handshake(original.into())),
            cleanup: Box::new(Error::ChildTerminationFailed(format!(
                "wait() after kill: {e}"
            ))),
        },
    }
}

/// Single-source availability probe: spawn `binary args`, wait
/// with the given timeout, and treat exit-status success as
/// availability. Stdin / stdout / stderr are all set to null
/// stdio (the probe only cares about the exit status);
/// `kill_on_drop(true)` is the last-resort backstop; on timeout
/// the probe explicitly runs `kill` + `wait` so the caller sees
/// a proof of termination — a `wait()` failure surfaces as a
/// `ChildTerminationFailed` (composite with `RequestTimeout` on
/// timeout), which the central `LspClientPool::with_lsp` exit
/// point can act on to poison the pool.
///
/// Callers:
/// - [`LspClient::start_with_timeout`] passes `["--version"]`.
/// - `pool::check_lsp_available` uses this via the
///   `AvailabilityStrategy` args dispatch.
///
/// The two production probe paths must not fork — a divergence in
/// termination-proof / signal handling would produce silent orphan
/// probes on one code path but not the other.
pub(super) async fn probe_binary(
    binary_path: &Path,
    args: &[&str],
    timeout_duration: Duration,
) -> Result<()> {
    let mut command = Command::new(binary_path);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::BinaryMissing(binary_path.to_path_buf())
        } else {
            Error::Spawn(e)
        }
    })?;
    match timeout(timeout_duration, child.wait()).await {
        Ok(Ok(status)) => {
            if status.success() {
                Ok(())
            } else {
                Err(Error::BinaryMissing(binary_path.to_path_buf()))
            }
        }
        Ok(Err(wait_err)) => Err(Error::ChildTerminationFailed(format!(
            "probe wait() failed: {wait_err}"
        ))),
        Err(_) => {
            let _ = child.kill().await;
            match child.wait().await {
                Ok(_) => Err(Error::RequestTimeout),
                Err(e) => Err(Error::OperationWithCleanupFailure {
                    original: Box::new(Error::RequestTimeout),
                    cleanup: Box::new(Error::ChildTerminationFailed(format!(
                        "probe wait() after kill: {e}"
                    ))),
                }),
            }
        }
    }
}

async fn check_binary_available(binary_path: &Path, request_timeout: Duration) -> Result<()> {
    probe_binary(binary_path, &["--version"], request_timeout).await
}

/// Bounded capture of child stderr. Everything accumulates in
/// `head` until the total line budget (`STDERR_HEAD_LINES` +
/// `STDERR_TAIL_LINES`) is exceeded; from then on the capture is
/// permanently split into a fixed head plus a rolling tail, each
/// capped at `STDERR_SECTION_BYTES`.
#[derive(Default)]
pub(super) struct StderrTail {
    head: String,
    tail: String,
    truncated: bool,
}

impl StderrTail {
    pub(super) fn clear(&mut self) {
        self.head.clear();
        self.tail.clear();
        self.truncated = false;
    }

    pub(super) fn push(&mut self, chunk: &[u8]) {
        let chunk = String::from_utf8_lossy(chunk);
        if !self.truncated {
            self.head.push_str(&chunk);
            let line_count = self.head.lines().count();
            if line_count <= STDERR_HEAD_LINES + STDERR_TAIL_LINES {
                return;
            }

            // First overflow: snapshot the whole buffer, then carve
            // it into the fixed head and the initial rolling tail.
            self.truncated = true;
            self.tail = self.head.clone();
            trim_to_first_lines(&mut self.head, STDERR_HEAD_LINES);
            trim_to_first_bytes(&mut self.head, STDERR_SECTION_BYTES);
            trim_to_last_bytes(&mut self.tail, STDERR_SECTION_BYTES);
            trim_to_last_lines(&mut self.tail, STDERR_TAIL_LINES);
            return;
        }

        self.tail.push_str(&chunk);
        trim_to_last_bytes(&mut self.tail, STDERR_SECTION_BYTES);
        trim_to_last_lines(&mut self.tail, STDERR_TAIL_LINES);
    }

    pub(super) fn text(&self) -> String {
        if self.truncated {
            format!(
                "{}\n{}\n{}",
                self.head.trim_end(),
                STDERR_OMISSION_MARKER,
                self.tail.trim_start()
            )
            .trim()
            .to_string()
        } else {
            self.head.trim().to_string()
        }
    }
}

/// Background task draining child stderr into the shared tail
/// buffer until EOF or a read error (both mean the pipe is gone).
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

fn trim_to_first_bytes(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

fn trim_to_first_lines(text: &mut String, max_lines: usize) {
    let line_count = text.lines().count();
    if line_count <= max_lines {
        return;
    }
    let mut keep_lines = max_lines;
    let mut end = text.len();
    for (idx, ch) in text.char_indices() {
        if ch == '\n' {
            keep_lines -= 1;
            if keep_lines == 0 {
                end = idx;
                break;
            }
        }
    }
    text.truncate(end);
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

/// Normalize a `textDocument/definition` result. Per the LSP spec
/// the result is `Location | Location[] | LocationLink[] | null`;
/// all shapes collapse to `Vec<Location>` (with `null` as empty).
/// For links, `targetSelectionRange` — the range of the symbol
/// name itself — is preferred over the broader `targetRange` when
/// present.
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
