//! Single LSP child process: spawning, JSON-RPC request/response
//! plumbing, readiness, bounded restarts, and shutdown.
//!
//! One background reader task (`reader::reader_loop`) owns stdout
//! and resolves responses into per-request oneshot channels
//! registered in `pending`; stdin sits behind a writer mutex. Each
//! installed transport has a monotonic generation that scopes reader
//! liveness, pending failure fan-out, progress, and server replies.
//! Forced teardown of an installed child is centralized in
//! `force_terminate`, the fail-closed path when termination cannot
//! be proven. Clean graceful shutdown reaps inline and delegates
//! its forced fallback to `force_terminate`; availability probes
//! and missing-stdio failures own and reap their local `Child`
//! directly.
use std::collections::HashMap;
use std::future::{Future, ready};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::process::{Child, Command};
#[cfg(test)]
use tokio::sync::watch;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{info, warn};

#[cfg(unix)]
use rustix::process::{Pid, Signal, getpgid, kill_process_group};

use super::error::{Error, Result};
use super::reader::{
    PendingRequest, ProgressState, SharedWriter, WorkspaceLoadDeadline, WorkspaceLoadWaitOutcome,
    WriterSlot, reader_loop,
};
use super::transport::write_lsp_message;
use super::types::{Location, LocationLink, Position, Url};

// Default per-request timeout (also bounds the `--version`
// availability probe in `start`).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
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

/// The leader child plus the immutable containment identity validated before
/// the process is published to the client. On Unix every production LSP spawn
/// must own a process group whose id is exactly the leader pid. Other
/// platforms retain the existing leader-only contract.
struct OwnedLspChild {
    child: Child,
    #[cfg(unix)]
    pgid: Pid,
    #[cfg(test)]
    leader_waits: Arc<AtomicUsize>,
}

impl OwnedLspChild {
    async fn wait_leader(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let result = self.child.wait().await;
        #[cfg(test)]
        self.leader_waits.fetch_add(1, Ordering::SeqCst);
        result
    }
}

/// Handle to one LSP server subprocess speaking JSON-RPC over
/// stdio.
///
/// Shared state lives behind `Arc`s so the background reader task
/// and `LspProcessControl` clones stay valid independently of this
/// handle. `shutdown(self)` is the graceful exit. Dropping the
/// client alone does not kill the child: the owned child/group sits behind a
/// shared `Arc<Mutex<..>>`, so leader `kill_on_drop(true)` fires only
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
    // Generation of the currently installed transport. Zero means
    // no live transport. Delayed readers may clear only the
    // generation they own.
    current_generation: Arc<AtomicU64>,
    // Monotonic source for non-zero transport generations.
    next_transport_generation: AtomicU64,
    // One-way latch: once set (shutdown or pool stop), respawns are
    // refused with `PoolStopped`.
    stopping: Arc<AtomicBool>,
    // Wakes Rust's bounded readiness wait when the control plane stops the
    // client. The atomic remains the source of truth; this notification only
    // provides prompt cancellation without moving either deadline.
    stopping_notify: Arc<Notify>,
    writer: WriterSlot,
    child: Arc<Mutex<Option<OwnedLspChild>>>,
    // In-flight requests by id and owning transport generation. A
    // reader failure drains only its own generation; explicit
    // process termination drains every entry.
    pub(super) pending: Arc<Mutex<HashMap<u64, PendingRequest>>>,
    progress: Arc<ProgressState>,
    stderr_tail: Arc<Mutex<StderrTail>>,
    #[cfg(test)]
    cleanup_pause: Arc<StdMutex<Option<TestCleanupPause>>>,
    #[cfg(test)]
    cleanup_runs: Arc<AtomicUsize>,
    #[cfg(test)]
    leader_waits: Arc<AtomicUsize>,
}

/// Cloneable control-plane handle for one LSP child process.
///
/// This deliberately contains no document or request-operation state. A pool
/// shutdown must be able to stop and reap the child while a normal analyzer
/// pass holds the entry's data-plane mutex for the duration of its work.
#[derive(Clone)]
pub(crate) struct LspProcessControl {
    current_generation: Arc<AtomicU64>,
    stopping: Arc<AtomicBool>,
    stopping_notify: Arc<Notify>,
    writer: WriterSlot,
    child: Arc<Mutex<Option<OwnedLspChild>>>,
    pending: Arc<Mutex<HashMap<u64, PendingRequest>>>,
    #[cfg(test)]
    cleanup_pause: Arc<StdMutex<Option<TestCleanupPause>>>,
    #[cfg(test)]
    cleanup_runs: Arc<AtomicUsize>,
    #[cfg(test)]
    leader_waits: Arc<AtomicUsize>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestCleanupPause {
    entered: watch::Sender<bool>,
    release: watch::Receiver<bool>,
}

impl LspProcessControl {
    #[cfg(test)]
    pub(super) fn leader_wait_count_for_test(&self) -> usize {
        self.leader_waits.load(Ordering::SeqCst)
    }

    /// Permanently prevent this client from spawning a replacement child, then
    /// terminate and reap the current child without taking the pool entry's
    /// data-plane mutex.
    pub(crate) async fn stop_and_terminate(&self) -> Result<()> {
        self.stopping.store(true, Ordering::SeqCst);
        self.stopping_notify.notify_waiters();
        #[cfg(test)]
        {
            self.cleanup_runs.fetch_add(1, Ordering::SeqCst);
            let pause = self
                .cleanup_pause
                .lock()
                .expect("test cleanup-pause mutex poisoned")
                .clone();
            if let Some(mut pause) = pause {
                pause.entered.send_replace(true);
                while !*pause.release.borrow() {
                    if pause.release.changed().await.is_err() {
                        break;
                    }
                }
            }
        }
        self.force_terminate().await
    }

    async fn force_terminate(&self) -> Result<()> {
        // Kill first. A wedged server can backpressure a pipe write while the
        // writer mutex is held; waiting for that mutex before kill would make
        // the process-control plane depend on the data plane it must unblock.
        let mut child_slot = self.child.lock().await;
        self.force_terminate_locked(&mut child_slot, None).await?;
        Ok(())
    }

    async fn force_terminate_generation(&self, generation: u64) -> Result<bool> {
        let mut child_slot = self.child.lock().await;
        self.force_terminate_locked(&mut child_slot, Some(generation))
            .await
    }

    async fn force_terminate_locked(
        &self,
        child_slot: &mut Option<OwnedLspChild>,
        expected_generation: Option<u64>,
    ) -> Result<bool> {
        match expected_generation {
            Some(expected) => {
                if self
                    .current_generation
                    .compare_exchange(expected, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    return Ok(false);
                }
            }
            None => self.current_generation.store(0, Ordering::SeqCst),
        }
        let termination_err = if let Some(child) = child_slot.as_mut() {
            terminate_owned_child(child).await.err()
        } else {
            None
        };
        *child_slot = None;
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
            None => Ok(true),
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
        client.start_standalone().await
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
        client.start_standalone().await
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

    /// Legacy non-pool constructors have no entry guard, so they retain a
    /// local cleanup wrapper. The production pool calls `start_process` only
    /// after `install_and_arm` has made cleanup non-cancellable.
    async fn start_standalone(self) -> Result<Self> {
        if let Err(original) = self.start_process().await {
            return Err(match self.force_terminate().await {
                Ok(()) => original,
                Err(cleanup) => Error::OperationWithCleanupFailure {
                    original: Box::new(original),
                    cleanup: Box::new(cleanup),
                },
            });
        }
        Ok(self)
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
            current_generation: Arc::new(AtomicU64::new(0)),
            next_transport_generation: AtomicU64::new(1),
            stopping: Arc::new(AtomicBool::new(false)),
            stopping_notify: Arc::new(Notify::new()),
            writer: Arc::new(Mutex::new(None)),
            child: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            progress: Arc::new(ProgressState::default()),
            stderr_tail: Arc::new(Mutex::new(StderrTail::default())),
            #[cfg(test)]
            cleanup_pause: Arc::new(StdMutex::new(None)),
            #[cfg(test)]
            cleanup_runs: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            leader_waits: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    pub(super) fn pause_cleanup_for_test(&self) -> (watch::Receiver<bool>, watch::Sender<bool>) {
        let (entered_sender, entered_receiver) = watch::channel(false);
        let (release_sender, release_receiver) = watch::channel(false);
        *self
            .cleanup_pause
            .lock()
            .expect("test cleanup-pause mutex poisoned") = Some(TestCleanupPause {
            entered: entered_sender,
            release: release_receiver,
        });
        (entered_receiver, release_sender)
    }

    #[cfg(test)]
    pub(super) fn cleanup_run_count_for_test(&self) -> usize {
        self.cleanup_runs.load(Ordering::SeqCst)
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

        // `kill_on_drop(true)` is only a leader-level last resort. The pool's
        // armed cleanup task is authoritative because it signals the verified
        // group and reaps the leader even after its first observer times out.
        let mut command = Command::new(binary_path);
        command
            .args(&self.args)
            .envs(self.env.iter().map(|(key, value)| (key, value)))
            .current_dir(&self.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let child = command.spawn().map_err(Error::Spawn)?;
        let mut child = own_spawned_child(
            child,
            #[cfg(test)]
            Arc::clone(&self.leader_waits),
        )
        .await?;
        let stdin = match child.child.stdin.take() {
            Some(stdin) => stdin,
            None => return Err(reap_local_child(&mut child, "missing child stdin").await),
        };
        let stdout = match child.child.stdout.take() {
            Some(stdout) => stdout,
            None => return Err(reap_local_child(&mut child, "missing child stdout").await),
        };
        self.stderr_tail.lock().await.clear();
        if let Some(stderr) = child.child.stderr.take() {
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
        // Initial pool installation is protected by its uncommitted-exit
        // guard. A committed client's later respawn failure is cleaned up at
        // the `ensure_running` boundary. The stopping race stays raw because
        // the installed final-control cleanup owns it.
        if self.stopping.load(Ordering::SeqCst) {
            return Err(Error::PoolStopped);
        }
        if let Err(err) = self.initialize().await {
            return Err(self.with_stderr_context(err).await);
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
            current_generation: Arc::clone(&self.current_generation),
            stopping: Arc::clone(&self.stopping),
            stopping_notify: Arc::clone(&self.stopping_notify),
            writer: Arc::clone(&self.writer),
            child: Arc::clone(&self.child),
            pending: Arc::clone(&self.pending),
            #[cfg(test)]
            cleanup_pause: Arc::clone(&self.cleanup_pause),
            #[cfg(test)]
            cleanup_runs: Arc::clone(&self.cleanup_runs),
            #[cfg(test)]
            leader_waits: Arc::clone(&self.leader_waits),
        }
    }

    #[cfg(test)]
    pub(super) fn transport_generation(&self) -> u64 {
        self.current_generation.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) async fn force_terminate_generation_for_test(
        &self,
        generation: u64,
    ) -> Result<bool> {
        self.process_control()
            .force_terminate_generation(generation)
            .await
    }

    pub(super) async fn install_transport<R, W>(&self, reader: R, writer: W)
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let generation = self
            .next_transport_generation
            .fetch_add(1, Ordering::SeqCst);
        debug_assert_ne!(generation, 0, "transport generation wrapped");
        let writer: SharedWriter = Arc::new(Mutex::new(Box::new(writer)));
        self.progress.reset_for_generation(generation).await;
        *self.writer.lock().await = Some((generation, Arc::clone(&writer)));
        self.current_generation.store(generation, Ordering::SeqCst);
        let pending = Arc::clone(&self.pending);
        let current_generation = Arc::clone(&self.current_generation);
        let progress = Arc::clone(&self.progress);
        tokio::spawn(async move {
            reader_loop(
                reader,
                generation,
                pending,
                current_generation,
                writer,
                progress,
            )
            .await;
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

    /// Wait for semantic progress quiescence under immutable hard and
    /// semantic-stall deadlines.
    ///
    /// This policy is currently selected only by Rust Tier3. Existing LSP
    /// integrations retain the raw progress strategy above.
    pub async fn wait_for_workspace_load_bounded(
        &self,
        hard_timeout: Duration,
        stall_timeout: Duration,
    ) -> Result<()> {
        self.ensure_running().await?;
        let generation = self.current_generation.load(Ordering::SeqCst);
        let readiness_started_at = Instant::now();
        let hard_deadline = readiness_started_at + hard_timeout;
        let stopped = self.stopping_notify.notified();
        tokio::pin!(stopped);
        stopped.as_mut().enable();
        // Close the check-to-subscribe race: a stop that landed after
        // `ensure_running` but before the notification was armed has already
        // set the durable atomic latch even though its wake was not retained.
        if self.stopping.load(Ordering::SeqCst) {
            return Err(Error::PoolStopped);
        }
        let outcome = tokio::select! {
            biased;
            () = &mut stopped => return Err(Error::PoolStopped),
            outcome = self.progress.wait_for_semantic_quiescence(
                readiness_started_at,
                hard_deadline,
                stall_timeout,
                WORKSPACE_LOAD_QUIET_PERIOD,
            ) => outcome,
        };

        // A concurrent stop or transport exit outranks readiness success and
        // deadline classification. Never publish a dead generation as ready.
        if self.stopping.load(Ordering::SeqCst) {
            return Err(Error::PoolStopped);
        }
        if self.current_generation.load(Ordering::SeqCst) != generation {
            return Err(Error::ServerExited(None.into()));
        }

        match outcome {
            WorkspaceLoadWaitOutcome::Complete(completed_via) => {
                info!(?completed_via, "workspace load complete");
                Ok(())
            }
            WorkspaceLoadWaitOutcome::Deadline(deadline) => {
                let deadline = match deadline {
                    WorkspaceLoadDeadline::Hard => "hard",
                    WorkspaceLoadDeadline::Stall => "stall",
                };
                warn!(
                    deadline,
                    hard_ms = hard_timeout.as_millis(),
                    stall_ms = stall_timeout.as_millis(),
                    "workspace load readiness deadline exceeded"
                );
                Err(Error::ReadinessTimeout)
            }
        }
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

    /// Gracefully ask the server to stop, then terminate the verified process
    /// group and reap its leader exactly once. Signalling precedes `wait()` so
    /// a reaped leader's numeric PGID cannot be reused before containment.
    /// Graceful protocol errors and
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
        self.stopping_notify.notify_waiters();
        let mut protocol_err: Option<Error> = None;
        if self.current_generation.load(Ordering::SeqCst) != 0 {
            match self.request::<Value>("shutdown", Value::Null).await {
                Ok(_) => {
                    if let Err(e) = self.notify("exit", Value::Null).await {
                        protocol_err = Some(e);
                    }
                }
                Err(e) => protocol_err = Some(e),
            }
        }
        let cleanup = self.force_terminate().await;
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
        if self.current_generation.load(Ordering::SeqCst) != 0 {
            return Ok(());
        }
        let attempt = self.restarts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt > self.max_restarts {
            return Err(Error::ServerExited(None.into()));
        }
        match self.spawn_process().await {
            Ok(()) => Ok(()),
            Err(Error::PoolStopped) => Err(Error::PoolStopped),
            Err(original) => match self.force_terminate().await {
                Ok(()) => Err(original),
                Err(cleanup) => Err(Error::OperationWithCleanupFailure {
                    original: Box::new(original),
                    cleanup: Box::new(cleanup),
                }),
            },
        }
    }

    /// Send a JSON-RPC request and await its matching response.
    ///
    /// The oneshot receiver is registered in `pending` before the
    /// message is written, so a fast reply cannot race the
    /// registration. One deadline covers both the stdin write and
    /// response wait. Failure shapes:
    /// - write error or timeout: the pending entry is removed here;
    ///   a write timeout also force-terminates the transport because
    ///   the partially-written JSON-RPC frame cannot be reused;
    /// - channel closed without a reply (map drained by a
    ///   termination path): reported as `ServerExited`;
    /// - `Err` delivered by the reader (server `error` object, or a
    ///   fan-out replica when the reader loop dies): passed through.
    async fn request<T>(&self, method: &str, params: Value) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.request_inner(method, params, || ready(())).await
    }

    #[cfg(test)]
    pub(super) async fn request_with_snapshot_hook<T, F, Fut>(
        &self,
        method: &str,
        params: Value,
        after_snapshot: F,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        self.request_inner(method, params, after_snapshot).await
    }

    async fn request_inner<T, F, Fut>(
        &self,
        method: &str,
        params: Value,
        after_snapshot: F,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let (generation, writer) = self.transport_snapshot().await?;
        after_snapshot().await;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(
            id,
            PendingRequest {
                generation,
                sender: tx,
            },
        );
        if !self.transport_is_current(generation).await {
            self.pending.lock().await.remove(&id);
            return Err(Error::ServerExited(None.into()));
        }
        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let deadline = Instant::now() + self.timeout;

        if let Err(err) = self
            .write_message_on_until(deadline, generation, &writer, &message)
            .await
        {
            self.pending.lock().await.remove(&id);
            return Err(self.with_stderr_context(err).await);
        }

        // Ensure the pending slot is reclaimed on every exit path —
        // including a Timeout — so a never-replying server cannot leak
        // entries unboundedly across repeated `request` calls.
        let response = match timeout_at(deadline, rx).await {
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
        let message = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.write_message_until(Instant::now() + self.timeout, &message)
            .await
    }

    /// Bound one stdin write by the caller's operation deadline.
    /// Cancellation may leave a partial JSON-RPC frame in the pipe,
    /// so a timeout force-terminates the transport before returning.
    async fn write_message_until(&self, deadline: Instant, message: &Value) -> Result<()> {
        let (generation, writer) = self.transport_snapshot().await?;
        self.write_message_on_until(deadline, generation, &writer, message)
            .await
    }

    async fn write_message_on_until(
        &self,
        deadline: Instant,
        generation: u64,
        writer: &SharedWriter,
        message: &Value,
    ) -> Result<()> {
        match timeout_at(deadline, Self::write_message_on(writer, message)).await {
            Ok(result) => result,
            Err(_) => match self
                .process_control()
                .force_terminate_generation(generation)
                .await
            {
                Ok(true) => Err(Error::RequestTimeout),
                Ok(false) => Err(Error::ServerExited(None.into())),
                Err(cleanup) => Err(Error::OperationWithCleanupFailure {
                    original: Box::new(Error::RequestTimeout),
                    cleanup: Box::new(cleanup),
                }),
            },
        }
    }

    /// Snapshot the installed generation and writer under one mutex.
    async fn transport_snapshot(&self) -> Result<(u64, SharedWriter)> {
        self.writer
            .lock()
            .await
            .clone()
            .ok_or_else(|| Error::ServerExited(None.into()))
    }

    async fn transport_is_current(&self, generation: u64) -> bool {
        if self.current_generation.load(Ordering::SeqCst) != generation {
            return false;
        }
        self.writer
            .lock()
            .await
            .as_ref()
            .is_some_and(|(installed, _)| *installed == generation)
    }

    async fn write_message_on(writer: &SharedWriter, message: &Value) -> Result<()> {
        let mut writer = writer.lock().await;
        write_lsp_message(&mut *writer, message).await
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

#[cfg(unix)]
async fn own_spawned_child(
    mut child: Child,
    #[cfg(test)] leader_waits: Arc<AtomicUsize>,
) -> Result<OwnedLspChild> {
    let Some(raw_pid) = child.id() else {
        return Err(
            reject_unverified_child(&mut child, "spawned LSP leader has no process id").await,
        );
    };
    let Some(pid) = Pid::from_raw(raw_pid as i32) else {
        return Err(reject_unverified_child(
            &mut child,
            "spawned LSP leader has an invalid process id",
        )
        .await);
    };
    match validate_process_group_identity(pid, getpgid(Some(pid))) {
        Ok(pgid) => Ok(OwnedLspChild {
            child,
            pgid,
            #[cfg(test)]
            leader_waits,
        }),
        Err(reason) => Err(reject_unverified_child(&mut child, &reason).await),
    }
}

#[cfg(unix)]
pub(super) fn validate_process_group_identity(
    leader: Pid,
    observed: rustix::io::Result<Pid>,
) -> std::result::Result<Pid, String> {
    match observed {
        Ok(pgid) if pgid == leader => Ok(pgid),
        Ok(pgid) => Err(format!(
            "spawned LSP process-group mismatch: leader={}, observed={}",
            leader.as_raw_nonzero(),
            pgid.as_raw_nonzero()
        )),
        Err(error) => Err(format!(
            "could not validate spawned LSP process group for leader {}: {error}",
            leader.as_raw_nonzero()
        )),
    }
}

#[cfg(not(unix))]
async fn own_spawned_child(
    child: Child,
    #[cfg(test)] leader_waits: Arc<AtomicUsize>,
) -> Result<OwnedLspChild> {
    Ok(OwnedLspChild {
        child,
        #[cfg(test)]
        leader_waits,
    })
}

/// A process-group identity failure happens before publication, so only the
/// local leader handle is safe to operate on. Reap the leader but retain the
/// containment failure as termination-unproven: a mismatched group must never
/// be signalled by inference or followed by a replacement spawn.
#[cfg(unix)]
pub(super) async fn reject_unverified_child(child: &mut Child, reason: &str) -> Error {
    let kill_error = child.kill().await.err();
    let wait_error = child.wait().await.err();
    let mut facts = vec![reason.to_string()];
    if let Some(error) = kill_error {
        facts.push(format!("leader kill after validation failure: {error}"));
    }
    if let Some(error) = wait_error {
        facts.push(format!("leader wait after validation failure: {error}"));
    }
    Error::OperationWithCleanupFailure {
        original: Box::new(Error::Handshake(reason.into())),
        cleanup: Box::new(Error::ChildTerminationFailed(facts.join("; "))),
    }
}

/// Signal the immutable, spawn-validated Unix process group before reaping the
/// leader exactly once. Group-signal failures are kept even when leader wait
/// succeeds because descendant containment is then unproven. ESRCH means the
/// group is already absent, but never substitutes for leader `wait()`.
async fn terminate_owned_child(child: &mut OwnedLspChild) -> Result<()> {
    #[cfg(unix)]
    let group_error = classify_group_signal(
        child.pgid,
        kill_process_group(child.pgid, Signal::KILL),
        "SIGKILL process group",
    );

    #[cfg(not(unix))]
    let group_error = {
        // Non-Unix retains the existing leader-only behavior. This is not a
        // tree-containment claim; Windows Job Object support is separate work.
        let _ = child.child.kill().await;
        None::<String>
    };

    let wait_error = child.wait_leader().await.err();
    match (group_error, wait_error) {
        (None, None) => Ok(()),
        (group, wait) => {
            let mut facts = Vec::new();
            if let Some(error) = group {
                facts.push(error);
            }
            if let Some(error) = wait {
                facts.push(format!("leader wait after kill: {error}"));
            }
            Err(Error::ChildTerminationFailed(facts.join("; ")))
        }
    }
}

#[cfg(unix)]
pub(super) fn classify_group_signal(
    pgid: Pid,
    result: rustix::io::Result<()>,
    stage: &str,
) -> Option<String> {
    match result {
        Ok(()) | Err(rustix::io::Errno::SRCH) => None,
        Err(error) => Some(format!("{stage} {}: {error}", pgid.as_raw_nonzero())),
    }
}

/// Kill + reap a child that was validated but never handed off to
/// `self.child`. Missing-stdio and stop-before-publication paths use the same
/// containment operation as installed children.
async fn reap_local_child(child: &mut OwnedLspChild, original: &str) -> Error {
    match terminate_owned_child(child).await {
        Ok(()) => Error::Handshake(original.into()),
        Err(cleanup) => Error::OperationWithCleanupFailure {
            original: Box::new(Error::Handshake(original.into())),
            cleanup: Box::new(cleanup),
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
    // Spawn inventory: availability probes are intentionally leader-only.
    // They execute a bounded `--version`-style command, never become a Ready
    // LSP transport, and are reaped locally. Only the long-lived server spawn
    // in `spawn_process` receives owned process-group containment.
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
