//! Daemon main loop.
//!
//! Owns the two UDS listeners (`cairn.sock` and `control.sock`) and a
//! pluggable [`LineHandler`] pair — one for the read-only data RPC,
//! one for the control protocol. Concrete handlers live in
//! [`crate::data_rpc`] and [`crate::ctl`]; this module owns the
//! framing, the accept loops, and the shared shutdown signal.
//!
//! `cairn.sock` speaks plain JSON-RPC 2.0, not MCP. MCP framing is
//! the job of `cairn mcp` (the stdio front-end in the `cairn` crate),
//! which translates each MCP `tools/call` into either a data RPC
//! (`get_outline` / `find_symbols` / `list_repos`) or a control message
//! (`register_repo` / `reindex_repo`) and wraps the response back into
//! the MCP shape. Out-of-tree consumers (cairn-graph, cairn-audit,
//! IDE plugins) hit the daemon directly over the JSON-RPC surface
//! without speaking MCP at all.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::jobs::JobManager;
use crate::sockets::{SocketPaths, bind_socket_with_mode};
use crate::startup::{ReadyDaemon, StartupControlHandler, StartupDataHandler, StartupGate};

/// Implementations receive one newline-delimited request line at a
/// time and return one response line.
#[async_trait::async_trait]
pub trait LineHandler: Send + Sync {
    /// Process one request line. Returning `None` ends the connection
    /// (the server closes the stream cleanly).
    async fn handle(&self, line: &str) -> Option<String>;
}

/// Hand-off bundle used to start the daemon. The two handlers are
/// usually different concrete types (data RPC and control protocol),
/// but they share a uniform line-in / line-out shape over UDS.
pub struct Daemon {
    pub paths: SocketPaths,
    pub data_handler: Arc<dyn LineHandler>,
    pub control_handler: Arc<dyn LineHandler>,
    /// Shared shutdown signal. The daemon parks on `notified()`;
    /// signal it via `notify_waiters` (the control handler's
    /// `shutdown` RPC does). `notify_waiters` wakes only waiters
    /// already registered — it stores no permit — so the run loop
    /// arms its `notified()` future before serving; the transition
    /// itself is one-way.
    pub shutdown: Arc<Notify>,
    /// Shutdown is ordered by ownership boundaries: stop accepting and drain
    /// admitted RPCs, close job admission and cancel active analyzers, reap LSP
    /// children so pending requests unwind, then drain job workers.
    pub job_manager: Option<Arc<JobManager>>,
    /// Reconcile driver — required in production so the startup
    /// revision-staleness scan can route parser-revision drift
    /// through the durable state machine rather than the
    /// synchronous full-reindex helper. Tests that don't exercise
    /// the drift path may pass `None`.
    pub reconcile: Option<Arc<crate::reconcile::RepoReconcileManager>>,
    /// Canonical repository lifecycle owner. When present, teardown stops its
    /// intent task before dropping job/watcher/reconcile runtime bindings.
    pub lifecycle: Option<Arc<crate::lifecycle::RepoLifecycleManager>>,
}

/// Bound on waiting for in-flight connection tasks after an accept
/// loop stops accepting. Applied per socket; both loops drain
/// concurrently inside the same teardown future.
const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Per-entry bound handed to the global LSP pool shutdown: each
/// pooled language server gets this long to shut down cleanly.
const LSP_ENTRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on draining the job scheduler and worker tasks after job
/// admission is closed and active analyzer runs are cancelled.
const JOB_MANAGER_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Overall teardown budget, measured only from the shutdown
/// notification (idle daemon lifetime never counts against it).
/// Equals the sum of the component upper bounds — 2s connection
/// drain + 1s lifecycle join in `run_bound` + 5s LSP pool + 2s job
/// drain — with no slack; exceeding it yields
/// `Error::ShutdownDeadlineExceeded`.
const DAEMON_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

impl Daemon {
    /// Bind both sockets, run accept loops until `shutdown` is
    /// notified, then drop the listeners (and unlink the socket
    /// files as a side effect on Unix).
    ///
    /// # Errors
    /// Bind / accept failures propagate.
    pub async fn run(self) -> Result<()> {
        self.run_with_shutdown_timeout(DAEMON_SHUTDOWN_TIMEOUT)
            .await
    }

    async fn run_with_shutdown_timeout(self, shutdown_timeout: Duration) -> Result<()> {
        self.paths.ensure()?;
        let cairn = bind_socket_with_mode(&self.paths.cairn)?;
        let ctrl = bind_socket_with_mode(&self.paths.control)?;
        info!(cairn = %self.paths.cairn.display(), control = %self.paths.control.display(), "daemon listening");
        if let Some(job_manager) = self.job_manager.clone() {
            spawn_revision_staleness_scan(job_manager, self.reconcile.clone());
        }
        run_bound(
            self.paths,
            cairn,
            ctrl,
            self.data_handler,
            self.control_handler,
            self.shutdown,
            RuntimeOwnership::Static {
                job_manager: self.job_manager,
                lifecycle: self.lifecycle,
            },
            shutdown_timeout,
        )
        .await
    }
}

/// Daemon sockets bound before runtime initialization completes.
pub struct InitializingDaemon {
    paths: SocketPaths,
    cairn: UnixListener,
    control: UnixListener,
    gate: Arc<StartupGate>,
    shutdown: Arc<Notify>,
}

impl InitializingDaemon {
    /// Bind both sockets synchronously so callers can begin initialization only
    /// after transport availability is guaranteed.
    pub fn bind(paths: SocketPaths, gate: Arc<StartupGate>, shutdown: Arc<Notify>) -> Result<Self> {
        paths.ensure()?;
        let cairn = bind_socket_with_mode(&paths.cairn)?;
        let control = bind_socket_with_mode(&paths.control)?;
        info!(cairn = %paths.cairn.display(), control = %paths.control.display(), "daemon listening; initialization in progress");
        Ok(Self {
            paths,
            cairn,
            control,
            gate,
            shutdown,
        })
    }

    /// Serve the startup-gated handlers until shutdown. Teardown
    /// drives whatever the gate owns at that instant — see
    /// `RuntimeOwnership::begin_shutdown` for the publication race.
    pub async fn run(self) -> Result<()> {
        self.run_with_shutdown_timeout(DAEMON_SHUTDOWN_TIMEOUT)
            .await
    }

    async fn run_with_shutdown_timeout(self, shutdown_timeout: Duration) -> Result<()> {
        run_bound(
            self.paths,
            self.cairn,
            self.control,
            Arc::new(StartupDataHandler::new(self.gate.clone())),
            Arc::new(StartupControlHandler::new(self.gate.clone())),
            self.shutdown,
            RuntimeOwnership::Startup(self.gate),
            shutdown_timeout,
        )
        .await
    }
}

/// Who owns the runtime resources when teardown begins.
///
/// `Static` is the plain [`Daemon::run`] path: the caller handed the
/// resources over up front. `Startup` is the gated path: the bundle
/// lives behind the [`StartupGate`] until ready publication, so
/// teardown must race publication to claim it.
enum RuntimeOwnership {
    Static {
        job_manager: Option<Arc<JobManager>>,
        lifecycle: Option<Arc<crate::lifecycle::RepoLifecycleManager>>,
    },
    Startup(Arc<StartupGate>),
}

/// Resources teardown drives after the Running -> ShuttingDown
/// transition. `None` fields simply skip that teardown stage.
struct TeardownResources {
    job_manager: Option<Arc<JobManager>>,
    lifecycle: Option<Arc<crate::lifecycle::RepoLifecycleManager>>,
    /// Keeps the full [`ReadyDaemon`] bundle (watcher, reconcile,
    /// handlers) alive until the explicit `drop(resources)` at the
    /// end of teardown, so nothing is torn down mid-drain.
    _ready: Option<ReadyDaemon>,
}

impl RuntimeOwnership {
    /// Consume the ownership token and surface the resources this
    /// teardown must drive. Anything not surfaced stays with its
    /// current owner (the initializer, when publication lost).
    fn begin_shutdown(self) -> TeardownResources {
        match self {
            Self::Static {
                job_manager,
                lifecycle,
            } => TeardownResources {
                job_manager,
                lifecycle,
                _ready: None,
            },
            Self::Startup(gate) => {
                // This transition is the single linearization point between
                // ready publication and shutdown. If publication won, teardown
                // takes the bundle. If shutdown won, the initializer retains
                // ownership and performs partial cleanup.
                let ready = gate.begin_shutdown();
                TeardownResources {
                    job_manager: ready.as_ref().map(|ready| ready.job_manager.clone()),
                    lifecycle: ready.as_ref().map(|ready| ready.lifecycle.clone()),
                    _ready: ready,
                }
            }
        }
    }
}

/// Shared serving + teardown core for both daemon flavors: run the
/// two accept loops until `shutdown` fires, then execute the ordered
/// teardown under `shutdown_timeout`. Socket files are unlinked
/// best-effort on every exit path so the next daemon can bind.
#[allow(clippy::too_many_arguments)]
async fn run_bound(
    paths: SocketPaths,
    cairn: UnixListener,
    control: UnixListener,
    data_handler: Arc<dyn LineHandler>,
    control_handler: Arc<dyn LineHandler>,
    shutdown: Arc<Notify>,
    ownership: RuntimeOwnership,
    shutdown_timeout: Duration,
) -> Result<()> {
    let mut cairn_task = spawn_accept_loop("cairn", cairn, data_handler, shutdown.clone());
    let mut ctrl_task = spawn_accept_loop("control", control, control_handler, shutdown.clone());

    // The daemon lifetime is unbounded. Only teardown work after the
    // Running -> ShuttingDown transition consumes the shutdown budget.
    shutdown.notified().await;

    let teardown = async {
        // Stop accepting first and let already-admitted RPCs finish.
        let _ = tokio::join!(&mut cairn_task, &mut ctrl_task);
        let resources = ownership.begin_shutdown();
        // Close job admission and cancel active analyzer runs first
        // so no new work lands while later stages drain.
        if let Some(job_manager) = &resources.job_manager {
            job_manager.begin_shutdown();
        }
        // Bounded join only: a timeout here detaches the owner task
        // instead of failing the teardown. Removals whose pre-delete
        // intent already committed are resumed by the next startup
        // sweep; a queued missing-root intent that never reached its
        // durable commit is only re-detected by that same sweep.
        if let Some(lifecycle) = &resources.lifecycle
            && let Err(err) = lifecycle.shutdown(Duration::from_secs(1)).await
        {
            warn!(
                error = %err,
                "repository lifecycle shutdown did not drain; durable state will recover on next startup"
            );
        }
        test_observe_lsp_pool_shutdown();
        crate::lsp::pool::shutdown_global_bounded_if_initialized(LSP_ENTRY_SHUTDOWN_TIMEOUT)
            .await?;
        if let Some(job_manager) = &resources.job_manager {
            job_manager.shutdown(JOB_MANAGER_DRAIN_TIMEOUT).await;
        }
        drop(resources);
        Ok(())
    };
    let result = match tokio::time::timeout(shutdown_timeout, teardown).await {
        Ok(result) => result,
        Err(_) => {
            // Budget blown: abort the accept/drain tasks outright and
            // surface a typed error so callers can tell a deadline
            // miss from an I/O failure.
            cairn_task.abort();
            ctrl_task.abort();
            Err(crate::Error::ShutdownDeadlineExceeded {
                timeout_ms: u64::try_from(shutdown_timeout.as_millis()).unwrap_or(u64::MAX),
            })
        }
    };

    // Dropping a UnixListener does not unlink its path; remove the
    // socket files explicitly (best-effort) even on failed teardown.
    let _ = std::fs::remove_file(&paths.cairn);
    let _ = std::fs::remove_file(&paths.control);
    if result.is_ok() {
        info!("daemon stopped");
    }
    result
}

/// Start the revision drift scan after ready publication.
///
/// Fire-and-forget: the scan runs on the blocking thread pool (it
/// reads SQLite synchronously) while a detached observer task logs
/// the outcome. Failures and panics are logged and swallowed — the
/// daemon keeps serving, at the cost of no automatic drift recovery
/// until the next boot. Nothing joins these tasks at shutdown.
pub fn spawn_revision_staleness_scan(
    job_manager: Arc<JobManager>,
    reconcile: Option<Arc<crate::reconcile::RepoReconcileManager>>,
) {
    let cas_data_dir = job_manager.cas_data_dir().clone();
    let scan_handle = tokio::task::spawn_blocking(move || {
        crate::workspace_analyzer::check_revision_staleness_and_enqueue(
            &cas_data_dir,
            &job_manager,
            reconcile.as_ref(),
        )
    });
    tokio::spawn(async move {
        match scan_handle.await {
            Ok(Ok(_summary)) => {}
            Ok(Err(err)) => {
                warn!(error = %err, "revision staleness scan failed; daemon continues");
            }
            Err(join_err) => {
                tracing::error!(
                    error = %join_err,
                    "revision staleness scan panicked; daemon continues (no auto-rerun this boot)"
                );
            }
        }
    });
}

/// Clean up a fully constructed bundle that lost the ready-publication race.
///
/// Mirrors the teardown ordering in `run_bound` (job admission close
/// -> lifecycle join -> LSP pool -> job drain); the caller is the
/// initializer, which retained ownership because shutdown won.
pub async fn shutdown_unpublished_resources(resources: ReadyDaemon) -> Result<()> {
    resources.job_manager.begin_shutdown();
    if let Err(err) = resources.lifecycle.shutdown(Duration::from_secs(1)).await {
        warn!(
            error = %err,
            "unpublished repository lifecycle did not drain; durable state will recover on next startup"
        );
    }
    crate::lsp::pool::shutdown_global_bounded_if_initialized(LSP_ENTRY_SHUTDOWN_TIMEOUT).await?;
    resources
        .job_manager
        .shutdown(JOB_MANAGER_DRAIN_TIMEOUT)
        .await;
    drop(resources);
    Ok(())
}

/// Accept connections on `listener` until `shutdown` fires, serving
/// each connection on its own task.
///
/// The returned handle completes only after the bounded connection
/// drain, so awaiting it means no connection task is still running.
fn spawn_accept_loop(
    name: &'static str,
    listener: UnixListener,
    handler: Arc<dyn LineHandler>,
    shutdown: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Per-loop JoinSet: dropping or aborting this task cancels
        // its connection tasks instead of leaking them, and shutdown
        // can drain them with a bound.
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                () = shutdown.notified() => {
                    debug!(socket = name, "accept loop received shutdown");
                    break;
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, _addr)) => {
                        let h = handler.clone();
                        connections.spawn(async move {
                            if let Err(e) = serve_one(stream, h).await {
                                warn!(error = %e, "{name} connection ended with error", name = name);
                            }
                        });
                    }
                    Err(e) => {
                        error!(?e, socket = name, "accept failed");
                        // Brief backoff to avoid spinning on a persistent error.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        }
        drain_connections(name, connections).await;
    })
}

/// Wait up to [`CONNECTION_DRAIN_TIMEOUT`] for in-flight connection
/// tasks to finish, then abort the stragglers and await the aborts
/// so no connection task outlives its accept loop.
async fn drain_connections(name: &'static str, mut connections: JoinSet<()>) {
    let drained = tokio::time::timeout(CONNECTION_DRAIN_TIMEOUT, async {
        while let Some(result) = connections.join_next().await {
            if let Err(err) = result {
                warn!(error = %err, socket = name, "connection task failed during shutdown");
            }
        }
    })
    .await;
    if drained.is_err() {
        let remaining = connections.len();
        connections.abort_all();
        warn!(
            socket = name,
            remaining,
            timeout_secs = CONNECTION_DRAIN_TIMEOUT.as_secs(),
            "timed out draining connection tasks"
        );
        // Await abort completion; join_next resolves promptly for
        // aborted tasks (their cancellation JoinError is ignored).
        while connections.join_next().await.is_some() {}
    }
}

#[cfg(test)]
fn test_observe_lsp_pool_shutdown() {
    if let Some(observer) = LSP_POOL_SHUTDOWN_OBSERVER
        .lock()
        .expect("lsp pool shutdown observer poisoned")
        .as_ref()
    {
        observer();
    }
}

#[cfg(not(test))]
fn test_observe_lsp_pool_shutdown() {}

/// Test-only hook fired just before the LSP pool shutdown call so
/// shutdown-ordering tests can record the begin -> lsp -> drain
/// sequence. Process-global: tests must clear it after use.
#[cfg(test)]
static LSP_POOL_SHUTDOWN_OBSERVER: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

/// Per-line byte cap on the UDS framing. JSON-RPC requests in practice
/// stay well under 1 MiB; the cap is a guard against a misbehaving (or
/// hostile) peer streaming an unbounded line and pinning the daemon's
/// memory. Apply per connection-side; the trust boundary is still
/// "0700 socket dir on the owning UID", but cheap defense in depth.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Like [`AsyncBufReadExt::read_line`] but enforces [`MAX_LINE_BYTES`]
/// and returns `InvalidData` if a single line exceeds the cap. Uses
/// `Vec<u8>` so we don't pay UTF-8 validation on the hot path; the
/// handler does its own JSON parse downstream.
///
/// Returns the final `buf.len()` (the caller clears `buf` between
/// lines); the newline is included in both the buffer and the cap.
/// On EOF any bytes read so far are returned as an unterminated
/// final line, so 0 with an empty starting `buf` means clean EOF.
async fn read_line_capped<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<usize> {
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(buf.len());
        }
        let (done, n) = match available.iter().position(|&b| b == b'\n') {
            Some(i) => (true, i + 1),
            None => (false, available.len()),
        };
        if buf.len() + n > max {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line exceeds {max} bytes"),
            ));
        }
        buf.extend_from_slice(&available[..n]);
        reader.consume(n);
        if done {
            return Ok(buf.len());
        }
    }
}

/// Per-connection loop: read one newline-delimited request, dispatch
/// it to the handler, write back exactly one response line.
///
/// Returns `Ok(())` on peer EOF or when the handler returns `None`
/// (server-initiated close). Blank lines are skipped, not answered.
/// Oversized or non-UTF-8 input tears this connection down with
/// `InvalidData`; the daemon itself keeps serving other connections.
async fn serve_one(stream: UnixStream, handler: Arc<dyn LineHandler>) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut buf: Vec<u8> = Vec::new();
    loop {
        buf.clear();
        let n = read_line_capped(&mut reader, &mut buf, MAX_LINE_BYTES).await?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        let line = match std::str::from_utf8(&buf) {
            Ok(s) => s,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "non-UTF-8 request line",
                ));
            }
        };
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() {
            continue;
        }
        match handler.handle(trimmed).await {
            Some(mut resp) => {
                if !resp.ends_with('\n') {
                    resp.push('\n');
                }
                write.write_all(resp.as_bytes()).await?;
                write.flush().await?;
            }
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anchor::AnchorName;
    use crate::cas::registry as cas_registry;
    use crate::cas::store as cas_store;
    use crate::ctl::CtlHandler;
    use crate::data_rpc::DataRpc;
    use crate::lifecycle::{RemovalIntent, RepoLifecycleManager};
    use crate::paths::{CasDataDir, path_hash};
    use crate::query::{FindSymbolsArgs, find_symbols};
    use crate::reconcile::RepoReconcileManager;
    use crate::testutil::init_repo;
    use crate::watcher::WatchManager;
    use cairn_watch::WatchBackend;
    use serde_json::json;
    use std::path::Path;
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    struct EchoHandler;

    #[async_trait::async_trait]
    impl LineHandler for EchoHandler {
        async fn handle(&self, line: &str) -> Option<String> {
            Some(format!("echo: {line}"))
        }
    }

    struct BlockingHandler {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl LineHandler for BlockingHandler {
        async fn handle(&self, _line: &str) -> Option<String> {
            self.entered.notify_waiters();
            self.release.notified().await;
            Some("released".into())
        }
    }

    fn runtime_tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    /// Build a fully wired production resource bundle (lifecycle,
    /// jobs, reconcile, watcher) on a fresh temp CAS dir, with the
    /// given handlers substituted for the real RPC surfaces.
    fn ready_resources_with_handlers(
        data_handler: Arc<dyn LineHandler>,
        control_handler: Arc<dyn LineHandler>,
        _shutdown: Arc<Notify>,
    ) -> (tempfile::TempDir, ReadyDaemon) {
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let jobs = crate::jobs::JobManager::with_lifecycle(cas.clone(), lifecycle.clone());
        let reconcile = RepoReconcileManager::new_with_lifecycle(
            cas.clone(),
            Some(jobs.clone()),
            lifecycle.clone(),
        );
        let watcher = Arc::new(WatchManager::with_reconcile(cas, reconcile.clone()));
        (
            data,
            ReadyDaemon {
                data_handler,
                control_handler,
                job_manager: jobs,
                reconcile,
                lifecycle,
                watch_manager: watcher,
            },
        )
    }

    #[tokio::test]
    async fn round_trip_one_request() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());

        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            async move {
                let daemon = Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: None,
                };
                daemon.run().await.unwrap();
            }
        });

        // Give the daemon a moment to bind.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut conn = UnixStream::connect(&paths.cairn).await.unwrap();
        conn.write_all(b"hello\n").await.unwrap();
        conn.flush().await.unwrap();

        let mut buf = vec![0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        let resp = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(resp.contains("echo: hello"), "got: {resp:?}");

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(1), daemon_task).await;
    }

    #[tokio::test]
    async fn idle_daemon_outlives_its_teardown_deadline() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let mut daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            async move {
                Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: None,
                }
                .run_with_shutdown_timeout(Duration::from_millis(50))
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !daemon_task.is_finished(),
            "idle lifetime must not consume the teardown deadline"
        );
        let response = send_control_request(&paths.control, "health-check").await;
        assert!(response.contains("echo: health-check"), "got: {response:?}");

        shutdown.notify_waiters();
        let result = tokio::time::timeout(Duration::from_secs(1), &mut daemon_task)
            .await
            .expect("daemon did not stop after notification")
            .expect("daemon task panicked");
        assert!(result.is_ok(), "daemon teardown failed: {result:?}");
    }

    #[tokio::test]
    async fn initializing_daemon_outlives_deadline_and_acknowledges_shutdown() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let gate = StartupGate::new(shutdown.clone(), "test-version");
        let daemon = InitializingDaemon::bind(paths.clone(), gate, shutdown).unwrap();
        let mut daemon_task =
            tokio::spawn(daemon.run_with_shutdown_timeout(Duration::from_millis(50)));

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !daemon_task.is_finished(),
            "initialization must not consume the teardown deadline"
        );
        let status = send_control_request(
            &paths.control,
            r#"{"jsonrpc":"2.0","id":1,"method":"status","params":null}"#,
        )
        .await;
        let status: serde_json::Value = serde_json::from_str(&status).unwrap();
        assert_eq!(status["result"]["daemon_version"], "test-version");
        assert_eq!(status["result"]["initialization"]["state"], "initializing");

        let response = send_control_request(
            &paths.control,
            r#"{"jsonrpc":"2.0","id":2,"method":"shutdown","params":null}"#,
        )
        .await;
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["result"]["ok"], true);
        let result = tokio::time::timeout(Duration::from_secs(1), &mut daemon_task)
            .await
            .expect("initializing daemon did not stop")
            .expect("initializing daemon task panicked");
        assert!(result.is_ok(), "daemon teardown failed: {result:?}");
    }

    #[tokio::test]
    async fn initializing_daemon_deadline_aborts_blocked_ready_connection() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let gate = StartupGate::new(shutdown.clone(), "test-version");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (_data, resources) = ready_resources_with_handlers(
            Arc::new(BlockingHandler {
                entered: entered.clone(),
                release,
            }),
            Arc::new(EchoHandler),
            shutdown.clone(),
        );
        assert!(gate.publish_ready(resources).is_ok());
        let daemon = InitializingDaemon::bind(paths.clone(), gate, shutdown.clone()).unwrap();
        let daemon_task =
            tokio::spawn(daemon.run_with_shutdown_timeout(Duration::from_millis(100)));

        let mut conn = UnixStream::connect(&paths.cairn).await.unwrap();
        conn.write_all(b"hold\n").await.unwrap();
        conn.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("blocking ready handler was not entered");
        shutdown.notify_waiters();

        let result = tokio::time::timeout(Duration::from_secs(1), daemon_task)
            .await
            .expect("daemon exceeded test bound")
            .expect("daemon task panicked");
        assert!(matches!(
            result,
            Err(crate::Error::ShutdownDeadlineExceeded { timeout_ms: 100 })
        ));
    }

    #[tokio::test]
    async fn control_shutdown_acknowledges_before_clean_daemon_exit() {
        let runtime_tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(runtime_tmp.path().join("runtime"));
        let cas_data_dir = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas_data_dir.ensure().unwrap();
        let shutdown = Arc::new(Notify::new());
        let control_handler = Arc::new(CtlHandler::new(
            cas_data_dir,
            shutdown.clone(),
            "test-version",
        ));
        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            async move {
                Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler,
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: None,
                }
                .run_with_shutdown_timeout(Duration::from_secs(1))
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            send_control_request(
                &paths.control,
                r#"{"jsonrpc":"2.0","id":1,"method":"shutdown","params":{}}"#,
            ),
        )
        .await
        .expect("shutdown acknowledgement timed out");
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["result"]["ok"], true);

        let result = tokio::time::timeout(Duration::from_secs(2), daemon_task)
            .await
            .expect("daemon did not exit after acknowledged shutdown")
            .expect("daemon task panicked");
        assert!(result.is_ok(), "daemon teardown failed: {result:?}");
        assert!(!paths.cairn.exists());
        assert!(!paths.control.exists());
    }

    #[tokio::test]
    async fn shutdown_drains_in_flight_connection_tasks() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let mut daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let daemon = Daemon {
                    paths,
                    data_handler: Arc::new(BlockingHandler { entered, release }),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: None,
                };
                daemon.run().await.unwrap();
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut conn = UnixStream::connect(&paths.cairn).await.unwrap();
        conn.write_all(b"hold\n").await.unwrap();
        conn.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("blocking handler was not entered");

        shutdown.notify_waiters();
        tokio::select! {
            result = &mut daemon_task => panic!("daemon stopped before draining connection: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        release.notify_waiters();
        let mut buf = vec![0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        let resp = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(resp.contains("released"), "got: {resp:?}");
        drop(conn);
        tokio::time::timeout(Duration::from_secs(1), daemon_task)
            .await
            .expect("daemon did not finish after connection released")
            .expect("daemon task panicked");
    }

    #[tokio::test]
    async fn daemon_cancels_jobs_then_stops_lsp_before_job_drain() {
        let tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let cas_data_dir = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas_data_dir.ensure().unwrap();
        let job_manager = crate::jobs::JobManager::new(cas_data_dir);
        let shutdown = Arc::new(Notify::new());
        let events = Arc::new(Mutex::new(Vec::new()));

        // Install the process-global shutdown observers; they are
        // cleared again below so other tests are unaffected.
        {
            let events = events.clone();
            *crate::jobs::JOB_MANAGER_SHUTDOWN_OBSERVER
                .lock()
                .expect("job observer poisoned") = Some(Box::new(move || {
                events.lock().expect("events poisoned").push("begin");
            }));
        }
        {
            let events = events.clone();
            *LSP_POOL_SHUTDOWN_OBSERVER
                .lock()
                .expect("lsp observer poisoned") = Some(Box::new(move || {
                events.lock().expect("events poisoned").push("lsp");
            }));
        }
        {
            let events = events.clone();
            *crate::jobs::JOB_MANAGER_DRAIN_OBSERVER
                .lock()
                .expect("job drain observer poisoned") = Some(Box::new(move || {
                events.lock().expect("events poisoned").push("drain");
            }));
        }

        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            let job_manager = job_manager.clone();
            async move {
                let daemon = Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: Some(job_manager),
                    reconcile: None,
                    lifecycle: None,
                };
                daemon.run().await.unwrap();
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), daemon_task)
            .await
            .expect("daemon did not stop")
            .expect("daemon task panicked");

        *crate::jobs::JOB_MANAGER_SHUTDOWN_OBSERVER
            .lock()
            .expect("job observer poisoned") = None;
        *LSP_POOL_SHUTDOWN_OBSERVER
            .lock()
            .expect("lsp observer poisoned") = None;
        *crate::jobs::JOB_MANAGER_DRAIN_OBSERVER
            .lock()
            .expect("job drain observer poisoned") = None;

        let events = events.lock().expect("events poisoned");
        let begin = events
            .iter()
            .position(|event| *event == "begin")
            .expect("job admission close was not observed");
        let drain = events
            .iter()
            .rposition(|event| *event == "drain")
            .expect("job drain was not observed");
        assert!(
            begin < drain && events[begin + 1..drain].contains(&"lsp"),
            "expected begin -> lsp -> drain ordering, got {events:?}"
        );
    }

    #[tokio::test]
    async fn daemon_shutdown_deadline_is_typed_and_aborts_connection_drain() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                Daemon {
                    paths,
                    data_handler: Arc::new(BlockingHandler { entered, release }),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: None,
                }
                .run_with_shutdown_timeout(Duration::from_millis(100))
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut conn = UnixStream::connect(&paths.cairn).await.unwrap();
        conn.write_all(b"hold\n").await.unwrap();
        conn.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("blocking handler was not entered");

        shutdown.notify_waiters();
        let result = tokio::time::timeout(Duration::from_secs(1), daemon_task)
            .await
            .expect("daemon exceeded test bound")
            .expect("daemon task panicked");
        assert!(matches!(
            result,
            Err(crate::Error::ShutdownDeadlineExceeded { timeout_ms: 100 })
        ));
        assert!(!paths.cairn.exists());
        assert!(!paths.control.exists());
    }

    #[tokio::test]
    async fn clean_teardown_does_not_await_reconcile_register_and_state_recovers() {
        let runtime_tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(runtime_tmp.path().join("runtime"));
        let cas = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas.ensure().unwrap();
        let repo_hash = path_hash(&repo.path().canonicalize().unwrap());
        {
            let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, "demo", repo.path().to_str().unwrap(), &repo_hash, 1)
                .unwrap();
            tx.commit().unwrap();
        }

        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        reconcile.set_test_register_hook({
            let gate = gate.clone();
            Arc::new(move |_, _, _, _| {
                let (lock, wake) = &*gate;
                let mut state = lock.lock().unwrap();
                state.0 = true;
                wake.notify_all();
                while !state.1 {
                    state = wake.wait(state).unwrap();
                }
                Ok(())
            })
        });

        let shutdown = Arc::new(Notify::new());
        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            let reconcile = reconcile.clone();
            async move {
                Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: None,
                    reconcile: Some(reconcile),
                    lifecycle: None,
                }
                .run()
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        reconcile
            .request_dirty_by_alias(
                "demo".into(),
                crate::reconcile::ReconcileTrigger::WatchEvent,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if gate.0.lock().unwrap().0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reconcile register hook did not enter");

        shutdown.notify_waiters();
        let daemon_result = tokio::time::timeout(Duration::from_secs(1), daemon_task)
            .await
            .expect("clean daemon teardown awaited blocked reconcile work")
            .expect("daemon task panicked");
        assert!(daemon_result.is_ok());
        let interrupted = {
            let index = cas_registry::open(&cas.index_db_path()).unwrap();
            cas_registry::get_reconcile_state(&index, &repo_hash)
                .unwrap()
                .unwrap()
        };
        assert_eq!(interrupted.desired_generation, 1);
        assert_eq!(interrupted.applied_generation, 0);
        assert_eq!(interrupted.attempt_generation, Some(1));

        let recovered = RepoReconcileManager::new(cas.clone(), None);
        recovered.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        let recovered_hashes = recovered
            .recover_interrupted_attempts_without_wake()
            .await
            .unwrap();
        assert_eq!(recovered_hashes, vec![repo_hash.clone()]);
        recovered
            .prime_startup_reconcile(recovered_hashes)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = {
                    let index = cas_registry::open(&cas.index_db_path()).unwrap();
                    cas_registry::get_reconcile_state(&index, &repo_hash)
                        .unwrap()
                        .unwrap()
                };
                if state.applied_generation == 2 && state.attempt_generation.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("startup recovery did not apply the abandoned generation");

        {
            let (lock, wake) = &*gate;
            let mut state = lock.lock().unwrap();
            state.1 = true;
            wake.notify_all();
        }
        reconcile.shutdown(Duration::from_secs(2)).await;
        recovered.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn removal_in_progress_does_not_make_clean_daemon_shutdown_fail() {
        let runtime_tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(runtime_tmp.path().join("runtime"));
        let cas = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas.ensure().unwrap();
        let root = repo.path().canonicalize().unwrap();
        let repo_hash = path_hash(&root);
        {
            let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, "demo", &root.to_string_lossy(), &repo_hash, 1).unwrap();
            tx.commit().unwrap();
        }

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        lifecycle.startup_sweep().await.unwrap();
        let jobs = crate::jobs::JobManager::with_lifecycle(cas.clone(), lifecycle.clone());
        let reconcile = RepoReconcileManager::new_with_lifecycle(
            cas.clone(),
            Some(jobs.clone()),
            lifecycle.clone(),
        );
        let watchers = Arc::new(WatchManager::with_reconcile(cas, reconcile.clone()));
        lifecycle
            .bind_runtime(
                Arc::downgrade(&jobs),
                Arc::downgrade(&watchers),
                Arc::downgrade(&reconcile),
            )
            .unwrap();

        // Keep one admitted read alive so the removal owner remains blocked
        // in its lease drain beyond the daemon's lifecycle join budget.
        let lease = lifecycle.acquire_by_repo_hash(&repo_hash).unwrap();
        lifecycle
            .request_removal(RemovalIntent::LastAliasRemoved {
                repo_hash: repo_hash.clone(),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    lifecycle.acquire_by_repo_hash(&repo_hash),
                    Err(crate::Error::RepositoryUnavailable { .. })
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("removal owner did not close repository admission");

        let shutdown = Arc::new(Notify::new());
        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let shutdown = shutdown.clone();
            let lifecycle = lifecycle.clone();
            let jobs = jobs.clone();
            let reconcile = reconcile.clone();
            async move {
                Daemon {
                    paths,
                    data_handler: Arc::new(EchoHandler),
                    control_handler: Arc::new(EchoHandler),
                    shutdown,
                    job_manager: Some(jobs),
                    reconcile: Some(reconcile),
                    lifecycle: Some(lifecycle),
                }
                .run()
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.notify_waiters();
        let result = tokio::time::timeout(Duration::from_secs(3), daemon_task)
            .await
            .expect("daemon teardown exceeded the test bound")
            .expect("daemon task panicked");
        assert!(
            result.is_ok(),
            "a lifecycle join timeout must not fail clean daemon teardown: {result:?}"
        );

        // The timed-out owner task is detached, not aborted. Once the lease
        // drains it must finish the already-durable removal.
        drop(lease);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let removed = {
                    let index = cas_registry::open(&data_tmp.path().join("index.db")).unwrap();
                    cas_registry::lookup_repository(&index, &repo_hash)
                        .unwrap()
                        .is_none()
                };
                if removed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("durable removal did not finish after the lease drained");
        reconcile.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn read_line_capped_rejects_oversized_line() {
        // Stream a payload that exceeds the cap with no newline. The
        // helper must return InvalidData rather than buffer unboundedly.
        let cap = 64usize;
        let payload = vec![b'x'; cap * 4];
        let mut reader = BufReader::new(&payload[..]);
        let mut buf = Vec::new();
        let err = read_line_capped(&mut reader, &mut buf, cap)
            .await
            .expect_err("expected line-too-long error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_line_capped_accepts_line_at_limit() {
        // A line whose total length (including newline) is exactly the
        // cap should succeed.
        let cap = 64usize;
        let mut payload = vec![b'a'; cap - 1];
        payload.push(b'\n');
        let mut reader = BufReader::new(&payload[..]);
        let mut buf = Vec::new();
        let n = read_line_capped(&mut reader, &mut buf, cap).await.unwrap();
        assert_eq!(n, cap);
    }

    #[tokio::test]
    async fn watcher_reindexes_repo_registered_via_daemon_control() {
        // Wire the production lifecycle and reconcile path so registration
        // catch-up and later watcher events both execute real indexing work.
        let (repo, _) = init_repo(&[("src/lib.rs", "pub fn initial_symbol() {}\n")]);
        let runtime_tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(runtime_tmp.path().join("runtime"));
        let cas_data_dir = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas_data_dir.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas_data_dir.clone());
        let reconcile =
            RepoReconcileManager::new_with_lifecycle(cas_data_dir.clone(), None, lifecycle.clone());
        let watch_manager = Arc::new(WatchManager::with_backend_and_reconcile(
            cas_data_dir.clone(),
            WatchBackend::Poll,
            reconcile.clone(),
        ));
        let shutdown = Arc::new(Notify::new());

        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let cas_data_dir = cas_data_dir.clone();
            let shutdown = shutdown.clone();
            let watch_manager = watch_manager.clone();
            let reconcile = reconcile.clone();
            let lifecycle = lifecycle.clone();
            async move {
                let daemon = Daemon {
                    paths,
                    data_handler: Arc::new(DataRpc::with_lifecycle(
                        cas_data_dir.clone(),
                        Some(lifecycle.clone()),
                    )),
                    control_handler: Arc::new(CtlHandler::with_full_context(
                        cas_data_dir,
                        shutdown.clone(),
                        env!("CARGO_PKG_VERSION"),
                        Some(watch_manager),
                        None,
                        Some(reconcile),
                        Some(lifecycle.clone()),
                    )),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: Some(lifecycle),
                };
                daemon.run().await.unwrap();
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let register = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "register_repo",
            "params": {
                "alias": "watched",
                "path": repo.path(),
            }
        });
        let response = send_control_request(&paths.control, &register.to_string()).await;
        assert!(
            response.contains("\"result\""),
            "register response: {response}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;

        let canonical = std::fs::canonicalize(repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas_data_dir.store_db_path(&repo_hash);
        let index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let baseline_state = cas_registry::get_reconcile_state(&index, &repo_hash)
            .unwrap()
            .expect("reconcile state row must exist for watched repo");
        let baseline_desired = baseline_state.desired_generation;
        assert!(
            baseline_desired >= 1
                && baseline_state.applied_generation >= baseline_desired
                && baseline_state.attempt_generation.is_none(),
            "registration catch-up must leave a clean applied generation: {baseline_state:?}"
        );

        let symbol_name = "daemon_watcher_probe_symbol";
        std::fs::write(
            repo.path().join("src/lib.rs"),
            format!("pub fn initial_symbol() {{}}\npub fn {symbol_name}() {{}}\n"),
        )
        .unwrap();

        // Poll: durable generation must advance (watcher event →
        // reconcile manager → desired++) AND the symbol must
        // land in the store (worker executed the reindex).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut saw_symbol = false;
        let mut last_state = Some(baseline_state);
        while tokio::time::Instant::now() < deadline {
            saw_symbol = symbol_exists(&store_path, &canonical, symbol_name);
            last_state = cas_registry::get_reconcile_state(&index, &repo_hash).unwrap();
            let reconcile_applied = last_state.as_ref().is_some_and(|state| {
                state.desired_generation > baseline_desired
                    && state.applied_generation >= state.desired_generation
            });
            if saw_symbol && reconcile_applied {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let state = last_state.expect("reconcile state row must exist for watched repo");
        assert!(
            saw_symbol,
            "watcher did not reindex symbol {symbol_name}; last reconcile state: {state:?}"
        );
        assert!(
            state.desired_generation > baseline_desired,
            "watcher event must bump desired_generation above baseline {baseline_desired}, got {state:?}"
        );
        assert!(
            state.applied_generation >= state.desired_generation,
            "reconcile worker must apply the watcher generation, got {state:?}"
        );

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(1), daemon_task).await;
    }

    #[tokio::test]
    async fn watcher_register_reports_degraded_when_watcher_start_fails() {
        // A failed watcher start persists `WatcherState::Failed` so
        // status/doctor can observe the degradation. The response also keeps
        // the existing `watcher_failed` field for control clients.
        let (repo, _) = init_repo(&[("src/lib.rs", "pub fn initial_symbol() {}\n")]);
        let runtime_tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(runtime_tmp.path().join("runtime"));
        let cas_data_dir = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas_data_dir.ensure().unwrap();
        let reconcile = RepoReconcileManager::new(cas_data_dir.clone(), None);
        // The failing-watcher constructor doesn't accept a
        // reconcile driver directly, but we can bolt one on by
        // constructing a manager with the failing backend and
        // then wiring the reconcile field via `with_backend_and_reconcile`.
        // The failing-watcher fake is `WatchBackend::Poll` +
        // injected failure flag; wire an equivalent here.
        let mut watch_manager = WatchManager::with_backend_and_reconcile(
            cas_data_dir.clone(),
            WatchBackend::Poll,
            reconcile.clone(),
        );
        watch_manager.set_fail_watcher_start(true);
        let watch_manager = Arc::new(watch_manager);
        let shutdown = Arc::new(Notify::new());

        let daemon_task = tokio::spawn({
            let paths = paths.clone();
            let cas_data_dir = cas_data_dir.clone();
            let shutdown = shutdown.clone();
            let watch_manager = watch_manager.clone();
            let reconcile = reconcile.clone();
            async move {
                let daemon = Daemon {
                    paths,
                    data_handler: Arc::new(DataRpc::new(cas_data_dir.clone())),
                    control_handler: Arc::new(CtlHandler::with_full_context(
                        cas_data_dir,
                        shutdown.clone(),
                        env!("CARGO_PKG_VERSION"),
                        Some(watch_manager),
                        None,
                        Some(reconcile),
                        None,
                    )),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
                    lifecycle: None,
                };
                daemon.run().await.unwrap();
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let register = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "register_repo",
            "params": {
                "alias": "degraded",
                "path": repo.path(),
            }
        });
        let response = send_control_request(&paths.control, &register.to_string()).await;
        let value: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(value["result"]["ok"], true);
        assert_eq!(value["result"]["alias"], "degraded");
        assert!(
            value["result"]["watcher_failed"]
                .as_str()
                .is_some_and(|s| s.contains("injected watcher start failure")),
            "register response: {response}"
        );

        let index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "degraded")
            .unwrap()
            .expect("alias must be registered");
        assert!(!watch_manager.is_watching_alias("degraded"));

        // The state must be durable before the registration response returns.
        let observed_failed = cas_registry::get_reconcile_state(&index, &entry.repo_hash)
            .unwrap()
            .is_some_and(|state| {
                state.watcher_state == cas_registry::WatcherState::Failed
                    && state
                        .watcher_error
                        .as_deref()
                        .is_some_and(|error| error.contains("injected watcher start failure"))
            });
        assert!(
            observed_failed,
            "watcher failure must be persisted on the reconcile state row"
        );

        shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(1), daemon_task).await;
    }

    async fn send_control_request(socket: &Path, line: &str) -> String {
        let mut conn = UnixStream::connect(socket).await.unwrap();
        conn.write_all(line.as_bytes()).await.unwrap();
        conn.write_all(b"\n").await.unwrap();
        conn.flush().await.unwrap();

        let mut reader = BufReader::new(conn);
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        response
    }

    fn symbol_exists(store_path: &Path, repo_root: &Path, symbol_name: &str) -> bool {
        let Ok(conn) = cas_store::open_existing(store_path) else {
            return false;
        };
        let Ok(worktree_id) = conn.query_row(
            "SELECT worktree_id FROM worktrees WHERE path = ?1",
            [repo_root.to_string_lossy().as_ref()],
            |row| row.get::<_, i64>(0),
        ) else {
            return false;
        };
        find_symbols(
            &conn,
            &AnchorName::tentative(worktree_id),
            &FindSymbolsArgs {
                query: Some(symbol_name.to_string()),
                ..FindSymbolsArgs::default()
            },
        )
        .is_ok_and(|hits| hits.iter().any(|hit| hit.name == symbol_name))
    }
}
