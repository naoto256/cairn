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
    pub shutdown: Arc<Notify>,
    /// Shutdown is ordered by ownership boundaries: stop accepting first so no
    /// new request can enter, drain in-flight connections so active replies are
    /// not cut off, stop analyzer jobs before the shared LSP pool so workers
    /// cannot race pool teardown, and finally reap LSP child processes.
    pub job_manager: Option<Arc<JobManager>>,
    /// Reconcile driver — required in production so the startup
    /// revision-staleness scan can route parser-revision drift
    /// through the durable state machine rather than the
    /// synchronous full-reindex helper. Tests that don't exercise
    /// the drift path may pass `None`.
    pub reconcile: Option<Arc<crate::reconcile::RepoReconcileManager>>,
}

const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const JOB_MANAGER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

impl Daemon {
    /// Bind both sockets, run accept loops until `shutdown` is
    /// notified, then drop the listeners (and unlink the socket
    /// files as a side effect on Unix).
    ///
    /// # Errors
    /// Bind / accept failures propagate.
    pub async fn run(self) -> Result<()> {
        self.paths.ensure()?;
        let cairn = bind_socket_with_mode(&self.paths.cairn)?;
        let ctrl = bind_socket_with_mode(&self.paths.control)?;
        info!(cairn = %self.paths.cairn.display(), control = %self.paths.control.display(), "daemon listening");

        // Revision-staleness scan: enqueue analyzer reruns for any
        // registered alias whose tentative manifest carries a
        // `workspace_analysis_runs` row at an old `analyzer_revision`
        // (or no row at all). Runs once per daemon boot, fire-and-
        // forget on a blocking thread because every step is sync
        // rusqlite I/O.
        //
        // Crash-isolation invariant: failures inside the scan must
        // never escape this spawn. The scan itself downgrades
        // per-alias errors to a `warn!` and continues; an outer-level
        // failure (e.g. alias-index unreadable) is logged here.
        if let Some(job_manager) = self.job_manager.clone() {
            let cas_data_dir_for_staleness = job_manager.cas_data_dir().clone();
            let reconcile_for_staleness = self.reconcile.clone();
            // The scan is sync SQLite I/O, so we hand it to the blocking
            // pool. We then `spawn` an awaiter so a panic inside the
            // blocking thread surfaces as a `JoinError` and gets
            // logged instead of vanishing silently. The awaiter does
            // not block daemon startup; the outer `if let` returns
            // immediately.
            let scan_handle = tokio::task::spawn_blocking(move || {
                crate::workspace_analyzer::check_revision_staleness_and_enqueue(
                    &cas_data_dir_for_staleness,
                    &job_manager,
                    reconcile_for_staleness.as_ref(),
                )
            });
            tokio::spawn(async move {
                match scan_handle.await {
                    Ok(Ok(_summary)) => {
                        // staleness module already emits a structured
                        // `info!` summary; nothing more to do here.
                    }
                    Ok(Err(err)) => {
                        warn!(
                            error = %err,
                            "revision staleness scan failed; daemon continues"
                        );
                    }
                    Err(join_err) => {
                        // Panic inside the blocking thread. Loud so a
                        // never-fires invariant violation doesn't sit
                        // hidden in production.
                        tracing::error!(
                            error = %join_err,
                            "revision staleness scan panicked; daemon continues (no auto-rerun this boot)"
                        );
                    }
                }
            });
        }

        let cairn_task = spawn_accept_loop(
            "cairn",
            cairn,
            self.data_handler.clone(),
            self.shutdown.clone(),
        );
        let ctrl_task = spawn_accept_loop(
            "control",
            ctrl,
            self.control_handler.clone(),
            self.shutdown.clone(),
        );

        // Wait for both accept loops to stop accepting and drain their
        // in-flight connection tasks before subsystem teardown begins.
        let _ = tokio::join!(cairn_task, ctrl_task);
        if let Some(job_manager) = &self.job_manager {
            job_manager.shutdown(JOB_MANAGER_DRAIN_TIMEOUT).await;
        }
        test_observe_lsp_pool_shutdown();
        if let Err(err) = crate::lsp::pool::shutdown_global_if_initialized().await {
            warn!(error = %err, "lsp pool shutdown failed");
        }
        info!("daemon stopped");

        // Best-effort cleanup of socket files; the OS leaves them
        // behind after the listener is dropped.
        let _ = std::fs::remove_file(&self.paths.cairn);
        let _ = std::fs::remove_file(&self.paths.control);
        Ok(())
    }
}

fn spawn_accept_loop(
    name: &'static str,
    listener: UnixListener,
    handler: Arc<dyn LineHandler>,
    shutdown: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
    use crate::paths::{CasDataDir, path_hash};
    use crate::query::{FindSymbolsArgs, find_symbols};
    use crate::reconcile::RepoReconcileManager;
    use crate::testutil::init_repo;
    use crate::watcher::WatchManager;
    use cairn_watch::WatchBackend;
    use serde_json::json;
    use std::path::Path;
    use std::sync::Mutex;
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
    async fn daemon_shuts_down_jobs_before_lsp_pool() {
        let tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let cas_data_dir = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas_data_dir.ensure().unwrap();
        let job_manager = crate::jobs::JobManager::new(cas_data_dir);
        let shutdown = Arc::new(Notify::new());
        let events = Arc::new(Mutex::new(Vec::new()));

        {
            let events = events.clone();
            *crate::jobs::JOB_MANAGER_SHUTDOWN_OBSERVER
                .lock()
                .expect("job observer poisoned") = Some(Box::new(move || {
                events.lock().expect("events poisoned").push("jobs");
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

        assert_eq!(
            events.lock().expect("events poisoned").as_slice(),
            ["jobs", "lsp"]
        );
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
        // With Phase 2, watcher events land in the reconcile
        // manager which then executes the register/enqueue work.
        // The test therefore wires a real reconcile driver and
        // asserts BOTH the durable generation bump AND the
        // symbol appearing in the store — the reindex is real
        // work, not just a durable intent record.
        let (repo, _) = init_repo(&[("src/lib.rs", "pub fn initial_symbol() {}\n")]);
        let runtime_tmp = runtime_tempdir();
        let data_tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(runtime_tmp.path().join("runtime"));
        let cas_data_dir = Arc::new(CasDataDir::with_root(data_tmp.path().to_path_buf()));
        cas_data_dir.ensure().unwrap();
        let reconcile = RepoReconcileManager::new(cas_data_dir.clone(), None);
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
                    )),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
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
        // Phase 2: a failed watcher start now ALSO persists
        // `WatcherState::Failed` on the reconcile state row so
        // status/doctor can observe the degradation. The old
        // in-response `watcher_failed` string is preserved so
        // wire consumers keep working through the transition.
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
                    )),
                    shutdown,
                    job_manager: None,
                    reconcile: None,
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

        // Poll — record_watcher_failed is fire-and-forget via
        // tokio::spawn, so give it a moment to land.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut observed_failed = false;
        while tokio::time::Instant::now() < deadline {
            let state = cas_registry::get_reconcile_state(&index, &entry.repo_hash).unwrap();
            if let Some(s) = state
                && s.watcher_state == cas_registry::WatcherState::Failed
                && s.watcher_error
                    .as_deref()
                    .is_some_and(|e| e.contains("injected watcher start failure"))
            {
                observed_failed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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
        let Ok(conn) = cas_store::open(store_path) else {
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
