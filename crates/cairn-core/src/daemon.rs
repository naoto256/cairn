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

use std::future::Future;
use std::os::fd::{AsFd, OwnedFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, Interest};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::jobs::JobManager;
use crate::sockets::{DaemonLockGuard, SocketPaths, bind_socket_with_mode};
use crate::startup::{ReadyDaemon, StartupControlHandler, StartupDataHandler, StartupGate};

/// Implementations receive one newline-delimited request line at a
/// time and return one response line.
#[async_trait::async_trait]
pub trait LineHandler: Send + Sync {
    /// Process one request line. Returning `None` ends the connection
    /// (the server closes the stream cleanly).
    async fn handle(&self, line: &str) -> Option<String>;
}

pub(crate) trait RequestCancelTarget: Send + Sync {
    fn cancel(&self);
}

struct RequestCancellationState {
    active: usize,
    next_target_id: u64,
    targets: Vec<(u64, Weak<dyn RequestCancelTarget>)>,
}

#[cfg(test)]
pub(crate) struct BlockingQueryStartBarrier {
    pub(crate) started: std::sync::mpsc::Sender<()>,
    pub(crate) release: std::sync::mpsc::Receiver<()>,
}

/// Keeps one admitted blocking operation visible until every resource it owns
/// has been released.
pub(crate) struct RequestDrainGuard {
    cancellation: Weak<RequestCancellation>,
    active: bool,
}

impl Drop for RequestDrainGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let Some(cancellation) = self.cancellation.upgrade() else {
            return;
        };
        let drained = {
            let mut state = cancellation
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            debug_assert!(state.active > 0);
            state.active = state.active.saturating_sub(1);
            state.active == 0
        };
        if drained {
            cancellation.drained_notify.notify_waiters();
        }
    }
}

/// Removes a request-local cancellation target when its blocking owner exits.
pub(crate) struct RequestCancelTargetGuard {
    cancellation: Weak<RequestCancellation>,
    id: Option<u64>,
}

impl Drop for RequestCancelTargetGuard {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let Some(cancellation) = self.cancellation.upgrade() else {
            return;
        };
        let mut state = cancellation
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.targets.retain(|(registered, _)| *registered != id);
    }
}

#[cfg(test)]
struct CancellationWaitBarrier {
    future_created: tokio::sync::Semaphore,
    allow_enable: tokio::sync::Semaphore,
    enabled: tokio::sync::Semaphore,
    allow_second_check: tokio::sync::Semaphore,
    before_first_poll: tokio::sync::Semaphore,
    allow_first_poll: tokio::sync::Semaphore,
}

#[cfg(test)]
impl CancellationWaitBarrier {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            future_created: tokio::sync::Semaphore::new(0),
            allow_enable: tokio::sync::Semaphore::new(0),
            enabled: tokio::sync::Semaphore::new(0),
            allow_second_check: tokio::sync::Semaphore::new(0),
            before_first_poll: tokio::sync::Semaphore::new(0),
            allow_first_poll: tokio::sync::Semaphore::new(0),
        })
    }
}

/// Request-local cancellation authority installed by the socket owner.
///
/// Long-running handlers can clone the current token without adding it to the
/// public JSON-RPC method signatures. Cancellation is level-triggered so a
/// blocking query that registers after peer disconnect still observes it.
pub(crate) struct RequestCancellation {
    cancelled: AtomicBool,
    notify: Notify,
    state: Mutex<RequestCancellationState>,
    drained_notify: Notify,
    #[cfg(test)]
    cooperative_started: AtomicBool,
    #[cfg(test)]
    cooperative_notify: Notify,
    #[cfg(test)]
    non_close_readiness: tokio::sync::Semaphore,
    #[cfg(test)]
    cancel_wait_enabled: tokio::sync::Semaphore,
    #[cfg(test)]
    cooperative_wait_enabled: tokio::sync::Semaphore,
    #[cfg(test)]
    cancel_wait_barrier: Mutex<Option<Arc<CancellationWaitBarrier>>>,
    #[cfg(test)]
    blocking_query_start: Mutex<Option<BlockingQueryStartBarrier>>,
}

impl RequestCancellation {
    pub(crate) fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
            state: Mutex::new(RequestCancellationState {
                active: 0,
                next_target_id: 0,
                targets: Vec::new(),
            }),
            drained_notify: Notify::new(),
            #[cfg(test)]
            cooperative_started: AtomicBool::new(false),
            #[cfg(test)]
            cooperative_notify: Notify::new(),
            #[cfg(test)]
            non_close_readiness: tokio::sync::Semaphore::new(0),
            #[cfg(test)]
            cancel_wait_enabled: tokio::sync::Semaphore::new(0),
            #[cfg(test)]
            cooperative_wait_enabled: tokio::sync::Semaphore::new(0),
            #[cfg(test)]
            cancel_wait_barrier: Mutex::new(None),
            #[cfg(test)]
            blocking_query_start: Mutex::new(None),
        }
    }

    pub(crate) fn cancel(&self) {
        let (first, targets) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let first = !self.cancelled.swap(true, Ordering::AcqRel);
            let mut targets = Vec::with_capacity(state.targets.len());
            state.targets.retain(|(_, target)| {
                if let Some(target) = target.upgrade() {
                    targets.push(target);
                    true
                } else {
                    false
                }
            });
            (first, targets)
        };
        if first {
            self.notify.notify_waiters();
        }
        for target in targets {
            target.cancel();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn admit(self: &Arc<Self>) -> Option<RequestDrainGuard> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.cancelled.load(Ordering::Acquire) {
            return None;
        }
        state.active += 1;
        Some(RequestDrainGuard {
            cancellation: Arc::downgrade(self),
            active: true,
        })
    }

    pub(crate) fn register_target(
        self: &Arc<Self>,
        target: &Arc<dyn RequestCancelTarget>,
    ) -> RequestCancelTargetGuard {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.cancelled.load(Ordering::Acquire) {
            drop(state);
            target.cancel();
            return RequestCancelTargetGuard {
                cancellation: Arc::downgrade(self),
                id: None,
            };
        }
        let id = state.next_target_id;
        state.next_target_id = state.next_target_id.wrapping_add(1);
        state.targets.push((id, Arc::downgrade(target)));
        RequestCancelTargetGuard {
            cancellation: Arc::downgrade(self),
            id: Some(id),
        }
    }

    pub(crate) async fn drained(&self) {
        loop {
            {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.active == 0 {
                    return;
                }
            }
            let notified = self.drained_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.active == 0 {
                    return;
                }
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            #[cfg(test)]
            let barrier = self.cancel_wait_barrier.lock().unwrap().take();
            #[cfg(test)]
            if let Some(barrier) = barrier.as_ref() {
                barrier.future_created.add_permits(1);
                barrier.allow_enable.acquire().await.unwrap().forget();
            }
            notified.as_mut().enable();
            #[cfg(test)]
            self.cancel_wait_enabled.add_permits(1);
            #[cfg(test)]
            if let Some(barrier) = barrier.as_ref() {
                barrier.enabled.add_permits(1);
                barrier.allow_second_check.acquire().await.unwrap().forget();
            }
            // The second bit check closes the window before Notified captured
            // the current broadcast generation; later notify_waiters broadcasts
            // are retained by that generation snapshot.
            if self.is_cancelled() {
                return;
            }
            #[cfg(test)]
            if let Some(barrier) = barrier.as_ref() {
                barrier.before_first_poll.add_permits(1);
                barrier.allow_first_poll.acquire().await.unwrap().forget();
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_cooperative_cancel_started(&self) {
        if !self.cooperative_started.swap(true, Ordering::AcqRel) {
            self.cooperative_notify.notify_waiters();
        }
    }

    #[cfg(test)]
    pub(crate) async fn cooperative_cancel_started(&self) {
        loop {
            if self.cooperative_started.load(Ordering::Acquire) {
                return;
            }
            let notified = self.cooperative_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.cooperative_wait_enabled.add_permits(1);
            if self.cooperative_started.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    fn mark_non_close_readiness(&self) {
        self.non_close_readiness.add_permits(1);
    }

    #[cfg(test)]
    async fn non_close_readiness_processed(&self) {
        self.non_close_readiness.acquire().await.unwrap().forget();
    }

    #[cfg(test)]
    async fn cancel_wait_is_enabled(&self) {
        self.cancel_wait_enabled.acquire().await.unwrap().forget();
    }

    #[cfg(test)]
    async fn cooperative_wait_is_enabled(&self) {
        self.cooperative_wait_enabled
            .acquire()
            .await
            .unwrap()
            .forget();
    }

    #[cfg(test)]
    fn install_cancel_wait_barrier(&self) -> Arc<CancellationWaitBarrier> {
        let barrier = CancellationWaitBarrier::new();
        *self.cancel_wait_barrier.lock().unwrap() = Some(barrier.clone());
        barrier
    }

    #[cfg(test)]
    pub(crate) fn install_blocking_query_start_barrier(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        *self.blocking_query_start.lock().unwrap() = Some(BlockingQueryStartBarrier {
            started: started_sender,
            release: release_receiver,
        });
        (started_receiver, release_sender)
    }

    #[cfg(test)]
    pub(crate) fn take_blocking_query_start_barrier(&self) -> Option<BlockingQueryStartBarrier> {
        self.blocking_query_start.lock().unwrap().take()
    }
}

tokio::task_local! {
    static REQUEST_CANCELLATION: Arc<RequestCancellation>;
}

pub(crate) fn current_request_cancellation() -> Option<Arc<RequestCancellation>> {
    REQUEST_CANCELLATION.try_with(Arc::clone).ok()
}

#[cfg(test)]
pub(crate) async fn with_request_cancellation<F>(
    cancellation: Arc<RequestCancellation>,
    future: F,
) -> F::Output
where
    F: Future,
{
    REQUEST_CANCELLATION.scope(cancellation, future).await
}

/// Hand-off bundle used to start the daemon. The two handlers are
/// usually different concrete types (data RPC and control protocol),
/// but they share a uniform line-in / line-out shape over UDS.
pub struct Daemon {
    pub paths: SocketPaths,
    pub data_handler: Arc<dyn LineHandler>,
    pub control_handler: Arc<dyn LineHandler>,
    /// Shared shutdown signal. The daemon arms its shutdown future before
    /// serving; signal it via `notify_waiters` (the control handler's
    /// `shutdown` RPC does).
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
    /// notified, then drop the listeners and explicitly unlink the socket
    /// files (dropping a `UnixListener` does not remove its path). Exclusive
    /// ownership is acquired before stale-node cleanup and retained through
    /// final unlink. The shutdown waiter is armed synchronously when this
    /// method is called, before the returned future is first polled.
    ///
    /// # Errors
    /// Bind / accept failures propagate.
    pub fn run(self) -> impl Future<Output = Result<()>> {
        self.run_with_shutdown_timeout(DAEMON_SHUTDOWN_TIMEOUT)
    }

    fn run_with_shutdown_timeout(
        self,
        shutdown_timeout: Duration,
    ) -> impl Future<Output = Result<()>> {
        // Arm while constructing the future, before its first poll can race a
        // shutdown signal.
        let shutdown_wait = arm_shutdown(self.shutdown.clone());
        async move {
            let daemon_lock = self.paths.acquire_daemon_lock()?;
            self.paths.ensure()?;
            let cairn = bind_socket_with_mode(&self.paths.cairn)?;
            let ctrl = bind_socket_with_mode(&self.paths.control)?;
            info!(cairn = %self.paths.cairn.display(), control = %self.paths.control.display(), "daemon listening");
            if let Some(job_manager) = self.job_manager.clone() {
                spawn_revision_staleness_scan(job_manager, self.reconcile.clone());
            }
            run_bound(
                self.paths,
                daemon_lock,
                cairn,
                ctrl,
                self.data_handler,
                self.control_handler,
                shutdown_wait,
                RuntimeOwnership::Static {
                    job_manager: self.job_manager,
                    lifecycle: self.lifecycle,
                },
                shutdown_timeout,
            )
            .await
        }
    }
}

/// Daemon sockets bound before runtime initialization completes.
pub struct InitializingDaemon {
    paths: SocketPaths,
    daemon_lock: DaemonLockGuard,
    cairn: UnixListener,
    control: UnixListener,
    gate: Arc<StartupGate>,
    shutdown_wait: ArmedShutdown,
}

impl InitializingDaemon {
    /// Claim exclusive ownership and bind both sockets synchronously so callers
    /// can begin initialization only after transport availability is guaranteed.
    pub fn bind(paths: SocketPaths, gate: Arc<StartupGate>, shutdown: Arc<Notify>) -> Result<Self> {
        // Arm synchronously before initialization can race a shutdown signal.
        let shutdown_wait = arm_shutdown(shutdown);
        let daemon_lock = paths.acquire_daemon_lock()?;
        paths.ensure()?;
        let cairn = bind_socket_with_mode(&paths.cairn)?;
        let control = bind_socket_with_mode(&paths.control)?;
        info!(cairn = %paths.cairn.display(), control = %paths.control.display(), "daemon listening; initialization in progress");
        Ok(Self {
            paths,
            daemon_lock,
            cairn,
            control,
            gate,
            shutdown_wait,
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
            self.daemon_lock,
            self.cairn,
            self.control,
            Arc::new(StartupDataHandler::new(self.gate.clone())),
            Arc::new(StartupControlHandler::new(self.gate.clone())),
            self.shutdown_wait,
            RuntimeOwnership::Startup(self.gate),
            shutdown_timeout,
        )
        .await
    }
}

type ArmedShutdown = Pin<Box<OwnedNotified>>;

fn arm_shutdown(shutdown: Arc<Notify>) -> ArmedShutdown {
    let mut wait = Box::pin(shutdown.notified_owned());
    wait.as_mut().enable();
    wait
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
/// teardown under `shutdown_timeout`. The daemon lock remains held while
/// socket files are unlinked best-effort on every exit path, so a successor
/// cannot bind between listener teardown and node cleanup.
#[allow(clippy::too_many_arguments)]
async fn run_bound(
    paths: SocketPaths,
    daemon_lock: DaemonLockGuard,
    cairn: UnixListener,
    control: UnixListener,
    data_handler: Arc<dyn LineHandler>,
    control_handler: Arc<dyn LineHandler>,
    mut shutdown_wait: ArmedShutdown,
    ownership: RuntimeOwnership,
    shutdown_timeout: Duration,
) -> Result<()> {
    // Per-loop notifications use `notify_one`, whose retained permit also
    // covers the small window before each spawned accept task first polls.
    let cairn_shutdown = Arc::new(Notify::new());
    let ctrl_shutdown = Arc::new(Notify::new());
    let mut cairn_task = spawn_accept_loop("cairn", cairn, data_handler, cairn_shutdown.clone());
    let mut ctrl_task =
        spawn_accept_loop("control", control, control_handler, ctrl_shutdown.clone());

    // The daemon lifetime is unbounded. Only teardown work after the
    // Running -> ShuttingDown transition consumes the shutdown budget.
    shutdown_wait.as_mut().await;
    cairn_shutdown.notify_one();
    ctrl_shutdown.notify_one();

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
    // Keep ownership through removal so a successor cannot bind between
    // listener teardown and socket-node cleanup.
    drop(daemon_lock);
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
/// Returns `Ok(())` on peer EOF, on a full disconnect observed during
/// handler execution, or when the handler returns `None`. Blank lines
/// are skipped, not answered.
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
        let response =
            handle_with_peer_close_cancellation(&mut reader, &write, &handler, trimmed).await?;
        match response {
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

fn duplicate_write_close_observer(stream: &UnixStream) -> std::io::Result<AsyncFd<OwnedFd>> {
    let duplicate = stream.as_fd().try_clone_to_owned()?;
    AsyncFd::with_interest(duplicate, Interest::WRITABLE)
}

async fn clear_initial_writable(observer: &AsyncFd<OwnedFd>) -> std::io::Result<bool> {
    let mut readiness = observer.writable().await?;
    let is_write_closed = readiness.ready().is_write_closed();
    readiness.clear_ready();
    Ok(is_write_closed)
}

#[cfg(test)]
async fn observe_write_close(observer: &AsyncFd<OwnedFd>) -> std::io::Result<()> {
    observe_write_close_with(observer, None).await
}

async fn observe_write_close_for_request(
    observer: &AsyncFd<OwnedFd>,
    cancellation: &RequestCancellation,
) -> std::io::Result<()> {
    observe_write_close_with(observer, Some(cancellation)).await
}

async fn observe_write_close_with(
    observer: &AsyncFd<OwnedFd>,
    cancellation: Option<&RequestCancellation>,
) -> std::io::Result<()> {
    #[cfg(not(test))]
    let _ = cancellation;
    loop {
        let mut readiness = observer.writable().await?;
        if readiness.ready().is_write_closed() {
            return Ok(());
        }
        // AsyncFd readiness is edge-driven after `clear_ready`; a spurious
        // writable notification is re-armed without polling in a busy loop.
        readiness.clear_ready();
        #[cfg(test)]
        if let Some(cancellation) = cancellation {
            cancellation.mark_non_close_readiness();
        }
    }
}

async fn reader_has_pending_bytes(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> std::io::Result<bool> {
    // `fill_buf` observes late pipeline input without consuming it, preserving
    // the existing reader as the sole authority for request order.
    Ok(!reader.fill_buf().await?.is_empty())
}

async fn cancel_drop_and_drain_handler<F>(
    cancellation: &Arc<RequestCancellation>,
    handler: Pin<Box<F>>,
) -> Option<String>
where
    F: Future<Output = Option<String>>,
{
    cancellation.cancel();
    drop(handler);
    cancellation.drained().await;
    None
}

async fn handle_with_peer_close_cancellation(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    write: &tokio::net::unix::OwnedWriteHalf,
    handler: &Arc<dyn LineHandler>,
    line: &str,
) -> std::io::Result<Option<String>> {
    let cancellation = Arc::new(RequestCancellation::new());
    if !reader.buffer().is_empty() {
        return Ok(REQUEST_CANCELLATION
            .scope(cancellation, handler.handle(line))
            .await);
    }

    let observer = duplicate_write_close_observer(write.as_ref())?;
    let initially_write_closed = clear_initial_writable(&observer).await?;
    let mut handler =
        Box::pin(REQUEST_CANCELLATION.scope(cancellation.clone(), handler.handle(line)));

    if initially_write_closed {
        // A close may already be ready when the observer is registered. Late
        // bytes still outrank it: only read EOF plus WRITE_CLOSED is a full
        // peer close that may cancel the handler.
        return Ok(tokio::select! {
            biased;
            response = handler.as_mut() => response,
            has_pending = reader_has_pending_bytes(reader) => {
                if has_pending? {
                    handler.await
                } else {
                    cancel_drop_and_drain_handler(&cancellation, handler).await
                }
            }
        });
    }

    let first = tokio::select! {
        biased;
        response = handler.as_mut() => return Ok(response),
        has_pending = reader_has_pending_bytes(reader) => Some(has_pending?),
        closed = observe_write_close_for_request(&observer, &cancellation) => {
            closed?;
            None
        },
    };

    Ok(match first {
        Some(true) => handler.await,
        Some(false) => {
            // Read EOF can be a half-close. Keep the response path alive until
            // either the handler finishes or the write side also closes.
            tokio::select! {
                biased;
                response = handler.as_mut() => response,
                closed = observe_write_close_for_request(&observer, &cancellation) => {
                    closed?;
                    cancel_drop_and_drain_handler(&cancellation, handler).await
                }
            }
        }
        None => {
            // WRITE_CLOSED alone is insufficient when a pipelined request may
            // already be readable. Preserve late bytes and let the handler
            // finish; cancel only when the non-consuming read observes EOF.
            tokio::select! {
                biased;
                response = handler.as_mut() => response,
                has_pending = reader_has_pending_bytes(reader) => {
                    if has_pending? {
                        handler.await
                    } else {
                        cancel_drop_and_drain_handler(&cancellation, handler).await
                    }
                }
            }
        }
    })
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
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::{Condvar, Mutex, mpsc};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    use tokio::sync::Semaphore;

    struct EchoHandler;

    #[async_trait::async_trait]
    impl LineHandler for EchoHandler {
        async fn handle(&self, line: &str) -> Option<String> {
            Some(format!("echo: {line}"))
        }
    }

    #[tokio::test]
    async fn request_cancellation_wait_is_level_triggered_without_lost_wakeup() {
        let before_future = Arc::new(RequestCancellation::new());
        before_future.cancel();
        tokio::time::timeout(Duration::from_secs(1), before_future.cancelled())
            .await
            .expect("cancel-before-future was lost");

        let before_enable = Arc::new(RequestCancellation::new());
        let barrier = before_enable.install_cancel_wait_barrier();
        let waiter = tokio::spawn({
            let cancellation = before_enable.clone();
            async move { cancellation.cancelled().await }
        });
        barrier.future_created.acquire().await.unwrap().forget();
        before_enable.cancel();
        barrier.allow_enable.add_permits(1);
        barrier.enabled.acquire().await.unwrap().forget();
        barrier.allow_second_check.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancel-before-enable was lost")
            .unwrap();

        let before_check = Arc::new(RequestCancellation::new());
        let barrier = before_check.install_cancel_wait_barrier();
        let waiter = tokio::spawn({
            let cancellation = before_check.clone();
            async move { cancellation.cancelled().await }
        });
        barrier.future_created.acquire().await.unwrap().forget();
        barrier.allow_enable.add_permits(1);
        barrier.enabled.acquire().await.unwrap().forget();
        before_check.cancel();
        barrier.allow_second_check.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancel-after-enable-before-check was lost")
            .unwrap();

        let before_poll = Arc::new(RequestCancellation::new());
        let barrier = before_poll.install_cancel_wait_barrier();
        let waiter = tokio::spawn({
            let cancellation = before_poll.clone();
            async move { cancellation.cancelled().await }
        });
        barrier.future_created.acquire().await.unwrap().forget();
        barrier.allow_enable.add_permits(1);
        barrier.enabled.acquire().await.unwrap().forget();
        barrier.allow_second_check.add_permits(1);
        barrier.before_first_poll.acquire().await.unwrap().forget();
        before_poll.cancel();
        barrier.allow_first_poll.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancel-after-check-before-first-poll was lost")
            .unwrap();

        let awaiting = Arc::new(RequestCancellation::new());
        let first = tokio::spawn({
            let cancellation = awaiting.clone();
            async move { cancellation.cancelled().await }
        });
        let second = tokio::spawn({
            let cancellation = awaiting.clone();
            async move { cancellation.cancelled().await }
        });
        awaiting.cancel_wait_is_enabled().await;
        awaiting.cancel_wait_is_enabled().await;
        tokio::task::yield_now().await;
        awaiting.cancel();
        awaiting.cancel();
        for waiter in [first, second] {
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("broadcast or duplicate cancellation was lost")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn cooperative_interrupt_ack_wait_is_level_triggered() {
        let before_future = Arc::new(RequestCancellation::new());
        before_future.mark_cooperative_cancel_started();
        tokio::time::timeout(
            Duration::from_secs(1),
            before_future.cooperative_cancel_started(),
        )
        .await
        .expect("cooperative ack before future was lost");

        let awaiting = Arc::new(RequestCancellation::new());
        let waiter = tokio::spawn({
            let cancellation = awaiting.clone();
            async move { cancellation.cooperative_cancel_started().await }
        });
        awaiting.cooperative_wait_is_enabled().await;
        awaiting.mark_cooperative_cancel_started();
        awaiting.mark_cooperative_cancel_started();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cooperative ack after enable was lost")
            .unwrap();
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

    struct HandlerDropGuard(Arc<Notify>);

    impl Drop for HandlerDropGuard {
        fn drop(&mut self) {
            self.0.notify_waiters();
        }
    }

    struct DropAwareBlockingHandler {
        entered: Arc<Notify>,
        release: Arc<Semaphore>,
        dropped: Arc<Notify>,
    }

    struct CancellationReportingHandler {
        token_sender: Mutex<Option<tokio::sync::oneshot::Sender<Arc<RequestCancellation>>>>,
        dropped: Arc<Notify>,
    }

    struct SqliteTerminalGuard(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for SqliteTerminalGuard {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    struct SyncReleaseOnDrop(Option<mpsc::Sender<()>>);

    impl SyncReleaseOnDrop {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for SyncReleaseOnDrop {
        fn drop(&mut self) {
            self.release();
        }
    }

    struct InterruptingSnapshotHandler {
        calls: std::sync::atomic::AtomicUsize,
        ctx: crate::data_rpc::DataCtx,
        token_sender: Mutex<Option<tokio::sync::oneshot::Sender<Arc<RequestCancellation>>>>,
        handler_dropped: Arc<Notify>,
        query_begin: Option<Arc<Semaphore>>,
        sqlite_started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        sqlite_release: Mutex<Option<mpsc::Receiver<()>>>,
        sqlite_error: Mutex<Option<tokio::sync::oneshot::Sender<Option<rusqlite::ErrorCode>>>>,
        sqlite_terminal: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    struct PanickingSnapshotHandler {
        ctx: crate::data_rpc::DataCtx,
        result: Mutex<Option<tokio::sync::oneshot::Sender<bool>>>,
    }

    #[async_trait::async_trait]
    impl LineHandler for PanickingSnapshotHandler {
        async fn handle(&self, _line: &str) -> Option<String> {
            let result = crate::data_rpc::helpers::query_one_or_all_snapshots(
                &self.ctx,
                crate::data_rpc::helpers::SnapshotQueryRequest {
                    requested_repo: Some("demo".into()),
                    anchor: None,
                    branch: None,
                    method_name: "panic drain integration test",
                    effective_limit: 10,
                    verbose_tier3: false,
                    exact_file: None,
                },
                |_, _, _| -> crate::Result<Vec<i64>> { panic!("intentional blocking query panic") },
                |_: &[i64]| BTreeSet::new(),
                |_| {},
            )
            .await;
            if let Some(sender) = self.result.lock().unwrap().take() {
                let _ = sender.send(result.is_err());
            }
            None
        }
    }

    #[async_trait::async_trait]
    impl LineHandler for InterruptingSnapshotHandler {
        async fn handle(&self, _line: &str) -> Option<String> {
            if self.calls.fetch_add(1, Ordering::AcqRel) != 0 {
                return Some("healthy".into());
            }
            let _handler_drop = HandlerDropGuard(self.handler_dropped.clone());
            let cancellation = current_request_cancellation().unwrap();
            self.token_sender
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(cancellation)
                .unwrap_or_else(|_| panic!("request-token receiver dropped"));
            if let Some(query_begin) = self.query_begin.as_ref() {
                query_begin.acquire().await.unwrap().forget();
            }
            let mut sqlite_started = self.sqlite_started.lock().unwrap().take();
            let sqlite_release = self.sqlite_release.lock().unwrap().take().unwrap();
            let mut sqlite_error = self.sqlite_error.lock().unwrap().take();
            let mut sqlite_terminal = self.sqlite_terminal.lock().unwrap().take();
            let result = crate::data_rpc::helpers::query_one_or_all_snapshots(
                &self.ctx,
                crate::data_rpc::helpers::SnapshotQueryRequest {
                    requested_repo: Some("demo".into()),
                    anchor: None,
                    branch: None,
                    method_name: "socket cancellation integration test",
                    effective_limit: 10,
                    verbose_tier3: false,
                    exact_file: None,
                },
                move |_, connection, _| {
                    let _terminal = SqliteTerminalGuard(sqlite_terminal.take());
                    let mut statement = connection.prepare(
                        "WITH RECURSIVE sequence(value) AS (\
                         SELECT 1 UNION ALL \
                         SELECT value + 1 FROM sequence WHERE value < 1000000000\
                         ) SELECT value FROM sequence",
                    )?;
                    let mut rows = statement.query([])?;
                    let first: i64 = rows.next()?.unwrap().get(0)?;
                    assert_eq!(first, 1);
                    sqlite_started.take().unwrap().send(()).unwrap();
                    sqlite_release.recv().unwrap();
                    let error = rows
                        .next()
                        .expect_err("SQLite step ignored peer-close interruption");
                    if let Some(sender) = sqlite_error.take() {
                        let _ = sender.send(error.sqlite_error_code());
                    }
                    Err::<Vec<i64>, _>(error.into())
                },
                |_: &[i64]| BTreeSet::new(),
                |_| {},
            )
            .await;
            let _ = result;
            None
        }
    }

    #[async_trait::async_trait]
    impl LineHandler for CancellationReportingHandler {
        async fn handle(&self, _line: &str) -> Option<String> {
            let _drop_guard = HandlerDropGuard(self.dropped.clone());
            let cancellation = current_request_cancellation().unwrap();
            self.token_sender
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(cancellation.clone())
                .unwrap_or_else(|_| panic!("cancellation receiver dropped"));
            cancellation.cancelled().await;
            None
        }
    }

    #[async_trait::async_trait]
    impl LineHandler for DropAwareBlockingHandler {
        async fn handle(&self, _line: &str) -> Option<String> {
            let _drop_guard = HandlerDropGuard(self.dropped.clone());
            self.entered.notify_waiters();
            self.release.acquire().await.unwrap().forget();
            Some("released".into())
        }
    }

    struct OrderedBlockingHandler {
        calls: Mutex<Vec<String>>,
        first_entered: Arc<Notify>,
        first_release: Arc<Semaphore>,
    }

    #[async_trait::async_trait]
    impl LineHandler for OrderedBlockingHandler {
        async fn handle(&self, line: &str) -> Option<String> {
            let is_first = {
                let mut calls = self.calls.lock().unwrap();
                calls.push(line.to_string());
                calls.len() == 1
            };
            if is_first {
                self.first_entered.notify_waiters();
                let permit = self.first_release.acquire().await.unwrap();
                permit.forget();
            }
            Some(format!("handled:{line}"))
        }
    }

    #[tokio::test]
    async fn duplicated_write_observer_reports_full_peer_close() {
        let (peer, stream) = UnixStream::pair().unwrap();
        let observer = duplicate_write_close_observer(&stream).unwrap();
        assert!(!clear_initial_writable(&observer).await.unwrap());

        drop(peer);

        tokio::time::timeout(Duration::from_secs(1), observe_write_close(&observer))
            .await
            .expect("full peer close did not produce WRITE_CLOSED")
            .unwrap();
    }

    #[tokio::test]
    async fn initially_ready_write_close_is_not_rejected() {
        let (peer, stream) = UnixStream::pair().unwrap();
        drop(peer);
        let observer = duplicate_write_close_observer(&stream).unwrap();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), clear_initial_writable(&observer))
                .await
                .expect("initial WRITE_CLOSED readiness was not observable")
                .unwrap()
        );
    }

    #[tokio::test]
    async fn duplicated_write_observer_ignores_peer_write_shutdown() {
        let (mut peer, mut stream) = UnixStream::pair().unwrap();
        let observer = duplicate_write_close_observer(&stream).unwrap();
        assert!(!clear_initial_writable(&observer).await.unwrap());
        let mut observer_task = tokio::spawn(async move { observe_write_close(&observer).await });

        peer.shutdown().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut observer_task)
                .await
                .is_err(),
            "peer shutdown(Write) was mistaken for a full close"
        );

        stream.write_all(b"response\n").await.unwrap();
        stream.flush().await.unwrap();
        let mut response = String::new();
        BufReader::new(peer).read_line(&mut response).await.unwrap();
        assert_eq!(response, "response\n");

        observer_task.abort();
        assert!(observer_task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn write_observer_rearms_after_non_close_writable_readiness() {
        let (mut peer, stream) = UnixStream::pair().unwrap();
        let observer = duplicate_write_close_observer(&stream).unwrap();
        assert!(!clear_initial_writable(&observer).await.unwrap());

        let chunk = vec![b'x'; 64 * 1024];
        let mut written = 0_usize;
        loop {
            match stream.try_write(&chunk) {
                Ok(n) => {
                    written += n;
                    assert!(
                        written < 64 * 1024 * 1024,
                        "send buffer did not reach backpressure within the 64 MiB carrier bound"
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("filling send buffer failed: {error}"),
            }
        }

        let cancellation = Arc::new(RequestCancellation::new());
        let observer_task = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { observe_write_close_for_request(&observer, &cancellation).await }
        });
        let mut drained = vec![0_u8; 64 * 1024];
        let drained_len = peer.read(&mut drained).await.unwrap();
        assert!(drained_len > 0);
        tokio::time::timeout(
            Duration::from_secs(1),
            cancellation.non_close_readiness_processed(),
        )
        .await
        .expect("observer did not process non-close writable readiness");
        assert!(
            !observer_task.is_finished(),
            "non-close writable readiness terminated the observer"
        );

        let (heartbeat_tx, heartbeat_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = heartbeat_tx.send(());
        });
        tokio::time::timeout(Duration::from_secs(1), heartbeat_rx)
            .await
            .expect("observer prevented unrelated runtime progress")
            .unwrap();

        drop(peer);
        tokio::time::timeout(Duration::from_secs(1), observer_task)
            .await
            .expect("full close after non-close readiness did not terminate observer")
            .expect("observer task panicked")
            .expect("write-close observation failed");
        drop(stream);
    }

    #[tokio::test]
    async fn serve_one_cancels_blocked_handler_after_non_close_writable_edge_then_full_close() {
        let (server, mut peer) = UnixStream::pair().unwrap();
        let mut duplicate_writer =
            std::fs::File::from(server.as_fd().try_clone_to_owned().unwrap());
        let (token_sender, token_receiver) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(Notify::new());
        let dropped_wait = dropped.notified();
        let handler: Arc<dyn LineHandler> = Arc::new(CancellationReportingHandler {
            token_sender: Mutex::new(Some(token_sender)),
            dropped: dropped.clone(),
        });
        let server_task = tokio::spawn(serve_one(server, handler));

        peer.write_all(b"blocked\n").await.unwrap();
        peer.flush().await.unwrap();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), token_receiver)
            .await
            .expect("handler did not publish its cancellation token")
            .expect("handler dropped its cancellation token sender");
        let chunk = vec![b'x'; 64 * 1024];
        let mut written = 0_usize;
        loop {
            match std::io::Write::write(&mut duplicate_writer, &chunk) {
                Ok(n) => {
                    written += n;
                    assert!(
                        written < 64 * 1024 * 1024,
                        "send buffer did not reach backpressure within the 64 MiB carrier bound"
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("filling duplicated send descriptor failed: {error}"),
            }
        }

        let mut drained = vec![0_u8; 64 * 1024];
        assert!(peer.read(&mut drained).await.unwrap() > 0);
        tokio::time::timeout(
            Duration::from_secs(1),
            cancellation.non_close_readiness_processed(),
        )
        .await
        .expect("request observer did not rearm after non-close writable readiness");
        assert!(!cancellation.is_cancelled());
        assert!(!server_task.is_finished());

        let (heartbeat_sender, heartbeat_receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = heartbeat_sender.send(());
        });
        tokio::time::timeout(Duration::from_secs(1), heartbeat_receiver)
            .await
            .expect("request observer prevented unrelated runtime progress")
            .unwrap();

        drop(peer);
        tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
            .await
            .expect("full close did not cancel the request token");
        tokio::time::timeout(Duration::from_secs(1), dropped_wait)
            .await
            .expect("cancelled handler did not reach terminal drop");
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("serve_one did not terminate after handler cancellation")
            .expect("serve_one task panicked")
            .expect("serve_one failed");
        drop(duplicate_writer);
    }

    #[tokio::test]
    async fn peer_eof_interrupts_actual_snapshot_sql_and_next_connection_remains_healthy() {
        let mut fixture = crate::data_rpc::helpers::test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        drop(index);
        let lifecycle = RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        fixture.ctx.lifecycle = Some(lifecycle.clone());

        let (token_sender, token_receiver) = tokio::sync::oneshot::channel();
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let (error_sender, error_receiver) = tokio::sync::oneshot::channel();
        let (sqlite_terminal_sender, sqlite_terminal_receiver) = tokio::sync::oneshot::channel();
        let handler_dropped = Arc::new(Notify::new());
        let handler_dropped_wait = handler_dropped.notified();
        let mut release_guard = SyncReleaseOnDrop(Some(release_sender));
        let handler = Arc::new(InterruptingSnapshotHandler {
            calls: std::sync::atomic::AtomicUsize::new(0),
            ctx: fixture.ctx.clone(),
            token_sender: Mutex::new(Some(token_sender)),
            handler_dropped: handler_dropped.clone(),
            query_begin: None,
            sqlite_started: Mutex::new(Some(started_sender)),
            sqlite_release: Mutex::new(Some(release_receiver)),
            sqlite_error: Mutex::new(Some(error_sender)),
            sqlite_terminal: Mutex::new(Some(sqlite_terminal_sender)),
        });

        let (server, mut client) = UnixStream::pair().unwrap();
        let first_connection = tokio::spawn(serve_one(server, handler.clone()));
        client.write_all(b"query\n").await.unwrap();
        client.flush().await.unwrap();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), token_receiver)
            .await
            .expect("query handler did not publish its request token")
            .expect("query handler dropped its token sender");
        tokio::time::timeout(Duration::from_secs(1), started_receiver)
            .await
            .expect("actual snapshot SQL did not produce its first row")
            .unwrap();

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), handler_dropped_wait)
            .await
            .expect("peer EOF did not drop the handler future before SQLite drained");
        tokio::time::timeout(
            Duration::from_secs(1),
            cancellation.cooperative_cancel_started(),
        )
        .await
        .expect("peer EOF did not issue SQLite interrupts");
        assert!(
            !first_connection.is_finished(),
            "connection terminated before the admitted blocking query drained"
        );
        release_guard.release();
        let sqlite_error = tokio::time::timeout(Duration::from_secs(1), error_receiver)
            .await
            .expect("interrupted SQLite step did not return")
            .unwrap();
        assert_eq!(
            sqlite_error,
            Some(rusqlite::ErrorCode::OperationInterrupted)
        );
        tokio::time::timeout(Duration::from_secs(1), sqlite_terminal_receiver)
            .await
            .expect("SQLite statement/transaction did not reach terminal drop")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), first_connection)
            .await
            .expect("first connection did not reach terminal EOF")
            .expect("first connection task panicked")
            .expect("first connection failed");

        let (server, mut client) = UnixStream::pair().unwrap();
        let second_connection = tokio::spawn(serve_one(server, handler));
        client.write_all(b"next\n").await.unwrap();
        client.flush().await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            BufReader::new(&mut client).read_line(&mut response),
        )
        .await
        .expect("next request did not receive a response")
        .unwrap();
        assert_eq!(response, "healthy\n");
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), second_connection)
            .await
            .expect("healthy successor connection did not terminate")
            .expect("healthy successor task panicked")
            .expect("healthy successor connection failed");

        tokio::time::timeout(
            Duration::from_secs(1),
            lifecycle.begin_removal_and_wait(&entry.repo_hash),
        )
        .await
        .expect("snapshot lease remained active after query terminal")
        .unwrap();
    }

    #[tokio::test]
    async fn admitted_query_stays_active_when_handler_drops_before_blocking_start() {
        let mut fixture = crate::data_rpc::helpers::test_support::registered_fixture();
        let lifecycle = RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        fixture.ctx.lifecycle = Some(lifecycle.clone());

        let (token_sender, token_receiver) = tokio::sync::oneshot::channel();
        let handler_dropped = Arc::new(Notify::new());
        let handler_dropped_wait = handler_dropped.notified();
        let query_begin = Arc::new(Semaphore::new(0));
        let (unused_started, _unused_started_receiver) = tokio::sync::oneshot::channel();
        let (_unused_release_sender, unused_release) = mpsc::channel();
        let (unused_error, _unused_error_receiver) = tokio::sync::oneshot::channel();
        let (unused_terminal, _unused_terminal_receiver) = tokio::sync::oneshot::channel();
        let handler = Arc::new(InterruptingSnapshotHandler {
            calls: std::sync::atomic::AtomicUsize::new(0),
            ctx: fixture.ctx,
            token_sender: Mutex::new(Some(token_sender)),
            handler_dropped: handler_dropped.clone(),
            query_begin: Some(query_begin.clone()),
            sqlite_started: Mutex::new(Some(unused_started)),
            sqlite_release: Mutex::new(Some(unused_release)),
            sqlite_error: Mutex::new(Some(unused_error)),
            sqlite_terminal: Mutex::new(Some(unused_terminal)),
        });

        let (server, mut client) = UnixStream::pair().unwrap();
        let connection = tokio::spawn(serve_one(server, handler));
        client.write_all(b"query\n").await.unwrap();
        client.flush().await.unwrap();
        let cancellation = token_receiver.await.unwrap();
        let (blocking_started, release_blocking) =
            cancellation.install_blocking_query_start_barrier();
        query_begin.add_permits(1);
        tokio::task::spawn_blocking(move || blocking_started.recv().unwrap())
            .await
            .unwrap();

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), handler_dropped_wait)
            .await
            .expect("peer EOF did not drop handler after pre-spawn admission");
        assert!(
            !connection.is_finished(),
            "drain completed while the admitted blocking closure was paused"
        );
        release_blocking.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("connection did not finish after blocking ownership drained")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn blocking_query_panic_releases_resources_before_request_drain() {
        let mut fixture = crate::data_rpc::helpers::test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        drop(index);
        let lifecycle = RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        fixture.ctx.lifecycle = Some(lifecycle.clone());
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
        let handler = Arc::new(PanickingSnapshotHandler {
            ctx: fixture.ctx,
            result: Mutex::new(Some(result_sender)),
        });

        let (server, mut client) = UnixStream::pair().unwrap();
        let connection = tokio::spawn(serve_one(server, handler));
        client.write_all(b"panic\n").await.unwrap();
        client.flush().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), result_receiver)
                .await
                .expect("blocking panic did not reach handler terminal")
                .unwrap(),
            "blocking panic was not mapped to a query error"
        );
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("connection did not terminate after blocking panic")
            .unwrap()
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            lifecycle.begin_removal_and_wait(&entry.repo_hash),
        )
        .await
        .expect("blocking panic left a snapshot lease active")
        .unwrap();
    }

    #[tokio::test]
    async fn buffered_pipeline_bypasses_write_close_observer() {
        let (mut peer, stream) = UnixStream::pair().unwrap();
        peer.write_all(b"first\nsecond\n").await.unwrap();
        peer.flush().await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut first = String::new();
        reader.read_line(&mut first).await.unwrap();
        assert_eq!(first, "first\n");
        assert!(
            !reader.buffer().is_empty(),
            "pipelined request was not retained by the existing reader"
        );

        // A buffered request remains under the existing reader's ownership;
        // no duplicated descriptor is armed for this handler interval.
        let observer = reader
            .buffer()
            .is_empty()
            .then(|| duplicate_write_close_observer(reader.get_ref()));
        assert!(observer.is_none());

        drop(peer);
        let mut second = String::new();
        reader.read_line(&mut second).await.unwrap();
        assert_eq!(second, "second\n");
        let mut eof = String::new();
        assert_eq!(reader.read_line(&mut eof).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn late_pipeline_bytes_outlive_a_full_close_edge_without_handler_cancellation() {
        let (server, mut client) = UnixStream::pair().unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let dropped = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let dropped_wait = dropped.notified();
        let handler: Arc<dyn LineHandler> = Arc::new(DropAwareBlockingHandler {
            entered: entered.clone(),
            release: release.clone(),
            dropped: dropped.clone(),
        });
        let server_task = tokio::spawn(async move {
            let (read, write) = server.into_split();
            let mut reader = BufReader::new(read);
            let mut first = String::new();
            reader.read_line(&mut first).await.unwrap();
            assert_eq!(first, "first\n");
            let response =
                handle_with_peer_close_cancellation(&mut reader, &write, &handler, "first")
                    .await
                    .unwrap();
            let mut second = String::new();
            reader.read_line(&mut second).await.unwrap();
            let mut eof = String::new();
            let eof_len = reader.read_line(&mut eof).await.unwrap();
            (response, second, eof_len)
        });

        client.write_all(b"first\n").await.unwrap();
        client.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("first handler did not enter");
        client.write_all(b"second\n").await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        assert!(
            tokio::time::timeout(Duration::from_millis(50), dropped_wait)
                .await
                .is_err(),
            "full-close edge cancelled the handler despite buffered late input"
        );
        release.add_permits(1);

        let (response, second, eof_len) = tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("late pipeline helper did not finish")
            .expect("late pipeline helper task panicked");
        assert_eq!(response.as_deref(), Some("released"));
        assert_eq!(second, "second\n");
        assert_eq!(eof_len, 0);
    }

    #[tokio::test]
    async fn dropping_write_close_observer_releases_duplicated_fd() {
        let (mut peer, stream) = UnixStream::pair().unwrap();
        let observer = duplicate_write_close_observer(&stream).unwrap();
        assert!(!clear_initial_writable(&observer).await.unwrap());
        let observer_task = tokio::spawn(async move { observe_write_close(&observer).await });
        drop(stream);

        let mut byte = [0_u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), peer.read(&mut byte))
                .await
                .is_err(),
            "duplicated descriptor was not retained by the observer task"
        );

        observer_task.abort();
        assert!(observer_task.await.unwrap_err().is_cancelled());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte))
                .await
                .expect("peer did not observe EOF after observer cancellation")
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn serve_one_drops_blocked_handler_after_full_peer_close() {
        let (server, mut client) = UnixStream::pair().unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let dropped = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let dropped_wait = dropped.notified();
        let handler: Arc<dyn LineHandler> = Arc::new(DropAwareBlockingHandler {
            entered: entered.clone(),
            release: release.clone(),
            dropped: dropped.clone(),
        });
        let server_task = tokio::spawn(serve_one(server, handler));

        client.write_all(b"blocked\n").await.unwrap();
        client.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("handler did not enter");
        drop(client);

        let result = tokio::time::timeout(Duration::from_secs(1), async {
            dropped_wait.await;
            server_task.await.unwrap()
        })
        .await;
        if result.is_err() {
            release.add_permits(1);
        }
        result
            .expect("full peer close did not cancel the handler")
            .expect("connection task failed after full peer close");
    }

    #[tokio::test]
    async fn serve_one_answers_after_peer_write_shutdown_then_observes_eof() {
        let (server, client) = UnixStream::pair().unwrap();
        let server_task = tokio::spawn(serve_one(server, Arc::new(EchoHandler)));
        let (read, mut write) = client.into_split();

        write.write_all(b"half-close\n").await.unwrap();
        write.shutdown().await.unwrap();
        let mut response = String::new();
        BufReader::new(read).read_line(&mut response).await.unwrap();
        assert_eq!(response, "echo: half-close\n");
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server did not observe EOF after responding")
            .expect("connection task panicked")
            .expect("connection task failed after peer write shutdown");
    }

    #[tokio::test]
    async fn serve_one_preserves_pipelined_request_order_while_first_handler_waits() {
        let (server, client) = UnixStream::pair().unwrap();
        let (read, mut write) = client.into_split();
        let mut reader = BufReader::new(read);
        let first_entered = Arc::new(Notify::new());
        let first_release = Arc::new(Semaphore::new(0));
        let entered_wait = first_entered.notified();
        let handler = Arc::new(OrderedBlockingHandler {
            calls: Mutex::new(Vec::new()),
            first_entered: first_entered.clone(),
            first_release: first_release.clone(),
        });
        let server_task = tokio::spawn(serve_one(server, handler.clone()));

        write.write_all(b"first\n").await.unwrap();
        write.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("first handler did not enter");
        assert_eq!(&*handler.calls.lock().unwrap(), &["first"]);
        write.write_all(b"second\n").await.unwrap();
        write.shutdown().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), reader.fill_buf())
                .await
                .is_err(),
            "serve_one responded before the blocked handler was released"
        );
        first_release.add_permits(1);

        let mut first_response = String::new();
        reader.read_line(&mut first_response).await.unwrap();
        let mut second_response = String::new();
        reader.read_line(&mut second_response).await.unwrap();
        assert_eq!(first_response, "handled:first\n");
        assert_eq!(second_response, "handled:second\n");
        assert_eq!(&*handler.calls.lock().unwrap(), &["first", "second"]);

        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server did not finish after pipelined write shutdown")
            .expect("connection task panicked")
            .expect("connection task failed after pipelined write shutdown");
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
    async fn ready_daemon_observes_shutdown_fired_before_run_is_polled() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let run = Daemon {
            paths: paths.clone(),
            data_handler: Arc::new(EchoHandler),
            control_handler: Arc::new(EchoHandler),
            shutdown: shutdown.clone(),
            job_manager: None,
            reconcile: None,
            lifecycle: None,
        }
        .run_with_shutdown_timeout(Duration::from_millis(100));

        shutdown.notify_waiters();

        let result = tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .expect("pre-fired ready-daemon shutdown notification was lost");
        assert!(result.is_ok(), "daemon teardown failed: {result:?}");
        assert!(!paths.cairn.exists());
        assert!(!paths.control.exists());
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
    async fn daemon_holds_socket_ownership_until_cleanup_finishes() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let daemon_task = tokio::spawn({
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
                .run()
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !(paths.cairn.exists() && paths.control.exists()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("daemon did not bind both sockets");

        let err = paths
            .acquire_daemon_lock()
            .expect_err("a running daemon must retain socket ownership");
        assert!(matches!(err, crate::Error::InvalidArgument(_)));
        assert!(paths.cairn.exists());
        assert!(paths.control.exists());

        shutdown.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), daemon_task)
            .await
            .expect("daemon did not finish cleanup")
            .expect("daemon task panicked")
            .expect("daemon cleanup failed");
        assert!(!paths.cairn.exists());
        assert!(!paths.control.exists());
        paths
            .acquire_daemon_lock()
            .expect("successor must acquire ownership after cleanup");
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
    async fn initializing_daemon_observes_shutdown_fired_before_run() {
        let tmp = runtime_tempdir();
        let paths = SocketPaths::with_runtime_dir(tmp.path().join("runtime"));
        let shutdown = Arc::new(Notify::new());
        let gate = StartupGate::new(shutdown.clone(), "test-version");
        let daemon = InitializingDaemon::bind(paths.clone(), gate, shutdown.clone()).unwrap();

        shutdown.notify_waiters();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            daemon.run_with_shutdown_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("pre-fired shutdown notification was lost");
        assert!(result.is_ok(), "daemon teardown failed: {result:?}");
        assert!(!paths.cairn.exists());
        assert!(!paths.control.exists());
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
        let symbol_source = Path::new("src/daemon_watcher_probe.rs");
        let symbol_path = repo.path().join(symbol_source);
        assert!(
            !symbol_path.exists(),
            "watcher create-edge fixture must start absent: {}",
            symbol_path.display()
        );
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

        let canonical = std::fs::canonicalize(repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas_data_dir.store_db_path(&repo_hash);
        let index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let baseline_state = wait_for_reconcile_terminal_success(
            &index,
            &repo_hash,
            1,
            None,
            Duration::from_secs(20),
        )
        .await;
        let baseline_desired = baseline_state.desired_generation;

        let symbol_name = "daemon_watcher_probe_symbol";
        let mut symbol_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&symbol_path)
            .expect("watcher probe source creation must succeed");
        std::io::Write::write_all(
            &mut symbol_file,
            format!("pub fn {symbol_name}() {{}}\n").as_bytes(),
        )
        .expect("watcher probe source write must succeed");
        drop(symbol_file);

        // Poll: durable generation must advance (watcher event →
        // reconcile manager → desired++) AND the symbol must
        // land in the store (worker executed the reindex).
        let state = wait_for_reconcile_terminal_success(
            &index,
            &repo_hash,
            baseline_desired + 1,
            Some((&store_path, &canonical, symbol_name, symbol_source)),
            Duration::from_secs(20),
        )
        .await;
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

    async fn wait_for_reconcile_terminal_success(
        index: &rusqlite::Connection,
        repo_hash: &str,
        minimum_desired_generation: i64,
        required_symbol: Option<(&Path, &Path, &str, &Path)>,
        timeout: Duration,
    ) -> cas_registry::RepoReconcileState {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let state = cas_registry::get_reconcile_state(index, repo_hash).unwrap();
            let saw_required_symbol = required_symbol.is_none_or(
                |(store_path, repo_root, symbol_name, symbol_source)| {
                    symbol_exists(store_path, repo_root, symbol_name, symbol_source)
                },
            );
            if let Some(state) = &state {
                if state.desired_generation >= minimum_desired_generation
                    && state.applied_generation == state.desired_generation
                    && state.attempt_generation.is_none()
                    && state.last_success_ns.is_some()
                    && state.last_error.is_none()
                    && state.consecutive_failures == 0
                    && state.next_retry_at_ns.is_none()
                    && saw_required_symbol
                {
                    return state.clone();
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "reconcile did not reach terminal success for desired generation >= {minimum_desired_generation}; saw required symbol: {saw_required_symbol}; last state: {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn symbol_exists(
        store_path: &Path,
        repo_root: &Path,
        symbol_name: &str,
        symbol_source: &Path,
    ) -> bool {
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
        .is_ok_and(|hits| {
            hits.iter()
                .any(|hit| hit.name == symbol_name && Path::new(&hit.path) == symbol_source)
        })
    }
}
