//! Connection serving: newline framing, accept loops, and request cancellation.
//!
//! This module owns the UDS line protocol and the request-local
//! cancellation token installed for each connection. Daemon bind,
//! ready publication, and ordered teardown live in the parent module.

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
use tokio::task::JoinSet;
use tracing::{debug, error, warn};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerCloseTestFault {
    InitialClear,
    Reader,
    Observer,
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
    initial_clear_completed: tokio::sync::Semaphore,
    #[cfg(test)]
    cancel_wait_enabled: tokio::sync::Semaphore,
    #[cfg(test)]
    cooperative_wait_enabled: tokio::sync::Semaphore,
    #[cfg(test)]
    cancel_wait_barrier: Mutex<Option<Arc<CancellationWaitBarrier>>>,
    #[cfg(test)]
    blocking_query_start: Mutex<Option<BlockingQueryStartBarrier>>,
    #[cfg(test)]
    peer_close_fault: Mutex<Option<PeerCloseTestFault>>,
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
            initial_clear_completed: tokio::sync::Semaphore::new(0),
            #[cfg(test)]
            cancel_wait_enabled: tokio::sync::Semaphore::new(0),
            #[cfg(test)]
            cooperative_wait_enabled: tokio::sync::Semaphore::new(0),
            #[cfg(test)]
            cancel_wait_barrier: Mutex::new(None),
            #[cfg(test)]
            blocking_query_start: Mutex::new(None),
            #[cfg(test)]
            peer_close_fault: Mutex::new(None),
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
    async fn initial_clear_completed(&self) {
        self.initial_clear_completed
            .acquire()
            .await
            .unwrap()
            .forget();
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

    #[cfg(test)]
    fn install_peer_close_fault(&self, fault: PeerCloseTestFault) {
        *self.peer_close_fault.lock().unwrap() = Some(fault);
    }

    #[cfg(test)]
    fn take_peer_close_fault(&self, fault: PeerCloseTestFault) -> Option<std::io::Error> {
        let mut installed = self.peer_close_fault.lock().unwrap();
        if *installed != Some(fault) {
            return None;
        }
        installed.take();
        Some(std::io::Error::other(format!(
            "injected {fault:?} peer-close observation failure"
        )))
    }

    #[cfg(test)]
    fn active_resources(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.active, state.targets.len())
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

/// Bound on waiting for in-flight connection tasks after an accept
/// loop stops accepting. Applied per socket; both loops drain
/// concurrently inside the same teardown future.
const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Accept connections on `listener` until `shutdown` fires, serving
/// each connection on its own task.
///
/// The returned handle completes only after the bounded connection
/// drain, so awaiting it means no connection task is still running.
pub(super) fn spawn_accept_loop(
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
                    debug!(target: "cairn_core::daemon", socket = name, "accept loop received shutdown");
                    break;
                }
                accepted = listener.accept() => match accepted {
                    Ok((stream, _addr)) => {
                        let h = handler.clone();
                        connections.spawn(async move {
                            if let Err(e) = serve_one(stream, h).await {
                                warn!(target: "cairn_core::daemon", error = %e, socket = name, "{name} connection ended with error", name = name);
                            }
                        });
                    }
                    Err(e) => {
                        error!(target: "cairn_core::daemon", ?e, socket = name, "accept failed");
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
                warn!(target: "cairn_core::daemon", error = %err, socket = name, "connection task failed during shutdown");
            }
        }
    })
    .await;
    if drained.is_err() {
        let remaining = connections.len();
        connections.abort_all();
        warn!(
            target: "cairn_core::daemon",
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

async fn clear_initial_writable_for_request(
    observer: &AsyncFd<OwnedFd>,
    cancellation: &RequestCancellation,
) -> std::io::Result<bool> {
    #[cfg(not(test))]
    let _ = cancellation;
    #[cfg(test)]
    if let Some(error) = cancellation.take_peer_close_fault(PeerCloseTestFault::InitialClear) {
        return Err(error);
    }
    let result = clear_initial_writable(observer).await;
    #[cfg(test)]
    if matches!(result, Ok(false)) {
        cancellation.initial_clear_completed.add_permits(1);
    }
    result
}

#[cfg(test)]
async fn observe_write_close(observer: &AsyncFd<OwnedFd>) -> std::io::Result<()> {
    observe_write_close_with(observer, None).await
}

async fn observe_write_close_for_request(
    observer: &AsyncFd<OwnedFd>,
    cancellation: &RequestCancellation,
) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(error) = cancellation.take_peer_close_fault(PeerCloseTestFault::Observer) {
        return Err(error);
    }
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

async fn reader_has_pending_bytes_for_request(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    cancellation: &RequestCancellation,
) -> std::io::Result<bool> {
    #[cfg(not(test))]
    let _ = cancellation;
    #[cfg(test)]
    if let Some(error) = cancellation.take_peer_close_fault(PeerCloseTestFault::Reader) {
        return Err(error);
    }
    reader_has_pending_bytes(reader).await
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

async fn cancel_drop_and_drain_handler_after_error<F>(
    cancellation: &Arc<RequestCancellation>,
    handler: Pin<Box<F>>,
    error: std::io::Error,
) -> std::io::Result<Option<String>>
where
    F: Future<Output = Option<String>>,
{
    let _ = cancel_drop_and_drain_handler(cancellation, handler).await;
    Err(error)
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

    let mut handler =
        Box::pin(REQUEST_CANCELLATION.scope(cancellation.clone(), handler.handle(line)));
    let observer = match duplicate_write_close_observer(write.as_ref()) {
        Ok(observer) => observer,
        Err(error) => {
            return cancel_drop_and_drain_handler_after_error(&cancellation, handler, error).await;
        }
    };
    let initially_write_closed = tokio::select! {
        biased;
        response = handler.as_mut() => return Ok(response),
        result = clear_initial_writable_for_request(&observer, &cancellation) => result,
    };
    let initially_write_closed = match initially_write_closed {
        Ok(closed) => closed,
        Err(error) => {
            return cancel_drop_and_drain_handler_after_error(&cancellation, handler, error).await;
        }
    };

    if initially_write_closed {
        // A close may already be ready when the observer is registered. Late
        // bytes still outrank it: only read EOF plus WRITE_CLOSED is a full
        // peer close that may cancel the handler.
        let has_pending = tokio::select! {
            biased;
            response = handler.as_mut() => return Ok(response),
            has_pending = reader_has_pending_bytes_for_request(reader, &cancellation) => has_pending,
        };
        return match has_pending {
            Ok(true) => Ok(handler.await),
            Ok(false) => Ok(cancel_drop_and_drain_handler(&cancellation, handler).await),
            Err(error) => {
                cancel_drop_and_drain_handler_after_error(&cancellation, handler, error).await
            }
        };
    }

    let first = tokio::select! {
        biased;
        response = handler.as_mut() => return Ok(response),
        has_pending = reader_has_pending_bytes_for_request(reader, &cancellation) => has_pending.map(Some),
        closed = observe_write_close_for_request(&observer, &cancellation) => closed.map(|()| None),
    };
    let first = match first {
        Ok(first) => first,
        Err(error) => {
            return cancel_drop_and_drain_handler_after_error(&cancellation, handler, error).await;
        }
    };

    match first {
        Some(true) => Ok(handler.await),
        Some(false) => {
            // Read EOF can be a half-close. Keep the response path alive until
            // either the handler finishes or the write side also closes.
            let closed = tokio::select! {
                biased;
                response = handler.as_mut() => return Ok(response),
                closed = observe_write_close_for_request(&observer, &cancellation) => closed,
            };
            match closed {
                Ok(()) => Ok(cancel_drop_and_drain_handler(&cancellation, handler).await),
                Err(error) => {
                    cancel_drop_and_drain_handler_after_error(&cancellation, handler, error).await
                }
            }
        }
        None => {
            // WRITE_CLOSED alone is insufficient when a pipelined request may
            // already be readable. Preserve late bytes and let the handler
            // finish; cancel only when the non-consuming read observes EOF.
            let has_pending = tokio::select! {
                biased;
                response = handler.as_mut() => return Ok(response),
                has_pending = reader_has_pending_bytes_for_request(reader, &cancellation) => has_pending,
            };
            match has_pending {
                Ok(true) => Ok(handler.await),
                Ok(false) => Ok(cancel_drop_and_drain_handler(&cancellation, handler).await),
                Err(error) => {
                    cancel_drop_and_drain_handler_after_error(&cancellation, handler, error).await
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::registry as cas_registry;
    use crate::lifecycle::RepoLifecycleManager;
    use std::collections::BTreeSet;
    use std::sync::{Mutex, mpsc};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;
    use tokio::sync::{Notify, Semaphore};

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

    struct FaultAfterAdmissionHandler {
        fault: PeerCloseTestFault,
        token_sender: Mutex<Option<tokio::sync::oneshot::Sender<Arc<RequestCancellation>>>>,
        dropped: Arc<Notify>,
    }

    #[derive(Clone, Default)]
    struct CapturedConnectionLog(Arc<Mutex<Vec<u8>>>);

    impl CapturedConnectionLog {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for CapturedConnectionLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedConnectionLog {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
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

    async fn drain_until_non_close_readiness(
        peer: &mut UnixStream,
        cancellation: &RequestCancellation,
        written: usize,
        drained: &std::sync::atomic::AtomicUsize,
    ) -> usize {
        let mut total = 0_usize;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            tokio::select! {
                biased;
                () = cancellation.non_close_readiness_processed() => return total,
                result = peer.read(&mut buffer), if total < written => {
                    match result {
                        Ok(0) => panic!(
                            "peer reached EOF before non-close writable readiness"
                        ),
                        Ok(read) => {
                            total += read;
                            assert!(
                                total <= written,
                                "peer drained more bytes than the carrier wrote"
                            );
                            drained.store(total, std::sync::atomic::Ordering::Release);
                        }
                        Err(error) => panic!(
                            "draining peer before non-close writable readiness failed: {error}"
                        ),
                    }
                }
            }
        }
    }

    async fn drain_until_initial_clear(
        peer: &mut UnixStream,
        cancellation: &RequestCancellation,
        written: usize,
        drained: &std::sync::atomic::AtomicUsize,
    ) -> usize {
        let mut total = 0_usize;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            tokio::select! {
                biased;
                () = cancellation.initial_clear_completed() => return total,
                result = peer.read(&mut buffer), if total < written => {
                    match result {
                        Ok(0) => panic!("peer reached EOF before initial writable clear"),
                        Ok(read) => {
                            total += read;
                            assert!(
                                total <= written,
                                "peer drained more bytes than the carrier wrote"
                            );
                            drained.store(total, std::sync::atomic::Ordering::Release);
                        }
                        Err(error) => {
                            panic!("draining peer before initial writable clear failed: {error}")
                        }
                    }
                }
            }
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
    impl LineHandler for FaultAfterAdmissionHandler {
        async fn handle(&self, _line: &str) -> Option<String> {
            let _drop_guard = HandlerDropGuard(self.dropped.clone());
            let cancellation = current_request_cancellation().unwrap();
            cancellation.install_peer_close_fault(self.fault);
            let _drain = cancellation
                .admit()
                .expect("fault carrier was cancelled before admission");
            self.token_sender
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(cancellation)
                .unwrap_or_else(|_| panic!("fault carrier token receiver dropped"));
            std::future::pending().await
        }
    }

    async fn assert_peer_close_error_cancels_and_drains(fault: PeerCloseTestFault) {
        let (server, mut peer) = UnixStream::pair().unwrap();
        let (token_sender, token_receiver) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(Notify::new());
        let dropped_wait = dropped.notified();
        let handler: Arc<dyn LineHandler> = Arc::new(FaultAfterAdmissionHandler {
            fault,
            token_sender: Mutex::new(Some(token_sender)),
            dropped: dropped.clone(),
        });
        let server_task = tokio::spawn(serve_one(server, handler));

        peer.write_all(b"fault\n").await.unwrap();
        peer.flush().await.unwrap();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), token_receiver)
            .await
            .expect("fault carrier handler was not polled")
            .expect("fault carrier handler dropped its token sender");
        let error = tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("peer-close observation error did not terminate serve_one")
            .expect("serve_one task panicked")
            .expect_err("injected peer-close observation error was ignored");
        assert!(error.to_string().contains(&format!("{fault:?}")));
        tokio::time::timeout(Duration::from_secs(1), dropped_wait)
            .await
            .expect("errored handler did not reach terminal drop");
        assert!(cancellation.is_cancelled());
        assert_eq!(cancellation.active_resources(), (0, 0));
        drop(peer);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn accept_loop_connection_error_records_structured_socket_field() {
        let output = CapturedConnectionLog::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(output.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);

        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("control.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let shutdown = Arc::new(Notify::new());
        let (token_sender, token_receiver) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(Notify::new());
        let dropped_wait = dropped.notified();
        let handler: Arc<dyn LineHandler> = Arc::new(FaultAfterAdmissionHandler {
            fault: PeerCloseTestFault::Observer,
            token_sender: Mutex::new(Some(token_sender)),
            dropped: dropped.clone(),
        });
        let accept_loop = spawn_accept_loop("control", listener, handler, shutdown.clone());

        let mut peer = UnixStream::connect(&socket).await.unwrap();
        peer.write_all(b"fault\n").await.unwrap();
        peer.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), token_receiver)
            .await
            .expect("fault carrier handler was not polled")
            .expect("fault carrier handler dropped its token sender");
        tokio::time::timeout(Duration::from_secs(1), dropped_wait)
            .await
            .expect("fault carrier handler did not reach terminal drop");
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(1), accept_loop)
            .await
            .expect("accept loop did not stop after connection failure")
            .expect("accept loop task panicked");

        let captured = output.contents();
        assert_eq!(
            captured
                .matches("control connection ended with error")
                .count(),
            1
        );
        assert!(captured.contains(" WARN cairn_core::daemon:"));
        assert!(captured.contains("error="));
        assert!(captured.contains("socket=\"control\""));
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
        let drained_progress = std::sync::atomic::AtomicUsize::new(0);
        let drained = tokio::time::timeout(
            Duration::from_secs(1),
            drain_until_non_close_readiness(&mut peer, &cancellation, written, &drained_progress),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "non-close writable readiness timed out: written={written}, drained={}",
                drained_progress.load(std::sync::atomic::Ordering::Acquire)
            )
        });
        assert!(drained > 0);
        assert!(drained <= written);
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

        let drained_progress = std::sync::atomic::AtomicUsize::new(0);
        let drained = tokio::time::timeout(
            Duration::from_secs(1),
            drain_until_non_close_readiness(&mut peer, &cancellation, written, &drained_progress),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "non-close writable readiness timed out: written={written}, drained={}",
                drained_progress.load(std::sync::atomic::Ordering::Acquire)
            )
        });
        assert!(drained > 0);
        assert!(drained <= written);
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
    async fn serve_one_polls_handler_while_initial_writable_readiness_is_blocked() {
        let (server, mut peer) = UnixStream::pair().unwrap();
        let chunk = vec![b'x'; 64 * 1024];
        let mut written = 0_usize;
        tokio::time::timeout(Duration::from_secs(1), server.writable())
            .await
            .expect("server did not become writable within the setup bound")
            .expect("server writable readiness failed");
        loop {
            match server.try_write(&chunk) {
                Ok(count) => {
                    assert!(count > 0, "send-buffer fill made zero-byte progress");
                    written += count;
                    assert!(
                        written <= 64 * 1024 * 1024,
                        "send buffer did not reach backpressure within the 64 MiB carrier bound"
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        written > 0,
                        "send-buffer fill observed WouldBlock before writing any bytes"
                    );
                    break;
                }
                Err(error) => panic!("filling server send buffer failed: {error}"),
            }
        }

        let (token_sender, token_receiver) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(Notify::new());
        let dropped_wait = dropped.notified();
        let handler: Arc<dyn LineHandler> = Arc::new(CancellationReportingHandler {
            token_sender: Mutex::new(Some(token_sender)),
            dropped: dropped.clone(),
        });
        let mut server_task = tokio::spawn(serve_one(server, handler));
        peer.write_all(b"blocked\n").await.unwrap();
        peer.flush().await.unwrap();
        let cancellation = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                token = token_receiver => token.expect("handler dropped its cancellation token sender"),
                result = &mut server_task => panic!("serve_one terminated before handler admission: {result:?}"),
            }
        })
            .await
            .expect("handler was not polled before initial writable readiness")
            ;

        let drained_progress = std::sync::atomic::AtomicUsize::new(0);
        let drained = tokio::time::timeout(
            Duration::from_secs(1),
            drain_until_initial_clear(&mut peer, &cancellation, written, &drained_progress),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "initial readiness did not recover: written={written}, drained={}",
                drained_progress.load(std::sync::atomic::Ordering::Acquire)
            )
        });
        assert!(drained <= written);
        assert!(!server_task.is_finished());
        assert!(!cancellation.is_cancelled());

        drop(peer);
        tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
            .await
            .expect("full close did not cancel the admitted handler");
        tokio::time::timeout(Duration::from_secs(1), dropped_wait)
            .await
            .expect("cancelled handler did not reach terminal drop");
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("serve_one did not terminate after handler cancellation")
            .expect("serve_one task panicked")
            .expect("serve_one failed");
    }

    #[tokio::test]
    async fn initial_writable_error_cancels_and_drains_admitted_handler() {
        assert_peer_close_error_cancels_and_drains(PeerCloseTestFault::InitialClear).await;
    }

    #[tokio::test]
    async fn reader_error_cancels_and_drains_admitted_handler() {
        assert_peer_close_error_cancels_and_drains(PeerCloseTestFault::Reader).await;
    }

    #[tokio::test]
    async fn observer_error_cancels_and_drains_admitted_handler() {
        assert_peer_close_error_cancels_and_drains(PeerCloseTestFault::Observer).await;
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
}
