//! Background read loop and readiness tracking for one LSP session.
//!
//! A single `reader_loop` task owns the server's stdout and
//! dispatches every incoming message: responses are routed to
//! pending request waiters, a small set of server-to-client requests
//! is answered inline, and `$/progress` traffic feeds
//! `ProgressState`, which `wait_for_quiescence` consults to decide
//! when the initial workspace load has settled.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::time::sleep;
use tracing::{debug, info};

use super::error::{Error, Result};
use super::transport::{read_lsp_message, write_lsp_message};

/// Drain `reader` until EOF or a transport error, dispatching each
/// message.
///
/// Per-message dispatch, first match wins:
/// - server-to-client requests we support (currently only
///   `window/workDoneProgress/create`) are answered inline through
///   `writer`; a failed reply write is ignored here
/// - responses are matched against `pending` by numeric id; a
///   response whose waiter is gone (e.g. the request timed out) is
///   dropped
/// - `$/progress` and `rust-analyzer/serverStatus` notifications
///   update `progress`
/// - other notifications are logged at debug level and discarded;
///   other server requests are silently dropped without a response
///
/// On EOF or read error the loop clears `alive` and fails every
/// pending waiter (see `fail_pending`), so callers blocked on a
/// response observe the server's death instead of hanging.
pub(super) async fn reader_loop<R>(
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
                if let Some(response) = server_request_response(&message) {
                    if let Some(writer) = writer.lock().await.as_mut() {
                        let _ = write_lsp_message(writer, &response).await;
                    }
                } else if let Some((id, result)) = response_result(&message) {
                    let tx = pending.lock().await.remove(&id);
                    if let Some(tx) = tx {
                        let _ = tx.send(result);
                    }
                } else if is_progress_notification(&message) {
                    progress.record(&message).await;
                } else if is_server_status_notification(&message) {
                    progress.record_server_status(&message).await;
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

/// Aggregated view of the server's `$/progress` traffic, used to
/// infer when the workspace load has settled.
///
/// `notify` wakes `wait_for_quiescence` after every recorded change;
/// combined with `change_seq` in the snapshot, this lets the waiter
/// detect activity that both started and finished while it slept.
#[derive(Default)]
pub(super) struct ProgressState {
    inner: Mutex<ProgressSnapshot>,
    notify: Notify,
}

#[derive(Default)]
struct ProgressSnapshot {
    /// Progress tokens with a `begin` but no matching `end` yet.
    active_tokens: HashSet<String>,
    /// True once any `begin` has been seen since the last `reset`.
    /// Guards against declaring quiescence before the server has
    /// started reporting work at all.
    saw_begin: bool,
    /// Monotonic count of recorded progress events; lets
    /// `wait_for_quiescence` detect churn during its quiet period.
    change_seq: u64,
}

impl ProgressState {
    /// Clear every field so the readiness state does not persist
    /// across `LspClient::spawn_process` restarts. Without this,
    /// a respawned child inherits `saw_begin = true` from the
    /// prior server and readiness completes prematurely (no
    /// `begin` was actually observed for the new session).
    pub(super) async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.active_tokens.clear();
        inner.saw_begin = false;
        inner.change_seq = 0;
    }

    /// Fold one `$/progress` notification into the snapshot.
    ///
    /// Only `begin` and `end` change the active-token set, but any
    /// message with a `params.value.kind` (including `report`) bumps
    /// `change_seq` and wakes waiters, restarting an in-flight quiet
    /// period. Messages without a kind are ignored; a missing token
    /// is bucketed under a `"<missing>"` placeholder.
    pub(super) async fn record(&self, message: &Value) {
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

    /// Log `rust-analyzer/serverStatus` notifications. Observability
    /// only: the `quiescent` flag is not fed into the readiness
    /// decision, which relies solely on `$/progress` bookkeeping.
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

    /// Resolve once the workspace load looks complete: at least one
    /// progress `begin` has been observed and no tokens have been
    /// active for a full, uninterrupted `quiet_period`.
    ///
    /// Any progress event landing during the quiet period (detected
    /// via `change_seq`) restarts the wait. This never resolves if
    /// the server emits no progress at all, so callers must bound it
    /// with an overall timeout of their own.
    pub(super) async fn wait_for_quiescence(
        &self,
        quiet_period: Duration,
    ) -> WorkspaceLoadComplete {
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

/// Reason `wait_for_quiescence` returned. Currently the only signal
/// is `$/progress` quiescence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkspaceLoadComplete {
    ProgressQuiescence,
}

// LSP progress tokens may be integers or strings; normalize both to
// a string key so they share one set.
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

/// Build the reply for server-to-client requests this client
/// supports.
///
/// Only `window/workDoneProgress/create` is handled; it is answered
/// with a `null` result, matching the spec's void response. `None`
/// means the message is not such a request and dispatch continues.
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

/// Interpret `message` as a JSON-RPC response to one of our
/// requests, returning the request id and outcome.
///
/// Anything carrying a `method` is a request or notification, never
/// a response. Ids are matched as `u64` because that is the only id
/// shape this client emits (`LspClient::next_id`); responses with
/// any other id type yield `None` and are dropped. An `error` member
/// missing the numeric `code` that JSON-RPC 2.0 requires degrades to
/// `Error::Protocol`. A success response without a `result` member
/// maps to `Value::Null`.
pub(super) fn response_result(message: &Value) -> Option<(u64, Result<Value>)> {
    if message.get("method").is_some() {
        return None;
    }
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

/// Fail every in-flight request waiter with a per-receiver copy of
/// `err`, draining the pending map.
async fn fail_pending(pending: &Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>, err: Error) {
    // Two shapes reach here:
    //
    // 1. Clean EOF (`read_lsp_message` returns `Ok(None)`) —
    //    passed in as `Error::ServerExited(None)`. Reconstructed
    //    per receiver so downstream `matches!(_, Err(ServerExited(_)
    //    | ServerExitedWithStderr {..}))` in `PoolEntry::with_lsp_client`
    //    actually fires; a Protocol fallback would silently
    //    turn the respawn cleanup into dead code.
    // 2. Transport / protocol read errors (`Err(_)` from
    //    `read_lsp_message`) — `Error` is not `Clone` (it wraps
    //    `std::io::Error`), so per-receiver we render the text
    //    into `Error::Protocol` as a lossy fallback. When a new
    //    failure shape needs variant preservation, add it to the
    //    match rather than extending the Protocol fallback.
    let mut pending = pending.lock().await;
    for (_, tx) in pending.drain() {
        let replica = match &err {
            Error::ServerExited(_) => Error::ServerExited(None.into()),
            other => Error::Protocol(other.to_string()),
        };
        let _ = tx.send(Err(replica));
    }
}
