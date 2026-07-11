//! Control-socket handler.
//!
//! Plugs into the daemon's `control.sock` as a [`LineHandler`]. Each
//! request is one newline-terminated JSON-RPC 2.0 envelope (same
//! shape as the data socket); each response is one
//! newline-terminated JSON-RPC reply.
//!
//! Verbs (`register_repo`, `remove_repo`, `status`, `doctor`,
//! `reindex_repo`, `prune`, `shutdown`) live in [`methods`] and register
//! themselves into [`CONTROL_METHODS`] via `#[distributed_slice]`.
//! Adding a new verb is a one-file change — same pattern the data
//! RPC and MCP front-end already use.
//!
//! The control surface stays admin-shaped (mutations, lifecycle). The
//! read-only query surface lives on the data socket. Both share the
//! envelope so a future LSP / IDE front-end can speak both without
//! protocol bifurcation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use linkme::distributed_slice;
use serde_json::Value;
use tokio::sync::Notify;

use crate::Result;
use crate::daemon::LineHandler;
use crate::jobs::JobManager;
use crate::jsonrpc_dispatch::{self, RpcMethod};
use crate::paths::CasDataDir;
use crate::watcher::WatchManager;

pub mod methods;

// ─── trait + registry ──────────────────────────────────────────────────────

/// One control-socket method. Each implementer lives in its own
/// [`methods`] sub-module and registers a constructor into
/// [`CONTROL_METHODS`] via `#[distributed_slice]`.
#[async_trait::async_trait]
pub trait ControlMethod: Send + Sync {
    /// JSON-RPC method name (e.g. `"register_repo"`). Must match the
    /// `method` field a client sends.
    fn name(&self) -> &'static str;

    /// Run the method. `params` is the request's `params` (or
    /// `Value::Null` when omitted). Successful results become the
    /// JSON-RPC `result`; errors become an `error` envelope via the
    /// shared helper.
    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value>;
}

/// Linker-time registry of control-socket methods. Mirrors the
/// `DATA_METHODS` / `MCP_TOOLS` pattern.
#[allow(unsafe_code)]
#[distributed_slice]
pub static CONTROL_METHODS: [fn() -> Box<dyn ControlMethod>] = [..];

/// Shared state each [`ControlMethod`] needs at dispatch time.
#[derive(Clone)]
pub struct CtlCtx {
    pub cas_data_dir: Arc<CasDataDir>,
    pub shutdown: Arc<Notify>,
    pub watch_manager: Option<Arc<WatchManager>>,
    pub job_manager: Option<Arc<JobManager>>,
    pub reconcile: Option<Arc<crate::reconcile::RepoReconcileManager>>,
    pub version: &'static str,
    pub started_at: Instant,
}

#[async_trait::async_trait]
impl RpcMethod<CtlCtx> for dyn ControlMethod {
    fn name(&self) -> &'static str {
        ControlMethod::name(self)
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        ControlMethod::dispatch(self, ctx, params).await
    }
}

// ─── handler ───────────────────────────────────────────────────────────────

/// Concrete control handler. One instance per daemon. The dispatch
/// table is materialised once from [`CONTROL_METHODS`] at
/// construction.
pub struct CtlHandler {
    ctx: CtlCtx,
    methods: HashMap<&'static str, Box<dyn ControlMethod>>,
}

impl CtlHandler {
    #[must_use]
    pub fn new(
        cas_data_dir: Arc<CasDataDir>,
        shutdown: Arc<Notify>,
        version: &'static str,
    ) -> Self {
        Self::with_watch_manager_and_jobs(cas_data_dir, shutdown, version, None, None)
    }

    #[must_use]
    pub fn with_watch_manager(
        cas_data_dir: Arc<CasDataDir>,
        shutdown: Arc<Notify>,
        version: &'static str,
        watch_manager: Option<Arc<WatchManager>>,
    ) -> Self {
        Self::with_watch_manager_and_jobs(cas_data_dir, shutdown, version, watch_manager, None)
    }

    #[must_use]
    pub fn with_watch_manager_and_jobs(
        cas_data_dir: Arc<CasDataDir>,
        shutdown: Arc<Notify>,
        version: &'static str,
        watch_manager: Option<Arc<WatchManager>>,
        job_manager: Option<Arc<JobManager>>,
    ) -> Self {
        Self::with_full_context(
            cas_data_dir,
            shutdown,
            version,
            watch_manager,
            job_manager,
            None,
        )
    }

    /// Full constructor including the reconcile driver so manual
    /// reindex requests route through the durable state machine.
    #[must_use]
    pub fn with_full_context(
        cas_data_dir: Arc<CasDataDir>,
        shutdown: Arc<Notify>,
        version: &'static str,
        watch_manager: Option<Arc<WatchManager>>,
        job_manager: Option<Arc<JobManager>>,
        reconcile: Option<Arc<crate::reconcile::RepoReconcileManager>>,
    ) -> Self {
        let methods = jsonrpc_dispatch::method_table(&CONTROL_METHODS);
        Self {
            ctx: CtlCtx {
                cas_data_dir,
                shutdown,
                watch_manager,
                job_manager,
                reconcile,
                version,
                started_at: Instant::now(),
            },
            methods,
        }
    }
}

#[async_trait::async_trait]
impl LineHandler for CtlHandler {
    async fn handle(&self, line: &str) -> Option<String> {
        Some(jsonrpc_dispatch::handle_line("control", &self.methods, &self.ctx, line).await)
    }
}

// ─── helpers shared with method modules ───────────────────────────────────

/// Decode `params` into a typed args struct. Returns
/// `Error::InvalidParams` which the envelope helper maps to
/// `INVALID_PARAMS`.
pub(crate) fn parse_params<T: serde::de::DeserializeOwned>(params: Value) -> Result<T> {
    jsonrpc_dispatch::parse_params(params)
}
