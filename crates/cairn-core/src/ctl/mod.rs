//! Control-socket handler.
//!
//! Plugs into the daemon's `control.sock` as a [`LineHandler`]. Each
//! request is one newline-terminated JSON-RPC 2.0 envelope (same
//! shape as the data socket); each response is one
//! newline-terminated JSON-RPC reply.
//!
//! Verbs (`register_repo`, `remove_repo`, `status`, `doctor`,
//! `reindex_repo`, `shutdown`) live in [`methods`] and register
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

use cairn_proto::jsonrpc::{
    JsonRpcVersion, Request, RequestId, Response, ResponseError, error_code,
};
use linkme::distributed_slice;
use serde_json::Value;
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::daemon::LineHandler;
use crate::paths::CasDataDir;
use crate::{Error, Result};

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
    pub version: &'static str,
    pub started_at: Instant,
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
        let mut methods: HashMap<&'static str, Box<dyn ControlMethod>> = HashMap::new();
        for ctor in CONTROL_METHODS {
            let method = ctor();
            methods.insert(method.name(), method);
        }
        Self {
            ctx: CtlCtx {
                cas_data_dir,
                shutdown,
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
        let req: Request = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                let resp = error_resp(
                    RequestId::Number(0),
                    error_code::PARSE_ERROR,
                    format!("invalid JSON-RPC envelope: {e}"),
                );
                return Some(serialize(&resp));
            }
        };
        debug!(method = %req.method, "control request");
        Some(serialize(&self.dispatch(req).await))
    }
}

impl CtlHandler {
    async fn dispatch(&self, req: Request) -> Response {
        let id = req.id.clone();
        let Some(method) = self.methods.get(req.method.as_str()) else {
            return error_resp(
                id,
                error_code::METHOD_NOT_FOUND,
                format!("unknown method: {}", req.method),
            );
        };
        let params = req.params.clone().unwrap_or(Value::Null);
        match method.dispatch(&self.ctx, params).await {
            Ok(value) => ok_resp(id, value),
            Err(err) => error_from(id, &err),
        }
    }
}

// ─── helpers shared with method modules ───────────────────────────────────

/// Decode `params` into a typed args struct. Returns
/// `Error::InvalidArgument` which the envelope helper maps to
/// `INVALID_PARAMS`.
pub(crate) fn parse_params<T: serde::de::DeserializeOwned>(params: Value) -> Result<T> {
    serde_json::from_value(params)
        .map_err(|e| Error::InvalidArgument(format!("invalid params: {e}")))
}

fn ok_resp(id: RequestId, result: Value) -> Response {
    Response {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: Some(result),
        error: None,
    }
}

fn error_resp(id: RequestId, code: i32, message: impl Into<String>) -> Response {
    Response {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: None,
        error: Some(ResponseError {
            code,
            message: message.into(),
            data: None,
        }),
    }
}

fn error_from(id: RequestId, err: &Error) -> Response {
    let msg = err.to_string();
    let code = match err {
        Error::InvalidArgument(s) if s.starts_with("invalid params") => error_code::INVALID_PARAMS,
        Error::InvalidArgument(s) if s.starts_with("no repo ") => error_code::REPO_NOT_FOUND,
        _ => error_code::INTERNAL_ERROR,
    };
    error_resp(id, code, msg)
}

fn serialize(resp: &Response) -> String {
    serde_json::to_string(resp).unwrap_or_else(|e| {
        warn!(error = %e, "control response serialization failed");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"serialization failed"}}"#
            .to_string()
    })
}
