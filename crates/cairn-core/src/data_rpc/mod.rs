//! Daemon data RPC — plain JSON-RPC 2.0 over `cairn.sock`.
//!
//! This is the kernel API. Out-of-tree consumers (cairn's MCP front-end,
//! a future LSP front-end, cairn-graph, cairn-audit, IDE plugins) talk
//! to the daemon through this surface; no protocol-specific wrapping.
//!
//! Each method lives in its own module under [`methods`] and registers
//! itself into the [`DATA_METHODS`] distributed slice. Adding a new
//! method is a one-file change: write a `struct Foo; impl DataMethod
//! for Foo` and a `#[distributed_slice]` entry, and the dispatcher
//! picks it up automatically. The cross-cutting amenities the methods
//! share — a snapshot-target resolver for cross-branch queries and
//! the JSON-RPC envelope helpers — live here on [`DataCtx`] / in this
//! module.
//!
//! Admin verbs (`register_repo`, `reindex_repo`, `status`, `doctor`,
//! `shutdown`) live on a separate control socket so the data plane
//! stays read-only by construction. The MCP front-end translates
//! `register_repo` / `reindex_repo` tools into [`cairn_proto::control`]
//! messages on that other socket; the daemon never speaks MCP itself.
//!
//! Wire shape (one request per line, one response per line):
//!
//! ```text
//! → {"jsonrpc":"2.0","id":1,"method":"get_outline","params":{"repo":"demo","file":"src/lib.rs"}}
//! ← {"jsonrpc":"2.0","id":1,"result":{"items":[...]}}
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use cairn_proto::jsonrpc::{
    JsonRpcVersion, Request, RequestId, Response, ResponseError, error_code,
};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::{debug, warn};

use crate::daemon::LineHandler;
use crate::paths::CasDataDir;
use crate::{Error, Result};

pub mod methods;

// ─── trait + registry ──────────────────────────────────────────────────────

/// One JSON-RPC method exposed on the data socket. Each implementer
/// lives in its own [`methods`] sub-module and registers a constructor
/// into [`DATA_METHODS`] via `#[distributed_slice]`. The constructor
/// indirection lets registration be `const`-evaluable while the
/// returned trait object owns whatever per-method state (none, today)
/// the implementation needs.
#[async_trait::async_trait]
pub trait DataMethod: Send + Sync {
    /// JSON-RPC method name advertised on the wire (e.g. `"get_outline"`).
    /// Must match the `method` field a client sends.
    fn name(&self) -> &'static str;

    /// Run the method. `params` is the request's `params` field (or
    /// `Value::Null` when omitted). On success the returned [`Value`]
    /// becomes the JSON-RPC `result`. On error the [`Error`] is
    /// translated into a JSON-RPC error by [`error_from`].
    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value>;
}

/// Linker-time registry of data-RPC methods. Mirrors the pattern used
/// by `cairn-lang-api::LANGUAGE_BACKENDS`: each method module contributes
/// one entry; the daemon collects them at startup and dispatches by
/// name.
#[allow(unsafe_code)]
#[distributed_slice]
pub static DATA_METHODS: [fn() -> Box<dyn DataMethod>] = [..];

/// Shared state each [`DataMethod`] gets at dispatch time. Holds the
/// per-repo CAS root used by every method to open the right store.
#[derive(Clone)]
pub struct DataCtx {
    pub cas_data_dir: Arc<CasDataDir>,
}

// ─── handler ───────────────────────────────────────────────────────────────

/// Plain-JSON-RPC handler bound to `cairn.sock`. One instance per
/// daemon. The dispatch table is materialised once from
/// [`DATA_METHODS`] at construction.
pub struct DataRpc {
    ctx: DataCtx,
    methods: HashMap<&'static str, Box<dyn DataMethod>>,
}

impl DataRpc {
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Self {
        let mut methods: HashMap<&'static str, Box<dyn DataMethod>> = HashMap::new();
        for ctor in DATA_METHODS {
            let method = ctor();
            methods.insert(method.name(), method);
        }
        Self {
            ctx: DataCtx { cas_data_dir },
            methods,
        }
    }
}

#[async_trait::async_trait]
impl LineHandler for DataRpc {
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
        debug!(method = %req.method, "data RPC");
        let resp = self.dispatch(req).await;
        Some(serialize(&resp))
    }
}

impl DataRpc {
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

// ─── helpers shared by method modules ─────────────────────────────────────

/// Decode `params` (the raw `Value` from the JSON-RPC envelope) into a
/// concrete args struct. Returns an `Error::InvalidArgument` (which
/// [`error_from`] maps to `error_code::INVALID_PARAMS`) on shape
/// mismatch.
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
        Error::InvalidArgument(s) if s.contains("has no snapshot") => {
            error_code::SNAPSHOT_NOT_READY
        }
        _ => error_code::INTERNAL_ERROR,
    };
    error_resp(id, code, msg)
}

fn serialize(resp: &Response) -> String {
    serde_json::to_string(resp).unwrap_or_else(|e| {
        warn!(error = %e, "data RPC response serialization failed");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"serialization failed"}}"#
            .to_string()
    })
}



