//! Shared JSON-RPC dispatch helpers for daemon sockets.
//!
//! Control and data sockets intentionally expose different method sets,
//! but their JSON-RPC envelope handling is identical. Keeping the common
//! parse/lookup/error path here avoids drift without hiding the method
//! business logic in macros.
//!
//! # Framing and message shape
//!
//! Each socket connection is line-framed (see
//! [`crate::daemon::LineHandler`]): one JSON-RPC message per newline,
//! one response line per request. Two deviations from the spec are
//! worth calling out. Batch requests — JSON arrays of envelopes, as
//! permitted by JSON-RPC 2.0 §6 — fail to deserialize into a single
//! [`Request`] struct and surface as [`error_code::PARSE_ERROR`];
//! there is no array-level fan-out here. Notifications (requests
//! that omit `id`) are surfaced the same way instead of being
//! silently accepted with no response, because [`Request::id`] is a
//! required field. The `PARSE_ERROR` code covers both, so a
//! structurally invalid Request (missing `method`, wrong `jsonrpc`
//! version, etc.) is not distinguished from invalid JSON on the
//! wire — strictly, the spec reserves `INVALID_REQUEST` (-32600)
//! for the former.
//!
//! # Error-code selection
//!
//! Envelope parse failures use [`error_code::PARSE_ERROR`] (-32700).
//! Unknown method names use [`error_code::METHOD_NOT_FOUND`]
//! (-32601). Method-body failures pass through
//! [`crate::jsonrpc_errors::error_from`], which selects the final
//! code from the typed [`Error`] variant (see that module for the
//! full table).

use std::collections::HashMap;
use std::time::Instant;

use cairn_proto::jsonrpc::{
    Request, RequestId, Response, error_code, error_response as error_resp, ok_response as ok_resp,
    serialize_response as serialize,
};
use serde_json::Value;
use tracing::{debug, error};

use crate::{Error, Result, jsonrpc_errors};

/// One JSON-RPC method in a daemon method table.
///
/// The `Ctx` parameter is the shared handler state (e.g. [`crate::ctl::CtlCtx`]
/// for the control socket, the data-plane context for `data_rpc`). The
/// `Ok(Value)` returned by [`Self::dispatch`] becomes the JSON-RPC
/// `result`; an `Err` is routed through [`crate::jsonrpc_errors::error_from`]
/// to pick the wire code and — for a subset of typed errors — a
/// structured `data` payload.
#[async_trait::async_trait]
pub(crate) trait RpcMethod<Ctx>: Send + Sync {
    /// JSON-RPC method name.
    fn name(&self) -> &'static str;

    /// Run the method with raw JSON-RPC params.
    async fn dispatch(&self, ctx: &Ctx, params: Value) -> Result<Value>;
}

/// Materialize a distributed-slice constructor table into a lookup map.
///
/// Method names are used verbatim as `HashMap` keys, so a duplicate
/// entry silently overwrites its predecessor. `#[distributed_slice]`
/// visitation order is linker-defined and not stable across builds,
/// which is why registrations are expected to be uniquely named.
pub(crate) fn method_table<Ctx, M>(constructors: &[fn() -> Box<M>]) -> HashMap<&'static str, Box<M>>
where
    M: RpcMethod<Ctx> + ?Sized,
{
    let mut methods = HashMap::new();
    for ctor in constructors {
        let method = ctor();
        methods.insert(method.name(), method);
    }
    methods
}

/// Parse and dispatch one newline-delimited JSON-RPC request.
///
/// The returned string is exactly one serialized [`Response`],
/// suitable for direct write-back by the framing layer. On envelope
/// parse failure the response uses [`error_code::PARSE_ERROR`]
/// (-32700); the echoed id is `RequestId::Number(0)` — a placeholder
/// chosen by this implementation, distinct from the [`RequestId::Null`]
/// the JSON-RPC 2.0 spec (§5 Response Object) prescribes when the
/// request id cannot be recovered.
pub(crate) async fn handle_line<Ctx, M>(
    plane: &'static str,
    methods: &HashMap<&'static str, Box<M>>,
    ctx: &Ctx,
    line: &str,
) -> String
where
    M: RpcMethod<Ctx> + ?Sized,
{
    let req: Request = match serde_json::from_str(line) {
        Ok(req) => req,
        Err(err) => {
            let resp = error_resp(
                RequestId::Number(0),
                error_code::PARSE_ERROR,
                format!("invalid JSON-RPC envelope: {err}"),
            );
            return serialize(&resp);
        }
    };
    debug!(method = %req.method, plane, "JSON-RPC request");
    serialize(&dispatch_request(plane, methods, ctx, req).await)
}

/// Parse and dispatch one data-plane request, injecting daemon-side timing into
/// successful result objects. Control RPCs intentionally keep the lean legacy
/// response shape; timing is an agent-facing data-query diagnostic.
///
/// Envelope parse failures follow the same rules as [`handle_line`]:
/// [`error_code::PARSE_ERROR`] with `RequestId::Number(0)` echoed.
/// Timing injection only touches successful, object-shaped results
/// (see [`inject_timing`]).
pub(crate) async fn handle_line_with_result_timing<Ctx, M>(
    plane: &'static str,
    methods: &HashMap<&'static str, Box<M>>,
    ctx: &Ctx,
    line: &str,
) -> String
where
    M: RpcMethod<Ctx> + ?Sized,
{
    let req: Request = match serde_json::from_str(line) {
        Ok(req) => req,
        Err(err) => {
            let resp = error_resp(
                RequestId::Number(0),
                error_code::PARSE_ERROR,
                format!("invalid JSON-RPC envelope: {err}"),
            );
            return serialize(&resp);
        }
    };
    debug!(method = %req.method, plane, "JSON-RPC request");
    serialize(&dispatch_request_with_result_timing(plane, methods, ctx, req).await)
}

/// Dispatch a parsed request against `methods`.
///
/// The response id echoes `req.id` verbatim (numeric, string, or
/// null). An unknown method name maps to
/// [`error_code::METHOD_NOT_FOUND`] (-32601). Method-body errors
/// pass through [`log_sqlite_failure`] first (which fires only when
/// the error carries a SQLite extended code) and then through
/// [`jsonrpc_errors::error_from`], which picks the final wire code
/// and message.
pub(crate) async fn dispatch_request<Ctx, M>(
    plane: &'static str,
    methods: &HashMap<&'static str, Box<M>>,
    ctx: &Ctx,
    req: Request,
) -> Response
where
    M: RpcMethod<Ctx> + ?Sized,
{
    let id = req.id.clone();
    let Some(method) = methods.get(req.method.as_str()) else {
        return error_resp(
            id,
            error_code::METHOD_NOT_FOUND,
            format!("unknown method: {}", req.method),
        );
    };
    let params = req.params.clone().unwrap_or(Value::Null);
    match method.dispatch(ctx, params).await {
        Ok(value) => ok_resp(id, value),
        Err(err) => {
            log_sqlite_failure(plane, method.name(), &err);
            jsonrpc_errors::error_from(id, &err)
        }
    }
}

/// Dispatch a parsed data-plane request and add `timing.server_ms` to object
/// results. The measurement starts immediately before the method body runs so
/// it captures daemon work rather than client/socket round-trip latency.
///
/// Non-object results (JSON arrays, scalars) pass through unchanged;
/// only object results receive a top-level `"timing"` field. Error
/// responses are not annotated. Unknown-method and error handling
/// otherwise mirror [`dispatch_request`].
pub(crate) async fn dispatch_request_with_result_timing<Ctx, M>(
    plane: &'static str,
    methods: &HashMap<&'static str, Box<M>>,
    ctx: &Ctx,
    req: Request,
) -> Response
where
    M: RpcMethod<Ctx> + ?Sized,
{
    let id = req.id.clone();
    let Some(method) = methods.get(req.method.as_str()) else {
        return error_resp(
            id,
            error_code::METHOD_NOT_FOUND,
            format!("unknown method: {}", req.method),
        );
    };
    let params = req.params.clone().unwrap_or(Value::Null);
    let start = Instant::now();
    match method.dispatch(ctx, params).await {
        Ok(mut value) => {
            inject_timing(&mut value, start.elapsed());
            ok_resp(id, value)
        }
        Err(err) => {
            log_sqlite_failure(plane, method.name(), &err);
            jsonrpc_errors::error_from(id, &err)
        }
    }
}

/// Emit a targeted diagnostic log line for SQLite-backed method
/// failures. Fires only when the error is an [`Error::Sqlite`]
/// whose `sqlite_error()` yields a native code — other failure
/// classes, and `Sqlite` variants wrapping non-SQLite `rusqlite`
/// errors, are ignored so this log stream stays a narrow DB-failure
/// signal rather than a generic error firehose.
fn log_sqlite_failure(plane: &'static str, method: &'static str, err: &Error) {
    let Some(extended_code) = err.sqlite_extended_code() else {
        return;
    };
    error!(
        plane,
        method,
        error = %err,
        sqlite_code = ?err.sqlite_error_code(),
        sqlite_extended_code = extended_code,
        "SQLite-backed RPC method failed"
    );
}

/// Insert a `"timing": { "server_ms": <u64> }` field into an object
/// result. Non-object results (arrays, scalars, null) pass through
/// untouched. Elapsed durations that overflow `u64` milliseconds are
/// saturated at [`u64::MAX`], and any pre-existing top-level
/// `"timing"` key is overwritten.
fn inject_timing(value: &mut Value, elapsed: std::time::Duration) {
    let Value::Object(object) = value else {
        return;
    };
    let server_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    object.insert(
        "timing".to_string(),
        serde_json::json!({ "server_ms": server_ms }),
    );
}

/// Decode `params` into a typed args struct.
///
/// Serde deserialization failures are reshaped as
/// [`Error::InvalidParams`], which
/// [`crate::jsonrpc_errors::error_from`] converts to
/// [`error_code::INVALID_PARAMS`] (-32602) on the wire.
pub(crate) fn parse_params<T: serde::de::DeserializeOwned>(params: Value) -> Result<T> {
    serde_json::from_value(params).map_err(|err| Error::InvalidParams(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Echo;

    #[async_trait::async_trait]
    impl RpcMethod<()> for Echo {
        fn name(&self) -> &'static str {
            "echo"
        }

        async fn dispatch(&self, _ctx: &(), params: Value) -> Result<Value> {
            Ok(params)
        }
    }

    fn echo_ctor() -> Box<dyn RpcMethod<()>> {
        Box::new(Echo)
    }

    #[tokio::test]
    async fn handle_line_dispatches_known_method() {
        let methods = method_table(&[echo_ctor as fn() -> Box<dyn RpcMethod<()>>]);
        let response = handle_line(
            "test",
            &methods,
            &(),
            r#"{"jsonrpc":"2.0","id":1,"method":"echo","params":{"ok":true}}"#,
        )
        .await;

        assert_eq!(response, r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#);
    }

    #[tokio::test]
    async fn handle_line_reports_parse_error() {
        let methods = method_table(&[echo_ctor as fn() -> Box<dyn RpcMethod<()>>]);
        let response = handle_line("test", &methods, &(), "{").await;

        assert!(response.contains(r#""code":-32700"#));
        assert!(response.contains("invalid JSON-RPC envelope"));
    }

    #[tokio::test]
    async fn dispatch_request_reports_unknown_method() {
        let methods = method_table(&[echo_ctor as fn() -> Box<dyn RpcMethod<()>>]);
        let response = dispatch_request(
            "test",
            &methods,
            &(),
            Request {
                jsonrpc: cairn_proto::jsonrpc::JsonRpcVersion::V2,
                id: RequestId::Number(7),
                method: "missing".into(),
                params: Some(json!({})),
            },
        )
        .await;

        assert_eq!(response.error.unwrap().code, error_code::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn dispatch_injects_timing_into_data_plane_response() {
        let methods = method_table(&[echo_ctor as fn() -> Box<dyn RpcMethod<()>>]);
        let response = dispatch_request_with_result_timing(
            "test",
            &methods,
            &(),
            Request {
                jsonrpc: cairn_proto::jsonrpc::JsonRpcVersion::V2,
                id: RequestId::Number(9),
                method: "echo".into(),
                params: Some(json!({"ok": true})),
            },
        )
        .await;

        let result = response.result.unwrap();
        assert_eq!(result["ok"], true);
        assert!(result["timing"]["server_ms"].as_u64().is_some());
    }
}
