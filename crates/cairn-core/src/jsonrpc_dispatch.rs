//! Shared JSON-RPC dispatch helpers for daemon sockets.
//!
//! Control and data sockets intentionally expose different method sets,
//! but their JSON-RPC envelope handling is identical. Keeping the common
//! parse/lookup/error path here avoids drift without hiding the method
//! business logic in macros.

use std::collections::HashMap;
use std::time::Instant;

use cairn_proto::jsonrpc::{
    Request, RequestId, Response, error_code, error_response as error_resp, ok_response as ok_resp,
    serialize_response as serialize,
};
use serde_json::Value;
use tracing::debug;

use crate::{Error, Result, jsonrpc_errors};

/// One JSON-RPC method in a daemon method table.
#[async_trait::async_trait]
pub(crate) trait RpcMethod<Ctx>: Send + Sync {
    /// JSON-RPC method name.
    fn name(&self) -> &'static str;

    /// Run the method with raw JSON-RPC params.
    async fn dispatch(&self, ctx: &Ctx, params: Value) -> Result<Value>;
}

/// Materialize a distributed-slice constructor table into a lookup map.
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
    serialize(&dispatch_request(methods, ctx, req).await)
}

/// Parse and dispatch one data-plane request, injecting daemon-side timing into
/// successful result objects. Control RPCs intentionally keep the lean legacy
/// response shape; timing is an agent-facing data-query diagnostic.
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
    serialize(&dispatch_request_with_result_timing(methods, ctx, req).await)
}

/// Dispatch a parsed request against `methods`.
pub(crate) async fn dispatch_request<Ctx, M>(
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
        Err(err) => jsonrpc_errors::error_from(id, &err),
    }
}

/// Dispatch a parsed data-plane request and add `timing.server_ms` to object
/// results. The measurement starts immediately before the method body runs so
/// it captures daemon work rather than client/socket round-trip latency.
pub(crate) async fn dispatch_request_with_result_timing<Ctx, M>(
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
        Err(err) => jsonrpc_errors::error_from(id, &err),
    }
}

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
