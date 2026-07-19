//! JSON-RPC 2.0 envelope shared by every JSON-RPC-framed cairn surface.
//!
//! Both the daemon's data RPC (on `cairn.sock`) and the MCP server in
//! `cairn serve` ride this envelope; `mcp.rs` layers the MCP-specific
//! handshake types on top. Keeping the envelope in its own module lets
//! out-of-tree consumers (cairn-graph, cairn-audit, IDE plugins) speak
//! plain JSON-RPC to the daemon without pulling in MCP types they will
//! never use.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC 2.0 request envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// Protocol version. Cairn only accepts `"2.0"`.
    pub jsonrpc: JsonRpcVersion,
    /// Caller-supplied correlation id. Responses echo this value unchanged.
    pub id: RequestId,
    /// Method name, such as `get_outline`, `find_symbols`, or a control verb.
    pub method: String,
    /// Method-specific argument object. `None` serializes as an omitted
    /// `params` field and is accepted by no-argument methods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// JSON-RPC 2.0 response envelope. Either `result` or `error` is present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Protocol version. Always `"2.0"` for responses constructed here.
    pub jsonrpc: JsonRpcVersion,
    /// Request id being answered, or [`RequestId::Null`] when no valid id
    /// could be recovered from the request.
    pub id: RequestId,
    /// Successful method result. Omitted when [`Self::error`] is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// Failure payload. Omitted when [`Self::result`] is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

/// JSON-RPC error object.
///
/// Standard codes and Cairn's implementation-defined codes live in
/// [`error_code`]. Producers use `data` only when a caller can act on
/// structured details beyond the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseError {
    /// Numeric JSON-RPC error code. See [`error_code`] for the canonical
    /// values emitted by Cairn.
    pub code: i32,
    /// Human-readable diagnostic message.
    pub message: String,
    /// Optional structured error details. `None` is omitted on the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Constant `"2.0"`. Modeled as a unit-like enum so misformatted messages
/// fail to deserialize loudly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonRpcVersion {
    /// JSON-RPC version string `"2.0"`.
    #[serde(rename = "2.0")]
    V2,
}

/// JSON-RPC request ids can be either a number or a string; cairn accepts
/// both and echoes the same shape back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    /// Numeric request id. Cairn accepts and echoes signed integers.
    Number(i64),
    /// String request id. Used when clients need opaque correlation tokens.
    String(String),
    /// Null id. Used for invalid-request responses where no valid id is
    /// available; Cairn does not model notifications separately today.
    Null,
}

/// Standard JSON-RPC error codes plus Cairn extensions.
///
/// `-32700..=-32603` are the JSON-RPC 2.0 standard codes. Cairn-specific
/// failures use the implementation-defined `-32000..=-32099` range.
pub mod error_code {
    /// Invalid JSON text.
    pub const PARSE_ERROR: i32 = -32700;
    /// JSON text is valid but does not have the request-envelope shape.
    pub const INVALID_REQUEST: i32 = -32600;
    /// The request names a method that the selected socket does not serve.
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// Method parameters failed deserialization or semantic validation.
    pub const INVALID_PARAMS: i32 = -32602;
    /// Unexpected server-side failure not represented by a narrower code.
    pub const INTERNAL_ERROR: i32 = -32603;

    /// Requested repository alias is not registered.
    pub const REPO_NOT_FOUND: i32 = -32001;
    /// Requested file is not present in the selected snapshot's index.
    pub const FILE_NOT_INDEXED: i32 = -32002;
    /// More than one physical declaration matches a source lookup.
    pub const AMBIGUOUS_SOURCE: i32 = -32003;
    /// A non-file-targeted lookup could not prove current-snapshot freshness.
    pub const SNAPSHOT_STALE: i32 = -32004;
}

/// Construct a successful JSON-RPC response.
#[must_use]
pub fn ok_response(id: RequestId, result: Value) -> Response {
    Response {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: Some(result),
        error: None,
    }
}

/// Construct an error JSON-RPC response with no `data` payload.
#[must_use]
pub fn error_response(id: RequestId, code: i32, message: impl Into<String>) -> Response {
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

/// Serialize a JSON-RPC response, falling back to a minimal internal-error
/// envelope if serialization itself fails.
#[must_use]
pub fn serialize_response(resp: &Response) -> String {
    serde_json::to_string(resp).unwrap_or_else(|_| {
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"serialization failed"}}"#
            .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trip() {
        let req = Request {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(7),
            method: "get_outline".into(),
            params: Some(json!({"repo": "demo", "file": "src/lib.rs"})),
        };
        let s = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&s).unwrap();
        assert_eq!(back.method, "get_outline");
    }

    #[test]
    fn request_id_accepts_string_or_number() {
        let s: RequestId = serde_json::from_str("\"abc\"").unwrap();
        assert!(matches!(s, RequestId::String(_)));
        let n: RequestId = serde_json::from_str("42").unwrap();
        assert!(matches!(n, RequestId::Number(42)));
        let null: RequestId = serde_json::from_str("null").unwrap();
        assert!(matches!(null, RequestId::Null));
    }

    #[test]
    fn jsonrpc_version_rejects_non_two() {
        let result: Result<JsonRpcVersion, _> = serde_json::from_str("\"1.0\"");
        assert!(result.is_err());
    }

    #[test]
    fn response_helpers_build_success_and_error_envelopes() {
        let ok = ok_response(RequestId::Number(1), json!({"ok": true}));
        assert_eq!(ok.jsonrpc, JsonRpcVersion::V2);
        assert!(ok.result.is_some());
        assert!(ok.error.is_none());

        let err = error_response(
            RequestId::String("x".into()),
            error_code::INVALID_PARAMS,
            "bad",
        );
        assert_eq!(err.jsonrpc, JsonRpcVersion::V2);
        assert!(err.result.is_none());
        assert_eq!(err.error.unwrap().code, error_code::INVALID_PARAMS);
    }
}
