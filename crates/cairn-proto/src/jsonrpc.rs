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
    pub jsonrpc: JsonRpcVersion,
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// JSON-RPC 2.0 response envelope. Either `result` or `error` is present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: JsonRpcVersion,
    pub id: RequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Constant `"2.0"`. Modeled as a unit-like enum so misformatted messages
/// fail to deserialize loudly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonRpcVersion {
    #[serde(rename = "2.0")]
    V2,
}

/// JSON-RPC request ids can be either a number or a string; cairn accepts
/// both and echoes the same shape back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

/// Standard JSON-RPC error codes plus cairn extensions.
pub mod error_code {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;

    // Cairn-specific (-32000 .. -32099 is the implementation-defined range).
    pub const REPO_NOT_FOUND: i32 = -32001;
    pub const FILE_NOT_INDEXED: i32 = -32002;
    pub const SNAPSHOT_NOT_READY: i32 = -32003;
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
    }

    #[test]
    fn jsonrpc_version_rejects_non_two() {
        let result: Result<JsonRpcVersion, _> = serde_json::from_str("\"1.0\"");
        assert!(result.is_err());
    }
}
