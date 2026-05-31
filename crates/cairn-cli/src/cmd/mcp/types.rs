//! MCP (Model Context Protocol) wire types — the LLM-client-facing
//! envelope used by [`crate::cmd::serve`].
//!
//! MCP rides JSON-RPC 2.0; the envelope itself lives in
//! `cairn_proto::jsonrpc` and is shared with the daemon's data RPC
//! and any other JSON-RPC-framed consumer. This module only models
//! the MCP-specific handshake (`initialize`, `tools/list`,
//! `tools/call`) and the content-block result wrapping.
//!
//! Lives in `cairn-cli`, not in `cairn-proto`, because MCP framing is
//! a property of the `cairn serve` front-end alone — the daemon and
//! every out-of-tree consumer (cairn-graph, cairn-audit, future LSP
//! front-end) speak the protocol-neutral surfaces in
//! `cairn_proto::{methods, control}` instead. If a second MCP
//! front-end ever appears (a Python binding, say), promoting this
//! file back into a shared crate is mechanical.
//!
//! The full MCP surface is documented at
//! <https://modelcontextprotocol.io/>; we only model the subset cairn
//! actually uses.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── initialize ─────────────────────────────────────────────────────────────
//
// We only model the *result* shape; the inbound `InitializeParams`
// (protocolVersion / capabilities / clientInfo) is accepted as
// untyped JSON since we currently echo a fixed protocol version
// regardless of what the client requested. Add a typed Params struct
// here when that policy changes.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    #[serde(rename = "serverInfo")]
    pub server_info: ServerInfo,
    /// Optional free-form guidance shown to the model after the
    /// initialize handshake. Use to nudge a default-tool policy
    /// that individual tool descriptions cannot enforce alone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerCapabilities {
    pub tools: ToolsCapability,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolsCapability {
    #[serde(rename = "listChanged", default)]
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
}

// ─── tools/list ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolsListResult {
    pub tools: Vec<ToolSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

// ─── tools/call ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolsCallParams {
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolsCallResult {
    pub content: Vec<ContentBlock>,
    #[serde(rename = "isError", default)]
    pub is_error: bool,
}

/// MCP content blocks. cairn currently only emits text (JSON-encoded
/// payloads stringified).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ContentBlock {
    Text { text: String },
}
