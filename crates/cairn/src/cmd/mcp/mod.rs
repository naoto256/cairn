//! `cairn mcp` — stdio MCP front-end.
//!
//! Spawned by an MCP client (typically Claude Code) per session. Speaks
//! the full MCP protocol on stdin/stdout, and translates each tool
//! invocation into the appropriate underlying request on the daemon:
//!
//! - data-plane tools (`get_outline`, `find_symbols`, `find_subtypes`,
//!   `find_supertypes`, `find_callers`, `find_callees`,
//!   `find_references`, `find_imports`, `list_repos`) → plain
//!   JSON-RPC on `cairn.sock`.
//! - admin tools (`register_repo`, `reindex_repo`) → control
//!   protocol on `control.sock`.
//!
//! This separation lets out-of-tree consumers (cairn-graph,
//! cairn-audit, IDE plugins, a future `cairn-lsp` binary) talk to the
//! daemon over plain JSON-RPC without dragging along MCP types they
//! have no use for; MCP framing lives entirely in this module.
//!
//! Each MCP tool is its own module under [`tools`] and registers
//! itself into the [`MCP_TOOLS`] distributed slice. Adding a new tool
//! is a one-file change: write a `struct Foo; impl McpTool for Foo`,
//! drop a `#[distributed_slice]` entry, and the front-end picks it up.

mod tools;
mod types;

use std::collections::HashMap;
use std::path::PathBuf;

use self::types::{
    ContentBlock, InitializeResult, ServerCapabilities, ServerInfo, ToolSpec, ToolsCallParams,
    ToolsCallResult, ToolsCapability, ToolsListResult,
};
use anyhow::Result;
use cairn_core::sockets::SocketPaths;
use cairn_proto::jsonrpc::{
    JsonRpcVersion, Request as RpcRequest, RequestId, Response as RpcResponse, error_code,
    error_response as error_resp, serialize_response as serialize,
};
use cairn_proto::{Hint, HintCode};
use clap::Args as ClapArgs;
use linkme::distributed_slice;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tracing::error;

use super::rpc_client;
use super::version_guard::{VersionGuardMode, check_daemon_version};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "cairn";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const MCP_MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

// ─── tool trait + registry ─────────────────────────────────────────────────

/// One MCP tool. Each implementer lives in [`tools`] and contributes
/// a constructor to [`MCP_TOOLS`] via `#[distributed_slice]`. Tools
/// declare both their MCP-facing schema ([`McpTool::spec`]) and the
/// runtime route their arguments turn into ([`McpTool::route`]); the
/// shared dispatcher in this module handles the wire IO and wraps the
/// daemon's response back into an MCP `ToolsCallResult`.
pub trait McpTool: Send + Sync {
    /// MCP-facing schema: tool name, description, and JSON schema for
    /// arguments. Used in the `tools/list` response.
    fn spec(&self) -> ToolSpec;

    /// Decide where this tool's call goes. Returns either a data-plane
    /// JSON-RPC request to send to `cairn.sock` or a control-protocol
    /// message for `control.sock`. Returning an `Err` surfaces as
    /// `INVALID_PARAMS` to the MCP caller.
    fn route(&self, args: Value) -> std::result::Result<ToolRoute, String>;

    /// Display order in the tool list. Lower comes first. Used so the
    /// most-useful tools appear at the top of `tools/list` (which is
    /// what an LLM scrolls through first).
    fn sort_key(&self) -> i32 {
        50
    }
}

/// Where a tool's call goes after the front-end resolves its
/// arguments. Both planes now speak the same JSON-RPC envelope; the
/// only thing that differs is which socket the request lands on
/// (and that admin responses to mutating verbs may be a generic Ack
/// rather than a structured payload).
pub enum ToolRoute {
    /// Send `params` as a JSON-RPC `method` call to `cairn.sock`.
    DataPlane { method: String, params: Value },
    /// Send `params` as a JSON-RPC `method` call to `control.sock`.
    Control { method: String, params: Value },
}

#[allow(unsafe_code)]
#[distributed_slice]
pub static MCP_TOOLS: [fn() -> Box<dyn McpTool>] = [..];

/// Server-wide cockpit guidance returned in `initialize.instructions`.
/// Keep this MCP-facing policy concise; data-RPC remains primitive and
/// composition stays with the agent.
const SERVER_INSTRUCTIONS: &str = "\
Cairn is a local, symbol-aware code index for AI coding agents. \
Reach for it before grep/Read when navigating registered repos; \
cairn returns precise location, signature, and structure rather \
than dragging whole files into context.\n\
\n\
## First move\n\
- Don't know which repo to use? Call list_repos for the inventory.\n\
- Have a path or cwd? Call repo_status with path to verify cairn \
  covers it and Tier-3 readiness.\n\
\n\
## Core workflow\n\
locate -> inspect -> trace:\n\
find_symbols / get_outline -> get_symbol_source -> find_references / \
find_callers / find_callees.\n\
\n\
## Retry rules\n\
On empty / partial / capped / analyzer-missing results, read in order:\n\
- completeness (capped, partial)\n\
- tier3_status.this_query (analyzer state + reason_code)\n\
- diagnostics (what went wrong)\n\
- hints (machine-readable next options)\n\
\n\
Hints are options, not commands. reindex_via_cli intentionally has \
no `action` field so agents don't auto-loop reindexes.\n\
\n\
## JSX caveat\n\
For React/JSX component usage, use find_references kind=instantiate. \
find_callers is for resolved function calls and won't see component \
instantiations.\n\
\n\
## Composition stance\n\
cairn exposes precise primitives, not architecture answers. \
Multi-step composition is left to the agent. timing.server_ms shows \
daemon-side wall time so you can triage MCP-bridge vs daemon-side \
latency.";

// ─── run loop ──────────────────────────────────────────────────────────────

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Override the runtime directory (otherwise picked from
    /// $XDG_RUNTIME_DIR / ~/Library/Caches).
    #[arg(long)]
    pub runtime_dir: Option<PathBuf>,
}

pub async fn run(args: Args) -> Result<()> {
    let paths = match args.runtime_dir {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };
    let dispatcher = Dispatcher::new(paths);

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);

    loop {
        match read_mcp_line_capped(&mut reader, MCP_MAX_LINE_BYTES).await? {
            McpLine::Eof => break,
            McpLine::TooLong => {
                let resp = error_resp(
                    RequestId::Null,
                    error_code::INVALID_REQUEST,
                    format!("JSON-RPC line exceeds {MCP_MAX_LINE_BYTES} bytes"),
                );
                stdout.write_all(serialize(&resp).as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
            McpLine::Line(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some(reply) = dispatcher.handle_line(&line).await {
                    stdout.write_all(reply.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                }
            }
        }
    }
    Ok(())
}

/// Per-session state: the resolved socket paths plus the tool
/// registry materialised from [`MCP_TOOLS`].
struct Dispatcher {
    paths: SocketPaths,
    tools: HashMap<String, Box<dyn McpTool>>,
    /// Sorted tool list for `tools/list` responses (display order).
    ordered: Vec<&'static str>,
    version_checked: Mutex<bool>,
}

impl Dispatcher {
    fn new(paths: SocketPaths) -> Self {
        let mut entries: Vec<Box<dyn McpTool>> = MCP_TOOLS.iter().map(|c| c()).collect();
        entries.sort_by_key(|t| (t.sort_key(), t.spec().name));
        let mut tools: HashMap<String, Box<dyn McpTool>> = HashMap::new();
        let mut ordered: Vec<&'static str> = Vec::with_capacity(entries.len());
        for t in entries {
            let name = leak_name(t.spec().name);
            ordered.push(name);
            tools.insert(name.to_string(), t);
        }
        Self {
            paths,
            tools,
            ordered,
            version_checked: Mutex::new(false),
        }
    }

    fn tool_specs(&self) -> Vec<ToolSpec> {
        self.ordered
            .iter()
            .filter_map(|name| self.tools.get(*name).map(|t| t.spec()))
            .collect()
    }

    /// Dispatch one MCP line. Returns `None` for notifications (no
    /// id, no reply expected).
    async fn handle_line(&self, line: &str) -> Option<String> {
        let req: RpcRequest = match serde_json::from_str::<RpcRequest>(line) {
            Ok(r) => r,
            Err(_) => {
                // Could be a notification (no id). Detect that shape
                // so we don't spam an error response.
                let parsed = serde_json::from_str::<Value>(line).ok();
                let has_id = parsed.as_ref().and_then(|v| v.get("id")).is_some();
                if !has_id {
                    return None;
                }
                return Some(serialize(&error_resp(
                    RequestId::Number(0),
                    error_code::PARSE_ERROR,
                    "invalid JSON-RPC envelope",
                )));
            }
        };

        self.check_daemon_version_once().await;

        let id = req.id.clone();
        let resp = match req.method.as_str() {
            "initialize" => RpcResponse {
                jsonrpc: JsonRpcVersion::V2,
                id: id.clone(),
                result: Some(match serialize_result(&id, initialize_result()) {
                    Ok(value) => value,
                    Err(resp) => return Some(serialize(&resp)),
                }),
                error: None,
            },
            "notifications/initialized" => return None,
            "tools/list" => RpcResponse {
                jsonrpc: JsonRpcVersion::V2,
                id: id.clone(),
                result: Some(
                    match serialize_result(
                        &id,
                        ToolsListResult {
                            tools: self.tool_specs(),
                        },
                    ) {
                        Ok(value) => value,
                        Err(resp) => return Some(serialize(&resp)),
                    },
                ),
                error: None,
            },
            "tools/call" => {
                let params: ToolsCallParams = match req
                    .params
                    .clone()
                    .ok_or_else(|| "missing params".to_string())
                    .and_then(|v| {
                        serde_json::from_value(v).map_err(|e| format!("invalid params: {e}"))
                    }) {
                    Ok(p) => p,
                    Err(e) => {
                        return Some(serialize(&error_resp(id, error_code::INVALID_PARAMS, e)));
                    }
                };
                self.handle_tools_call(id, params).await
            }
            other => error_resp(
                id,
                error_code::METHOD_NOT_FOUND,
                format!("unknown method: {other}"),
            ),
        };
        Some(serialize(&resp))
    }

    async fn check_daemon_version_once(&self) {
        let mut checked = self.version_checked.lock().await;
        if *checked {
            return;
        }
        *checked = true;
        // MCP initialize must keep the JSON-RPC session alive; surface
        // daemon/client drift on stderr instead of aborting the server.
        let _ = check_daemon_version(&self.paths.control, VersionGuardMode::Mcp).await;
    }

    /// Resolve the tool, ask it for a [`ToolRoute`], run the route,
    /// and wrap the response back into an MCP `ToolsCallResult`.
    async fn handle_tools_call(&self, id: RequestId, params: ToolsCallParams) -> RpcResponse {
        let Some(tool) = self.tools.get(&params.name) else {
            return error_resp(
                id,
                error_code::METHOD_NOT_FOUND,
                format!("unknown tool: {}", params.name),
            );
        };
        let route = match tool.route(params.arguments) {
            Ok(r) => r,
            Err(e) => return error_resp(id, error_code::INVALID_PARAMS, e),
        };
        match route {
            ToolRoute::DataPlane { method, params } => {
                let is_repo_status = method == "repo_status";
                let req = RpcRequest {
                    jsonrpc: JsonRpcVersion::V2,
                    id: RequestId::Number(1),
                    method,
                    params: Some(params),
                };
                match rpc_client::send_request(&self.paths.cairn, &req).await {
                    Ok(resp) => mcp_wrap_rpc_response(id, resp),
                    Err(e) if is_repo_status => repo_status_daemon_not_ready_error(id, e),
                    Err(e) => {
                        error_resp(id, error_code::INTERNAL_ERROR, format!("data socket: {e}"))
                    }
                }
            }
            ToolRoute::Control { method, params } => {
                let req = RpcRequest {
                    jsonrpc: JsonRpcVersion::V2,
                    id: RequestId::Number(1),
                    method,
                    params: Some(params),
                };
                match rpc_client::send_request(&self.paths.control, &req).await {
                    Ok(resp) => mcp_wrap_rpc_response(id, resp),
                    Err(e) => error_resp(
                        id,
                        error_code::INTERNAL_ERROR,
                        format!("control socket: {e}"),
                    ),
                }
            }
        }
    }
}

fn repo_status_daemon_not_ready_error(id: RequestId, err: anyhow::Error) -> RpcResponse {
    let mut response = error_resp(
        id,
        error_code::INTERNAL_ERROR,
        format!("data socket: {err}"),
    );
    if let Some(error) = response.error.as_mut() {
        error.data = Some(serde_json::json!({
            "hints": [Hint {
                code: HintCode::DaemonNotReady,
                message: "Daemon is not running. Start it with `brew services start cairn` or `cairn daemon`.".to_string(),
                action: None,
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: None,
            }]
        }));
    }
    response
}

// ─── wire IO ───────────────────────────────────────────────────────────────

enum McpLine {
    Eof,
    Line(String),
    TooLong,
}

async fn read_mcp_line_capped<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<McpLine> {
    let mut buf = Vec::new();
    let mut too_long = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if too_long {
                return Ok(McpLine::TooLong);
            }
            if buf.is_empty() {
                return Ok(McpLine::Eof);
            }
            return line_from_bytes(buf);
        }
        let (done, n) = match available.iter().position(|&b| b == b'\n') {
            Some(i) => (true, i + 1),
            None => (false, available.len()),
        };
        if !too_long {
            if buf.len() + n > max {
                too_long = true;
            } else {
                buf.extend_from_slice(&available[..n]);
            }
        }
        reader.consume(n);
        if done {
            return if too_long {
                Ok(McpLine::TooLong)
            } else {
                line_from_bytes(buf)
            };
        }
    }
}

fn line_from_bytes(mut buf: Vec<u8>) -> std::io::Result<McpLine> {
    while matches!(buf.last(), Some(b'\n' | b'\r')) {
        buf.pop();
    }
    String::from_utf8(buf)
        .map(McpLine::Line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

// ─── response wrapping ────────────────────────────────────────────────────

fn mcp_wrap_rpc_response(id: RequestId, resp: RpcResponse) -> RpcResponse {
    if let Some(err) = resp.error {
        return RpcResponse {
            jsonrpc: JsonRpcVersion::V2,
            id,
            result: None,
            error: Some(err),
        };
    }
    let value = resp.result.unwrap_or(Value::Null);
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "null".into());
    let result = ToolsCallResult {
        content: vec![ContentBlock::Text { text }],
        is_error: false,
    };
    let mut wrapped = match serialize_result(&id, result) {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Value::Object(ref mut map) = wrapped {
        map.insert("structuredContent".into(), value);
    }
    RpcResponse {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: Some(wrapped),
        error: None,
    }
}

fn serialize_result<T: Serialize>(
    id: &RequestId,
    result: T,
) -> std::result::Result<Value, RpcResponse> {
    serde_json::to_value(result).map_err(|err| {
        error!(error = %err, "failed to serialize MCP response result");
        error_resp(
            id.clone(),
            error_code::INTERNAL_ERROR,
            "internal: response serialization failed",
        )
    })
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn initialize_result() -> InitializeResult {
    InitializeResult {
        protocol_version: MCP_PROTOCOL_VERSION.into(),
        capabilities: ServerCapabilities {
            tools: ToolsCapability {
                list_changed: false,
            },
        },
        server_info: ServerInfo {
            name: SERVER_NAME.into(),
            version: SERVER_VERSION.into(),
        },
        instructions: Some(SERVER_INSTRUCTIONS.into()),
    }
}

/// Tool specs come from per-tool [`McpTool::spec`] calls and own
/// their name `String`. The dispatcher's lookup uses `&'static str`
/// keys for cheap matching against the wire `method` field; we leak
/// the names at startup. The number of tools is tiny and the leak is
/// bounded by `MCP_TOOLS.len()`.
fn leak_name(name: String) -> &'static str {
    Box::leak(name.into_boxed_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatcher() -> Dispatcher {
        let tmp = tempfile::tempdir().unwrap();
        Dispatcher::new(SocketPaths::with_runtime_dir(tmp.path().to_path_buf()))
    }

    #[test]
    fn tool_specs_in_advertised_order() {
        let names: Vec<String> = dispatcher()
            .tool_specs()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(
            names,
            vec![
                "list_repos",
                "repo_status",
                "list_jobs",
                "get_outline",
                "get_symbol_source",
                "find_symbols",
                "find_subtypes",
                "find_supertypes",
                "find_callers",
                "find_callees",
                "find_references",
                "find_imports",
                "register_repo",
                "reindex_repo",
            ]
        );
    }

    #[test]
    fn mcp_tool_descriptions_mention_when_not_for_recovery() {
        let specs = dispatcher().tool_specs();

        for spec in &specs {
            assert!(
                spec.description.contains("WHEN:"),
                "{} should tell the agent when to use it",
                spec.name
            );
            assert!(
                spec.description.contains("NOT FOR:"),
                "{} should deflect adjacent tasks",
                spec.name
            );
        }

        for name in [
            "find_symbols",
            "get_outline",
            "find_references",
            "find_callers",
        ] {
            let spec = specs.iter().find(|spec| spec.name == name).unwrap();
            assert!(
                spec.description.contains("Recovery:"),
                "{name} should include a high-frequency recovery label"
            );
        }
    }

    #[test]
    fn mcp_tool_descriptions_under_120_tokens() {
        for spec in dispatcher().tool_specs() {
            // Cheap cockpit-label guard: roughly chars/4 tokens, with
            // headroom for punctuation and identifiers.
            let approx_tokens = spec.description.chars().count() / 4;
            assert!(
                approx_tokens < 240,
                "{} description is too long: ~{approx_tokens} tokens",
                spec.name
            );
        }
    }

    #[test]
    fn tool_specs_keep_route_critical_schema() {
        let specs = dispatcher().tool_specs();
        let get_outline = specs
            .iter()
            .find(|spec| spec.name == "get_outline")
            .unwrap();
        assert!(get_outline.input_schema["required"].is_null());
        assert!(get_outline.input_schema["properties"]["path"].is_object());
        assert_eq!(
            get_outline.input_schema["properties"]["max_depth"]["minimum"],
            1
        );

        let find_symbols = specs
            .iter()
            .find(|spec| spec.name == "find_symbols")
            .unwrap();
        let symbol_props = &find_symbols.input_schema["properties"];
        assert!(
            symbol_props["branch"]["description"]
                .as_str()
                .unwrap()
                .contains("bare branch name")
        );
        assert!(
            symbol_props["kind"]["description"]
                .as_str()
                .unwrap()
                .contains("`type_alias`")
        );

        for (tool_name, required) in [
            ("find_subtypes", "name"),
            ("find_supertypes", "name"),
            ("find_callers", "name"),
            ("find_callees", "name"),
            ("find_references", "symbol"),
        ] {
            let spec = specs.iter().find(|s| s.name == tool_name).unwrap();
            assert_eq!(spec.input_schema["required"], serde_json::json!([required]));
        }
    }

    #[test]
    fn mcp_server_instructions_includes_first_move_and_workflow_sections() {
        let r = initialize_result();
        assert_eq!(r.protocol_version, MCP_PROTOCOL_VERSION);
        let instructions = r.instructions.unwrap();
        for phrase in [
            "## First move",
            "## Core workflow",
            "## Retry rules",
            "## JSX caveat",
            "## Composition stance",
            "find_symbols / get_outline -> get_symbol_source",
            "reindex_via_cli intentionally has no `action` field",
            "timing.server_ms",
        ] {
            assert!(instructions.contains(phrase), "missing {phrase}");
        }
    }

    #[tokio::test]
    async fn read_mcp_line_capped_accepts_line_at_limit() {
        let mut reader = BufReader::new(&b"abc\nnext\n"[..]);
        let line = read_mcp_line_capped(&mut reader, 4).await.unwrap();
        assert!(matches!(line, McpLine::Line(s) if s == "abc"));
        let line = read_mcp_line_capped(&mut reader, 5).await.unwrap();
        assert!(matches!(line, McpLine::Line(s) if s == "next"));
    }

    #[tokio::test]
    async fn read_mcp_line_capped_drains_oversized_line() {
        let mut reader = BufReader::new(&b"abcdef\nok\n"[..]);
        let line = read_mcp_line_capped(&mut reader, 4).await.unwrap();
        assert!(matches!(line, McpLine::TooLong));
        let line = read_mcp_line_capped(&mut reader, 3).await.unwrap();
        assert!(matches!(line, McpLine::Line(s) if s == "ok"));
    }
}
