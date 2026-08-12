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

use std::collections::{HashMap, VecDeque};
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
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tracing::error;

use super::rpc_client;
use super::version_guard::{VersionGuardMode, check_daemon_version};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "cairn";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Hard cap on a single stdin line. Anything longer is drained and
/// answered with an INVALID_REQUEST so a runaway client cannot push
/// the front-end into unbounded buffering.
const MCP_MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
const MCP_REQUEST_QUEUE_CAPACITY: usize = 64;
/// Total serialized payload retained behind one active request. Count and byte
/// caps are independent so 64 maximum-sized MCP lines cannot pin gigabytes.
const MCP_REQUEST_QUEUE_BYTES: usize = 64 * 1024 * 1024;

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
// ─── run loop ──────────────────────────────────────────────────────────────

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Override the runtime directory (otherwise picked from
    /// $XDG_RUNTIME_DIR / ~/Library/Caches).
    #[arg(long)]
    pub runtime_dir: Option<PathBuf>,
}

/// Entry point for `cairn mcp`. Runs one MCP session on stdin/stdout
/// until EOF, dispatching every well-formed line through
/// [`Dispatcher::handle_line`] and writing at most one framed
/// response per input line. Notifications (missing `id`) and blank
/// lines produce no reply; oversized lines answer with
/// `INVALID_REQUEST` and continue the session.
pub async fn run(args: Args) -> Result<()> {
    let paths = match args.runtime_dir {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };
    let dispatcher = Dispatcher::new(paths);

    let stdin = tokio::io::stdin();
    run_mcp_session(
        BufReader::new(stdin),
        tokio::io::stdout(),
        dispatcher,
        SessionLimits::PRODUCTION,
    )
    .await
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
    /// Materialise the tool registry from [`MCP_TOOLS`] and remember
    /// the resolved socket paths. Tools are sorted by
    /// `(sort_key, name)` so `tools/list` is deterministic and the
    /// most-useful entries land at the top.
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

    /// Rebuild the ordered [`ToolSpec`] list served by `tools/list`.
    /// Called on every request rather than cached because tool count
    /// is tiny and each `spec()` call is cheap.
    fn tool_specs(&self) -> Vec<ToolSpec> {
        self.ordered
            .iter()
            .filter_map(|name| self.tools.get(*name).map(|t| t.spec()))
            .collect()
    }

    /// Dispatch one MCP line. Returns `None` for notifications (no
    /// id, no reply expected).
    ///
    /// Envelope handling differs from the daemon's JSON-RPC dispatch
    /// (`cairn_core::jsonrpc_dispatch`): the MCP surface silently
    /// drops any input that lacks an id, including well-formed
    /// notifications and inputs whose raw JSON also fails to parse.
    /// See the module doc on `cairn_proto::jsonrpc` for the
    /// daemon-side envelope contract.
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

    /// Run the daemon/client version compatibility check at most
    /// once per session. Failures are logged inside
    /// [`check_daemon_version`]; this helper does not propagate
    /// them so a version drift never aborts an MCP session that
    /// the client is otherwise capable of driving.
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

/// Attach a `DaemonNotReady` hint to the socket-error response for
/// `repo_status` calls.
///
/// `repo_status` is the natural tool an MCP client reaches for when
/// the daemon may be down, so a plain "data socket: connection
/// refused" message would be diagnosable but not actionable. Adding
/// the hint gives the client enough structure to surface a
/// start-the-daemon suggestion. Other tools produce the same
/// unhinted INTERNAL_ERROR shape via their own envelope for the
/// same socket error class.
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

/// Outcome of one line-read from stdin. `TooLong` signals that the
/// oversize line was already drained so the next call resumes on
/// the following line.
enum McpLine {
    Eof,
    Line(String),
    TooLong,
}

/// Read one line, completed by newline or EOF, up to `max` bytes.
/// An EOF-terminated non-empty line is returned as
/// [`McpLine::Line`] just like a newline-terminated one; only a
/// zero-length read at line start becomes [`McpLine::Eof`]. Bytes
/// past the cap are consumed and discarded so the stream stays
/// framed; the caller sees [`McpLine::TooLong`] instead of
/// receiving a partial line. Trailing `\n`/`\r` are stripped from
/// the returned string.
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

/// Adapt a daemon JSON-RPC response into an MCP `tools/call` reply.
///
/// Errors are forwarded verbatim (code, message, and any structured
/// `data` such as initialization hints) so callers see the same
/// diagnostics they would over a direct socket. Successful results
/// are wrapped in a `ToolsCallResult` with the JSON payload
/// stringified into a single `text` content block and also duplicated
/// into `structuredContent` — MCP clients that understand structured
/// results can consume the object directly, while text-only clients
/// still see the payload.
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

/// Serialize an MCP result payload into a JSON `Value`, or turn a
/// serialization failure into an INTERNAL_ERROR response the caller
/// can return directly. Logs the underlying error before demoting
/// it so operators can trace which method produced the malformed
/// payload.
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

/// Build the `initialize` response returned to every client. The
/// protocol version is a hard-coded constant — cairn does not
/// negotiate with the client's advertised version and simply echoes
/// what it supports today.
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
        // MCP serverInstructions retired in v0.7.0; the same guidance now
        // ships as `plugin/SERVER_INSTRUCTIONS.md` and is injected via the
        // plugin's `SessionStart` hook so it survives Claude Code's
        // serverInstructions size cap and reaches Codex hosts that ignore
        // the field. Keep `None` here to avoid two copies drifting.
        instructions: None,
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

#[derive(Clone)]
struct ActiveRequest {
    id: RequestId,
    method: String,
}

enum PendingItem {
    Dispatch {
        line: String,
        identity: Option<ActiveRequest>,
    },
    ReadyReply(RpcResponse),
}

#[derive(Clone, Copy)]
struct SessionLimits {
    queued_items: usize,
    queued_bytes: usize,
}

impl SessionLimits {
    const PRODUCTION: Self = Self {
        queued_items: MCP_REQUEST_QUEUE_CAPACITY,
        queued_bytes: MCP_REQUEST_QUEUE_BYTES,
    };
}

impl PendingItem {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Dispatch { line, .. } => line.capacity(),
            Self::ReadyReply(response) => serialize(response).len() + 1,
        }
    }
}

enum ReaderEvent {
    Line(McpLine),
    Error(std::io::Error),
}

struct ReaderPumpGuard(tokio::task::JoinHandle<()>);

impl Drop for ReaderPumpGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn pump_mcp_reader<R>(mut reader: R, sender: tokio::sync::mpsc::Sender<ReaderEvent>)
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let event = match read_mcp_line_capped(&mut reader, MCP_MAX_LINE_BYTES).await {
            Ok(line) => ReaderEvent::Line(line),
            Err(error) => ReaderEvent::Error(error),
        };
        let terminal = matches!(
            event,
            ReaderEvent::Line(McpLine::Eof) | ReaderEvent::Error(_)
        );
        if sender.send(event).await.is_err() || terminal {
            return;
        }
    }
}

async fn next_reader_line(
    receiver: &mut tokio::sync::mpsc::Receiver<ReaderEvent>,
) -> Result<McpLine> {
    match receiver.recv().await {
        Some(ReaderEvent::Line(line)) => Ok(line),
        Some(ReaderEvent::Error(error)) => Err(error.into()),
        None => Ok(McpLine::Eof),
    }
}

fn request_identity(line: &str) -> Option<ActiveRequest> {
    let request = serde_json::from_str::<RpcRequest>(line).ok()?;
    Some(ActiveRequest {
        id: request.id,
        method: request.method,
    })
}

fn cancellation_target(line: &str) -> Option<RequestId> {
    let notification = serde_json::from_str::<Value>(line).ok()?;
    (notification.get("jsonrpc")?.as_str()? == "2.0").then_some(())?;
    (!notification.as_object()?.contains_key("id")).then_some(())?;
    (notification.get("method")?.as_str()? == "notifications/cancelled").then_some(())?;
    let params = notification.get("params")?.as_object()?;
    if params
        .get("reason")
        .is_some_and(|reason| !reason.is_string())
    {
        return None;
    }
    serde_json::from_value(params.get("requestId")?.clone()).ok()
}

fn cancellation_matches(active: &ActiveRequest, target: &RequestId) -> bool {
    active.method != "initialize" && active.id == *target
}

fn pending_has_id(pending: &VecDeque<PendingItem>, id: &RequestId) -> bool {
    pending.iter().any(|item| {
        matches!(
            item,
            PendingItem::Dispatch {
                identity: Some(identity),
                ..
            } if identity.id == *id
        )
    })
}

fn cancel_pending(pending: &mut VecDeque<PendingItem>, target: &RequestId) -> usize {
    let before = pending
        .iter()
        .map(PendingItem::retained_bytes)
        .sum::<usize>();
    pending.retain(|item| {
        !matches!(
            item,
            PendingItem::Dispatch {
                identity: Some(identity),
                ..
            } if cancellation_matches(identity, target)
        )
    });
    before.saturating_sub(pending.iter().map(PendingItem::retained_bytes).sum())
}

async fn write_mcp_response<W>(stdout: &mut W, response: &RpcResponse) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    stdout.write_all(serialize(response).as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

async fn run_mcp_session<R, W>(
    reader: R,
    mut stdout: W,
    dispatcher: Dispatcher,
    limits: SessionLimits,
) -> Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    // One line of read-ahead is enough to surface EOF/cancellation while the
    // executor owns the only stdout writer and the bounded FIFO.
    let (reader_sender, mut reader_receiver) = tokio::sync::mpsc::channel(1);
    let _reader_pump = ReaderPumpGuard(tokio::spawn(pump_mcp_reader(reader, reader_sender)));
    let mut pending = VecDeque::new();
    let mut pending_bytes = 0_usize;
    loop {
        let (line, active) = match pending.pop_front() {
            Some(PendingItem::ReadyReply(response)) => {
                pending_bytes = pending_bytes.saturating_sub(serialize(&response).len() + 1);
                write_mcp_response(&mut stdout, &response).await?;
                continue;
            }
            Some(PendingItem::Dispatch { line, identity }) => {
                pending_bytes = pending_bytes.saturating_sub(line.capacity());
                (line, identity)
            }
            None => match next_reader_line(&mut reader_receiver).await? {
                McpLine::Eof => return Ok(()),
                McpLine::TooLong => {
                    write_mcp_response(
                        &mut stdout,
                        &error_resp(
                            RequestId::Null,
                            error_code::INVALID_REQUEST,
                            format!("JSON-RPC line exceeds {MCP_MAX_LINE_BYTES} bytes"),
                        ),
                    )
                    .await?;
                    continue;
                }
                McpLine::Line(line) if line.trim().is_empty() => continue,
                McpLine::Line(line) => {
                    if cancellation_target(&line).is_some() {
                        continue;
                    }
                    let identity = request_identity(&line);
                    (line, identity)
                }
            },
        };

        let dispatch = dispatcher.handle_line(&line);
        tokio::pin!(dispatch);
        let reply = loop {
            tokio::select! {
                biased;
                reply = &mut dispatch => break reply,
                next = next_reader_line(&mut reader_receiver) => {
                    let queued = match next? {
                        McpLine::Eof => return Ok(()),
                        McpLine::TooLong => PendingItem::ReadyReply(error_resp(
                            RequestId::Null,
                            error_code::INVALID_REQUEST,
                            format!("JSON-RPC line exceeds {MCP_MAX_LINE_BYTES} bytes"),
                        )),
                        McpLine::Line(next_line) if next_line.trim().is_empty() => continue,
                        McpLine::Line(next_line) => {
                            if let Some(target) = cancellation_target(&next_line) {
                                let removed = cancel_pending(&mut pending, &target);
                                pending_bytes = pending_bytes.saturating_sub(removed);
                                if active.as_ref().is_some_and(|request| {
                                    cancellation_matches(request, &target)
                                }) {
                                    break None;
                                }
                                continue;
                            }
                            let identity = request_identity(&next_line);
                            if let Some(next) = &identity
                                && (active.as_ref().is_some_and(|request| request.id == next.id)
                                    || pending_has_id(&pending, &next.id))
                            {
                                PendingItem::ReadyReply(error_resp(
                                    next.id.clone(),
                                    error_code::INVALID_REQUEST,
                                    "duplicate in-flight or queued request id",
                                ))
                            } else {
                                PendingItem::Dispatch {
                                    line: next_line,
                                    identity,
                                }
                            }
                        }
                    };
                    let queued_bytes = queued.retained_bytes();
                    if pending.len() >= limits.queued_items
                        || pending_bytes.saturating_add(queued_bytes) > limits.queued_bytes
                    {
                        let id = match &queued {
                            PendingItem::Dispatch {
                                identity: Some(identity),
                                ..
                            } => Some(identity.id.clone()),
                            PendingItem::Dispatch { identity: None, .. } => None,
                            PendingItem::ReadyReply(response) => Some(response.id.clone()),
                        };
                        if let Some(id) = id {
                            let response = error_resp(
                                id,
                                error_code::INTERNAL_ERROR,
                                "MCP request queue capacity exceeded; session terminated",
                            );
                            let _ = write_mcp_response(&mut stdout, &response).await;
                        }
                        return Ok(());
                    }
                    pending_bytes += queued_bytes;
                    pending.push_back(queued);
                }
            }
        };
        if let Some(reply) = reply {
            stdout.write_all(reply.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_proto::jsonrpc::ok_response;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixListener;
    use tokio::sync::Notify;

    const TEST_SESSION_LIMITS: SessionLimits = SessionLimits {
        queued_items: 2,
        queued_bytes: 1024,
    };

    async fn test_dispatcher(paths: SocketPaths) -> Dispatcher {
        let dispatcher = Dispatcher::new(paths);
        *dispatcher.version_checked.lock().await = true;
        dispatcher
    }

    fn list_repos_call(id: i64) -> String {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": "list_repos", "arguments": {}}
        })
        .to_string()
    }

    fn padded_list_repos_call(id: i64, padding_bytes: usize) -> String {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": "list_repos",
                "arguments": {"padding": "x".repeat(padding_bytes)}
            }
        })
        .to_string()
    }

    fn cancellation_notification(id: i64) -> String {
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": {"requestId": id, "reason": "caller cancelled"}
        })
        .to_string()
    }

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
    fn initializing_error_data_is_forwarded_without_rewriting() {
        let data = serde_json::json!({
            "initialization": {
                "state": "initializing",
                "phase": "watcher_barrier",
                "completed_phases": 4,
                "total_phases": 7,
                "detail": "arming_registered_watchers"
            },
            "hints": [{"code": "daemon_not_ready", "message": "Retry later."}]
        });
        let response = RpcResponse {
            jsonrpc: JsonRpcVersion::V2,
            id: RequestId::Number(1),
            result: None,
            error: Some(cairn_proto::jsonrpc::ResponseError {
                code: error_code::DAEMON_INITIALIZING,
                message: "daemon is initializing".into(),
                data: Some(data.clone()),
            }),
        };

        let forwarded = mcp_wrap_rpc_response(RequestId::String("mcp".into()), response);
        let error = forwarded.error.unwrap();
        assert_eq!(error.code, error_code::DAEMON_INITIALIZING);
        assert_eq!(error.data, Some(data));
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

    /// MCP serverInstructions are intentionally `None` from v0.7.0
    /// onward; the equivalent guidance is shipped as
    /// `plugin/SERVER_INSTRUCTIONS.md` and injected via the plugin's
    /// `SessionStart` hook. This pins the omission so a future
    /// well-meaning revert that re-adds a `Some(...)` here trips the
    /// test instead of silently re-introducing the two-copy drift.
    #[test]
    fn mcp_serverinstructions_omitted_in_favor_of_plugin_session_hook() {
        let r = initialize_result();
        assert!(
            r.instructions.is_none(),
            "MCP serverInstructions should be None — guidance now ships via the plugin SessionStart hook"
        );
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

    #[tokio::test]
    async fn stdin_eof_drops_inflight_daemon_request_without_stdout_reply() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let daemon_entered = Arc::new(Notify::new());
        let daemon_eof = Arc::new(Notify::new());
        let entered_wait = daemon_entered.notified();
        let eof_wait = daemon_eof.notified();
        let daemon_task = tokio::spawn({
            let daemon_entered = daemon_entered.clone();
            let daemon_eof = daemon_eof.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                reader.read_line(&mut request).await.unwrap();
                let request: RpcRequest = serde_json::from_str(request.trim()).unwrap();
                assert_eq!(request.id, RequestId::Number(1));
                assert_eq!(request.method, "list_repos");
                daemon_entered.notify_waiters();
                let mut remainder = Vec::new();
                reader.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.is_empty());
                daemon_eof.notify_waiters();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, mut output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        let request = json!({
            "jsonrpc": "2.0",
            "id": 73,
            "method": "tools/call",
            "params": {"name": "list_repos", "arguments": {}}
        });
        input
            .write_all(request.to_string().as_bytes())
            .await
            .unwrap();
        input.write_all(b"\n").await.unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("daemon request did not enter");

        drop(input);

        tokio::time::timeout(Duration::from_secs(1), eof_wait)
            .await
            .expect("MCP stdin EOF did not close the daemon request socket");
        tokio::time::timeout(Duration::from_secs(1), session_task)
            .await
            .expect("MCP session did not terminate after stdin EOF")
            .expect("MCP session task panicked")
            .expect("MCP session failed");
        daemon_task.await.unwrap();
        let mut stdout = Vec::new();
        output.read_to_end(&mut stdout).await.unwrap();
        assert!(stdout.is_empty(), "cancelled request wrote an MCP response");
    }

    #[tokio::test]
    async fn pipelined_mcp_requests_keep_ids_order_and_single_stdout_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let daemon_task = tokio::spawn(async move {
            for sequence in 1..=2 {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut request = String::new();
                BufReader::new(read).read_line(&mut request).await.unwrap();
                let request: RpcRequest = serde_json::from_str(request.trim()).unwrap();
                assert_eq!(request.id, RequestId::Number(1));
                assert_eq!(request.method, "list_repos");
                let response = ok_response(RequestId::Number(1), json!({"sequence": sequence}));
                write
                    .write_all(format!("{}\n", serialize(&response)).as_bytes())
                    .await
                    .unwrap();
                write.flush().await.unwrap();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        for id in [73, 74] {
            let request = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": "list_repos", "arguments": {}}
            });
            input
                .write_all(request.to_string().as_bytes())
                .await
                .unwrap();
            input.write_all(b"\n").await.unwrap();
        }
        input.flush().await.unwrap();

        let mut output = BufReader::new(output);
        for expected_id in [73, 74] {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
                .await
                .expect("pipelined MCP response timed out")
                .unwrap();
            let response: RpcResponse = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(response.id, RequestId::Number(expected_id));
        }
        daemon_task.await.unwrap();
        drop(input);
        tokio::time::timeout(Duration::from_secs(1), session_task)
            .await
            .expect("MCP session did not terminate")
            .expect("MCP session task panicked")
            .expect("MCP session failed");
        let mut trailing = String::new();
        assert_eq!(output.read_line(&mut trailing).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn matching_cancel_drops_active_request_then_drains_queued_fifo() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let active_entered = Arc::new(Notify::new());
        let active_eof = Arc::new(Notify::new());
        let entered_wait = active_entered.notified();
        let eof_wait = active_eof.notified();
        let daemon_task = tokio::spawn({
            let active_entered = active_entered.clone();
            let active_eof = active_eof.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                reader.read_line(&mut request).await.unwrap();
                active_entered.notify_waiters();
                let mut remainder = Vec::new();
                reader.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.is_empty());
                active_eof.notify_waiters();

                for sequence in 1..=2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (read, mut write) = stream.into_split();
                    let mut request = String::new();
                    BufReader::new(read).read_line(&mut request).await.unwrap();
                    let request: RpcRequest = serde_json::from_str(request.trim()).unwrap();
                    assert_eq!(request.id, RequestId::Number(1));
                    let response = ok_response(RequestId::Number(1), json!({"sequence": sequence}));
                    write
                        .write_all(format!("{}\n", serialize(&response)).as_bytes())
                        .await
                        .unwrap();
                    write.flush().await.unwrap();
                }
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        input
            .write_all(list_repos_call(73).as_bytes())
            .await
            .unwrap();
        input.write_all(b"\n").await.unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active daemon request did not enter");
        for id in [74, 75] {
            input
                .write_all(list_repos_call(id).as_bytes())
                .await
                .unwrap();
            input.write_all(b"\n").await.unwrap();
        }
        input
            .write_all(cancellation_notification(73).as_bytes())
            .await
            .unwrap();
        input.write_all(b"\n").await.unwrap();
        input.flush().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), eof_wait)
            .await
            .expect("matching cancellation did not close the active daemon socket");
        let mut output = BufReader::new(output);
        for expected_id in [74, 75] {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
                .await
                .expect("queued MCP response timed out")
                .unwrap();
            let response: RpcResponse = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(response.id, RequestId::Number(expected_id));
        }
        daemon_task.await.unwrap();
        drop(input);
        session_task.await.unwrap().unwrap();
        let mut trailing = String::new();
        assert_eq!(output.read_line(&mut trailing).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn nonmatching_cancel_keeps_active_request_alive() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut request = String::new();
                BufReader::new(read).read_line(&mut request).await.unwrap();
                entered.notify_waiters();
                release.notified().await;
                let response = ok_response(RequestId::Number(1), json!({"kept": true}));
                write
                    .write_all(format!("{}\n", serialize(&response)).as_bytes())
                    .await
                    .unwrap();
                write.flush().await.unwrap();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            SessionLimits {
                queued_items: 4,
                queued_bytes: 4096,
            },
        ));
        input
            .write_all(list_repos_call(73).as_bytes())
            .await
            .unwrap();
        input.write_all(b"\n").await.unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active daemon request did not enter");
        input
            .write_all(cancellation_notification(999).as_bytes())
            .await
            .unwrap();
        input.write_all(b"\n").await.unwrap();
        for invalid in [
            json!({"jsonrpc":"2.0","id":99,"method":"notifications/cancelled","params":{"requestId":73}}),
            json!({"method":"notifications/cancelled","params":{"requestId":73}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":73,"reason":1}}),
        ] {
            assert!(cancellation_target(&invalid.to_string()).is_none());
            input
                .write_all(format!("{invalid}\n").as_bytes())
                .await
                .unwrap();
        }
        input.flush().await.unwrap();
        release.notify_waiters();

        let mut output = BufReader::new(output);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
            .await
            .expect("active response timed out after nonmatching cancel")
            .unwrap();
        let response: RpcResponse = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response.id, RequestId::Number(73));
        daemon_task.await.unwrap();
        drop(input);
        session_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn matching_cancel_removes_only_the_queued_request() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                for sequence in 1..=2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (read, mut write) = stream.into_split();
                    let mut request = String::new();
                    BufReader::new(read).read_line(&mut request).await.unwrap();
                    if sequence == 1 {
                        entered.notify_waiters();
                        release.notified().await;
                    }
                    let response = ok_response(RequestId::Number(1), json!({"sequence": sequence}));
                    write
                        .write_all(format!("{}\n", serialize(&response)).as_bytes())
                        .await
                        .unwrap();
                    write.flush().await.unwrap();
                }
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        input
            .write_all(format!("{}\n", list_repos_call(73)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active daemon request did not enter");
        for id in [74, 75] {
            input
                .write_all(format!("{}\n", list_repos_call(id)).as_bytes())
                .await
                .unwrap();
        }
        input
            .write_all(format!("{}\n", cancellation_notification(74)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        release.notify_waiters();

        let mut output = BufReader::new(output);
        for expected_id in [73, 75] {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
                .await
                .expect("response after queued cancellation timed out")
                .unwrap();
            let response: RpcResponse = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(response.id, RequestId::Number(expected_id));
        }
        daemon_task.await.unwrap();
        drop(input);
        session_task.await.unwrap().unwrap();
        let mut trailing = String::new();
        assert_eq!(output.read_line(&mut trailing).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn duplicate_pending_id_keeps_synthetic_error_in_fifo_position() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                for sequence in 1..=2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (read, mut write) = stream.into_split();
                    let mut request = String::new();
                    BufReader::new(read).read_line(&mut request).await.unwrap();
                    if sequence == 1 {
                        entered.notify_waiters();
                        release.notified().await;
                    }
                    let response = ok_response(RequestId::Number(1), json!({"sequence": sequence}));
                    write
                        .write_all(format!("{}\n", serialize(&response)).as_bytes())
                        .await
                        .unwrap();
                    write.flush().await.unwrap();
                }
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        input
            .write_all(format!("{}\n", list_repos_call(73)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active daemon request did not enter");
        for id in [74, 74] {
            input
                .write_all(format!("{}\n", list_repos_call(id)).as_bytes())
                .await
                .unwrap();
        }
        input.flush().await.unwrap();
        release.notify_waiters();

        let mut output = BufReader::new(output);
        for expected_id in [73, 74] {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
                .await
                .expect("normal response before duplicate error timed out")
                .unwrap();
            let response: Value = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(response["id"], expected_id);
            assert!(response.get("result").is_some());
        }
        let mut duplicate = String::new();
        tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut duplicate))
            .await
            .expect("queued duplicate-id error timed out")
            .unwrap();
        let duplicate: Value = serde_json::from_str(duplicate.trim()).unwrap();
        assert_eq!(duplicate["id"], 74);
        assert_eq!(duplicate["error"]["code"], error_code::INVALID_REQUEST);

        daemon_task.await.unwrap();
        drop(input);
        session_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn queue_overflow_writes_one_error_then_terminates_the_session() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let active_eof = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let eof_wait = active_eof.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let active_eof = active_eof.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                reader.read_line(&mut request).await.unwrap();
                entered.notify_waiters();
                let mut remainder = Vec::new();
                reader.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.is_empty());
                active_eof.notify_waiters();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        input
            .write_all(format!("{}\n", list_repos_call(73)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active daemon request did not enter");
        for id in [74, 75, 76] {
            input
                .write_all(format!("{}\n", list_repos_call(id)).as_bytes())
                .await
                .unwrap();
        }
        input.flush().await.unwrap();

        let mut output = BufReader::new(output);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
            .await
            .expect("overflow error timed out")
            .unwrap();
        let response: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response["id"], 76);
        assert_eq!(response["error"]["code"], error_code::INTERNAL_ERROR);
        tokio::time::timeout(Duration::from_secs(1), eof_wait)
            .await
            .expect("overflow did not drop the active daemon request");
        tokio::time::timeout(Duration::from_secs(1), session_task)
            .await
            .expect("overflow did not terminate the MCP session")
            .expect("MCP session task panicked")
            .expect("MCP session failed");
        daemon_task.await.unwrap();
        let mut trailing = String::new();
        assert_eq!(output.read_line(&mut trailing).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn production_limits_preserve_seventeen_small_requests_in_fifo_order() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let daemon_task = tokio::spawn(async move {
            for sequence in 1..=17 {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut request = String::new();
                BufReader::new(read).read_line(&mut request).await.unwrap();
                let response = ok_response(RequestId::Number(1), json!({"sequence": sequence}));
                write
                    .write_all(format!("{}\n", serialize(&response)).as_bytes())
                    .await
                    .unwrap();
                write.flush().await.unwrap();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(64 * 1024);
        let (session_output, output) = tokio::io::duplex(64 * 1024);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            SessionLimits::PRODUCTION,
        ));
        for id in 1..=17 {
            input
                .write_all(format!("{}\n", list_repos_call(id)).as_bytes())
                .await
                .unwrap();
        }
        input.flush().await.unwrap();

        let mut output = BufReader::new(output);
        for expected_id in 1..=17 {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
                .await
                .expect("production-limit response timed out")
                .unwrap();
            let response: RpcResponse = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(response.id, RequestId::Number(expected_id));
        }
        daemon_task.await.unwrap();
        drop(input);
        session_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancelling_large_pending_request_releases_its_byte_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                for sequence in 1..=2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (read, mut write) = stream.into_split();
                    let mut request = String::new();
                    BufReader::new(read).read_line(&mut request).await.unwrap();
                    if sequence == 1 {
                        entered.notify_waiters();
                        release.notified().await;
                    }
                    let response = ok_response(RequestId::Number(1), json!({"sequence": sequence}));
                    write
                        .write_all(format!("{}\n", serialize(&response)).as_bytes())
                        .await
                        .unwrap();
                    write.flush().await.unwrap();
                }
            }
        });

        let (mut input, session_input) = tokio::io::duplex(32 * 1024);
        let (session_output, output) = tokio::io::duplex(16 * 1024);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            SessionLimits {
                queued_items: 4,
                queued_bytes: 5000,
            },
        ));
        input
            .write_all(format!("{}\n", list_repos_call(73)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active request did not enter");
        input
            .write_all(format!("{}\n", padded_list_repos_call(74, 3000)).as_bytes())
            .await
            .unwrap();
        input
            .write_all(format!("{}\n", cancellation_notification(74)).as_bytes())
            .await
            .unwrap();
        input
            .write_all(format!("{}\n", padded_list_repos_call(75, 3000)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        release.notify_waiters();

        let mut output = BufReader::new(output);
        for expected_id in [73, 75] {
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
                .await
                .expect("response after pending-byte cancellation timed out")
                .unwrap();
            let response: RpcResponse = serde_json::from_str(line.trim()).unwrap();
            assert_eq!(response.id, RequestId::Number(expected_id));
        }
        daemon_task.await.unwrap();
        drop(input);
        session_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn queued_byte_budget_overflow_writes_one_error_and_terminates() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let active_eof = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let eof_wait = active_eof.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let active_eof = active_eof.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                reader.read_line(&mut request).await.unwrap();
                entered.notify_waiters();
                let mut remainder = Vec::new();
                reader.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.is_empty());
                active_eof.notify_waiters();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(32 * 1024);
        let (session_output, output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            SessionLimits {
                queued_items: 4,
                queued_bytes: 5000,
            },
        ));
        input
            .write_all(format!("{}\n", list_repos_call(73)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active request did not enter");
        for id in [74, 75] {
            input
                .write_all(format!("{}\n", padded_list_repos_call(id, 3000)).as_bytes())
                .await
                .unwrap();
        }
        input.flush().await.unwrap();

        let mut output = BufReader::new(output);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(1), output.read_line(&mut line))
            .await
            .expect("byte-budget overflow response timed out")
            .unwrap();
        let response: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(response["id"], 75);
        assert_eq!(response["error"]["code"], error_code::INTERNAL_ERROR);
        tokio::time::timeout(Duration::from_secs(1), eof_wait)
            .await
            .expect("byte-budget overflow did not drop active daemon request");
        tokio::time::timeout(Duration::from_secs(1), session_task)
            .await
            .expect("byte-budget overflow did not terminate session")
            .unwrap()
            .unwrap();
        daemon_task.await.unwrap();
    }

    #[tokio::test]
    async fn reader_pump_reaches_terminal_after_eof() {
        let (input, reader) = tokio::io::duplex(64);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let pump = tokio::spawn(pump_mcp_reader(BufReader::new(reader), sender));
        drop(input);
        assert!(matches!(
            receiver.recv().await,
            Some(ReaderEvent::Line(McpLine::Eof))
        ));
        tokio::time::timeout(Duration::from_secs(1), pump)
            .await
            .expect("reader pump did not terminate after EOF")
            .expect("reader pump panicked");
    }

    #[tokio::test]
    async fn notification_overflow_terminates_without_writing_a_response() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = SocketPaths::with_runtime_dir(tmp.path().to_path_buf());
        let listener = UnixListener::bind(&paths.cairn).unwrap();
        let entered = Arc::new(Notify::new());
        let active_eof = Arc::new(Notify::new());
        let entered_wait = entered.notified();
        let eof_wait = active_eof.notified();
        let daemon_task = tokio::spawn({
            let entered = entered.clone();
            let active_eof = active_eof.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                reader.read_line(&mut request).await.unwrap();
                entered.notify_waiters();
                let mut remainder = Vec::new();
                reader.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.is_empty());
                active_eof.notify_waiters();
            }
        });

        let (mut input, session_input) = tokio::io::duplex(4096);
        let (session_output, mut output) = tokio::io::duplex(4096);
        let session_task = tokio::spawn(run_mcp_session(
            BufReader::new(session_input),
            session_output,
            test_dispatcher(paths).await,
            TEST_SESSION_LIMITS,
        ));
        input
            .write_all(format!("{}\n", list_repos_call(73)).as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered_wait)
            .await
            .expect("active daemon request did not enter");
        for id in [74, 75] {
            input
                .write_all(format!("{}\n", list_repos_call(id)).as_bytes())
                .await
                .unwrap();
        }
        input
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        input.flush().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), eof_wait)
            .await
            .expect("notification overflow did not drop the daemon request");
        tokio::time::timeout(Duration::from_secs(1), session_task)
            .await
            .expect("notification overflow did not terminate the MCP session")
            .expect("MCP session task panicked")
            .expect("MCP session failed");
        daemon_task.await.unwrap();
        let mut stdout = Vec::new();
        output.read_to_end(&mut stdout).await.unwrap();
        assert!(stdout.is_empty(), "notification overflow wrote a response");
    }

    #[test]
    fn initialize_request_is_not_cancelled_by_notification() {
        let active = ActiveRequest {
            id: RequestId::Number(73),
            method: "initialize".into(),
        };
        assert!(!cancellation_matches(&active, &RequestId::Number(73)));
        assert!(
            cancellation_target(
                &json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"wrong":73}})
                    .to_string()
            )
            .is_none()
        );
        assert!(
            cancellation_target(
                &json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":73,"reason":1}})
                    .to_string()
            )
            .is_none()
        );
    }
}
