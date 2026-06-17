//! `register_repo` MCP tool — admin: routes to control.sock.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "register_repo".into(),
        description: "Register a repo path under an alias so cairn can index it.\n\nWHEN: list_repos does not include the repo you need, or repo_status by path cannot resolve it.\nNOT FOR: Refreshing an already registered repo after edits; the watcher handles that. Use reindex_repo only for suspected missed changes.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "path":  {"type": "string", "description": "Absolute path to the repo root."},
                "alias": {"type": "string", "description": "Short identifier used in subsequent queries (e.g. the repo name)."},
            },
            "required": ["path", "alias"],
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::control(spec, "register_repo", 90));
