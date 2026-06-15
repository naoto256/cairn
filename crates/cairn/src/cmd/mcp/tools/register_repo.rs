//! `register_repo` MCP tool — admin: routes to control.sock.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "register_repo".into(),
        description: "Add a repository to cairn's index so `get_outline` and `find_symbols` can see it. Use proactively the first time you start navigating an unfamiliar codebase: one call pays for the initial full index, after which the daemon's file watcher keeps the index in sync with the working tree automatically — no need to re-register on edits or branch switches. Idempotent on alias for the same path.".into(),
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
