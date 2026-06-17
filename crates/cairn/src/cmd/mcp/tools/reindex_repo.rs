//! `reindex_repo` MCP tool — admin: routes to control.sock.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "reindex_repo".into(),
        description: "Force a full rebuild of a registered repo's index.\n\nWHEN: diagnostics or repo_status indicate analyzer work was not recorded after the watcher should have run.\nNOT FOR: Day-to-day edits or normal branch switches; the daemon watcher keeps those indexed.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "alias": {"type": "string"},
            },
            "required": ["alias"],
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::control(spec, "reindex_repo", 100));
