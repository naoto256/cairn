//! `find_callees` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, VERBOSE_TIER3_DESC};

fn spec() -> ToolSpec {
    ToolSpec {
        name: "find_callees".into(),
        description: "Find what a function calls (outgoing edges).\n\nWHEN: Tracing a function's downstream behavior.\nNOT FOR: Incoming calls; use find_callers. Other kinds of references; use find_references.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                "name":   {"type": "string", "description": "Caller (enclosing) symbol. Matches `symbols.qualified` via the enclosing FK on each ref row."},
                "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                "limit":  {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                "verbose_tier3": {"type": "boolean", "description": VERBOSE_TIER3_DESC},
            },
            "required": ["name"],
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "find_callees", 43));
