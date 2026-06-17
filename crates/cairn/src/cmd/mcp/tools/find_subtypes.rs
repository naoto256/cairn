//! `find_subtypes` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, VERBOSE_TIER3_DESC};

fn spec() -> ToolSpec {
    ToolSpec {
        name: "find_subtypes".into(),
        description: "Types that implement / extend / mix in the given name.\n\nWHEN: Finding implementors of a trait/interface, or subclasses.\nNOT FOR: Methods on a type; use find_symbols container=.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                "name":   {"type": "string", "description": "Base type / trait / interface / class. Matches the supertype side of every type-relation edge — i.e. every type that implements / extends / mixes in `name`."},
                "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                "limit":  {"type": "integer", "minimum": 1, "maximum": 500, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
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
    || Box::new(ForwardingTool::data(spec, "find_subtypes", 40));
