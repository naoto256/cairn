//! `find_callees` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

struct FindCallees;

impl McpTool for FindCallees {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_callees".into(),
            description: format!(
                "Default tool for \"what does `name` call?\" — every resolved outgoing call from inside `name`'s body. Use when answering \"what does this function do?\" without reading the source, \"what surface area does `init` touch?\", or mapping a function's effective call graph. Omit `repo` to search every registered repo; each hit carries its repo in the `location` prefix (`repo:branch:file:line`). Each hit gives the queried symbol as `enclosing_qualified`, the callee as `target_qualified` (+ bare `target_name`), the call-site location inside the caller, and a single-line `snippet` of the call. Backed by `find_references` with `direction=outgoing` and `kind=call`, with the default \"resolved calls only\" filter — unresolved method calls, type refs, and annotations are excluded; reach for `find_references` directly when you need them. {COMPLETENESS_REASON_DESC} Items already returned are valid."
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "name":   {"type": "string", "description": "Caller (enclosing) symbol. Matches `symbols.qualified` via the enclosing FK on each ref row."},
                    "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                    "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                },
                "required": ["name"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_callees".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        43
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindCallees);
