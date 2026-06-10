//! `find_callers` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

struct FindCallers;

impl McpTool for FindCallers {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_callers".into(),
            description: format!(
                "Default tool for \"who calls `name`?\" — every resolved call site whose target is `name`. Use when answering \"is anything still calling this function?\", \"where does `parse_args` get used?\", \"who would I have to update if I changed this signature?\". Omit `repo` to search every registered repo; each hit carries its repo in the `location` prefix (`repo:branch:file:line`). Each hit gives the caller's qualified name (`enclosing_qualified`), the call target (`target_qualified` resolved when known, `target_name` always), the call-site location, and a single-line `snippet` so you can read each call without an extra round trip. Backed by `find_references` with `direction=incoming` and `kind=call`; reach for `find_references` directly when you need other ref kinds (`type`, `import`, `read`, `write`, `annotation`) or to inspect unresolved method-call noise. {COMPLETENESS_REASON_DESC} Items already returned are valid."
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "name":   {"type": "string", "description": "Callee symbol. Matches `refs.target_qualified` first when the name carries `::`, falling back to the bare last segment; bare names go straight to the name index."},
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
            method: "find_callers".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        42
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindCallers);
