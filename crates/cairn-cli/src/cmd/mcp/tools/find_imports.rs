//! `find_imports` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC};

struct FindImports;

impl McpTool for FindImports {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_imports".into(),
            description: "List `use` statements across a repo's snapshots. Pass `file` to see exactly what one file depends on; omit it to enumerate every import in the snapshot (useful for dependency-shape questions). Each hit carries the path, the dotted module on the left of the final `::`, the imported name (or `*` for globs), an optional `as` alias, and a `is_reexport` flag set for `pub use`. Sourced from the syn semantic layer. Results may carry `completeness: partial` while the Tier-2 analyzer is still running or when a probe detects more matches than `limit`.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string"},
                    "file":   {"type": "string", "description": "Path relative to repo root."},
                    "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                    "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                },
                "required": ["repo"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_imports".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        50
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindImports);
