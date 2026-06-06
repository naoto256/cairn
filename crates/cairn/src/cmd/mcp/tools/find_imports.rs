//! `find_imports` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

struct FindImports;

impl McpTool for FindImports {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_imports".into(),
            description: format!(
                "List `use` statements across registered repo snapshots. Omit `repo` to search every registered repo; each hit carries its repo in the `location` prefix (`repo:branch:file:line`). Pass `file` to see exactly what one file depends on; omit it to enumerate every import in the snapshot (useful for dependency-shape questions). Each hit carries the path, the dotted module on the left of the final `::`, the imported name (or `*` for globs), an optional `as` alias, and a `is_reexport` flag set for `pub use`. Sourced from the syn semantic layer. {COMPLETENESS_REASON_DESC}"
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "file":   {"type": "string", "description": "Path relative to repo root."},
                    "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                    "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                },
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
