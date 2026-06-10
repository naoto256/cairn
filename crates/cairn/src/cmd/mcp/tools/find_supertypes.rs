//! `find_supertypes` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

struct FindSupertypes;

impl McpTool for FindSupertypes {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_supertypes".into(),
            description: format!(
                "Default tool for \"what does `name` extend / implement / mix in?\" — every type that `name` points at on the supertype side of a type-relation edge. Use for Rust questions like \"what traits does `Foo` implement?\", TypeScript \"what does `Dog` extend?\" or \"what interfaces does `Service` implement?\", Python \"what does `Subclass` inherit from?\". Omit `repo` to search every registered repo; each hit carries its repo in the `location` prefix (`repo:branch:file:line`). Returns the queried name as `type_qualified`, the base it points at as `interface_qualified`, the edge `kind` (`trait` for Rust trait impls, `inherent` for Rust inherent impls, `inherit` for class extends, `implements` for TypeScript implements, `mixin` for mixin classes), and the branch. Reads the same Tier-2 `implementations` table that `find_subtypes` walks from the other side. {COMPLETENESS_REASON_DESC} Items already returned are valid."
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "name":   {"type": "string", "description": "Subtype name. Matches the subtype side of every type-relation edge — i.e. every base type / trait / interface / mixin `name` extends or implements."},
                    "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                    "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 500, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                },
                "required": ["name"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_supertypes".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        41
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindSupertypes);
