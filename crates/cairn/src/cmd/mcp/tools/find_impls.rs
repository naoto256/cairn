//! `find_impls` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

struct FindImpls;

impl McpTool for FindImpls {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_impls".into(),
            description: format!(
                "Default tool for trait/impl questions. Omit `repo` to search every registered repo; each hit carries its repo in the `location` prefix (`repo:branch:file:line`). Given a `trait` name, returns every `impl Trait for Foo` block — answering \"what types implement Display?\". Given a `type` name, returns every trait that type implements, plus any inherent (`impl Foo {{}}`) blocks — answering \"what does Foo do?\". Returns location + branch + kind (`trait` or `inherent`). Uses the syn-based semantic layer, so results reflect the current source without rust-analyzer or rustc running. At least one of `trait` / `type` must be supplied; both may be combined. {COMPLETENESS_REASON_DESC} Items already returned are valid."
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "trait":  {"type": "string", "description": "Match impl blocks implementing this trait. e.g. `Display`."},
                    "type":   {"type": "string", "description": "Match impl blocks targeting this type. e.g. `Foo` or `crate::module::Foo`."},
                    "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                    "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 500, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                },
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_impls".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        40
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindImpls);
