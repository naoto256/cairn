//! `find_impls` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct FindImpls;

impl McpTool for FindImpls {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_impls".into(),
            description: "Default tool for trait/impl questions. Given a `trait` name, returns every `impl Trait for Foo` block in the repo — answering \"what types implement Display?\". Given a `type` name, returns every trait that type implements, plus any inherent (`impl Foo {}`) blocks — answering \"what does Foo do?\". Returns location + branch + kind (`trait` or `inherent`). Uses the syn-based semantic layer, so results reflect the current source without rust-analyzer or rustc running. At least one of `trait` / `type` must be supplied; both may be combined. Results may carry `completeness: partial` while the Tier-2 analyzer is still running; items already returned are valid.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string"},
                    "trait":  {"type": "string", "description": "Match impl blocks implementing this trait. e.g. `Display`."},
                    "type":   {"type": "string", "description": "Match impl blocks targeting this type. e.g. `Foo` or `crate::module::Foo`."},
                    "branch": {"type": "string", "description": "Restrict to a single snapshot (bare branch name, `HEAD`, `tag/<v>`, or `tentative/<id>`). Omit to use `HEAD`."},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "required": ["repo"],
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
