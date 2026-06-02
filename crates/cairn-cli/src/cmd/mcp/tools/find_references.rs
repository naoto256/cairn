//! `find_references` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct FindReferences;

impl McpTool for FindReferences {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_references".into(),
            description: "Symmetric reference tool — both directions of the call/use graph in one primitive.\n\n  • `direction=incoming` (default) — \"who references `symbol`\". Each hit is a use site whose target is `symbol`; the hit carries the enclosing function's qualified name + `repo:branch:file:line`.\n  • `direction=outgoing` — \"what does `symbol` reference\" (callees + type uses inside symbol's body). Each hit is a reference whose enclosing container is `symbol`. Use this to map a function's outgoing call graph without `grep`.\n\nA `::`-bearing symbol matches the fully-qualified path first and falls back to the bare last segment if nothing matches; bare names skip straight to the name index. Pass `kind` to restrict to a single RefKind. Available wherever cairn has run its Tier-2 analyzer (Rust + Python today). Results may carry `completeness: partial` either because Tier-2 is still warming, because more matches exist than `limit`, or because method-call receiver types aren't resolved (Tier-3 territory).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string"},
                    "symbol": {"type": "string", "description": "Anchor symbol. Incoming: matched as the *target* (who calls X). Outgoing: matched as the *enclosing container* (what does X call). `crate::module::foo` form supported."},
                    "direction": {
                        "type": "string",
                        "description": "`incoming` (default) lists references TO `symbol`; `outgoing` lists references FROM `symbol`'s body (its callees / type uses).",
                        "enum": ["incoming", "outgoing"],
                    },
                    "kind":   {
                        "type": "string",
                        "description": "Restrict to one RefKind. Omit for every kind.",
                        "enum": ["call", "type", "import", "instantiate", "read", "write", "override", "macro_invoke", "annotation"],
                    },
                    "branch": {"type": "string", "description": "Restrict to a single snapshot (bare branch name, `HEAD`, `tag/<v>`, or `tentative/<id>`). Omit to use `HEAD`."},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Cap on hits. Truncation is surfaced via `completeness: partial`."},
                },
                "required": ["repo", "symbol"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_references".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        45
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindReferences);
