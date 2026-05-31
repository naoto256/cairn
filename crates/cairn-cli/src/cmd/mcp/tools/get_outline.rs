//! `get_outline` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct GetOutline;

impl McpTool for GetOutline {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "get_outline".into(),
            description: "Default tool for 'what does this file contain?' questions. Returns every function, class, method, and (for markdown) heading defined in one file, in line order, with signatures and doc strings — without loading the file body. Use this instead of `Read` when you only need the structural shape of a file, especially for files larger than a few hundred lines. Result may carry `completeness: partial` while the file's Tier-2 enrichment is still warming; the listed items are still valid.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo": {"type": "string"},
                    "file": {"type": "string", "description": "Path relative to the repo root."},
                },
                "required": ["repo", "file"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "get_outline".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        20
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(GetOutline);
