//! `list_repos` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct ListRepos;

impl McpTool for ListRepos {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "list_repos".into(),
            description: "Discover which repositories cairn is currently indexing. Call this at the start of a session before reaching for code-navigation tools: if the project you're working in is already listed, prefer `get_outline` / `find_symbols` over `grep` / `Read`; if it isn't, call `register_repo` once to add it. Cheap, no arguments.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, _args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "list_repos".into(),
            params: Value::Null,
        })
    }

    fn sort_key(&self) -> i32 {
        10
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(ListRepos);
