//! `list_repos` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "list_repos".into(),
        description: "List registered repos with alias, root, language coverage, and aggregate status. Use this to pick a repo alias before query tools. For per-repo snapshot/analyzer detail use `repo_status`; for background analyzer jobs use `list_jobs`.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Optional alias/root substring filter."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of repos to return."
                }
            },
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "list_repos", 10));
