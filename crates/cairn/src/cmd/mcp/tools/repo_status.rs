//! `repo_status` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::VERBOSE_TIER3_DESC;
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "repo_status".into(),
        description: format!(
            "Status of one registered repo: language coverage, snapshot summary, current anchor, and Tier-3 analyzer readiness. Use when you have a repo alias or path and need to verify it is indexed before querying. {VERBOSE_TIER3_DESC}"
        ),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo": {
                    "type": "string",
                    "description": "Repository alias. Exactly one of repo or path is required."
                },
                "path": {
                    "type": "string",
                    "description": "Filesystem path under a registered repository. Exactly one of repo or path is required."
                },
                "include_snapshots": {
                    "type": "boolean",
                    "description": "Include per-snapshot detail. Defaults to false."
                },
                "verbose_tier3": {
                    "type": "boolean",
                    "description": VERBOSE_TIER3_DESC
                }
            },
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "repo_status", 11));
