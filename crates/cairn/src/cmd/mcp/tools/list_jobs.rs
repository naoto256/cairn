//! `list_jobs` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "list_jobs".into(),
        description: "Background analyzer job status: queued / running / terminal, with timing and progress metrics.\n\nWHEN: Diagnosing why a Tier-3 analyzer is slow or stalled.\nNOT FOR: One-shot \"is the index ready\" check; use repo_status.tier3_status.this_repo.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo": {
                    "type": "string",
                    "description": "Restrict jobs to one repo alias."
                },
                "state": {
                    "type": "string",
                    "description": "Restrict jobs to one lifecycle state such as queued, running, succeeded, failed, skipped, timed_out, or cancelled."
                },
                "include_terminal": {
                    "type": "boolean",
                    "description": "Include completed jobs. Defaults to false so the tool stays small."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of jobs to return."
                }
            },
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "list_jobs", 12));
