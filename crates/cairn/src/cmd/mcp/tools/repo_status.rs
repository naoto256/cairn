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
        description: "Entry-point status for one repo or path: resolves to alias, reports current snapshot readiness, and shows whether symbol-aware queries are usable.\n\nWHEN: You have a repo alias (or a path under one) and need to verify cairn covers it before querying.\nNOT FOR: Multi-repo inventory; use list_repos. Job-level diagnostics; use list_jobs.".into(),
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
