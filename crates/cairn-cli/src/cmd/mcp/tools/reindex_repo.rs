//! `reindex_repo` MCP tool — admin: routes to control.sock.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct ReindexRepo;

impl McpTool for ReindexRepo {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "reindex_repo".into(),
            description: "Force a full rebuild of a registered repository's index. Rare — only needed when the file watcher might have missed changes (e.g. the daemon was down during a large `git restore` / rebase / sparse-checkout, or a snapshot DB has been manually deleted). Day-to-day edits do not need this.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "alias": {"type": "string"},
                },
                "required": ["alias"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::Control {
            method: "reindex_repo".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        100
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(ReindexRepo);
