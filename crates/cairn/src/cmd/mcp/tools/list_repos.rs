//! `list_repos` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "list_repos".into(),
        description: "**Start here.** Returns the registered repository inventory plus, per snapshot, the language enrichment matrix (which Tier-1 / Tier-2 / Tier-3 facts the index has for each language). Use this to figure out which repos exist, which languages they cover, and which tiers are warm before sending narrower queries. Per-snapshot `status` tells you whether the index is usable: `ready` — symbols indexed, query away; `stale` — files exist for analyzer-capable languages but no symbols are indexed yet, run `reindex_repo` before trusting empty query results; `no_analyzer` — only languages without a semantic backend; `empty` — no indexable files. If the project you're working in is already listed, prefer `get_outline` / `find_symbols` over `grep` / `Read`; if it isn't, call `register_repo` once to add it. Job history is omitted by default; set `include_jobs=true` only when you need it.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "include_jobs": {
                    "type": "boolean",
                    "description": "Include analyzer job details. Defaults to false because job history can be large."
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
