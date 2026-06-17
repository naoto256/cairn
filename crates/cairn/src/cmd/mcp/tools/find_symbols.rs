//! `find_symbols` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, SYMBOL_KIND_DESC, VERBOSE_TIER3_DESC};

fn spec() -> ToolSpec {
    ToolSpec {
        name: "find_symbols".into(),
        description: "Find symbol definitions (functions, classes, methods, structs, etc.) by name, kind, container, or path.\n\nWHEN: You need to locate where something is defined before reading code. Narrow filters (kind/container/path) keep responses small.\nNOT FOR: Free-form text search inside function bodies; use grep. Per-repo inventory; use list_repos.\n\nRecovery: If empty, hints suggest fuzzy / drop filters / widen scope. Check tier3_status.this_query.ready before trusting semantic absence.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query":  {"type": "string", "description": "Name / qualified text. Optional; pair with `kind` / `container` / `path` for structural enumeration."},
                "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                "kind":   {"type": "string", "description": SYMBOL_KIND_DESC},
                "container": {"type": "string", "description": "Qualified-prefix scope. `Widget` returns members of Widget (Widget::* / Widget.*)."},
                "include_inherited": {"type": "boolean", "description": "When `container` is set, walk the implementations table and include members inherited from base types. Tier-2 dependent — `partial{semantic}` on syntactic-only snapshots."},
                "path":   {"type": "string", "description": "File-path **string** prefix relative to repo root — byte-level, not directory-aware. Include the trailing `/` to scope to a directory (`crates/foo/` matches only files under `crates/foo/`); `crates/foo` (no slash) also matches sibling `crates/foo_bar/...`. Omit the slash when you want a filename prefix (`crates/foo/src/lib` matches `lib.rs` and `lib_helper.rs`)."},
                "fuzzy":  {"type": "boolean", "description": "When `query` is set, run SQLite FTS5 over name + qualified + doc instead of exact name / qualified matching. Spaces between bare tokens are AND, quoted text is an exact-order phrase, and prefix matching requires an explicit trailing `*`."},
                "limit":  {"type": "integer", "minimum": 1, "maximum": 500, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                "signature_only": {"type": "boolean", "description": "Drop the `signature` field from each hit. Use for broad enumerations (e.g. `kind=\"function\"` over a directory) where the signature dominates wire / context cost."},
                "verbose_tier3": {"type": "boolean", "description": VERBOSE_TIER3_DESC},
            },
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "find_symbols", 30));
