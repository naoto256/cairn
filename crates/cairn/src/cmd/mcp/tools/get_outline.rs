//! `get_outline` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;
use super::{COMPLETENESS_REASON_DESC, SYMBOL_KIND_DESC, VERBOSE_TIER3_DESC};

fn spec() -> ToolSpec {
    ToolSpec {
        name: "get_outline".into(),
        description: format!(
            "Default tool for 'what does this file or directory contain?' questions. Pass `file` for a single-file outline (the current behavior; `file` is omitted on each item because the request names it) or pass `path` to enumerate outlines across every file under that repo-root-relative string prefix in one call (each item carries `file`). Directory-mode items are returned in file → line order. Returns functions, classes, methods, and (for markdown) headings with signatures and doc strings — without loading file bodies. On wide paths (a crate or package root) narrow before raising `limit`: `kind` keeps one symbol kind, and `max_depth: 1` keeps files directly under the prefix — the module-level summary shape. Default limit is 200 and maximum limit is 1000. {COMPLETENESS_REASON_DESC} Items already returned are valid."
        ),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                "file": {"type": "string", "description": "Path relative to the repo root for single-file outline mode."},
                "path": {"type": "string", "description": "Repo-root-relative file-path string prefix for directory mode. Include the trailing `/` to scope to a directory."},
                "kind": {"type": "string", "description": SYMBOL_KIND_DESC},
                "max_depth": {"type": "integer", "minimum": 1, "description": "Directory-mode cap on directory depth relative to `path`, counted by `/` separators after the prefix. `1` keeps items from files directly under the prefix (module-level summary); `2` adds one nested level. Omit for unlimited depth. Ignored in single-file mode."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Directory-mode cap on items. Defaults to 200; `completeness: partial` with reason `cap` means more items matched."},
                "verbose_tier3": {"type": "boolean", "description": VERBOSE_TIER3_DESC},
            },
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "get_outline", 20));
