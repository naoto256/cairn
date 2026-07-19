//! `get_symbol_source` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, VERBOSE_TIER3_DESC};

fn spec() -> ToolSpec {
    ToolSpec {
        name: "get_symbol_source".into(),
        description: "Read the source of one symbol by qualified name.\n\nWHEN: After find_symbols returned a hit and you want the body of one specific entry.\nNOT FOR: Browsing files (use get_outline) or fuzzy matching (use find_symbols).".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo":      {"type": "string", "description": "Repository alias. Omit to search every registered repo; duplicate physical declarations return an ambiguity error with candidates."},
                "qualified": {"type": "string", "description": "Fully-qualified name, e.g. `crate::cli::parse_args` or `MyStruct::new`. Use `find_symbols` first if you only have a bare name."},
                "branch":    {"type": "string", "description": BRANCH_PARAM_DESC},
                "anchor":    {"type": "string", "description": ANCHOR_PARAM_DESC},
                "file":      {"type": "string", "description": "Path relative to repo root. Optional; only needed when the same qualified name exists in multiple files."},
                "line":      {"type": "integer", "minimum": 1, "description": "Optional 1-indexed declaration start line. Requires `file` and is compared directly with the candidate line_start."},
                "signature_only": {"type": "boolean", "description": "Return only the signature + doc string (no body bytes). Cheap API-surface peek; the `source` field is empty when this is set, `signature` and `doc` carry everything."},
                "verbose_tier3": {"type": "boolean", "description": VERBOSE_TIER3_DESC},
            },
            "required": ["qualified"],
            "dependentRequired": {"line": ["file"]},
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "get_symbol_source", 25));

#[cfg(test)]
mod tests {
    use super::spec;

    #[test]
    fn schema_exposes_one_indexed_line_with_file_dependency() {
        let schema = spec().input_schema;

        assert_eq!(schema["properties"]["line"]["minimum"], 1);
        assert_eq!(schema["dependentRequired"]["line"][0], "file");
    }
}
