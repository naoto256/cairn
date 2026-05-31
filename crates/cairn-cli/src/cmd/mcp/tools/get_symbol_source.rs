//! `get_symbol_source` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct GetSymbolSource;

impl McpTool for GetSymbolSource {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "get_symbol_source".into(),
            description: "Default tool for reading the source of one specific symbol. Given a fully-qualified name (what `find_symbols` and `get_outline` return as `qualified`), returns the exact text of that function / struct / impl / enum / const as it appears in the file — signature, doc comment, and body. Much cheaper than `Read`-ing the whole file when you only need to look at one definition, and unambiguous about *which* `fn handle` you got. Pair with `find_symbols` to go from a free-form name to its source in two calls. If the same qualified name exists in multiple files (rare), pass `file` to disambiguate.\n\nSet `signature_only=true` to peek at the API surface (signature + doc string) without paying for the body bytes — useful for \"what does this take and return\" / \"what does the docstring say\" questions, or when iterating across many candidates before committing to a deep read.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":      {"type": "string"},
                    "qualified": {"type": "string", "description": "Fully-qualified name, e.g. `crate::cli::parse_args` or `MyStruct::new`. Use `find_symbols` first if you only have a bare name."},
                    "branch":    {"type": "string", "description": "Restrict to a single snapshot. Omit to use the first matching branch."},
                    "file":      {"type": "string", "description": "Path relative to repo root. Optional; only needed when the same qualified name exists in multiple files."},
                    "signature_only": {"type": "boolean", "description": "Return only the signature + doc string (no body bytes). Cheap API-surface peek; the `source` field is empty when this is set, `signature` and `doc` carry everything."},
                },
                "required": ["repo", "qualified"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "get_symbol_source".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        25
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(GetSymbolSource);
