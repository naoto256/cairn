//! `find_symbols` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};

struct FindSymbols;

impl McpTool for FindSymbols {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_symbols".into(),
            description: "Default tool for any structural symbol question — \"where is X defined\", \"what classes exist\", \"what methods does Widget provide\", \"what's in `crates/foo/`\". Every filter is optional and they AND together, but at least one of `query` / `kind` / `container` / `path` must be supplied (otherwise the call would dump every symbol). One call returns location, signature, kind, originating repo + branch, against an always-current index — strictly faster and lower-token than `grep`.\n\nFour shapes worth knowing:\n  • `{query: \"parse_args\"}` — name lookup. `fuzzy=true` runs an FTS5 search over name + qualified + doc; default is exact name / qualified match.\n  • `{kind: \"class\"}` — list every class (or struct / trait / enum / function / method / …) in scope. Pair with `path` to scope by directory.\n  • `{container: \"Widget\"}` — list Widget's members (methods + nested types). `include_inherited=true` walks the impl/inherit chain and unions in inherited members; on a syntactic-only snapshot the response is reported as `partial` because impls is a Tier-2 fact.\n  • `{path: \"crates/foo/\"}` — file-path prefix scope; combines with the others.\n\nDefaults: `repo=None` searches every registered repo (hits carry their `repo`); `branch=None` searches every indexed branch (hits carry their `branch`). `limit` defaults to 50 and the response is `completeness: partial` (reason names the cap) when more matches exist — raise `limit` to see them. Fall back to `grep` only for free-form text inside symbol bodies.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query":  {"type": "string", "description": "Name / qualified text. Optional; pair with `kind` / `container` / `path` for structural enumeration."},
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "branch": {"type": "string", "description": "Restrict to a single snapshot. Omit to search every indexed branch."},
                    "kind":   {"type": "string", "description": "Restrict to one SymbolKind (e.g. `class`, `function`, `method`, `struct`, `trait`, `enum`)."},
                    "container": {"type": "string", "description": "Qualified-prefix scope. `Widget` returns members of Widget (Widget::* / Widget.*)."},
                    "include_inherited": {"type": "boolean", "description": "When `container` is set, walk the implementations table and include members inherited from base types. Tier-2 dependent — `partial{semantic}` on syntactic-only snapshots."},
                    "path":   {"type": "string", "description": "File-path **string** prefix relative to repo root — byte-level, not directory-aware. Include the trailing `/` to scope to a directory (`crates/foo/` matches only files under `crates/foo/`); `crates/foo` (no slash) also matches sibling `crates/foo_bar/...`. Omit the slash when you want a filename prefix (`crates/foo/src/lib` matches `lib.rs` and `lib_helper.rs`)."},
                    "fuzzy":  {"type": "boolean", "description": "When `query` is set, run an FTS5 search over name + qualified + doc instead of exact match."},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 500, "description": "Cap on hits. Truncation is surfaced via `completeness: partial` so silent caps don't bite."},
                    "signature_only": {"type": "boolean", "description": "Drop the `signature` field from each hit. Use for broad enumerations (e.g. `kind=\"function\"` over a directory) where the signature dominates wire / context cost."},
                },
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_symbols".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        30
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindSymbols);
