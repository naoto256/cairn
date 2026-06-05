//! `find_symbols` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

const SYMBOL_KIND_DESC: &str = "Restrict to one SymbolKind. Use snake_case strings: `function`, `method`, `constructor`, `getter`, `setter`, `class`, `struct`, `enum`, `union`, `trait`, `impl`, `interface`, `type_alias`, `field`, `property`, `constant`, `variable`, `parameter`, `module`, `namespace`, `package`, `macro`, `section`, or `test`. Aliases such as `fn` or `Function` are not valid.";

struct FindSymbols;

impl McpTool for FindSymbols {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_symbols".into(),
            description: format!(
                "Default tool for any structural symbol question — \"where is X defined\", \"what classes exist\", \"what methods does Widget provide\", \"what's in `crates/foo/`\". Every filter is optional and they AND together, but at least one of `query` / `kind` / `container` / `path` must be supplied (otherwise the call would dump every symbol). Best practice: start with the *narrowest* of `kind` / `container` / `path` you can. `query` alone (especially `fuzzy=true`) over a multi-repo registry can dump hundreds of hits before the cap; pairing it with `kind=\"function\"`, `container=\"Widget\"`, or `path=\"crates/foo/\"` keeps response size and consumer reasoning manageable. One call returns location, signature, kind, language, originating repo + branch, against an always-current index — strictly faster and lower-token than `grep`. Results are sorted by language (alphabetical, syntactic-only hits last), then file path, then line, so consumers can group consecutive hits by `language`.\n\nFour shapes worth knowing:\n  • `{{query: \"parse_args\"}}` — name lookup. Default is exact name / qualified match. `fuzzy=true` runs SQLite FTS5 over name + qualified + doc: bare tokens separated by spaces are AND-ed (`\"User Auth\"` requires both full tokens), quoted text is an exact-order phrase (`\"\\\"User Authentication\\\"\"`), and prefix matching requires an explicit `*` (`\"Authent*\"`). Use `fuzzy=true` when you only remember a fragment (for example `\"Auth*\"` to find Authentication / Authorizer / AuthHandler), or for multi-token AND search across name + qualified + doc (for example `\"User Cache\"` finds rows that mention both). Keep `fuzzy=false` (default) when you have the exact name — exact match is faster and unambiguous.\n  • `{{kind: \"class\"}}` — list every class (or struct / trait / enum / function / method / …) in scope. Pair with `path` to scope by directory.\n  • `{{container: \"Widget\"}}` — list Widget's members (methods + nested types). `include_inherited=true` walks the impl/inherit chain and unions in inherited members; on a syntactic-only snapshot the response is reported as `partial` because impls is a Tier-2 fact.\n  • `{{path: \"crates/foo/\"}}` — file-path prefix scope; combines with the others.\n\nDefaults: `repo=None` searches every registered repo (hits carry their `repo`); omitting both `branch` and `anchor` resolves to the registered worktree's `tentative/<id>` snapshot, which includes uncommitted edits the daemon's file watcher has picked up (falling back to `HEAD` when no tentative snapshot exists yet). Pass `anchor=\"HEAD\"` explicitly to scope to committed-only state; pass `branch=\"<name>\"` for a bare branch name. `limit` defaults to 50; raise `limit` when partial reason is `cap`. {COMPLETENESS_REASON_DESC} Fall back to `grep` only for free-form text inside symbol bodies."
            ),
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
