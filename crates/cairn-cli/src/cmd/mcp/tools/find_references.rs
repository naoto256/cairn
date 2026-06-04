//! `find_references` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, COMPLETENESS_REASON_DESC};

const REF_KIND_DESC: &str = "Restrict to one RefKind. Use snake_case strings: `call`, `type`, `import`, `instantiate`, `read`, `write`, `override`, `macro_invoke`, or `annotation`. Omit for every kind.";

struct FindReferences;

impl McpTool for FindReferences {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "find_references".into(),
            description: format!(
                "Symmetric reference tool — both directions of the call/use graph in one primitive. Omit `repo` to search every registered repo; each hit carries its repo in the `location` prefix (`repo:branch:file:line`).\n\n  • `direction=incoming` (default) — \"who references `symbol`\". Each hit is a use site whose target is `symbol`; the hit carries the enclosing function's qualified name + `repo:branch:file:line`.\n  • `direction=outgoing` — \"what does `symbol` reference\". By default this returns only resolved call refs (`kind=call` with non-empty `target_qualified`) so it can map a function's call graph without unresolved method-call, type-ref, or annotation noise. Set `include_noise=true` to return the full legacy ref set.\n\nWhen Tier-3 data is available for a call site, bare-name Tier-2 refs at that same byte range are suppressed in the default view; if Tier-3 is unavailable, Tier-2 refs remain as fallback. `include_noise=true` shows both rows for inspection.\n\nA `::`-bearing symbol matches the fully-qualified path first and falls back to the bare last segment if nothing matches; bare names skip straight to the name index. Pass `kind` to restrict to a single RefKind; for outgoing type/unresolved refs also set `include_noise=true`. Available wherever cairn has run its Tier-2 analyzer (Rust + Python today), with Tier-3 enrichment preferred when present. {COMPLETENESS_REASON_DESC}"
            ),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "repo":   {"type": "string", "description": "Repository alias. Omit to search every registered repo."},
                    "symbol": {"type": "string", "description": "Anchor symbol. Incoming: matched as the *target* (who calls X). Outgoing: matched as the *enclosing container* (what does X call). `crate::module::foo` form supported."},
                    "direction": {
                        "type": "string",
                        "description": "`incoming` (default) lists references TO `symbol`; `outgoing` lists references FROM `symbol`'s body. Outgoing defaults to resolved call refs only.",
                        "enum": ["incoming", "outgoing"],
                    },
                    "include_noise": {
                        "type": "boolean",
                        "description": "Outgoing only: when true, include unresolved method calls, type refs, annotations, and other non-default refs. Default false returns only resolved call refs.",
                    },
                    "kind":   {
                        "type": "string",
                        "description": REF_KIND_DESC,
                        "enum": ["call", "type", "import", "instantiate", "read", "write", "override", "macro_invoke", "annotation"],
                    },
                    "branch": {"type": "string", "description": BRANCH_PARAM_DESC},
                    "anchor": {"type": "string", "description": ANCHOR_PARAM_DESC},
                    "limit":  {"type": "integer", "minimum": 1, "maximum": 1000, "description": "Cap on hits. If a probe finds more rows beyond this cap, the response is `completeness: partial` with reason `cap`."},
                },
                "required": ["symbol"],
                "additionalProperties": false,
            }),
        }
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: "find_references".into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        45
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(FindReferences);
