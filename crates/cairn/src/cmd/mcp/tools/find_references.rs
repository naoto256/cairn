//! `find_references` MCP tool.

use linkme::distributed_slice;
use serde_json::json;

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool};
use super::forwarding::ForwardingTool;
use super::{ANCHOR_PARAM_DESC, BRANCH_PARAM_DESC, VERBOSE_TIER3_DESC};

const REF_KIND_DESC: &str = "Restrict to one RefKind. Use snake_case strings: `call`, `type`, `import`, `instantiate`, `read`, `write`, `override`, `macro_invoke`, or `annotation`. Omit for every kind.";

fn spec() -> ToolSpec {
    ToolSpec {
        name: "find_references".into(),
        description: "Find usage sites of a symbol: calls, type references, imports, reads, writes, annotations.\n\nWHEN: You know a symbol exists and want all places using it.\nNOT FOR: JSX component instantiation only; use `kind=instantiate`. Plain \"who calls this function\"; find_callers is tighter.\n\nRecovery: hints suggest direction / kind / scope adjustments.".into(),
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
                "verbose_tier3": {"type": "boolean", "description": VERBOSE_TIER3_DESC},
            },
            "required": ["symbol"],
            "additionalProperties": false,
        }),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> =
    || Box::new(ForwardingTool::data(spec, "find_references", 45));
