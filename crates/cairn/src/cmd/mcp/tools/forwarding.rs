//! Shared implementation for MCP tools that only forward arguments.
//!
//! Tool descriptions and schemas stay in each tool module because
//! that is the review surface an LLM sees. This helper only removes
//! the repeated route/sort plumbing for plain data-plane forwarding.

use serde_json::Value;

use super::super::types::ToolSpec;
use super::super::{McpTool, ToolRoute};

pub(super) struct ForwardingTool {
    spec: fn() -> ToolSpec,
    method: &'static str,
    sort_key: i32,
}

impl ForwardingTool {
    pub(super) const fn data(spec: fn() -> ToolSpec, method: &'static str, sort_key: i32) -> Self {
        Self {
            spec,
            method,
            sort_key,
        }
    }
}

impl McpTool for ForwardingTool {
    fn spec(&self) -> ToolSpec {
        (self.spec)()
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(ToolRoute::DataPlane {
            method: self.method.into(),
            params: args,
        })
    }

    fn sort_key(&self) -> i32 {
        self.sort_key
    }
}
