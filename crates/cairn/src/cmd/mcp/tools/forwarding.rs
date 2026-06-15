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
    plane: Plane,
    sort_key: i32,
}

#[derive(Debug, Clone, Copy)]
enum Plane {
    Data,
    Control,
}

impl ForwardingTool {
    pub(super) const fn data(spec: fn() -> ToolSpec, method: &'static str, sort_key: i32) -> Self {
        Self {
            spec,
            method,
            plane: Plane::Data,
            sort_key,
        }
    }

    pub(super) const fn control(
        spec: fn() -> ToolSpec,
        method: &'static str,
        sort_key: i32,
    ) -> Self {
        Self {
            spec,
            method,
            plane: Plane::Control,
            sort_key,
        }
    }
}

impl McpTool for ForwardingTool {
    fn spec(&self) -> ToolSpec {
        (self.spec)()
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        Ok(match self.plane {
            Plane::Data => ToolRoute::DataPlane {
                method: self.method.into(),
                params: args,
            },
            Plane::Control => ToolRoute::Control {
                method: self.method.into(),
                params: args,
            },
        })
    }

    fn sort_key(&self) -> i32 {
        self.sort_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> ToolSpec {
        ToolSpec {
            name: "test".into(),
            description: "test".into(),
            input_schema: json!({"type": "object"}),
        }
    }

    #[test]
    fn data_forwarder_routes_to_data_plane() {
        let tool = ForwardingTool::data(spec, "find_symbols", 30);
        let route = tool.route(json!({"query": "Foo"})).unwrap();

        match route {
            ToolRoute::DataPlane { method, params } => {
                assert_eq!(method, "find_symbols");
                assert_eq!(params, json!({"query": "Foo"}));
            }
            ToolRoute::Control { .. } => panic!("expected data-plane route"),
        }
        assert_eq!(tool.sort_key(), 30);
    }

    #[test]
    fn control_forwarder_routes_to_control_plane() {
        let tool = ForwardingTool::control(spec, "register_repo", 90);
        let route = tool
            .route(json!({"alias": "repo", "path": "/tmp/repo"}))
            .unwrap();

        match route {
            ToolRoute::Control { method, params } => {
                assert_eq!(method, "register_repo");
                assert_eq!(params, json!({"alias": "repo", "path": "/tmp/repo"}));
            }
            ToolRoute::DataPlane { .. } => panic!("expected control-plane route"),
        }
        assert_eq!(tool.sort_key(), 90);
    }
}
