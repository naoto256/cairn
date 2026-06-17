//! `repo_status` MCP tool.

use linkme::distributed_slice;
use serde_json::{Value, json};

use super::super::types::ToolSpec;
use super::super::{MCP_TOOLS, McpTool, ToolRoute};
use super::VERBOSE_TIER3_DESC;

fn spec() -> ToolSpec {
    ToolSpec {
        name: "repo_status".into(),
        description: "Entry-point status for one repo or path: resolves to alias, reports current snapshot readiness, and shows whether symbol-aware queries are usable. Omit both `repo` and `path` to auto-resolve from the current working directory.\n\nWHEN: You have a repo alias (or a path under one) and need to verify cairn covers it before querying.\nNOT FOR: Multi-repo inventory; use list_repos. Job-level diagnostics; use list_jobs.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "repo": {
                    "type": "string",
                    "description": "Repository alias. Exactly one of repo or path is required."
                },
                "path": {
                    "type": "string",
                    "description": "Filesystem path under a registered repository. Exactly one of repo or path is required."
                },
                "include_snapshots": {
                    "type": "boolean",
                    "description": "Include per-snapshot detail. Defaults to false."
                },
                "verbose_tier3": {
                    "type": "boolean",
                    "description": VERBOSE_TIER3_DESC
                }
            },
            "additionalProperties": false,
        }),
    }
}

struct RepoStatusTool;

impl McpTool for RepoStatusTool {
    fn spec(&self) -> ToolSpec {
        spec()
    }

    fn route(&self, args: Value) -> Result<ToolRoute, String> {
        let mut params = match args {
            Value::Null => json!({}),
            Value::Object(_) => args,
            other => {
                return Err(format!(
                    "repo_status arguments must be an object, got {other}"
                ));
            }
        };
        let has_repo = params
            .get("repo")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty());
        let has_path = params
            .get("path")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty());
        if !has_repo && !has_path {
            // MCP can compose a convenient zero-arg entrypoint from the
            // server process cwd; the data-RPC method keeps its stricter
            // exactly-one repo/path contract.
            let cwd = std::env::current_dir().map_err(|e| format!("cwd resolution failed: {e}"))?;
            params["path"] = Value::String(cwd.to_string_lossy().into_owned());
        }
        Ok(ToolRoute::DataPlane {
            method: "repo_status".into(),
            params,
        })
    }

    fn sort_key(&self) -> i32 {
        11
    }
}

#[allow(unsafe_code)]
#[distributed_slice(MCP_TOOLS)]
static REGISTER: fn() -> Box<dyn McpTool> = || Box::new(RepoStatusTool);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_status_with_no_args_injects_cwd_into_path() {
        let route = RepoStatusTool.route(json!({})).unwrap();
        let ToolRoute::DataPlane { method, params } = route else {
            panic!("repo_status should route to data plane");
        };
        assert_eq!(method, "repo_status");
        let path = params["path"].as_str().unwrap();
        assert!(!path.is_empty());
    }
}
