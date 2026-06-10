//! Rust Tier-3 workspace analyzer.
//!
//! The syn Tier-2 analyzer records method calls by bare method name.
//! This crate asks rust-analyzer for the definition under each method
//! identifier so the core runner can persist a resolved
//! `target_qualified` ref. The LSP pipeline itself (pooling, document
//! sync, retry, path mapping) lives in cairn-core's definition-pass
//! substrate; this crate contributes the rust-analyzer launch spec and
//! the grammar-specific call-site extraction.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::lsp::Position;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind, WORKSPACE_ANALYZERS,
    WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile, run_lsp_definition_pass,
};
use cairn_core::{Error, Result};
use linkme::distributed_slice;
use serde_json::{Value, json};
use tree_sitter::Node;

const ANALYZER_ID: &str = "rust-analyzer-lsp";
const ANALYZER_REVISION: u32 = 2;
const POOL_CONFIG_ID: &str = "rust-analyzer-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const WORKSPACE_LOAD_TIMEOUT: Duration = Duration::from_secs(120);

pub struct RustAnalyzerWorkspaceAnalyzer;

impl WorkspaceAnalyzer for RustAnalyzerWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "rust"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-rust"
    }

    fn config_paths(&self) -> &'static [&'static str] {
        &["Cargo.toml", "rust-toolchain.toml", "rust-toolchain"]
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        run_lsp_definition_pass(
            LspDefinitionPass {
                analyzer_id: ANALYZER_ID,
                pool_analyzer_id: None,
                language: "rust",
                ref_kind: RefKind::Call,
                spawn_spec: LspSpawnSpec {
                    binary: rust_analyzer_binary(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: POOL_CONFIG_ID.to_string(),
                    request_timeout: REQUEST_TIMEOUT,
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::ProgressQuiescence {
                        timeout: WORKSPACE_LOAD_TIMEOUT,
                    },
                    language_id: "rust",
                    launch_args: Vec::new(),
                    initialization_options: rust_analyzer_initialization_options(POOL_CONFIG_ID),
                },
                retry: DefinitionRetryPolicy {
                    retry_empty_definition: false,
                    retry_file_not_found: true,
                },
                collect_definition_sites: collect_method_calls,
            },
            repo_root,
            files,
        )
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_RUST_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(RustAnalyzerWorkspaceAnalyzer);

fn rust_analyzer_binary() -> PathBuf {
    std::env::var_os("RUST_ANALYZER")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rust-analyzer"))
}

fn rust_analyzer_initialization_options(config_hash: &str) -> Value {
    json!({
        "cairnConfigHash": config_hash,
        "experimental": {
            "serverStatusNotification": true
        },
    })
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    let language: tree_sitter::Language = tree_sitter_rust::LANGUAGE.into();
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter rust: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter rust parse failed".into()))?;
    let mut out = Vec::new();
    collect_method_calls_from_node(tree.root_node(), &mut out);
    Ok(out)
}

fn collect_method_calls_from_node(node: Node<'_>, out: &mut Vec<DefinitionSite>) {
    if node.kind() == "call_expression"
        && let Some(function) = node.child_by_field_name("function")
        && let Some(method) = method_identifier(function)
    {
        let start = method.start_position();
        out.push(DefinitionSite {
            position: Position {
                line: u32::try_from(start.row).unwrap_or(u32::MAX),
                character: u32::try_from(start.column).unwrap_or(u32::MAX),
            },
            byte_start: method.start_byte(),
            byte_end: method.end_byte(),
        });
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_method_calls_from_node(child, out);
    }
}

fn method_identifier(function: Node<'_>) -> Option<Node<'_>> {
    match function.kind() {
        "field_expression" => function.child_by_field_name("field"),
        "scoped_identifier" | "generic_function" | "scoped_type_identifier" => function
            .child_by_field_name("name")
            .or_else(|| last_identifier_child(function)),
        _ => None,
    }
}

fn last_identifier_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut found = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind().ends_with("identifier") {
            found = Some(child);
        } else if let Some(inner) = last_identifier_child(child) {
            found = Some(inner);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_call_collector_finds_method_identifier_positions() {
        let source = br#"
struct Foo;
impl Foo {
    fn bar(&self) {}
}
fn main() {
    let f = Foo;
    f.bar();
    String::new();
}
"#;

        let calls = collect_method_calls(source).unwrap();

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].position.line, 7);
        assert_eq!(calls[0].position.character, 6);
        assert!(calls[0].byte_end > calls[0].byte_start);
    }
}
