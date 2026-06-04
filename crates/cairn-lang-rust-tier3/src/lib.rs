//! Rust Tier-3 workspace analyzer.
//!
//! The syn Tier-2 analyzer records method calls by bare method name.
//! This crate asks rust-analyzer for the definition under each method
//! identifier so the core runner can persist a resolved
//! `target_qualified` ref.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use cairn_core::lsp::{Location, LspClient, Position, Url};
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    ResolvedRef, WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile,
};
use cairn_core::{Error, Result};
use linkme::distributed_slice;
use tree_sitter::Node;

const ANALYZER_ID: &str = "rust-analyzer-lsp";
const ANALYZER_REVISION: u32 = 1;

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

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        let binary = rust_analyzer_binary();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::InvalidArgument(format!("rust-analyzer runtime: {e}")))?;
        runtime.block_on(async {
            let client = LspClient::start(&binary, repo_root, "rust-analyzer-lsp-v1")
                .await
                .map_err(map_lsp_error)?;
            let mut facts = WorkspaceFacts::default();
            let result = collect_resolved_refs(&client, repo_root, files, &mut facts).await;
            let shutdown = client.shutdown().await.map_err(map_lsp_error);
            result.and(shutdown)?;
            Ok(facts)
        })
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

async fn collect_resolved_refs(
    client: &LspClient,
    repo_root: &Path,
    files: &[WorkspaceFile],
    facts: &mut WorkspaceFacts,
) -> Result<()> {
    for file in files {
        let Some(path) = &file.worktree_path else {
            continue;
        };
        let source = std::fs::read(path)?;
        let calls = collect_method_calls(&source)?;
        if calls.is_empty() {
            continue;
        }
        let uri = Url::from_file_path(path).map_err(map_lsp_error)?;
        for call in calls {
            let locations = client
                .definition(&uri, call.position)
                .await
                .map_err(map_lsp_error)?;
            for target in locations {
                let target_path = location_to_repo_path(repo_root, &target);
                facts.resolved_refs.push(ResolvedRef {
                    source_path: file.path.clone(),
                    source_position: call.position,
                    source_byte_range: call.byte_start..call.byte_end,
                    target,
                    target_path,
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MethodCallSite {
    position: Position,
    byte_start: usize,
    byte_end: usize,
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<MethodCallSite>> {
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

fn collect_method_calls_from_node(node: Node<'_>, out: &mut Vec<MethodCallSite>) {
    if node.kind() == "call_expression"
        && let Some(function) = node.child_by_field_name("function")
        && let Some(method) = method_identifier(function)
    {
        let start = method.start_position();
        out.push(MethodCallSite {
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

fn location_to_repo_path(repo_root: &Path, location: &Location) -> Option<String> {
    let path = file_uri_to_path(location.uri.as_str())?;
    let rel = path.strip_prefix(repo_root).ok()?;
    Some(rel.to_string_lossy().replace('\\', "/"))
}

fn file_uri_to_path(uri: &str) -> Option<PathBuf> {
    let path = uri.strip_prefix("file://")?;
    percent_decode(path).map(PathBuf::from)
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = *bytes.get(i + 1)?;
            let lo = *bytes.get(i + 2)?;
            out.push(hex_value(hi)? * 16 + hex_value(lo)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn map_lsp_error(err: cairn_core::lsp::Error) -> Error {
    Error::InvalidArgument(err.to_string())
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

    #[test]
    fn file_uri_maps_to_repo_relative_path() {
        let location = Location {
            uri: Url::from("file:///tmp/repo/crates/foo/src/lib.rs"),
            range: cairn_core::lsp::Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: 1,
                },
            },
        };

        let rel = location_to_repo_path(Path::new("/tmp/repo"), &location).unwrap();
        assert_eq!(rel, "crates/foo/src/lib.rs");
    }
}
