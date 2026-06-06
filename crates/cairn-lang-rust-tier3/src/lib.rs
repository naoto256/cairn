//! Rust Tier-3 workspace analyzer.
//!
//! The syn Tier-2 analyzer records method calls by bare method name.
//! This crate asks rust-analyzer for the definition under each method
//! identifier so the core runner can persist a resolved
//! `target_qualified` ref.

#![forbid(unsafe_code)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::lsp::pool::{
    self as lsp_pool, AvailabilityStrategy, LspSpawnSpec, PoolKey, PooledLsp, ReadinessStrategy,
};
use cairn_core::lsp::{Location, Position, Url};
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    ResolvedRef, WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile,
};
use cairn_core::{Error, Result};
use linkme::distributed_slice;
use tracing::debug;
use tree_sitter::Node;

const ANALYZER_ID: &str = "rust-analyzer-lsp";
const ANALYZER_REVISION: u32 = 1;
const CONFIG_HASH: &str = "rust-analyzer-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const WORKSPACE_LOAD_TIMEOUT: Duration = Duration::from_secs(120);
const CONTENT_MODIFIED_RETRY_DELAY: Duration = Duration::from_millis(100);

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

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        let binary = rust_analyzer_binary();
        let key = PoolKey::lsp("rust", repo_root, ANALYZER_ID, &binary, CONFIG_HASH)
            .map_err(map_lsp_error)?;
        let spawn_spec = LspSpawnSpec {
            binary,
            workspace_root: repo_root.to_path_buf(),
            config_hash: CONFIG_HASH.to_string(),
            request_timeout: REQUEST_TIMEOUT,
            availability: AvailabilityStrategy::VersionFlag,
            readiness: ReadinessStrategy::ProgressQuiescence {
                timeout: WORKSPACE_LOAD_TIMEOUT,
            },
            language_id: "rust",
        };
        let repo_root = repo_root.to_path_buf();
        let files = files.to_vec();
        let pool = lsp_pool::global().map_err(map_lsp_error)?;
        pool.with_lsp(key, spawn_spec, |client| {
            Box::pin(async move {
                let mut facts = WorkspaceFacts::default();
                collect_resolved_refs(client, &repo_root, &files, &mut facts)
                    .await
                    .map_err(core_error_to_lsp)?;
                Ok(facts)
            })
        })
        .map_err(map_lsp_error)
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
    client: &mut PooledLsp<'_>,
    repo_root: &Path,
    files: &[WorkspaceFile],
    facts: &mut WorkspaceFacts,
) -> Result<()> {
    for file in files {
        let Some(path) = &file.worktree_path else {
            continue;
        };
        let source = std::fs::read_to_string(path)?;
        let calls = collect_method_calls(source.as_bytes())?;
        if calls.is_empty() {
            continue;
        }
        let uri = Url::from_file_path(path).map_err(map_lsp_error)?;
        client
            .sync_document(&uri, &source)
            .await
            .map_err(map_lsp_error)?;
        for call in calls {
            let locations = definition_with_retry(client, &uri, call.position).await?;
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

async fn definition_with_retry(
    client: &PooledLsp<'_>,
    uri: &Url,
    position: Position,
) -> Result<Vec<Location>> {
    definition_with_retry_from(|| client.definition(uri, position), uri, position).await
}

async fn definition_with_retry_from<F, Fut>(
    mut definition: F,
    uri: &Url,
    position: Position,
) -> Result<Vec<Location>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = cairn_core::lsp::Result<Vec<Location>>>,
{
    let mut delay = Duration::from_millis(200);
    let mut retried_content_modified = false;
    for attempt in 0..3 {
        match definition().await {
            Ok(locations) => return Ok(locations),
            Err(err) if err.is_content_modified() && !retried_content_modified => {
                debug!(
                    uri = uri.as_str(),
                    ?position,
                    "rust-analyzer content modified; retrying definition once"
                );
                retried_content_modified = true;
                tokio::time::sleep(CONTENT_MODIFIED_RETRY_DELAY).await;
            }
            Err(err) if is_file_not_found(&err) && attempt < 2 => {
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Err(err) => return Err(map_lsp_error(err)),
        }
    }
    Ok(Vec::new())
}

fn is_file_not_found(err: &cairn_core::lsp::Error) -> bool {
    matches!(err, cairn_core::lsp::Error::Protocol(message) if message.contains("file not found"))
        || matches!(
            err,
            cairn_core::lsp::Error::ResponseError { message, .. } if message.contains("file not found")
        )
}

fn core_error_to_lsp(err: Error) -> cairn_core::lsp::Error {
    match err {
        Error::Lsp(err) => err,
        err => cairn_core::lsp::Error::Protocol(err.to_string()),
    }
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
    Error::Lsp(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::future::ready;

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

    #[tokio::test]
    async fn content_modified_retry_success_preserves_locations() {
        let attempts = Cell::new(0);
        let uri = Url::from("file:///tmp/repo/src/lib.rs");
        let position = Position {
            line: 3,
            character: 12,
        };
        let location = Location {
            uri: Url::from("file:///tmp/repo/src/lib.rs"),
            range: cairn_core::lsp::Range {
                start: Position {
                    line: 9,
                    character: 4,
                },
                end: Position {
                    line: 9,
                    character: 7,
                },
            },
        };

        let locations = definition_with_retry_from(
            || {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    ready(Err(cairn_core::lsp::Error::ResponseError {
                        code: cairn_core::lsp::CONTENT_MODIFIED_ERROR_CODE,
                        message: "content modified".into(),
                    }))
                } else {
                    ready(Ok(vec![location.clone()]))
                }
            },
            &uri,
            position,
        )
        .await
        .unwrap();

        assert_eq!(locations, vec![location]);
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test]
    async fn repeated_content_modified_retries_once_then_returns_error() {
        let attempts = Cell::new(0);
        let uri = Url::from("file:///tmp/repo/src/lib.rs");
        let position = Position {
            line: 3,
            character: 12,
        };

        let locations = definition_with_retry_from(
            || {
                attempts.set(attempts.get() + 1);
                ready(Err(cairn_core::lsp::Error::ResponseError {
                    code: cairn_core::lsp::CONTENT_MODIFIED_ERROR_CODE,
                    message: "content modified".into(),
                }))
            },
            &uri,
            position,
        )
        .await
        .unwrap_err();

        assert!(matches!(locations, Error::Lsp(err) if err.is_content_modified()));
        assert_eq!(attempts.get(), 2);
    }
}
