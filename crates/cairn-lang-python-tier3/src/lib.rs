//! Python Tier-3 workspace analyzer backed by pyright LSP.
//!
//! The tree-sitter Tier-2 analyzer records receiver calls by bare
//! method name. This crate asks pyright for the definition under each
//! attribute call's method identifier so the core runner can persist a
//! resolved `target_qualified` ref.

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

const ANALYZER_ID: &str = "pyright-lsp";
const ANALYZER_REVISION: u32 = 1;
const CONFIG_HASH: &str = "pyright-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONTENT_MODIFIED_RETRY_DELAY: Duration = Duration::from_millis(100);

pub struct PyrightWorkspaceAnalyzer;

impl WorkspaceAnalyzer for PyrightWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "python"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-python"
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        let binary = pyright_binary();
        let key = PoolKey::lsp("python", repo_root, ANALYZER_ID, &binary, CONFIG_HASH)
            .map_err(map_lsp_error)?;
        let spawn_spec = LspSpawnSpec {
            binary,
            workspace_root: repo_root.to_path_buf(),
            config_hash: CONFIG_HASH.to_string(),
            request_timeout: REQUEST_TIMEOUT,
            availability: AvailabilityStrategy::PathExistsExecutable,
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "python",
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
static REGISTER_PYTHON_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(PyrightWorkspaceAnalyzer);

fn pyright_binary() -> PathBuf {
    std::env::var_os("PYRIGHT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("pyright-langserver"))
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
    let mut retried_empty_definition = false;
    let mut retried_content_modified = false;
    for attempt in 0..3 {
        match definition().await {
            Ok(locations) if !locations.is_empty() => return Ok(locations),
            // Pyright can return an empty definition result for a
            // document that was just didOpen'd before its analysis pass
            // completes. One retry covers the typical post-didOpen lag.
            Ok(_) if !retried_empty_definition => {
                retried_empty_definition = true;
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Ok(locations) => return Ok(locations),
            Err(err) if err.is_content_modified() && !retried_content_modified => {
                debug!(
                    uri = uri.as_str(),
                    ?position,
                    "pyright content modified; retrying definition once"
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
    let language: tree_sitter::Language = tree_sitter_python::LANGUAGE.into();
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter python: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter python parse failed".into()))?;
    let mut out = Vec::new();
    collect_method_calls_from_node(tree.root_node(), &mut out);
    Ok(out)
}

fn collect_method_calls_from_node(node: Node<'_>, out: &mut Vec<MethodCallSite>) {
    if node.kind() == "call"
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
        "attribute" => function.child_by_field_name("attribute"),
        _ => None,
    }
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
    use std::fs;
    use std::future::ready;
    use std::process::Command;

    use cairn_core::anchor::AnchorName;
    use cairn_core::cas::store;
    use cairn_core::query::{FindReferencesArgs, find_references};
    use cairn_core::register::register_repo;
    use cairn_lang_python as _;

    #[test]
    fn method_call_collector_finds_attribute_call_positions() {
        let source = br#"
class Foo:
    def bar(self):
        pass

def main(obj):
    obj.method()
    foo()
"#;

        let calls = collect_method_calls(source).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].position.line, 6);
        assert_eq!(calls[0].position.character, 8);
        assert!(calls[0].byte_end > calls[0].byte_start);
    }

    #[test]
    fn nested_receiver_chain_reports_innermost_method() {
        let source = b"def main(a):\n    a.b.c()\n";

        let calls = collect_method_calls(source).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].position.line, 1);
        assert_eq!(calls[0].position.character, 8);
    }

    #[test]
    fn malformed_python_input_does_not_panic() {
        let calls = collect_method_calls(b"def main(:\n").unwrap();
        assert!(calls.is_empty());
    }

    #[test]
    fn file_uri_maps_to_repo_relative_path() {
        let location = Location {
            uri: Url::from("file:///tmp/repo/pkg/foo.py"),
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
        assert_eq!(rel, "pkg/foo.py");
    }

    #[tokio::test]
    async fn content_modified_retry_success_preserves_locations() {
        let attempts = Cell::new(0);
        let uri = Url::from("file:///tmp/repo/pkg/foo.py");
        let position = Position {
            line: 3,
            character: 12,
        };
        let location = Location {
            uri: Url::from("file:///tmp/repo/pkg/foo.py"),
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
    async fn empty_definition_retries_once_then_returns_resolved() {
        let attempts = Cell::new(0);
        let uri = Url::from("file:///tmp/repo/pkg/foo.py");
        let position = Position {
            line: 5,
            character: 6,
        };
        let location = Location {
            uri: Url::from("file:///tmp/repo/pkg/foo.py"),
            range: cairn_core::lsp::Range {
                start: Position {
                    line: 1,
                    character: 8,
                },
                end: Position {
                    line: 1,
                    character: 9,
                },
            },
        };

        let locations = definition_with_retry_from(
            || {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    ready(Ok(Vec::new()))
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
    async fn repeated_empty_definition_retries_once_then_returns_empty() {
        let attempts = Cell::new(0);
        let uri = Url::from("file:///tmp/repo/pkg/foo.py");
        let position = Position {
            line: 5,
            character: 6,
        };

        let locations = definition_with_retry_from(
            || {
                attempts.set(attempts.get() + 1);
                ready(Ok(Vec::new()))
            },
            &uri,
            position,
        )
        .await
        .unwrap();

        assert!(locations.is_empty());
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test]
    async fn repeated_content_modified_retries_once_then_returns_error() {
        let attempts = Cell::new(0);
        let uri = Url::from("file:///tmp/repo/pkg/foo.py");
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

    /// Runs only when pyright-langserver is available on PATH (or via
    /// PYRIGHT) because it exercises the real LSP binary startup and
    /// definition path instead of the unit-test mocks.
    #[test]
    fn real_pyright_register_repo_surfaces_resolved_method_reference() {
        assert_real_pyright_fixture_resolves(
            &[
                ("pkg/__init__.py", ""),
                ("pkg/a.py", "class A:\n    def m(self):\n        pass\n"),
                (
                    "pkg/b.py",
                    "from pkg.a import A\n\n\ndef run():\n    a = A()\n    a.m()\n",
                ),
            ],
            "pkg/b.py",
            6,
        );
    }

    #[test]
    fn real_pyright_register_repo_resolves_relative_import_method_reference() {
        assert_real_pyright_fixture_resolves(
            &[
                ("pkg/__init__.py", ""),
                ("pkg/a.py", "class A:\n    def m(self):\n        pass\n"),
                (
                    "pkg/b.py",
                    "from .a import A\n\n\ndef run():\n    a = A()\n    a.m()\n",
                ),
            ],
            "pkg/b.py",
            6,
        );
    }

    #[test]
    fn real_pyright_register_repo_resolves_cross_package_method_reference() {
        assert_real_pyright_fixture_resolves(
            &[
                ("pkg_a/__init__.py", ""),
                ("pkg_a/a.py", "class A:\n    def m(self):\n        pass\n"),
                ("pkg_b/__init__.py", ""),
                (
                    "pkg_b/b.py",
                    "from pkg_a.a import A\n\n\ndef run():\n    a = A()\n    a.m()\n",
                ),
            ],
            "pkg_b/b.py",
            6,
        );
    }

    fn assert_real_pyright_fixture_resolves(
        files: &[(&str, &str)],
        expected_path: &str,
        expected_line: u32,
    ) {
        if resolve_test_pyright().is_none() {
            eprintln!("pyright not on PATH; skipping integration test");
            return;
        }

        let repo = tempfile::tempdir().unwrap();
        for (path, content) in files {
            let path = repo.path().join(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }
        git(repo.path(), &["init"]);
        git(repo.path(), &["add", "."]);
        git(repo.path(), &["commit", "-m", "fixture"]);

        let db = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 0).unwrap();

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "m".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        let runs = workspace_runs(&conn);
        let ref_sources = ref_sources(&conn);

        assert!(
            hits.iter().any(|hit| {
                hit.path == expected_path
                    && hit.line == expected_line
                    && hit.target_name == "m"
                    && hit.target_qualified.as_deref() == Some("A.m")
            }),
            "expected resolved pyright method ref in {expected_path}, got hits={hits:?}, runs={runs:?}, ref_sources={ref_sources:?}"
        );
    }

    fn resolve_test_pyright() -> Option<PathBuf> {
        let binary = pyright_binary();
        if binary.components().count() > 1 {
            return binary.exists().then_some(binary);
        }
        std::env::var_os("PATH").and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join(&binary))
                .find(|candidate| candidate.exists())
        })
    }

    fn git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn workspace_runs(conn: &rusqlite::Connection) -> Vec<(String, String, Option<String>)> {
        let mut stmt = conn
            .prepare(
                "SELECT analyzer_id, status, error
                 FROM workspace_analysis_runs
                 ORDER BY analyzer_id",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }

    fn ref_sources(conn: &rusqlite::Connection) -> Vec<(String, Option<String>)> {
        let mut stmt = conn
            .prepare(
                "SELECT source, target_qualified
                 FROM refs
                 ORDER BY source, target_qualified",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }
}
