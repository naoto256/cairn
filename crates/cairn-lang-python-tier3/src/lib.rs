//! Python Tier-3 workspace analyzer backed by pyright LSP.
//!
//! The tree-sitter Tier-2 analyzer records receiver calls by bare
//! method name. This crate asks pyright for the definition under each
//! attribute call's method identifier so the core runner can persist a
//! resolved `target_qualified` ref. The LSP pipeline itself (pooling,
//! document sync, retry, path mapping) lives in cairn-core's
//! definition-pass substrate; this crate contributes the pyright
//! launch spec and the grammar-specific call-site extraction.

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
use serde_json::json;
use tree_sitter::Node;

const ANALYZER_ID: &str = "pyright-lsp";
const ANALYZER_REVISION: u32 = 2;
const POOL_CONFIG_ID: &str = "pyright-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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

    fn config_paths(&self) -> &'static [&'static str] {
        &["pyrightconfig.json", "pyproject.toml"]
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
                language: "python",
                ref_kind: RefKind::Call,
                spawn_spec: LspSpawnSpec {
                    binary: pyright_binary(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: POOL_CONFIG_ID.to_string(),
                    request_timeout: REQUEST_TIMEOUT,
                    availability: AvailabilityStrategy::PathExistsExecutable,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "python",
                    launch_args: vec!["--stdio".to_string()],
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy {
                    // Pyright can return an empty definition result for
                    // a document that was just didOpen'd before its
                    // analysis pass completes.
                    retry_empty_definition: true,
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
static REGISTER_PYTHON_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(PyrightWorkspaceAnalyzer);

fn pyright_binary() -> PathBuf {
    std::env::var_os("PYRIGHT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("pyright-langserver"))
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
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

fn collect_method_calls_from_node(node: Node<'_>, out: &mut Vec<DefinitionSite>) {
    if node.kind() == "call"
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
        "attribute" => function.child_by_field_name("attribute"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
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
