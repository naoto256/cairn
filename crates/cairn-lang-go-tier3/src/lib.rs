//! Go Tier-3 workspace analyzer backed by gopls LSP.
//!
//! The tree-sitter Tier-1 backend records Go method calls by bare
//! method name. This crate asks gopls for the definition under each
//! selector call's method identifier so the core runner can persist a
//! resolved `target_qualified` ref. The LSP pipeline itself (pooling,
//! document sync, retry, path mapping) lives in cairn-core's
//! definition-pass substrate; this crate contributes the gopls launch
//! spec and the grammar-specific call-site extraction.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::lsp::Position;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::lsp_discovery::discover_lsp_binary;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind,
    WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile, run_lsp_definition_pass,
};
use cairn_core::{Error, Result};
use linkme::distributed_slice;
use serde_json::json;
use tree_sitter::Node;

const ANALYZER_ID: &str = "gopls-lsp";
const ANALYZER_REVISION: u32 = 2;
const POOL_CONFIG_ID: &str = "gopls-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct GoplsWorkspaceAnalyzer;

impl WorkspaceAnalyzer for GoplsWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "go"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-go"
    }

    fn config_paths(&self) -> &'static [&'static str] {
        &["go.mod", "go.work"]
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_lsp_definition_pass(
            LspDefinitionPass {
                analyzer_id: ANALYZER_ID,
                pool_analyzer_id: None,
                language: "go",
                ref_kind: RefKind::Call,
                spawn_spec: LspSpawnSpec {
                    binary: gopls_binary(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: POOL_CONFIG_ID.to_string(),
                    request_timeout: REQUEST_TIMEOUT,
                    availability: AvailabilityStrategy::VersionNoFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "go",
                    launch_args: Vec::new(),
                    env: Vec::new(),
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy {
                    retry_empty_definition: true,
                    retry_file_not_found: false,
                },
                collect_definition_sites: collect_method_calls,
                suppress_definition_targets_at_requested_sites: false,
            },
            repo_root,
            files,
            progress,
        )
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_GO_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(GoplsWorkspaceAnalyzer);

fn gopls_binary() -> PathBuf {
    discover_lsp_binary("gopls", Some("GOPLS")).unwrap_or_else(|| PathBuf::from("gopls"))
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    let language: tree_sitter::Language = tree_sitter_go::LANGUAGE.into();
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter go: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter go parse failed".into()))?;
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
        "selector_expression" => function.child_by_field_name("field"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::anchor::AnchorName;
    use cairn_core::cas::store;
    use cairn_core::query::{FindReferencesArgs, find_references};
    use cairn_core::register::register_repo;
    use cairn_lang_go as _;
    use std::fs;
    use std::process::Command;

    #[test]
    fn method_call_collector_finds_selector_calls_and_skips_bare_calls() {
        let source = b"package main\n\nfunc f() {\n    foo()\n    recv.Method()\n}\n";
        let calls = collect_method_calls(source).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            DefinitionSite {
                position: Position {
                    line: 4,
                    character: 9
                },
                byte_start: 44,
                byte_end: 50,
            }
        );
    }

    #[test]
    fn nested_receiver_chain_reports_innermost_method() {
        let source = b"package main\n\nfunc f() {\n    a.b.Method()\n}\n";
        let calls = collect_method_calls(source).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].position,
            Position {
                line: 3,
                character: 8
            }
        );
    }

    #[test]
    fn malformed_go_input_does_not_panic() {
        let calls = collect_method_calls(b"package main\nfunc { recv.Method(").unwrap();
        assert!(calls.is_empty());
    }

    #[test]
    fn real_gopls_register_repo_surfaces_resolved_method_reference() {
        if !real_gopls_fixture_available() {
            eprintln!("gopls or go toolchain not on PATH; skipping integration test");
            return;
        }

        let repo = tempfile::tempdir().unwrap();
        write_fixture(
            repo.path(),
            &[
                ("go.mod", "module example.com/test\n\ngo 1.22\n"),
                (
                    "pkg/a.go",
                    "package pkg\n\ntype A struct{}\n\nfunc (a A) M() {}\n",
                ),
                (
                    "pkg/b/b.go",
                    "package b\n\nimport \"example.com/test/pkg\"\n\nfunc F() {\n    var x pkg.A\n    x.M()\n}\n",
                ),
            ],
        );
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
                symbol: "M".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        let runs = workspace_runs(&conn);
        let ref_sources = ref_sources(&conn);

        assert!(
            hits.iter().any(|hit| {
                hit.path == "pkg/b/b.go"
                    && hit.line == 7
                    && hit.target_name == "M"
                    && hit.target_qualified.as_deref() == Some("A.M")
            }),
            "expected resolved gopls method ref in pkg/b/b.go, got hits={hits:?}, runs={runs:?}, ref_sources={ref_sources:?}"
        );
    }

    fn write_fixture(repo: &Path, files: &[(&str, &str)]) {
        for (path, content) in files {
            let path = repo.join(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }
    }

    fn real_gopls_fixture_available() -> bool {
        resolve_test_binary(&gopls_binary()).is_some()
            && resolve_test_binary(Path::new("go")).is_some()
    }

    fn resolve_test_binary(binary: &Path) -> Option<PathBuf> {
        if binary.components().count() > 1 {
            return binary.exists().then_some(binary.to_path_buf());
        }
        std::env::var_os("PATH").and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join(binary))
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
