//! C, C++, and Objective-C Tier-3 workspace analyzers backed by clangd.
//!
//! clangd is one server for the Clang family, but Cairn's workspace
//! analyzer runner routes input files by one Tier-1 `parser_id`.
//! This crate therefore registers three analyzers, one per parser,
//! while sharing a single `clangd-lsp` pool key so a daemon can reuse
//! one clangd subprocess for C, C++, and Objective-C files in the same
//! repository.
//!
//! Each analyzer asks clangd for `textDocument/definition` at call
//! identifiers and include path tokens. Cross-file calls become
//! resolved call refs when the returned location maps back to an
//! indexed symbol. Includes become import refs; if clangd resolves an
//! include to a file location that is not inside any symbol range,
//! core persists the target file path as the import target. Template
//! instantiation and preprocessor branch handling are intentionally
//! best-effort: Cairn records what clangd resolves for the current
//! compile command / fallback flags.

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
use tree_sitter::{Language, Node, Parser};

const CLANGD_POOL_ID: &str = "clangd-lsp";
const C_ANALYZER_ID: &str = "clangd-c-lsp";
const CPP_ANALYZER_ID: &str = "clangd-cpp-lsp";
const OBJC_ANALYZER_ID: &str = "clangd-objc-lsp";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "clangd-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const WORKSPACE_LOAD_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Debug, Clone, Copy)]
struct ClangdLanguage {
    analyzer_id: &'static str,
    language: &'static str,
    parser_id: &'static str,
    language_id: &'static str,
    tree_sitter_language: fn() -> Language,
    fallback_flags: &'static [&'static str],
    call_collector: fn(Node<'_>) -> Option<Node<'_>>,
}

const C_LANGUAGE: ClangdLanguage = ClangdLanguage {
    analyzer_id: C_ANALYZER_ID,
    language: "c",
    parser_id: "tree-sitter-c",
    language_id: "c",
    tree_sitter_language: c_language,
    fallback_flags: &["-xc", "-std=c17"],
    call_collector: c_call_identifier,
};

const CPP_LANGUAGE: ClangdLanguage = ClangdLanguage {
    analyzer_id: CPP_ANALYZER_ID,
    language: "cpp",
    parser_id: "tree-sitter-cpp",
    language_id: "cpp",
    tree_sitter_language: cpp_language,
    fallback_flags: &["-xc++", "-std=c++17"],
    call_collector: cpp_call_identifier,
};

const OBJC_LANGUAGE: ClangdLanguage = ClangdLanguage {
    analyzer_id: OBJC_ANALYZER_ID,
    language: "objc",
    parser_id: "tree-sitter-objc",
    language_id: "objective-c",
    tree_sitter_language: objc_language,
    fallback_flags: &["-xobjective-c"],
    call_collector: objc_call_identifier,
};

pub struct ClangdCWorkspaceAnalyzer;
pub struct ClangdCppWorkspaceAnalyzer;
pub struct ClangdObjcWorkspaceAnalyzer;

impl WorkspaceAnalyzer for ClangdCWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        C_LANGUAGE.analyzer_id
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        C_LANGUAGE.language
    }

    fn parser_id(&self) -> &'static str {
        C_LANGUAGE.parser_id
    }

    fn config_paths(&self) -> &'static [&'static str] {
        clangd_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        run_clangd_passes(C_LANGUAGE, repo_root, files)
    }
}

impl WorkspaceAnalyzer for ClangdCppWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        CPP_LANGUAGE.analyzer_id
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        CPP_LANGUAGE.language
    }

    fn parser_id(&self) -> &'static str {
        CPP_LANGUAGE.parser_id
    }

    fn config_paths(&self) -> &'static [&'static str] {
        clangd_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        run_clangd_passes(CPP_LANGUAGE, repo_root, files)
    }
}

impl WorkspaceAnalyzer for ClangdObjcWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        OBJC_LANGUAGE.analyzer_id
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        OBJC_LANGUAGE.language
    }

    fn parser_id(&self) -> &'static str {
        OBJC_LANGUAGE.parser_id
    }

    fn config_paths(&self) -> &'static [&'static str] {
        clangd_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        run_clangd_passes(OBJC_LANGUAGE, repo_root, files)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_C_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(ClangdCWorkspaceAnalyzer);

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_CPP_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(ClangdCppWorkspaceAnalyzer);

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_OBJC_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(ClangdObjcWorkspaceAnalyzer);

fn clangd_config_paths() -> &'static [&'static str] {
    &["compile_commands.json", "compile_flags.txt", ".clangd"]
}

fn run_clangd_passes(
    language: ClangdLanguage,
    repo_root: &Path,
    files: &[WorkspaceFile],
) -> Result<WorkspaceFacts> {
    let mut facts = run_clangd_pass(
        language,
        repo_root,
        files,
        RefKind::Call,
        call_collector_for(language),
    )?;
    let include_facts = run_clangd_pass(
        language,
        repo_root,
        files,
        RefKind::Import,
        include_collector_for(language),
    )?;
    facts.resolved_refs.extend(include_facts.resolved_refs);
    Ok(facts)
}

fn run_clangd_pass(
    language: ClangdLanguage,
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: language.analyzer_id,
            pool_analyzer_id: Some(CLANGD_POOL_ID),
            language: "clangd",
            ref_kind,
            spawn_spec: clangd_spawn_spec(language, repo_root),
            retry: DefinitionRetryPolicy {
                retry_empty_definition: true,
                retry_file_not_found: true,
            },
            collect_definition_sites: collect,
        },
        repo_root,
        files,
    )
}

fn clangd_spawn_spec(language: ClangdLanguage, repo_root: &Path) -> LspSpawnSpec {
    LspSpawnSpec {
        binary: clangd_binary(),
        workspace_root: repo_root.to_path_buf(),
        config_hash: POOL_CONFIG_ID.to_string(),
        request_timeout: REQUEST_TIMEOUT,
        availability: AvailabilityStrategy::VersionFlag,
        // clangd indexes compile_commands.json on first document open and emits
        // LSP progress notifications during indexing; waiting for quiescence
        // prevents definition() from racing the indexer.
        readiness: ReadinessStrategy::ProgressQuiescence {
            timeout: WORKSPACE_LOAD_TIMEOUT,
        },
        language_id: language.language_id,
        launch_args: Vec::new(),
        initialization_options: json!({
            "fallbackFlags": language.fallback_flags,
        }),
    }
}

fn call_collector_for(language: ClangdLanguage) -> fn(&[u8]) -> Result<Vec<DefinitionSite>> {
    match language.analyzer_id {
        C_ANALYZER_ID => collect_c_calls,
        CPP_ANALYZER_ID => collect_cpp_calls,
        OBJC_ANALYZER_ID => collect_objc_calls,
        _ => collect_c_calls,
    }
}

fn include_collector_for(language: ClangdLanguage) -> fn(&[u8]) -> Result<Vec<DefinitionSite>> {
    match language.analyzer_id {
        C_ANALYZER_ID => collect_c_includes,
        CPP_ANALYZER_ID => collect_cpp_includes,
        OBJC_ANALYZER_ID => collect_objc_includes,
        _ => collect_c_includes,
    }
}

fn clangd_binary() -> PathBuf {
    std::env::var_os("CLANGD")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("clangd"))
}

fn collect_calls(source: &[u8], language: ClangdLanguage) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, language, SiteKind::Call)
}

fn collect_includes(source: &[u8], language: ClangdLanguage) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, language, SiteKind::Include)
}

fn collect_c_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_calls(source, C_LANGUAGE)
}

fn collect_cpp_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_calls(source, CPP_LANGUAGE)
}

fn collect_objc_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_calls(source, OBJC_LANGUAGE)
}

fn collect_c_includes(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_includes(source, C_LANGUAGE)
}

fn collect_cpp_includes(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_includes(source, CPP_LANGUAGE)
}

fn collect_objc_includes(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_includes(source, OBJC_LANGUAGE)
}

#[derive(Debug, Clone, Copy)]
enum SiteKind {
    Call,
    Include,
}

fn collect_sites(
    source: &[u8],
    language: ClangdLanguage,
    site_kind: SiteKind,
) -> Result<Vec<DefinitionSite>> {
    let mut parser = Parser::new();
    parser
        .set_language(&(language.tree_sitter_language)())
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter {}: {e}", language.language)))?;
    let tree = parser.parse(source, None).ok_or_else(|| {
        Error::InvalidArgument(format!("tree-sitter {} parse failed", language.language))
    })?;
    let mut out = Vec::new();
    collect_sites_from_node(tree.root_node(), source, language, site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(
    node: Node<'_>,
    source: &[u8],
    language: ClangdLanguage,
    site_kind: SiteKind,
    out: &mut Vec<DefinitionSite>,
) {
    match site_kind {
        SiteKind::Call if node.kind() == "call_expression" => {
            if let Some(identifier) = (language.call_collector)(node) {
                out.push(site_from_node(identifier, 0, 0));
            }
        }
        SiteKind::Call if language.language == "objc" && node.kind() == "message_expression" => {
            if let Some(method) = node.child_by_field_name("method") {
                out.push(site_from_node(method, 0, 0));
            }
        }
        SiteKind::Include if node.kind() == "preproc_include" => {
            if let Some(path) = node.child_by_field_name("path") {
                out.push(include_site(path, source));
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_sites_from_node(child, source, language, site_kind, out);
    }
}

fn c_call_identifier(call: Node<'_>) -> Option<Node<'_>> {
    let function = call.child_by_field_name("function")?;
    (function.kind() == "identifier").then_some(function)
}

fn cpp_call_identifier(call: Node<'_>) -> Option<Node<'_>> {
    let function = call.child_by_field_name("function")?;
    match function.kind() {
        "identifier" | "field_identifier" => Some(function),
        _ => function
            .child_by_field_name("field")
            .or_else(|| function.child_by_field_name("name"))
            .or_else(|| last_identifier_child(function)),
    }
}

fn objc_call_identifier(call: Node<'_>) -> Option<Node<'_>> {
    c_call_identifier(call)
}

fn last_identifier_child(node: Node<'_>) -> Option<Node<'_>> {
    if matches!(
        node.kind(),
        "identifier" | "field_identifier" | "type_identifier"
    ) {
        return Some(node);
    }
    let mut cursor = node.walk();
    let children = node.named_children(&mut cursor).collect::<Vec<_>>();
    children.into_iter().rev().find_map(last_identifier_child)
}

fn include_site(path: Node<'_>, source: &[u8]) -> DefinitionSite {
    let raw = path.utf8_text(source).unwrap_or_default().trim();
    let trim_delimiter = raw.len() >= 2
        && ((raw.starts_with('"') && raw.ends_with('"'))
            || (raw.starts_with('<') && raw.ends_with('>')));
    let offset = usize::from(trim_delimiter);
    site_from_node(path, offset, offset)
}

fn site_from_node(
    node: Node<'_>,
    byte_start_offset: usize,
    byte_end_trim: usize,
) -> DefinitionSite {
    let start = node.start_position();
    DefinitionSite {
        position: Position {
            line: u32::try_from(start.row).unwrap_or(u32::MAX),
            character: u32::try_from(start.column.saturating_add(byte_start_offset))
                .unwrap_or(u32::MAX),
        },
        byte_start: node.start_byte().saturating_add(byte_start_offset),
        byte_end: node.end_byte().saturating_sub(byte_end_trim),
    }
}

fn c_language() -> Language {
    tree_sitter_c::LANGUAGE.into()
}

fn cpp_language() -> Language {
    tree_sitter_cpp::LANGUAGE.into()
}

fn objc_language() -> Language {
    tree_sitter_objc::LANGUAGE.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::lsp::Url;
    use std::fs;

    #[test]
    fn clangd_spawn_spec_waits_for_progress_quiescence() {
        let spec = clangd_spawn_spec(C_LANGUAGE, Path::new("/tmp/repo"));

        assert_eq!(spec.request_timeout, REQUEST_TIMEOUT);
        assert!(matches!(
            spec.readiness,
            ReadinessStrategy::ProgressQuiescence {
                timeout: WORKSPACE_LOAD_TIMEOUT
            }
        ));
    }

    #[test]
    fn c_collectors_find_calls_and_includes() {
        let source = br#"#include "defs.h"
int main(void) { return add(1, 2); }
"#;

        let calls = collect_calls(source, C_LANGUAGE).unwrap();
        let includes = collect_includes(source, C_LANGUAGE).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(source_text(source, calls[0]), "add");
        assert_eq!(includes.len(), 1);
        assert_eq!(source_text(source, includes[0]), "defs.h");
    }

    #[test]
    fn cpp_collector_finds_member_and_qualified_calls() {
        let source = br#"
namespace app {
void run();
struct Widget { void draw(); };
void f(Widget w) { w.draw(); app::run(); }
}
"#;

        let calls = collect_calls(source, CPP_LANGUAGE).unwrap();
        let names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"draw"));
        assert!(names.contains(&"run"));
    }

    #[test]
    fn objc_collector_finds_c_calls_messages_and_imports() {
        let source = br#"#import "Widget.h"
@implementation Widget
- (void)draw { helper(); [self render]; }
@end
"#;

        let calls = collect_calls(source, OBJC_LANGUAGE).unwrap();
        let includes = collect_includes(source, OBJC_LANGUAGE).unwrap();
        let names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"helper"));
        assert!(names.contains(&"render"));
        assert_eq!(source_text(source, includes[0]), "Widget.h");
    }

    #[test]
    fn lsp_mock_resolves_cross_file_call() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.c");
        let target = tmp.path().join("defs.h");
        fs::write(&source, "int main(void) { return add(1, 2); }\n").unwrap();
        fs::write(&target, "int add(int a, int b);\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_c_calls,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.c");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("defs.h")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Call);
    }

    #[test]
    fn lsp_mock_resolves_cross_file_include() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.c");
        let target = tmp.path().join("defs.h");
        fs::write(
            &source,
            "#include \"defs.h\"\nint main(void) { return 0; }\n",
        )
        .unwrap();
        fs::write(&target, "int add(int a, int b);\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Import,
            collect_c_includes,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.c");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("defs.h")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Import);
    }

    fn source_text(source: &[u8], site: DefinitionSite) -> &str {
        std::str::from_utf8(&source[site.byte_start..site.byte_end]).unwrap()
    }

    fn python3() -> Option<PathBuf> {
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|_| PathBuf::from("python3"))
    }

    fn run_mock_lsp_pass(
        python: &Path,
        repo_root: &Path,
        source: &Path,
        target: &Path,
        ref_kind: RefKind,
        collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    ) -> WorkspaceFacts {
        let script = repo_root.join("mock_lsp.py");
        fs::write(&script, mock_lsp_script()).unwrap();
        let target_uri = Url::from_file_path(target).unwrap().as_str().to_string();
        run_lsp_definition_pass(
            LspDefinitionPass {
                analyzer_id: "mock-clangd-c-lsp",
                pool_analyzer_id: Some("mock-clangd-lsp"),
                language: "mock-clangd",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "c",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy::default(),
                collect_definition_sites: collect,
            },
            repo_root,
            &[WorkspaceFile {
                path: "main.c".into(),
                blob_sha: "blob".into(),
                worktree_path: Some(source.to_path_buf()),
            }],
        )
        .unwrap()
    }

    fn mock_lsp_script() -> &'static str {
        r#"
import json
import sys

target_uri = sys.argv[1]

def read_message():
    header = b""
    while not header.endswith(b"\r\n\r\n"):
        chunk = sys.stdin.buffer.read(1)
        if not chunk:
            return None
        header += chunk
    length = 0
    for line in header.decode("ascii").split("\r\n"):
        if line.lower().startswith("content-length:"):
            length = int(line.split(":", 1)[1].strip())
    if length == 0:
        return None
    return json.loads(sys.stdin.buffer.read(length))

def send(payload):
    data = json.dumps(payload).encode("utf-8")
    sys.stdout.buffer.write(b"Content-Length: " + str(len(data)).encode("ascii") + b"\r\n\r\n")
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

while True:
    msg = read_message()
    if msg is None:
        break
    method = msg.get("method")
    if "id" not in msg:
        continue
    if method == "initialize":
        result = {"capabilities": {"textDocumentSync": 2, "definitionProvider": True}}
    elif method == "textDocument/definition":
        result = [{
            "uri": target_uri,
            "range": {
                "start": {"line": 0, "character": 0},
                "end": {"line": 0, "character": 1}
            }
        }]
    elif method == "shutdown":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": None})
        break
    else:
        result = None
    send({"jsonrpc": "2.0", "id": msg["id"], "result": result})
"#
    }
}
