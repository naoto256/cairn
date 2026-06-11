//! TypeScript, JavaScript, and TSX Tier-3 workspace analyzers backed
//! by typescript-language-server.
//!
//! typescript-language-server fronts one TypeScript language service
//! for all three dialects. Cairn still registers one analyzer per
//! Tier-1 parser id so the workspace runner can feed each analyzer the
//! files that were actually parsed by that dialect. All three
//! analyzers share one `typescript-language-server-lsp` pool key.

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
use tree_sitter::{Language, Node, Parser};

const TYPESCRIPT_POOL_ID: &str = "typescript-language-server-lsp";
const TS_ANALYZER_ID: &str = "typescript-language-server-ts-lsp";
const JS_ANALYZER_ID: &str = "typescript-language-server-js-lsp";
const TSX_ANALYZER_ID: &str = "typescript-language-server-tsx-lsp";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "typescript-language-server-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, Copy)]
struct TsLanguage {
    analyzer_id: &'static str,
    language: &'static str,
    parser_id: &'static str,
    language_id: &'static str,
    tree_sitter_language: fn() -> Language,
}

const TS_LANGUAGE: TsLanguage = TsLanguage {
    analyzer_id: TS_ANALYZER_ID,
    language: "typescript",
    parser_id: "tree-sitter-typescript",
    language_id: "typescript",
    tree_sitter_language: ts_language,
};

const JS_LANGUAGE: TsLanguage = TsLanguage {
    analyzer_id: JS_ANALYZER_ID,
    language: "javascript",
    parser_id: "tree-sitter-javascript",
    language_id: "javascript",
    tree_sitter_language: js_language,
};

const TSX_LANGUAGE: TsLanguage = TsLanguage {
    analyzer_id: TSX_ANALYZER_ID,
    language: "tsx",
    parser_id: "tree-sitter-tsx",
    language_id: "typescriptreact",
    tree_sitter_language: tsx_language,
};

pub struct TypescriptLanguageServerTsAnalyzer;
pub struct TypescriptLanguageServerJsAnalyzer;
pub struct TypescriptLanguageServerTsxAnalyzer;

impl WorkspaceAnalyzer for TypescriptLanguageServerTsAnalyzer {
    fn id(&self) -> &'static str {
        TS_LANGUAGE.analyzer_id
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        TS_LANGUAGE.language
    }

    fn parser_id(&self) -> &'static str {
        TS_LANGUAGE.parser_id
    }

    fn config_paths(&self) -> &'static [&'static str] {
        ts_config_paths()
    }

    fn pool_group(&self) -> Option<&'static str> {
        Some(TYPESCRIPT_POOL_ID)
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_ts_passes(TS_LANGUAGE, repo_root, files, progress)
    }
}

impl WorkspaceAnalyzer for TypescriptLanguageServerJsAnalyzer {
    fn id(&self) -> &'static str {
        JS_LANGUAGE.analyzer_id
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        JS_LANGUAGE.language
    }

    fn parser_id(&self) -> &'static str {
        JS_LANGUAGE.parser_id
    }

    fn config_paths(&self) -> &'static [&'static str] {
        ts_config_paths()
    }

    fn pool_group(&self) -> Option<&'static str> {
        Some(TYPESCRIPT_POOL_ID)
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_ts_passes(JS_LANGUAGE, repo_root, files, progress)
    }
}

impl WorkspaceAnalyzer for TypescriptLanguageServerTsxAnalyzer {
    fn id(&self) -> &'static str {
        TSX_LANGUAGE.analyzer_id
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        TSX_LANGUAGE.language
    }

    fn parser_id(&self) -> &'static str {
        TSX_LANGUAGE.parser_id
    }

    fn config_paths(&self) -> &'static [&'static str] {
        ts_config_paths()
    }

    fn pool_group(&self) -> Option<&'static str> {
        Some(TYPESCRIPT_POOL_ID)
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_ts_passes(TSX_LANGUAGE, repo_root, files, progress)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_TS_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(TypescriptLanguageServerTsAnalyzer);

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_JS_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(TypescriptLanguageServerJsAnalyzer);

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_TSX_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(TypescriptLanguageServerTsxAnalyzer);

fn ts_config_paths() -> &'static [&'static str] {
    &["tsconfig.json", "jsconfig.json", "package.json"]
}

fn run_ts_passes(
    language: TsLanguage,
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    let mut facts = run_ts_pass(
        language,
        repo_root,
        files,
        RefKind::Call,
        call_collector_for(language),
        progress,
    )?;
    let import_facts = run_ts_pass(
        language,
        repo_root,
        files,
        RefKind::Import,
        import_collector_for(language),
        progress,
    )?;
    facts.resolved_refs.extend(import_facts.resolved_refs);
    Ok(facts)
}

fn run_ts_pass(
    language: TsLanguage,
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: language.analyzer_id,
            // typescript-language-server multiplexes TS, JS, and TSX through the
            // same TypeScript project service. Sharing the pool avoids starting
            // three servers for the same repo while preserving per-parser runs.
            pool_analyzer_id: Some(TYPESCRIPT_POOL_ID),
            language: "typescript-language-server",
            ref_kind,
            spawn_spec: typescript_spawn_spec(language, repo_root),
            retry: DefinitionRetryPolicy {
                retry_empty_definition: true,
                retry_file_not_found: true,
            },
            collect_definition_sites: collect,
        },
        repo_root,
        files,
        progress,
    )
}

fn typescript_spawn_spec(language: TsLanguage, repo_root: &Path) -> LspSpawnSpec {
    LspSpawnSpec {
        binary: typescript_language_server_binary(),
        workspace_root: repo_root.to_path_buf(),
        config_hash: POOL_CONFIG_ID.to_string(),
        request_timeout: REQUEST_TIMEOUT,
        availability: AvailabilityStrategy::VersionFlag,
        // typescript-language-server emits `$/typescriptVersion` and
        // `window/logMessage` but does not emit `$/progress` notifications, so
        // ProgressQuiescence would wait forever for a begin that never arrives.
        // Standalone measurement showed `textDocument/definition` returns in
        // ~2ms post-initialize, so we treat the initialize response as the
        // readiness signal and rely on request_timeout to bound any genuinely
        // slow request.
        readiness: ReadinessStrategy::InitializeResponseOnly,
        language_id: language.language_id,
        // Unlike clangd/pyright/gopls, this server requires an explicit stdio
        // transport flag. The env/PATH binary discovery stays separate so doctor
        // and runtime use the same executable rule.
        launch_args: vec!["--stdio".to_string()],
        env: Vec::new(),
        initialization_options: json!({}),
    }
}

fn call_collector_for(language: TsLanguage) -> fn(&[u8]) -> Result<Vec<DefinitionSite>> {
    match language.analyzer_id {
        TS_ANALYZER_ID => collect_ts_calls,
        JS_ANALYZER_ID => collect_js_calls,
        TSX_ANALYZER_ID => collect_tsx_calls,
        _ => collect_ts_calls,
    }
}

fn import_collector_for(language: TsLanguage) -> fn(&[u8]) -> Result<Vec<DefinitionSite>> {
    match language.analyzer_id {
        TS_ANALYZER_ID => collect_ts_imports,
        JS_ANALYZER_ID => collect_js_imports,
        TSX_ANALYZER_ID => collect_tsx_imports,
        _ => collect_ts_imports,
    }
}

fn typescript_language_server_binary() -> PathBuf {
    discover_lsp_binary(
        "typescript-language-server",
        Some("TYPESCRIPT_LANGUAGE_SERVER"),
    )
    .unwrap_or_else(|| PathBuf::from("typescript-language-server"))
}

fn collect_ts_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_calls(source, TS_LANGUAGE)
}

fn collect_js_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_calls(source, JS_LANGUAGE)
}

fn collect_tsx_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_calls(source, TSX_LANGUAGE)
}

fn collect_ts_imports(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_imports(source, TS_LANGUAGE)
}

fn collect_js_imports(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_imports(source, JS_LANGUAGE)
}

fn collect_tsx_imports(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_imports(source, TSX_LANGUAGE)
}

fn collect_calls(source: &[u8], language: TsLanguage) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, language, SiteKind::Call)
}

fn collect_imports(source: &[u8], language: TsLanguage) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, language, SiteKind::Import)
}

#[derive(Debug, Clone, Copy)]
enum SiteKind {
    Call,
    Import,
}

fn collect_sites(
    source: &[u8],
    language: TsLanguage,
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
    collect_sites_from_node(tree.root_node(), source, site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(
    node: Node<'_>,
    source: &[u8],
    site_kind: SiteKind,
    out: &mut Vec<DefinitionSite>,
) {
    match site_kind {
        SiteKind::Call if node.kind() == "call_expression" => {
            if let Some(identifier) = call_identifier(node) {
                out.push(site_from_node(identifier, 0, 0));
            }
        }
        SiteKind::Import if is_import_like(node.kind()) => {
            if let Some(source_node) = import_source_node(node) {
                out.push(string_content_site(source_node, source));
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_sites_from_node(child, source, site_kind, out);
    }
}

fn call_identifier(call: Node<'_>) -> Option<Node<'_>> {
    let function = call.child_by_field_name("function")?;
    match function.kind() {
        "identifier" | "property_identifier" => Some(function),
        "member_expression" | "subscript_expression" | "optional_chain" => {
            function.child_by_field_name("property")
        }
        "call_expression" => call_identifier(function),
        _ => last_identifier_child(function),
    }
}

fn last_identifier_child(node: Node<'_>) -> Option<Node<'_>> {
    if matches!(
        node.kind(),
        "identifier" | "property_identifier" | "shorthand_property_identifier"
    ) {
        return Some(node);
    }
    let mut cursor = node.walk();
    let children = node.named_children(&mut cursor).collect::<Vec<_>>();
    children.into_iter().rev().find_map(last_identifier_child)
}

fn is_import_like(kind: &str) -> bool {
    matches!(
        kind,
        "import_statement" | "export_statement" | "internal_module"
    )
}

fn import_source_node(node: Node<'_>) -> Option<Node<'_>> {
    if let Some(source) = node.child_by_field_name("source") {
        return Some(source);
    }
    // Tree-sitter's TS/JS grammars expose import/export sources consistently
    // today, but the fallback keeps older grammar shapes and re-export forms
    // from silently losing import refs if the field name is absent.
    find_first_string_child(node)
}

fn find_first_string_child(node: Node<'_>) -> Option<Node<'_>> {
    if is_string_literal(node.kind()) {
        return Some(node);
    }
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find_map(find_first_string_child)
}

fn is_string_literal(kind: &str) -> bool {
    matches!(kind, "string" | "string_fragment")
}

fn string_content_site(node: Node<'_>, source: &[u8]) -> DefinitionSite {
    let raw = node.utf8_text(source).unwrap_or_default();
    let offset = usize::from(
        raw.len() >= 2
            && ((raw.starts_with('"') && raw.ends_with('"'))
                || (raw.starts_with('\'') && raw.ends_with('\''))),
    );
    site_from_node(node, offset, offset)
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

fn ts_language() -> Language {
    tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
}

fn tsx_language() -> Language {
    tree_sitter_typescript::LANGUAGE_TSX.into()
}

fn js_language() -> Language {
    tree_sitter_javascript::LANGUAGE.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::lsp::Url;
    use std::fs;

    #[test]
    fn typescript_spawn_spec_uses_initialize_response_only() {
        let spec = typescript_spawn_spec(TS_LANGUAGE, Path::new("/tmp/repo"));

        assert_eq!(spec.request_timeout, REQUEST_TIMEOUT);
        assert!(matches!(
            spec.readiness,
            ReadinessStrategy::InitializeResponseOnly
        ));
    }

    #[test]
    fn typescript_analyzers_share_pool_group() {
        assert_eq!(
            TypescriptLanguageServerTsAnalyzer.pool_group(),
            Some(TYPESCRIPT_POOL_ID)
        );
        assert_eq!(
            TypescriptLanguageServerJsAnalyzer.pool_group(),
            Some(TYPESCRIPT_POOL_ID)
        );
        assert_eq!(
            TypescriptLanguageServerTsxAnalyzer.pool_group(),
            Some(TYPESCRIPT_POOL_ID)
        );
    }

    #[test]
    fn ts_collectors_find_calls_and_imports() {
        let source = br#"import { helper } from "./dep";
export { thing } from "./other";
function main() { return helper(); }
"#;

        let calls = collect_calls(source, TS_LANGUAGE).unwrap();
        let imports = collect_imports(source, TS_LANGUAGE).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(source_text(source, calls[0]), "helper");
        assert_eq!(imports.len(), 2);
        assert_eq!(source_text(source, imports[0]), "./dep");
        assert_eq!(source_text(source, imports[1]), "./other");
    }

    #[test]
    fn js_collector_finds_member_calls_and_imports() {
        let source = br#"import util from "./util.js";
export * from "./reexport.js";
client.render();
"#;

        let calls = collect_calls(source, JS_LANGUAGE).unwrap();
        let imports = collect_imports(source, JS_LANGUAGE).unwrap();

        assert_eq!(source_text(source, calls[0]), "render");
        assert_eq!(source_text(source, imports[0]), "./util.js");
        assert_eq!(source_text(source, imports[1]), "./reexport.js");
    }

    #[test]
    fn tsx_collector_keeps_typescript_calls_alive() {
        let source = br#"import { helper } from "./dep";
export function View() { return <button onClick={() => helper()} />; }
"#;

        let calls = collect_calls(source, TSX_LANGUAGE).unwrap();
        let imports = collect_imports(source, TSX_LANGUAGE).unwrap();
        let names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"helper"));
        assert_eq!(source_text(source, imports[0]), "./dep");
    }

    #[test]
    fn lsp_mock_resolves_cross_file_call() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.ts");
        let target = tmp.path().join("dep.ts");
        fs::write(&source, "import { helper } from './dep';\nhelper();\n").unwrap();
        fs::write(&target, "export function helper() {}\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_ts_calls,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.ts");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("dep.ts")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Call);
    }

    #[test]
    fn lsp_mock_resolves_cross_file_import() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.ts");
        let target = tmp.path().join("dep.ts");
        fs::write(&source, "import { helper } from './dep';\n").unwrap();
        fs::write(&target, "export function helper() {}\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Import,
            collect_ts_imports,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.ts");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("dep.ts")
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
                analyzer_id: "mock-typescript-language-server-ts-lsp",
                pool_analyzer_id: Some("mock-typescript-language-server-lsp"),
                language: "mock-typescript-language-server",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "typescript",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
                    env: Vec::new(),
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy::default(),
                collect_definition_sites: collect,
            },
            repo_root,
            &[WorkspaceFile {
                path: "main.ts".into(),
                blob_sha: "blob".into(),
                worktree_path: Some(source.to_path_buf()),
            }],
            &AnalyzerProgress::default(),
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
