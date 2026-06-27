//! TypeScript, JavaScript, and TSX Tier-3 workspace analyzers backed
//! by typescript-language-server.
//!
//! typescript-language-server fronts one TypeScript language service
//! for all three dialects. Cairn still registers one analyzer per
//! Tier-1 parser id so the workspace runner can feed each analyzer the
//! files that were actually parsed by that dialect. All three
//! analyzers share one `typescript-language-server-lsp` pool key.

#![forbid(unsafe_code)]

mod site_collector;

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::Result;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::lsp_discovery::discover_lsp_binary;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionCollector,
    LspMultiKindDefinitionPass, RefKind, WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts,
    WorkspaceFile, run_lsp_multi_kind_definition_pass,
};
use linkme::distributed_slice;
use serde_json::json;
use tree_sitter::Language;

use site_collector::{collect_calls, collect_imports};

const TYPESCRIPT_POOL_ID: &str = "typescript-language-server-lsp";
const TS_ANALYZER_ID: &str = "typescript-language-server-ts-lsp";
const JS_ANALYZER_ID: &str = "typescript-language-server-js-lsp";
const TSX_ANALYZER_ID: &str = "typescript-language-server-tsx-lsp";
const ANALYZER_REVISION: u32 = 2;
const POOL_CONFIG_ID: &str = "typescript-language-server-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, Copy)]
pub(crate) struct TsLanguage {
    pub(crate) analyzer_id: &'static str,
    pub(crate) language: &'static str,
    pub(crate) parser_id: &'static str,
    pub(crate) language_id: &'static str,
    pub(crate) tree_sitter_language: fn() -> Language,
}

pub(crate) const TS_LANGUAGE: TsLanguage = TsLanguage {
    analyzer_id: TS_ANALYZER_ID,
    language: "typescript",
    parser_id: "tree-sitter-typescript",
    language_id: "typescript",
    tree_sitter_language: ts_language,
};

pub(crate) const JS_LANGUAGE: TsLanguage = TsLanguage {
    analyzer_id: JS_ANALYZER_ID,
    language: "javascript",
    parser_id: "tree-sitter-javascript",
    language_id: "javascript",
    tree_sitter_language: js_language,
};

pub(crate) const TSX_LANGUAGE: TsLanguage = TsLanguage {
    analyzer_id: TSX_ANALYZER_ID,
    language: "tsx",
    parser_id: "tree-sitter-tsx",
    language_id: "typescriptreact",
    tree_sitter_language: tsx_language,
};

/// TypeScript Tier-3 workspace analyzer backed by typescript-language-server.
/// It reuses the shared LSP pool and only consumes files parsed by the
/// TypeScript Tier-1 backend.
pub struct TypescriptLanguageServerTsAnalyzer;

/// JavaScript Tier-3 workspace analyzer backed by typescript-language-server.
/// It shares the TypeScript language service pool while preserving the
/// JavaScript parser id for input selection.
pub struct TypescriptLanguageServerJsAnalyzer;

/// TSX Tier-3 workspace analyzer backed by typescript-language-server.
/// It uses the TypeScript React LSP language id while keeping a separate
/// analyzer id for run staleness and provenance.
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
    run_lsp_multi_kind_definition_pass(
        LspMultiKindDefinitionPass {
            analyzer_id: language.analyzer_id,
            // typescript-language-server multiplexes TS, JS, and TSX through the
            // same TypeScript project service. Sharing the pool avoids starting
            // three servers for the same repo while preserving per-parser runs.
            pool_analyzer_id: Some(TYPESCRIPT_POOL_ID),
            language: "typescript-language-server",
            spawn_spec: typescript_spawn_spec(language, repo_root),
            retry: DefinitionRetryPolicy {
                retry_empty_definition: true,
                retry_file_not_found: true,
            },
            collectors: vec![
                LspDefinitionCollector {
                    ref_kind: RefKind::Call,
                    collect_definition_sites: call_collector_for(language),
                },
                LspDefinitionCollector {
                    ref_kind: RefKind::Import,
                    collect_definition_sites: import_collector_for(language),
                },
            ],
            suppress_definition_targets_at_requested_sites: false,
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
        run_lsp_multi_kind_definition_pass(
            LspMultiKindDefinitionPass {
                analyzer_id: "mock-typescript-language-server-ts-lsp",
                pool_analyzer_id: Some("mock-typescript-language-server-lsp"),
                language: "mock-typescript-language-server",
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
                collectors: vec![LspDefinitionCollector {
                    ref_kind,
                    collect_definition_sites: collect,
                }],
                suppress_definition_targets_at_requested_sites: false,
            },
            repo_root,
            &[WorkspaceFile {
                path: "main.ts".into(),
                blob_sha: "blob".into(),
                worktree_path: Some(source.to_path_buf()),
                source_bytes: None,
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
