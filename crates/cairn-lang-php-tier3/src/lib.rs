//! PHP Tier-3 workspace analysis via PHPantom LSP.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::Result;
use cairn_core::lsp::Position;
#[cfg(test)]
use cairn_core::lsp::Url;
use cairn_core::lsp_discovery::discover_lsp_binary_candidates;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind,
    WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile, run_lsp_definition_pass,
};
use cairn_lang_api::LanguageBackend as _;
use cairn_lang_php::PhpBackend;
use linkme::distributed_slice;
use serde_json::json;
use tree_sitter::{Node, Parser};

use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::workspace_analyzer::WORKSPACE_ANALYZERS;

const ANALYZER_ID: &str = "phpantom-lsp";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "phpantom-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct PhpantomLspWorkspaceAnalyzer;

impl WorkspaceAnalyzer for PhpantomLspWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "php"
    }

    fn parser_id(&self) -> &'static str {
        PhpBackend.parser_id()
    }

    fn config_paths(&self) -> &'static [&'static str] {
        php_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_phpantom_lsp_passes(repo_root, files, progress)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_PHPANTOM_LSP_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(PhpantomLspWorkspaceAnalyzer);

fn php_config_paths() -> &'static [&'static str] {
    &[
        "composer.json",
        "composer.lock",
        "phpstan.neon",
        "phpstan.neon.dist",
        ".phpactor.json",
        ".phpactor.yml",
    ]
}

fn run_phpantom_lsp_passes(
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    let mut facts =
        run_phpantom_lsp_pass(repo_root, files, RefKind::Call, collect_calls, progress)?;
    let type_facts =
        run_phpantom_lsp_pass(repo_root, files, RefKind::Type, collect_type_refs, progress)?;
    facts.resolved_refs.extend(type_facts.resolved_refs);
    Ok(facts)
}

fn run_phpantom_lsp_pass(
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: ANALYZER_ID,
            // PHP has one Tier-1 backend here, so PHPantom LSP is already the
            // full pool identity. Shared pool ids are only needed for
            // multi-dialect crates such as clangd and TypeScript.
            pool_analyzer_id: None,
            language: "php",
            ref_kind,
            spawn_spec: LspSpawnSpec {
                binary: phpantom_lsp_binary(),
                workspace_root: repo_root.to_path_buf(),
                config_hash: POOL_CONFIG_ID.to_string(),
                request_timeout: REQUEST_TIMEOUT,
                availability: AvailabilityStrategy::VersionFlag,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "php",
                launch_args: Vec::new(),
                env: Vec::new(),
                initialization_options: json!({}),
            },
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

fn phpantom_lsp_binary() -> PathBuf {
    discover_lsp_binary_candidates(&["phpantom_lsp", "phpantom-lsp"], Some("PHPANTOM_LSP"))
        .unwrap_or_else(|| PathBuf::from("phpantom_lsp"))
}

fn collect_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::Call)
}

fn collect_type_refs(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::TypeRef)
}

#[derive(Debug, Clone, Copy)]
enum SiteKind {
    Call,
    TypeRef,
}

fn collect_sites(source: &[u8], site_kind: SiteKind) -> Result<Vec<DefinitionSite>> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_php::LANGUAGE_PHP.into())
        .expect("tree-sitter-php grammar is loadable");
    let Some(tree) = parser.parse(source, None) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    collect_sites_from_node(tree.root_node(), site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(node: Node<'_>, site_kind: SiteKind, out: &mut Vec<DefinitionSite>) {
    match site_kind {
        SiteKind::Call if is_call_expression(node.kind()) => {
            if let Some(site) = call_name(node) {
                out.push(site_from_node(site));
            }
        }
        SiteKind::TypeRef => {
            if let Some(site) = type_reference_site(node) {
                out.push(site_from_node(site));
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_sites_from_node(child, site_kind, out);
    }
}

fn is_call_expression(kind: &str) -> bool {
    matches!(
        kind,
        "function_call_expression"
            | "member_call_expression"
            | "nullsafe_member_call_expression"
            | "scoped_call_expression"
    )
}

fn call_name(node: Node<'_>) -> Option<Node<'_>> {
    node.child_by_field_name("function")
        .or_else(|| node.child_by_field_name("name"))
        .or_else(|| last_named_child(node))
        .and_then(rightmost_name)
}

fn rightmost_name(node: Node<'_>) -> Option<Node<'_>> {
    match node.kind() {
        "name" | "variable_name" => Some(node),
        "qualified_name" | "namespace_name" => last_named_child(node).and_then(rightmost_name),
        _ => last_named_child(node).and_then(rightmost_name),
    }
}

fn type_reference_site(node: Node<'_>) -> Option<Node<'_>> {
    if !matches!(node.kind(), "name" | "qualified_name" | "namespace_name") {
        return None;
    }
    if has_name_container_parent(node) {
        return None;
    }
    if is_declaration_name(node) || !is_type_usage_context(node) {
        return None;
    }
    Some(node)
}

fn has_name_container_parent(node: Node<'_>) -> bool {
    node.parent().is_some_and(|parent| {
        matches!(parent.kind(), "qualified_name" | "namespace_name")
            && contains_range(parent, node)
            && !same_range(parent, node)
    })
}

fn is_declaration_name(node: Node<'_>) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    matches!(
        parent.kind(),
        "class_declaration"
            | "interface_declaration"
            | "trait_declaration"
            | "enum_declaration"
            | "function_definition"
            | "method_declaration"
    ) && parent
        .child_by_field_name("name")
        .is_some_and(|name| same_range(name, node))
}

fn is_type_usage_context(node: Node<'_>) -> bool {
    // PHP's `name` nodes also appear in expressions. Keep Tier-3 type refs to
    // syntactic type positions plus conformance/heritage and instanceof sites;
    // declaration names are filtered separately to avoid self-resolving refs.
    let mut current = Some(node);
    while let Some(candidate) = current {
        let Some(parent) = candidate.parent() else {
            return false;
        };
        if child_field_contains(parent, "type", node)
            || child_field_contains(parent, "return_type", node)
            || child_field_contains(parent, "base", node)
            || child_field_contains(parent, "interfaces", node)
        {
            return true;
        }
        if matches!(
            parent.kind(),
            "base_clause"
                | "class_interface_clause"
                | "named_type"
                | "optional_type"
                | "union_type"
                | "intersection_type"
                | "cast_type"
                | "instanceof"
                | "instanceof_expression"
        ) {
            return true;
        }
        if is_instanceof_right_operand(parent, node) {
            return true;
        }
        if matches!(
            parent.kind(),
            "name" | "qualified_name" | "namespace_name" | "named_type" | "optional_type"
        ) {
            current = Some(parent);
            continue;
        }
        return false;
    }
    false
}

fn is_instanceof_right_operand(parent: Node<'_>, needle: Node<'_>) -> bool {
    if parent.kind() != "binary_expression"
        || !child_field_contains(parent, "right", needle)
        || !has_child_kind(parent, "instanceof")
    {
        return false;
    }
    true
}

fn has_child_kind(parent: Node<'_>, kind: &str) -> bool {
    let mut cursor = parent.walk();
    parent
        .children(&mut cursor)
        .any(|child| child.kind() == kind)
}

fn child_field_contains(parent: Node<'_>, field: &str, needle: Node<'_>) -> bool {
    parent
        .child_by_field_name(field)
        .is_some_and(|child| contains_range(child, needle))
}

fn contains_range(haystack: Node<'_>, needle: Node<'_>) -> bool {
    haystack.start_byte() <= needle.start_byte() && needle.end_byte() <= haystack.end_byte()
}

fn last_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).last()
}

fn same_range(left: Node<'_>, right: Node<'_>) -> bool {
    left.start_byte() == right.start_byte() && left.end_byte() == right.end_byte()
}

fn site_from_node(node: Node<'_>) -> DefinitionSite {
    let start = node.start_position();
    DefinitionSite {
        position: Position {
            line: u32::try_from(start.row).unwrap_or(u32::MAX),
            character: u32::try_from(start.column).unwrap_or(u32::MAX),
        },
        byte_start: node.start_byte(),
        byte_end: node.end_byte(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::workspace_analyzer::DefinitionRetryPolicy;
    use std::fs;
    use std::process::Command;

    #[test]
    fn call_collector_finds_function_member_and_scoped_calls() {
        let source = br#"<?php
namespace App;

build();
$service->execute();
$service?->maybe();
Helper::make();
"#;
        let sites = collect_calls(source).unwrap();
        let texts = sites
            .iter()
            .map(|site| source_text(source, site))
            .collect::<Vec<_>>();
        assert!(texts.contains(&"build"));
        assert!(texts.contains(&"execute"));
        assert!(texts.contains(&"maybe"));
        assert!(texts.contains(&"make"));
    }

    #[test]
    fn type_ref_collector_skips_declarations_and_keeps_usages() {
        let source = br#"<?php
namespace App;

class Declared extends BaseWidget implements IWidget {
    private Input|OtherInput $input;

    public function build(Widget $widget): Result {
        return $widget instanceof OtherWidget ? new Result() : new Result();
    }
}
"#;
        let sites = collect_type_refs(source).unwrap();
        let texts = sites
            .iter()
            .map(|site| source_text(source, site))
            .collect::<Vec<_>>();
        assert!(!texts.contains(&"Declared"));
        assert!(texts.contains(&"BaseWidget"));
        assert!(texts.contains(&"IWidget"));
        assert!(texts.contains(&"Input"));
        assert!(texts.contains(&"OtherInput"));
        assert!(texts.contains(&"Widget"));
        assert!(texts.contains(&"Result"));
        assert!(texts.contains(&"OtherWidget"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_method_call() {
        let Some(python) = python3() else {
            eprintln!("skipping mock LSP test: python3 not found");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Runner.php");
        let target = tmp.path().join("Service.php");
        fs::write(
            &source,
            "<?php\nclass Runner { public function run(Service $service): void { $service->execute(); } }\n",
        )
        .unwrap();
        fs::write(
            &target,
            "<?php\nclass Service { public function execute(): void {} }\n",
        )
        .unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_calls,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        let resolved = &facts.resolved_refs[0];
        assert_eq!(resolved.source_path, "Runner.php");
        assert_eq!(resolved.kind, RefKind::Call);
        assert_eq!(resolved.target_path.as_deref(), Some("Service.php"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_class_reference() {
        let Some(python) = python3() else {
            eprintln!("skipping mock LSP test: python3 not found");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Runner.php");
        let target = tmp.path().join("Widget.php");
        fs::write(
            &source,
            "<?php\nclass Runner { public function build(Widget $widget): Widget { return $widget; } }\n",
        )
        .unwrap();
        fs::write(&target, "<?php\nclass Widget {}\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Type,
            collect_type_refs,
        );

        assert!(!facts.resolved_refs.is_empty());
        let resolved = &facts.resolved_refs[0];
        assert_eq!(resolved.source_path, "Runner.php");
        assert_eq!(resolved.kind, RefKind::Type);
        assert_eq!(resolved.target_path.as_deref(), Some("Widget.php"));
    }

    fn source_text<'a>(source: &'a [u8], site: &DefinitionSite) -> &'a str {
        std::str::from_utf8(&source[site.byte_start..site.byte_end]).unwrap()
    }

    fn python3() -> Option<PathBuf> {
        for candidate in ["python3", "python"] {
            if Command::new(candidate).arg("--version").output().is_ok() {
                return Some(PathBuf::from(candidate));
            }
        }
        None
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
                analyzer_id: "mock-phpantom-lsp",
                pool_analyzer_id: None,
                language: "mock-php",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "php",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
                    env: Vec::new(),
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy::default(),
                collect_definition_sites: collect,
            },
            repo_root,
            &[WorkspaceFile {
                path: source.file_name().unwrap().to_string_lossy().to_string(),
                blob_sha: "blob".into(),
                worktree_path: Some(source.to_path_buf()),
            }],
            &AnalyzerProgress::default(),
        )
        .unwrap()
    }

    fn mock_lsp_script() -> &'static str {
        r#"import json
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
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": {"capabilities": {"definitionProvider": True}}})
    elif method == "initialized":
        pass
    elif method == "textDocument/didOpen":
        pass
    elif method == "textDocument/definition":
        send({
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": [{
                "uri": target_uri,
                "range": {
                    "start": {"line": 1, "character": 6},
                    "end": {"line": 1, "character": 12}
                }
            }]
        })
    elif method == "shutdown":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": None})
    elif method == "exit":
        break
"#
    }
}
