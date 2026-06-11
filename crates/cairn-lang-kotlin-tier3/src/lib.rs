//! Kotlin Tier-3 workspace analyzer backed by fwcd's kotlin-language-server.
//!
//! This crate follows the single-language Tier-3 pattern that
//! rust-tier3 / python-tier3 / go-tier3 / java-tier3 / ruby-tier3 /
//! swift-tier3 / csharp-tier3 / php-tier3 all use: one analyzer id, no
//! shared sibling pool, and tree-sitter collectors that ask the LSP to
//! resolve cross-file call and type definition sites.
//!
//! Two specifics worth flagging:
//!
//! * kotlin-language-server is JVM-backed and keeps importing Gradle /
//!   Maven projects after `initialize` resolves, so the launch spec uses
//!   `ProgressQuiescence` readiness with the same 180 s / 45 s timeouts
//!   jdtls uses — definition queries land on a settled index instead of
//!   a still-importing one.
//! * Cairn's call sites are usually written as `navigation_expression`
//!   chains (`obj.method()`). The collector reaches the *tail*
//!   identifier so the resolved definition is the method, not the
//!   receiver.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::Result;
use cairn_core::lsp::Position;
#[cfg(test)]
use cairn_core::lsp::Url;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::lsp_discovery::discover_lsp_binary;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind,
    WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile, run_lsp_definition_pass,
};
use cairn_lang_api::LanguageBackend as _;
use cairn_lang_kotlin::KotlinBackend;
use linkme::distributed_slice;
use serde_json::json;
use tree_sitter::{Node, Parser};

const ANALYZER_ID: &str = "kotlin-language-server";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "kotlin-language-server-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const WORKSPACE_LOAD_TIMEOUT: Duration = Duration::from_secs(180);

pub struct KotlinLanguageServerWorkspaceAnalyzer;

impl WorkspaceAnalyzer for KotlinLanguageServerWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "kotlin"
    }

    fn parser_id(&self) -> &'static str {
        KotlinBackend.parser_id()
    }

    fn config_paths(&self) -> &'static [&'static str] {
        kotlin_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_kotlin_language_server_passes(repo_root, files, progress)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_KOTLIN_LANGUAGE_SERVER_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(KotlinLanguageServerWorkspaceAnalyzer);

fn kotlin_config_paths() -> &'static [&'static str] {
    &[
        "build.gradle",
        "build.gradle.kts",
        "settings.gradle",
        "settings.gradle.kts",
        "pom.xml",
        ".kotlin-language-server",
    ]
}

fn run_kotlin_language_server_passes(
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    let mut facts = run_kotlin_language_server_pass(
        repo_root,
        files,
        RefKind::Call,
        collect_call_sites,
        progress,
    )?;
    let type_facts = run_kotlin_language_server_pass(
        repo_root,
        files,
        RefKind::Type,
        collect_type_refs,
        progress,
    )?;
    facts.resolved_refs.extend(type_facts.resolved_refs);
    Ok(facts)
}

fn run_kotlin_language_server_pass(
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: ANALYZER_ID,
            pool_analyzer_id: None,
            language: "kotlin",
            ref_kind,
            spawn_spec: LspSpawnSpec {
                binary: kotlin_language_server_binary(),
                workspace_root: repo_root.to_path_buf(),
                config_hash: POOL_CONFIG_ID.to_string(),
                request_timeout: REQUEST_TIMEOUT,
                // kotlin-language-server has no `--version` (or `--help`)
                // flag: jcommander throws ParameterException for any unknown
                // main parameter and the wrapper still exits 0, leaving stderr
                // noise that VersionFlag rejects. The binary we already
                // resolved is sufficient evidence of availability, so check
                // that the path is executable instead.
                availability: AvailabilityStrategy::PathExistsExecutable,
                // kotlin-language-server is JVM-backed like jdtls and can keep
                // importing Gradle/Maven projects after initialize. Waiting for
                // progress quiescence gives definition queries a settled index.
                readiness: ReadinessStrategy::ProgressQuiescence {
                    timeout: WORKSPACE_LOAD_TIMEOUT,
                },
                language_id: "kotlin",
                launch_args: Vec::new(),
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

fn kotlin_language_server_binary() -> PathBuf {
    discover_lsp_binary("kotlin-language-server", Some("KOTLIN_LANGUAGE_SERVER"))
        .unwrap_or_else(|| PathBuf::from("kotlin-language-server"))
}

fn collect_call_sites(source: &[u8]) -> Result<Vec<DefinitionSite>> {
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
        .set_language(&tree_sitter_kotlin_ng::LANGUAGE.into())
        .expect("tree-sitter-kotlin-ng grammar is loadable");
    let Some(tree) = parser.parse(source, None) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    collect_sites_from_node(tree.root_node(), site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(node: Node<'_>, site_kind: SiteKind, out: &mut Vec<DefinitionSite>) {
    match site_kind {
        SiteKind::Call if node.kind() == "call_expression" => {
            if let Some(site) = call_identifier(node) {
                out.push(site_from_node(site));
            }
        }
        SiteKind::TypeRef if is_type_reference_site(node) => {
            out.push(site_from_node(node));
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_sites_from_node(child, site_kind, out);
    }
}

fn call_identifier(node: Node<'_>) -> Option<Node<'_>> {
    first_named_child(node).and_then(callee_identifier)
}

fn callee_identifier(node: Node<'_>) -> Option<Node<'_>> {
    match node.kind() {
        "identifier" => Some(node),
        "navigation_expression" => last_descendant_of_kind(node, "identifier"),
        "call_expression" => call_identifier(node),
        _ => last_descendant_of_kind(node, "identifier"),
    }
}

fn is_type_reference_site(node: Node<'_>) -> bool {
    if !matches!(node.kind(), "identifier" | "type_identifier") {
        return false;
    }
    if is_declaration_name(node) || !is_type_usage_context(node) {
        return false;
    }
    true
}

fn is_declaration_name(node: Node<'_>) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    matches!(
        parent.kind(),
        "class_declaration" | "object_declaration" | "type_alias" | "enum_entry"
    ) && parent
        .child_by_field_name("name")
        .or_else(|| parent.child_by_field_name("type"))
        .is_some_and(|name| same_range(name, node))
}

fn is_type_usage_context(node: Node<'_>) -> bool {
    // `type_identifier` appears in both declarations and real type positions.
    // Keep the LSP type pass to inheritance/delegation, signatures, property
    // types, and generic arguments; declaration names are filtered separately.
    let mut current = Some(node);
    while let Some(candidate) = current {
        let Some(parent) = candidate.parent() else {
            return false;
        };
        if child_field_contains(parent, "type", node)
            || child_field_contains(parent, "return_type", node)
            || child_field_contains(parent, "receiver_type", node)
        {
            return true;
        }
        if matches!(
            parent.kind(),
            "delegation_specifier"
                | "delegation_specifiers"
                | "user_type"
                | "nullable_type"
                | "non_nullable_type"
                | "type_arguments"
                | "type_projection"
                | "function_type"
        ) {
            return true;
        }
        if matches!(
            parent.kind(),
            "type_identifier"
                | "user_type"
                | "nullable_type"
                | "non_nullable_type"
                | "type_arguments"
                | "type_projection"
        ) {
            current = Some(parent);
            continue;
        }
        return false;
    }
    false
}

fn child_field_contains(parent: Node<'_>, field: &str, needle: Node<'_>) -> bool {
    parent
        .child_by_field_name(field)
        .is_some_and(|child| contains_range(child, needle))
}

fn contains_range(haystack: Node<'_>, needle: Node<'_>) -> bool {
    haystack.start_byte() <= needle.start_byte() && needle.end_byte() <= haystack.end_byte()
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

fn last_descendant_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    if node.kind() == kind {
        return Some(node);
    }
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .filter_map(|child| last_descendant_of_kind(child, kind))
        .last()
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
    use std::fs;
    use std::process::Command;

    #[test]
    fn call_collector_finds_bare_constructor_and_navigation_calls() {
        let source = br#"
class Runner {
    fun run(service: Service) {
        work()
        service.execute()
        Service.create().execute()
    }
}
"#;
        let sites = collect_call_sites(source).unwrap();
        let texts = sites
            .iter()
            .map(|site| source_text(source, site))
            .collect::<Vec<_>>();
        assert!(texts.contains(&"work"));
        assert!(texts.contains(&"execute"));
        assert!(texts.contains(&"create"));
    }

    #[test]
    fn type_ref_collector_skips_declarations_and_keeps_usages() {
        let source = br#"
class Declared<T : Bound>(private val input: Input) : Base(), IWidget {
    val items: List<OtherWidget> = emptyList()

    fun build(widget: Widget): Result<Widget> = Result(widget)
}

typealias Alias = Result<Widget>
"#;
        let sites = collect_type_refs(source).unwrap();
        let texts = sites
            .iter()
            .map(|site| source_text(source, site))
            .collect::<Vec<_>>();
        assert!(!texts.contains(&"Declared"));
        assert!(!texts.contains(&"Alias"));
        assert!(texts.contains(&"Bound"));
        assert!(texts.contains(&"Input"));
        assert!(texts.contains(&"Base"));
        assert!(texts.contains(&"IWidget"));
        assert!(texts.contains(&"List"));
        assert!(texts.contains(&"OtherWidget"));
        assert!(texts.contains(&"Widget"));
        assert!(texts.contains(&"Result"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_method_call() {
        let Some(python) = python3() else {
            eprintln!("skipping mock LSP test: python3 not found");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Runner.kt");
        let target = tmp.path().join("Service.kt");
        fs::write(
            &source,
            "class Runner { fun run(service: Service) { service.execute() } }\n",
        )
        .unwrap();
        fs::write(&target, "class Service { fun execute() {} }\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_call_sites,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        let resolved = &facts.resolved_refs[0];
        assert_eq!(resolved.source_path, "Runner.kt");
        assert_eq!(resolved.kind, RefKind::Call);
        assert_eq!(resolved.target_path.as_deref(), Some("Service.kt"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_class_reference() {
        let Some(python) = python3() else {
            eprintln!("skipping mock LSP test: python3 not found");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Runner.kt");
        let target = tmp.path().join("Widget.kt");
        fs::write(
            &source,
            "class Runner { fun build(widget: Widget): Widget = widget }\n",
        )
        .unwrap();
        fs::write(&target, "class Widget\n").unwrap();

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
        assert_eq!(resolved.source_path, "Runner.kt");
        assert_eq!(resolved.kind, RefKind::Type);
        assert_eq!(resolved.target_path.as_deref(), Some("Widget.kt"));
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
                analyzer_id: "mock-kotlin-language-server",
                pool_analyzer_id: None,
                language: "mock-kotlin",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "kotlin",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
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
                    "start": {"line": 0, "character": 6},
                    "end": {"line": 0, "character": 12}
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
