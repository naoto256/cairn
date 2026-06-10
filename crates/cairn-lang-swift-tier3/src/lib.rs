//! Swift Tier-3 workspace analyzer backed by Apple's sourcekit-lsp.
//!
//! This crate follows the single-language Tier-3 pattern: one analyzer id,
//! no shared sibling pool, and tree-sitter collectors that ask sourcekit-lsp
//! to resolve cross-file call and type definition sites.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::Command;
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
use tree_sitter::{Node, Parser};

const ANALYZER_ID: &str = "sourcekit-lsp";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "sourcekit-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct SourcekitLspWorkspaceAnalyzer;

impl WorkspaceAnalyzer for SourcekitLspWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "swift"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-swift"
    }

    fn config_paths(&self) -> &'static [&'static str] {
        swift_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
    ) -> Result<WorkspaceFacts> {
        run_sourcekit_lsp_passes(repo_root, files)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_SOURCEKIT_LSP_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(SourcekitLspWorkspaceAnalyzer);

fn swift_config_paths() -> &'static [&'static str] {
    &[
        "Package.swift",
        "Package.resolved",
        ".swift-version",
        ".swift-format",
        "compile_commands.json",
    ]
}

fn run_sourcekit_lsp_passes(repo_root: &Path, files: &[WorkspaceFile]) -> Result<WorkspaceFacts> {
    let mut facts = run_sourcekit_lsp_pass(repo_root, files, RefKind::Call, collect_call_sites)?;
    let type_facts = run_sourcekit_lsp_pass(repo_root, files, RefKind::Type, collect_type_refs)?;
    facts.resolved_refs.extend(type_facts.resolved_refs);
    Ok(facts)
}

fn run_sourcekit_lsp_pass(
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: ANALYZER_ID,
            // Swift has a single Tier-1 parser/backend here. Keeping the pool key
            // equal to the analyzer id avoids the shared-pool indirection needed
            // only by multi-dialect crates like clangd and TypeScript.
            pool_analyzer_id: None,
            language: "swift",
            ref_kind,
            spawn_spec: LspSpawnSpec {
                binary: sourcekit_lsp_binary(),
                workspace_root: repo_root.to_path_buf(),
                config_hash: POOL_CONFIG_ID.to_string(),
                request_timeout: REQUEST_TIMEOUT,
                availability: AvailabilityStrategy::VersionFlag,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "swift",
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
    )
}

fn sourcekit_lsp_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("SOURCEKIT_LSP").map(PathBuf::from) {
        return path;
    }
    sourcekit_lsp_from_xcrun().unwrap_or_else(|| PathBuf::from("sourcekit-lsp"))
}

fn sourcekit_lsp_from_xcrun() -> Option<PathBuf> {
    // On macOS, sourcekit-lsp commonly lives inside the active Xcode toolchain
    // and is not guaranteed to be on PATH. `xcrun --find` follows xcode-select,
    // which keeps Cairn aligned with the developer's selected toolchain.
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("xcrun")
            .args(["--find", "sourcekit-lsp"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let path = String::from_utf8(output.stdout).ok()?;
        let path = PathBuf::from(path.trim());
        path.is_file().then_some(path)
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

fn collect_call_sites(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::Call)
}

fn collect_type_refs(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::Type)
}

#[derive(Debug, Clone, Copy)]
enum SiteKind {
    Call,
    Type,
}

fn collect_sites(source: &[u8], site_kind: SiteKind) -> Result<Vec<DefinitionSite>> {
    let language: tree_sitter::Language = tree_sitter_swift::LANGUAGE.into();
    let mut parser = Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter swift: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter swift parse failed".into()))?;
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
            if let Some(callee) = first_named_child(node).and_then(call_identifier) {
                out.push(site_from_node(callee));
            }
        }
        SiteKind::Type if is_type_reference_site(node, source) => out.push(site_from_node(node)),
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_sites_from_node(child, source, site_kind, out);
    }
}

fn call_identifier(node: Node<'_>) -> Option<Node<'_>> {
    match node.kind() {
        "simple_identifier" => Some(node),
        "navigation_expression" => {
            let suffix = node.child_by_field_name("suffix")?;
            let member = first_named_child(suffix)?;
            (member.kind() == "simple_identifier").then_some(member)
        }
        _ => None,
    }
}

fn is_type_reference_site(node: Node<'_>, source: &[u8]) -> bool {
    if node.kind() != "type_identifier" {
        return false;
    }
    // Definition queries on declaration names resolve back to the declaration
    // itself and duplicate Tier-2 symbols. Keep inheritance/conformance,
    // parameter, return, property, and expression type usages instead.
    !is_declaration_type_name(node, source)
}

fn is_declaration_type_name(node: Node<'_>, source: &[u8]) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    if matches!(parent.kind(), "class_declaration" | "protocol_declaration")
        && parent
            .child_by_field_name("name")
            .is_some_and(|name| same_range(name, node))
    {
        return true;
    }
    if parent.kind() != "user_type" {
        return false;
    }
    let Some(grandparent) = parent.parent() else {
        return false;
    };
    grandparent.kind() == "class_declaration"
        && grandparent
            .child_by_field_name("declaration_kind")
            .is_some_and(|kind| node_text(kind, source) == "extension")
        && grandparent
            .child_by_field_name("name")
            .is_some_and(|name| same_range(name, parent))
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

fn same_range(left: Node<'_>, right: Node<'_>) -> bool {
    left.start_byte() == right.start_byte() && left.end_byte() == right.end_byte()
}

fn node_text<'a>(node: Node<'_>, source: &'a [u8]) -> &'a str {
    node.utf8_text(source).unwrap_or_default()
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
    use cairn_core::lsp::Url;
    use std::fs;

    #[test]
    fn call_collector_finds_bare_and_navigation_calls() {
        let source = br#"
func run(service: Service) {
  helper()
  service.work()
  Service.make().work()
}
"#;

        let calls = collect_call_sites(source).unwrap();
        let mut names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();
        names.sort_unstable();

        assert_eq!(names, ["helper", "make", "work", "work"]);
    }

    #[test]
    fn type_ref_collector_skips_declaration_names_and_keeps_usages() {
        let source = br#"
protocol Repo {}
struct Main: Repo {
  let service: Service
  func run(_ arg: Service) -> Result<Service> { fatalError() }
}
extension Main: Codable {}
"#;

        let refs = collect_type_refs(source).unwrap();
        let names = refs
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"Repo"));
        assert!(names.contains(&"Service"));
        assert!(names.contains(&"Result"));
        assert!(names.contains(&"Codable"));
        assert!(!names.contains(&"Main"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_method_call() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.swift");
        let target = tmp.path().join("service.swift");
        fs::write(
            &source,
            "func run(service: Service) {\n  service.work()\n}\n",
        )
        .unwrap();
        fs::write(&target, "struct Service { func work() {} }\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_call_sites,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.swift");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("service.swift")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Call);
    }

    #[test]
    fn lsp_mock_resolves_cross_file_type_reference() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.swift");
        let target = tmp.path().join("service.swift");
        fs::write(&source, "let service: Service\n").unwrap();
        fs::write(&target, "struct Service {}\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Type,
            collect_type_refs,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.swift");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("service.swift")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Type);
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
                analyzer_id: "mock-sourcekit-lsp",
                pool_analyzer_id: None,
                language: "mock-swift",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "swift",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy::default(),
                collect_definition_sites: collect,
            },
            repo_root,
            &[WorkspaceFile {
                path: "main.swift".into(),
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
