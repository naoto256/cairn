//! Java Tier-3 workspace analyzer backed by Eclipse JDT Language Server.
//!
//! The crate mirrors the single-language Tier-3 shape used by rust-analyzer,
//! pyright, and gopls: one static `WORKSPACE_ANALYZERS` registration, one LSP
//! pool, and grammar-specific extraction of definition sites that jdtls resolves.

#![forbid(unsafe_code)]

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::lsp::Position;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::lsp_discovery::discover_lsp_binary;
use cairn_core::manifest::ManifestId;
use cairn_core::paths::path_hash;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind,
    WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile, run_lsp_definition_pass,
};
use cairn_core::{Error, Result};
use linkme::distributed_slice;
use serde_json::json;
use tracing::warn;
use tree_sitter::{Node, Parser};

const ANALYZER_ID: &str = "jdtls-lsp";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "jdtls-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const WORKSPACE_LOAD_TIMEOUT: Duration = Duration::from_secs(180);

pub struct JdtlsWorkspaceAnalyzer;

impl WorkspaceAnalyzer for JdtlsWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "java"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-java"
    }

    fn config_paths(&self) -> &'static [&'static str] {
        java_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_jdtls_passes(repo_root, files, progress)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_JDTLS_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(JdtlsWorkspaceAnalyzer);

fn java_config_paths() -> &'static [&'static str] {
    &[
        "pom.xml",
        "build.gradle",
        "build.gradle.kts",
        "settings.gradle",
        ".classpath",
        ".project",
    ]
}

fn run_jdtls_passes(
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    let mut facts = run_jdtls_pass(
        repo_root,
        files,
        RefKind::Call,
        collect_method_calls,
        progress,
    )?;
    let type_facts = run_jdtls_pass(repo_root, files, RefKind::Type, collect_type_refs, progress)?;
    facts.resolved_refs.extend(type_facts.resolved_refs);
    Ok(facts)
}

fn run_jdtls_pass(
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    let dirs = jdtls_workspace_dirs(repo_root)?;
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: ANALYZER_ID,
            pool_analyzer_id: None,
            language: "java",
            ref_kind,
            spawn_spec: LspSpawnSpec {
                binary: jdtls_binary(),
                workspace_root: repo_root.to_path_buf(),
                config_hash: POOL_CONFIG_ID.to_string(),
                request_timeout: REQUEST_TIMEOUT,
                availability: AvailabilityStrategy::PathExistsExecutable,
                // jdtls is a JVM-backed Eclipse server and may still be importing
                // Maven/Gradle/Eclipse projects after `initialize`. Waiting for
                // progress quiescence trades startup latency for stable refs.
                readiness: ReadinessStrategy::ProgressQuiescence {
                    timeout: WORKSPACE_LOAD_TIMEOUT,
                },
                language_id: "java",
                // jdtls requires writable OSGi configuration and workspace
                // metadata directories. Keep both outside the repo so indexing
                // never dirties the worktree, and derive them from the canonical
                // path to isolate sibling checkouts while preserving warm caches.
                launch_args: jdtls_launch_args(&dirs.configuration, &dirs.data),
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

fn jdtls_binary() -> PathBuf {
    discover_lsp_binary("jdtls", Some("JDTLS")).unwrap_or_else(|| PathBuf::from("jdtls"))
}

fn jdtls_launch_args(configuration_dir: &Path, data_dir: &Path) -> Vec<String> {
    vec![
        "-configuration".to_string(),
        configuration_dir.to_string_lossy().to_string(),
        "-data".to_string(),
        data_dir.to_string_lossy().to_string(),
    ]
}

fn jdtls_workspace_dir(repo_root: &Path) -> Result<PathBuf> {
    // jdtls expects stable per-workspace metadata dirs for its persistent
    // index and OSGi state. We derive one root from the canonicalised repo
    // path so sibling checkouts do not collide, while a single checkout
    // keeps warm caches across daemon restarts.
    let root = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    let data_dir = dirs::data_dir()
        .ok_or_else(|| Error::InvalidArgument("platform has no user data directory".into()))?;
    Ok(data_dir
        .join("cairn")
        .join("jdtls-workspaces")
        .join(path_hash(&root)))
}

fn jdtls_fallback_workspace_dir(repo_root: &Path) -> PathBuf {
    let root = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    std::env::temp_dir()
        .join("cairn-jdtls-workspaces")
        .join(path_hash(&root))
}

struct JdtlsWorkspaceDirs {
    configuration: PathBuf,
    data: PathBuf,
}

fn jdtls_workspace_dirs(repo_root: &Path) -> Result<JdtlsWorkspaceDirs> {
    match prepare_jdtls_workspace_dirs(jdtls_workspace_dir(repo_root)?) {
        Ok(dirs) => Ok(dirs),
        Err(error) if error.kind() == ErrorKind::PermissionDenied => {
            let fallback = jdtls_fallback_workspace_dir(repo_root);
            warn!(
                primary_error = %error,
                fallback = %fallback.display(),
                "falling back to temporary jdtls workspace directory"
            );
            prepare_jdtls_workspace_dirs(fallback).map_err(Error::Io)
        }
        Err(error) => Err(Error::Io(error)),
    }
}

fn prepare_jdtls_workspace_dirs(workspace_dir: PathBuf) -> std::io::Result<JdtlsWorkspaceDirs> {
    let dirs = JdtlsWorkspaceDirs {
        configuration: workspace_dir.join("configuration"),
        data: workspace_dir.join("data"),
    };
    std::fs::create_dir_all(&dirs.configuration)?;
    std::fs::create_dir_all(&dirs.data)?;
    Ok(dirs)
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::MethodCall)
}

fn collect_type_refs(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::TypeRef)
}

#[derive(Debug, Clone, Copy)]
enum SiteKind {
    MethodCall,
    TypeRef,
}

fn collect_sites(source: &[u8], site_kind: SiteKind) -> Result<Vec<DefinitionSite>> {
    let language: tree_sitter::Language = tree_sitter_java::LANGUAGE.into();
    let mut parser = Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter java: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter java parse failed".into()))?;
    let mut out = Vec::new();
    collect_sites_from_node(tree.root_node(), site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(node: Node<'_>, site_kind: SiteKind, out: &mut Vec<DefinitionSite>) {
    match site_kind {
        SiteKind::MethodCall if node.kind() == "method_invocation" => {
            if let Some(name) = node.child_by_field_name("name") {
                out.push(site_from_node(name));
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

fn is_type_reference_site(node: Node<'_>) -> bool {
    if !matches!(node.kind(), "type_identifier" | "scoped_type_identifier") {
        return false;
    }
    let Some(parent) = node.parent() else {
        return false;
    };
    // Definition queries on declaration names resolve back to the declaration
    // itself and add duplicate self-refs. Keep references that occur in field,
    // parameter, extends/implements, and expression type positions instead.
    !matches!(
        parent.kind(),
        "class_declaration"
            | "interface_declaration"
            | "enum_declaration"
            | "record_declaration"
            | "annotation_type_declaration"
            | "constructor_declaration"
    )
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
    fn jdtls_launch_args_include_configuration_and_data_dirs() {
        let args = jdtls_launch_args(
            Path::new("/tmp/cairn-jdtls/configuration"),
            Path::new("/tmp/cairn-jdtls/data"),
        );

        assert_eq!(
            args,
            [
                "-configuration",
                "/tmp/cairn-jdtls/configuration",
                "-data",
                "/tmp/cairn-jdtls/data"
            ]
        );
    }

    #[test]
    fn jdtls_workspace_dir_is_stable_and_outside_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        fs::create_dir(&repo).unwrap();

        let first = jdtls_workspace_dir(&repo).unwrap();
        let second = jdtls_workspace_dir(&repo).unwrap();

        assert_eq!(first, second);
        assert!(first.ends_with(path_hash(&repo.canonicalize().unwrap())));
        assert!(
            first
                .components()
                .any(|component| component.as_os_str() == "cairn")
        );
        assert!(!first.starts_with(&repo));
    }

    #[test]
    fn jdtls_fallback_workspace_dir_is_stable_and_outside_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        fs::create_dir(&repo).unwrap();

        let first = jdtls_fallback_workspace_dir(&repo);
        let second = jdtls_fallback_workspace_dir(&repo);

        assert_eq!(first, second);
        assert!(first.ends_with(path_hash(&repo.canonicalize().unwrap())));
        assert!(
            first
                .components()
                .any(|component| component.as_os_str() == "cairn-jdtls-workspaces")
        );
        assert!(!first.starts_with(&repo));
    }

    #[test]
    fn prepare_jdtls_workspace_dirs_creates_configuration_and_data_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let dirs = prepare_jdtls_workspace_dirs(tmp.path().join("workspace")).unwrap();

        assert!(dirs.configuration.is_dir());
        assert!(dirs.data.is_dir());
        assert_eq!(dirs.configuration.file_name().unwrap(), "configuration");
        assert_eq!(dirs.data.file_name().unwrap(), "data");
    }

    #[test]
    fn method_call_collector_finds_method_names() {
        let source = br#"
class Main {
  void run(Service service) {
    service.work();
    Service.make().work();
  }
}
"#;

        let calls = collect_method_calls(source).unwrap();
        let mut names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();
        names.sort_unstable();

        assert_eq!(names, ["make", "work", "work"]);
    }

    #[test]
    fn type_ref_collector_skips_declarations_and_keeps_usages() {
        let source = br#"
import pkg.Service;
class Main extends Base implements Runnable {
  Service service;
  void run(Service arg) {}
}
"#;

        let refs = collect_type_refs(source).unwrap();
        let names = refs
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"Service"));
        assert!(names.contains(&"Base"));
        assert!(names.contains(&"Runnable"));
        assert!(!names.contains(&"Main"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_method_call() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Main.java");
        let target = tmp.path().join("Service.java");
        fs::write(
            &source,
            "class Main { void run(Service service) { service.work(); } }\n",
        )
        .unwrap();
        fs::write(&target, "class Service { void work() {} }\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_method_calls,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "Main.java");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("Service.java")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Call);
    }

    #[test]
    fn lsp_mock_resolves_cross_file_class_reference() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Main.java");
        let target = tmp.path().join("Service.java");
        fs::write(&source, "class Main { Service service; }\n").unwrap();
        fs::write(&target, "class Service {}\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Type,
            collect_type_refs,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "Main.java");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("Service.java")
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
                analyzer_id: "mock-jdtls-lsp",
                pool_analyzer_id: None,
                language: "mock-java",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "java",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy::default(),
                collect_definition_sites: collect,
            },
            repo_root,
            &[WorkspaceFile {
                path: "Main.java".into(),
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
