//! Ruby Tier-3 workspace analyzer backed by Shopify ruby-lsp.
//!
//! This is a single-language Tier-3 crate: one analyzer id, one LSP process
//! pool, and tree-sitter collectors that ask ruby-lsp to resolve sites that
//! Tier-2 can only keep as name-level Ruby facts.

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
use tree_sitter::{Node, Parser};

const ANALYZER_ID: &str = "ruby-lsp";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "ruby-lsp-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const GEMFILE_WITHOUT_LOCK_REASON: &str =
    "Gemfile without Gemfile.lock; run bundle install to enable ruby-lsp";

pub struct RubyLspWorkspaceAnalyzer;

impl WorkspaceAnalyzer for RubyLspWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "ruby"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-ruby"
    }

    fn config_paths(&self) -> &'static [&'static str] {
        ruby_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_ruby_lsp_passes(repo_root, files, progress)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_RUBY_LSP_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(RubyLspWorkspaceAnalyzer);

fn ruby_config_paths() -> &'static [&'static str] {
    &[
        "Gemfile",
        "Gemfile.lock",
        ".rubocop.yml",
        ".ruby-version",
        ".ruby-lsp/",
    ]
}

fn run_ruby_lsp_passes(
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    preflight_workspace(repo_root)?;
    let mut facts = run_ruby_lsp_pass(
        repo_root,
        files,
        RefKind::Call,
        collect_method_calls,
        progress,
    )?;
    let type_facts = run_ruby_lsp_pass(
        repo_root,
        files,
        RefKind::Type,
        collect_constant_refs,
        progress,
    )?;
    facts.resolved_refs.extend(type_facts.resolved_refs);
    Ok(facts)
}

fn preflight_workspace(repo_root: &Path) -> Result<()> {
    if repo_root.join("Gemfile").is_file() && !repo_root.join("Gemfile.lock").is_file() {
        return Err(cairn_core::lsp::Error::WorkspaceUnsuitable(
            GEMFILE_WITHOUT_LOCK_REASON.into(),
        )
        .into());
    }
    Ok(())
}

fn run_ruby_lsp_pass(
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
            language: "ruby",
            ref_kind,
            spawn_spec: LspSpawnSpec {
                binary: ruby_lsp_binary(),
                workspace_root: repo_root.to_path_buf(),
                config_hash: POOL_CONFIG_ID.to_string(),
                request_timeout: REQUEST_TIMEOUT,
                availability: AvailabilityStrategy::VersionFlag,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "ruby",
                // ruby-lsp often runs as `bundle exec ruby-lsp`, but Cairn's LSP
                // pool owns a single executable path today. Prefer the standalone
                // wrapper now; project-local Bundler launch can be layered later.
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

fn ruby_lsp_binary() -> PathBuf {
    discover_lsp_binary("ruby-lsp", Some("RUBY_LSP")).unwrap_or_else(|| PathBuf::from("ruby-lsp"))
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::MethodCall)
}

fn collect_constant_refs(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, SiteKind::ConstantRef)
}

#[derive(Debug, Clone, Copy)]
enum SiteKind {
    MethodCall,
    ConstantRef,
}

fn collect_sites(source: &[u8], site_kind: SiteKind) -> Result<Vec<DefinitionSite>> {
    let language: tree_sitter::Language = tree_sitter_ruby::LANGUAGE.into();
    let mut parser = Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter ruby: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter ruby parse failed".into()))?;
    let mut out = Vec::new();
    collect_sites_from_node(tree.root_node(), site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(node: Node<'_>, site_kind: SiteKind, out: &mut Vec<DefinitionSite>) {
    match site_kind {
        SiteKind::MethodCall if is_call_like(node.kind()) => {
            if let Some(method) = node.child_by_field_name("method") {
                out.push(site_from_node(method));
            }
        }
        SiteKind::ConstantRef if is_constant_reference_site(node) => {
            out.push(site_from_node(node));
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_sites_from_node(child, site_kind, out);
    }
}

fn is_call_like(kind: &str) -> bool {
    matches!(kind, "call" | "command" | "method_call")
}

fn is_constant_reference_site(node: Node<'_>) -> bool {
    if node.kind() != "constant" {
        return false;
    }
    let Some(parent) = node.parent() else {
        return false;
    };
    // Declaration constants resolve to themselves and would duplicate Tier-2
    // symbol rows. Tier-3 focuses on cross-file usage sites: superclass,
    // mixins, constructor calls, annotations, and other constant reads.
    !matches!(parent.kind(), "class" | "module")
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
    fn method_call_collector_finds_receiver_and_command_calls() {
        let source = br#"
require "service"
Service.new.call
helper :value
"#;

        let calls = collect_method_calls(source).unwrap();
        let names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"require"));
        assert!(names.contains(&"new"));
        assert!(names.contains(&"call"));
        assert!(names.contains(&"helper"));
    }

    #[test]
    fn constant_collector_skips_class_and_module_declarations() {
        let source = br#"
module App
  class Main < Service
    include Mixin
    def build
      Service.new
    end
  end
end
"#;

        let refs = collect_constant_refs(source).unwrap();
        let names = refs
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"Service"));
        assert!(names.contains(&"Mixin"));
        assert!(!names.contains(&"App"));
        assert!(!names.contains(&"Main"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_method_call() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.rb");
        let target = tmp.path().join("service.rb");
        fs::write(&source, "require './service'\nService.new.call\n").unwrap();
        fs::write(&target, "class Service\n  def call; end\nend\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_method_calls,
        );

        assert_eq!(facts.resolved_refs.len(), 3);
        assert_eq!(facts.resolved_refs[0].source_path, "main.rb");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("service.rb")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Call);
    }

    #[test]
    fn lsp_mock_resolves_cross_file_class_reference() {
        let Some(python) = python3() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("main.rb");
        let target = tmp.path().join("service.rb");
        fs::write(&source, "Service.new\n").unwrap();
        fs::write(&target, "class Service; end\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Type,
            collect_constant_refs,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        assert_eq!(facts.resolved_refs[0].source_path, "main.rb");
        assert_eq!(
            facts.resolved_refs[0].target_path.as_deref(),
            Some("service.rb")
        );
        assert_eq!(facts.resolved_refs[0].kind, RefKind::Type);
    }

    #[test]
    fn preflight_rejects_gemfile_without_lockfile() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("Gemfile"),
            "source 'https://rubygems.org'\n",
        )
        .unwrap();

        let err = preflight_workspace(tmp.path()).unwrap_err();

        assert!(matches!(
            err,
            Error::Lsp(cairn_core::lsp::Error::WorkspaceUnsuitable(reason))
                if reason == GEMFILE_WITHOUT_LOCK_REASON
        ));
    }

    #[test]
    fn preflight_accepts_gemfile_with_lockfile() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("Gemfile"),
            "source 'https://rubygems.org'\n",
        )
        .unwrap();
        fs::write(tmp.path().join("Gemfile.lock"), "GEM\n").unwrap();

        preflight_workspace(tmp.path()).unwrap();
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
                analyzer_id: "mock-ruby-lsp",
                pool_analyzer_id: None,
                language: "mock-ruby",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "ruby",
                    launch_args: vec![script.to_string_lossy().to_string(), target_uri],
                    env: Vec::new(),
                    initialization_options: json!({}),
                },
                retry: DefinitionRetryPolicy::default(),
                collect_definition_sites: collect,
            },
            repo_root,
            &[WorkspaceFile {
                path: "main.rb".into(),
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
