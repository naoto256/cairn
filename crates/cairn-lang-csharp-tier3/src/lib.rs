//! C# Tier-3 workspace analysis via `csharp-ls`.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::Result;
use cairn_core::lsp::Position;
#[cfg(test)]
use cairn_core::lsp::Url;
use cairn_core::lsp_discovery::discover_lsp_binary;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind,
    WorkspaceAnalyzer, WorkspaceFacts, WorkspaceFile, run_lsp_definition_pass,
};
use cairn_lang_api::LanguageBackend as _;
use cairn_lang_csharp::CsharpBackend;
use linkme::distributed_slice;
use serde_json::json;
use tree_sitter::{Node, Parser};

use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::workspace_analyzer::WORKSPACE_ANALYZERS;

const ANALYZER_ID: &str = "csharp-ls";
const ANALYZER_REVISION: u32 = 1;
const POOL_CONFIG_ID: &str = "csharp-ls-v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct CsharpLsWorkspaceAnalyzer;

impl WorkspaceAnalyzer for CsharpLsWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "csharp"
    }

    fn parser_id(&self) -> &'static str {
        CsharpBackend.parser_id()
    }

    fn config_paths(&self) -> &'static [&'static str] {
        csharp_config_paths()
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_csharp_ls_passes(repo_root, files, progress)
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_CSHARP_LS_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(CsharpLsWorkspaceAnalyzer);

fn csharp_config_paths() -> &'static [&'static str] {
    &[
        "*.csproj",
        "*.sln",
        "*.slnx",
        "global.json",
        "Directory.Packages.props",
        "Directory.Build.props",
    ]
}

fn run_csharp_ls_passes(
    repo_root: &Path,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    let mut facts = run_csharp_ls_pass(
        repo_root,
        files,
        RefKind::Call,
        collect_method_calls,
        progress,
    )?;
    let type_facts =
        run_csharp_ls_pass(repo_root, files, RefKind::Type, collect_type_refs, progress)?;
    facts.resolved_refs.extend(type_facts.resolved_refs);
    Ok(facts)
}

fn run_csharp_ls_pass(
    repo_root: &Path,
    files: &[WorkspaceFile],
    ref_kind: RefKind,
    collect: fn(&[u8]) -> Result<Vec<DefinitionSite>>,
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass(
        LspDefinitionPass {
            analyzer_id: ANALYZER_ID,
            // C# has one Tier-1 parser/backend here, so the analyzer id is
            // already the full pool identity. Shared pool ids are only needed
            // for multi-dialect crates such as clangd and TypeScript.
            pool_analyzer_id: None,
            language: "csharp",
            ref_kind,
            spawn_spec: LspSpawnSpec {
                binary: csharp_ls_binary(),
                workspace_root: repo_root.to_path_buf(),
                config_hash: POOL_CONFIG_ID.to_string(),
                request_timeout: REQUEST_TIMEOUT,
                availability: AvailabilityStrategy::VersionFlag,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "csharp",
                launch_args: Vec::new(),
                env: dotnet_env(),
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

fn csharp_ls_binary() -> PathBuf {
    discover_lsp_binary("csharp-ls", Some("CSHARP_LS"))
        .unwrap_or_else(|| PathBuf::from("csharp-ls"))
}

fn dotnet_env() -> Vec<(String, String)> {
    // T4 showed launchd's minimal environment can make csharp-ls see zero
    // MSBuild instances: MSBuildLocator.QueryVisualStudioInstances returns an
    // empty sequence, then csharp-ls fails initializeMSBuild with
    // InvalidOperationException. Supplying DOTNET_ROOT for the standard SDK
    // install fixes the same binary/workspace without changing PATH globally.
    dotnet_env_with(
        std::env::var_os("DOTNET_ROOT"),
        std::env::var_os("PATH"),
        standard_dotnet_roots(),
    )
}

fn dotnet_env_with(
    dotnet_root: Option<OsString>,
    current_path: Option<OsString>,
    roots: impl IntoIterator<Item = PathBuf>,
) -> Vec<(String, String)> {
    if dotnet_root.is_some() {
        return Vec::new();
    }
    let Some(root) = roots.into_iter().find(|root| root.join("sdk").is_dir()) else {
        return Vec::new();
    };
    vec![
        ("DOTNET_ROOT".into(), root.to_string_lossy().into_owned()),
        ("PATH".into(), prepend_path(&root, current_path)),
    ]
}

fn standard_dotnet_roots() -> Vec<PathBuf> {
    let mut roots = vec![
        PathBuf::from("/usr/local/share/dotnet"),
        PathBuf::from("/opt/homebrew/share/dotnet"),
        PathBuf::from("/opt/homebrew/opt/dotnet/libexec"),
    ];
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".dotnet"));
    }
    roots
}

fn prepend_path(root: &Path, current_path: Option<OsString>) -> String {
    let paths = std::iter::once(root.to_path_buf()).chain(
        current_path
            .as_deref()
            .into_iter()
            .flat_map(std::env::split_paths),
    );
    std::env::join_paths(paths)
        .unwrap_or_else(|_| root.as_os_str().to_os_string())
        .to_string_lossy()
        .into_owned()
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
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_c_sharp::LANGUAGE.into())
        .expect("tree-sitter-c-sharp grammar is loadable");
    let Some(tree) = parser.parse(source, None) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    collect_sites_from_node(tree.root_node(), site_kind, &mut out);
    Ok(out)
}

fn collect_sites_from_node(node: Node<'_>, site_kind: SiteKind, out: &mut Vec<DefinitionSite>) {
    match site_kind {
        SiteKind::MethodCall if node.kind() == "invocation_expression" => {
            if let Some(function) = node
                .child_by_field_name("function")
                .or_else(|| first_named_child(node))
                .and_then(call_identifier)
            {
                out.push(site_from_node(function));
            }
        }
        SiteKind::TypeRef => {
            if let Some(site) = type_reference_identifier(node) {
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

fn call_identifier(node: Node<'_>) -> Option<Node<'_>> {
    match node.kind() {
        "identifier" => Some(node),
        "generic_name" => node
            .child_by_field_name("name")
            .or_else(|| first_named_child(node)),
        "member_access_expression"
        | "member_binding_expression"
        | "conditional_access_expression" => node
            .child_by_field_name("name")
            .and_then(call_identifier)
            .or_else(|| last_named_child(node).and_then(call_identifier)),
        "qualified_name" => last_named_child(node).and_then(call_identifier),
        _ => None,
    }
}

fn type_reference_identifier(node: Node<'_>) -> Option<Node<'_>> {
    let site = match node.kind() {
        "identifier" => node,
        "generic_name" => node
            .child_by_field_name("name")
            .or_else(|| first_named_child(node))?,
        _ => return None,
    };

    if is_declaration_name(site) || !is_type_usage_context(node) {
        return None;
    }
    Some(site)
}

fn is_declaration_name(node: Node<'_>) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    matches!(
        parent.kind(),
        "class_declaration"
            | "struct_declaration"
            | "interface_declaration"
            | "record_declaration"
            | "enum_declaration"
            | "delegate_declaration"
            | "method_declaration"
            | "constructor_declaration"
            | "property_declaration"
    ) && parent
        .child_by_field_name("name")
        .is_some_and(|name| same_range(name, node))
}

fn is_type_usage_context(node: Node<'_>) -> bool {
    // C# uses `identifier` for both expression names and type names. Keep the
    // LSP pass focused on syntactic type positions: declared type fields,
    // base/conformance lists, and generic argument/constraint containers.
    // Declaration names are filtered separately to avoid self-resolving refs.
    let mut current = Some(node);
    while let Some(candidate) = current {
        let Some(parent) = candidate.parent() else {
            return false;
        };
        if child_field_contains(parent, "type", node)
            || child_field_contains(parent, "returns", node)
            || child_field_contains(parent, "return_type", node)
            || child_field_contains(parent, "base", node)
        {
            return true;
        }
        if matches!(
            parent.kind(),
            "base_list"
                | "type_argument_list"
                | "type_parameter_constraints_clause"
                | "type_constraint"
                | "array_type"
                | "nullable_type"
                | "pointer_type"
                | "qualified_name"
        ) {
            return true;
        }
        if matches!(
            parent.kind(),
            "identifier"
                | "generic_name"
                | "qualified_name"
                | "array_type"
                | "nullable_type"
                | "pointer_type"
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
    fn method_call_collector_finds_invocation_names() {
        let source = br#"
class Runner {
    void Run(Service service) {
        service.Execute();
        Helper.Make<string>();
        Local();
    }
}
"#;
        let sites = collect_method_calls(source).unwrap();
        let texts = sites
            .iter()
            .map(|site| source_text(source, site))
            .collect::<Vec<_>>();
        assert!(texts.contains(&"Execute"));
        assert!(texts.contains(&"Make"));
        assert!(texts.contains(&"Local"));
    }

    #[test]
    fn type_ref_collector_skips_declarations_and_keeps_usages() {
        let source = br#"
class Declared : BaseWidget, IWidget {
    Result<string> Build(Input input) {
        List<OtherWidget> copy = new List<OtherWidget>();
        return null;
    }
    public Output Value { get; }
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
        assert!(texts.contains(&"Result"));
        assert!(texts.contains(&"Input"));
        assert!(texts.contains(&"List"));
        assert!(texts.contains(&"OtherWidget"));
        assert!(texts.contains(&"Output"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_method_call() {
        let Some(python) = python3() else {
            eprintln!("skipping mock LSP test: python3 not found");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Runner.cs");
        let target = tmp.path().join("Service.cs");
        fs::write(
            &source,
            "class Runner { void Run(Service service) { service.Execute(); } }\n",
        )
        .unwrap();
        fs::write(&target, "class Service { public void Execute() {} }\n").unwrap();

        let facts = run_mock_lsp_pass(
            &python,
            tmp.path(),
            &source,
            &target,
            RefKind::Call,
            collect_method_calls,
        );

        assert_eq!(facts.resolved_refs.len(), 1);
        let resolved = &facts.resolved_refs[0];
        assert_eq!(resolved.source_path, "Runner.cs");
        assert_eq!(resolved.kind, RefKind::Call);
        assert_eq!(resolved.target_path.as_deref(), Some("Service.cs"));
    }

    #[test]
    fn lsp_mock_resolves_cross_file_type_reference() {
        let Some(python) = python3() else {
            eprintln!("skipping mock LSP test: python3 not found");
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("Runner.cs");
        let target = tmp.path().join("Widget.cs");
        fs::write(
            &source,
            "class Runner { Widget Build(Widget input) => input; }\n",
        )
        .unwrap();
        fs::write(&target, "class Widget {}\n").unwrap();

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
        assert_eq!(resolved.source_path, "Runner.cs");
        assert_eq!(resolved.kind, RefKind::Type);
        assert_eq!(resolved.target_path.as_deref(), Some("Widget.cs"));
    }

    #[test]
    fn dotnet_env_respects_existing_dotnet_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");
        std::fs::create_dir_all(root.join("sdk")).unwrap();

        let env = dotnet_env_with(
            Some(OsString::from("/already/set")),
            Some(OsString::from("/usr/bin")),
            [root],
        );

        assert!(env.is_empty());
    }

    #[test]
    fn dotnet_env_sets_root_and_prepends_path_for_existing_sdk_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");
        std::fs::create_dir_all(root.join("sdk")).unwrap();

        let env = dotnet_env_with(None, Some(OsString::from("/usr/bin")), [root.clone()]);

        assert_eq!(
            env,
            vec![
                (
                    "DOTNET_ROOT".to_string(),
                    root.to_string_lossy().into_owned(),
                ),
                (
                    "PATH".to_string(),
                    std::env::join_paths([root.as_path(), Path::new("/usr/bin")])
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                ),
            ]
        );
    }

    #[test]
    fn dotnet_env_is_empty_when_no_sdk_root_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");
        std::fs::create_dir_all(&root).unwrap();

        let env = dotnet_env_with(None, Some(OsString::from("/usr/bin")), [root]);

        assert!(env.is_empty());
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
                analyzer_id: "mock-csharp-ls",
                pool_analyzer_id: None,
                language: "mock-csharp",
                ref_kind,
                spawn_spec: LspSpawnSpec {
                    binary: python.to_path_buf(),
                    workspace_root: repo_root.to_path_buf(),
                    config_hash: target_uri.clone(),
                    request_timeout: Duration::from_secs(5),
                    availability: AvailabilityStrategy::VersionFlag,
                    readiness: ReadinessStrategy::InitializeResponseOnly,
                    language_id: "csharp",
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
