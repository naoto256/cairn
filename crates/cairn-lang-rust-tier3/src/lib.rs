//! Rust Tier-3 workspace analyzer.
//!
//! The syn Tier-2 analyzer records method calls by bare method name.
//! This crate asks rust-analyzer for the definition under each method
//! identifier so the core runner can persist a resolved
//! `target_qualified` ref. The LSP pipeline itself (pooling, document
//! sync, retry, path mapping) lives in cairn-core's definition-pass
//! substrate; this crate contributes the rust-analyzer launch spec and
//! the grammar-specific call-site extraction.
//!
//! `config_paths` returns the workspace files that feed the
//! analyzer-currency `config_hash`
//! (`cairn_core::workspace_analyzer::expected`):
//! `Cargo.toml`, `rust-toolchain.toml`, `rust-toolchain`. Editing any
//! forces a rust-analyzer re-run even when no source blob changed.
//!
//! Rust's semantic-progress readiness policy defers all definition
//! requests until rust-analyzer's `$/progress` stream is quiet,
//! bounded by immutable hard and semantic-stall deadlines. Alongside
//! that, `retry_empty_definition
//! = false` is a Rust-specific policy choice; readiness does not
//! determine retry policy. Other languages retain their existing
//! readiness strategies. File-backed runs also place rust-analyzer's
//! private Cargo target under the daemon-owned repository state directory.
//! This keeps proc-macro dylibs out of protected worktree locations and
//! makes their lifetime follow repository-store cleanup.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_core::lsp::Position;
use cairn_core::lsp::pool::{AvailabilityStrategy, LspSpawnSpec, ReadinessStrategy};
use cairn_core::lsp_discovery::discover_lsp_binary;
use cairn_core::manifest::ManifestId;
use cairn_core::paths::path_hash;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, DefinitionRetryPolicy, DefinitionSite, LspDefinitionPass, RefKind,
    WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceAnalyzerContext, WorkspaceFacts,
    WorkspaceFile, run_lsp_definition_pass_with_semantic_readiness,
};
use cairn_core::{Error, Result};
use linkme::distributed_slice;
use serde_json::{Value, json};
use tree_sitter::Node;

const ANALYZER_ID: &str = "rust-analyzer-lsp";
const ANALYZER_REVISION: u32 = 5;
const POOL_CONFIG_ID: &str = "rust-analyzer-lsp-v3";
const POOL_GROUP: &str = "rust-analyzer-cargo";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const WORKSPACE_LOAD_HARD_TIMEOUT: Duration = Duration::from_secs(120);
const WORKSPACE_LOAD_STALL_TIMEOUT: Duration = Duration::from_secs(90);

pub struct RustAnalyzerWorkspaceAnalyzer;

impl WorkspaceAnalyzer for RustAnalyzerWorkspaceAnalyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "rust"
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-rust"
    }

    fn uses_lsp_pool(&self) -> bool {
        true
    }

    fn defer_stall_watchdog_until_active_work(&self) -> bool {
        true
    }

    fn config_paths(&self) -> &'static [&'static str] {
        &["Cargo.toml", "rust-toolchain.toml", "rust-toolchain"]
    }

    fn pool_group(&self) -> Option<&'static str> {
        Some(POOL_GROUP)
    }

    fn analyze_workspace(
        &self,
        repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_rust_analyzer(repo_root, None, files, progress)
    }

    fn analyze_workspace_with_context(
        &self,
        context: &WorkspaceAnalyzerContext<'_>,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        run_rust_analyzer(context.repo_root(), context.state_dir(), files, progress)
    }
}

fn run_rust_analyzer(
    repo_root: &Path,
    workspace_state_dir: Option<&Path>,
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Result<WorkspaceFacts> {
    run_lsp_definition_pass_with_semantic_readiness(
        rust_analyzer_definition_pass(repo_root, workspace_state_dir)?,
        repo_root,
        files,
        progress,
        WORKSPACE_LOAD_STALL_TIMEOUT,
    )
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
static REGISTER_RUST_WORKSPACE_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(RustAnalyzerWorkspaceAnalyzer);

fn rust_analyzer_definition_pass(
    repo_root: &Path,
    workspace_state_dir: Option<&Path>,
) -> Result<LspDefinitionPass> {
    // rust-analyzer invokes Cargo for build scripts and proc macros even when
    // check-on-save is disabled. Keep those private artifacts with Cairn's
    // per-repository state instead of reusing the worktree's target directory.
    let target = rust_analyzer_target_config(workspace_state_dir)?;
    Ok(LspDefinitionPass {
        analyzer_id: ANALYZER_ID,
        pool_analyzer_id: None,
        language: "rust",
        ref_kind: RefKind::Call,
        spawn_spec: LspSpawnSpec {
            binary: rust_analyzer_binary(),
            workspace_root: repo_root.to_path_buf(),
            config_hash: target.pool_config_hash.clone(),
            request_timeout: REQUEST_TIMEOUT,
            availability: AvailabilityStrategy::VersionFlag,
            readiness: ReadinessStrategy::ProgressQuiescence {
                timeout: WORKSPACE_LOAD_HARD_TIMEOUT,
            },
            language_id: "rust",
            launch_args: Vec::new(),
            env: Vec::new(),
            initialization_options: rust_analyzer_initialization_options(
                &target.pool_config_hash,
                target.target_dir.as_deref(),
            ),
        },
        retry: DefinitionRetryPolicy {
            retry_empty_definition: false,
            retry_file_not_found: true,
        },
        collect_definition_sites: collect_method_calls,
        suppress_definition_targets_at_requested_sites: false,
    })
}

fn rust_analyzer_binary() -> PathBuf {
    discover_lsp_binary("rust-analyzer", Some("RUST_ANALYZER"))
        .unwrap_or_else(|| PathBuf::from("rust-analyzer"))
}

#[derive(Debug)]
struct RustAnalyzerTargetConfig {
    target_dir: Option<String>,
    pool_config_hash: String,
}

fn rust_analyzer_target_config(
    workspace_state_dir: Option<&Path>,
) -> Result<RustAnalyzerTargetConfig> {
    rust_analyzer_target_config_with_canonicalize(workspace_state_dir, |path| {
        std::fs::canonicalize(path)
    })
}

fn rust_analyzer_target_config_with_canonicalize<F>(
    workspace_state_dir: Option<&Path>,
    canonicalize: F,
) -> Result<RustAnalyzerTargetConfig>
where
    F: FnOnce(&Path) -> std::io::Result<PathBuf>,
{
    let Some(workspace_state_dir) = workspace_state_dir else {
        return Ok(RustAnalyzerTargetConfig {
            target_dir: None,
            pool_config_hash: format!("{POOL_CONFIG_ID}:target=default"),
        });
    };

    let state_dir = canonicalize(workspace_state_dir).map_err(Error::Io)?;
    if !state_dir.is_absolute() {
        return Err(Error::InvalidArgument(
            "rust-analyzer state directory did not resolve to an absolute path".into(),
        ));
    }
    let target_dir = state_dir.join("rust-analyzer-target");
    let Some(target_dir_str) = target_dir.to_str() else {
        return Err(Error::InvalidArgument(
            "rust-analyzer state directory is not valid UTF-8".into(),
        ));
    };

    Ok(RustAnalyzerTargetConfig {
        target_dir: Some(target_dir_str.to_owned()),
        pool_config_hash: format!("{POOL_CONFIG_ID}:target={}", path_hash(&target_dir)),
    })
}

fn rust_analyzer_initialization_options(config_hash: &str, target_dir: Option<&str>) -> Value {
    let mut options = json!({
        "cairnConfigHash": config_hash,
        "checkOnSave": { "enable": false },
        "experimental": {
            "serverStatusNotification": true
        },
    });
    if let Some(target_dir) = target_dir {
        options["cargo"]["targetDir"] = json!(target_dir);
    }
    options
}

fn collect_method_calls(source: &[u8]) -> Result<Vec<DefinitionSite>> {
    let language: tree_sitter::Language = tree_sitter_rust::LANGUAGE.into();
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter rust: {e}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| Error::InvalidArgument("tree-sitter rust parse failed".into()))?;
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
        "field_expression" => function.child_by_field_name("field"),
        "scoped_identifier" | "generic_function" | "scoped_type_identifier" => function
            .child_by_field_name("name")
            .or_else(|| last_identifier_child(function)),
        _ => None,
    }
}

fn last_identifier_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut found = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind().ends_with("identifier") {
            found = Some(child);
        } else if let Some(inner) = last_identifier_child(child) {
            found = Some(inner);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_analyzer_uses_bounded_semantic_readiness_without_flycheck() {
        let pass = rust_analyzer_definition_pass(Path::new("/tmp/repo"), None).unwrap();

        assert_eq!(ANALYZER_REVISION, 5);
        assert_eq!(
            RustAnalyzerWorkspaceAnalyzer.pool_group(),
            Some("rust-analyzer-cargo")
        );
        assert_eq!(POOL_CONFIG_ID, "rust-analyzer-lsp-v3");
        assert_eq!(
            pass.spawn_spec.initialization_options["checkOnSave"]["enable"],
            false
        );
        assert_eq!(
            pass.spawn_spec.config_hash,
            "rust-analyzer-lsp-v3:target=default"
        );
        assert_eq!(
            pass.spawn_spec.initialization_options["cairnConfigHash"],
            pass.spawn_spec.config_hash
        );
        assert!(
            pass.spawn_spec
                .initialization_options
                .get("cargo")
                .is_none()
        );
        assert!(matches!(
            pass.spawn_spec.readiness,
            ReadinessStrategy::ProgressQuiescence { timeout }
                if timeout == Duration::from_secs(120)
        ));
        assert_eq!(WORKSPACE_LOAD_STALL_TIMEOUT, Duration::from_secs(90));
    }

    #[test]
    fn rust_analyzer_target_follows_daemon_state_and_preserves_default_fallback() {
        let repo = Path::new("/tmp/repo");
        let first = rust_analyzer_definition_pass(repo, Some(Path::new("."))).unwrap();
        let repeated = rust_analyzer_definition_pass(repo, Some(Path::new("."))).unwrap();
        let second = rust_analyzer_definition_pass(repo, Some(Path::new(".."))).unwrap();
        let fallback = rust_analyzer_definition_pass(repo, None).unwrap();

        let first_target = first.spawn_spec.initialization_options["cargo"]["targetDir"]
            .as_str()
            .unwrap();
        assert!(Path::new(first_target).is_absolute());
        assert!(first_target.ends_with("rust-analyzer-target"));
        assert_eq!(
            first.spawn_spec.config_hash,
            repeated.spawn_spec.config_hash
        );
        assert_eq!(
            first.spawn_spec.initialization_options,
            repeated.spawn_spec.initialization_options
        );
        assert_ne!(first.spawn_spec.config_hash, second.spawn_spec.config_hash);
        assert_ne!(
            first.spawn_spec.config_hash,
            fallback.spawn_spec.config_hash
        );
        assert_ne!(
            second.spawn_spec.config_hash,
            fallback.spawn_spec.config_hash
        );
        assert!(
            fallback
                .spawn_spec
                .initialization_options
                .get("cargo")
                .is_none()
        );
    }

    #[test]
    fn rust_analyzer_target_path_failures_are_pre_spawn_errors() {
        let permission_error =
            rust_analyzer_target_config_with_canonicalize(Some(Path::new("state")), |_| {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            })
            .unwrap_err();
        assert!(matches!(
            permission_error,
            Error::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied
        ));

        let relative =
            rust_analyzer_target_config_with_canonicalize(Some(Path::new("state")), |_| {
                Ok(PathBuf::from("still-relative"))
            })
            .unwrap_err();
        assert!(matches!(relative, Error::InvalidArgument(_)));
    }

    #[cfg(unix)]
    #[test]
    fn rust_analyzer_rejects_non_utf8_target_before_spawn() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let mut invalid = PathBuf::from("/");
        invalid.push(OsString::from_vec(vec![0xff]));
        let error = rust_analyzer_target_config_with_canonicalize(Some(Path::new("state")), |_| {
            Ok(invalid)
        })
        .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn method_call_collector_finds_method_identifier_positions() {
        let source = br#"
struct Foo;
impl Foo {
    fn bar(&self) {}
}
fn main() {
    let f = Foo;
    f.bar();
    String::new();
}
"#;

        let calls = collect_method_calls(source).unwrap();

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].position.line, 7);
        assert_eq!(calls[0].position.character, 6);
        assert!(calls[0].byte_end > calls[0].byte_start);
    }
}
