//! PHP Tier-2.5 in-process workspace analyzer.
//!
//! This crate is the tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-php-tier3` crate: it walks PHP source with tree-sitter,
//! builds a per-workspace const/MRO/use-graph, and emits resolution-layer
//! rows (`source = "tier25-php-resolver"`) for the cases that the grammar
//! can pin without LSP help.
//!
//! Scope (Stage 1, 2nd wave):
//!
//! * Class / interface / trait / enum lookup: lexical (within the
//!   current namespace + `use` imports) → fully-qualified `\Foo\Bar` →
//!   PSR-4 best-effort lookup by path.
//! * MRO walk: `extends` + `implements` + `use TraitA;` chain for
//!   static method dispatch on a known receiver type.
//! * Static method dispatch: `Foo::bar()`, `self::bar()`,
//!   `parent::bar()`, `static::bar()`.
//! * `use Foo\Bar;` import resolution as Import resolution rows, using
//!   the workspace's class index (Composer PSR-4 best-effort: map
//!   `\Foo\Bar\Baz` to any file whose top-level class qualifies as
//!   `Foo\Bar\Baz`).
//!
//! Out of scope (left to Tier-3 / never):
//!
//! * `$obj->method()` where the receiver type is unknown.
//! * `call_user_func` / `call_user_func_array` / variadic dispatch.
//! * `eval` / `create_function`.
//! * Magic methods (`__call`, `__callStatic`) routing.
//! * Variable-class / variable-method dispatch.
//! * Closure / first-class-callable resolution.

#![deny(unsafe_code)]

use std::collections::HashMap;
use std::path::Path;

use cairn_core::Result;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, ResolutionKind, WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts,
    WorkspaceFile, WorkspaceResolution,
};
use linkme::distributed_slice;

pub mod const_resolver;
pub mod dispatch;
pub mod mro;
pub mod require_graph;

#[cfg(test)]
mod tests;

use const_resolver::{ConstIndex, FileConstFacts};
use dispatch::MethodIndex;
use mro::Mro;
use require_graph::RequireGraph;

pub const ANALYZER_ID: &str = "php-resolver";
pub const TIER_PREFIX: &str = "tier25";
pub const ANALYZER_REVISION: u32 = 1;
pub const PARSER_ID: &str = "tree-sitter-php";
pub const RESOLUTION_SOURCE: &str = "tier25-php-resolver";

/// In-process tree-sitter resolver for PHP.
pub struct PhpTier25Analyzer;

impl WorkspaceAnalyzer for PhpTier25Analyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn tier_prefix(&self) -> &'static str {
        TIER_PREFIX
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "php"
    }

    fn parser_id(&self) -> &'static str {
        PARSER_ID
    }

    fn analyze_workspace(
        &self,
        _repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        let resolutions = analyze_files(files, progress);
        Ok(WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions,
        })
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
#[allow(unsafe_code)]
static REGISTER_PHP_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(PhpTier25Analyzer);

/// Parse every visible PHP file and emit resolutions across the workspace.
/// Public for unit-test access.
#[must_use]
pub fn analyze_files(
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Vec<WorkspaceResolution> {
    // 1. Per-file parse + extract facts.
    let mut per_file: Vec<(String, Vec<u8>, FileConstFacts)> = Vec::new();
    for f in files {
        if progress.is_cancelled() {
            break;
        }
        let Some(source) = read_blob(f) else {
            progress.tick();
            continue;
        };
        let facts = match const_resolver::parse_file(&source) {
            Some(f) => f,
            None => {
                progress.tick();
                continue;
            }
        };
        per_file.push((f.path.clone(), source, facts));
        progress.tick();
    }

    // 2. Build cross-file const + use-import graphs.
    let const_index = ConstIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &const_index);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for `use` statements that point at a
    // workspace-defined symbol.
    for (path, _, _) in &per_file {
        for edge in require_graph.edges_for(path) {
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: edge.site_byte_start..edge.site_byte_end,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: edge.target_path.clone(),
                target_qualified: edge.target_qualified.clone(),
            });
        }
    }

    // 4. Build MRO + method index + emit constant-reference and dispatch
    // resolutions.
    let mro = Mro::build(&per_file, &const_index);
    let methods = MethodIndex::build(&per_file);

    // Per-file `use` alias map (alias-name → fully-qualified target).
    let mut alias_maps: HashMap<&str, HashMap<String, String>> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut m: HashMap<String, String> = HashMap::new();
        for u in &facts.use_imports {
            m.insert(u.alias.clone(), u.qualified.clone());
        }
        alias_maps.insert(path.as_str(), m);
    }

    for (path, _, facts) in &per_file {
        let aliases = alias_maps.get(path.as_str()).cloned().unwrap_or_default();

        // Constant / type references (`Foo`, `Foo\Bar`, `\Foo\Bar`).
        for cref in &facts.const_refs {
            let resolved = const_index.resolve(
                &cref.parts,
                cref.absolute,
                cref.namespace.as_deref(),
                &aliases,
            );
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: cref.byte_start..cref.byte_end,
                kind: ResolutionKind::Type,
                semantic_kind: None,
                target_path: resolved.as_ref().map(|r| r.path.clone()),
                target_qualified: resolved.map(|r| r.qualified),
            });
        }

        // Method calls — only static / self / parent / static:: shapes
        // where the receiver type is pinnable from the grammar.
        for call in &facts.method_calls {
            let Some(resolved) =
                dispatch::resolve_call(call, &const_index, &mro, &methods, &aliases)
            else {
                // Unresolvable (`$obj->method()`, `call_user_func`, etc.)
                // — Tier-2.5 does NOT emit a "site observed" row for
                // these. They belong to Tier-3.
                continue;
            };
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: call.byte_start..call.byte_end,
                kind: ResolutionKind::Call,
                semantic_kind: None,
                target_path: Some(resolved.path),
                target_qualified: Some(resolved.qualified),
            });
        }
    }

    resolutions
}

fn read_blob(file: &WorkspaceFile) -> Option<Vec<u8>> {
    let path = file.worktree_path.as_ref()?;
    std::fs::read(path).ok()
}
