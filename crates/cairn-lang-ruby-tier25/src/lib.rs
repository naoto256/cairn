//! Ruby Tier-2.5 in-process workspace analyzer.
//!
//! This crate is the tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-ruby-tier3` crate: it walks Ruby source with tree-sitter,
//! builds a per-workspace const/MRO/require graph, and emits resolution-layer
//! rows (`source = "tier25-ruby-resolver"`) for the cases that the grammar
//! can pin without LSP help.
//!
//! Scope (Stage 1, 1st wave):
//!
//! * Constant lookup: lexical → Module ancestors → top-level `Object` →
//!   autoload-declared paths. Qualified `Foo::Bar` resolves through the same
//!   chain.
//! * MRO walk: `include` / `extend` / `prepend` chains for static method
//!   dispatch on a known receiver type.
//! * Static method dispatch: receiver is a constant, `self`, or `super`.
//! * `require` / `require_relative` / `autoload` file resolution as Import
//!   resolutions.
//!
//! Out of scope (left to Tier-3 / never):
//!
//! * `obj.method` where the receiver type is unknown.
//! * `define_method` / `method_missing` / `send` / `public_send`.
//! * `eval` / `class_eval` / `instance_eval`.
//! * Refinements (`using`).
//! * Block / Proc / Lambda dispatch.
//! * Rails magic.

// linkme expands to an unsafe-tagged static; allow it locally while still
// deny-ing unsafe elsewhere via the workspace lint config.
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

pub const ANALYZER_ID: &str = "ruby-resolver";
pub const TIER_PREFIX: &str = "tier25";
pub const ANALYZER_REVISION: u32 = 2;
pub const PARSER_ID: &str = "tree-sitter-ruby";
pub const RESOLUTION_SOURCE: &str = "tier25-ruby-resolver";

/// In-process tree-sitter resolver for Ruby.
pub struct RubyTier25Analyzer;

impl WorkspaceAnalyzer for RubyTier25Analyzer {
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
        "ruby"
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
static REGISTER_RUBY_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(RubyTier25Analyzer);

/// Parse every visible Ruby file and emit resolutions across the workspace.
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

    // 2. Build cross-file const + require/autoload graphs.
    let file_paths: Vec<String> = per_file.iter().map(|(p, _, _)| p.clone()).collect();
    let const_index = ConstIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &file_paths);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for require / require_relative / autoload.
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
    let mut autoload_map: HashMap<String, String> = HashMap::new();
    for (path, _, facts) in &per_file {
        for (qualified, rel_target) in &facts.autoloads {
            if let Some(resolved) = require_graph::resolve_relative(path, rel_target, &file_paths) {
                autoload_map.insert(qualified.clone(), resolved);
            }
        }
    }

    for (path, _, facts) in &per_file {
        // Constant references emitted as Type resolutions (Foo, Foo::Bar, etc.)
        for cref in &facts.const_refs {
            let resolved =
                const_index.resolve(&cref.parts, &cref.lexical_scope, &mro, &autoload_map);
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: cref.byte_start..cref.byte_end,
                kind: ResolutionKind::Type,
                semantic_kind: None,
                target_path: resolved.as_ref().map(|r| r.path.clone()),
                target_qualified: resolved.map(|r| r.qualified),
            });
        }

        // Method calls — only when the receiver is statically known.
        for call in &facts.method_calls {
            let Some(resolved) = dispatch::resolve_call(call, &const_index, &mro, &methods) else {
                // Unresolvable (obj.foo, send, etc.) — Tier-2.5 does NOT
                // emit a "site observed" row for these. They belong to
                // Tier-3.
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
