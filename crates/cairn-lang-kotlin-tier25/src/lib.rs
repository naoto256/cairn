//! Kotlin Tier-2.5 in-process workspace analyzer.
//!
//! This crate is the tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-kotlin-tier3` crate: it walks Kotlin source with
//! tree-sitter, builds a per-workspace package / class / import graph,
//! and emits resolution-layer rows (`source =
//! "tier25-kotlin-resolver"`) for the cases that the grammar can pin
//! without LSP help.
//!
//! Scope (Stage 2, canary lead):
//!
//! * Class / object / interface / function lookup: import alias →
//!   in-package → wildcard-import → bare FQN. Workspace-local only —
//!   no kotlin-stdlib / Android SDK / external-jar resolution.
//! * MRO walk: single-superclass chain plus interfaces in declaration
//!   order; the `inherit` vs `implement` distinction is taken from
//!   the Tier-2 `constructor_invocation` heuristic.
//! * Static dispatch: `Cls.method(...)`, `Cls.Companion.method(...)`,
//!   `pkg.Cls.method(...)`, `this.method(...)`, `super.method(...)`,
//!   bare top-level `foo(...)` (including imported and wildcard-
//!   imported targets), and a best-effort unique-name match for
//!   extension functions on known receivers.
//! * `import` (plain, aliased, and wildcard) recorded as Import
//!   resolutions when the target lives in the workspace.
//!
//! Out of scope (left to Tier-3 / never):
//!
//! * `obj.method()` where `obj` is a local / parameter with unknown
//!   type (no annotation propagation).
//! * Reflection (`KFunction::invoke`, `Class.forName(...)`,
//!   `KClass.members`).
//! * Extension functions on dynamic receivers (where the receiver
//!   type can't be pinned).
//! * `when` expression branches that switch on type at runtime
//!   (smart-cast-driven dispatch).
//! * kotlin-stdlib / Android SDK / external-jar classpath resolution.
//! * JVM `invokevirtual` precedence across complex multi-interface
//!   diamonds (linearization is best-effort; precise rules live in
//!   Tier-3 once a JVM resolver is wired in).

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

use const_resolver::{FileConstFacts, ImportKind, PackageIndex};
use dispatch::MethodIndex;
use mro::Mro;
use require_graph::RequireGraph;

pub const ANALYZER_ID: &str = "kotlin-resolver";
pub const TIER_PREFIX: &str = "tier25";
// Bumped for resolutions.target_path persistence (schema v10): the persist
// layer now writes target_path directly into resolutions, so existing
// workspace_analysis_runs need to be invalidated and re-run to populate
// the new column. Analyzer logic itself is unchanged.
pub const ANALYZER_REVISION: u32 = 2;
pub const PARSER_ID: &str = "tree-sitter-kotlin-ng";
pub const RESOLUTION_SOURCE: &str = "tier25-kotlin-resolver";

/// In-process tree-sitter resolver for Kotlin.
pub struct KotlinTier25Analyzer;

impl WorkspaceAnalyzer for KotlinTier25Analyzer {
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
        "kotlin"
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
static REGISTER_KOTLIN_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(KotlinTier25Analyzer);

/// Parse every visible Kotlin file and emit resolutions across the
/// workspace. Public for unit-test access.
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

    // 2. Build cross-file package index + require graph.
    let package_index = PackageIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &package_index);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for `import …` statements whose
    // target lives in the workspace.
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

    // 4. Build MRO + method index + emit type-reference and dispatch
    // resolutions.
    let mro = Mro::build(&per_file, &package_index);
    let methods = MethodIndex::build(&per_file);

    // Per-file alias map: short-name → FQN. Wildcard imports don't
    // produce a single binding; they're consulted at call resolution
    // sites directly through `file_facts.import_bindings`.
    let mut alias_maps: HashMap<&str, HashMap<String, String>> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut m: HashMap<String, String> = HashMap::new();
        for binding in &facts.import_bindings {
            if binding.kind == ImportKind::Wildcard {
                continue;
            }
            if let Some(q) = require_graph.resolve_binding(path, binding) {
                m.insert(binding.local.clone(), q);
            }
        }
        alias_maps.insert(path.as_str(), m);
    }

    for (path, _, facts) in &per_file {
        let aliases = alias_maps.get(path.as_str()).cloned().unwrap_or_default();

        // Type references (base classes, etc.). The resolver re-derives
        // resolution using the same alias/in-package/wildcard cascade
        // the MRO uses, so the JOIN against ImplFact `interface_byte_
        // range` lines up.
        for tref in &facts.type_refs {
            let resolved = resolve_dotted(&tref.parts, &aliases, facts, &package_index);
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: tref.byte_start..tref.byte_end,
                kind: ResolutionKind::Type,
                semantic_kind: None,
                target_path: resolved.as_ref().map(|r| r.0.clone()),
                target_qualified: resolved.map(|r| r.1),
            });
        }

        // Method calls — only static shapes where the receiver type
        // is pinnable from the grammar.
        for call in &facts.method_calls {
            let Some(resolved) =
                dispatch::resolve_call(call, &package_index, &mro, &methods, &aliases, facts)
            else {
                // Unresolvable (`obj.method()`, reflection, etc.) —
                // Tier-2.5 does NOT emit a "site observed" row for
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

/// Resolve a dotted reference (a heritage base name, a constructor
/// site) under Kotlin lookup rules: alias-map → in-package →
/// wildcard-import → bare FQN. Returns `(target_path,
/// target_qualified)` only when the resolution pins to a workspace
/// file; `None` otherwise so the Tier-2.5 row records "site observed,
/// target external" with no target.
fn resolve_dotted(
    parts: &[String],
    aliases: &HashMap<String, String>,
    facts: &FileConstFacts,
    package_index: &PackageIndex,
) -> Option<(String, String)> {
    if parts.is_empty() {
        return None;
    }
    let head = &parts[0];
    let tail = if parts.len() > 1 {
        Some(parts[1..].join("."))
    } else {
        None
    };

    // 1. Alias substitution.
    if let Some(target) = aliases.get(head) {
        let candidate = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if let Some(hit) = package_index.lookup(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
        // Alias resolved a name we can't pin to a workspace file —
        // leave it unresolved at the path level (consumers see the
        // qualified via the Tier-2 ImportFact).
        return None;
    }

    // 2. In-package lookup.
    if let Some(pkg) = facts.package.as_deref().filter(|s| !s.is_empty()) {
        let candidate = format!("{pkg}.{}", parts.join("."));
        if let Some(hit) = package_index.lookup(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
    }

    // 3. Wildcard-import expansion.
    for b in &facts.import_bindings {
        if b.kind == ImportKind::Wildcard {
            let candidate = format!("{}.{}", b.fqn, parts.join("."));
            if let Some(hit) = package_index.lookup(&candidate) {
                return Some((hit.path.clone(), hit.qualified.clone()));
            }
        }
    }

    // 4. Bare FQN as written.
    let bare = parts.join(".");
    if let Some(hit) = package_index.lookup(&bare) {
        return Some((hit.path.clone(), hit.qualified.clone()));
    }

    None
}
