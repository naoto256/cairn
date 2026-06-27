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
use require_graph::{RequireGraph, ResolvedBinding};

pub const ANALYZER_ID: &str = "kotlin-resolver";
pub const TIER_PREFIX: &str = "tier25";
// Bumped for resolutions.target_path persistence (schema v10): the persist
// layer now writes target_path directly into resolutions, so existing
// workspace_analysis_runs need to be invalidated and re-run to populate
// the new column. Analyzer logic itself is unchanged.
// Bumped for resolutions.manifest_id persistence (schema v11): the
// persist layer now scopes resolution rows to the writing manifest,
// so existing workspace_analysis_runs need to be invalidated and the
// analyzer re-run to repopulate rows with manifest_id Some. Analyzer
// logic itself is unchanged.
// Bumped for path-aware dispatch: `PackageIndex` and `RequireGraph`
// bindings now key by `(path, qualified)` so cross-file same-name
// class collisions no longer collapse to a first-hit, and the
// dispatch resolution chain validates every candidate before
// adoption. Stale `workspace_analysis_runs` rows must be invalidated
// so cached call edges that fell through to tier2-fact get re-run
// against the path-aware resolver.
//
// rev 5: dispatch chain gained a narrow Stage 7.5 that strips the
// JVM `<File>Kt` synthetic-class suffix for Java→Kotlin top-level
// function calls. `FooKt.bar()` from a Java caller now resolves to
// `com.foo.bar` when no literal workspace `class/object FooKt`
// exists. The normalization stays in front of the
// `alias_head_bound` short-circuit (PR #219 contract preserved) and
// never routes into `get_unique_by_name`. Pre-rev-5 runs would have
// fallen through to tier2-fact for these callsites; daemons must
// invalidate cached runs so the path-aware resolver re-records the
// edge. The PR #220 startup staleness scanner auto-enqueues the
// rerun.
// Bumped to 6 for CodeRabbit follow-ups #2/#10: import-edge
// resolutions now set `target_qualified = None` (contract clean-up
// matching the other Tier-2.5 backends), and root-package top-level
// functions are indexed in the package callable map (owner="" /
// package=None mapping) so bare `helper()` and synthetic
// `FileKt.helper()` calls resolve. Cached runs need invalidation.
pub const ANALYZER_REVISION: u32 = 6;
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

    fn requires_materialized_files(&self) -> bool {
        true
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
    //
    // Phase 1 contract (matches Ruby / JavaScript): Import-edge rows
    // record `target_path` only; `target_qualified` is forced to
    // `None`. The require_graph still computes the qualified name
    // internally (for the binding map / wildcard expansion), but
    // leaking it into the persisted row lets `persist.rs` path-scoped
    // lookup (`(blob_sha, parser_id, qualified)`) spuriously pin a
    // symbol_id for an import edge, turning a "no single target
    // file" import semantic into "specific symbol's file". The
    // manifest-wide fallback in persist.rs is gated on
    // `kind != Import`, but the path-scoped fast path runs first and
    // is not gated, so we scrub `target_qualified` at the source.
    for (path, _, _) in &per_file {
        for edge in require_graph.edges_for(path) {
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: edge.site_byte_start..edge.site_byte_end,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: edge.target_path.clone(),
                target_qualified: None,
            });
        }
    }

    // 4. Build MRO + method index + emit type-reference and dispatch
    // resolutions.
    let mro = Mro::build(&per_file, &package_index);
    let methods = MethodIndex::build(&per_file);

    // Per-file alias map: short-name → ResolvedBinding (path + FQN).
    // Wildcard imports don't produce a single binding; they're
    // consulted at call resolution sites directly through
    // `file_facts.import_bindings`. The binding carries `target_path`
    // so dispatch can use `PackageIndex::lookup_in_file` rather than a
    // path-agnostic fallback that loses cross-file collision info.
    let mut alias_maps: HashMap<&str, HashMap<String, ResolvedBinding>> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut m: HashMap<String, ResolvedBinding> = HashMap::new();
        for binding in &facts.import_bindings {
            if binding.kind == ImportKind::Wildcard {
                continue;
            }
            if let Some(rb) = require_graph.resolve_binding(path, binding) {
                m.insert(binding.local.clone(), rb.clone());
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
                dispatch::resolve_call(path, call, &package_index, &mro, &methods, &aliases, facts)
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
    // v0.7.0 D PR: the runner pre-reads workspace files for
    // Tier-2.5 analyzers (`requires_materialized_files() == true`)
    // and attaches the bytes here. Reading `worktree_path` directly
    // would re-open a race window between the runner's readability
    // check and the analyzer's actual read.
    file.source_bytes.as_deref().map(<[u8]>::to_vec)
}

/// Resolve a dotted reference (a heritage base name, a constructor
/// site) under Kotlin lookup rules: alias-map → in-package →
/// wildcard-import → bare FQN. Returns `(target_path,
/// target_qualified)` only when the resolution pins to a workspace
/// file; `None` otherwise so the Tier-2.5 row records "site observed,
/// target external" with no target.
fn resolve_dotted(
    parts: &[String],
    aliases: &HashMap<String, ResolvedBinding>,
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

    // 1. Alias substitution. If the import binding pinned a workspace
    // file, look the candidate up there directly — collisions on the
    // short qualified across other files must not steal this hit.
    if let Some(binding) = aliases.get(head) {
        let candidate = match &tail {
            Some(t) => format!("{}.{}", binding.target_qualified, t),
            None => binding.target_qualified.clone(),
        };
        if let Some(target_path) = &binding.target_path {
            if let Some(hit) = package_index.lookup_in_file(target_path, &candidate) {
                return Some((hit.path.clone(), hit.qualified.clone()));
            }
        }
        // External binding (kotlin-stdlib / jar) or workspace-resolved
        // import whose candidate doesn't exist in the bound file —
        // fall back to path-agnostic unique lookup. Collisions stay
        // None on purpose: better to under-resolve than to silently
        // pick the wrong file.
        if let Some(hit) = package_index.lookup_unique(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
        return None;
    }

    // 2. In-package lookup. The current file's package + parts is
    // expected to be unique workspace-wide, but we still go through
    // `lookup_unique` so a future duplicate-FQN bug doesn't masquerade
    // as a successful resolution.
    if let Some(pkg) = facts.package.as_deref().filter(|s| !s.is_empty()) {
        let candidate = format!("{pkg}.{}", parts.join("."));
        if let Some(hit) = package_index.lookup_unique(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
    }

    // 3. Wildcard-import expansion.
    for b in &facts.import_bindings {
        if b.kind == ImportKind::Wildcard {
            let candidate = format!("{}.{}", b.fqn, parts.join("."));
            if let Some(hit) = package_index.lookup_unique(&candidate) {
                return Some((hit.path.clone(), hit.qualified.clone()));
            }
        }
    }

    // 4. Bare FQN as written.
    let bare = parts.join(".");
    if let Some(hit) = package_index.lookup_unique(&bare) {
        return Some((hit.path.clone(), hit.qualified.clone()));
    }

    None
}
