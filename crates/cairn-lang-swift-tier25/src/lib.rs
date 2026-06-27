//! Swift Tier-2.5 in-process workspace analyzer.
//!
//! This crate is the tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-swift-tier3` crate: it walks Swift source with
//! tree-sitter, builds a per-workspace module / class / import graph,
//! and emits resolution-layer rows (`source =
//! "tier25-swift-resolver"`) for the cases that the grammar can pin
//! without LSP help.
//!
//! Scope (Stage 2, wave 2A):
//!
//! * Class / struct / enum / protocol / function lookup: import-bound
//!   alias → in-module → bare FQN. Workspace-local only — no
//!   Foundation / UIKit / SwiftUI / Combine / SPM-external resolution.
//! * Heritage walk: a class declaration's inheritance list is treated
//!   as parents in declaration order. Swift's syntax cannot tell a
//!   class-superclass apart from a protocol conformance from the
//!   inheritance clause alone (the Tier-2 backend records every edge
//!   as `"inherit"` for the same reason), so MRO is a best-effort
//!   linearization that walks every resolved parent. Conflicts that
//!   would need protocol-witness-table precedence belong to Tier-3.
//! * Static dispatch: `Cls.method(...)`, `module.Cls.method(...)`,
//!   `self.method(...)`, `super.method(...)`, bare top-level
//!   `foo(...)` (including imported targets), and a best-effort
//!   unique-name match for protocol-extension default
//!   implementations on known receivers.
//! * `import` (plain) recorded as Import resolutions when the target
//!   lives in the workspace. Swift has no wildcard or aliased import
//!   syntax — `import struct Foundation.Date` is treated as a plain
//!   import on `Foundation.Date`.
//!
//! Out of scope (left to Tier-3 / never):
//!
//! * `obj.method()` where `obj` is a local / parameter with unknown
//!   type (no annotation propagation).
//! * Existential / `Any` / dynamic dispatch through `AnyObject`.
//! * Mirror, KVC, Objective-C `performSelector` style runtime
//!   dispatch.
//! * Foundation / UIKit / SwiftUI / Combine / Apple framework
//!   resolution — those stay tier-2-fact fallback.
//! * SPM external-target / external-package resolution.
//! * Protocol-witness-table precedence for diamond conformances
//!   (linearization is best-effort).

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

use const_resolver::{FileConstFacts, PackageIndex};
use dispatch::MethodIndex;
use mro::Mro;
use require_graph::RequireGraph;

pub const ANALYZER_ID: &str = "swift-resolver";
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
pub const ANALYZER_REVISION: u32 = 3;
pub const PARSER_ID: &str = "tree-sitter-swift";
pub const RESOLUTION_SOURCE: &str = "tier25-swift-resolver";

/// In-process tree-sitter resolver for Swift.
pub struct SwiftTier25Analyzer;

impl WorkspaceAnalyzer for SwiftTier25Analyzer {
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
        "swift"
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
static REGISTER_SWIFT_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(SwiftTier25Analyzer);

/// Parse every visible Swift file and emit resolutions across the
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

    // Per-file alias map: short-name → FQN. Swift imports have no
    // alias / wildcard syntax, so every binding is plain and a
    // `local → fqn` map is sufficient.
    let mut alias_maps: HashMap<&str, HashMap<String, String>> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut m: HashMap<String, String> = HashMap::new();
        for binding in &facts.import_bindings {
            if let Some(q) = require_graph.resolve_binding(path, binding) {
                m.insert(binding.local.clone(), q);
            }
        }
        alias_maps.insert(path.as_str(), m);
    }

    for (path, _, facts) in &per_file {
        let aliases = alias_maps.get(path.as_str()).cloned().unwrap_or_default();

        // Type references (base classes / protocols, etc.). The
        // resolver re-derives resolution using the same alias /
        // in-module / bare cascade the MRO uses, so the JOIN against
        // ImplFact `interface_byte_range` lines up.
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
    // v0.7.0 D PR: the runner pre-reads workspace files for
    // Tier-2.5 analyzers (`requires_materialized_files() == true`)
    // and attaches the bytes here. Reading `worktree_path` directly
    // would re-open a race window between the runner's readability
    // check and the analyzer's actual read.
    file.source_bytes.as_deref().map(<[u8]>::to_vec)
}

/// Resolve a dotted reference (a heritage base name, a constructor
/// site) under Swift lookup rules: alias-map (= imported binding) →
/// in-module → bare FQN. Returns `(target_path, target_qualified)`
/// only when the resolution pins to a workspace file; `None`
/// otherwise so the Tier-2.5 row records "site observed, target
/// external" with no target.
///
/// Swift has no wildcard import, so the `wildcard` branch from the
/// Kotlin counterpart is intentionally absent.
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

    // 1. Alias substitution (workspace-resolved imported binding).
    if let Some(target) = aliases.get(head) {
        let candidate = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if let Some(hit) = package_index.lookup(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
        return None;
    }

    // 2. In-module lookup. Swift has no `package` declaration, so the
    // module name is best-effort derived from the file's containing
    // `Package.swift` target (or `None` for loose files). When the
    // module is known, prepend it.
    if let Some(module) = facts.module.as_deref().filter(|s| !s.is_empty()) {
        let candidate = format!("{module}.{}", parts.join("."));
        if let Some(hit) = package_index.lookup(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
    }

    // 3. Bare FQN as written (covers root-module declarations and
    // composite names like `Outer.Inner`).
    let bare = parts.join(".");
    if let Some(hit) = package_index.lookup(&bare) {
        return Some((hit.path.clone(), hit.qualified.clone()));
    }

    None
}
