//! C# Tier-2.5 in-process workspace analyzer.
//!
//! Tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-csharp-tier3`: walks C# source with tree-sitter-c-sharp,
//! builds a per-workspace namespace / type / `using` graph, and emits
//! resolution-layer rows (`source = "tier25-csharp-resolver"`) for the
//! cases the grammar can pin without LSP help.
//!
//! Scope (Stage 2, Wave 2A):
//!
//! * Class / struct / interface / record / enum lookup: alias →
//!   same-namespace → containing namespace → `using`-imported →
//!   `using static` → bare FQN. Workspace-local only — BCL
//!   (`System.*`) and NuGet packages are *not* resolved.
//! * Type hierarchy: first base = class/struct/record/interface;
//!   subsequent bases = interfaces. The Tier-2 backend collapses both
//!   to `kind="inherit"`, so we mirror that and just append all
//!   resolved bases.
//! * Static dispatch: `Cls.Method(...)`, `ns.Cls.Method(...)`,
//!   `this.Method(...)`, `base.Method(...)`, bare top-level `Foo()`
//!   resolved via `using static` or same-namespace, and best-effort
//!   unique-name extension-method match.
//! * `using` (plain, alias, `using static`, `global using`) recorded
//!   as Import resolutions when the target lives in the workspace.
//! * Partial classes: declarations across files with the same
//!   namespace + class name are merged into one type.
//!
//! Out of scope (Tier-3 / never):
//!
//! * `obj.Method()` where `obj` is a local / parameter / property of
//!   unresolved type (no type inference at Tier-2.5).
//! * `dynamic` dispatch, reflection (`MethodInfo.Invoke`,
//!   `Type.GetMethod`), expression trees (`Expression<Func<…>>`).
//! * BCL / NuGet / `Microsoft.Extensions.*` external resolution —
//!   target_path stays `None` (Tier-2 fact fallback).
//! * `using` directive scope-limiting (a `using` inside a namespace
//!   block technically only applies inside that block; we treat all
//!   `using` directives as file-scoped, matching the Tier-1 backend's
//!   model).

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

pub const ANALYZER_ID: &str = "csharp-resolver";
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
pub const PARSER_ID: &str = "tree-sitter-c-sharp";
pub const RESOLUTION_SOURCE: &str = "tier25-csharp-resolver";

/// In-process tree-sitter resolver for C#.
pub struct CSharpTier25Analyzer;

impl WorkspaceAnalyzer for CSharpTier25Analyzer {
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
        "csharp"
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
static REGISTER_CSHARP_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(CSharpTier25Analyzer);

/// Parse every visible C# file and emit resolutions across the
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

    // 2. Build cross-file namespace index + using graph.
    let package_index = PackageIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &package_index);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for `using` directives whose target
    // lives in the workspace.
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

    // Per-file alias map: short-name → FQN. `using static` is not a
    // single binding; it's consulted at call sites directly via
    // `file_facts.import_bindings`.
    let mut alias_maps: HashMap<&str, HashMap<String, String>> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut m: HashMap<String, String> = HashMap::new();
        for binding in &facts.import_bindings {
            if binding.kind == ImportKind::Static {
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

        // Type references (base classes / interfaces).
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

        // Method calls — only static shapes where the receiver type is
        // pinnable from the grammar.
        for call in &facts.method_calls {
            let Some(resolved) =
                dispatch::resolve_call(call, &package_index, &mro, &methods, &aliases, facts)
            else {
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

/// Resolve a dotted reference (a heritage base name, a `using` target)
/// under C# lookup rules: alias → same-namespace → containing-namespace
/// chain → `using`-imported → bare FQN.
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

    // 1. Alias substitution (using-alias or imported name).
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

    // 2. Same-namespace (and outer containing namespaces) lookup.
    if let Some(ns) = facts.package.as_deref().filter(|s| !s.is_empty()) {
        for prefix in containing_namespaces(ns) {
            let candidate = if prefix.is_empty() {
                parts.join(".")
            } else {
                format!("{prefix}.{}", parts.join("."))
            };
            if let Some(hit) = package_index.lookup(&candidate) {
                return Some((hit.path.clone(), hit.qualified.clone()));
            }
        }
    }

    // 3. `using` namespace expansion: for each `using X.Y;` binding
    // whose local matches no concrete symbol, try `X.Y.<parts>`.
    for b in &facts.import_bindings {
        if b.kind == ImportKind::Plain {
            // Already covered by alias map when the FQN matches a
            // workspace symbol; here the binding's FQN is a namespace
            // that contributes a wildcard expansion.
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

/// Yield each containing namespace prefix from most-specific to root.
/// `"Foo.Bar.Baz"` → `["Foo.Bar.Baz", "Foo.Bar", "Foo", ""]`.
pub(crate) fn containing_namespaces(ns: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let parts: Vec<&str> = ns.split('.').collect();
    for i in (0..=parts.len()).rev() {
        out.push(parts[..i].join("."));
    }
    out
}
