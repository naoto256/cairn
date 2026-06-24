//! Python Tier-2.5 in-process workspace analyzer.
//!
//! This crate is the tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-python-tier3` crate: it walks Python source with
//! tree-sitter, builds a per-workspace module/class/import graph, and
//! emits resolution-layer rows (`source = "tier25-python-resolver"`)
//! for the cases that the grammar can pin without LSP help.
//!
//! Scope (Stage 1, 3rd wave):
//!
//! * Class / module lookup: lexical (module globals) → `from x import y`
//!   bindings → `import x as y` bindings. Workspace-local only — no
//!   site-packages resolution.
//! * MRO walk: linear single-inheritance chain plus best-effort C3 for
//!   multiple inheritance, scoped to base classes whose qualified name
//!   resolves through the workspace import graph.
//! * Static dispatch: `Cls.method(...)`, `self.method(...)` (resolved
//!   through the lexically enclosing class), `super().method(...)`
//!   (parent walk), and module-attribute calls (`mod.fn(...)` where
//!   `mod` is a workspace `import` binding).
//! * `import` / `from x import y` (absolute and relative) recorded as
//!   Import resolutions when the target lives in the workspace,
//!   including `__init__.py` package resolution.
//!
//! Out of scope (left to Tier-3 / never):
//!
//! * `obj.method()` where the receiver type is unknown (no annotation
//!   propagation).
//! * `getattr` / `setattr` / `__getattr__` dynamic dispatch.
//! * Metaclass-induced method synthesis.
//! * Decorator transformations that rewrite a function signature
//!   (`@property`, descriptors, etc.).
//! * `eval` / `exec`.
//! * Stdlib / site-packages resolution.

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

use const_resolver::{FileConstFacts, ModuleIndex};
use dispatch::MethodIndex;
use mro::Mro;
use require_graph::RequireGraph;

pub const ANALYZER_ID: &str = "python-resolver";
pub const TIER_PREFIX: &str = "tier25";
pub const ANALYZER_REVISION: u32 = 1;
pub const PARSER_ID: &str = "tree-sitter-python";
pub const RESOLUTION_SOURCE: &str = "tier25-python-resolver";

/// In-process tree-sitter resolver for Python.
pub struct PythonTier25Analyzer;

impl WorkspaceAnalyzer for PythonTier25Analyzer {
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
        "python"
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
static REGISTER_PYTHON_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(PythonTier25Analyzer);

/// Parse every visible Python file and emit resolutions across the workspace.
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
        let module = file_to_module(&f.path);
        let is_package_init = f.path.ends_with("/__init__.py") || f.path == "__init__.py";
        let facts = match const_resolver::parse_file(&source, module, is_package_init) {
            Some(f) => f,
            None => {
                progress.tick();
                continue;
            }
        };
        per_file.push((f.path.clone(), source, facts));
        progress.tick();
    }

    // 2. Build cross-file module index + require graph.
    let module_index = ModuleIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &module_index);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for `import` / `from ... import ...`
    // statements whose target lives in the workspace.
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
    let mro = Mro::build(&per_file, &module_index, &require_graph);
    let methods = MethodIndex::build(&per_file);

    // Per-file alias map: short-name → fully-qualified workspace symbol.
    // Built from the require-graph (only edges that resolved to a
    // workspace target contribute aliases).
    let mut alias_maps: HashMap<&str, HashMap<String, String>> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut m: HashMap<String, String> = HashMap::new();
        for binding in &facts.import_bindings {
            // The require-graph stores resolved qualifieds keyed by
            // binding range; re-walk facts and ask the graph for the
            // resolved qualified.
            if let Some(q) = require_graph.resolve_binding(path, binding) {
                m.insert(binding.local.clone(), q);
            }
        }
        alias_maps.insert(path.as_str(), m);
    }

    for (path, _, facts) in &per_file {
        let aliases = alias_maps.get(path.as_str()).cloned().unwrap_or_default();

        // Type references (base classes, `Foo()` constructions, etc.).
        for tref in &facts.type_refs {
            let resolved = resolve_dotted(
                &tref.parts,
                &aliases,
                facts.module.as_deref(),
                &module_index,
            );
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: tref.byte_start..tref.byte_end,
                kind: ResolutionKind::Type,
                semantic_kind: None,
                target_path: resolved.as_ref().map(|r| r.0.clone()),
                target_qualified: resolved.map(|r| r.1),
            });
        }

        // Method calls — only static / self / super / module-attribute
        // shapes where the receiver type is pinnable from the grammar.
        for call in &facts.method_calls {
            let Some(resolved) =
                dispatch::resolve_call(call, &module_index, &mro, &methods, &aliases, facts)
            else {
                // Unresolvable (`obj.method()`, `getattr`, etc.) —
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

/// Convert a repo-relative file path to its Python module qualified name.
///
/// `src/flask/app.py` → `src.flask.app`
/// `flask/__init__.py` → `flask`
/// `top.py` → `top`
///
/// We intentionally include path-prefix segments (`src.flask.app`
/// rather than `flask.app`) because cairn doesn't know the project's
/// import root without setup.cfg / pyproject parsing. The require-graph
/// later resolves both the bare (`flask.app`) and prefixed
/// (`src.flask.app`) forms by also indexing trailing-segment
/// candidates.
fn file_to_module(path: &str) -> Option<String> {
    let stripped = path.strip_suffix(".py")?;
    let segs: Vec<&str> = stripped.split('/').collect();
    if segs.is_empty() {
        return None;
    }
    let normalised: Vec<&str> = if segs.last().copied() == Some("__init__") {
        segs[..segs.len() - 1].to_vec()
    } else {
        segs
    };
    if normalised.is_empty() {
        return None;
    }
    Some(normalised.join("."))
}

/// Best-effort resolution of a dotted reference under Python lexical
/// rules: alias-map (covers `from x import Y` and `import x as Y`) →
/// in-module lookup → workspace global module-index lookup.
///
/// Returns `(target_path, target_qualified)` when resolution
/// succeeds, mirroring the shape `WorkspaceResolution` records.
fn resolve_dotted(
    parts: &[String],
    aliases: &HashMap<String, String>,
    module: Option<&str>,
    module_index: &ModuleIndex,
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

    // 1. Alias substitution: `head` was bound by an import.
    if let Some(target) = aliases.get(head) {
        let qualified = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if let Some(hit) = module_index.lookup(&qualified) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
        // Resolved by alias but no concrete symbol — record qualified
        // without a path so consumers can still see the resolved name.
        return Some((String::new(), qualified)).filter(|(p, _)| !p.is_empty());
    }

    // 2. Same-module lookup: prepend the file's own module name.
    if let Some(m) = module.filter(|m| !m.is_empty()) {
        let candidate = format!("{m}.{}", parts.join("."));
        if let Some(hit) = module_index.lookup(&candidate) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
    }

    // 3. Global lookup against the workspace index.
    let qualified = parts.join(".");
    if let Some(hit) = module_index.lookup(&qualified) {
        return Some((hit.path.clone(), hit.qualified.clone()));
    }

    None
}
