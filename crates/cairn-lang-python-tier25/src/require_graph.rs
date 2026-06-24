//! `import x` / `from x import y` → workspace file resolution.
//!
//! Tier-2.5 only resolves Python imports whose target lives inside the
//! workspace — stdlib / site-packages stays unresolved (Tier-3
//! territory). Resolution walks the workspace `ModuleIndex` built by
//! `const_resolver`.
//!
//! Relative imports (`from . import x`, `from ..pkg import y`) anchor
//! at the file's own module qualified name and pop one segment per
//! leading dot; absolute imports look up the module directly. Each
//! emitted edge produces one `WorkspaceResolution` row, and the per-
//! file alias map exposed via `resolve_binding` lets the rest of the
//! resolver (dispatch / MRO) know which short-name maps to which
//! workspace symbol.

use std::collections::HashMap;

use crate::const_resolver::{
    FileConstFacts, ImportBinding, ImportKind, ModuleIndex, strip_leading_dots,
};

#[derive(Debug, Clone)]
pub struct RequireEdge {
    pub site_byte_start: u32,
    pub site_byte_end: u32,
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
}

#[derive(Debug, Default)]
pub struct RequireGraph {
    edges: HashMap<String, Vec<RequireEdge>>,
    /// `(file, binding-local-name) → resolved qualified` so the
    /// resolver pass can re-look-up what each binding produced
    /// without re-walking the per-file facts.
    bindings: HashMap<(String, String), String>,
}

impl RequireGraph {
    pub fn build(
        per_file: &[(String, Vec<u8>, FileConstFacts)],
        module_index: &ModuleIndex,
    ) -> Self {
        let mut edges: HashMap<String, Vec<RequireEdge>> = HashMap::new();
        let mut bindings: HashMap<(String, String), String> = HashMap::new();

        for (path, _, facts) in per_file {
            let mut list = Vec::new();
            let module = facts.module.as_deref();
            for b in &facts.import_bindings {
                let resolved =
                    resolve_binding_into_qualified(b, module, facts.is_package_init, module_index);
                let (target_path, target_qualified) = match &resolved {
                    Some(q) => {
                        // Try symbol first, fall back to module.
                        if let Some(hit) = module_index.lookup(q) {
                            (Some(hit.path.clone()), Some(hit.qualified.clone()))
                        } else if let Some(p) = module_index.module_path(q) {
                            (Some(p.to_string()), Some(q.clone()))
                        } else {
                            (None, Some(q.clone()))
                        }
                    }
                    None => (None, None),
                };
                if let Some(q) = &target_qualified {
                    bindings.insert((path.clone(), b.local.clone()), q.clone());
                }
                list.push(RequireEdge {
                    site_byte_start: b.site_byte_start,
                    site_byte_end: b.site_byte_end,
                    target_path,
                    target_qualified,
                });
            }
            edges.insert(path.clone(), list);
        }

        Self { edges, bindings }
    }

    pub fn edges_for(&self, path: &str) -> &[RequireEdge] {
        self.edges.get(path).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Re-look-up what an `ImportBinding` produced, so the rest of the
    /// resolver doesn't repeat the matching logic.
    pub fn resolve_binding(&self, path: &str, binding: &ImportBinding) -> Option<String> {
        self.bindings
            .get(&(path.to_string(), binding.local.clone()))
            .cloned()
    }
}

/// Given an `ImportBinding` from `file_module`, return the workspace
/// qualified name it should resolve to (if any).
///
/// The mapping is:
///
/// `import x.y`              → qualified = `x.y`
/// `import x.y as z`         → qualified = `x.y` (binding `z` points at module `x.y`)
/// `from m import f`         → qualified = `m.f`
/// `from m import f as g`    → qualified = `m.f`
/// `from . import sub`       → resolve `.` to parent module, then `parent.sub`
/// `from ..pkg import sub`   → pop two parent segments, then `pkg.sub`
///
/// Wildcard `from m import *` resolves to the module itself.
fn resolve_binding_into_qualified(
    b: &ImportBinding,
    file_module: Option<&str>,
    is_package_init: bool,
    module_index: &ModuleIndex,
) -> Option<String> {
    match b.kind {
        ImportKind::Plain | ImportKind::Aliased => {
            let m = &b.module;
            if module_index.module_path(m).is_some() {
                Some(m.clone())
            } else {
                // Not a workspace module — leave unresolved.
                None
            }
        }
        ImportKind::From => {
            let absolute = absolute_module(b, file_module, is_package_init)?;
            if b.local == "*" {
                // wildcard import — point at the module.
                return Some(absolute);
            }
            let imported = b.imported.clone().unwrap_or_else(|| b.local.clone());
            // Two possible meanings for `from M import N`:
            //   1. `N` is a name defined in module `M` → qualified
            //      `M.N`. Workspace lookup finds it via either
            //      `by_qualified[M.N]` or any suffix index.
            //   2. `N` is a submodule of `M` (i.e. `M/N.py` or
            //      `M/N/__init__.py`) → qualified module `M.N`.
            // The module_index handles both via `lookup` (symbol) and
            // `module_path` (module). Reach for the symbol first.
            let candidate = format!("{absolute}.{imported}");
            if module_index.lookup(&candidate).is_some()
                || module_index.module_path(&candidate).is_some()
            {
                return Some(candidate);
            }
            // Fallback: bare `from m import f` where module `m` isn't
            // in the workspace but `f` is a workspace top-level symbol
            // (rare, but covers `from app import Flask` against a
            // workspace `flask.app.Flask` whose module name isn't
            // pinned).
            if module_index.lookup(&imported).is_some() {
                return Some(imported);
            }
            // Module-only resolution as a last resort.
            if module_index.module_path(&absolute).is_some() {
                return Some(absolute);
            }
            None
        }
    }
}

/// Resolve a (possibly relative) import module name to an absolute
/// workspace module path.
///
/// `level == 0`: absolute import, the module is `b.module` verbatim.
/// `level == 1`: the *current package*. For a file `pkg/sub/x.py`
///   (module `pkg.sub.x`), `.` resolves to `pkg.sub`. For
///   `pkg/sub/__init__.py` (module `pkg.sub`, `is_package_init`),
///   `.` already resolves to `pkg.sub` — so we skip the "pop the
///   file's module" step.
/// `level >= 2`: one parent per extra dot.
///
/// Returns `None` when the file isn't in a package or the dots
/// over-pop.
fn absolute_module(
    b: &ImportBinding,
    file_module: Option<&str>,
    is_package_init: bool,
) -> Option<String> {
    if b.level == 0 {
        return Some(b.module.clone());
    }
    let m = file_module?;
    let mut parts: Vec<&str> = m.split('.').collect();
    // For non-`__init__.py` files, `from .` means the package the file
    // lives in — i.e. pop the file's module name once before counting
    // the remaining `level - 1` extra dots.
    if !is_package_init {
        if parts.is_empty() {
            return None;
        }
        parts.pop();
    }
    let extra = b.level.saturating_sub(1) as usize;
    if extra > parts.len() {
        return None;
    }
    for _ in 0..extra {
        parts.pop();
    }
    let base = parts.join(".");
    let suffix = strip_leading_dots(&b.module);
    if suffix.is_empty() {
        return Some(base);
    }
    if base.is_empty() {
        return Some(suffix.to_string());
    }
    Some(format!("{base}.{suffix}"))
}
