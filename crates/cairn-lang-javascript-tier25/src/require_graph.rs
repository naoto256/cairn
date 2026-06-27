//! Module-specifier → workspace-file resolution.
//!
//! For each `ImportBinding` in a file:
//!   * relative specifier (`./foo`, `../foo/bar`): normalize against
//!     the source file's directory, then probe `<p>`, `<p>.js`,
//!     `<p>.mjs`, `<p>.cjs`, `<p>.jsx`, `<p>/index.js`,
//!     `<p>/index.mjs`, `<p>/index.cjs` in that order. Hit ⇒
//!     `RequireEdge.target_path = Some(path)`.
//!   * bare specifier (`express`, `lodash`): no target path. The
//!     specifier string is *not* stuffed into
//!     `RequireEdge.target_qualified` — import edges target a file,
//!     not a symbol, and JavaScript's contract since Phase 3 is to
//!     leave `target_qualified = None` for both resolved and
//!     unresolved imports, mirroring Ruby. Other Tier-2.5 backends
//!     (PHP / Python / Kotlin / Swift / C#) populate
//!     `RequireEdge.target_qualified` with a real symbol or module
//!     FQN where they know one — that variance is intentional and
//!     persist.rs treats `kind = Import` as a hard gate for the
//!     manifest-wide qualified-only symbol fallback, so a leaked FQN
//!     cannot silently re-point an import edge in either contract.
//!   * `node:`-prefixed (`node:fs`): same `(None, None)` shape.
//!   * path-alias (`@/foo`, leading non-relative non-bare): same.
//!
//! `ResolvedBinding.target_qualified` is a separate concept used
//! downstream by [`crate::dispatch`] to map local aliases to the
//! exported FQN they refer to (`const x = require('./util').helper`
//! → `target_qualified = "util.helper"`); that lives on the binding
//! map and never reaches the `RequireEdge` ImportFact. Both fields
//! happen to share a name, but the import-edge axis stays at the
//! file level only.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{FileConstFacts, ImportBinding, ImportKind, PackageIndex};

#[derive(Debug, Clone)]
pub struct RequireEdge {
    pub site_byte_start: u32,
    pub site_byte_end: u32,
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedBinding {
    pub local: String,
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
    /// For ESM named / default / CJS imports, the name as exported
    /// from the target module (`"X"`, `"default"`). None for namespace
    /// imports and side-effects.
    pub target_exported_name: Option<String>,
    /// Original import shape, preserved through alias projection so
    /// dispatch can distinguish `Foo.bar` on a default import (property
    /// access on the default-exported value — cannot be pinned at
    /// Tier-2.5) from `Foo.bar` on a namespace import (named export
    /// `bar` of the module — pinnable). Without this, a default-
    /// imported `Foo` was being treated namespace-style, mis-pointing
    /// `Foo.bar()` at any same-named export in the target module.
    pub import_kind: ImportKind,
    /// True when this binding's name was traced through a re-export
    /// chain that `PackageIndex::build` dropped as cyclic or
    /// over-budget. Distinct from "external / npm import": those have
    /// `target_path = None` but `dropped_chain = false` and downstream
    /// emitters surface a fact-fallback row. A `dropped_chain = true`
    /// binding has no resolvable origin within the workspace — emitting
    /// any Type / Call row keyed off it would fabricate an edge into
    /// the dropped cycle / mid-chain barrel. See R2 v0.7.0 dogfood
    /// follow-up (rc3 catch).
    pub dropped_chain: bool,
}

impl Default for ResolvedBinding {
    fn default() -> Self {
        Self {
            local: String::new(),
            target_path: None,
            target_qualified: None,
            target_exported_name: None,
            // Default to the most-restrictive kind so an accidental
            // default-constructed binding doesn't unlock the namespace
            // member lookup path.
            import_kind: ImportKind::SideEffect,
            dropped_chain: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct RequireGraph {
    edges: HashMap<String, Vec<RequireEdge>>,
    bindings: HashMap<String, Vec<ResolvedBinding>>,
}

impl RequireGraph {
    pub fn build(
        per_file: &[(String, Vec<u8>, FileConstFacts)],
        package_index: &PackageIndex,
    ) -> Self {
        // Collect every workspace path so we can probe relative
        // resolutions against the actual file set.
        let path_set: HashSet<&str> = per_file.iter().map(|(p, _, _)| p.as_str()).collect();

        let mut edges: HashMap<String, Vec<RequireEdge>> = HashMap::new();
        let mut bindings: HashMap<String, Vec<ResolvedBinding>> = HashMap::new();
        // Deduplicate sites within a file: many bindings can share the
        // same `from './foo'` site, but we only want one Import row.
        let mut seen_sites: HashSet<(String, u32, u32)> = HashSet::new();

        for (path, _, facts) in per_file {
            let mut edge_list: Vec<RequireEdge> = Vec::new();
            let mut bind_list: Vec<ResolvedBinding> = Vec::new();

            for b in &facts.import_bindings {
                let initial_target = resolve_module(path, &b.module, &path_set);
                // For namespace/side-effect imports, no symbol-level
                // re-export walk is possible (no `imported_name`).
                // For named / default / CJS member imports, follow the
                // re-export chain so `import { X } from './barrel'`
                // gets `target_path = origin.js` on the binding (and
                // downstream Type / Call rows land on the origin).
                let (binding_target_path, binding_target_qualified, binding_dropped_chain) =
                    resolve_binding_target(b, &initial_target, package_index);

                let key = (path.clone(), b.site_byte_start, b.site_byte_end);
                if seen_sites.insert(key) {
                    edge_list.push(RequireEdge {
                        site_byte_start: b.site_byte_start,
                        site_byte_end: b.site_byte_end,
                        // Import-edge `target_path` stays at the
                        // module-specifier resolution (i.e. the barrel
                        // file itself). The barrel is what the `from
                        // './barrel'` *site* points at — re-export
                        // resolution is a per-binding (per-name)
                        // concept, not a per-site one. Dispatch / Type
                        // rows below carry the origin file via the
                        // binding map.
                        target_path: initial_target.clone(),
                        // Import edges target a *file*, not a symbol:
                        // `b.module` is the module specifier (`./db`,
                        // `lodash`, `node:fs`) and never matches
                        // `symbols.qualified`. Mirroring Ruby's
                        // Phase 1 require-graph fix, leave this `None`
                        // so persist.rs skips the symbol lookup
                        // entirely and `target_path` remains the
                        // source of truth on the resolutions row.
                        target_qualified: None,
                    });
                }

                if b.kind != ImportKind::SideEffect && !b.local.is_empty() {
                    bind_list.push(ResolvedBinding {
                        local: b.local.clone(),
                        target_path: binding_target_path,
                        target_qualified: binding_target_qualified,
                        target_exported_name: b.imported_name.clone(),
                        import_kind: b.kind,
                        dropped_chain: binding_dropped_chain,
                    });
                }
            }
            edges.insert(path.clone(), edge_list);
            bindings.insert(path.clone(), bind_list);
        }

        Self { edges, bindings }
    }

    pub fn edges_for(&self, path: &str) -> &[RequireEdge] {
        self.edges.get(path).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn bindings_for(&self, path: &str) -> &[ResolvedBinding] {
        self.bindings.get(path).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Resolve a module specifier against the workspace file set.
/// Returns `Some(path)` only for relative specifiers that hit an
/// existing workspace file. Bare / `node:` / alias specifiers return
/// None (Tier-2 fact fallback).
pub(crate) fn resolve_module(
    source_path: &str,
    specifier: &str,
    workspace_files: &HashSet<&str>,
) -> Option<String> {
    if !is_relative(specifier) {
        return None;
    }
    let base_dir = parent_of(source_path);
    let joined = join_path(base_dir, specifier);
    let normalized = normalize(&joined);

    let candidates = candidate_paths(&normalized);
    candidates
        .into_iter()
        .find(|c| workspace_files.contains(c.as_str()))
}

fn is_relative(spec: &str) -> bool {
    spec.starts_with("./") || spec.starts_with("../") || spec == "." || spec == ".."
}

fn parent_of(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[..i],
        None => "",
    }
}

fn join_path(base: &str, rel: &str) -> String {
    if base.is_empty() {
        rel.to_string()
    } else {
        format!("{base}/{rel}")
    }
}

/// Collapse `.` and `..` segments. Leading `..` segments that escape
/// the workspace root remain (they just won't match anything).
fn normalize(path: &str) -> String {
    let mut out: Vec<&str> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if !matches!(out.last(), Some(&"..") | None) {
                    out.pop();
                } else {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    out.join("/")
}

fn candidate_paths(normalized: &str) -> Vec<String> {
    let exts = [".js", ".mjs", ".cjs", ".jsx"];
    let mut out: Vec<String> = Vec::new();
    // Exact match (e.g. `./foo.js` already has the extension).
    if has_known_extension(normalized) {
        out.push(normalized.to_string());
        // index siblings rarely apply when an extension is given, skip.
        return out;
    }
    out.push(normalized.to_string());
    for ext in &exts {
        out.push(format!("{normalized}{ext}"));
    }
    // index.* siblings.
    for ext in &exts {
        if normalized.is_empty() {
            out.push(format!("index{ext}"));
        } else {
            out.push(format!("{normalized}/index{ext}"));
        }
    }
    out
}

fn has_known_extension(path: &str) -> bool {
    [".js", ".mjs", ".cjs", ".jsx", ".json"]
        .iter()
        .any(|e| path.ends_with(e))
}

/// Resolve a single import binding's `(target_path,
/// target_qualified)` pair, following any re-export chain so a
/// consumer's `import { X } from './barrel'` lands on the origin file.
///
/// The returned `target_path` may differ from `initial_target` when
/// the imported name is re-exported through the immediate target — in
/// that case it's rewritten to the origin file the chain terminates
/// on. The `target_qualified` is the local symbol name in the origin
/// file (or, for outside-workspace targets, the specifier-prefixed
/// fact-fallback form).
fn resolve_binding_target(
    b: &ImportBinding,
    initial_target: &Option<String>,
    package_index: &PackageIndex,
) -> (Option<String>, Option<String>, bool) {
    if let Some(target_path) = initial_target {
        if let Some(imported) = &b.imported_name {
            // 1. lookup_export already follows re-export chains, so a
            //    hit here gives the origin file + local symbol name.
            if let Some(hit) = package_index.lookup_export(target_path, imported) {
                return (Some(hit.path.clone()), Some(hit.qualified.clone()), false);
            }
            // 2. The barrel re-exports the name but the origin's
            //    symbol table didn't record a matching ClassDef /
            //    FunctionDef (e.g. `export const X = expr` where `X`
            //    is a value, not a class). Still rewrite target_path
            //    to the origin so import rows / fallback queries
            //    converge on the right file, and surface the resolved
            //    origin-name as `target_qualified`.
            if let Some((origin_path, origin_name)) =
                package_index.resolve_reexport(target_path, imported)
            {
                return (Some(origin_path), Some(origin_name), false);
            }
            // 3. No export and no *resolvable* re-export. Before
            //    falling through to the Tier-2 barrel-fact fallback,
            //    ask PackageIndex whether `target_path` actually does
            //    re-export `imported` syntactically but had its chain
            //    dropped (cycle / over-budget). If so, the fallback
            //    would fabricate a binding `(target_path, imported)`
            //    that points into the cycle / mid-chain barrel — a
            //    fake origin for a name that has no resolvable origin.
            //    Return unresolved instead so downstream consumers
            //    (subtype index, type resolution) don't record a
            //    synthesized edge into the cycle.
            //
            //    R2 dogfood catch (v0.7.0 cycle-fix follow-up):
            //      cycle-a.js: `export { X } from './cycle-b';`
            //      cycle-b.js: `export { X } from './cycle-a';`
            //      main.js:    `import { X } from './cycle-a';
            //                   class CycleSub extends X {}`
            //    Previously emitted a Type row
            //      target_path=cycle-a.js, target_qualified=X
            //    even though no `class X {}` is defined anywhere in
            //    the cycle.
            if package_index.is_reexport_dropped(target_path, imported) {
                return (None, None, true);
            }
            // 4. The file genuinely has no entry for `imported` — keep
            //    the barrel as target and surface the imported name
            //    (Tier-2 fact fallback). Covers the legitimate case
            //    where the barrel exposes a definition Tier-2.5
            //    didn't index (`export const X = expr`, etc.).
            return (Some(target_path.clone()), Some(imported.clone()), false);
        }
        // Namespace / side-effect import: file-level binding only.
        return (Some(target_path.clone()), Some(target_path.clone()), false);
    }
    // Outside the workspace: fall back to the specifier string + any
    // imported name. `import fs from 'node:fs'` → "node:fs.default";
    // `import { readFile } from 'node:fs'` → "node:fs.readFile";
    // `import express from 'express'` → "express.default".
    let qualified = if let Some(imported) = &b.imported_name {
        Some(format!("{}.{}", b.module, imported))
    } else {
        Some(b.module.clone())
    };
    (None, qualified, false)
}
