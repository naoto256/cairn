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

#[derive(Debug, Clone, Default)]
pub struct ResolvedBinding {
    pub local: String,
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
    /// For ESM named / default / CJS imports, the name as exported
    /// from the target module (`"X"`, `"default"`). None for namespace
    /// imports and side-effects.
    pub target_exported_name: Option<String>,
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
                let target_path = resolve_module(path, &b.module, &path_set);
                let target_qualified = qualified_for(b, &target_path, package_index);

                let key = (path.clone(), b.site_byte_start, b.site_byte_end);
                if seen_sites.insert(key) {
                    edge_list.push(RequireEdge {
                        site_byte_start: b.site_byte_start,
                        site_byte_end: b.site_byte_end,
                        target_path: target_path.clone(),
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
                        target_path: target_path.clone(),
                        target_qualified,
                        target_exported_name: b.imported_name.clone(),
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
fn resolve_module(
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

fn qualified_for(
    b: &ImportBinding,
    target_path: &Option<String>,
    package_index: &PackageIndex,
) -> Option<String> {
    if let Some(target_path) = target_path {
        if let Some(imported) = &b.imported_name {
            if let Some(hit) = package_index.lookup_export(target_path, imported) {
                return Some(hit.qualified.clone());
            }
            return Some(imported.clone());
        }
        return Some(target_path.clone());
    }
    // Outside the workspace: fall back to the specifier string + any
    // imported name. `import fs from 'node:fs'` → "node:fs.default";
    // `import { readFile } from 'node:fs'` → "node:fs.readFile";
    // `import express from 'express'` → "express.default".
    if let Some(imported) = &b.imported_name {
        Some(format!("{}.{}", b.module, imported))
    } else {
        Some(b.module.clone())
    }
}
