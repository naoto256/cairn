//! `import Foundation` / `import UIKit.UIView` → workspace file
//! resolution.
//!
//! Tier-2.5 only resolves Swift imports whose target lives inside
//! the workspace — Foundation / UIKit / SwiftUI / Combine / Apple
//! framework imports and SPM-external targets stay unresolved
//! (Tier-3 territory once a SourceKit-LSP resolver is wired in).
//!
//! Resolution walks the workspace `PackageIndex`:
//!   * `import Module` → if any workspace file declares module
//!     `Module`, record the module as the resolved qualified
//!     (multiple files; no single target path).
//!   * `import Module.Type` → look up `Module.Type` as a symbol.
//!     On hit, emit `(target_path, target_qualified)`.
//!   * Apple frameworks stay unresolved (`target_path: None`) but
//!     keep their dotted qualified for downstream tooling.

use std::collections::HashMap;

use crate::const_resolver::{FileConstFacts, ImportBinding, PackageIndex};

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
    bindings: HashMap<(String, String), String>,
}

impl RequireGraph {
    pub fn build(
        per_file: &[(String, Vec<u8>, FileConstFacts)],
        package_index: &PackageIndex,
    ) -> Self {
        let mut edges: HashMap<String, Vec<RequireEdge>> = HashMap::new();
        let mut bindings: HashMap<(String, String), String> = HashMap::new();

        for (path, _, facts) in per_file {
            let mut list = Vec::new();
            for b in &facts.import_bindings {
                let (target_path, target_qualified) = resolve_import(b, package_index);
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

    pub fn resolve_binding(&self, path: &str, binding: &ImportBinding) -> Option<String> {
        self.bindings
            .get(&(path.to_string(), binding.local.clone()))
            .cloned()
    }
}

fn resolve_import(
    b: &ImportBinding,
    package_index: &PackageIndex,
) -> (Option<String>, Option<String>) {
    // 1. Try the full dotted path as a workspace symbol.
    if let Some(hit) = package_index.lookup(&b.fqn) {
        return (Some(hit.path.clone()), Some(hit.qualified.clone()));
    }
    // 2. Check whether the head segment is a workspace module.
    if package_index.has_module(&b.local) {
        return (None, Some(b.fqn.clone()));
    }
    // 3. Apple framework / SPM-external — leave unresolved at the
    // path level, but keep the qualified for downstream tooling.
    (None, Some(b.fqn.clone()))
}
