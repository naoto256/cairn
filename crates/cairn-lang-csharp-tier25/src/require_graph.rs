//! `using A.B;` → workspace file resolution.
//!
//! Resolution walks the workspace `PackageIndex`:
//!   * `using A.B.Type;` → lookup `A.B.Type` as a symbol; on hit emit
//!     `(target_path, target_qualified)`.
//!   * `using F = A.B.Type;` → alias case; same lookup.
//!   * `using A.B;` (namespace) → if any workspace file declares
//!     namespace `A.B` or any sub-namespace, emit an Import row with
//!     `target_qualified = "A.B"` and `target_path = None` (each file
//!     is a separate target).
//!   * `using static A.B.C;` → lookup type `A.B.C`; on hit emit the
//!     type as target.
//!   * BCL / NuGet — `target_path = None`, `target_qualified =
//!     "System.X.Y"` (Tier-2-fact fallback).

use std::collections::HashMap;

use crate::const_resolver::{FileConstFacts, ImportBinding, ImportKind, PackageIndex};

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
                    if b.kind != ImportKind::Static {
                        bindings.insert((path.clone(), b.local.clone()), q.clone());
                    }
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
    match b.kind {
        ImportKind::Aliased | ImportKind::Static => {
            // Try as a concrete symbol first.
            if let Some(hit) = package_index.lookup(&b.fqn) {
                return (Some(hit.path.clone()), Some(hit.qualified.clone()));
            }
            (None, Some(b.fqn.clone()))
        }
        ImportKind::Plain => {
            // `using A.B;` — A.B is a namespace. Try as a symbol
            // (rarely matches) then as a namespace.
            if let Some(hit) = package_index.lookup(&b.fqn) {
                return (Some(hit.path.clone()), Some(hit.qualified.clone()));
            }
            // Both branches return the same shape (no workspace path,
            // qualified preserved); the `has_package` check is kept as
            // documentation of "this is a known namespace" — useful at
            // query time though not load-bearing here.
            let _ = package_index.has_package(&b.fqn);
            (None, Some(b.fqn.clone()))
        }
    }
}
