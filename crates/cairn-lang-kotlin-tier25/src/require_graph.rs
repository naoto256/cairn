//! `import com.foo.Bar` → workspace file resolution.
//!
//! Tier-2.5 only resolves Kotlin imports whose target lives inside the
//! workspace — kotlin-stdlib / Android SDK / external jars stay
//! unresolved (Tier-3 territory once a JVM resolver is wired in).
//!
//! Resolution walks the workspace `PackageIndex`:
//!   * `import com.foo.Bar` → look up `com.foo.Bar` as a symbol. On
//!     hit, emit `(target_path, target_qualified)`.
//!   * `import com.foo.Bar as B` → same lookup, the binding's local
//!     name is the alias.
//!   * `import com.foo.*` → if any workspace file declares package
//!     `com.foo`, emit an Import row anchored at the wildcard token
//!     with `target_qualified = "com.foo"` (the package) and no
//!     specific target file (each file in the package is a separate
//!     resolution target).
//!
//! The graph also exposes the per-file alias map (`resolve_binding`)
//! so MRO / dispatch can ask "what does this short name resolve to in
//! file X?" without re-walking facts.

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
    /// `(file, local-name) → resolved FQN` so the resolver pass can
    /// re-look-up what each binding produced without re-walking
    /// per-file facts.
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
                    // Wildcard bindings have local `*`; skip storing
                    // them as alias bindings (they expand at use
                    // sites instead).
                    if b.kind != ImportKind::Wildcard {
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

    /// Look up the resolved FQN this binding produced.
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
        ImportKind::Plain | ImportKind::Aliased => {
            // Try as a symbol (class / function / const) first.
            if let Some(hit) = package_index.lookup(&b.fqn) {
                return (Some(hit.path.clone()), Some(hit.qualified.clone()));
            }
            // External jar / stdlib — leave unresolved (target_path
            // None), but still keep the qualified so downstream
            // tooling can see what the import points at.
            (None, Some(b.fqn.clone()))
        }
        ImportKind::Wildcard => {
            if package_index.has_package(&b.fqn) {
                // No single target file; record the package as the
                // qualified target so consumers can fan out at query
                // time.
                (None, Some(b.fqn.clone()))
            } else {
                (None, Some(b.fqn.clone()))
            }
        }
    }
}
