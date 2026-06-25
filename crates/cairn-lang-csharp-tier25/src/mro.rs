//! Method Resolution Order for C#.
//!
//! C# allows single class inheritance plus multiple interface
//! implementation; for `struct` only interfaces; `record` is class-like;
//! `interface` extends other interfaces. The Tier-2 backend does *not*
//! distinguish `class` vs `interface` bases (both use
//! `kind="inherit"`), so at this layer we collect every resolved base
//! in declaration order and linearize.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{BaseEdge, FileConstFacts, ImportBinding, ImportKind, PackageIndex};
use crate::containing_namespaces;

#[derive(Debug, Default)]
pub struct Mro {
    chain: HashMap<String, Vec<String>>,
    parents: HashMap<String, Vec<String>>,
}

impl Mro {
    pub fn build(
        per_file: &[(String, Vec<u8>, FileConstFacts)],
        package_index: &PackageIndex,
    ) -> Self {
        let mut parents: HashMap<String, Vec<String>> = HashMap::new();
        let mut classes: HashSet<String> = HashSet::new();

        for (_path, _, facts) in per_file {
            let aliases = build_aliases(facts);
            for class in &facts.class_defs {
                classes.insert(class.qualified.clone());
            }
            for edge in &facts.base_edges {
                if let Some(resolved) = resolve_base(edge, &aliases, facts, package_index) {
                    parents
                        .entry(edge.owner.clone())
                        .or_default()
                        .push(resolved);
                }
            }
        }

        let mut chain: HashMap<String, Vec<String>> = HashMap::new();
        for class in &classes {
            chain.insert(class.clone(), linearize(class, &parents, &classes));
        }
        Self { chain, parents }
    }

    pub fn ancestors(&self, class: &str) -> Vec<String> {
        self.chain
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }

    pub fn parents_of(&self, class: &str) -> &[String] {
        self.parents.get(class).map(Vec::as_slice).unwrap_or(&[])
    }
}

fn build_aliases(facts: &FileConstFacts) -> HashMap<String, String> {
    let mut aliases: HashMap<String, String> = HashMap::new();
    for b in &facts.import_bindings {
        if let Some((local, fqn)) = alias_binding(b) {
            aliases.insert(local, fqn);
        }
    }
    aliases
}

pub(crate) fn alias_binding(b: &ImportBinding) -> Option<(String, String)> {
    match b.kind {
        ImportKind::Aliased => Some((b.local.clone(), b.fqn.clone())),
        // Plain `using A.B;` doesn't bind `B` as a type alias in C# —
        // it brings the *contents* of namespace A.B into scope. So we
        // don't add it to the alias map; instead the wildcard
        // expansion path in `resolve_base` handles it.
        ImportKind::Plain | ImportKind::Static => None,
    }
}

fn resolve_base(
    edge: &BaseEdge,
    aliases: &HashMap<String, String>,
    facts: &FileConstFacts,
    package_index: &PackageIndex,
) -> Option<String> {
    if edge.parts.is_empty() {
        return None;
    }
    let head = &edge.parts[0];
    let tail = if edge.parts.len() > 1 {
        Some(edge.parts[1..].join("."))
    } else {
        None
    };
    if let Some(target) = aliases.get(head) {
        let candidate = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if package_index.lookup(&candidate).is_some() {
            return Some(candidate);
        }
        return Some(candidate);
    }
    // Same-namespace and containing-namespace lookup.
    if let Some(ns) = facts.package.as_deref().filter(|s| !s.is_empty()) {
        for prefix in containing_namespaces(ns) {
            let candidate = if prefix.is_empty() {
                edge.parts.join(".")
            } else {
                format!("{prefix}.{}", edge.parts.join("."))
            };
            if package_index.lookup(&candidate).is_some() {
                return Some(candidate);
            }
        }
    }
    // `using` namespace expansion.
    for b in &facts.import_bindings {
        if b.kind == ImportKind::Plain {
            let candidate = format!("{}.{}", b.fqn, edge.parts.join("."));
            if package_index.lookup(&candidate).is_some() {
                return Some(candidate);
            }
        }
    }
    // Bare FQN.
    let bare = edge.parts.join(".");
    if package_index.lookup(&bare).is_some() {
        return Some(bare);
    }
    None
}

fn linearize(
    class: &str,
    parents: &HashMap<String, Vec<String>>,
    classes: &HashSet<String>,
) -> Vec<String> {
    const MAX_HOPS: usize = 512;
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut queue: Vec<String> = vec![class.to_string()];
    let mut hops = 0usize;
    while let Some(cur) = queue.first().cloned() {
        queue.remove(0);
        if hops >= MAX_HOPS {
            break;
        }
        hops += 1;
        if !seen.insert(cur.clone()) {
            continue;
        }
        out.push(cur.clone());
        if let Some(bases) = parents.get(&cur) {
            for b in bases {
                if seen.contains(b) {
                    continue;
                }
                if classes.contains(b) {
                    queue.push(b.clone());
                } else {
                    seen.insert(b.clone());
                    out.push(b.clone());
                }
            }
        }
    }
    out
}
