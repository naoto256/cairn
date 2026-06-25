//! Method Resolution Order computation for Swift.
//!
//! Swift has single class inheritance plus multiple protocol
//! conformance, but the inheritance clause syntax cannot tell which
//! entry is the superclass — the Tier-2 backend records every edge
//! as "inherit" for the same reason. For Tier-2.5 we compute a
//! best-effort linearization that walks every resolved parent in
//! declaration order. This matches the common case (single
//! superclass + one or two protocol conformances); precise witness
//! table dispatch belongs to Tier-3 once a SourceKit-backed
//! resolver is wired in.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{BaseEdge, FileConstFacts, ImportBinding, PackageIndex};

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
            let computed = linearize(class, &parents, &classes);
            chain.insert(class.clone(), computed);
        }

        Self { chain, parents }
    }

    /// Ancestors innermost-first, used for both instance and static
    /// dispatch.
    pub fn ancestors(&self, class: &str) -> Vec<String> {
        self.chain
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }

    /// Direct parent set (workspace-resolvable bases only). Used by
    /// `super.method()` walks.
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

/// Reduce an `ImportBinding` to a `(local, fqn)` alias pair for use
/// in dotted-name resolution. Swift imports are all plain bindings
/// of the form `local = first dotted segment`, `fqn = full path`.
pub(crate) fn alias_binding(b: &ImportBinding) -> Option<(String, String)> {
    Some((b.local.clone(), b.fqn.clone()))
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
    // 1. Alias match on the head.
    if let Some(target) = aliases.get(head) {
        let candidate = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if package_index.lookup(&candidate).is_some() {
            return Some(candidate);
        }
        // Keep the resolved name even when not pinnable, so subclass
        // relationships travel across import boundaries.
        return Some(candidate);
    }
    // 2. Same-module lookup.
    if let Some(module) = facts.module.as_deref().filter(|s| !s.is_empty()) {
        let candidate = format!("{module}.{}", edge.parts.join("."));
        if package_index.lookup(&candidate).is_some() {
            return Some(candidate);
        }
    }
    // 3. Bare FQN as written.
    let bare = edge.parts.join(".");
    if package_index.lookup(&bare).is_some() {
        return Some(bare);
    }
    None
}

/// Best-effort linearization. The class itself first, then BFS over
/// resolved parents.
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
