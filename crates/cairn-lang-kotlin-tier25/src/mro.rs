//! Method Resolution Order computation for Kotlin.
//!
//! Kotlin has single class inheritance plus multiple interface
//! implementation. For Tier-2.5 we compute a best-effort linearization
//! that walks the superclass chain first, then interfaces in order of
//! declaration. This matches the JVM's `getInterfaces()` order well
//! enough for static dispatch (the common case is single inheritance
//! plus a handful of interfaces; conflicts that would matter for JVM
//! invokevirtual resolution belong to Tier-3).
//!
//! The `inherit` vs `implement` distinction comes from the Tier-2
//! `constructor_invocation` heuristic (recorded in
//! `BaseEdge::is_constructor_invocation`). It's load-bearing here for
//! one reason only: `inherit` edges contribute the superclass to the
//! *front* of the ancestor chain (matching JVM extends), while
//! `implement` edges append interfaces *after* the superclass chain.
//! Conflicts on diamond interface inheritance are resolved left-to-
//! right; calling code that actually depends on JVM-defined precedence
//! belongs to Tier-3.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{BaseEdge, FileConstFacts, ImportBinding, ImportKind, PackageIndex};

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
        // 1. Collect each class's resolved parent FQNs.
        let mut parents: HashMap<String, Vec<String>> = HashMap::new();
        let mut interfaces: HashMap<String, Vec<String>> = HashMap::new();
        let mut classes: HashSet<String> = HashSet::new();

        for (_path, _, facts) in per_file {
            // Build the file's alias map from imports — same shape as
            // analyze_files uses, but local to MRO computation.
            let aliases = build_aliases(facts);
            for class in &facts.class_defs {
                classes.insert(class.qualified.clone());
            }
            for edge in &facts.base_edges {
                if let Some(resolved) = resolve_base(edge, &aliases, facts, package_index) {
                    if edge.is_constructor_invocation {
                        parents
                            .entry(edge.owner.clone())
                            .or_default()
                            .push(resolved);
                    } else {
                        interfaces
                            .entry(edge.owner.clone())
                            .or_default()
                            .push(resolved);
                    }
                }
            }
        }

        // 2. Compute chains: class itself, then walk single
        // superclass chain, then interfaces (own + inherited). Stable,
        // deterministic, and cheap.
        let mut chain: HashMap<String, Vec<String>> = HashMap::new();
        let combined_parents: HashMap<String, Vec<String>> = classes
            .iter()
            .map(|c| {
                let mut all = parents.get(c).cloned().unwrap_or_default();
                all.extend(interfaces.get(c).cloned().unwrap_or_default());
                (c.clone(), all)
            })
            .collect();
        for class in &classes {
            let computed = linearize(class, &combined_parents, &classes);
            chain.insert(class.clone(), computed);
        }

        Self {
            chain,
            parents: combined_parents,
        }
    }

    /// Ancestors innermost-first, used for both instance and static
    /// dispatch.
    pub fn ancestors(&self, class: &str) -> Vec<String> {
        self.chain
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }

    /// Direct superclass / interface set (workspace-resolvable bases
    /// only). Used by `super.method()` walks.
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

/// Reduce an `ImportBinding` to a `(local, fqn)` alias pair for use in
/// dotted-name resolution. Wildcard imports don't produce a single
/// local binding — they're handled separately at the call site.
pub(crate) fn alias_binding(b: &ImportBinding) -> Option<(String, String)> {
    match b.kind {
        ImportKind::Plain | ImportKind::Aliased => Some((b.local.clone(), b.fqn.clone())),
        ImportKind::Wildcard => None,
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
    // 1. Alias match on the head.
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
        // Even when we can't pin it to a workspace symbol, keep the
        // resolved name so subclass relationships travel across import
        // boundaries (matches the Python tier-2.5 fallback).
        return Some(candidate);
    }
    // 2. Same-package lookup: prepend the file's package.
    if let Some(pkg) = facts.package.as_deref().filter(|s| !s.is_empty()) {
        let candidate = format!("{pkg}.{}", edge.parts.join("."));
        if package_index.lookup(&candidate).is_some() {
            return Some(candidate);
        }
    }
    // 3. Wildcard-import expansion: for each `import x.y.*` binding,
    // try `x.y.<bare>` (and the bare resolved head + tail).
    for b in &facts.import_bindings {
        if b.kind == ImportKind::Wildcard {
            let candidate = format!("{}.{}", b.fqn, edge.parts.join("."));
            if package_index.lookup(&candidate).is_some() {
                return Some(candidate);
            }
        }
    }
    // 4. Bare FQN as written (covers fully-qualified usage at the
    // declaration site, like `class Sub : com.foo.Base()`).
    let bare = edge.parts.join(".");
    if package_index.lookup(&bare).is_some() {
        return Some(bare);
    }
    None
}

/// Best-effort linearization. The class itself first, then BFS over
/// resolved parents (workspace classes get their own ancestors
/// inlined; non-workspace parents appear once and don't recurse).
/// Bounded by `MAX_HOPS` to keep pathological cycles from looping.
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
                    // Non-workspace parent: record once, don't recurse.
                    seen.insert(b.clone());
                    out.push(b.clone());
                }
            }
        }
    }
    out
}
