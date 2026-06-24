//! Method Resolution Order computation for Python.
//!
//! Python's MRO is officially C3 linearization (multiple inheritance
//! with consistent left-to-right + monotonic ordering). For Stage 1 we
//! implement a best-effort:
//!
//!   * The class itself.
//!   * Linearized C3 over the **workspace-resolvable** bases (we
//!     intentionally skip bases we cannot resolve to a workspace
//!     class — those are typically stdlib types like `object`,
//!     `Exception`, `abc.ABC`, etc., and Tier-2.5 should not invent
//!     edges for them).
//!   * On C3 conflict (the linearization is not consistent), we fall
//!     back to a stable DFS so callers always see a deterministic
//!     ancestor list rather than no chain at all.
//!
//! `super().method()` resolution is driven by the same chain: it walks
//! the lexical class's MRO starting *after* the lexical class itself.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::FileConstFacts;
use crate::const_resolver::ModuleIndex;
use crate::require_graph::RequireGraph;

#[derive(Debug, Default)]
pub struct Mro {
    chain: HashMap<String, Vec<String>>,
    parents: HashMap<String, Vec<String>>,
}

impl Mro {
    pub fn build(
        per_file: &[(String, Vec<u8>, FileConstFacts)],
        module_index: &ModuleIndex,
        require_graph: &RequireGraph,
    ) -> Self {
        // 1. Collect class -> [resolved parent qualified] across the
        // workspace. We resolve each base name through the same
        // alias/in-module/workspace cascade the rest of the resolver
        // uses, but rooted in **the file that declared the
        // subclass**.
        let mut parents: HashMap<String, Vec<String>> = HashMap::new();
        let mut classes: HashSet<String> = HashSet::new();

        for (path, _, facts) in per_file {
            // Per-file alias map: same shape as the one analyze_files
            // builds, but lives here so MRO can be computed before
            // we hand alias maps to dispatch.
            let mut aliases: HashMap<String, String> = HashMap::new();
            for binding in &facts.import_bindings {
                if let Some(q) = require_graph.resolve_binding(path, binding) {
                    aliases.insert(binding.local.clone(), q);
                }
            }

            for class in &facts.class_defs {
                classes.insert(class.qualified.clone());
            }

            for edge in &facts.base_edges {
                let resolved =
                    resolve_base(&edge.parts, &aliases, facts.module.as_deref(), module_index);
                if let Some(q) = resolved {
                    parents.entry(edge.owner.clone()).or_default().push(q);
                }
            }
        }

        // 2. Compute C3 chains. Best-effort: if C3 fails (inconsistent
        // bases), fall back to DFS so callers still get a chain.
        let mut chain: HashMap<String, Vec<String>> = HashMap::new();
        for class in &classes {
            let computed = c3_linearize(class, &parents, &classes)
                .unwrap_or_else(|| dfs_ancestors(class, &parents, &classes));
            chain.insert(class.clone(), computed);
        }

        Self { chain, parents }
    }

    /// Ancestors innermost-first, used for both instance and static
    /// dispatch in Stage 1.
    pub fn ancestors(&self, class: &str) -> Vec<String> {
        self.chain
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }

    /// First positional parent (for `super()` when only one base).
    pub fn parent_of(&self, class: &str) -> Option<&str> {
        self.parents
            .get(class)
            .and_then(|v| v.first())
            .map(String::as_str)
    }
}

fn resolve_base(
    parts: &[String],
    aliases: &std::collections::HashMap<String, String>,
    module: Option<&str>,
    module_index: &ModuleIndex,
) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let head = &parts[0];
    let tail = if parts.len() > 1 {
        Some(parts[1..].join("."))
    } else {
        None
    };
    if let Some(target) = aliases.get(head) {
        let combined = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if module_index.lookup(&combined).is_some() {
            return Some(combined);
        }
        // alias resolved a name we can't pin to a workspace class —
        // still consider this the canonical qualified for MRO purposes
        // so subclass relationships travel across import boundaries.
        return Some(combined);
    }
    if let Some(m) = module.filter(|s| !s.is_empty()) {
        let candidate = format!("{m}.{}", parts.join("."));
        if module_index.lookup(&candidate).is_some() {
            return Some(candidate);
        }
    }
    let bare = parts.join(".");
    if module_index.lookup(&bare).is_some() {
        return Some(bare);
    }
    None
}

/// C3 linearization. Returns `None` when the bases are inconsistent
/// (caller falls back to DFS).
fn c3_linearize(
    class: &str,
    parents: &HashMap<String, Vec<String>>,
    classes: &HashSet<String>,
) -> Option<Vec<String>> {
    fn linearize(
        class: &str,
        parents: &HashMap<String, Vec<String>>,
        classes: &HashSet<String>,
        depth: u32,
    ) -> Option<Vec<String>> {
        if depth > 64 {
            return None;
        }
        let mut out: Vec<String> = vec![class.to_string()];
        let bases = parents.get(class).cloned().unwrap_or_default();
        if bases.is_empty() {
            return Some(out);
        }
        let mut sequences: Vec<Vec<String>> = Vec::new();
        for base in &bases {
            // Workspace bases get their full linearization; non-workspace
            // bases participate as one-element sequences so they appear
            // exactly once in the merged result.
            if classes.contains(base) {
                let lin = linearize(base, parents, classes, depth + 1)?;
                sequences.push(lin);
            } else {
                sequences.push(vec![base.clone()]);
            }
        }
        sequences.push(bases.clone());

        // Standard C3 merge.
        loop {
            sequences.retain(|s| !s.is_empty());
            if sequences.is_empty() {
                break;
            }
            // Pick a head that does not appear in the tail of any
            // other sequence.
            let mut candidate: Option<String> = None;
            for seq in &sequences {
                let head = &seq[0];
                let in_tail = sequences
                    .iter()
                    .any(|s| s.iter().skip(1).any(|x| x == head));
                if !in_tail {
                    candidate = Some(head.clone());
                    break;
                }
            }
            let chosen = candidate?;
            out.push(chosen.clone());
            for seq in sequences.iter_mut() {
                if seq.first() == Some(&chosen) {
                    seq.remove(0);
                }
            }
        }
        Some(out)
    }
    linearize(class, parents, classes, 0)
}

/// Stable DFS fallback when C3 cannot linearize.
fn dfs_ancestors(
    class: &str,
    parents: &HashMap<String, Vec<String>>,
    classes: &HashSet<String>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = vec![class.to_string()];
    let mut hops = 0u32;
    while let Some(cur) = stack.pop() {
        if hops > 256 {
            break;
        }
        hops += 1;
        if !seen.insert(cur.clone()) {
            continue;
        }
        out.push(cur.clone());
        if let Some(bases) = parents.get(&cur) {
            // push in reverse so we visit left-to-right (workspace
            // bases first, matching readable inheritance order).
            for b in bases.iter().rev() {
                if classes.contains(b) {
                    stack.push(b.clone());
                } else if !seen.contains(b) {
                    // record non-workspace bases in the chain so super()
                    // walks have a deterministic next step.
                    seen.insert(b.clone());
                    out.push(b.clone());
                }
            }
        }
    }
    out
}
