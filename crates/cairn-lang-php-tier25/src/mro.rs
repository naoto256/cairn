//! Method Resolution Order computation for PHP.
//!
//! PHP's MRO is simpler than Ruby's:
//!
//!   the class itself
//!   used traits (resolved before the parent — `use` brings methods in
//!     on top of the inherited ones)
//!   parent class (recursively)
//!   implemented interfaces (for static / type-level lookups)
//!
//! For Stage 1 we model:
//!   - instance chain: class → traits → parent chain → interfaces
//!   - static chain  : identical to instance for our resolution purposes
//!     (PHP `static::` is late-static binding; without runtime info we
//!     fall back to the same chain).

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{ConstIndex, FileConstFacts, MixinKind};

#[derive(Debug, Default)]
pub struct Mro {
    chain: HashMap<String, Vec<String>>,
    parent_of: HashMap<String, String>,
}

impl Mro {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)], const_index: &ConstIndex) -> Self {
        let mut extends: HashMap<String, Vec<String>> = HashMap::new();
        let mut implements: HashMap<String, Vec<String>> = HashMap::new();
        let mut traits: HashMap<String, Vec<String>> = HashMap::new();
        let mut classes: HashSet<String> = HashSet::new();

        // First pass: collect per-file namespace + aliases to resolve
        // base/interface/trait names that were written in their
        // imported (short) form.
        let mut file_aliases: HashMap<usize, (Option<String>, HashMap<String, String>)> =
            HashMap::new();
        for (i, (_, _, facts)) in per_file.iter().enumerate() {
            let mut m: HashMap<String, String> = HashMap::new();
            for u in &facts.use_imports {
                m.insert(u.alias.clone(), u.qualified.clone());
            }
            file_aliases.insert(i, (facts.namespace.clone(), m));
        }

        for (i, (_, _, facts)) in per_file.iter().enumerate() {
            let (ns, aliases) = file_aliases.get(&i).cloned().unwrap_or_default();
            for c in &facts.class_defs {
                classes.insert(c.qualified.clone());
            }
            for m in &facts.mixins {
                let resolved = resolve_target_qualified(
                    &m.module_parts,
                    m.absolute,
                    ns.as_deref(),
                    &aliases,
                    const_index,
                );
                let bucket = match m.kind {
                    MixinKind::Extends => extends.entry(m.owner.clone()).or_default(),
                    MixinKind::Implements => implements.entry(m.owner.clone()).or_default(),
                    MixinKind::TraitUse => traits.entry(m.owner.clone()).or_default(),
                };
                bucket.push(resolved);
            }
        }

        let mut parent_of: HashMap<String, String> = HashMap::new();
        for (owner, parents) in &extends {
            if let Some(first) = parents.first() {
                parent_of.insert(owner.clone(), first.clone());
            }
        }

        let mut chain: HashMap<String, Vec<String>> = HashMap::new();
        for class in &classes {
            chain.insert(
                class.clone(),
                compute_chain(class, &extends, &implements, &traits, &classes),
            );
        }

        Self { chain, parent_of }
    }

    /// Ancestors innermost-first, used for both instance and static
    /// dispatch in Stage 1.
    pub fn ancestors(&self, class: &str) -> Vec<String> {
        self.chain
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }

    /// Direct parent class qualified name (for `parent::method`).
    pub fn parent_of(&self, class: &str) -> Option<&str> {
        self.parent_of.get(class).map(String::as_str)
    }
}

fn compute_chain(
    class: &str,
    extends: &HashMap<String, Vec<String>>,
    implements: &HashMap<String, Vec<String>>,
    traits: &HashMap<String, Vec<String>>,
    classes: &HashSet<String>,
) -> Vec<String> {
    let mut chain: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let push = |chain: &mut Vec<String>, seen: &mut HashSet<String>, name: &str| {
        if seen.insert(name.to_string()) {
            chain.push(name.to_string());
        }
    };
    push(&mut chain, &mut seen, class);
    if let Some(ts) = traits.get(class) {
        for t in ts {
            push(&mut chain, &mut seen, t);
        }
    }
    let mut cursor = extends.get(class).and_then(|v| v.first()).cloned();
    let mut hops = 0;
    while let Some(parent) = cursor {
        if hops > 64 {
            break;
        }
        push(&mut chain, &mut seen, &parent);
        if !classes.contains(&parent) {
            break;
        }
        if let Some(ts) = traits.get(&parent) {
            for t in ts {
                push(&mut chain, &mut seen, t);
            }
        }
        cursor = extends.get(&parent).and_then(|v| v.first()).cloned();
        hops += 1;
    }
    if let Some(is) = implements.get(class) {
        for i in is {
            push(&mut chain, &mut seen, i);
        }
    }
    chain
}

fn resolve_target_qualified(
    parts: &[String],
    absolute: bool,
    namespace: Option<&str>,
    aliases: &HashMap<String, String>,
    const_index: &ConstIndex,
) -> String {
    if let Some(t) = const_index.resolve(parts, absolute, namespace, aliases) {
        return t.qualified;
    }
    if absolute {
        return parts.join("\\");
    }
    if let Some(head) = parts.first() {
        if let Some(alias_target) = aliases.get(head) {
            if parts.len() == 1 {
                return alias_target.clone();
            }
            return format!("{alias_target}\\{}", parts[1..].join("\\"));
        }
    }
    match namespace {
        Some(ns) if !ns.is_empty() => format!("{ns}\\{}", parts.join("\\")),
        _ => parts.join("\\"),
    }
}
