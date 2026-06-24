//! Method Resolution Order computation for the workspace.
//!
//! For Ruby 1.9+, Module includes are flattened into a linear MRO with
//! the following precedence (innermost-first):
//!
//!   prepended modules (last `prepend` first)
//!   the class itself
//!   included modules (last `include` first)
//!   superclass chain (recursively)
//!
//! `extend` adds methods to the singleton class, which we track on a
//! parallel singleton chain so `Foo.bar` dispatch can resolve extended
//! module methods.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{ConstIndex, FileConstFacts, MixinKind};

#[derive(Debug, Default)]
pub struct Mro {
    instance: HashMap<String, Vec<String>>,
    singleton: HashMap<String, Vec<String>>,
}

impl Mro {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)], const_index: &ConstIndex) -> Self {
        // Aggregate inheritance facts across files.
        let mut superclass: HashMap<String, String> = HashMap::new();
        let mut includes: HashMap<String, Vec<String>> = HashMap::new();
        let mut prepends: HashMap<String, Vec<String>> = HashMap::new();
        let mut extends: HashMap<String, Vec<String>> = HashMap::new();
        let mut classes: HashSet<String> = HashSet::new();

        for (_, _, facts) in per_file {
            for c in &facts.class_defs {
                classes.insert(c.qualified.clone());
                if let Some(parts) = &c.superclass {
                    let qualified = parts.join("::");
                    let resolved = const_index
                        .get(&qualified)
                        .map(|t| t.qualified.clone())
                        .unwrap_or(qualified);
                    superclass.entry(c.qualified.clone()).or_insert(resolved);
                }
            }
            for m in &facts.mixins {
                let qualified = m.module_parts.join("::");
                let resolved = const_index
                    .get(&qualified)
                    .map(|t| t.qualified.clone())
                    .unwrap_or(qualified);
                let bucket = match m.kind {
                    MixinKind::Include => includes.entry(m.owner.clone()).or_default(),
                    MixinKind::Prepend => prepends.entry(m.owner.clone()).or_default(),
                    MixinKind::Extend => extends.entry(m.owner.clone()).or_default(),
                };
                bucket.push(resolved);
            }
        }

        let mut instance: HashMap<String, Vec<String>> = HashMap::new();
        let mut singleton: HashMap<String, Vec<String>> = HashMap::new();
        for class in &classes {
            instance.insert(
                class.clone(),
                compute_instance_chain(class, &superclass, &includes, &prepends, &classes),
            );
            singleton.insert(
                class.clone(),
                compute_singleton_chain(
                    class,
                    &extends,
                    &superclass,
                    &includes,
                    &prepends,
                    &classes,
                ),
            );
        }
        Self {
            instance,
            singleton,
        }
    }

    /// Ancestors for instance-method dispatch, innermost-first.
    pub fn ancestors(&self, class: &str) -> Vec<String> {
        self.instance
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }

    /// Ancestors for singleton-method dispatch (`Foo.bar`).
    pub fn singleton_ancestors(&self, class: &str) -> Vec<String> {
        self.singleton
            .get(class)
            .cloned()
            .unwrap_or_else(|| vec![class.to_string()])
    }
}

fn compute_instance_chain(
    class: &str,
    superclass: &HashMap<String, String>,
    includes: &HashMap<String, Vec<String>>,
    prepends: &HashMap<String, Vec<String>>,
    classes: &HashSet<String>,
) -> Vec<String> {
    let mut chain: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let visit = |chain: &mut Vec<String>, seen: &mut HashSet<String>, name: &str| {
        if seen.insert(name.to_string()) {
            chain.push(name.to_string());
        }
    };

    // prepended modules, last-prepended takes precedence.
    if let Some(ps) = prepends.get(class) {
        for p in ps.iter().rev() {
            visit(&mut chain, &mut seen, p);
        }
    }
    visit(&mut chain, &mut seen, class);
    if let Some(is) = includes.get(class) {
        for i in is.iter().rev() {
            visit(&mut chain, &mut seen, i);
        }
    }
    let mut cursor = superclass.get(class).cloned();
    let mut hops = 0;
    while let Some(parent) = cursor {
        if hops > 64 || !classes.contains(&parent) {
            visit(&mut chain, &mut seen, &parent);
            break;
        }
        let sub = compute_instance_chain(&parent, superclass, includes, prepends, classes);
        for s in sub {
            visit(&mut chain, &mut seen, &s);
        }
        cursor = superclass.get(&parent).cloned();
        hops += 1;
    }
    chain
}

fn compute_singleton_chain(
    class: &str,
    extends: &HashMap<String, Vec<String>>,
    superclass: &HashMap<String, String>,
    includes: &HashMap<String, Vec<String>>,
    prepends: &HashMap<String, Vec<String>>,
    classes: &HashSet<String>,
) -> Vec<String> {
    let mut chain: Vec<String> = vec![class.to_string()];
    let mut seen: HashSet<String> = chain.iter().cloned().collect();
    if let Some(es) = extends.get(class) {
        for e in es.iter().rev() {
            if seen.insert(e.clone()) {
                chain.push(e.clone());
            }
        }
    }
    // Superclass's singleton chain participates too — methods defined on a
    // parent's singleton class are visible on the child class. Stage 1 keeps
    // this minimal: we just walk parent classes and add them.
    let mut cursor = superclass.get(class).cloned();
    let mut hops = 0;
    while let Some(parent) = cursor {
        if hops > 64 {
            break;
        }
        if seen.insert(parent.clone()) {
            chain.push(parent.clone());
        }
        if !classes.contains(&parent) {
            break;
        }
        cursor = superclass.get(&parent).cloned();
        hops += 1;
    }
    let _ = (includes, prepends);
    chain
}
