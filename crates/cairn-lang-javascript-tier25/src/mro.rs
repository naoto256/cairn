//! Method Resolution Order for JavaScript.
//!
//! JS classes are single-inheritance (`class Sub extends Base`). The
//! base expression may be:
//!   * an identifier (`extends Base`) — resolved through this file's
//!     import bindings or same-file class definitions;
//!   * a member access (`extends ns.Base`) — `ns` must be a namespace
//!     import (`import * as ns from './foo'`) and `Base` an export of
//!     that module;
//!   * a call expression (`extends Mixin(Base)`) — deliberately
//!     skipped (mixin factory; we cannot statically pin Base without
//!     evaluating the call).
//!
//! Cross-file resolution: an `extends Base` where `Base` is imported
//! from another workspace file walks the require_graph to find the
//! exported FQN in that target file, then re-keys MRO under the
//! cross-file qualified pair `(target_path, qualified)`.

use std::collections::{HashMap, HashSet};

use crate::const_resolver::{BaseEdge, FileConstFacts, PackageIndex};
use crate::require_graph::RequireGraph;

/// Class identity is `(path, qualified)` since JS class FQNs are
/// short and only file-unique.
type ClassKey = (String, String);

#[derive(Debug, Default)]
pub struct Mro {
    /// For each class, the linearized ancestor list (self first).
    chain: HashMap<ClassKey, Vec<ClassKey>>,
}

impl Mro {
    pub fn build(
        per_file: &[(String, Vec<u8>, FileConstFacts)],
        package_index: &PackageIndex,
        require_graph: &RequireGraph,
    ) -> Self {
        let mut parents: HashMap<ClassKey, Vec<ClassKey>> = HashMap::new();
        let mut classes: HashSet<ClassKey> = HashSet::new();

        for (path, _, facts) in per_file {
            for class in &facts.class_defs {
                classes.insert((path.clone(), class.qualified.clone()));
            }
            for edge in &facts.base_edges {
                if let Some(parent_key) =
                    resolve_base(edge, path, facts, package_index, require_graph)
                {
                    parents
                        .entry((path.clone(), edge.owner.clone()))
                        .or_default()
                        .push(parent_key);
                }
            }
        }

        let mut chain: HashMap<ClassKey, Vec<ClassKey>> = HashMap::new();
        for class in &classes {
            chain.insert(class.clone(), linearize(class.clone(), &parents));
        }
        Self { chain }
    }

    /// Return ancestor qualified names in MRO order, starting with the
    /// class itself.
    pub fn ancestors_of(&self, path: &str, class: &str) -> Vec<ClassKey> {
        let key = (path.to_string(), class.to_string());
        self.chain.get(&key).cloned().unwrap_or_else(|| vec![key])
    }
}

fn resolve_base(
    edge: &BaseEdge,
    path: &str,
    facts: &FileConstFacts,
    package_index: &PackageIndex,
    require_graph: &RequireGraph,
) -> Option<ClassKey> {
    if edge.parts.is_empty() {
        return None;
    }
    let head = &edge.parts[0];

    // 1. Import binding: `import { Base }` or `const Base = require(...)`.
    let bindings = require_graph.bindings_for(path);
    if let Some(b) = bindings.iter().find(|b| &b.local == head) {
        // `extends Base` (head alone): the binding tells us the target
        // file and exported name; we re-derive the local class FQN in
        // that file via the export table.
        if edge.parts.len() == 1 {
            if let Some(target_path) = &b.target_path {
                // Resolve the exported name to the local class FQN.
                if let Some(exported_name) = &b.target_exported_name {
                    if let Some(hit) = package_index.lookup_export(target_path, exported_name) {
                        return Some((hit.path.clone(), hit.qualified.clone()));
                    }
                }
            }
            return None;
        }
        // `extends Ns.Base`: namespace import + member.
        if let Some(target_path) = &b.target_path {
            let member = edge.parts[1..].join(".");
            if let Some(hit) = package_index.lookup_export(target_path, &member) {
                return Some((hit.path.clone(), hit.qualified.clone()));
            }
        }
        return None;
    }

    // 2. Same-file class.
    if edge.parts.len() == 1 {
        for class in &facts.class_defs {
            if class.qualified == *head {
                return Some((path.to_string(), class.qualified.clone()));
            }
        }
    }

    // 3. Workspace-unique class name (best-effort).
    if edge.parts.len() == 1 {
        if let Some(hit) = package_index.lookup_unique_class(head) {
            return Some((hit.path.clone(), hit.qualified.clone()));
        }
    }

    None
}

fn linearize(class: ClassKey, parents: &HashMap<ClassKey, Vec<ClassKey>>) -> Vec<ClassKey> {
    const MAX_HOPS: usize = 256;
    let mut out: Vec<ClassKey> = Vec::new();
    let mut seen: HashSet<ClassKey> = HashSet::new();
    let mut cur = Some(class);
    let mut hops = 0usize;
    while let Some(c) = cur.take() {
        if hops >= MAX_HOPS {
            break;
        }
        hops += 1;
        if !seen.insert(c.clone()) {
            break;
        }
        out.push(c.clone());
        cur = parents.get(&c).and_then(|v| v.first().cloned());
    }
    out
}
