//! Static method dispatch for Tier-2.5 Ruby.
//!
//! We resolve a call when the receiver is statically pinnable:
//! a constant (`Foo.bar`), `self` inside a known class, or `super`.
//! Unknown / arbitrary expressions are *not* recorded — those belong to
//! Tier-3.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, ConstIndex, FileConstFacts, MethodCall};
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

/// Workspace-wide method index keyed by `(owner_qualified, method_name,
/// singleton)`. Built once per analyzer run alongside the const + MRO.
#[derive(Debug, Default)]
pub struct MethodIndex {
    by_owner: HashMap<(String, String, bool), MethodEntry>,
}

#[derive(Debug, Clone)]
struct MethodEntry {
    qualified: String,
    path: String,
}

impl MethodIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_owner = HashMap::new();
        for (path, _, facts) in per_file {
            for m in &facts.method_defs {
                by_owner
                    .entry((m.owner.clone(), m.name.clone(), m.singleton))
                    .or_insert(MethodEntry {
                        qualified: m.qualified.clone(),
                        path: path.clone(),
                    });
            }
        }
        Self { by_owner }
    }

    fn get(&self, owner: &str, method: &str, singleton: bool) -> Option<&MethodEntry> {
        self.by_owner
            .get(&(owner.to_string(), method.to_string(), singleton))
    }
}

pub fn resolve_call(
    call: &MethodCall,
    const_index: &ConstIndex,
    mro: &Mro,
    methods: &MethodIndex,
) -> Option<DispatchResolution> {
    match &call.receiver {
        CallReceiver::Const(parts) => {
            let target =
                const_index.resolve(parts, &call.lexical_scope, mro, &Default::default())?;
            resolve_on_singleton_chain(&target.qualified, &call.method, mro, methods)
        }
        CallReceiver::Self_ => {
            if call.lexical_scope.is_empty() {
                return None;
            }
            let owner = call.lexical_scope.join("::");
            if call.in_singleton_context {
                resolve_on_singleton_chain(&owner, &call.method, mro, methods)
            } else {
                resolve_on_instance_chain(&owner, &call.method, mro, methods)
            }
        }
        CallReceiver::Super => {
            if call.lexical_scope.is_empty() {
                return None;
            }
            let owner = call.lexical_scope.join("::");
            for ancestor in mro.ancestors(&owner).into_iter().skip(1) {
                if let Some(hit) = methods.get(&ancestor, &call.method, false) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            None
        }
        // Bare `foo(...)` inside a class body — Ruby's implicit self
        // dispatch. With a known lexical owner we can run the same MRO
        // walk as `self.foo`; outside any class (top-level scripts)
        // there is nothing to anchor on and Tier-3 must take over.
        CallReceiver::None => {
            if call.lexical_scope.is_empty() {
                return None;
            }
            let owner = call.lexical_scope.join("::");
            if call.in_singleton_context {
                resolve_on_singleton_chain(&owner, &call.method, mro, methods)
            } else {
                resolve_on_instance_chain(&owner, &call.method, mro, methods)
            }
        }
        CallReceiver::Unknown => None,
    }
}

fn resolve_on_singleton_chain(
    owner: &str,
    method: &str,
    mro: &Mro,
    methods: &MethodIndex,
) -> Option<DispatchResolution> {
    for ancestor in mro.singleton_ancestors(owner) {
        if let Some(hit) = methods.get(&ancestor, method, true) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    None
}

fn resolve_on_instance_chain(
    owner: &str,
    method: &str,
    mro: &Mro,
    methods: &MethodIndex,
) -> Option<DispatchResolution> {
    for ancestor in mro.ancestors(owner) {
        if let Some(hit) = methods.get(&ancestor, method, false) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    None
}
