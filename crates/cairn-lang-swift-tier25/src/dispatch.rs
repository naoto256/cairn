//! Static method dispatch for Tier-2.5 Swift.
//!
//! We resolve a call when the receiver is statically pinnable:
//!   * `Cls.method(...)` — `Cls` is a workspace type (resolved
//!     through the alias map → in-module → bare FQN cascade).
//!   * `module.Cls.method(...)` — fully-qualified static call.
//!   * `self.method(...)` — current lexical type's MRO walk.
//!   * `super.method(...)` — MRO walk starting after the lexical
//!     type.
//!   * `foo(...)` — bare callee resolved through the alias map
//!     (covers `import struct Foundation.Date` style imports) → top-
//!     level function lookup.
//!
//! `obj.method(...)` where `obj` is a local variable / parameter,
//! existential / `Any` dispatch, Mirror / KVC reflection, and
//! Foundation / UIKit / SwiftUI framework calls are deliberately
//! *not* recorded. Protocol-extension default implementations on a
//! statically-known receiver are best-effort matched by name only.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, FileConstFacts, MethodCall, PackageIndex};
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

/// Workspace-wide method index keyed by `(owner_qualified,
/// method_name)`. The owner is either a class FQN or an empty
/// string (top-level functions in module-less Swift files).
#[derive(Debug, Default)]
pub struct MethodIndex {
    by_owner: HashMap<(String, String), MethodEntry>,
    by_module: HashMap<(String, String), MethodEntry>,
    by_name: HashMap<String, Vec<MethodEntry>>,
}

#[derive(Debug, Clone)]
struct MethodEntry {
    qualified: String,
    path: String,
}

impl MethodIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_owner = HashMap::new();
        let mut by_module = HashMap::new();
        let mut by_name: HashMap<String, Vec<MethodEntry>> = HashMap::new();
        for (path, _, facts) in per_file {
            for m in &facts.method_defs {
                let entry = MethodEntry {
                    qualified: m.qualified.clone(),
                    path: path.clone(),
                };
                by_owner
                    .entry((m.owner.clone(), m.name.clone()))
                    .or_insert(entry.clone());
                if let Some(module) = facts.module.as_deref() {
                    if m.owner == module {
                        by_module
                            .entry((module.to_string(), m.name.clone()))
                            .or_insert(entry.clone());
                    }
                }
                by_name.entry(m.name.clone()).or_default().push(entry);
            }
        }
        Self {
            by_owner,
            by_module,
            by_name,
        }
    }

    fn get_method(&self, owner: &str, method: &str) -> Option<&MethodEntry> {
        self.by_owner.get(&(owner.to_string(), method.to_string()))
    }

    fn get_module_callable(&self, module: &str, name: &str) -> Option<&MethodEntry> {
        self.by_module.get(&(module.to_string(), name.to_string()))
    }

    fn get_unique_by_name(&self, name: &str) -> Option<&MethodEntry> {
        let bucket = self.by_name.get(name)?;
        if bucket.len() == 1 {
            bucket.first()
        } else {
            None
        }
    }
}

pub fn resolve_call(
    call: &MethodCall,
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    match &call.receiver {
        CallReceiver::Dotted { parts } => resolve_dotted_call(
            parts,
            &call.method,
            package_index,
            mro,
            methods,
            aliases,
            file_facts,
        ),
        CallReceiver::SelfRef => {
            let owner = call.lexical_class.clone()?;
            for ancestor in mro.ancestors(&owner) {
                if let Some(hit) = methods.get_method(&ancestor, &call.method) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            None
        }
        CallReceiver::SuperRef => {
            let owner = call.lexical_class.as_deref()?;
            let chain = mro.ancestors(owner);
            for ancestor in chain.into_iter().skip(1) {
                if let Some(hit) = methods.get_method(&ancestor, &call.method) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            None
        }
        CallReceiver::Bare { name } => {
            // 1. Alias-bound bare name (imported binding that
            // resolves to a workspace top-level callable).
            if let Some(target_fqn) = aliases.get(name) {
                if let Some(hit) = package_index.lookup(target_fqn) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            // 2. Current-module top-level function.
            if let Some(module) = file_facts.module.as_deref() {
                if let Some(hit) = methods.get_module_callable(module, name) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            // 3. Bare top-level callable (module-less workspace
            // file). Indexed under owner = "".
            if let Some(hit) = methods.get_method("", name) {
                return Some(DispatchResolution {
                    path: hit.path.clone(),
                    qualified: hit.qualified.clone(),
                });
            }
            None
        }
        CallReceiver::Unknown => None,
    }
}

fn resolve_dotted_call(
    parts: &[String],
    method: &str,
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    let class_qualified =
        resolve_dotted_to_qualified(parts, aliases, file_facts.module.as_deref(), file_facts)?;

    // 1. Class-method dispatch: the dotted prefix resolves to a
    // workspace type — walk its MRO.
    if package_index.lookup(&class_qualified).is_some() {
        for ancestor in mro.ancestors(&class_qualified) {
            if let Some(hit) = methods.get_method(&ancestor, method) {
                return Some(DispatchResolution {
                    path: hit.path.clone(),
                    qualified: hit.qualified.clone(),
                });
            }
        }
    }
    // 2. Module-level call: `module.foo()` where `module` is a
    // workspace module.
    if package_index.has_module(&class_qualified) {
        if let Some(hit) = methods.get_module_callable(&class_qualified, method) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    // 3. Composite FQN: `parts.method` resolves directly to a
    // workspace symbol (covers `Cls.staticMember` chains).
    let composite = format!("{class_qualified}.{method}");
    if let Some(hit) = package_index.lookup(&composite) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    // 4. Last resort: name-only unique match. Protocol-extension
    // default implementation lookup falls through to this branch.
    if let Some(hit) = methods.get_unique_by_name(method) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    None
}

fn resolve_dotted_to_qualified(
    parts: &[String],
    aliases: &HashMap<String, String>,
    module: Option<&str>,
    _file_facts: &FileConstFacts,
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
        return Some(match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        });
    }
    if let Some(m) = module.filter(|s| !s.is_empty()) {
        return Some(format!("{m}.{}", parts.join(".")));
    }
    Some(parts.join("."))
}
