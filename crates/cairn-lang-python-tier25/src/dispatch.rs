//! Static method dispatch for Tier-2.5 Python.
//!
//! We resolve a call when the receiver is statically pinnable:
//!   * `Cls.method(...)` — `Cls` is a workspace class (resolved through
//!     the alias map → in-module → workspace index cascade).
//!   * `mod.fn(...)` — `mod` is a workspace `import` binding and `fn`
//!     is a module-level function/class in that module.
//!   * `self.method(...)` — current lexical class's MRO walk.
//!   * `cls.method(...)` — same as `self.method` for Tier-2.5.
//!   * `super().method(...)` — MRO walk starting at the lexical
//!     class's first parent.
//!   * `foo(...)` — bare name resolved through the alias map (covers
//!     `from m import foo` and `import foo`).
//!
//! `obj.method(...)` (unknown receiver type) and `getattr` /
//! attribute-set / metaclass / decorator-rewritten callables are
//! deliberately *not* recorded.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, FileConstFacts, MethodCall, ModuleIndex};
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

/// Workspace-wide method index keyed by `(class_qualified, method_name)`.
/// Module-level functions are indexed under their module's qualified
/// name as the owner (so `pkg.mod.foo` lives at owner `pkg.mod`).
#[derive(Debug, Default)]
pub struct MethodIndex {
    by_owner: HashMap<(String, String), MethodEntry>,
    /// Module-level callables: `(module_qualified, name) → entry`.
    by_module: HashMap<(String, String), MethodEntry>,
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
        for (path, _, facts) in per_file {
            for m in &facts.method_defs {
                by_owner
                    .entry((m.owner.clone(), m.name.clone()))
                    .or_insert(MethodEntry {
                        qualified: m.qualified.clone(),
                        path: path.clone(),
                    });
            }
            // Module-level: top-level class defs + top-level functions
            // (which `const_resolver` records as method_defs whose owner
            // is the module itself).
            if let Some(module) = facts.module.as_deref() {
                for class in &facts.class_defs {
                    if let Some(name) = class.qualified.strip_prefix(&format!("{module}.")) {
                        // Only record bare-name (`Foo`) — nested
                        // classes are reachable through their parent
                        // class's qualified anyway.
                        if !name.contains('.') {
                            by_module
                                .entry((module.to_string(), name.to_string()))
                                .or_insert(MethodEntry {
                                    qualified: class.qualified.clone(),
                                    path: path.clone(),
                                });
                        }
                    }
                }
                for m in &facts.method_defs {
                    if m.owner == module {
                        by_module
                            .entry((module.to_string(), m.name.clone()))
                            .or_insert(MethodEntry {
                                qualified: m.qualified.clone(),
                                path: path.clone(),
                            });
                    }
                }
            }
        }
        Self {
            by_owner,
            by_module,
        }
    }

    fn get_method(&self, owner: &str, method: &str) -> Option<&MethodEntry> {
        self.by_owner.get(&(owner.to_string(), method.to_string()))
    }

    fn get_module_callable(&self, module: &str, name: &str) -> Option<&MethodEntry> {
        self.by_module.get(&(module.to_string(), name.to_string()))
    }
}

pub fn resolve_call(
    call: &MethodCall,
    module_index: &ModuleIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    match &call.receiver {
        CallReceiver::Dotted { parts } => resolve_dotted_call(
            parts,
            &call.method,
            module_index,
            mro,
            methods,
            aliases,
            file_facts,
        ),
        CallReceiver::SelfRef | CallReceiver::ClsRef => {
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
            // Skip the lexical class itself; super walks from the next
            // ancestor.
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
            // `foo()` — alias map first (covers `from m import foo` /
            // `import foo`), then same-module function.
            if let Some(target_qualified) = aliases.get(name) {
                if let Some(hit) = module_index.lookup(target_qualified) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            if let Some(module) = file_facts.module.as_deref() {
                if let Some(hit) = methods.get_module_callable(module, name) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            None
        }
        CallReceiver::Unknown => None,
    }
}

fn resolve_dotted_call(
    parts: &[String],
    method: &str,
    module_index: &ModuleIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    // First, try to resolve the dotted prefix as a class. If we land on
    // a workspace class, do MRO-walked method lookup.
    let class_qualified = resolve_dotted_to_qualified(parts, aliases, file_facts.module.as_deref());
    let class_qualified = match class_qualified {
        Some(q) => q,
        None => {
            // Fall back: maybe parts is a module reference.
            return resolve_module_attribute(parts, method, module_index, methods, aliases);
        }
    };
    if module_index.lookup(&class_qualified).is_some() {
        for ancestor in mro.ancestors(&class_qualified) {
            if let Some(hit) = methods.get_method(&ancestor, method) {
                return Some(DispatchResolution {
                    path: hit.path.clone(),
                    qualified: hit.qualified.clone(),
                });
            }
        }
    }
    // Otherwise treat as module attribute (`mod.fn(...)`).
    resolve_module_attribute(parts, method, module_index, methods, aliases)
}

fn resolve_module_attribute(
    parts: &[String],
    method: &str,
    module_index: &ModuleIndex,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
) -> Option<DispatchResolution> {
    // Build a candidate module name: alias-substituted prefix + dotted
    // remainder. We try the alias substitution first; if the head isn't
    // aliased, the bare dotted path is also tried (in case the file
    // does `import pkg.mod` and then calls `pkg.mod.fn()`).
    if parts.is_empty() {
        return None;
    }
    let head = &parts[0];
    let rest: Vec<String> = parts[1..].to_vec();

    let module_qualified = if let Some(target) = aliases.get(head) {
        if rest.is_empty() {
            target.clone()
        } else {
            format!("{target}.{}", rest.join("."))
        }
    } else {
        parts.join(".")
    };

    // If the module path is itself a workspace module, look up
    // `(module, method)` in the method index.
    if module_index.module_path(&module_qualified).is_some() {
        if let Some(hit) = methods.get_module_callable(&module_qualified, method) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    // Fall through: maybe the *whole dotted path* (parts + method) was
    // an aliased binding from `from m import some_fn` (no, we'd hit
    // `Bare` for that). One last attempt: `qualified = module_qualified + method`
    // pinned to module_index.
    let candidate = format!("{module_qualified}.{method}");
    if let Some(hit) = module_index.lookup(&candidate) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    None
}

/// Resolve a dotted prefix to its workspace qualified, applying
/// alias-map → in-module → bare lookup rules. Returns the qualified
/// name even when the workspace can't pin it to a file, because the
/// caller decides whether to treat unresolved heads as module
/// references or to drop them.
fn resolve_dotted_to_qualified(
    parts: &[String],
    aliases: &HashMap<String, String>,
    module: Option<&str>,
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
