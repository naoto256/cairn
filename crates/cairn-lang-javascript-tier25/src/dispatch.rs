//! Static method dispatch for Tier-2.5 JavaScript.
//!
//! Resolves a call when the receiver is statically pinnable:
//!   * `Cls.staticMethod()` — class static / module-binding member.
//!   * `this.method()` — current lexical class's MRO walk.
//!   * `super.method()` — MRO walk starting after the lexical class.
//!   * `foo()` — bare callee resolved through file-local top-level
//!     function or import binding.
//!   * `Ns.X()` — namespace import + member.
//!   * `new Foo().bar()` — receiver of `bar()` is the newly-
//!     constructed instance of `Foo`; walk Foo's MRO.

use std::collections::HashMap;

use crate::AliasTarget;
use crate::const_resolver::{CallReceiver, FileConstFacts, ImportKind, MethodCall, PackageIndex};
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

#[derive(Debug, Default)]
pub struct MethodIndex {
    /// (path, owner_class_qualified, method_name) → entry.
    by_owner: HashMap<(String, String, String), MethodEntry>,
    /// (path, function_name) → entry. Top-level functions.
    by_function: HashMap<(String, String), MethodEntry>,
}

#[derive(Debug, Clone)]
struct MethodEntry {
    qualified: String,
    path: String,
}

impl MethodIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_owner = HashMap::new();
        let mut by_function = HashMap::new();
        for (path, _, facts) in per_file {
            for m in &facts.method_defs {
                by_owner
                    .entry((path.clone(), m.owner.clone(), m.name.clone()))
                    .or_insert(MethodEntry {
                        qualified: m.qualified.clone(),
                        path: path.clone(),
                    });
            }
            for f in &facts.function_defs {
                by_function
                    .entry((path.clone(), f.name.clone()))
                    .or_insert(MethodEntry {
                        qualified: f.qualified.clone(),
                        path: path.clone(),
                    });
            }
        }
        Self {
            by_owner,
            by_function,
        }
    }

    fn get_method(&self, path: &str, owner: &str, name: &str) -> Option<&MethodEntry> {
        self.by_owner
            .get(&(path.to_string(), owner.to_string(), name.to_string()))
    }

    fn get_function(&self, path: &str, name: &str) -> Option<&MethodEntry> {
        self.by_function.get(&(path.to_string(), name.to_string()))
    }
}

pub fn resolve_call(
    source_path: &str,
    call: &MethodCall,
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, AliasTarget>,
    _file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    match &call.receiver {
        CallReceiver::ThisRef => {
            let owner = call.lexical_class.as_deref()?;
            // The class lives in this file (lexical_class came from
            // the visitor of source_path); walk its MRO chain.
            let ancestors = mro.ancestors_of(source_path, owner);
            for (apath, aname) in ancestors {
                if let Some(hit) = methods.get_method(&apath, &aname, &call.method) {
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
            let ancestors = mro.ancestors_of(source_path, owner);
            for (apath, aname) in ancestors.into_iter().skip(1) {
                if let Some(hit) = methods.get_method(&apath, &aname, &call.method) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            None
        }
        CallReceiver::Bare { name } => {
            // 1. Same-class MRO if inside a class.
            if let Some(owner) = call.lexical_class.as_deref() {
                let ancestors = mro.ancestors_of(source_path, owner);
                for (apath, aname) in ancestors {
                    if let Some(hit) = methods.get_method(&apath, &aname, name) {
                        return Some(DispatchResolution {
                            path: hit.path.clone(),
                            qualified: hit.qualified.clone(),
                        });
                    }
                }
            }
            // 2. File-local top-level function.
            if let Some(hit) = methods.get_function(source_path, name) {
                return Some(DispatchResolution {
                    path: hit.path.clone(),
                    qualified: hit.qualified.clone(),
                });
            }
            // 3. Imported binding: `import { foo } from './bar'; foo()`.
            if let Some(alias) = aliases.get(name) {
                if let Some(target_path) = &alias.target_path {
                    if let Some(target_qualified) = &alias.target_qualified {
                        if let Some(hit) =
                            package_index.lookup_in_file(target_path, target_qualified)
                        {
                            return Some(DispatchResolution {
                                path: hit.path.clone(),
                                qualified: hit.qualified.clone(),
                            });
                        }
                    }
                }
            }
            None
        }
        CallReceiver::Dotted { parts } => resolve_dotted_call(
            source_path,
            parts,
            &call.method,
            package_index,
            mro,
            methods,
            aliases,
        ),
        CallReceiver::NewExpr { class } => {
            // Mirror the `Cls.method()` path (see `resolve_dotted_call`
            // single-part case): if no alias / unique-workspace class
            // is found, fall back to a same-file class. Without this
            // fallback, `new Foo().bar()` in the file that declares
            // `Foo` fails to resolve whenever another workspace file
            // also defines a `Foo`, because `lookup_unique_class` is
            // ambiguous.
            let class_target = resolve_class_target(class, aliases, package_index)
                .or_else(|| same_file_class(source_path, class, package_index))?;
            let ancestors = mro.ancestors_of(&class_target.path, &class_target.qualified);
            for (apath, aname) in ancestors {
                if let Some(hit) = methods.get_method(&apath, &aname, &call.method) {
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
    source_path: &str,
    parts: &[String],
    method: &str,
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, AliasTarget>,
) -> Option<DispatchResolution> {
    if parts.is_empty() {
        return None;
    }
    let head = &parts[0];

    // 1. `Cls.method()` — single-part receiver.
    if parts.len() == 1 {
        let class_target = resolve_class_target(head, aliases, package_index)
            .or_else(|| same_file_class(source_path, head, package_index))?;
        // a) static-style: `Cls.method` registered as composite FQN.
        let composite = format!("{}.{}", class_target.qualified, method);
        if let Some(hit) = package_index.lookup_in_file(&class_target.path, &composite) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
        // b) instance-method-on-class lookup via MRO.
        let ancestors = mro.ancestors_of(&class_target.path, &class_target.qualified);
        for (apath, aname) in ancestors {
            if let Some(hit) = methods.get_method(&apath, &aname, method) {
                return Some(DispatchResolution {
                    path: hit.path.clone(),
                    qualified: hit.qualified.clone(),
                });
            }
        }
        return None;
    }

    // 2. `Ns.X()` — head is a namespace alias, tail = export name.
    //
    // Only an `import * as Ns from './foo'` shape lets us treat
    // `Ns.X` as the *named export* `X` of the target module. For a
    // default / named / CJS import (`import Foo from './foo'` etc.),
    // `Foo.X` is a runtime property access on the imported value
    // and we cannot statically pin it to a same-named export of the
    // module — doing so would silently mis-route (e.g. a default
    // import of class `Foo` calling `Foo.bar()` would jump to a
    // sibling export `bar` that happens to live in the same file).
    if let Some(alias) = aliases.get(head) {
        if alias.import_kind == ImportKind::EsmNamespace {
            if let Some(target_path) = &alias.target_path {
                let exported = &parts[1];
                if let Some(hit) = package_index.lookup_export(target_path, exported) {
                    let composite = format!("{}.{}", hit.qualified, method);
                    if let Some(static_hit) = package_index.lookup_in_file(&hit.path, &composite) {
                        return Some(DispatchResolution {
                            path: static_hit.path.clone(),
                            qualified: static_hit.qualified.clone(),
                        });
                    }
                    let ancestors = mro.ancestors_of(&hit.path, &hit.qualified);
                    for (apath, aname) in ancestors {
                        if let Some(method_hit) = methods.get_method(&apath, &aname, method) {
                            return Some(DispatchResolution {
                                path: method_hit.path.clone(),
                                qualified: method_hit.qualified.clone(),
                            });
                        }
                    }
                }
            }
        }
    }
    None
}

fn resolve_class_target(
    name: &str,
    aliases: &HashMap<String, AliasTarget>,
    package_index: &PackageIndex,
) -> Option<ResolvedClass> {
    if let Some(alias) = aliases.get(name) {
        if let (Some(target_path), Some(qualified)) =
            (alias.target_path.clone(), alias.target_qualified.clone())
        {
            return Some(ResolvedClass {
                path: target_path,
                qualified,
            });
        }
    }
    if let Some(hit) = package_index.lookup_unique_class(name) {
        return Some(ResolvedClass {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    None
}

fn same_file_class(
    source_path: &str,
    name: &str,
    package_index: &PackageIndex,
) -> Option<ResolvedClass> {
    package_index
        .lookup_in_file(source_path, name)
        .map(|hit| ResolvedClass {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        })
}

#[derive(Debug, Clone)]
struct ResolvedClass {
    path: String,
    qualified: String,
}
