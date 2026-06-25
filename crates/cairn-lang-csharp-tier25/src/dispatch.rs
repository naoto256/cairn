//! Static method dispatch for Tier-2.5 C#.
//!
//! Resolves a call when the receiver is statically pinnable:
//!   * `Cls.Method(...)` — class static / nested type member.
//!   * `ns.Cls.Method(...)` — fully-qualified.
//!   * `this.Method(...)` — current lexical class's MRO walk.
//!   * `base.Method(...)` — MRO walk starting after the lexical class.
//!   * `Foo(...)` — bare callee resolved through alias / same-class
//!     method / same-namespace top-level (impossible in normal C#
//!     except top-level statements; we still accept it) /
//!     `using static` member lookup.
//!   * Best-effort name-only unique match for extension methods.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, FileConstFacts, ImportKind, MethodCall, PackageIndex};
use crate::containing_namespaces;
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

#[derive(Debug, Default)]
pub struct MethodIndex {
    by_owner: HashMap<(String, String), MethodEntry>,
    /// Package-level callables (top-level statements / Program.cs).
    by_package: HashMap<(String, String), MethodEntry>,
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
        let mut by_package = HashMap::new();
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
                if Some(m.owner.as_str()) == facts.package.as_deref() {
                    by_package
                        .entry((m.owner.clone(), m.name.clone()))
                        .or_insert(entry.clone());
                }
                by_name.entry(m.name.clone()).or_default().push(entry);
            }
        }
        Self {
            by_owner,
            by_package,
            by_name,
        }
    }

    fn get_method(&self, owner: &str, method: &str) -> Option<&MethodEntry> {
        self.by_owner.get(&(owner.to_string(), method.to_string()))
    }

    fn get_package_callable(&self, package: &str, name: &str) -> Option<&MethodEntry> {
        self.by_package
            .get(&(package.to_string(), name.to_string()))
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
        CallReceiver::ThisRef => {
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
            // 1. Lexical-class MRO: `Foo()` inside class X resolves to
            // `X.Foo` (or an ancestor's `Foo`).
            if let Some(owner) = call.lexical_class.as_deref() {
                for ancestor in mro.ancestors(owner) {
                    if let Some(hit) = methods.get_method(&ancestor, name) {
                        return Some(DispatchResolution {
                            path: hit.path.clone(),
                            qualified: hit.qualified.clone(),
                        });
                    }
                }
            }
            // 2. Alias-bound bare name.
            if let Some(target_fqn) = aliases.get(name) {
                if let Some(hit) = package_index.lookup(target_fqn) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            // 3. Same-namespace top-level (top-level statements).
            if let Some(pkg) = file_facts.package.as_deref() {
                for prefix in containing_namespaces(pkg) {
                    if let Some(hit) = methods.get_package_callable(&prefix, name) {
                        return Some(DispatchResolution {
                            path: hit.path.clone(),
                            qualified: hit.qualified.clone(),
                        });
                    }
                }
            }
            // 4. `using static A.B.C;` — try as a static member of C.
            for b in &file_facts.import_bindings {
                if b.kind == ImportKind::Static {
                    let candidate = format!("{}.{}", b.fqn, name);
                    if let Some(hit) = package_index.lookup(&candidate) {
                        return Some(DispatchResolution {
                            path: hit.path.clone(),
                            qualified: hit.qualified.clone(),
                        });
                    }
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
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, String>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    let class_qualified =
        resolve_dotted_to_qualified(parts, aliases, file_facts.package.as_deref(), file_facts)?;
    // 1. Class-method dispatch (walk MRO).
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
    // 2. Namespace-level: `ns.Foo()` where `ns` is a known namespace.
    if package_index.has_package(&class_qualified) {
        if let Some(hit) = methods.get_package_callable(&class_qualified, method) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    // 3. Composite FQN: `parts.method` is itself a workspace symbol
    // (e.g. `Cls.CONST` lookup).
    let composite = format!("{class_qualified}.{method}");
    if let Some(hit) = package_index.lookup(&composite) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    // 4. Last resort: unique name match (best-effort extension method
    // / instance dispatch on a known type).
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
    package: Option<&str>,
    file_facts: &FileConstFacts,
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
    // Same-namespace and containing-namespace lookup. We return the
    // first candidate even if it doesn't pin a workspace symbol; the
    // caller distinguishes the path.
    if let Some(p) = package.filter(|s| !s.is_empty()) {
        return Some(format!("{p}.{}", parts.join(".")));
    }
    // Wildcard imports.
    for b in &file_facts.import_bindings {
        if b.kind == ImportKind::Plain {
            return Some(format!("{}.{}", b.fqn, parts.join(".")));
        }
    }
    Some(parts.join("."))
}
