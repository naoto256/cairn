//! Static method dispatch for Tier-2.5 Kotlin.
//!
//! We resolve a call when the receiver is statically pinnable:
//!   * `Cls.method(...)` — `Cls` is a workspace class (resolved through
//!     the alias map → in-package → wildcard-import → bare FQN cascade).
//!   * `Cls.Companion.method(...)` / `Cls.STATIC.method(...)` — class
//!     companion or nested object members.
//!   * `pkg.Cls.method(...)` — fully-qualified static call.
//!   * `this.method(...)` — current lexical class's MRO walk.
//!   * `super.method(...)` — MRO walk starting after the lexical class.
//!   * `foo(...)` — bare callee resolved through the alias map
//!     (covers `import x.y.foo`) → top-level function in current
//!     package → wildcard-import top-level lookup.
//!
//! `obj.method(...)` where `obj` is a local variable / parameter, and
//! reflection (`KFunction::invoke`, `Class.forName(...)`, etc.), and
//! extension functions on dynamic receivers are deliberately *not*
//! recorded. Extension functions on a statically-known receiver are
//! best-effort matched by name only.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, FileConstFacts, ImportKind, MethodCall, PackageIndex};
use crate::mro::Mro;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

/// Workspace-wide method index keyed by `(owner_qualified,
/// method_name)`. The owner is either a class FQN, a companion FQN,
/// or a package FQN (for top-level functions).
#[derive(Debug, Default)]
pub struct MethodIndex {
    by_owner: HashMap<(String, String), MethodEntry>,
    /// Package-level callables: `(package_fqn, name) → entry`. Lets
    /// `import com.foo.helper` resolve to the top-level function
    /// defined in package `com.foo`.
    by_package: HashMap<(String, String), MethodEntry>,
    /// Name-only fallback for extension functions / receiver-unknown
    /// calls. Best-effort: only used when no precise owner match
    /// found AND the workspace has exactly one method with this name
    /// (collisions stay unresolved).
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
                // Top-level functions: indexed under their package FQN
                // (the owner *is* the package).
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

    /// Unique name-only match (best-effort extension-function lookup).
    /// Returns `None` on collision or absence.
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
            // Skip the lexical class itself.
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
            // 1. Alias-bound bare name (`import com.foo.helper` ⇒
            // `helper()` resolves to `com.foo.helper`).
            if let Some(target_fqn) = aliases.get(name) {
                if let Some(hit) = package_index.lookup(target_fqn) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            // 2. Current-package top-level function.
            if let Some(pkg) = file_facts.package.as_deref() {
                if let Some(hit) = methods.get_package_callable(pkg, name) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            // 3. Wildcard-imported package top-level function.
            for b in &file_facts.import_bindings {
                if b.kind == ImportKind::Wildcard {
                    if let Some(hit) = methods.get_package_callable(&b.fqn, name) {
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
    // 1. Class-method dispatch: the dotted prefix resolves to a
    // workspace class — walk its MRO.
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
    // 2. Package-level call: `pkg.foo()` where `pkg` is a workspace
    // package. The dotted prefix is the package, the method is its
    // top-level function.
    if package_index.has_package(&class_qualified) {
        if let Some(hit) = methods.get_package_callable(&class_qualified, method) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    // 3. Composite FQN: `parts.method` resolves directly to a workspace
    // symbol (covers `Cls.STATIC_FIELD` chains via the package index).
    let composite = format!("{class_qualified}.{method}");
    if let Some(hit) = package_index.lookup(&composite) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    // 4. Last resort: name-only unique match. This is the best-effort
    // extension-function path — only fires when one workspace method
    // has that name, so collisions stay None.
    if let Some(hit) = methods.get_unique_by_name(method) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    None
}

/// Best-effort resolution of a dotted prefix to its workspace
/// qualified, applying alias → in-package → wildcard → bare lookup
/// rules. Returns the qualified name even when the workspace can't
/// pin it to a file, because the caller decides whether to treat
/// unresolved heads as package references or to drop them.
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
    // In-package: prepend file's package.
    if let Some(p) = package.filter(|s| !s.is_empty()) {
        return Some(format!("{p}.{}", parts.join(".")));
    }
    // Wildcard imports contribute candidate prefixes too — try them
    // before the bare form (so `import com.foo.*` + `Bar.baz()`
    // resolves to `com.foo.Bar.baz`).
    for b in &file_facts.import_bindings {
        if b.kind == ImportKind::Wildcard {
            return Some(format!("{}.{}", b.fqn, parts.join(".")));
        }
    }
    Some(parts.join("."))
}
