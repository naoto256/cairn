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
//!   * `foo(...)` — bare callee resolved through lexical class MRO,
//!     the alias map (covers `import x.y.foo`), top-level function in
//!     current package, or wildcard-import top-level lookup.
//!
//! `obj.method(...)` where `obj` is a local variable / parameter, and
//! reflection (`KFunction::invoke`, `Class.forName(...)`, etc.), and
//! extension functions on dynamic receivers are deliberately *not*
//! recorded. Extension functions on a statically-known receiver are
//! best-effort matched by name only.
//!
//! **Path discipline.** Every candidate qualified produced by the
//! resolver cascade is validated against `PackageIndex` before being
//! adopted — either via `lookup_in_file(path, qualified)` (when the
//! resolver knows where the candidate should live) or via
//! `lookup_unique(qualified)` (when the candidate isn't path-bound,
//! e.g. an external-binding alias fallback). Returning an unchecked
//! qualified from any stage would cut off later fallbacks; doing so is
//! the bug Sub-fix B exists to prevent.

use std::collections::HashMap;

use crate::const_resolver::{CallReceiver, FileConstFacts, ImportKind, MethodCall, PackageIndex};
use crate::mro::Mro;
use crate::require_graph::ResolvedBinding;

#[derive(Debug, Clone)]
pub struct DispatchResolution {
    pub path: String,
    pub qualified: String,
}

/// Workspace-wide method index keyed by `(owner_qualified,
/// method_name)`. The owner is either a class FQN, a companion FQN,
/// or a package FQN (for top-level functions).
///
/// Owner-FQN is unique workspace-wide under Kotlin's package rules,
/// so we don't need to key by `(path, owner)` the way the JS backend
/// does (JS qualifieds are file-local short names; Kotlin qualifieds
/// already include the package prefix). MRO walks remain FQN-keyed
/// for the same reason — see `Mro::ancestors` for the explicit
/// argument.
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

/// Candidate class target with both path and qualified — every stage
/// of the resolver chain returns this rather than a bare string, so
/// every adoption goes through a `PackageIndex` validation step.
#[derive(Debug, Clone)]
struct ResolvedClass {
    path: String,
    qualified: String,
}

#[allow(clippy::too_many_arguments)]
pub fn resolve_call(
    source_path: &str,
    call: &MethodCall,
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, ResolvedBinding>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    match &call.receiver {
        CallReceiver::Dotted { parts } => resolve_dotted_call(
            source_path,
            parts,
            &call.method,
            package_index,
            mro,
            methods,
            aliases,
            file_facts,
        ),
        CallReceiver::ThisRef => {
            // `this.method()` — walk lexical class's MRO including the
            // class itself.
            let owner = call.lexical_class.as_deref()?;
            walk_mro(owner, &call.method, mro, methods, 0)
        }
        CallReceiver::SuperRef => {
            // `super.method()` — skip the lexical class itself; we want
            // the inherited definition.
            let owner = call.lexical_class.as_deref()?;
            walk_mro(owner, &call.method, mro, methods, 1)
        }
        CallReceiver::Bare { name } => {
            // 1. Lexical-class MRO: bare `foo()` inside a class body
            //    must check inherited methods before falling out to
            //    top-level / alias resolution. Mirrors JS Bare stage.
            if let Some(owner) = call.lexical_class.as_deref() {
                if let Some(hit) = walk_mro(owner, name, mro, methods, 0) {
                    return Some(hit);
                }
            }
            // 2. Alias-bound bare name (`import com.foo.helper` ⇒
            //    `helper()` resolves to `com.foo.helper`). Use the
            //    binding's `target_path` to avoid first-hit on
            //    qualified collision.
            if let Some(binding) = aliases.get(name) {
                if let Some(hit) =
                    lookup_via_binding(binding, &binding.target_qualified, package_index)
                {
                    return Some(DispatchResolution {
                        path: hit.path,
                        qualified: hit.qualified,
                    });
                }
            }
            // 3. Current-package top-level function.
            if let Some(pkg) = file_facts.package.as_deref() {
                if let Some(hit) = methods.get_package_callable(pkg, name) {
                    return Some(DispatchResolution {
                        path: hit.path.clone(),
                        qualified: hit.qualified.clone(),
                    });
                }
            }
            // 4. Wildcard-imported package top-level function.
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
            // 5. Last-resort unique-name match (best-effort extension
            //    fallback).
            if let Some(hit) = methods.get_unique_by_name(name) {
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

/// Walk `class`'s MRO from `skip` ancestors deep (0 = include class
/// itself, 1 = skip-self for `super`). Returns the first method
/// matching `name`.
fn walk_mro(
    class: &str,
    name: &str,
    mro: &Mro,
    methods: &MethodIndex,
    skip: usize,
) -> Option<DispatchResolution> {
    for ancestor in mro.ancestors(class).into_iter().skip(skip) {
        if let Some(hit) = methods.get_method(&ancestor, name) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }
    None
}

/// Validate an `(binding, qualified)` pair into a workspace target.
/// When the binding pins a file (workspace-resolved import), require
/// the candidate to live in that file. Otherwise fall back to a
/// path-agnostic unique lookup. Never falls through to a first-hit.
fn lookup_via_binding(
    binding: &ResolvedBinding,
    qualified: &str,
    package_index: &PackageIndex,
) -> Option<ResolvedClass> {
    if let Some(target_path) = &binding.target_path {
        if let Some(hit) = package_index.lookup_in_file(target_path, qualified) {
            return Some(ResolvedClass {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
        // Binding had a path but the candidate doesn't live there. Do
        // NOT silently fall through — that would re-introduce the
        // collision bug.
        return None;
    }
    package_index
        .lookup_unique(qualified)
        .map(|hit| ResolvedClass {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        })
}

#[allow(clippy::too_many_arguments)]
fn resolve_dotted_call(
    source_path: &str,
    parts: &[String],
    method: &str,
    package_index: &PackageIndex,
    mro: &Mro,
    methods: &MethodIndex,
    aliases: &HashMap<String, ResolvedBinding>,
    file_facts: &FileConstFacts,
) -> Option<DispatchResolution> {
    if parts.is_empty() {
        return None;
    }

    // Terminal-alias contract: once an `import` binds `parts[0]`, the
    // dotted prefix is finalized to that binding. Stage 6 / 7 may
    // succeed inside the binding's scope, but if neither does, the
    // resolver must return None — downstream Stage 8-10 fallbacks
    // would silently re-interpret the user's `import` and adopt an
    // unrelated workspace symbol that happens to share a short name.
    // This pins R2's path-bound-alias contract end-to-end.
    let alias_head_bound = aliases.contains_key(&parts[0]);

    // Stage 6 (the heaviest): resolve the dotted prefix to a class
    // target, then dispatch the method through its MRO.
    if let Some(class_target) =
        resolve_class_target(source_path, parts, aliases, file_facts, package_index)
    {
        // (a) static-style: `Cls.method` registered as composite FQN
        // (companion members, nested-object members) — try the
        // composite first via path-aware lookup.
        let composite = format!("{}.{}", class_target.qualified, method);
        if let Some(hit) = package_index.lookup_in_file(&class_target.path, &composite) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
        // (b) instance-method MRO walk anchored at the class FQN.
        if let Some(hit) = walk_mro(&class_target.qualified, method, mro, methods, 0) {
            return Some(hit);
        }
    }

    // Stage 7: best-effort package-like alias shapes. When a dotted
    // call's head is an import binding, retry the tail against
    // package-level callables in the bound qualified namespace.
    // Kotlin does not have JS's `import * as F` namespace concept,
    // but the resolver still encounters `Container.foo()` shapes
    // where `Container` resolves via the alias map and `foo` ends up
    // as a top-level callable in the bound package.
    if parts.len() >= 2 {
        let head = &parts[0];
        let tail = &parts[1..];
        if let Some(binding) = aliases.get(head) {
            let package_candidate = binding.target_qualified.clone();
            // `F.bar()` — single-segment tail = top-level function in
            // the bound package.
            if tail.len() == 1 {
                if let Some(hit) = methods.get_package_callable(&package_candidate, &tail[0]) {
                    // Top-level callable; treat `method` as the
                    // attribute on it (this stays None unless there's
                    // a workspace symbol — extremely rare for Kotlin,
                    // but harmless).
                    if hit.qualified == format!("{}.{}", package_candidate, tail[0]) {
                        let composite = format!("{}.{}", hit.qualified, method);
                        if let Some(static_hit) =
                            package_index.lookup_in_file(&hit.path, &composite)
                        {
                            return Some(DispatchResolution {
                                path: static_hit.path.clone(),
                                qualified: static_hit.qualified.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    // Stage 7.5 — JVM `<File>Kt` synthetic-class normalization.
    //
    // Kotlin top-level functions JVM-compile into a synthetic class
    // whose name is the source file's stem + `Kt` (`Foo.kt` →
    // `FooKt`). Java callers cross-calling `FooKt.bar()` come into
    // this resolver as `parts=["FooKt"]`, `method="bar"` — Stage 6
    // misses because no workspace `FooKt` class exists, and Stage 8
    // misses because `FooKt` is not a package.
    //
    // Narrow normalization rules (R2 strict):
    //   * Only fires when `!alias_head_bound` — `import x.FooKt; FooKt.bar()`
    //     is terminal at the binding (PR #219 contract). Stage 7
    //     already handles the bound case via `get_package_callable`
    //     on `binding.target_qualified`.
    //   * Only fires when the dotted prefix ends with a `*Kt`-suffixed
    //     segment (`FooKt`, `com.x.FooKt`).
    //   * Only fires when no literal `class/object FooKt` exists at
    //     that FQN (a real `object FooKt` wins — verified through
    //     `lookup_unique`, package-agnostic uniqueness).
    //   * Only routes into `methods.get_package_callable(stripped,
    //     method)` — never `get_unique_by_name` (R2 strict: would
    //     re-introduce the collision class of bug PR #219 closed).
    //   * Strips ONE trailing `Kt` segment: `com.x.FooKt` → package
    //     `com.x`. `@file:JvmName("Custom")` (which would produce a
    //     `Custom` synthetic instead of `<File>Kt`) is documented as
    //     a known limitation in CHANGELOG; tree-sitter-kotlin-ng does
    //     not surface that annotation reliably, so it's a Tier-3 LSP
    //     concern.
    if !alias_head_bound {
        if let Some(head) = parts.last() {
            if head.ends_with("Kt") && head.len() > 2 {
                // For bare `FooKt.bar()` the `parts` slice has no
                // explicit package prefix — Java callers cross-calling
                // a Kotlin top-level function in the *same package*
                // come in this way. Fall back to the calling file's
                // own package so `package util; UtilKt.bar()` routes
                // to `util.bar`, not the root-package `bar` (which
                // does not exist as a top-level callable in the
                // workspace).
                let same_package_prefix: &str = file_facts.package.as_deref().unwrap_or("");
                let literal_qualified = if parts.len() == 1 && !same_package_prefix.is_empty() {
                    format!("{same_package_prefix}.{head}")
                } else {
                    parts.join(".")
                };
                // Real `class FooKt` / `object FooKt` always wins —
                // the synthetic strip only applies when no such
                // workspace symbol exists. We also check the
                // same-package literal to keep a real `object UtilKt`
                // in the calling file's package from being shadowed.
                if package_index.lookup_unique(&literal_qualified).is_none() {
                    // Drop the `*Kt` segment entirely — Kotlin
                    // top-level callables are keyed by their
                    // *package* FQN, not by `package.FileStem`.
                    // `com.x.FooKt.bar()` → package `com.x`;
                    // bare `FooKt.bar()` in `package util` → `util`.
                    let pkg = if parts.len() == 1 {
                        same_package_prefix.to_string()
                    } else {
                        parts[..parts.len() - 1].join(".")
                    };
                    if let Some(hit) = methods.get_package_callable(&pkg, method) {
                        return Some(DispatchResolution {
                            path: hit.path.clone(),
                            qualified: hit.qualified.clone(),
                        });
                    }
                }
            }
        }
    }

    // Terminal-alias short-circuit (see top-of-function comment).
    // If `parts[0]` was an `import`-bound head, the prefix's meaning
    // is fixed by Stage 6 / 7. A miss there is unresolved — never
    // re-interpreted by package / composite / unique-name fallbacks.
    if alias_head_bound {
        return None;
    }

    // Stage 8: `pkg.foo()` where the dotted prefix itself is a
    // workspace package. Validated through `has_package` + path-aware
    // lookup of the package callable.
    let prefix = parts.join(".");
    if package_index.has_package(&prefix) {
        if let Some(hit) = methods.get_package_callable(&prefix, method) {
            return Some(DispatchResolution {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }

    // Stage 9: composite FQN `parts.method` resolves directly to a
    // workspace symbol (covers `Cls.STATIC_FIELD` chains via the
    // package index). Path-agnostic but uniqueness-gated.
    let composite = format!("{prefix}.{method}");
    if let Some(hit) = package_index.lookup_unique(&composite) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }

    // Stage 10 — best-effort unique-name match (extension functions on
    // receivers whose type isn't pinnable). Only fires when one
    // workspace method has this name; collisions stay unresolved.
    if let Some(hit) = methods.get_unique_by_name(method) {
        return Some(DispatchResolution {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }
    None
}

/// Resolve a dotted prefix (everything before `.method`) into a
/// validated `(path, qualified)` class target. Each candidate stage
/// is checked against `PackageIndex` — we never return an unchecked
/// String the way the pre-fix resolver did.
///
/// Order mirrors Kotlin's lookup rules: alias map (path-constrained)
/// → same-package → wildcard import → bare FQN → same-file class
/// fallback (handles `Inner.method()` where `Inner` is declared in
/// the calling file under no package prefix).
fn resolve_class_target(
    source_path: &str,
    parts: &[String],
    aliases: &HashMap<String, ResolvedBinding>,
    file_facts: &FileConstFacts,
    package_index: &PackageIndex,
) -> Option<ResolvedClass> {
    let head = &parts[0];
    let tail = if parts.len() > 1 {
        Some(parts[1..].join("."))
    } else {
        None
    };

    // 1. Alias substitution. When the binding pinned a workspace file
    //    use `lookup_in_file`; otherwise fall back to `lookup_unique`.
    //
    //    Terminal: if an `import` binds `head`, the head's meaning is
    //    fixed by that binding. A path-bound miss must NOT silently
    //    fall through to same-package / wildcard / bare-FQN stages —
    //    doing so would let `import pkg.b.Registry; Registry.Inner.x()`
    //    re-resolve to a `Registry.Inner` declared in some *other*
    //    package whose head also happens to be `Registry`. The
    //    `lookup_via_binding` result is final for the alias-head case.
    if let Some(binding) = aliases.get(head) {
        let candidate = match &tail {
            Some(t) => format!("{}.{}", binding.target_qualified, t),
            None => binding.target_qualified.clone(),
        };
        return lookup_via_binding(binding, &candidate, package_index);
    }

    // 2. Same-package lookup.
    if let Some(pkg) = file_facts.package.as_deref().filter(|s| !s.is_empty()) {
        let candidate = format!("{pkg}.{}", parts.join("."));
        if let Some(hit) = package_index.lookup_unique(&candidate) {
            return Some(ResolvedClass {
                path: hit.path.clone(),
                qualified: hit.qualified.clone(),
            });
        }
    }

    // 3. Wildcard-import expansion.
    for b in &file_facts.import_bindings {
        if b.kind == ImportKind::Wildcard {
            let candidate = format!("{}.{}", b.fqn, parts.join("."));
            if let Some(hit) = package_index.lookup_unique(&candidate) {
                return Some(ResolvedClass {
                    path: hit.path.clone(),
                    qualified: hit.qualified.clone(),
                });
            }
        }
    }

    // 4. Bare FQN as written (covers `com.foo.Registry.build()` where
    //    the head is `com`).
    let bare = parts.join(".");
    if let Some(hit) = package_index.lookup_unique(&bare) {
        return Some(ResolvedClass {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }

    // 5. Same-file class fallback. Build `pkg.parts[0]` (or just
    //    `parts[0]` for root package) and look it up in the calling
    //    file — useful when the class is declared inline and the
    //    prefix is just the class name.
    let same_file_qualified = match file_facts.package.as_deref().filter(|s| !s.is_empty()) {
        Some(pkg) => format!("{pkg}.{}", parts[0]),
        None => parts[0].clone(),
    };
    if let Some(hit) = package_index.lookup_in_file(source_path, &same_file_qualified) {
        // Append the dotted tail if any (preserves
        // `Inner.NestedObject.member` chains).
        let qualified = match &tail {
            Some(t) => format!("{}.{}", hit.qualified, t),
            None => hit.qualified.clone(),
        };
        // Verify the extended qualified actually exists in that file
        // before adopting it.
        if let Some(extended) = package_index.lookup_in_file(source_path, &qualified) {
            return Some(ResolvedClass {
                path: extended.path.clone(),
                qualified: extended.qualified.clone(),
            });
        }
        // Otherwise return the bare same-file class; MRO walking
        // takes care of the method dispatch from there.
        return Some(ResolvedClass {
            path: hit.path.clone(),
            qualified: hit.qualified.clone(),
        });
    }

    None
}
