//! JavaScript Tier-2.5 in-process workspace analyzer.
//!
//! Tree-sitter-driven, no LSP. Walks `.js` / `.mjs` / `.cjs` / `.jsx`
//! with tree-sitter-javascript, builds a per-workspace module graph
//! spanning CommonJS (`require`) and ESM (`import` / `export`) at the
//! same time, and emits resolution-layer rows
//! (`source = "tier25-javascript-resolver"`) for the cases the grammar
//! can pin without LSP help.
//!
//! Scope (Stage 2, Wave 2B canary):
//!
//! * Module imports: ESM (`import X from './foo'`,
//!   `import { X, Y } from './foo'`, `import * as Ns from './foo'`,
//!   `import './foo'`) + CommonJS (`require('./foo')`, destructured
//!   `const { X } = require('./foo')`, member `require('./foo').X`).
//! * Relative path resolution only: `./foo`, `../foo/bar` → first
//!   existing of `<p>`, `<p>.js`, `<p>.mjs`, `<p>.cjs`, `<p>/index.js`,
//!   `<p>/index.mjs`, `<p>/index.cjs`. Workspace-local only.
//! * Export reverse-index: `module.exports = X`, `exports.X = ...`,
//!   `module.exports = { X, Y }`, `export default X`, `export { X }`,
//!   `export const X = ...`, `export class X`, `export function f`,
//!   `export * from './bar'`.
//! * Class hierarchy: `class Dog extends Animal {}` (single super,
//!   dotted bases like `extends ns.Base`).
//! * Static dispatch: `Cls.staticMethod()`, `this.method()`,
//!   `super.method()`, bare `foo()` resolved through file-local
//!   top-levels or import bindings, `Ns.X()` via namespace import.
//!
//! Out of scope (Tier-3 / never at 2.5):
//!
//! * Bare specifiers (`express`, `lodash`) — npm packages: import
//!   edges record `target_path = None, target_qualified = None` and
//!   fall through to the Tier-2 fact layer. The specifier string is
//!   not stuffed into `target_qualified` (Phase 1 contract).
//! * `node:fs` and other `node:`-prefixed builtins: same shape as
//!   bare specifiers.
//! * Bundler / `tsconfig` path aliases (`@/foo`, webpack `resolve.alias`).
//! * Expression-position / side-effect `require(...)` calls
//!   (`require('./setup')` as a statement, `app.use(require(...))`,
//!   `module.exports = require(...)`): the Tier-1 emitter only sees
//!   binding-form `const X = require(...)` today. Tracked as a
//!   follow-up to extend `extract_cjs_requires`.
//! * `obj.method()` where `obj` is a local of unresolved class.
//! * Mixin factories (`class Foo extends Mixin(Base)`) — base is a call
//!   expression and we deliberately skip it from MRO.
//! * `Object.setPrototypeOf`, `Object.assign(Cls.prototype, ...)`,
//!   duck-typed prototype chains.

#![deny(unsafe_code)]

use std::collections::HashMap;
use std::path::Path;

use cairn_core::Result;
use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, ResolutionKind, WORKSPACE_ANALYZERS, WorkspaceAnalyzer, WorkspaceFacts,
    WorkspaceFile, WorkspaceResolution,
};
use linkme::distributed_slice;

pub mod const_resolver;
pub mod dispatch;
pub mod mro;
pub mod require_graph;

#[cfg(test)]
mod tests;

use const_resolver::{FileConstFacts, PackageIndex};
use dispatch::MethodIndex;
use mro::Mro;
use require_graph::RequireGraph;

pub const ANALYZER_ID: &str = "javascript-resolver";
pub const TIER_PREFIX: &str = "tier25";
// Bumped for resolutions.target_path persistence (schema v10) and the
// require-graph hack fix that switches Import-edge target_qualified
// from a path-shaped module specifier (`./db`) to `None`. Persist now
// writes target_path directly into the row and the symbol lookup is
// skipped for import edges, matching the Phase 1 contract the other
// six Tier-2.5 analyzers adopted at v10 landing. Analyzer logic itself
// is otherwise unchanged.
pub const ANALYZER_REVISION: u32 = 2;
pub const PARSER_ID: &str = "tree-sitter-javascript";
pub const RESOLUTION_SOURCE: &str = "tier25-javascript-resolver";

/// In-process tree-sitter resolver for JavaScript (CommonJS + ESM).
pub struct JavaScriptTier25Analyzer;

impl WorkspaceAnalyzer for JavaScriptTier25Analyzer {
    fn id(&self) -> &'static str {
        ANALYZER_ID
    }

    fn tier_prefix(&self) -> &'static str {
        TIER_PREFIX
    }

    fn revision(&self) -> u32 {
        ANALYZER_REVISION
    }

    fn language(&self) -> &'static str {
        "javascript"
    }

    fn parser_id(&self) -> &'static str {
        PARSER_ID
    }

    fn analyze_workspace(
        &self,
        _repo_root: &Path,
        _manifest_id: ManifestId,
        files: &[WorkspaceFile],
        progress: &AnalyzerProgress,
    ) -> Result<WorkspaceFacts> {
        let resolutions = analyze_files(files, progress);
        Ok(WorkspaceFacts {
            resolved_refs: Vec::new(),
            resolutions,
        })
    }
}

#[distributed_slice(WORKSPACE_ANALYZERS)]
#[allow(unsafe_code)]
static REGISTER_JAVASCRIPT_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(JavaScriptTier25Analyzer);

/// Parse every visible JS file and emit cross-workspace resolutions.
/// Public for unit-test access.
#[must_use]
pub fn analyze_files(
    files: &[WorkspaceFile],
    progress: &AnalyzerProgress,
) -> Vec<WorkspaceResolution> {
    // 1. Per-file parse + extract facts.
    let mut per_file: Vec<(String, Vec<u8>, FileConstFacts)> = Vec::new();
    for f in files {
        if progress.is_cancelled() {
            break;
        }
        let Some(source) = read_blob(f) else {
            progress.tick();
            continue;
        };
        let facts = match const_resolver::parse_file(&source) {
            Some(f) => f,
            None => {
                progress.tick();
                continue;
            }
        };
        per_file.push((f.path.clone(), source, facts));
        progress.tick();
    }

    // 2. Build workspace symbol index + module-resolution graph.
    let package_index = PackageIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &package_index);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for every import/require site.
    for (path, _, _) in &per_file {
        for edge in require_graph.edges_for(path) {
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: edge.site_byte_start..edge.site_byte_end,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: edge.target_path.clone(),
                target_qualified: edge.target_qualified.clone(),
            });
        }
    }

    // 4. Build MRO + method index + emit type-reference and dispatch
    // resolutions.
    let mro = Mro::build(&per_file, &package_index, &require_graph);
    let methods = MethodIndex::build(&per_file);

    // Per-file alias map: short-name → resolved target FQN. JS aliases
    // can come from ESM named imports, default imports, namespace
    // imports, and CJS `const X = require(...)`. The require_graph
    // already has a unified view; we just project that to a plain map.
    let mut alias_maps: HashMap<&str, HashMap<String, AliasTarget>> = HashMap::new();
    for (path, _, _facts) in &per_file {
        let mut m: HashMap<String, AliasTarget> = HashMap::new();
        for binding in require_graph.bindings_for(path) {
            m.insert(
                binding.local.clone(),
                AliasTarget {
                    target_path: binding.target_path.clone(),
                    target_qualified: binding.target_qualified.clone(),
                },
            );
        }
        alias_maps.insert(path.as_str(), m);
    }

    for (path, _, facts) in &per_file {
        let aliases = alias_maps.get(path.as_str()).cloned().unwrap_or_default();

        // Type references (base classes / interfaces).
        for tref in &facts.type_refs {
            let resolved = resolve_dotted_type(&tref.parts, &aliases, facts, &package_index);
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: tref.byte_start..tref.byte_end,
                kind: ResolutionKind::Type,
                semantic_kind: None,
                target_path: resolved.as_ref().and_then(|r| r.0.clone()),
                target_qualified: resolved.map(|r| r.1),
            });
        }

        // Method calls — only static shapes where the receiver is
        // pinnable from the grammar.
        for call in &facts.method_calls {
            let Some(resolved) =
                dispatch::resolve_call(path, call, &package_index, &mro, &methods, &aliases, facts)
            else {
                continue;
            };
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: call.byte_start..call.byte_end,
                kind: ResolutionKind::Call,
                semantic_kind: None,
                target_path: Some(resolved.path),
                target_qualified: Some(resolved.qualified),
            });
        }
    }

    resolutions
}

fn read_blob(file: &WorkspaceFile) -> Option<Vec<u8>> {
    let path = file.worktree_path.as_ref()?;
    std::fs::read(path).ok()
}

/// Resolved target of a local alias (import binding). `target_path =
/// None` means the alias points outside the workspace (bare specifier
/// or `node:` builtin); `target_qualified` is still recorded for fact
/// fallback at query time.
#[derive(Debug, Clone, Default)]
pub struct AliasTarget {
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
}

/// Resolve a dotted type reference in a base-class position.
///
/// `class Sub extends ns.Base {}` → `parts = ["ns", "Base"]`. JS has no
/// namespace concept of its own, so:
///   * `parts[0]` alias-bound to another module → use that module's
///     export of `parts[1..]`.
///   * `parts[0]` is a same-file class → return it directly.
///   * Otherwise we can't pin (return None).
fn resolve_dotted_type(
    parts: &[String],
    aliases: &HashMap<String, AliasTarget>,
    facts: &FileConstFacts,
    package_index: &PackageIndex,
) -> Option<(Option<String>, String)> {
    if parts.is_empty() {
        return None;
    }
    let head = &parts[0];

    // 1. Alias substitution.
    if let Some(alias) = aliases.get(head) {
        // Namespace alias (`import * as Ns from './foo'`) with member:
        // `Ns.Foo` → look up `Foo` as an export of the target module.
        if parts.len() > 1 {
            if let Some(target_path) = &alias.target_path {
                let member = parts[1..].join(".");
                if let Some(hit) = package_index.lookup_export(target_path, &member) {
                    return Some((Some(hit.path.clone()), hit.qualified.clone()));
                }
            }
            // Couldn't resolve through namespace; still surface the
            // qualified form for fact fallback.
            return Some((
                alias.target_path.clone(),
                format!(
                    "{}.{}",
                    alias.target_qualified.as_deref().unwrap_or(head),
                    parts[1..].join(".")
                ),
            ));
        }
        // Bare alias: `extends Base` where `Base` was imported.
        return Some((
            alias.target_path.clone(),
            alias
                .target_qualified
                .clone()
                .unwrap_or_else(|| head.clone()),
        ));
    }

    // 2. Same-file top-level class.
    for class in &facts.class_defs {
        if class.qualified == *head {
            // FileConstFacts doesn't carry the path; PackageIndex
            // canonicalizes per-file class FQNs and remembers the path.
            if let Some(hit) = package_index.lookup_class_in_file(&class.qualified) {
                return Some((Some(hit.path.clone()), hit.qualified.clone()));
            }
        }
    }

    // 3. Workspace-wide unique class with that name (best-effort).
    if let Some(hit) = package_index.lookup_unique_class(head) {
        return Some((Some(hit.path.clone()), hit.qualified.clone()));
    }

    None
}
