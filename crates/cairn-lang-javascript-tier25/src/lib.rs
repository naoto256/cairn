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
//! * `exports.X = require('./x')` named re-export: only the strict
//!   `module.exports = require('./x')` shape is recognized here, and as
//!   an edge-only `ImportKind::SideEffect` (no `ResolvedBinding`, no
//!   `target_qualified`). Named re-export graph semantics (resolving
//!   `import { X } from './outer'` through a re-export chain) are out
//!   of scope for this revision.
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

use const_resolver::{FileConstFacts, ImportKind, PackageIndex};
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
// Bumped for resolutions.manifest_id persistence (schema v11): the
// persist layer now scopes resolution rows to the writing manifest,
// so existing workspace_analysis_runs need to be invalidated and the
// analyzer re-run to repopulate rows with manifest_id Some. Analyzer
// logic itself is unchanged.
// Bumped to 4: const_resolver now emits an ImportBinding (and the
// require_graph a RequireEdge with `target_path` populated for
// in-workspace targets) for statement-position
// (`require('./setup');`), expression-position
// (`app.use(require('./routes'))`, argument-nested), and
// `module.exports = require('./x')` re-export shapes — previously only
// binding-form (`const X = require('./foo')`) reached this path. The
// rerun of `const_resolver` / `require_graph` is driven by PR #220's
// analyzer-revision staleness scanner; this is the 2nd real use case
// of that scanner after Wave 2C's PR-α (CJS binding-form expansion).
// Bumped to 5 for CodeRabbit follow-ups #8/#9/#11/#12: same-file class
// lookup is path-aware (`lookup_class_in_file`), `NewExpr` gets a
// same-file fallback parallel to `Cls.method()`, nested
// `function_declaration` is no longer indexed as a workspace symbol,
// and `ResolvedBinding` / `AliasTarget` preserve `ImportKind` so
// dotted dispatch (`Foo.bar()`) on default imports no longer silently
// rebinds to a sibling named export. Cached runs need invalidation.
pub const ANALYZER_REVISION: u32 = 5;
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

    fn requires_materialized_files(&self) -> bool {
        true
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
                    import_kind: binding.import_kind,
                    dropped_chain: binding.dropped_chain,
                },
            );
        }
        alias_maps.insert(path.as_str(), m);
    }

    for (path, _, facts) in &per_file {
        let aliases = alias_maps.get(path.as_str()).cloned().unwrap_or_default();

        // Type references (base classes / interfaces).
        for tref in &facts.type_refs {
            // R2 v0.7.0 dogfood follow-up: if the head of the dotted
            // type reference resolves through an alias whose re-export
            // chain was dropped (cycle / over-budget), do NOT emit a
            // tier25 Type row. The alias has `target_path = None,
            // target_qualified = None`, so the row would otherwise be
            // a fact-shaped lie attributed to the tier25 resolver. The
            // Tier-2 syntactic backend still emits the bare `extends`
            // fact, which is the correct level of confidence for this
            // case. Restricted to dropped-chain aliases so genuine
            // external (npm) imports — which also yield `(None, None)`
            // — keep their existing Type row + fact fallback.
            if let Some(head) = tref.parts.first() {
                if let Some(alias) = aliases.get(head) {
                    if alias.dropped_chain {
                        continue;
                    }
                }
            }
            let resolved = resolve_dotted_type(path, &tref.parts, &aliases, facts, &package_index);
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
    // v0.7.0 D PR: the runner pre-reads workspace files for
    // Tier-2.5 analyzers (`requires_materialized_files() == true`)
    // and attaches the bytes here. Reading `worktree_path` directly
    // would re-open a race window between the runner's readability
    // check and the analyzer's actual read.
    file.source_bytes.as_deref().map(<[u8]>::to_vec)
}

/// Resolved target of a local alias (import binding). `target_path =
/// None` means the alias points outside the workspace (bare specifier
/// or `node:` builtin); `target_qualified` is still recorded for fact
/// fallback at query time.
#[derive(Debug, Clone)]
pub struct AliasTarget {
    pub target_path: Option<String>,
    pub target_qualified: Option<String>,
    /// Carried from `ResolvedBinding::import_kind`. Lets dispatch /
    /// type-resolution differentiate `Foo.bar` semantics across
    /// import shapes: namespace import → `bar` is a named export of
    /// the module; default / named / CJS import → `bar` is a runtime
    /// property of the imported value and can't be statically pinned
    /// at Tier-2.5.
    pub import_kind: ImportKind,
    /// Carried from `ResolvedBinding::dropped_chain`. True when the
    /// import binding traced through a re-export chain that was
    /// dropped (cycle / over-budget). Type and dispatch emit sites
    /// must suppress rows keyed off a dropped-chain alias: there is no
    /// resolvable origin in the workspace, and surfacing a row with
    /// `target_path = None, target_qualified = None` would fabricate
    /// a tier25-source edge into a non-existent symbol. External
    /// (npm) bindings have `dropped_chain = false` and are unaffected.
    pub dropped_chain: bool,
}

impl Default for AliasTarget {
    fn default() -> Self {
        Self {
            target_path: None,
            target_qualified: None,
            // Restrictive default — see `ResolvedBinding::default`.
            import_kind: ImportKind::SideEffect,
            dropped_chain: false,
        }
    }
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
    source_path: &str,
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
        // Only the namespace shape lets us re-bind `Ns.Foo` to the
        // module's named export `Foo`; for default / named / CJS
        // imports, `Foo.bar` in type position would be `<runtime
        // property>` and cannot be resolved at Tier-2.5.
        if parts.len() > 1 {
            if alias.import_kind == ImportKind::EsmNamespace {
                if let Some(target_path) = &alias.target_path {
                    let member = parts[1..].join(".");
                    if let Some(hit) = package_index.lookup_export(target_path, &member) {
                        return Some((Some(hit.path.clone()), hit.qualified.clone()));
                    }
                }
            }
            // Couldn't resolve through namespace (either non-namespace
            // alias shape, or namespace lookup missed); still surface
            // the qualified form for fact fallback.
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
    //
    // The visitor saw this ClassDef in `facts` (i.e. it lives in
    // `source_path`); we must pass `source_path` through so
    // `lookup_class_in_file` scopes to *this* file. Otherwise a
    // workspace with two files defining `class Foo` would have
    // file A's `extends Foo` arbitrarily resolve to file B's `Foo`.
    for class in &facts.class_defs {
        if class.qualified == *head {
            if let Some(hit) = package_index.lookup_class_in_file(source_path, &class.qualified) {
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
