//! Python Tier-2.5 in-process workspace analyzer.
//!
//! This crate is the tree-sitter-driven counterpart to the LSP-backed
//! `cairn-lang-python-tier3` crate: it walks Python source with
//! tree-sitter, builds a per-workspace module/class/import graph, and
//! emits resolution-layer rows (`source = "tier25-python-resolver"`)
//! for the cases that the grammar can pin without LSP help.
//!
//! Scope (Stage 1, 3rd wave):
//!
//! * Class / module lookup: lexical (module globals) → `from x import y`
//!   bindings → `import x as y` bindings. Workspace-local only — no
//!   site-packages resolution.
//! * MRO walk: linear single-inheritance chain plus best-effort C3 for
//!   multiple inheritance, scoped to base classes whose qualified name
//!   resolves through the workspace import graph.
//! * Static dispatch: `Cls.method(...)`, `self.method(...)` (resolved
//!   through the lexically enclosing class), `super().method(...)`
//!   (parent walk), and module-attribute calls (`mod.fn(...)` where
//!   `mod` is a workspace `import` binding).
//! * `import` / `from x import y` (absolute and relative) recorded as
//!   Import resolutions when the target lives in the workspace,
//!   including `__init__.py` package resolution.
//!
//! Out of scope (left to Tier-3 / never):
//!
//! * `obj.method()` where the receiver type is unknown (no annotation
//!   propagation).
//! * `getattr` / `setattr` / `__getattr__` dynamic dispatch.
//! * Metaclass-induced method synthesis.
//! * Decorator transformations that rewrite a function signature
//!   (`@property`, descriptors, etc.).
//! * `eval` / `exec`.
//! * Stdlib / site-packages resolution.

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

use const_resolver::{FileConstFacts, ImportBinding, ImportKind, ModuleIndex};
use dispatch::MethodIndex;
use mro::Mro;
use require_graph::{RequireGraph, resolve_binding_occurrence};

pub const ANALYZER_ID: &str = "python-resolver";
pub const TIER_PREFIX: &str = "tier25";
// Bumped for resolutions.target_path persistence (schema v10): the persist
// layer now writes target_path directly into resolutions, so existing
// workspace_analysis_runs need to be invalidated and re-run to populate
// the new column. Analyzer logic itself is unchanged.
// Bumped for resolutions.manifest_id persistence (schema v11): the
// persist layer now scopes resolution rows to the writing manifest,
// so existing workspace_analysis_runs need to be invalidated and the
// analyzer re-run to repopulate rows with manifest_id Some. Analyzer
// logic itself is unchanged.
// Bumped to 4: import-edge resolutions
// now set `target_qualified = None` to honor the `WorkspaceResolution`
// contract (path-scoped lookup no longer pins symbol_id for imports).
// Cached runs need invalidation.
// Bumped to 5: explicit-operand super calls remain unresolved, and nested
// functions inside class methods no longer enter the method index.
// Bumped to 6: alias-authoritative type references now retain their
// canonical qualified identity even when no concrete workspace path
// exists. Cached pathless resolutions from revision 5 omitted it.
pub const ANALYZER_REVISION: u32 = 6;
pub const PARSER_ID: &str = "tree-sitter-python";
pub const RESOLUTION_SOURCE: &str = "tier25-python-resolver";

/// In-process tree-sitter resolver for Python.
pub struct PythonTier25Analyzer;

impl WorkspaceAnalyzer for PythonTier25Analyzer {
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
        "python"
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
static REGISTER_PYTHON_TIER25_ANALYZER: fn() -> Box<dyn WorkspaceAnalyzer> =
    || Box::new(PythonTier25Analyzer);

/// Parse every visible Python file and emit resolutions across the workspace.
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
        let module = file_to_module(&f.path);
        let is_package_init = f.path.ends_with("/__init__.py") || f.path == "__init__.py";
        let facts = match const_resolver::parse_file(&source, module, is_package_init) {
            Some(f) => f,
            None => {
                progress.tick();
                continue;
            }
        };
        per_file.push((f.path.clone(), source, facts));
        progress.tick();
    }

    // 2. Build cross-file module index + require graph.
    let module_index = ModuleIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &module_index);

    let mut resolutions: Vec<WorkspaceResolution> = Vec::new();

    // 3. Emit Import resolutions for `import` / `from ... import ...`
    // statements whose target lives in the workspace.
    //
    // Import-edge contract (shared with Ruby / JavaScript): rows
    // record `target_path` only; `target_qualified` is forced to
    // `None`. The require_graph still computes the qualified name
    // internally for binding resolution, but leaking it into the
    // persisted row lets `persist.rs` path-scoped lookup
    // (`(blob_sha, parser_id, qualified)`) spuriously pin a
    // symbol_id for an import edge, turning a "no single target
    // file" import semantic into "specific symbol's file". The
    // manifest-wide fallback in persist.rs is gated on
    // `kind != Import`, but the path-scoped fast path runs first
    // and is not gated, so we scrub `target_qualified` at the
    // source.
    for (path, _, _) in &per_file {
        for edge in require_graph.edges_for(path) {
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: edge.site_byte_start..edge.site_byte_end,
                kind: ResolutionKind::Import,
                semantic_kind: None,
                target_path: edge.target_path.clone(),
                target_qualified: None,
            });
        }
    }

    // 4. Build MRO + method index + emit type-reference and dispatch
    // resolutions.
    let mro = Mro::build(&per_file, &module_index, &require_graph);
    let methods = MethodIndex::build(&per_file);

    // Method dispatch retains the require-graph's existing final
    // per-local alias strings. Type resolution retains each binding
    // occurrence so a later unresolved import can shadow an earlier
    // resolved one without changing dispatch or MRO behavior.
    let mut dispatch_alias_maps: HashMap<&str, HashMap<String, String>> = HashMap::new();
    let mut type_alias_event_maps: HashMap<&str, TypeAliasEvents> = HashMap::new();
    for (path, _, facts) in &per_file {
        let mut dispatch_aliases: HashMap<String, String> = HashMap::new();
        let mut type_alias_events = TypeAliasEvents::new();
        for (ordinal, binding) in facts.import_bindings.iter().enumerate() {
            if let Some(qualified) = require_graph.resolve_binding(path, binding) {
                dispatch_aliases.insert(binding.local.clone(), qualified.clone());
            }
            let occurrence = resolve_binding_occurrence(
                binding,
                facts.module.as_deref(),
                facts.is_package_init,
                &module_index,
            );
            let authority = type_alias_authority(binding, occurrence, &module_index);
            if let Some(authority) = authority {
                type_alias_events
                    .entry(binding.local.clone())
                    .or_default()
                    .push(AliasAuthorityEvent {
                        site_byte_start: binding.site_byte_start,
                        ordinal,
                        authority,
                    });
            }
        }
        dispatch_alias_maps.insert(path.as_str(), dispatch_aliases);
        type_alias_event_maps.insert(path.as_str(), type_alias_events);
    }

    for (path, _, facts) in &per_file {
        let dispatch_aliases = dispatch_alias_maps
            .get(path.as_str())
            .cloned()
            .unwrap_or_default();
        let type_alias_events = type_alias_event_maps.get(path.as_str());

        // Type references (base classes, `Foo()` constructions, etc.).
        for tref in &facts.type_refs {
            let authority = tref.parts.first().and_then(|head| {
                type_alias_events
                    .and_then(|events| alias_authority_at(events, head, tref.byte_start))
            });
            let resolved = resolve_dotted(
                &tref.parts,
                authority,
                facts.module.as_deref(),
                &module_index,
            );
            resolutions.push(WorkspaceResolution {
                source_path: path.clone(),
                site_byte_range: tref.byte_start..tref.byte_end,
                kind: ResolutionKind::Type,
                semantic_kind: None,
                target_path: resolved
                    .as_ref()
                    .and_then(|resolution| resolution.target_path.clone()),
                target_qualified: resolved.map(|resolution| resolution.target_qualified),
            });
        }

        // Method calls — only static / self / super / module-attribute
        // shapes where the receiver type is pinnable from the grammar.
        for call in &facts.method_calls {
            let Some(resolved) = dispatch::resolve_call(
                call,
                &module_index,
                &mro,
                &methods,
                &dispatch_aliases,
                facts,
            ) else {
                // Unresolvable (`obj.method()`, `getattr`, etc.) —
                // Tier-2.5 does NOT emit a "site observed" row for
                // these. They belong to Tier-3.
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

/// Convert a repo-relative file path to its Python module qualified name.
///
/// `src/flask/app.py` → `src.flask.app`
/// `flask/__init__.py` → `flask`
/// `top.py` → `top`
///
/// We intentionally include path-prefix segments (`src.flask.app`
/// rather than `flask.app`) because cairn doesn't know the project's
/// import root without setup.cfg / pyproject parsing. The require-graph
/// later resolves both the bare (`flask.app`) and prefixed
/// (`src.flask.app`) forms by also indexing trailing-segment
/// candidates.
fn file_to_module(path: &str) -> Option<String> {
    let stripped = path.strip_suffix(".py")?;
    let segs: Vec<&str> = stripped.split('/').collect();
    if segs.is_empty() {
        return None;
    }
    let normalised: Vec<&str> = if segs.last().copied() == Some("__init__") {
        segs[..segs.len() - 1].to_vec()
    } else {
        segs
    };
    if normalised.is_empty() {
        return None;
    }
    Some(normalised.join("."))
}

/// Path and qualified identity are independent: an import alias can
/// prove the canonical name even when no concrete workspace symbol
/// supplies a path.
struct DottedResolution {
    target_path: Option<String>,
    target_qualified: String,
}

/// Resolution authority retained from the import form.
#[derive(Clone)]
enum AliasAuthority {
    /// A direct module import or proven imported submodule owns the
    /// module identity, so a dotted member can retain its qualified
    /// name without a path.
    Module(String),
    /// `from module import member` is authoritative only after the
    /// workspace index proves the imported member itself.
    ConcreteSymbol(String),
    /// The import still owns its local name and therefore blocks
    /// fallback, but does not prove a publishable target identity.
    UnresolvedImport,
}

struct AliasAuthorityEvent {
    site_byte_start: u32,
    ordinal: usize,
    authority: AliasAuthority,
}

type TypeAliasEvents = HashMap<String, Vec<AliasAuthorityEvent>>;

/// Select the latest import event visible before one type reference.
///
/// Byte order is the authority available in the current file-level
/// model. The ordinal makes multiple bindings at the same import site
/// deterministic without claiming conditional or function-scope
/// dominance.
fn alias_authority_at<'a>(
    events: &'a TypeAliasEvents,
    head: &str,
    reference_byte_start: u32,
) -> Option<&'a AliasAuthority> {
    events
        .get(head)?
        .iter()
        .filter(|event| event.site_byte_start <= reference_byte_start)
        .max_by_key(|event| (event.site_byte_start, event.ordinal))
        .map(|event| &event.authority)
}

/// Preserve the name that each import form actually binds.
///
/// A plain dotted import binds its first segment, while an aliased
/// dotted import binds the full resolved module. From-imports require
/// proof of the imported leaf because their graph fallback may identify
/// only the containing module.
fn type_alias_authority(
    binding: &ImportBinding,
    resolved: Option<String>,
    module_index: &ModuleIndex,
) -> Option<AliasAuthority> {
    // A wildcard affects the module namespace but does not bind the
    // literal parser placeholder as a local identifier.
    if matches!(binding.kind, ImportKind::From)
        && binding.local == "*"
        && binding.imported.is_none()
    {
        return None;
    }

    match binding.kind {
        ImportKind::Plain => Some(match resolved {
            Some(qualified) if module_index.module_path(&qualified).is_some() => {
                match qualified.split('.').next() {
                    Some(top_level) if !top_level.is_empty() => {
                        AliasAuthority::Module(top_level.to_string())
                    }
                    _ => AliasAuthority::UnresolvedImport,
                }
            }
            _ => AliasAuthority::UnresolvedImport,
        }),
        ImportKind::Aliased => Some(match resolved {
            Some(qualified) if module_index.module_path(&qualified).is_some() => {
                AliasAuthority::Module(qualified)
            }
            _ => AliasAuthority::UnresolvedImport,
        }),
        ImportKind::From => Some(match resolved {
            Some(qualified)
                if qualified.rsplit('.').next() == binding.imported.as_deref()
                    && module_index.module_path(&qualified).is_some() =>
            {
                AliasAuthority::Module(qualified)
            }
            Some(qualified)
                if qualified.rsplit('.').next() == binding.imported.as_deref()
                    && module_index.lookup(&qualified).is_some() =>
            {
                AliasAuthority::ConcreteSymbol(qualified)
            }
            _ => AliasAuthority::UnresolvedImport,
        }),
    }
}

/// Best-effort resolution of a dotted reference under Python lexical
/// rules: alias-map (covers `from x import Y` and `import x as Y`) →
/// in-module lookup → workspace global module-index lookup.
///
/// Once an alias binds the head, failure to find a concrete path does
/// not permit fallback under another authority.
fn resolve_dotted(
    parts: &[String],
    authority: Option<&AliasAuthority>,
    module: Option<&str>,
    module_index: &ModuleIndex,
) -> Option<DottedResolution> {
    if parts.is_empty() {
        return None;
    }
    let tail = if parts.len() > 1 {
        Some(parts[1..].join("."))
    } else {
        None
    };

    // 1. Alias substitution: `head` was bound by an import.
    if let Some(authority) = authority {
        let (target, pathless_authoritative) = match authority {
            AliasAuthority::Module(target) => (target, true),
            AliasAuthority::ConcreteSymbol(target) => (target, false),
            AliasAuthority::UnresolvedImport => return None,
        };
        let qualified = match &tail {
            Some(t) => format!("{target}.{t}"),
            None => target.clone(),
        };
        if let Some(hit) = module_index.lookup(&qualified) {
            return Some(DottedResolution {
                target_path: Some(hit.path.clone()),
                target_qualified: hit.qualified.clone(),
            });
        }
        return pathless_authoritative.then_some(DottedResolution {
            target_path: None,
            target_qualified: qualified,
        });
    }

    // 2. Same-module lookup: prepend the file's own module name.
    if let Some(m) = module.filter(|m| !m.is_empty()) {
        let candidate = format!("{m}.{}", parts.join("."));
        if let Some(hit) = module_index.lookup(&candidate) {
            return Some(DottedResolution {
                target_path: Some(hit.path.clone()),
                target_qualified: hit.qualified.clone(),
            });
        }
    }

    // 3. Global lookup against the workspace index.
    let qualified = parts.join(".");
    if let Some(hit) = module_index.lookup(&qualified) {
        return Some(DottedResolution {
            target_path: Some(hit.path.clone()),
            target_qualified: hit.qualified.clone(),
        });
    }

    None
}
