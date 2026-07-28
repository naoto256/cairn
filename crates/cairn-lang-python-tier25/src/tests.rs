//! Unit tests for the Python Tier-2.5 resolver.

use cairn_core::manifest::ManifestId;
use cairn_core::workspace_analyzer::{
    AnalyzerProgress, ResolutionKind, WorkspaceAnalyzer, WorkspaceFile, WorkspaceResolution,
};
use tree_sitter::Parser;

use crate::const_resolver::{ImportKind, ModuleIndex, parse_file};
use crate::mro::Mro;
use crate::require_graph::RequireGraph;
use crate::{
    ANALYZER_REVISION, AliasAuthority, AliasAuthorityEvent, PythonTier25Analyzer, TypeAliasEvents,
    alias_authority_at, analyze_files, type_alias_authority,
};

// ─── helpers ──────────────────────────────────────────────────────────────

fn write_files(root: &std::path::Path, files: &[(&str, &str)]) -> Vec<WorkspaceFile> {
    let mut out = Vec::new();
    for (rel, content) in files {
        let abs = root.join(rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&abs, content).unwrap();
        out.push(WorkspaceFile {
            path: (*rel).to_string(),
            blob_sha: format!("blob-{rel}"),
            worktree_path: Some(abs),
            source_bytes: Some(std::sync::Arc::from(content.as_bytes())),
        });
    }
    out
}

fn run(root: &std::path::Path, files: &[(&str, &str)]) -> Vec<WorkspaceResolution> {
    let wsf = write_files(root, files);
    analyze_files(&wsf, &AnalyzerProgress::default())
}

fn run_published(
    root: &std::path::Path,
    manifest_id: ManifestId,
    files: &[(&str, &str)],
) -> Vec<WorkspaceResolution> {
    let files = write_files(root, files);
    PythonTier25Analyzer
        .analyze_workspace(root, manifest_id, &files, &AnalyzerProgress::default())
        .unwrap()
        .resolutions
}

fn imports_of(res: &[WorkspaceResolution], source: &str) -> Vec<WorkspaceResolution> {
    res.iter()
        .filter(|r| r.source_path == source && r.kind == ResolutionKind::Import)
        .cloned()
        .collect()
}

fn types_of(res: &[WorkspaceResolution], source: &str) -> Vec<WorkspaceResolution> {
    res.iter()
        .filter(|r| r.source_path == source && r.kind == ResolutionKind::Type)
        .cloned()
        .collect()
}

fn calls_of(res: &[WorkspaceResolution], source: &str) -> Vec<WorkspaceResolution> {
    res.iter()
        .filter(|r| r.source_path == source && r.kind == ResolutionKind::Call)
        .cloned()
        .collect()
}

fn assert_python_root_clean(source: &str) {
    let language: tree_sitter::Language = tree_sitter_python::LANGUAGE.into();
    let mut parser = Parser::new();
    parser.set_language(&language).unwrap();
    let tree = parser.parse(source, None).unwrap();
    assert!(
        !tree.root_node().has_error(),
        "fixture must parse without recovery: {}",
        tree.root_node().to_sexp()
    );
}

fn mro_for(files: &[(&str, &str)]) -> Mro {
    let mut per_file = Vec::new();
    for (path, source) in files {
        let module = crate::file_to_module(path);
        let is_package_init = path.ends_with("/__init__.py") || *path == "__init__.py";
        let facts = parse_file(source.as_bytes(), module, is_package_init).unwrap();
        per_file.push(((*path).to_string(), source.as_bytes().to_vec(), facts));
    }
    let module_index = ModuleIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &module_index);
    Mro::build(&per_file, &module_index, &require_graph)
}

// ─── const_resolver (lexical / module-globals / aliases) ─────────────────

#[test]
fn analyzer_revision_tracks_mro_fallback_ordering() {
    assert_eq!(ANALYZER_REVISION, 7);
    assert_eq!(PythonTier25Analyzer.revision(), 7);
}

#[test]
fn pathless_alias_preserves_qualified_identity_without_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let provider = "\
class _Dynamic:
    pass

def __getattr__(name):
    if name == \"Dynamic\":
        return _Dynamic
    raise AttributeError(name)
";
    let caller = "\
class p:
    class Dynamic:
        pass

import provider as p

class Child(p.Dynamic):
    pass
";
    assert_python_root_clean(provider);
    assert_python_root_clean(caller);

    let provider_facts =
        parse_file(provider.as_bytes(), Some("provider".to_string()), false).unwrap();
    let caller_facts = parse_file(caller.as_bytes(), Some("caller".to_string()), false).unwrap();
    let per_file = vec![
        (
            "provider.py".to_string(),
            provider.as_bytes().to_vec(),
            provider_facts,
        ),
        (
            "caller.py".to_string(),
            caller.as_bytes().to_vec(),
            caller_facts,
        ),
    ];
    let module_index = ModuleIndex::build(&per_file);
    assert!(
        module_index.lookup("provider.Dynamic").is_none(),
        "the dynamic module attribute must not have a concrete symbol path"
    );
    assert_eq!(
        module_index
            .lookup("caller.p.Dynamic")
            .map(|hit| hit.path.as_str()),
        Some("caller.py"),
        "the same-module decoy must make fallback observable"
    );

    let require_graph = RequireGraph::build(&per_file, &module_index);
    let binding = per_file[1]
        .2
        .import_bindings
        .iter()
        .find(|binding| binding.local == "p")
        .expect("import provider as p should produce an ImportBinding");
    assert_eq!(binding.module, "provider");
    assert_eq!(
        require_graph
            .resolve_binding("caller.py", binding)
            .as_deref(),
        Some("provider"),
        "the import alias is the authority for the dotted reference"
    );

    let resolutions = run_published(
        tmp.path(),
        ManifestId(1),
        &[("provider.py", provider), ("caller.py", caller)],
    );
    let target = resolutions
        .iter()
        .find(|resolution| {
            resolution.source_path == "caller.py" && resolution.kind == ResolutionKind::Type
        })
        .expect("Child(p.Dynamic) should emit a Type resolution");
    assert_eq!(target.target_path, None);
    assert_eq!(
        target.target_qualified.as_deref(),
        Some("provider.Dynamic"),
        "alias authority must preserve the canonical identity without falling back to caller.p.Dynamic"
    );
    assert!(
        resolutions
            .iter()
            .filter(|resolution| {
                resolution.source_path == "caller.py" && resolution.kind == ResolutionKind::Import
            })
            .all(|resolution| resolution.target_qualified.is_none()),
        "Import rows retain their path-only contract"
    );
}

#[test]
fn dynamic_from_import_does_not_invent_module_identity_or_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let provider = "\
class _Dynamic:
    pass

def __getattr__(name):
    if name == \"Dynamic\":
        return _Dynamic
    raise AttributeError(name)
";
    let caller = "\
class D:
    pass

from provider import Dynamic as D

class Child(D):
    pass
";
    assert_python_root_clean(provider);
    assert_python_root_clean(caller);

    let provider_facts =
        parse_file(provider.as_bytes(), Some("provider".to_string()), false).unwrap();
    let caller_facts = parse_file(caller.as_bytes(), Some("caller".to_string()), false).unwrap();
    let per_file = vec![
        (
            "provider.py".to_string(),
            provider.as_bytes().to_vec(),
            provider_facts,
        ),
        (
            "caller.py".to_string(),
            caller.as_bytes().to_vec(),
            caller_facts,
        ),
    ];
    let module_index = ModuleIndex::build(&per_file);
    assert!(module_index.lookup("provider.Dynamic").is_none());
    assert_eq!(
        module_index
            .lookup("caller.D")
            .map(|target| target.path.as_str()),
        Some("caller.py"),
        "the same-module decoy makes forbidden fallback observable"
    );

    let binding = per_file[1]
        .2
        .import_bindings
        .iter()
        .find(|binding| binding.local == "D")
        .expect("from provider import Dynamic as D should produce a binding");
    assert!(matches!(binding.kind, ImportKind::From));
    assert_eq!(binding.module, "provider");
    assert_eq!(binding.imported.as_deref(), Some("Dynamic"));
    let require_graph = RequireGraph::build(&per_file, &module_index);
    assert_eq!(
        require_graph
            .resolve_binding("caller.py", binding)
            .as_deref(),
        Some("provider"),
        "module-only fallback is import-edge evidence, not member identity"
    );

    let resolutions = run_published(
        tmp.path(),
        ManifestId(3),
        &[("provider.py", provider), ("caller.py", caller)],
    );
    let target = resolutions
        .iter()
        .find(|resolution| {
            resolution.source_path == "caller.py" && resolution.kind == ResolutionKind::Type
        })
        .expect("Child(D) should emit a fail-closed Type resolution");
    assert_eq!(target.target_path, None);
    assert_eq!(
        target.target_qualified, None,
        "a dynamic from-import miss must not become provider or caller.D"
    );
}

#[test]
fn absolute_from_import_submodule_proves_module_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let package = "";
    let submodule = "class Base:\n    pass\n";
    let caller = "from pkg import sub\n\nclass Child(sub.Base):\n    pass\n";
    assert_python_root_clean(package);
    assert_python_root_clean(submodule);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(4),
        &[
            ("pkg/__init__.py", package),
            ("pkg/sub.py", submodule),
            ("caller.py", caller),
        ],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .find(|resolution| resolution.target_qualified.is_some())
        .expect("sub.Base should resolve through the imported submodule");
    assert_eq!(target.target_path.as_deref(), Some("pkg/sub.py"));
    assert_eq!(target.target_qualified.as_deref(), Some("pkg.sub.Base"));
}

#[test]
fn relative_from_import_submodule_proves_module_authority() {
    let tmp = tempfile::tempdir().unwrap();
    let package = "";
    let submodule = "class Base:\n    pass\n";
    let caller = "from . import sub\n\nclass Child(sub.Base):\n    pass\n";
    assert_python_root_clean(package);
    assert_python_root_clean(submodule);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(5),
        &[
            ("pkg/__init__.py", package),
            ("pkg/sub.py", submodule),
            ("pkg/caller.py", caller),
        ],
    );
    let target = types_of(&resolutions, "pkg/caller.py")
        .into_iter()
        .find(|resolution| resolution.target_qualified.is_some())
        .expect("relative sub.Base should resolve through the sibling module");
    assert_eq!(target.target_path.as_deref(), Some("pkg/sub.py"));
    assert_eq!(target.target_qualified.as_deref(), Some("pkg.sub.Base"));
}

#[test]
fn module_level_class_resolves_in_same_file() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Widget:\n    pass\n\nclass Caller(Widget):\n    pass\n";
    let res = run(tmp.path(), &[("widget.py", src)]);
    let types = types_of(&res, "widget.py");
    let widget_ref = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("widget.Widget"))
        .expect("base Widget should resolve");
    assert_eq!(widget_ref.target_path.as_deref(), Some("widget.py"));
}

#[test]
fn from_import_creates_binding_for_short_name() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let caller = "from widget import Widget\n\nclass Sub(Widget):\n    pass\n";
    let res = run(tmp.path(), &[("widget.py", widget), ("caller.py", caller)]);
    let types = types_of(&res, "caller.py");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("widget.Widget"))
        .expect("Widget from `from widget import Widget` should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("widget.py"));
}

#[test]
fn from_import_with_explicit_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let caller = "from widget import Widget as W\n\nclass Sub(W):\n    pass\n";
    assert_python_root_clean(widget);
    assert_python_root_clean(caller);
    let resolutions = run_published(
        tmp.path(),
        ManifestId(6),
        &[("widget.py", widget), ("caller.py", caller)],
    );
    let types = types_of(&resolutions, "caller.py");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("widget.Widget"))
        .expect("`as W` alias should resolve to widget.Widget");
    assert_eq!(hit.target_path.as_deref(), Some("widget.py"));
}

#[test]
fn import_module_as_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let caller = "import widget as w\n\nclass Sub(w.Widget):\n    pass\n";
    assert_python_root_clean(widget);
    assert_python_root_clean(caller);
    let resolutions = run_published(
        tmp.path(),
        ManifestId(2),
        &[("widget.py", widget), ("caller.py", caller)],
    );
    let types = types_of(&resolutions, "caller.py");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("widget.Widget"))
        .expect("w.Widget via `import widget as w` should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("widget.py"));
}

#[test]
fn plain_dotted_import_binds_the_top_level_package() {
    let tmp = tempfile::tempdir().unwrap();
    let package = "class Dynamic:\n    pass\n";
    let submodule = "class Base:\n    pass\n";
    let caller = "import pkg.sub\n\nclass Child(pkg.Dynamic):\n    pass\n";
    assert_python_root_clean(package);
    assert_python_root_clean(submodule);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(7),
        &[
            ("pkg/__init__.py", package),
            ("pkg/sub.py", submodule),
            ("caller.py", caller),
        ],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .find(|resolution| resolution.target_qualified.is_some())
        .expect("pkg.Dynamic should resolve through the bound top-level package");
    assert_eq!(target.target_path.as_deref(), Some("pkg/__init__.py"));
    assert_eq!(target.target_qualified.as_deref(), Some("pkg.Dynamic"));
}

#[test]
fn plain_dotted_import_retains_nested_module_lookup() {
    let tmp = tempfile::tempdir().unwrap();
    let package = "";
    let submodule = "class Base:\n    pass\n";
    let caller = "import pkg.sub\n\nclass Child(pkg.sub.Base):\n    pass\n";
    assert_python_root_clean(package);
    assert_python_root_clean(submodule);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(8),
        &[
            ("pkg/__init__.py", package),
            ("pkg/sub.py", submodule),
            ("caller.py", caller),
        ],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .find(|resolution| resolution.target_qualified.is_some())
        .expect("pkg.sub.Base should resolve through the dotted import");
    assert_eq!(target.target_path.as_deref(), Some("pkg/sub.py"));
    assert_eq!(target.target_qualified.as_deref(), Some("pkg.sub.Base"));
}

#[test]
fn aliased_dotted_import_binds_the_full_module() {
    let tmp = tempfile::tempdir().unwrap();
    let package = "";
    let submodule = "class Base:\n    pass\n";
    let caller = "import pkg.sub as p\n\nclass Child(p.Base):\n    pass\n";
    assert_python_root_clean(package);
    assert_python_root_clean(submodule);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(9),
        &[
            ("pkg/__init__.py", package),
            ("pkg/sub.py", submodule),
            ("caller.py", caller),
        ],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .find(|resolution| resolution.target_qualified.is_some())
        .expect("p.Base should resolve through the full aliased module");
    assert_eq!(target.target_path.as_deref(), Some("pkg/sub.py"));
    assert_eq!(target.target_qualified.as_deref(), Some("pkg.sub.Base"));
}

#[test]
fn unresolved_aliased_import_blocks_same_module_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let caller = "\
class ext:
    class Dynamic:
        pass

import external as ext

class Child(ext.Dynamic):
    pass
";
    assert_python_root_clean(caller);

    let resolutions = run_published(tmp.path(), ManifestId(10), &[("caller.py", caller)]);
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .next()
        .expect("Child(ext.Dynamic) should emit a fail-closed Type resolution");
    assert_eq!(target.target_path, None);
    assert_eq!(target.target_qualified, None);
}

#[test]
fn unresolved_plain_import_blocks_same_module_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let caller = "\
class external:
    class Dynamic:
        pass

import external

class Child(external.Dynamic):
    pass
";
    assert_python_root_clean(caller);

    let resolutions = run_published(tmp.path(), ManifestId(11), &[("caller.py", caller)]);
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .next()
        .expect("Child(external.Dynamic) should emit a fail-closed Type resolution");
    assert_eq!(target.target_path, None);
    assert_eq!(target.target_qualified, None);
}

#[test]
fn unresolved_plain_dotted_import_blocks_same_module_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let caller = "\
class external:
    class sub:
        class Base:
            pass

import external.sub

class Child(external.sub.Base):
    pass
";
    assert_python_root_clean(caller);

    let resolutions = run_published(tmp.path(), ManifestId(12), &[("caller.py", caller)]);
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .next()
        .expect("Child(external.sub.Base) should emit a fail-closed Type resolution");
    assert_eq!(target.target_path, None);
    assert_eq!(target.target_qualified, None);
}

#[test]
fn wildcard_import_does_not_create_a_literal_alias_authority() {
    let source = "from external import *\n";
    assert_python_root_clean(source);
    let facts = parse_file(source.as_bytes(), Some("caller".to_string()), false).unwrap();
    let binding = facts
        .import_bindings
        .iter()
        .find(|binding| binding.local == "*")
        .expect("wildcard imports retain an import-edge binding")
        .clone();
    assert!(matches!(binding.kind, ImportKind::From));
    assert_eq!(binding.imported, None);

    let per_file = vec![("caller.py".to_string(), source.as_bytes().to_vec(), facts)];
    let module_index = ModuleIndex::build(&per_file);
    let require_graph = RequireGraph::build(&per_file, &module_index);
    let resolved = require_graph.resolve_binding("caller.py", &binding);
    assert!(
        type_alias_authority(&binding, resolved, &module_index).is_none(),
        "wildcard imports do not bind the literal local name `*`"
    );
}

#[test]
fn later_unresolved_import_shadows_a_resolved_occurrence() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = "class Dynamic:\n    pass\n";
    let caller = "\
import workspace as dep
import external_provider as dep

class Child(dep.Dynamic):
    pass
";
    assert_python_root_clean(workspace);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(13),
        &[("workspace.py", workspace), ("caller.py", caller)],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .next()
        .expect("Child(dep.Dynamic) should emit a fail-closed Type resolution");
    assert_eq!(target.target_path, None);
    assert_eq!(target.target_qualified, None);
}

#[test]
fn later_resolved_import_replaces_an_unresolved_occurrence() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = "class Dynamic:\n    pass\n";
    let caller = "\
import external_provider as dep
import workspace as dep

class Child(dep.Dynamic):
    pass
";
    assert_python_root_clean(workspace);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(14),
        &[("workspace.py", workspace), ("caller.py", caller)],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .next()
        .expect("the later workspace binding should resolve dep.Dynamic");
    assert_eq!(target.target_path.as_deref(), Some("workspace.py"));
    assert_eq!(
        target.target_qualified.as_deref(),
        Some("workspace.Dynamic")
    );
}

#[test]
fn type_references_see_only_preceding_import_occurrences() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = "class Dynamic:\n    pass\n";
    let caller = "\
import workspace as dep

class Before(dep.Dynamic):
    pass

import external_provider as dep

class After(dep.Dynamic):
    pass
";
    assert_python_root_clean(workspace);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(15),
        &[("workspace.py", workspace), ("caller.py", caller)],
    );
    let mut targets = types_of(&resolutions, "caller.py");
    targets.sort_by_key(|resolution| resolution.site_byte_range.start);
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].target_path.as_deref(), Some("workspace.py"));
    assert_eq!(
        targets[0].target_qualified.as_deref(),
        Some("workspace.Dynamic")
    );
    assert_eq!(targets[1].target_path, None);
    assert_eq!(targets[1].target_qualified, None);
}

#[test]
fn same_statement_rebinding_uses_the_later_occurrence() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = "class Dynamic:\n    pass\n";
    let caller = "\
import workspace as dep, external_provider as dep

class Child(dep.Dynamic):
    pass
";
    assert_python_root_clean(workspace);
    assert_python_root_clean(caller);

    let resolutions = run_published(
        tmp.path(),
        ManifestId(16),
        &[("workspace.py", workspace), ("caller.py", caller)],
    );
    let target = types_of(&resolutions, "caller.py")
        .into_iter()
        .next()
        .expect("the later same-statement binding should own dep");
    assert_eq!(target.target_path, None);
    assert_eq!(target.target_qualified, None);
}

#[test]
fn same_site_alias_events_use_vector_ordinal() {
    let mut events = TypeAliasEvents::new();
    events.insert(
        "dep".to_string(),
        vec![
            AliasAuthorityEvent {
                site_byte_start: 10,
                ordinal: 0,
                authority: AliasAuthority::Module("workspace".to_string()),
            },
            AliasAuthorityEvent {
                site_byte_start: 10,
                ordinal: 1,
                authority: AliasAuthority::UnresolvedImport,
            },
        ],
    );

    assert!(matches!(
        alias_authority_at(&events, "dep", 20),
        Some(AliasAuthority::UnresolvedImport)
    ));
}

#[test]
fn unaliased_dotted_reference_keeps_same_module_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let caller = "\
class p:
    class Dynamic:
        pass

class Child(p.Dynamic):
    pass
";
    assert_python_root_clean(caller);
    let res = run(tmp.path(), &[("caller.py", caller)]);
    let target = types_of(&res, "caller.py")
        .into_iter()
        .find(|resolution| resolution.target_qualified.is_some())
        .expect("unaliased p.Dynamic should use the same-module index");
    assert_eq!(target.target_path.as_deref(), Some("caller.py"));
    assert_eq!(target.target_qualified.as_deref(), Some("caller.p.Dynamic"));
}

#[test]
fn nested_class_qualified_path_includes_parent() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Outer:\n    class Inner:\n        pass\n";
    let _res = run(tmp.path(), &[("m.py", src)]);
    // Inner's class def is resolved at its module-qualified name when
    // referenced via `Outer.Inner` from elsewhere. Verify via import:
    let other = "from m import Outer\n\nclass S(Outer.Inner):\n    pass\n";
    let res2 = run(tmp.path(), &[("m.py", src), ("other.py", other)]);
    let types = types_of(&res2, "other.py");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("m.Outer.Inner")),
        "Outer.Inner should resolve; got {:#?}",
        types
    );
}

// ─── MRO ──────────────────────────────────────────────────────────────────

#[test]
fn single_inheritance_chain_resolves_method() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class A:
    def foo(self):
        pass

class B(A):
    def bar(self):
        self.foo()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.A.foo")),
        "self.foo() should resolve to A.foo via MRO; got {:#?}",
        calls
    );
}

#[test]
fn multiple_inheritance_c3_resolves_method_from_earliest_base() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class A:
    def go(self):
        pass

class B:
    def go(self):
        pass

class C(A, B):
    def trigger(self):
        self.go()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    let go = calls
        .iter()
        .find(|c| c.target_qualified.is_some())
        .expect("self.go should resolve to some target");
    // C3: C -> A -> B -> object — so A.go wins.
    assert_eq!(go.target_qualified.as_deref(), Some("m.A.go"));
}

#[test]
fn deep_fallback_preserves_left_to_right_mixed_base_order() {
    let provider = "\
class _Dynamic:
    pass

def __getattr__(name):
    if name == \"Dynamic\":
        return _Dynamic
    raise AttributeError(name)
";
    let mut source = String::from(
        "import provider as p\n\nclass C0:\n    def ping(self):\n        return \"local\"\n\n",
    );
    for index in 1..=64 {
        source.push_str(&format!("class C{index}(C{}): pass\n", index - 1));
    }
    source.push_str(
        "\nclass Leaf(C64, p.Dynamic):\n    def call(self):\n        return self.ping()\n",
    );
    assert_python_root_clean(provider);
    assert_python_root_clean(&source);

    let mro = mro_for(&[("provider.py", provider), ("m.py", &source)]);
    let chain = mro.ancestors("m.Leaf");
    assert_eq!(
        chain.get(1).map(String::as_str),
        Some("m.C64"),
        "fallback must preserve Python's left-to-right base authority"
    );
    assert_eq!(chain.iter().position(|owner| owner == "m.C0"), Some(65));
    assert_eq!(
        chain.iter().position(|owner| owner == "provider.Dynamic"),
        Some(66)
    );

    let tmp = tempfile::tempdir().unwrap();
    let resolutions = run_published(
        tmp.path(),
        ManifestId(17),
        &[("provider.py", provider), ("m.py", &source)],
    );
    assert!(
        calls_of(&resolutions, "m.py")
            .iter()
            .any(|call| call.target_qualified.as_deref() == Some("m.C0.ping")),
        "published dispatch must keep resolving the local left base"
    );
}

#[test]
fn normal_c3_diamond_keeps_left_to_right_order() {
    let source = "\
class Root:
    def ping(self):
        return \"root\"

class Left(Root):
    pass

class Right(Root):
    pass

class Leaf(Left, Right):
    def call(self):
        return self.ping()
";
    assert_python_root_clean(source);

    let mro = mro_for(&[("m.py", source)]);
    assert_eq!(
        mro.ancestors("m.Leaf"),
        ["m.Leaf", "m.Left", "m.Right", "m.Root"]
    );

    let tmp = tempfile::tempdir().unwrap();
    let resolutions = run_published(tmp.path(), ManifestId(18), &[("m.py", source)]);
    assert!(
        calls_of(&resolutions, "m.py")
            .iter()
            .any(|call| call.target_qualified.as_deref() == Some("m.Root.ping"))
    );
}

#[test]
fn same_leaf_bases_follow_inheritance_order_not_file_order() {
    let left = "class Base:\n    def pick(self):\n        return \"left\"\n";
    let right = "class Base:\n    def pick(self):\n        return \"right\"\n";
    let caller = "\
import left as L
import right as R

class Leaf(L.Base, R.Base):
    def call(self):
        return self.pick()
";
    assert_python_root_clean(left);
    assert_python_root_clean(right);
    assert_python_root_clean(caller);

    let tmp = tempfile::tempdir().unwrap();
    let resolutions = run_published(
        tmp.path(),
        ManifestId(19),
        &[
            ("right.py", right),
            ("left.py", left),
            ("caller.py", caller),
        ],
    );
    assert!(
        calls_of(&resolutions, "caller.py")
            .iter()
            .any(|call| call.target_qualified.as_deref() == Some("left.Base.pick")),
        "same-leaf owners must follow the declared base order"
    );
}

#[test]
fn super_call_resolves_to_parent_method() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Base:
    def step(self):
        pass

class Child(Base):
    def step(self):
        super().step()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Base.step")),
        "super().step() should resolve to Base.step; got {:#?}",
        calls
    );
}

#[test]
fn explicit_operand_super_call_stays_unresolved() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Base:
    def step(self):
        pass

class Child(Base):
    def step(self):
        super(Base, self).step()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");

    assert!(
        calls.is_empty(),
        "explicit-operand super calls must remain unresolved: {calls:#?}"
    );
}

#[test]
fn nested_function_inside_method_is_not_indexed_as_method() {
    let src = b"\
class Service:
    def outer(self):
        def inner():
            pass
        return inner()
";
    let facts = parse_file(src, Some("service".to_string()), false).expect("source should parse");

    assert!(
        facts
            .method_defs
            .iter()
            .any(|method| method.name == "outer"),
        "{:#?}",
        facts.method_defs
    );
    assert!(
        facts
            .method_defs
            .iter()
            .all(|method| method.name != "inner"),
        "{:#?}",
        facts.method_defs
    );
}

#[test]
fn cross_file_inheritance_resolves_via_import() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "class Animal:\n    def speak(self):\n        pass\n";
    let dog = "\
from animal import Animal

class Dog(Animal):
    def bark(self):
        self.speak()
";
    let res = run(tmp.path(), &[("animal.py", animal), ("dog.py", dog)]);
    let calls = calls_of(&res, "dog.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("animal.Animal.speak")),
        "self.speak() should resolve via cross-file MRO; got {:#?}",
        calls
    );
}

#[test]
fn extends_clause_emits_type_resolution_at_base_name_range() {
    // The Tier-2 backend stores the base name's byte range in
    // `implementations.interface_byte_start/end`. The resolver MUST
    // emit a Type resolution at exactly that span for find_subtypes /
    // find_supertypes to flip kind_source.
    let tmp = tempfile::tempdir().unwrap();
    let animal = "class Animal:\n    pass\n";
    let dog = "from animal import Animal\n\nclass Dog(Animal):\n    pass\n";
    let res = run(tmp.path(), &[("animal.py", animal), ("dog.py", dog)]);
    let types = types_of(&res, "dog.py");
    let pos = dog.rfind("Animal").unwrap() as u32;
    let r = types
        .iter()
        .find(|t| t.site_byte_range.start == pos)
        .expect("Type resolution at base name");
    assert_eq!(r.target_qualified.as_deref(), Some("animal.Animal"));
    assert_eq!(r.target_path.as_deref(), Some("animal.py"));
}

// ─── static dispatch ──────────────────────────────────────────────────────

#[test]
fn class_static_call_via_class_name() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo:
    def bar(self):
        pass

Foo.bar(None)
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Foo.bar")),
        "Foo.bar(None) should resolve; got {:#?}",
        calls
    );
}

#[test]
fn self_call_resolves_in_current_class() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo:
    def bar(self):
        pass

    def go(self):
        self.bar()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Foo.bar")),
        "self.bar() should resolve to Foo.bar; got {:#?}",
        calls
    );
}

#[test]
fn cls_call_resolves_like_self() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo:
    @classmethod
    def build(cls):
        pass

    @classmethod
    def make(cls):
        cls.build()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Foo.build")),
        "cls.build() should resolve via class MRO; got {:#?}",
        calls
    );
}

#[test]
fn module_level_function_call_via_import_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let util = "def helper():\n    pass\n";
    let caller = "from util import helper\n\nhelper()\n";
    let res = run(tmp.path(), &[("util.py", util), ("caller.py", caller)]);
    let calls = calls_of(&res, "caller.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("util.helper")),
        "imported helper() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn module_attribute_call_via_import_module() {
    let tmp = tempfile::tempdir().unwrap();
    let util = "def helper():\n    pass\n";
    let caller = "import util\n\nutil.helper()\n";
    let res = run(tmp.path(), &[("util.py", util), ("caller.py", caller)]);
    let calls = calls_of(&res, "caller.py");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("util.helper")),
        "util.helper() should resolve; got {:#?}",
        calls
    );
}

// ─── require_graph (imports → workspace files) ───────────────────────────
//
// Import-edge contract: persisted resolutions carry `target_path`
// only — `target_qualified` is always `None` (matches Ruby /
// JavaScript). The qualified name still lives on the require_graph
// internally for binding lookup; we just don't leak it into the row,
// because persist.rs path-scoped lookup would otherwise spuriously
// pin a workspace symbol_id to the import edge.

fn import_targets_path(imps: &[WorkspaceResolution], path: &str) -> bool {
    imps.iter()
        .any(|r| r.target_path.as_deref() == Some(path) && r.target_qualified.is_none())
}

#[test]
fn from_import_emits_resolution_for_workspace_class() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let main = "from widget import Widget\n";
    let res = run(tmp.path(), &[("widget.py", widget), ("main.py", main)]);
    let imps = imports_of(&res, "main.py");
    assert!(
        import_targets_path(&imps, "widget.py"),
        "from import should emit a resolution pinned to widget.py with target_qualified=None; got {:#?}",
        imps,
    );
}

#[test]
fn import_module_emits_resolution() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let main = "import widget\n";
    let res = run(tmp.path(), &[("widget.py", widget), ("main.py", main)]);
    let imps = imports_of(&res, "main.py");
    assert!(
        import_targets_path(&imps, "widget.py"),
        "import widget should resolve to module file with target_qualified=None; got {:#?}",
        imps,
    );
}

#[test]
fn relative_import_resolves_through_package() {
    let tmp = tempfile::tempdir().unwrap();
    let pkg_init = "";
    let widget = "class Widget:\n    pass\n";
    let caller = "from .widget import Widget\n";
    let res = run(
        tmp.path(),
        &[
            ("pkg/__init__.py", pkg_init),
            ("pkg/widget.py", widget),
            ("pkg/caller.py", caller),
        ],
    );
    let imps = imports_of(&res, "pkg/caller.py");
    assert!(
        import_targets_path(&imps, "pkg/widget.py"),
        "relative `from .widget import Widget` should resolve to pkg/widget.py with target_qualified=None; got {:#?}",
        imps,
    );
}

#[test]
fn relative_dot_import_resolves_sibling_module() {
    let tmp = tempfile::tempdir().unwrap();
    let pkg_init = "";
    let sub_init = "";
    let widget = "class Widget:\n    pass\n";
    let caller = "from . import widget\n";
    let res = run(
        tmp.path(),
        &[
            ("pkg/__init__.py", pkg_init),
            ("pkg/sub/__init__.py", sub_init),
            ("pkg/sub/widget.py", widget),
            ("pkg/sub/caller.py", caller),
        ],
    );
    let imps = imports_of(&res, "pkg/sub/caller.py");
    assert!(
        imps.iter().any(|r| r
            .target_path
            .as_deref()
            .map(|p| p.contains("widget"))
            .unwrap_or(false)
            && r.target_qualified.is_none()),
        "`from . import widget` should resolve to a widget path with target_qualified=None; got {:#?}",
        imps,
    );
}

#[test]
fn package_init_resolves_for_import() {
    let tmp = tempfile::tempdir().unwrap();
    let init = "class Root:\n    pass\n";
    let caller = "from pkg import Root\n";
    let res = run(
        tmp.path(),
        &[("pkg/__init__.py", init), ("caller.py", caller)],
    );
    let imps = imports_of(&res, "caller.py");
    assert!(
        import_targets_path(&imps, "pkg/__init__.py"),
        "package __init__.py Root should resolve to pkg/__init__.py with target_qualified=None; got {:#?}",
        imps,
    );
}

#[test]
fn absolute_dotted_import_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let main = "from pkg.sub.widget import Widget\n";
    let res = run(
        tmp.path(),
        &[
            ("pkg/__init__.py", ""),
            ("pkg/sub/__init__.py", ""),
            ("pkg/sub/widget.py", widget),
            ("main.py", main),
        ],
    );
    let imps = imports_of(&res, "main.py");
    assert!(
        import_targets_path(&imps, "pkg/sub/widget.py"),
        "absolute dotted import should resolve to pkg/sub/widget.py with target_qualified=None; got {:#?}",
        imps,
    );
}

#[test]
fn import_target_qualified_is_none_even_when_require_graph_resolved() {
    // Import rows remain path-only even
    // when the require_graph internally resolves a qualified target
    // (here "widget.Widget"), the persisted Import
    // WorkspaceResolution must carry `target_qualified = None`.
    // Otherwise persist.rs path-scoped `(blob_sha, parser_id,
    // qualified)` lookup would spuriously pin a workspace symbol_id
    // to the import edge.
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let main = "from widget import Widget\n";
    let res = run(tmp.path(), &[("widget.py", widget), ("main.py", main)]);
    let imps = imports_of(&res, "main.py");
    assert!(!imps.is_empty(), "expected at least one import row");
    for r in &imps {
        assert!(
            r.target_qualified.is_none(),
            "Import row must have target_qualified=None even when binding resolved internally; got {:?}",
            r
        );
    }
}

// ─── 諦め: things Tier-2.5 must NOT resolve ─────────────────────────────

#[test]
fn unknown_receiver_method_call_is_not_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
def go(obj):
    obj.render()
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    assert!(
        calls
            .iter()
            .all(|c| c.target_qualified.is_none()
                || c.target_qualified.as_deref() != Some("m.render")),
        "obj.render() must not resolve to a workspace target; got {:#?}",
        calls
    );
    // We don't emit Call resolutions for unresolvable sites — so the
    // resolutions array should not contain any Call row at all for the
    // `render` site.
    assert!(
        calls.is_empty(),
        "obj.render() should not produce a Call resolution; got {:#?}",
        calls
    );
}

#[test]
fn getattr_setattr_is_not_emitted_as_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo:
    def bar(self):
        pass

getattr(Foo, 'bar')()
setattr(Foo, 'bar', None)
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let calls = calls_of(&res, "m.py");
    // The grammar reports `getattr(...)` as a bare call to `getattr`,
    // and the trailing `()` is a call-on-call which we drop. Neither
    // should be resolved to Foo.bar.
    let bar_hit = calls
        .iter()
        .any(|c| c.target_qualified.as_deref() == Some("m.Foo.bar"));
    assert!(
        !bar_hit,
        "getattr-shaped dispatch must not resolve to Foo.bar; got {:#?}",
        calls
    );
}

#[test]
fn metaclass_keyword_base_does_not_create_inheritance_edge() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Meta(type):
    pass

class Foo(metaclass=Meta):
    pass
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let types = types_of(&res, "m.py");
    // The keyword arg `metaclass=Meta` lives inside the superclasses
    // node but tree-sitter exposes it as a `keyword_argument`, which
    // dotted_parts() rejects. So no Type ref for Meta should be
    // emitted from Foo's superclasses.
    let foo_meta = types
        .iter()
        .any(|t| t.target_qualified.as_deref() == Some("m.Meta"));
    assert!(
        !foo_meta,
        "metaclass=Meta should not emit a Type ref pointing at Foo's metaclass arg; got {:#?}",
        types
    );
}

#[test]
fn decorator_transformation_does_not_resolve_property_attribute_access() {
    // A `@property`-decorated getter is read as `obj.x` (no parens).
    // Tier-2.5 doesn't model decorators, so we'd happily resolve a
    // literal `Foo.name()` to the wrapped function — that's a known
    // out-of-scope limitation. What Tier-2.5 must NOT do is invent a
    // Call resolution for the *attribute access* shape `obj.name`,
    // because no call node exists there. This test pins that the
    // attribute access produces no Call row at all.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo:
    @property
    def name(self):
        return 'x'

obj = Foo()
_ = obj.name
";
    let res = run(tmp.path(), &[("m.py", src)]);
    let name_calls: Vec<_> = calls_of(&res, "m.py")
        .into_iter()
        .filter(|c| c.target_qualified.as_deref() == Some("m.Foo.name"))
        .collect();
    assert!(
        name_calls.is_empty(),
        "obj.name attribute access must not produce a Call resolution to Foo.name; got {:#?}",
        name_calls
    );
}

// ─── glue ─────────────────────────────────────────────────────────────────

#[test]
fn analyzer_returns_facts_with_resolutions_field() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[]);
    assert!(res.is_empty());
}

#[test]
fn empty_file_does_not_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[("empty.py", "")]);
    assert!(res.is_empty());
}
