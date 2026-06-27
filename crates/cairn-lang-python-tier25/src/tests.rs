//! Unit tests for the Python Tier-2.5 resolver.

use cairn_core::workspace_analyzer::{
    AnalyzerProgress, ResolutionKind, WorkspaceFile, WorkspaceResolution,
};

use crate::analyze_files;

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

// ─── const_resolver (lexical / module-globals / aliases) ─────────────────

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
    let res = run(tmp.path(), &[("widget.py", widget), ("caller.py", caller)]);
    let types = types_of(&res, "caller.py");
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
    let res = run(tmp.path(), &[("widget.py", widget), ("caller.py", caller)]);
    let types = types_of(&res, "caller.py");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("widget.Widget"))
        .expect("w.Widget via `import widget as w` should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("widget.py"));
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

#[test]
fn from_import_emits_resolution_for_workspace_class() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "class Widget:\n    pass\n";
    let main = "from widget import Widget\n";
    let res = run(tmp.path(), &[("widget.py", widget), ("main.py", main)]);
    let imps = imports_of(&res, "main.py");
    assert!(
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("widget.Widget")),
        "from import should emit a resolution; got {:#?}",
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
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("widget")),
        "import widget should resolve to module; got {:#?}",
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
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("pkg.widget.Widget")),
        "relative `from .widget import Widget` should resolve; got {:#?}",
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
            .target_qualified
            .as_deref()
            .map(|q| q.contains("widget"))
            .unwrap_or(false)),
        "`from . import widget` should resolve; got {:#?}",
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
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("pkg.Root")),
        "package __init__.py Root should resolve; got {:#?}",
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
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("pkg.sub.widget.Widget")),
        "absolute dotted import should resolve; got {:#?}",
        imps,
    );
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
