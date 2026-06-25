//! Unit tests for the C# Tier-2.5 resolver.

use cairn_core::workspace_analyzer::{
    AnalyzerProgress, ResolutionKind, WorkspaceFile, WorkspaceResolution,
};

use crate::analyze_files;

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

// ─── const_resolver / package lookups ──────────────────────────────────

#[test]
fn class_in_same_namespace_resolves_without_using() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "namespace Co.App;\npublic class Widget {}\n";
    let sub = "namespace Co.App;\npublic class Sub : Widget {}\n";
    let res = run(tmp.path(), &[("Widget.cs", base), ("Sub.cs", sub)]);
    let types = types_of(&res, "Sub.cs");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Co.App.Widget"))
        .expect("same-namespace Widget should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.cs"));
}

#[test]
fn block_scoped_namespace_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "namespace Co.App { public class Widget {} }\n";
    let sub = "namespace Co.App { public class Sub : Widget {} }\n";
    let res = run(tmp.path(), &[("W.cs", base), ("S.cs", sub)]);
    let types = types_of(&res, "S.cs");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("Co.App.Widget")),
        "block-scoped namespace Widget should resolve; got {:#?}",
        types
    );
}

#[test]
fn fully_qualified_base_resolves_directly() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "namespace Co.App;\npublic class Widget {}\n";
    let sub = "namespace Other;\npublic class Sub : Co.App.Widget {}\n";
    let res = run(tmp.path(), &[("W.cs", base), ("S.cs", sub)]);
    let types = types_of(&res, "S.cs");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Co.App.Widget"))
        .expect("FQN base should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("W.cs"));
}

#[test]
fn plain_using_resolves_short_name() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "namespace Co.App;\npublic class Widget {}\n";
    let sub = "using Co.App;\nnamespace Other;\npublic class Sub : Widget {}\n";
    let res = run(tmp.path(), &[("W.cs", base), ("S.cs", sub)]);
    let types = types_of(&res, "S.cs");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Co.App.Widget"))
        .expect("using-imported Widget should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("W.cs"));
}

#[test]
fn alias_using_resolves_through_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "namespace Co.App;\npublic class Widget {}\n";
    let sub = "using W = Co.App.Widget;\nnamespace Other;\npublic class Sub : W {}\n";
    let res = run(tmp.path(), &[("W.cs", base), ("S.cs", sub)]);
    let types = types_of(&res, "S.cs");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Co.App.Widget"))
        .expect("aliased using should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("W.cs"));
}

#[test]
fn nested_type_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let outer = "namespace N;\npublic class Outer { public class Inner {} }\n";
    let sub = "namespace N;\npublic class Sub : Outer.Inner {}\n";
    let res = run(tmp.path(), &[("O.cs", outer), ("S.cs", sub)]);
    let types = types_of(&res, "S.cs");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("N.Outer.Inner")),
        "nested type Outer.Inner should resolve; got {:#?}",
        types
    );
}

// ─── MRO / type hierarchy ──────────────────────────────────────────────

#[test]
fn single_inheritance_resolves_this_method_call() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class A { public void Foo() {} }\npublic class B : A { public void Bar() { this.Foo(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.A.Foo")),
        "this.Foo() should resolve to A.Foo; got {:#?}",
        calls
    );
}

#[test]
fn base_call_resolves_to_parent_method() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class Base { public virtual void Step() {} }\npublic class Child : Base { public override void Step() { base.Step(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.Base.Step")),
        "base.Step() should resolve to Base.Step; got {:#?}",
        calls
    );
}

#[test]
fn interface_method_resolves_via_implement_edge() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic interface IGreeter { void Greet(); }\npublic class Service : IGreeter { public void Greet() {} public void Trigger() { this.Greet(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.Service.Greet")),
        "this.Greet() should resolve to Service.Greet (own method beats interface ancestor); got {:#?}",
        calls
    );
}

#[test]
fn cross_file_inheritance_via_using() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "namespace Zoo;\npublic class Animal { public virtual void Speak() {} }\n";
    let dog = "using Zoo;\nnamespace App;\npublic class Dog : Animal { public void Bark() { this.Speak(); } }\n";
    let res = run(tmp.path(), &[("Animal.cs", animal), ("Dog.cs", dog)]);
    let calls = calls_of(&res, "Dog.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Zoo.Animal.Speak")),
        "this.Speak() should resolve via cross-file MRO; got {:#?}",
        calls
    );
}

#[test]
fn heritage_emits_type_resolution_at_base_name_range() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "namespace Zoo;\npublic class Animal {}\n";
    let dog = "namespace Zoo;\npublic class Dog : Animal {}\n";
    let res = run(tmp.path(), &[("Animal.cs", animal), ("Dog.cs", dog)]);
    let types = types_of(&res, "Dog.cs");
    let pos = dog.rfind("Animal").unwrap() as u32;
    let r = types
        .iter()
        .find(|t| t.site_byte_range.start == pos)
        .expect("Type resolution at base name");
    assert_eq!(r.target_qualified.as_deref(), Some("Zoo.Animal"));
    assert_eq!(r.target_path.as_deref(), Some("Animal.cs"));
}

#[test]
fn class_with_class_and_interfaces() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace N;\npublic class A {}\npublic interface IFoo {}\npublic interface IBar {}\npublic class Sub : A, IFoo, IBar {}\n";
    let res = run(tmp.path(), &[("N.cs", src)]);
    let types = types_of(&res, "N.cs");
    let names: Vec<&str> = types
        .iter()
        .filter_map(|t| t.target_qualified.as_deref())
        .collect();
    assert!(names.contains(&"N.A"), "missing N.A in {:#?}", names);
    assert!(names.contains(&"N.IFoo"));
    assert!(names.contains(&"N.IBar"));
}

#[test]
fn record_inherits_record() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace N;\npublic record A(int X);\npublic record B(int X) : A(X);\n";
    let res = run(tmp.path(), &[("N.cs", src)]);
    let types = types_of(&res, "N.cs");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("N.A")),
        "record B : A should produce N.A type resolution; got {:#?}",
        types
    );
}

#[test]
fn struct_implements_interface() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace N;\npublic interface IFoo {}\npublic struct Point : IFoo {}\n";
    let res = run(tmp.path(), &[("N.cs", src)]);
    let types = types_of(&res, "N.cs");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("N.IFoo")),
        "struct : IFoo should resolve; got {:#?}",
        types
    );
}

// ─── static dispatch ───────────────────────────────────────────────────

#[test]
fn static_method_call_via_class_name() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class Registry { public static void Build() {} }\npublic class C { public void Go() { Registry.Build(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.Registry.Build")),
        "Registry.Build() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn this_call_resolves_in_current_class() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class Foo { public void Bar() {} public void Go() { this.Bar(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.Foo.Bar")),
        "this.Bar() should resolve to Foo.Bar; got {:#?}",
        calls
    );
}

#[test]
fn fully_qualified_static_call_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let svc = "namespace Co.App;\npublic class Registry { public static void Build() {} }\n";
    let caller =
        "namespace Other;\npublic class C { public void Go() { Co.App.Registry.Build(); } }\n";
    let res = run(tmp.path(), &[("R.cs", svc), ("M.cs", caller)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Co.App.Registry.Build")),
        "FQN static call should resolve; got {:#?}",
        calls
    );
}

#[test]
fn bare_call_in_same_class_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class C { public void Helper() {} public void Render() { Helper(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.C.Helper")),
        "bare Helper() in same class should resolve; got {:#?}",
        calls
    );
}

#[test]
fn using_static_resolves_bare_method() {
    let tmp = tempfile::tempdir().unwrap();
    let lib = "namespace Lib;\npublic static class Helpers { public static void Run() {} }\n";
    let caller = "using static Lib.Helpers;\nnamespace App;\npublic class C { public void Go() { Run(); } }\n";
    let res = run(tmp.path(), &[("Lib.cs", lib), ("C.cs", caller)]);
    let calls = calls_of(&res, "C.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Lib.Helpers.Run")),
        "using static should resolve bare Run(); got {:#?}",
        calls
    );
}

#[test]
fn extension_method_resolved_via_unique_name_match() {
    // `static class Ext { static void Doubled(this int x) }` — call
    // `5.Doubled()` resolves by unique-name best-effort.
    let tmp = tempfile::tempdir().unwrap();
    let ext = "namespace M;\npublic static class Ext { public static int Doubled(this int x) { return x * 2; } }\npublic class C { public void Go() { var y = 5; y.Doubled(); } }\n";
    let res = run(tmp.path(), &[("M.cs", ext)]);
    let calls = calls_of(&res, "M.cs");
    // Receiver `y` is local int — Tier-2.5 can't pin int, but the
    // method name is unique in the workspace so the best-effort
    // fallback fires.
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.Ext.Doubled")),
        "extension method should resolve via unique-name match; got {:#?}",
        calls
    );
}

// ─── partial-class merge ──────────────────────────────────────────────

#[test]
fn partial_class_methods_merge_across_files() {
    let tmp = tempfile::tempdir().unwrap();
    let p1 = "namespace M;\npublic partial class P { public void A() {} }\n";
    let p2 = "namespace M;\npublic partial class P { public void Go() { this.A(); } }\n";
    let res = run(tmp.path(), &[("P1.cs", p1), ("P2.cs", p2)]);
    let calls = calls_of(&res, "P2.cs");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("M.P.A")),
        "partial class members should merge so this.A() resolves; got {:#?}",
        calls
    );
}

// ─── require_graph (using → workspace files) ──────────────────────────

#[test]
fn using_workspace_namespace_records_qualified() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "namespace Co.App;\npublic class Widget {}\n";
    let main = "using Co.App;\nnamespace M;\n";
    let res = run(tmp.path(), &[("W.cs", widget), ("M.cs", main)]);
    let imps = imports_of(&res, "M.cs");
    assert!(
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("Co.App")),
        "workspace using should record namespace qualified; got {:#?}",
        imps
    );
}

#[test]
fn external_using_records_qualified_without_path() {
    let tmp = tempfile::tempdir().unwrap();
    let main = "using System.Collections.Generic;\nnamespace M;\n";
    let res = run(tmp.path(), &[("M.cs", main)]);
    let imps = imports_of(&res, "M.cs");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("System.Collections.Generic"))
        .expect("external using should record qualified");
    assert!(
        hit.target_path.is_none(),
        "BCL using must have no target_path; got {:?}",
        hit.target_path
    );
}

#[test]
fn alias_using_resolution_uses_target_fqn() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "namespace Co.App;\npublic class Widget {}\n";
    let main = "using W = Co.App.Widget;\nnamespace M;\n";
    let res = run(tmp.path(), &[("Widget.cs", widget), ("M.cs", main)]);
    let imps = imports_of(&res, "M.cs");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("Co.App.Widget"))
        .expect("aliased using should resolve to target FQN");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.cs"));
}

#[test]
fn using_static_records_type_qualified() {
    let tmp = tempfile::tempdir().unwrap();
    let helpers = "namespace Lib;\npublic static class Helpers {}\n";
    let main = "using static Lib.Helpers;\nnamespace M;\n";
    let res = run(tmp.path(), &[("H.cs", helpers), ("M.cs", main)]);
    let imps = imports_of(&res, "M.cs");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("Lib.Helpers"))
        .expect("using static should record type qualified");
    assert_eq!(hit.target_path.as_deref(), Some("H.cs"));
}

#[test]
fn import_emits_resolution_at_path_byte_range() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "namespace Co.App;\npublic class Widget {}\n";
    let main = "using Co.App;\nnamespace M;\n";
    let res = run(tmp.path(), &[("W.cs", widget), ("M.cs", main)]);
    let imps = imports_of(&res, "M.cs");
    let path_start = main.find("Co.App").unwrap() as u32;
    let path_end = path_start + "Co.App".len() as u32;
    let hit = imps
        .iter()
        .find(|r| r.site_byte_range.start == path_start && r.site_byte_range.end == path_end)
        .expect("import resolution must be pinned at the dotted path span");
    assert_eq!(hit.target_qualified.as_deref(), Some("Co.App"));
}

// ─── 諦め: things Tier-2.5 must NOT resolve ──────────────────────────────

#[test]
fn unknown_receiver_method_call_is_not_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class C { public void Go(object obj) { obj.Render(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls.iter().all(|c| c
            .target_qualified
            .as_deref()
            .map(|q| !q.ends_with(".Render"))
            .unwrap_or(true)),
        "obj.Render() must not resolve to any workspace target; got {:#?}",
        calls
    );
}

#[test]
fn reflection_invoke_is_not_emitted_as_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "namespace M;\npublic class Foo { public void Bar() {} }\npublic class C { public void Go() { var m = typeof(Foo).GetMethod(\"Bar\"); m.Invoke(new Foo(), null); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    let bar_hit = calls
        .iter()
        .any(|c| c.target_qualified.as_deref() == Some("M.Foo.Bar"));
    assert!(
        !bar_hit,
        "reflection invoke must not resolve to Foo.Bar; got {:#?}",
        calls
    );
}

#[test]
fn dynamic_call_is_not_invented() {
    // `dynamic d; d.Render();` — receiver type unknowable; with no
    // workspace method named Render we must produce nothing.
    let tmp = tempfile::tempdir().unwrap();
    let src =
        "namespace M;\npublic class C { public void Go() { dynamic d = null; d.Render(); } }\n";
    let res = run(tmp.path(), &[("M.cs", src)]);
    let calls = calls_of(&res, "M.cs");
    assert!(
        calls.iter().all(|c| c
            .target_qualified
            .as_deref()
            .map(|q| !q.ends_with(".Render"))
            .unwrap_or(true)),
        "dynamic d.Render() must not invent target; got {:#?}",
        calls
    );
}

// ─── glue ───────────────────────────────────────────────────────────────

#[test]
fn analyzer_returns_facts_with_resolutions_field() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[]);
    assert!(res.is_empty());
}

#[test]
fn empty_file_does_not_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[("Empty.cs", "")]);
    assert!(res.is_empty());
}

#[test]
fn malformed_csharp_degrades_to_partial_resolutions() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "namespace M;\npublic class Base {}\n";
    let broken = "namespace M;\npublic class Sub : Base {}\npublic void Broken() { int x = ; }\n";
    let res = run(tmp.path(), &[("Base.cs", base), ("Sub.cs", broken)]);
    let types = types_of(&res, "Sub.cs");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("M.Base")),
        "intact Sub : Base should still resolve despite a downstream parse error; got {:#?}",
        types
    );
}

#[test]
fn analyzer_id_and_revision_are_stable() {
    use crate::{ANALYZER_ID, ANALYZER_REVISION, PARSER_ID, RESOLUTION_SOURCE, TIER_PREFIX};
    assert_eq!(ANALYZER_ID, "csharp-resolver");
    assert_eq!(TIER_PREFIX, "tier25");
    assert_eq!(ANALYZER_REVISION, 1);
    assert_eq!(PARSER_ID, "tree-sitter-c-sharp");
    assert_eq!(RESOLUTION_SOURCE, "tier25-csharp-resolver");
}

#[test]
fn root_namespace_file_resolves_class() {
    // No namespace declaration: classes live at the top.
    let tmp = tempfile::tempdir().unwrap();
    let base = "public class Widget {}\n";
    let sub = "public class Sub : Widget {}\n";
    let res = run(tmp.path(), &[("W.cs", base), ("S.cs", sub)]);
    let types = types_of(&res, "S.cs");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Widget"))
        .expect("root-namespace Widget should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("W.cs"));
}
