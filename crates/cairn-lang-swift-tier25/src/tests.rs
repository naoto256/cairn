//! Unit tests for the Swift Tier-2.5 resolver.

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

// ─── const_resolver (lexical / module / imports) ─────────────────────────

#[test]
fn class_in_workspace_resolves_without_import() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "open class Widget {}\n";
    let sub = "class Sub: Widget {}\n";
    let res = run(tmp.path(), &[("Widget.swift", base), ("Sub.swift", sub)]);
    let types = types_of(&res, "Sub.swift");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Widget"))
        .expect("Widget base should resolve via bare workspace lookup");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.swift"));
}

#[test]
fn struct_protocol_conformance_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let proto = "protocol Greeter {}\n";
    let user = "struct User: Greeter {}\n";
    let res = run(
        tmp.path(),
        &[("Greeter.swift", proto), ("User.swift", user)],
    );
    let types = types_of(&res, "User.swift");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Greeter"))
        .expect("Greeter conformance should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Greeter.swift"));
}

#[test]
fn nested_type_qualifies_under_outer() {
    let tmp = tempfile::tempdir().unwrap();
    let outer = "struct Outer {\n    struct Inner {}\n}\n";
    let user = "class C: Outer.Inner {}\n";
    let res = run(tmp.path(), &[("Outer.swift", outer), ("User.swift", user)]);
    let types = types_of(&res, "User.swift");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Outer.Inner"))
        .expect("nested type reference should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Outer.swift"));
}

#[test]
fn enum_inheritance_clause_is_recorded() {
    let tmp = tempfile::tempdir().unwrap();
    let proto = "protocol Codable {}\n";
    let enum_ = "enum Status: Codable {\n    case ok\n    case error\n}\n";
    let res = run(
        tmp.path(),
        &[("Codable.swift", proto), ("Status.swift", enum_)],
    );
    let types = types_of(&res, "Status.swift");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Codable"))
        .expect("enum Codable conformance should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Codable.swift"));
}

#[test]
fn protocol_inherits_from_protocol() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "protocol Storage {}\n";
    let sub = "protocol Repository: Storage {}\n";
    let res = run(
        tmp.path(),
        &[("Storage.swift", base), ("Repository.swift", sub)],
    );
    let types = types_of(&res, "Repository.swift");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Storage"))
        .expect("protocol Storage refinement should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Storage.swift"));
}

// ─── MRO ──────────────────────────────────────────────────────────────────

#[test]
fn single_inheritance_resolves_self_method_call() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class A {
    func foo() {}
}

class B: A {
    func bar() {
        self.foo()
    }
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("A.foo")),
        "self.foo() should resolve to A.foo via MRO; got {:#?}",
        calls
    );
}

#[test]
fn super_call_resolves_to_parent_method() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Base {
    func step() {}
}

class Child: Base {
    override func step() {
        super.step()
    }
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Base.step")),
        "super.step() should resolve to Base.step; got {:#?}",
        calls
    );
}

#[test]
fn protocol_method_resolves_via_conformance_edge() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
protocol Greeter {
    func greet()
}

class Service: Greeter {
    func greet() {}
    func trigger() {
        self.greet()
    }
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Service.greet")),
        "self.greet() should resolve to Service.greet; got {:#?}",
        calls
    );
}

#[test]
fn cross_file_inheritance_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "class Animal {\n    func speak() {}\n}\n";
    let dog = "class Dog: Animal {\n    func bark() {\n        self.speak()\n    }\n}\n";
    let res = run(tmp.path(), &[("Animal.swift", animal), ("Dog.swift", dog)]);
    let calls = calls_of(&res, "Dog.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Animal.speak")),
        "self.speak() should resolve via cross-file MRO; got {:#?}",
        calls
    );
}

#[test]
fn heritage_emits_type_resolution_at_base_name_range() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "class Animal {}\n";
    let dog = "class Dog: Animal {}\n";
    let res = run(tmp.path(), &[("Animal.swift", animal), ("Dog.swift", dog)]);
    let types = types_of(&res, "Dog.swift");
    let pos = dog.rfind("Animal").unwrap() as u32;
    let r = types
        .iter()
        .find(|t| t.site_byte_range.start == pos)
        .expect("Type resolution at base name");
    assert_eq!(r.target_qualified.as_deref(), Some("Animal"));
    assert_eq!(r.target_path.as_deref(), Some("Animal.swift"));
}

// ─── static dispatch ──────────────────────────────────────────────────────

#[test]
fn static_method_via_class_name() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Service {
    static func build() -> Service { Service() }
}

func caller() {
    Service.build()
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Service.build")),
        "Service.build() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn top_level_function_call_resolves_within_workspace() {
    let tmp = tempfile::tempdir().unwrap();
    let util = "func helper() {}\n";
    let caller = "func main() {\n    helper()\n}\n";
    let res = run(tmp.path(), &[("Util.swift", util), ("Main.swift", caller)]);
    let calls = calls_of(&res, "Main.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("helper")),
        "module-less helper() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn self_call_resolves_in_current_class() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo {
    func bar() {}
    func go() {
        self.bar()
    }
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Foo.bar")),
        "self.bar() should resolve to Foo.bar; got {:#?}",
        calls
    );
}

#[test]
fn self_init_call_resolves() {
    // `self.init(...)` is a convenience-init delegation. The
    // resolver treats `init` as a regular method name and the call
    // resolves through the MRO walk.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo {
    init() {}
    init(x: Int) {
        self.init()
    }
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Foo.init")),
        "self.init() should resolve to Foo.init; got {:#?}",
        calls
    );
}

#[test]
fn struct_method_resolves_via_self() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
struct Point {
    func magnitude() -> Double { 0.0 }
    func describe() {
        let _ = self.magnitude()
    }
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Point.magnitude")),
        "struct self.magnitude() should resolve; got {:#?}",
        calls
    );
}

// ─── require_graph (imports → workspace files) ───────────────────────────

#[test]
fn apple_framework_import_records_qualified_without_path() {
    let tmp = tempfile::tempdir().unwrap();
    let main = "import Foundation\n";
    let res = run(tmp.path(), &[("Main.swift", main)]);
    let imps = imports_of(&res, "Main.swift");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("Foundation"))
        .expect("Foundation import should record qualified");
    assert!(
        hit.target_path.is_none(),
        "Apple framework import must have no target_path; got {:?}",
        hit.target_path
    );
}

#[test]
fn dotted_apple_framework_import_records_full_path_qualified() {
    let tmp = tempfile::tempdir().unwrap();
    let main = "import UIKit.UIView\n";
    let res = run(tmp.path(), &[("Main.swift", main)]);
    let imps = imports_of(&res, "Main.swift");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("UIKit.UIView"))
        .expect("UIKit.UIView import should record qualified");
    assert!(
        hit.target_path.is_none(),
        "UIKit framework import must have no target_path; got {:?}",
        hit.target_path
    );
}

#[test]
fn import_emits_resolution_at_path_byte_range() {
    let tmp = tempfile::tempdir().unwrap();
    let main = "import Foundation\n";
    let res = run(tmp.path(), &[("Main.swift", main)]);
    let imps = imports_of(&res, "Main.swift");
    let path_start = main.find("Foundation").unwrap() as u32;
    let path_end = path_start + "Foundation".len() as u32;
    let hit = imps
        .iter()
        .find(|r| r.site_byte_range.start == path_start && r.site_byte_range.end == path_end)
        .expect("import resolution must be pinned at the dotted path span");
    assert_eq!(hit.target_qualified.as_deref(), Some("Foundation"));
}

// ─── 諦め: things Tier-2.5 must NOT resolve ─────────────────────────────

#[test]
fn unknown_receiver_method_call_is_not_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
func go(obj: Any) {
    obj.render()
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    assert!(
        calls.iter().all(|c| c
            .target_qualified
            .as_deref()
            .map(|q| !q.ends_with(".render"))
            .unwrap_or(true)),
        "obj.render() must not resolve to any workspace target; got {:#?}",
        calls
    );
}

#[test]
fn mirror_reflection_is_not_emitted_as_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
func go(x: Any) {
    let m = Mirror(reflecting: x)
    _ = m
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    // `Mirror(reflecting:)` is an Apple framework call; it must not
    // resolve to any workspace target.
    assert!(
        calls.iter().all(|c| c
            .target_qualified
            .as_deref()
            .map(|q| !q.ends_with(".Mirror"))
            .unwrap_or(true)),
        "Mirror reflection must not resolve; got {:#?}",
        calls
    );
}

#[test]
fn obj_property_access_does_not_emit_call_resolution() {
    // Property access (no parens) is not a call_expression; we must
    // not invent a Call row for it.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Foo {
    let name: String = \"x\"
}

func go() {
    let obj = Foo()
    let _ = obj.name
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    let name_hits: Vec<_> = calls
        .iter()
        .filter(|c| c.target_qualified.as_deref() == Some("Foo.name"))
        .collect();
    assert!(
        name_hits.is_empty(),
        "obj.name property access must not produce a Call row; got {:#?}",
        name_hits
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
    let res = run(tmp.path(), &[("Empty.swift", "")]);
    assert!(res.is_empty());
}

#[test]
fn malformed_swift_degrades_to_partial_resolutions() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "class Base {}\n";
    let broken = "class Sub: Base {}\n\nfunc broken() { let x: = 1 }\n";
    let res = run(tmp.path(), &[("Base.swift", base), ("Sub.swift", broken)]);
    let types = types_of(&res, "Sub.swift");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("Base")),
        "intact Sub: Base should still resolve despite a downstream parse error; got {:#?}",
        types,
    );
}

#[test]
fn analyzer_id_and_revision_are_stable() {
    use crate::{ANALYZER_ID, ANALYZER_REVISION, PARSER_ID, RESOLUTION_SOURCE, TIER_PREFIX};
    assert_eq!(ANALYZER_ID, "swift-resolver");
    assert_eq!(TIER_PREFIX, "tier25");
    assert_eq!(ANALYZER_REVISION, 3);
    assert_eq!(PARSER_ID, "tree-sitter-swift");
    assert_eq!(RESOLUTION_SOURCE, "tier25-swift-resolver");
}

#[test]
fn extension_emits_class_def() {
    // tree-sitter-swift's `extension` parses as a `class_declaration`
    // with `declaration_kind = "extension"`. Tier-2.5 records it as
    // a ClassDef with `kind = Extension` so dotted lookup still
    // finds the extended type.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
class Store {}

extension Store {
    func reload() {}
}

func caller() {
    Store.reload()
}
";
    let res = run(tmp.path(), &[("M.swift", src)]);
    let calls = calls_of(&res, "M.swift");
    // `Store.reload()` is a member-lookup-on-instance via the
    // type's name. Static-dispatch resolves through MethodIndex's
    // owner lookup with owner = "Store".
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Store.reload")),
        "Store.reload() should resolve via extension method index; got {:#?}",
        calls
    );
}
