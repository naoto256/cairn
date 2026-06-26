//! Unit tests for the Kotlin Tier-2.5 resolver.

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

// ─── const_resolver (lexical / package / aliases) ────────────────────────

#[test]
fn class_in_same_package_resolves_without_import() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "package com.foo\n\nopen class Widget\n";
    let sub = "package com.foo\n\nclass Sub : Widget()\n";
    let res = run(tmp.path(), &[("Widget.kt", base), ("Sub.kt", sub)]);
    let types = types_of(&res, "Sub.kt");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("Widget base should resolve via same-package lookup");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

#[test]
fn fully_qualified_base_resolves_directly() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "package com.foo\n\nopen class Widget\n";
    let sub = "package other\n\nclass Sub : com.foo.Widget()\n";
    let res = run(tmp.path(), &[("Widget.kt", base), ("Sub.kt", sub)]);
    let types = types_of(&res, "Sub.kt");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("FQN base should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

#[test]
fn plain_import_creates_binding_for_short_name() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "package com.foo\n\nopen class Widget\n";
    let sub = "package other\n\nimport com.foo.Widget\n\nclass Sub : Widget()\n";
    let res = run(tmp.path(), &[("Widget.kt", base), ("Sub.kt", sub)]);
    let types = types_of(&res, "Sub.kt");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("imported Widget should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

#[test]
fn aliased_import_resolves_through_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "package com.foo\n\nopen class Widget\n";
    let sub = "package other\n\nimport com.foo.Widget as W\n\nclass Sub : W()\n";
    let res = run(tmp.path(), &[("Widget.kt", base), ("Sub.kt", sub)]);
    let types = types_of(&res, "Sub.kt");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("aliased import should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

#[test]
fn wildcard_import_resolves_unqualified_use() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "package com.foo\n\nopen class Widget\n";
    let sub = "package other\n\nimport com.foo.*\n\nclass Sub : Widget()\n";
    let res = run(tmp.path(), &[("Widget.kt", base), ("Sub.kt", sub)]);
    let types = types_of(&res, "Sub.kt");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("wildcard import should resolve Widget");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

// ─── MRO ──────────────────────────────────────────────────────────────────

#[test]
fn single_inheritance_resolves_this_method_call() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

open class A {
    fun foo() {}
}

class B : A() {
    fun bar() {
        this.foo()
    }
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.A.foo")),
        "this.foo() should resolve to A.foo via MRO; got {:#?}",
        calls
    );
}

#[test]
fn super_call_resolves_to_parent_method() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

open class Base {
    open fun step() {}
}

class Child : Base() {
    override fun step() {
        super.step()
    }
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Base.step")),
        "super.step() should resolve to Base.step; got {:#?}",
        calls
    );
}

#[test]
fn interface_method_resolves_via_implement_edge() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

interface Greeter {
    fun greet() {}
}

class Service : Greeter {
    fun trigger() {
        this.greet()
    }
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Greeter.greet")),
        "this.greet() should resolve to Greeter.greet through implement edge; got {:#?}",
        calls
    );
}

#[test]
fn cross_file_inheritance_via_import() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "package zoo\n\nopen class Animal {\n    open fun speak() {}\n}\n";
    let dog =
        "package zoo\n\nclass Dog : Animal() {\n    fun bark() {\n        this.speak()\n    }\n}\n";
    let res = run(tmp.path(), &[("Animal.kt", animal), ("Dog.kt", dog)]);
    let calls = calls_of(&res, "Dog.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("zoo.Animal.speak")),
        "this.speak() should resolve via cross-file MRO; got {:#?}",
        calls
    );
}

#[test]
fn heritage_emits_type_resolution_at_base_name_range() {
    // The Tier-2 backend stores the base name's byte range. Tier-2.5
    // MUST emit a Type resolution at exactly that span so
    // find_subtypes / find_supertypes can flip kind_source.
    let tmp = tempfile::tempdir().unwrap();
    let animal = "package zoo\n\nopen class Animal\n";
    let dog = "package zoo\n\nclass Dog : Animal()\n";
    let res = run(tmp.path(), &[("Animal.kt", animal), ("Dog.kt", dog)]);
    let types = types_of(&res, "Dog.kt");
    let pos = dog.rfind("Animal").unwrap() as u32;
    let r = types
        .iter()
        .find(|t| t.site_byte_range.start == pos)
        .expect("Type resolution at base name");
    assert_eq!(r.target_qualified.as_deref(), Some("zoo.Animal"));
    assert_eq!(r.target_path.as_deref(), Some("Animal.kt"));
}

// ─── static dispatch ──────────────────────────────────────────────────────

#[test]
fn companion_method_call_via_class_name() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

class Service {
    companion object {
        fun build(): Service = Service()
    }
}

fun caller() {
    Service.Companion.build()
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Service.Companion.build")),
        "Service.Companion.build() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn top_level_function_call_via_import() {
    let tmp = tempfile::tempdir().unwrap();
    let util = "package util\n\nfun helper() {}\n";
    let caller = "package app\n\nimport util.helper\n\nfun main() {\n    helper()\n}\n";
    let res = run(tmp.path(), &[("Util.kt", util), ("Main.kt", caller)]);
    let calls = calls_of(&res, "Main.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("util.helper")),
        "imported helper() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn top_level_function_call_same_package_no_import() {
    let tmp = tempfile::tempdir().unwrap();
    let util = "package util\n\nfun helper() {}\n";
    let caller = "package util\n\nfun main() {\n    helper()\n}\n";
    let res = run(tmp.path(), &[("Util.kt", util), ("Main.kt", caller)]);
    let calls = calls_of(&res, "Main.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("util.helper")),
        "same-package helper() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn wildcard_import_resolves_bare_top_level_call() {
    let tmp = tempfile::tempdir().unwrap();
    let util = "package util\n\nfun helper() {}\n";
    let caller = "package app\n\nimport util.*\n\nfun main() {\n    helper()\n}\n";
    let res = run(tmp.path(), &[("Util.kt", util), ("Main.kt", caller)]);
    let calls = calls_of(&res, "Main.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("util.helper")),
        "wildcard-imported helper() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn this_call_resolves_in_current_class() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

class Foo {
    fun bar() {}
    fun go() {
        this.bar()
    }
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Foo.bar")),
        "this.bar() should resolve to Foo.bar; got {:#?}",
        calls
    );
}

#[test]
fn fully_qualified_static_call_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let svc = "package com.foo\n\nobject Registry {\n    fun build() {}\n}\n";
    let caller = "package other\n\nfun main() {\n    com.foo.Registry.build()\n}\n";
    let res = run(tmp.path(), &[("Registry.kt", svc), ("Main.kt", caller)]);
    let calls = calls_of(&res, "Main.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("com.foo.Registry.build")),
        "FQN static call should resolve; got {:#?}",
        calls
    );
}

// ─── require_graph (imports → workspace files) ───────────────────────────

#[test]
fn plain_import_emits_resolution_for_workspace_class() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "package com.foo\n\nclass Widget\n";
    let main = "package app\n\nimport com.foo.Widget\n";
    let res = run(tmp.path(), &[("Widget.kt", widget), ("Main.kt", main)]);
    let imps = imports_of(&res, "Main.kt");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("import should emit a resolution");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

#[test]
fn external_import_records_qualified_without_path() {
    let tmp = tempfile::tempdir().unwrap();
    let main = "package app\n\nimport kotlinx.coroutines.delay\n";
    let res = run(tmp.path(), &[("Main.kt", main)]);
    let imps = imports_of(&res, "Main.kt");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("kotlinx.coroutines.delay"))
        .expect("external import should still record the qualified name");
    assert!(
        hit.target_path.is_none(),
        "external import must have no target_path; got {:?}",
        hit.target_path
    );
}

#[test]
fn wildcard_import_records_package_qualified() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "package com.foo\n\nclass Widget\n";
    let main = "package app\n\nimport com.foo.*\n";
    let res = run(tmp.path(), &[("Widget.kt", widget), ("Main.kt", main)]);
    let imps = imports_of(&res, "Main.kt");
    assert!(
        imps.iter()
            .any(|r| r.target_qualified.as_deref() == Some("com.foo")),
        "wildcard import should record package qualified; got {:#?}",
        imps,
    );
}

#[test]
fn aliased_import_resolution_uses_target_fqn() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "package com.foo\n\nclass Widget\n";
    let main = "package app\n\nimport com.foo.Widget as W\n";
    let res = run(tmp.path(), &[("Widget.kt", widget), ("Main.kt", main)]);
    let imps = imports_of(&res, "Main.kt");
    let hit = imps
        .iter()
        .find(|r| r.target_qualified.as_deref() == Some("com.foo.Widget"))
        .expect("aliased import should still resolve to the target FQN");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

#[test]
fn import_emits_resolution_at_path_byte_range() {
    // The Tier-2 backend pins ImportFact.byte_range at the dotted
    // path span. Tier-2.5 must emit at the same span so the join
    // lines up for find_imports.
    let tmp = tempfile::tempdir().unwrap();
    let widget = "package com.foo\n\nclass Widget\n";
    let main = "package app\n\nimport com.foo.Widget\n";
    let res = run(tmp.path(), &[("Widget.kt", widget), ("Main.kt", main)]);
    let imps = imports_of(&res, "Main.kt");
    let path_start = main.find("com.foo.Widget").unwrap() as u32;
    let path_end = path_start + "com.foo.Widget".len() as u32;
    let hit = imps
        .iter()
        .find(|r| r.site_byte_range.start == path_start && r.site_byte_range.end == path_end)
        .expect("import resolution must be pinned at the dotted path span");
    assert_eq!(hit.target_qualified.as_deref(), Some("com.foo.Widget"));
}

// ─── 諦め: things Tier-2.5 must NOT resolve ─────────────────────────────

#[test]
fn unknown_receiver_method_call_is_not_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

fun go(obj: Any) {
    obj.render()
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
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
fn reflection_invoke_is_not_emitted_as_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

class Foo {
    fun bar() {}
}

fun go() {
    val m = Foo::class.java.getMethod(\"bar\")
    m.invoke(Foo())
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    let bar_hit = calls
        .iter()
        .any(|c| c.target_qualified.as_deref() == Some("m.Foo.bar"));
    assert!(
        !bar_hit,
        "reflection invoke must not resolve to Foo.bar; got {:#?}",
        calls
    );
}

#[test]
fn extension_function_on_dynamic_receiver_is_not_invented() {
    // Extension function `String.shout()` is declared, and `s.shout()`
    // is called on a parameter of unknown type. Best-effort name-only
    // match should NOT invent a resolution when more than one
    // workspace method shares the name. Here there's just one, so the
    // best-effort match fires — but receiver-type ambiguity means we
    // accept either "resolves to the unique target" OR "doesn't
    // resolve". What we must NOT do is invent a wrong target.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

fun String.shout(): String = uppercase()

fun go(x: Any) {
    x.toString()
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    // toString isn't declared in the workspace, so it must not resolve
    // to any workspace target.
    assert!(
        calls.iter().all(|c| c
            .target_qualified
            .as_deref()
            .map(|q| !q.ends_with(".toString"))
            .unwrap_or(true)),
        "x.toString() must not invent a target; got {:#?}",
        calls
    );
}

#[test]
fn when_expression_branches_do_not_synthesize_dispatch() {
    // `when` doesn't dispatch by type at Tier-2.5; method calls inside
    // branches resolve like any other call, but the `when` itself
    // doesn't produce a resolution.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

fun route(x: Int): Int = when (x) {
    1 -> 100
    else -> 0
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    assert!(
        calls.is_empty(),
        "when expression must not synthesize any Call resolution; got {:#?}",
        calls
    );
}

#[test]
fn obj_property_access_does_not_emit_call_resolution() {
    // Attribute access (no parens) is not a call_expression; we must
    // not invent a Call row for it.
    let tmp = tempfile::tempdir().unwrap();
    let src = "\
package m

class Foo {
    val name: String = \"x\"
}

fun go() {
    val obj = Foo()
    val n = obj.name
}
";
    let res = run(tmp.path(), &[("M.kt", src)]);
    let calls = calls_of(&res, "M.kt");
    let name_hits: Vec<_> = calls
        .iter()
        .filter(|c| c.target_qualified.as_deref() == Some("m.Foo.name"))
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
    let res = run(tmp.path(), &[("Empty.kt", "")]);
    assert!(res.is_empty());
}

#[test]
fn malformed_kotlin_degrades_to_partial_resolutions() {
    // A file with a parse error in the middle should still produce
    // resolutions for the intact regions.
    let tmp = tempfile::tempdir().unwrap();
    let base = "package m\n\nopen class Base\n";
    let broken = "package m\n\nclass Sub : Base()\n\nfun broken() { val x: = 1 }\n";
    let res = run(tmp.path(), &[("Base.kt", base), ("Sub.kt", broken)]);
    let types = types_of(&res, "Sub.kt");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("m.Base")),
        "intact Sub : Base() should still resolve despite a downstream parse error; got {:#?}",
        types,
    );
}

#[test]
fn analyzer_id_and_revision_are_stable() {
    use crate::{ANALYZER_ID, ANALYZER_REVISION, PARSER_ID, RESOLUTION_SOURCE, TIER_PREFIX};
    assert_eq!(ANALYZER_ID, "kotlin-resolver");
    assert_eq!(TIER_PREFIX, "tier25");
    assert_eq!(ANALYZER_REVISION, 4);
    assert_eq!(PARSER_ID, "tree-sitter-kotlin-ng");
    assert_eq!(RESOLUTION_SOURCE, "tier25-kotlin-resolver");
}

#[test]
fn root_package_file_resolves_class() {
    let tmp = tempfile::tempdir().unwrap();
    // No package declaration: file lives in the root package.
    let base = "open class Widget\n";
    let sub = "class Sub : Widget()\n";
    let res = run(tmp.path(), &[("Widget.kt", base), ("Sub.kt", sub)]);
    let types = types_of(&res, "Sub.kt");
    let hit = types
        .iter()
        .find(|t| t.target_qualified.as_deref() == Some("Widget"))
        .expect("root-package Widget should resolve");
    assert_eq!(hit.target_path.as_deref(), Some("Widget.kt"));
}

// ─── path-aware dispatch (Sub-fix A / A0 / B / C) ────────────────────────

#[test]
fn dotted_call_with_same_name_class_in_multiple_files_picks_correct_target_via_path() {
    // Two distinct packages each declare an `object Registry` with a
    // `build()` member; their qualifieds (`pkg.a.Registry`,
    // `pkg.b.Registry`) differ — this isn't the strict
    // same-FQN-HashMap-collision case but the everyday
    // `same short name in different packages` case. The caller imports
    // the one in `pkg.b`; the alias binding's `target_path` must steer
    // the resolution to the bound file rather than the first
    // `Registry` the resolver happens to visit.
    let tmp = tempfile::tempdir().unwrap();
    let a = "package pkg.a\n\nobject Registry {\n    fun build() {}\n}\n";
    let b = "package pkg.b\n\nobject Registry {\n    fun build() {}\n}\n";
    let caller = "\
package app

import pkg.b.Registry

fun main() {
    Registry.build()
}
";
    let res = run(tmp.path(), &[("A.kt", a), ("B.kt", b), ("Main.kt", caller)]);
    let calls = calls_of(&res, "Main.kt");
    let build_hits: Vec<_> = calls
        .iter()
        .filter(|c| {
            c.target_qualified
                .as_deref()
                .map(|q| q.ends_with(".build"))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !build_hits.is_empty(),
        "Registry.build() should resolve to one of the workspace Registry objects; got {:#?}",
        calls,
    );
    assert!(
        build_hits.iter().all(
            |c| c.target_qualified.as_deref() == Some("pkg.b.Registry.build")
                && c.target_path.as_deref() == Some("B.kt")
        ),
        "Registry.build() must resolve to pkg.b.Registry.build (binding's pinned file), \
         not pkg.a's; got {:#?}",
        build_hits,
    );
}

#[test]
fn dotted_call_resolves_via_validated_candidate_chain_when_pkg_prefix_does_not_exist() {
    // `Foo` is not in `app`'s package and there is no `import` for it,
    // but `app` imports the package via wildcard and `pkg.foo` declares
    // `Foo`. The buggy resolver would short-circuit on the
    // package+parts string ("app.Foo") and return it unchecked, killing
    // the wildcard fallback. This test pins the validated chain.
    let tmp = tempfile::tempdir().unwrap();
    let lib = "\
package pkg.foo

object Foo {
    fun bar() {}
}
";
    // A second `bar` exists on an unrelated class so the
    // get_unique_by_name fallback can't paper over a missing
    // wildcard-import resolution path. The dispatcher must reach
    // pkg.foo.Foo.bar through the validated candidate chain (alias →
    // same-package → wildcard → bare FQN), not through name-only.
    let noise = "\
package pkg.other

object Sink {
    fun bar() {}
}
";
    let caller = "\
package app

import pkg.foo.*

fun go() {
    Foo.bar()
}
";
    let res = run(
        tmp.path(),
        &[("Lib.kt", lib), ("Noise.kt", noise), ("Main.kt", caller)],
    );
    let calls = calls_of(&res, "Main.kt");
    let bar_hits: Vec<_> = calls
        .iter()
        .filter(|c| c.target_qualified.as_deref() == Some("pkg.foo.Foo.bar"))
        .collect();
    assert!(
        !bar_hits.is_empty(),
        "Foo.bar() must resolve via wildcard import when no same-package candidate \
         exists; the dispatcher must not short-circuit on an unchecked `app.Foo`; \
         got {:#?}",
        calls,
    );
    assert_eq!(bar_hits[0].target_path.as_deref(), Some("Lib.kt"));
}

#[test]
fn bare_call_resolves_via_lexical_class_mro() {
    // Inside `Sub.run`, the bare `foo()` must walk Sub's MRO and find
    // `Base.foo`. Pre-Sub-fix-C, the Bare branch ignored
    // `lexical_class` entirely and only checked alias / same-package /
    // wildcard. A second unrelated class also declares `foo`, so the
    // best-effort unique-name fallback can't paper over a missing MRO
    // walk — without lexical-class MRO this call stays unresolved.
    let tmp = tempfile::tempdir().unwrap();
    let base = "\
package m

open class Base {
    fun foo() {}
}

class Other {
    fun foo() {}
}
";
    let sub = "\
package m

class Sub : Base() {
    fun run() {
        foo()
    }
}
";
    let res = run(tmp.path(), &[("Base.kt", base), ("Sub.kt", sub)]);
    let calls = calls_of(&res, "Sub.kt");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("m.Base.foo")
                && c.target_path.as_deref() == Some("Base.kt")),
        "bare foo() must resolve to m.Base.foo via lexical-class MRO; got {:#?}",
        calls,
    );
}

#[test]
fn import_alias_call_uses_binding_target_path_not_first_qualified_hit() {
    // Sub-fix A0 pin. Two files declare a top-level function `helper`
    // in distinct packages. The caller `import pkg.first.helper`s,
    // then calls `helper()`. The binding's `target_path` must be
    // preserved through the alias map so dispatch picks the file the
    // import points at, not whichever package the HashMap visited
    // first.
    let tmp = tempfile::tempdir().unwrap();
    let first = "package pkg.first\n\nfun helper() {}\n";
    let second = "package pkg.second\n\nfun helper() {}\n";
    let caller = "\
package app

import pkg.first.helper

fun main() {
    helper()
}
";
    let res = run(
        tmp.path(),
        &[
            ("First.kt", first),
            ("Second.kt", second),
            ("Main.kt", caller),
        ],
    );
    let calls = calls_of(&res, "Main.kt");
    let helper_hits: Vec<_> = calls
        .iter()
        .filter(|c| {
            c.target_qualified
                .as_deref()
                .map(|q| q.ends_with(".helper"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        helper_hits.len(),
        1,
        "helper() should produce exactly one resolution; got {:#?}",
        helper_hits,
    );
    assert_eq!(
        helper_hits[0].target_qualified.as_deref(),
        Some("pkg.first.helper"),
        "import binding must steer dispatch to pkg.first.helper, not the other file",
    );
    assert_eq!(helper_hits[0].target_path.as_deref(), Some("First.kt"));
}

#[test]
fn alias_bound_dotted_prefix_does_not_fall_back_to_same_package_when_bound_file_misses() {
    // `import pkg.b.Registry` path-binds `Registry` to `B.kt`. The
    // caller writes `Registry.Inner.build()`, but the bound file has
    // no `Inner`. There IS an `app.Registry.Inner.build` in the
    // caller's own package — without a terminal alias contract the
    // resolver would silently fall through alias → same-package and
    // adopt `app.Registry.Inner.build`, silently re-interpreting the
    // user's `import`. The terminal contract is: once the alias head
    // matches, the dotted prefix is finalized; a miss means
    // unresolved, not "try the next stage".
    let tmp = tempfile::tempdir().unwrap();
    let app = "\
package app

object Registry {
    object Inner {
        fun build() {}
    }
}
";
    let b = "\
package pkg.b

object Registry {}
";
    let caller = "\
package app

import pkg.b.Registry

fun main() {
    Registry.Inner.build()
}
";
    let res = run(
        tmp.path(),
        &[("App.kt", app), ("B.kt", b), ("Main.kt", caller)],
    );
    let calls = calls_of(&res, "Main.kt");
    let leaked: Vec<_> = calls
        .iter()
        .filter(|c| c.target_qualified.as_deref() == Some("app.Registry.Inner.build"))
        .collect();
    assert!(
        leaked.is_empty(),
        "alias-bound prefix must NOT fall through to same-package; \
         leaked into app.Registry.Inner.build: {:#?}",
        leaked,
    );
}
