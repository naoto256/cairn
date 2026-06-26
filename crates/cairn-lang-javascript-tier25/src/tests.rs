//! Unit tests for the JavaScript Tier-2.5 resolver.

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

// ─── CJS resolve ──────────────────────────────────────────────────────

#[test]
fn cjs_default_require_resolves_module_path() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "class X {}\nmodule.exports = X;\n";
    let main = "const X = require('./foo');\nclass Sub extends X {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    let hit = imps
        .iter()
        .find(|r| r.target_path.as_deref() == Some("foo.js"))
        .expect("CJS require of ./foo should resolve");
    // Import edges target a *file*, not a symbol; the require-graph
    // sets `target_qualified = None` (Phase 3, matching the Ruby
    // and other tier25 backends' Phase 1 contract) so persist.rs
    // skips the symbol lookup and `target_path` remains the source
    // of truth.
    assert!(hit.target_qualified.is_none());
}

#[test]
fn cjs_destructured_require_emits_per_binding() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "class X {}\nclass Y {}\nmodule.exports = { X, Y };\n";
    let main = "const { X, Y } = require('./foo');\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    // Single Import row per import site even with multiple bindings.
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("foo.js")),
        "destructured require should resolve to foo.js; got {:#?}",
        imps
    );
}

#[test]
fn cjs_member_require_resolves_class_export() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "class Y {}\nmodule.exports = { Y };\n";
    let main = "const Y = require('./foo').Y;\nclass Sub extends Y {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("foo.js")
                && t.target_qualified.as_deref() == Some("Y")),
        "Sub extends Y (from require('./foo').Y) should resolve; got {:#?}",
        types
    );
}

#[test]
fn cjs_require_resolves_index_js() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = "module.exports = class X {};\n";
    let main = "const X = require('./pkg');\n";
    let res = run(tmp.path(), &[("pkg/index.js", idx), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("pkg/index.js")),
        "require('./pkg') should resolve to pkg/index.js; got {:#?}",
        imps
    );
}

#[test]
fn cjs_module_exports_object_inverts_for_consumer() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "class A {}\nclass B {}\nmodule.exports = { A, B };\n";
    let main = "const { A } = require('./foo');\nclass Sub extends A {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("A")
                && t.target_path.as_deref() == Some("foo.js")),
        "Sub extends A should resolve via module.exports inversion; got {:#?}",
        types
    );
}

// ─── ESM resolve ──────────────────────────────────────────────────────

#[test]
fn esm_default_import_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "export default class X {}\n";
    let main = "import X from './foo';\nclass Sub extends X {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("foo.js")),
        "ESM default import should resolve; got {:#?}",
        types
    );
}

#[test]
fn esm_named_import_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "export class X {}\nexport class Y {}\n";
    let main = "import { X, Y } from './foo';\nclass Sub extends X {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("X")
                && t.target_path.as_deref() == Some("foo.js")),
        "ESM named X should resolve; got {:#?}",
        types
    );
}

#[test]
fn esm_namespace_import_with_member_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "export class X {}\n";
    let main = "import * as Ns from './foo';\nclass Sub extends Ns.X {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("X")
                && t.target_path.as_deref() == Some("foo.js")),
        "Ns.X via namespace import should resolve; got {:#?}",
        types
    );
}

#[test]
fn esm_side_effect_import_emits_import_row() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "console.log('side');\n";
    let main = "import './foo';\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("foo.js")),
        "side-effect import './foo' should emit Import row; got {:#?}",
        imps
    );
}

#[test]
fn esm_reexport_from_records_import() {
    let tmp = tempfile::tempdir().unwrap();
    let bar = "export class X {}\n";
    let main = "export { X } from './bar';\n";
    let res = run(tmp.path(), &[("bar.js", bar), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("bar.js")),
        "re-export should resolve target path; got {:#?}",
        imps
    );
}

// ─── mixed CJS + ESM ──────────────────────────────────────────────────

#[test]
fn cjs_and_esm_files_can_cross_resolve() {
    let tmp = tempfile::tempdir().unwrap();
    let cjs = "class A {}\nmodule.exports = { A };\n";
    let esm = "import { A } from './a';\nexport class B extends A {}\n";
    let res = run(tmp.path(), &[("a.js", cjs), ("b.js", esm)]);
    let types = types_of(&res, "b.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("a.js")
                && t.target_qualified.as_deref() == Some("A")),
        "ESM file should resolve A from CJS file; got {:#?}",
        types
    );
}

#[test]
fn mjs_resolves_cjs_export() {
    let tmp = tempfile::tempdir().unwrap();
    let cjs = "class A {}\nmodule.exports = { A };\n";
    let mjs = "import { A } from './a.cjs';\nexport class B extends A {}\n";
    let res = run(tmp.path(), &[("a.cjs", cjs), ("b.mjs", mjs)]);
    let types = types_of(&res, "b.mjs");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("a.cjs")),
        ".mjs importing .cjs should resolve; got {:#?}",
        types
    );
}

#[test]
fn cjs_requiring_esm_file_records_target() {
    let tmp = tempfile::tempdir().unwrap();
    let esm = "export class A {}\n";
    let cjs = "const { A } = require('./esm');\n";
    let res = run(tmp.path(), &[("esm.js", esm), ("cjs.js", cjs)]);
    let imps = imports_of(&res, "cjs.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("esm.js")),
        "CJS require of ESM should at least pin the file; got {:#?}",
        imps
    );
}

// ─── class hierarchy ──────────────────────────────────────────────────

#[test]
fn same_file_extends_resolves_type_ref() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Animal {}\nclass Dog extends Animal {}\n";
    let res = run(tmp.path(), &[("a.js", src)]);
    let types = types_of(&res, "a.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("Animal")
                && t.target_path.as_deref() == Some("a.js")),
        "Dog extends Animal (same file) should resolve; got {:#?}",
        types
    );
}

#[test]
fn cross_file_extends_via_esm_import() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "export class Animal {}\n";
    let dog = "import { Animal } from './animal';\nexport class Dog extends Animal {}\n";
    let res = run(tmp.path(), &[("animal.js", animal), ("dog.js", dog)]);
    let types = types_of(&res, "dog.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("animal.js")),
        "Dog extends Animal cross-file ESM should resolve; got {:#?}",
        types
    );
}

#[test]
fn cross_file_extends_via_cjs_require() {
    let tmp = tempfile::tempdir().unwrap();
    let animal = "class Animal {}\nmodule.exports = Animal;\n";
    let dog = "const Animal = require('./animal');\nclass Dog extends Animal {}\n";
    let res = run(tmp.path(), &[("animal.js", animal), ("dog.js", dog)]);
    let types = types_of(&res, "dog.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("animal.js")),
        "Dog extends Animal cross-file CJS should resolve; got {:#?}",
        types
    );
}

#[test]
fn dotted_extends_via_namespace_import() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "export class Base {}\n";
    let main = "import * as ns from './foo';\nclass A extends ns.Base {}\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("Base")),
        "extends ns.Base should resolve through namespace import; got {:#?}",
        types
    );
}

#[test]
fn mixin_factory_extends_is_skipped_from_mro() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Base {}\nfunction Mixin(B) { return class extends B {}; }\nclass Foo extends Mixin(Base) {}\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let types = types_of(&res, "m.js");
    // The base is a call expression — we deliberately emit nothing.
    assert!(
        types
            .iter()
            .all(|t| t.target_qualified.as_deref() != Some("Base")
                || t.site_byte_range.start != src.rfind("Mixin(Base)").unwrap() as u32),
        "Mixin(Base) call-expr base must not produce a Type resolution; got {:#?}",
        types
    );
}

// ─── dispatch ─────────────────────────────────────────────────────────

#[test]
fn static_call_via_class_name() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Registry { static build() {} }\nclass C { go() { Registry.build(); } }\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let calls = calls_of(&res, "m.js");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Registry.build")),
        "Registry.build() should resolve; got {:#?}",
        calls
    );
}

#[test]
fn this_call_resolves_in_current_class() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Foo { bar() {} go() { this.bar(); } }\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let calls = calls_of(&res, "m.js");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Foo.bar")),
        "this.bar() should resolve to Foo.bar; got {:#?}",
        calls
    );
}

#[test]
fn super_call_resolves_to_parent_method() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class Base { step() {} }\nclass Child extends Base { step() { super.step(); } }\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let calls = calls_of(&res, "m.js");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("Base.step")),
        "super.step() should resolve to Base.step; got {:#?}",
        calls
    );
}

#[test]
fn top_level_function_call_resolves_in_file() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "function helper() {}\nfunction main() { helper(); }\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let calls = calls_of(&res, "m.js");
    assert!(
        calls
            .iter()
            .any(|c| c.target_qualified.as_deref() == Some("helper")),
        "bare helper() should resolve to top-level function; got {:#?}",
        calls
    );
}

#[test]
fn imported_function_bare_call_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let foo = "export function greet() {}\n";
    let main = "import { greet } from './foo';\nfunction run() { greet(); }\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let calls = calls_of(&res, "main.js");
    assert!(
        calls
            .iter()
            .any(|c| c.target_path.as_deref() == Some("foo.js")
                && c.target_qualified.as_deref() == Some("greet")),
        "imported greet() should resolve cross-file; got {:#?}",
        calls
    );
}

// ─── 諦め範囲 ──────────────────────────────────────────────────────────

#[test]
fn unknown_receiver_method_call_is_not_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "function go(obj) { obj.render(); }\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let calls = calls_of(&res, "m.js");
    assert!(
        calls.iter().all(|c| c
            .target_qualified
            .as_deref()
            .map(|q| !q.ends_with(".render"))
            .unwrap_or(true)),
        "obj.render() with unknown obj must not resolve; got {:#?}",
        calls
    );
}

#[test]
fn bare_specifier_import_records_no_path() {
    // Phase 3 contract: bare specifiers (npm packages) produce an
    // Import resolution whose `target_path` and `target_qualified`
    // are both `None`. Import edges target a file, not a symbol, so
    // the specifier string (`express`) intentionally does not become
    // a path-shaped `target_qualified`.
    let tmp = tempfile::tempdir().unwrap();
    let src = "const express = require('express');\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let imps = imports_of(&res, "m.js");
    assert_eq!(imps.len(), 1, "exactly one import row expected: {imps:#?}");
    let hit = &imps[0];
    assert!(hit.target_path.is_none());
    assert!(hit.target_qualified.is_none());
}

#[test]
fn node_builtin_import_records_no_path() {
    // Same as bare_specifier: `node:fs` is a builtin module, not a
    // workspace file or workspace symbol.
    let tmp = tempfile::tempdir().unwrap();
    let src = "import fs from 'node:fs';\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let imps = imports_of(&res, "m.js");
    assert_eq!(imps.len(), 1, "exactly one import row expected: {imps:#?}");
    let hit = &imps[0];
    assert!(hit.target_path.is_none());
    assert!(hit.target_qualified.is_none());
}

#[test]
fn path_alias_import_records_no_path() {
    // `@/foo` is a webpack/tsconfig-style path alias the resolver
    // does not understand. Should fall back to the same "no path,
    // no qualified" shape as bare specifiers — the resolver
    // refuses to invent a workspace path.
    let tmp = tempfile::tempdir().unwrap();
    let src = "import x from '@/foo';\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let imps = imports_of(&res, "m.js");
    assert_eq!(imps.len(), 1, "exactly one import row expected: {imps:#?}");
    let hit = &imps[0];
    assert!(hit.target_path.is_none());
    assert!(hit.target_qualified.is_none());
}

#[test]
fn object_set_prototype_of_does_not_invent_hierarchy() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "class A {}\nclass B {}\nObject.setPrototypeOf(B.prototype, A.prototype);\n";
    let res = run(tmp.path(), &[("m.js", src)]);
    let types = types_of(&res, "m.js");
    // We never emit a Type resolution for B extending A here because
    // there's no `extends` syntax.
    assert!(
        types.is_empty()
            || !types
                .iter()
                .any(|t| t.target_qualified.as_deref() == Some("A")),
        "Object.setPrototypeOf must not invent B→A; got {:#?}",
        types
    );
}

// ─── PR-β: expanded require() ImportBinding emit ──────────────────────

#[test]
fn statement_require_flows_to_require_edge_target_path() {
    // Top-level `require('./setup');` is a side-effect import — the
    // require_graph still produces a RequireEdge with a real
    // `target_path` so the resolutions row can fall through to the
    // workspace file.
    let tmp = tempfile::tempdir().unwrap();
    let setup = "console.log('init');\n";
    let main = "require('./setup');\n";
    let res = run(tmp.path(), &[("setup.js", setup), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("setup.js")),
        "statement-position require should resolve to setup.js; got {:#?}",
        imps
    );
}

#[test]
fn expression_require_flows_to_require_edge_target_path() {
    // `app.use(require('./routes'))` — argument-nested require call.
    let tmp = tempfile::tempdir().unwrap();
    let routes = "module.exports = {};\n";
    let main = "const app = {};\napp.use(require('./routes'));\n";
    let res = run(tmp.path(), &[("routes.js", routes), ("main.js", main)]);
    let imps = imports_of(&res, "main.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("routes.js")),
        "expression-position require should resolve to routes.js; got {:#?}",
        imps
    );
}

#[test]
fn module_exports_require_emits_module_edge() {
    // `module.exports = require('./inner')` — re-export shape. Tier-2.5
    // emits the require edge with a `target_path`, but does NOT create
    // a ResolvedBinding (edge-only contract; named re-export graph
    // semantics are out of scope for this PR).
    let tmp = tempfile::tempdir().unwrap();
    let inner = "module.exports = class X {};\n";
    let outer = "module.exports = require('./inner');\n";
    let res = run(tmp.path(), &[("inner.js", inner), ("outer.js", outer)]);
    let imps = imports_of(&res, "outer.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("inner.js")),
        "module.exports = require('./inner') should pin inner.js; got {:#?}",
        imps
    );
    // target_qualified must remain None — import edges target a file,
    // not a symbol (Phase 1 contract).
    let hit = imps
        .iter()
        .find(|r| r.target_path.as_deref() == Some("inner.js"))
        .unwrap();
    assert!(hit.target_qualified.is_none());
}

#[test]
fn exports_named_reexport_does_not_emit_tier25_import_binding() {
    // `exports.X = require('./inner')` — named re-export, scope-out
    // per spec. The assignment visitor recognises the LHS and claims
    // the RHS call site in `seen_require_sites` without emitting,
    // suppressing the generic call_expression visitor's side-effect
    // ImportBinding. We check at the `FileConstFacts` layer because
    // require_graph dedups downstream — the upstream gate is where
    // the scope-out contract must hold.
    let src = b"exports.foo = require('./inner');\n";
    let facts = crate::const_resolver::parse_file(src).expect("parses");
    let leaked: Vec<_> = facts
        .import_bindings
        .iter()
        .filter(|b| b.module == "./inner")
        .collect();
    assert!(
        leaked.is_empty(),
        "exports.X = require('./inner') must NOT produce an ImportBinding \
         (named re-export is scope-out); got: {leaked:#?}",
    );
}

#[test]
fn module_exports_named_reexport_does_not_emit_tier25_import_binding() {
    // Same scope-out as above, nested form: `module.exports.X = require(...)`.
    let src = b"module.exports.foo = require('./inner');\n";
    let facts = crate::const_resolver::parse_file(src).expect("parses");
    let leaked: Vec<_> = facts
        .import_bindings
        .iter()
        .filter(|b| b.module == "./inner")
        .collect();
    assert!(
        leaked.is_empty(),
        "module.exports.X = require('./inner') must NOT produce an \
         ImportBinding (named re-export is scope-out); got: {leaked:#?}",
    );
}

#[test]
fn binding_form_require_not_double_emitted() {
    // `const X = require('./foo')` reaches both the binding-form path
    // (emit_var_declaration → try_emit_cjs_require) and the generic
    // expression-position visitor (try_emit_expression_position_require)
    // because the visitor walks into the variable_declarator's RHS
    // call_expression. The shared `seen_require_sites` guarantees a
    // single ImportBinding (Cjs flavor, with `local = "X"`) — the
    // generic visitor must skip the same call site.
    //
    // We check at the `FileConstFacts` layer rather than the resolutions
    // layer, because `require_graph.rs` already dedups by
    // (path, site_byte_start, site_byte_end) at edge-emission time, so
    // an `import_bindings`-level dup would still collapse to a single
    // Import row downstream. This test pins the upstream dedup
    // separately so a future refactor can't silently lean on the
    // edge-level dedup and break the alias map.
    let src = b"const X = require('./foo');\n";
    let facts = crate::const_resolver::parse_file(src).expect("parses");
    assert_eq!(
        facts.import_bindings.len(),
        1,
        "exactly one ImportBinding for the require site; got {:#?}",
        facts.import_bindings
    );
    assert_eq!(facts.import_bindings[0].local, "X");
    assert!(matches!(
        facts.import_bindings[0].kind,
        crate::const_resolver::ImportKind::Cjs
    ));
}

#[test]
fn analyzer_revision_bumped_for_expanded_require_emit() {
    // Revision 4 (PR-β): const_resolver now emits ImportBinding for
    // statement / expression / re-export `require(...)` shapes.
    // The 2nd real use case of PR #220's analyzer-revision staleness
    // scanner (after Wave 2C's PR-α CJS binding-form expansion).
    use crate::ANALYZER_REVISION;
    assert_eq!(ANALYZER_REVISION, 4);
}

// ─── glue ─────────────────────────────────────────────────────────────

#[test]
fn analyzer_id_and_revision_are_stable() {
    use crate::{ANALYZER_ID, ANALYZER_REVISION, PARSER_ID, RESOLUTION_SOURCE, TIER_PREFIX};
    assert_eq!(ANALYZER_ID, "javascript-resolver");
    assert_eq!(TIER_PREFIX, "tier25");
    assert_eq!(ANALYZER_REVISION, 4);
    assert_eq!(PARSER_ID, "tree-sitter-javascript");
    assert_eq!(RESOLUTION_SOURCE, "tier25-javascript-resolver");
}

#[test]
fn empty_file_does_not_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[("Empty.js", "")]);
    assert!(res.is_empty());
}

#[test]
fn malformed_js_degrades_to_partial_resolutions() {
    let tmp = tempfile::tempdir().unwrap();
    let base = "export class Base {}\n";
    let broken = "import { Base } from './base';\nclass Sub extends Base {}\nfunction broken() { let x = ; }\n";
    let res = run(tmp.path(), &[("base.js", base), ("sub.js", broken)]);
    let types = types_of(&res, "sub.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("base.js")),
        "intact extends Base should still resolve despite parse error; got {:#?}",
        types
    );
}
