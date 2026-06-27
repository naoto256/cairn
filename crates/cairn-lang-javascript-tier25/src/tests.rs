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

// ─── glue ─────────────────────────────────────────────────────────────

#[test]
fn analyzer_id_and_revision_are_stable() {
    use crate::{ANALYZER_ID, ANALYZER_REVISION, PARSER_ID, RESOLUTION_SOURCE, TIER_PREFIX};
    assert_eq!(ANALYZER_ID, "javascript-resolver");
    assert_eq!(TIER_PREFIX, "tier25");
    assert_eq!(ANALYZER_REVISION, 5);
    assert_eq!(PARSER_ID, "tree-sitter-javascript");
    assert_eq!(RESOLUTION_SOURCE, "tier25-javascript-resolver");
}

#[test]
fn empty_file_does_not_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[("Empty.js", "")]);
    assert!(res.is_empty());
}

// ─── 0.7.1 follow-up: resolver correctness fixes ─────────────────────

/// #8: `lookup_class_in_file` must scope to the requested path. Two
/// files both define `class Foo`; file A's same-file `extends Foo`
/// must resolve to A's Foo, not B's. Before the fix, the lookup
/// fell back to "first hit across the workspace", so the test below
/// would have flapped on insertion order.
#[test]
fn lookup_class_in_file_returns_only_same_file_class() {
    let tmp = tempfile::tempdir().unwrap();
    let a = "class Foo {}\nclass Bar extends Foo {}\n";
    let b = "class Foo {}\n";
    let res = run(tmp.path(), &[("a.js", a), ("b.js", b)]);
    let types = types_of(&res, "a.js");
    // The `extends Foo` site in a.js must point at a.js's Foo. The
    // pre-fix bug let it leak to b.js because both classes share
    // the qualified name and `lookup_class_in_file` returned the
    // first hit irrespective of path.
    assert!(
        types
            .iter()
            .any(|t| t.target_qualified.as_deref() == Some("Foo")
                && t.target_path.as_deref() == Some("a.js")),
        "extends Foo in a.js must resolve to a.js, not b.js; got {:#?}",
        types
    );
    assert!(
        types
            .iter()
            .all(|t| t.target_path.as_deref() != Some("b.js")),
        "no a.js type ref should leak into b.js; got {:#?}",
        types
    );
}

/// #9: `new Foo().bar()` in the file that declares `Foo` must
/// resolve even when another file also defines a same-named class.
/// Pre-fix, `lookup_unique_class` returned None on the ambiguous
/// name and `NewExpr` had no same-file fallback (unlike
/// `Cls.method()`).
#[test]
fn new_expr_same_file_class_resolves_when_duplicate_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let a = "class Foo { bar() {} }\nfunction main() { new Foo().bar(); }\n";
    let b = "class Foo {}\n";
    let res = run(tmp.path(), &[("a.js", a), ("b.js", b)]);
    let calls = calls_of(&res, "a.js");
    assert!(
        calls
            .iter()
            .any(|c| c.target_path.as_deref() == Some("a.js")
                && c.target_qualified.as_deref() == Some("Foo.bar")),
        "new Foo().bar() in a.js must resolve to a.js Foo.bar even with duplicate class in b.js; got {:#?}",
        calls
    );
}

/// #11: `emit_function` indexes only top-level function declarations.
/// Nested helpers (`function inner() {}` inside another function)
/// must not appear in the workspace symbol index, so a sibling call
/// to `inner()` from outside `outer` cannot statically pin to them.
#[test]
fn nested_function_declaration_not_indexed_as_workspace_symbol() {
    let tmp = tempfile::tempdir().unwrap();
    // outer is top-level; inner is nested. A second file calls
    // `inner()` at module scope — if `inner` had leaked into the
    // index, dispatch would pin it.
    let a = "function outer() { function inner() {} }\n";
    let b = "inner();\n";
    let res = run(tmp.path(), &[("a.js", a), ("b.js", b)]);
    // No file should see `inner` as a resolved target.
    let all_inner_hits: Vec<_> = res
        .iter()
        .filter(|r| r.target_qualified.as_deref() == Some("inner"))
        .collect();
    assert!(
        all_inner_hits.is_empty(),
        "nested `inner` must not be a workspace-addressable target; got {:#?}",
        all_inner_hits
    );
    // Sanity: outer should still be indexed (it's top-level).
    let outer_calls = run(
        tmp.path(),
        &[("a.js", a), ("c.js", "function outer() {}\nouter();\n")],
    );
    let c_calls = calls_of(&outer_calls, "c.js");
    assert!(
        c_calls
            .iter()
            .any(|r| r.target_qualified.as_deref() == Some("outer")),
        "top-level outer() must remain resolvable; got {:#?}",
        c_calls
    );
}

/// #11 (follow-up): the depth gate must also fire for declarations
/// nested inside a `function_expression` body, not just inside another
/// `function_declaration`. Pre-follow-up, the visitor only bumped
/// `function_depth` around `function_declaration` descents, so
/// `const outer = function() { function inner(){} };` left
/// `function_depth == 0` when reaching `inner` and `inner` leaked
/// into the workspace symbol index.
#[test]
fn nested_function_in_function_expression_not_indexed() {
    let tmp = tempfile::tempdir().unwrap();
    let a = "const outer = function() { function inner() {} };
";
    let b = "inner();
";
    let res = run(tmp.path(), &[("a.js", a), ("b.js", b)]);
    let all_inner_hits: Vec<_> = res
        .iter()
        .filter(|r| r.target_qualified.as_deref() == Some("inner"))
        .collect();
    assert!(
        all_inner_hits.is_empty(),
        "`inner` declared inside a function_expression body must not be a workspace-addressable target; got {:#?}",
        all_inner_hits
    );
}

/// #11 (follow-up): same as above for `arrow_function` bodies.
/// `const outer = () => { function inner(){} };` must not leak
/// `inner`.
#[test]
fn nested_function_in_arrow_function_not_indexed() {
    let tmp = tempfile::tempdir().unwrap();
    let a = "const outer = () => { function inner() {} };
";
    let b = "inner();
";
    let res = run(tmp.path(), &[("a.js", a), ("b.js", b)]);
    let all_inner_hits: Vec<_> = res
        .iter()
        .filter(|r| r.target_qualified.as_deref() == Some("inner"))
        .collect();
    assert!(
        all_inner_hits.is_empty(),
        "`inner` declared inside an arrow_function body must not be a workspace-addressable target; got {:#?}",
        all_inner_hits
    );
}

/// #11 (follow-up): pin the negative — bumping depth on the
/// expression-shaped scopes must not over-suppress real top-level
/// `function_declaration`s. A bare `function exported() {}` at module
/// scope is still addressable from another file.
#[test]
fn top_level_function_declaration_still_indexed() {
    let tmp = tempfile::tempdir().unwrap();
    // Mirror the sanity shape of
    // `nested_function_declaration_not_indexed_as_workspace_symbol`:
    // declaration + call in the same file. If depth-bumping over
    // expression-shaped function scopes accidentally over-suppressed
    // real top-level declarations, the call here would fail to resolve.
    let a = "function exported() {}\nexported();\n";
    let res = run(tmp.path(), &[("a.js", a)]);
    let calls = calls_of(&res, "a.js");
    assert!(
        calls
            .iter()
            .any(|r| r.target_qualified.as_deref() == Some("exported")),
        "top-level `exported()` must remain workspace-addressable; got {:#?}",
        calls
    );
}

/// #12: `ResolvedBinding` / `AliasTarget` now carry `import_kind`,
/// so dispatch can distinguish `Foo.bar()` on a default import (a
/// runtime property of the imported value — not pinnable at
/// Tier-2.5) from `Foo.bar()` on a namespace import (a named export
/// — pinnable). Pre-fix, both shapes routed through the
/// namespace-export lookup, so a default-imported `Foo` calling
/// `Foo.bar()` would silently re-bind to any same-named export of
/// the target module.
#[test]
fn default_import_dotted_access_resolves_to_property_of_default_export() {
    let tmp = tempfile::tempdir().unwrap();
    // The bug shape: `Mod.Inner.method()` where `Mod` is a default
    // import. Pre-fix, dispatch's 2-part dotted path treated
    // `Mod.Inner` as the namespace-style "named export Inner of
    // ./foo" lookup regardless of import kind, so a default-
    // imported `Mod` would silently pin `Mod.Inner.method()` to a
    // sibling named export `Inner` (here: class `Inner` exported
    // from ./foo) — wrong: `Mod` is the default-exported class, so
    // `Mod.Inner` is a runtime property access we can't statically
    // pin at Tier-2.5.
    let foo = "export default class Mod {}\nexport class Inner { method() {} }\n";
    let main = "import Mod from './foo';\nMod.Inner.method();\n";
    let res = run(tmp.path(), &[("foo.js", foo), ("main.js", main)]);
    let calls = calls_of(&res, "main.js");
    let leaked = calls.iter().find(|c| {
        c.target_path.as_deref() == Some("foo.js")
            && c.target_qualified.as_deref() == Some("Inner.method")
    });
    assert!(
        leaked.is_none(),
        "default-imported Mod.Inner.method() must not silently route to foo.js::Inner.method (the pre-fix namespace-shape pin); got {:#?}",
        calls
    );

    // The namespace import shape *should* still resolve, to pin
    // the fix the right way around.
    let ns_main = "import * as Mod from './foo';\nMod.Inner.method();\n";
    let res_ns = run(tmp.path(), &[("foo.js", foo), ("ns.js", ns_main)]);
    let ns_calls = calls_of(&res_ns, "ns.js");
    assert!(
        ns_calls
            .iter()
            .any(|c| c.target_path.as_deref() == Some("foo.js")
                && c.target_qualified.as_deref() == Some("Inner.method")),
        "namespace-imported Mod.Inner.method() must still resolve to foo.js::Inner.method; got {:#?}",
        ns_calls
    );
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

// ─── ESM re-export chain (CodeRabbit #7) ──────────────────────────────
//
// Coverage for the fix that makes `PackageIndex` ingest `facts.reexports`
// and `RequireGraph` rewrite a binding's `target_path` through the chain
// so `import { X } from './barrel'` lands on the file that *defines* X,
// not the barrel that forwards it.

#[test]
fn esm_reexport_named_chains_through_barrel() {
    // `import { X } from './barrel'` where `barrel.js` re-exports `X`
    // from `./x` must produce a Type resolution for `extends X` that
    // points at `x.js` (the origin), not `barrel.js`. Before the #7
    // fix, the binding's target_path stopped at `barrel.js` and the
    // Type row inherited that wrong file.
    let tmp = tempfile::tempdir().unwrap();
    let x = "export class X {}\n";
    let barrel = "export { X } from './x';\n";
    let main = "import { X } from './barrel';\nclass Sub extends X {}\n";
    let res = run(
        tmp.path(),
        &[("x.js", x), ("barrel.js", barrel), ("main.js", main)],
    );
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("x.js")
                && t.target_qualified.as_deref() == Some("X")),
        "Sub extends X (via re-export through barrel) should resolve to \
         x.js / X, not barrel.js; got {:#?}",
        types
    );
}

#[test]
fn esm_reexport_named_import_row_targets_origin() {
    // Documented choice: the IMPORT row's `target_path` stays at the
    // barrel (`barrel.js`) because the row is tied to the import-site
    // byte range (`from './barrel'`), and the site itself resolves to
    // the barrel file. The per-symbol re-export walk is a *binding*
    // concept and surfaces in the Type / Call rows (see
    // `esm_reexport_named_chains_through_barrel`). This test pins that
    // contract: import row at the barrel, type row at the origin.
    let tmp = tempfile::tempdir().unwrap();
    let x = "export class X {}\n";
    let barrel = "export { X } from './x';\n";
    let main = "import { X } from './barrel';\nclass Sub extends X {}\n";
    let res = run(
        tmp.path(),
        &[("x.js", x), ("barrel.js", barrel), ("main.js", main)],
    );
    let imps = imports_of(&res, "main.js");
    assert!(
        imps.iter()
            .any(|r| r.target_path.as_deref() == Some("barrel.js")),
        "import row should still target the barrel (site-level \
         resolution); got {:#?}",
        imps
    );
    // And the per-symbol type row must follow through.
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("x.js")),
        "type row should follow the re-export chain to x.js; got {:#?}",
        types
    );
}

#[test]
fn esm_reexport_default_renamed() {
    // `export { default as Y } from './x'` exposes `x`'s default
    // export under the new name `Y`. A consumer's `import { Y } from
    // './barrel'` should land on `x.js`. The `target_qualified` is
    // expected to be the *local* name in the origin file — for
    // `export default class X {}` the local name is `X` (the class
    // declaration's identifier), so the type row reads `X`, not `Y`
    // and not `default`. This matches the existing `esm_default_*`
    // behaviour where the consumer's local alias is invisible to the
    // resolution layer.
    let tmp = tempfile::tempdir().unwrap();
    let x = "export default class X {}\n";
    let barrel = "export { default as Y } from './x';\n";
    let main = "import { Y } from './barrel';\nclass Sub extends Y {}\n";
    let res = run(
        tmp.path(),
        &[("x.js", x), ("barrel.js", barrel), ("main.js", main)],
    );
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("x.js")
                && t.target_qualified.as_deref() == Some("X")),
        "Sub extends Y (default re-export renamed) should resolve to \
         x.js / X; got {:#?}",
        types
    );
}

#[test]
fn esm_reexport_chain_two_hops() {
    // a.js → re-exports from b.js → re-exports from c.js (origin).
    // The flattener in `PackageIndex::build` follows all hops up to
    // `MAX_REEXPORT_HOPS`, so a consumer's `import { X } from './a'`
    // should land on `c.js` (the origin), not `b.js` (the intermediate
    // barrel).
    let tmp = tempfile::tempdir().unwrap();
    let c = "export class X {}\n";
    let b = "export { X } from './c';\n";
    let a = "export { X } from './b';\n";
    let main = "import { X } from './a';\nclass Sub extends X {}\n";
    let res = run(
        tmp.path(),
        &[("c.js", c), ("b.js", b), ("a.js", a), ("main.js", main)],
    );
    let types = types_of(&res, "main.js");
    assert!(
        types
            .iter()
            .any(|t| t.target_path.as_deref() == Some("c.js")
                && t.target_qualified.as_deref() == Some("X")),
        "two-hop re-export chain a→b→c should resolve consumer's X to \
         c.js (origin), not the intermediate b.js; got {:#?}",
        types
    );
}

#[test]
fn esm_reexport_cycle_does_not_recurse_or_emit_binding() {
    // Cycle: a.js → b.js → a.js (mutual re-export of X with no real
    // origin). Before the cycle guard, the flattener inserted a
    // truncated entry that pointed back into the cycle and
    // `lookup_export`'s recursion blew the stack. The guard now drops
    // the entry from `reexports_by_path` and `lookup_export` returns
    // None.
    //
    // R2 follow-up (v0.7.0): the original cycle fix still left
    // `resolve_binding_target` falling through to the Tier-2 barrel-fact
    // fallback when `lookup_export` returned None, fabricating
    // `(target_path=a.js, target_qualified=X)` — a Type row pointing
    // into the cycle for a class that does not exist anywhere. The
    // `PackageIndex::is_reexport_dropped` gate now suppresses that
    // fallback whenever the file IS a re-exporter whose chain was
    // dropped.
    //
    // Hard invariants checked here:
    //   1. `run` terminates normally — reaching the assertion below
    //      proves no stack overflow / panic happened in the flattener
    //      or lookup.
    //   2. The analyzer still records an import row for main.js (the
    //      file wasn't silently dropped because of the cycle).
    //   3. NO Type row for `Sub extends X` is pinned to a cycle node:
    //      neither `target_path = a.js` nor `target_path = b.js`
    //      with `target_qualified = X` is acceptable. The R2 dogfood
    //      catch was the previous test's assertion (`Some("a.js") |
    //      None`) silently accepting the fabricated `Some("a.js")`
    //      binding; that loophole is now closed.
    let tmp = tempfile::tempdir().unwrap();
    let a = "export { X } from './b';\n";
    let b = "export { X } from './a';\n";
    let main = "import { X } from './a';\nclass Sub extends X {}\n";
    let res = run(tmp.path(), &[("a.js", a), ("b.js", b), ("main.js", main)]);
    // (1) — implicit: we got here without panicking.
    // (2) — analyzer processed main.js's import.
    let imports = imports_of(&res, "main.js");
    assert!(
        !imports.is_empty(),
        "cyclic re-export should still record main.js's import row; got {:#?}",
        res
    );
    // (3) — no fabricated binding into the cycle.
    let types = types_of(&res, "main.js");
    for t in &types {
        // Any Type row whose target lands on a cycle node IS the
        // fabricated binding — the bug R2 caught.
        let into_cycle = matches!(t.target_path.as_deref(), Some("a.js") | Some("b.js"))
            && t.target_qualified.as_deref() == Some("X");
        assert!(
            !into_cycle,
            "cyclic re-export must NOT synthesize a Type-row binding \
             pointing into the cycle (a.js / b.js with qualified=X); \
             got {:#?}",
            t
        );
    }
}

#[test]
fn esm_reexport_cycle_does_not_emit_fabricated_binding() {
    // Targeted regression for the R2 v0.7.0 dogfood catch: even though
    // the cycle flatten guard prevents stack overflow, the
    // `resolve_binding_target` fallback used to fabricate
    // `(target_path=cycle-a.js, target_qualified=X)` because
    // `lookup_export` returning None is ambiguous between "the file has
    // a local X" and "the file re-exports X but the chain was dropped".
    // `PackageIndex::is_reexport_dropped` disambiguates them; this test
    // pins the resulting external behaviour.
    //
    // Asserted shape:
    //   * The analyzer terminates.
    //   * main.js still gets an import row (the file isn't dropped).
    //   * NO Type row for `CycleSub extends X` resolves to either
    //     cycle-a.js or cycle-b.js with target_qualified=X. The only
    //     acceptable shapes are: no Type row at all, or a row with
    //     target_path=None (unresolved).
    let tmp = tempfile::tempdir().unwrap();
    let cycle_a = "export { X } from './cycle-b';\n";
    let cycle_b = "export { X } from './cycle-a';\n";
    let cycle_main = "import { X } from './cycle-a';\nclass CycleSub extends X {}\n";
    let res = run(
        tmp.path(),
        &[
            ("cycle-a.js", cycle_a),
            ("cycle-b.js", cycle_b),
            ("cycle-main.js", cycle_main),
        ],
    );
    let imports = imports_of(&res, "cycle-main.js");
    assert!(
        !imports.is_empty(),
        "cyclic re-export should still record cycle-main.js's import \
         row; got {:#?}",
        res
    );
    let types = types_of(&res, "cycle-main.js");
    // R2 rc3 catch: previously the row survived as
    //   { kind: Type, target_path: None, target_qualified: None }
    // because `resolve_dotted_type` happily rebuilt the row from the
    // alias even after `resolve_binding_target` returned (None, None).
    // Strengthened contract: NO tier25 Type row may key off the
    // cycle-dropped alias `X`. The Tier-2 syntactic backend still
    // emits the bare `extends` fact at its own confidence level.
    for t in &types {
        let into_cycle = matches!(
            t.target_path.as_deref(),
            Some("cycle-a.js") | Some("cycle-b.js")
        ) && t.target_qualified.as_deref() == Some("X");
        assert!(
            !into_cycle,
            "CycleSub extends X must NOT fabricate a binding pointing \
             into the cycle (cycle-a.js / cycle-b.js with \
             target_qualified=X); got {:#?}",
            t
        );
        // The fallthrough (None, None) row is also banned now.
        let unresolved_x = t.target_path.is_none()
            && (t.target_qualified.as_deref() == Some("X") || t.target_qualified.is_none());
        assert!(
            !unresolved_x,
            "cycle-dropped alias must produce zero tier25 Type rows \
             (no fact-shaped (None, None) row attributed to the \
             resolver). got {:#?}",
            t
        );
    }
}

#[test]
fn esm_reexport_chain_over_hop_budget_terminates() {
    // 9-hop chain a→b→c→d→e→f→g→h→i→j.js (j defines X). The chain has
    // 9 re-export edges; `MAX_REEXPORT_HOPS = 8` permits at most 8
    // iterations of the flatten loop, so it must abort *before* reaching
    // `j`. Per the cycle/over-budget policy in `PackageIndex::build`,
    // the entry is then dropped entirely (rather than inserting a
    // truncated mid-chain target that would misresolve consumers).
    // Consumers importing X from `a` therefore must *not* end up
    // pinned to `j.js` (origin) or to any intermediate barrel (a.js,
    // b.js, …, i.js). The R2 v0.7.0 follow-up additionally requires
    // that the over-budget drop NOT fall through to the Tier-2
    // barrel-fact fallback either — `a.js` syntactically re-exports
    // `X`, so fabricating `(a.js, "X")` would be wrong for the same
    // reason as the cycle case (no local `class X {}` exists at any
    // of the intermediate barrels). After the
    // `PackageIndex::is_reexport_dropped` gate, the only acceptable
    // outcomes are: no Type row at all, or a row with
    // target_path=None (unresolved).
    let tmp = tempfile::tempdir().unwrap();
    let j = "export class X {}\n";
    let i = "export { X } from './j';\n";
    let h = "export { X } from './i';\n";
    let g = "export { X } from './h';\n";
    let f = "export { X } from './g';\n";
    let e = "export { X } from './f';\n";
    let d = "export { X } from './e';\n";
    let c = "export { X } from './d';\n";
    let b = "export { X } from './c';\n";
    let a = "export { X } from './b';\n";
    let main = "import { X } from './a';\nclass Sub extends X {}\n";
    let res = run(
        tmp.path(),
        &[
            ("j.js", j),
            ("i.js", i),
            ("h.js", h),
            ("g.js", g),
            ("f.js", f),
            ("e.js", e),
            ("d.js", d),
            ("c.js", c),
            ("b.js", b),
            ("a.js", a),
            ("main.js", main),
        ],
    );
    let types = types_of(&res, "main.js");
    let intermediate: [&str; 10] = [
        "a.js", "b.js", "c.js", "d.js", "e.js", "f.js", "g.js", "h.js", "i.js", "j.js",
    ];
    for t in &types {
        // R2 rc3 strengthening: like the cycle case, an over-budget
        // chain must produce ZERO tier25 Type rows for the dropped
        // alias `X` — neither origin-pinned, intermediate-barrel-
        // pinned, NOR fact-shaped (None, None).
        if let Some(tp) = t.target_path.as_deref() {
            assert!(
                !intermediate.contains(&tp),
                "9-hop re-export chain a→…→j (over MAX_REEXPORT_HOPS=8) \
                 must not pin consumer's X to any chain file (origin or \
                 intermediate barrel). got {:#?}",
                t
            );
        }
        let unresolved_x = t.target_path.is_none()
            && (t.target_qualified.as_deref() == Some("X") || t.target_qualified.is_none());
        assert!(
            !unresolved_x,
            "over-budget alias must produce zero tier25 Type rows \
             (no fact-shaped (None, None) row attributed to the \
             resolver). got {:#?}",
            t
        );
    }
}
