//! Unit tests for the PHP Tier-2.5 resolver.

use std::collections::HashMap;

use cairn_core::workspace_analyzer::{
    AnalyzerProgress, ResolutionKind, WorkspaceFile, WorkspaceResolution,
};

use crate::analyze_files;
use crate::const_resolver::{CallReceiver, ConstIndex, parse_file};
use crate::dispatch::{MethodIndex, resolve_call};
use crate::mro::Mro;

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

fn aliases_from_file(src: &str) -> HashMap<String, String> {
    let f = parse_file(src.as_bytes()).unwrap();
    f.use_imports
        .iter()
        .map(|u| (u.alias.clone(), u.qualified.clone()))
        .collect()
}

// ─── const_resolver ───────────────────────────────────────────────────────

#[test]
fn use_import_creates_alias_for_short_name() {
    let src = "<?php\nnamespace App;\nuse Foo\\Bar\\Baz;\n";
    let f = parse_file(src.as_bytes()).unwrap();
    assert_eq!(f.use_imports.len(), 1);
    assert_eq!(f.use_imports[0].alias, "Baz");
    assert_eq!(f.use_imports[0].qualified, "Foo\\Bar\\Baz");
}

#[test]
fn use_import_with_explicit_alias() {
    let src = "<?php\nuse Foo\\Bar as B;\n";
    let f = parse_file(src.as_bytes()).unwrap();
    assert_eq!(f.use_imports[0].alias, "B");
    assert_eq!(f.use_imports[0].qualified, "Foo\\Bar");
}

#[test]
fn group_use_imports_expand_to_each_clause() {
    let src = "<?php\nuse App\\Traits\\{Timestamps, SoftDeletes as SD};\n";
    let f = parse_file(src.as_bytes()).unwrap();
    let aliases: HashMap<_, _> = f
        .use_imports
        .iter()
        .map(|u| (u.alias.clone(), u.qualified.clone()))
        .collect();
    assert_eq!(
        aliases.get("Timestamps").map(String::as_str),
        Some("App\\Traits\\Timestamps"),
    );
    assert_eq!(
        aliases.get("SD").map(String::as_str),
        Some("App\\Traits\\SoftDeletes"),
    );
}

#[test]
fn fqn_lookup_resolves_absolute_name() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "<?php\nnamespace App\\Models;\nclass Widget {}\n";
    let main = "<?php\nnew \\App\\Models\\Widget();\n";
    let res = run(tmp.path(), &[("Widget.php", widget), ("main.php", main)]);
    let pos = main.find("\\App\\Models\\Widget").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "main.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
    });
    assert!(
        r.is_some(),
        "\\App\\Models\\Widget should resolve; got {:#?}",
        res
    );
    assert_eq!(
        r.unwrap().target_qualified.as_deref(),
        Some("App\\Models\\Widget"),
    );
}

#[test]
fn namespaced_lookup_finds_class_in_same_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "<?php\nnamespace App\\Models;\nclass Widget {}\n";
    let main =
        "<?php\nnamespace App\\Models;\nclass Caller { public function go() { new Widget(); } }\n";
    let res = run(tmp.path(), &[("Widget.php", widget), ("Caller.php", main)]);
    let pos = main.rfind("Widget").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "Caller.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
    });
    assert!(
        r.is_some(),
        "Widget in same ns should resolve; got {:#?}",
        res
    );
    assert_eq!(
        r.unwrap().target_qualified.as_deref(),
        Some("App\\Models\\Widget"),
    );
}

#[test]
fn use_alias_resolves_short_name_to_imported_target() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "<?php\nnamespace App\\Models;\nclass Widget {}\n";
    let main = "<?php\nnamespace Other;\nuse App\\Models\\Widget;\nnew Widget();\n";
    let res = run(tmp.path(), &[("Widget.php", widget), ("main.php", main)]);
    let pos = main.rfind("Widget").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "main.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
    });
    assert!(r.is_some(), "Widget via use should resolve; got {:#?}", res);
    assert_eq!(
        r.unwrap().target_qualified.as_deref(),
        Some("App\\Models\\Widget"),
    );
}

#[test]
fn use_alias_with_renaming() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "<?php\nnamespace App\\Models;\nclass Widget {}\n";
    let main = "<?php\nuse App\\Models\\Widget as W;\nnew W();\n";
    let res = run(tmp.path(), &[("Widget.php", widget), ("main.php", main)]);
    let pos = main.rfind("W()").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "main.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
    });
    assert!(r.is_some(), "W (aliased) should resolve");
    assert_eq!(
        r.unwrap().target_qualified.as_deref(),
        Some("App\\Models\\Widget"),
    );
}

#[test]
fn const_index_resolves_with_aliases() {
    let src1 = "<?php\nnamespace A\\B;\nclass Thing {}\n";
    let facts1 = parse_file(src1.as_bytes()).unwrap();
    let per_file = vec![("A.php".to_string(), src1.as_bytes().to_vec(), facts1)];
    let ci = ConstIndex::build(&per_file);
    let mut aliases = HashMap::new();
    aliases.insert("Thing".to_string(), "A\\B\\Thing".to_string());
    let r = ci.resolve(&["Thing".to_string()], false, None, &aliases);
    assert!(r.is_some());
    assert_eq!(r.unwrap().qualified, "A\\B\\Thing");
}

// ─── MRO ──────────────────────────────────────────────────────────────────

#[test]
fn mro_extends_chain_in_order() {
    let src = "<?php\nclass A {}\nclass B extends A {}\nclass C extends B {}\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts)];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let chain = mro.ancestors("C");
    assert_eq!(chain[0], "C");
    let b = chain.iter().position(|x| x == "B").unwrap();
    let a = chain.iter().position(|x| x == "A").unwrap();
    assert!(b < a, "B should precede A: {chain:?}");
}

#[test]
fn mro_implements_appears_in_chain() {
    let src = "<?php\ninterface I {}\nclass C implements I {}\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts)];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let chain = mro.ancestors("C");
    assert!(chain.contains(&"I".to_string()), "chain: {chain:?}");
}

#[test]
fn mro_trait_use_appears_before_parent() {
    let src = "<?php\ntrait T {}\nclass Base {}\nclass C extends Base { use T; }\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts)];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let chain = mro.ancestors("C");
    let t = chain.iter().position(|x| x == "T").unwrap();
    let base = chain.iter().position(|x| x == "Base").unwrap();
    assert!(t < base, "T should precede Base: {chain:?}");
}

#[test]
fn mro_resolves_namespaced_parent_via_use_alias() {
    let src = "<?php\nnamespace App;\nuse Lib\\Animal;\nclass Dog extends Animal {}\n";
    let lib = "<?php\nnamespace Lib;\nclass Animal {}\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let lib_facts = parse_file(lib.as_bytes()).unwrap();
    let per_file = vec![
        ("dog.php".to_string(), src.as_bytes().to_vec(), facts),
        ("animal.php".to_string(), lib.as_bytes().to_vec(), lib_facts),
    ];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let chain = mro.ancestors("App\\Dog");
    assert!(
        chain.iter().any(|c| c == "Lib\\Animal"),
        "should resolve via use alias: {chain:?}",
    );
}

// ─── dispatch ─────────────────────────────────────────────────────────────

#[test]
fn dispatch_static_call_via_class_name() {
    let src = "<?php\nclass Foo { public static function bar() {} }\nFoo::bar();\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| c.method == "bar")
        .expect("expected bar call");
    let aliases = HashMap::new();
    let r = resolve_call(call, &ci, &mro, &mi, &aliases).expect("Foo::bar should resolve");
    assert_eq!(r.qualified, "Foo::bar");
    assert_eq!(r.path, "a.php");
}

#[test]
fn dispatch_self_call_resolves_in_current_class() {
    let src = "<?php\nclass Foo {\n  public static function bar() {}\n  public function go() { self::bar(); }\n}\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| c.method == "bar" && matches!(c.receiver, CallReceiver::SelfClass))
        .expect("expected self::bar call");
    let aliases = HashMap::new();
    let r = resolve_call(call, &ci, &mro, &mi, &aliases).expect("self::bar should resolve");
    assert_eq!(r.qualified, "Foo::bar");
}

#[test]
fn dispatch_parent_call_resolves_to_parent_class() {
    let src = "<?php\nclass Base { public function step() {} }\nclass Child extends Base { public function go() { parent::step(); } }\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| matches!(c.receiver, CallReceiver::Parent))
        .expect("expected parent::step call");
    let aliases = HashMap::new();
    let r = resolve_call(call, &ci, &mro, &mi, &aliases).expect("parent::step should resolve");
    assert_eq!(r.qualified, "Base::step");
}

#[test]
fn dispatch_static_late_binding_resolves_to_lexical_class() {
    let src = "<?php\nclass Foo {\n  public static function make() { return static::build(); }\n  public static function build() {}\n}\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| matches!(c.receiver, CallReceiver::StaticClass))
        .expect("expected static::build call");
    let aliases = HashMap::new();
    let r = resolve_call(call, &ci, &mro, &mi, &aliases).expect("static::build should resolve");
    assert_eq!(r.qualified, "Foo::build");
}

#[test]
fn dispatch_fqn_static_call() {
    let src = "<?php\nnamespace App;\nclass Foo { public static function bar() {} }\nnamespace Other;\n\\App\\Foo::bar();\n";
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.php".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| c.method == "bar")
        .expect("expected bar call");
    let aliases =
        aliases_from_file("<?php\nnamespace App;\nclass Foo { public static function bar() {} }\n");
    let r = resolve_call(call, &ci, &mro, &mi, &aliases).expect("FQN static call should resolve");
    assert_eq!(r.qualified, "App\\Foo::bar");
}

// ─── require_graph ────────────────────────────────────────────────────────

// Phase 1 contract: persisted Import resolutions carry `target_path`
// only — `target_qualified` is always `None` (matches Ruby /
// JavaScript). The qualified name still lives on the require_graph
// internally for binding lookup; we just don't leak it into the row,
// because persist.rs path-scoped lookup would otherwise spuriously
// pin a workspace symbol_id to the import edge.
#[test]
fn use_import_emits_resolution_for_workspace_class() {
    let tmp = tempfile::tempdir().unwrap();
    let widget = "<?php\nnamespace App\\Models;\nclass Widget {}\n";
    let main = "<?php\nuse App\\Models\\Widget;\n";
    let res = run(tmp.path(), &[("Widget.php", widget), ("main.php", main)]);
    let r = res
        .iter()
        .find(|r| r.source_path == "main.php" && r.kind == ResolutionKind::Import);
    assert!(
        r.is_some(),
        "use import should emit a resolution; got {:#?}",
        res,
    );
    let r = r.unwrap();
    assert_eq!(r.target_path.as_deref(), Some("Widget.php"));
    assert!(
        r.target_qualified.is_none(),
        "Import row must not carry target_qualified; got {:?}",
        r.target_qualified
    );
}

#[test]
fn group_use_import_emits_resolution_per_clause() {
    let tmp = tempfile::tempdir().unwrap();
    let a = "<?php\nnamespace App\\Traits;\ntrait Timestamps {}\ntrait SoftDeletes {}\n";
    let main = "<?php\nuse App\\Traits\\{Timestamps, SoftDeletes};\n";
    let res = run(tmp.path(), &[("Traits.php", a), ("main.php", main)]);
    let imports: Vec<_> = res
        .iter()
        .filter(|r| r.source_path == "main.php" && r.kind == ResolutionKind::Import)
        .collect();
    assert_eq!(imports.len(), 2, "got {:#?}", imports);
    for r in &imports {
        assert_eq!(r.target_path.as_deref(), Some("Traits.php"));
        assert!(
            r.target_qualified.is_none(),
            "Import row must not carry target_qualified; got {:?}",
            r.target_qualified
        );
    }
}

#[test]
fn import_target_qualified_is_none_even_when_require_graph_resolved() {
    // Regression for CodeRabbit PR #231 finding C-2 (php): even when
    // the require_graph internally resolves a qualified target (here
    // "App\\Models\\Widget"), the persisted Import
    // WorkspaceResolution must carry `target_qualified = None`.
    // Otherwise persist.rs path-scoped `(blob_sha, parser_id,
    // qualified)` lookup would spuriously pin a workspace symbol_id
    // to the import edge.
    let tmp = tempfile::tempdir().unwrap();
    let widget = "<?php\nnamespace App\\Models;\nclass Widget {}\n";
    let main = "<?php\nuse App\\Models\\Widget;\n";
    let res = run(tmp.path(), &[("Widget.php", widget), ("main.php", main)]);
    let imps: Vec<_> = res
        .iter()
        .filter(|r| r.source_path == "main.php" && r.kind == ResolutionKind::Import)
        .collect();
    assert!(!imps.is_empty(), "expected at least one import row");
    for r in &imps {
        assert!(
            r.target_qualified.is_none(),
            "Import row must have target_qualified=None even when binding resolved internally; got {:?}",
            r
        );
    }
}

#[test]
fn extends_clause_emits_type_resolution_at_base_name_range() {
    // The Tier-2 backend stores the heritage name's byte range in
    // `implementations.interface_byte_start/end`. The find_subtypes /
    // find_supertypes queries LEFT JOIN that against the resolutions
    // table on the same `(site_byte_start, site_byte_end)`, so the
    // resolver MUST emit a Type resolution at exactly that span for the
    // result row's `kind_source` to flip from `tier2-direct-php` to
    // `tier25-php-resolver`. This test pins that contract.
    let tmp = tempfile::tempdir().unwrap();
    let base = "<?php\nnamespace App;\nclass Animal {}\n";
    let dog = "<?php\nnamespace App;\nclass Dog extends Animal {}\n";
    let res = run(tmp.path(), &[("Animal.php", base), ("Dog.php", dog)]);
    let pos = dog.find("Animal {}").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "Dog.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
            && r.site_byte_range.end == pos + "Animal".len() as u32
    });
    assert!(
        r.is_some(),
        "extends Animal should emit a Type resolution at the base name range; got {:#?}",
        res
    );
    assert_eq!(r.unwrap().target_qualified.as_deref(), Some("App\\Animal"),);
}

#[test]
fn implements_clause_emits_type_resolution_per_interface() {
    let tmp = tempfile::tempdir().unwrap();
    let i = "<?php\nnamespace App;\ninterface Walker {}\ninterface Runner {}\n";
    let dog = "<?php\nnamespace App;\nclass Dog implements Walker, Runner {}\n";
    let res = run(tmp.path(), &[("Iface.php", i), ("Dog.php", dog)]);
    let walker_pos = dog.find("Walker").unwrap() as u32;
    let runner_pos = dog.find("Runner").unwrap() as u32;
    assert!(res.iter().any(|r| {
        r.source_path == "Dog.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == walker_pos
            && r.target_qualified.as_deref() == Some("App\\Walker")
    }));
    assert!(res.iter().any(|r| {
        r.source_path == "Dog.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == runner_pos
            && r.target_qualified.as_deref() == Some("App\\Runner")
    }));
}

#[test]
fn trait_use_in_class_body_emits_type_resolution_per_trait() {
    let tmp = tempfile::tempdir().unwrap();
    let t = "<?php\nnamespace App;\ntrait Timestamps {}\n";
    let c = "<?php\nnamespace App;\nclass Widget { use Timestamps; }\n";
    let res = run(tmp.path(), &[("Trait.php", t), ("Widget.php", c)]);
    let pos = c.find("Timestamps;").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "Widget.php"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
    });
    assert!(
        r.is_some(),
        "use Timestamps should emit a Type resolution; got {:#?}",
        res
    );
    assert_eq!(
        r.unwrap().target_qualified.as_deref(),
        Some("App\\Timestamps"),
    );
}

// ─── 諦め: things Tier-2.5 must NOT resolve ─────────────────────────────

#[test]
fn member_call_on_dynamic_receiver_is_not_emitted() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "<?php\n$obj->doThing();\n";
    let res = run(tmp.path(), &[("a.php", src)]);
    assert!(
        res.iter().all(|r| r.kind != ResolutionKind::Call),
        "\\$obj->method must produce zero Call resolutions; got {:#?}",
        res
    );
}

#[test]
fn call_user_func_is_not_emitted_as_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "<?php\nclass Foo { public function go() {} }\ncall_user_func([new Foo(), 'go']);\n";
    let res = run(tmp.path(), &[("a.php", src)]);
    let dispatched = res.iter().any(|r| {
        r.kind == ResolutionKind::Call && r.target_qualified.as_deref() == Some("Foo::go")
    });
    assert!(!dispatched, "call_user_func must not resolve dispatch");
}

#[test]
fn variable_method_name_static_call_is_not_emitted() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "<?php\nclass Foo { public static function bar() {} }\n$m = 'bar';\nFoo::$m();\n";
    let res = run(tmp.path(), &[("a.php", src)]);
    let dispatched = res.iter().any(|r| {
        r.kind == ResolutionKind::Call && r.target_qualified.as_deref() == Some("Foo::bar")
    });
    assert!(
        !dispatched,
        "Foo::\\$m() must not resolve to Foo::bar; got {:#?}",
        res
    );
}

// ─── glue ─────────────────────────────────────────────────────────────────

#[test]
fn analyzer_returns_facts_with_resolutions_field() {
    let tmp = tempfile::tempdir().unwrap();
    let res = run(tmp.path(), &[]);
    assert!(res.is_empty());
}
