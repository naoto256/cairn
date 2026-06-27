//! Unit tests for the Ruby Tier-2.5 resolver.

use std::path::PathBuf;

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

fn find_type_at(
    resolutions: &[WorkspaceResolution],
    path: &str,
    token: &str,
    source: &str,
) -> Option<WorkspaceResolution> {
    let byte_start = source.find(token)? as u32;
    let byte_end = byte_start + token.len() as u32;
    resolutions
        .iter()
        .find(|r| {
            r.source_path == path
                && r.kind == ResolutionKind::Type
                && r.site_byte_range.start == byte_start
                && r.site_byte_range.end == byte_end
        })
        .cloned()
}

// ─── const_resolver ───────────────────────────────────────────────────────

#[test]
fn lexical_lookup_finds_sibling_class_in_same_module() {
    let tmp = tempfile::tempdir().unwrap();
    let main = r#"
module App
  class Service
    def call; end
  end

  class Main
    def go
      Service.new
    end
  end
end
"#;
    let res = run(tmp.path(), &[("main.rb", main)]);
    let hit = find_type_at(&res, "main.rb", "Service.new", main);
    // Service should be at the call site for `Service.new`. The TYPE site is
    // the standalone `Service`, not the dotted call — look for first
    // occurrence of `Service` inside `Service.new`.
    let token_pos = main.find("Service.new").unwrap() as u32;
    let res_for_service = res.iter().find(|r| {
        r.source_path == "main.rb"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == token_pos
    });
    assert!(
        res_for_service.is_some(),
        "Service should resolve via lexical scope; got {:#?}, hit={:?}",
        res,
        hit
    );
    let r = res_for_service.unwrap();
    assert_eq!(r.target_path.as_deref(), Some("main.rb"));
    assert_eq!(r.target_qualified.as_deref(), Some("App::Service"));
}

#[test]
fn ancestor_lookup_resolves_constant_from_included_module() {
    let tmp = tempfile::tempdir().unwrap();
    let mixin = r#"
module Helpers
  Constant = 1
  class Util
    def call; end
  end
end
"#;
    let main = r#"
require_relative "helpers"
class App
  include Helpers
  def go
    Util.new
  end
end
"#;
    let res = run(tmp.path(), &[("helpers.rb", mixin), ("main.rb", main)]);
    let util_pos = main.find("Util.new").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "main.rb"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == util_pos
    });
    assert!(
        r.is_some(),
        "Util should resolve via include ancestor; got {:#?}",
        res
    );
    assert_eq!(
        r.unwrap().target_qualified.as_deref(),
        Some("Helpers::Util")
    );
}

#[test]
fn autoload_lookup_resolves_constant() {
    let tmp = tempfile::tempdir().unwrap();
    let main = r#"
autoload :Foo, "foo"
Foo.new
"#;
    let target = "class Foo; end\n";
    let res = run(tmp.path(), &[("main.rb", main), ("foo.rb", target)]);
    let foo_pos = main.rfind("Foo.new").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "main.rb"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == foo_pos
    });
    assert!(
        r.is_some(),
        "Foo should resolve via autoload; got {:#?}",
        res
    );
    assert_eq!(r.unwrap().target_path.as_deref(), Some("foo.rb"));
    assert_eq!(r.unwrap().target_qualified.as_deref(), Some("Foo"));
}

#[test]
fn qualified_lookup_resolves_two_part_constant() {
    let tmp = tempfile::tempdir().unwrap();
    let lib = r#"
module App
  class Service; end
end
"#;
    let main = r#"
App::Service.new
"#;
    let res = run(tmp.path(), &[("lib.rb", lib), ("main.rb", main)]);
    let pos = main.find("App::Service").unwrap() as u32;
    let r = res.iter().find(|r| {
        r.source_path == "main.rb"
            && r.kind == ResolutionKind::Type
            && r.site_byte_range.start == pos
    });
    assert!(
        r.is_some(),
        "App::Service should resolve qualified; got {:#?}",
        res
    );
    assert_eq!(r.unwrap().target_qualified.as_deref(), Some("App::Service"));
}

// ─── MRO ──────────────────────────────────────────────────────────────────

#[test]
fn mro_include_chain_puts_includee_after_self() {
    let src = r#"
module M; end
class C
  include M
end
"#;
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.rb".to_string(), src.as_bytes().to_vec(), facts)];
    let const_index = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &const_index);
    let chain = mro.ancestors("C");
    let c_idx = chain.iter().position(|x| x == "C").unwrap();
    let m_idx = chain.iter().position(|x| x == "M").unwrap();
    assert!(c_idx < m_idx, "C should appear before M: {chain:?}");
}

#[test]
fn mro_prepend_puts_prepended_before_self() {
    let src = r#"
module Logger; end
class Worker
  prepend Logger
end
"#;
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.rb".to_string(), src.as_bytes().to_vec(), facts)];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let chain = mro.ancestors("Worker");
    let w_idx = chain.iter().position(|x| x == "Worker").unwrap();
    let l_idx = chain.iter().position(|x| x == "Logger").unwrap();
    assert!(
        l_idx < w_idx,
        "Logger should appear before Worker: {chain:?}"
    );
}

#[test]
fn mro_extend_appears_on_singleton_chain_only() {
    let src = r#"
module Counter; end
class Box
  extend Counter
end
"#;
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.rb".to_string(), src.as_bytes().to_vec(), facts)];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let instance = mro.ancestors("Box");
    let singleton = mro.singleton_ancestors("Box");
    assert!(
        !instance.contains(&"Counter".to_string()),
        "extend must NOT enter instance chain: {instance:?}"
    );
    assert!(
        singleton.contains(&"Counter".to_string()),
        "extend must enter singleton chain: {singleton:?}"
    );
}

// ─── dispatch ─────────────────────────────────────────────────────────────

#[test]
fn dispatch_const_receiver_resolves_singleton_method() {
    let src = r#"
class Foo
  def self.bar; end
end
Foo.bar
"#;
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.rb".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| c.method == "bar")
        .expect("expected `bar` call site");
    let r = resolve_call(call, &ci, &mro, &mi).expect("Foo.bar should resolve");
    assert_eq!(r.qualified, "Foo.bar");
    assert_eq!(r.path, "a.rb");
}

#[test]
fn dispatch_self_receiver_resolves_instance_method() {
    let src = r#"
class Foo
  def helper; end
  def go
    self.helper
  end
end
"#;
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.rb".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| c.method == "helper")
        .expect("expected `helper` call site");
    assert_eq!(call.receiver, CallReceiver::Self_);
    let r = resolve_call(call, &ci, &mro, &mi).expect("self.helper should resolve");
    assert_eq!(r.qualified, "Foo#helper");
}

#[test]
fn dispatch_super_resolves_to_parent_method() {
    let src = r#"
class Base
  def step; end
end
class Child < Base
  def step
    super
  end
end
"#;
    let facts = parse_file(src.as_bytes()).unwrap();
    let per_file = vec![("a.rb".to_string(), src.as_bytes().to_vec(), facts.clone())];
    let ci = ConstIndex::build(&per_file);
    let mro = Mro::build(&per_file, &ci);
    let mi = MethodIndex::build(&per_file);
    let call = facts
        .method_calls
        .iter()
        .find(|c| matches!(c.receiver, CallReceiver::Super))
        .expect("expected super call site");
    let r = resolve_call(call, &ci, &mro, &mi).expect("super should resolve to Base#step");
    assert_eq!(r.qualified, "Base#step");
}

// ─── require_graph ────────────────────────────────────────────────────────

#[test]
fn require_relative_resolves_workspace_file() {
    let tmp = tempfile::tempdir().unwrap();
    let main = "require_relative \"lib/util\"\n";
    let util = "class Util; end\n";
    let res = run(tmp.path(), &[("main.rb", main), ("lib/util.rb", util)]);
    let r = res
        .iter()
        .find(|r| r.source_path == "main.rb" && r.kind == ResolutionKind::Import);
    assert!(
        r.is_some(),
        "require_relative should emit an import; got {:#?}",
        res
    );
    assert_eq!(r.unwrap().target_path.as_deref(), Some("lib/util.rb"));
}

#[test]
fn autoload_emits_workspace_resolution_for_constant_use() {
    let tmp = tempfile::tempdir().unwrap();
    let main = r#"
autoload :Foo, "foo"
Foo.new
"#;
    let target = "class Foo; end\n";
    let res = run(tmp.path(), &[("main.rb", main), ("foo.rb", target)]);
    // The autoload itself is recorded by const_index, and the const ref
    // gets a resolved type row pointing at foo.rb.
    let r = res.iter().find(|r| {
        r.source_path == "main.rb"
            && r.kind == ResolutionKind::Type
            && r.target_path.as_deref() == Some("foo.rb")
    });
    assert!(
        r.is_some(),
        "Foo via autoload should resolve; got {:#?}",
        res
    );
}

// ─── 諦め: things Tier-2.5 must NOT resolve ─────────────────────────────

#[test]
fn unknown_receiver_method_call_is_not_emitted() {
    let tmp = tempfile::tempdir().unwrap();
    let src = "obj.do_thing\n";
    let res = run(tmp.path(), &[("a.rb", src)]);
    assert!(
        res.iter().all(|r| r.kind != ResolutionKind::Call),
        "obj.method must produce zero Call resolutions; got {:#?}",
        res
    );
}

#[test]
fn define_method_is_not_emitted() {
    let tmp = tempfile::tempdir().unwrap();
    let src = r#"
class Foo
  define_method(:bar) { puts "x" }
end
Foo.new.bar
"#;
    let res = run(tmp.path(), &[("a.rb", src)]);
    // define_method as a bare-receiver call with no def — bar is not a real
    // method def, so dispatch must NOT resolve `Foo.new.bar` (and the
    // receiver `Foo.new` itself isn't a const, so it's Unknown).
    let bar_call_resolved = res.iter().any(|r| {
        r.kind == ResolutionKind::Call && r.target_qualified.as_deref() == Some("Foo#bar")
    });
    assert!(!bar_call_resolved, "define_method'd `bar` must not resolve");
}

#[test]
fn send_call_is_not_emitted() {
    let tmp = tempfile::tempdir().unwrap();
    let src = r#"
class Foo
  def go; end
end
Foo.new.send(:go)
"#;
    let res = run(tmp.path(), &[("a.rb", src)]);
    // `Foo.new.send(:go)` — send is on an unknown receiver; even if we
    // could pin `send`, Tier-2.5 explicitly refuses dynamic dispatch.
    let send_resolved = res
        .iter()
        .any(|r| r.kind == ResolutionKind::Call && r.target_qualified.as_deref() == Some("Foo#go"));
    assert!(
        !send_resolved,
        "send-based dispatch must not resolve to Foo#go"
    );
}

// ─── glue: ensure analyzer registers and round-trips empty inputs ─────────

#[test]
fn analyzer_returns_facts_with_resolutions_field() {
    let tmp = tempfile::tempdir().unwrap();
    let _ = PathBuf::from(tmp.path());
    let res = run(tmp.path(), &[]);
    assert!(res.is_empty());
}
