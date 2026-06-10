//! Ruby Tier-2 analyzer (semantic enrichment over the same
//! tree-sitter parse the syntactic pass uses).
//!
//! Mirrors the Python backend's Tier-2 shape: structural extraction,
//! not type resolution (receiver types are ruby-lsp / Tier-3
//! territory). The statically-faithful facts for Ruby are:
//!
//! - **inheritance edges** — `class Dog < Animal` becomes an
//!   [`ImplFact`] with `kind = "inherit"`, so
//!   `find_impls trait=Animal` answers "what subclasses Animal".
//! - **mixin edges** — `include M` / `extend M` / `prepend M` inside a
//!   class or module body become [`ImplFact`]s with `kind` set to the
//!   mixin verb. These are Ruby's interface-implementation analog.
//! - **refs** — call sites (`foo()`, `obj.render` → [`RefKind::Call`]),
//!   name-level only: a method call's receiver type is unknown without
//!   Tier-3, so `target_qualified` stays `None`. Paren-less zero-arg
//!   calls are indistinguishable from local-variable reads in the
//!   grammar (both parse as `identifier`) and are deliberately not
//!   emitted. Dynamic dispatch (`send(:name)`, `method_missing`) is
//!   not resolved — `send` itself appears as the call target.
//!
//! `require` / `require_relative` imports are emitted by the
//! **syntactic** pass (like Go), so this analyzer leaves
//! `SemanticFacts.imports` empty rather than duplicating rows.
//!
//! Qualified names mirror the syntactic pass exactly: containers join
//! with `::`, instance methods attach with `#`, singleton methods with
//! `.` — see the crate docs. That keeps `RefFact.enclosing_qualified`
//! and `ImplFact.type_qualified` resolvable against
//! `symbols.qualified`.

use std::sync::Arc;

use cairn_lang_api::{Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts};
use cairn_lang_treesitter_generic::{child_by_field, collapse_ws, line_of, node_text};
use tree_sitter::{Node, Parser};

use crate::{method_separator, within_singleton_class};

/// Ruby semantic analyzer. Re-parses the source with tree-sitter-ruby
/// (the same grammar the syntactic pass uses) and walks for
/// inheritance / mixin edges and call refs.
pub struct RubyAnalyzer;

impl Analyzer for RubyAnalyzer {
    fn name(&self) -> &'static str {
        "ruby-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_ruby::LANGUAGE.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut facts = SemanticFacts::default();
        let mut scope_stack: Vec<String> = Vec::new();
        walk(tree.root_node(), source, &mut scope_stack, None, &mut facts);
        Ok(facts)
    }
}

/// Declaration-shaped call names the ref pass skips: they are either
/// represented as symbols / imports / impl edges already, or are
/// visibility markers rather than meaningful call targets.
const DECLARATIVE_CALLS: &[&str] = &[
    "require",
    "require_relative",
    "attr_accessor",
    "attr_reader",
    "attr_writer",
    "include",
    "extend",
    "prepend",
    "define_method",
    "private",
    "protected",
    "public",
    "module_function",
];

/// Recursive walk maintaining:
/// - `scope_stack`: enclosing class / module names (joined with `::`
///   for qualified names), mirroring the syntactic pass's
///   `NestingTracker`.
/// - `enclosing`: qualified name of the nearest enclosing method /
///   class / module, or `None` at top level. Refs attach this as
///   `enclosing_qualified`.
fn walk(
    node: Node<'_>,
    source: &[u8],
    scope_stack: &mut Vec<String>,
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    match node.kind() {
        "module" | "class" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = qualify(scope_stack, &name);
            if node.kind() == "class" {
                emit_superclass(node, source, &qualified, facts);
            }
            scope_stack.push(name);
            recurse(node, source, scope_stack, Some(&qualified), facts);
            scope_stack.pop();
            return;
        }
        "method" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source);
            let qualified = method_qualify(scope_stack, name, within_singleton_class(node));
            recurse(node, source, scope_stack, Some(&qualified), facts);
            return;
        }
        "singleton_method" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source);
            // `def self.x` qualifies under the enclosing container,
            // `def Foo.x` under the explicit constant — mirroring the
            // syntactic pass.
            let qualified = match child_by_field(node, "object") {
                Some(obj) if obj.kind() != "self" => {
                    format!("{}.{name}", node_text(obj, source))
                }
                _ => method_qualify(scope_stack, name, true),
            };
            recurse(node, source, scope_stack, Some(&qualified), facts);
            return;
        }
        "call" => {
            handle_call(node, source, scope_stack, enclosing, facts);
            // fall through to recurse into receiver / arguments / block.
        }
        _ => {}
    }
    recurse(node, source, scope_stack, enclosing, facts);
}

fn recurse(
    node: Node<'_>,
    source: &[u8],
    scope_stack: &mut Vec<String>,
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            walk(cursor.node(), source, scope_stack, enclosing, facts);
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

fn qualify(scope_stack: &[String], name: &str) -> String {
    if scope_stack.is_empty() {
        name.to_string()
    } else {
        format!("{}::{name}", scope_stack.join("::"))
    }
}

fn method_qualify(scope_stack: &[String], name: &str, singleton: bool) -> String {
    if scope_stack.is_empty() {
        name.to_string()
    } else {
        format!(
            "{}{}{name}",
            scope_stack.join("::"),
            method_separator(singleton)
        )
    }
}

/// `class Dog < Animal` — the `superclass` field wraps the expression
/// after `<`. The base text is stored as written (`Base::Animal` stays
/// dotted), matching how a user would query `find_impls trait=…`.
fn emit_superclass(
    class_node: Node<'_>,
    source: &[u8],
    type_qualified: &str,
    facts: &mut SemanticFacts,
) {
    let Some(superclass) = child_by_field(class_node, "superclass") else {
        return;
    };
    let Some(expr) = superclass.named_child(0) else {
        return;
    };
    let base = collapse_ws(node_text(expr, source));
    if base.is_empty() {
        return;
    }
    facts.impls.push(ImplFact {
        type_qualified: type_qualified.to_string(),
        interface_qualified: Some(base),
        kind: "inherit".to_string(),
        line: line_of(class_node),
    });
}

/// A `call` node. Mixin verbs become impl edges; declaration-shaped
/// names are skipped; everything else becomes a name-level `Call` ref.
fn handle_call(
    call_node: Node<'_>,
    source: &[u8],
    scope_stack: &[String],
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let Some(method) = child_by_field(call_node, "method") else {
        return;
    };
    let has_receiver = child_by_field(call_node, "receiver").is_some();
    let method_name = node_text(method, source);

    if !has_receiver {
        if matches!(method_name, "include" | "extend" | "prepend") && !scope_stack.is_empty() {
            emit_mixins(call_node, source, method_name, scope_stack, facts);
            return;
        }
        if DECLARATIVE_CALLS.contains(&method_name) {
            return;
        }
    }

    // The method name must be a plain identifier (or operator-ish
    // identifier); exotic callees (`obj.send(:x)` still has `send` as
    // the identifier) are covered, computed callees are not.
    if method.kind() != "identifier" {
        return;
    }
    if method_name.is_empty() {
        return;
    }
    facts.refs.push(RefFact {
        target_name: method_name.to_string(),
        target_qualified: None,
        kind: RefKind::Call,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: enclosing.map(str::to_string),
        byte_range: method.byte_range(),
        line: line_of(method),
    });
}

/// `include A, B` → one [`ImplFact`] per constant argument with
/// `kind = "include"` (resp. `extend` / `prepend`). Non-constant
/// arguments (`include Module.new`) are skipped.
fn emit_mixins(
    call_node: Node<'_>,
    source: &[u8],
    verb: &str,
    scope_stack: &[String],
    facts: &mut SemanticFacts,
) {
    let Some(args) = child_by_field(call_node, "arguments") else {
        return;
    };
    let type_qualified = scope_stack.join("::");
    let line = line_of(call_node);
    let mut cursor = args.walk();
    for arg in args.named_children(&mut cursor) {
        let module = match arg.kind() {
            "constant" | "scope_resolution" => collapse_ws(node_text(arg, source)),
            _ => continue,
        };
        if module.is_empty() {
            continue;
        }
        facts.impls.push(ImplFact {
            type_qualified: type_qualified.clone(),
            interface_qualified: Some(module),
            kind: verb.to_string(),
            line,
        });
    }
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(RubyAnalyzer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        RubyAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    // ─── inheritance ───────────────────────────────────────────────

    #[test]
    fn single_inheritance_edge() {
        let f = semantic("class Dog < Animal\nend\n");
        assert_eq!(f.impls.len(), 1);
        assert_eq!(f.impls[0].type_qualified, "Dog");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(f.impls[0].kind, "inherit");
    }

    #[test]
    fn scoped_superclass_kept_verbatim() {
        let f = semantic("class E < Base::Animal\nend\n");
        assert_eq!(
            f.impls[0].interface_qualified.as_deref(),
            Some("Base::Animal")
        );
    }

    #[test]
    fn no_superclass_no_edge() {
        let f = semantic("class Plain\nend\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn nested_class_qualifies_under_modules() {
        let f = semantic("module Outer\n  class Inner < Base\n  end\nend\n");
        assert_eq!(f.impls[0].type_qualified, "Outer::Inner");
    }

    // ─── mixins ────────────────────────────────────────────────────

    #[test]
    fn include_extend_prepend_edges() {
        let src = "\
class Widget
  include Loggable
  extend Helpers
  prepend Patch
end
";
        let f = semantic(src);
        let kinds: Vec<(&str, &str)> = f
            .impls
            .iter()
            .map(|i| (i.kind.as_str(), i.interface_qualified.as_deref().unwrap()))
            .collect();
        assert_eq!(
            kinds,
            &[
                ("include", "Loggable"),
                ("extend", "Helpers"),
                ("prepend", "Patch"),
            ]
        );
        assert!(f.impls.iter().all(|i| i.type_qualified == "Widget"));
    }

    #[test]
    fn include_with_multiple_and_scoped_modules() {
        let f = semantic("module M\n  include A, Deep::B\nend\n");
        let ifaces: Vec<&str> = f
            .impls
            .iter()
            .filter_map(|i| i.interface_qualified.as_deref())
            .collect();
        assert_eq!(ifaces, &["A", "Deep::B"]);
        assert!(f.impls.iter().all(|i| i.kind == "include"));
    }

    #[test]
    fn top_level_include_skipped() {
        // `include` outside a class/module body monkey-patches Object;
        // there is no type to attach the edge to.
        let f = semantic("include Helpers\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn computed_mixin_skipped() {
        let f = semantic("class C\n  include Module.new\nend\n");
        assert!(f.impls.is_empty());
    }

    // ─── refs: calls ───────────────────────────────────────────────

    #[test]
    fn top_level_call_has_no_enclosing() {
        let f = semantic("greet()\n");
        let c = calls(&f);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].target_name, "greet");
        assert_eq!(c[0].target_qualified, None);
        assert_eq!(c[0].enclosing_qualified, None);
    }

    #[test]
    fn call_inside_method_enclosed_by_qualified_method() {
        let src = "class W\n  def render\n    helper(1)\n  end\nend\n";
        let f = semantic(src);
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W#render"));
    }

    #[test]
    fn call_inside_singleton_method_uses_dot_qualifier() {
        let src = "class W\n  def self.build\n    setup(1)\n  end\nend\n";
        let f = semantic(src);
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "setup")
            .expect("setup call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.build"));
    }

    #[test]
    fn receiver_call_is_name_level_unresolved() {
        let f = semantic("def run(obj)\n  obj.render\nend\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "render")
            .expect("render call missing");
        // Receiver type unknown without Tier-3 → name-level only.
        assert_eq!(hit.target_qualified, None);
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("run"));
    }

    #[test]
    fn parenless_zero_arg_call_not_emitted() {
        // Documented best-effort limit: `helper` with no parens and no
        // args parses as a bare identifier, indistinguishable from a
        // variable read, so no ref is emitted.
        let f = semantic("def run\n  helper\nend\n");
        assert!(calls(&f).is_empty());
    }

    #[test]
    fn declarative_calls_not_emitted_as_refs() {
        let src = "\
require \"json\"

class C
  include M
  attr_reader :x
  private def hidden; end
end
";
        let f = semantic(src);
        assert!(calls(&f).is_empty());
    }

    #[test]
    fn dynamic_send_resolves_to_send_only() {
        // Documented best-effort limit: `send(:name)` is dynamic
        // dispatch; the ref targets `send`, not the symbol it invokes.
        let f = semantic("def run(obj)\n  obj.send(:render)\nend\n");
        let names: Vec<&str> = calls(&f).iter().map(|r| r.target_name.as_str()).collect();
        assert_eq!(names, &["send"]);
    }

    #[test]
    fn imports_left_to_syntactic_pass() {
        let f = semantic("require \"json\"\n");
        assert!(f.imports.is_empty());
    }
}
