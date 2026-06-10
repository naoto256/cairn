//! Java Tier-2 analyzer (semantic enrichment over the same
//! tree-sitter parse the syntactic pass uses).
//!
//! Like the Python analyzer, cairn's Java Tier-2 is *structural*
//! extraction, not type resolution (receiver types are jdtls / Tier-3
//! territory). The statically-faithful facts are:
//!
//! - **inheritance edges** — `class Dog extends Animal` and
//!   `class Dog implements Pet` are Java's analogs to Rust's
//!   `impl Trait for Type`. Emitted as [`ImplFact`]s with
//!   `kind = "extends"` / `"implements"` so `find_impls
//!   trait=Animal` answers "what subclasses / implements Animal".
//!   Interface-to-interface `extends` edges use `"extends"` too.
//!   Generic arguments are stripped from the supertype
//!   (`Comparable<Point>` → `Comparable`) to match how users query;
//!   dotted names stay verbatim (`java.lang.Runnable`).
//! - **refs** — method calls (`render()`, `obj.render()` →
//!   [`RefKind::Call`]) and instantiations (`new Widget()` →
//!   [`RefKind::Instantiate`]). Name-level only: a call's receiver
//!   type is unknown without Tier-3, so `target_qualified` stays
//!   `None`.
//!
//! Imports are already emitted by the syntactic pass (Java import
//! paths need no resolution), so [`SemanticFacts::imports`] stays
//! empty here.
//!
//! Qualified names are built from the **type** nesting only (classes,
//! interfaces, enums, records, annotation types), matching the
//! syntactic pass's `NestingTracker`, which pushes exactly those
//! containers. A local class declared inside a method qualifies under
//! the enclosing type, not the method.

use std::sync::Arc;

use cairn_lang_api::{Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts};
use cairn_lang_treesitter_generic::{child_by_field, collapse_ws, line_of, node_text};
use tree_sitter::{Node, Parser};

/// Java semantic analyzer. Re-parses the source with tree-sitter-java
/// (the same grammar the syntactic pass uses) and walks for
/// inheritance edges and call / instantiation refs.
pub struct JavaAnalyzer;

impl Analyzer for JavaAnalyzer {
    fn name(&self) -> &'static str {
        "java-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_java::LANGUAGE.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut facts = SemanticFacts::default();
        let mut type_stack: Vec<String> = Vec::new();
        walk(tree.root_node(), source, &mut type_stack, None, &mut facts);
        Ok(facts)
    }
}

/// Recursive walk maintaining:
/// - `type_stack`: enclosing type names (methods are not pushed,
///   matching the syntactic pass — see module docs), used to build
///   qualified names.
/// - `enclosing`: qualified name of the nearest enclosing type /
///   method / constructor, or `None` at top level. Refs attach this as
///   `enclosing_qualified` so the indexer can resolve
///   `refs.enclosing_id` against the symbols table. Field-initializer
///   refs attach the enclosing type.
fn walk(
    node: Node<'_>,
    source: &[u8],
    type_stack: &mut Vec<String>,
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    match node.kind() {
        "class_declaration"
        | "interface_declaration"
        | "enum_declaration"
        | "record_declaration"
        | "annotation_type_declaration" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = qualify(type_stack, &name);
            emit_supertype_edges(node, source, &qualified, facts);
            type_stack.push(name);
            recurse(node, source, type_stack, Some(&qualified), facts);
            type_stack.pop();
            return;
        }
        "method_declaration" | "constructor_declaration" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = qualify(type_stack, &name);
            recurse(node, source, type_stack, Some(&qualified), facts);
            return;
        }
        "method_invocation" => {
            emit_call(node, source, enclosing, facts);
            // fall through to recurse into receiver / arguments
            // (chained and nested calls).
        }
        "object_creation_expression" => {
            emit_instantiation(node, source, enclosing, facts);
            // fall through to recurse into the arguments.
        }
        _ => {}
    }
    recurse(node, source, type_stack, enclosing, facts);
}

fn recurse(
    node: Node<'_>,
    source: &[u8],
    type_stack: &mut Vec<String>,
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            walk(cursor.node(), source, type_stack, enclosing, facts);
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

/// Join the enclosing type names with `.` to mirror the syntactic
/// pass's `NestingTracker` (which uses `"."` for Java).
fn qualify(type_stack: &[String], name: &str) -> String {
    if type_stack.is_empty() {
        name.to_string()
    } else {
        format!("{}.{name}", type_stack.join("."))
    }
}

/// Inheritance edges for one type declaration:
/// - class `superclass` field (`extends Base`) → `"extends"`.
/// - `interfaces` field (`implements A, B` on classes / enums /
///   records) → `"implements"`.
/// - interface `extends_interfaces` child (`interface I extends J, K`)
///   → `"extends"`.
fn emit_supertype_edges(
    node: Node<'_>,
    source: &[u8],
    type_qualified: &str,
    facts: &mut SemanticFacts,
) {
    let line = line_of(node);
    if let Some(superclass) = child_by_field(node, "superclass") {
        let mut cursor = superclass.walk();
        for ty in superclass.named_children(&mut cursor) {
            push_edge(facts, type_qualified, ty, source, "extends", line);
        }
    }
    if let Some(interfaces) = child_by_field(node, "interfaces") {
        emit_type_list_edges(
            interfaces,
            source,
            type_qualified,
            "implements",
            line,
            facts,
        );
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "extends_interfaces" {
            emit_type_list_edges(child, source, type_qualified, "extends", line, facts);
        }
    }
}

/// `super_interfaces` / `extends_interfaces` both wrap a `type_list`.
fn emit_type_list_edges(
    wrapper: Node<'_>,
    source: &[u8],
    type_qualified: &str,
    kind: &str,
    line: u32,
    facts: &mut SemanticFacts,
) {
    let mut cursor = wrapper.walk();
    for child in wrapper.named_children(&mut cursor) {
        if child.kind() != "type_list" {
            continue;
        }
        let mut list_cursor = child.walk();
        for ty in child.named_children(&mut list_cursor) {
            push_edge(facts, type_qualified, ty, source, kind, line);
        }
    }
}

fn push_edge(
    facts: &mut SemanticFacts,
    type_qualified: &str,
    ty: Node<'_>,
    source: &[u8],
    kind: &str,
    line: u32,
) {
    let base = base_type_name(ty, source);
    if base.is_empty() {
        return;
    }
    facts.impls.push(ImplFact {
        type_qualified: type_qualified.to_string(),
        interface_qualified: Some(base),
        kind: kind.to_string(),
        line,
    });
}

/// Strip generic arguments from a supertype (`Comparable<Point>` →
/// `Comparable`); dotted names stay verbatim (`java.lang.Runnable`).
fn base_type_name(node: Node<'_>, source: &[u8]) -> String {
    match node.kind() {
        "generic_type" => node
            .named_child(0)
            .map(|inner| base_type_name(inner, source))
            .unwrap_or_default(),
        _ => collapse_ws(node_text(node, source)),
    }
}

/// The last dotted segment of a path (`pkg.Foo` → `Foo`). Used as the
/// `target_name` so `find_references symbol=Foo` matches.
fn last_segment(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

/// `render()`, `obj.render()`, `Widget.create()` → `Call` ref on the
/// method name. The receiver type is unknown without Tier-3, so
/// `target_qualified` stays `None` — name-level only.
fn emit_call(
    call_node: Node<'_>,
    source: &[u8],
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let Some(name_node) = child_by_field(call_node, "name") else {
        return;
    };
    let target_name = node_text(name_node, source).to_string();
    if target_name.is_empty() {
        return;
    }
    facts.refs.push(RefFact {
        target_name,
        target_qualified: None,
        kind: RefKind::Call,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: enclosing.map(str::to_string),
        byte_range: name_node.byte_range(),
        line: line_of(name_node),
    });
}

/// `new Widget()` / `new pkg.Widget<T>()` → `Instantiate` ref on the
/// base type name's last segment.
fn emit_instantiation(
    new_node: Node<'_>,
    source: &[u8],
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let Some(ty) = child_by_field(new_node, "type") else {
        return;
    };
    let base = base_type_name(ty, source);
    if base.is_empty() {
        return;
    }
    facts.refs.push(RefFact {
        target_name: last_segment(&base).to_string(),
        target_qualified: None,
        kind: RefKind::Instantiate,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: enclosing.map(str::to_string),
        byte_range: ty.byte_range(),
        line: line_of(ty),
    });
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(JavaAnalyzer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        JavaAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn edges(f: &SemanticFacts) -> Vec<(&str, &str, &str)> {
        f.impls
            .iter()
            .map(|i| {
                (
                    i.type_qualified.as_str(),
                    i.interface_qualified.as_deref().unwrap_or(""),
                    i.kind.as_str(),
                )
            })
            .collect()
    }

    #[test]
    fn extends_and_implements_edges() {
        let f = semantic("class Dog extends Animal implements Pet, Comparable<Dog> {}\n");
        assert_eq!(
            edges(&f),
            vec![
                ("Dog", "Animal", "extends"),
                ("Dog", "Pet", "implements"),
                ("Dog", "Comparable", "implements"),
            ]
        );
    }

    #[test]
    fn interface_extends_edges() {
        let f = semantic("interface Closer extends AutoCloseable, Flushable {}\n");
        assert_eq!(
            edges(&f),
            vec![
                ("Closer", "AutoCloseable", "extends"),
                ("Closer", "Flushable", "extends"),
            ]
        );
    }

    #[test]
    fn enum_and_record_implements_edges() {
        let f = semantic(
            "enum Color implements Stringer {}\nrecord Point(int x) implements Comparable<Point> {}\n",
        );
        assert_eq!(
            edges(&f),
            vec![
                ("Color", "Stringer", "implements"),
                ("Point", "Comparable", "implements"),
            ]
        );
    }

    #[test]
    fn dotted_supertype_kept_verbatim() {
        let f = semantic("class Task extends java.util.TimerTask {}\n");
        assert_eq!(
            f.impls[0].interface_qualified.as_deref(),
            Some("java.util.TimerTask")
        );
    }

    #[test]
    fn no_supertypes_no_edges() {
        let f = semantic("class Plain {}\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn nested_type_qualifies_under_outer() {
        let f = semantic("class Outer { class Inner extends Base {} }\n");
        assert_eq!(edges(&f), vec![("Outer.Inner", "Base", "extends")]);
    }

    #[test]
    fn local_class_qualifies_under_type_not_method() {
        // Methods are not part of the qualified path, matching the
        // syntactic pass's NestingTracker.
        let f = semantic("class Outer { void make() { class Local extends Base {} } }\n");
        assert_eq!(edges(&f), vec![("Outer.Local", "Base", "extends")]);
    }

    #[test]
    fn imports_left_to_syntactic_pass() {
        let f = semantic("import java.util.List;\nclass C {}\n");
        assert!(f.imports.is_empty());
    }

    // ─── refs: calls ───────────────────────────────────────────────

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    #[test]
    fn call_inside_method_enclosed_by_qualified_method() {
        let f = semantic("class W { void render() { helper(); } }\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.render"));
        assert_eq!(hit.target_qualified, None);
    }

    #[test]
    fn receiver_call_is_name_level_unresolved() {
        let f = semantic("class W { void run(Widget obj) { obj.render(); } }\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "render")
            .expect("render call missing");
        assert_eq!(hit.target_qualified, None);
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.run"));
    }

    #[test]
    fn nested_call_arguments_also_emit() {
        let f = semantic("class W { void m() { outer(inner()); } }\n");
        let names: Vec<&str> = calls(&f).iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"outer"));
        assert!(names.contains(&"inner"));
    }

    #[test]
    fn field_initializer_call_enclosed_by_type() {
        let f = semantic("class W { int size = compute(); }\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "compute")
            .expect("compute call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W"));
    }

    #[test]
    fn constructor_body_call_enclosed_by_constructor() {
        let f = semantic("class W { W() { init(); } }\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "init")
            .expect("init call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.W"));
    }

    // ─── refs: instantiations ──────────────────────────────────────

    #[test]
    fn new_expression_emits_instantiate_ref() {
        let f = semantic("class W { Object m() { return new java.util.ArrayList<String>(); } }\n");
        let hit = f
            .refs
            .iter()
            .find(|r| r.kind == RefKind::Instantiate)
            .expect("instantiate ref missing");
        assert_eq!(hit.target_name, "ArrayList");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.m"));
    }

    #[test]
    fn constructor_argument_calls_recursed() {
        let f = semantic("class W { void m() { use(new Widget(make())); } }\n");
        let names: Vec<&str> = f.refs.iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"use"));
        assert!(names.contains(&"Widget"));
        assert!(names.contains(&"make"));
    }
}
