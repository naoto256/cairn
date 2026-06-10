//! Blob-scoped Kotlin Tier-2 analyzer.
//!
//! Reparses one `.kt` / `.kts` blob with tree-sitter-kotlin-ng and emits
//! delegation (inheritance / interface-implementation) edges plus
//! name-level call and constructor refs. Imports stay a Tier-1 surface.

use std::sync::Arc;

use cairn_lang_api::{Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts};
use cairn_lang_treesitter_generic::{child_by_field, line_of, node_text};
use tree_sitter::{Node, Parser};

pub struct KotlinAnalyzer;

impl Analyzer for KotlinAnalyzer {
    fn name(&self) -> &'static str {
        "kotlin-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_kotlin_ng::LANGUAGE.into();
        let mut parser = Parser::new();
        if parser.set_language(&language).is_err() {
            return Ok(SemanticFacts::default());
        }
        let Some(tree) = parser.parse(source, None) else {
            return Ok(SemanticFacts::default());
        };

        let mut walker = KtSemanticWalker {
            facts: SemanticFacts::default(),
            containers: Vec::new(),
            enclosing: None,
        };
        walker.walk(tree.root_node(), source);
        Ok(walker.facts)
    }
}

pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(KotlinAnalyzer)
}

struct KtSemanticWalker {
    facts: SemanticFacts,
    containers: Vec<String>,
    enclosing: Option<String>,
}

impl KtSemanticWalker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            "class_declaration" | "object_declaration" | "companion_object" => {
                self.walk_container(node, source);
            }
            "function_declaration" => self.walk_callable(node, source),
            "call_expression" => {
                self.emit_call(node, source);
                self.walk_children(node, source);
            }
            _ => self.walk_children(node, source),
        }
    }

    fn walk_children(&mut self, node: Node<'_>, source: &[u8]) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.walk(child, source);
        }
    }

    fn walk_container(&mut self, node: Node<'_>, source: &[u8]) {
        let name = child_by_field(node, "name").map_or_else(
            || (node.kind() == "companion_object").then(|| "Companion".to_string()),
            |n| Some(node_text(n, source).to_string()),
        );
        let Some(name) = name else {
            self.walk_children(node, source);
            return;
        };

        let qualified = self.qualify(&name);
        let previous_enclosing = self.enclosing.replace(qualified.clone());
        self.emit_delegation(node, source, &qualified);
        self.containers.push(name);
        self.walk_children(node, source);
        self.containers.pop();
        self.enclosing = previous_enclosing;
    }

    /// Delegation specifiers after `:`. A `constructor_invocation`
    /// (`Base()`) is a superclass call → `inherit`; a bare `user_type`
    /// (`Greeter`, possibly with `by` delegation) is an interface →
    /// `implement`. Kotlin syntax does not distinguish further without
    /// resolution, so this mirrors the source-level shape.
    fn emit_delegation(&mut self, node: Node<'_>, source: &[u8], type_qualified: &str) {
        let Some(specifiers) = find_direct_child(node, "delegation_specifiers") else {
            return;
        };
        let mut cursor = specifiers.walk();
        for specifier in specifiers.named_children(&mut cursor) {
            if specifier.kind() != "delegation_specifier" {
                continue;
            }
            let (base, kind) =
                if let Some(invocation) = find_descendant(specifier, "constructor_invocation") {
                    (find_direct_child(invocation, "user_type"), "inherit")
                } else {
                    (find_descendant(specifier, "user_type"), "implement")
                };
            let Some(base) = base else { continue };
            let Some(base_name) = user_type_name(base, source) else {
                continue;
            };
            self.facts.impls.push(ImplFact {
                type_qualified: type_qualified.to_string(),
                interface_qualified: Some(base_name),
                kind: kind.to_string(),
                line: line_of(base),
            });
        }
    }

    fn walk_callable(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name) = child_by_field(node, "name") else {
            self.walk_children(node, source);
            return;
        };
        let qualified = self.qualify(node_text(name, source));
        let previous_enclosing = self.enclosing.replace(qualified);
        self.walk_children(node, source);
        self.enclosing = previous_enclosing;
    }

    /// A `call_expression` is `callee value_arguments`. A bare
    /// `identifier` callee covers both function and constructor calls
    /// (`compute(x)`, `Service()`) and resolves at name level; a
    /// `navigation_expression` callee (`obj.method()`) keeps only the
    /// member name, unresolved.
    fn emit_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(callee) = node.named_child(0) else {
            return;
        };
        let Some((target_name, target_qualified, target_node)) = callee_target(callee, source)
        else {
            return;
        };
        self.facts.refs.push(RefFact {
            target_name,
            target_qualified,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: target_node.start_byte()..target_node.end_byte(),
            line: line_of(target_node),
        });
    }

    fn qualify(&self, name: &str) -> String {
        if self.containers.is_empty() {
            name.to_string()
        } else {
            let mut qualified = self.containers.join(".");
            qualified.push('.');
            qualified.push_str(name);
            qualified
        }
    }
}

fn callee_target<'a>(node: Node<'a>, source: &[u8]) -> Option<(String, Option<String>, Node<'a>)> {
    match node.kind() {
        "identifier" => {
            let name = node_text(node, source).to_string();
            Some((name.clone(), Some(name), node))
        }
        "navigation_expression" => {
            let member = last_direct_child_of_kind(node, "identifier")?;
            Some((node_text(member, source).to_string(), None, member))
        }
        _ => None,
    }
}

/// Dotted name of a `user_type`, generics stripped: identifiers joined
/// with `.`; `type_arguments` children are skipped.
fn user_type_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut parts: Vec<&str> = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" {
            parts.push(node_text(child, source));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

fn find_direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

fn find_descendant<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == kind {
            return Some(child);
        }
        if let Some(found) = find_descendant(child, kind) {
            return Some(found);
        }
    }
    None
}

fn last_direct_child_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .filter(|c| c.kind() == kind)
        .last()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        KotlinAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn call_ref<'a>(refs: &'a [RefFact], name: &str) -> &'a RefFact {
        refs.iter()
            .find(|r| r.kind == RefKind::Call && r.target_name == name)
            .unwrap_or_else(|| panic!("call ref {name} missing: {refs:?}"))
    }

    #[test]
    fn emits_superclass_inherit_and_interface_implement_edges() {
        let facts = semantic("class Service : Base(), Greeter {}");
        let rows: Vec<(&str, Option<&str>, &str)> = facts
            .impls
            .iter()
            .map(|i| {
                (
                    i.type_qualified.as_str(),
                    i.interface_qualified.as_deref(),
                    i.kind.as_str(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("Service", Some("Base"), "inherit"),
                ("Service", Some("Greeter"), "implement"),
            ]
        );
    }

    #[test]
    fn strips_generic_arguments_from_delegation_names() {
        let facts = semantic("class Impl : Container<String>() {}");
        assert_eq!(facts.impls.len(), 1, "{:?}", facts.impls);
        assert_eq!(
            facts.impls[0].interface_qualified.as_deref(),
            Some("Container")
        );
        assert_eq!(facts.impls[0].kind, "inherit");
    }

    #[test]
    fn nested_sealed_variants_qualify_through_parent() {
        let facts = semantic(
            "sealed class Result {\n\
                 data class Success(val v: Int) : Result()\n\
             }",
        );
        assert!(facts.impls.iter().any(|i| {
            i.type_qualified == "Result.Success"
                && i.interface_qualified.as_deref() == Some("Result")
                && i.kind == "inherit"
        }));
    }

    #[test]
    fn interface_delegation_by_keyword_is_implement() {
        let facts = semantic("class Wrapper(g: Greeter) : Greeter by g {}");
        assert!(facts.impls.iter().any(|i| {
            i.type_qualified == "Wrapper"
                && i.interface_qualified.as_deref() == Some("Greeter")
                && i.kind == "implement"
        }));
    }

    #[test]
    fn object_declaration_emits_delegation_edges() {
        let facts = semantic("object Singleton : Base()");
        assert!(facts.impls.iter().any(|i| {
            i.type_qualified == "Singleton"
                && i.interface_qualified.as_deref() == Some("Base")
                && i.kind == "inherit"
        }));
    }

    #[test]
    fn emits_call_refs_with_function_enclosing() {
        let refs = semantic("fun caller() { compute(1) }").refs;
        let call = call_ref(&refs, "compute");
        assert_eq!(call.target_qualified.as_deref(), Some("compute"));
        assert_eq!(call.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn constructor_calls_are_name_level_call_refs() {
        let refs = semantic("fun caller(): Service { return Service() }").refs;
        let call = call_ref(&refs, "Service");
        assert_eq!(call.target_qualified.as_deref(), Some("Service"));
    }

    #[test]
    fn member_calls_stay_unresolved() {
        let refs = semantic("fun caller(s: Service) { s.fetch(1) }").refs;
        let call = call_ref(&refs, "fetch");
        assert_eq!(call.target_qualified, None);
        assert_eq!(call.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn method_enclosing_is_class_qualified() {
        let refs = semantic("class C { fun m() { helper() } }").refs;
        let call = call_ref(&refs, "helper");
        assert_eq!(call.enclosing_qualified.as_deref(), Some("C.m"));
    }

    #[test]
    fn companion_member_enclosing_qualifies_through_companion() {
        let refs = semantic(
            "class Service {\n\
                 companion object {\n\
                     fun create() { build() }\n\
                 }\n\
             }",
        )
        .refs;
        let call = call_ref(&refs, "build");
        assert_eq!(
            call.enclosing_qualified.as_deref(),
            Some("Service.Companion.create")
        );
    }

    #[test]
    fn calls_inside_when_branches_are_collected() {
        let refs = semantic(
            "fun route(x: Int) {\n\
                 when (x) {\n\
                     1 -> first()\n\
                     else -> fallback()\n\
                 }\n\
             }",
        )
        .refs;
        assert_eq!(
            call_ref(&refs, "first").enclosing_qualified.as_deref(),
            Some("route")
        );
        assert_eq!(
            call_ref(&refs, "fallback").enclosing_qualified.as_deref(),
            Some("route")
        );
    }

    #[test]
    fn empty_or_malformed_input_degrades_to_empty_facts() {
        assert!(semantic("").refs.is_empty());
        let facts = semantic("class {");
        assert!(facts.doc_overrides.is_empty());
        assert!(facts.imports.is_empty());
    }

    #[test]
    fn recovered_parse_keeps_facts_from_valid_regions() {
        let facts = semantic(
            "class Ok : Base() {}\n\
             fun broken() { val x: = 1 }\n\
             fun alsoOk() { fine() }\n",
        );
        assert!(facts.impls.iter().any(|i| i.type_qualified == "Ok"));
        assert!(
            facts
                .refs
                .iter()
                .any(|r| r.kind == RefKind::Call && r.target_name == "fine")
        );
    }
}
