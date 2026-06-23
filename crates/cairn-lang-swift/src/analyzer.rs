//! Blob-scoped Swift Tier-2 analyzer.
//!
//! Reparses one `.swift` blob with tree-sitter and emits:
//! - inheritance-clause edges for nominal types and protocols. Swift
//!   syntax cannot distinguish a superclass from a protocol
//!   conformance inside the clause, so every edge is emitted as
//!   `"inherit"`.
//! - extension edges: an `"extension"` (inherent-like) edge from every
//!   `extension Foo` to `Foo`, plus an `"implement"` edge per protocol
//!   in the extension's inheritance clause (extensions can only add
//!   conformances, never superclasses).
//! - same-file call refs at name level. Bare calls (`foo()`,
//!   `Type()`) carry a resolved `target_qualified` set to the bare
//!   name. Member calls (`obj.method()`) parse as
//!   `navigation_expression`; a post-pass resolves the tail segment
//!   against a same-file index of method, constructor (`init`),
//!   property, and enum-case qualified names — extension members
//!   qualify under the extended type, protocol property requirements
//!   and enum cases under the containing nominal. When several
//!   same-file definitions share the bare name, the first one walked
//!   wins; cross-file callees keep `target_qualified: None` and stay
//!   hidden from `find_references`' default outgoing view (visible
//!   with `include_noise`).

use std::collections::HashMap;
use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts, SyntacticKind,
};
use cairn_lang_treesitter_generic::{child_by_field, line_of, node_text};
use tree_sitter::{Node, Parser};

/// Swift Tier-2 analyzer backed by tree-sitter. It requires no external Swift
/// toolchain and intentionally limits resolution to same-file facts.
pub struct SwiftAnalyzer;

impl Analyzer for SwiftAnalyzer {
    fn name(&self) -> &'static str {
        "swift-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_swift::LANGUAGE.into();
        let mut parser = Parser::new();
        if parser.set_language(&language).is_err() {
            return Ok(SemanticFacts::default());
        }
        let Some(tree) = parser.parse(source, None) else {
            return Ok(SemanticFacts::default());
        };

        let mut walker = SwiftSemanticWalker {
            facts: SemanticFacts::default(),
            containers: Vec::new(),
            enclosing: None,
            member_qualifieds: HashMap::new(),
        };
        walker.walk(tree.root_node(), source);
        walker.resolve_same_file_member_calls();
        Ok(walker.facts)
    }
}

/// Constructs the blob-scoped Swift semantic analyzer registered by the Swift
/// language backend.
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(SwiftAnalyzer)
}

struct SwiftSemanticWalker {
    facts: SemanticFacts,
    containers: Vec<String>,
    enclosing: Option<String>,
    /// Bare member name → first-walked qualified name for
    /// same-file methods, constructors (`init`), properties (incl.
    /// protocol requirements), and enum cases. Used to resolve
    /// `obj.member` navigation_expression calls after the walk.
    member_qualifieds: HashMap<String, String>,
}

impl SwiftSemanticWalker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            "class_declaration" | "protocol_declaration" => {
                self.walk_container(node, source);
            }
            "function_declaration" | "protocol_function_declaration" => {
                self.walk_callable(node, source, None);
            }
            "init_declaration" => {
                self.walk_callable(node, source, Some("init"));
            }
            "property_declaration" | "protocol_property_declaration" => {
                self.record_property_members(node, source);
                self.walk_children(node, source);
            }
            "enum_entry" => {
                self.record_enum_case_members(node, source);
                self.walk_children(node, source);
            }
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
        let Some(name) = crate::declared_type_name(node, source) else {
            self.walk_children(node, source);
            return;
        };
        let is_extension = child_by_field(node, "declaration_kind")
            .is_some_and(|k| node_text(k, source) == "extension");

        // An extension's members qualify under the *extended* type, so
        // its qualified name ignores file-level nesting the same way
        // Tier-1 does (extensions are file-scope only in Swift).
        let qualified = self.qualify(&name);

        if is_extension {
            self.facts.impls.push(ImplFact {
                type_qualified: qualified.clone(),
                interface_qualified: None,
                kind: "extension".to_string(),
                // Bare `extension Foo {}` (no conformance clause) —
                // the declaration itself is the syntactic shape.
                syntactic_kind: Some(SyntacticKind::Extension),
                line: line_of(node),
            });
        }
        for (base, base_node) in inheritance_entries(node, source) {
            self.facts.impls.push(ImplFact {
                type_qualified: qualified.clone(),
                interface_qualified: Some(base),
                kind: if is_extension { "implement" } else { "inherit" }.to_string(),
                // Swift declarations and extensions both use a `:`
                // heritage list; the syntactic shape is `Colon`
                // regardless of which semantic kind we settle on.
                syntactic_kind: Some(SyntacticKind::Colon),
                line: line_of(base_node),
            });
        }

        let previous_enclosing = self.enclosing.replace(qualified);
        self.containers.push(name);
        self.walk_children(node, source);
        self.containers.pop();
        self.enclosing = previous_enclosing;
    }

    fn walk_callable(&mut self, node: Node<'_>, source: &[u8], fixed_name: Option<&str>) {
        let name = match fixed_name {
            Some(fixed) => Some(fixed.to_string()),
            None => child_by_field(node, "name")
                .filter(|n| n.kind() == "simple_identifier")
                .map(|n| node_text(n, source).to_string()),
        };
        let Some(name) = name else {
            self.walk_children(node, source);
            return;
        };
        let qualified = self.qualify(&name);
        if !self.containers.is_empty() {
            self.record_member(&name, &qualified);
        }
        let previous_enclosing = self.enclosing.replace(qualified);
        self.walk_children(node, source);
        self.enclosing = previous_enclosing;
    }

    fn record_property_members(&mut self, node: Node<'_>, source: &[u8]) {
        if self.containers.is_empty() {
            return;
        }
        let mut cursor = node.walk();
        let patterns: Vec<Node<'_>> = node.children_by_field_name("name", &mut cursor).collect();
        let mut names: Vec<String> = Vec::new();
        for pattern in patterns {
            collect_pattern_names(pattern, source, &mut names);
        }
        for name in names {
            let qualified = self.qualify(&name);
            self.member_qualifieds.entry(name).or_insert(qualified);
        }
    }

    fn record_enum_case_members(&mut self, node: Node<'_>, source: &[u8]) {
        if self.containers.is_empty() {
            return;
        }
        let mut cursor = node.walk();
        for name_node in node.children_by_field_name("name", &mut cursor) {
            if !name_node.is_named() {
                continue;
            }
            let name = node_text(name_node, source).to_string();
            let qualified = self.qualify(&name);
            self.member_qualifieds.entry(name).or_insert(qualified);
        }
    }

    fn record_member(&mut self, name: &str, qualified: &str) {
        self.member_qualifieds
            .entry(name.to_string())
            .or_insert_with(|| qualified.to_string());
    }

    fn resolve_same_file_member_calls(&mut self) {
        for r in self.facts.refs.iter_mut() {
            if r.kind != RefKind::Call || r.target_qualified.is_some() {
                continue;
            }
            if let Some(qualified) = self.member_qualifieds.get(&r.target_name) {
                r.target_qualified = Some(qualified.clone());
            }
        }
    }

    fn emit_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(callee) = first_named_child(node) else {
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

/// `(base_name, name_node)` for every entry in a declaration's
/// inheritance clause. Generic arguments are stripped
/// (`Collection<User>` → `Collection`).
fn inheritance_entries<'a>(node: Node<'a>, source: &[u8]) -> Vec<(String, Node<'a>)> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() != "inheritance_specifier" {
            continue;
        }
        let Some(ty) = child_by_field(child, "inherits_from") else {
            continue;
        };
        if let Some(name) = base_type_name(ty, source) {
            out.push((name, ty));
        }
    }
    out
}

fn base_type_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    match node.kind() {
        "type_identifier" => Some(node_text(node, source).to_string()),
        "user_type" => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .find(|c| c.kind() == "type_identifier")
                .map(|c| node_text(c, source).to_string())
        }
        _ => None,
    }
}

fn callee_target<'a>(node: Node<'a>, source: &[u8]) -> Option<(String, Option<String>, Node<'a>)> {
    match node.kind() {
        "simple_identifier" => {
            let name = node_text(node, source).to_string();
            Some((name.clone(), Some(name), node))
        }
        // `obj.method()` — keep the member name unresolved; receiver
        // resolution needs more than one blob's worth of context.
        "navigation_expression" => {
            let suffix = child_by_field(node, "suffix")?;
            let member = first_named_child(suffix)?;
            if member.kind() != "simple_identifier" {
                return None;
            }
            let name = node_text(member, source).to_string();
            Some((name, None, member))
        }
        _ => None,
    }
}

fn collect_pattern_names(node: Node<'_>, source: &[u8], out: &mut Vec<String>) {
    if node.kind() == "simple_identifier" {
        out.push(node_text(node, source).to_string());
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_pattern_names(child, source, out);
    }
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        SwiftAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn impls(src: &str) -> Vec<ImplFact> {
        semantic(src).impls
    }

    fn refs(src: &str) -> Vec<RefFact> {
        semantic(src).refs
    }

    #[test]
    fn emits_inheritance_clause_edges_for_nominal_types() {
        let impls = impls("class Dog: Animal, Pet {}\nstruct User: Codable {}");
        let rows: Vec<(&str, Option<&str>, &str)> = impls
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
                ("Dog", Some("Animal"), "inherit"),
                ("Dog", Some("Pet"), "inherit"),
                ("User", Some("Codable"), "inherit"),
            ]
        );
    }

    #[test]
    fn emits_protocol_inheritance_edges() {
        let impls = impls("protocol Repository: Storage, AnyObject {}");
        assert_eq!(impls.len(), 2, "{impls:?}");
        assert!(impls.iter().all(|i| i.type_qualified == "Repository"));
        assert!(impls.iter().all(|i| i.kind == "inherit"));
    }

    #[test]
    fn emits_extension_and_conformance_edges() {
        let impls = impls("extension UserStore: Repository, Equatable { func find() {} }");
        let rows: Vec<(&str, Option<&str>, &str)> = impls
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
                ("UserStore", None, "extension"),
                ("UserStore", Some("Repository"), "implement"),
                ("UserStore", Some("Equatable"), "implement"),
            ]
        );
    }

    #[test]
    fn plain_extension_emits_extension_edge_only() {
        let impls = impls("extension Array where Element == Int { var total: Int { 0 } }");
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].type_qualified, "Array");
        assert_eq!(impls[0].interface_qualified, None);
        assert_eq!(impls[0].kind, "extension");
    }

    #[test]
    fn strips_generic_arguments_from_inheritance_names() {
        let impls = impls("struct Stack: Collection<Int> {}");
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].interface_qualified.as_deref(), Some("Collection"));
    }

    #[test]
    fn emits_bare_call_refs_with_enclosing_function() {
        let refs = refs("func caller() { helper() }");
        let call = &refs[0];
        assert_eq!(call.target_name, "helper");
        assert_eq!(call.target_qualified.as_deref(), Some("helper"));
        assert_eq!(call.enclosing_qualified.as_deref(), Some("caller"));
        assert_eq!(call.kind, RefKind::Call);
    }

    #[test]
    fn member_calls_stay_unresolved_when_no_same_file_definition() {
        // No same-file definition of `add` exists → cross-file callee
        // keeps `target_qualified: None` so the default outgoing view
        // hides it from `find_references`.
        let refs = refs("func caller() { store.add(user) }");
        let call = refs
            .iter()
            .find(|r| r.target_name == "add")
            .expect("add call");
        assert_eq!(call.target_qualified, None);
        assert_eq!(call.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn navigation_expression_call_resolves_against_same_file_method() {
        // `store.add(user)` resolves to `UserStore.add` because the
        // method is defined in the same file. Bare-name fallback alone
        // would leave it None and hide it from `find_references`.
        let refs = refs(
            "class UserStore {\n\
             \tfunc add(_ user: User) {}\n\
             }\n\
             func caller(store: UserStore, user: User) { store.add(user) }\n",
        );
        let call = refs
            .iter()
            .find(|r| r.target_name == "add" && r.enclosing_qualified.as_deref() == Some("caller"))
            .expect("caller's add call");
        assert_eq!(call.target_qualified.as_deref(), Some("UserStore.add"));
    }

    #[test]
    fn navigation_expression_resolves_against_extension_init_and_property() {
        let refs = refs(
            "extension UserStore {\n\
             \tinit(seed: Int) {}\n\
             \tvar handler: () -> Void { {} }\n\
             }\n\
             func caller(store: UserStore) {\n\
             \tstore.init()\n\
             \tstore.handler()\n\
             }\n",
        );
        let init_call = refs
            .iter()
            .find(|r| r.target_name == "init" && r.enclosing_qualified.as_deref() == Some("caller"))
            .expect("init call");
        assert_eq!(
            init_call.target_qualified.as_deref(),
            Some("UserStore.init")
        );
        let handler_call = refs
            .iter()
            .find(|r| {
                r.target_name == "handler" && r.enclosing_qualified.as_deref() == Some("caller")
            })
            .expect("handler call");
        assert_eq!(
            handler_call.target_qualified.as_deref(),
            Some("UserStore.handler")
        );
    }

    #[test]
    fn navigation_expression_takes_first_candidate_when_method_name_collides() {
        // Two same-file methods share the bare name `find`. The
        // first-walked definition wins; cross-file disambiguation is
        // out of scope for the same-file resolver.
        let refs = refs(
            "class Store {\n\
             \tfunc find() {}\n\
             }\n\
             class UserStore {\n\
             \tfunc find() {}\n\
             }\n\
             func caller(s: UserStore) { s.find() }\n",
        );
        let call = refs
            .iter()
            .find(|r| r.target_name == "find" && r.enclosing_qualified.as_deref() == Some("caller"))
            .expect("find call");
        assert_eq!(call.target_qualified.as_deref(), Some("Store.find"));
    }

    #[test]
    fn method_and_init_enclosing_are_qualified() {
        let refs = refs(
            "class Store {\n\
             \tinit() { setup() }\n\
             \tfunc add() { validate() }\n\
             }",
        );
        assert!(refs.iter().any(|r| {
            r.target_name == "setup" && r.enclosing_qualified.as_deref() == Some("Store.init")
        }));
        assert!(refs.iter().any(|r| {
            r.target_name == "validate" && r.enclosing_qualified.as_deref() == Some("Store.add")
        }));
    }

    #[test]
    fn trailing_closure_calls_are_captured() {
        let refs = refs("func caller() { items.map { transform($0) } }");
        assert!(refs.iter().any(|r| r.target_name == "map"));
        assert!(refs.iter().any(|r| {
            r.target_name == "transform"
                && r.target_qualified.as_deref() == Some("transform")
                && r.enclosing_qualified.as_deref() == Some("caller")
        }));
    }

    #[test]
    fn extension_member_calls_qualify_under_extended_type() {
        let refs = refs("extension Store { func reload() { fetch() } }");
        let call = &refs[0];
        assert_eq!(call.target_name, "fetch");
        assert_eq!(call.enclosing_qualified.as_deref(), Some("Store.reload"));
    }

    #[test]
    fn empty_or_malformed_input_is_empty_ok() {
        assert!(semantic("").refs.is_empty());
        let facts = semantic("func {");
        assert!(facts.impls.is_empty());
    }

    #[test]
    fn recovered_parse_keeps_facts_from_valid_regions() {
        let facts = semantic(
            "class Ok: Base {}\n\
             func broken() { let x: = }\n\
             func alsoOk() { bar() }\n",
        );
        assert!(facts.impls.iter().any(|i| {
            i.type_qualified == "Ok" && i.interface_qualified.as_deref() == Some("Base")
        }));
        assert!(facts.refs.iter().any(|r| {
            r.target_name == "bar" && r.enclosing_qualified.as_deref() == Some("alsoOk")
        }));
    }

    #[test]
    fn bare_extension_emits_syntactic_extension() {
        let impls = impls(
            "extension Foo {}
",
        );
        let ext = impls
            .iter()
            .find(|i| i.kind == "extension")
            .expect("extension self-edge missing");
        assert_eq!(ext.syntactic_kind, Some(SyntacticKind::Extension));
    }
}
