//! Blob-scoped Objective-C Tier-2 analyzer.
//!
//! Reparses one `.m` blob with tree-sitter and emits:
//! - `inherit` edges from `@interface Foo : Bar` declarations.
//! - `implement` edges from every protocol in a class's or category's
//!   `<P1, P2>` conformance list, and from a `@protocol P : Q1, Q2`
//!   protocol-inheritance list.
//! - `extension` edges from category declarations (both
//!   `@interface Foo (Cat)` and `@implementation Foo (Cat)`). The
//!   model mirrors how the Swift backend treats `extension Foo:
//!   Protocol` — the category becomes an `extension` row plus an
//!   `implement` row per protocol it adds; it never carries an
//!   `inherit` row (categories cannot change the superclass).
//! - Same-file message-send call refs. The selector head is the bare
//!   target name; a post-pass resolves it against a same-file index of
//!   method short names, with category members keyed under the base
//!   class. Cross-file callees keep `target_qualified: None`.

use std::collections::HashMap;
use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts, SyntacticKind,
};
use cairn_lang_treesitter_generic::{child_by_field, line_of, node_text};
use tree_sitter::{Node, Parser};

pub struct ObjcAnalyzer;

impl Analyzer for ObjcAnalyzer {
    fn name(&self) -> &'static str {
        "objc-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_objc::LANGUAGE.into();
        let mut parser = Parser::new();
        if parser.set_language(&language).is_err() {
            return Ok(SemanticFacts::default());
        }
        let Some(tree) = parser.parse(source, None) else {
            return Ok(SemanticFacts::default());
        };

        let mut walker = ObjcSemanticWalker {
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

pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(ObjcAnalyzer)
}

struct ObjcSemanticWalker {
    facts: SemanticFacts,
    /// Names of the lexically enclosing containers. ObjC classes,
    /// protocols, and categories all qualify members directly under
    /// the class / protocol name (no file-level module path), so this
    /// usually holds at most one entry — but the structure mirrors
    /// the Swift analyzer for symmetry.
    containers: Vec<String>,
    /// Qualified name of the enclosing method, for the `enclosing_qualified`
    /// field of emitted call refs.
    enclosing: Option<String>,
    /// Bare member name → first-walked qualified name for same-file
    /// methods and properties. Used by `resolve_same_file_member_calls`
    /// to resolve `[receiver method]` call refs whose receiver type
    /// cannot be statically known.
    member_qualifieds: HashMap<String, String>,
}

impl ObjcSemanticWalker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            "class_interface" | "class_implementation" => {
                self.walk_class_container(node, source);
            }
            "protocol_declaration" => {
                self.walk_protocol_container(node, source);
            }
            "method_declaration" | "method_definition" => {
                self.walk_method(node, source);
            }
            "property_declaration" => {
                self.record_property_members(node, source);
                self.walk_children(node, source);
            }
            "instance_variable" => {
                self.record_instance_variable_members(node, source);
                self.walk_children(node, source);
            }
            "message_expression" => {
                self.emit_message_call(node, source);
                self.walk_children(node, source);
            }
            "call_expression" => {
                self.emit_c_call(node, source);
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

    fn walk_class_container(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name) = first_named_identifier_text(node, source) else {
            self.walk_children(node, source);
            return;
        };
        let is_category = child_by_field(node, "category").is_some();
        let qualified = self.qualify(&name);

        if is_category {
            let cat_range = node.byte_range();
            self.facts.impls.push(ImplFact {
                type_qualified: qualified.clone(),
                interface_qualified: None,
                kind: "extension".to_string(),
                syntactic_kind: Some(SyntacticKind::Category),
                line: line_of(node),
                interface_byte_range: Some((cat_range.start as u32, cat_range.end as u32)),
            });
            // Categories never carry an inherit edge; conformance
            // protocols become `implement` rows.
            for (proto, proto_node) in protocol_list_entries(node, source) {
                let pr = proto_node.byte_range();
                self.facts.impls.push(ImplFact {
                    type_qualified: qualified.clone(),
                    interface_qualified: Some(proto),
                    kind: "implement".to_string(),
                    syntactic_kind: Some(SyntacticKind::ProtocolList),
                    line: line_of(proto_node),
                    interface_byte_range: Some((pr.start as u32, pr.end as u32)),
                });
            }
        } else {
            if let Some(super_node) = child_by_field(node, "superclass") {
                let super_name = node_text(super_node, source).to_string();
                let sr = super_node.byte_range();
                self.facts.impls.push(ImplFact {
                    type_qualified: qualified.clone(),
                    interface_qualified: Some(super_name),
                    kind: "inherit".to_string(),
                    syntactic_kind: Some(SyntacticKind::InterfaceColon),
                    line: line_of(super_node),
                    interface_byte_range: Some((sr.start as u32, sr.end as u32)),
                });
            }
            for (proto, proto_node) in protocol_list_entries(node, source) {
                let pr = proto_node.byte_range();
                self.facts.impls.push(ImplFact {
                    type_qualified: qualified.clone(),
                    interface_qualified: Some(proto),
                    kind: "implement".to_string(),
                    syntactic_kind: Some(SyntacticKind::ProtocolList),
                    line: line_of(proto_node),
                    interface_byte_range: Some((pr.start as u32, pr.end as u32)),
                });
            }
        }

        let previous = self.enclosing.replace(qualified);
        self.containers.push(name);
        self.walk_children(node, source);
        self.containers.pop();
        self.enclosing = previous;
    }

    fn walk_protocol_container(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name) = first_named_identifier_text(node, source) else {
            self.walk_children(node, source);
            return;
        };
        let qualified = self.qualify(&name);

        for (parent, parent_node) in protocol_list_entries(node, source) {
            let pr = parent_node.byte_range();
            self.facts.impls.push(ImplFact {
                type_qualified: qualified.clone(),
                interface_qualified: Some(parent),
                kind: "inherit".to_string(),
                // `@protocol Foo <Bar>` — protocol-to-protocol
                // inheritance reuses the same `<…>` lexical shape as
                // class adoption, so it maps to `ProtocolList` too.
                syntactic_kind: Some(SyntacticKind::ProtocolList),
                line: line_of(parent_node),
                interface_byte_range: Some((pr.start as u32, pr.end as u32)),
            });
        }

        let previous = self.enclosing.replace(qualified);
        self.containers.push(name);
        self.walk_children(node, source);
        self.containers.pop();
        self.enclosing = previous;
    }

    fn walk_method(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(short) = method_short_name_text(node, source) else {
            self.walk_children(node, source);
            return;
        };
        let qualified = self.qualify(&short);
        if !self.containers.is_empty() {
            self.member_qualifieds
                .entry(short)
                .or_insert_with(|| qualified.clone());
        }
        let previous = self.enclosing.replace(qualified);
        self.walk_children(node, source);
        self.enclosing = previous;
    }

    fn record_property_members(&mut self, node: Node<'_>, source: &[u8]) {
        if self.containers.is_empty() {
            return;
        }
        let Some(inner) = first_named_child(node).and_then(|first| {
            if first.kind() == "property_attributes_declaration" {
                first.next_named_sibling()
            } else {
                Some(first)
            }
        }) else {
            return;
        };
        if inner.kind() != "struct_declaration" {
            return;
        }
        for name in struct_declaration_names(inner, source) {
            let qualified = self.qualify(&name);
            self.member_qualifieds.entry(name).or_insert(qualified);
        }
    }

    fn record_instance_variable_members(&mut self, node: Node<'_>, source: &[u8]) {
        if self.containers.is_empty() {
            return;
        }
        let Some(decl) = first_named_child(node) else {
            return;
        };
        if decl.kind() != "struct_declaration" {
            return;
        }
        for name in struct_declaration_names(decl, source) {
            let qualified = self.qualify(&name);
            self.member_qualifieds.entry(name).or_insert(qualified);
        }
    }

    fn emit_message_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(head) = child_by_field(node, "method") else {
            return;
        };
        let name = node_text(head, source).to_string();
        self.facts.refs.push(RefFact {
            target_name: name,
            target_qualified: None,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: head.byte_range(),
            line: line_of(head),
        });
    }

    fn emit_c_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(callee) = child_by_field(node, "function") else {
            return;
        };
        if callee.kind() != "identifier" {
            return;
        }
        let name = node_text(callee, source).to_string();
        // A bare `foo()` could be either a C function call or a
        // parenthesized macro invocation; we record both and let the
        // resolver decide. Same-file C functions resolve to their
        // bare name (matching the C backend), other callees stay
        // unresolved.
        self.facts.refs.push(RefFact {
            target_name: name,
            target_qualified: None,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: callee.byte_range(),
            line: line_of(callee),
        });
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

// ─── helpers ───────────────────────────────────────────────────────────────

fn first_named_identifier_text(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|n| n.kind() == "identifier")
        .map(|n| node_text(n, source).to_string())
}

fn method_short_name_text(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|n| n.kind() == "identifier")
        .map(|n| node_text(n, source).to_string())
}

/// `(base_name, type_identifier_node)` for every entry in a class's
/// or protocol's `<P1, P2>` conformance list. Handles both the
/// `parameterized_arguments` shape used by class declarations and the
/// `protocol_reference_list` shape used by protocol declarations.
fn protocol_list_entries<'a>(node: Node<'a>, source: &[u8]) -> Vec<(String, Node<'a>)> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "parameterized_arguments" || child.kind() == "protocol_reference_list" {
            collect_identifiers(child, source, &mut out);
        }
    }
    out
}

fn collect_identifiers<'a>(node: Node<'a>, source: &[u8], out: &mut Vec<(String, Node<'a>)>) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "identifier" || child.kind() == "type_identifier" {
            out.push((node_text(child, source).to_string(), child));
        } else {
            collect_identifiers(child, source, out);
        }
    }
}

fn struct_declaration_names(node: Node<'_>, source: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "identifier" | "field_identifier" => {
                out.push(node_text(child, source).to_string());
            }
            "struct_declarator" => {
                if let Some(name) = innermost_identifier(child, source) {
                    out.push(name);
                }
            }
            _ => {}
        }
    }
    out
}

/// Walk a declarator chain (`*x`, `(*fp)`, `x[10]`, plain `x`) down to
/// the bound identifier and return its text.
fn innermost_identifier(node: Node<'_>, source: &[u8]) -> Option<String> {
    match node.kind() {
        "identifier" | "field_identifier" => Some(node_text(node, source).to_string()),
        _ => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if let Some(name) = innermost_identifier(child, source) {
                    return Some(name);
                }
            }
            None
        }
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
        ObjcAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn impls(src: &str) -> Vec<ImplFact> {
        semantic(src).impls
    }

    fn refs(src: &str) -> Vec<RefFact> {
        semantic(src).refs
    }

    #[test]
    fn emits_inherit_edge_for_superclass() {
        let impls = impls("@interface Dog : Animal\n@end");
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].type_qualified, "Dog");
        assert_eq!(impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(impls[0].kind, "inherit");
    }

    #[test]
    fn emits_implement_edges_for_class_protocol_conformance() {
        let impls = impls("@interface Dog : Animal <NSCopying, NSCoding>\n@end");
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
        assert!(rows.contains(&("Dog", Some("Animal"), "inherit")));
        assert!(rows.contains(&("Dog", Some("NSCopying"), "implement")));
        assert!(rows.contains(&("Dog", Some("NSCoding"), "implement")));
    }

    #[test]
    fn category_emits_extension_plus_implement_only() {
        let impls = impls(
            "@interface Person (Logging) <NSCoding>\n\
             - (void)logSelf;\n\
             @end\n\
             @implementation Person (Logging)\n\
             - (void)logSelf {}\n\
             @end\n",
        );
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
        // Both the interface category and the implementation category
        // emit an extension row; the conformance protocol on the
        // interface side becomes an implement row. Categories never
        // emit an inherit row.
        let extensions = rows.iter().filter(|r| r.2 == "extension").count();
        assert_eq!(extensions, 2, "{rows:?}");
        assert!(rows.contains(&("Person", Some("NSCoding"), "implement")));
        assert!(rows.iter().all(|r| r.2 != "inherit"));
    }

    #[test]
    fn protocol_inheritance_edges_are_inherit() {
        let impls = impls("@protocol Repository <Storage, NSCopying>\n@end");
        assert_eq!(impls.len(), 2, "{impls:?}");
        assert!(impls.iter().all(|i| i.type_qualified == "Repository"));
        assert!(impls.iter().all(|i| i.kind == "inherit"));
    }

    #[test]
    fn message_send_resolves_against_same_file_method() {
        let refs = refs(
            "@implementation Foo\n\
             - (void)bar { return; }\n\
             - (void)caller { [self bar]; }\n\
             @end\n",
        );
        let call = refs
            .iter()
            .find(|r| {
                r.target_name == "bar" && r.enclosing_qualified.as_deref() == Some("Foo.caller")
            })
            .expect("bar call from caller");
        assert_eq!(call.target_qualified.as_deref(), Some("Foo.bar"));
    }

    #[test]
    fn message_send_to_cross_file_method_stays_unresolved() {
        let refs = refs(
            "@implementation Foo\n\
             - (void)x { [bar baz]; }\n\
             @end\n",
        );
        let call = refs
            .iter()
            .find(|r| r.target_name == "baz")
            .expect("baz call");
        assert_eq!(call.target_qualified, None);
        assert_eq!(call.enclosing_qualified.as_deref(), Some("Foo.x"));
    }

    #[test]
    fn category_method_calls_resolve_under_base_class() {
        // A method defined inside a category qualifies under the base
        // class, so a `[self logSelf]` call from any same-file caller
        // resolves to `Person.logSelf`.
        let refs = refs(
            "@implementation Person (Logging)\n\
             - (void)logSelf {}\n\
             @end\n\
             @implementation Person\n\
             - (void)trigger { [self logSelf]; }\n\
             @end\n",
        );
        let call = refs
            .iter()
            .find(|r| r.target_name == "logSelf")
            .expect("logSelf call");
        assert_eq!(call.target_qualified.as_deref(), Some("Person.logSelf"));
    }

    #[test]
    fn empty_or_malformed_input_is_empty_ok() {
        assert!(semantic("").refs.is_empty());
        let facts = semantic("@interface Broken : ");
        assert!(facts.impls.is_empty() || facts.impls.iter().all(|i| i.type_qualified == "Broken"));
    }

    #[test]
    fn category_emits_syntactic_category() {
        let impls = impls(
            "@interface Foo (CatName)
@end
",
        );
        let ext = impls
            .iter()
            .find(|i| i.kind == "extension")
            .expect("category extension edge missing");
        assert_eq!(ext.syntactic_kind, Some(SyntacticKind::Category));
    }
}
