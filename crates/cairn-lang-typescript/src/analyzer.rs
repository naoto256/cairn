//! Blob-scoped TypeScript Tier-2 analyzer.
//!
//! PR1 intentionally stays syntactic: it reparses one `.ts` blob with
//! tree-sitter and emits call refs, annotation/type refs, and heritage
//! impl edges. Imports and doc overrides remain Tier-1/follow-up surfaces.

use std::sync::Arc;

use cairn_lang_api::{Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts, TypeRole};
use cairn_lang_treesitter_generic::{child_by_field, line_of, node_text};
use tree_sitter::{Node, Parser};

pub struct TypescriptAnalyzer;

impl Analyzer for TypescriptAnalyzer {
    fn name(&self) -> &'static str {
        "typescript-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into();
        let mut parser = Parser::new();
        if parser.set_language(&language).is_err() {
            return Ok(SemanticFacts::default());
        }
        let Some(tree) = parser.parse(source, None) else {
            return Ok(SemanticFacts::default());
        };

        let mut walker = TsSemanticWalker {
            facts: SemanticFacts::default(),
            containers: Vec::new(),
            enclosing: None,
        };
        walker.walk(tree.root_node(), source);
        Ok(walker.facts)
    }
}

pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(TypescriptAnalyzer)
}

struct TsSemanticWalker {
    facts: SemanticFacts,
    containers: Vec<String>,
    enclosing: Option<String>,
}

impl TsSemanticWalker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            // TS namespaces (`internal_module`) are not yet pushed;
            // doing so would require Tier-1 to recognize them too.
            // Tracked as a future PR.
            "class_declaration" | "interface_declaration" | "enum_declaration" => {
                self.walk_container(node, source);
            }
            "function_declaration" | "method_definition" | "method_signature" => {
                self.walk_callable(node, source);
            }
            "call_expression" => {
                self.emit_call(node, source);
                self.walk_children(node, source);
            }
            "new_expression" => {
                self.emit_constructor_call(node, source);
                self.walk_children(node, source);
            }
            "public_field_definition" | "property_signature" => {
                self.emit_direct_type_annotation(node, source, TypeRole::Field);
                self.walk_children(node, source);
            }
            "type_alias_declaration" => {
                self.emit_type_alias(node, source);
                self.walk_children(node, source);
            }
            "type_parameter" => {
                self.emit_type_parameter_bound(node, source);
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
        if let Some(name) = declaration_name(node, source) {
            let qualified = self.qualify(&name);
            let previous_enclosing = self.enclosing.replace(qualified.clone());
            self.emit_heritage(node, source, &qualified);
            self.containers.push(name);
            self.walk_children(node, source);
            self.containers.pop();
            self.enclosing = previous_enclosing;
        } else {
            self.walk_children(node, source);
        }
    }

    fn emit_heritage(&mut self, node: Node<'_>, source: &[u8], type_qualified: &str) {
        match node.kind() {
            "class_declaration" => {
                if let Some(heritage) = find_direct_child(node, "class_heritage") {
                    for clause in direct_children_by_kind(heritage, "extends_clause") {
                        self.emit_heritage_clause(clause, source, type_qualified, "inherit");
                    }
                    for clause in direct_children_by_kind(heritage, "implements_clause") {
                        self.emit_heritage_clause(clause, source, type_qualified, "implement");
                    }
                }
            }
            "interface_declaration" => {
                for clause in direct_children_by_kind(node, "extends_type_clause") {
                    self.emit_heritage_clause(clause, source, type_qualified, "inherit");
                }
            }
            _ => {}
        }
    }

    fn emit_heritage_clause(
        &mut self,
        clause: Node<'_>,
        source: &[u8],
        type_qualified: &str,
        kind: &str,
    ) {
        let mut cursor = clause.walk();
        for child in clause.named_children(&mut cursor) {
            if let Some((interface_qualified, name_node)) = heritage_name(child, source) {
                self.facts.impls.push(ImplFact {
                    type_qualified: type_qualified.to_string(),
                    interface_qualified: Some(interface_qualified),
                    kind: kind.to_string(),
                    line: line_of(name_node),
                });
            }
        }
    }

    fn walk_callable(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name) = callable_name(node, source) else {
            self.walk_children(node, source);
            return;
        };
        let qualified = self.qualify(&name);
        let previous_enclosing = self.enclosing.replace(qualified);

        if let Some(parameters) = child_by_field(node, "parameters")
            .or_else(|| find_direct_child(node, "formal_parameters"))
        {
            self.emit_parameter_types(parameters, source);
        }
        self.emit_return_type(node, source);
        self.walk_children(node, source);

        self.enclosing = previous_enclosing;
    }

    fn emit_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(function) = child_by_field(node, "function") else {
            return;
        };
        let Some((target_name, target_qualified, target_node)) = callee_target(function, source)
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

    fn emit_constructor_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(constructor) = child_by_field(node, "constructor")
            .or_else(|| child_by_field(node, "function"))
            .or_else(|| first_named_child(node))
        else {
            return;
        };
        let Some((target_name, target_qualified, target_node)) = callee_target(constructor, source)
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

    fn emit_parameter_types(&mut self, node: Node<'_>, source: &[u8]) {
        for annotation in descendant_nodes(node, "type_annotation") {
            self.emit_type_annotation(annotation, source, TypeRole::Param);
        }
    }

    fn emit_return_type(&mut self, node: Node<'_>, source: &[u8]) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "type_annotation" {
                self.emit_type_annotation(child, source, TypeRole::Return);
            }
        }
    }

    fn emit_direct_type_annotation(&mut self, node: Node<'_>, source: &[u8], role: TypeRole) {
        if let Some(annotation) = find_direct_child(node, "type_annotation") {
            self.emit_type_annotation(annotation, source, role);
        }
    }

    fn emit_type_alias(&mut self, node: Node<'_>, source: &[u8]) {
        if let Some(value) = child_by_field(node, "value").or_else(|| last_named_child(node)) {
            self.emit_type_expr(value, source, TypeRole::Alias);
        }
    }

    fn emit_type_parameter_bound(&mut self, node: Node<'_>, source: &[u8]) {
        if let Some(bound) = child_by_field(node, "constraint")
            .or_else(|| named_child_after_keyword(node, source, "extends"))
        {
            self.emit_type_expr(bound, source, TypeRole::Bound);
        }
    }

    fn emit_type_annotation(&mut self, node: Node<'_>, source: &[u8], role: TypeRole) {
        if let Some(ty) = first_named_child(node) {
            self.emit_type_expr(ty, source, role);
        }
    }

    fn emit_type_expr(&mut self, node: Node<'_>, source: &[u8], role: TypeRole) {
        match node.kind() {
            "type_identifier" | "identifier" | "nested_type_identifier" => {
                self.push_type_ref(node, source, role);
            }
            "generic_type" => {
                if let Some(name) = first_named_child(node) {
                    self.emit_type_expr(name, source, role);
                }
                self.emit_type_descendants(node, source, TypeRole::GenericArg);
            }
            "type_arguments" | "type_parameters" => {
                self.emit_type_descendants(node, source, role);
            }
            "predefined_type" => {}
            _ => self.emit_type_descendants(node, source, role),
        }
    }

    fn emit_type_descendants(&mut self, node: Node<'_>, source: &[u8], role: TypeRole) {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            self.emit_type_expr(child, source, role);
        }
    }

    fn push_type_ref(&mut self, node: Node<'_>, source: &[u8], role: TypeRole) {
        let text = normalized_text(node, source);
        if text.is_empty() || is_builtin_type(&text) {
            return;
        }
        let target_name = text.rsplit('.').next().unwrap_or(&text).to_string();
        self.facts.refs.push(RefFact {
            target_name,
            target_qualified: text.contains('.').then_some(text),
            kind: RefKind::Type,
            type_role: Some(role),
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: node.start_byte()..node.end_byte(),
            line: line_of(node),
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

fn declaration_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    child_by_field(node, "name")
        .or_else(|| child_by_field(node, "identifier"))
        .map(|name| node_text(name, source).to_string())
}

fn callable_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    child_by_field(node, "name")
        .or_else(|| child_by_field(node, "property"))
        .map(|name| node_text(name, source).to_string())
}

fn callee_target<'a>(node: Node<'a>, source: &[u8]) -> Option<(String, Option<String>, Node<'a>)> {
    match node.kind() {
        "identifier" => {
            let name = node_text(node, source).to_string();
            Some((name.clone(), Some(name), node))
        }
        "instantiation_expression" | "optional_chain" => {
            let callee = first_named_child(node)?;
            callee_target(callee, source)
        }
        "member_expression" => {
            let property = child_by_field(node, "property").or_else(|| last_named_child(node))?;
            let name = node_text(property, source).to_string();
            // PR2/PR3 may add import-derived alias tracking (walk
            // import_statement nodes, build alias set, then qualify member
            // calls whose root is in that set). See design doc open
            // questions #1 + #2.
            Some((name, None, property))
        }
        _ => None,
    }
}

fn normalized_text(node: Node<'_>, source: &[u8]) -> String {
    node_text(node, source).split_whitespace().collect()
}

fn heritage_name<'a>(node: Node<'a>, source: &[u8]) -> Option<(String, Node<'a>)> {
    match node.kind() {
        "identifier" | "type_identifier" | "nested_type_identifier" => {
            Some((normalized_text(node, source), node))
        }
        "generic_type" | "expression_with_type_arguments" => {
            first_named_child(node).and_then(|child| heritage_name(child, source))
        }
        // Mixin / computed bases need semantic resolution, so PR2 skips them.
        "call_expression" | "new_expression" | "subscript_expression" => None,
        "type_arguments" => None,
        _ => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .find_map(|child| heritage_name(child, source))
        }
    }
}

fn is_builtin_type(text: &str) -> bool {
    matches!(
        text,
        "any"
            | "bigint"
            | "boolean"
            | "never"
            | "null"
            | "number"
            | "object"
            | "string"
            | "symbol"
            | "undefined"
            | "unknown"
            | "void"
    )
}

fn find_direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .find(|child| child.kind() == kind)
}

fn direct_children_by_kind<'a>(node: Node<'a>, kind: &str) -> Vec<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .filter(|child| child.kind() == kind)
        .collect()
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

fn last_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).last()
}

fn descendant_nodes<'a>(node: Node<'a>, kind: &'static str) -> Vec<Node<'a>> {
    let mut out = Vec::new();
    collect_descendants(node, kind, &mut out);
    out
}

fn collect_descendants<'a>(node: Node<'a>, kind: &'static str, out: &mut Vec<Node<'a>>) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == kind {
            out.push(child);
        }
        collect_descendants(child, kind, out);
    }
}

fn named_child_after_keyword<'a>(node: Node<'a>, source: &[u8], keyword: &str) -> Option<Node<'a>> {
    let mut saw_keyword = false;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if saw_keyword && child.is_named() {
            return Some(child);
        }
        if node_text(child, source) == keyword {
            saw_keyword = true;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_lang_api::LanguageBackend;

    fn semantic(src: &str) -> SemanticFacts {
        TypescriptAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn refs(src: &str) -> Vec<RefFact> {
        semantic(src).refs
    }

    fn impls(src: &str) -> Vec<ImplFact> {
        semantic(src).impls
    }

    fn call_ref<'a>(refs: &'a [RefFact], name: &str) -> &'a RefFact {
        refs.iter()
            .find(|r| r.kind == RefKind::Call && r.target_name == name)
            .unwrap()
    }

    #[test]
    fn emits_call_refs_with_function_enclosing() {
        let refs = refs("function caller() { foo(); }");
        let call = refs
            .iter()
            .find(|r| r.kind == RefKind::Call && r.target_name == "foo")
            .unwrap();
        assert_eq!(call.target_qualified.as_deref(), Some("foo"));
        assert_eq!(call.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn keeps_member_call_refs_unresolved() {
        let refs = refs("function caller() { ns.foo(); mod.ns.bar(); }");
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Call && r.target_name == "foo" && r.target_qualified.is_none()
        }));
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Call && r.target_name == "bar" && r.target_qualified.is_none()
        }));
    }

    #[test]
    fn keeps_receiver_method_calls_unresolved() {
        let refs = refs("function caller(obj: Obj) { obj.method(); }");
        let call = call_ref(&refs, "method");
        assert_eq!(call.target_qualified, None);
    }

    #[test]
    fn handles_optional_chaining_calls_unresolved() {
        let refs = refs("function caller(obj: Obj) { obj?.method(); obj?.deep?.other(); }");
        let method = call_ref(&refs, "method");
        let other = call_ref(&refs, "other");

        assert_eq!(method.target_qualified, None);
        assert_eq!(other.target_qualified, None);
    }

    #[test]
    fn handles_generic_callees_with_existing_call_policy() {
        let refs = refs("function caller(obj: Obj) { foo<T>(); obj.method<T>(); }");
        let bare = call_ref(&refs, "foo");
        let method = call_ref(&refs, "method");

        assert_eq!(bare.target_qualified.as_deref(), Some("foo"));
        assert_eq!(method.target_qualified, None);
    }

    #[test]
    fn handles_constructor_calls_as_call_refs() {
        let refs = refs("function caller() { new Foo(); new Box<T>(); new mod.Foo(); }");

        assert_eq!(
            call_ref(&refs, "Foo").target_qualified.as_deref(),
            Some("Foo")
        );
        assert_eq!(
            call_ref(&refs, "Box").target_qualified.as_deref(),
            Some("Box")
        );
        assert!(
            refs.iter().any(|r| {
                r.kind == RefKind::Call && r.target_name == "Foo" && r.target_qualified.is_none()
            }),
            "{refs:?}"
        );
    }

    #[test]
    fn namespace_import_member_call_stays_unresolved() {
        let refs = refs("import * as ns from './x';\nfunction caller() { ns.foo(); }");
        let call = call_ref(&refs, "foo");

        assert_eq!(call.target_qualified, None);
        assert_eq!(call.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn emits_class_method_enclosing() {
        let refs = refs("class C { m() { helper(); } }");
        let call = refs
            .iter()
            .find(|r| r.kind == RefKind::Call && r.target_name == "helper")
            .unwrap();
        assert_eq!(call.enclosing_qualified.as_deref(), Some("C.m"));
    }

    #[test]
    fn emits_parameter_return_field_alias_and_bound_type_refs() {
        let refs = refs(
            "class C<T extends Base> { field: Field; m(user: User): Result { return value; } }\n\
             type Alias = Target;",
        );
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Type
                && r.target_name == "Base"
                && r.type_role == Some(TypeRole::Bound)
        }));
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Type
                && r.target_name == "Field"
                && r.type_role == Some(TypeRole::Field)
        }));
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Type
                && r.target_name == "User"
                && r.type_role == Some(TypeRole::Param)
        }));
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Type
                && r.target_name == "Result"
                && r.type_role == Some(TypeRole::Return)
        }));
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Type
                && r.target_name == "Target"
                && r.type_role == Some(TypeRole::Alias)
        }));
    }

    #[test]
    fn preserves_dotted_type_refs() {
        let refs = refs("function f(x: ns.Type): mod.Result { return x; }");
        assert!(refs.iter().any(|r| {
            r.target_name == "Type" && r.target_qualified.as_deref() == Some("ns.Type")
        }));
        assert!(refs.iter().any(|r| {
            r.target_name == "Result" && r.target_qualified.as_deref() == Some("mod.Result")
        }));
    }

    #[test]
    fn leaves_non_heritage_fact_sets_empty() {
        let facts = semantic("import { Foo } from './foo'; class C {}");
        assert!(facts.imports.is_empty());
        assert!(facts.impls.is_empty());
        assert!(facts.doc_overrides.is_empty());
    }

    #[test]
    fn pure_import_file_emits_no_semantic_facts() {
        let facts = semantic("import { x } from './y';");

        assert!(facts.imports.is_empty());
        assert!(facts.refs.is_empty());
        assert!(facts.impls.is_empty());
        assert!(facts.doc_overrides.is_empty());
    }

    #[test]
    fn tsx_patterns_remain_out_of_scope() {
        let backend = crate::TypescriptBackend;

        assert_eq!(backend.file_patterns(), &["*.ts", "*.mts", "*.cts"]);
        assert!(!backend.file_patterns().contains(&"*.tsx"));
    }

    #[test]
    fn recovered_parse_keeps_refs_from_valid_regions() {
        let refs = refs(
            "function ok() { foo(); }\n\
             function broken() { let x: =; }\n\
             function alsoOk() { bar(); }\n",
        );
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Call
                && r.target_name == "foo"
                && r.enclosing_qualified.as_deref() == Some("ok")
        }));
        assert!(refs.iter().any(|r| {
            r.kind == RefKind::Call
                && r.target_name == "bar"
                && r.enclosing_qualified.as_deref() == Some("alsoOk")
        }));
    }

    #[test]
    fn recovered_parse_keeps_mixed_valid_shapes() {
        let facts = semantic(
            "module broken { interface Mid extends Base { value: ; } }\n\
             class Derived extends Parent { ok() { foo(); } broken() { let x: =; } }\n\
             function generic() { bad<; recovered<T>(); }\n",
        );

        assert!(facts.impls.iter().any(|i| {
            i.type_qualified == "Mid" && i.interface_qualified.as_deref() == Some("Base")
        }));
        assert!(facts.impls.iter().any(|i| {
            i.type_qualified == "Derived" && i.interface_qualified.as_deref() == Some("Parent")
        }));
        assert!(facts.refs.iter().any(|r| {
            r.kind == RefKind::Call
                && r.target_name == "foo"
                && r.enclosing_qualified.as_deref() == Some("Derived.ok")
        }));
        assert!(facts.refs.iter().any(|r| {
            r.kind == RefKind::Call
                && r.target_name == "recovered"
                && r.target_qualified.as_deref() == Some("recovered")
        }));
    }

    #[test]
    fn emits_class_extends_inheritance_edge() {
        let impls = impls(
            "class A {}\n\
             \n\
             class Dog extends Animal {}",
        );
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].type_qualified, "Dog");
        assert_eq!(impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(impls[0].kind, "inherit");
        assert_eq!(impls[0].line, 3);
    }

    #[test]
    fn emits_class_implements_edge() {
        let impls = impls("class Dog implements Pet {}");
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].type_qualified, "Dog");
        assert_eq!(impls[0].interface_qualified.as_deref(), Some("Pet"));
        assert_eq!(impls[0].kind, "implement");
    }

    #[test]
    fn emits_class_extends_and_multiple_implements_edges() {
        let impls = impls("class Dog extends Animal implements Pet, Walker {}");
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
                ("Dog", Some("Pet"), "implement"),
                ("Dog", Some("Walker"), "implement"),
            ]
        );
    }

    #[test]
    fn emits_interface_extends_edge() {
        let impls = impls("interface A extends B {}");
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].type_qualified, "A");
        assert_eq!(impls[0].interface_qualified.as_deref(), Some("B"));
        assert_eq!(impls[0].kind, "inherit");
    }

    #[test]
    fn emits_interface_multiple_extends_edges() {
        let impls = impls("interface A extends B, C {}");
        let bases: Vec<&str> = impls
            .iter()
            .map(|i| i.interface_qualified.as_deref().unwrap())
            .collect();
        assert_eq!(bases, vec!["B", "C"]);
        assert!(impls.iter().all(|i| i.type_qualified == "A"));
        assert!(impls.iter().all(|i| i.kind == "inherit"));
    }

    #[test]
    fn strips_generic_arguments_from_heritage_names() {
        let impls = impls("class Dog extends Animal<string> {}");
        assert_eq!(impls.len(), 1, "{impls:?}");
        assert_eq!(impls[0].interface_qualified.as_deref(), Some("Animal"));
    }

    #[test]
    fn skips_call_expression_mixin_bases() {
        let impls = impls("class C extends Mixin(Base) {}");
        assert!(impls.is_empty(), "{impls:?}");
    }

    #[test]
    fn namespace_class_heritage_matches_tier1_unqualified_container_limit() {
        let impls = impls("namespace ns { export class Dog extends Animal {} }");
        assert!(
            impls.iter().any(|i| {
                i.type_qualified == "Dog"
                    && i.interface_qualified.as_deref() == Some("Animal")
                    && i.kind == "inherit"
            }),
            "{impls:?}"
        );
    }

    #[test]
    fn recovered_parse_keeps_impls_from_valid_regions() {
        let impls = impls(
            "class Ok extends Base {}\n\
             function broken() { let x: =; }\n\
             interface AlsoOk extends Other {}\n",
        );
        assert!(impls.iter().any(|i| {
            i.type_qualified == "Ok" && i.interface_qualified.as_deref() == Some("Base")
        }));
        assert!(impls.iter().any(|i| {
            i.type_qualified == "AlsoOk" && i.interface_qualified.as_deref() == Some("Other")
        }));
    }

    #[test]
    fn empty_or_malformed_input_is_empty_ok() {
        assert_eq!(semantic("").refs.len(), 0);
        assert_eq!(semantic("function {").refs.len(), 0);
    }

    #[test]
    fn semantic_degrade_is_empty_facts_not_error() {
        // TypeScript Tier-2 intentionally never returns ExtractError:
        // parser setup/tree absence degrade to empty SemanticFacts, and
        // unsupported recovered syntax is skipped node-by-node. PR #48's
        // parse-pipeline preservation is therefore a no-op for TS today.
        let facts = TypescriptAnalyzer
            .extract_semantic(b"\0\0\0 function {")
            .unwrap();

        assert!(facts.imports.is_empty());
        assert!(facts.refs.is_empty());
        assert!(facts.impls.is_empty());
        assert!(facts.doc_overrides.is_empty());
    }
}
