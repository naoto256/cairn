//! `cairn-lang-csharp` — C# backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-c-sharp parse tree and emits
//! [`SymbolFact`]s for namespaces, classes, interfaces, structs,
//! records, enums, methods, constructors, properties, fields,
//! constants, events, delegates, plus [`ImportFact`]s for `using`
//! directives. Indexers, operators, and destructors are deliberately
//! out of scope for this slice.
//!
//! Tier-2 (semantic): the [`analyzer`] module re-walks the same grammar
//! for base-list inheritance edges, `using` refs, and same-file call /
//! `new` refs. See the module docs there for the kind conventions.
//!
//! `partial` types fall out naturally: every `class_declaration` node
//! emits its own symbol, so both halves of a partial class are indexed
//! (same qualified name, two rows).

#![forbid(unsafe_code)]

mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct CsharpBackend;

impl LanguageBackend for CsharpBackend {
    fn name(&self) -> &'static str {
        "csharp"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.cs"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-c-sharp"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_c_sharp::LANGUAGE.into();
        extract(source, &language, CsharpVisitor::new(source.len()))
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_CSHARP: fn() -> Box<dyn LanguageBackend> = || Box::new(CsharpBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct CsharpVisitor {
    nesting: NestingTracker,
    /// Total source length; a file-scoped namespace's declarations are
    /// *siblings* of the namespace node, so its container scope must
    /// extend to end-of-file rather than the node's own end byte.
    source_len: usize,
}

impl CsharpVisitor {
    fn new(source_len: usize) -> Self {
        Self {
            nesting: NestingTracker::new("."),
            source_len,
        }
    }
}

impl Visitor for CsharpVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        if node.kind() == "using_directive" {
            if let Some(import) = match_using(node, source) {
                facts.imports.push(import);
            }
            return;
        }

        // Multi-declarator members (`int a, b;`) emit one symbol per
        // declarator and need their own path.
        match node.kind() {
            "field_declaration" => {
                let kind = if has_modifier(node, source, "const") {
                    SymbolKind::Constant
                } else {
                    SymbolKind::Field
                };
                self.emit_declarators(node, source, facts, kind);
                return;
            }
            "event_field_declaration" => {
                self.emit_declarators(node, source, facts, SymbolKind::Other("event".into()));
                return;
            }
            _ => {}
        }

        let Some((kind, name, body_start)) = match_csharp_item(node, source) else {
            return;
        };

        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let doc = doc_from_preceding_comments(node, source);
        let visibility = Some(csharp_visibility(node, source));
        let parent_idx = self.nesting.current_parent();

        let idx = facts.symbols.len();
        let container = is_container(&kind);
        let node_kind = node.kind();
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind,
            signature,
            doc,
            visibility,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start,
            parent_idx,
        });

        if container {
            // File-scoped namespaces (`namespace Foo;`) end at the
            // semicolon but scope the whole rest of the file.
            let byte_end = if node_kind == "file_scoped_namespace_declaration" {
                self.source_len
            } else {
                node.end_byte()
            };
            self.nesting.push(idx, byte_end);
        }
    }
}

impl CsharpVisitor {
    /// `field_declaration` / `event_field_declaration` wrap a
    /// `variable_declaration` holding one or more `variable_declarator`s
    /// (`public int a, b;`). Emit one symbol per declared name.
    fn emit_declarators(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
        kind: SymbolKind,
    ) {
        let Some(var_decl) = first_child_of_kind(node, "variable_declaration") else {
            return;
        };
        let visibility = Some(csharp_visibility(node, source));
        let doc = doc_from_preceding_comments(node, source);
        let mut cursor = var_decl.walk();
        for declarator in var_decl.named_children(&mut cursor) {
            if declarator.kind() != "variable_declarator" {
                continue;
            }
            let Some(name_node) = child_by_field(declarator, "name") else {
                continue;
            };
            let name = node_text(name_node, source).to_string();
            facts.symbols.push(SymbolFact {
                qualified: self.nesting.qualified_for(&name, facts),
                name,
                kind: kind.clone(),
                signature: signature_slice(node, source, None),
                doc: doc.clone(),
                visibility,
                byte_range: node.byte_range(),
                line_range: line_of(node)..end_line_of(node),
                body_start: None,
                parent_idx: self.nesting.current_parent(),
            });
        }
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Namespace
            | SymbolKind::Class
            | SymbolKind::Interface
            | SymbolKind::Struct
            | SymbolKind::Enum
    )
}

fn match_csharp_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    let kind = match node.kind() {
        "namespace_declaration" | "file_scoped_namespace_declaration" => SymbolKind::Namespace,
        "class_declaration" => SymbolKind::Class,
        "interface_declaration" => SymbolKind::Interface,
        "struct_declaration" => SymbolKind::Struct,
        // `record struct S(...)` parses as a `record_declaration` with
        // a `struct` keyword token; plain `record` is class-like.
        "record_declaration" => {
            if has_keyword_child(node, "struct") {
                SymbolKind::Struct
            } else {
                SymbolKind::Class
            }
        }
        "enum_declaration" => SymbolKind::Enum,
        "method_declaration" => SymbolKind::Method,
        "constructor_declaration" => SymbolKind::Constructor,
        "property_declaration" => SymbolKind::Property,
        "delegate_declaration" => SymbolKind::Other("delegate".into()),
        _ => return None,
    };

    let name_node = child_by_field(node, "name")?;
    let name = node_text(name_node, source).to_string();

    let body_start = match node.kind() {
        // Auto-property body is the accessor list; an expression-bodied
        // property (`int X => …;`) uses the value expression instead.
        "property_declaration" => child_by_field(node, "accessors")
            .map(|n| n.start_byte())
            .or_else(|| child_by_field(node, "value").map(|n| n.start_byte())),
        _ => child_by_field(node, "body").map(|n| n.start_byte()),
    };

    Some((kind, name, body_start))
}

/// Access-modifier mapping. C# defaults differ by position (types
/// default to `internal`, members to `private`), but this slice
/// deliberately collapses "no access modifier" to `internal` →
/// [`Visibility::Crate`], matching the backend spec.
fn csharp_visibility(node: Node<'_>, source: &[u8]) -> Visibility {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "modifier" {
            continue;
        }
        match node_text(child, source) {
            "public" => return Visibility::Public,
            // `private protected` arrives as two separate modifier
            // nodes; the combo stays assembly-scoped (Crate), so bare
            // `private` only wins when `protected` is absent.
            "private" => {
                return if has_modifier(node, source, "protected") {
                    Visibility::Crate
                } else {
                    Visibility::Private
                };
            }
            "internal" | "protected" => return Visibility::Crate,
            _ => {}
        }
    }
    Visibility::Crate
}

fn has_modifier(node: Node<'_>, source: &[u8], text: &str) -> bool {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .any(|c| c.kind() == "modifier" && node_text(c, source) == text)
}

fn has_keyword_child(node: Node<'_>, keyword: &str) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .any(|c| !c.is_named() && c.kind() == keyword)
}

fn first_child_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).find(|c| c.kind() == kind)
}

/// `using System.Collections;` / `using F = System.Func<int>;` /
/// `using static System.Math;` / `global using X;`.
fn match_using(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    // The used namespace / type is the (single) named child that is a
    // type expression; an alias (`using F = …`) sits in the `name` field.
    let alias_id = child_by_field(node, "name").map(|n| n.id());
    let mut target: Option<Node<'_>> = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if matches!(
            child.kind(),
            "identifier" | "qualified_name" | "generic_name" | "alias_qualified_name"
        ) && alias_id != Some(child.id())
        {
            target = Some(child);
        }
    }
    let target = target?;
    let to_module = node_text(target, source).to_string();
    if to_module.is_empty() {
        return None;
    }

    let alias = child_by_field(node, "name").map(|n| node_text(n, source).to_string());
    // `using static` brings members into scope, like Go's dot import.
    let imported = has_keyword_child(node, "static").then(|| "*".to_string());

    Some(ImportFact {
        to_module,
        imported,
        alias,
        is_reexport: false,
        line: line_of(node),
    })
}

/// XML doc comments (`/// <summary>…`) and plain `//` runs directly
/// above a declaration. Same contiguity rules as the Go backend: a
/// blank line or non-comment sibling resets the run.
fn doc_from_preceding_comments(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| {
        (sibling.kind() == "comment").then(|| DocCommentPart::Append(strip_csharp_doc_marker(text)))
    })
}

fn strip_csharp_doc_marker(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix("///") {
        rest.trim().to_string()
    } else if let Some(rest) = trimmed.strip_prefix("//") {
        rest.trim().to_string()
    } else if let Some(inner) = trimmed
        .strip_prefix("/*")
        .and_then(|s| s.strip_suffix("*/"))
    {
        inner
            .lines()
            .map(|line| line.trim().trim_start_matches('*').trim())
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(src: &str) -> SyntacticFacts {
        CsharpBackend.extract_syntactic(src.as_bytes()).unwrap()
    }

    fn symbol<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("{name} missing"))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(CsharpBackend.parser_id(), "tree-sitter-c-sharp");
    }

    #[test]
    fn extracts_core_declaration_kinds() {
        let f = facts(
            r#"
namespace App.Core
{
    public class Widget
    {
        public int Count;
        public const int Max = 10;
        public string Name { get; set; }
        public event System.Action Changed;

        public Widget() {}

        /// <summary>Renders the widget.</summary>
        public string Render(int depth) { return ""; }
    }

    public interface IShape {}
    public struct Point {}
    public record Person(string Name);
    public record struct Coord(int X, int Y);
    public enum Color { Red, Green }
    public delegate int Op(int a, int b);
}
"#,
        );

        assert_eq!(symbol(&f, "App.Core").kind, SymbolKind::Namespace);
        assert_eq!(symbol(&f, "Widget").kind, SymbolKind::Class);
        assert_eq!(symbol(&f, "Count").kind, SymbolKind::Field);
        assert_eq!(symbol(&f, "Max").kind, SymbolKind::Constant);
        assert_eq!(symbol(&f, "Name").kind, SymbolKind::Property);
        assert_eq!(
            symbol(&f, "Changed").kind,
            SymbolKind::Other("event".into())
        );
        assert_eq!(symbol(&f, "Render").kind, SymbolKind::Method);
        assert_eq!(symbol(&f, "IShape").kind, SymbolKind::Interface);
        assert_eq!(symbol(&f, "Point").kind, SymbolKind::Struct);
        assert_eq!(symbol(&f, "Person").kind, SymbolKind::Class);
        assert_eq!(symbol(&f, "Coord").kind, SymbolKind::Struct);
        assert_eq!(symbol(&f, "Color").kind, SymbolKind::Enum);
        assert_eq!(symbol(&f, "Op").kind, SymbolKind::Other("delegate".into()));

        let ctor = f
            .symbols
            .iter()
            .find(|s| s.kind == SymbolKind::Constructor)
            .expect("constructor missing");
        assert_eq!(ctor.name, "Widget");
        assert_eq!(ctor.qualified, "App.Core.Widget.Widget");

        assert_eq!(symbol(&f, "Render").qualified, "App.Core.Widget.Render");
        assert!(
            symbol(&f, "Render")
                .doc
                .as_deref()
                .unwrap()
                .contains("Renders the widget")
        );
    }

    #[test]
    fn qualified_names_nest_through_classes_and_namespaces() {
        let f = facts("namespace N { class Outer { class Inner { void M() {} } } }");
        assert_eq!(symbol(&f, "Inner").qualified, "N.Outer.Inner");
        assert_eq!(symbol(&f, "M").qualified, "N.Outer.Inner.M");
        let inner = symbol(&f, "Inner");
        assert_eq!(f.symbols[inner.parent_idx.unwrap()].name, "Outer");
    }

    #[test]
    fn file_scoped_namespace_scopes_rest_of_file() {
        let f = facts("namespace App.Web;\n\npublic class Controller {}\n");
        assert_eq!(symbol(&f, "App.Web").kind, SymbolKind::Namespace);
        assert_eq!(symbol(&f, "Controller").qualified, "App.Web.Controller");
    }

    #[test]
    fn partial_class_emits_both_declarations() {
        let f = facts("partial class P { void A() {} }\npartial class P { void B() {} }\n");
        let halves: Vec<_> = f.symbols.iter().filter(|s| s.name == "P").collect();
        assert_eq!(halves.len(), 2);
        assert_eq!(symbol(&f, "A").qualified, "P.A");
        assert_eq!(symbol(&f, "B").qualified, "P.B");
    }

    #[test]
    fn generics_kept_in_signature_and_name_is_bare() {
        let f = facts(
            "public class Repo<T> where T : class { public T Get<U>(U key) { return default; } }",
        );
        assert_eq!(symbol(&f, "Repo").kind, SymbolKind::Class);
        assert!(
            symbol(&f, "Repo")
                .signature
                .as_deref()
                .unwrap()
                .contains("Repo<T>")
        );
        assert!(
            symbol(&f, "Get")
                .signature
                .as_deref()
                .unwrap()
                .contains("Get<U>(U key)")
        );
    }

    #[test]
    fn properties_auto_and_expression_bodied() {
        let f = facts("class C { public int Auto { get; set; } public int Expr => Auto * 2; }");
        let auto = symbol(&f, "Auto");
        assert_eq!(auto.kind, SymbolKind::Property);
        assert!(!auto.signature.as_deref().unwrap().contains("get;"));
        let expr = symbol(&f, "Expr");
        assert_eq!(expr.kind, SymbolKind::Property);
    }

    #[test]
    fn extension_method_is_plain_method() {
        let f =
            facts("public static class Ext { public static int Doubled(this int x) => x * 2; }");
        let m = symbol(&f, "Doubled");
        assert_eq!(m.kind, SymbolKind::Method);
        assert_eq!(m.qualified, "Ext.Doubled");
        assert!(m.signature.as_deref().unwrap().contains("this int x"));
    }

    #[test]
    fn attributes_do_not_break_extraction() {
        let f = facts(
            "[System.Serializable]\npublic class Tagged { [Obsolete(\"old\")] public void M() {} }",
        );
        assert_eq!(symbol(&f, "Tagged").kind, SymbolKind::Class);
        assert_eq!(symbol(&f, "M").kind, SymbolKind::Method);
    }

    #[test]
    fn visibility_from_modifiers_with_internal_default() {
        let f = facts(
            r#"
public class A {}
internal class B {}
class C {}
class D
{
    public int Pub;
    private int Priv;
    protected int Prot;
    internal int Int;
    protected internal int ProtInt;
    private protected int PrivProt;
    int NoMod;
}
"#,
        );
        assert_eq!(symbol(&f, "A").visibility, Some(Visibility::Public));
        assert_eq!(symbol(&f, "B").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "C").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "Pub").visibility, Some(Visibility::Public));
        assert_eq!(symbol(&f, "Priv").visibility, Some(Visibility::Private));
        assert_eq!(symbol(&f, "Prot").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "Int").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "ProtInt").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "PrivProt").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "NoMod").visibility, Some(Visibility::Crate));
    }

    #[test]
    fn multi_declarator_field_emits_each_name() {
        let f = facts("class C { public int a, b; }");
        assert_eq!(symbol(&f, "a").kind, SymbolKind::Field);
        assert_eq!(symbol(&f, "b").kind, SymbolKind::Field);
        assert_eq!(symbol(&f, "b").qualified, "C.b");
    }

    #[test]
    fn extracts_using_directives() {
        let f = facts(
            "using System;\nusing System.Collections.Generic;\nusing F = System.Func<int>;\nusing static System.Math;\nglobal using App.Core;\n",
        );
        assert_eq!(f.imports.len(), 5);
        assert_eq!(f.imports[0].to_module, "System");
        assert_eq!(f.imports[1].to_module, "System.Collections.Generic");
        assert_eq!(f.imports[2].alias.as_deref(), Some("F"));
        assert_eq!(f.imports[2].to_module, "System.Func<int>");
        assert_eq!(f.imports[3].to_module, "System.Math");
        assert_eq!(f.imports[3].imported.as_deref(), Some("*"));
        assert_eq!(f.imports[4].to_module, "App.Core");
        assert!(f.imports.iter().all(|i| !i.is_reexport));
    }

    #[test]
    fn ignores_local_variables() {
        let f = facts("class C { void M() { int local = 1; const int LC = 2; } }");
        assert!(f.symbols.iter().all(|s| s.name != "local"));
        assert!(f.symbols.iter().all(|s| s.name != "LC"));
    }
}
