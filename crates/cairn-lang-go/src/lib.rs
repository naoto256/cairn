//! `cairn-lang-go` — Go backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-go parse tree and emits
//! [`SymbolFact`]s for functions, methods, named types, top-level
//! constants, top-level variables, and import declarations. Tier-2
//! semantic enrichment is deliberately reserved for a future gopls
//! integration.

#![forbid(unsafe_code)]

use cairn_lang_api::{
    ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct GoBackend;

impl LanguageBackend for GoBackend {
    fn name(&self) -> &'static str {
        "go"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.go"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-go"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_go::LANGUAGE.into();
        extract(source, &language, GoVisitor::new())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_GO: fn() -> Box<dyn LanguageBackend> = || Box::new(GoBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct GoVisitor {
    nesting: NestingTracker,
}

impl GoVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
        }
    }
}

impl Visitor for GoVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        if node.kind() == "import_spec" {
            if let Some(import) = match_import(node, source) {
                facts.imports.push(import);
            }
            return;
        }

        match node.kind() {
            "const_spec" if is_top_level_value_spec(node) => {
                emit_named_specs(node, source, facts, SymbolKind::Constant);
            }
            "var_spec" if is_top_level_value_spec(node) => {
                emit_named_specs(node, source, facts, SymbolKind::Variable);
            }
            _ => {
                let Some((kind, name, qualified, body_start)) = match_go_item(node, source) else {
                    return;
                };
                emit_symbol(node, source, facts, kind, name, qualified, body_start);
            }
        }
    }
}

fn match_go_item(
    node: Node<'_>,
    source: &[u8],
) -> Option<(SymbolKind, String, String, Option<usize>)> {
    match node.kind() {
        "function_declaration" => {
            let name = child_by_field(node, "name")?;
            let name = node_text(name, source).to_string();
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Function, name.clone(), name, body))
        }
        "method_declaration" => {
            let name = child_by_field(node, "name")?;
            let name = node_text(name, source).to_string();
            let receiver = child_by_field(node, "receiver")
                .and_then(|n| receiver_type_name(n, source))
                .unwrap_or_default();
            let qualified = if receiver.is_empty() {
                name.clone()
            } else {
                format!("{receiver}.{name}")
            };
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Method, name, qualified, body))
        }
        "type_spec" | "type_alias" => {
            let name = child_by_field(node, "name")?;
            let name = node_text(name, source).to_string();
            let ty = child_by_field(node, "type")?;
            let kind = match ty.kind() {
                "struct_type" => SymbolKind::Struct,
                "interface_type" => SymbolKind::Interface,
                _ => SymbolKind::TypeAlias,
            };
            let body = match kind {
                SymbolKind::Struct | SymbolKind::Interface => Some(ty.start_byte()),
                _ => None,
            };
            Some((kind, name.clone(), name, body))
        }
        _ => None,
    }
}

fn emit_named_specs(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts, kind: SymbolKind) {
    let mut cursor = node.walk();
    for child in node.children_by_field_name("name", &mut cursor) {
        if !child.is_named() {
            continue;
        }
        let name = node_text(child, source).to_string();
        emit_symbol(node, source, facts, kind.clone(), name.clone(), name, None);
    }
}

fn emit_symbol(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    kind: SymbolKind,
    name: String,
    qualified: String,
    body_start: Option<usize>,
) {
    let signature = signature_slice(node, source, body_start);
    let doc = extract_doc(node, source);
    let parent_idx = None;
    let visibility = Some(go_visibility(&name));

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
}

fn go_visibility(name: &str) -> Visibility {
    if name
        .as_bytes()
        .first()
        .is_some_and(|first| first.is_ascii_uppercase())
    {
        Visibility::Public
    } else {
        Visibility::Private
    }
}

fn match_import(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let path = child_by_field(node, "path")?;
    let to_module = strip_go_string(node_text(path, source));
    if to_module.is_empty() {
        return None;
    }

    let alias = child_by_field(node, "name").map(|n| node_text(n, source).to_string());
    let imported = if alias.as_deref() == Some(".") {
        Some("*".to_string())
    } else {
        default_imported_name(&to_module)
    };

    Some(ImportFact {
        to_module,
        imported,
        alias,
        is_reexport: false,
        line: line_of(node),

        byte_range: None,
    })
}

fn default_imported_name(to_module: &str) -> Option<String> {
    to_module
        .rsplit('/')
        .find(|part| !part.is_empty())
        .map(std::string::ToString::to_string)
}

fn strip_go_string(text: &str) -> String {
    let trimmed = text.trim();
    for delim in ['"', '`'] {
        if let Some(inner) = trimmed
            .strip_prefix(delim)
            .and_then(|s| s.strip_suffix(delim))
        {
            return inner.to_string();
        }
    }
    trimmed.to_string()
}

fn receiver_type_name(receiver: Node<'_>, source: &[u8]) -> Option<String> {
    let mut cursor = receiver.walk();
    for child in receiver.named_children(&mut cursor) {
        if child.kind() == "parameter_declaration"
            || child.kind() == "variadic_parameter_declaration"
        {
            let ty = child_by_field(child, "type")?;
            return Some(base_type_name(ty, source));
        }
    }
    None
}

fn base_type_name(node: Node<'_>, source: &[u8]) -> String {
    match node.kind() {
        "pointer_type" => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .next()
                .map(|n| base_type_name(n, source))
                .unwrap_or_default()
        }
        "qualified_type" => child_by_field(node, "name")
            .map(|n| node_text(n, source).to_string())
            .unwrap_or_else(|| node_text(node, source).trim().to_string()),
        "generic_type" => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .next()
                .map(|n| base_type_name(n, source))
                .unwrap_or_default()
        }
        _ => node_text(node, source).trim().to_string(),
    }
}

fn is_top_level_value_spec(node: Node<'_>) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    if parent.kind() == "const_declaration" || parent.kind() == "var_declaration" {
        return parent
            .parent()
            .is_some_and(|grand| grand.kind() == "source_file");
    }
    if parent.kind() == "var_spec_list" {
        return parent
            .parent()
            .and_then(|decl| decl.parent())
            .is_some_and(|grand| grand.kind() == "source_file");
    }
    false
}

fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    doc_from_preceding_comments(node, source).or_else(|| {
        node.parent().and_then(|parent| match parent.kind() {
            "type_declaration" | "const_declaration" | "var_declaration" => {
                doc_from_preceding_comments(parent, source)
            }
            _ => None,
        })
    })
}

fn doc_from_preceding_comments(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| {
        (sibling.kind() == "comment").then(|| DocCommentPart::Append(strip_go_doc_marker(text)))
    })
}

fn strip_go_doc_marker(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix("//") {
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

    fn symbol<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("{name} missing"))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(GoBackend.parser_id(), "tree-sitter-go");
    }

    #[test]
    fn extracts_function_method_struct_interface_typealias_const_var() {
        let src = br#"
package main

// doc on F
func F(x int) string { return "" }

type S struct { x int }

// doc on m
func (s *S) m() {}

type I interface { F(int) string }

type T = string

const C = 42

var V = 0
"#;
        let facts = GoBackend.extract_syntactic(src).unwrap();

        assert_eq!(symbol(&facts, "F").kind, SymbolKind::Function);
        assert_eq!(symbol(&facts, "S").kind, SymbolKind::Struct);
        assert_eq!(symbol(&facts, "m").kind, SymbolKind::Method);
        assert_eq!(symbol(&facts, "m").qualified, "S.m");
        assert_eq!(symbol(&facts, "I").kind, SymbolKind::Interface);
        assert_eq!(symbol(&facts, "T").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&facts, "C").kind, SymbolKind::Constant);
        assert_eq!(symbol(&facts, "V").kind, SymbolKind::Variable);
        assert!(
            symbol(&facts, "F")
                .doc
                .as_deref()
                .unwrap()
                .contains("doc on F")
        );
        assert!(
            symbol(&facts, "m")
                .doc
                .as_deref()
                .unwrap()
                .contains("doc on m")
        );
    }

    #[test]
    fn visibility_follows_go_export_capitalization() {
        let src = br#"
package main

func Foo() {}
func bar() {}

type Animal struct {}
type animal struct {}

type Recv struct {}
func (r *Recv) Method() {}
func (r *Recv) method() {}

const ConstName = 1
const constName = 2

var VarName = 3
var varName = 4
"#;
        let facts = GoBackend.extract_syntactic(src).unwrap();

        assert_eq!(symbol(&facts, "Foo").visibility, Some(Visibility::Public));
        assert_eq!(symbol(&facts, "bar").visibility, Some(Visibility::Private));
        assert_eq!(
            symbol(&facts, "Animal").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "animal").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "Method").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "method").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "ConstName").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "constName").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "VarName").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "varName").visibility,
            Some(Visibility::Private)
        );
    }

    #[test]
    fn extracts_imports_including_alias_dot_blank() {
        let src = br#"
package main

import (
    "fmt"
    f "fmt"
    . "fmt"
    _ "fmt"
)
"#;
        let facts = GoBackend.extract_syntactic(src).unwrap();
        assert_eq!(facts.imports.len(), 4);

        assert_eq!(facts.imports[0].to_module, "fmt");
        assert_eq!(facts.imports[0].imported.as_deref(), Some("fmt"));
        assert_eq!(facts.imports[0].alias, None);
        assert_eq!(facts.imports[1].alias.as_deref(), Some("f"));
        assert_eq!(facts.imports[2].imported.as_deref(), Some("*"));
        assert_eq!(facts.imports[2].alias.as_deref(), Some("."));
        assert_eq!(facts.imports[3].alias.as_deref(), Some("_"));
        assert!(facts.imports.iter().all(|i| !i.is_reexport));
    }

    #[test]
    fn ignores_function_local_const_and_var() {
        let src = br#"
package main

func F() {
    const C = 1
    var V = 2
}
"#;
        let facts = GoBackend.extract_syntactic(src).unwrap();
        assert!(facts.symbols.iter().all(|s| s.name != "C"));
        assert!(facts.symbols.iter().all(|s| s.name != "V"));
    }
}
