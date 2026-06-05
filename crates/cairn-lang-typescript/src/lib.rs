//! `cairn-lang-typescript` — TypeScript backend.
//!
//! Tier-1 (syntactic): walks the tree-sitter-typescript parse tree and emits
//! symbols for declarations plus import facts. `.tsx` uses a separate grammar
//! and is intentionally left for a follow-up backend.

#![forbid(unsafe_code)]

mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    NestingTracker, Visitor, child_by_field, end_line_of, extract, line_of, node_text,
    signature_slice, truncate,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct TypescriptBackend;

impl LanguageBackend for TypescriptBackend {
    fn name(&self) -> &'static str {
        "typescript"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.ts", "*.mts", "*.cts"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-typescript"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into();
        extract(source, &language, TypescriptVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_TYPESCRIPT: fn() -> Box<dyn LanguageBackend> = || Box::new(TypescriptBackend);

struct TypescriptVisitor {
    nesting: NestingTracker,
}

impl TypescriptVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
        }
    }
}

impl Visitor for TypescriptVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        if node.kind() == "import_statement" {
            extract_imports(node, source, facts);
            return;
        }

        let Some((mut kind, name, body_start)) = match_typescript_item(node, source) else {
            return;
        };

        if matches!(kind, SymbolKind::Function)
            && matches!(self.nesting.parent_kind(facts), Some(SymbolKind::Class))
        {
            kind = SymbolKind::Method;
        }

        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = typescript_visibility(node, source);
        let doc = extract_jsdoc(node, source);
        let parent_idx = self.nesting.current_parent();

        let idx = facts.symbols.len();
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind: kind.clone(),
            signature,
            doc,
            visibility,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start,
            parent_idx,
        });

        if is_container(&kind) {
            self.nesting.push(idx, node.end_byte());
        }
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Class | SymbolKind::Interface | SymbolKind::Enum
    )
}

fn match_typescript_item(
    node: Node<'_>,
    source: &[u8],
) -> Option<(SymbolKind, String, Option<usize>)> {
    match node.kind() {
        "function_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Function,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "method_definition" | "method_signature" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Method,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "class_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Class, node_text(name, source).to_string(), body))
        }
        "interface_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Interface,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "type_alias_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::TypeAlias,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "enum_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Enum, node_text(name, source).to_string(), body))
        }
        _ => None,
    }
}

fn typescript_visibility(node: Node<'_>, source: &[u8]) -> Option<Visibility> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "accessibility_modifier" {
            return match node_text(child, source) {
                "public" => Some(Visibility::Public),
                "private" => Some(Visibility::Private),
                "protected" => Some(Visibility::Crate),
                _ => None,
            };
        }
    }
    None
}

fn extract_jsdoc(node: Node<'_>, source: &[u8]) -> Option<String> {
    let parent = node.parent()?;
    let mut cursor = parent.walk();
    let mut last_doc: Option<String> = None;

    for sibling in parent.children(&mut cursor) {
        if sibling.start_byte() >= node.start_byte() {
            break;
        }
        if sibling.kind() == "comment" {
            let text = node_text(sibling, source);
            if text.trim_start().starts_with("/**") {
                last_doc = Some(strip_jsdoc_markers(text));
            } else {
                last_doc = None;
            }
        } else if !sibling.is_extra() {
            last_doc = None;
        }
    }

    last_doc.filter(|doc| !doc.is_empty())
}

fn strip_jsdoc_markers(text: &str) -> String {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix("/**")
        .and_then(|s| s.strip_suffix("*/"))
        .unwrap_or(trimmed);

    let lines: Vec<&str> = inner
        .lines()
        .map(|line| {
            line.trim()
                .strip_prefix('*')
                .map(str::trim_start)
                .unwrap_or_else(|| line.trim())
        })
        .filter(|line| !line.is_empty())
        .collect();
    truncate(&lines.join("\n"), 1024)
}

fn extract_imports(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let Some(source_node) = child_by_field(node, "source") else {
        return;
    };
    let to_module = strip_string_literal(node_text(source_node, source));
    let line = line_of(node);

    // tree-sitter-typescript shape:
    //   import_statement
    //     ├─ "type"? (type-only modifier — handled transparently)
    //     ├─ import_clause?
    //     │   ├─ identifier             (default binding)
    //     │   ├─ named_imports          ({ a, b as c })
    //     │   │   └─ import_specifier+
    //     │   └─ namespace_import       (* as ns)
    //     └─ source = string
    //
    // Each binding form (default / named / namespace) emits an independent
    // `ImportFact`; the previous implementation guarded the default emit
    // on "no named found", which dropped the default in mixed forms like
    // `import React, { useState } from 'react'`.
    let mut emitted_any = false;

    if let Some(clause) = find_child_by_kind(node, "import_clause") {
        // 1. default binding — direct `identifier` child of import_clause
        if let Some(default_alias) = default_alias_in_clause(clause, source) {
            facts.imports.push(ImportFact {
                to_module: to_module.clone(),
                imported: Some("default".to_string()),
                alias: Some(default_alias),
                is_reexport: false,
                line,
            });
            emitted_any = true;
        }

        // 2. named + namespace
        let mut cursor = clause.walk();
        for child in clause.children(&mut cursor) {
            match child.kind() {
                "named_imports" => {
                    let mut nc = child.walk();
                    for spec in child.children(&mut nc) {
                        if spec.kind() != "import_specifier" {
                            continue;
                        }
                        if let Some((imported, alias)) = import_specifier_parts(spec, source) {
                            facts.imports.push(ImportFact {
                                to_module: to_module.clone(),
                                imported: Some(imported),
                                alias,
                                is_reexport: false,
                                line,
                            });
                            emitted_any = true;
                        }
                    }
                }
                "namespace_import" => {
                    if let Some(alias) = namespace_alias(child, source) {
                        facts.imports.push(ImportFact {
                            to_module: to_module.clone(),
                            imported: Some("*".to_string()),
                            alias: Some(alias),
                            is_reexport: false,
                            line,
                        });
                        emitted_any = true;
                    }
                }
                _ => {}
            }
        }
    }

    // Side-effect-only: `import "./styles.css"` — no clause, or a clause
    // that didn't yield any concrete binding.
    if !emitted_any {
        facts.imports.push(ImportFact {
            to_module,
            imported: None,
            alias: None,
            is_reexport: false,
            line,
        });
    }
}

fn find_child_by_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

fn default_alias_in_clause(clause: Node<'_>, source: &[u8]) -> Option<String> {
    // The default binding is the direct `identifier` child of
    // `import_clause`. Identifiers nested inside `named_imports` /
    // `namespace_import` belong to those forms and must not be picked up.
    let mut cursor = clause.walk();
    for child in clause.children(&mut cursor) {
        if child.kind() == "identifier" {
            return Some(node_text(child, source).to_string());
        }
    }
    None
}

fn namespace_alias(node: Node<'_>, source: &[u8]) -> Option<String> {
    child_by_field(node, "name")
        .map(|n| node_text(n, source).to_string())
        .or_else(|| last_identifier(node, source))
}

fn import_specifier_parts(node: Node<'_>, source: &[u8]) -> Option<(String, Option<String>)> {
    let name = child_by_field(node, "name")?;
    let imported = node_text(name, source).to_string();
    let alias = child_by_field(node, "alias").map(|n| node_text(n, source).to_string());
    Some((imported, alias))
}

fn last_identifier(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut out = None;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" || child.kind() == "type_identifier" {
            out = Some(node_text(child, source).to_string());
        } else {
            out = last_identifier(child, source).or(out);
        }
    }
    out
}

fn strip_string_literal(text: &str) -> String {
    let trimmed = text.trim();
    for quote in ['"', '\'', '`'] {
        if let Some(inner) = trimmed
            .strip_prefix(quote)
            .and_then(|s| s.strip_suffix(quote))
        {
            return inner.to_string();
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol_by_name<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts.symbols.iter().find(|s| s.name == name).unwrap()
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(TypescriptBackend.parser_id(), "tree-sitter-typescript");
    }

    #[test]
    fn extracts_function_class_interface_type_alias_and_enum() {
        let src = br#"
/** doc on f */
function f(x: number): string { return ""; }
class C {
    m(): void {}
}
interface I { x: number; }
type T = string;
enum E { A, B }
"#;
        let facts = TypescriptBackend.extract_syntactic(src).unwrap();

        assert_eq!(symbol_by_name(&facts, "f").kind, SymbolKind::Function);
        assert_eq!(symbol_by_name(&facts, "C").kind, SymbolKind::Class);
        assert_eq!(symbol_by_name(&facts, "m").kind, SymbolKind::Method);
        assert_eq!(symbol_by_name(&facts, "I").kind, SymbolKind::Interface);
        assert_eq!(symbol_by_name(&facts, "T").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol_by_name(&facts, "E").kind, SymbolKind::Enum);
        assert_eq!(symbol_by_name(&facts, "f").doc.as_deref(), Some("doc on f"));
        assert_eq!(
            symbol_by_name(&facts, "f").signature.as_deref(),
            Some("function f(x: number): string")
        );
    }

    #[test]
    fn extracts_imports() {
        let src = br#"
import { foo, bar as baz } from "./mod";
import * as ns from "x";
import type { T } from "y";
"#;
        let facts = TypescriptBackend.extract_syntactic(src).unwrap();

        assert_eq!(facts.imports.len(), 4);
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./mod" && i.imported.as_deref() == Some("foo") && i.alias.is_none()
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./mod"
                && i.imported.as_deref() == Some("bar")
                && i.alias.as_deref() == Some("baz")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "x"
                && i.imported.as_deref() == Some("*")
                && i.alias.as_deref() == Some("ns")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "y" && i.imported.as_deref() == Some("T") && i.alias.is_none()
        }));
    }

    #[test]
    fn extracts_default_only_import() {
        let facts = TypescriptBackend
            .extract_syntactic(b"import React from \"react\";\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "react");
        assert_eq!(i.imported.as_deref(), Some("default"));
        assert_eq!(i.alias.as_deref(), Some("React"));
    }

    #[test]
    fn extracts_default_and_named_imports() {
        // Regression: previously the default binding was dropped whenever a
        // named import was present in the same statement.
        let facts = TypescriptBackend
            .extract_syntactic(b"import React, { useState, useEffect as ue } from \"react\";\n")
            .unwrap();

        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("React")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react" && i.imported.as_deref() == Some("useState") && i.alias.is_none()
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("useEffect")
                && i.alias.as_deref() == Some("ue")
        }));
        assert_eq!(facts.imports.len(), 3);
    }

    #[test]
    fn extracts_default_and_namespace_imports() {
        let facts = TypescriptBackend
            .extract_syntactic(b"import React, * as ReactNS from \"react\";\n")
            .unwrap();

        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("React")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("*")
                && i.alias.as_deref() == Some("ReactNS")
        }));
        assert_eq!(facts.imports.len(), 2);
    }

    #[test]
    fn extracts_side_effect_only_import() {
        let facts = TypescriptBackend
            .extract_syntactic(b"import \"./styles.css\";\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "./styles.css");
        assert!(i.imported.is_none());
        assert!(i.alias.is_none());
    }

    #[test]
    fn nested_class_method_qualified_name() {
        let facts = TypescriptBackend
            .extract_syntactic(b"class A { b(): void {} }")
            .unwrap();
        let method = symbol_by_name(&facts, "b");
        assert_eq!(method.qualified, "A.b");
        assert_eq!(method.kind, SymbolKind::Method);
    }
}
