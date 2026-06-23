//! Tree-sitter walker that collects call and import sites from TypeScript,
//! JavaScript, and TSX sources. Shared by all three Tier-3 analyzers in this
//! crate.

use std::collections::HashSet;

use cairn_core::lsp::Position;
use cairn_core::workspace_analyzer::DefinitionSite;
use cairn_core::{Error, Result};
use tree_sitter::{Node, Parser};

use crate::TsLanguage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SiteKind {
    Call,
    Import,
}

pub(crate) fn collect_calls(source: &[u8], language: TsLanguage) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, language, SiteKind::Call)
}

pub(crate) fn collect_imports(source: &[u8], language: TsLanguage) -> Result<Vec<DefinitionSite>> {
    collect_sites(source, language, SiteKind::Import)
}

fn collect_sites(
    source: &[u8],
    language: TsLanguage,
    site_kind: SiteKind,
) -> Result<Vec<DefinitionSite>> {
    let mut parser = Parser::new();
    parser
        .set_language(&(language.tree_sitter_language)())
        .map_err(|e| Error::InvalidArgument(format!("tree-sitter {}: {e}", language.language)))?;
    let tree = parser.parse(source, None).ok_or_else(|| {
        Error::InvalidArgument(format!("tree-sitter {} parse failed", language.language))
    })?;
    let mut collector = TsSiteCollector::new(source, site_kind);
    collector.walk(tree.root_node());
    Ok(collector.out)
}

#[derive(Debug, Default)]
struct LexicalScope {
    bindings: HashSet<String>,
    skip_local_calls: bool,
}

struct TsSiteCollector<'a> {
    source: &'a [u8],
    site_kind: SiteKind,
    out: Vec<DefinitionSite>,
    scopes: Vec<LexicalScope>,
}

impl<'a> TsSiteCollector<'a> {
    fn new(source: &'a [u8], site_kind: SiteKind) -> Self {
        Self {
            source,
            site_kind,
            out: Vec::new(),
            scopes: vec![LexicalScope::default()],
        }
    }

    fn walk(&mut self, node: Node<'_>) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            "import_statement" => {
                self.collect_import_bindings(node);
                self.emit_import(node);
                self.walk_children(node);
            }
            "variable_declarator" => {
                self.collect_variable_binding(node);
                self.walk_children(node);
            }
            "for_in_statement" | "for_of_statement" => {
                self.collect_for_binding(node);
                self.walk_children(node);
            }
            "function_declaration" | "class_declaration" => {
                self.collect_declaration_name(node);
                if node.kind() == "function_declaration" {
                    self.walk_callable(node);
                } else {
                    self.walk_children(node);
                }
            }
            "function" | "function_expression" | "method_definition" | "arrow_function" => {
                self.walk_callable(node);
            }
            "call_expression" => {
                self.emit_call(node);
                self.walk_children(node);
            }
            _ => {
                self.emit_import(node);
                self.walk_children(node);
            }
        }
    }

    fn walk_children(&mut self, node: Node<'_>) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.walk(child);
        }
    }

    fn walk_callable(&mut self, node: Node<'_>) {
        self.scopes.push(LexicalScope {
            bindings: HashSet::new(),
            skip_local_calls: true,
        });
        if let Some(parameters) = node
            .child_by_field_name("parameters")
            .or_else(|| direct_child(node, "formal_parameters"))
        {
            self.collect_binding_pattern(parameters);
        }
        self.walk_children(node);
        self.scopes.pop();
    }

    fn emit_call(&mut self, node: Node<'_>) {
        if self.site_kind != SiteKind::Call {
            return;
        }
        let Some(identifier) = call_identifier(node) else {
            return;
        };
        // Local bindings resolve to positions inside their enclosing
        // function/component. Persisting those LSP locations would round
        // the target up to that enclosing symbol, making React setters
        // like `setValue()` look like calls to the component itself.
        if is_bare_identifier_call(node)
            && identifier_text(identifier, self.source)
                .is_some_and(|name| self.is_skipped_local_binding(name))
        {
            return;
        }
        self.out.push(site_from_node(identifier, 0, 0));
    }

    fn emit_import(&mut self, node: Node<'_>) {
        if self.site_kind != SiteKind::Import || !is_import_like(node.kind()) {
            return;
        }
        if let Some(source_node) = import_source_node(node) {
            self.out.push(string_content_site(source_node, self.source));
        }
    }

    fn collect_variable_binding(&mut self, node: Node<'_>) {
        if let Some(name) = node.child_by_field_name("name") {
            self.collect_binding_pattern(name);
        }
    }

    fn collect_for_binding(&mut self, node: Node<'_>) {
        if let Some(left) = node.child_by_field_name("left") {
            self.collect_binding_pattern(left);
        }
    }

    fn collect_declaration_name(&mut self, node: Node<'_>) {
        if let Some(name) = node.child_by_field_name("name") {
            self.collect_binding_pattern(name);
        }
    }

    fn collect_import_bindings(&mut self, node: Node<'_>) {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if !is_string_literal(child.kind()) {
                self.collect_binding_pattern(child);
            }
        }
    }

    fn collect_binding_pattern(&mut self, node: Node<'_>) {
        match node.kind() {
            "identifier"
            | "shorthand_property_identifier"
            | "shorthand_property_identifier_pattern" => {
                if let Some(name) = identifier_text(node, self.source) {
                    self.insert_binding(name);
                }
            }
            "pair_pattern" => {
                if let Some(value) = node.child_by_field_name("value") {
                    self.collect_binding_pattern(value);
                } else if let Some(key) = node.child_by_field_name("key") {
                    self.collect_binding_pattern(key);
                }
            }
            "type_annotation" | "type_identifier" | "predefined_type" => {}
            _ => {
                let mut cursor = node.walk();
                for child in node.named_children(&mut cursor) {
                    self.collect_binding_pattern(child);
                }
            }
        }
    }

    fn insert_binding(&mut self, name: &str) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.bindings.insert(name.to_string());
        }
    }

    fn is_skipped_local_binding(&self, name: &str) -> bool {
        self.scopes
            .iter()
            .rev()
            .any(|scope| scope.skip_local_calls && scope.bindings.contains(name))
    }
}

fn call_identifier(call: Node<'_>) -> Option<Node<'_>> {
    let function = call.child_by_field_name("function")?;
    match function.kind() {
        "identifier" | "property_identifier" => Some(function),
        "member_expression" | "subscript_expression" | "optional_chain" => {
            function.child_by_field_name("property")
        }
        "call_expression" => call_identifier(function),
        _ => last_identifier_child(function),
    }
}

fn is_bare_identifier_call(call: Node<'_>) -> bool {
    call.child_by_field_name("function")
        .is_some_and(|function| function.kind() == "identifier")
}

fn last_identifier_child(node: Node<'_>) -> Option<Node<'_>> {
    if matches!(
        node.kind(),
        "identifier" | "property_identifier" | "shorthand_property_identifier"
    ) {
        return Some(node);
    }
    let mut cursor = node.walk();
    let children = node.named_children(&mut cursor).collect::<Vec<_>>();
    children.into_iter().rev().find_map(last_identifier_child)
}

fn identifier_text<'a>(node: Node<'_>, source: &'a [u8]) -> Option<&'a str> {
    matches!(
        node.kind(),
        "identifier" | "shorthand_property_identifier" | "shorthand_property_identifier_pattern"
    )
    .then(|| node.utf8_text(source).ok())
    .flatten()
}

fn direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .find(|child| child.kind() == kind)
}

fn is_import_like(kind: &str) -> bool {
    matches!(
        kind,
        "import_statement" | "export_statement" | "internal_module"
    )
}

fn import_source_node(node: Node<'_>) -> Option<Node<'_>> {
    if let Some(source) = node.child_by_field_name("source") {
        return Some(source);
    }
    // Tree-sitter's TS/JS grammars expose import/export sources consistently
    // today, but the fallback keeps older grammar shapes and re-export forms
    // from silently losing import refs if the field name is absent.
    find_first_string_child(node)
}

fn find_first_string_child(node: Node<'_>) -> Option<Node<'_>> {
    if is_string_literal(node.kind()) {
        return Some(node);
    }
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find_map(find_first_string_child)
}

fn is_string_literal(kind: &str) -> bool {
    matches!(kind, "string" | "string_fragment")
}

fn string_content_site(node: Node<'_>, source: &[u8]) -> DefinitionSite {
    let raw = node.utf8_text(source).unwrap_or_default();
    let offset = usize::from(
        raw.len() >= 2
            && ((raw.starts_with('"') && raw.ends_with('"'))
                || (raw.starts_with('\'') && raw.ends_with('\''))),
    );
    site_from_node(node, offset, offset)
}

fn site_from_node(
    node: Node<'_>,
    byte_start_offset: usize,
    byte_end_trim: usize,
) -> DefinitionSite {
    let start = node.start_position();
    DefinitionSite {
        position: Position {
            line: u32::try_from(start.row).unwrap_or(u32::MAX),
            character: u32::try_from(start.column.saturating_add(byte_start_offset))
                .unwrap_or(u32::MAX),
        },
        byte_start: node.start_byte().saturating_add(byte_start_offset),
        byte_end: node.end_byte().saturating_sub(byte_end_trim),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JS_LANGUAGE, TS_LANGUAGE, TSX_LANGUAGE};

    #[test]
    fn ts_collectors_find_calls_and_imports() {
        let source = br#"import { helper } from "./dep";
export { thing } from "./other";
function main() { return helper(); }
"#;

        let calls = collect_calls(source, TS_LANGUAGE).unwrap();
        let imports = collect_imports(source, TS_LANGUAGE).unwrap();

        assert_eq!(calls.len(), 1);
        assert_eq!(source_text(source, calls[0]), "helper");
        assert_eq!(imports.len(), 2);
        assert_eq!(source_text(source, imports[0]), "./dep");
        assert_eq!(source_text(source, imports[1]), "./other");
    }

    #[test]
    fn js_collector_finds_member_calls_and_imports() {
        let source = br#"import util from "./util.js";
export * from "./reexport.js";
client.render();
"#;

        let calls = collect_calls(source, JS_LANGUAGE).unwrap();
        let imports = collect_imports(source, JS_LANGUAGE).unwrap();

        assert_eq!(source_text(source, calls[0]), "render");
        assert_eq!(source_text(source, imports[0]), "./util.js");
        assert_eq!(source_text(source, imports[1]), "./reexport.js");
    }

    #[test]
    fn tsx_collector_keeps_typescript_calls_alive() {
        let source = br#"import { helper } from "./dep";
	export function View() { return <button onClick={() => helper()} />; }
"#;

        let calls = collect_calls(source, TSX_LANGUAGE).unwrap();
        let imports = collect_imports(source, TSX_LANGUAGE).unwrap();
        let names = calls
            .iter()
            .map(|site| source_text(source, *site))
            .collect::<Vec<_>>();

        assert!(names.contains(&"helper"));
        assert_eq!(source_text(source, imports[0]), "./dep");
    }

    #[test]
    fn tsx_collector_skips_usestate_destructured_setter_calls() {
        let source = br#"import { useState } from "react";
export function View() {
  const [value, setValue] = useState();
  return <button onClick={() => setValue(value)}>{format(value)}</button>;
}
"#;

        let names = call_names(source, TSX_LANGUAGE);

        assert!(names.contains(&"useState"), "{names:?}");
        assert!(names.contains(&"format"), "{names:?}");
        assert!(!names.contains(&"setValue"), "{names:?}");
    }

    #[test]
    fn ts_collector_skips_renamed_and_nested_destructuring_locals() {
        let source = br#"function run(obj) {
  const { a: alias, b: { c } } = obj;
  alias();
  c();
  external();
}
"#;

        let names = call_names(source, TS_LANGUAGE);

        assert_eq!(names, vec!["external"]);
    }

    #[test]
    fn ts_collector_filters_outer_scope_local_bindings_from_nested_functions() {
        let source = br#"function outer() {
  const x = make();
  function inner() { x(); }
  inner();
}
"#;

        let names = call_names(source, TS_LANGUAGE);

        assert_eq!(names, vec!["make"]);
    }

    #[test]
    fn ts_collector_keeps_member_calls_on_local_receivers() {
        let source = br#"function run(items) {
  items.map((item) => item.fn());
  for (const entry of items) entry.fn();
  class Foo { method() { this.method(); } }
}
"#;

        let names = call_names(source, TS_LANGUAGE);

        assert!(names.contains(&"map"), "{names:?}");
        assert_eq!(names.iter().filter(|name| **name == "fn").count(), 2);
        assert!(names.contains(&"method"), "{names:?}");
        assert!(!names.contains(&"item"), "{names:?}");
        assert!(!names.contains(&"entry"), "{names:?}");
    }

    #[test]
    fn tsx_collector_leaves_jsx_components_to_tier2_instantiate_refs() {
        let source = br#"function View() {
  return <LineageFlow onDone={() => helper()} />;
}
"#;

        let names = call_names(source, TSX_LANGUAGE);

        assert_eq!(names, vec!["helper"]);
    }

    fn source_text(source: &[u8], site: DefinitionSite) -> &str {
        std::str::from_utf8(&source[site.byte_start..site.byte_end]).unwrap()
    }

    fn call_names(source: &[u8], language: TsLanguage) -> Vec<&str> {
        collect_calls(source, language)
            .unwrap()
            .into_iter()
            .map(|site| source_text(source, site))
            .collect()
    }
}
