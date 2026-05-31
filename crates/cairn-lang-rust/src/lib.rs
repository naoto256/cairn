//! `cairn-lang-rust` — Rust backend.
//!
//! Two layers:
//! - **Syntactic** (this file): walks a tree-sitter-rust parse tree
//!   and emits [`SymbolFact`]s for every top-level and nested item we
//!   care about — functions, methods, structs, enums, unions, traits,
//!   impls, modules, type aliases, and `#[test]`-annotated functions.
//! - **Semantic** ([`analyzer::RustAnalyzer`]): a `syn`-based pass
//!   that runs afterwards and emits impl edges, use imports, and
//!   `#[doc = "..."]` overrides. Wired through
//!   [`LanguageBackend::analyzer`].

#![forbid(unsafe_code)]

pub mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    NestingTracker, Visitor, child_by_field, end_line_of, extract, line_of, node_text,
    signature_slice, truncate,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance. Stateless; constructed fresh on each
/// [`LANGUAGE_BACKENDS`] resolution.
pub struct RustBackend;

impl LanguageBackend for RustBackend {
    fn name(&self) -> &'static str {
        "rust"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.rs"]
    }

    fn parser_id(&self) -> &'static str {
        concat!("tree-sitter-rust@", env!("CARGO_PKG_VERSION"))
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_rust::LANGUAGE.into();
        extract(source, &language, RustVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(Arc::new(analyzer::RustAnalyzer))
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_RUST: fn() -> Box<dyn LanguageBackend> = || Box::new(RustBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct RustVisitor {
    nesting: NestingTracker,
}

impl RustVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("::"),
        }
    }
}

impl Visitor for RustVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let start = node.start_byte();
        self.nesting.pop_outside(start);

        let Some((kind, name, body_start)) = match_rust_item(node, source) else {
            return;
        };

        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = extract_visibility(node, source);
        let doc = extract_doc(node, source);
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
        SymbolKind::Module
            | SymbolKind::Struct
            | SymbolKind::Enum
            | SymbolKind::Trait
            | SymbolKind::Impl
            | SymbolKind::Union
    )
}

/// Classify a tree-sitter-rust node into a (kind, name, body_start)
/// tuple if it represents something we want to index.
fn match_rust_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    match node.kind() {
        "function_item" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            let kind = if has_test_attribute(node, source) {
                SymbolKind::Test
            } else {
                SymbolKind::Function
            };
            Some((kind, node_text(name, source).to_string(), body))
        }
        "function_signature_item" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Function,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "struct_item" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Struct,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "enum_item" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Enum, node_text(name, source).to_string(), body))
        }
        "union_item" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Union, node_text(name, source).to_string(), body))
        }
        "trait_item" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Trait, node_text(name, source).to_string(), body))
        }
        "type_item" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::TypeAlias,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "mod_item" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Module,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "impl_item" => {
            // impl <Trait> for <Type> { ... }  →  name from `type` field.
            //
            // The raw `type` text includes generic args (`Foo<T>`,
            // `Walker<'a>`). We strip those so the qualified path of
            // items inside the impl reads as `Foo::bar`, matching
            // what the syn-based Tier-2 analyzer emits as the
            // `enclosing_qualified` of refs. If the two diverged
            // (the original behavior pre-fix), the indexer's
            // pending-ref resolution couldn't join enclosing
            // qualified → enclosing symbol id, and ~3 % of refs lost
            // their attribution. See INDEXER_REVISION history.
            let ty = child_by_field(node, "type")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            let normalized = strip_generic_args(node_text(ty, source));
            Some((SymbolKind::Impl, normalized, body))
        }
        "macro_definition" => {
            let name = child_by_field(node, "name")?;
            Some((SymbolKind::Macro, node_text(name, source).to_string(), None))
        }
        "const_item" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Constant,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "static_item" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Variable,
                node_text(name, source).to_string(),
                None,
            ))
        }
        _ => None,
    }
}

/// Normalize a tree-sitter-extracted impl self-type to the
/// generic-free form so `qualified` matches what the syn analyzer
/// emits as `enclosing_qualified`. Examples:
///
/// - `"Walker<'a>"`            → `"Walker"`
/// - `"Foo<T>"`                → `"Foo"`
/// - `"std::collections::HashMap<K, V>"` → `"std::collections::HashMap"`
/// - `"Foo"`                   → `"Foo"`  (unchanged)
///
/// Exotic self-types that aren't named paths (tuple impls,
/// reference impls, fn-pointer impls) fall back to the raw text;
/// they're rare enough that any leftover mismatch is acceptable
/// for now, and the syn side has the same blind spot
/// (`type_path_string` only resolves `Type::Path`).
fn strip_generic_args(s: &str) -> String {
    match s.find('<') {
        Some(i) => s[..i].trim().to_string(),
        None => s.trim().to_string(),
    }
}

fn has_test_attribute(node: Node<'_>, source: &[u8]) -> bool {
    let parent = match node.parent() {
        Some(p) => p,
        None => return false,
    };
    let mut cursor = parent.walk();
    for sibling in parent.children(&mut cursor) {
        if sibling.start_byte() >= node.start_byte() {
            break;
        }
        if sibling.kind() == "attribute_item" {
            let text = node_text(sibling, source);
            if text.contains("test") {
                return true;
            }
        }
    }
    false
}

fn extract_visibility(node: Node<'_>, source: &[u8]) -> Option<Visibility> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "visibility_modifier" {
            let text = node_text(child, source);
            return Some(if text.contains("crate") {
                Visibility::Crate
            } else {
                Visibility::Public
            });
        }
    }
    None
}

/// Walk preceding siblings of `node` (within its parent) collecting
/// contiguous `line_comment` / `block_comment` nodes that are doc
/// comments (start with `///` or `//!`, `/**` or `/*!`).
fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    let parent = node.parent()?;
    let mut cursor = parent.walk();
    let mut lines: Vec<String> = Vec::new();
    let mut prev_end_row: Option<usize> = None;

    for sibling in parent.children(&mut cursor) {
        if sibling.start_byte() >= node.start_byte() {
            break;
        }
        let kind = sibling.kind();
        if kind == "line_comment" || kind == "block_comment" {
            let text = node_text(sibling, source);
            if !text.starts_with("///")
                && !text.starts_with("//!")
                && !text.starts_with("/**")
                && !text.starts_with("/*!")
            {
                // Not a doc comment — break the contiguous run.
                lines.clear();
                prev_end_row = None;
                continue;
            }
            // Only keep contiguous (adjacent rows).
            let start_row = sibling.start_position().row;
            if let Some(p) = prev_end_row {
                if start_row > p + 1 {
                    lines.clear();
                }
            }
            lines.push(strip_doc_markers(text));
            prev_end_row = Some(sibling.end_position().row);
        } else if !sibling.is_extra() && sibling.kind() != "attribute_item" {
            // A non-comment, non-attribute sibling resets the run.
            lines.clear();
            prev_end_row = None;
        }
    }

    if lines.is_empty() {
        None
    } else {
        let joined = lines.join("\n");
        Some(truncate(&joined, 1024))
    }
}

fn strip_doc_markers(text: &str) -> String {
    let t = text.trim();
    if let Some(rest) = t.strip_prefix("///") {
        rest.trim().to_string()
    } else if let Some(rest) = t.strip_prefix("//!") {
        rest.trim().to_string()
    } else if let Some(inner) = t.strip_prefix("/**").and_then(|s| s.strip_suffix("*/")) {
        inner.trim().to_string()
    } else if let Some(inner) = t.strip_prefix("/*!").and_then(|s| s.strip_suffix("*/")) {
        inner.trim().to_string()
    } else {
        t.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(facts: &SyntacticFacts) -> Vec<&str> {
        facts.symbols.iter().map(|s| s.name.as_str()).collect()
    }

    #[test]
    fn extracts_top_level_function() {
        let facts = RustBackend.extract_syntactic(b"fn hello() {}").unwrap();
        assert_eq!(names(&facts), &["hello"]);
        let s = &facts.symbols[0];
        assert_eq!(s.kind, SymbolKind::Function);
        assert!(s.signature.as_deref().unwrap().starts_with("fn hello"));
    }

    #[test]
    fn extracts_struct_and_nested_methods() {
        let src = br"
struct Foo { x: i32 }

impl Foo {
    fn bar(&self) -> i32 { self.x }
    fn baz(&self) {}
}
";
        let facts = RustBackend.extract_syntactic(src).unwrap();
        let n = names(&facts);
        assert!(n.contains(&"Foo"));
        assert!(n.contains(&"bar"));
        assert!(n.contains(&"baz"));
        // bar's parent should be the impl, whose name is "Foo".
        let bar = facts.symbols.iter().find(|s| s.name == "bar").unwrap();
        let parent_idx = bar.parent_idx.unwrap();
        assert_eq!(facts.symbols[parent_idx].name, "Foo");
        assert_eq!(facts.symbols[parent_idx].kind, SymbolKind::Impl);
        assert_eq!(bar.qualified, "Foo::bar");
    }

    #[test]
    fn generic_impl_self_type_is_stripped_in_qualified() {
        // Regression guard: pre-fix, `impl<'a> Walker<'a> { fn
        // visit_item() {} }` produced qualified =
        // `Walker<'a>::visit_item`, but the syn analyzer's
        // `enclosing_qualified` for refs inside that method was
        // `Walker::visit_item` (path_to_string strips args). The
        // mismatch broke the indexer's join, costing ~3 % of refs
        // their enclosing attribution. Tier-1 and Tier-2 must
        // agree on the canonical form.
        let src = br"
impl<'a> Walker<'a> {
    fn visit_item(&mut self) {}
}

impl<T: Clone> Container<T> {
    fn push(&mut self, item: T) {}
}

impl std::fmt::Display for Box<dyn Trait> {
    fn fmt(&self) {}
}
";
        let facts = RustBackend.extract_syntactic(src).unwrap();
        let by_name: std::collections::HashMap<&str, &SymbolFact> =
            facts.symbols.iter().map(|s| (s.name.as_str(), s)).collect();
        // Method qualifieds must NOT contain `<` — that would
        // mean the impl-block's type still carries generics.
        let visit_item = by_name.get("visit_item").expect("visit_item missing");
        assert_eq!(visit_item.qualified, "Walker::visit_item");
        let push = by_name.get("push").expect("push missing");
        assert_eq!(push.qualified, "Container::push");
        let fmt = by_name.get("fmt").expect("fmt missing");
        // `Box<dyn Trait>` → `Box` (everything past `<` dropped,
        // including any trailing path qualifier of the inner ty).
        assert_eq!(fmt.qualified, "Box::fmt");
    }

    #[test]
    fn recognizes_test_attribute() {
        let src = br"
#[test]
fn it_works() {}
";
        let facts = RustBackend.extract_syntactic(src).unwrap();
        let s = &facts.symbols[0];
        assert_eq!(s.name, "it_works");
        assert_eq!(s.kind, SymbolKind::Test);
    }

    #[test]
    fn captures_public_visibility() {
        let facts = RustBackend.extract_syntactic(b"pub fn hi() {}").unwrap();
        assert_eq!(facts.symbols[0].visibility, Some(Visibility::Public));
    }

    #[test]
    fn captures_crate_visibility() {
        let facts = RustBackend
            .extract_syntactic(b"pub(crate) fn hi() {}")
            .unwrap();
        assert_eq!(facts.symbols[0].visibility, Some(Visibility::Crate));
    }

    #[test]
    fn captures_doc_comment() {
        let src = br"/// Greet someone.
/// Multi-line.
fn hi() {}
";
        let facts = RustBackend.extract_syntactic(src).unwrap();
        let doc = facts.symbols[0].doc.as_deref().unwrap();
        assert!(doc.contains("Greet someone"));
        assert!(doc.contains("Multi-line"));
    }

    #[test]
    fn signature_excludes_body() {
        let facts = RustBackend
            .extract_syntactic(b"fn f(x: i32) -> i32 { x + 1 }")
            .unwrap();
        let sig = facts.symbols[0].signature.as_deref().unwrap();
        assert!(sig.contains("fn f(x: i32) -> i32"));
        assert!(!sig.contains("x + 1"));
    }

    #[test]
    fn module_nesting_in_qualified_name() {
        let src = br"
mod outer {
    fn inner() {}
}
";
        let facts = RustBackend.extract_syntactic(src).unwrap();
        let inner = facts.symbols.iter().find(|s| s.name == "inner").unwrap();
        assert_eq!(inner.qualified, "outer::inner");
    }

    #[test]
    fn parser_id_includes_version() {
        let id = RustBackend.parser_id();
        assert!(id.starts_with("tree-sitter-rust@"));
    }
}
