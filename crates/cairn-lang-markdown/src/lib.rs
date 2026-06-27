//! `cairn-lang-markdown` — Markdown backend.
//!
//! Walks a tree-sitter-md parse tree and emits a [`SymbolFact`] for
//! each ATX (`#`-prefix) or setext (underline) heading. Heading depth
//! drives the parent / child relationship: when the visitor sees a
//! level-3 heading after a level-2 one, the level-2 is its parent;
//! a subsequent level-1 resets the stack.
//!
//! With this in place, `get_outline README.md` returns the document's
//! table of contents and `find_symbols "Quickstart"` jumps to the
//! relevant chapter — the L1 piece of the doc-intelligence plan.
//! Paragraph-level full-text search (L2) is deferred to 0.3.0 via
//! the analyzers.sock protocol.

#![forbid(unsafe_code)]

use cairn_lang_api::{
    ExtractError, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind, SymbolScope,
    SyntacticFacts,
};
use cairn_lang_treesitter_generic::{Visitor, end_line_of, extract, line_of, node_text};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct MarkdownBackend;

impl LanguageBackend for MarkdownBackend {
    fn name(&self) -> &'static str {
        "markdown"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.md", "*.markdown"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-md"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_md::LANGUAGE.into();
        extract(source, &language, MarkdownVisitor::new())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_MARKDOWN: fn() -> Box<dyn LanguageBackend> = || Box::new(MarkdownBackend);

// ─── visitor ────────────────────────────────────────────────────────────────

/// Heading hierarchy tracker. Markdown headings are not nested in
/// tree-sitter-md's AST — they appear as a flat sequence of block
/// nodes — so the visitor maintains its own level-based stack.
struct MarkdownVisitor {
    /// Stack of `(symbol_idx, heading_level)` for ancestors currently
    /// in scope. A new heading at level N pops anything at level >= N
    /// before pushing itself.
    parents: Vec<(usize, u8)>,
}

impl MarkdownVisitor {
    fn new() -> Self {
        Self {
            parents: Vec::new(),
        }
    }

    fn pop_at_or_below(&mut self, level: u8) {
        while let Some(&(_, l)) = self.parents.last() {
            if l >= level {
                self.parents.pop();
            } else {
                break;
            }
        }
    }

    fn current_parent(&self) -> Option<usize> {
        self.parents.last().map(|(idx, _)| *idx)
    }

    fn qualified_for(&self, name: &str, facts: &SyntacticFacts) -> String {
        let mut path: Vec<&str> = self
            .parents
            .iter()
            .map(|(idx, _)| facts.symbols[*idx].name.as_str())
            .collect();
        path.push(name);
        path.join(" > ")
    }
}

impl Visitor for MarkdownVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some((level, name, signature)) = classify_heading(node, source) else {
            return;
        };

        // Heading hierarchy: pop ancestors that are at or below this
        // level, then push self.
        self.pop_at_or_below(level);
        let parent_idx = self.current_parent();
        let qualified = self.qualified_for(&name, facts);

        let idx = facts.symbols.len();
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind: SymbolKind::Section,
            signature: Some(signature),
            doc: None,
            visibility: None,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            // For a heading "body_start" has no clean analogue
            // (Markdown headings are one-liners); leave None so the
            // outline path treats the heading text itself as the
            // signature, which is what readers expect.
            body_start: None,
            parent_idx,
            scope: SymbolScope::TopLevel,
        });

        self.parents.push((idx, level));
    }
}

/// Pull `(level, name, signature)` out of a heading node, or `None`
/// if `node` is not a heading.
///
/// - ATX headings (`# Foo`, `## Bar`) are matched via the
///   `atx_h{1..6}_marker` child node.
/// - Setext headings (`Foo\n===`, `Bar\n---`) use `setext_h1_underline`
///   or `setext_h2_underline`.
fn classify_heading(node: Node<'_>, source: &[u8]) -> Option<(u8, String, String)> {
    match node.kind() {
        "atx_heading" => {
            let level = atx_level(node)?;
            let text = heading_text(node, source);
            let signature = node_text(node, source).trim().to_string();
            Some((level, text, signature))
        }
        "setext_heading" => {
            let level = setext_level(node)?;
            let text = heading_text(node, source);
            let signature = first_line(node_text(node, source));
            Some((level, text, signature))
        }
        _ => None,
    }
}

fn atx_level(node: Node<'_>) -> Option<u8> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        let l = match child.kind() {
            "atx_h1_marker" => 1,
            "atx_h2_marker" => 2,
            "atx_h3_marker" => 3,
            "atx_h4_marker" => 4,
            "atx_h5_marker" => 5,
            "atx_h6_marker" => 6,
            _ => continue,
        };
        return Some(l);
    }
    None
}

fn setext_level(node: Node<'_>) -> Option<u8> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "setext_h1_underline" => return Some(1),
            "setext_h2_underline" => return Some(2),
            _ => continue,
        }
    }
    None
}

/// Heading text. ATX nodes carry the inline span directly; setext
/// nodes wrap the text in a `paragraph` and then add the underline as
/// a sibling, so we walk through both shapes.
fn heading_text(node: Node<'_>, source: &[u8]) -> String {
    // Direct inline child (ATX shape).
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "inline" {
            return node_text(child, source).trim().to_string();
        }
    }
    // Inline nested under a paragraph (setext shape).
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "paragraph" {
            let mut grand = child.walk();
            for inner in child.children(&mut grand) {
                if inner.kind() == "inline" {
                    return node_text(inner, source).trim().to_string();
                }
            }
            // No nested inline: take the paragraph text directly.
            return node_text(child, source).trim().to_string();
        }
    }
    // Last-ditch fallback: first line of the node, stripped of `#`s
    // (would only kick in for an unusual / empty heading).
    let raw = node_text(node, source).trim();
    let first = raw.lines().next().unwrap_or("").trim();
    first
        .trim_start_matches('#')
        .trim()
        .trim_end_matches('#')
        .trim()
        .to_string()
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(facts: &SyntacticFacts) -> Vec<&str> {
        facts.symbols.iter().map(|s| s.name.as_str()).collect()
    }

    #[test]
    fn atx_headings_extracted_in_order() {
        let src = b"# Introduction\n\nIntro body.\n\n## Quickstart\n\nDo this.\n\n## Architecture\n\nThis way.\n";
        let facts = MarkdownBackend.extract_syntactic(src).unwrap();
        assert_eq!(
            names(&facts),
            &["Introduction", "Quickstart", "Architecture"]
        );
        for s in &facts.symbols {
            assert_eq!(s.kind, SymbolKind::Section);
        }
    }

    #[test]
    fn heading_hierarchy_produces_parent_relationship() {
        let src = b"# Top\n\n## Child A\n\n### Grandchild\n\n## Child B\n\n# Sibling\n";
        let facts = MarkdownBackend.extract_syntactic(src).unwrap();
        let by_name: std::collections::HashMap<&str, usize> = facts
            .symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (s.name.as_str(), i))
            .collect();
        let top = by_name["Top"];
        let child_a = by_name["Child A"];
        let grand = by_name["Grandchild"];
        let child_b = by_name["Child B"];
        let sibling = by_name["Sibling"];
        assert_eq!(facts.symbols[child_a].parent_idx, Some(top));
        assert_eq!(facts.symbols[grand].parent_idx, Some(child_a));
        assert_eq!(facts.symbols[child_b].parent_idx, Some(top));
        // Sibling H1 must reset the stack to top-level.
        assert_eq!(facts.symbols[sibling].parent_idx, None);
    }

    #[test]
    fn qualified_name_joins_via_arrow() {
        let src = b"# Top\n\n## Child\n\n### Grand\n";
        let facts = MarkdownBackend.extract_syntactic(src).unwrap();
        let grand = facts.symbols.iter().find(|s| s.name == "Grand").unwrap();
        assert_eq!(grand.qualified, "Top > Child > Grand");
    }

    #[test]
    fn setext_headings_supported() {
        let src = b"Top heading\n===========\n\nSub\n---\n\nBody.\n";
        let facts = MarkdownBackend.extract_syntactic(src).unwrap();
        let n = names(&facts);
        assert!(n.contains(&"Top heading"), "got: {n:?}");
        assert!(n.contains(&"Sub"), "got: {n:?}");
        let sub = facts.symbols.iter().find(|s| s.name == "Sub").unwrap();
        // setext "---" → level 2, parent is the level-1 above.
        assert!(sub.parent_idx.is_some());
    }

    #[test]
    fn empty_heading_does_not_panic() {
        // "# " followed by newline produces an atx heading with an
        // empty inline span; the visitor should not crash.
        let src = b"# \n\n## Real\n";
        let facts = MarkdownBackend.extract_syntactic(src).unwrap();
        // The empty heading still becomes a Section (name "" is
        // acceptable here; the consumer can filter).
        assert!(facts.symbols.iter().any(|s| s.name == "Real"));
    }

    #[test]
    fn file_with_no_headings_yields_no_symbols() {
        let src = b"Just a paragraph.\n\nAnother one.\n";
        let facts = MarkdownBackend.extract_syntactic(src).unwrap();
        assert!(facts.symbols.is_empty());
    }

    #[test]
    fn parser_id_is_stable() {
        let id = MarkdownBackend.parser_id();
        assert_eq!(id, "tree-sitter-md");
    }
}
