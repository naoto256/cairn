//! `cairn-lang-treesitter-generic` — shared scaffolding for
//! tree-sitter-based language backends.
//!
//! Provides:
//! - A reusable [`extract`] entry point that parses with a supplied
//!   `Language` and walks the resulting tree with a caller-provided
//!   [`Visitor`] strategy.
//! - [`NestingTracker`], the parent-stack mechanism every nesting-aware
//!   backend uses to compute qualified names like `Foo::bar` or
//!   `Foo.bar`. Each backend supplies its own qualified-name separator.
//! - Small text helpers ([`collapse_ws`], [`truncate`]) used by
//!   per-language signature and doc-comment extraction.

#![forbid(unsafe_code)]

use cairn_lang_api::{ExtractError, SyntacticFacts};
use tree_sitter::{Language, Node, Parser, Tree};

/// What a per-language backend supplies to drive extraction. The
/// scaffolding parses the buffer, walks the tree, and hands each node
/// to the visitor; the visitor mutates the `facts` accumulator.
pub trait Visitor {
    /// Called once at the top-level node before walking children. The
    /// implementation typically maintains a stack-of-parents so nested
    /// declarations can compute their qualified names.
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts);
}

/// Parse `source` with the supplied tree-sitter `Language`, then walk
/// the tree breadth-first, invoking `visitor` on every node.
///
/// # Errors
/// Returns [`ExtractError::ParserFailure`] if tree-sitter cannot accept
/// the language or returns no tree.
pub fn extract<V: Visitor>(
    source: &[u8],
    language: &Language,
    mut visitor: V,
) -> Result<SyntacticFacts, ExtractError> {
    let mut parser = Parser::new();
    parser
        .set_language(language)
        .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
    let tree: Tree = parser
        .parse(source, None)
        .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

    let mut facts = SyntacticFacts::default();
    walk(tree.root_node(), source, &mut visitor, &mut facts);
    Ok(facts)
}

fn walk<V: Visitor>(node: Node<'_>, source: &[u8], visitor: &mut V, facts: &mut SyntacticFacts) {
    visitor.visit_node(node, source, facts);
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            walk(cursor.node(), source, visitor, facts);
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

// ─── NestingTracker ────────────────────────────────────────────────────────

/// Manages the parent-symbol stack while walking a tree. Each entry is
/// `(symbol_index_in_facts, byte_end_of_parent)`. The byte end lets
/// [`Self::pop_outside`] drop parents whose range the cursor has left.
///
/// Each backend constructs one tracker, picks a separator for qualified
/// names (`"::"` for Rust, `"."` for Python, ...) and calls into the
/// tracker from inside its [`Visitor::visit_node`] implementation.
pub struct NestingTracker {
    parents: Vec<(usize, usize)>,
    separator: &'static str,
}

impl NestingTracker {
    /// `separator` is the join string used by [`Self::qualified_for`]
    /// (e.g. `"::"`, `"."`, `"/"`).
    #[must_use]
    pub fn new(separator: &'static str) -> Self {
        Self {
            parents: Vec::new(),
            separator,
        }
    }

    /// Drop every parent whose byte range ends at or before
    /// `byte_start`. Call at the top of each visit before classifying
    /// the current node so `current_parent` reflects lexical nesting.
    pub fn pop_outside(&mut self, byte_start: usize) {
        while let Some(&(_, end)) = self.parents.last() {
            if end <= byte_start {
                self.parents.pop();
            } else {
                break;
            }
        }
    }

    /// Push a new parent. `byte_end` should be the closing-brace (or
    /// other end-of-scope) byte of the container.
    pub fn push(&mut self, symbol_idx: usize, byte_end: usize) {
        self.parents.push((symbol_idx, byte_end));
    }

    #[must_use]
    pub fn current_parent(&self) -> Option<usize> {
        self.parents.last().map(|(idx, _)| *idx)
    }

    /// Index into `facts.symbols` for the immediate parent's kind, if
    /// any. Lets a backend ask "am I currently inside a class?".
    #[must_use]
    pub fn parent_kind<'a>(
        &self,
        facts: &'a SyntacticFacts,
    ) -> Option<&'a cairn_lang_api::SymbolKind> {
        self.parents
            .last()
            .map(|(idx, _)| &facts.symbols[*idx].kind)
    }

    /// Build a fully-qualified name by joining every parent's name with
    /// the configured separator and appending `name`.
    #[must_use]
    pub fn qualified_for(&self, name: &str, facts: &SyntacticFacts) -> String {
        let mut path: Vec<&str> = self
            .parents
            .iter()
            .map(|(idx, _)| facts.symbols[*idx].name.as_str())
            .collect();
        path.push(name);
        path.join(self.separator)
    }
}

// ─── small utilities ───────────────────────────────────────────────────────

/// Borrow the UTF-8 slice covered by a node. Invalid UTF-8 currently
/// returns an empty string because this API borrows from `source`; use
/// a future `Cow<'a, str>` helper if lossy text retention is needed.
#[must_use]
pub fn node_text<'a>(node: Node<'_>, source: &'a [u8]) -> &'a str {
    std::str::from_utf8(&source[node.byte_range()]).unwrap_or("")
}

/// How a language-specific doc classifier wants a preceding sibling to
/// affect the current doc run.
pub enum DocCommentPart {
    /// Add one line/block to the current contiguous doc run.
    Append(String),
    /// Replace the current run with one block. Used for block doc
    /// comments where the closest block wins over earlier comments.
    Replace(String),
    /// Clear any candidate doc seen so far.
    Reset,
}

/// Extract the doc comment immediately above `node`.
///
/// Tree-sitter exposes comments as extra sibling nodes in most grammars,
/// but each language has different marker rules (`#`, `///`, `/** */`,
/// Javadoc vs plain block comments). This helper owns the adjacency
/// policy while callers decide whether a sibling is a doc, a reset, or
/// irrelevant extra trivia.
#[must_use]
pub fn extract_doc_above_node(
    node: Node<'_>,
    source: &[u8],
    mut classify: impl FnMut(Node<'_>, &str) -> Option<DocCommentPart>,
) -> Option<String> {
    let parent = node.parent()?;
    let mut cursor = parent.walk();
    let mut lines: Vec<String> = Vec::new();
    let mut prev_end_row: Option<usize> = None;

    for sibling in parent.children(&mut cursor) {
        if sibling.start_byte() >= node.start_byte() {
            break;
        }
        let sibling_text = node_text(sibling, source);
        match classify(sibling, sibling_text) {
            Some(DocCommentPart::Append(text)) => {
                if prev_end_row.is_some_and(|prev| sibling.start_position().row > prev + 1) {
                    lines.clear();
                }
                lines.push(text);
                prev_end_row = Some(occupied_end_row(sibling, sibling_text));
            }
            Some(DocCommentPart::Replace(text)) => {
                if prev_end_row.is_some_and(|prev| sibling.start_position().row > prev + 1) {
                    lines.clear();
                }
                lines.clear();
                lines.push(text);
                prev_end_row = Some(occupied_end_row(sibling, sibling_text));
            }
            Some(DocCommentPart::Reset) => {
                lines.clear();
                prev_end_row = None;
            }
            None if !sibling.is_extra() => {
                lines.clear();
                prev_end_row = None;
            }
            None => {}
        }
    }

    if prev_end_row.is_some_and(|prev| node.start_position().row > prev + 1) {
        return None;
    }
    if lines.is_empty() {
        None
    } else {
        Some(truncate(&lines.join("\n"), 1024))
    }
}

fn occupied_end_row(node: Node<'_>, text: &str) -> usize {
    let trailing_newlines = text
        .as_bytes()
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'\n')
        .count();
    node.end_position().row.saturating_sub(trailing_newlines)
}

/// 1-based line number for a tree-sitter point (which uses 0-based rows).
#[must_use]
pub fn line_of(node: Node<'_>) -> u32 {
    u32::try_from(node.start_position().row).unwrap_or(u32::MAX) + 1
}

#[must_use]
pub fn end_line_of(node: Node<'_>) -> u32 {
    u32::try_from(node.end_position().row).unwrap_or(u32::MAX) + 1
}

/// Return the named child whose grammar field is `field_name`, if any.
#[must_use]
pub fn child_by_field<'a>(node: Node<'a>, field_name: &str) -> Option<Node<'a>> {
    node.child_by_field_name(field_name)
}

/// Trim leading/trailing whitespace and collapse internal runs of
/// whitespace into single spaces. Used by signature extraction to keep
/// outline output to a single line.
#[must_use]
pub fn collapse_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_ws = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_was_ws {
                out.push(' ');
                last_was_ws = true;
            }
        } else {
            out.push(ch);
            last_was_ws = false;
        }
    }
    out.trim().to_string()
}

/// Truncate `s` to at most `max` bytes, appending a `…` ellipsis if
/// truncation actually happens. Respects UTF-8 char boundaries.
#[must_use]
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

/// Extract the signature slice between `node.start_byte()` and
/// `body_start` (or the node's end if no body), then trim + collapse
/// whitespace. Returns `None` for empty or invalid-utf-8 slices.
#[must_use]
pub fn signature_slice(node: Node<'_>, source: &[u8], body_start: Option<usize>) -> Option<String> {
    let end = body_start.unwrap_or(node.end_byte());
    let slice = source.get(node.start_byte()..end)?;
    let s = std::str::from_utf8(slice).ok()?.trim();
    if s.is_empty() {
        None
    } else {
        Some(collapse_ws(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tree_sitter::Parser;

    #[test]
    fn collapse_ws_normalizes_runs() {
        assert_eq!(collapse_ws("  fn   foo  (  )   "), "fn foo ( )");
        assert_eq!(collapse_ws("fn\n\nfoo()"), "fn foo()");
    }

    #[test]
    fn truncate_respects_char_boundary() {
        // 4-byte codepoint at the boundary
        let s = "abc🦀def";
        let t = truncate(s, 4);
        // Truncation should not split the codepoint.
        assert!(t.ends_with('…'));
        assert!(t.is_char_boundary(t.len() - "…".len()));
    }

    #[test]
    fn truncate_short_input_unchanged() {
        assert_eq!(truncate("abc", 10), "abc");
    }

    #[test]
    fn extract_doc_above_node_collects_adjacent_run_and_strips_markers() {
        let src = "/// first\n/// second\nfn f() {}\n";

        let doc = first_function_doc(src);

        assert_eq!(doc.as_deref(), Some("first\nsecond"));
    }

    #[test]
    fn extract_doc_above_node_breaks_on_blank_line() {
        let src = "/// stale\n\nfn f() {}\n";

        let doc = first_function_doc(src);

        assert_eq!(doc, None);
    }

    #[test]
    fn extract_doc_above_node_resets_on_non_doc_comment() {
        let src = "/// stale\n// plain\nfn f() {}\n";

        let doc = first_function_doc(src);

        assert_eq!(doc, None);
    }

    #[test]
    fn extract_doc_above_node_replace_keeps_closest_block() {
        let src = "/** old */\n/** new */\nfn f() {}\n";

        let doc = first_function_doc(src);

        assert_eq!(doc.as_deref(), Some("new"));
    }

    fn first_function_doc(src: &str) -> Option<String> {
        let mut parser = Parser::new();
        let language: tree_sitter::Language = tree_sitter_rust::LANGUAGE.into();
        parser.set_language(&language).unwrap();
        let tree = parser.parse(src, None).unwrap();
        let root = tree.root_node();
        let mut cursor = root.walk();
        let node = root
            .children(&mut cursor)
            .find(|node| node.kind() == "function_item")
            .expect("function_item missing");
        extract_doc_above_node(node, src.as_bytes(), rust_doc_part)
    }

    fn rust_doc_part(node: Node<'_>, text: &str) -> Option<DocCommentPart> {
        if node.kind() != "line_comment" && node.kind() != "block_comment" {
            return None;
        }
        let trimmed = text.trim();
        if let Some(rest) = trimmed.strip_prefix("///") {
            Some(DocCommentPart::Append(rest.trim().to_string()))
        } else if let Some(inner) = trimmed
            .strip_prefix("/**")
            .and_then(|s| s.strip_suffix("*/"))
        {
            Some(DocCommentPart::Replace(inner.trim().to_string()))
        } else {
            Some(DocCommentPart::Reset)
        }
    }
}

// Behavioral tests for `Visitor` / `extract` / `NestingTracker` live in
// the per-language backends (cairn-lang-rust, cairn-lang-python) so we
// exercise this scaffolding with a real grammar.
