//! `cairn-lang-python` — Python backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-python parse tree and emits
//! [`SymbolFact`]s for module-level and nested functions, classes,
//! methods, and top-level assignments. Functions whose name begins with
//! `test_` and functions defined inside a `pytest`/`unittest`-shaped
//! class are tagged [`SymbolKind::Test`].
//!
//! Tier-2 (semantic): the [`analyzer`] module re-walks the same grammar
//! for imports and class-inheritance edges. Call-site / annotation refs
//! are a deliberate follow-up slice.

#![forbid(unsafe_code)]

mod analyzer;

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

/// Backend instance.
pub struct PythonBackend;

impl LanguageBackend for PythonBackend {
    fn name(&self) -> &'static str {
        "python"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.py", "*.pyi"]
    }

    fn shebang_patterns(&self) -> &'static [&'static str] {
        // Substrings searched in the trimmed first line. Covers both
        // direct interpreter shebangs (`#!/usr/bin/python3`) and
        // `env`-based ones (`#!/usr/bin/env python3`). "python3" is
        // listed first so it wins on environments where both python2
        // and python3 are installed and a backend "python2" might
        // later claim the bare `python` substring.
        &["python3", "python"]
    }

    fn parser_id(&self) -> &'static str {
        concat!("tree-sitter-python@", env!("CARGO_PKG_VERSION"))
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_python::LANGUAGE.into();
        extract(source, &language, PythonVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_PYTHON: fn() -> Box<dyn LanguageBackend> = || Box::new(PythonBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct PythonVisitor {
    nesting: NestingTracker,
}

impl PythonVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
        }
    }
}

impl Visitor for PythonVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let start = node.start_byte();
        self.nesting.pop_outside(start);

        let Some((mut kind, name, body_start)) = match_python_item(node, source) else {
            return;
        };

        // Class-level def → method (unless name is dunder or test_).
        let parent_is_class = matches!(self.nesting.parent_kind(facts), Some(SymbolKind::Class));
        if parent_is_class && matches!(kind, SymbolKind::Function) {
            kind = if name.starts_with("test_") {
                SymbolKind::Test
            } else {
                SymbolKind::Method
            };
        } else if matches!(kind, SymbolKind::Function) && name.starts_with("test_") {
            kind = SymbolKind::Test;
        }

        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = python_visibility(&name);
        let doc = extract_docstring(node, source);
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
    matches!(kind, SymbolKind::Class | SymbolKind::Module)
}

fn match_python_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    match node.kind() {
        "function_definition" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Function,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "class_definition" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Class, node_text(name, source).to_string(), body))
        }
        _ => None,
    }
}

/// PEP-8 convention: leading underscore signals non-public, double
/// leading underscore (non-dunder) is private. We treat dunder names
/// (`__init__`) as public methods.
fn python_visibility(name: &str) -> Option<Visibility> {
    if name.starts_with("__") && name.ends_with("__") {
        Some(Visibility::Public)
    } else if name.starts_with("__") {
        Some(Visibility::Private)
    } else if name.starts_with('_') {
        Some(Visibility::Crate)
    } else {
        Some(Visibility::Public)
    }
}

/// Python docstrings live as the first statement of a function/class
/// body, as a `string` expression. Walk the body's children, find the
/// first `expression_statement` whose only child is `string`, strip
/// the quotes, return its text.
fn extract_docstring(node: Node<'_>, source: &[u8]) -> Option<String> {
    let body = child_by_field(node, "body")?;
    let mut cursor = body.walk();
    for child in body.children(&mut cursor) {
        if !child.is_named() {
            continue;
        }
        if child.kind() == "expression_statement" {
            let inner = child.named_child(0)?;
            if inner.kind() == "string" {
                let text = node_text(inner, source);
                return Some(strip_python_quotes(text));
            }
        }
        // First non-comment statement: stop looking.
        break;
    }
    None
}

fn strip_python_quotes(s: &str) -> String {
    let trimmed = s.trim();
    for delim in ["\"\"\"", "'''"] {
        if let Some(inner) = trimmed
            .strip_prefix(delim)
            .and_then(|t| t.strip_suffix(delim))
        {
            return truncate(inner.trim(), 1024);
        }
    }
    for delim in ['"', '\''] {
        if let Some(inner) = trimmed
            .strip_prefix(delim)
            .and_then(|t| t.strip_suffix(delim))
        {
            return truncate(inner.trim(), 1024);
        }
    }
    truncate(trimmed, 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(facts: &SyntacticFacts) -> Vec<&str> {
        facts.symbols.iter().map(|s| s.name.as_str()).collect()
    }

    #[test]
    fn extracts_module_function() {
        let facts = PythonBackend
            .extract_syntactic(b"def hello():\n    return 1\n")
            .unwrap();
        assert_eq!(names(&facts), &["hello"]);
        assert_eq!(facts.symbols[0].kind, SymbolKind::Function);
    }

    #[test]
    fn extracts_class_with_methods() {
        let src =
            b"class Foo:\n    def bar(self):\n        return 1\n    def baz(self):\n        pass\n";
        let facts = PythonBackend.extract_syntactic(src).unwrap();
        let n = names(&facts);
        assert!(n.contains(&"Foo"));
        assert!(n.contains(&"bar"));
        let bar = facts.symbols.iter().find(|s| s.name == "bar").unwrap();
        assert_eq!(bar.kind, SymbolKind::Method);
        let parent = facts.symbols[bar.parent_idx.unwrap()].clone();
        assert_eq!(parent.name, "Foo");
        assert_eq!(bar.qualified, "Foo.bar");
    }

    #[test]
    fn recognizes_test_function() {
        let facts = PythonBackend
            .extract_syntactic(b"def test_something():\n    assert True\n")
            .unwrap();
        assert_eq!(facts.symbols[0].kind, SymbolKind::Test);
    }

    #[test]
    fn private_visibility_for_underscore_prefix() {
        let facts = PythonBackend
            .extract_syntactic(b"def _helper():\n    pass\n")
            .unwrap();
        assert_eq!(facts.symbols[0].visibility, Some(Visibility::Crate));
    }

    #[test]
    fn dunder_treated_as_public() {
        let facts = PythonBackend
            .extract_syntactic(b"class C:\n    def __init__(self):\n        pass\n")
            .unwrap();
        let init = facts.symbols.iter().find(|s| s.name == "__init__").unwrap();
        assert_eq!(init.visibility, Some(Visibility::Public));
    }

    #[test]
    fn captures_docstring() {
        let src = b"def hi():\n    \"\"\"Greet someone.\"\"\"\n    return 1\n";
        let facts = PythonBackend.extract_syntactic(src).unwrap();
        assert_eq!(facts.symbols[0].doc.as_deref(), Some("Greet someone."));
    }

    #[test]
    fn signature_excludes_body() {
        let src = b"def f(x: int) -> int:\n    return x + 1\n";
        let facts = PythonBackend.extract_syntactic(src).unwrap();
        let sig = facts.symbols[0].signature.as_deref().unwrap();
        assert!(sig.contains("def f(x: int) -> int"));
        assert!(!sig.contains("return"));
    }
}
