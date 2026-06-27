//! `cairn-lang-ruby` — Ruby backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-ruby parse tree and emits
//! [`SymbolFact`]s for modules, classes, methods (instance and
//! singleton), constants, `attr_accessor`-family declarations, and
//! `require` / `require_relative` imports. Methods named `test_*` are
//! tagged [`SymbolKind::Test`] (minitest convention).
//!
//! Tier-2 (semantic): the [`analyzer`] module re-walks the same grammar
//! for inheritance edges (`class Dog < Animal`), mixin edges
//! (`include` / `extend` / `prepend`), and call-site refs. Tier-3
//! (ruby-lsp) is out of scope for this backend.
//!
//! Qualified-name convention follows Ruby notation: nesting joins with
//! `::` (`Outer::Greeter`), instance methods attach with `#`
//! (`Greeter#greet`), singleton methods with `.` (`Greeter.build`).
//! The Tier-2 analyzer mirrors the same scheme so
//! `RefFact.enclosing_qualified` resolves against `symbols.qualified`.
//!
//! Known best-effort limits (deliberate for a syntactic floor):
//! - `define_method(:name)` with a literal symbol/string is emitted as
//!   a [`SymbolKind::Method`]; computed names are not.

#![forbid(unsafe_code)]

mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SymbolScope, SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct RubyBackend;

impl LanguageBackend for RubyBackend {
    fn name(&self) -> &'static str {
        "ruby"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        // Literal (non-`*.`) patterns match the file's basename in any
        // directory, so `sub/dir/Gemfile` is claimed too.
        &["*.rb", "*.rake", "Gemfile", "Rakefile"]
    }

    fn shebang_patterns(&self) -> &'static [&'static str] {
        // Substring match against the trimmed first line covers both
        // `#!/usr/bin/ruby` and `#!/usr/bin/env ruby`.
        &["ruby"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-ruby"
    }

    fn parser_revision(&self) -> u32 {
        // v3: ImportFact now carries the argument-string byte range for
        // `require` / `require_relative`, so the Tier-2.5 require-graph
        // resolver can pin its `resolutions` row at the exact site and
        // `find_imports` can LEFT JOIN it.
        // v4: `load "foo.rb"` and `autoload :Foo, "foo"` now emit
        // ImportFact rows with the same `string_content` byte range as
        // `require` / `require_relative`, so the Tier-2.5 require-graph
        // resolver can pin them too. Pre-v4 rows lack these imports
        // entirely; bumping the revision forces a re-parse.
        4
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_ruby::LANGUAGE.into();
        extract(source, &language, RubyVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_RUBY: fn() -> Box<dyn LanguageBackend> = || Box::new(RubyBackend);

// ─── shared helpers (Tier-1 + Tier-2) ──────────────────────────────────────

/// True when `node` (a `method` node) sits directly inside a
/// `class << self` block: `method` → `body_statement` →
/// `singleton_class`. Such methods are singleton methods of the
/// enclosing class and qualify with `.` instead of `#`.
pub(crate) fn within_singleton_class(node: Node<'_>) -> bool {
    node.parent()
        .and_then(|p| p.parent())
        .is_some_and(|gp| gp.kind() == "singleton_class")
}

/// The `#` / `.` separator a method should use in its qualified name.
pub(crate) fn method_separator(singleton: bool) -> &'static str {
    if singleton { "." } else { "#" }
}

// ─── visitor ───────────────────────────────────────────────────────────────

struct RubyVisitor {
    nesting: NestingTracker,
    root_visibility: Visibility,
    visibility_scopes: Vec<(usize, Visibility)>,
}

impl RubyVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("::"),
            root_visibility: Visibility::Public,
            visibility_scopes: Vec::new(),
        }
    }

    fn pop_visibility_scopes(&mut self, byte_start: usize) {
        while self
            .visibility_scopes
            .last()
            .is_some_and(|(end, _)| byte_start >= *end)
        {
            self.visibility_scopes.pop();
        }
    }

    fn current_visibility(&self) -> Visibility {
        self.visibility_scopes
            .last()
            .map_or(self.root_visibility, |(_, visibility)| *visibility)
    }

    fn set_current_visibility(&mut self, visibility: Visibility) {
        if let Some((_, current)) = self.visibility_scopes.last_mut() {
            *current = visibility;
        } else {
            self.root_visibility = visibility;
        }
    }

    fn symbol_visibility(&self, kind: &SymbolKind, node: Node<'_>, source: &[u8]) -> Visibility {
        if let Some(visibility) = modifier_visibility(node, source) {
            return visibility;
        }
        match kind {
            SymbolKind::Function | SymbolKind::Method | SymbolKind::Test | SymbolKind::Property => {
                self.current_visibility()
            }
            _ => Visibility::Public,
        }
    }

    fn update_visibility_section(&mut self, node: Node<'_>, source: &[u8]) -> bool {
        if child_by_field(node, "receiver").is_some() {
            return false;
        }
        let Some(method) = child_by_field(node, "method") else {
            return false;
        };
        if method.kind() != "identifier" {
            return false;
        }
        let visibility = match node_text(method, source) {
            "public" => Visibility::Public,
            "private" => Visibility::Private,
            "protected" => Visibility::Crate,
            _ => return false,
        };
        if child_by_field(node, "arguments").is_some_and(|args| args.named_child_count() > 0) {
            return false;
        }
        self.set_current_visibility(visibility);
        true
    }

    fn update_bare_visibility_section(&mut self, node: Node<'_>, source: &[u8]) -> bool {
        if node
            .parent()
            .is_none_or(|parent| parent.kind() != "body_statement")
        {
            return false;
        }
        let visibility = match node_text(node, source) {
            "public" => Visibility::Public,
            "private" => Visibility::Private,
            "protected" => Visibility::Crate,
            _ => return false,
        };
        self.set_current_visibility(visibility);
        true
    }

    /// Qualified name for a method-like symbol under the current
    /// container, with Ruby's `#` (instance) / `.` (singleton)
    /// notation. Top-level methods keep their bare name.
    fn method_qualified(&self, name: &str, singleton: bool, facts: &SyntacticFacts) -> String {
        match self.nesting.current_parent() {
            Some(idx) => format!(
                "{}{}{name}",
                facts.symbols[idx].qualified,
                method_separator(singleton)
            ),
            None => name.to_string(),
        }
    }

    fn emit(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
        spec: EmitSpec,
    ) -> usize {
        let EmitSpec {
            kind,
            name,
            qualified,
            body_start,
        } = spec;
        let signature = signature_slice(node, source, body_start.or(Some(node.end_byte())));
        let visibility = Some(self.symbol_visibility(&kind, node, source));
        let doc = extract_doc(node, source);
        let parent_idx = self.nesting.current_parent();

        let idx = facts.symbols.len();
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
            scope: SymbolScope::TopLevel,
        });
        idx
    }

    fn emit_container(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let kind = if node.kind() == "module" {
            SymbolKind::Module
        } else {
            SymbolKind::Class
        };
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        // `module A::B` keeps the compound name verbatim.
        let name = node_text(name_node, source).to_string();
        let qualified = self.nesting.qualified_for(&name, facts);
        let body_start = container_body_start(node);
        let idx = self.emit(
            node,
            source,
            facts,
            EmitSpec {
                kind,
                name,
                qualified,
                body_start,
            },
        );
        self.nesting.push(idx, node.end_byte());
        self.visibility_scopes
            .push((node.end_byte(), Visibility::Public));
    }

    fn emit_method(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        let name = node_text(name_node, source).to_string();
        let singleton = within_singleton_class(node);
        let qualified = self.method_qualified(&name, singleton, facts);
        let kind = method_kind(&name, self.nesting.current_parent().is_some());
        let body_start = method_body_start(node);
        self.emit(
            node,
            source,
            facts,
            EmitSpec {
                kind,
                name,
                qualified,
                body_start,
            },
        );
    }

    /// `def self.build` / `def Foo.build`. The `object` field carries
    /// the receiver: `self` qualifies under the enclosing container,
    /// an explicit constant qualifies under that constant's text.
    fn emit_singleton_method(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        let name = node_text(name_node, source).to_string();
        let qualified = match child_by_field(node, "object") {
            Some(obj) if obj.kind() != "self" => {
                format!("{}.{name}", node_text(obj, source))
            }
            _ => self.method_qualified(&name, true, facts),
        };
        let kind = method_kind(&name, self.nesting.current_parent().is_some());
        let body_start = method_body_start(node);
        self.emit(
            node,
            source,
            facts,
            EmitSpec {
                kind,
                name,
                qualified,
                body_start,
            },
        );
    }

    /// `NAME = value` (left side a plain constant). Compound left
    /// sides (`A::B = …`, multiple assignment) are skipped.
    fn emit_constant(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(left) = child_by_field(node, "left") else {
            return;
        };
        if left.kind() != "constant" {
            return;
        }
        let name = node_text(left, source).to_string();
        let qualified = self.nesting.qualified_for(&name, facts);
        self.emit(
            node,
            source,
            facts,
            EmitSpec {
                kind: SymbolKind::Constant,
                name,
                qualified,
                body_start: None,
            },
        );
    }

    /// Receiver-less calls that declare things: `attr_*`,
    /// `require` / `require_relative`, and `define_method`.
    fn handle_call(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        if child_by_field(node, "receiver").is_some() {
            return;
        }
        let Some(method) = child_by_field(node, "method") else {
            return;
        };
        if method.kind() != "identifier" {
            return;
        }
        match node_text(method, source) {
            "attr_accessor" | "attr_reader" | "attr_writer" => {
                self.emit_attrs(node, source, facts);
            }
            "require" | "require_relative" | "load" => {
                if let Some(import) = match_require(node, source) {
                    facts.imports.push(import);
                }
            }
            "autoload" => {
                if let Some(import) = match_autoload(node, source) {
                    facts.imports.push(import);
                }
            }
            "define_method" => self.emit_define_method(node, source, facts),
            _ => {}
        }
    }

    /// One [`SymbolKind::Property`] per `:symbol` argument. The whole
    /// call is the signature; each property points at its own symbol
    /// literal so outline ranges stay tight.
    fn emit_attrs(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(args) = child_by_field(node, "arguments") else {
            return;
        };
        let signature = signature_slice(node, source, None);
        let parent_idx = self.nesting.current_parent();
        let mut cursor = args.walk();
        for arg in args.named_children(&mut cursor) {
            if arg.kind() != "simple_symbol" {
                continue;
            }
            let name = node_text(arg, source).trim_start_matches(':').to_string();
            if name.is_empty() {
                continue;
            }
            let qualified = self.method_qualified(&name, false, facts);
            facts.symbols.push(SymbolFact {
                name,
                qualified,
                kind: SymbolKind::Property,
                signature: signature.clone(),
                doc: None,
                visibility: Some(self.current_visibility()),
                byte_range: arg.byte_range(),
                line_range: line_of(arg)..end_line_of(arg),
                body_start: None,
                parent_idx,
                scope: SymbolScope::TopLevel,
            });
        }
    }

    /// Best-effort `define_method(:name) { … }`: a literal symbol or
    /// string argument yields a Method symbol; computed names don't.
    fn emit_define_method(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(args) = child_by_field(node, "arguments") else {
            return;
        };
        let Some(first) = args.named_child(0) else {
            return;
        };
        let name = match first.kind() {
            "simple_symbol" => node_text(first, source).trim_start_matches(':').to_string(),
            "string" => string_content(first, source).unwrap_or_default(),
            _ => return,
        };
        if name.is_empty() {
            return;
        }
        let qualified = self.method_qualified(&name, false, facts);
        let body_start = child_by_field(node, "block").map(|n| n.start_byte());
        self.emit(
            node,
            source,
            facts,
            EmitSpec {
                kind: SymbolKind::Method,
                name,
                qualified,
                body_start,
            },
        );
    }
}

/// What [`RubyVisitor::emit`] needs to materialize one [`SymbolFact`];
/// the remaining fields derive from the node itself.
struct EmitSpec {
    kind: SymbolKind,
    name: String,
    qualified: String,
    body_start: Option<usize>,
}

impl Visitor for RubyVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());
        self.pop_visibility_scopes(node.start_byte());

        match node.kind() {
            "module" | "class" => self.emit_container(node, source, facts),
            "method" => self.emit_method(node, source, facts),
            "singleton_method" => self.emit_singleton_method(node, source, facts),
            "assignment" => self.emit_constant(node, source, facts),
            "call" => {
                if !self.update_visibility_section(node, source) {
                    self.handle_call(node, source, facts);
                }
            }
            "identifier" => {
                self.update_bare_visibility_section(node, source);
            }
            _ => {}
        }
    }
}

// ─── classification helpers ────────────────────────────────────────────────

fn method_kind(name: &str, has_container: bool) -> SymbolKind {
    if name.starts_with("test_") {
        SymbolKind::Test
    } else if has_container {
        SymbolKind::Method
    } else {
        SymbolKind::Function
    }
}

/// Body start for `class` / `module`: the `body` field when present,
/// otherwise right after the superclass / name so the signature of an
/// empty container (`class Foo; end`) excludes the `end` keyword.
fn container_body_start(node: Node<'_>) -> Option<usize> {
    child_by_field(node, "body")
        .map(|n| n.start_byte())
        .or_else(|| child_by_field(node, "superclass").map(|n| n.end_byte()))
        .or_else(|| child_by_field(node, "name").map(|n| n.end_byte()))
}

/// Body start for `method` / `singleton_method`, with the same
/// empty-body fallback (after the parameter list / name).
fn method_body_start(node: Node<'_>) -> Option<usize> {
    child_by_field(node, "body")
        .map(|n| n.start_byte())
        .or_else(|| child_by_field(node, "parameters").map(|n| n.end_byte()))
        .or_else(|| child_by_field(node, "name").map(|n| n.end_byte()))
}

/// Visibility from the inline-modifier form: `private def foo … end`
/// parses as `call(method: "private", arguments: (argument_list
/// (method …)))`, so a method whose grandparent call is named
/// `public` / `private` / `protected` adjusts visibility. Ruby's `protected` is
/// closer to "visible to sibling instances" than Rust's crate scope,
/// but [`Visibility::Crate`] is the nearest of the three buckets.
fn modifier_visibility(node: Node<'_>, source: &[u8]) -> Option<Visibility> {
    let args = node.parent()?;
    if args.kind() != "argument_list" {
        return None;
    }
    let call = args.parent()?;
    if call.kind() != "call" {
        return None;
    }
    let method = child_by_field(call, "method")?;
    match node_text(method, source) {
        "public" => Some(Visibility::Public),
        "private" => Some(Visibility::Private),
        "protected" => Some(Visibility::Crate),
        _ => None,
    }
}

/// `require "json"` / `require_relative "../lib/util"` /
/// `load "foo.rb"` → one [`ImportFact`] with the string verbatim as the
/// module path. The first string argument is the file/module literal
/// for all three verbs, so a single matcher serves them.
fn match_require(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let args = child_by_field(node, "arguments")?;
    let first = args.named_child(0)?;
    if first.kind() != "string" {
        return None;
    }
    let content = string_content_node(first)?;
    let to_module = node_text(content, source).to_string();
    if to_module.is_empty() {
        return None;
    }
    // The argument string's content node (the bytes between the
    // quotes) is what a require-graph resolver wants to pin its
    // resolution at — both `require "foo"` and `require_relative
    // "./foo"` reduce to the same site shape so a single Tier-2.5
    // resolver can answer either form without re-parsing.
    let range = content.byte_range();
    Some(ImportFact {
        to_module,
        imported: None,
        alias: None,
        is_reexport: false,
        line: line_of(node),
        byte_range: Some((range.start as u32, range.end as u32)),
    })
}

/// `autoload :Foo, "path/to/foo"` → one [`ImportFact`] keyed on the
/// path literal (second argument). The symbol literal is the constant
/// name and is *not* the import target; the path is what
/// `find_imports` and the Tier-2.5 require-graph need.
fn match_autoload(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let args = child_by_field(node, "arguments")?;
    let path_node = args.named_child(1)?;
    if path_node.kind() != "string" {
        return None;
    }
    let content = string_content_node(path_node)?;
    let to_module = node_text(content, source).to_string();
    if to_module.is_empty() {
        return None;
    }
    let range = content.byte_range();
    Some(ImportFact {
        to_module,
        imported: None,
        alias: None,
        is_reexport: false,
        line: line_of(node),
        byte_range: Some((range.start as u32, range.end as u32)),
    })
}

fn string_content_node(string_node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = string_node.walk();
    string_node
        .named_children(&mut cursor)
        .find(|c| c.kind() == "string_content")
}

fn string_content(string_node: Node<'_>, source: &[u8]) -> Option<String> {
    string_content_node(string_node).map(|c| node_text(c, source).to_string())
}

// ─── doc comments ──────────────────────────────────────────────────────────

/// Ruby docs are the contiguous run of `#` comment lines immediately
/// preceding the declaration. For the inline-modifier form
/// (`private def foo`), comments precede the wrapping call, so the
/// search climbs to it first.
fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    let anchor = match node.parent() {
        Some(p) if p.kind() == "argument_list" => p.parent()?,
        _ => node,
    };
    doc_from_preceding_comments(anchor, source).or_else(|| {
        // Comments preceding the *first* statement of a class/module
        // body attach outside the `body_statement`, as direct children
        // of the container node — re-anchor on the body itself.
        let parent = anchor.parent()?;
        if parent.kind() == "body_statement" && parent.start_byte() == anchor.start_byte() {
            doc_from_preceding_comments(parent, source)
        } else {
            None
        }
    })
}

fn doc_from_preceding_comments(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| {
        (sibling.kind() == "comment").then(|| DocCommentPart::Append(strip_comment_marker(text)))
    })
}

fn strip_comment_marker(text: &str) -> String {
    text.trim().trim_start_matches('#').trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn syntactic(src: &str) -> SyntacticFacts {
        RubyBackend.extract_syntactic(src.as_bytes()).unwrap()
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
        assert_eq!(RubyBackend.parser_id(), "tree-sitter-ruby");
    }

    #[test]
    fn claims_ruby_paths_and_shebangs() {
        use cairn_lang_api::{pick_backend_for_path, pick_backend_for_shebang};
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(RubyBackend)];
        for path in [
            "app/models/user.rb",
            "lib/tasks/db.rake",
            "Gemfile",
            "sub/dir/Gemfile",
            "Rakefile",
        ] {
            assert!(
                pick_backend_for_path(&backends, path).is_some(),
                "{path} not claimed"
            );
        }
        assert!(pick_backend_for_path(&backends, "Gemfile.lock").is_none());
        assert!(pick_backend_for_shebang(&backends, "#!/usr/bin/env ruby").is_some());
        assert!(pick_backend_for_shebang(&backends, "#!/usr/bin/ruby").is_some());
    }

    #[test]
    fn extracts_nested_module_class_and_methods() {
        let src = "\
module Outer
  module Inner
    class Greeter
      def greet(who)
        who
      end

      def self.build
        new
      end
    end
  end
end
";
        let facts = syntactic(src);
        assert_eq!(symbol(&facts, "Outer").kind, SymbolKind::Module);
        assert_eq!(symbol(&facts, "Inner").qualified, "Outer::Inner");
        let greeter = symbol(&facts, "Greeter");
        assert_eq!(greeter.kind, SymbolKind::Class);
        assert_eq!(greeter.qualified, "Outer::Inner::Greeter");

        let greet = symbol(&facts, "greet");
        assert_eq!(greet.kind, SymbolKind::Method);
        assert_eq!(greet.qualified, "Outer::Inner::Greeter#greet");
        assert_eq!(facts.symbols[greet.parent_idx.unwrap()].name, "Greeter");

        let build = symbol(&facts, "build");
        assert_eq!(build.kind, SymbolKind::Method);
        assert_eq!(build.qualified, "Outer::Inner::Greeter.build");
    }

    #[test]
    fn compound_module_name_kept_verbatim() {
        let facts = syntactic("module A::B\n  def m; end\nend\n");
        assert_eq!(symbol(&facts, "A::B").kind, SymbolKind::Module);
        assert_eq!(symbol(&facts, "m").qualified, "A::B#m");
    }

    #[test]
    fn top_level_def_is_function() {
        let facts = syntactic("def helper\n  1\nend\n");
        let h = symbol(&facts, "helper");
        assert_eq!(h.kind, SymbolKind::Function);
        assert_eq!(h.qualified, "helper");
    }

    #[test]
    fn test_prefixed_method_is_test() {
        let src = "class FooTest\n  def test_addition\n    assert true\n  end\nend\n";
        let facts = syntactic(src);
        assert_eq!(symbol(&facts, "test_addition").kind, SymbolKind::Test);
    }

    #[test]
    fn singleton_class_block_methods_qualify_with_dot() {
        let src = "\
class Config
  class << self
    def load
      new
    end
  end
end
";
        let facts = syntactic(src);
        assert_eq!(symbol(&facts, "load").qualified, "Config.load");
    }

    #[test]
    fn explicit_receiver_singleton_method() {
        let facts = syntactic("class Foo; end\ndef Foo.bar\n  1\nend\n");
        assert_eq!(symbol(&facts, "bar").qualified, "Foo.bar");
    }

    #[test]
    fn extracts_constants() {
        let src = "class Config\n  VERSION = \"1.0\"\nend\nTOP = 1\n";
        let facts = syntactic(src);
        let v = symbol(&facts, "VERSION");
        assert_eq!(v.kind, SymbolKind::Constant);
        assert_eq!(v.qualified, "Config::VERSION");
        assert_eq!(symbol(&facts, "TOP").qualified, "TOP");
    }

    #[test]
    fn attr_family_emits_properties() {
        let src = "\
class User
  attr_accessor :name, :age
  attr_reader :id
  attr_writer :token
end
";
        let facts = syntactic(src);
        for name in ["name", "age", "id", "token"] {
            let p = symbol(&facts, name);
            assert_eq!(p.kind, SymbolKind::Property, "{name}");
            assert_eq!(p.qualified, format!("User#{name}"));
        }
        assert_eq!(
            symbol(&facts, "name").signature.as_deref(),
            Some("attr_accessor :name, :age")
        );
    }

    #[test]
    fn extracts_requires_as_imports() {
        let src = "require \"json\"\nrequire_relative \"../lib/util\"\n";
        let facts = syntactic(src);
        let mods: Vec<&str> = facts.imports.iter().map(|i| i.to_module.as_str()).collect();
        assert_eq!(mods, &["json", "../lib/util"]);
        assert!(facts.imports.iter().all(|i| !i.is_reexport));
        // Every emitted import row carries a `string_content` byte range
        // so the Tier-2.5 require-graph can LEFT JOIN on the exact site.
        assert!(facts.imports.iter().all(|i| i.byte_range.is_some()));
    }

    #[test]
    fn extracts_load_and_autoload_as_imports() {
        let src = "load \"./foo.rb\"\nautoload :Bar, \"./bar\"\n";
        let facts = syntactic(src);
        let mods: Vec<&str> = facts.imports.iter().map(|i| i.to_module.as_str()).collect();
        assert_eq!(mods, &["./foo.rb", "./bar"]);
        // Same byte-range invariant as require/require_relative: the
        // range covers the `string_content` (text between the quotes)
        // so the Tier-2.5 require-graph resolver can pin its
        // `resolutions` row at the exact site.
        for imp in &facts.imports {
            let (s, e) = imp.byte_range.expect("byte_range required");
            // Cross-check: bytes between the quotes match `to_module`.
            assert_eq!(
                &src.as_bytes()[s as usize..e as usize],
                imp.to_module.as_bytes()
            );
        }
    }

    #[test]
    fn inline_private_modifier_sets_visibility() {
        let src = "\
class Safe
  private def hidden; end
  protected def guarded; end
  def open; end
end
";
        let facts = syntactic(src);
        assert_eq!(
            symbol(&facts, "hidden").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "guarded").visibility,
            Some(Visibility::Crate)
        );
        assert_eq!(symbol(&facts, "open").visibility, Some(Visibility::Public));
    }

    #[test]
    fn bare_private_marker_sets_section_visibility() {
        let src = "class S\n  private\n\n  def hidden; end\nend\n";
        let facts = syntactic(src);
        assert_eq!(
            symbol(&facts, "hidden").visibility,
            Some(Visibility::Private)
        );
    }

    #[test]
    fn define_method_with_literal_symbol_is_best_effort_method() {
        let src = "class D\n  define_method(:dynamic) { 42 }\nend\n";
        let facts = syntactic(src);
        let d = symbol(&facts, "dynamic");
        assert_eq!(d.kind, SymbolKind::Method);
        assert_eq!(d.qualified, "D#dynamic");
    }

    #[test]
    fn define_method_with_computed_name_is_skipped() {
        // Documented best-effort limit: a computed name can't be
        // extracted statically.
        let src = "class D\n  define_method(name.to_sym) { 42 }\nend\n";
        let facts = syntactic(src);
        assert!(facts.symbols.iter().all(|s| s.name == "D"));
    }

    #[test]
    fn captures_preceding_comment_as_doc() {
        let src = "\
# Greets people.
# Politely.
class Greeter
  # Say hi.
  def hi; end
end
";
        let facts = syntactic(src);
        assert_eq!(
            symbol(&facts, "Greeter").doc.as_deref(),
            Some("Greets people.\nPolitely.")
        );
        assert_eq!(symbol(&facts, "hi").doc.as_deref(), Some("Say hi."));
    }

    #[test]
    fn blank_line_breaks_doc_association() {
        let src = "# Stale comment.\n\ndef floating; end\n";
        let facts = syntactic(src);
        assert_eq!(symbol(&facts, "floating").doc, None);
    }

    #[test]
    fn signature_excludes_body() {
        let src = "class Greeter < Base\n  def greet(who)\n    who.to_s\n  end\nend\n";
        let facts = syntactic(src);
        let class_sig = symbol(&facts, "Greeter").signature.as_deref().unwrap();
        assert_eq!(class_sig, "class Greeter < Base");
        let sig = symbol(&facts, "greet").signature.as_deref().unwrap();
        assert!(sig.contains("def greet(who)"));
        assert!(!sig.contains("to_s"));
    }

    #[test]
    fn empty_body_signature_excludes_end() {
        let facts = syntactic("class Empty; end\n");
        assert_eq!(
            symbol(&facts, "Empty").signature.as_deref(),
            Some("class Empty")
        );
    }
}
