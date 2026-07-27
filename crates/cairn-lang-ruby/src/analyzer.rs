//! Ruby Tier-2 analyzer (semantic enrichment over the same
//! tree-sitter parse the syntactic pass uses).
//!
//! Mirrors the Python backend's Tier-2 shape: structural extraction,
//! not type resolution (receiver types are ruby-lsp / Tier-3
//! territory). The statically-faithful facts for Ruby are:
//!
//! - **inheritance edges** — `class Dog < Animal` becomes an
//!   [`ImplFact`] with `kind = "inherit"`, so
//!   `find_subtypes name=Animal` answers "what subclasses Animal".
//! - **mixin edges** — `include M` / `extend M` / `prepend M` inside a
//!   class or module body become [`ImplFact`]s with `kind` set to the
//!   mixin verb. These are Ruby's interface-implementation analog.
//! - **refs** — call sites (`foo()`, `obj.render` → [`RefKind::Call`]),
//!   name-level only: a method call's receiver type is unknown without
//!   Tier-3, so `target_qualified` stays `None` for cross-file callees.
//!   As a same-file best effort, a post-pass fills `target_qualified`
//!   for an exact constant receiver such as `Foo.m`, or when a
//!   receiver-less call name matches a method defined in this file —
//!   `Foo#m` (instance), `Foo.m` (singleton), or the bare top-level
//!   name. Dynamic receivers remain unresolved. When a receiver-less
//!   call has both instance and singleton candidates, the first one
//!   seen in source order wins. Cross-file callees keep
//!   `target_qualified: None`, which hides them from `find_references`'
//!   default outgoing view (visible with `include_noise`). Paren-less
//!   zero-arg calls are indistinguishable from local-variable reads in
//!   the grammar (both parse as `identifier`) and are deliberately not
//!   emitted. Dynamic dispatch (`send(:name)`, `method_missing`) is not
//!   resolved — `send` itself appears as the call target.
//!
//! - **type refs** — the base-class name in `class Dog < Animal` and the
//!   mixin module names in `include M` / `extend M` / `prepend M` are
//!   emitted as [`RefKind::Type`] rows with `target_qualified = None`
//!   (the token text lives in `target_name`; Tier-2.5 resolves it into a
//!   `symbols.qualified`). This is intentional layering: Tier-2 emits
//!   the syntactic site, Tier-2.5 / Tier-3 resolve the target. Without
//!   this, `find_callers` over a class name returns nothing for
//!   base-class / mixin sites.
//! - **require-family calls** (`require`, `require_relative`, `load`,
//!   `autoload`) are emitted as [`RefKind::Call`] like any other call.
//!   The "is this a declaration?" classification belongs to the
//!   consumer (Tier-2.5 / Tier-3 / the UI), not to the syntactic pass.
//!   Pure declaration / visibility verbs (`attr_*`, visibility markers,
//!   `define_method`, and the mixin verbs themselves) remain skipped
//!   because they are already represented as symbols or impl edges.
//!
//! `require` / `require_relative` imports are also emitted by the
//! **syntactic** pass as import edges (like Go), so this analyzer
//! leaves `SemanticFacts.imports` empty rather than duplicating rows —
//! the Call ref above is the *call-site* view, not the import edge.
//!
//! Qualified names mirror the syntactic pass exactly: containers join
//! with `::`, instance methods attach with `#`, singleton methods with
//! `.` — see the crate docs. That keeps `RefFact.enclosing_qualified`
//! and `ImplFact.type_qualified` resolvable against
//! `symbols.qualified`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts, SyntacticKind,
};
use cairn_lang_treesitter_generic::{child_by_field, collapse_ws, line_of, node_text};
use tree_sitter::{Node, Parser};

use crate::{method_separator, within_singleton_class};

/// Ruby semantic analyzer. Re-parses the source with tree-sitter-ruby
/// (the same grammar the syntactic pass uses) and walks for
/// inheritance / mixin edges and call refs.
pub struct RubyAnalyzer;

impl Analyzer for RubyAnalyzer {
    fn name(&self) -> &'static str {
        "ruby-treesitter"
    }

    fn revision(&self) -> u32 {
        // v2 resolves relative and absolute constant receivers through
        // same-file lexical owners, then maps the canonical lookup back
        // to the published singleton symbol.
        2
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_ruby::LANGUAGE.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut facts = SemanticFacts::default();
        let mut scope_stack = ScopeStack::default();
        let mut resolver = SameFileResolver::default();
        walk(
            tree.root_node(),
            source,
            &mut scope_stack,
            None,
            &mut resolver,
            &mut facts,
        );
        resolver.resolve(&mut facts);
        Ok(facts)
    }
}

#[derive(Default)]
struct SameFileResolver {
    short_names: HashMap<String, String>,
    singleton_methods: HashMap<String, SingletonTarget>,
    constant_owners: HashSet<String>,
    receiver_authorities: HashMap<usize, ReceiverAuthority>,
}

struct SingletonTarget {
    published_qualified: String,
    definitions: usize,
}

#[derive(Default)]
struct ScopeStack {
    segments: Vec<String>,
}

enum ScopeFrame {
    Nested,
    Absolute(Vec<String>),
}

enum MethodDefinitionAuthority {
    Instance,
    /// The enclosing scope proves the singleton owner (`def self.x` or
    /// `class << self; def x`), so it can enter the exact index.
    ScopedSingleton,
    /// `def Alpha.x` may name a lexically relative constant. Keep it
    /// available to the legacy short-name fallback, but do not treat
    /// its raw object text as an exact owner.
    ExplicitObjectSingleton,
}

impl ScopeStack {
    /// Enter one declaration scope and return the state needed to
    /// restore its caller. A leading `::` starts at Ruby's root rather
    /// than inheriting the surrounding lexical owner.
    fn enter(&mut self, name: String) -> ScopeFrame {
        let frame = if name.starts_with("::") {
            ScopeFrame::Absolute(std::mem::take(&mut self.segments))
        } else {
            ScopeFrame::Nested
        };
        self.segments.push(name);
        frame
    }

    fn exit(&mut self, frame: ScopeFrame) {
        match frame {
            ScopeFrame::Nested => {
                self.segments.pop();
            }
            ScopeFrame::Absolute(previous) => self.segments = previous,
        }
    }

    fn qualified(&self, name: &str) -> String {
        if name.starts_with("::") || self.segments.is_empty() {
            name.to_string()
        } else {
            format!("{}::{name}", self.segments.join("::"))
        }
    }

    fn method_qualified(&self, name: &str, singleton: bool) -> String {
        if self.segments.is_empty() {
            name.to_string()
        } else {
            format!(
                "{}{}{name}",
                self.segments.join("::"),
                method_separator(singleton)
            )
        }
    }

    fn as_slice(&self) -> &[String] {
        &self.segments
    }

    fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

impl SameFileResolver {
    fn record_owner(&mut self, qualified: &str) {
        self.constant_owners
            .insert(qualified.trim_start_matches("::").to_string());
    }

    fn record_method(&mut self, name: &str, qualified: &str, authority: MethodDefinitionAuthority) {
        self.short_names
            .entry(name.to_string())
            .or_insert_with(|| qualified.to_string());
        if matches!(authority, MethodDefinitionAuthority::ScopedSingleton) {
            let canonical = qualified.trim_start_matches("::").to_string();
            let target =
                self.singleton_methods
                    .entry(canonical)
                    .or_insert_with(|| SingletonTarget {
                        published_qualified: qualified.to_string(),
                        definitions: 0,
                    });
            target.definitions += 1;
        }
    }

    /// Push a call fact and its receiver authority at the same index.
    ///
    /// Keeping both writes together prevents later non-call refs from
    /// shifting the receiver metadata away from its call fact.
    fn push_call(
        &mut self,
        facts: &mut SemanticFacts,
        fact: RefFact,
        receiver: Option<Node<'_>>,
        source: &[u8],
        scope_stack: &ScopeStack,
    ) {
        let index = facts.refs.len();
        if let Some(receiver) = receiver {
            let authority = match receiver.kind() {
                "constant" | "scope_resolution" => {
                    let owner = node_text(receiver, source)
                        .trim_start_matches("::")
                        .to_string();
                    if absolute_scope_resolution(receiver) {
                        ReceiverAuthority::Absolute { owner }
                    } else {
                        ReceiverAuthority::Lexical {
                            owner,
                            scope: scope_stack.as_slice().to_vec(),
                        }
                    }
                }
                // Preserve the existing self/super short-name behavior.
                "self" | "super" => {
                    facts.refs.push(fact);
                    return;
                }
                _ => ReceiverAuthority::Dynamic,
            };
            self.receiver_authorities.insert(index, authority);
        }
        facts.refs.push(fact);
    }

    fn lexical_owner(&self, owner: &str, scope: &[String]) -> Option<String> {
        for depth in (0..=scope.len()).rev() {
            let prefix = scope[..depth].join("::");
            let prefix = prefix.trim_start_matches("::");
            let candidate = if prefix.is_empty() {
                owner.to_string()
            } else {
                format!("{prefix}::{owner}")
            };
            if self.constant_owners.contains(&candidate) {
                return Some(candidate);
            }
        }
        None
    }

    /// Resolve same-file calls after every definition is indexed.
    ///
    /// Absolute receivers use their canonical owner. Relative
    /// receivers use the nearest same-file lexical owner and never
    /// fall through a shadowing owner to a global method. Dynamic
    /// receivers fail closed. Calls without an explicit receiver
    /// retain the existing first-definition short-name fallback.
    fn resolve(&self, facts: &mut SemanticFacts) {
        for (index, r) in facts.refs.iter_mut().enumerate() {
            if r.kind == RefKind::Call && r.target_qualified.is_none() {
                match self.receiver_authorities.get(&index) {
                    Some(ReceiverAuthority::Absolute { owner }) => {
                        self.resolve_owned_singleton(owner, r);
                    }
                    Some(ReceiverAuthority::Lexical { owner, scope }) => {
                        if let Some(owner) = self.lexical_owner(owner, scope) {
                            self.resolve_owned_singleton(&owner, r);
                        }
                    }
                    Some(_) => {}
                    None => {
                        if let Some(qualified) = self.short_names.get(r.target_name.as_str()) {
                            r.target_qualified = Some(qualified.clone());
                        }
                    }
                }
            }
        }
    }

    fn resolve_owned_singleton(&self, owner: &str, r: &mut RefFact) {
        if !self.constant_owners.contains(owner) {
            return;
        }
        let qualified = format!("{owner}.{}", r.target_name);
        if let Some(target) = self
            .singleton_methods
            .get(&qualified)
            .filter(|target| target.definitions == 1)
        {
            r.target_qualified = Some(target.published_qualified.clone());
        }
    }
}

enum ReceiverAuthority {
    /// Leading `::` fixes lookup at the top-level namespace.
    Absolute { owner: String },
    /// Relative constant lookup starts from this lexical scope.
    Lexical { owner: String, scope: Vec<String> },
    /// Receiver type cannot be proven by this analyzer.
    Dynamic,
}

/// True when a scope-resolution receiver starts with Ruby's root `::`.
///
/// tree-sitter-ruby 0.23.1 represents the root segment as the only
/// `scope_resolution` in the chain without a `scope` field.
fn absolute_scope_resolution(mut node: Node<'_>) -> bool {
    if node.kind() != "scope_resolution" {
        return false;
    }
    loop {
        match child_by_field(node, "scope") {
            Some(scope) if scope.kind() == "scope_resolution" => node = scope,
            Some(_) => return false,
            None => return true,
        }
    }
}

/// Declaration / visibility verbs the ref pass skips: they are either
/// represented as symbols / impl edges already (mixin verbs, `attr_*`,
/// `define_method`) or are visibility markers rather than meaningful
/// call targets (`private` / `protected` / `public` / `module_function`).
///
/// The require-family verbs (`require`, `require_relative`, `load`,
/// `autoload`) are intentionally **not** in this list: they are real
/// call sites, and downstream tiers (Tier-2.5 require-graph,
/// `find_callers require_relative`) need them as Call refs. The
/// "this is a load-time declaration, not a runtime call" judgement
/// belongs to the consumer, not to the syntactic pass.
const DECLARATIVE_CALLS: &[&str] = &[
    "attr_accessor",
    "attr_reader",
    "attr_writer",
    "include",
    "extend",
    "prepend",
    "define_method",
    "private",
    "protected",
    "public",
    "module_function",
];

/// Recursive walk maintaining:
/// - `scope_stack`: enclosing class / module names, with an explicit
///   reset/restore frame for root-qualified declarations.
/// - `enclosing`: qualified name of the nearest enclosing method /
///   class / module, or `None` at top level. Refs attach this as
///   `enclosing_qualified`.
fn walk(
    node: Node<'_>,
    source: &[u8],
    scope_stack: &mut ScopeStack,
    enclosing: Option<&str>,
    resolver: &mut SameFileResolver,
    facts: &mut SemanticFacts,
) {
    match node.kind() {
        "module" | "class" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = scope_stack.qualified(&name);
            resolver.record_owner(&qualified);
            if node.kind() == "class" {
                emit_superclass(node, source, &qualified, facts);
            }
            let frame = scope_stack.enter(name);
            recurse(node, source, scope_stack, Some(&qualified), resolver, facts);
            scope_stack.exit(frame);
            return;
        }
        "method" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source);
            let singleton = within_singleton_class(node);
            let qualified = scope_stack.method_qualified(name, singleton);
            let authority = if singleton {
                MethodDefinitionAuthority::ScopedSingleton
            } else {
                MethodDefinitionAuthority::Instance
            };
            resolver.record_method(name, &qualified, authority);
            recurse(node, source, scope_stack, Some(&qualified), resolver, facts);
            return;
        }
        "singleton_method" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source);
            // `def self.x` qualifies under the enclosing container,
            // `def Foo.x` under the explicit constant — mirroring the
            // syntactic pass.
            let (qualified, authority) = match child_by_field(node, "object") {
                Some(obj) if obj.kind() != "self" => (
                    format!("{}.{name}", node_text(obj, source)),
                    MethodDefinitionAuthority::ExplicitObjectSingleton,
                ),
                _ => (
                    scope_stack.method_qualified(name, true),
                    MethodDefinitionAuthority::ScopedSingleton,
                ),
            };
            resolver.record_method(name, &qualified, authority);
            recurse(node, source, scope_stack, Some(&qualified), resolver, facts);
            return;
        }
        "call" => {
            handle_call(node, source, scope_stack, enclosing, resolver, facts);
            // fall through to recurse into receiver / arguments / block.
        }
        _ => {}
    }
    recurse(node, source, scope_stack, enclosing, resolver, facts);
}

fn recurse(
    node: Node<'_>,
    source: &[u8],
    scope_stack: &mut ScopeStack,
    enclosing: Option<&str>,
    resolver: &mut SameFileResolver,
    facts: &mut SemanticFacts,
) {
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            walk(
                cursor.node(),
                source,
                scope_stack,
                enclosing,
                resolver,
                facts,
            );
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

/// `class Dog < Animal` — the `superclass` field wraps the expression
/// after `<`. The base text is stored as written (`Base::Animal` stays
/// dotted), matching how a user would query `find_subtypes name=…`.
fn emit_superclass(
    class_node: Node<'_>,
    source: &[u8],
    type_qualified: &str,
    facts: &mut SemanticFacts,
) {
    let Some(superclass) = child_by_field(class_node, "superclass") else {
        return;
    };
    let Some(expr) = superclass.named_child(0) else {
        return;
    };
    let base = collapse_ws(node_text(expr, source));
    if base.is_empty() {
        return;
    }
    let range = expr.byte_range();
    let line = line_of(class_node);
    facts.impls.push(ImplFact {
        type_qualified: type_qualified.to_string(),
        interface_qualified: Some(base.clone()),
        kind: "inherit".to_string(),
        syntactic_kind: Some(SyntacticKind::LessThan),
        line,
        interface_byte_range: Some((range.start as u32, range.end as u32)),
    });
    // Tier-2 also surfaces the base-class name as a Type ref so
    // `find_callers Animal` returns the `Dog < Animal` site without
    // a resolution-layer assist. Tier-2.5 fills `target_qualified`
    // later by joining `resolutions` on `(blob, byte_range, kind)`.
    facts.refs.push(RefFact {
        target_name: base,
        target_qualified: None,
        kind: RefKind::Type,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: Some(type_qualified.to_string()),
        byte_range: range,
        line,
    });
}

/// A `call` node. Mixin verbs become impl edges; declaration-shaped
/// names are skipped; everything else becomes a name-level `Call` ref.
fn handle_call(
    call_node: Node<'_>,
    source: &[u8],
    scope_stack: &ScopeStack,
    enclosing: Option<&str>,
    resolver: &mut SameFileResolver,
    facts: &mut SemanticFacts,
) {
    let Some(method) = child_by_field(call_node, "method") else {
        return;
    };
    let receiver = child_by_field(call_node, "receiver");
    let has_receiver = receiver.is_some();
    let method_name = node_text(method, source);

    if !has_receiver {
        if matches!(method_name, "include" | "extend" | "prepend") && !scope_stack.is_empty() {
            emit_mixins(
                call_node,
                source,
                method_name,
                scope_stack.as_slice(),
                facts,
            );
            return;
        }
        if DECLARATIVE_CALLS.contains(&method_name) {
            return;
        }
    }

    // The method name must be a plain identifier (or operator-ish
    // identifier); exotic callees (`obj.send(:x)` still has `send` as
    // the identifier) are covered, computed callees are not.
    if method.kind() != "identifier" {
        return;
    }
    if method_name.is_empty() {
        return;
    }
    resolver.push_call(
        facts,
        RefFact {
            target_name: method_name.to_string(),
            target_qualified: None,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: enclosing.map(str::to_string),
            byte_range: method.byte_range(),
            line: line_of(method),
        },
        receiver,
        source,
        scope_stack,
    );
}

/// `include A, B` → one [`ImplFact`] per constant argument with
/// `kind = "include"` (resp. `extend` / `prepend`). Non-constant
/// arguments (`include Module.new`) are skipped.
fn emit_mixins(
    call_node: Node<'_>,
    source: &[u8],
    verb: &str,
    scope_stack: &[String],
    facts: &mut SemanticFacts,
) {
    let Some(args) = child_by_field(call_node, "arguments") else {
        return;
    };
    let type_qualified = scope_stack.join("::");
    let line = line_of(call_node);
    let mut cursor = args.walk();
    for arg in args.named_children(&mut cursor) {
        let module = match arg.kind() {
            "constant" | "scope_resolution" => collapse_ws(node_text(arg, source)),
            _ => continue,
        };
        if module.is_empty() {
            continue;
        }
        let syntactic = match verb {
            "include" => SyntacticKind::Include,
            "extend" => SyntacticKind::ExtendKw,
            "prepend" => SyntacticKind::Prepend,
            // Caller `handle_call` guards on these three verbs; any
            // other value reaching here is a bug, so default to
            // `Include` rather than panic.
            _ => SyntacticKind::Include,
        };
        let range = arg.byte_range();
        facts.impls.push(ImplFact {
            type_qualified: type_qualified.clone(),
            interface_qualified: Some(module.clone()),
            kind: verb.to_string(),
            syntactic_kind: Some(syntactic),
            line,
            interface_byte_range: Some((range.start as u32, range.end as u32)),
        });
        // Same rationale as `emit_superclass`: emit the mixin module
        // name as a Type ref so cross-file callers ("who mentions
        // Loggable?") light up at Tier-2 without a resolution-layer
        // assist.
        facts.refs.push(RefFact {
            target_name: module,
            target_qualified: None,
            kind: RefKind::Type,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: Some(type_qualified.clone()),
            byte_range: range,
            line,
        });
    }
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(RubyAnalyzer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_lang_api::{LanguageBackend, SymbolKind};

    fn assert_clean_parse(src: &str) {
        let language: tree_sitter::Language = tree_sitter_ruby::LANGUAGE.into();
        let mut parser = Parser::new();
        parser.set_language(&language).unwrap();
        let tree = parser.parse(src.as_bytes(), None).unwrap();
        assert!(
            !tree.root_node().has_error(),
            "{}",
            tree.root_node().to_sexp()
        );
    }

    fn semantic(src: &str) -> SemanticFacts {
        assert_clean_parse(src);
        crate::RubyBackend
            .analyzer()
            .expect("Ruby semantic analyzer")
            .extract_semantic(src.as_bytes())
            .unwrap()
    }

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    // ─── inheritance ───────────────────────────────────────────────

    #[test]
    fn single_inheritance_edge() {
        let f = semantic("class Dog < Animal\nend\n");
        assert_eq!(f.impls.len(), 1);
        assert_eq!(f.impls[0].type_qualified, "Dog");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(f.impls[0].kind, "inherit");
    }

    #[test]
    fn scoped_superclass_kept_verbatim() {
        let f = semantic("class E < Base::Animal\nend\n");
        assert_eq!(
            f.impls[0].interface_qualified.as_deref(),
            Some("Base::Animal")
        );
    }

    #[test]
    fn no_superclass_no_edge() {
        let f = semantic("class Plain\nend\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn nested_class_qualifies_under_modules() {
        let f = semantic("module Outer\n  class Inner < Base\n  end\nend\n");
        assert_eq!(f.impls[0].type_qualified, "Outer::Inner");
    }

    // ─── mixins ────────────────────────────────────────────────────

    #[test]
    fn include_extend_prepend_edges() {
        let src = "\
class Widget
  include Loggable
  extend Helpers
  prepend Patch
end
";
        let f = semantic(src);
        let kinds: Vec<(&str, &str)> = f
            .impls
            .iter()
            .map(|i| (i.kind.as_str(), i.interface_qualified.as_deref().unwrap()))
            .collect();
        assert_eq!(
            kinds,
            &[
                ("include", "Loggable"),
                ("extend", "Helpers"),
                ("prepend", "Patch"),
            ]
        );
        assert!(f.impls.iter().all(|i| i.type_qualified == "Widget"));
    }

    #[test]
    fn include_with_multiple_and_scoped_modules() {
        let f = semantic("module M\n  include A, Deep::B\nend\n");
        let ifaces: Vec<&str> = f
            .impls
            .iter()
            .filter_map(|i| i.interface_qualified.as_deref())
            .collect();
        assert_eq!(ifaces, &["A", "Deep::B"]);
        assert!(f.impls.iter().all(|i| i.kind == "include"));
    }

    #[test]
    fn top_level_include_skipped() {
        // `include` outside a class/module body monkey-patches Object;
        // there is no type to attach the edge to.
        let f = semantic("include Helpers\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn computed_mixin_skipped() {
        let f = semantic("class C\n  include Module.new\nend\n");
        assert!(f.impls.is_empty());
    }

    // ─── refs: calls ───────────────────────────────────────────────

    #[test]
    fn top_level_call_has_no_enclosing() {
        let f = semantic("greet()\n");
        let c = calls(&f);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].target_name, "greet");
        assert_eq!(c[0].target_qualified, None);
        assert_eq!(c[0].enclosing_qualified, None);
    }

    #[test]
    fn call_inside_method_enclosed_by_qualified_method() {
        let src = "class W\n  def render\n    helper(1)\n  end\n  def helper(_); end\nend\n";
        let f = semantic(src);
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W#render"));
        assert_eq!(hit.target_qualified.as_deref(), Some("W#helper"));
    }

    #[test]
    fn same_file_callee_resolves_to_qualified_target() {
        // Definition trails the call site lexically — the post-pass
        // resolves it regardless of order, mirroring the C backend.
        let src = "\
class W
  def render
    helper(1)
    setup()
  end
  def helper(_); end
  def self.setup; end
end
";
        let f = semantic(src);
        let helper = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(helper.target_qualified.as_deref(), Some("W#helper"));
        let setup = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "setup")
            .expect("setup call missing");
        assert_eq!(setup.target_qualified.as_deref(), Some("W.setup"));
    }

    #[test]
    fn ambiguous_short_name_takes_first_definition() {
        // Instance and singleton variants share the short name `run`.
        // With no receiver type at the call site, the first definition
        // in source order wins — documented best-effort behavior.
        let src = "\
class W
  def run; end
  def self.run; end
  def kick
    run()
  end
end
";
        let f = semantic(src);
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("W#run"));
    }

    #[test]
    fn explicit_constant_receivers_resolve_independent_of_definition_order() {
        for src in [
            "\
class Alpha
  def self.run; end
end
class Beta
  def self.run; end
end
Alpha.run()
Beta.run()
",
            "\
class Beta
  def self.run; end
end
class Alpha
  def self.run; end
end
Alpha.run()
Beta.run()
",
        ] {
            let facts = semantic(src);
            let targets: Vec<Option<&str>> = calls(&facts)
                .into_iter()
                .filter(|r| r.target_name == "run")
                .map(|r| r.target_qualified.as_deref())
                .collect();
            assert_eq!(targets, [Some("Alpha.run"), Some("Beta.run")]);
        }
    }

    #[test]
    fn relative_receiver_uses_nearest_lexical_owner() {
        let src = "\
class Alpha
  def self.run; end
end
module Outer
  class Alpha
    def self.run; end
  end
  def self.invoke
    Alpha.run()
  end
end
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Outer::Alpha.run"));
    }

    #[test]
    fn lexical_owner_without_method_blocks_global_fallback() {
        let src = "\
class Alpha
  def self.run; end
end
module Outer
  class Alpha
  end
  def self.invoke
    Alpha.run()
  end
end
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified, None);
    }

    #[test]
    fn absolute_receiver_uses_canonical_owner() {
        let src = "\
module Outer
  class Nested
    def self.run; end
  end
end
::Outer::Nested.run()
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Outer::Nested.run"));
    }

    #[test]
    fn absolute_declaration_target_matches_published_symbol() {
        let src = "\
class ::Alpha
  def self.run; end
end
::Alpha.run()
";
        let syntactic = crate::RubyBackend
            .extract_syntactic(src.as_bytes())
            .unwrap();
        let published = syntactic
            .symbols
            .iter()
            .find(|symbol| symbol.name == "run" && symbol.kind == SymbolKind::Method)
            .expect("singleton run symbol missing")
            .qualified
            .as_str();
        let facts = semantic(src);
        let target = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing")
            .target_qualified
            .as_deref();
        assert_eq!(target, Some(published));
    }

    #[test]
    fn nested_absolute_declaration_target_matches_published_symbol() {
        let src = "\
module Outer
  class ::Alpha
    class Nested
      def self.work; end
    end
    def self.run; end
    def self.invoke
      Nested.work()
    end
  end
end
::Alpha.run()
::Alpha::Nested.work()
";
        let syntactic = crate::RubyBackend
            .extract_syntactic(src.as_bytes())
            .unwrap();
        let published = |name: &str| {
            syntactic
                .symbols
                .iter()
                .find(|symbol| symbol.name == name && symbol.kind == SymbolKind::Method)
                .expect("singleton symbol missing")
                .qualified
                .as_str()
        };
        let facts = semantic(src);
        let run = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(run.target_qualified.as_deref(), Some(published("run")));

        let work_targets: Vec<_> = calls(&facts)
            .into_iter()
            .filter(|r| r.target_name == "work")
            .map(|r| r.target_qualified.as_deref())
            .collect();
        assert_eq!(
            work_targets,
            [Some(published("work")), Some(published("work"))]
        );
    }

    #[test]
    fn relative_qualified_receiver_requires_same_file_owner() {
        let src = "\
module Outer
  module Inner
    class Nested
      def self.run; end
    end
  end
  def self.invoke
    Inner::Nested.run()
    Missing::Nested.run()
  end
end
";
        let facts = semantic(src);
        let targets: Vec<Option<&str>> = calls(&facts)
            .into_iter()
            .filter(|r| r.target_name == "run")
            .map(|r| r.target_qualified.as_deref())
            .collect();
        assert_eq!(targets, [Some("Outer::Inner::Nested.run"), None]);
    }

    #[test]
    fn non_static_explicit_receivers_stay_unresolved() {
        let src = "\
class Alpha
  def self.run; end
end
Missing.run()
def invoke(obj)
  obj.run()
end
";
        let facts = semantic(src);
        let targets: Vec<Option<&str>> = calls(&facts)
            .into_iter()
            .filter(|r| r.target_name == "run")
            .map(|r| r.target_qualified.as_deref())
            .collect();
        assert_eq!(targets, [None, None]);
    }

    #[test]
    fn ambiguous_qualified_receiver_stays_unresolved() {
        let src = "\
class Alpha
  def self.run; end
  def self.run; end
end
Alpha.run()
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified, None);
    }

    #[test]
    fn explicit_object_singleton_definition_stays_out_of_exact_index() {
        let src = "\
class Alpha; end
module Outer
  class Alpha; end
  def Alpha.run; end
  Alpha.run()
end
::Alpha.run()
";
        let facts = semantic(src);
        let targets: Vec<_> = calls(&facts)
            .into_iter()
            .filter(|r| r.target_name == "run")
            .map(|r| r.target_qualified.as_deref())
            .collect();
        assert_eq!(targets, [None, None]);
    }

    #[test]
    fn explicit_object_singleton_keeps_receiverless_fallback() {
        let src = "\
module Outer
  class Alpha; end
  def Alpha.run; end
end
run()
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Alpha.run"));
    }

    #[test]
    fn semantic_absolute_scope_exit_restores_outer_owner() {
        let src = "\
module Outer
  class ::Alpha
    def self.run; end
  end
  class Sibling
    def self.after; end
    def self.invoke
      Sibling.after()
    end
  end
end
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "after")
            .expect("after call missing");
        assert_eq!(
            hit.target_qualified.as_deref(),
            Some("Outer::Sibling.after")
        );
    }

    #[test]
    fn singleton_class_method_enters_exact_index() {
        let src = "\
class Alpha
  class << self
    def run; end
  end
end
Alpha.run()
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Alpha.run"));
    }

    #[test]
    fn receiverless_call_keeps_short_name_best_effort() {
        let src = "\
class Alpha
  def self.run; end
end
run()
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Alpha.run"));
    }

    #[test]
    fn self_receiver_keeps_existing_short_name_resolution() {
        let src = "\
class Alpha
  def self.run; end
  def self.invoke
    self.run()
  end
end
";
        let facts = semantic(src);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Alpha.run"));
    }

    #[test]
    fn super_keeps_existing_non_call_behavior() {
        let src = "\
class Parent
  def run; end
end
class Child < Parent
  def run
    super()
  end
end
";
        let facts = semantic(src);
        assert!(
            calls(&facts).into_iter().all(|r| r.target_name != "super"),
            "super should remain outside this analyzer's call facts"
        );
    }

    #[test]
    fn super_receiver_keeps_ref_index_alignment() {
        let src = "\
class Parent
  def run; end
end
class Child < Parent
  def run
    super.run()
  end
end
";
        let facts = semantic(src);
        assert_eq!(facts.refs[0].kind, RefKind::Type);
        let hit = calls(&facts)
            .into_iter()
            .find(|r| r.target_name == "run")
            .expect("super.run call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("Parent#run"));
    }

    #[test]
    fn analyzer_revision_covers_receiver_qualified_resolution() {
        assert_eq!(
            crate::RubyBackend
                .analyzer()
                .expect("Ruby semantic analyzer")
                .revision(),
            2
        );
    }

    #[test]
    fn call_inside_singleton_method_uses_dot_qualifier() {
        let src = "class W\n  def self.build\n    setup(1)\n  end\nend\n";
        let f = semantic(src);
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "setup")
            .expect("setup call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.build"));
    }

    #[test]
    fn receiver_call_is_name_level_unresolved() {
        let f = semantic("def run(obj)\n  obj.render\nend\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "render")
            .expect("render call missing");
        // Receiver type unknown without Tier-3 → name-level only.
        assert_eq!(hit.target_qualified, None);
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("run"));
    }

    #[test]
    fn parenless_zero_arg_call_not_emitted() {
        // Documented best-effort limit: `helper` with no parens and no
        // args parses as a bare identifier, indistinguishable from a
        // variable read, so no ref is emitted.
        let f = semantic("def run\n  helper\nend\n");
        assert!(calls(&f).is_empty());
    }

    #[test]
    fn declaration_only_calls_not_emitted_as_refs() {
        // Declaration / visibility verbs are skipped: `include` is an
        // impl edge, `attr_reader` is a symbol, `private` is a marker.
        // Require-family calls (`require`, `require_relative`) are NOT
        // in this list — they emit as Call refs so cross-file callers
        // can find them. See `require_family_emits_as_call_ref` below.
        let src = "\
class C
  include M
  attr_reader :x
  private def hidden; end
end
";
        let f = semantic(src);
        assert!(calls(&f).is_empty(), "got {:?}", calls(&f));
    }

    #[test]
    fn require_family_emits_as_call_ref() {
        // The syntactic pass treats `require` / `require_relative` /
        // `load` / `autoload` as ordinary calls. Whether they are
        // "really" runtime calls or load-time declarations is a
        // judgement for Tier-2.5 / Tier-3 / the UI to make.
        let f = semantic("require \"json\"\nrequire_relative \"util\"\nload \"x.rb\"\n");
        let names: Vec<&str> = calls(&f).iter().map(|r| r.target_name.as_str()).collect();
        assert_eq!(names, &["require", "require_relative", "load"]);
        // Targets are name-only: Tier-2.5 resolves the require graph.
        assert!(calls(&f).iter().all(|r| r.target_qualified.is_none()));
    }

    // ─── refs: types ───────────────────────────────────────────────

    #[test]
    fn superclass_emits_type_ref() {
        let f = semantic(
            "class Dog < Animal
end
",
        );
        let types: Vec<&RefFact> = f.refs.iter().filter(|r| r.kind == RefKind::Type).collect();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].target_name, "Animal");
        // Resolution is left to Tier-2.5.
        assert_eq!(types[0].target_qualified, None);
        assert_eq!(types[0].enclosing_qualified.as_deref(), Some("Dog"));
    }

    #[test]
    fn mixin_modules_emit_type_refs() {
        let src = "class W
  include Loggable
  extend Helpers
  prepend Patch
end
";
        let f = semantic(src);
        let names: Vec<&str> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::Type)
            .map(|r| r.target_name.as_str())
            .collect();
        assert_eq!(names, &["Loggable", "Helpers", "Patch"]);
        // Enclosing is the class the mixin sits inside.
        assert!(
            f.refs
                .iter()
                .filter(|r| r.kind == RefKind::Type)
                .all(|r| r.enclosing_qualified.as_deref() == Some("W"))
        );
    }

    #[test]
    fn scoped_superclass_type_ref_keeps_full_path() {
        let f = semantic(
            "class E < Base::Animal
end
",
        );
        let t = f
            .refs
            .iter()
            .find(|r| r.kind == RefKind::Type)
            .expect("type ref missing");
        assert_eq!(t.target_name, "Base::Animal");
    }

    #[test]
    fn dynamic_send_resolves_to_send_only() {
        // Documented best-effort limit: `send(:name)` is dynamic
        // dispatch; the ref targets `send`, not the symbol it invokes.
        let f = semantic("def run(obj)\n  obj.send(:render)\nend\n");
        let names: Vec<&str> = calls(&f).iter().map(|r| r.target_name.as_str()).collect();
        assert_eq!(names, &["send"]);
    }

    #[test]
    fn imports_left_to_syntactic_pass() {
        let f = semantic("require \"json\"\n");
        assert!(f.imports.is_empty());
    }

    #[test]
    fn include_extend_prepend_emit_syntactic_kinds() {
        let src = "class W\n  include Loggable\n  extend Helpers\n  prepend Patch\nend\n";
        let f = semantic(src);
        let by_kind: std::collections::HashMap<&str, Option<SyntacticKind>> = f
            .impls
            .iter()
            .map(|i| (i.kind.as_str(), i.syntactic_kind))
            .collect();
        assert_eq!(
            by_kind.get("include").copied().flatten(),
            Some(SyntacticKind::Include)
        );
        assert_eq!(
            by_kind.get("extend").copied().flatten(),
            Some(SyntacticKind::ExtendKw)
        );
        assert_eq!(
            by_kind.get("prepend").copied().flatten(),
            Some(SyntacticKind::Prepend)
        );
    }
}
