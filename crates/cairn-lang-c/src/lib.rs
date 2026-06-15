//! `cairn-lang-c` — C backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-c parse tree and emits
//! [`SymbolFact`]s for function definitions and prototypes, struct /
//! union / enum / typedef declarations, top-level variables, `#define`
//! macros, and enum constants. `static` maps to [`Visibility::Private`]
//! (translation-unit scope); everything else is `Public`.
//!
//! Tier-2 facts ride the same pass:
//! - `#include` directives become [`ImportFact`]s. A local include
//!   (`#include "foo.h"`) stores the bare path `foo.h`; a system
//!   include (`#include <stdio.h>`) keeps its angle brackets
//!   (`<stdio.h>`) so the import graph can tell the two apart.
//! - Same-file call sites inside function bodies become [`RefFact`]s.
//!   A callee that is defined (or prototyped) in the same file gets a
//!   resolved `target_qualified`; a cross-file callee stays unresolved
//!   and is therefore hidden from `find_references`' default outgoing
//!   view (visible with `include_noise`). This is best-effort: a
//!   parenthesized macro invocation is syntactically indistinguishable
//!   from a function call, so it is recorded as an (unresolved) call
//!   too.
//!
//! Preprocessor conditionals are handled naively: tree-sitter parses
//! every branch, so a symbol defined in both arms of an `#ifdef` /
//! `#else` pair is indexed twice (dedup is a future concern). `.h`
//! headers are routed by the register layer using a small content
//! heuristic, so this backend only claims `.c` directly. Tier-3
//! (clangd) is out of scope for this backend.

#![forbid(unsafe_code)]

use cairn_lang_api::{
    ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, RefFact, RefKind, SymbolFact,
    SymbolKind, SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct CBackend;

impl LanguageBackend for CBackend {
    fn name(&self) -> &'static str {
        "c"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.c"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-c"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_c::LANGUAGE.into();
        let mut facts = extract(source, &language, CVisitor::new())?;
        resolve_same_file_callees(&mut facts);
        Ok(facts)
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_C: fn() -> Box<dyn LanguageBackend> = || Box::new(CBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct CVisitor {
    /// Tracks the enclosing function definition so call refs can set
    /// `enclosing_idx`. C declarations are otherwise flat, so the
    /// separator never appears in a qualified name.
    nesting: NestingTracker,
}

impl CVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("::"),
        }
    }
}

impl Visitor for CVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        match node.kind() {
            "preproc_include" => {
                if let Some(import) = match_include(node, source) {
                    facts.imports.push(import);
                }
            }
            // The preprocessor is scope-blind, so `#define` is emitted
            // wherever it appears (even inside a function body).
            "preproc_def" | "preproc_function_def" => emit_macro(node, source, facts),
            "function_definition" => {
                if let Some(idx) = emit_function_definition(node, source, facts) {
                    self.nesting.push(idx, node.end_byte());
                }
            }
            "declaration" if is_top_level(node) => emit_declaration(node, source, facts),
            "type_definition" if is_top_level(node) => emit_typedef(node, source, facts),
            "struct_specifier" | "union_specifier" | "enum_specifier" if is_top_level(node) => {
                emit_tagged_type(node, source, facts);
            }
            "call_expression" => emit_call_ref(node, source, facts, self.nesting.current_parent()),
            _ => {}
        }
    }
}

// ─── symbol emission ───────────────────────────────────────────────────────

struct SymbolParts {
    kind: SymbolKind,
    name: String,
    body_start: Option<usize>,
    visibility: Visibility,
    parent_idx: Option<usize>,
}

/// Push one symbol and return its index in `facts.symbols`. C has no
/// namespaces, so `qualified` is always the bare name.
fn emit_symbol(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    parts: SymbolParts,
) -> usize {
    let signature = signature_slice(node, source, parts.body_start);
    let doc = extract_doc(node, source);

    facts.symbols.push(SymbolFact {
        qualified: parts.name.clone(),
        name: parts.name,
        kind: parts.kind,
        signature,
        doc,
        visibility: Some(parts.visibility),
        byte_range: node.byte_range(),
        line_range: line_of(node)..end_line_of(node),
        body_start: parts.body_start,
        parent_idx: parts.parent_idx,
    });
    facts.symbols.len() - 1
}

fn emit_function_definition(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
) -> Option<usize> {
    let declarator = child_by_field(node, "declarator")?;
    let (name_node, _) = classify_declarator(declarator)?;
    let body_start = child_by_field(node, "body").map(|n| n.start_byte());
    Some(emit_symbol(
        node,
        source,
        facts,
        SymbolParts {
            kind: SymbolKind::Function,
            name: node_text(name_node, source).to_string(),
            body_start,
            visibility: c_visibility(node, source),
            parent_idx: None,
        },
    ))
}

/// A top-level `declaration` is either a function prototype (its
/// declarator chain contains a `function_declarator`) or a global
/// variable. A function *pointer* variable counts as a variable.
fn emit_declaration(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let visibility = c_visibility(node, source);
    let mut cursor = node.walk();
    let declarators: Vec<Node<'_>> = node
        .children_by_field_name("declarator", &mut cursor)
        .collect();
    for declarator in declarators {
        let Some((name_node, is_function)) = classify_declarator(declarator) else {
            continue;
        };
        let kind = if is_function {
            SymbolKind::Function
        } else {
            SymbolKind::Variable
        };
        emit_symbol(
            node,
            source,
            facts,
            SymbolParts {
                kind,
                name: node_text(name_node, source).to_string(),
                body_start: None,
                visibility,
                parent_idx: None,
            },
        );
    }
}

/// `typedef`. The aliased name gets kind `Struct` / `Union` / `Enum`
/// when it names an *anonymous* aggregate defined in place
/// (`typedef struct { ... } foo_t;` — the idiomatic C pattern), and
/// `TypeAlias` otherwise. A named aggregate in the same statement
/// (`typedef struct foo { ... } foo_t;`) is emitted separately by the
/// specifier arm of the visitor.
fn emit_typedef(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let ty = child_by_field(node, "type");
    // Cut the signature at the aggregate body, if any, so a large
    // struct does not bloat the outline.
    let body_start = ty
        .and_then(|t| child_by_field(t, "body"))
        .map(|b| b.start_byte());
    let kind_for = |_: &Node<'_>| -> SymbolKind {
        let Some(ty) = ty else {
            return SymbolKind::TypeAlias;
        };
        let anonymous_body =
            child_by_field(ty, "body").is_some() && child_by_field(ty, "name").is_none();
        match ty.kind() {
            "struct_specifier" if anonymous_body => SymbolKind::Struct,
            "union_specifier" if anonymous_body => SymbolKind::Union,
            "enum_specifier" if anonymous_body => SymbolKind::Enum,
            _ => SymbolKind::TypeAlias,
        }
    };

    let mut cursor = node.walk();
    let declarators: Vec<Node<'_>> = node
        .children_by_field_name("declarator", &mut cursor)
        .collect();
    for declarator in declarators {
        let Some((name_node, _)) = classify_declarator(declarator) else {
            continue;
        };
        emit_symbol(
            node,
            source,
            facts,
            SymbolParts {
                kind: kind_for(&declarator),
                name: node_text(name_node, source).to_string(),
                body_start,
                visibility: Visibility::Public,
                parent_idx: None,
            },
        );
    }
}

/// Named `struct` / `union` / `enum` definition (a specifier with a
/// body). Fires wherever the specifier appears — standalone, as a
/// declaration's type, or inside a typedef — which covers every
/// placement with one rule. Enum constants are emitted as `Constant`
/// children; for an anonymous enum (`enum { A, B };`) the constants
/// are still emitted, just without a parent.
fn emit_tagged_type(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let Some(body) = child_by_field(node, "body") else {
        return; // `struct foo;` forward declaration or plain type use.
    };

    let kind = match node.kind() {
        "struct_specifier" => SymbolKind::Struct,
        "union_specifier" => SymbolKind::Union,
        _ => SymbolKind::Enum,
    };

    let parent_idx = child_by_field(node, "name").map(|name_node| {
        emit_symbol(
            node,
            source,
            facts,
            SymbolParts {
                kind: kind.clone(),
                name: node_text(name_node, source).to_string(),
                body_start: Some(body.start_byte()),
                visibility: Visibility::Public,
                parent_idx: None,
            },
        )
    });

    if kind != SymbolKind::Enum {
        return;
    }
    let mut cursor = body.walk();
    let enumerators: Vec<Node<'_>> = body
        .named_children(&mut cursor)
        .filter(|child| child.kind() == "enumerator")
        .collect();
    for enumerator in enumerators {
        let Some(name_node) = child_by_field(enumerator, "name") else {
            continue;
        };
        emit_symbol(
            enumerator,
            source,
            facts,
            SymbolParts {
                kind: SymbolKind::Constant,
                name: node_text(name_node, source).to_string(),
                body_start: None,
                visibility: Visibility::Public,
                parent_idx,
            },
        );
    }
}

fn emit_macro(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let Some(name_node) = child_by_field(node, "name") else {
        return;
    };
    // Cutting at the replacement text keeps the signature to
    // `#define FOO` / `#define FOO(a, b)`.
    let body_start = child_by_field(node, "value").map(|n| n.start_byte());
    emit_symbol(
        node,
        source,
        facts,
        SymbolParts {
            kind: SymbolKind::Macro,
            name: node_text(name_node, source).to_string(),
            body_start,
            visibility: Visibility::Public,
            parent_idx: None,
        },
    );
}

// ─── refs and imports ──────────────────────────────────────────────────────

/// Fill `target_qualified` for call refs whose callee is a function
/// defined (or prototyped) in the same file. C qualified names are the
/// bare name, so the resolution is a name-set lookup; running it after
/// the walk also resolves calls that lexically precede the definition.
/// Cross-file callees keep `target_qualified: None`, which hides them
/// from `find_references`' default outgoing view (visible with
/// `include_noise`).
fn resolve_same_file_callees(facts: &mut SyntacticFacts) {
    let SyntacticFacts { symbols, refs, .. } = facts;
    let functions: std::collections::HashSet<&str> = symbols
        .iter()
        .filter(|s| s.kind == SymbolKind::Function)
        .map(|s| s.qualified.as_str())
        .collect();
    for r in refs.iter_mut() {
        if r.kind == RefKind::Call
            && r.target_qualified.is_none()
            && functions.contains(r.target_name.as_str())
        {
            r.target_qualified = Some(r.target_name.clone());
        }
    }
}

/// Best-effort same-file call ref. Only direct `identifier(...)` calls
/// inside a function definition are recorded; calls through function
/// pointers reached via field or subscript expressions are skipped.
/// Parenthesized macro invocations parse as `call_expression` too and
/// are deliberately kept — see the module docs.
fn emit_call_ref(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    enclosing_idx: Option<usize>,
) {
    let Some(enclosing_idx) = enclosing_idx else {
        return; // e.g. a call inside a global initializer.
    };
    let Some(function) = child_by_field(node, "function") else {
        return;
    };
    if function.kind() != "identifier" {
        return;
    }
    facts.refs.push(RefFact {
        target_name: node_text(function, source).to_string(),
        target_qualified: None,
        kind: RefKind::Call,
        type_role: None,
        enclosing_idx: Some(enclosing_idx),
        enclosing_qualified: None,
        byte_range: function.byte_range(),
        line: line_of(function),
    });
}

fn match_include(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let path = child_by_field(node, "path")?;
    let raw = node_text(path, source).trim();
    let to_module = if path.kind() == "string_literal" {
        // Local include: store the bare path.
        raw.trim_matches('"').to_string()
    } else {
        // System include: keep `<...>` as the system marker.
        raw.to_string()
    };
    if to_module.is_empty() || to_module == "<>" {
        return None;
    }
    Some(ImportFact {
        to_module,
        imported: None,
        alias: None,
        is_reexport: false,
        line: line_of(node),
    })
}

// ─── declarator analysis ───────────────────────────────────────────────────

/// Walk a declarator chain down to the declared identifier.
///
/// Returns the name node and whether the chain declares a function
/// (definition or prototype). A parenthesized pointer directly under a
/// `function_declarator` — `int (*fp)(int)` — declares a function
/// *pointer* and is reported as not-a-function. A function returning a
/// function pointer (`int (*get(int))(char)`) is rare enough that it
/// is also reported as not-a-function in v1.
fn classify_declarator(node: Node<'_>) -> Option<(Node<'_>, bool)> {
    match node.kind() {
        "identifier" | "type_identifier" | "field_identifier" => Some((node, false)),
        "init_declarator"
        | "pointer_declarator"
        | "array_declarator"
        | "attributed_declarator"
        | "parenthesized_declarator" => classify_declarator(declarator_child(node)?),
        "function_declarator" => {
            let inner = child_by_field(node, "declarator")?;
            let is_pointer = inner.kind() == "parenthesized_declarator"
                && first_named_child(inner).is_some_and(|n| n.kind() == "pointer_declarator");
            let (name, _) = classify_declarator(inner)?;
            Some((name, !is_pointer))
        }
        _ => None,
    }
}

fn declarator_child(node: Node<'_>) -> Option<Node<'_>> {
    child_by_field(node, "declarator").or_else(|| first_named_child(node))
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

// ─── scope and visibility ──────────────────────────────────────────────────

/// True when `node` sits at translation-unit scope, looking through
/// preprocessor conditionals and declaration wrappers. Function bodies
/// and aggregate member lists end the search.
fn is_top_level(node: Node<'_>) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "translation_unit" => return true,
            "function_definition" | "compound_statement" | "field_declaration_list" => {
                return false;
            }
            _ => current = parent.parent(),
        }
    }
    false
}

/// `static` means translation-unit scope, which maps to `Private`.
fn c_visibility(node: Node<'_>, source: &[u8]) -> Visibility {
    let mut cursor = node.walk();
    let is_static = node.named_children(&mut cursor).any(|child| {
        child.kind() == "storage_class_specifier" && node_text(child, source) == "static"
    });
    if is_static {
        Visibility::Private
    } else {
        Visibility::Public
    }
}

// ─── doc comments ──────────────────────────────────────────────────────────

/// Doc comment for a symbol: the comment block immediately above the
/// node, or above its wrapping declaration when the node is a specifier
/// nested in a `declaration` / `type_definition`.
fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    doc_from_preceding_comments(node, source).or_else(|| {
        node.parent().and_then(|parent| match parent.kind() {
            "declaration" | "type_definition" => doc_from_preceding_comments(parent, source),
            _ => None,
        })
    })
}

fn doc_from_preceding_comments(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| {
        (sibling.kind() == "comment").then(|| DocCommentPart::Append(strip_c_comment_marker(text)))
    })
}

fn strip_c_comment_marker(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix("//") {
        rest.trim_start_matches('/').trim().to_string()
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

    fn facts(src: &[u8]) -> SyntacticFacts {
        CBackend.extract_syntactic(src).unwrap()
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
        assert_eq!(CBackend.parser_id(), "tree-sitter-c");
    }

    #[test]
    fn claims_c_extension_only() {
        assert_eq!(CBackend.file_patterns(), &["*.c"]);
    }

    #[test]
    fn extracts_function_struct_union_enum_typedef_macro_global() {
        let src = br#"
#include <stdio.h>

#define MAX_SIZE 128
#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* doc on point */
struct point { int x; int y; };

union number { int i; float f; };

enum color { RED, GREEN = 5 };

typedef int my_int;

// doc on add
int add(int a, int b) { return a + b; }

int global_counter = 0;
"#;
        let facts = facts(src);

        assert_eq!(symbol(&facts, "add").kind, SymbolKind::Function);
        assert_eq!(symbol(&facts, "point").kind, SymbolKind::Struct);
        assert_eq!(symbol(&facts, "number").kind, SymbolKind::Union);
        assert_eq!(symbol(&facts, "color").kind, SymbolKind::Enum);
        assert_eq!(symbol(&facts, "my_int").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&facts, "MAX_SIZE").kind, SymbolKind::Macro);
        assert_eq!(symbol(&facts, "MIN").kind, SymbolKind::Macro);
        assert_eq!(symbol(&facts, "global_counter").kind, SymbolKind::Variable);

        // Enum constants hang off the enum symbol.
        let color_idx = facts.symbols.iter().position(|s| s.name == "color");
        assert_eq!(symbol(&facts, "RED").kind, SymbolKind::Constant);
        assert_eq!(symbol(&facts, "RED").parent_idx, color_idx);
        assert_eq!(symbol(&facts, "GREEN").parent_idx, color_idx);

        assert!(
            symbol(&facts, "add")
                .doc
                .as_deref()
                .unwrap()
                .contains("doc on add")
        );
        assert!(
            symbol(&facts, "point")
                .doc
                .as_deref()
                .unwrap()
                .contains("doc on point")
        );

        // Signatures stop at the body.
        assert_eq!(
            symbol(&facts, "add").signature.as_deref(),
            Some("int add(int a, int b)")
        );
        assert_eq!(
            symbol(&facts, "MIN").signature.as_deref(),
            Some("#define MIN(a, b)")
        );
    }

    #[test]
    fn prototype_and_definition_coexist() {
        let src = br#"
int init(void);

int init(void) { return 0; }
"#;
        let facts = facts(src);
        let inits: Vec<_> = facts.symbols.iter().filter(|s| s.name == "init").collect();
        assert_eq!(inits.len(), 2);
        assert!(inits.iter().all(|s| s.kind == SymbolKind::Function));
        // The definition has a body; the prototype does not.
        assert_eq!(inits.iter().filter(|s| s.body_start.is_some()).count(), 1);
    }

    #[test]
    fn static_symbols_are_private() {
        let src = br#"
static int helper(void) { return 1; }
static int counter = 0;
int exported(void) { return 2; }
extern int shared;
"#;
        let facts = facts(src);
        assert_eq!(
            symbol(&facts, "helper").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "counter").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "exported").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "shared").visibility,
            Some(Visibility::Public)
        );
    }

    #[test]
    fn typedef_struct_pattern_and_function_pointers() {
        let src = br#"
typedef struct { int x; int y; } point_t;

typedef struct node { struct node *next; } node_t;

typedef void (*callback_t)(int);

int (*global_handler)(int, char *);

int *returns_pointer(void) { return 0; }
"#;
        let facts = facts(src);

        // Anonymous struct typedef: the alias IS the struct.
        assert_eq!(symbol(&facts, "point_t").kind, SymbolKind::Struct);
        assert_eq!(
            symbol(&facts, "point_t").signature.as_deref(),
            Some("typedef struct")
        );

        // Named struct typedef: both names indexed, alias as TypeAlias.
        assert_eq!(symbol(&facts, "node").kind, SymbolKind::Struct);
        assert_eq!(symbol(&facts, "node_t").kind, SymbolKind::TypeAlias);

        // Function-pointer typedef is an alias, not a function.
        assert_eq!(symbol(&facts, "callback_t").kind, SymbolKind::TypeAlias);

        // Global function pointer is a variable, not a function.
        assert_eq!(symbol(&facts, "global_handler").kind, SymbolKind::Variable);

        // Function returning a pointer is still a function.
        assert_eq!(symbol(&facts, "returns_pointer").kind, SymbolKind::Function);
    }

    #[test]
    fn ifdef_branches_both_indexed() {
        let src = br#"
#ifdef FAST_PATH
int compute(int x) { return x << 1; }
#else
int compute(int x) { return x * 2; }
#endif
"#;
        let facts = facts(src);
        let computes: Vec<_> = facts
            .symbols
            .iter()
            .filter(|s| s.name == "compute")
            .collect();
        assert_eq!(computes.len(), 2, "both #ifdef arms are indexed");
        assert!(computes.iter().all(|s| s.kind == SymbolKind::Function));
    }

    #[test]
    fn includes_distinguish_local_from_system() {
        let src = br#"
#include <stdio.h>
#include <sys/types.h>
#include "local.h"
#include "nested/util.h"
"#;
        let facts = facts(src);
        let modules: Vec<&str> = facts.imports.iter().map(|i| i.to_module.as_str()).collect();
        assert_eq!(
            modules,
            ["<stdio.h>", "<sys/types.h>", "local.h", "nested/util.h"],
            "system includes keep angle brackets, local includes are bare paths"
        );
        assert!(facts.imports.iter().all(|i| !i.is_reexport));
    }

    #[test]
    fn same_file_call_refs_attach_to_enclosing_function() {
        let src = br#"
#define LOG(msg) puts(msg)

int helper(int x) { return x; }

int caller(int x) {
    LOG("hi");
    return helper(x);
}
"#;
        let facts = facts(src);
        let caller_idx = facts.symbols.iter().position(|s| s.name == "caller");

        let helper_ref = facts
            .refs
            .iter()
            .find(|r| r.target_name == "helper")
            .expect("call to helper recorded");
        assert_eq!(helper_ref.kind, RefKind::Call);
        assert_eq!(helper_ref.enclosing_idx, caller_idx);
        // Same-file callee: resolved, so the default outgoing view of
        // find_references (resolved calls only) can show it.
        assert_eq!(helper_ref.target_qualified.as_deref(), Some("helper"));

        // Best-effort caveat: a parenthesized macro invocation parses
        // as a call_expression and cannot be told apart from a real
        // function call, so LOG(...) is recorded as a call too.
        assert!(
            facts.refs.iter().any(|r| r.target_name == "LOG"),
            "macro invocations are (knowingly) recorded as calls"
        );
    }

    #[test]
    fn same_file_callees_resolve_cross_file_callees_stay_unresolved() {
        let src = br#"
int prototyped(int x);

int forward_target(int x) { return x; }

int caller(int x) {
    extern_helper(x); /* not declared anywhere in this file */
    prototyped(x);    /* prototype above, definition elsewhere */
    return forward_target(x);
}

int late_definition(int x) { return calls_late(x); }
int calls_late(int x) { return x; }
"#;
        let facts = facts(src);
        let resolved = |name: &str| {
            facts
                .refs
                .iter()
                .find(|r| r.target_name == name)
                .unwrap_or_else(|| panic!("{name} ref missing"))
                .target_qualified
                .as_deref()
        };

        // Prototype-only and definition callees resolve in-file.
        assert_eq!(resolved("prototyped"), Some("prototyped"));
        assert_eq!(resolved("forward_target"), Some("forward_target"));
        // Resolution runs after the walk, so a call that lexically
        // precedes the callee's definition still resolves.
        assert_eq!(resolved("calls_late"), Some("calls_late"));
        // A callee with no declaration in this file stays unresolved
        // (hidden from the default outgoing view, shown by
        // include_noise).
        assert_eq!(resolved("extern_helper"), None);
    }

    #[test]
    fn ignores_function_local_declarations() {
        let src = br#"
void f(void) {
    int local = 1;
    struct inner { int x; };
    int proto(void);
}
"#;
        let facts = facts(src);
        assert!(facts.symbols.iter().all(|s| s.name != "local"));
        assert!(facts.symbols.iter().all(|s| s.name != "inner"));
        assert!(facts.symbols.iter().all(|s| s.name != "proto"));
        assert_eq!(symbol(&facts, "f").kind, SymbolKind::Function);
    }

    #[test]
    fn c_parser_ignores_cpp_only_header_surface_but_keeps_c_declarations() {
        // The register-layer `.h` router sends obvious C++ headers to
        // tree-sitter-cpp. This parser-level regression still protects
        // the old graceful-degradation property for any C++-shaped input
        // explicitly parsed as C.
        let src = br#"
#include <string>

class Widget {
public:
    void render();
};

typedef int handle_t;
"#;
        let facts = facts(src);
        assert_eq!(symbol(&facts, "handle_t").kind, SymbolKind::TypeAlias);
        assert_eq!(facts.imports.len(), 1);
        assert_eq!(facts.imports[0].to_module, "<string>");
    }
}
