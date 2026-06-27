//! `cairn-lang-objc` — Objective-C backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-objc parse tree and emits
//! [`SymbolFact`]s for `@interface` / `@implementation` declarations,
//! categories, `@protocol`s, instance and class methods, `@property`
//! declarations, instance variables, plus the C-shaped surface
//! (function definitions, struct / union / enum / typedef, top-level
//! variables, `#define` macros). `#import` / `#include` directives
//! become [`ImportFact`]s, with system (`<Foundation/Foundation.h>`)
//! and local (`"MyHeader.h"`) forms distinguished the same way the C
//! backend distinguishes them.
//!
//! Categories (`@interface Foo (CatName)` / `@implementation Foo
//! (CatName)`) share the `class_interface` / `class_implementation`
//! node kinds with the base declarations — the grammar surfaces the
//! category name in a `category` field. Members declared inside a
//! category qualify under the *base* class (`Foo.method`), matching
//! how the Swift backend treats `extension` members; the category name
//! itself is not part of the qualified name.
//!
//! Visibility:
//! - `static` C functions / variables map to [`Visibility::Private`]
//!   (translation-unit scope), matching the C backend.
//! - `@public` / `@protected` / `@private` ivar specifiers map to
//!   `Public` / `Crate` / `Private` respectively. `Crate` is the
//!   closest analog the enum offers for `@protected`.
//! - Methods and properties have no ObjC-level visibility keyword; per
//!   community convention every member appearing in a public header is
//!   API surface, so they default to `Public`.
//!
//! File patterns: only `.m` is claimed. ObjC++ (`.mm`) is left
//! unclaimed, and headers (`.h`) go through the register layer's
//! C-family header router.
//!
//! Tier-2 lives in [`analyzer`]: superclass inheritance, protocol
//! conformance, category extension edges, and same-file message-send
//! call resolution. Tier-3 (sourcekit-lsp / clangd workspace
//! analyzers) is out of scope for this backend.

#![forbid(unsafe_code)]

mod analyzer;
mod macros;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, RefFact, RefKind,
    SymbolFact, SymbolKind, SymbolScope, SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct ObjcBackend;

impl LanguageBackend for ObjcBackend {
    fn name(&self) -> &'static str {
        "objc"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        // `.mm` (ObjC++) is deliberately left unclaimed; `.h` is routed
        // by content in the register layer.
        &["*.m"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-objc"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_objc::LANGUAGE.into();
        // Blank known Apple SDK decoration macros (NS_ASSUME_NONNULL_BEGIN,
        // NS_SWIFT_NAME(...), __attribute__((...)), etc.) before parsing.
        // tree-sitter-objc treats these as unknown tokens and folds the
        // following `@interface` into an ERROR node, so they must be
        // neutralized for the class extractor to see anything in Apple-
        // style headers. Whitespace replacement preserves byte offsets.
        let preprocessed = macros::neutralize_apple_macros(source);
        let mut facts = extract(&preprocessed, &language, ObjcVisitor::new())?;
        resolve_same_file_callees(&mut facts);
        Ok(facts)
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_OBJC: fn() -> Box<dyn LanguageBackend> = || Box::new(ObjcBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct ObjcVisitor {
    nesting: NestingTracker,
    /// Tracks the running ivar visibility inside an `instance_variables`
    /// block. Reset at every block entry so a `@private` in one class
    /// does not bleed into the next.
    ivar_visibility: Visibility,
    ivar_block_end: Option<usize>,
}

impl ObjcVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
            ivar_visibility: Visibility::Public,
            ivar_block_end: None,
        }
    }

    fn reset_ivar_state_if_outside(&mut self, byte_start: usize) {
        if let Some(end) = self.ivar_block_end {
            if byte_start >= end {
                self.ivar_block_end = None;
                self.ivar_visibility = Visibility::Public;
            }
        }
    }
}

impl Visitor for ObjcVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());
        self.reset_ivar_state_if_outside(node.start_byte());

        match node.kind() {
            "preproc_include" => {
                if let Some(import) = match_include(node, source) {
                    facts.imports.push(import);
                }
            }
            "preproc_def" | "preproc_function_def" => emit_macro(node, source, facts),
            "class_interface" | "class_implementation" => {
                self.emit_class_container(node, source, facts);
            }
            "protocol_declaration" => {
                self.emit_protocol(node, source, facts);
            }
            "method_declaration" | "method_definition" => {
                self.emit_method(node, source, facts);
            }
            "property_declaration" => {
                self.emit_property(node, source, facts);
            }
            "instance_variables" => {
                self.ivar_visibility = Visibility::Public;
                self.ivar_block_end = Some(node.end_byte());
            }
            "instance_variable" => {
                self.handle_instance_variable(node, source, facts);
            }
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
            "message_expression" => {
                emit_message_ref(node, source, facts, self.nesting.current_parent());
            }
            _ => {}
        }
    }
}

impl ObjcVisitor {
    /// `@interface` / `@implementation`. Categories share both node
    /// kinds with their base; the `category` field disambiguates. A
    /// category becomes a kind=`Impl` symbol whose name is the base
    /// class (so members qualify under it); a base declaration becomes
    /// a `Class` symbol. Both `@interface` and `@implementation` of a
    /// non-category emit a `Class` row — same pattern the C backend
    /// uses for prototype-and-definition pairs.
    fn emit_class_container(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name_node) = first_named_identifier(node) else {
            return;
        };
        let name = node_text(name_node, source).to_string();
        let is_category = child_by_field(node, "category").is_some();
        let kind = if is_category {
            SymbolKind::Impl
        } else {
            SymbolKind::Class
        };
        // Cut the signature at the first member so a long body does
        // not bloat the outline. `instance_variables`, member methods,
        // and properties all come after the protocol list.
        let body_start = first_member_byte(node);
        let idx = self.emit_symbol(node, source, facts, kind, name, body_start);
        self.nesting.push(idx, node.end_byte());
    }

    fn emit_protocol(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name_node) = first_named_identifier(node) else {
            return;
        };
        let name = node_text(name_node, source).to_string();
        let body_start = first_member_byte(node);
        let idx = self.emit_symbol(node, source, facts, SymbolKind::Interface, name, body_start);
        self.nesting.push(idx, node.end_byte());
    }

    fn emit_method(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name) = method_short_name(node, source) else {
            return;
        };
        let body_start = child_by_field(node, "body").map(|n| n.start_byte());
        let kind = if self.in_type_container(facts) {
            SymbolKind::Method
        } else {
            SymbolKind::Function
        };
        self.emit_symbol(node, source, facts, kind, name, body_start);
    }

    fn emit_property(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        // `property_declaration` always contains a `struct_declaration`
        // (the type + declarator pair); the declarator carries the
        // bound identifier. Cut the signature at the trailing `;` of
        // the inner declaration so the outline shows just the API line.
        let Some(inner) = first_named_child(node).and_then(|first| {
            if first.kind() == "property_attributes_declaration" {
                first.next_named_sibling()
            } else {
                Some(first)
            }
        }) else {
            return;
        };
        if inner.kind() != "struct_declaration" {
            return;
        }
        let names = struct_declaration_names(inner, source);
        if names.is_empty() {
            return;
        }
        let kind = if self.in_type_container(facts) {
            SymbolKind::Property
        } else {
            SymbolKind::Variable
        };
        for name in names {
            self.emit_symbol(node, source, facts, kind.clone(), name, None);
        }
    }

    fn handle_instance_variable(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
    ) {
        // The block alternates visibility markers and field
        // declarations; both surface as `instance_variable`.
        if let Some(vis) = child_by_field(node, "visibility")
            .or_else(|| first_named_child(node))
            .filter(|n| n.kind() == "visibility_specification")
        {
            self.ivar_visibility = visibility_from_specifier(vis, source);
            return;
        }
        let Some(decl) = first_named_child(node) else {
            return;
        };
        if decl.kind() != "struct_declaration" {
            return;
        }
        let ivar_vis = Some(self.ivar_visibility);
        for name in struct_declaration_names(decl, source) {
            self.emit_with(
                node,
                source,
                facts,
                EmitArgs {
                    kind: SymbolKind::Field,
                    name,
                    body_start: None,
                    visibility: ivar_vis,
                },
            );
        }
    }

    fn in_type_container(&self, facts: &SyntacticFacts) -> bool {
        matches!(
            self.nesting.parent_kind(facts),
            Some(SymbolKind::Class | SymbolKind::Interface | SymbolKind::Impl)
        )
    }

    fn emit_symbol(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
        kind: SymbolKind,
        name: String,
        body_start: Option<usize>,
    ) -> usize {
        self.emit_with(
            node,
            source,
            facts,
            EmitArgs {
                kind,
                name,
                body_start,
                visibility: None,
            },
        )
    }

    fn emit_with(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
        args: EmitArgs,
    ) -> usize {
        let EmitArgs {
            kind,
            name,
            body_start,
            visibility: visibility_override,
        } = args;
        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = visibility_override.or_else(|| Some(default_visibility(node, source)));
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
}

struct EmitArgs {
    kind: SymbolKind,
    name: String,
    body_start: Option<usize>,
    /// Override for the visibility derived by `default_visibility`.
    /// Set when the caller already knows the right value (e.g. an
    /// ivar inside a `@private` run).
    visibility: Option<Visibility>,
}

// ─── C-shaped emission (reused from the C backend's contract) ──────────────

struct SymbolParts {
    kind: SymbolKind,
    name: String,
    body_start: Option<usize>,
    visibility: Visibility,
    parent_idx: Option<usize>,
}

fn emit_symbol_flat(
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
        scope: SymbolScope::TopLevel,
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
    Some(emit_symbol_flat(
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
        emit_symbol_flat(
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

fn emit_typedef(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let ty = child_by_field(node, "type");
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
        emit_symbol_flat(
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

fn emit_tagged_type(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let Some(body) = child_by_field(node, "body") else {
        return;
    };
    let kind = match node.kind() {
        "struct_specifier" => SymbolKind::Struct,
        "union_specifier" => SymbolKind::Union,
        _ => SymbolKind::Enum,
    };

    let parent_idx = child_by_field(node, "name").map(|name_node| {
        emit_symbol_flat(
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
        emit_symbol_flat(
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
    let body_start = child_by_field(node, "value").map(|n| n.start_byte());
    emit_symbol_flat(
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

// ─── refs ──────────────────────────────────────────────────────────────────

/// Fill `target_qualified` for call refs whose callee is a function /
/// method defined in the same file. ObjC qualified names use `.` to
/// join container and member (`Foo.bar`); message-send refs key on the
/// bare selector head and match against the suffix after the last `.`
/// in any same-file method's qualified name. Same-file C-style calls
/// resolve against bare function names when that name is unique, the
/// same way the C backend does. Cross-file and ambiguous callees keep
/// `target_qualified: None`.
fn resolve_same_file_callees(facts: &mut SyntacticFacts) {
    let SyntacticFacts { symbols, refs, .. } = facts;
    let mut by_short: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
    for s in symbols.iter() {
        match s.kind {
            SymbolKind::Function | SymbolKind::Method => {
                let candidates = by_short.entry(s.name.as_str()).or_default();
                if !candidates.contains(&s.qualified.as_str()) {
                    candidates.push(s.qualified.as_str());
                }
            }
            _ => {}
        }
    }
    for r in refs.iter_mut() {
        if r.kind == RefKind::Call && r.target_qualified.is_none() {
            if let Some(candidates) = by_short.get(r.target_name.as_str()) {
                // A Tier-1 name-only match is evidence-based only when unique; collisions stay unresolved.
                if let [qualified] = candidates.as_slice() {
                    r.target_qualified = Some((*qualified).to_string());
                }
            }
        }
    }
}

/// Same-file C-style call ref (identical to the C backend's logic).
fn emit_call_ref(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    enclosing_idx: Option<usize>,
) {
    let Some(enclosing_idx) = enclosing_idx else {
        return;
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

/// `[receiver method:arg]`. The selector's first identifier is the
/// short name; receiver resolution is out of scope (see module docs).
/// Emitted from the Tier-1 pass too so refs land regardless of whether
/// the analyzer ever runs.
fn emit_message_ref(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    enclosing_idx: Option<usize>,
) {
    let Some(enclosing_idx) = enclosing_idx else {
        return;
    };
    let Some(selector_head) = message_selector_head(node) else {
        return;
    };
    facts.refs.push(RefFact {
        target_name: node_text(selector_head, source).to_string(),
        target_qualified: None,
        kind: RefKind::Call,
        type_role: None,
        enclosing_idx: Some(enclosing_idx),
        enclosing_qualified: None,
        byte_range: selector_head.byte_range(),
        line: line_of(selector_head),
    });
}

fn match_include(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let path = child_by_field(node, "path")?;
    let raw = node_text(path, source).trim();
    let to_module = if path.kind() == "string_literal" {
        raw.trim_matches('"').to_string()
    } else {
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

        byte_range: None,
    })
}

// ─── declarator analysis (lifted from the C backend) ───────────────────────

fn classify_declarator(node: Node<'_>) -> Option<(Node<'_>, bool)> {
    match node.kind() {
        "identifier" | "type_identifier" | "field_identifier" => Some((node, false)),
        // `struct_declarator` wraps an ObjC `@property` or ivar field
        // declarator; the inner declarator chain matches the C shapes
        // below, so recursing is enough.
        "struct_declarator"
        | "init_declarator"
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

// ─── ObjC-specific helpers ─────────────────────────────────────────────────

/// The first `identifier` child of an `@interface` / `@implementation`
/// / `@protocol` node — the class or protocol name. Categories have a
/// second `identifier` for the category name carried in the `category`
/// field, which we do not surface as part of the qualified name.
fn first_named_identifier(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|n| n.kind() == "identifier")
}

/// Byte where the class / protocol body begins. We treat the first
/// member-shaped child (property, method, ivar block, struct_declaration,
/// or implementation_definition) as the body marker so the signature
/// stops at the protocol-conformance list (`<Greeter>`) at the latest.
fn first_member_byte(node: Node<'_>) -> Option<usize> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|n| {
            matches!(
                n.kind(),
                "property_declaration"
                    | "method_declaration"
                    | "instance_variables"
                    | "implementation_definition"
                    | "struct_declaration"
                    | "qualified_protocol_interface_declaration"
            )
        })
        .map(|n| n.start_byte())
}

/// First selector segment of a method declaration / definition. ObjC
/// selectors are multi-part (`foo:bar:`), but the bare short name keeps
/// `find_symbols` / `find_references` resolution consistent with how
/// the Swift backend treats labelled-argument calls (member name only).
fn method_short_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|n| n.kind() == "identifier")
        .map(|n| node_text(n, source).to_string())
}

/// Selector head of `[receiver method:arg…]`. The grammar exposes one
/// or more `method` fields (one per selector segment); the head is the
/// first.
fn message_selector_head(node: Node<'_>) -> Option<Node<'_>> {
    child_by_field(node, "method")
}

/// Pull the bound identifiers out of a `struct_declaration` (the
/// internal node a `@property` or ivar wraps around its C-shaped
/// type + declarator pair). One declaration can bind several names
/// (`int x, y;`) so we return a vector.
fn struct_declaration_names(node: Node<'_>, source: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    let declarators: Vec<Node<'_>> = node
        .children(&mut cursor)
        .filter(|c| {
            matches!(
                c.kind(),
                "struct_declarator"
                    | "field_identifier"
                    | "identifier"
                    | "pointer_declarator"
                    | "array_declarator"
                    | "init_declarator"
                    | "parenthesized_declarator"
            )
        })
        .collect();
    for declarator in declarators {
        if let Some((name, _)) = classify_declarator(declarator) {
            out.push(node_text(name, source).to_string());
        } else if let Some(inner) = first_named_child(declarator) {
            // `struct_declarator` wraps a bare identifier in some
            // grammar paths the classifier above does not catch.
            if inner.kind() == "identifier" || inner.kind() == "field_identifier" {
                out.push(node_text(inner, source).to_string());
            }
        }
    }
    out
}

fn visibility_from_specifier(node: Node<'_>, source: &[u8]) -> Visibility {
    let text = node_text(node, source);
    if text.contains("@public") {
        Visibility::Public
    } else if text.contains("@protected") {
        // `Crate` is the closest analog the enum offers for the ObjC
        // `@protected` visibility (visible to the class and its
        // subclasses, but not to outside callers).
        Visibility::Crate
    } else if text.contains("@private") {
        Visibility::Private
    } else if text.contains("@package") {
        Visibility::Crate
    } else {
        Visibility::Public
    }
}

fn default_visibility(node: Node<'_>, source: &[u8]) -> Visibility {
    // Methods and properties declared inside an interface / protocol
    // are public API surface by ObjC convention. The C backend's
    // `static` rule still applies to free functions and globals.
    match node.kind() {
        "function_definition" | "declaration" => c_visibility(node, source),
        _ => Visibility::Public,
    }
}

// ─── scope and visibility (C-derived helpers) ──────────────────────────────

fn is_top_level(node: Node<'_>) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "translation_unit" => return true,
            "function_definition"
            | "compound_statement"
            | "field_declaration_list"
            | "class_interface"
            | "class_implementation"
            | "protocol_declaration"
            | "instance_variables" => {
                return false;
            }
            _ => current = parent.parent(),
        }
    }
    false
}

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

fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    doc_from_preceding_comments(node, source).or_else(|| {
        node.parent().and_then(|parent| match parent.kind() {
            "declaration" | "type_definition" | "implementation_definition" => {
                doc_from_preceding_comments(parent, source)
            }
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
        ObjcBackend.extract_syntactic(src).unwrap()
    }

    fn find<'a>(facts: &'a SyntacticFacts, qualified: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.qualified == qualified)
            .unwrap_or_else(|| panic!("{qualified} missing"))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(ObjcBackend.parser_id(), "tree-sitter-objc");
    }

    #[test]
    fn claims_only_dot_m_files() {
        // `.mm` is unclaimed; `.h` is routed by content in register.
        assert_eq!(ObjcBackend.file_patterns(), &["*.m"]);
    }

    #[test]
    fn extracts_class_with_superclass_protocols_methods_and_properties() {
        let src = br#"
#import <Foundation/Foundation.h>
#import "Local.h"

@interface Person : NSObject <NSCopying, NSCoding>
@property (nonatomic, copy) NSString *name;
@property (readonly) NSInteger age;
- (instancetype)initWithName:(NSString *)name;
- (void)sayHello;
+ (Person *)defaultPerson;
@end

@implementation Person
- (instancetype)initWithName:(NSString *)name { return self; }
- (void)sayHello { NSLog(@"hi"); }
+ (Person *)defaultPerson { return nil; }
@end
"#;
        let facts = facts(src);

        // Both @interface and @implementation emit a Class row (the
        // declaration / definition pair matches the C-backend pattern).
        let persons: Vec<&SymbolFact> = facts
            .symbols
            .iter()
            .filter(|s| s.qualified == "Person" && s.kind == SymbolKind::Class)
            .collect();
        assert_eq!(persons.len(), 2, "{persons:?}");

        // Properties qualify under the class.
        assert_eq!(find(&facts, "Person.name").kind, SymbolKind::Property);
        assert_eq!(find(&facts, "Person.age").kind, SymbolKind::Property);

        // Methods land under the class with method short name as
        // identifier. Class methods (`+`) and instance methods (`-`)
        // are both kind=Method at Tier-1.
        assert_eq!(find(&facts, "Person.initWithName").kind, SymbolKind::Method);
        assert_eq!(find(&facts, "Person.sayHello").kind, SymbolKind::Method);
        assert_eq!(
            find(&facts, "Person.defaultPerson").kind,
            SymbolKind::Method
        );

        // Class-method vs instance-method distinction is carried by
        // the signature's `+` / `-` prefix.
        assert!(
            find(&facts, "Person.defaultPerson")
                .signature
                .as_deref()
                .unwrap()
                .trim_start()
                .starts_with('+')
        );
        assert!(
            find(&facts, "Person.sayHello")
                .signature
                .as_deref()
                .unwrap()
                .trim_start()
                .starts_with('-')
        );
    }

    #[test]
    fn category_members_qualify_under_base_class() {
        let src = br#"
@interface Person : NSObject
- (void)greet;
@end

@interface Person (Logging) <NSCoding>
- (void)logSelf;
@property (nonatomic, strong) id sink;
@end

@implementation Person (Logging)
- (void)logSelf { NSLog(@"hi"); }
@end
"#;
        let facts = facts(src);

        // Category interface and implementation emit Impl rows whose
        // `name` is the base class — extension-style, like Swift.
        let categories: Vec<&SymbolFact> = facts
            .symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Impl)
            .collect();
        assert_eq!(categories.len(), 2, "{categories:?}");
        assert!(categories.iter().all(|s| s.name == "Person"));

        // Members declared inside the category qualify under the base
        // class, not under the category name.
        assert_eq!(find(&facts, "Person.logSelf").kind, SymbolKind::Method);
        assert_eq!(find(&facts, "Person.sink").kind, SymbolKind::Property);
    }

    #[test]
    fn protocol_with_required_optional_sections() {
        let src = br#"
@protocol Drawable <NSObject>
@required
- (void)draw;
@optional
- (BOOL)needsRedraw;
@property (readonly) CGRect bounds;
@end
"#;
        let facts = facts(src);
        assert_eq!(find(&facts, "Drawable").kind, SymbolKind::Interface);
        assert_eq!(find(&facts, "Drawable.draw").kind, SymbolKind::Method);
        assert_eq!(
            find(&facts, "Drawable.needsRedraw").kind,
            SymbolKind::Method
        );
        assert_eq!(find(&facts, "Drawable.bounds").kind, SymbolKind::Property);
    }

    #[test]
    fn ivar_visibility_tracks_specifier_runs() {
        let src = br#"
@interface Foo : NSObject {
@public
    int pubVar;
@private
    int privVar;
@protected
    int protVar;
}
@end
"#;
        let facts = facts(src);
        assert_eq!(
            find(&facts, "Foo.pubVar").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            find(&facts, "Foo.privVar").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            find(&facts, "Foo.protVar").visibility,
            Some(Visibility::Crate),
            "@protected maps to the closest analog cairn exposes"
        );
        assert!(facts.symbols.iter().all(|s| s.kind != SymbolKind::Field
            || matches!(
                s.qualified.as_str(),
                "Foo.pubVar" | "Foo.privVar" | "Foo.protVar"
            )));
    }

    #[test]
    fn imports_distinguish_system_from_local() {
        let src = br#"
#import <Foundation/Foundation.h>
#import "Local.h"
#include "legacy.h"
"#;
        let facts = facts(src);
        let modules: Vec<&str> = facts.imports.iter().map(|i| i.to_module.as_str()).collect();
        assert_eq!(
            modules,
            ["<Foundation/Foundation.h>", "Local.h", "legacy.h"],
            "#import system keeps angle brackets, #import / #include local are bare paths"
        );
    }

    #[test]
    fn c_shaped_surface_still_works() {
        let src = br#"
#define MAX 10
typedef int my_int;

struct point { int x; int y; };
enum color { RED, GREEN };

static int helper(void) { return 0; }
int exported(void) { return 0; }
int global = 0;
"#;
        let facts = facts(src);
        assert_eq!(find(&facts, "MAX").kind, SymbolKind::Macro);
        assert_eq!(find(&facts, "my_int").kind, SymbolKind::TypeAlias);
        assert_eq!(find(&facts, "point").kind, SymbolKind::Struct);
        assert_eq!(find(&facts, "color").kind, SymbolKind::Enum);
        assert_eq!(find(&facts, "RED").kind, SymbolKind::Constant);
        assert_eq!(find(&facts, "helper").visibility, Some(Visibility::Private));
        assert_eq!(
            find(&facts, "exported").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(find(&facts, "global").kind, SymbolKind::Variable);
    }

    #[test]
    fn message_send_refs_resolve_against_same_file_methods() {
        let src = br#"
@interface Foo : NSObject
- (void)bar;
- (void)baz:(int)x;
@end

@implementation Foo
- (void)bar {
    [self baz:1];
    [Foo classMethod];
}
- (void)baz:(int)x { return; }
+ (void)classMethod {
    [self bar];
}
@end
"#;
        let facts = facts(src);

        // `[self baz:1]` resolves to Foo.baz because Foo.baz exists in
        // the same file. The selector head is `baz`.
        let baz_call = facts
            .refs
            .iter()
            .find(|r| r.target_name == "baz")
            .expect("baz call ref");
        assert_eq!(baz_call.kind, RefKind::Call);
        assert_eq!(baz_call.target_qualified.as_deref(), Some("Foo.baz"));

        // `[self bar]` from +classMethod resolves to Foo.bar.
        let bar_call = facts
            .refs
            .iter()
            .find(|r| r.target_name == "bar")
            .expect("bar call ref");
        assert_eq!(bar_call.target_qualified.as_deref(), Some("Foo.bar"));

        // `[Foo classMethod]` resolves to Foo.classMethod.
        let class_call = facts
            .refs
            .iter()
            .find(|r| r.target_name == "classMethod")
            .expect("classMethod call ref");
        assert_eq!(
            class_call.target_qualified.as_deref(),
            Some("Foo.classMethod")
        );
    }

    #[test]
    fn message_send_to_unknown_method_stays_unresolved() {
        let src = br#"
@implementation Foo
- (void)x { [bar baz]; }
@end
"#;
        let facts = facts(src);
        let baz = facts
            .refs
            .iter()
            .find(|r| r.target_name == "baz")
            .expect("baz call ref");
        assert_eq!(baz.target_qualified, None);
    }

    #[test]
    fn same_name_method_collisions_stay_unresolved() {
        // Two same-file classes both define `find`. Receiver resolution
        // is grammatically impossible from a message-send in Tier-1, so
        // the ref must stay unresolved instead of picking a candidate by
        // traversal order.
        let src = br#"
@implementation A
- (void)find { return; }
@end

@implementation B
- (void)find { return; }
- (void)caller { [self find]; }
@end
"#;
        let facts = facts(src);
        let call = facts
            .refs
            .iter()
            .find(|r| r.target_name == "find")
            .expect("find call");
        assert_eq!(call.target_qualified, None);
    }

    #[test]
    fn extracts_class_inside_ns_assume_nonnull_wrapper() {
        // Apple SDK headers (and dependents like AFNetworking) wrap every
        // declaration in NS_ASSUME_NONNULL_BEGIN/END. tree-sitter-objc
        // does not model the macro and falls into ERROR recovery, so
        // without the pre-parse blanking the class symbol is never
        // emitted. Regression test for the bug fixed in this commit.
        let src = br#"NS_ASSUME_NONNULL_BEGIN

@interface AFHTTPSessionManager : AFURLSessionManager <NSSecureCoding, NSCopying>
- (void)foo;
@end

NS_ASSUME_NONNULL_END
"#;
        let facts = facts(src);
        let cls = find(&facts, "AFHTTPSessionManager");
        assert_eq!(cls.kind, SymbolKind::Class);
        // Line range survives the macro blanking unchanged (line 3 of the
        // source: BEGIN, blank, @interface).
        assert_eq!(cls.line_range.start, 3);
        assert_eq!(
            find(&facts, "AFHTTPSessionManager.foo").kind,
            SymbolKind::Method
        );
    }

    #[test]
    fn extracts_class_with_swift_name_macro_between_name_and_colon() {
        let src = br#"
@interface Foo NS_SWIFT_NAME(BarSwift) : Base
@end
"#;
        let facts = facts(src);
        let cls = find(&facts, "Foo");
        assert_eq!(cls.kind, SymbolKind::Class);
    }

    #[test]
    fn extracts_class_with_attribute_decoration() {
        let src = br#"
__attribute__((deprecated("use NewFoo"))) @interface Foo : Bar
@end
"#;
        let facts = facts(src);
        assert_eq!(find(&facts, "Foo").kind, SymbolKind::Class);
    }

    #[test]
    fn extracts_category_inside_ns_assume_nonnull() {
        let src = br#"NS_ASSUME_NONNULL_BEGIN
@interface Foo (Logging)
- (void)log;
@end
NS_ASSUME_NONNULL_END
"#;
        let facts = facts(src);
        let category = facts
            .symbols
            .iter()
            .find(|s| s.kind == SymbolKind::Impl && s.name == "Foo")
            .expect("category Impl row");
        assert_eq!(category.kind, SymbolKind::Impl);
        assert_eq!(find(&facts, "Foo.log").kind, SymbolKind::Method);
    }

    #[test]
    fn extracts_class_with_class_availability_macro() {
        // `NS_CLASS_AVAILABLE_IOS(7_0)` is a function-form macro before
        // `@interface`. Caught by the NS_CLASS_AVAILABLE prefix family.
        let src = br#"
NS_CLASS_AVAILABLE_IOS(7_0) @interface Foo : Bar
@end
"#;
        let facts = facts(src);
        assert_eq!(find(&facts, "Foo").kind, SymbolKind::Class);
    }

    #[test]
    fn empty_input_is_empty_ok() {
        let facts = ObjcBackend.extract_syntactic(b"").unwrap();
        assert!(facts.symbols.is_empty());
        assert!(facts.imports.is_empty());
        assert!(facts.refs.is_empty());
    }
}
