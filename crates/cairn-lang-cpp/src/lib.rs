//! `cairn-lang-cpp` — C++ backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-cpp parse tree and emits
//! [`SymbolFact`]s for namespaces, classes / structs / unions, enums
//! (scoped and unscoped), functions, methods (in-class definitions and
//! out-of-class `T::method` definitions), constructors, destructors,
//! fields, global / static variables, `typedef`s, `using` aliases, and
//! `#define` macros, plus [`ImportFact`]s for `#include` directives.
//!
//! Visibility mapping:
//! - `public:` members → [`Visibility::Public`].
//! - `protected:` / `private:` members → [`Visibility::Crate`].
//! - A `class` member with no access specifier → [`Visibility::Crate`]
//!   (C++ default access for `class` is `private`; cairn's tiered
//!   `Visibility` collapses both to `Crate`).
//! - A `struct` / `union` member with no access specifier →
//!   [`Visibility::Public`] (C++ default for `struct` / `union`).
//! - Declarations inside an anonymous namespace → [`Visibility::Crate`]
//!   (internal linkage by language rule).
//! - File / namespace-scope declarations carrying `static` →
//!   [`Visibility::Private`] (translation-unit-local linkage).
//! - Everything else at file / namespace scope → [`Visibility::Public`].
//!
//! Tier-2 (semantic): the [`analyzer`] module re-walks the same grammar
//! for `base_class_clause` inheritance edges (single `"inherit"` kind,
//! matching the cross-backend taxonomy) and same-file call / `new`
//! refs. See its module docs.
//!
//! Limitations:
//! - The `.h` extension is intentionally NOT claimed here: the register
//!   layer routes ambiguous C-family headers by content so a `.h` blob is
//!   still indexed once, under either the C or C++ parser id.
//! - Template instantiation callee resolution across files is out of
//!   scope; same-file resolution still works. Overloads (more than one
//!   same-file method sharing a bare name) deliberately leave the call
//!   ref unresolved rather than guessing.
//! - Tier-3 (clangd) is reserved for a future slice.

#![forbid(unsafe_code)]

mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, collapse_ws, end_line_of, extract,
    extract_doc_above_node, line_of, node_text,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct CppBackend;

impl LanguageBackend for CppBackend {
    fn name(&self) -> &'static str {
        "cpp"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &[
            "*.cpp", "*.cc", "*.cxx", "*.hpp", "*.hxx", "*.hh", "*.h++", "*.C", "*.H",
        ]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-cpp"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_cpp::LANGUAGE.into();
        extract(source, &language, CppVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_CPP: fn() -> Box<dyn LanguageBackend> = || Box::new(CppBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct CppVisitor {
    nesting: NestingTracker,
    /// One entry per open class / struct / union body: `(end_byte,
    /// current_access)`. The access flips on every `access_specifier`
    /// node we visit; pop is byte-range driven.
    access_stack: Vec<(usize, Visibility)>,
    /// Byte end of every open anonymous namespace; declarations sitting
    /// inside one get `Visibility::Crate` (C++ internal linkage rule).
    anon_ns_ends: Vec<usize>,
}

impl CppVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("::"),
            access_stack: Vec::new(),
            anon_ns_ends: Vec::new(),
        }
    }

    fn pop_access_outside(&mut self, byte_start: usize) {
        while let Some(&(end, _)) = self.access_stack.last() {
            if end <= byte_start {
                self.access_stack.pop();
            } else {
                break;
            }
        }
    }

    fn pop_anon_ns_outside(&mut self, byte_start: usize) {
        while let Some(&end) = self.anon_ns_ends.last() {
            if end <= byte_start {
                self.anon_ns_ends.pop();
            } else {
                break;
            }
        }
    }

    fn in_class(&self) -> bool {
        !self.access_stack.is_empty()
    }

    fn in_anon_namespace(&self) -> bool {
        !self.anon_ns_ends.is_empty()
    }

    fn current_class_access(&self) -> Option<Visibility> {
        self.access_stack.last().map(|(_, v)| *v)
    }

    fn visibility_for(&self, node: Node<'_>, source: &[u8]) -> Visibility {
        if let Some(access) = self.current_class_access() {
            return access;
        }
        if self.in_anon_namespace() {
            return Visibility::Crate;
        }
        if has_static_storage(node, source) {
            Visibility::Private
        } else {
            Visibility::Public
        }
    }

    fn update_access(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(slot) = self.access_stack.last_mut() else {
            return;
        };
        let text = node_text(node, source);
        let trimmed = text.trim_end_matches(':').trim();
        slot.1 = match trimmed {
            "public" => Visibility::Public,
            // `protected` and `private` both collapse to Crate — cairn's
            // tiered Visibility lacks a separate `Protected`.
            "protected" | "private" => Visibility::Crate,
            _ => slot.1,
        };
    }
}

impl Visitor for CppVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let start = node.start_byte();
        self.nesting.pop_outside(start);
        self.pop_access_outside(start);
        self.pop_anon_ns_outside(start);

        match node.kind() {
            "access_specifier" => self.update_access(node, source),
            "preproc_include" => {
                if let Some(import) = match_include(node, source) {
                    facts.imports.push(import);
                }
            }
            // The preprocessor is scope-blind; emit `#define` wherever
            // it appears.
            "preproc_def" | "preproc_function_def" => emit_macro(node, source, facts),
            "namespace_definition" => self.handle_namespace(node, source, facts),
            "class_specifier" | "struct_specifier" | "union_specifier" => {
                self.handle_record(node, source, facts);
            }
            "enum_specifier" => self.handle_enum(node, source, facts),
            "function_definition" => {
                self.handle_function_definition(node, source, facts);
            }
            "field_declaration" => self.handle_field_declaration(node, source, facts),
            "declaration" if at_namespace_or_top(node) => {
                self.handle_declaration(node, source, facts);
            }
            "type_definition" if at_namespace_or_top(node) => {
                self.handle_typedef(node, source, facts);
            }
            "alias_declaration" if at_namespace_or_top(node) || self.in_class() => {
                self.handle_alias(node, source, facts);
            }
            _ => {}
        }
    }
}

// ─── handlers ──────────────────────────────────────────────────────────────

impl CppVisitor {
    /// `namespace foo { ... }`, `namespace foo::bar { ... }` (C++17
    /// nested-namespace form), `inline namespace foo { ... }`, and the
    /// anonymous form `namespace { ... }`. Each named segment pushes a
    /// `Namespace` symbol; the anonymous form pushes only the visibility
    /// flag (`anon_ns_ends`), not a symbol.
    fn handle_namespace(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let name_node = child_by_field(node, "name");
        let body = child_by_field(node, "body");
        let body_start = body.map(|b| b.start_byte());
        let scope_end = body.map_or(node.end_byte(), |b| b.end_byte());

        let Some(name_node) = name_node else {
            // Anonymous namespace.
            self.anon_ns_ends.push(scope_end);
            return;
        };

        let segments = split_qualified(node_text(name_node, source));
        if segments.is_empty() {
            self.anon_ns_ends.push(scope_end);
            return;
        }

        for (i, seg) in segments.iter().enumerate() {
            let parent_idx = self.nesting.current_parent();
            // Only the outermost segment carries the full `namespace foo
            // { ... }` signature slice; deeper segments share the same
            // node so the join becomes redundant but harmless.
            let signature = if i == 0 {
                let body_for_sig = if segments.len() == 1 {
                    body_start
                } else {
                    None
                };
                template_aware_signature(node, source, body_for_sig)
            } else {
                Some(format!("namespace {seg}"))
            };
            let doc = if i == 0 {
                extract_doc(node, source)
            } else {
                None
            };
            let visibility = Some(self.visibility_for(node, source));
            let idx = facts.symbols.len();
            facts.symbols.push(SymbolFact {
                qualified: self.nesting.qualified_for(seg, facts),
                name: seg.clone(),
                kind: SymbolKind::Namespace,
                signature,
                doc,
                visibility,
                byte_range: node.byte_range(),
                line_range: line_of(node)..end_line_of(node),
                body_start,
                parent_idx,
            });
            self.nesting.push(idx, scope_end);
        }
    }

    /// `class Foo : Bases { ... };` / `struct Foo { ... };` /
    /// `union U { ... };`. A forward declaration (no body) is skipped.
    fn handle_record(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(body) = child_by_field(node, "body") else {
            return;
        };
        let kind = match node.kind() {
            "class_specifier" => SymbolKind::Class,
            "struct_specifier" => SymbolKind::Struct,
            "union_specifier" => SymbolKind::Union,
            _ => return,
        };
        let default_access = match node.kind() {
            "class_specifier" => Visibility::Crate,
            _ => Visibility::Public,
        };
        let body_start = Some(body.start_byte());
        let body_end = body.end_byte();
        let visibility = Some(self.visibility_for(node, source));

        let name_node = child_by_field(node, "name");
        let symbol_idx = if let Some(name_node) = name_node {
            let name = node_text(name_node, source).to_string();
            let parent_idx = self.nesting.current_parent();
            let idx = facts.symbols.len();
            facts.symbols.push(SymbolFact {
                qualified: self.nesting.qualified_for(&name, facts),
                name,
                kind,
                signature: template_aware_signature(node, source, body_start),
                doc: extract_doc(node, source),
                visibility,
                byte_range: node.byte_range(),
                line_range: line_of(node)..end_line_of(node),
                body_start,
                parent_idx,
            });
            Some(idx)
        } else {
            None
        };

        self.access_stack.push((body_end, default_access));
        if let Some(idx) = symbol_idx {
            self.nesting.push(idx, body_end);
        }
    }

    /// `enum E { ... };` / `enum class E : T { ... };`. Enumerators emit
    /// as `Constant` children. For a scoped enum (`enum class` /
    /// `enum struct`) the enumerator's `qualified` lives under the enum;
    /// for an unscoped enum, enumerators live at the enum's parent scope
    /// (their names leak into surrounding scope by language rule).
    fn handle_enum(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(body) = child_by_field(node, "body") else {
            return;
        };
        let body_start = Some(body.start_byte());
        let scoped = is_scoped_enum(node);
        let visibility = Some(self.visibility_for(node, source));

        let enum_info = if let Some(name_node) = child_by_field(node, "name") {
            let name = node_text(name_node, source).to_string();
            let qualified = self.nesting.qualified_for(&name, facts);
            let idx = facts.symbols.len();
            facts.symbols.push(SymbolFact {
                qualified: qualified.clone(),
                name,
                kind: SymbolKind::Enum,
                signature: template_aware_signature(node, source, body_start),
                doc: extract_doc(node, source),
                visibility,
                byte_range: node.byte_range(),
                line_range: line_of(node)..end_line_of(node),
                body_start,
                parent_idx: self.nesting.current_parent(),
            });
            Some((idx, qualified))
        } else {
            None
        };

        let mut cursor = body.walk();
        let enumerators: Vec<Node<'_>> = body
            .named_children(&mut cursor)
            .filter(|c| c.kind() == "enumerator")
            .collect();
        for enumerator in enumerators {
            let Some(name_node) = child_by_field(enumerator, "name") else {
                continue;
            };
            let name = node_text(name_node, source).to_string();
            let (parent_idx, qualified) = match (scoped, &enum_info) {
                (true, Some((idx, qualified))) => (Some(*idx), format!("{qualified}::{name}")),
                _ => (
                    self.nesting.current_parent(),
                    self.nesting.qualified_for(&name, facts),
                ),
            };
            facts.symbols.push(SymbolFact {
                qualified,
                name,
                kind: SymbolKind::Constant,
                signature: node_text(enumerator, source).trim().to_string().into(),
                doc: None,
                visibility,
                byte_range: enumerator.byte_range(),
                line_range: line_of(enumerator)..end_line_of(enumerator),
                body_start: None,
                parent_idx,
            });
        }
    }

    /// A `function_definition` that appears at namespace / file scope
    /// (`void Foo::bar() { ... }`) or as an inline method inside a class
    /// body (`class W { void m() { ... } };`).
    fn handle_function_definition(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
    ) {
        let Some(declarator) = child_by_field(node, "declarator") else {
            return;
        };
        let Some(info) = classify_declarator(declarator, source) else {
            return;
        };
        if !info.is_function {
            return;
        }
        let body_start = child_by_field(node, "body").map(|n| n.start_byte());

        let (qualified, parent_idx, kind) = self.resolve_function_identity(&info, facts);
        let visibility = Some(self.visibility_for(node, source));

        facts.symbols.push(SymbolFact {
            qualified,
            name: info.name_text.clone(),
            kind,
            signature: template_aware_signature(node, source, body_start),
            doc: extract_doc(node, source),
            visibility,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start,
            parent_idx,
        });
    }

    /// `field_declaration` inside a class body. Three shapes occur:
    /// 1. method declaration without body — `void m();` — emit as
    ///    `Method` / `Constructor` / `Destructor`.
    /// 2. data member(s) — `int a, b;` — emit one `Field` per name. A
    ///    `static const` data member is emitted as `Constant`.
    /// 3. nothing of interest — bitfield-only or pure-token oddities are
    ///    silently skipped.
    fn handle_field_declaration(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
    ) {
        if !self.in_class() {
            return; // out-of-class field_declarations are unusual; skip.
        }
        let mut cursor = node.walk();
        let declarators: Vec<Node<'_>> = node
            .children_by_field_name("declarator", &mut cursor)
            .collect();
        let visibility = Some(self.visibility_for(node, source));
        let doc = extract_doc(node, source);

        let static_storage = has_static_storage(node, source);
        let is_const = has_const_specifier(node, source);

        for declarator in declarators {
            let Some(info) = classify_declarator(declarator, source) else {
                continue;
            };
            if info.is_function {
                let (qualified, parent_idx, kind) = self.resolve_function_identity(&info, facts);
                facts.symbols.push(SymbolFact {
                    qualified,
                    name: info.name_text.clone(),
                    kind,
                    signature: template_aware_signature(node, source, None),
                    doc: doc.clone(),
                    visibility,
                    byte_range: node.byte_range(),
                    line_range: line_of(node)..end_line_of(node),
                    body_start: None,
                    parent_idx,
                });
            } else {
                let kind = if static_storage && is_const {
                    SymbolKind::Constant
                } else {
                    SymbolKind::Field
                };
                let qualified = self.nesting.qualified_for(&info.name_text, facts);
                facts.symbols.push(SymbolFact {
                    qualified,
                    name: info.name_text.clone(),
                    kind,
                    signature: template_aware_signature(node, source, None),
                    doc: doc.clone(),
                    visibility,
                    byte_range: node.byte_range(),
                    line_range: line_of(node)..end_line_of(node),
                    body_start: None,
                    parent_idx: self.nesting.current_parent(),
                });
            }
        }
    }

    /// A top-level / namespace-scope `declaration`: prototypes
    /// (`void f();`), global variables (`int g = 0;`), or function
    /// pointers (treated as variables, matching the C backend).
    fn handle_declaration(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let visibility = Some(self.visibility_for(node, source));
        let doc = extract_doc(node, source);
        let mut cursor = node.walk();
        let declarators: Vec<Node<'_>> = node
            .children_by_field_name("declarator", &mut cursor)
            .collect();
        let is_const = has_const_specifier(node, source);
        let is_static = has_static_storage(node, source);
        for declarator in declarators {
            let Some(info) = classify_declarator(declarator, source) else {
                continue;
            };
            let kind = if info.is_function {
                SymbolKind::Function
            } else if is_const && (is_static || !self.in_class()) {
                SymbolKind::Constant
            } else {
                SymbolKind::Variable
            };
            let (qualified, parent_idx) = if info.is_function {
                let (q, p, _) = self.resolve_function_identity(&info, facts);
                (q, p)
            } else {
                (
                    self.nesting.qualified_for(&info.name_text, facts),
                    self.nesting.current_parent(),
                )
            };
            facts.symbols.push(SymbolFact {
                qualified,
                name: info.name_text.clone(),
                kind,
                signature: template_aware_signature(node, source, None),
                doc: doc.clone(),
                visibility,
                byte_range: node.byte_range(),
                line_range: line_of(node)..end_line_of(node),
                body_start: None,
                parent_idx,
            });
        }
    }

    fn handle_typedef(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let visibility = Some(self.visibility_for(node, source));
        let doc = extract_doc(node, source);
        let mut cursor = node.walk();
        let declarators: Vec<Node<'_>> = node
            .children_by_field_name("declarator", &mut cursor)
            .collect();
        for declarator in declarators {
            let Some(info) = classify_declarator(declarator, source) else {
                continue;
            };
            facts.symbols.push(SymbolFact {
                qualified: self.nesting.qualified_for(&info.name_text, facts),
                name: info.name_text.clone(),
                kind: SymbolKind::TypeAlias,
                signature: template_aware_signature(node, source, None),
                doc: doc.clone(),
                visibility,
                byte_range: node.byte_range(),
                line_range: line_of(node)..end_line_of(node),
                body_start: None,
                parent_idx: self.nesting.current_parent(),
            });
        }
    }

    fn handle_alias(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        let name = node_text(name_node, source).to_string();
        let visibility = Some(self.visibility_for(node, source));
        facts.symbols.push(SymbolFact {
            qualified: self.nesting.qualified_for(&name, facts),
            name,
            kind: SymbolKind::TypeAlias,
            signature: template_aware_signature(node, source, None),
            doc: extract_doc(node, source),
            visibility,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start: None,
            parent_idx: self.nesting.current_parent(),
        });
    }

    /// Pick the right kind / qualified / parent for a function-shaped
    /// declarator. Handles three placements:
    /// - in-class method (`void m();` inside a class body) — qualified
    ///   under the enclosing class, parent = class symbol index;
    /// - out-of-class definition (`void Foo::bar() {}` at namespace
    ///   scope) — qualified prepends the explicit `Foo::` path on top of
    ///   the current namespace, parent left `None` (the class may be
    ///   declared in another file);
    /// - free function — qualified under the current namespace only.
    fn resolve_function_identity(
        &self,
        info: &DeclaratorInfo,
        facts: &SyntacticFacts,
    ) -> (String, Option<usize>, SymbolKind) {
        let in_class = self.in_class();
        let class_parent_name = self
            .nesting
            .current_parent()
            .and_then(|idx| facts.symbols.get(idx))
            .filter(|s| matches!(s.kind, SymbolKind::Class | SymbolKind::Struct))
            .map(|s| s.name.as_str());

        let kind = if info.is_destructor {
            SymbolKind::Other("destructor".into())
        } else if in_class && class_parent_name == Some(info.name_text.as_str()) {
            SymbolKind::Constructor
        } else if info.qualified_prefix.is_some() {
            // Out-of-class `Foo::bar` / `Foo::~Foo` / `Foo::Foo`.
            let bare = strip_destructor_tilde(&info.name_text);
            let class = info
                .qualified_prefix
                .as_deref()
                .and_then(|p| p.rsplit("::").next())
                .unwrap_or("");
            if bare == class && !class.is_empty() {
                if info.name_text.starts_with('~') {
                    SymbolKind::Other("destructor".into())
                } else {
                    SymbolKind::Constructor
                }
            } else {
                SymbolKind::Method
            }
        } else if in_class {
            SymbolKind::Method
        } else {
            SymbolKind::Function
        };

        let qualified = if let Some(prefix) = &info.qualified_prefix {
            // `Foo::bar` text: prepend current namespace.
            let nested_prefix = self.nesting.qualified_for(prefix, facts);
            format!("{nested_prefix}::{}", info.name_text)
        } else {
            self.nesting.qualified_for(&info.name_text, facts)
        };

        let parent_idx = if info.qualified_prefix.is_some() {
            None
        } else {
            self.nesting.current_parent()
        };
        (qualified, parent_idx, kind)
    }
}

// ─── helpers ───────────────────────────────────────────────────────────────

struct DeclaratorInfo<'a> {
    /// Bare name as it appears at the leaf (`bar` for `Foo::bar`,
    /// `~Foo` for a destructor, `operator+` for an operator overload).
    name_text: String,
    /// Out-of-class explicit scope path, sans trailing `::`. `None` for
    /// in-class / free declarations.
    qualified_prefix: Option<String>,
    /// True if the declarator chain contains a `function_declarator`.
    is_function: bool,
    /// True if the leaf is a `destructor_name` / starts with `~`.
    is_destructor: bool,
    /// Anchor node; kept for completeness but currently unused.
    _name_node: Node<'a>,
}

fn classify_declarator<'a>(node: Node<'a>, source: &[u8]) -> Option<DeclaratorInfo<'a>> {
    match node.kind() {
        "identifier" | "type_identifier" | "field_identifier" | "operator_name" => {
            Some(DeclaratorInfo {
                name_text: node_text(node, source).trim().to_string(),
                qualified_prefix: None,
                is_function: false,
                is_destructor: false,
                _name_node: node,
            })
        }
        "destructor_name" => Some(DeclaratorInfo {
            name_text: collapse_ws(node_text(node, source)),
            qualified_prefix: None,
            is_function: false,
            is_destructor: true,
            _name_node: node,
        }),
        "qualified_identifier" => {
            let full = collapse_ws(node_text(node, source));
            let (prefix, name) = match full.rfind("::") {
                Some(pos) => (Some(full[..pos].to_string()), full[pos + 2..].to_string()),
                None => (None, full.clone()),
            };
            let is_destructor = name.starts_with('~');
            Some(DeclaratorInfo {
                name_text: name,
                qualified_prefix: prefix,
                is_function: false,
                is_destructor,
                _name_node: node,
            })
        }
        "init_declarator"
        | "pointer_declarator"
        | "reference_declarator"
        | "array_declarator"
        | "attributed_declarator"
        | "parenthesized_declarator" => {
            let inner = child_by_field(node, "declarator").or_else(|| first_named_child(node))?;
            classify_declarator(inner, source)
        }
        "function_declarator" => {
            let inner = child_by_field(node, "declarator")?;
            // `int (*fp)(...)` — function pointer; declares a variable.
            let pointer_chain = inner.kind() == "parenthesized_declarator"
                && first_named_child(inner).is_some_and(|n| {
                    matches!(n.kind(), "pointer_declarator" | "reference_declarator")
                });
            let mut info = classify_declarator(inner, source)?;
            if !pointer_chain {
                info.is_function = true;
            }
            Some(info)
        }
        _ => None,
    }
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

fn split_qualified(text: &str) -> Vec<String> {
    text.split("::")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn strip_destructor_tilde(name: &str) -> &str {
    name.strip_prefix('~').unwrap_or(name)
}

/// Detect `enum class` / `enum struct` (scoped enums). The keyword is
/// an anonymous token sandwiched between `enum` and the name.
fn is_scoped_enum(node: Node<'_>) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .any(|c| !c.is_named() && (c.kind() == "class" || c.kind() == "struct"))
}

fn has_static_storage(node: Node<'_>, source: &[u8]) -> bool {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).any(|child| {
        child.kind() == "storage_class_specifier" && node_text(child, source).trim() == "static"
    })
}

fn has_const_specifier(node: Node<'_>, source: &[u8]) -> bool {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .any(|child| child.kind() == "type_qualifier" && node_text(child, source).trim() == "const")
}

/// True when `node` sits at translation-unit / namespace scope, looking
/// through preprocessor and linkage wrappers. Stops at function bodies
/// and aggregate member lists.
fn at_namespace_or_top(node: Node<'_>) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "translation_unit" | "declaration_list" | "linkage_specification" => return true,
            "function_definition"
            | "compound_statement"
            | "field_declaration_list"
            | "for_statement"
            | "for_range_loop"
            | "if_statement"
            | "while_statement"
            | "do_statement"
            | "switch_statement"
            | "case_statement" => return false,
            _ => current = parent.parent(),
        }
    }
    false
}

/// Slice from the (possibly template-wrapping) parent's start byte up to
/// the body, so the signature carries `template<typename T>` prefixes.
fn template_aware_signature(
    node: Node<'_>,
    source: &[u8],
    body_start: Option<usize>,
) -> Option<String> {
    let start = template_parent_start(node).unwrap_or_else(|| node.start_byte());
    let end = body_start.unwrap_or_else(|| node.end_byte());
    let slice = source.get(start..end)?;
    let s = std::str::from_utf8(slice).ok()?.trim();
    if s.is_empty() {
        None
    } else {
        Some(collapse_ws(s))
    }
}

fn template_parent_start(node: Node<'_>) -> Option<usize> {
    let parent = node.parent()?;
    if parent.kind() == "template_declaration" {
        Some(parent.start_byte())
    } else {
        None
    }
}

fn emit_macro(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let Some(name_node) = child_by_field(node, "name") else {
        return;
    };
    let body_start = child_by_field(node, "value").map(|n| n.start_byte());
    let name = node_text(name_node, source).to_string();
    facts.symbols.push(SymbolFact {
        qualified: name.clone(),
        name,
        kind: SymbolKind::Macro,
        signature: template_aware_signature(node, source, body_start),
        doc: extract_doc(node, source),
        visibility: Some(Visibility::Public),
        byte_range: node.byte_range(),
        line_range: line_of(node)..end_line_of(node),
        body_start,
        parent_idx: None,
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
    })
}

// ─── doc comments ──────────────────────────────────────────────────────────

fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    doc_from_preceding_comments(node, source).or_else(|| {
        node.parent().and_then(|parent| match parent.kind() {
            "declaration" | "type_definition" | "template_declaration" => {
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
    if let Some(rest) = trimmed.strip_prefix("///") {
        rest.trim().to_string()
    } else if let Some(rest) = trimmed.strip_prefix("//") {
        rest.trim_start_matches('/').trim().to_string()
    } else if let Some(inner) = trimmed
        .strip_prefix("/**")
        .and_then(|s| s.strip_suffix("*/"))
    {
        inner
            .lines()
            .map(|line| line.trim().trim_start_matches('*').trim())
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string()
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

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(src: &str) -> SyntacticFacts {
        CppBackend.extract_syntactic(src.as_bytes()).unwrap()
    }

    fn symbol<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("{name} missing"))
    }

    fn symbols_named<'a>(facts: &'a SyntacticFacts, name: &str) -> Vec<&'a SymbolFact> {
        facts.symbols.iter().filter(|s| s.name == name).collect()
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(CppBackend.parser_id(), "tree-sitter-cpp");
    }

    #[test]
    fn claims_cpp_extensions_not_dot_h() {
        let patterns = CppBackend.file_patterns();
        assert!(patterns.contains(&"*.cpp"));
        assert!(patterns.contains(&"*.cc"));
        assert!(patterns.contains(&"*.hpp"));
        assert!(patterns.contains(&"*.hh"));
        assert!(
            !patterns.contains(&"*.h"),
            ".h is routed by content in the register layer",
        );
    }

    #[test]
    fn extracts_namespace_class_function() {
        let f = facts(
            r#"
namespace app {
    class Widget {
    public:
        int render();
    };

    int helper(int x) { return x; }
}
"#,
        );
        assert_eq!(symbol(&f, "app").kind, SymbolKind::Namespace);
        assert_eq!(symbol(&f, "Widget").kind, SymbolKind::Class);
        assert_eq!(symbol(&f, "Widget").qualified, "app::Widget");
        assert_eq!(symbol(&f, "render").kind, SymbolKind::Method);
        assert_eq!(symbol(&f, "render").qualified, "app::Widget::render");
        assert_eq!(symbol(&f, "helper").kind, SymbolKind::Function);
        assert_eq!(symbol(&f, "helper").qualified, "app::helper");
    }

    #[test]
    fn class_with_base_clause_does_not_break_extraction() {
        let f = facts("class Dog : public Animal, IBark { public: void bark(); };");
        assert_eq!(symbol(&f, "Dog").kind, SymbolKind::Class);
        assert_eq!(symbol(&f, "bark").qualified, "Dog::bark");
    }

    #[test]
    fn access_specifier_drives_member_visibility() {
        let f = facts(
            r#"
class W {
    int implicit_private;
public:
    int pub;
    void pub_method();
protected:
    int prot;
private:
    int priv;
};

struct S {
    int implicit_public;
private:
    int priv;
};
"#,
        );
        assert_eq!(
            symbol(&f, "implicit_private").visibility,
            Some(Visibility::Crate)
        );
        assert_eq!(symbol(&f, "pub").visibility, Some(Visibility::Public));
        assert_eq!(
            symbol(&f, "pub_method").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(symbol(&f, "prot").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "priv").visibility, Some(Visibility::Crate));
        assert_eq!(
            symbol(&f, "implicit_public").visibility,
            Some(Visibility::Public)
        );
    }

    #[test]
    fn out_of_class_method_definition_qualifies_under_class() {
        let f = facts(
            r#"
namespace app {
    class W {
    public:
        void render();
        static int counter();
        W();
        ~W();
    };

    void W::render() {}
    int W::counter() { return 0; }
    W::W() {}
    W::~W() {}
}
"#,
        );
        let renders = symbols_named(&f, "render");
        assert_eq!(renders.len(), 2, "in-class decl + out-of-class def");
        assert!(renders.iter().all(|s| s.qualified == "app::W::render"));
        assert_eq!(symbol(&f, "counter").qualified, "app::W::counter");
        let ctors: Vec<_> = f
            .symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Constructor)
            .collect();
        assert!(!ctors.is_empty(), "constructor emitted");
        assert!(ctors.iter().any(|s| s.qualified == "app::W::W"));
        let dtors: Vec<_> = f
            .symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Other("destructor".into()))
            .collect();
        assert!(!dtors.is_empty(), "destructor emitted");
    }

    #[test]
    fn template_class_and_function_keep_template_params_in_signature() {
        let f = facts(
            r#"
template<typename T>
class Box {
public:
    T value;
    T get();
};

template<typename T>
T identity(T x) { return x; }
"#,
        );
        let box_sig = symbol(&f, "Box").signature.as_deref().unwrap();
        assert!(box_sig.contains("template<typename T>"));
        let id_sig = symbol(&f, "identity").signature.as_deref().unwrap();
        assert!(id_sig.contains("template<typename T>"));
        assert!(id_sig.contains("identity(T x)"));
    }

    #[test]
    fn inline_namespace_still_qualifies_children() {
        let f = facts(
            r#"
namespace lib {
    inline namespace v1 {
        class API {};
    }
}
"#,
        );
        let api = symbol(&f, "API");
        assert_eq!(api.qualified, "lib::v1::API");
    }

    #[test]
    fn scoped_enum_qualifies_enumerators_under_enum() {
        let f = facts(
            r#"
enum class Color { Red, Green = 5, Blue };
enum Plain { ONE, TWO };
"#,
        );
        assert_eq!(symbol(&f, "Color").kind, SymbolKind::Enum);
        let red = symbol(&f, "Red");
        assert_eq!(red.kind, SymbolKind::Constant);
        assert_eq!(red.qualified, "Color::Red");
        let one = symbol(&f, "ONE");
        assert_eq!(one.kind, SymbolKind::Constant);
        // Unscoped enum: enumerators leak to enum's parent scope.
        assert_eq!(one.qualified, "ONE");
    }

    #[test]
    fn typedef_and_using_alias_both_indexed() {
        let f = facts(
            r#"
typedef int my_int;
using your_int = int;

namespace n {
    typedef long ll;
    using uint = unsigned;
}
"#,
        );
        assert_eq!(symbol(&f, "my_int").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&f, "your_int").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&f, "ll").qualified, "n::ll");
        assert_eq!(symbol(&f, "uint").qualified, "n::uint");
    }

    #[test]
    fn const_static_field_is_constant_plain_static_is_field() {
        let f = facts(
            r#"
class C {
public:
    static const int LIMIT = 10;
    static int counter;
    const int rate = 5;
};
"#,
        );
        assert_eq!(symbol(&f, "LIMIT").kind, SymbolKind::Constant);
        assert_eq!(symbol(&f, "counter").kind, SymbolKind::Field);
        assert_eq!(symbol(&f, "rate").kind, SymbolKind::Field);
    }

    #[test]
    fn static_method_inside_class_is_method() {
        let f = facts("class C { public: static int make(); };");
        let make = symbol(&f, "make");
        assert_eq!(make.kind, SymbolKind::Method);
        assert_eq!(make.qualified, "C::make");
    }

    #[test]
    fn anonymous_namespace_members_become_crate() {
        let f = facts(
            r#"
namespace {
    int hidden = 0;
    void helper() {}
}

int exported = 0;
static int file_local = 0;
"#,
        );
        assert_eq!(symbol(&f, "hidden").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "helper").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "exported").visibility, Some(Visibility::Public));
        assert_eq!(
            symbol(&f, "file_local").visibility,
            Some(Visibility::Private)
        );
    }

    #[test]
    fn macros_and_includes_extracted() {
        let f = facts(
            r#"
#include <vector>
#include "local.hpp"

#define MAX 10
#define MIN(a, b) ((a) < (b) ? (a) : (b))

int compute() { return 0; }
"#,
        );
        let modules: Vec<&str> = f.imports.iter().map(|i| i.to_module.as_str()).collect();
        assert!(modules.contains(&"<vector>"));
        assert!(modules.contains(&"local.hpp"));
        assert_eq!(symbol(&f, "MAX").kind, SymbolKind::Macro);
        assert_eq!(symbol(&f, "MIN").kind, SymbolKind::Macro);
        assert_eq!(symbol(&f, "compute").kind, SymbolKind::Function);
    }

    #[test]
    fn nested_namespace_c17_form_qualifies_each_segment() {
        let f = facts("namespace a::b::c { class X {}; }");
        assert_eq!(symbol(&f, "X").qualified, "a::b::c::X");
    }
}
