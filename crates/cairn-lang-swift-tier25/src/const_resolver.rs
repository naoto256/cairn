//! Per-file extraction of Swift class / struct / enum / protocol /
//! function / property / import facts and the workspace
//! `PackageIndex` that maps fully-qualified names to defining files.
//!
//! Swift has no `package` declaration keyword (the keyword exists
//! only as a *visibility* modifier). For Tier-2.5 we treat the
//! module as `None` and qualify names with bare lexical nesting
//! (`Outer.Inner.member`). A future revision can derive module names
//! from `Package.swift` once SPM target mapping is wired in; until
//! then bare qualifieds are sufficient for workspace-local
//! resolution.

use std::collections::HashMap;

use tree_sitter::{Node, Parser};

/// Resolved symbol target: where it lives and its fully qualified
/// name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageTarget {
    pub path: String,
    pub qualified: String,
}

/// Syntactic kind of a Swift nominal declaration. Tier-2.5 keeps
/// this around so future dispatch refinements can distinguish
/// protocol existentials from concrete types; today it's emitted
/// for completeness only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwiftTypeKind {
    Class,
    Struct,
    Enum,
    Protocol,
    Extension,
}

/// Per-file extracted Swift facts.
#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    /// Best-effort module name. Always `None` until SPM target
    /// mapping is wired in — kept as an `Option<String>` so the
    /// resolver code already handles the eventual upgrade path.
    pub module: Option<String>,
    /// Class / struct / enum / protocol / extension declarations
    /// declared in this file.
    pub class_defs: Vec<ClassDef>,
    /// Function and method definitions (top-level functions, member
    /// functions, init / deinit).
    pub method_defs: Vec<MethodDef>,
    /// Top-level / member property definitions (workspace-resolvable
    /// constants — not load-bearing for dispatch).
    pub const_defs: Vec<ConstDef>,
    /// Direct heritage edges. Swift cannot distinguish class
    /// inheritance from protocol conformance syntactically; each
    /// entry records the base as written.
    pub base_edges: Vec<BaseEdge>,
    /// Every `import` declaration: each binding's local name → FQN.
    pub import_bindings: Vec<ImportBinding>,
    /// Type-position references emitted at base-name byte ranges (the
    /// span Tier-2 stores as `interface_byte_range`).
    pub type_refs: Vec<TypeRef>,
    /// Statically pinnable call sites.
    pub method_calls: Vec<MethodCall>,
}

#[derive(Debug, Clone)]
pub struct ClassDef {
    /// Fully qualified name including lexical nesting
    /// (`Outer.Inner`).
    pub qualified: String,
    pub kind: SwiftTypeKind,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodDef {
    /// Fully qualified method name (`Service.fetch`,
    /// `Service.init`, or bare `helper` for top-level).
    pub qualified: String,
    /// Owner qualified name — class FQN or empty string for
    /// top-level functions (Swift has no package keyword).
    pub owner: String,
    pub name: String,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct ConstDef {
    pub qualified: String,
    pub owner: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct BaseEdge {
    /// Qualified name of the subclass (`Dog`).
    pub owner: String,
    /// Dotted parts of the base-class expression as written
    /// (`["Animal"]`, `["Module", "Base"]`). Generic args are
    /// already stripped at extraction time.
    pub parts: Vec<String>,
}

/// One binding produced by an `import` declaration.
///
/// Swift has no alias (`import X as Y`) or wildcard (`import X.*`)
/// syntax. `import Foundation`, `import struct Foundation.Date`,
/// and `import UIKit.UIView` all surface as a single plain binding;
/// the `local` name is the *first* dotted segment (the module
/// itself), matching the convention used at use sites — `Date`
/// remains `Foundation.Date` in source even after the dotted import.
#[derive(Debug, Clone)]
pub struct ImportBinding {
    /// Short name introduced into the file's namespace — the first
    /// dotted segment of the import path.
    pub local: String,
    /// Full dotted import path (`UIKit.UIView`,
    /// `Foundation.Date`).
    pub fqn: String,
    pub site_byte_start: u32,
    pub site_byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct TypeRef {
    pub parts: Vec<String>,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodCall {
    pub receiver: CallReceiver,
    pub method: String,
    /// Byte range covering just the method-name identifier (same
    /// span the Tier-2 backend records on the resolved-callee site).
    pub byte_start: u32,
    pub byte_end: u32,
    /// Qualified name of the lexically enclosing class / struct /
    /// enum / protocol / extension (for `self.foo` / `super.foo`).
    pub lexical_class: Option<String>,
    /// Qualified name of the lexically enclosing function /
    /// initializer / method (used to deduplicate enclosing context).
    pub lexical_function: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallReceiver {
    /// `Cls.method()` / `module.Cls.method()`. The dotted prefix as
    /// a chain of identifiers.
    Dotted { parts: Vec<String> },
    /// `self.method()` — receiver is `self`, resolved via the
    /// lexical type's MRO.
    SelfRef,
    /// `super.method()` — MRO walk starting after the lexical type.
    SuperRef,
    /// `foo()` — bare identifier callee (top-level function,
    /// imported function, or constructor invocation).
    Bare { name: String },
    /// Anything else — not resolvable at Tier-2.5.
    Unknown,
}

/// Parse a single Swift source blob and extract its facts.
#[must_use]
pub fn parse_file(source: &[u8]) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_swift::LANGUAGE.into();
    let mut parser = Parser::new();
    parser.set_language(&language).ok()?;
    let tree = parser.parse(source, None)?;
    let mut visitor = Visitor::new(source);
    visitor.walk(tree.root_node());
    Some(visitor.facts)
}

struct Visitor<'a> {
    source: &'a [u8],
    facts: FileConstFacts,
    /// Lexical container stack: each entry's qualified-name segment
    /// active inside that container.
    container_stack: Vec<ContainerFrame>,
}

#[derive(Debug, Clone)]
struct ContainerFrame {
    /// Bare segment name pushed onto the qualified chain.
    name: String,
    /// True when this frame is a class / struct / enum / protocol /
    /// extension (i.e. eligible to own methods).
    is_class: bool,
}

impl<'a> Visitor<'a> {
    fn new(source: &'a [u8]) -> Self {
        Self {
            source,
            facts: FileConstFacts::default(),
            container_stack: Vec::new(),
        }
    }

    fn text(&self, node: Node<'_>) -> &str {
        std::str::from_utf8(&self.source[node.byte_range()]).unwrap_or("")
    }

    /// Build the qualified name for a leaf `name` declared inside
    /// the current container chain.
    fn qualify_in_scope(&self, leaf: &str) -> String {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(m) = self.facts.module.as_deref() {
            if !m.is_empty() {
                parts.push(m);
            }
        }
        for f in &self.container_stack {
            parts.push(&f.name);
        }
        parts.push(leaf);
        parts.join(".")
    }

    /// The innermost class frame's qualified name (used for `self`
    /// / `super` and for method owners).
    fn current_class(&self) -> Option<String> {
        let mut last_class_idx: Option<usize> = None;
        for (i, frame) in self.container_stack.iter().enumerate() {
            if frame.is_class {
                last_class_idx = Some(i);
            }
        }
        let cut = last_class_idx?;
        let mut out: Vec<&str> = Vec::new();
        if let Some(m) = self.facts.module.as_deref() {
            if !m.is_empty() {
                out.push(m);
            }
        }
        for frame in &self.container_stack[..=cut] {
            out.push(&frame.name);
        }
        Some(out.join("."))
    }

    fn current_function(&self) -> Option<String> {
        let mut last_fn_idx: Option<usize> = None;
        for (i, frame) in self.container_stack.iter().enumerate() {
            if !frame.is_class {
                last_fn_idx = Some(i);
            }
        }
        let cut = last_fn_idx?;
        let mut out: Vec<&str> = Vec::new();
        if let Some(m) = self.facts.module.as_deref() {
            if !m.is_empty() {
                out.push(m);
            }
        }
        for frame in &self.container_stack[..=cut] {
            out.push(&frame.name);
        }
        Some(out.join("."))
    }

    /// Owner FQN for a top-level (no enclosing class) callable: the
    /// file's module, or empty when the file has no module
    /// (workspace-loose Swift file).
    fn top_level_owner(&self) -> String {
        self.facts.module.clone().unwrap_or_default()
    }

    fn walk(&mut self, node: Node<'_>) {
        if node.is_error() || node.is_missing() {
            self.descend_children(node);
            return;
        }
        match node.kind() {
            "import_declaration" => {
                self.emit_import(node);
                return;
            }
            "class_declaration" => {
                self.enter_class_like(node);
                return;
            }
            "protocol_declaration" => {
                self.enter_protocol(node);
                return;
            }
            "function_declaration" | "protocol_function_declaration" => {
                self.enter_function(node, None);
                return;
            }
            "init_declaration" => {
                self.enter_function(node, Some("init"));
                return;
            }
            "deinit_declaration" => {
                self.enter_function(node, Some("deinit"));
                return;
            }
            "property_declaration" | "protocol_property_declaration" => {
                self.emit_property(node);
                // Fall through so initializer expressions still get
                // visited for nested call sites.
                self.descend_children(node);
                return;
            }
            "call_expression" => {
                self.emit_call(node);
                // Fall through so we visit nested calls inside the
                // arguments / trailing closures.
            }
            _ => {}
        }
        self.descend_children(node);
    }

    fn descend_children(&mut self, node: Node<'_>) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.walk(child);
        }
    }

    /// Enter a `class_declaration` node — which in tree-sitter-swift
    /// covers `class`, `struct`, `enum`, and `extension` (the actual
    /// keyword is on the `declaration_kind` field).
    fn enter_class_like(&mut self, node: Node<'_>) {
        let keyword = find_field(node, "declaration_kind")
            .map(|n| self.text(n).to_string())
            .unwrap_or_else(|| "class".to_string());
        let kind = match keyword.as_str() {
            "struct" => SwiftTypeKind::Struct,
            "enum" => SwiftTypeKind::Enum,
            "extension" => SwiftTypeKind::Extension,
            _ => SwiftTypeKind::Class,
        };

        let Some(name) = declared_type_name(node, self.source) else {
            self.descend_children(node);
            return;
        };
        let qualified = self.qualify_in_scope(&name);

        // Heritage walk: every entry in the `:` list shows up as an
        // `inheritance_specifier` child with a single `inherits_from`
        // field. Swift's syntax cannot tell us whether the first
        // entry is a class-superclass or a protocol; we treat them
        // all uniformly here and let the MRO logic linearize.
        let mut cursor = node.walk();
        for specifier in node.children(&mut cursor) {
            if specifier.kind() != "inheritance_specifier" {
                continue;
            }
            let Some(base_node) = find_field(specifier, "inherits_from") else {
                continue;
            };
            let Some(parts) = type_parts(base_node, self.source) else {
                continue;
            };
            let r = base_name_range(base_node).unwrap_or_else(|| base_node.byte_range());
            self.facts.base_edges.push(BaseEdge {
                owner: qualified.clone(),
                parts: parts.clone(),
            });
            self.facts.type_refs.push(TypeRef {
                parts,
                byte_start: r.start as u32,
                byte_end: r.end as u32,
            });
        }

        self.facts.class_defs.push(ClassDef {
            qualified: qualified.clone(),
            kind,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        self.container_stack.push(ContainerFrame {
            name,
            is_class: true,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn enter_protocol(&mut self, node: Node<'_>) {
        let Some(name) = declared_type_name(node, self.source) else {
            self.descend_children(node);
            return;
        };
        let qualified = self.qualify_in_scope(&name);

        let mut cursor = node.walk();
        for specifier in node.children(&mut cursor) {
            if specifier.kind() != "inheritance_specifier" {
                continue;
            }
            let Some(base_node) = find_field(specifier, "inherits_from") else {
                continue;
            };
            let Some(parts) = type_parts(base_node, self.source) else {
                continue;
            };
            let r = base_name_range(base_node).unwrap_or_else(|| base_node.byte_range());
            self.facts.base_edges.push(BaseEdge {
                owner: qualified.clone(),
                parts: parts.clone(),
            });
            self.facts.type_refs.push(TypeRef {
                parts,
                byte_start: r.start as u32,
                byte_end: r.end as u32,
            });
        }

        self.facts.class_defs.push(ClassDef {
            qualified: qualified.clone(),
            kind: SwiftTypeKind::Protocol,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        self.container_stack.push(ContainerFrame {
            name,
            is_class: true,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn enter_function(&mut self, node: Node<'_>, fixed_name: Option<&str>) {
        let name = match fixed_name {
            Some(s) => s.to_string(),
            None => {
                let Some(name_node) = find_field(node, "name") else {
                    self.descend_children(node);
                    return;
                };
                if name_node.kind() != "simple_identifier" {
                    self.descend_children(node);
                    return;
                }
                self.text(name_node).to_string()
            }
        };
        let qualified = self.qualify_in_scope(&name);
        let owner = if let Some(class) = self.current_class() {
            class
        } else {
            self.top_level_owner()
        };
        self.facts.method_defs.push(MethodDef {
            qualified: qualified.clone(),
            owner,
            name: name.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        self.container_stack.push(ContainerFrame {
            name,
            is_class: false,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn emit_property(&mut self, node: Node<'_>) {
        // Skip function-local bindings — they're noise in a workspace
        // index and the Tier-1 backend already filters them out.
        if is_inside_function_body(node) {
            return;
        }
        let mut cursor = node.walk();
        let patterns: Vec<Node<'_>> = node.children_by_field_name("name", &mut cursor).collect();
        let mut names: Vec<String> = Vec::new();
        for pattern in patterns {
            collect_pattern_names(pattern, self.source, &mut names);
        }
        let owner = if let Some(class) = self.current_class() {
            class
        } else {
            self.top_level_owner()
        };
        for name in names {
            let qualified = self.qualify_in_scope(&name);
            self.facts.const_defs.push(ConstDef {
                qualified,
                owner: owner.clone(),
                name,
            });
        }
    }

    fn emit_call(&mut self, node: Node<'_>) {
        // tree-sitter-swift `call_expression` shape: first named
        // child is the callee (`simple_identifier` for bare,
        // `navigation_expression` for member access).
        let Some(callee) = node.named_child(0) else {
            return;
        };
        let (receiver, method, name_node) = classify_call(callee, self.source);
        let Some(method) = method else { return };
        let r = name_node.byte_range();
        self.facts.method_calls.push(MethodCall {
            receiver,
            method,
            byte_start: r.start as u32,
            byte_end: r.end as u32,
            lexical_class: self.current_class(),
            lexical_function: self.current_function(),
        });
    }

    fn emit_import(&mut self, node: Node<'_>) {
        // tree-sitter-swift import_declaration: identifier holds the
        // dotted path (`Foundation`, `UIKit.UIView`).
        let mut cursor = node.walk();
        let path_node = node
            .named_children(&mut cursor)
            .find(|c| c.kind() == "identifier");
        let Some(path_node) = path_node else { return };
        let path_text = self.text(path_node).trim().to_string();
        if path_text.is_empty() {
            return;
        }
        let head = path_text
            .split('.')
            .next()
            .unwrap_or(&path_text)
            .to_string();
        let r = path_node.byte_range();
        self.facts.import_bindings.push(ImportBinding {
            local: head,
            fqn: path_text,
            site_byte_start: r.start as u32,
            site_byte_end: r.end as u32,
        });
    }
}

/// Name of a `class_declaration` / `protocol_declaration`. Nominal
/// types carry a `type_identifier`; extensions carry a `user_type`
/// whose first `type_identifier` is the extended base name.
fn declared_type_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let name = find_field(node, "name")?;
    match name.kind() {
        "type_identifier" => Some(
            std::str::from_utf8(&source[name.byte_range()])
                .ok()?
                .to_string(),
        ),
        "user_type" => {
            let mut cursor = name.walk();
            name.named_children(&mut cursor)
                .find(|c| c.kind() == "type_identifier")
                .and_then(|c| std::str::from_utf8(&source[c.byte_range()]).ok())
                .map(str::to_string)
        }
        _ => std::str::from_utf8(&source[name.byte_range()])
            .ok()
            .map(str::to_string),
    }
}

/// `type_identifier` / `user_type` → dotted parts. `Module.Base<T>`
/// → `["Module", "Base"]`, `Foo` → `["Foo"]`.
fn type_parts(node: Node<'_>, source: &[u8]) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    walk_type_parts(node, source, &mut parts);
    if parts.is_empty() { None } else { Some(parts) }
}

fn walk_type_parts(node: Node<'_>, source: &[u8], out: &mut Vec<String>) {
    match node.kind() {
        "type_identifier" | "simple_identifier" => {
            if let Ok(text) = std::str::from_utf8(&source[node.byte_range()]) {
                out.push(text.to_string());
            }
        }
        "user_type" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                // Skip generic argument clauses — they're a sibling
                // node, not a child of the identifier we care about.
                if child.kind() == "type_arguments" {
                    continue;
                }
                walk_type_parts(child, source, out);
            }
        }
        _ => {
            // Some grammar variants wrap the user_type in
            // `type_annotation` or similar; descend through named
            // children once to find the identifier(s).
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "type_arguments" {
                    continue;
                }
                walk_type_parts(child, source, out);
            }
        }
    }
}

/// Byte range to pin a heritage Type resolution at. Matches the
/// span the Tier-2 backend records on `interface_byte_range` so the
/// JOIN aligns.
fn base_name_range(node: Node<'_>) -> Option<std::ops::Range<usize>> {
    // Use the whole base node range — the Tier-2 backend records
    // `ty.byte_range()` for inheritance bases as well.
    Some(node.byte_range())
}

fn collect_pattern_names(node: Node<'_>, source: &[u8], out: &mut Vec<String>) {
    if node.kind() == "simple_identifier" {
        if let Ok(text) = std::str::from_utf8(&source[node.byte_range()]) {
            out.push(text.to_string());
        }
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_pattern_names(child, source, out);
    }
}

/// Property nodes double as function-local bindings in
/// tree-sitter-swift. Same rule as the Tier-1 backend: only
/// declarations outside executable scopes are emitted.
fn is_inside_function_body(node: Node<'_>) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        match parent.kind() {
            "function_body" | "computed_property" | "computed_getter" | "computed_setter"
            | "lambda_literal" | "statements" => return true,
            "class_body" | "protocol_body" | "enum_class_body" | "source_file" => return false,
            _ => current = parent.parent(),
        }
    }
    false
}

/// Classify a `call_expression` callee. Returns `(receiver,
/// method_name, name_node)`. `name_node` is the identifier whose
/// byte range Tier-2 records on the resolved-callee site.
fn classify_call<'tree>(
    callee: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    match callee.kind() {
        "simple_identifier" => {
            let name = std::str::from_utf8(&source[callee.byte_range()])
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                return (CallReceiver::Unknown, None, callee);
            }
            (
                CallReceiver::Bare { name: name.clone() },
                Some(name),
                callee,
            )
        }
        "navigation_expression" => classify_navigation_call(callee, source),
        _ => (CallReceiver::Unknown, None, callee),
    }
}

fn classify_navigation_call<'tree>(
    nav: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    // tree-sitter-swift navigation_expression: `target` field for
    // the receiver, `suffix` field for the `.member` segment whose
    // first named child is a `simple_identifier`.
    let target = find_field(nav, "target");
    let suffix = match find_field(nav, "suffix") {
        Some(s) => s,
        None => return (CallReceiver::Unknown, None, nav),
    };
    let member = match first_named_child(suffix) {
        Some(m) if m.kind() == "simple_identifier" => m,
        _ => return (CallReceiver::Unknown, None, nav),
    };
    let method = std::str::from_utf8(&source[member.byte_range()])
        .unwrap_or("")
        .to_string();
    if method.is_empty() {
        return (CallReceiver::Unknown, None, member);
    }

    // Flatten the receiver into a dotted identifier chain (or a
    // self / super marker).
    let mut idents: Vec<String> = Vec::new();
    let mut saw_self = false;
    let mut saw_super = false;
    let mut unknown_receiver = false;
    if let Some(t) = target {
        flatten_receiver(
            t,
            source,
            &mut idents,
            &mut saw_self,
            &mut saw_super,
            &mut unknown_receiver,
        );
    }

    let receiver = if unknown_receiver {
        CallReceiver::Unknown
    } else if saw_super && idents.is_empty() {
        CallReceiver::SuperRef
    } else if saw_self && idents.is_empty() {
        CallReceiver::SelfRef
    } else if !idents.is_empty() {
        CallReceiver::Dotted { parts: idents }
    } else {
        CallReceiver::Unknown
    };
    (receiver, Some(method), member)
}

fn flatten_receiver(
    node: Node<'_>,
    source: &[u8],
    idents: &mut Vec<String>,
    saw_self: &mut bool,
    saw_super: &mut bool,
    unknown: &mut bool,
) {
    match node.kind() {
        "simple_identifier" => {
            if let Ok(text) = std::str::from_utf8(&source[node.byte_range()]) {
                let s = text.to_string();
                if s == "self" {
                    *saw_self = true;
                } else if s == "super" {
                    *saw_super = true;
                } else if !s.is_empty() {
                    idents.push(s);
                }
            }
        }
        "self_expression" => *saw_self = true,
        "super_expression" => *saw_super = true,
        "navigation_expression" => {
            // Recurse into nested nav: `a.b.c.method()` parses as a
            // chain.
            let inner_target = find_field(node, "target");
            let inner_suffix = find_field(node, "suffix");
            if let Some(t) = inner_target {
                flatten_receiver(t, source, idents, saw_self, saw_super, unknown);
            }
            if let Some(s) = inner_suffix {
                if let Some(member) = first_named_child(s) {
                    if member.kind() == "simple_identifier" {
                        if let Ok(text) = std::str::from_utf8(&source[member.byte_range()]) {
                            idents.push(text.to_string());
                        }
                    } else {
                        *unknown = true;
                    }
                }
            }
        }
        _ => {
            // Any other receiver shape (call result, literal,
            // parenthesized expression, subscript, etc.) — not
            // resolvable at Tier-2.5.
            *unknown = true;
        }
    }
}

fn find_field<'a>(node: Node<'a>, field: &str) -> Option<Node<'a>> {
    node.child_by_field_name(field)
}

fn first_named_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).next()
}

// ─── workspace-wide package index ─────────────────────────────────────────

/// Workspace symbol index: every class / struct / enum / protocol /
/// function / property's FQN → defining file. Swift FQNs in
/// Tier-2.5 are bare lexical chains (`Outer.Inner.member`) because
/// the language has no `package` keyword.
#[derive(Debug, Default)]
pub struct PackageIndex {
    by_qualified: HashMap<String, PackageTarget>,
    files_by_module: HashMap<String, Vec<String>>,
}

impl PackageIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_qualified: HashMap<String, PackageTarget> = HashMap::new();
        let mut files_by_module: HashMap<String, Vec<String>> = HashMap::new();

        for (path, _, facts) in per_file {
            if let Some(module) = facts.module.as_deref() {
                files_by_module
                    .entry(module.to_string())
                    .or_default()
                    .push(path.clone());
            }
            for class in &facts.class_defs {
                by_qualified
                    .entry(class.qualified.clone())
                    .or_insert(PackageTarget {
                        path: path.clone(),
                        qualified: class.qualified.clone(),
                    });
            }
            for m in &facts.method_defs {
                by_qualified
                    .entry(m.qualified.clone())
                    .or_insert(PackageTarget {
                        path: path.clone(),
                        qualified: m.qualified.clone(),
                    });
            }
            for c in &facts.const_defs {
                by_qualified
                    .entry(c.qualified.clone())
                    .or_insert(PackageTarget {
                        path: path.clone(),
                        qualified: c.qualified.clone(),
                    });
            }
        }

        Self {
            by_qualified,
            files_by_module,
        }
    }

    pub fn lookup(&self, qualified: &str) -> Option<&PackageTarget> {
        self.by_qualified.get(qualified)
    }

    pub fn files_in_module(&self, module: &str) -> &[String] {
        self.files_by_module
            .get(module)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn has_module(&self, module: &str) -> bool {
        self.files_by_module.contains_key(module)
    }
}
