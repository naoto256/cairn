//! Per-file extraction of C# class/struct/interface/record/enum/method/
//! using facts and the workspace `PackageIndex` that maps fully-
//! qualified names to defining files.
//!
//! C# qualified names are namespace + type-nesting + member, mirroring
//! the Tier-1 backend's `NestingTracker(".")`. Namespaces can be
//! block-scoped (`namespace A { ... }`) or file-scoped
//! (`namespace A;` — applies to the rest of the file).

use std::collections::HashMap;

use tree_sitter::{Node, Parser};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageTarget {
    pub path: String,
    pub qualified: String,
}

#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    /// The *outermost* namespace declared in this file. Used as the
    /// default prefix for top-level callable lookup. For files with
    /// multiple sibling namespaces we still record class FQNs
    /// correctly (the container stack tracks every namespace), but
    /// "current package" for bare-call resolution defaults to the
    /// first one seen — pragmatically enough.
    pub package: Option<String>,
    /// Class / struct / interface / record / enum definitions.
    pub class_defs: Vec<ClassDef>,
    /// Method, constructor, and top-level statement definitions.
    pub method_defs: Vec<MethodDef>,
    /// Constant fields (declared with `const`).
    pub const_defs: Vec<ConstDef>,
    /// Direct heritage edges from `base_list`.
    pub base_edges: Vec<BaseEdge>,
    /// Every `using` directive: bindings local-name → FQN.
    pub import_bindings: Vec<ImportBinding>,
    /// Type-position references at base-name byte ranges.
    pub type_refs: Vec<TypeRef>,
    /// Statically pinnable call sites.
    pub method_calls: Vec<MethodCall>,
}

#[derive(Debug, Clone)]
pub struct ClassDef {
    pub qualified: String,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodDef {
    pub qualified: String,
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
    pub owner: String,
    pub parts: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportKind {
    /// `using System.Collections;` — `local = "Collections"`,
    /// `fqn = "System.Collections"`.
    Plain,
    /// `using F = System.Func<int>;` — `local = "F"`, `fqn = "System.Func<int>"`.
    Aliased,
    /// `using static System.Math;` — `local = "*"`, `fqn = "System.Math"`.
    Static,
}

#[derive(Debug, Clone)]
pub struct ImportBinding {
    pub kind: ImportKind,
    pub local: String,
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
    pub byte_start: u32,
    pub byte_end: u32,
    pub lexical_class: Option<String>,
    pub lexical_function: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallReceiver {
    /// `Cls.Method()` / `ns.Cls.Method()` — receiver is a dotted
    /// identifier chain.
    Dotted { parts: Vec<String> },
    /// `this.Method()` — current lexical class's MRO walk.
    ThisRef,
    /// `base.Method()` — MRO walk starting after the lexical class.
    SuperRef,
    /// `Foo()` — bare callee (top-level / same-class / using-static).
    Bare { name: String },
    /// Anything else.
    Unknown,
}

/// Parse a single C# source blob and extract its facts.
#[must_use]
pub fn parse_file(source: &[u8]) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_c_sharp::LANGUAGE.into();
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
    /// Container stack: each entry is a namespace or type segment.
    container_stack: Vec<ContainerFrame>,
}

impl<'a> Visitor<'a> {
    fn pop_outside(&mut self, byte: usize) {
        while let Some(top) = self.container_stack.last() {
            if top.end_byte <= byte {
                self.container_stack.pop();
            } else {
                break;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ContainerFrame {
    /// Bare segment name pushed onto the qualified chain. For
    /// namespaces this can itself be dotted (`A.B`) since `namespace
    /// A.B` is a single node.
    name: String,
    /// True when this frame is a class / struct / interface / record /
    /// enum (i.e. eligible to own methods).
    is_class: bool,
    /// Byte offset at which this frame goes out of scope. For block-
    /// scoped types and namespaces this is `node.end_byte()`; for
    /// file-scoped namespaces (`namespace A;`) this is the file
    /// length (rest of the file).
    end_byte: usize,
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

    /// Build the qualified name for a leaf declared in the current
    /// container chain.
    fn qualify_in_scope(&self, leaf: &str) -> String {
        let mut parts: Vec<&str> = Vec::new();
        for f in &self.container_stack {
            parts.push(&f.name);
        }
        parts.push(leaf);
        parts.join(".")
    }

    /// Innermost class frame's qualified name (this/base targets).
    fn current_class(&self) -> Option<String> {
        let mut last_class_idx: Option<usize> = None;
        for (i, frame) in self.container_stack.iter().enumerate() {
            if frame.is_class {
                last_class_idx = Some(i);
            }
        }
        let cut = last_class_idx?;
        let out: Vec<&str> = self.container_stack[..=cut]
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        Some(out.join("."))
    }

    fn current_function(&self) -> Option<String> {
        // We don't push function frames onto the container stack at
        // Tier-2.5; the dispatch resolver doesn't need the enclosing
        // function FQN — only the enclosing class. Kept as a stub for
        // API parity with other tier25 backends.
        None
    }

    /// Outermost (default) package for bare callable lookup. Returns
    /// the first namespace pushed onto the stack, or empty.
    fn outermost_namespace(&self) -> String {
        self.facts.package.clone().unwrap_or_default()
    }

    fn walk(&mut self, node: Node<'_>) {
        self.pop_outside(node.start_byte());
        if node.is_error() || node.is_missing() {
            self.descend_children(node);
            return;
        }
        match node.kind() {
            "using_directive" => {
                self.emit_using(node);
                return;
            }
            "namespace_declaration" | "file_scoped_namespace_declaration" => {
                self.enter_namespace(node);
                return;
            }
            "class_declaration"
            | "interface_declaration"
            | "struct_declaration"
            | "record_declaration"
            | "enum_declaration" => {
                self.enter_class(node);
                return;
            }
            "method_declaration" | "constructor_declaration" => {
                self.enter_function(node);
                return;
            }
            "field_declaration" => {
                self.emit_field(node);
                return;
            }
            "invocation_expression" => {
                self.emit_call(node);
                // Fall through to recurse into arguments.
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

    fn enter_namespace(&mut self, node: Node<'_>) {
        let Some(name_node) = find_field(node, "name") else {
            self.descend_children(node);
            return;
        };
        let name = self.text(name_node).trim().to_string();
        if name.is_empty() {
            self.descend_children(node);
            return;
        }
        if self.facts.package.is_none() {
            // Record the *outermost* namespace as the file's default
            // package. Sibling namespaces still get their FQNs right
            // because we push every name on the stack.
            self.facts.package = Some(name.clone());
        }
        // File-scoped namespace declarations don't contain their
        // declarations as children — those are siblings at the
        // compilation_unit level. Push with end_byte = source.len so
        // the scope persists across subsequent sibling walks; the
        // pop_outside hook at walk() will retire stale frames before
        // crossing into a later top-level scope.
        let end_byte = if node.kind() == "file_scoped_namespace_declaration" {
            self.source.len()
        } else {
            node.end_byte()
        };
        self.container_stack.push(ContainerFrame {
            name,
            is_class: false,
            end_byte,
        });
        self.descend_children(node);
        // For block-scoped namespaces, pop_outside handles cleanup on
        // the parent's next walk step. For file-scoped, we deliberately
        // leave the frame in place.
    }

    fn enter_class(&mut self, node: Node<'_>) {
        let Some(name_node) = find_field(node, "name") else {
            self.descend_children(node);
            return;
        };
        let name = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name);

        // Record heritage edges + type refs at base-name byte ranges.
        if let Some(base_list) = find_direct_child(node, "base_list") {
            let mut cursor = base_list.walk();
            for child in base_list.named_children(&mut cursor) {
                let base_node = if child.kind() == "primary_constructor_base_type" {
                    match child.named_child(0) {
                        Some(n) => n,
                        None => continue,
                    }
                } else if child.kind() == "argument_list" {
                    continue;
                } else {
                    child
                };
                let Some(parts) = type_parts(base_node, self.source) else {
                    continue;
                };
                let r = base_node.byte_range();
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
        }

        self.facts.class_defs.push(ClassDef {
            qualified: qualified.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        let end_byte = node.end_byte();
        self.container_stack.push(ContainerFrame {
            name,
            is_class: true,
            end_byte,
        });
        self.descend_children(node);
        // pop_outside cleans up when we leave this byte range.
    }

    fn enter_function(&mut self, node: Node<'_>) {
        let Some(name_node) = find_field(node, "name") else {
            self.descend_children(node);
            return;
        };
        let name_text = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name_text);
        let owner = self
            .current_class()
            .unwrap_or_else(|| self.outermost_namespace());
        self.facts.method_defs.push(MethodDef {
            qualified: qualified.clone(),
            owner,
            name: name_text,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        // We don't push a function frame: this matches Kotlin's pattern
        // of recording method body call sites with the enclosing class
        // as `lexical_class`. (Nested local functions / lambdas are
        // out of scope at Tier-2.5.)
        self.descend_children(node);
    }

    fn emit_field(&mut self, node: Node<'_>) {
        // Constant field — record under the enclosing class as a
        // package-index entry (used for `Cls.Constant` lookup).
        let is_const = has_modifier(node, self.source, "const");
        if !is_const {
            self.descend_children(node);
            return;
        }
        let Some(var_decl) = find_direct_child(node, "variable_declaration") else {
            self.descend_children(node);
            return;
        };
        let owner = self
            .current_class()
            .unwrap_or_else(|| self.outermost_namespace());
        let mut cursor = var_decl.walk();
        for declarator in var_decl.named_children(&mut cursor) {
            if declarator.kind() != "variable_declarator" {
                continue;
            }
            let Some(name_node) = find_field(declarator, "name") else {
                continue;
            };
            let name = self.text(name_node).to_string();
            let qualified = self.qualify_in_scope(&name);
            self.facts.const_defs.push(ConstDef {
                qualified,
                owner: owner.clone(),
                name,
            });
        }
        self.descend_children(node);
    }

    fn emit_call(&mut self, node: Node<'_>) {
        let Some(function) = find_field(node, "function") else {
            return;
        };
        let (receiver, method, name_node) = classify_call(function, self.source);
        let Some(method) = method else { return };
        let name_range = name_node.byte_range();
        self.facts.method_calls.push(MethodCall {
            receiver,
            method,
            byte_start: name_range.start as u32,
            byte_end: name_range.end as u32,
            lexical_class: self.current_class(),
            lexical_function: self.current_function(),
        });
    }

    fn emit_using(&mut self, node: Node<'_>) {
        // Three shapes:
        //   using A.B;            (Plain)
        //   using F = A.B.C;      (Aliased — `name` field is the alias)
        //   using static A.B.C;   (Static)
        //   global using …;       (treated like the non-global form)
        let alias_id = find_field(node, "name").map(|n| n.id());
        let mut cursor = node.walk();
        let mut target: Option<Node<'_>> = None;
        for child in node.named_children(&mut cursor) {
            if matches!(
                child.kind(),
                "identifier" | "qualified_name" | "generic_name" | "alias_qualified_name"
            ) && alias_id != Some(child.id())
            {
                target = Some(child);
            }
        }
        let Some(target) = target else { return };
        let fqn = self.text(target).trim().to_string();
        if fqn.is_empty() {
            return;
        }
        let r = target.byte_range();
        let is_static = has_keyword_child(node, "static");
        let alias = find_field(node, "name").map(|n| self.text(n).to_string());

        let (kind, local) = if is_static {
            (ImportKind::Static, "*".to_string())
        } else if let Some(a) = alias {
            (ImportKind::Aliased, a)
        } else {
            // `using A.B;` — locally binds `B` to the FQN `A.B`. This
            // is *not* exactly C# semantics (C# imports all the
            // *contents* of namespace `A.B`), but it's the practical
            // shape for Tier-2.5: we use the binding both as a direct
            // alias (for `B` references that resolve to namespace
            // `A.B`) and as a wildcard expansion candidate when
            // resolving bare type names.
            let leaf = fqn.rsplit('.').next().unwrap_or(&fqn).to_string();
            (ImportKind::Plain, leaf)
        };
        self.facts.import_bindings.push(ImportBinding {
            kind,
            local,
            fqn,
            site_byte_start: r.start as u32,
            site_byte_end: r.end as u32,
        });
    }
}

/// Strip generic args from a type reference node and return identifier
/// parts. `A.B<T>.C` → `["A","B","C"]`.
pub(crate) fn type_parts(node: Node<'_>, source: &[u8]) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    collect_type_parts(node, source, &mut parts);
    if parts.is_empty() { None } else { Some(parts) }
}

fn collect_type_parts(node: Node<'_>, source: &[u8], parts: &mut Vec<String>) {
    match node.kind() {
        "identifier" => {
            if let Ok(t) = std::str::from_utf8(&source[node.byte_range()]) {
                parts.push(t.to_string());
            }
        }
        "qualified_name" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                collect_type_parts(child, source, parts);
            }
        }
        "generic_name" => {
            // Bare class name + type arg list. Take the leading
            // identifier only.
            if let Some(first) = node.named_child(0) {
                if first.kind() == "identifier" {
                    if let Ok(t) = std::str::from_utf8(&source[first.byte_range()]) {
                        parts.push(t.to_string());
                    }
                }
            }
        }
        _ => {}
    }
}

fn classify_call<'tree>(
    function: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    match function.kind() {
        "identifier" => {
            let name = std::str::from_utf8(&source[function.byte_range()])
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                return (CallReceiver::Unknown, None, function);
            }
            (
                CallReceiver::Bare { name: name.clone() },
                Some(name),
                function,
            )
        }
        "generic_name" => {
            // Foo<T>(…) — the leading identifier is the method name.
            let Some(name_node) = function.named_child(0) else {
                return (CallReceiver::Unknown, None, function);
            };
            let name = std::str::from_utf8(&source[name_node.byte_range()])
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                return (CallReceiver::Unknown, None, function);
            }
            (
                CallReceiver::Bare { name: name.clone() },
                Some(name),
                name_node,
            )
        }
        "member_access_expression" => classify_member_access(function, source),
        _ => (CallReceiver::Unknown, None, function),
    }
}

fn classify_member_access<'tree>(
    member: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    // member_access_expression has fields: `expression` (the receiver)
    // and `name` (the member identifier or generic_name).
    let receiver_node = member.child_by_field_name("expression");
    let name_field = match member.child_by_field_name("name") {
        Some(n) => n,
        None => return (CallReceiver::Unknown, None, member),
    };
    let name_id_node = if name_field.kind() == "generic_name" {
        match name_field.named_child(0) {
            Some(n) => n,
            None => return (CallReceiver::Unknown, None, member),
        }
    } else {
        name_field
    };
    let method = std::str::from_utf8(&source[name_id_node.byte_range()])
        .unwrap_or("")
        .to_string();
    if method.is_empty() {
        return (CallReceiver::Unknown, None, name_id_node);
    }

    let receiver = match receiver_node {
        Some(r) => classify_receiver(r, source),
        None => CallReceiver::Unknown,
    };
    (receiver, Some(method), name_id_node)
}

fn classify_receiver(node: Node<'_>, source: &[u8]) -> CallReceiver {
    match node.kind() {
        "this_expression" | "this" => CallReceiver::ThisRef,
        "base_expression" | "base" => CallReceiver::SuperRef,
        "identifier" => {
            let name = std::str::from_utf8(&source[node.byte_range()])
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                CallReceiver::Unknown
            } else {
                CallReceiver::Dotted { parts: vec![name] }
            }
        }
        "member_access_expression" => {
            // Walk nested member access into a dotted chain.
            let mut parts: Vec<String> = Vec::new();
            if flatten_member_chain(node, source, &mut parts) {
                if parts.is_empty() {
                    CallReceiver::Unknown
                } else {
                    CallReceiver::Dotted { parts }
                }
            } else {
                CallReceiver::Unknown
            }
        }
        "qualified_name" => {
            // Some grammars use qualified_name even outside of `using`.
            if let Some(parts) = type_parts(node, source) {
                CallReceiver::Dotted { parts }
            } else {
                CallReceiver::Unknown
            }
        }
        _ => CallReceiver::Unknown,
    }
}

/// Flatten a `member_access_expression` chain into a list of
/// identifiers. Returns false on anything non-dotted (literals, call
/// results, etc.) so the caller can mark the receiver Unknown rather
/// than emit a partial chain.
fn flatten_member_chain(node: Node<'_>, source: &[u8], parts: &mut Vec<String>) -> bool {
    match node.kind() {
        "identifier" => {
            if let Ok(t) = std::str::from_utf8(&source[node.byte_range()]) {
                parts.push(t.to_string());
            }
            true
        }
        "member_access_expression" => {
            let Some(expr) = node.child_by_field_name("expression") else {
                return false;
            };
            let Some(name) = node.child_by_field_name("name") else {
                return false;
            };
            if !flatten_member_chain(expr, source, parts) {
                return false;
            }
            let name_id = if name.kind() == "generic_name" {
                match name.named_child(0) {
                    Some(n) => n,
                    None => return false,
                }
            } else if name.kind() == "identifier" {
                name
            } else {
                return false;
            };
            if let Ok(t) = std::str::from_utf8(&source[name_id.byte_range()]) {
                parts.push(t.to_string());
            }
            true
        }
        _ => false,
    }
}

fn find_direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

fn find_field<'a>(node: Node<'a>, field: &str) -> Option<Node<'a>> {
    node.child_by_field_name(field)
}

fn has_keyword_child(node: Node<'_>, keyword: &str) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .any(|c| !c.is_named() && c.kind() == keyword)
}

fn has_modifier(node: Node<'_>, source: &[u8], text: &str) -> bool {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).any(|c| {
        c.kind() == "modifier"
            && std::str::from_utf8(&source[c.byte_range()])
                .map(|t| t == text)
                .unwrap_or(false)
    })
}

// ─── workspace-wide package index ─────────────────────────────────────────

/// Workspace symbol index: every class / struct / interface / record /
/// method / const's FQN → defining file. C# `partial` types map the
/// same FQN to multiple files — the first declaration in iteration
/// order wins (the others are still reachable as separate Tier-2
/// rows).
#[derive(Debug, Default)]
pub struct PackageIndex {
    by_qualified: HashMap<String, PackageTarget>,
    /// Files indexed by their declared (outermost) namespace.
    files_by_package: HashMap<String, Vec<String>>,
    /// Every namespace that has at least one workspace member.
    known_namespaces: HashMap<String, ()>,
}

impl PackageIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_qualified: HashMap<String, PackageTarget> = HashMap::new();
        let mut files_by_package: HashMap<String, Vec<String>> = HashMap::new();
        let mut known_namespaces: HashMap<String, ()> = HashMap::new();

        for (path, _, facts) in per_file {
            if let Some(pkg) = facts.package.as_deref() {
                files_by_package
                    .entry(pkg.to_string())
                    .or_default()
                    .push(path.clone());
                // Every prefix of the namespace is a known namespace
                // (so `using A;` matches when only `A.B.C` is declared).
                let parts: Vec<&str> = pkg.split('.').collect();
                for i in 1..=parts.len() {
                    known_namespaces.insert(parts[..i].join("."), ());
                }
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
            files_by_package,
            known_namespaces,
        }
    }

    pub fn lookup(&self, qualified: &str) -> Option<&PackageTarget> {
        self.by_qualified.get(qualified)
    }

    pub fn files_in_package(&self, package: &str) -> &[String] {
        self.files_by_package
            .get(package)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn has_package(&self, package: &str) -> bool {
        self.known_namespaces.contains_key(package)
    }
}
