//! Per-file extraction of Kotlin class/object/function/property/import
//! facts and the workspace `PackageIndex` that maps fully-qualified
//! names to defining files.
//!
//! Kotlin packages are declared explicitly with `package com.foo.bar`
//! (not derived from the file path), so the file's qualified prefix
//! comes from a `package_header` node rather than `file_to_module`.

use std::collections::HashMap;

use tree_sitter::{Node, Parser};

/// Resolved symbol target: where it lives and its fully qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageTarget {
    pub path: String,
    pub qualified: String,
}

/// Per-file extracted Kotlin facts.
#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    /// Package qualified name (`com.foo.bar`) declared by `package …`.
    /// `None` when the file is in the root package or no package
    /// declaration was found.
    pub package: Option<String>,
    /// Class / object / interface / enum / companion-object definitions
    /// declared in this file.
    pub class_defs: Vec<ClassDef>,
    /// Function and method definitions (top-level functions, member
    /// functions, extension functions). Companion-object members are
    /// owned by `Container.Companion`.
    pub method_defs: Vec<MethodDef>,
    /// Const / top-level property definitions (workspace-resolvable
    /// constants — used by `find_subtypes` style lookup, but not load-
    /// bearing for dispatch).
    pub const_defs: Vec<ConstDef>,
    /// Direct heritage edges: `class Service : Base(), Greeter` →
    /// two edges, one with `is_constructor_invocation = true` (Base)
    /// and one with `false` (Greeter).
    pub base_edges: Vec<BaseEdge>,
    /// Every `import` declaration: each binding's local name → FQN.
    pub import_bindings: Vec<ImportBinding>,
    /// Type-position references emitted at base-name byte ranges (the
    /// span Tier-2 stores as `interface_byte_range`). One per resolved
    /// base + one per heritage user_type so `find_subtypes` /
    /// `find_supertypes` flip `kind_source` at the right site.
    pub type_refs: Vec<TypeRef>,
    /// Statically pinnable call sites.
    pub method_calls: Vec<MethodCall>,
}

#[derive(Debug, Clone)]
pub struct ClassDef {
    /// Fully qualified name including package + nesting
    /// (`com.foo.Service.Inner`).
    pub qualified: String,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodDef {
    /// Fully qualified method name (`com.foo.Service.fetch`,
    /// `com.foo.Service.Companion.create`, or `com.foo.helper` for
    /// top-level).
    pub qualified: String,
    /// Owner qualified name — class FQN, companion FQN, or file's
    /// package FQN for top-level functions.
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
    /// Qualified name of the subclass (`com.foo.Dog`).
    pub owner: String,
    /// Dotted parts of the base-class expression as written
    /// (`["Greeter"]`, `["pkg", "Base"]`). Generic args are already
    /// stripped at extraction time.
    pub parts: Vec<String>,
    /// True when the base appears as `Base()` (constructor
    /// invocation → `inherit`). False for bare `Greeter` →
    /// `implement`. Mirrors the Tier-2 heuristic exactly so this layer
    /// can re-derive the kind without re-parsing.
    pub is_constructor_invocation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportKind {
    /// `import com.foo.Bar` — `local = "Bar"`, `fqn = "com.foo.Bar"`.
    Plain,
    /// `import com.foo.Bar as B` — `local = "B"`, `fqn = "com.foo.Bar"`.
    Aliased,
    /// `import com.foo.*` — `local = "*"`, `fqn = "com.foo"` (the
    /// package itself; consumers expand on demand).
    Wildcard,
}

/// One binding produced by an `import` declaration.
#[derive(Debug, Clone)]
pub struct ImportBinding {
    pub kind: ImportKind,
    /// Short name introduced into the file's namespace. `*` for wildcard.
    pub local: String,
    /// Dotted FQN; for `Wildcard` this is the package the `*` selects.
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
    /// Byte range covering just the method-name identifier (same span
    /// the Tier-2 backend records on the resolved-callee site).
    pub byte_start: u32,
    pub byte_end: u32,
    /// Qualified name of the lexically enclosing class / object /
    /// companion-object (for `this.foo` / `super.foo`).
    pub lexical_class: Option<String>,
    /// Qualified name of the lexically enclosing function / method
    /// (used to deduplicate enclosing context — not load-bearing for
    /// dispatch).
    pub lexical_function: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallReceiver {
    /// `Cls.method()` / `pkg.Cls.method()` / `Companion.method()`. The
    /// dotted prefix as a chain of identifiers. The resolver decides
    /// between class-method dispatch and top-level / package lookup by
    /// looking the head up in the alias map and package index.
    Dotted { parts: Vec<String> },
    /// `this.method()` — receiver is `this`, resolved via the lexical
    /// class's MRO.
    ThisRef,
    /// `super.method()` — late-bound dispatch up the MRO.
    SuperRef,
    /// `foo()` — bare identifier callee (top-level function, imported
    /// function, or constructor invocation).
    Bare { name: String },
    /// Anything else (`obj.method()` where `obj` is a local variable,
    /// `arr[0].method()`, lambda invocations, etc.) — not resolvable
    /// at Tier-2.5.
    Unknown,
}

/// Parse a single Kotlin source blob and extract its facts.
#[must_use]
pub fn parse_file(source: &[u8]) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_kotlin_ng::LANGUAGE.into();
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
    /// active inside that container. Pushing a class adds the bare
    /// class name; pushing a function adds the function name.
    container_stack: Vec<ContainerFrame>,
}

#[derive(Debug, Clone)]
struct ContainerFrame {
    /// Bare segment name pushed onto the qualified chain.
    name: String,
    /// True when this frame is a class / object / interface / enum /
    /// companion_object (i.e. eligible to own methods).
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

    /// Build the qualified name for a leaf `name` declared inside the
    /// current container chain, prefixed by the file's package
    /// declaration.
    fn qualify_in_scope(&self, leaf: &str) -> String {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(p) = self.facts.package.as_deref() {
            if !p.is_empty() {
                parts.push(p);
            }
        }
        for f in &self.container_stack {
            parts.push(&f.name);
        }
        parts.push(leaf);
        parts.join(".")
    }

    /// The innermost class frame's qualified name (used for `this`/
    /// `super` and for method owners).
    fn current_class(&self) -> Option<String> {
        // Walk back through the stack collecting names until we find a
        // class frame; the qualified prefix is package + all frames up
        // to and including that class.
        let mut prefix: Vec<&str> = Vec::new();
        if let Some(p) = self.facts.package.as_deref() {
            if !p.is_empty() {
                prefix.push(p);
            }
        }
        let mut last_class_idx: Option<usize> = None;
        for (i, frame) in self.container_stack.iter().enumerate() {
            prefix.push(&frame.name);
            if frame.is_class {
                last_class_idx = Some(i);
            }
        }
        let cut = last_class_idx?;
        // Re-build the prefix up to `cut` inclusive.
        let mut out: Vec<&str> = Vec::new();
        if let Some(p) = self.facts.package.as_deref() {
            if !p.is_empty() {
                out.push(p);
            }
        }
        for frame in &self.container_stack[..=cut] {
            out.push(&frame.name);
        }
        Some(out.join("."))
    }

    fn current_function(&self) -> Option<String> {
        // The innermost non-class frame, if any.
        let mut last_fn_idx: Option<usize> = None;
        for (i, frame) in self.container_stack.iter().enumerate() {
            if !frame.is_class {
                last_fn_idx = Some(i);
            }
        }
        let cut = last_fn_idx?;
        let mut out: Vec<&str> = Vec::new();
        if let Some(p) = self.facts.package.as_deref() {
            if !p.is_empty() {
                out.push(p);
            }
        }
        for frame in &self.container_stack[..=cut] {
            out.push(&frame.name);
        }
        Some(out.join("."))
    }

    /// Owner FQN for a top-level (no enclosing class) callable: the
    /// file's package, or empty when the file has no package (root).
    fn top_level_owner(&self) -> String {
        self.facts.package.clone().unwrap_or_default()
    }

    fn walk(&mut self, node: Node<'_>) {
        if node.is_error() || node.is_missing() {
            // Continue walking children: tree-sitter recovers, and we
            // still want facts from intact siblings.
            self.descend_children(node);
            return;
        }
        match node.kind() {
            "package_header" => {
                if let Some(qid) = find_direct_child(node, "qualified_identifier")
                    .or_else(|| find_direct_child(node, "identifier"))
                {
                    let text = self.text(qid).trim().to_string();
                    if !text.is_empty() {
                        self.facts.package = Some(text);
                    }
                }
                return;
            }
            "import" => {
                self.emit_import(node);
                return;
            }
            "class_declaration" | "object_declaration" | "companion_object" => {
                self.enter_class(node);
                return;
            }
            "function_declaration" => {
                self.enter_function(node);
                return;
            }
            "property_declaration" => {
                self.emit_property(node);
                return;
            }
            "call_expression" => {
                self.emit_call(node);
                // Fall through so we visit nested calls inside the
                // arguments.
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

    fn enter_class(&mut self, node: Node<'_>) {
        let name = match class_name(node, self.source) {
            Some(n) => n,
            None => {
                self.descend_children(node);
                return;
            }
        };
        let qualified = self.qualify_in_scope(&name);

        // Record heritage edges + type references at the base-name
        // byte ranges. Match the Tier-2 backend's analyzer.rs walk so
        // the join site aligns.
        if let Some(specifiers) = find_direct_child(node, "delegation_specifiers") {
            let mut cursor = specifiers.walk();
            for specifier in specifiers.named_children(&mut cursor) {
                if specifier.kind() != "delegation_specifier" {
                    continue;
                }
                let (base_node, is_ctor) = if let Some(invocation) =
                    find_descendant(specifier, "constructor_invocation")
                {
                    (find_direct_child(invocation, "user_type"), true)
                } else {
                    (find_descendant(specifier, "user_type"), false)
                };
                let Some(base_node) = base_node else { continue };
                let Some(parts) = user_type_parts(base_node, self.source) else {
                    continue;
                };
                let r = base_node.byte_range();
                self.facts.base_edges.push(BaseEdge {
                    owner: qualified.clone(),
                    parts: parts.clone(),
                    is_constructor_invocation: is_ctor,
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
        self.container_stack.push(ContainerFrame {
            name,
            is_class: true,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn enter_function(&mut self, node: Node<'_>) {
        let Some(name_node) = find_field(node, "name") else {
            self.descend_children(node);
            return;
        };
        let name_text = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name_text);
        let owner = if let Some(class) = self.current_class() {
            class
        } else {
            self.top_level_owner()
        };
        self.facts.method_defs.push(MethodDef {
            qualified: qualified.clone(),
            owner,
            name: name_text.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        self.container_stack.push(ContainerFrame {
            name: name_text,
            is_class: false,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn emit_property(&mut self, node: Node<'_>) {
        // Properties / consts only at declaration sites (top level or
        // inside a class / object / companion / interface body). Locals
        // inside a function body are skipped — same rule as the Tier-1
        // backend.
        let parent_kind = node.parent().map(|p| p.kind()).unwrap_or("");
        if !matches!(
            parent_kind,
            "source_file" | "class_body" | "enum_class_body"
        ) {
            // Still descend to capture nested calls inside initializers.
            self.descend_children(node);
            return;
        }
        let Some(name) = property_name(node, self.source) else {
            self.descend_children(node);
            return;
        };
        let owner = if let Some(class) = self.current_class() {
            class
        } else {
            self.top_level_owner()
        };
        let qualified = self.qualify_in_scope(&name);
        self.facts.const_defs.push(ConstDef {
            qualified,
            owner,
            name,
        });
        self.descend_children(node);
    }

    fn emit_call(&mut self, node: Node<'_>) {
        // tree-sitter-kotlin-ng `call_expression` shape: `callee
        // value_arguments`. `callee` is the first named child.
        let Some(callee) = node.named_child(0) else {
            return;
        };
        let (receiver, method, name_node) = classify_call(callee, self.source);
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

    fn emit_import(&mut self, node: Node<'_>) {
        // tree-sitter-kotlin-ng shape: `import qualified_identifier (as
        // identifier)? (.*)?`. Same parsing rule the Tier-1 backend
        // uses, but we also track byte ranges and aliases.
        let path_node = find_direct_child(node, "qualified_identifier")
            .or_else(|| find_direct_child(node, "identifier"));
        let Some(path_node) = path_node else { return };
        let path_text = self.text(path_node).trim().to_string();
        if path_text.is_empty() {
            return;
        }
        let r = path_node.byte_range();
        let wildcard = has_direct_token(node, "*");
        if wildcard {
            self.facts.import_bindings.push(ImportBinding {
                kind: ImportKind::Wildcard,
                local: "*".to_string(),
                fqn: path_text,
                site_byte_start: r.start as u32,
                site_byte_end: r.end as u32,
            });
            return;
        }
        // Alias (`import a.b.C as D`) appears as a direct `identifier`
        // child after the `as` keyword, only when the path is a
        // qualified_identifier (so the alias identifier is unambiguous).
        let alias = if path_node.kind() == "qualified_identifier" {
            find_direct_child(node, "identifier").map(|n| self.text(n).to_string())
        } else {
            None
        };
        let (kind, local) = if let Some(a) = alias {
            (ImportKind::Aliased, a)
        } else {
            let leaf = path_text
                .rsplit('.')
                .next()
                .unwrap_or(&path_text)
                .to_string();
            (ImportKind::Plain, leaf)
        };
        self.facts.import_bindings.push(ImportBinding {
            kind,
            local,
            fqn: path_text,
            site_byte_start: r.start as u32,
            site_byte_end: r.end as u32,
        });
    }
}

fn class_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    if let Some(name) = find_field(node, "name") {
        let text = std::str::from_utf8(&source[name.byte_range()]).ok()?;
        return Some(text.to_string());
    }
    // companion_object name is optional; default `Companion`.
    if node.kind() == "companion_object" {
        return Some("Companion".to_string());
    }
    None
}

fn property_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let decl = find_direct_child(node, "variable_declaration")?;
    let name = find_direct_child(decl, "identifier")?;
    let text = std::str::from_utf8(&source[name.byte_range()]).ok()?;
    Some(text.to_string())
}

/// Strip generic args and return identifier parts. `pkg.Container<T>`
/// → `["pkg", "Container"]`, `Foo` → `["Foo"]`.
pub(crate) fn user_type_parts(node: Node<'_>, source: &[u8]) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" {
            let text = std::str::from_utf8(&source[child.byte_range()]).ok()?;
            parts.push(text.to_string());
        }
    }
    if parts.is_empty() { None } else { Some(parts) }
}

/// Classify a `call_expression` callee. Returns `(receiver,
/// method_name, name_node)`. `name_node` is the identifier whose byte
/// range Tier-2 records on the resolved-callee site.
fn classify_call<'tree>(
    callee: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    match callee.kind() {
        "identifier" => {
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
    // tree-sitter-kotlin-ng shape: `navigation_expression` is left-
    // recursive. `a.b.c` parses as
    //   navigation_expression(
    //     navigation_expression(identifier(a), identifier(b)),
    //     identifier(c)
    //   )
    // The final child is the member identifier; everything before it
    // is the receiver chain. We flatten the chain (recursing into
    // nested `navigation_expression` receivers) so the resolver sees
    // a single dotted path regardless of nesting depth.
    let mut idents: Vec<Node<'tree>> = Vec::new();
    let mut saw_this = false;
    let mut saw_super = false;
    let mut unknown_receiver = false;

    let member = match nav.named_child(nav.named_child_count().saturating_sub(1)) {
        Some(m) if m.kind() == "identifier" => m,
        _ => return (CallReceiver::Unknown, None, nav),
    };

    // Walk all named children except the last (the member). For each
    // such child, flatten it into the receiver identifier list.
    let last_idx = nav.named_child_count().saturating_sub(1);
    for i in 0..last_idx {
        let Some(child) = nav.named_child(i) else {
            continue;
        };
        match child.kind() {
            "identifier" => idents.push(child),
            "this_expression" => saw_this = true,
            "super_expression" => saw_super = true,
            "navigation_expression" => {
                flatten_navigation(child, &mut idents, &mut saw_this, &mut saw_super);
            }
            _ => {
                // Receiver shape we can't pin (e.g. literal, call
                // result, parenthesized expression) — bail to Unknown
                // rather than emit a partial dotted chain that would
                // mis-resolve.
                unknown_receiver = true;
            }
        }
    }

    let method = std::str::from_utf8(&source[member.byte_range()])
        .unwrap_or("")
        .to_string();
    if method.is_empty() {
        return (CallReceiver::Unknown, None, member);
    }

    let receiver_idents: Vec<String> = idents
        .iter()
        .map(|n| {
            std::str::from_utf8(&source[n.byte_range()])
                .unwrap_or("")
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect();

    let receiver = if unknown_receiver {
        CallReceiver::Unknown
    } else if saw_super && receiver_idents.is_empty() {
        CallReceiver::SuperRef
    } else if saw_this && receiver_idents.is_empty() {
        CallReceiver::ThisRef
    } else if !receiver_idents.is_empty() {
        CallReceiver::Dotted {
            parts: receiver_idents,
        }
    } else {
        CallReceiver::Unknown
    };
    (receiver, Some(method), member)
}

/// Flatten a nested `navigation_expression` receiver into the parent's
/// identifier list. Sets `saw_this` / `saw_super` if the chain begins
/// with a `this_expression` / `super_expression`.
fn flatten_navigation<'tree>(
    nav: Node<'tree>,
    idents: &mut Vec<Node<'tree>>,
    saw_this: &mut bool,
    saw_super: &mut bool,
) {
    let count = nav.named_child_count();
    for i in 0..count {
        let Some(child) = nav.named_child(i) else {
            continue;
        };
        match child.kind() {
            "identifier" => idents.push(child),
            "this_expression" => *saw_this = true,
            "super_expression" => *saw_super = true,
            "navigation_expression" => flatten_navigation(child, idents, saw_this, saw_super),
            _ => {}
        }
    }
}

fn find_direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

fn find_field<'a>(node: Node<'a>, field: &str) -> Option<Node<'a>> {
    node.child_by_field_name(field)
}

fn find_descendant<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == kind {
            return Some(child);
        }
        if let Some(found) = find_descendant(child, kind) {
            return Some(found);
        }
    }
    None
}

fn has_direct_token(node: Node<'_>, token: &str) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor).any(|c| c.kind() == token)
}

// ─── workspace-wide package index ─────────────────────────────────────────

/// Workspace symbol index: every class / object / interface / function
/// /constant's FQN → defining file. Kotlin uses FQNs that are explicit
/// (driven by `package` headers), so we *don't* index dotted suffixes
/// of class qualifieds — but we do index dotted suffixes of *imports*
/// because `import x.y.*` plus a use of `Y.method()` should still find
/// `x.y.Y` when the user wrote the bare class name.
#[derive(Debug, Default)]
pub struct PackageIndex {
    /// `(path, qualified) → target`. Path-aware so cross-file collisions
    /// (the same short class name declared in different files) stay
    /// distinguishable — no first-hit anti-pattern, no HashMap iteration
    /// order leaking into resolution results.
    by_file_qualified: HashMap<(String, String), PackageTarget>,
    /// Qualified → list of declaring (path, qualified) keys, sorted by
    /// path. Lets `lookup_all` / `lookup_unique` answer "where does
    /// `com.foo.Helper` live?" deterministically when the path isn't
    /// known up front.
    by_qualified_index: HashMap<String, Vec<(String, String)>>,
    /// Files indexed by their declared package. Lets the require-graph
    /// answer "does this `import com.foo.*` point at a workspace
    /// package?" without re-walking facts.
    files_by_package: HashMap<String, Vec<String>>,
}

impl PackageIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_file_qualified: HashMap<(String, String), PackageTarget> = HashMap::new();
        let mut by_qualified_index: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut files_by_package: HashMap<String, Vec<String>> = HashMap::new();

        for (path, _, facts) in per_file {
            if let Some(pkg) = facts.package.as_deref() {
                files_by_package
                    .entry(pkg.to_string())
                    .or_default()
                    .push(path.clone());
            }
            let mut insert = |qualified: &str| {
                let key = (path.clone(), qualified.to_string());
                by_file_qualified
                    .entry(key.clone())
                    .or_insert(PackageTarget {
                        path: path.clone(),
                        qualified: qualified.to_string(),
                    });
                by_qualified_index
                    .entry(qualified.to_string())
                    .or_default()
                    .push(key);
            };
            for class in &facts.class_defs {
                insert(&class.qualified);
            }
            for m in &facts.method_defs {
                insert(&m.qualified);
            }
            for c in &facts.const_defs {
                insert(&c.qualified);
            }
        }

        // Stabilize multi-hit ordering so `lookup_all` is deterministic
        // independent of file-walk order. Per-key dedup keeps the same
        // (path, qualified) pair from appearing twice when a definition
        // got inserted via multiple def buckets.
        for keys in by_qualified_index.values_mut() {
            keys.sort();
            keys.dedup();
        }

        Self {
            by_file_qualified,
            by_qualified_index,
            files_by_package,
        }
    }

    /// Path-aware exact lookup. Returns the symbol declared in `path`
    /// under the given qualified name, or `None` if that specific file
    /// does not declare it.
    pub fn lookup_in_file(&self, path: &str, qualified: &str) -> Option<&PackageTarget> {
        self.by_file_qualified
            .get(&(path.to_string(), qualified.to_string()))
    }

    /// All workspace targets matching `qualified`, sorted by path for
    /// deterministic iteration. Used by callers that want to scan
    /// collisions explicitly.
    pub fn lookup_all(&self, qualified: &str) -> Vec<&PackageTarget> {
        let Some(keys) = self.by_qualified_index.get(qualified) else {
            return Vec::new();
        };
        keys.iter()
            .filter_map(|k| self.by_file_qualified.get(k))
            .collect()
    }

    /// Path-agnostic lookup that adopts a target only when exactly one
    /// workspace file declares it. Replaces the old first-hit `lookup`
    /// pattern: collisions return `None` so dispatch falls through to
    /// the path-aware lookup rather than silently picking a winner.
    pub fn lookup_unique(&self, qualified: &str) -> Option<&PackageTarget> {
        let keys = self.by_qualified_index.get(qualified)?;
        if keys.len() == 1 {
            self.by_file_qualified.get(&keys[0])
        } else {
            None
        }
    }

    /// Files declaring this exact package (for wildcard-import
    /// resolution).
    pub fn files_in_package(&self, package: &str) -> &[String] {
        self.files_by_package
            .get(package)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// `true` when at least one workspace file declares this package.
    pub fn has_package(&self, package: &str) -> bool {
        self.files_by_package.contains_key(package)
    }
}
