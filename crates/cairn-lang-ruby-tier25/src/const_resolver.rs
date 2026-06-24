//! Per-file extraction of Ruby class/module/constant facts and a workspace
//! `ConstIndex` that resolves lexical → ancestor → top-level → autoload
//! constant lookups.

use std::collections::HashMap;

use tree_sitter::{Node, Parser, Tree};

use crate::mro::Mro;

/// Resolved constant: where it lives and its fully qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstTarget {
    pub path: String,
    pub qualified: String,
}

/// Per-file extracted facts. Stored on the analyzer side and consumed by
/// the resolver passes.
#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    /// Class / module definitions in this file, in lexical order.
    pub class_defs: Vec<ClassDef>,
    /// Method definitions in this file (instance + singleton).
    pub method_defs: Vec<MethodDef>,
    /// Mixin edges: (qualified class, mixin kind, mixed-in const path).
    pub mixins: Vec<MixinEdge>,
    /// Plain constant references (non-declaration sites).
    pub const_refs: Vec<ConstRef>,
    /// Method-call sites (resolver decides which ones are dispatchable).
    pub method_calls: Vec<MethodCall>,
    /// `autoload :Foo, "path/to/foo"` declarations keyed by qualified name.
    pub autoloads: Vec<(String, String)>,
    /// `require` / `require_relative` literal arguments with byte ranges.
    pub requires: Vec<RequireSite>,
}

#[derive(Debug, Clone)]
pub struct ClassDef {
    pub qualified: String,
    pub superclass: Option<Vec<String>>, // parts of `< Foo::Bar`
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodDef {
    pub qualified: String,
    pub owner: String, // qualified class/module owning the method
    pub name: String,
    pub singleton: bool, // self.foo / def Klass.foo / inside `class << self`
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixinKind {
    Include,
    Extend,
    Prepend,
}

#[derive(Debug, Clone)]
pub struct MixinEdge {
    pub owner: String,
    pub kind: MixinKind,
    pub module_parts: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ConstRef {
    pub parts: Vec<String>,
    pub lexical_scope: Vec<String>, // enclosing modules/classes at this site
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodCall {
    /// `Foo.bar` / `self.bar` / `super` / bare `bar()`.
    pub receiver: CallReceiver,
    pub method: String,
    pub byte_start: u32,
    pub byte_end: u32,
    /// Lexical class at the call site, used for `self.X` and `super`.
    pub lexical_scope: Vec<String>,
    /// True when site is inside `class << self`, def self.X, or a singleton
    /// def — affects how `self` is interpreted (singleton vs instance).
    pub in_singleton_context: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallReceiver {
    None,
    Self_,
    Super,
    Const(Vec<String>),
    /// Receiver is an arbitrary expression — unresolvable by Tier-2.5.
    Unknown,
}

#[derive(Debug, Clone)]
pub struct RequireSite {
    pub kind: RequireKind,
    pub literal: String,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequireKind {
    Require,
    RequireRelative,
    /// `load "path/to/file.rb"` — Ruby's `load` always takes a path
    /// literal (typically with `.rb`), resolved at runtime via
    /// `$LOAD_PATH`. For workspace-local resolution we treat it like
    /// `require_relative` when the literal is path-shaped, otherwise
    /// fall back to the same workspace lookup as `require`.
    Load,
    /// `autoload :Foo, "path/to/foo"` — the path component (second arg)
    /// is the import target. Resolution semantics mirror `require`
    /// (workspace lookup with `.rb` suffix); the symbol-name component
    /// is recorded separately in `autoloads` for const resolution.
    Autoload,
}

/// Parse a single Ruby source blob and extract its facts. Returns `None`
/// if the parser cannot construct a tree (we leave such files unresolved).
#[must_use]
pub fn parse_file(source: &[u8]) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_ruby::LANGUAGE.into();
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
    scope: Vec<String>,
    singleton_stack: Vec<bool>,
}

impl<'a> Visitor<'a> {
    fn new(source: &'a [u8]) -> Self {
        Self {
            source,
            facts: FileConstFacts::default(),
            scope: Vec::new(),
            singleton_stack: vec![false],
        }
    }

    fn current_qualified(&self) -> Option<String> {
        if self.scope.is_empty() {
            None
        } else {
            Some(self.scope.join("::"))
        }
    }

    fn text(&self, node: Node<'_>) -> &str {
        std::str::from_utf8(&self.source[node.byte_range()]).unwrap_or("")
    }

    fn walk(&mut self, node: Node<'_>) {
        // Walk the entire tree recursively, opening lexical scopes for
        // class/module/singleton_class definitions.
        match node.kind() {
            "class" | "module" => {
                let opened = self.open_class_or_module(node);
                if let Some((kind, _name)) = opened {
                    self.descend_children(node);
                    self.close_scope(kind);
                    return;
                }
            }
            "singleton_class" => {
                self.singleton_stack.push(true);
                self.descend_children(node);
                self.singleton_stack.pop();
                return;
            }
            "method" => {
                self.emit_method(node, false);
                self.descend_children(node);
                return;
            }
            "singleton_method" => {
                self.emit_method(node, true);
                self.descend_children(node);
                return;
            }
            "call" | "command" | "method_call" => {
                self.try_emit_mixin_or_autoload_or_require(node);
                self.emit_method_call(node);
            }
            "super" => {
                self.emit_super(node);
            }
            "constant" => {
                self.try_emit_const_ref(node);
            }
            "scope_resolution" => {
                self.try_emit_qualified_const_ref(node);
                // Children also visit individual `constant` nodes; we mark
                // the qualified site here and skip the inner pieces so we
                // don't double-count.
                return;
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

    fn open_class_or_module(&mut self, node: Node<'_>) -> Option<(&'static str, String)> {
        let name_node = node.child_by_field_name("name")?;
        let name_parts = parts_from_const_or_scope(name_node, self.source)?;
        // `class Foo::Bar` reopens at fully-qualified name irrespective of
        // surrounding scope; otherwise nest under the current scope.
        let qualified = if name_parts.len() > 1 || self.scope.is_empty() {
            name_parts.join("::")
        } else {
            let mut full = self.scope.clone();
            full.extend(name_parts.iter().cloned());
            full.join("::")
        };
        let superclass = node
            .child_by_field_name("superclass")
            .and_then(|sc| sc.child(1).or(Some(sc)))
            .and_then(|sc| parts_from_const_or_scope(sc, self.source));

        self.facts.class_defs.push(ClassDef {
            qualified: qualified.clone(),
            superclass,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });

        // For lexical-scope purposes track the qualified path as individual
        // segments so nested lookups join with `::` correctly.
        let pieces: Vec<String> = qualified.split("::").map(str::to_string).collect();
        self.scope.extend(pieces.iter().cloned());
        self.singleton_stack.push(false);
        Some((
            if node.kind() == "class" {
                "class"
            } else {
                "module"
            },
            qualified,
        ))
    }

    fn close_scope(&mut self, _kind: &str) {
        // Pop back to the parent scope. We pushed `qualified.split("::")`
        // pieces in `open_class_or_module`; record how many to pop by
        // recomputing the last class_def's piece count.
        if let Some(last) = self.facts.class_defs.last() {
            let n = last.qualified.split("::").count();
            for _ in 0..n {
                self.scope.pop();
            }
        }
        self.singleton_stack.pop();
    }

    fn emit_method(&mut self, node: Node<'_>, singleton: bool) {
        let Some(name_node) = node.child_by_field_name("name") else {
            return;
        };
        let name = self.text(name_node).to_string();
        let owner = self.current_qualified().unwrap_or_default();
        let effective_singleton = singleton || *self.singleton_stack.last().unwrap_or(&false);
        let sep = if effective_singleton { "." } else { "#" };
        let qualified = if owner.is_empty() {
            name.clone()
        } else {
            format!("{owner}{sep}{name}")
        };
        self.facts.method_defs.push(MethodDef {
            qualified,
            owner,
            name,
            singleton: effective_singleton,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }

    fn emit_super(&mut self, node: Node<'_>) {
        // Find the enclosing `method` (or `singleton_method`) to recover the
        // method name `super` is dispatching against. Walk up.
        let Some(method_name) = enclosing_method_name(node, self.source) else {
            return;
        };
        let in_singleton = *self.singleton_stack.last().unwrap_or(&false);
        self.facts.method_calls.push(MethodCall {
            receiver: CallReceiver::Super,
            method: method_name,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
            lexical_scope: self.scope.clone(),
            in_singleton_context: in_singleton,
        });
    }

    fn try_emit_mixin_or_autoload_or_require(&mut self, node: Node<'_>) {
        let method_node = node.child_by_field_name("method");
        let receiver = node.child_by_field_name("receiver");
        // Skip qualified calls — `Foo.include` isn't a mixin DSL call.
        if receiver.is_some() {
            return;
        }
        let Some(method_node) = method_node else {
            return;
        };
        let method = self.text(method_node);
        let args = node.child_by_field_name("arguments");

        match method {
            "include" | "extend" | "prepend" => {
                let kind = match method {
                    "include" => MixinKind::Include,
                    "extend" => MixinKind::Extend,
                    _ => MixinKind::Prepend,
                };
                let Some(owner) = self.current_qualified() else {
                    return;
                };
                if let Some(args) = args {
                    let mut cursor = args.walk();
                    for child in args.named_children(&mut cursor) {
                        if let Some(parts) = parts_from_const_or_scope(child, self.source) {
                            self.facts.mixins.push(MixinEdge {
                                owner: owner.clone(),
                                kind,
                                module_parts: parts,
                            });
                        }
                    }
                }
            }
            "autoload" => {
                // autoload(:Foo, "path/to/foo") — two roles in one call:
                // (1) declares a deferred constant binding (recorded in
                // `autoloads` so const-resolution can fall back to it),
                // and (2) is an import edge from the path literal to its
                // workspace file (recorded in `requires` so the
                // require-graph can pin a `resolutions` row at the same
                // byte range as the Tier-2 `imports` row).
                let Some(args) = args else { return };
                let mut cursor = args.walk();
                let kids: Vec<Node<'_>> = args.named_children(&mut cursor).collect();
                if kids.len() < 2 {
                    return;
                }
                let sym = self.text(kids[0]);
                let sym_name = sym.trim_start_matches(':');
                let path_node = kids[1];
                let Some(lit) = string_literal(path_node, self.source) else {
                    return;
                };
                let qualified = match self.current_qualified() {
                    Some(scope) => format!("{scope}::{sym_name}"),
                    None => sym_name.to_string(),
                };
                self.facts.autoloads.push((qualified, lit.clone()));
                if let Some((byte_start, byte_end)) = string_content_range(path_node) {
                    self.facts.requires.push(RequireSite {
                        kind: RequireKind::Autoload,
                        literal: lit,
                        byte_start,
                        byte_end,
                    });
                }
            }
            "require" | "require_relative" | "load" => {
                let Some(args) = args else { return };
                let mut cursor = args.walk();
                let first = args.named_children(&mut cursor).next();
                let Some(first) = first else { return };
                let Some(lit) = string_literal(first, self.source) else {
                    return;
                };
                // Pin the resolution site at the string_content range
                // (text between the quotes), matching what the Ruby
                // syntactic pass emits as `ImportFact::byte_range`.
                // This is what `find_imports` joins against in schema
                // v9 — using `first.byte_range()` would include the
                // quotes and miss every Tier-2 import row.
                let (byte_start, byte_end) = match string_content_range(first) {
                    Some(r) => (r.0, r.1),
                    None => return,
                };
                let kind = match method {
                    "require" => RequireKind::Require,
                    "require_relative" => RequireKind::RequireRelative,
                    _ => RequireKind::Load,
                };
                self.facts.requires.push(RequireSite {
                    kind,
                    literal: lit,
                    byte_start,
                    byte_end,
                });
            }
            _ => {}
        }
    }

    fn emit_method_call(&mut self, node: Node<'_>) {
        let Some(method_node) = node.child_by_field_name("method") else {
            return;
        };
        let method = self.text(method_node).to_string();
        // Ignore the mixin/require/autoload DSLs — already recorded.
        if matches!(
            method.as_str(),
            "include" | "extend" | "prepend" | "autoload" | "require" | "require_relative" | "load"
        ) {
            return;
        }
        let receiver = match node.child_by_field_name("receiver") {
            None => CallReceiver::None,
            Some(r) => match r.kind() {
                "self" => CallReceiver::Self_,
                "super" => CallReceiver::Super,
                "constant" | "scope_resolution" => parts_from_const_or_scope(r, self.source)
                    .map(CallReceiver::Const)
                    .unwrap_or(CallReceiver::Unknown),
                _ => CallReceiver::Unknown,
            },
        };
        let in_singleton = *self.singleton_stack.last().unwrap_or(&false);
        self.facts.method_calls.push(MethodCall {
            receiver,
            method,
            byte_start: method_node.start_byte() as u32,
            byte_end: method_node.end_byte() as u32,
            lexical_scope: self.scope.clone(),
            in_singleton_context: in_singleton,
        });
    }

    fn try_emit_const_ref(&mut self, node: Node<'_>) {
        // Skip declaration sites: the parent class/module/scope_resolution
        // surrounds the constant when it names the thing being declared.
        let Some(parent) = node.parent() else {
            return;
        };
        match parent.kind() {
            "class" | "module" | "scope_resolution" => return,
            _ => {}
        }
        // Skip the constant on the LHS of `Foo = ...`.
        if parent.kind() == "assignment" {
            if let Some(lhs) = parent.child_by_field_name("left") {
                if lhs.id() == node.id() {
                    return;
                }
            }
        }
        let parts = vec![self.text(node).to_string()];
        self.facts.const_refs.push(ConstRef {
            parts,
            lexical_scope: self.scope.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }

    fn try_emit_qualified_const_ref(&mut self, node: Node<'_>) {
        let Some(parent) = node.parent() else {
            return;
        };
        match parent.kind() {
            "class" | "module" => return,
            _ => {}
        }
        let Some(parts) = parts_from_const_or_scope(node, self.source) else {
            return;
        };
        if parts.is_empty() {
            return;
        }
        self.facts.const_refs.push(ConstRef {
            parts,
            lexical_scope: self.scope.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }
}

fn enclosing_method_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut cur = node.parent();
    while let Some(n) = cur {
        if n.kind() == "method" || n.kind() == "singleton_method" {
            let name_node = n.child_by_field_name("name")?;
            return Some(
                std::str::from_utf8(&source[name_node.byte_range()])
                    .ok()?
                    .to_string(),
            );
        }
        cur = n.parent();
    }
    None
}

fn parts_from_const_or_scope(node: Node<'_>, source: &[u8]) -> Option<Vec<String>> {
    match node.kind() {
        "constant" => Some(vec![
            std::str::from_utf8(&source[node.byte_range()])
                .ok()?
                .to_string(),
        ]),
        "scope_resolution" => {
            let mut parts: Vec<String> = Vec::new();
            walk_scope_parts(node, source, &mut parts);
            if parts.is_empty() { None } else { Some(parts) }
        }
        _ => None,
    }
}

fn walk_scope_parts(node: Node<'_>, source: &[u8], out: &mut Vec<String>) {
    if node.kind() == "constant" {
        if let Ok(s) = std::str::from_utf8(&source[node.byte_range()]) {
            out.push(s.to_string());
        }
        return;
    }
    if node.kind() == "scope_resolution" {
        if let Some(scope) = node.child_by_field_name("scope") {
            walk_scope_parts(scope, source, out);
        }
        if let Some(name) = node.child_by_field_name("name") {
            walk_scope_parts(name, source, out);
        }
    }
}

/// Byte range of the first `string_content` child of a `string` node —
/// the bytes between the quotes. Mirrors the Ruby syntactic pass so
/// the require-graph resolution sites line up with `imports.byte_*`.
fn string_content_range(node: Node<'_>) -> Option<(u32, u32)> {
    if node.kind() != "string" {
        return None;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "string_content" {
            return Some((child.start_byte() as u32, child.end_byte() as u32));
        }
    }
    None
}

fn string_literal(node: Node<'_>, source: &[u8]) -> Option<String> {
    if node.kind() != "string" {
        return None;
    }
    // Grab the content sandwiched between the quotes by collecting
    // string_content children.
    let mut out = String::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "string_content" {
            if let Ok(s) = std::str::from_utf8(&source[child.byte_range()]) {
                out.push_str(s);
            }
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

// ─── workspace-wide index ─────────────────────────────────────────────────

/// Workspace constant index: qualified name → defining file+symbol.
///
/// Populated from every parsed file's `class_defs` so a lookup like
/// "find module `Mixin` from file `foo.rb`" can resolve regardless of where
/// the definition lives. Lexical scopes shadow lower-priority scopes —
/// resolution walks from the innermost lexical scope outwards, then climbs
/// ancestors via [`Mro`], and finally falls back to `autoload`.
#[derive(Debug, Default)]
pub struct ConstIndex {
    by_qualified: HashMap<String, ConstTarget>,
}

impl ConstIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_qualified = HashMap::new();
        for (path, _, facts) in per_file {
            for class in &facts.class_defs {
                // First definition wins. Ruby allows reopening but we only
                // need any concrete file to resolve to.
                by_qualified
                    .entry(class.qualified.clone())
                    .or_insert(ConstTarget {
                        path: path.clone(),
                        qualified: class.qualified.clone(),
                    });
            }
        }
        Self { by_qualified }
    }

    pub fn get(&self, qualified: &str) -> Option<&ConstTarget> {
        self.by_qualified.get(qualified)
    }

    /// Resolve a constant reference. Walks: lexical scope → MRO of every
    /// enclosing class → top-level → autoload.
    pub fn resolve(
        &self,
        parts: &[String],
        lexical_scope: &[String],
        mro: &Mro,
        autoload_map: &HashMap<String, String>,
    ) -> Option<ConstTarget> {
        if parts.is_empty() {
            return None;
        }
        let joined = parts.join("::");

        // Qualified lookups starting with `::` or with multiple parts that
        // already match top-level resolve directly.
        if parts.len() > 1 {
            if let Some(t) = self.get(&joined) {
                return Some(t.clone());
            }
            // Try lexical-prefix interpretation: `Foo::Bar` could mean
            // `Current::Foo::Bar`.
            for k in (0..lexical_scope.len()).rev() {
                let prefix: Vec<&str> = lexical_scope[..=k].iter().map(String::as_str).collect();
                let candidate = format!("{}::{}", prefix.join("::"), joined);
                if let Some(t) = self.get(&candidate) {
                    return Some(t.clone());
                }
            }
            // Autoload by leading segment.
            if let Some(autoload) = self.try_autoload(&joined, autoload_map) {
                return Some(autoload);
            }
            return None;
        }

        // Single-part lookup: walk lexical → ancestors → top-level → autoload.
        let head = &parts[0];

        // 1. Lexical scope (innermost first).
        for k in (0..lexical_scope.len()).rev() {
            let prefix: Vec<&str> = lexical_scope[..=k].iter().map(String::as_str).collect();
            let candidate = format!("{}::{}", prefix.join("::"), head);
            if let Some(t) = self.get(&candidate) {
                return Some(t.clone());
            }
        }

        // 2. Ancestors via MRO for the innermost lexical class.
        if !lexical_scope.is_empty() {
            let owner = lexical_scope.join("::");
            for ancestor in mro.ancestors(&owner) {
                let candidate = format!("{ancestor}::{head}");
                if let Some(t) = self.get(&candidate) {
                    return Some(t.clone());
                }
            }
        }

        // 3. Top-level.
        if let Some(t) = self.get(head) {
            return Some(t.clone());
        }

        // 4. Autoload.
        if let Some(t) = self.try_autoload(head, autoload_map) {
            return Some(t);
        }

        None
    }

    fn try_autoload(
        &self,
        qualified: &str,
        autoload_map: &HashMap<String, String>,
    ) -> Option<ConstTarget> {
        let path = autoload_map.get(qualified)?;
        Some(ConstTarget {
            path: path.clone(),
            qualified: qualified.to_string(),
        })
    }
}

#[allow(dead_code)]
fn _force_tree_lifetime_use(_: Tree) {}
