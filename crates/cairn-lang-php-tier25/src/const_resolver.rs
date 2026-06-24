//! Per-file extraction of PHP class/interface/trait/enum/const facts and
//! a workspace `ConstIndex` that resolves `\Foo\Bar` lookups through the
//! file's `namespace` + `use` import chain.

use std::collections::HashMap;

use tree_sitter::{Node, Parser};

/// Resolved type or constant: where it lives and its fully qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstTarget {
    pub path: String,
    pub qualified: String,
}

/// Per-file extracted facts.
#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    /// Active `namespace Foo\Bar;` prefix (None at the unbraced
    /// top-level of a global-namespace file). When the file uses the
    /// braced form, this captures the *first* such prefix — at Stage 1
    /// we don't model multi-namespace files (extremely rare; PSR-12
    /// discourages them).
    pub namespace: Option<String>,
    /// Class / interface / trait / enum definitions.
    pub class_defs: Vec<ClassDef>,
    /// Method definitions (including `static`).
    pub method_defs: Vec<MethodDef>,
    /// Heritage edges: extends + implements + trait `use`.
    pub mixins: Vec<MixinEdge>,
    /// `use Foo\Bar;` / `use Foo\Bar as Baz;` / `use function ...` —
    /// recorded for alias resolution and for the require-graph.
    pub use_imports: Vec<UseImport>,
    /// Plain type references (`Foo`, `\Foo\Bar`, etc.) outside
    /// declarations and `use` statements.
    pub const_refs: Vec<ConstRef>,
    /// Method-call sites the resolver might want to pin.
    pub method_calls: Vec<MethodCall>,
}

#[derive(Debug, Clone)]
pub struct ClassDef {
    /// Fully qualified (with namespace), e.g. `App\Models\Widget`.
    pub qualified: String,
    pub byte_start: u32,
    pub byte_end: u32,
}

/// Heritage clause entry: parts + byte range of the name token.
#[derive(Debug, Clone)]
struct HeritageName {
    parts: Vec<String>,
    absolute: bool,
    byte_start: u32,
    byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodDef {
    pub qualified: String,
    pub owner: String,
    pub name: String,
    pub is_static: bool,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixinKind {
    Extends,
    Implements,
    TraitUse,
}

#[derive(Debug, Clone)]
pub struct MixinEdge {
    pub owner: String,
    pub kind: MixinKind,
    pub module_parts: Vec<String>,
    /// True when the source name began with a leading `\`, meaning the
    /// segments are already namespace-absolute.
    pub absolute: bool,
}

#[derive(Debug, Clone)]
pub struct UseImport {
    /// What the importer refers to it as in the rest of the file
    /// (`use Foo\Bar as Baz;` → `Baz`; `use Foo\Bar;` → `Bar`).
    pub alias: String,
    /// Fully qualified target, without leading `\`.
    pub qualified: String,
    /// Byte range of the imported path token (covering span for the
    /// group form). Mirrors `ImportFact::byte_range` so the
    /// require-graph row lands on the same site `find_imports` joins.
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct ConstRef {
    pub parts: Vec<String>,
    /// True when source began with `\` (absolute reference).
    pub absolute: bool,
    /// File's `namespace` prefix at this site (None for global ns).
    pub namespace: Option<String>,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct MethodCall {
    pub receiver: CallReceiver,
    pub method: String,
    pub byte_start: u32,
    pub byte_end: u32,
    /// File's namespace at the call site.
    pub namespace: Option<String>,
    /// Qualified name of the lexically enclosing class (for `self::`,
    /// `parent::`, `static::` resolution).
    pub lexical_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallReceiver {
    /// `Foo::bar()` or `\Foo\Bar::baz()` — receiver is a class name.
    Const { parts: Vec<String>, absolute: bool },
    /// `self::bar()`.
    SelfClass,
    /// `parent::bar()`.
    Parent,
    /// `static::bar()` — late-static dispatch.
    StaticClass,
    /// Anything else (member access, variable receiver, etc.) — not
    /// resolvable at Tier-2.5.
    Unknown,
}

/// Parse a single PHP source blob and extract its facts.
#[must_use]
pub fn parse_file(source: &[u8]) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_php::LANGUAGE_PHP.into();
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
    /// Active namespace prefix (stack so braced namespaces nest
    /// correctly, though Stage 1 keeps the first emitted prefix in
    /// `facts.namespace`).
    namespace_stack: Vec<Option<String>>,
    /// Lexical class container stack (qualified names).
    container_stack: Vec<String>,
}

impl<'a> Visitor<'a> {
    fn new(source: &'a [u8]) -> Self {
        Self {
            source,
            facts: FileConstFacts::default(),
            namespace_stack: vec![None],
            container_stack: Vec::new(),
        }
    }

    fn current_namespace(&self) -> Option<&str> {
        self.namespace_stack.last().and_then(|n| n.as_deref())
    }

    fn text(&self, node: Node<'_>) -> &str {
        std::str::from_utf8(&self.source[node.byte_range()]).unwrap_or("")
    }

    fn walk(&mut self, node: Node<'_>) {
        if node.is_error() || node.is_missing() {
            return;
        }
        match node.kind() {
            "namespace_definition" => {
                self.enter_namespace(node);
                return;
            }
            "namespace_use_declaration" => {
                self.emit_use(node);
                // No need to descend; the children are only path
                // fragments.
                return;
            }
            "class_declaration"
            | "interface_declaration"
            | "trait_declaration"
            | "enum_declaration" => {
                self.enter_container(node);
                return;
            }
            "method_declaration" => {
                self.emit_method(node);
                self.descend_children(node);
                return;
            }
            "use_declaration" => {
                // Trait `use` inside a class body.
                self.emit_trait_use(node);
                // Fall through — children are name nodes already
                // captured; no nested decls expected here.
                return;
            }
            "scoped_call_expression" => {
                self.emit_scoped_call(node);
                self.descend_children(node);
                return;
            }
            "name" | "qualified_name" => {
                // Plain type-name references (e.g. `Foo::CONST`,
                // `new Foo()`, type hints). Only emit refs whose
                // parent context is a "looks like a type reference"
                // shape. We are conservative — heritage edges and use
                // statements are handled separately so we skip refs
                // whose direct parent is a heritage clause / use
                // clause / namespace decl.
                self.try_emit_type_ref(node);
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
        let prefix = child_by_field(node, "name")
            .map(|n| self.text(n).to_string())
            .filter(|s| !s.is_empty());
        if self.facts.namespace.is_none() {
            self.facts.namespace = prefix.clone();
        }
        if let Some(body) = child_by_field(node, "body") {
            // Braced form: prefix scopes the block.
            self.namespace_stack.push(prefix);
            self.descend_children(body);
            self.namespace_stack.pop();
        } else {
            // Unbraced form: prefix applies to the rest of the file.
            self.namespace_stack.pop();
            self.namespace_stack.push(prefix);
        }
    }

    fn emit_use(&mut self, node: Node<'_>) {
        // `use App\Traits\{A, B as C};` — the prefix sits as a bare
        // `namespace_name` outside the braces.
        let mut group_prefix: Option<String> = None;
        let mut group_prefix_range: Option<(u32, u32)> = None;
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "namespace_name" => {
                    group_prefix = Some(self.text(child).to_string());
                    let r = child.byte_range();
                    group_prefix_range = Some((r.start as u32, r.end as u32));
                }
                "namespace_use_clause" => {
                    self.emit_use_clause(child, None, None);
                }
                "namespace_use_group" => {
                    let mut gc = child.walk();
                    for clause in child.named_children(&mut gc) {
                        if clause.kind() == "namespace_use_clause" {
                            self.emit_use_clause(
                                clause,
                                group_prefix.as_deref(),
                                group_prefix_range,
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn emit_use_clause(
        &mut self,
        clause: Node<'_>,
        prefix: Option<&str>,
        prefix_range: Option<(u32, u32)>,
    ) {
        let alias_node = child_by_field(clause, "alias");
        let alias = alias_node.map(|n| self.text(n).to_string());
        let mut target_text: Option<String> = None;
        let mut target_range: Option<(u32, u32)> = None;
        let mut cursor = clause.walk();
        if cursor.goto_first_child() {
            loop {
                let child = cursor.node();
                if cursor.field_name().is_none()
                    && matches!(child.kind(), "name" | "qualified_name" | "namespace_name")
                {
                    target_text = Some(self.text(child).to_string());
                    let r = child.byte_range();
                    target_range = Some((r.start as u32, r.end as u32));
                    break;
                }
                if !cursor.goto_next_sibling() {
                    break;
                }
            }
        }
        let Some(path) = target_text else { return };
        let stripped = strip_leading_backslash(&path);
        let qualified = match prefix {
            Some(p) => format!("{p}\\{stripped}"),
            None => stripped.to_string(),
        };
        let alias = alias.unwrap_or_else(|| last_segment(&qualified).to_string());
        let (s, e) = match (prefix_range, target_range) {
            (Some((ps, _)), Some((_, te))) => (ps, te),
            (None, Some(r)) => r,
            _ => return,
        };
        self.facts.use_imports.push(UseImport {
            alias,
            qualified,
            byte_start: s,
            byte_end: e,
        });
    }

    fn enter_container(&mut self, node: Node<'_>) {
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        let name = self.text(name_node).to_string();
        let qualified = qualify_with_ns(self.current_namespace(), &name);
        let mut extends: Vec<HeritageName> = Vec::new();
        let mut implements: Vec<HeritageName> = Vec::new();
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "base_clause" => {
                    for h in collect_clause_heritage(child, self.source) {
                        extends.push(h);
                    }
                }
                "class_interface_clause" => {
                    for h in collect_clause_heritage(child, self.source) {
                        implements.push(h);
                    }
                }
                _ => {}
            }
        }
        // Emit heritage edges (extends/implements) as mixins for MRO,
        // and as ConstRef (Type resolution sites) so each heritage name
        // gets a Tier-2.5 `resolutions` row pinned at the same byte
        // range the Tier-2 `implementations` table stored
        // (`interface_byte_range`). That join is what the find_subtypes
        // / find_supertypes queries use to surface
        // `kind_source = "tier25-php-resolver"`.
        for h in &extends {
            self.facts.mixins.push(MixinEdge {
                owner: qualified.clone(),
                kind: MixinKind::Extends,
                module_parts: h.parts.clone(),
                absolute: h.absolute,
            });
            self.facts.const_refs.push(ConstRef {
                parts: h.parts.clone(),
                absolute: h.absolute,
                namespace: self.current_namespace().map(str::to_string),
                byte_start: h.byte_start,
                byte_end: h.byte_end,
            });
        }
        for h in &implements {
            self.facts.mixins.push(MixinEdge {
                owner: qualified.clone(),
                kind: MixinKind::Implements,
                module_parts: h.parts.clone(),
                absolute: h.absolute,
            });
            self.facts.const_refs.push(ConstRef {
                parts: h.parts.clone(),
                absolute: h.absolute,
                namespace: self.current_namespace().map(str::to_string),
                byte_start: h.byte_start,
                byte_end: h.byte_end,
            });
        }
        let _ = (&extends, &implements);
        self.facts.class_defs.push(ClassDef {
            qualified: qualified.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        self.container_stack.push(qualified);
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn emit_trait_use(&mut self, node: Node<'_>) {
        let Some(owner) = self.container_stack.last().cloned() else {
            return;
        };
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if matches!(child.kind(), "name" | "qualified_name") {
                let text = self.text(child);
                let absolute = text.starts_with('\\');
                let stripped = strip_leading_backslash(text);
                let parts: Vec<String> = stripped.split('\\').map(str::to_string).collect();
                let byte_start = child.start_byte() as u32;
                let byte_end = child.end_byte() as u32;
                self.facts.mixins.push(MixinEdge {
                    owner: owner.clone(),
                    kind: MixinKind::TraitUse,
                    module_parts: parts.clone(),
                    absolute,
                });
                self.facts.const_refs.push(ConstRef {
                    parts,
                    absolute,
                    namespace: self.current_namespace().map(str::to_string),
                    byte_start,
                    byte_end,
                });
            }
        }
    }

    fn emit_method(&mut self, node: Node<'_>) {
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        let name = self.text(name_node).to_string();
        let Some(owner) = self.container_stack.last().cloned() else {
            return;
        };
        let is_static = has_static_modifier(node, self.source);
        let qualified = format!("{owner}::{name}");
        self.facts.method_defs.push(MethodDef {
            qualified,
            owner,
            name,
            is_static,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }

    fn emit_scoped_call(&mut self, node: Node<'_>) {
        // `Cls::stat()`, `self::stat()`, `parent::stat()`,
        // `static::stat()`. Skipping `$obj->method()` (member_call)
        // and `$obj?->method()` (nullsafe).
        let Some(scope_node) = child_by_field(node, "scope") else {
            return;
        };
        let Some(name_node) = child_by_field(node, "name") else {
            return;
        };
        if name_node.kind() != "name" {
            // dynamic method name (`Foo::$method()`).
            return;
        }
        let method = self.text(name_node).to_string();
        // `self::` / `parent::` / `static::` are emitted by
        // tree-sitter-php as a `relative_scope` containing the keyword
        // (or, in some grammar revisions, the bare keyword as the scope
        // child). Other tier-2.5-unfriendly shapes (variable receivers,
        // member-access chains) are recorded as `Unknown` and dropped
        // by the resolver.
        let scope_text = self.text(scope_node);
        let receiver = match scope_node.kind() {
            "name" => {
                let text = scope_text;
                match text {
                    "self" => CallReceiver::SelfClass,
                    "parent" => CallReceiver::Parent,
                    "static" => CallReceiver::StaticClass,
                    _ => CallReceiver::Const {
                        parts: vec![text.to_string()],
                        absolute: false,
                    },
                }
            }
            "qualified_name" => {
                let text = scope_text;
                let absolute = text.starts_with('\\');
                let stripped = strip_leading_backslash(text);
                let parts: Vec<String> = stripped.split('\\').map(str::to_string).collect();
                CallReceiver::Const { parts, absolute }
            }
            "relative_scope" => match scope_text {
                "self" => CallReceiver::SelfClass,
                "parent" => CallReceiver::Parent,
                "static" => CallReceiver::StaticClass,
                _ => CallReceiver::Unknown,
            },
            _ => CallReceiver::Unknown,
        };
        self.facts.method_calls.push(MethodCall {
            receiver,
            method,
            byte_start: name_node.start_byte() as u32,
            byte_end: name_node.end_byte() as u32,
            namespace: self.current_namespace().map(str::to_string),
            lexical_class: self.container_stack.last().cloned(),
        });
    }

    fn try_emit_type_ref(&mut self, node: Node<'_>) {
        let Some(parent) = node.parent() else {
            return;
        };
        // Skip declaration & import positions; their names aren't
        // user-visible type references.
        match parent.kind() {
            "namespace_definition"
            | "namespace_use_clause"
            | "namespace_use_declaration"
            | "namespace_use_group"
            | "use_declaration"
            | "class_declaration"
            | "interface_declaration"
            | "trait_declaration"
            | "enum_declaration"
            | "method_declaration"
            | "function_definition"
            | "property_element"
            | "variable_name"
            | "base_clause"
            | "class_interface_clause"
            | "scoped_call_expression"
            | "scoped_property_access_expression"
            | "function_call_expression"
            | "member_access_expression"
            | "member_call_expression"
            | "nullsafe_member_call_expression"
            | "named_type" => return,
            _ => {}
        }
        // Only emit refs that the user would consider "type-position":
        // we restrict to a handful of contexts that the grammar
        // exposes explicitly.
        match parent.kind() {
            "object_creation_expression" | "class_constant_access_expression" => {}
            _ => return,
        }
        let text = self.text(node);
        if text.is_empty() {
            return;
        }
        let absolute = text.starts_with('\\');
        let stripped = strip_leading_backslash(text);
        let parts: Vec<String> = stripped.split('\\').map(str::to_string).collect();
        if parts.iter().any(String::is_empty) {
            return;
        }
        self.facts.const_refs.push(ConstRef {
            parts,
            absolute,
            namespace: self.current_namespace().map(str::to_string),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }
}

fn child_by_field<'tree>(node: Node<'tree>, name: &str) -> Option<Node<'tree>> {
    node.child_by_field_name(name)
}

fn collect_clause_heritage(node: Node<'_>, source: &[u8]) -> Vec<HeritageName> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if !matches!(child.kind(), "name" | "qualified_name") {
            continue;
        }
        let text = std::str::from_utf8(&source[child.byte_range()]).unwrap_or("");
        if text.is_empty() {
            continue;
        }
        let absolute = text.starts_with('\\');
        let stripped = text.trim_start_matches('\\');
        let parts: Vec<String> = stripped.split('\\').map(str::to_string).collect();
        out.push(HeritageName {
            parts,
            absolute,
            byte_start: child.start_byte() as u32,
            byte_end: child.end_byte() as u32,
        });
    }
    out
}

fn has_static_modifier(node: Node<'_>, source: &[u8]) -> bool {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "static_modifier" {
            return true;
        }
        // Some grammar variants nest modifiers under a list.
        if child.kind() == "modifier_list" {
            let mut mc = child.walk();
            for m in child.children(&mut mc) {
                let t = std::str::from_utf8(&source[m.byte_range()]).unwrap_or("");
                if t == "static" {
                    return true;
                }
            }
        }
        // Most-common shape: bare `static` keyword as anonymous child.
        let t = std::str::from_utf8(&source[child.byte_range()]).unwrap_or("");
        if t == "static" && !child.is_named() {
            return true;
        }
    }
    false
}

fn qualify_with_ns(ns: Option<&str>, name: &str) -> String {
    match ns {
        Some(n) if !n.is_empty() => format!("{n}\\{name}"),
        _ => name.to_string(),
    }
}

pub(crate) fn strip_leading_backslash(s: &str) -> &str {
    s.strip_prefix('\\').unwrap_or(s)
}

pub(crate) fn last_segment(s: &str) -> &str {
    s.rsplit('\\').next().unwrap_or(s)
}

// ─── workspace-wide index ─────────────────────────────────────────────────

/// Workspace constant/type index: qualified name → defining file.
#[derive(Debug, Default)]
pub struct ConstIndex {
    by_qualified: HashMap<String, ConstTarget>,
}

impl ConstIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_qualified = HashMap::new();
        for (path, _, facts) in per_file {
            for class in &facts.class_defs {
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

    /// Resolve a constant/type reference under PHP namespace rules:
    ///
    /// * `\Foo\Bar` (absolute): look up `Foo\Bar` directly.
    /// * single-segment `Foo`: look up the alias map first, then
    ///   `{namespace}\Foo`, then global `Foo`.
    /// * multi-segment `Foo\Bar`: alias map on the first segment
    ///   substitutes the prefix; otherwise `{namespace}\Foo\Bar`,
    ///   then global `Foo\Bar`.
    pub fn resolve(
        &self,
        parts: &[String],
        absolute: bool,
        namespace: Option<&str>,
        aliases: &HashMap<String, String>,
    ) -> Option<ConstTarget> {
        if parts.is_empty() {
            return None;
        }
        if absolute {
            return self.get(&parts.join("\\")).cloned();
        }
        let head = &parts[0];
        let tail = if parts.len() > 1 {
            Some(parts[1..].join("\\"))
        } else {
            None
        };

        // Alias substitution.
        if let Some(target_qualified) = aliases.get(head) {
            let combined = match &tail {
                Some(t) => format!("{target_qualified}\\{t}"),
                None => target_qualified.clone(),
            };
            if let Some(t) = self.get(&combined) {
                return Some(t.clone());
            }
        }

        // Within current namespace.
        if let Some(ns) = namespace.filter(|s| !s.is_empty()) {
            let candidate = format!("{ns}\\{}", parts.join("\\"));
            if let Some(t) = self.get(&candidate) {
                return Some(t.clone());
            }
        }

        // Global namespace fallback.
        if let Some(t) = self.get(&parts.join("\\")) {
            return Some(t.clone());
        }

        None
    }
}
