//! Per-file extraction of Python class/function/import facts and the
//! workspace `ModuleIndex` that maps fully-qualified names to defining
//! files.

use std::collections::HashMap;

use tree_sitter::{Node, Parser};

/// Resolved symbol: where it lives and its fully qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleTarget {
    pub path: String,
    pub qualified: String,
}

/// Per-file extracted facts.
#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    /// Module qualified name (`pkg.sub.module`) — populated by the
    /// caller from the file path before parsing.
    pub module: Option<String>,
    /// True when the source file is `__init__.py` (so `module` is the
    /// package itself, not a module inside a package). Affects how
    /// relative-import parent walking works: `from .` inside
    /// `pkg/__init__.py` stays at `pkg`, while `from .` inside
    /// `pkg/x.py` walks up to `pkg`.
    pub is_package_init: bool,
    /// Class definitions.
    pub class_defs: Vec<ClassDef>,
    /// Function / method definitions.
    pub method_defs: Vec<MethodDef>,
    /// Heritage edges (positional base classes).
    pub base_edges: Vec<BaseEdge>,
    /// `import x` / `import x as y` / `from m import y` / `from . import y`
    /// — every binding the file produces, with the byte range of the
    /// site cairn pins the Tier-2 ImportFact to (the dotted module path).
    pub import_bindings: Vec<ImportBinding>,
    /// Type-position references (base-class names, constructor calls).
    pub type_refs: Vec<TypeRef>,
    /// Method-call sites the resolver might want to pin.
    pub method_calls: Vec<MethodCall>,
}

#[derive(Debug, Clone)]
pub struct ClassDef {
    /// Fully qualified name within the file's module context
    /// (`pkg.module.Outer.Inner`).
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
pub struct BaseEdge {
    /// Qualified name of the subclass (`pkg.module.Dog`).
    pub owner: String,
    /// Dotted parts of the base-class expression as written
    /// (`["abc", "ABC"]` for `class E(abc.ABC):`).
    pub parts: Vec<String>,
}

/// One binding produced by an `import`-family statement.
///
/// `import a.b`            → `local = "a"`, `module = "a.b"`, imported = None, kind = Plain
/// `import a.b as c`       → `local = "c"`, `module = "a.b"`, imported = None, kind = Aliased
/// `from m import x`       → `local = "x"`, `module = "m"`,   imported = Some("x"), kind = From
/// `from m import x as y`  → `local = "y"`, `module = "m"`,   imported = Some("x"), kind = From
/// `from . import sub`     → `local = "sub"`, `module = "."`, imported = Some("sub"), kind = From
/// `from ..pkg import sub` → `local = "sub"`, `module = "..pkg"`, imported = Some("sub"), kind = From
///
/// `site_byte_range` covers the same span the Tier-2
/// `ImportFact::byte_range` uses (the dotted module name, or the
/// `relative_import` node for `from . import …`) so that the persist
/// layer's join lines up.
#[derive(Debug, Clone)]
pub struct ImportBinding {
    pub kind: ImportKind,
    pub local: String,
    pub module: String,
    pub imported: Option<String>,
    pub site_byte_start: u32,
    pub site_byte_end: u32,
    /// Leading-dot count for relative imports (`.` = 1, `..pkg` = 2).
    /// Zero for absolute imports.
    pub level: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportKind {
    /// `import x`, `import x.y` (no alias).
    Plain,
    /// `import x as y`, `import x.y as z`.
    Aliased,
    /// `from m import a`, `from m import a as b`, `from . import c`.
    From,
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
    /// Byte range covering just the method-name identifier — same
    /// convention as PHP / Ruby (the span Tier-2 records on the
    /// resolved-callee site).
    pub byte_start: u32,
    pub byte_end: u32,
    /// Qualified name of the lexically enclosing class, if any (for
    /// `self.method`, `super().method`).
    pub lexical_class: Option<String>,
    /// Qualified name of the lexically enclosing function/method, if
    /// any (used for module-level `foo()` calls in tests).
    pub lexical_function: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallReceiver {
    /// `Cls.method()` / `mod.fn()` — the dotted prefix written as a
    /// chain of identifiers. Single-segment (`Cls.method`) and
    /// multi-segment (`pkg.mod.Cls.method`) are both represented;
    /// the resolver chooses between class-method dispatch and
    /// module-attribute dispatch by looking the head up in the alias
    /// map and module index.
    Dotted { parts: Vec<String> },
    /// `self.method()` — receiver is `self` and the call lives inside
    /// a class.
    SelfRef,
    /// `super().method()` — late-bound dispatch up the MRO.
    SuperRef,
    /// `cls.method()` — receiver is `cls` (classmethod-style). Treated
    /// as `self`-like for Tier-2.5 dispatch.
    ClsRef,
    /// `foo()` — bare name call (module-level function or imported
    /// name).
    Bare { name: String },
    /// Anything else (`obj.method()`, `arr[0].method()`, etc.) — not
    /// resolvable at Tier-2.5.
    Unknown,
}

/// Parse a single Python source blob and extract its facts.
#[must_use]
pub fn parse_file(
    source: &[u8],
    module: Option<String>,
    is_package_init: bool,
) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_python::LANGUAGE.into();
    let mut parser = Parser::new();
    parser.set_language(&language).ok()?;
    let tree = parser.parse(source, None)?;
    let mut visitor = Visitor::new(source, module, is_package_init);
    visitor.walk(tree.root_node());
    Some(visitor.facts)
}

struct Visitor<'a> {
    source: &'a [u8],
    facts: FileConstFacts,
    /// Lexical container stack. Each entry is the qualified prefix
    /// active inside that container. Pushing a class adds the class
    /// name; pushing a function adds the function name. For
    /// Python-style qualified names we keep the full nesting (matches
    /// PEP 3155 `__qualname__`).
    container_stack: Vec<ContainerFrame>,
}

#[derive(Debug, Clone)]
struct ContainerFrame {
    qualified: String,
    is_class: bool,
}

impl<'a> Visitor<'a> {
    fn new(source: &'a [u8], module: Option<String>, is_package_init: bool) -> Self {
        let facts = FileConstFacts {
            module,
            is_package_init,
            ..FileConstFacts::default()
        };
        Self {
            source,
            facts,
            container_stack: Vec::new(),
        }
    }

    fn text(&self, node: Node<'_>) -> &str {
        std::str::from_utf8(&self.source[node.byte_range()]).unwrap_or("")
    }

    /// The qualified prefix produced by the current container chain
    /// PLUS the file's module name. Used to qualify newly-defined
    /// symbols and to record class/function lexical context for calls.
    fn qualify_in_scope(&self, leaf: &str) -> String {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(m) = self.facts.module.as_deref() {
            if !m.is_empty() {
                parts.push(m);
            }
        }
        for f in &self.container_stack {
            parts.push(short_name(&f.qualified));
        }
        parts.push(leaf);
        parts.join(".")
    }

    fn current_class(&self) -> Option<String> {
        self.container_stack
            .iter()
            .rev()
            .find(|f| f.is_class)
            .map(|f| f.qualified.clone())
    }

    fn current_function(&self) -> Option<String> {
        self.container_stack
            .iter()
            .rev()
            .find(|f| !f.is_class)
            .map(|f| f.qualified.clone())
    }

    fn walk(&mut self, node: Node<'_>) {
        if node.is_error() || node.is_missing() {
            return;
        }
        match node.kind() {
            "import_statement" => {
                self.emit_import_statement(node);
                return;
            }
            "import_from_statement" => {
                self.emit_from_import(node);
                return;
            }
            "class_definition" => {
                self.enter_class(node);
                return;
            }
            "function_definition" => {
                self.enter_function(node);
                return;
            }
            "call" => {
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
        let Some(name_node) = node.child_by_field_name("name") else {
            return;
        };
        let name = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name);

        // Record base-class edges + type references at the base-name range.
        if let Some(supers) = node.child_by_field_name("superclasses") {
            let mut cursor = supers.walk();
            for child in supers.named_children(&mut cursor) {
                // Skip keyword args (`metaclass=Foo`) and comments.
                if let Some(parts) = dotted_parts(child, self.source) {
                    let r = child.byte_range();
                    self.facts.base_edges.push(BaseEdge {
                        owner: qualified.clone(),
                        parts: parts.clone(),
                    });
                    // Type-ref at the same byte range as the Tier-2
                    // ImplFact's `interface_byte_range`, so the join
                    // lines up.
                    self.facts.type_refs.push(TypeRef {
                        parts,
                        byte_start: r.start as u32,
                        byte_end: r.end as u32,
                    });
                }
            }
        }

        self.facts.class_defs.push(ClassDef {
            qualified: qualified.clone(),
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
        self.container_stack.push(ContainerFrame {
            qualified,
            is_class: true,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn enter_function(&mut self, node: Node<'_>) {
        let Some(name_node) = node.child_by_field_name("name") else {
            return;
        };
        let name = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name);
        // Record a method when nested directly inside a class, OR a
        // module-level callable when at top level (owner = the
        // file's module). Nested functions (inside another function)
        // are skipped — they can't be statically dispatched anyway.
        if let Some(owner) = self.current_class() {
            self.facts.method_defs.push(MethodDef {
                qualified: format!("{owner}.{name}"),
                owner,
                name: name.clone(),
                byte_start: node.start_byte() as u32,
                byte_end: node.end_byte() as u32,
            });
        } else if self.current_function().is_none() {
            if let Some(module) = self.facts.module.as_deref() {
                self.facts.method_defs.push(MethodDef {
                    qualified: format!("{module}.{name}"),
                    owner: module.to_string(),
                    name: name.clone(),
                    byte_start: node.start_byte() as u32,
                    byte_end: node.end_byte() as u32,
                });
            }
        }
        self.container_stack.push(ContainerFrame {
            qualified,
            is_class: false,
        });
        self.descend_children(node);
        self.container_stack.pop();
    }

    fn emit_call(&mut self, node: Node<'_>) {
        let Some(func) = node.child_by_field_name("function") else {
            return;
        };
        let (receiver, method_name, name_node) = classify_call(func, self.source);
        let Some(method) = method_name else { return };
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

    /// Emit one `ImportBinding` per binding the statement produces, and
    /// keep the source-site byte range pinned to the **dotted module
    /// path** (Tier-2 will emit `ImportFact::byte_range` over the same
    /// node, so the persist join matches).
    fn emit_import_statement(&mut self, node: Node<'_>) {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "dotted_name" => {
                    let module_text = self.text(child).to_string();
                    let local = module_text
                        .split('.')
                        .next()
                        .unwrap_or(&module_text)
                        .to_string();
                    let r = child.byte_range();
                    self.facts.import_bindings.push(ImportBinding {
                        kind: ImportKind::Plain,
                        local,
                        module: module_text,
                        imported: None,
                        site_byte_start: r.start as u32,
                        site_byte_end: r.end as u32,
                        level: 0,
                    });
                }
                "aliased_import" => {
                    let module_text = child
                        .child_by_field_name("name")
                        .map(|n| self.text(n).to_string())
                        .unwrap_or_default();
                    let alias = child
                        .child_by_field_name("alias")
                        .map(|n| self.text(n).to_string())
                        .unwrap_or_default();
                    if module_text.is_empty() || alias.is_empty() {
                        continue;
                    }
                    let r = child
                        .child_by_field_name("name")
                        .map(|n| n.byte_range())
                        .unwrap_or(child.byte_range());
                    self.facts.import_bindings.push(ImportBinding {
                        kind: ImportKind::Aliased,
                        local: alias,
                        module: module_text,
                        imported: None,
                        site_byte_start: r.start as u32,
                        site_byte_end: r.end as u32,
                        level: 0,
                    });
                }
                _ => {}
            }
        }
    }

    fn emit_from_import(&mut self, node: Node<'_>) {
        // `module_name` field is either a `dotted_name` (absolute) or a
        // `relative_import` (`.` / `..pkg`). Reach for the byte range
        // covering the whole module-name token; the relative-import node
        // already includes the leading dots.
        let module_node = node.child_by_field_name("module_name");
        let (module_text, level, module_range) = match module_node {
            Some(n) => {
                let text = self.text(n).to_string();
                let level = leading_dots(&text);
                let r = n.byte_range();
                (text, level, Some((r.start as u32, r.end as u32)))
            }
            None => (String::new(), 0, None),
        };
        let Some((site_start, site_end)) = module_range else {
            return;
        };

        // Manual cursor walk so we can read field names per child.
        let mut cursor = node.walk();
        if cursor.goto_first_child() {
            loop {
                let child = cursor.node();
                let field = cursor.field_name();
                if child.kind() == "wildcard_import" {
                    self.facts.import_bindings.push(ImportBinding {
                        kind: ImportKind::From,
                        local: "*".to_string(),
                        module: module_text.clone(),
                        imported: None,
                        site_byte_start: site_start,
                        site_byte_end: site_end,
                        level,
                    });
                } else if field == Some("name") {
                    match child.kind() {
                        "dotted_name" | "identifier" => {
                            let imported = self.text(child).to_string();
                            if !imported.is_empty() {
                                self.facts.import_bindings.push(ImportBinding {
                                    kind: ImportKind::From,
                                    local: last_segment(&imported).to_string(),
                                    module: module_text.clone(),
                                    imported: Some(imported),
                                    site_byte_start: site_start,
                                    site_byte_end: site_end,
                                    level,
                                });
                            }
                        }
                        "aliased_import" => {
                            let imported = child
                                .child_by_field_name("name")
                                .map(|n| self.text(n).to_string());
                            let alias = child
                                .child_by_field_name("alias")
                                .map(|n| self.text(n).to_string());
                            if let (Some(imp), Some(al)) = (imported, alias) {
                                self.facts.import_bindings.push(ImportBinding {
                                    kind: ImportKind::From,
                                    local: al,
                                    module: module_text.clone(),
                                    imported: Some(imp),
                                    site_byte_start: site_start,
                                    site_byte_end: site_end,
                                    level,
                                });
                            }
                        }
                        _ => {}
                    }
                }
                if !cursor.goto_next_sibling() {
                    break;
                }
            }
        }
    }
}

/// `pkg.sub.module.Outer.Inner` → `Inner`.
fn short_name(qualified: &str) -> &str {
    qualified.rsplit('.').next().unwrap_or(qualified)
}

pub(crate) fn last_segment(qualified: &str) -> &str {
    qualified.rsplit('.').next().unwrap_or(qualified)
}

/// Count leading `.` characters in a relative-import module token.
/// `.` → 1, `..` → 2, `..pkg` → 2, `pkg` → 0.
fn leading_dots(s: &str) -> u32 {
    s.bytes().take_while(|b| *b == b'.').count() as u32
}

/// Strip leading dots and return the bare module portion of a
/// relative-import token (`..pkg.sub` → `pkg.sub`, `.` → ``).
pub(crate) fn strip_leading_dots(s: &str) -> &str {
    s.trim_start_matches('.')
}

/// Read a tree-sitter expression node as a dotted-parts vector when
/// it's shaped like an identifier or `a.b.c` attribute chain. Returns
/// `None` for subscripts, calls, comprehensions, etc. — we intentionally
/// drop generic-like bases (`Generic[T]`) because the resolver only
/// resolves bare names.
fn dotted_parts(node: Node<'_>, source: &[u8]) -> Option<Vec<String>> {
    match node.kind() {
        "identifier" => Some(vec![
            std::str::from_utf8(&source[node.byte_range()])
                .ok()?
                .to_string(),
        ]),
        "attribute" => {
            let mut parts = Vec::new();
            walk_attribute(node, source, &mut parts)?;
            Some(parts)
        }
        _ => None,
    }
}

fn walk_attribute(node: Node<'_>, source: &[u8], parts: &mut Vec<String>) -> Option<()> {
    // tree-sitter-python `attribute` has fields `object` and
    // `attribute`. `object` may itself be an `attribute` (recursive).
    let object = node.child_by_field_name("object")?;
    match object.kind() {
        "identifier" => {
            parts.push(
                std::str::from_utf8(&source[object.byte_range()])
                    .ok()?
                    .to_string(),
            );
        }
        "attribute" => {
            walk_attribute(object, source, parts)?;
        }
        _ => return None,
    }
    let attr = node.child_by_field_name("attribute")?;
    if attr.kind() != "identifier" {
        return None;
    }
    parts.push(
        std::str::from_utf8(&source[attr.byte_range()])
            .ok()?
            .to_string(),
    );
    Some(())
}

/// Decide what shape of receiver a `call.function` node has.
///
/// Returns `(receiver, method_name, name_node_for_byte_range)`.
fn classify_call<'tree>(
    func: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    match func.kind() {
        // Bare call: `foo(...)`. The whole `func` is also the name
        // node.
        "identifier" => {
            let name = std::str::from_utf8(&source[func.byte_range()])
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                return (CallReceiver::Unknown, None, func);
            }
            (CallReceiver::Bare { name: name.clone() }, Some(name), func)
        }
        "attribute" => classify_attribute_call(func, source),
        _ => (CallReceiver::Unknown, None, func),
    }
}

fn classify_attribute_call<'tree>(
    attr: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    let Some(method_node) = attr.child_by_field_name("attribute") else {
        return (CallReceiver::Unknown, None, attr);
    };
    if method_node.kind() != "identifier" {
        return (CallReceiver::Unknown, None, attr);
    }
    let method = std::str::from_utf8(&source[method_node.byte_range()])
        .unwrap_or("")
        .to_string();
    if method.is_empty() {
        return (CallReceiver::Unknown, None, method_node);
    }
    let Some(receiver_node) = attr.child_by_field_name("object") else {
        return (CallReceiver::Unknown, None, method_node);
    };
    let receiver = match receiver_node.kind() {
        "identifier" => {
            let text = std::str::from_utf8(&source[receiver_node.byte_range()]).unwrap_or("");
            match text {
                "self" => CallReceiver::SelfRef,
                "cls" => CallReceiver::ClsRef,
                _ => CallReceiver::Dotted {
                    parts: vec![text.to_string()],
                },
            }
        }
        "attribute" => match dotted_parts(receiver_node, source) {
            Some(parts) => CallReceiver::Dotted { parts },
            None => CallReceiver::Unknown,
        },
        "call" => {
            // `super().method()` — the `call` here is the inner
            // `super(...)`. We only recognise the zero-argument
            // shape and bare `super` callee.
            if is_super_call(receiver_node, source) {
                CallReceiver::SuperRef
            } else {
                CallReceiver::Unknown
            }
        }
        _ => CallReceiver::Unknown,
    };
    (receiver, Some(method), method_node)
}

fn is_super_call(call_node: Node<'_>, source: &[u8]) -> bool {
    let Some(func) = call_node.child_by_field_name("function") else {
        return false;
    };
    if func.kind() != "identifier" {
        return false;
    }
    std::str::from_utf8(&source[func.byte_range()]) == Ok("super")
}

// ─── workspace-wide module index ──────────────────────────────────────────

/// Workspace symbol index: qualified name → defining file.
///
/// We index multiple "views" of every symbol so callers can resolve
/// either path-prefixed (`src.flask.app.Flask`) or
/// bare-package-rooted (`flask.app.Flask`) imports without composer /
/// pyproject parsing:
///
/// * the exact qualified as cairn computed it from the file path;
/// * each suffix of the dotted qualified — `src.flask.app.Flask` also
///   indexes as `flask.app.Flask` and `app.Flask` and `Flask`. The
///   first defining file wins per key, deterministic by per-file
///   visitation order.
#[derive(Debug, Default)]
pub struct ModuleIndex {
    by_qualified: HashMap<String, ModuleTarget>,
    /// Module-qualified-name → defining file. Lets the require-graph
    /// resolve `from foo.bar import Baz` even when `foo` is the
    /// top-level package (no `Baz` symbol qualified pulls the whole
    /// module into scope).
    modules: HashMap<String, String>,
}

impl ModuleIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_qualified: HashMap<String, ModuleTarget> = HashMap::new();
        let mut modules: HashMap<String, String> = HashMap::new();

        for (path, _, facts) in per_file {
            if let Some(m) = facts.module.as_deref().filter(|s| !s.is_empty()) {
                modules.entry(m.to_string()).or_insert_with(|| path.clone());
                // Index suffixes of the module name too, so a bare
                // `import flask` matches `src/flask/__init__.py`'s
                // module `src.flask`.
                for suffix in dotted_suffixes(m) {
                    if suffix == m {
                        continue;
                    }
                    modules
                        .entry(suffix.to_string())
                        .or_insert_with(|| path.clone());
                }
            }
            for class in &facts.class_defs {
                let target = ModuleTarget {
                    path: path.clone(),
                    qualified: class.qualified.clone(),
                };
                by_qualified
                    .entry(class.qualified.clone())
                    .or_insert(target.clone());
                for suffix in dotted_suffixes(&class.qualified) {
                    if suffix == class.qualified {
                        continue;
                    }
                    by_qualified
                        .entry(suffix.to_string())
                        .or_insert(target.clone());
                }
            }
            for m in &facts.method_defs {
                let target = ModuleTarget {
                    path: path.clone(),
                    qualified: m.qualified.clone(),
                };
                by_qualified
                    .entry(m.qualified.clone())
                    .or_insert(target.clone());
                for suffix in dotted_suffixes(&m.qualified) {
                    if suffix == m.qualified {
                        continue;
                    }
                    by_qualified
                        .entry(suffix.to_string())
                        .or_insert(target.clone());
                }
            }
        }

        Self {
            by_qualified,
            modules,
        }
    }

    pub fn lookup(&self, qualified: &str) -> Option<&ModuleTarget> {
        self.by_qualified.get(qualified)
    }

    pub fn module_path(&self, module: &str) -> Option<&str> {
        self.modules.get(module).map(String::as_str)
    }
}

/// Yield every dotted suffix of `q`: `a.b.c` → `a.b.c`, `b.c`, `c`.
fn dotted_suffixes(q: &str) -> impl Iterator<Item = &str> {
    let bytes = q.as_bytes();
    let mut starts = vec![0usize];
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'.' {
            starts.push(i + 1);
        }
    }
    starts.into_iter().map(move |start| &q[start..])
}
