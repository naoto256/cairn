//! Per-file extraction of JavaScript class / function / method / import
//! / export facts and the workspace `PackageIndex` that maps the
//! per-module export name → defining file.
//!
//! JavaScript has no namespace concept; the "qualified" name we record
//! is class-relative:
//!   * top-level `class Foo` / `function foo` → `"Foo"` / `"foo"`
//!   * class member `Foo.bar` → `"Foo.bar"` (or `"Foo.prototype.bar"`
//!     collapsed to `"Foo.bar"` — Tier-2.5 doesn't distinguish).
//!
//! File-uniqueness of these short qualified names is held by the
//! `PackageIndex`, which keys by `(path, qualified)`.

use std::collections::{HashMap, HashSet};

use tree_sitter::{Node, Parser};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageTarget {
    pub path: String,
    pub qualified: String,
}

#[derive(Debug, Default, Clone)]
pub struct FileConstFacts {
    pub class_defs: Vec<ClassDef>,
    pub method_defs: Vec<MethodDef>,
    pub function_defs: Vec<FunctionDef>,
    /// Direct heritage edges from `class X extends Y` / `class X extends ns.Y`.
    pub base_edges: Vec<BaseEdge>,
    /// Import bindings: ESM `import` clauses + CJS `require(...)`
    /// destinations. Each one captures a local name and the module
    /// specifier; resolution to a workspace file happens in
    /// `RequireGraph`.
    pub import_bindings: Vec<ImportBinding>,
    /// Type-position references at base-name byte ranges (for the
    /// `extends` site).
    pub type_refs: Vec<TypeRef>,
    /// Statically pinnable call sites.
    pub method_calls: Vec<MethodCall>,
    /// Export table: local symbol name (the one defined in this file)
    /// → exported-as name. Used to invert imports on the consumer side.
    /// `export default class X` records `("X", "default")`; `module.exports
    /// = X` records the same; `export { X as Y }` records `("X", "Y")`.
    pub exports: Vec<ExportBinding>,
    /// Re-exports of the form `export * from './bar'` /
    /// `export { X } from './bar'`.
    pub reexports: Vec<ReExport>,
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
pub struct FunctionDef {
    pub qualified: String,
    pub name: String,
    pub byte_start: u32,
    pub byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct BaseEdge {
    pub owner: String,
    pub parts: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportKind {
    /// `import { X } from './foo'` / `import X from './foo'` — named
    /// or default ESM import.
    Esm,
    /// `import * as Ns from './foo'` — namespace import. `local = "Ns"`,
    /// `imported_name = None`.
    EsmNamespace,
    /// `import './foo'` — side-effect; no local binding.
    SideEffect,
    /// `const X = require('./foo')` / `const { X } = require('./foo')`
    /// / `const X = require('./foo').X`.
    Cjs,
}

#[derive(Debug, Clone)]
pub struct ImportBinding {
    pub kind: ImportKind,
    /// Local name in this file's scope (for ESM default this is the
    /// alias used in source; for `import * as Ns`, this is `Ns`; for
    /// CJS destructure each binding gets one entry).
    pub local: String,
    /// Name as exported from the target module. `Some("default")` for
    /// default imports / `module.exports = X` consumers. `None` for
    /// `EsmNamespace` (the whole module is the value) and `SideEffect`.
    pub imported_name: Option<String>,
    /// Module specifier as written in source (e.g. `"./foo"`, `"express"`,
    /// `"node:fs"`). Path resolution to a workspace file is done in
    /// `RequireGraph`.
    pub module: String,
    /// Byte range of the module-specifier string node (the part inside
    /// the quotes is what we want pinned; we use the whole string node
    /// for stability).
    pub site_byte_start: u32,
    pub site_byte_end: u32,
}

#[derive(Debug, Clone)]
pub struct ExportBinding {
    /// Name as defined in this file (e.g. `X` for `export class X` or
    /// `export { X }`; for `module.exports = X` it's also `X`).
    pub local: String,
    /// Name as visible to importers (e.g. `"X"`, `"default"`).
    pub exported_as: String,
}

#[derive(Debug, Clone)]
pub struct ReExport {
    /// Module specifier (`./bar`).
    pub module: String,
    /// `None` → `export * from './bar'` (re-export everything).
    /// `Some(("X", "Y"))` → `export { X as Y } from './bar'` (re-export
    /// `X` from target as `Y`).
    pub names: Vec<(String, String)>,
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
    /// `Cls.method()` / `ns.Cls.method()` — receiver is a dotted
    /// identifier chain.
    Dotted { parts: Vec<String> },
    /// `this.method()` — current lexical class's MRO walk.
    ThisRef,
    /// `super.method()` — MRO walk starting after the lexical class.
    SuperRef,
    /// `foo()` — bare callee.
    Bare { name: String },
    /// `new Foo()` directly chained `new Foo().bar()` — head of the
    /// receiver is a constructor call. Tier-2.5 treats it like a Dotted
    /// receiver of class `Foo`.
    NewExpr { class: String },
    /// Anything else.
    Unknown,
}

/// Parse a single JS source blob and extract its facts.
#[must_use]
pub fn parse_file(source: &[u8]) -> Option<FileConstFacts> {
    let language: tree_sitter::Language = tree_sitter_javascript::LANGUAGE.into();
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
    container_stack: Vec<ContainerFrame>,
    /// Byte ranges of `require(...)` call sites already turned into an
    /// `ImportBinding`. Mirrors the Tier-1 backend's
    /// `seen_require_sites` so the binding-form path
    /// (`emit_var_declaration`) and the new statement / expression /
    /// re-export paths can't double-emit the same call. Keyed on the
    /// call site itself (not the module-literal range) so two distinct
    /// calls to the same specifier don't collide.
    seen_require_sites: HashSet<(u32, u32)>,
    /// How many `function_declaration` / `method_definition` frames we
    /// are currently inside. Used to gate `emit_function` so that
    /// nested function declarations (`function outer(){ function inner(){} }`
    /// or methods containing locally-declared helpers) don't leak into
    /// the workspace-wide `function_defs` index. Tier-2.5 only models
    /// top-level (module-scoped) functions as dispatch targets; nested
    /// helpers are file-local and not addressable from other files.
    function_depth: u32,
}

#[derive(Debug, Clone)]
struct ContainerFrame {
    /// Class name pushed onto the qualified chain.
    name: String,
    /// Byte offset at which this frame goes out of scope.
    end_byte: usize,
}

impl<'a> Visitor<'a> {
    fn new(source: &'a [u8]) -> Self {
        Self {
            source,
            facts: FileConstFacts::default(),
            container_stack: Vec::new(),
            seen_require_sites: HashSet::new(),
            function_depth: 0,
        }
    }

    fn text(&self, node: Node<'_>) -> &str {
        std::str::from_utf8(&self.source[node.byte_range()]).unwrap_or("")
    }

    fn pop_outside(&mut self, byte: usize) {
        while let Some(top) = self.container_stack.last() {
            if top.end_byte <= byte {
                self.container_stack.pop();
            } else {
                break;
            }
        }
    }

    fn current_class(&self) -> Option<String> {
        self.container_stack.last().map(|f| f.name.clone())
    }

    fn qualify_in_scope(&self, leaf: &str) -> String {
        let mut parts: Vec<&str> = Vec::new();
        for f in &self.container_stack {
            parts.push(&f.name);
        }
        parts.push(leaf);
        parts.join(".")
    }

    fn walk(&mut self, node: Node<'_>) {
        self.pop_outside(node.start_byte());
        if node.is_error() || node.is_missing() {
            self.descend_children(node);
            return;
        }
        match node.kind() {
            "import_statement" => {
                self.emit_import_statement(node);
                return;
            }
            "export_statement" => {
                self.emit_export_statement(node);
                // Fall through so we still visit declarations inside.
            }
            "class_declaration" => {
                self.enter_class(node);
                return;
            }
            "function_declaration" | "generator_function_declaration" => {
                // `emit_function` checks `function_depth` and only
                // records the top-level case. We bump *after* the
                // emit (the declaration name itself is at the parent
                // scope) and around the body descent so any nested
                // `function inner() {}` is suppressed.
                self.emit_function(node);
                self.function_depth = self.function_depth.saturating_add(1);
                self.descend_children(node);
                self.function_depth = self.function_depth.saturating_sub(1);
                return;
            }
            // Anonymous / expression-shaped function scopes. tree-sitter-
            // javascript exposes four kinds that introduce a new function
            // body but do *not* themselves declare a workspace-addressable
            // name: `function_expression` (named or anonymous function
            // expression), `arrow_function`, `generator_function`
            // (anonymous `function* () {}`), and the bare `function` kind
            // used for some anonymous function expressions. Bump
            // `function_depth` around their descent so any
            // `function inner() {}` declared inside their body is gated
            // by `emit_function`'s top-level check. Without this, e.g.
            // `const f = () => { function helper(){} };` would leak
            // `helper` into the workspace symbol index.
            "function_expression" | "arrow_function" | "generator_function" | "function" => {
                self.function_depth = self.function_depth.saturating_add(1);
                self.descend_children(node);
                self.function_depth = self.function_depth.saturating_sub(1);
                return;
            }
            "lexical_declaration" | "variable_declaration" => {
                self.emit_var_declaration(node);
                self.descend_children(node);
                return;
            }
            "assignment_expression" => {
                self.emit_assignment(node);
                self.try_emit_reexport_require(node);
                // Fall through to capture nested calls.
            }
            "call_expression" => {
                self.emit_call(node);
                self.try_emit_expression_position_require(node);
                // Fall through to recurse arguments.
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

    // ─── classes ─────────────────────────────────────────────────────

    fn enter_class(&mut self, node: Node<'_>) {
        let Some(name_node) = node.child_by_field_name("name") else {
            self.descend_children(node);
            return;
        };
        let name = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name);

        // Heritage: tree-sitter-javascript wraps `extends X` in a
        // `class_heritage` child. Inside is the base expression (which
        // may be an identifier, a member_expression, or a call
        // expression — the latter is `extends Mixin(Base)` and we skip
        // it).
        if let Some(heritage) = find_direct_child(node, "class_heritage") {
            let mut cursor = heritage.walk();
            for child in heritage.named_children(&mut cursor) {
                let parts = base_parts(child, self.source);
                let Some(parts) = parts else {
                    // call_expression / non-identifier base — skip from
                    // MRO. We still don't emit a type_ref since we
                    // can't pin the name.
                    continue;
                };
                let r = child.byte_range();
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

        // Walk body separately so the class frame is in-scope for its
        // member visits.
        let end_byte = node.end_byte();
        self.container_stack.push(ContainerFrame { name, end_byte });

        if let Some(body) = node.child_by_field_name("body") {
            let mut cursor = body.walk();
            for child in body.named_children(&mut cursor) {
                self.walk_class_member(child, &qualified);
            }
        }
        // pop_outside cleans up when we leave this byte range.
    }

    fn walk_class_member(&mut self, node: Node<'_>, owner: &str) {
        if node.kind() == "method_definition" {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = self.text(name_node).to_string();
                let qualified = format!("{owner}.{name}");
                self.facts.method_defs.push(MethodDef {
                    qualified,
                    owner: owner.to_string(),
                    name,
                    byte_start: node.start_byte() as u32,
                    byte_end: node.end_byte() as u32,
                });
            }
        }
        // Recurse to catch calls in the method body.
        self.descend_children(node);
    }

    // ─── top-level functions ─────────────────────────────────────────

    fn emit_function(&mut self, node: Node<'_>) {
        // Top-level-only gate. Tier-2.5 dispatch (`bare foo()` callee
        // resolution, etc.) addresses functions by `(path, name)` —
        // there is no lexical scoping in that contract. Indexing a
        // nested `function inner() {}` would let other call sites in
        // the same file resolve to it (or with classes_by_name-style
        // shadowing, hide a real top-level `inner`), which is a
        // file-local leakage of a private helper.
        //
        // `function_depth` is bumped not only around
        // `function_declaration` / `generator_function_declaration`
        // bodies but also around every expression-shaped function
        // scope (`function_expression`, `arrow_function`,
        // `generator_function`, bare `function`), so a declaration
        // nested inside e.g. `const f = function(){ ... }` or
        // `const f = () => { ... }` is also suppressed.
        if self.function_depth > 0 || !self.container_stack.is_empty() {
            return;
        }
        let Some(name_node) = node.child_by_field_name("name") else {
            return;
        };
        let name = self.text(name_node).to_string();
        let qualified = self.qualify_in_scope(&name);
        self.facts.function_defs.push(FunctionDef {
            qualified,
            name,
            byte_start: node.start_byte() as u32,
            byte_end: node.end_byte() as u32,
        });
    }

    // ─── var / lexical declarations: CJS require detection ───────────

    fn emit_var_declaration(&mut self, node: Node<'_>) {
        let mut cursor = node.walk();
        for declarator in node.named_children(&mut cursor) {
            if declarator.kind() != "variable_declarator" {
                continue;
            }
            let name_node = declarator.child_by_field_name("name");
            let value_node = declarator.child_by_field_name("value");
            let (Some(name_node), Some(value_node)) = (name_node, value_node) else {
                continue;
            };
            self.try_emit_cjs_require(name_node, value_node);
        }
    }

    /// Detect:
    ///   const X = require('./foo')                  → Cjs, local=X, imported=Some("default")
    ///   const X = require('./foo').Y                → Cjs, local=X, imported=Some("Y")
    ///   const { X, Y: Z } = require('./foo')        → Cjs, one entry per binding
    fn try_emit_cjs_require(&mut self, name_node: Node<'_>, value_node: Node<'_>) {
        // value may be `require('./foo')` or `require('./foo').Member`.
        let (require_call, member) = match value_node.kind() {
            "call_expression" => (value_node, None),
            "member_expression" => {
                let obj = value_node.child_by_field_name("object");
                let prop = value_node.child_by_field_name("property");
                let (Some(obj), Some(prop)) = (obj, prop) else {
                    return;
                };
                if obj.kind() != "call_expression" {
                    return;
                }
                let member_text = std::str::from_utf8(&self.source[prop.byte_range()])
                    .ok()
                    .map(|s| s.to_string());
                (obj, member_text)
            }
            _ => return,
        };
        // Confirm `function` is the identifier `require`.
        let Some(func) = require_call.child_by_field_name("function") else {
            return;
        };
        if func.kind() != "identifier" {
            return;
        }
        let func_name = self.text(func);
        if func_name != "require" {
            return;
        }
        let Some(args) = require_call.child_by_field_name("arguments") else {
            return;
        };
        let mut arg_cursor = args.walk();
        let Some(module_node) = args.named_children(&mut arg_cursor).next() else {
            return;
        };
        if module_node.kind() != "string" {
            return;
        }
        let Some(module) = string_literal_text(module_node, self.source) else {
            return;
        };
        let module_range = module_node.byte_range();
        // Claim this require call site so the generic
        // `try_emit_expression_position_require` doesn't double-emit a
        // SideEffect ImportBinding for the same call.
        let call_range = require_call.byte_range();
        self.seen_require_sites
            .insert((call_range.start as u32, call_range.end as u32));

        match name_node.kind() {
            "identifier" => {
                let local = self.text(name_node).to_string();
                let imported_name = match member {
                    Some(m) => Some(m),
                    None => Some("default".to_string()),
                };
                self.facts.import_bindings.push(ImportBinding {
                    kind: ImportKind::Cjs,
                    local,
                    imported_name,
                    module,
                    site_byte_start: module_range.start as u32,
                    site_byte_end: module_range.end as u32,
                });
            }
            "object_pattern" => {
                // `const { X, Y: Z } = require('./foo')`
                let mut cursor = name_node.walk();
                for child in name_node.named_children(&mut cursor) {
                    match child.kind() {
                        "shorthand_property_identifier_pattern" => {
                            let n = self.text(child).to_string();
                            self.facts.import_bindings.push(ImportBinding {
                                kind: ImportKind::Cjs,
                                local: n.clone(),
                                imported_name: Some(n),
                                module: module.clone(),
                                site_byte_start: module_range.start as u32,
                                site_byte_end: module_range.end as u32,
                            });
                        }
                        "pair_pattern" => {
                            let key = child.child_by_field_name("key");
                            let val = child.child_by_field_name("value");
                            if let (Some(k), Some(v)) = (key, val) {
                                let imported = self.text(k).to_string();
                                let local = self.text(v).to_string();
                                self.facts.import_bindings.push(ImportBinding {
                                    kind: ImportKind::Cjs,
                                    local,
                                    imported_name: Some(imported),
                                    module: module.clone(),
                                    site_byte_start: module_range.start as u32,
                                    site_byte_end: module_range.end as u32,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    // ─── ESM imports ─────────────────────────────────────────────────

    fn emit_import_statement(&mut self, node: Node<'_>) {
        let Some(source_node) = node.child_by_field_name("source") else {
            return;
        };
        let Some(module) = string_literal_text(source_node, self.source) else {
            return;
        };
        let r = source_node.byte_range();
        let site_byte_start = r.start as u32;
        let site_byte_end = r.end as u32;

        let mut emitted_any = false;

        if let Some(clause) = find_direct_child(node, "import_clause") {
            // default binding — direct identifier child of import_clause.
            let mut c = clause.walk();
            for child in clause.children(&mut c) {
                if child.kind() == "identifier" {
                    let local = self.text(child).to_string();
                    self.facts.import_bindings.push(ImportBinding {
                        kind: ImportKind::Esm,
                        local,
                        imported_name: Some("default".to_string()),
                        module: module.clone(),
                        site_byte_start,
                        site_byte_end,
                    });
                    emitted_any = true;
                }
            }
            // named imports + namespace import.
            let mut c2 = clause.walk();
            for child in clause.children(&mut c2) {
                match child.kind() {
                    "named_imports" => {
                        let mut nc = child.walk();
                        for spec in child.children(&mut nc) {
                            if spec.kind() != "import_specifier" {
                                continue;
                            }
                            let imported = spec
                                .child_by_field_name("name")
                                .map(|n| self.text(n).to_string());
                            let alias = spec
                                .child_by_field_name("alias")
                                .map(|n| self.text(n).to_string());
                            let Some(imported) = imported else { continue };
                            let local = alias.unwrap_or_else(|| imported.clone());
                            self.facts.import_bindings.push(ImportBinding {
                                kind: ImportKind::Esm,
                                local,
                                imported_name: Some(imported),
                                module: module.clone(),
                                site_byte_start,
                                site_byte_end,
                            });
                            emitted_any = true;
                        }
                    }
                    "namespace_import" => {
                        // `* as Ns` — find the identifier.
                        let mut nc = child.walk();
                        for sub in child.named_children(&mut nc) {
                            if sub.kind() == "identifier" {
                                let local = self.text(sub).to_string();
                                self.facts.import_bindings.push(ImportBinding {
                                    kind: ImportKind::EsmNamespace,
                                    local,
                                    imported_name: None,
                                    module: module.clone(),
                                    site_byte_start,
                                    site_byte_end,
                                });
                                emitted_any = true;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if !emitted_any {
            // Side-effect-only: `import './styles.css'`.
            self.facts.import_bindings.push(ImportBinding {
                kind: ImportKind::SideEffect,
                local: String::new(),
                imported_name: None,
                module,
                site_byte_start,
                site_byte_end,
            });
        }
    }

    // ─── ESM exports ─────────────────────────────────────────────────

    fn emit_export_statement(&mut self, node: Node<'_>) {
        // Shapes (covered):
        //   export default <expr>
        //   export default class X {}
        //   export default function f() {}
        //   export class X {}
        //   export function f() {}
        //   export const X = ...
        //   export { X, Y as Z }
        //   export { X } from './bar'
        //   export * from './bar'
        let source_node = node.child_by_field_name("source");
        let is_default = node
            .children(&mut node.walk())
            .any(|c| !c.is_named() && c.kind() == "default");

        // export {…} from './bar'  /  export * from './bar'
        if let Some(source_node) = source_node {
            let module = string_literal_text(source_node, self.source).unwrap_or_default();
            let r = source_node.byte_range();
            let names = collect_export_clause(node, self.source);
            // If no export_clause, treat as `export *`.
            let mut entries: Vec<(String, String)> = Vec::new();
            if names.is_empty() {
                // We can't enumerate exports of './bar' here; record the
                // wildcard as a single entry with an empty pair (the
                // require_graph treats this as a re-export marker).
            } else {
                for n in names {
                    entries.push(n);
                }
            }
            self.facts.reexports.push(ReExport {
                module,
                names: entries,
                site_byte_start: r.start as u32,
                site_byte_end: r.end as u32,
            });
            // Still register an ImportBinding so we emit an Import row
            // for the `from './bar'` site.
            if let Some(module_text) = string_literal_text(source_node, self.source) {
                self.facts.import_bindings.push(ImportBinding {
                    kind: ImportKind::SideEffect,
                    local: String::new(),
                    imported_name: None,
                    module: module_text,
                    site_byte_start: r.start as u32,
                    site_byte_end: r.end as u32,
                });
            }
            return;
        }

        // Local `export {…}` — no `from`.
        if let Some(clause) = find_direct_child(node, "export_clause") {
            let mut cursor = clause.walk();
            for spec in clause.named_children(&mut cursor) {
                if spec.kind() != "export_specifier" {
                    continue;
                }
                let name = spec
                    .child_by_field_name("name")
                    .map(|n| self.text(n).to_string());
                let alias = spec
                    .child_by_field_name("alias")
                    .map(|n| self.text(n).to_string());
                let Some(name) = name else { continue };
                let exported_as = alias.unwrap_or_else(|| name.clone());
                self.facts.exports.push(ExportBinding {
                    local: name,
                    exported_as,
                });
            }
            return;
        }

        // export default ... / export class / export function / export const
        let declaration = find_direct_child(node, "class_declaration")
            .or_else(|| find_direct_child(node, "function_declaration"))
            .or_else(|| find_direct_child(node, "lexical_declaration"))
            .or_else(|| find_direct_child(node, "variable_declaration"));
        if let Some(decl) = declaration {
            // Collect declared identifiers.
            let names = collect_declared_names(decl, self.source);
            for n in names {
                let exported_as = if is_default {
                    "default".to_string()
                } else {
                    n.clone()
                };
                self.facts.exports.push(ExportBinding {
                    local: n,
                    exported_as,
                });
            }
            return;
        }

        // `export default <expr>` where expr is just an identifier.
        if is_default {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "identifier" {
                    let local = self.text(child).to_string();
                    self.facts.exports.push(ExportBinding {
                        local,
                        exported_as: "default".to_string(),
                    });
                    return;
                }
            }
            // Anonymous default value (`export default { foo: 1 }`) →
            // record the marker with empty local.
            self.facts.exports.push(ExportBinding {
                local: String::new(),
                exported_as: "default".to_string(),
            });
        }
    }

    // ─── CJS module.exports = X / exports.X = ... ────────────────────

    fn emit_assignment(&mut self, node: Node<'_>) {
        let Some(left) = node.child_by_field_name("left") else {
            return;
        };
        let Some(right) = node.child_by_field_name("right") else {
            return;
        };
        if left.kind() != "member_expression" {
            return;
        }
        // Flatten the LHS into a dotted chain.
        let mut parts: Vec<String> = Vec::new();
        if !flatten_member_chain(left, self.source, &mut parts) {
            return;
        }
        // Recognized shapes (only):
        //   module.exports = X
        //   module.exports = { X, Y: Z }
        //   exports.X = <expr>
        //   module.exports.X = <expr>
        let is_module_exports = parts.len() == 2 && parts[0] == "module" && parts[1] == "exports";
        let is_exports_member = parts.len() == 2 && parts[0] == "exports";
        let is_module_exports_member =
            parts.len() == 3 && parts[0] == "module" && parts[1] == "exports";

        if is_module_exports {
            // RHS may be: identifier / object literal / class_expression
            // / function_expression.
            match right.kind() {
                "identifier" => {
                    let local = self.text(right).to_string();
                    self.facts.exports.push(ExportBinding {
                        local,
                        exported_as: "default".to_string(),
                    });
                }
                "object" => {
                    collect_object_exports(right, self.source, &mut self.facts.exports);
                    // Also record the default export shape for `import X
                    // from './foo'` consumers (the whole object).
                }
                "class" | "function_expression" | "arrow_function" => {
                    // Anonymous default — record marker.
                    self.facts.exports.push(ExportBinding {
                        local: String::new(),
                        exported_as: "default".to_string(),
                    });
                }
                _ => {}
            }
        } else if is_exports_member || is_module_exports_member {
            let exported_as = if is_exports_member {
                parts[1].clone()
            } else {
                parts[2].clone()
            };
            let local = if right.kind() == "identifier" {
                self.text(right).to_string()
            } else {
                exported_as.clone()
            };
            self.facts
                .exports
                .push(ExportBinding { local, exported_as });
        }
    }

    // ─── statement / expression / re-export `require(...)` ──────────
    //
    // Type alias kept inside the impl block via the helper below.

    /// Structural recognizer for `require('<literal>')`. Returns the
    /// module specifier text, the byte range of the string-literal
    /// source node (for `ImportBinding.site_byte_*`), and the call site
    /// byte range (for `seen_require_sites`).
    ///
    /// Must match the Tier-1 backend's `is_require_string_call` shape
    /// bit-for-bit:
    ///   * callee is `node.kind() == "identifier"` with text "require"
    ///     (rejects `require.resolve(...)` member callees)
    ///   * first positional arg is `node.kind() == "string"`
    ///     (rejects `require(name)`, ``require(`./` + x)``, etc.)
    fn is_require_string_call(&self, call: Node<'_>) -> Option<RequireCallShape> {
        if call.kind() != "call_expression" {
            return None;
        }
        let func = call.child_by_field_name("function")?;
        if func.kind() != "identifier" || self.text(func) != "require" {
            return None;
        }
        let args = call.child_by_field_name("arguments")?;
        let mut cursor = args.walk();
        let module_node = args.named_children(&mut cursor).next()?;
        if module_node.kind() != "string" {
            return None;
        }
        let module = string_literal_text(module_node, self.source)?;
        let mr = module_node.byte_range();
        let cr = call.byte_range();
        Some((
            module,
            (mr.start as u32, mr.end as u32),
            (cr.start as u32, cr.end as u32),
        ))
    }

    /// Statement-position (`require('./setup');`) and any
    /// expression-position (`app.use(require('./routes'))`,
    /// argument-nested, etc.) `require('<literal>')` call. Emits a
    /// `SideEffect` `ImportBinding` so the require-graph still produces
    /// a module edge / `target_path` — but no `ResolvedBinding` is
    /// created (the existing `kind != SideEffect && !local.is_empty()`
    /// guard in `require_graph.rs` already skips this kind), so the
    /// call doesn't leak a fake alias into `dispatch`'s alias map.
    fn try_emit_expression_position_require(&mut self, call: Node<'_>) {
        let Some((module, module_range, call_range)) = self.is_require_string_call(call) else {
            return;
        };
        if !self.seen_require_sites.insert(call_range) {
            return;
        }
        self.facts.import_bindings.push(ImportBinding {
            kind: ImportKind::SideEffect,
            local: String::new(),
            imported_name: None,
            module,
            site_byte_start: module_range.0,
            site_byte_end: module_range.1,
        });
    }

    /// `module.exports = require('./x')` re-export shape. Edge-only:
    /// `ImportKind::SideEffect` with empty `local`, no
    /// `ResolvedBinding`, and `target_qualified` stays `None` in the
    /// require_graph.
    ///
    /// Named re-export `exports.X = require('./x')` and
    /// `module.exports.X = require('./x')` are deliberately out of
    /// scope for this PR — Tier-2.5 does not yet model re-export
    /// graph semantics, so emitting an `ImportBinding` for the named
    /// shape would mislead `dispatch`'s alias map without any
    /// downstream consumer to resolve it.
    ///
    /// Named re-exports still need active handling: their RHS is a
    /// real `require(...)` call, and the generic `call_expression`
    /// visitor would otherwise emit a plain side-effect
    /// `ImportBinding` for them. We **claim** the RHS call site in
    /// `seen_require_sites` without emitting anything, keeping the
    /// scope-out contract intact end-to-end.
    fn try_emit_reexport_require(&mut self, node: Node<'_>) {
        let Some(left) = node.child_by_field_name("left") else {
            return;
        };
        let Some(right) = node.child_by_field_name("right") else {
            return;
        };
        let Some((module, module_range, call_range)) = self.is_require_string_call(right) else {
            return;
        };

        // Strict `module.exports = require(...)` — emit the re-export
        // edge.
        if self.is_module_exports_member(left) {
            if !self.seen_require_sites.insert(call_range) {
                return;
            }
            self.facts.import_bindings.push(ImportBinding {
                kind: ImportKind::SideEffect,
                local: String::new(),
                imported_name: None,
                module,
                site_byte_start: module_range.0,
                site_byte_end: module_range.1,
            });
            return;
        }

        // Named re-export (`exports.X = require(...)` /
        // `module.exports.X = require(...)`): scope-out per spec.
        // Claim the RHS call site so the generic visitor doesn't
        // fall through and emit a side-effect ImportBinding for it.
        if self.is_named_reexport_lhs(left) {
            self.seen_require_sites.insert(call_range);
        }
    }

    /// `module.exports` exact member-expression LHS.
    fn is_module_exports_member(&self, node: Node<'_>) -> bool {
        if node.kind() != "member_expression" {
            return false;
        }
        let Some(obj) = node.child_by_field_name("object") else {
            return false;
        };
        let Some(prop) = node.child_by_field_name("property") else {
            return false;
        };
        obj.kind() == "identifier"
            && self.text(obj) == "module"
            && prop.kind() == "property_identifier"
            && self.text(prop) == "exports"
    }

    /// Named-re-export LHS shapes: `exports.X` (single-level) or
    /// `module.exports.X` (nested). Both are scope-out for this PR;
    /// we detect them only to suppress the generic visitor's
    /// side-effect emission for their RHS require call.
    fn is_named_reexport_lhs(&self, node: Node<'_>) -> bool {
        if node.kind() != "member_expression" {
            return false;
        }
        let Some(obj) = node.child_by_field_name("object") else {
            return false;
        };
        if obj.kind() == "identifier" && self.text(obj) == "exports" {
            return true;
        }
        self.is_module_exports_member(obj)
    }

    // ─── calls ───────────────────────────────────────────────────────

    fn emit_call(&mut self, node: Node<'_>) {
        let Some(function) = node.child_by_field_name("function") else {
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
            lexical_function: None,
        });
    }
}

// ─── helpers ─────────────────────────────────────────────────────────

/// `(module_specifier_text, module_string_byte_range, call_site_byte_range)`.
/// Returned by `Visitor::is_require_string_call`; bundled so the three
/// distinct emit paths don't each take three positional return values.
type RequireCallShape = (String, (u32, u32), (u32, u32));

fn find_direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

fn string_literal_text(node: Node<'_>, source: &[u8]) -> Option<String> {
    if node.kind() != "string" {
        return None;
    }
    // tree-sitter-javascript wraps the body in a `string_fragment`.
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "string_fragment" {
            return Some(
                std::str::from_utf8(&source[child.byte_range()])
                    .ok()?
                    .to_string(),
            );
        }
    }
    // Empty string (`""`).
    Some(String::new())
}

/// Flatten `a.b.c` into ["a", "b", "c"]; returns false on anything
/// non-dotted.
fn flatten_member_chain(node: Node<'_>, source: &[u8], parts: &mut Vec<String>) -> bool {
    match node.kind() {
        "identifier" | "property_identifier" => {
            if let Ok(t) = std::str::from_utf8(&source[node.byte_range()]) {
                parts.push(t.to_string());
            }
            true
        }
        "this" => {
            parts.push("this".to_string());
            true
        }
        "member_expression" => {
            let Some(obj) = node.child_by_field_name("object") else {
                return false;
            };
            let Some(prop) = node.child_by_field_name("property") else {
                return false;
            };
            if !flatten_member_chain(obj, source, parts) {
                return false;
            }
            if let Ok(t) = std::str::from_utf8(&source[prop.byte_range()]) {
                parts.push(t.to_string());
            }
            true
        }
        _ => false,
    }
}

/// Extract identifier parts from a base expression (the thing on the
/// RHS of `extends`). Returns None for call expressions / anything
/// non-dotted.
fn base_parts(node: Node<'_>, source: &[u8]) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    match node.kind() {
        "identifier" => {
            if let Ok(t) = std::str::from_utf8(&source[node.byte_range()]) {
                parts.push(t.to_string());
                Some(parts)
            } else {
                None
            }
        }
        "member_expression" => {
            if flatten_member_chain(node, source, &mut parts) && !parts.is_empty() {
                Some(parts)
            } else {
                None
            }
        }
        _ => None,
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
        "member_expression" => classify_member_call(function, source),
        _ => (CallReceiver::Unknown, None, function),
    }
}

fn classify_member_call<'tree>(
    member: Node<'tree>,
    source: &[u8],
) -> (CallReceiver, Option<String>, Node<'tree>) {
    let prop = match member.child_by_field_name("property") {
        Some(n) => n,
        None => return (CallReceiver::Unknown, None, member),
    };
    let method = std::str::from_utf8(&source[prop.byte_range()])
        .unwrap_or("")
        .to_string();
    if method.is_empty() {
        return (CallReceiver::Unknown, None, prop);
    }
    let receiver_node = member.child_by_field_name("object");
    let receiver = match receiver_node {
        Some(r) => classify_receiver(r, source),
        None => CallReceiver::Unknown,
    };
    (receiver, Some(method), prop)
}

fn classify_receiver(node: Node<'_>, source: &[u8]) -> CallReceiver {
    match node.kind() {
        "this" => CallReceiver::ThisRef,
        "super" => CallReceiver::SuperRef,
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
        "member_expression" => {
            let mut parts: Vec<String> = Vec::new();
            if flatten_member_chain(node, source, &mut parts) && !parts.is_empty() {
                CallReceiver::Dotted { parts }
            } else {
                CallReceiver::Unknown
            }
        }
        "new_expression" => {
            // `new Foo().bar()` — receiver of bar() is `new Foo()`.
            let Some(constructor) = node.child_by_field_name("constructor") else {
                return CallReceiver::Unknown;
            };
            if constructor.kind() != "identifier" {
                return CallReceiver::Unknown;
            }
            let name = std::str::from_utf8(&source[constructor.byte_range()])
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                CallReceiver::Unknown
            } else {
                CallReceiver::NewExpr { class: name }
            }
        }
        _ => CallReceiver::Unknown,
    }
}

fn collect_export_clause(node: Node<'_>, source: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Some(clause) = find_direct_child(node, "export_clause") else {
        return out;
    };
    let mut cursor = clause.walk();
    for spec in clause.named_children(&mut cursor) {
        if spec.kind() != "export_specifier" {
            continue;
        }
        let name = spec.child_by_field_name("name").and_then(|n| {
            std::str::from_utf8(&source[n.byte_range()])
                .ok()
                .map(|s| s.to_string())
        });
        let alias = spec.child_by_field_name("alias").and_then(|n| {
            std::str::from_utf8(&source[n.byte_range()])
                .ok()
                .map(|s| s.to_string())
        });
        if let Some(name) = name {
            let exported_as = alias.unwrap_or_else(|| name.clone());
            out.push((name, exported_as));
        }
    }
    out
}

fn collect_declared_names(decl: Node<'_>, source: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    match decl.kind() {
        "class_declaration" | "function_declaration" => {
            if let Some(n) = decl.child_by_field_name("name") {
                if let Ok(t) = std::str::from_utf8(&source[n.byte_range()]) {
                    out.push(t.to_string());
                }
            }
        }
        "lexical_declaration" | "variable_declaration" => {
            let mut cursor = decl.walk();
            for declarator in decl.named_children(&mut cursor) {
                if declarator.kind() != "variable_declarator" {
                    continue;
                }
                if let Some(name) = declarator.child_by_field_name("name") {
                    if name.kind() == "identifier" {
                        if let Ok(t) = std::str::from_utf8(&source[name.byte_range()]) {
                            out.push(t.to_string());
                        }
                    }
                }
            }
        }
        _ => {}
    }
    out
}

fn collect_object_exports(obj: Node<'_>, source: &[u8], out: &mut Vec<ExportBinding>) {
    let mut cursor = obj.walk();
    for child in obj.named_children(&mut cursor) {
        match child.kind() {
            "shorthand_property_identifier" => {
                if let Ok(t) = std::str::from_utf8(&source[child.byte_range()]) {
                    out.push(ExportBinding {
                        local: t.to_string(),
                        exported_as: t.to_string(),
                    });
                }
            }
            "pair" => {
                let key = child.child_by_field_name("key");
                let val = child.child_by_field_name("value");
                if let (Some(k), Some(v)) = (key, val) {
                    let key_t = std::str::from_utf8(&source[k.byte_range()])
                        .ok()
                        .map(|s| s.to_string());
                    let val_t = std::str::from_utf8(&source[v.byte_range()])
                        .ok()
                        .map(|s| s.to_string());
                    if let (Some(key_t), Some(val_t)) = (key_t, val_t) {
                        out.push(ExportBinding {
                            local: val_t,
                            exported_as: key_t,
                        });
                    }
                }
            }
            _ => {}
        }
    }
}

// ─── workspace-wide package index ────────────────────────────────────

/// Maps `(path, qualified)` → workspace target. Unlike the C# index
/// which uses globally-unique FQNs, JS qualified names are short
/// (file-local), so we key by (path, name) for direct lookups and also
/// maintain an export-name → path map for cross-file resolution.
#[derive(Debug, Default)]
pub struct PackageIndex {
    /// (path, qualified) → target.
    by_file_qualified: HashMap<(String, String), PackageTarget>,
    /// path → (exported_as → local symbol qualified within target file).
    /// Populated from the per-file export table; consumer-side import
    /// resolution uses this to map `import { X } from './foo'` to
    /// `(foo.js, "X")` (or whatever local name X was exported under).
    exports_by_path: HashMap<String, HashMap<String, String>>,
    /// path → (exported_as → (origin_path, origin_exported_as)) for
    /// `export { X } from './x'` / `export { default as Y } from './x'`
    /// re-export chains. A consumer's `import { X } from './barrel'`
    /// should follow the chain to the origin file, not stop at the
    /// barrel. Populated from `facts.reexports` at build time and
    /// recursively flattened (depth-bounded — see [`MAX_REEXPORT_HOPS`]).
    reexports_by_path: HashMap<String, HashMap<String, (String, String)>>,
    /// `(path, exported_as)` entries whose re-export chain was *dropped*
    /// by [`PackageIndex::build`] because the chain was cyclic or
    /// exceeded [`MAX_REEXPORT_HOPS`]. Distinct from "the file does not
    /// re-export this name at all" — the file DOES syntactically re-export
    /// it (the raw `export { X } from './...'` is present), but we cannot
    /// resolve where it ultimately lands. Consumers of `lookup_export`
    /// (notably `resolve_binding_target`) must consult this set before
    /// falling through to the Tier-2 barrel-fact fallback: if the entry
    /// is dropped, the fallback would fabricate a binding pointing into
    /// the cycle / mid-chain barrel, which is strictly worse than
    /// returning unresolved.
    dropped_reexports: HashSet<(String, String)>,
    /// Class name → all files that define a class with that name.
    /// Used for best-effort unique-name resolution when no import
    /// binding pins the class.
    classes_by_name: HashMap<String, Vec<PackageTarget>>,
}

/// Maximum number of re-export hops we follow when flattening a
/// `barrel → barrel → … → origin` chain. Cycles among re-export files
/// are nonsense at the language level but we still bound the walk
/// defensively. Eight hops is well above any sane real-world depth.
const MAX_REEXPORT_HOPS: usize = 8;

impl PackageIndex {
    pub fn build(per_file: &[(String, Vec<u8>, FileConstFacts)]) -> Self {
        let mut by_file_qualified: HashMap<(String, String), PackageTarget> = HashMap::new();
        let mut exports_by_path: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut classes_by_name: HashMap<String, Vec<PackageTarget>> = HashMap::new();
        // First pass: collect the raw re-export table per file. Each
        // entry maps `exported_as` (as seen by importers of `path`) to
        // `(source_path, source_exported_as)` — the immediate one-hop
        // target file and the name that file in turn exports. We
        // resolve the module specifier via the same workspace probe
        // that `RequireGraph` uses so we converge on the same file id.
        let path_set: std::collections::HashSet<&str> =
            per_file.iter().map(|(p, _, _)| p.as_str()).collect();
        let mut raw_reexports: HashMap<String, HashMap<String, (String, String)>> = HashMap::new();
        for (path, _, facts) in per_file {
            let mut entries: HashMap<String, (String, String)> = HashMap::new();
            for r in &facts.reexports {
                // `export * from './bar'` records an empty `names` list
                // — we can't enumerate `./bar`'s exports here, so skip
                // wildcard re-exports for now (consumers fall back to
                // the existing barrel-targeted resolution).
                let Some(target_path) =
                    crate::require_graph::resolve_module(path, &r.module, &path_set)
                else {
                    continue;
                };
                for (src_name, exported_as) in &r.names {
                    entries.insert(exported_as.clone(), (target_path.clone(), src_name.clone()));
                }
            }
            if !entries.is_empty() {
                raw_reexports.insert(path.clone(), entries);
            }
        }

        for (path, _, facts) in per_file {
            for class in &facts.class_defs {
                let tgt = PackageTarget {
                    path: path.clone(),
                    qualified: class.qualified.clone(),
                };
                by_file_qualified
                    .entry((path.clone(), class.qualified.clone()))
                    .or_insert(tgt.clone());
                classes_by_name
                    .entry(class.qualified.clone())
                    .or_default()
                    .push(tgt);
            }
            for m in &facts.method_defs {
                by_file_qualified
                    .entry((path.clone(), m.qualified.clone()))
                    .or_insert(PackageTarget {
                        path: path.clone(),
                        qualified: m.qualified.clone(),
                    });
            }
            for fdef in &facts.function_defs {
                by_file_qualified
                    .entry((path.clone(), fdef.qualified.clone()))
                    .or_insert(PackageTarget {
                        path: path.clone(),
                        qualified: fdef.qualified.clone(),
                    });
            }

            let mut ex: HashMap<String, String> = HashMap::new();
            for e in &facts.exports {
                ex.insert(e.exported_as.clone(), e.local.clone());
            }
            exports_by_path.insert(path.clone(), ex);
        }

        // Flatten re-export chains: walk each `(path, exported_as)`
        // entry until we land on a file that *defines* (not re-exports)
        // the symbol. The flattened map is what `lookup_export`
        // consults; downstream consumers can treat it as a single-hop
        // alias from the barrel's exported name to the origin file's
        // local symbol name.
        //
        // Two failure modes deliberately produce *no* entry (so
        // `lookup_export` returns None rather than a nonsensical or
        // truncated origin):
        //   1. Cycle — e.g. `a re-exports X from b; b re-exports X from
        //      a`. We track `visited: HashSet<(path, name)>` along the
        //      chain; revisiting any node aborts the walk without
        //      inserting. Without this guard, the flattened entry would
        //      point back into the cycle and `lookup_export`'s recursion
        //      would stack-overflow.
        //   2. Chain longer than [`MAX_REEXPORT_HOPS`]. Inserting a
        //      truncated mid-chain entry would (a) misresolve consumers
        //      to a non-origin barrel and (b) risk lookup_export
        //      recursing through a chain that still has more re-export
        //      hops left. Dropping the entry entirely is safer than
        //      silently lying.
        let mut reexports_by_path: HashMap<String, HashMap<String, (String, String)>> =
            HashMap::new();
        let mut dropped_reexports: HashSet<(String, String)> = HashSet::new();
        for (path, entries) in &raw_reexports {
            let mut flat: HashMap<String, (String, String)> = HashMap::new();
            for (exported_as, (first_path, first_name)) in entries {
                let mut visited: std::collections::HashSet<(String, String)> =
                    std::collections::HashSet::new();
                visited.insert((path.clone(), exported_as.clone()));
                let mut cur_path = first_path.clone();
                let mut cur_name = first_name.clone();
                let mut terminated = false;
                let mut cycled = false;
                for _ in 0..MAX_REEXPORT_HOPS {
                    if !visited.insert((cur_path.clone(), cur_name.clone())) {
                        // Already on the chain — cycle. Abort without
                        // inserting (see comment above).
                        cycled = true;
                        break;
                    }
                    match raw_reexports.get(&cur_path).and_then(|m| m.get(&cur_name)) {
                        Some((next_path, next_name)) => {
                            cur_path = next_path.clone();
                            cur_name = next_name.clone();
                        }
                        None => {
                            terminated = true;
                            break;
                        }
                    }
                }
                if cycled || !terminated {
                    // Either looped or ran out of hop budget without
                    // landing on a file that locally defines the symbol.
                    // Drop the entry so consumers fall back to None — and
                    // record `(path, exported_as)` in `dropped_reexports`
                    // so `resolve_binding_target` can distinguish "file
                    // re-exports this name but the chain was unresolvable"
                    // from "file doesn't re-export this name at all". The
                    // former must NOT fall through to the Tier-2 barrel-
                    // fact fallback (it would fabricate a binding pointing
                    // into the cycle / mid-chain barrel — see R2 dogfood
                    // catch in v0.7.0 cycle-fix follow-up).
                    dropped_reexports.insert((path.clone(), exported_as.clone()));
                    continue;
                }
                flat.insert(exported_as.clone(), (cur_path, cur_name));
            }
            if !flat.is_empty() {
                reexports_by_path.insert(path.clone(), flat);
            }
        }

        Self {
            by_file_qualified,
            exports_by_path,
            reexports_by_path,
            dropped_reexports,
            classes_by_name,
        }
    }

    /// Returns `true` when `path` syntactically re-exports `exported_as`
    /// (`export { exported_as } from './…'`) but its chain was dropped
    /// at build time because of a cycle or because it exceeded
    /// [`MAX_REEXPORT_HOPS`]. Callers use this to distinguish a
    /// genuine "the file has its own local `exported_as`" miss in
    /// `lookup_export` from "the file re-exports it but we cannot
    /// pin where it lands" — only the latter must suppress the
    /// Tier-2 barrel-fact fallback to avoid fabricating a binding
    /// pointing into the cycle.
    pub fn is_reexport_dropped(&self, path: &str, exported_as: &str) -> bool {
        self.dropped_reexports
            .contains(&(path.to_string(), exported_as.to_string()))
    }

    /// Find a symbol in `path` by qualified name (short FQN, e.g. "X"
    /// or "X.method"). Returns None if not declared in that file.
    pub fn lookup_in_file(&self, path: &str, qualified: &str) -> Option<&PackageTarget> {
        self.by_file_qualified
            .get(&(path.to_string(), qualified.to_string()))
    }

    /// Find an exported name in a target file: returns the
    /// PackageTarget for the local definition behind that export name.
    ///
    /// If `target_path` is a barrel that re-exports `exported_as` from
    /// another file (`export { X } from './x'`), this follows the
    /// re-export chain to the origin file (depth-bounded by
    /// [`MAX_REEXPORT_HOPS`], chains pre-flattened at build time) and
    /// returns the PackageTarget in the origin file. This is what makes
    /// `import { X } from './barrel'` consumers land on `x.js` rather
    /// than the barrel.
    pub fn lookup_export(&self, target_path: &str, exported_as: &str) -> Option<&PackageTarget> {
        // Iterative walk with a `visited` guard. Build-time flattening
        // already drops cyclic and over-budget chains (see
        // `PackageIndex::build`), so `reexports_by_path` is guaranteed
        // to point at a terminal origin — but we keep the guard here
        // as a belt-and-suspenders defense against future regressions
        // that could otherwise blow the stack on adversarial input.
        let mut cur_path = target_path.to_string();
        let mut cur_name = exported_as.to_string();
        let mut visited: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        for _ in 0..=MAX_REEXPORT_HOPS {
            if !visited.insert((cur_path.clone(), cur_name.clone())) {
                return None;
            }
            // 1. Direct local export at the current file.
            if let Some(local) = self
                .exports_by_path
                .get(&cur_path)
                .and_then(|m| m.get(&cur_name))
            {
                if let Some(hit) = self
                    .by_file_qualified
                    .get(&(cur_path.clone(), local.clone()))
                {
                    return Some(hit);
                }
            }
            // 2. Re-export chain: barrel forwards `cur_name` to
            //    `(origin_path, origin_exported_as)`. Because the build
            //    step pre-flattens to a terminal origin, this loop
            //    normally takes at most one extra iteration.
            match self
                .reexports_by_path
                .get(&cur_path)
                .and_then(|m| m.get(&cur_name))
            {
                Some((origin_path, origin_exported)) => {
                    cur_path = origin_path.clone();
                    cur_name = origin_exported.clone();
                }
                None => return None,
            }
        }
        None
    }

    /// Resolve a barrel-forwarded name to its origin `(path,
    /// exported_as)` pair without requiring the origin file to have
    /// indexed the corresponding class/function. Returns `None` if the
    /// barrel doesn't re-export `exported_as`. Used by the require-
    /// graph binding resolver so an import row's `target_path` follows
    /// the re-export chain even when the origin lives outside the
    /// indexed symbol table (e.g. plain `export const X = ...`).
    pub fn resolve_reexport(
        &self,
        target_path: &str,
        exported_as: &str,
    ) -> Option<(String, String)> {
        self.reexports_by_path
            .get(target_path)
            .and_then(|m| m.get(exported_as))
            .cloned()
    }

    /// Same as `lookup_in_file` but doesn't require knowing the path
    /// in advance — picks the unique definition across the workspace.
    pub fn lookup_unique_class(&self, name: &str) -> Option<&PackageTarget> {
        let bucket = self.classes_by_name.get(name)?;
        if bucket.len() == 1 {
            bucket.first()
        } else {
            None
        }
    }

    /// Lookup a class by its short qualified name *within the given
    /// file*. Used by `resolve_dotted_type`'s same-file fast path: the
    /// visitor knows the qualified short name lives in `path`, and we
    /// must scope the lookup so that workspaces with multiple files
    /// defining the same class name don't bleed into the wrong one
    /// (e.g. file A's `extends Foo` resolving to file B's `Foo` just
    /// because B was indexed first).
    pub fn lookup_class_in_file(&self, path: &str, qualified: &str) -> Option<&PackageTarget> {
        let target = self
            .by_file_qualified
            .get(&(path.to_string(), qualified.to_string()))?;
        // Scope-check: the (path, qualified) index also carries
        // method / function entries; restrict to classes by verifying
        // the qualified name appears in the class bucket for this
        // path.
        let bucket = self.classes_by_name.get(qualified)?;
        bucket.iter().find(|t| t.path == path).map(|_| target)
    }
}
