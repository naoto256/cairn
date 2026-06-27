//! `cairn-lang-typescript` — TypeScript / TSX / JavaScript backends.
//!
//! Tier-1 (syntactic): walks the parse tree and emits symbols for
//! declarations plus import facts. Three sibling grammars share one
//! visitor: tree-sitter-typescript's TypeScript and TSX dialects, and
//! tree-sitter-javascript (which parses JSX natively, so `.jsx` rides
//! on the JavaScript backend). Node kinds that only exist in one
//! dialect (e.g. `interface_declaration`) simply never fire for the
//! others.

#![forbid(unsafe_code)]

mod analyzer;

use std::collections::HashSet;
use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SymbolScope, SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice, truncate,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Which of the three sibling grammars a backend instance drives.
/// Tier-1 and Tier-2 walk the same node kinds for all of them; the
/// dialect only selects the grammar and the identifying strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dialect {
    Typescript,
    Tsx,
    Javascript,
}

impl Dialect {
    pub(crate) fn language(self) -> tree_sitter::Language {
        match self {
            Self::Typescript => tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            Self::Tsx => tree_sitter_typescript::LANGUAGE_TSX.into(),
            Self::Javascript => tree_sitter_javascript::LANGUAGE.into(),
        }
    }
}

/// TypeScript backend instance (`.ts` / `.mts` / `.cts`).
pub struct TypescriptBackend;

impl LanguageBackend for TypescriptBackend {
    fn name(&self) -> &'static str {
        "typescript"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.ts", "*.mts", "*.cts"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-typescript"
    }

    /// Revision 4: nested function declarations now carry
    /// `SymbolScope::Nested` so `find_symbols` filters them out of
    /// workspace lookup while `get_outline` keeps showing them. Same
    /// input yields the same symbol rows but with a new `scope`
    /// column value, so the CAS-cached syntactic snapshot must be
    /// invalidated.
    ///
    /// Revision 3: `require('./x')` is now emitted as `ImportFact` for
    /// statement-position (`require('./setup')` as a top-level
    /// expression statement), expression-position (`app.use(require(...))`,
    /// argument-nested), and `module.exports = require('./x')` re-export
    /// shapes, in addition to the binding-form (`const X = require(...)`)
    /// that revision 2 introduced. Same input yields more `ImportFact`
    /// rows, so the CAS-cached syntactic snapshot must be invalidated.
    fn parser_revision(&self) -> u32 {
        4
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        extract(
            source,
            &Dialect::Typescript.language(),
            TypescriptVisitor::new(),
        )
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer(Dialect::Typescript))
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_TYPESCRIPT: fn() -> Box<dyn LanguageBackend> = || Box::new(TypescriptBackend);

/// TSX backend instance (`.tsx`). Same visitor as TypeScript over the
/// upstream TSX grammar (JSX syntax changes how a handful of TS
/// constructs parse, hence the separate grammar and backend).
pub struct TsxBackend;

impl LanguageBackend for TsxBackend {
    fn name(&self) -> &'static str {
        "tsx"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.tsx"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-tsx"
    }

    /// See [`TypescriptBackend::parser_revision`].
    fn parser_revision(&self) -> u32 {
        4
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        extract(source, &Dialect::Tsx.language(), TypescriptVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer(Dialect::Tsx))
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_TSX: fn() -> Box<dyn LanguageBackend> = || Box::new(TsxBackend);

/// JavaScript backend instance (`.js` / `.mjs` / `.cjs` / `.jsx`).
/// tree-sitter-javascript parses JSX natively, so `.jsx` needs no
/// separate grammar.
pub struct JavascriptBackend;

impl LanguageBackend for JavascriptBackend {
    fn name(&self) -> &'static str {
        "javascript"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.js", "*.mjs", "*.cjs", "*.jsx"]
    }

    fn shebang_patterns(&self) -> &'static [&'static str] {
        // Substrings searched in the trimmed first line. Covers both
        // `#!/usr/bin/node` and `#!/usr/bin/env node`.
        &["node"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-javascript"
    }

    /// See [`TypescriptBackend::parser_revision`].
    fn parser_revision(&self) -> u32 {
        4
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        extract(
            source,
            &Dialect::Javascript.language(),
            TypescriptVisitor::new(),
        )
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer(Dialect::Javascript))
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_JAVASCRIPT: fn() -> Box<dyn LanguageBackend> = || Box::new(JavascriptBackend);

struct TypescriptVisitor {
    nesting: NestingTracker,
    /// Byte ranges of `require(...)` call sites we have already emitted
    /// an `ImportFact` for. Key is the `call_expression` node's byte
    /// range — that uniquely identifies the call site even when two
    /// different visit paths (binding-form `extract_cjs_requires` and
    /// the generic statement / expression / re-export visitors below)
    /// reach the same node. Keying on the module-literal range alone
    /// would conflate distinct sites that happen to share the same
    /// specifier.
    seen_require_sites: HashSet<(usize, usize)>,
    /// End-byte positions of every function-body frame currently open.
    /// `function_declaration` / `function_expression` / `arrow_function`
    /// / `generator_function` / `generator_function_declaration` /
    /// bare `function` / `method_definition` all push their end byte
    /// here; the stack is drained from the top on every visit by
    /// dropping entries whose end byte is at or before the current
    /// node start. A symbol declared while this stack is non-empty is
    /// `SymbolScope::Nested` — file-local, not workspace-addressable.
    /// Mirrors the `function_depth` counter the Tier-2.5 JS backend
    /// uses for the same purpose, expressed as a byte-end stack so the
    /// generic top-down `Visitor::visit_node` walk (which has no
    /// pre/post hook to bracket descents) stays correct.
    function_frame_ends: Vec<usize>,
}

impl TypescriptVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
            seen_require_sites: HashSet::new(),
            function_frame_ends: Vec::new(),
        }
    }

    fn pop_function_frames(&mut self, byte_start: usize) {
        while let Some(&end) = self.function_frame_ends.last() {
            if end <= byte_start {
                self.function_frame_ends.pop();
            } else {
                break;
            }
        }
    }

    fn current_scope(&self) -> SymbolScope {
        if self.function_frame_ends.is_empty() {
            SymbolScope::TopLevel
        } else {
            SymbolScope::Nested
        }
    }
}

impl Visitor for TypescriptVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.pop_function_frames(node.start_byte());
        self.nesting.pop_outside(node.start_byte());

        // Anonymous / expression-shaped function scopes that don't emit
        // their own SymbolFact: opening their body must still mark any
        // declarations inside as Nested. Named function/method bodies
        // are pushed at the bottom of this function so the symbol's own
        // name keeps the outer scope.
        if matches!(
            node.kind(),
            "function_expression" | "arrow_function" | "generator_function" | "function"
        ) {
            self.function_frame_ends.push(node.end_byte());
        }

        if node.kind() == "import_statement" {
            extract_imports(node, source, facts);
            return;
        }

        // CommonJS `const X = require('./foo')` and friends. tree-sitter
        // surfaces these as `lexical_declaration` (let/const) or
        // `variable_declaration` (var). Don't `return` — leaf nodes
        // underneath still need normal symbol classification (none of
        // the require shapes themselves carry a SymbolKind, but a
        // declarator whose RHS is *not* a require call must fall through
        // unchanged).
        if matches!(node.kind(), "lexical_declaration" | "variable_declaration") {
            extract_cjs_requires(node, source, facts, &mut self.seen_require_sites);
        }

        // Expression-position `require('./x')`: every `call_expression`
        // whose callee is the bare identifier `require` and whose first
        // argument is a string literal becomes an `ImportFact`. This
        // catches statement-position calls (`require('./setup');`),
        // argument-nested calls (`app.use(require('./routes'))`), and
        // every other syntactic position. Binding-form
        // (`const X = require(...)`) is emitted with richer
        // `imported`/`alias` info by `extract_cjs_requires` above; the
        // shared `seen_require_sites` set guarantees this generic visitor
        // doesn't re-emit it.
        if node.kind() == "call_expression" {
            extract_expression_position_require(node, source, facts, &mut self.seen_require_sites);
        }

        // `module.exports = require('./x')` re-export shape. Visited at
        // the `assignment_expression` level so we can structurally match
        // the LHS as `member_expression(module.exports)` before
        // committing to a re-export emission. The call site is still
        // tracked in `seen_require_sites` so the generic
        // `call_expression` visitor above doesn't also emit a plain
        // side-effect ImportFact for the same RHS.
        if node.kind() == "assignment_expression" {
            extract_reexport_require(node, source, facts, &mut self.seen_require_sites);
        }

        let Some((mut kind, name, body_start)) = match_typescript_item(node, source) else {
            return;
        };

        if matches!(kind, SymbolKind::Function)
            && matches!(self.nesting.parent_kind(facts), Some(SymbolKind::Class))
        {
            kind = SymbolKind::Method;
        }

        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = typescript_visibility(node, source);
        let doc = extract_jsdoc(node, source);
        let parent_idx = self.nesting.current_parent();

        // Resolve scope *before* pushing the function-body frame
        // below: the declared name itself lives in the enclosing
        // scope, so a top-level `function foo` is TopLevel even
        // though its body opens a frame. Nested declarations
        // (declared while a function frame is already open) get
        // SymbolScope::Nested so `find_symbols` can filter them out
        // of workspace lookup without dropping them from
        // `get_outline`.
        let scope = self.current_scope();

        let idx = facts.symbols.len();
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind: kind.clone(),
            signature,
            doc,
            visibility,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start,
            parent_idx,
            scope,
        });

        if is_container(&kind) {
            self.nesting.push(idx, node.end_byte());
        }

        // Open a function-body frame so anything declared inside is
        // marked `Nested`. Includes named declarations and method
        // bodies (whose own name was just emitted in the outer scope).
        if matches!(
            node.kind(),
            "function_declaration" | "generator_function_declaration" | "method_definition"
        ) {
            self.function_frame_ends.push(node.end_byte());
        }
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Class | SymbolKind::Interface | SymbolKind::Enum
    )
}

fn match_typescript_item(
    node: Node<'_>,
    source: &[u8],
) -> Option<(SymbolKind, String, Option<usize>)> {
    match node.kind() {
        "function_declaration" | "generator_function_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Function,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "method_definition" | "method_signature" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Method,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "class_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Class, node_text(name, source).to_string(), body))
        }
        "interface_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Interface,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "type_alias_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::TypeAlias,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "enum_declaration" => {
            let name = child_by_field(node, "name")?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Enum, node_text(name, source).to_string(), body))
        }
        _ => None,
    }
}

fn typescript_visibility(node: Node<'_>, source: &[u8]) -> Option<Visibility> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "accessibility_modifier" {
            return match node_text(child, source) {
                "public" => Some(Visibility::Public),
                "private" => Some(Visibility::Private),
                "protected" => Some(Visibility::Crate),
                _ => None,
            };
        }
    }
    None
}

fn extract_jsdoc(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| {
        if sibling.kind() != "comment" {
            return None;
        }
        if text.trim_start().starts_with("/**") {
            Some(DocCommentPart::Replace(strip_jsdoc_markers(text)))
        } else {
            Some(DocCommentPart::Reset)
        }
    })
    .filter(|doc| !doc.is_empty())
}

fn strip_jsdoc_markers(text: &str) -> String {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix("/**")
        .and_then(|s| s.strip_suffix("*/"))
        .unwrap_or(trimmed);

    let lines: Vec<&str> = inner
        .lines()
        .map(|line| {
            line.trim()
                .strip_prefix('*')
                .map(str::trim_start)
                .unwrap_or_else(|| line.trim())
        })
        .filter(|line| !line.is_empty())
        .collect();
    truncate(&lines.join("\n"), 1024)
}

fn extract_imports(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let Some(source_node) = child_by_field(node, "source") else {
        return;
    };
    let to_module = strip_string_literal(node_text(source_node, source));
    let line = line_of(node);
    // The `from './foo'` source string node. Tier-2.5 (JS) needs this
    // span aligned so its Import resolutions can pin onto the Tier-1
    // ImportFact at the path span (mirroring CSharp/Swift/etc.). Emit
    // for every dialect — TS / TSX / JS share this extractor and TS
    // tier3 doesn't consume `byte_range` yet, so the wider emit is
    // forward-compatible.
    let source_byte_range: (u32, u32) = {
        let r = source_node.byte_range();
        (r.start as u32, r.end as u32)
    };

    // tree-sitter-typescript shape:
    //   import_statement
    //     ├─ "type"? (type-only modifier — handled transparently)
    //     ├─ import_clause?
    //     │   ├─ identifier             (default binding)
    //     │   ├─ named_imports          ({ a, b as c })
    //     │   │   └─ import_specifier+
    //     │   └─ namespace_import       (* as ns)
    //     └─ source = string
    //
    // Each binding form (default / named / namespace) emits an independent
    // `ImportFact`; the previous implementation guarded the default emit
    // on "no named found", which dropped the default in mixed forms like
    // `import React, { useState } from 'react'`.
    let mut emitted_any = false;

    if let Some(clause) = find_child_by_kind(node, "import_clause") {
        // 1. default binding — direct `identifier` child of import_clause
        if let Some(default_alias) = default_alias_in_clause(clause, source) {
            facts.imports.push(ImportFact {
                to_module: to_module.clone(),
                imported: Some("default".to_string()),
                alias: Some(default_alias),
                is_reexport: false,
                line,

                byte_range: Some(source_byte_range),
            });
            emitted_any = true;
        }

        // 2. named + namespace
        let mut cursor = clause.walk();
        for child in clause.children(&mut cursor) {
            match child.kind() {
                "named_imports" => {
                    let mut nc = child.walk();
                    for spec in child.children(&mut nc) {
                        if spec.kind() != "import_specifier" {
                            continue;
                        }
                        if let Some((imported, alias)) = import_specifier_parts(spec, source) {
                            facts.imports.push(ImportFact {
                                to_module: to_module.clone(),
                                imported: Some(imported),
                                alias,
                                is_reexport: false,
                                line,

                                byte_range: Some(source_byte_range),
                            });
                            emitted_any = true;
                        }
                    }
                }
                "namespace_import" => {
                    if let Some(alias) = namespace_alias(child, source) {
                        facts.imports.push(ImportFact {
                            to_module: to_module.clone(),
                            imported: Some("*".to_string()),
                            alias: Some(alias),
                            is_reexport: false,
                            line,

                            byte_range: Some(source_byte_range),
                        });
                        emitted_any = true;
                    }
                }
                _ => {}
            }
        }
    }

    // Side-effect-only: `import "./styles.css"` — no clause, or a clause
    // that didn't yield any concrete binding.
    if !emitted_any {
        facts.imports.push(ImportFact {
            to_module,
            imported: None,
            alias: None,
            is_reexport: false,
            line,

            byte_range: Some(source_byte_range),
        });
    }
}

/// Structural test for a `call_expression` of the bare identifier
/// `require` with a single static string literal argument. Returns the
/// module specifier text (quotes stripped), the byte range of the
/// string-literal source node (for `ImportFact.byte_range`), and the
/// byte range of the call site itself (for `seen_require_sites`).
///
/// The shape this accepts must match the Tier-2.5 JavaScript backend's
/// recognizer in `crates/cairn-lang-javascript-tier25/src/const_resolver.rs`
/// **bit for bit**:
///   * callee is `node.kind() == "identifier"` with text `"require"`.
///     Member-expression callees (`require.resolve('./x')`) are rejected
///     structurally — the call's `function` field is not an identifier.
///   * the first positional argument is `node.kind() == "string"`.
///     `template_string`, `identifier` (`require(name)`), and
///     `binary_expression` (`` require(`./` + name) ``) are all rejected.
///
/// Both backends must reject the same shapes; the regression tests
/// `require_dot_resolve_does_not_emit_*`, `dynamic_require_with_*`, and
/// `template_string_require_*` pin this contract.
/// `(module_specifier_text, module_string_byte_range, call_site_byte_range)`.
/// Three coordinates the require-emit callers need together.
type RequireCallShape = (String, (u32, u32), (usize, usize));

fn is_require_string_call(call: Node<'_>, source: &[u8]) -> Option<RequireCallShape> {
    if call.kind() != "call_expression" {
        return None;
    }
    let func = child_by_field(call, "function")?;
    if func.kind() != "identifier" || node_text(func, source) != "require" {
        return None;
    }
    let args = child_by_field(call, "arguments")?;
    let mut arg_cursor = args.walk();
    let module_node = args.named_children(&mut arg_cursor).next()?;
    if module_node.kind() != "string" {
        return None;
    }
    let module_text = strip_string_literal(node_text(module_node, source));
    let mr = module_node.byte_range();
    let module_range = (mr.start as u32, mr.end as u32);
    let cr = call.byte_range();
    let call_range = (cr.start, cr.end);
    Some((module_text, module_range, call_range))
}

/// Statement-position / expression-position generic visitor. Fires for
/// every `call_expression` node and emits a side-effect-shaped
/// `ImportFact` (`imported = None, alias = None, is_reexport = false`)
/// if the call matches `require('<literal>')`. The shared
/// `seen_require_sites` set suppresses duplicates with the binding-form
/// path (`extract_cjs_requires`) and the re-export path
/// (`extract_reexport_require`); both populate the same set before this
/// visitor runs against the same `call_expression`.
fn extract_expression_position_require(
    call: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    seen: &mut HashSet<(usize, usize)>,
) {
    let Some((to_module, module_range, call_range)) = is_require_string_call(call, source) else {
        return;
    };
    if !seen.insert(call_range) {
        return;
    }
    facts.imports.push(ImportFact {
        to_module,
        imported: None,
        alias: None,
        is_reexport: false,
        line: line_of(call),
        byte_range: Some(module_range),
    });
}

/// `module.exports = require('./x')` re-export shape. We accept only
/// the strict `member_expression(object=identifier("module"),
/// property=identifier("exports"))` LHS form — the `exports.X =
/// require('./x')` and `module.exports.X = require('./x')` named
/// re-exports are intentionally out of scope for this PR (Tier-2.5
/// cannot yet model named re-export graph semantics, so emitting a
/// `ImportFact { is_reexport: true }` for them would mislead
/// downstream consumers).
///
/// Named re-export shapes still need active handling: their RHS is a
/// real `require(...)` call, and the generic `call_expression`
/// visitor would otherwise emit a plain side-effect `ImportFact` for
/// them. We **claim** the RHS call site in `seen_require_sites`
/// without emitting anything, suppressing that downstream emission
/// and keeping the scope-out contract intact end-to-end. The
/// negative tests `exports_named_reexport_does_not_emit_import_fact`
/// and `module_exports_named_reexport_does_not_emit_import_fact`
/// pin this.
///
/// The Tier-2.5 backend treats the strict `module.exports = require(...)`
/// shape as edge-only as well (`ImportKind::SideEffect`, no
/// `ResolvedBinding`).
fn extract_reexport_require(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    seen: &mut HashSet<(usize, usize)>,
) {
    if node.kind() != "assignment_expression" {
        return;
    }
    let Some(left) = child_by_field(node, "left") else {
        return;
    };
    let Some(right) = child_by_field(node, "right") else {
        return;
    };
    // Only act on assignment whose RHS is a real `require('...')` call;
    // anything else is none of our business.
    let Some((to_module, module_range, call_range)) = is_require_string_call(right, source) else {
        return;
    };

    // Strict `module.exports = require(...)` — emit the re-export
    // ImportFact and claim the call site.
    if is_module_exports_member(left, source) {
        if !seen.insert(call_range) {
            return;
        }
        facts.imports.push(ImportFact {
            to_module,
            imported: None,
            alias: None,
            is_reexport: true,
            line: line_of(node),
            byte_range: Some(module_range),
        });
        return;
    }

    // Named re-export shapes (`exports.X = require(...)` /
    // `module.exports.X = require(...)`): scope-out per spec. Claim
    // the RHS call site so the generic `call_expression` visitor
    // doesn't fall through and emit a side-effect ImportFact for it.
    if is_named_reexport_lhs(left, source) {
        seen.insert(call_range);
    }
}

/// `module.exports` exact member-expression LHS.
fn is_module_exports_member(node: Node<'_>, source: &[u8]) -> bool {
    if node.kind() != "member_expression" {
        return false;
    }
    let Some(obj) = child_by_field(node, "object") else {
        return false;
    };
    let Some(prop) = child_by_field(node, "property") else {
        return false;
    };
    obj.kind() == "identifier"
        && node_text(obj, source) == "module"
        && prop.kind() == "property_identifier"
        && node_text(prop, source) == "exports"
}

/// Named-re-export LHS shapes: `exports.X` (single-level) or
/// `module.exports.X` (nested). Both are scope-out for this PR; we
/// detect them only to suppress the generic visitor's side-effect
/// emission for their RHS require call.
fn is_named_reexport_lhs(node: Node<'_>, source: &[u8]) -> bool {
    if node.kind() != "member_expression" {
        return false;
    }
    let Some(obj) = child_by_field(node, "object") else {
        return false;
    };
    // `exports.X` — obj is the bare identifier `exports`.
    if obj.kind() == "identifier" && node_text(obj, source) == "exports" {
        return true;
    }
    // `module.exports.X` — obj is itself a `module.exports`
    // member_expression.
    is_module_exports_member(obj, source)
}

/// Detect CommonJS `require(...)` bindings inside a `lexical_declaration`
/// or `variable_declaration` and emit `ImportFact`s for each.
///
/// Shapes handled (mirrors the Tier-2.5 JavaScript backend's
/// `try_emit_cjs_require`):
///   * `const X = require('./foo')`                  → `imported="default"`
///   * `const X = require('./foo').Y`                → `imported="Y"`
///   * `const { X, Y: Z } = require('./foo')`        → one fact per binding
///
/// `let` / `var` are accepted too — emission policy is shape-driven,
/// not binding-form-driven, so any rebinding pattern resolves to the
/// same module. Non-require RHS values are silently ignored.
///
/// Emitted on all three dialects (TS / TSX / JS). CommonJS in `.ts`
/// is rare but valid and shows up in mixed CJS/ESM codebases.
fn extract_cjs_requires(
    node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    seen: &mut HashSet<(usize, usize)>,
) {
    let mut cursor = node.walk();
    for declarator in node.named_children(&mut cursor) {
        if declarator.kind() != "variable_declarator" {
            continue;
        }
        let Some(name_node) = child_by_field(declarator, "name") else {
            continue;
        };
        let Some(value_node) = child_by_field(declarator, "value") else {
            continue;
        };
        try_emit_cjs_require(name_node, value_node, source, facts, seen);
    }
}

fn try_emit_cjs_require(
    name_node: Node<'_>,
    value_node: Node<'_>,
    source: &[u8],
    facts: &mut SyntacticFacts,
    seen: &mut HashSet<(usize, usize)>,
) {
    // RHS shape: `require('./foo')` or `require('./foo').Member`.
    let (require_call, member): (Node<'_>, Option<String>) = match value_node.kind() {
        "call_expression" => (value_node, None),
        "member_expression" => {
            let Some(obj) = child_by_field(value_node, "object") else {
                return;
            };
            let Some(prop) = child_by_field(value_node, "property") else {
                return;
            };
            if obj.kind() != "call_expression" {
                return;
            }
            (obj, Some(node_text(prop, source).to_string()))
        }
        _ => return,
    };

    let Some(func) = child_by_field(require_call, "function") else {
        return;
    };
    if func.kind() != "identifier" || node_text(func, source) != "require" {
        return;
    }
    let Some(args) = child_by_field(require_call, "arguments") else {
        return;
    };

    // First positional argument must be a string literal.
    let mut arg_cursor = args.walk();
    let Some(module_node) = args.named_children(&mut arg_cursor).next() else {
        return;
    };
    if module_node.kind() != "string" {
        return;
    }
    let to_module = strip_string_literal(node_text(module_node, source));
    let module_range = module_node.byte_range();
    let source_byte_range: (u32, u32) = (module_range.start as u32, module_range.end as u32);
    let line = line_of(require_call);

    // Mark the require call site as seen so the generic
    // `extract_expression_position_require` visitor doesn't also emit a
    // bare side-effect ImportFact for the same call. We register
    // unconditionally — even if the destructure pattern below yields no
    // bindings, the binding-form path "claims" this call.
    let call_range = require_call.byte_range();
    seen.insert((call_range.start, call_range.end));

    match name_node.kind() {
        "identifier" => {
            let alias = node_text(name_node, source).to_string();
            let imported = member.unwrap_or_else(|| "default".to_string());
            facts.imports.push(ImportFact {
                to_module,
                imported: Some(imported),
                alias: Some(alias),
                is_reexport: false,
                line,
                byte_range: Some(source_byte_range),
            });
        }
        "object_pattern" => {
            // `const { X, Y: Z } = require('./foo')`
            let mut cursor = name_node.walk();
            for child in name_node.named_children(&mut cursor) {
                match child.kind() {
                    "shorthand_property_identifier_pattern" => {
                        let n = node_text(child, source).to_string();
                        facts.imports.push(ImportFact {
                            to_module: to_module.clone(),
                            imported: Some(n.clone()),
                            alias: Some(n),
                            is_reexport: false,
                            line,
                            byte_range: Some(source_byte_range),
                        });
                    }
                    "pair_pattern" => {
                        let Some(key) = child_by_field(child, "key") else {
                            continue;
                        };
                        let Some(val) = child_by_field(child, "value") else {
                            continue;
                        };
                        let imported = node_text(key, source).to_string();
                        let alias = node_text(val, source).to_string();
                        facts.imports.push(ImportFact {
                            to_module: to_module.clone(),
                            imported: Some(imported),
                            alias: Some(alias),
                            is_reexport: false,
                            line,
                            byte_range: Some(source_byte_range),
                        });
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn find_child_by_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

fn default_alias_in_clause(clause: Node<'_>, source: &[u8]) -> Option<String> {
    // The default binding is the direct `identifier` child of
    // `import_clause`. Identifiers nested inside `named_imports` /
    // `namespace_import` belong to those forms and must not be picked up.
    let mut cursor = clause.walk();
    for child in clause.children(&mut cursor) {
        if child.kind() == "identifier" {
            return Some(node_text(child, source).to_string());
        }
    }
    None
}

fn namespace_alias(node: Node<'_>, source: &[u8]) -> Option<String> {
    child_by_field(node, "name")
        .map(|n| node_text(n, source).to_string())
        .or_else(|| last_identifier(node, source))
}

fn import_specifier_parts(node: Node<'_>, source: &[u8]) -> Option<(String, Option<String>)> {
    let name = child_by_field(node, "name")?;
    let imported = node_text(name, source).to_string();
    let alias = child_by_field(node, "alias").map(|n| node_text(n, source).to_string());
    Some((imported, alias))
}

fn last_identifier(node: Node<'_>, source: &[u8]) -> Option<String> {
    let mut out = None;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "identifier" || child.kind() == "type_identifier" {
            out = Some(node_text(child, source).to_string());
        } else {
            out = last_identifier(child, source).or(out);
        }
    }
    out
}

fn strip_string_literal(text: &str) -> String {
    let trimmed = text.trim();
    for quote in ['"', '\'', '`'] {
        if let Some(inner) = trimmed
            .strip_prefix(quote)
            .and_then(|s| s.strip_suffix(quote))
        {
            return inner.to_string();
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol_by_name<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts.symbols.iter().find(|s| s.name == name).unwrap()
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(TypescriptBackend.parser_id(), "tree-sitter-typescript");
    }

    #[test]
    fn extracts_function_class_interface_type_alias_and_enum() {
        let src = br#"
/** doc on f */
function f(x: number): string { return ""; }
class C {
    m(): void {}
}
interface I { x: number; }
type T = string;
enum E { A, B }
"#;
        let facts = TypescriptBackend.extract_syntactic(src).unwrap();

        assert_eq!(symbol_by_name(&facts, "f").kind, SymbolKind::Function);
        assert_eq!(symbol_by_name(&facts, "C").kind, SymbolKind::Class);
        assert_eq!(symbol_by_name(&facts, "m").kind, SymbolKind::Method);
        assert_eq!(symbol_by_name(&facts, "I").kind, SymbolKind::Interface);
        assert_eq!(symbol_by_name(&facts, "T").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol_by_name(&facts, "E").kind, SymbolKind::Enum);
        assert_eq!(symbol_by_name(&facts, "f").doc.as_deref(), Some("doc on f"));
        assert_eq!(
            symbol_by_name(&facts, "f").signature.as_deref(),
            Some("function f(x: number): string")
        );
    }

    #[test]
    fn extracts_imports() {
        let src = br#"
import { foo, bar as baz } from "./mod";
import * as ns from "x";
import type { T } from "y";
"#;
        let facts = TypescriptBackend.extract_syntactic(src).unwrap();

        assert_eq!(facts.imports.len(), 4);
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./mod" && i.imported.as_deref() == Some("foo") && i.alias.is_none()
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./mod"
                && i.imported.as_deref() == Some("bar")
                && i.alias.as_deref() == Some("baz")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "x"
                && i.imported.as_deref() == Some("*")
                && i.alias.as_deref() == Some("ns")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "y" && i.imported.as_deref() == Some("T") && i.alias.is_none()
        }));
    }

    #[test]
    fn extracts_default_only_import() {
        let facts = TypescriptBackend
            .extract_syntactic(b"import React from \"react\";\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "react");
        assert_eq!(i.imported.as_deref(), Some("default"));
        assert_eq!(i.alias.as_deref(), Some("React"));
    }

    #[test]
    fn extracts_default_and_named_imports() {
        // Regression: previously the default binding was dropped whenever a
        // named import was present in the same statement.
        let facts = TypescriptBackend
            .extract_syntactic(b"import React, { useState, useEffect as ue } from \"react\";\n")
            .unwrap();

        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("React")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react" && i.imported.as_deref() == Some("useState") && i.alias.is_none()
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("useEffect")
                && i.alias.as_deref() == Some("ue")
        }));
        assert_eq!(facts.imports.len(), 3);
    }

    #[test]
    fn extracts_default_and_namespace_imports() {
        let facts = TypescriptBackend
            .extract_syntactic(b"import React, * as ReactNS from \"react\";\n")
            .unwrap();

        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("React")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("*")
                && i.alias.as_deref() == Some("ReactNS")
        }));
        assert_eq!(facts.imports.len(), 2);
    }

    #[test]
    fn extracts_side_effect_only_import() {
        let facts = TypescriptBackend
            .extract_syntactic(b"import \"./styles.css\";\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "./styles.css");
        assert!(i.imported.is_none());
        assert!(i.alias.is_none());
    }

    #[test]
    fn nested_class_method_qualified_name() {
        let facts = TypescriptBackend
            .extract_syntactic(b"class A { b(): void {} }")
            .unwrap();
        let method = symbol_by_name(&facts, "b");
        assert_eq!(method.qualified, "A.b");
        assert_eq!(method.kind, SymbolKind::Method);
    }

    #[test]
    fn tsx_extracts_symbols_and_imports_through_jsx() {
        let src = br#"
import React, { useState } from "react";
import "./card.css";

/** Card component. */
function Card(props: CardProps): JSX.Element {
    return <div className="card">{props.title}</div>;
}

interface CardProps { title: string; }

class Panel extends React.Component {
    render() { return <Card title="x" />; }
}
"#;
        let facts = TsxBackend.extract_syntactic(src).unwrap();

        let card = symbol_by_name(&facts, "Card");
        assert_eq!(card.kind, SymbolKind::Function);
        assert_eq!(card.doc.as_deref(), Some("Card component."));
        assert_eq!(
            card.signature.as_deref(),
            Some("function Card(props: CardProps): JSX.Element")
        );
        assert_eq!(
            symbol_by_name(&facts, "CardProps").kind,
            SymbolKind::Interface
        );
        assert_eq!(symbol_by_name(&facts, "Panel").kind, SymbolKind::Class);
        let render = symbol_by_name(&facts, "render");
        assert_eq!(render.qualified, "Panel.render");
        assert_eq!(render.kind, SymbolKind::Method);

        assert!(facts.imports.iter().any(|i| {
            i.to_module == "react"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("React")
        }));
        assert!(
            facts
                .imports
                .iter()
                .any(|i| { i.to_module == "react" && i.imported.as_deref() == Some("useState") })
        );
        assert!(
            facts.imports.iter().any(|i| {
                i.to_module == "./card.css" && i.imported.is_none() && i.alias.is_none()
            })
        );
    }

    #[test]
    fn javascript_extracts_symbols_and_imports() {
        let src = br#"
import defaultExport, { named as alias } from "./mod";

/** doc on helper */
function helper(x) { return x; }

class Dog extends Animal {
    bark() {}
}
"#;
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();

        let helper = symbol_by_name(&facts, "helper");
        assert_eq!(helper.kind, SymbolKind::Function);
        assert_eq!(helper.doc.as_deref(), Some("doc on helper"));
        assert_eq!(helper.signature.as_deref(), Some("function helper(x)"));
        assert_eq!(symbol_by_name(&facts, "Dog").kind, SymbolKind::Class);
        let bark = symbol_by_name(&facts, "bark");
        assert_eq!(bark.qualified, "Dog.bark");
        assert_eq!(bark.kind, SymbolKind::Method);

        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./mod"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("defaultExport")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./mod"
                && i.imported.as_deref() == Some("named")
                && i.alias.as_deref() == Some("alias")
        }));
    }

    #[test]
    fn javascript_jsx_file_extracts_component_function() {
        let facts = JavascriptBackend
            .extract_syntactic(b"function App() { return <div><Widget /></div>; }\n")
            .unwrap();
        assert_eq!(symbol_by_name(&facts, "App").kind, SymbolKind::Function);
    }

    #[test]
    fn dialect_backends_report_stable_identity() {
        assert_eq!(TsxBackend.name(), "tsx");
        assert_eq!(TsxBackend.parser_id(), "tree-sitter-tsx");
        assert_eq!(JavascriptBackend.name(), "javascript");
        assert_eq!(JavascriptBackend.parser_id(), "tree-sitter-javascript");
        assert_eq!(JavascriptBackend.shebang_patterns(), &["node"]);
    }

    #[test]
    fn javascript_extracts_cjs_require_default() {
        let facts = JavascriptBackend
            .extract_syntactic(b"const foo = require('./foo');\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "./foo");
        assert_eq!(i.imported.as_deref(), Some("default"));
        assert_eq!(i.alias.as_deref(), Some("foo"));
        // byte_range must point at the string literal `'./foo'`
        // including the quotes — Tier-2.5's resolver anchors on this
        // span.
        let (s, e) = i.byte_range.expect("byte_range emitted");
        let span = &b"const foo = require('./foo');\n"[s as usize..e as usize];
        assert_eq!(span, b"'./foo'");
    }

    #[test]
    fn javascript_extracts_cjs_require_destructured() {
        let facts = JavascriptBackend
            .extract_syntactic(b"const { Router, Route: R } = require('./router');\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 2);
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./router"
                && i.imported.as_deref() == Some("Router")
                && i.alias.as_deref() == Some("Router")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./router"
                && i.imported.as_deref() == Some("Route")
                && i.alias.as_deref() == Some("R")
        }));
    }

    #[test]
    fn javascript_extracts_cjs_require_member() {
        let facts = JavascriptBackend
            .extract_syntactic(b"const Foo = require('./mod').Bar;\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "./mod");
        assert_eq!(i.imported.as_deref(), Some("Bar"));
        assert_eq!(i.alias.as_deref(), Some("Foo"));
    }

    #[test]
    fn javascript_cjs_and_esm_coexist() {
        let src = br#"
import { useState } from "react";
const fs = require('fs');
const { join } = require('path');
"#;
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        assert!(
            facts
                .imports
                .iter()
                .any(|i| { i.to_module == "react" && i.imported.as_deref() == Some("useState") })
        );
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "fs"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("fs")
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "path"
                && i.imported.as_deref() == Some("join")
                && i.alias.as_deref() == Some("join")
        }));
    }

    #[test]
    fn javascript_var_and_let_require_also_emit() {
        // Emission is shape-driven; let/var bindings reach the same path.
        let facts = JavascriptBackend
            .extract_syntactic(b"var a = require('./a'); let b = require('./b');\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 2);
        assert!(facts.imports.iter().any(|i| i.to_module == "./a"));
        assert!(facts.imports.iter().any(|i| i.to_module == "./b"));
    }

    #[test]
    fn javascript_non_require_call_is_ignored() {
        // RHS is a call but not `require(...)` — must not emit anything.
        let facts = JavascriptBackend
            .extract_syntactic(b"const x = compute('./foo');\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 0);
    }

    #[test]
    fn typescript_also_emits_cjs_require() {
        // Mixed CJS/ESM does appear in real .ts code (e.g. type-only
        // modules wrapped with `require` for runtime), so the emit
        // policy is dialect-agnostic.
        let facts = TypescriptBackend
            .extract_syntactic(b"const fs = require('fs');\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 1);
        assert_eq!(facts.imports[0].to_module, "fs");
        assert_eq!(facts.imports[0].imported.as_deref(), Some("default"));
    }

    #[test]
    fn parser_revision_bumped_for_expanded_require_emit() {
        // Revision 3 (PR-β): statement-position, expression-position,
        // and `module.exports = require(...)` re-export require calls
        // now also emit `ImportFact` rows. Bump signals the CAS-cached
        // syntactic snapshot to invalidate so the same input yields the
        // wider fact set without a manual reindex.
        assert_eq!(TypescriptBackend.parser_revision(), 4);
        assert_eq!(TsxBackend.parser_revision(), 4);
        assert_eq!(JavascriptBackend.parser_revision(), 4);
    }

    // ─── PR-β: expanded require() ImportFact emit ────────────────

    #[test]
    fn statement_position_require_emits_side_effect_import_fact() {
        // Top-level `require('./setup');` as a bare expression statement
        // must emit a side-effect ImportFact pinned at the module
        // literal span.
        let src = b"require('./setup');\n";
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        assert_eq!(facts.imports.len(), 1);
        let i = &facts.imports[0];
        assert_eq!(i.to_module, "./setup");
        assert!(i.imported.is_none());
        assert!(i.alias.is_none());
        assert!(!i.is_reexport);
        let (s, e) = i.byte_range.expect("byte_range emitted");
        assert_eq!(&src[s as usize..e as usize], b"'./setup'");
    }

    #[test]
    fn expression_position_require_emits_side_effect_import_fact() {
        // Argument-nested `require('./routes')` inside `app.use(...)`
        // must also emit. The binding-form path isn't triggered here.
        let src = b"app.use(require('./routes'));\n";
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        let hit = facts
            .imports
            .iter()
            .find(|i| i.to_module == "./routes")
            .expect("expression-position require should emit");
        assert!(hit.imported.is_none());
        assert!(hit.alias.is_none());
        assert!(!hit.is_reexport);
        // Exactly one ImportFact total for this site — no duplicate
        // from the generic visitor.
        assert_eq!(
            facts
                .imports
                .iter()
                .filter(|i| i.to_module == "./routes")
                .count(),
            1
        );
    }

    #[test]
    fn module_exports_assign_require_emits_reexport_import_fact() {
        // `module.exports = require('./inner')` is a re-export; the
        // ImportFact carries `is_reexport: true` and no
        // `imported`/`alias` (Tier-2.5 cannot model the export-name
        // mapping yet, so this is the only safe shape).
        let src = b"module.exports = require('./inner');\n";
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        let hit = facts
            .imports
            .iter()
            .find(|i| i.to_module == "./inner")
            .expect("module.exports = require(...) should emit");
        assert!(hit.is_reexport);
        assert!(hit.imported.is_none());
        assert!(hit.alias.is_none());
        // No duplicate from the generic expression visitor on the same
        // call site.
        assert_eq!(
            facts
                .imports
                .iter()
                .filter(|i| i.to_module == "./inner")
                .count(),
            1
        );
    }

    #[test]
    fn exports_named_reexport_does_not_emit_import_fact() {
        // `exports.X = require('./x')` is a named re-export. Tier-2.5
        // does not yet model named re-export graph semantics, so this
        // PR keeps it out of scope: the assignment visitor recognises
        // the LHS and *claims* the RHS call site in `seen_require_sites`
        // without emitting anything, suppressing the generic
        // `call_expression` visitor's side-effect ImportFact emission
        // for the same site.
        let src = b"exports.foo = require('./inner');\n";
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        assert!(
            facts.imports.iter().all(|i| i.to_module != "./inner"),
            "exports.X = require('./inner') must not emit any ImportFact \
             (scope-out); got: {:#?}",
            facts.imports
        );
    }

    #[test]
    fn module_exports_named_reexport_does_not_emit_import_fact() {
        // `module.exports.X = require('./x')` is the nested form of
        // the named re-export above. Same scope-out contract applies.
        let src = b"module.exports.foo = require('./inner');\n";
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        assert!(
            facts.imports.iter().all(|i| i.to_module != "./inner"),
            "module.exports.X = require('./inner') must not emit any \
             ImportFact (scope-out); got: {:#?}",
            facts.imports
        );
    }

    #[test]
    fn esm_and_cjs_imports_coexist_in_same_file() {
        // Mixed-paradigm files (common during ESM migration) must
        // surface both kinds of ImportFact independently.
        let src = br#"
import { foo } from './a';
const bar = require('./b');
"#;
        let facts = JavascriptBackend.extract_syntactic(src).unwrap();
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./a" && i.imported.as_deref() == Some("foo") && i.alias.is_none()
        }));
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "./b"
                && i.imported.as_deref() == Some("default")
                && i.alias.as_deref() == Some("bar")
        }));
    }

    #[test]
    fn require_dot_resolve_does_not_emit_import_fact() {
        // `require.resolve('./x')` is a member-expression callee, not
        // a bare `require` identifier — must be rejected structurally.
        let facts = JavascriptBackend
            .extract_syntactic(b"const p = require.resolve('./x');\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 0);
    }

    #[test]
    fn dynamic_require_with_identifier_arg_does_not_emit() {
        // Dynamic specifier — argument is an identifier, not a string
        // literal. Tier-1 can't resolve the target without runtime info.
        let facts = JavascriptBackend
            .extract_syntactic(b"const name = './a';\nrequire(name);\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 0);
    }

    #[test]
    fn template_string_require_does_not_emit() {
        // Template literal argument — recognizer requires
        // `node.kind() == "string"` exactly.
        let facts = JavascriptBackend
            .extract_syntactic(b"const x = './a';\nrequire(`./${x}`);\n")
            .unwrap();
        assert_eq!(facts.imports.len(), 0);
    }

    #[test]
    fn all_three_backends_register() {
        let mut names = cairn_lang_api::all_backends()
            .iter()
            .map(|b| b.name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["javascript", "tsx", "typescript"]);
    }

    // ─── scope distinction (find_symbols vs get_outline) ────────────

    fn scope_by_name(facts: &SyntacticFacts, name: &str) -> SymbolScope {
        facts
            .symbols
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.scope)
            .expect("symbol not found")
    }

    #[test]
    fn tier1_typescript_top_level_function_has_scope_top_level() {
        let facts = TypescriptBackend
            .extract_syntactic(
                b"function exported(x: number) { return x; }
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "exported"), SymbolScope::TopLevel);
    }

    #[test]
    fn tier1_typescript_nested_function_in_function_declaration_has_scope_nested() {
        // Inner `helper` is declared inside `outer`'s body: it must
        // not be reachable as a workspace symbol but must still appear
        // in the outline.
        let facts = TypescriptBackend
            .extract_syntactic(
                b"function outer() { function helper() {} }
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "outer"), SymbolScope::TopLevel);
        assert_eq!(scope_by_name(&facts, "helper"), SymbolScope::Nested);
    }

    #[test]
    fn tier1_typescript_nested_function_in_arrow_body_has_scope_nested() {
        // Arrow function bodies open a function frame even though the
        // arrow itself doesn't emit a SymbolFact.
        let facts = JavascriptBackend
            .extract_syntactic(
                b"const f = () => { function helper() {} };
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "helper"), SymbolScope::Nested);
    }

    #[test]
    fn tier1_typescript_class_method_has_scope_top_level() {
        // A method of a top-level class is workspace-addressable
        // (the class container does not make it nested — only an
        // enclosing function body does).
        let facts = TypescriptBackend
            .extract_syntactic(
                b"class C { m(): void {} }
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "C"), SymbolScope::TopLevel);
        assert_eq!(scope_by_name(&facts, "m"), SymbolScope::TopLevel);
    }

    #[test]
    fn tier1_typescript_iife_nested_function_has_scope_nested() {
        // `(function(){ function x(){} })()` — x is declared inside an
        // immediately-invoked function expression's body.
        let facts = JavascriptBackend
            .extract_syntactic(
                b"(function() { function x() {} })();
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "x"), SymbolScope::Nested);
    }

    #[test]
    fn tier1_typescript_method_inside_class_inside_function_has_scope_nested() {
        // Class declared inside a function body — the class and its
        // methods are nested.
        let facts = TypescriptBackend
            .extract_syntactic(
                b"function outer() { class Inner { m(): void {} } }
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "Inner"), SymbolScope::Nested);
        assert_eq!(scope_by_name(&facts, "m"), SymbolScope::Nested);
    }

    #[test]
    fn tier1_typescript_nested_function_in_generator_function_body_has_scope_nested() {
        // Regression: `generator_function_declaration` previously was
        // not matched by `match_typescript_item`, so the visitor
        // returned early before pushing the body frame. As a result,
        // function declarations nested inside `function* gen() { ... }`
        // leaked into workspace lookup as TopLevel.
        let facts = JavascriptBackend
            .extract_syntactic(
                b"function* gen() { function nested4() {} }
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "gen"), SymbolScope::TopLevel);
        assert_eq!(scope_by_name(&facts, "nested4"), SymbolScope::Nested);
    }

    #[test]
    fn tier1_typescript_default_export_named_function_is_top_level() {
        let facts = TypescriptBackend
            .extract_syntactic(
                b"export default function foo() {}
",
            )
            .unwrap();
        assert_eq!(scope_by_name(&facts, "foo"), SymbolScope::TopLevel);
    }
}
