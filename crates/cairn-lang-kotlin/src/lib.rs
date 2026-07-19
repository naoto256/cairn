//! `cairn-lang-kotlin` — Kotlin backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-kotlin-ng parse tree and emits
//! [`SymbolFact`]s for classes (incl. data / sealed / enum / annotation),
//! interfaces, objects, companion objects, functions (top-level, member,
//! extension), properties, constants, type aliases, and import declarations.
//! Tier-2 lives in [`analyzer`]: delegation (inheritance) edges plus
//! name-level call / constructor refs from the same blob.

#![forbid(unsafe_code)]

mod analyzer;

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

/// Backend instance.
pub struct KotlinBackend;

impl LanguageBackend for KotlinBackend {
    fn name(&self) -> &'static str {
        "kotlin"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.kt", "*.kts"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-kotlin-ng"
    }

    fn parser_revision(&self) -> u32 {
        // rev 2: emit byte_range on ImportFact for Tier-2.5 consumption.
        // The Kotlin Tier-2.5 resolver pins its `resolutions` row at the
        // same dotted-path span Tier-2 records here, so `find_imports`
        // can LEFT JOIN them.
        //
        // rev 3: prefix `SymbolFact.qualified` with the file's
        // `package` declaration when present. Prior revisions stored
        // bare names (`JsonAdapter`) which broke cross-parser
        // `(blob_sha, qualified)` matching in the Tier-2.5 persist
        // layer — sibling-language resolvers (Kotlin extending Java,
        // etc.) emit fully-qualified target_qualified strings
        // (`com.x.JsonAdapter`) and the bare-name symbols never
        // matched. Index/blame consumers continue to read the
        // unchanged `name` column for prefix and FTS lookups.
        //
        // rev 4: emit `enum_entry` (`SymbolKind::Constant`, qualified
        // `pkg.Enum.VALUE`) and both `primary_constructor` /
        // `secondary_constructor` (`SymbolKind::Constructor`, qualified
        // `pkg.Class.Class`). Mirrors the Java backend's
        // `enum_constant` / `constructor_declaration` shape so
        // cross-parser `MyEnum.VALUE` and `new Foo(...)` joins hit
        // the same `(kind, qualified)` row instead of falling through
        // to tier2-fact. Existing blob caches are invalidated; the
        // PR #220 staleness scanner auto-enqueues reindex at daemon
        // startup, no manual `cairn ctl repo reindex` required.
        4
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_kotlin_ng::LANGUAGE.into();
        extract(source, &language, KotlinVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_KOTLIN: fn() -> Box<dyn LanguageBackend> = || Box::new(KotlinBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct KotlinVisitor {
    nesting: NestingTracker,
    /// File-scoped `package com.example.app` prefix, captured from the
    /// `package_header` node. Persists for the lifetime of the visitor
    /// (one per file). `None` when the file declares no package
    /// (default/root package).
    package_prefix: Option<String>,
}

impl KotlinVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
            package_prefix: None,
        }
    }

    /// `com.example.app.Outer.Inner.method` — package, class-nesting
    /// path, and member name joined with `.`. Bare-named root types
    /// fall back to just the package when nesting is empty, and to
    /// the bare name when no package is declared.
    fn qualified_for(&self, name: &str, facts: &SyntacticFacts) -> String {
        let base = self.nesting.qualified_for(name, facts);
        match &self.package_prefix {
            Some(pkg) => format!("{pkg}.{base}"),
            None => base,
        }
    }
}

impl Visitor for KotlinVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        if node.kind() == "package_header" {
            if let Some(pkg) = match_package_header(node, source) {
                self.package_prefix = Some(pkg);
            }
            return;
        }

        if node.kind() == "import" {
            if let Some(import) = match_import(node, source) {
                facts.imports.push(import);
            }
            return;
        }

        let Some((kind, display_name, body_start)) = match_kotlin_item(node, source) else {
            return;
        };

        let kind = if matches!(kind, SymbolKind::Function)
            && matches!(
                self.nesting.parent_kind(facts),
                Some(SymbolKind::Class | SymbolKind::Interface | SymbolKind::Enum)
            ) {
            SymbolKind::Method
        } else {
            kind
        };

        // For extension functions `display_name` is `Receiver.name`; the
        // bare symbol name is always the last dot segment.
        let name = display_name
            .rsplit('.')
            .next()
            .unwrap_or(&display_name)
            .to_string();
        let qualified = self.qualified_for(&display_name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = kotlin_visibility(node, source);
        let doc = extract_kdoc(node, source);
        let parent_idx = self.nesting.current_parent();

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
            scope: SymbolScope::TopLevel,
        });

        if is_container(&kind) {
            self.nesting.push(idx, node.end_byte());
        }
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Class | SymbolKind::Interface | SymbolKind::Enum
    )
}

/// Classify one node into `(kind, display_name, body_start)`.
///
/// `display_name` is the name as it should appear in the qualified path —
/// for extension functions this includes the receiver (`String.shout`).
fn match_kotlin_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    match node.kind() {
        "class_declaration" => {
            let name = child_by_field(node, "name")?;
            let kind = if has_direct_token(node, "interface") {
                SymbolKind::Interface
            } else if has_class_modifier(node, "enum") {
                SymbolKind::Enum
            } else {
                SymbolKind::Class
            };
            Some((
                kind,
                node_text(name, source).to_string(),
                class_body_start(node),
            ))
        }
        "object_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Class,
                node_text(name, source).to_string(),
                class_body_start(node),
            ))
        }
        "companion_object" => {
            // The name is optional; Kotlin's default is `Companion`.
            let name = child_by_field(node, "name").map_or_else(
                || "Companion".to_string(),
                |n| node_text(n, source).to_string(),
            );
            Some((SymbolKind::Class, name, class_body_start(node)))
        }
        "function_declaration" => {
            let name = child_by_field(node, "name")?;
            let mut display = node_text(name, source).to_string();
            if let Some(receiver) = extension_receiver_name(node, name, source) {
                display = format!("{receiver}.{display}");
            }
            let body = child_by_field(node, "body")
                .or_else(|| find_direct_child(node, "function_body"))
                .map(|n| n.start_byte());
            Some((SymbolKind::Function, display, body))
        }
        "property_declaration" if is_declaration_site(node) => {
            let name = property_name(node, source)?;
            let kind = if has_property_modifier(node, "const") {
                SymbolKind::Constant
            } else {
                SymbolKind::Property
            };
            Some((kind, name, None))
        }
        // `val` / `var` parameters in a primary constructor declare
        // real properties (the data-class shape).
        "class_parameter" if has_direct_token(node, "val") || has_direct_token(node, "var") => {
            let name = find_direct_child(node, "identifier")?;
            Some((
                SymbolKind::Property,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "type_alias" => {
            // tree-sitter-kotlin-ng exposes the alias name under the
            // `type` field.
            let name = child_by_field(node, "type")?;
            Some((
                SymbolKind::TypeAlias,
                node_text(name, source).to_string(),
                None,
            ))
        }
        // Enum entries (`enum class Color { RED, GREEN }` → `RED`,
        // `GREEN`). Modeled as `Constant` for cross-parser symmetry
        // with the Java backend's `enum_constant` (Java↔Kotlin
        // interop lookups join on `(kind, qualified)`). Kotlin's
        // internal semantics treat each entry as a singleton object
        // of the enum class — closer to a Property — but interop
        // symmetry is the primary motivator: a Java caller writing
        // `MyEnum.VALUE` must hit the same `Constant` shape whether
        // the enum is declared in Java or Kotlin.
        "enum_entry" => {
            let name = find_direct_child(node, "identifier")?;
            Some((
                SymbolKind::Constant,
                node_text(name, source).to_string(),
                None,
            ))
        }
        // Primary constructor: `class Foo(val x: Int)`. Name = parent
        // class short name (Kotlin/Java convention `Foo.Foo` qualified),
        // body_start = None (primaries are not containers — the
        // `val`/`var` class_parameter children are emitted as
        // properties from their own visitor pass at the class_body
        // scope, not nested under the constructor).
        "primary_constructor" => {
            let parent = node.parent()?;
            let class_name = child_by_field(parent, "name")?;
            Some((
                SymbolKind::Constructor,
                node_text(class_name, source).to_string(),
                None,
            ))
        }
        // Secondary constructor: `class Foo { constructor(x: Int) { ... } }`.
        // Name = parent class short name (same qualified shape as
        // primary). body_start = the `block` child when present so
        // the signature slice stops at the brace.
        "secondary_constructor" => {
            let parent = node.parent()?;
            // Walk up past `class_body` to the enclosing class.
            let class_decl = parent
                .parent()
                .filter(|p| matches!(p.kind(), "class_declaration" | "object_declaration"))?;
            let class_name = child_by_field(class_decl, "name")?;
            let body_start = find_direct_child(node, "block").map(|n| n.start_byte());
            Some((
                SymbolKind::Constructor,
                node_text(class_name, source).to_string(),
                body_start,
            ))
        }
        _ => None,
    }
}

fn class_body_start(node: Node<'_>) -> Option<usize> {
    find_direct_child(node, "class_body")
        .or_else(|| find_direct_child(node, "enum_class_body"))
        .map(|n| n.start_byte())
}

/// Properties are emitted only at declaration sites (top level or inside
/// a class / object body), never for `val` locals inside function bodies.
fn is_declaration_site(node: Node<'_>) -> bool {
    node.parent()
        .is_some_and(|p| matches!(p.kind(), "source_file" | "class_body" | "enum_class_body"))
}

fn property_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let decl = find_direct_child(node, "variable_declaration")?;
    let name = find_direct_child(decl, "identifier")?;
    Some(node_text(name, source).to_string())
}

/// Receiver type of an extension function: the direct `user_type` (or
/// `nullable_type`) child that precedes the `name` field. Generic
/// arguments are stripped so `List<Int>.sum` yields `List.sum`.
fn extension_receiver_name(node: Node<'_>, name: Node<'_>, source: &[u8]) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.start_byte() >= name.start_byte() {
            break;
        }
        if matches!(child.kind(), "user_type" | "nullable_type") {
            let text = node_text(child, source);
            let base = text.split('<').next().unwrap_or(text).trim();
            let base = base.trim_end_matches('?');
            if !base.is_empty() {
                return Some(base.to_string());
            }
        }
    }
    None
}

fn has_direct_token(node: Node<'_>, token: &str) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor).any(|c| c.kind() == token)
}

fn has_class_modifier(node: Node<'_>, modifier: &str) -> bool {
    has_modifier(node, "class_modifier", modifier)
}

fn has_property_modifier(node: Node<'_>, modifier: &str) -> bool {
    has_modifier(node, "property_modifier", modifier)
}

fn has_modifier(node: Node<'_>, modifier_kind: &str, modifier: &str) -> bool {
    let Some(modifiers) = find_direct_child(node, "modifiers") else {
        return false;
    };
    let mut cursor = modifiers.walk();
    modifiers
        .children(&mut cursor)
        .any(|c| c.kind() == modifier_kind && has_direct_token(c, modifier))
}

fn kotlin_visibility(node: Node<'_>, source: &[u8]) -> Option<Visibility> {
    let modifiers = find_direct_child(node, "modifiers")?;
    let mut cursor = modifiers.walk();
    for child in modifiers.children(&mut cursor) {
        if child.kind() == "visibility_modifier" {
            return match node_text(child, source) {
                "public" => Some(Visibility::Public),
                "private" => Some(Visibility::Private),
                // `internal` (module-scoped) and `protected` map onto the
                // middle tier of cairn's three-level visibility model.
                "internal" | "protected" => Some(Visibility::Crate),
                _ => None,
            };
        }
    }
    None
}

fn find_direct_child<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == kind)
}

// ─── package ───────────────────────────────────────────────────────────────

/// Extract the dotted package name from a `package_header` node.
/// Returns the path text (e.g. `"com.example.app"`) when present,
/// `None` when the node has no recognizable identifier child (which
/// the grammar should not produce, but we keep the visitor permissive
/// so a parse-error file still emits its symbols under the default
/// package rather than crashing).
fn match_package_header(node: Node<'_>, source: &[u8]) -> Option<String> {
    let path = find_direct_child(node, "qualified_identifier")
        .or_else(|| find_direct_child(node, "identifier"))?;
    let text = node_text(path, source).trim();
    if text.is_empty() {
        return None;
    }
    Some(text.to_string())
}

// ─── imports ───────────────────────────────────────────────────────────────

fn match_import(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let path = find_direct_child(node, "qualified_identifier")
        .or_else(|| find_direct_child(node, "identifier"))?;
    let path_text = node_text(path, source).trim().to_string();
    if path_text.is_empty() {
        return None;
    }

    let wildcard = has_direct_token(node, "*");
    // An alias (`import a.b.C as D`) appears as a direct `identifier`
    // child after the `as` keyword; the dotted path itself is wrapped in
    // `qualified_identifier`, so a direct identifier is unambiguous when
    // the path node is a qualified_identifier.
    let alias = if path.kind() == "qualified_identifier" {
        find_direct_child(node, "identifier").map(|n| node_text(n, source).to_string())
    } else {
        None
    };

    let (to_module, imported) = if wildcard {
        (path_text, Some("*".to_string()))
    } else {
        match path_text.rsplit_once('.') {
            Some((module, leaf)) => (module.to_string(), Some(leaf.to_string())),
            None => (path_text.clone(), Some(path_text)),
        }
    };

    // rev 2: pin byte_range at the dotted-path span so Tier-2.5's
    // `resolutions` row joins against the same site for find_imports.
    let path_range = path.byte_range();
    Some(ImportFact {
        to_module,
        imported,
        alias,
        is_reexport: false,
        line: line_of(node),

        byte_range: Some((path_range.start as u32, path_range.end as u32)),
    })
}

// ─── doc comments ──────────────────────────────────────────────────────────

/// KDoc: the closest preceding `/** ... */` block comment, with nothing but
/// extras between it and the declaration.
fn extract_kdoc(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| match sibling.kind() {
        "block_comment" if text.trim_start().starts_with("/**") => {
            Some(DocCommentPart::Replace(strip_kdoc_markers(text)))
        }
        "block_comment" | "line_comment" => Some(DocCommentPart::Reset),
        _ => None,
    })
    .filter(|doc| !doc.is_empty())
}

fn strip_kdoc_markers(text: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = br#"
package com.example.app

import kotlinx.coroutines.delay
import com.example.lib.Base as B
import com.example.util.*

/** Max retry budget. */
const val MAX_RETRIES: Int = 3

typealias Handler = (String) -> Unit

interface Greeter {
    fun greet(name: String): String
}

sealed class Result<out T> {
    data class Success<T>(val value: T) : Result<T>()
    data class Failure(val error: Throwable) : Result<Nothing>()
}

data class User(val id: Long, var name: String) {
    constructor(id: Long) : this(id, "anon") {
        require(id >= 0)
    }
}

object Registry {
    val users = mutableListOf<User>()
}

enum class Color { RED, GREEN }

/** Greets and fetches. */
class Service(private val repo: String) : B(), Greeter {
    val cache: MutableMap<String, User> = mutableMapOf()
    internal var counter = 0

    companion object {
        const val TAG = "com.example.app.Service"
        fun create(): Service = Service("default")
    }

    override fun greet(name: String): String = "Hello, $name"

    suspend fun fetch(id: Long): User {
        delay(100)
        return User(id, "user-$id")
    }

    class Inner {
        private fun helper() = 42
    }
}

fun String.shout(): String = uppercase() + "!"

suspend fun process(input: String): Result<User> {
    val local = Service.create()
    val kind = when (input) {
        "a" -> 1
        else -> 0
    }
    return Result.Success(local.fetch(kind.toLong()))
}
"#;

    fn facts() -> SyntacticFacts {
        KotlinBackend.extract_syntactic(FIXTURE).unwrap()
    }

    fn symbol<'a>(facts: &'a SyntacticFacts, qualified: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.qualified == qualified)
            .unwrap_or_else(|| panic!("{qualified} missing: {:?}", facts.symbols))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(KotlinBackend.parser_id(), "tree-sitter-kotlin-ng");
    }

    #[test]
    fn claims_kt_and_kts() {
        assert_eq!(KotlinBackend.file_patterns(), &["*.kt", "*.kts"]);
    }

    #[test]
    fn extracts_class_interface_object_enum_typealias() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.Greeter").kind,
            SymbolKind::Interface
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Result").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.User").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Registry").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Color").kind,
            SymbolKind::Enum
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Handler").kind,
            SymbolKind::TypeAlias
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service").kind,
            SymbolKind::Class
        );
    }

    #[test]
    fn nested_data_classes_are_qualified_under_sealed_parent() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.Result.Success").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Result.Failure").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Result.Success").parent_idx,
            Some(
                facts
                    .symbols
                    .iter()
                    .position(|s| s.qualified == "com.example.app.Result")
                    .unwrap()
            )
        );
    }

    #[test]
    fn data_class_constructor_val_var_params_become_properties() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.User.id").kind,
            SymbolKind::Property
        );
        assert_eq!(
            symbol(&facts, "com.example.app.User.name").kind,
            SymbolKind::Property
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.repo").visibility,
            Some(Visibility::Private)
        );
    }

    #[test]
    fn companion_object_members_qualify_through_companion() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.Service.Companion").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.Companion.TAG").kind,
            SymbolKind::Constant
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.Companion.create").kind,
            SymbolKind::Method
        );
    }

    #[test]
    fn member_functions_are_methods_and_top_level_are_functions() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.Service.greet").kind,
            SymbolKind::Method
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.fetch").kind,
            SymbolKind::Method
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Greeter.greet").kind,
            SymbolKind::Method
        );
        assert_eq!(
            symbol(&facts, "com.example.app.process").kind,
            SymbolKind::Function
        );
        assert!(
            symbol(&facts, "com.example.app.Service.fetch")
                .signature
                .as_deref()
                .unwrap()
                .contains("suspend fun fetch")
        );
    }

    #[test]
    fn extension_function_carries_receiver_in_qualified_name() {
        let facts = facts();
        let shout = symbol(&facts, "com.example.app.String.shout");
        assert_eq!(shout.kind, SymbolKind::Function);
        assert_eq!(shout.name, "shout");
    }

    #[test]
    fn nested_class_and_visibility() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.Service.Inner").kind,
            SymbolKind::Class
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.Inner.helper").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.counter").visibility,
            Some(Visibility::Crate)
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.cache").visibility,
            None
        );
    }

    #[test]
    fn const_and_properties() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.MAX_RETRIES").kind,
            SymbolKind::Constant
        );
        assert_eq!(
            symbol(&facts, "com.example.app.MAX_RETRIES").doc.as_deref(),
            Some("Max retry budget.")
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Registry.users").kind,
            SymbolKind::Property
        );
        assert_eq!(
            symbol(&facts, "com.example.app.Service.cache").kind,
            SymbolKind::Property
        );
    }

    #[test]
    fn kdoc_attaches_to_class() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "com.example.app.Service").doc.as_deref(),
            Some("Greets and fetches.")
        );
    }

    #[test]
    fn kdoc_adjacency_characterization() {
        let cases: &[(&str, &[u8], Option<&str>)] = &[
            (
                "Direct",
                b"/**\n * Direct KDoc.\n * Second line.\n */\nclass Direct",
                Some("Direct KDoc.\nSecond line."),
            ),
            (
                "Closest",
                b"/** Earlier. */\n/** Closest. */\nclass Closest",
                Some("Closest."),
            ),
            (
                "BlankLine",
                b"/** Separated today. */\n\nclass BlankLine",
                None,
            ),
            (
                "LineReset",
                b"/** Stale. */\n// reset\nclass LineReset",
                None,
            ),
        ];

        for (name, source, expected) in cases {
            let facts = KotlinBackend.extract_syntactic(source).unwrap();
            let symbol = facts
                .symbols
                .iter()
                .find(|symbol| symbol.name == *name)
                .unwrap_or_else(|| panic!("symbol {name} missing"));
            assert_eq!(symbol.doc.as_deref(), *expected, "case {name}");
        }
    }

    #[test]
    fn locals_inside_function_bodies_are_ignored() {
        let facts = facts();
        assert!(facts.symbols.iter().all(|s| s.name != "local"));
        assert!(facts.symbols.iter().all(|s| s.name != "kind"));
    }

    #[test]
    fn enum_entries_emit_as_constants_qualified_under_enum() {
        let facts = facts();
        // `enum class Color { RED, GREEN }` — each entry must surface
        // as a `Constant` at `pkg.Color.<NAME>`, matching the Java
        // backend's `enum_constant` shape for cross-parser interop.
        let red = symbol(&facts, "com.example.app.Color.RED");
        assert_eq!(red.kind, SymbolKind::Constant);
        assert_eq!(red.name, "RED");
        let green = symbol(&facts, "com.example.app.Color.GREEN");
        assert_eq!(green.kind, SymbolKind::Constant);
        // parent_idx threads back through the Enum container.
        let color_idx = facts
            .symbols
            .iter()
            .position(|s| s.qualified == "com.example.app.Color")
            .unwrap();
        assert_eq!(red.parent_idx, Some(color_idx));
    }

    #[test]
    fn primary_constructor_emits_under_class_qualified() {
        let facts = facts();
        // `data class User(val id: Long, var name: String)` — the
        // primary constructor surfaces as `Constructor` at
        // `pkg.User.User` (name = class short name, parent qualified
        // shape mirrors Java).
        let ctors: Vec<_> = facts
            .symbols
            .iter()
            .filter(|s| s.qualified == "com.example.app.User.User")
            .collect();
        assert!(
            !ctors.is_empty(),
            "expected at least one User.User constructor; got {:?}",
            facts
                .symbols
                .iter()
                .map(|s| (&s.name, &s.qualified, &s.kind))
                .collect::<Vec<_>>()
        );
        assert!(ctors.iter().all(|c| c.kind == SymbolKind::Constructor));
        assert!(ctors.iter().all(|c| c.name == "User"));
        // At least one of them (the primary) is body-less.
        assert!(ctors.iter().any(|c| c.body_start.is_none()));
    }

    #[test]
    fn secondary_constructor_emits_with_body_start() {
        let facts = facts();
        // The fixture `User` declares a secondary `constructor(id: Long)`
        // alongside its primary — there must be two ctor rows, and
        // exactly one of them carries a `body_start`.
        let ctors: Vec<_> = facts
            .symbols
            .iter()
            .filter(|s| {
                s.qualified == "com.example.app.User.User" && s.kind == SymbolKind::Constructor
            })
            .collect();
        assert_eq!(
            ctors.len(),
            2,
            "expected primary + secondary; got {:?}",
            ctors
        );
        let bodied = ctors.iter().filter(|c| c.body_start.is_some()).count();
        assert_eq!(bodied, 1, "exactly the secondary should carry body_start");
    }

    #[test]
    fn constructors_are_not_pushed_as_containers() {
        // Regression guard: ctors must not become nesting parents
        // (would re-qualify any sibling symbol under the ctor).
        let facts = facts();
        let user_idx = facts
            .symbols
            .iter()
            .position(|s| s.qualified == "com.example.app.User")
            .unwrap();
        let id_prop = symbol(&facts, "com.example.app.User.id");
        // `id` is the constructor `val` param — its parent must be
        // the class, not the synthetic constructor symbol.
        assert_eq!(id_prop.parent_idx, Some(user_idx));
    }

    #[test]
    fn extracts_imports_with_alias_and_wildcard() {
        let facts = facts();
        assert_eq!(facts.imports.len(), 3);

        let delay = &facts.imports[0];
        assert_eq!(delay.to_module, "kotlinx.coroutines");
        assert_eq!(delay.imported.as_deref(), Some("delay"));
        assert_eq!(delay.alias, None);

        let base = &facts.imports[1];
        assert_eq!(base.to_module, "com.example.lib");
        assert_eq!(base.imported.as_deref(), Some("Base"));
        assert_eq!(base.alias.as_deref(), Some("B"));

        let util = &facts.imports[2];
        assert_eq!(util.to_module, "com.example.util");
        assert_eq!(util.imported.as_deref(), Some("*"));
        assert_eq!(util.alias, None);

        assert!(facts.imports.iter().all(|i| !i.is_reexport));
    }
}
