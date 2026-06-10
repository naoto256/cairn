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
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    NestingTracker, Visitor, child_by_field, end_line_of, extract, line_of, node_text,
    signature_slice, truncate,
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
}

impl KotlinVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
        }
    }
}

impl Visitor for KotlinVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

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
        let qualified = self.nesting.qualified_for(&display_name, facts);
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

    Some(ImportFact {
        to_module,
        imported,
        alias,
        is_reexport: false,
        line: line_of(node),
    })
}

// ─── doc comments ──────────────────────────────────────────────────────────

/// KDoc: the closest preceding `/** ... */` block comment, with nothing but
/// extras between it and the declaration.
fn extract_kdoc(node: Node<'_>, source: &[u8]) -> Option<String> {
    let parent = node.parent()?;
    let mut cursor = parent.walk();
    let mut last_doc: Option<String> = None;

    for sibling in parent.children(&mut cursor) {
        if sibling.start_byte() >= node.start_byte() {
            break;
        }
        match sibling.kind() {
            "block_comment" => {
                let text = node_text(sibling, source);
                if text.trim_start().starts_with("/**") {
                    last_doc = Some(strip_kdoc_markers(text));
                } else {
                    last_doc = None;
                }
            }
            "line_comment" => last_doc = None,
            _ if sibling.is_extra() => {}
            _ => last_doc = None,
        }
    }

    last_doc.filter(|doc| !doc.is_empty())
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

data class User(val id: Long, var name: String)

object Registry {
    val users = mutableListOf<User>()
}

enum class Color { RED, GREEN }

/** Greets and fetches. */
class Service(private val repo: String) : B(), Greeter {
    val cache: MutableMap<String, User> = mutableMapOf()
    internal var counter = 0

    companion object {
        const val TAG = "Service"
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
        assert_eq!(symbol(&facts, "Greeter").kind, SymbolKind::Interface);
        assert_eq!(symbol(&facts, "Result").kind, SymbolKind::Class);
        assert_eq!(symbol(&facts, "User").kind, SymbolKind::Class);
        assert_eq!(symbol(&facts, "Registry").kind, SymbolKind::Class);
        assert_eq!(symbol(&facts, "Color").kind, SymbolKind::Enum);
        assert_eq!(symbol(&facts, "Handler").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&facts, "Service").kind, SymbolKind::Class);
    }

    #[test]
    fn nested_data_classes_are_qualified_under_sealed_parent() {
        let facts = facts();
        assert_eq!(symbol(&facts, "Result.Success").kind, SymbolKind::Class);
        assert_eq!(symbol(&facts, "Result.Failure").kind, SymbolKind::Class);
        assert_eq!(
            symbol(&facts, "Result.Success").parent_idx,
            Some(
                facts
                    .symbols
                    .iter()
                    .position(|s| s.qualified == "Result")
                    .unwrap()
            )
        );
    }

    #[test]
    fn data_class_constructor_val_var_params_become_properties() {
        let facts = facts();
        assert_eq!(symbol(&facts, "User.id").kind, SymbolKind::Property);
        assert_eq!(symbol(&facts, "User.name").kind, SymbolKind::Property);
        assert_eq!(
            symbol(&facts, "Service.repo").visibility,
            Some(Visibility::Private)
        );
    }

    #[test]
    fn companion_object_members_qualify_through_companion() {
        let facts = facts();
        assert_eq!(symbol(&facts, "Service.Companion").kind, SymbolKind::Class);
        assert_eq!(
            symbol(&facts, "Service.Companion.TAG").kind,
            SymbolKind::Constant
        );
        assert_eq!(
            symbol(&facts, "Service.Companion.create").kind,
            SymbolKind::Method
        );
    }

    #[test]
    fn member_functions_are_methods_and_top_level_are_functions() {
        let facts = facts();
        assert_eq!(symbol(&facts, "Service.greet").kind, SymbolKind::Method);
        assert_eq!(symbol(&facts, "Service.fetch").kind, SymbolKind::Method);
        assert_eq!(symbol(&facts, "Greeter.greet").kind, SymbolKind::Method);
        assert_eq!(symbol(&facts, "process").kind, SymbolKind::Function);
        assert!(
            symbol(&facts, "Service.fetch")
                .signature
                .as_deref()
                .unwrap()
                .contains("suspend fun fetch")
        );
    }

    #[test]
    fn extension_function_carries_receiver_in_qualified_name() {
        let facts = facts();
        let shout = symbol(&facts, "String.shout");
        assert_eq!(shout.kind, SymbolKind::Function);
        assert_eq!(shout.name, "shout");
    }

    #[test]
    fn nested_class_and_visibility() {
        let facts = facts();
        assert_eq!(symbol(&facts, "Service.Inner").kind, SymbolKind::Class);
        assert_eq!(
            symbol(&facts, "Service.Inner.helper").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(
            symbol(&facts, "Service.counter").visibility,
            Some(Visibility::Crate)
        );
        assert_eq!(symbol(&facts, "Service.cache").visibility, None);
    }

    #[test]
    fn const_and_properties() {
        let facts = facts();
        assert_eq!(symbol(&facts, "MAX_RETRIES").kind, SymbolKind::Constant);
        assert_eq!(
            symbol(&facts, "MAX_RETRIES").doc.as_deref(),
            Some("Max retry budget.")
        );
        assert_eq!(symbol(&facts, "Registry.users").kind, SymbolKind::Property);
        assert_eq!(symbol(&facts, "Service.cache").kind, SymbolKind::Property);
    }

    #[test]
    fn kdoc_attaches_to_class() {
        let facts = facts();
        assert_eq!(
            symbol(&facts, "Service").doc.as_deref(),
            Some("Greets and fetches.")
        );
    }

    #[test]
    fn locals_inside_function_bodies_are_ignored() {
        let facts = facts();
        assert!(facts.symbols.iter().all(|s| s.name != "local"));
        assert!(facts.symbols.iter().all(|s| s.name != "kind"));
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
