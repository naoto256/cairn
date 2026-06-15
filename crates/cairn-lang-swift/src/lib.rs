//! `cairn-lang-swift` — Swift backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-swift parse tree and emits
//! [`SymbolFact`]s for classes, structs, enums (including cases),
//! protocols, extensions, functions, initializers, properties,
//! typealiases, and import declarations.
//!
//! Tier-2 lives in [`analyzer`]: inheritance/conformance edges,
//! extension → extended-type edges, and same-file name-level call refs.
//!
//! Grammar notes (alex-pinkus tree-sitter-swift): `struct`, `enum`,
//! and `extension` declarations all surface as `class_declaration`
//! nodes; the `declaration_kind` field carries the actual keyword.
//! Protocols have their own `protocol_declaration` kind.

#![forbid(unsafe_code)]

mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct SwiftBackend;

impl LanguageBackend for SwiftBackend {
    fn name(&self) -> &'static str {
        "swift"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.swift"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-swift"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_swift::LANGUAGE.into();
        extract(source, &language, SwiftVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_SWIFT: fn() -> Box<dyn LanguageBackend> = || Box::new(SwiftBackend);

struct SwiftVisitor {
    nesting: NestingTracker,
}

impl SwiftVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
        }
    }
}

impl Visitor for SwiftVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        match node.kind() {
            "import_declaration" => {
                if let Some(import) = match_import(node, source) {
                    facts.imports.push(import);
                }
            }
            "property_declaration" | "protocol_property_declaration"
                if !is_function_local(node) =>
            {
                self.emit_properties(node, source, facts);
            }
            "enum_entry" => {
                self.emit_enum_cases(node, source, facts);
            }
            _ => {
                let Some((mut kind, name, body_start)) = match_swift_item(node, source) else {
                    return;
                };

                if matches!(kind, SymbolKind::Function) && self.in_type_container(facts) {
                    kind = SymbolKind::Method;
                }

                let idx = self.emit_symbol(node, source, facts, kind.clone(), name, body_start);
                if is_container(&kind) {
                    self.nesting.push(idx, node.end_byte());
                }
            }
        }
    }
}

impl SwiftVisitor {
    fn in_type_container(&self, facts: &SyntacticFacts) -> bool {
        matches!(
            self.nesting.parent_kind(facts),
            Some(
                SymbolKind::Class
                    | SymbolKind::Struct
                    | SymbolKind::Enum
                    | SymbolKind::Interface
                    | SymbolKind::Impl
            )
        )
    }

    /// One `property_declaration` can bind several names
    /// (`var a = 1, b = 2`, `let (x, y) = pair`); emit one symbol per
    /// bound identifier.
    fn emit_properties(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let kind = property_kind(node, self.in_type_container(facts));
        let body_start = property_body_start(node);
        let mut cursor = node.walk();
        let patterns: Vec<Node<'_>> = node.children_by_field_name("name", &mut cursor).collect();
        for pattern in patterns {
            for name in bound_identifiers(pattern, source) {
                self.emit_symbol(node, source, facts, kind.clone(), name, body_start);
            }
        }
    }

    fn emit_enum_cases(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let mut cursor = node.walk();
        let names: Vec<String> = node
            .children_by_field_name("name", &mut cursor)
            .filter(|n| n.is_named())
            .map(|n| node_text(n, source).to_string())
            .collect();
        for name in names {
            self.emit_symbol(node, source, facts, SymbolKind::Constant, name, None);
        }
    }

    fn emit_symbol(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
        kind: SymbolKind,
        name: String,
        body_start: Option<usize>,
    ) -> usize {
        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = swift_visibility(node, source);
        let doc = extract_doc(node, source);
        let parent_idx = self.nesting.current_parent();

        let idx = facts.symbols.len();
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind,
            signature,
            doc,
            visibility,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start,
            parent_idx,
        });
        idx
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Class
            | SymbolKind::Struct
            | SymbolKind::Enum
            | SymbolKind::Interface
            | SymbolKind::Impl
    )
}

fn match_swift_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    match node.kind() {
        // struct / enum / extension share this node kind; the
        // `declaration_kind` field carries the actual keyword.
        "class_declaration" => {
            let keyword = child_by_field(node, "declaration_kind")
                .map(|n| node_text(n, source))
                .unwrap_or("class");
            let kind = match keyword {
                "struct" => SymbolKind::Struct,
                "enum" => SymbolKind::Enum,
                "extension" => SymbolKind::Impl,
                _ => SymbolKind::Class,
            };
            let name = declared_type_name(node, source)?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((kind, name, body))
        }
        "protocol_declaration" => {
            let name = declared_type_name(node, source)?;
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Interface, name, body))
        }
        "function_declaration" | "protocol_function_declaration" => {
            // The grammar reuses the `name` field for the return type;
            // the first `name` child is the function's identifier.
            let name = child_by_field(node, "name")?;
            if name.kind() != "simple_identifier" {
                return None;
            }
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((
                SymbolKind::Function,
                node_text(name, source).to_string(),
                body,
            ))
        }
        "init_declaration" => {
            let body = child_by_field(node, "body").map(|n| n.start_byte());
            Some((SymbolKind::Constructor, "init".to_string(), body))
        }
        "typealias_declaration" | "associatedtype_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::TypeAlias,
                node_text(name, source).to_string(),
                None,
            ))
        }
        _ => None,
    }
}

/// Name of a `class_declaration` / `protocol_declaration`. Nominal
/// types carry a `type_identifier`; extensions carry a `user_type`
/// (possibly generic, e.g. `Array<Element>`) whose first
/// `type_identifier` is the extended base name.
fn declared_type_name(node: Node<'_>, source: &[u8]) -> Option<String> {
    let name = child_by_field(node, "name")?;
    match name.kind() {
        "type_identifier" => Some(node_text(name, source).to_string()),
        "user_type" => {
            let mut cursor = name.walk();
            name.named_children(&mut cursor)
                .find(|c| c.kind() == "type_identifier")
                .map(|c| node_text(c, source).to_string())
        }
        _ => Some(node_text(name, source).to_string()),
    }
}

fn property_kind(node: Node<'_>, in_type: bool) -> SymbolKind {
    if in_type {
        return SymbolKind::Property;
    }
    let mutability = child_by_field(node, "name")
        .and_then(|_| {
            let mut cursor = node.walk();
            node.children(&mut cursor)
                .find(|c| c.kind() == "value_binding_pattern")
        })
        .and_then(|binding| child_by_field(binding, "mutability"))
        .map(|n| n.kind().to_string());
    match mutability.as_deref() {
        Some("var") => SymbolKind::Variable,
        _ => SymbolKind::Constant,
    }
}

/// Body start for a property: the computed accessor block if present,
/// otherwise the `=` of the initializer so the signature stays
/// `let x: Int` rather than dragging the value expression along.
fn property_body_start(node: Node<'_>) -> Option<usize> {
    if let Some(computed) = child_by_field(node, "computed_value") {
        return Some(computed.start_byte());
    }
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .find(|c| c.kind() == "=" || c.kind() == "protocol_property_requirements")
        .map(|c| c.start_byte())
}

fn bound_identifiers(pattern: Node<'_>, source: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    collect_bound_identifiers(pattern, source, &mut out);
    out
}

fn collect_bound_identifiers(node: Node<'_>, source: &[u8], out: &mut Vec<String>) {
    if node.kind() == "simple_identifier" {
        out.push(node_text(node, source).to_string());
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_bound_identifiers(child, source, out);
    }
}

/// `property_declaration` doubles as the local-binding node inside
/// function bodies, accessors, and closures; those are not API surface
/// and would swamp the index, so only declarations outside executable
/// scopes are emitted.
fn is_function_local(node: Node<'_>) -> bool {
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

fn swift_visibility(node: Node<'_>, source: &[u8]) -> Option<Visibility> {
    let mut cursor = node.walk();
    let modifiers = node
        .children(&mut cursor)
        .find(|c| c.kind() == "modifiers")?;
    let mut mod_cursor = modifiers.walk();
    for child in modifiers.children(&mut mod_cursor) {
        if child.kind() == "visibility_modifier" {
            // Visibility modifiers can carry a scope suffix like
            // `private(set)`; classify on the leading keyword.
            let text = node_text(child, source);
            let keyword = text.split('(').next().unwrap_or(text).trim();
            return match keyword {
                "public" | "open" => Some(Visibility::Public),
                "internal" | "package" => Some(Visibility::Crate),
                "private" | "fileprivate" => Some(Visibility::Private),
                _ => None,
            };
        }
    }
    None
}

fn match_import(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let mut cursor = node.walk();
    let path = node
        .named_children(&mut cursor)
        .find(|c| c.kind() == "identifier")?;
    let to_module = node_text(path, source).trim().to_string();
    if to_module.is_empty() {
        return None;
    }
    let imported = to_module
        .rsplit('.')
        .find(|part| !part.is_empty())
        .map(str::to_string);
    Some(ImportFact {
        to_module,
        imported,
        alias: None,
        is_reexport: false,
        line: line_of(node),
    })
}

// ─── doc comments ──────────────────────────────────────────────────────────

/// Swift doc comments are `///` line runs or `/** ... */` blocks
/// immediately preceding the declaration. Plain `//` and `/* */`
/// comments are not docs.
fn extract_doc(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| match sibling.kind() {
        "comment" | "multiline_comment" => {
            let trimmed = text.trim_start();
            if trimmed.starts_with("///") {
                Some(DocCommentPart::Append(strip_doc_line(text)))
            } else if trimmed.starts_with("/**") {
                Some(DocCommentPart::Replace(strip_doc_block(text)))
            } else {
                Some(DocCommentPart::Reset)
            }
        }
        _ => None,
    })
}

fn strip_doc_line(text: &str) -> String {
    text.trim().trim_start_matches('/').trim().to_string()
}

fn strip_doc_block(text: &str) -> String {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix("/**")
        .and_then(|s| s.strip_suffix("*/"))
        .unwrap_or(trimmed);
    inner
        .lines()
        .map(|line| line.trim().trim_start_matches('*').trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"
import Foundation
import UIKit.UIView

/// A user model.
public struct User: Codable, Identifiable {
    public let id: Int
    var name: String = ""

    public init(id: Int) {
        self.id = id
    }
}

open class UserStore: ObservableObject {
    static let shared = UserStore()

    func add(_ user: User) {
        let local = user
        store(local)
    }

    func fetchAll() async throws -> [User] {
        return []
    }
}

enum NetworkError: Error {
    case timeout
    case http(code: Int, message: String)
    case fast, slow
}

protocol Repository {
    associatedtype Item
    func find(id: Int) -> Item?
    var count: Int { get }
}

extension UserStore: Repository {
    typealias Item = User

    func find(id: Int) -> User? { nil }
    var count: Int { 0 }
}

extension Array where Element == User {
    var names: [String] { [] }
}

struct Outer {
    struct Inner {
        let x: Int
    }
}

@propertyWrapper
struct Clamped<Value: Comparable> {
    var value: Value
}

func process<T: Sequence>(_ items: T) -> [String] where T.Element: CustomStringConvertible {
    return []
}

typealias UserHandler = (User) -> Void

private let topConstant = 42
var topVariable: Int = 0
"#;

    fn facts() -> SyntacticFacts {
        SwiftBackend.extract_syntactic(FIXTURE.as_bytes()).unwrap()
    }

    fn symbol<'a>(facts: &'a SyntacticFacts, qualified: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.qualified == qualified)
            .unwrap_or_else(|| panic!("{qualified} missing"))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(SwiftBackend.parser_id(), "tree-sitter-swift");
    }

    #[test]
    fn extracts_tier1_symbol_kinds() {
        let facts = facts();

        assert_eq!(symbol(&facts, "User").kind, SymbolKind::Struct);
        assert_eq!(symbol(&facts, "UserStore").kind, SymbolKind::Class);
        assert_eq!(symbol(&facts, "NetworkError").kind, SymbolKind::Enum);
        assert_eq!(symbol(&facts, "Repository").kind, SymbolKind::Interface);
        assert_eq!(symbol(&facts, "Clamped").kind, SymbolKind::Struct);
        assert_eq!(symbol(&facts, "process").kind, SymbolKind::Function);
        assert_eq!(symbol(&facts, "User.init").kind, SymbolKind::Constructor);
        assert_eq!(symbol(&facts, "UserHandler").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&facts, "topConstant").kind, SymbolKind::Constant);
        assert_eq!(symbol(&facts, "topVariable").kind, SymbolKind::Variable);
    }

    #[test]
    fn extensions_are_impls_qualifying_members_under_extended_type() {
        let facts = facts();

        let extensions: Vec<&SymbolFact> = facts
            .symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Impl)
            .collect();
        assert_eq!(extensions.len(), 2, "{extensions:?}");
        assert!(extensions.iter().any(|s| s.name == "UserStore"));
        assert!(extensions.iter().any(|s| s.name == "Array"));

        // Members declared in `extension UserStore` qualify under it.
        let find = facts
            .symbols
            .iter()
            .filter(|s| s.qualified == "UserStore.find")
            .count();
        assert_eq!(find, 1);
        assert_eq!(symbol(&facts, "UserStore.Item").kind, SymbolKind::TypeAlias);
        assert_eq!(symbol(&facts, "Array.names").kind, SymbolKind::Property);
    }

    #[test]
    fn methods_properties_and_enum_cases_nest_under_their_type() {
        let facts = facts();

        assert_eq!(symbol(&facts, "UserStore.add").kind, SymbolKind::Method);
        assert_eq!(
            symbol(&facts, "UserStore.fetchAll").kind,
            SymbolKind::Method
        );
        assert_eq!(symbol(&facts, "User.id").kind, SymbolKind::Property);
        assert_eq!(symbol(&facts, "User.name").kind, SymbolKind::Property);
        assert_eq!(
            symbol(&facts, "UserStore.shared").kind,
            SymbolKind::Property
        );

        // Enum cases, including associated-value and multi-name forms.
        for case in ["timeout", "http", "fast", "slow"] {
            assert_eq!(
                symbol(&facts, &format!("NetworkError.{case}")).kind,
                SymbolKind::Constant,
            );
        }
    }

    #[test]
    fn protocol_members_and_nested_types_are_extracted() {
        let facts = facts();

        assert_eq!(
            symbol(&facts, "Repository.Item").kind,
            SymbolKind::TypeAlias
        );
        assert_eq!(symbol(&facts, "Repository.find").kind, SymbolKind::Method);
        assert_eq!(
            symbol(&facts, "Repository.count").kind,
            SymbolKind::Property
        );
        assert_eq!(symbol(&facts, "Outer.Inner").kind, SymbolKind::Struct);
        assert_eq!(symbol(&facts, "Outer.Inner.x").kind, SymbolKind::Property);
    }

    #[test]
    fn function_local_bindings_are_skipped() {
        let facts = facts();
        assert!(facts.symbols.iter().all(|s| s.name != "local"));
    }

    #[test]
    fn visibility_maps_swift_modifiers() {
        let facts = facts();

        assert_eq!(symbol(&facts, "User").visibility, Some(Visibility::Public));
        assert_eq!(
            symbol(&facts, "UserStore").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "topConstant").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(symbol(&facts, "NetworkError").visibility, None);
    }

    #[test]
    fn doc_comment_and_signature_are_extracted() {
        let facts = facts();

        assert_eq!(symbol(&facts, "User").doc.as_deref(), Some("A user model."));
        assert_eq!(
            symbol(&facts, "process").signature.as_deref(),
            Some(
                "func process<T: Sequence>(_ items: T) -> [String] \
                 where T.Element: CustomStringConvertible"
            )
        );
        // Initializer value is excluded from the signature.
        assert_eq!(
            symbol(&facts, "topConstant").signature.as_deref(),
            Some("private let topConstant")
        );
    }

    #[test]
    fn extracts_imports_with_dotted_submodule_paths() {
        let facts = facts();

        assert_eq!(facts.imports.len(), 2);
        assert_eq!(facts.imports[0].to_module, "Foundation");
        assert_eq!(facts.imports[0].imported.as_deref(), Some("Foundation"));
        assert_eq!(facts.imports[1].to_module, "UIKit.UIView");
        assert_eq!(facts.imports[1].imported.as_deref(), Some("UIView"));
        assert!(facts.imports.iter().all(|i| !i.is_reexport));
    }

    #[test]
    fn empty_input_is_empty_ok() {
        let facts = SwiftBackend.extract_syntactic(b"").unwrap();
        assert!(facts.symbols.is_empty());
        assert!(facts.imports.is_empty());
    }
}
