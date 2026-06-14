//! `cairn-lang-php` — PHP backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-php parse tree (the mixed
//! HTML/PHP grammar, `LANGUAGE_PHP`) and emits [`SymbolFact`]s for
//! namespaces, classes, interfaces, traits, enums (cases included),
//! functions, methods, properties, constants (`const` and `define()`),
//! plus [`ImportFact`]s for `use` statements.
//!
//! Tier-2 (semantic): the [`analyzer`] module re-walks the same grammar
//! for `extends` / `implements` / trait-`use` edges and name-level
//! call / instantiation refs. Calls and `new` expressions whose callee
//! is defined in the same file have their `target_qualified` filled in
//! a post-walk pass (cross-file targets stay `None`).
//!
//! Qualified names use PHP's native separators: `\` joins namespace
//! segments, `::` joins a class-like container to its members —
//! `App\Models\Widget::render`. The namespace prefix is tracked outside
//! [`NestingTracker`] (which only supports a single separator).

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

/// Display name used for anonymous classes (`new class { ... }`),
/// matching how the PHP runtime reports them.
pub(crate) const ANONYMOUS_CLASS_NAME: &str = "class@anonymous";

/// Backend instance.
pub struct PhpBackend;

impl LanguageBackend for PhpBackend {
    fn name(&self) -> &'static str {
        "php"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.php", "*.phtml"]
    }

    fn shebang_patterns(&self) -> &'static [&'static str] {
        &["php"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-php"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        // `LANGUAGE_PHP` is the mixed HTML/PHP grammar: a `.php` file is
        // HTML with embedded `<?php ?>` sections, so the pure-PHP
        // variant (`LANGUAGE_PHP_ONLY`) would fail on template files.
        let language: tree_sitter::Language = tree_sitter_php::LANGUAGE_PHP.into();
        extract(source, &language, PhpVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_PHP: fn() -> Box<dyn LanguageBackend> = || Box::new(PhpBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

/// The active `namespace Foo\Bar;` scope. A braced namespace ends at
/// its closing brace; an unbraced one runs to the end of the file.
struct NamespaceScope {
    prefix: String,
    end_byte: usize,
    symbol_idx: usize,
}

struct PhpVisitor {
    nesting: NestingTracker,
    namespace: Option<NamespaceScope>,
}

impl PhpVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("::"),
            namespace: None,
        }
    }

    /// `App\Models\Widget::render` — class-nesting path joined with
    /// `::`, prefixed by the active namespace joined with `\`.
    fn qualified_for(&self, name: &str, facts: &SyntacticFacts) -> String {
        let base = self.nesting.qualified_for(name, facts);
        match &self.namespace {
            Some(ns) => format!("{}\\{base}", ns.prefix),
            None => base,
        }
    }

    fn current_parent(&self) -> Option<usize> {
        self.nesting
            .current_parent()
            .or(self.namespace.as_ref().map(|ns| ns.symbol_idx))
    }
}

impl Visitor for PhpVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let start = node.start_byte();
        self.nesting.pop_outside(start);
        if let Some(ns) = &self.namespace
            && ns.end_byte <= start
        {
            self.namespace = None;
        }

        match node.kind() {
            "namespace_definition" => self.emit_namespace(node, source, facts),
            "namespace_use_declaration" => emit_use_imports(node, source, facts),
            "property_declaration" => self.emit_properties(node, source, facts),
            "const_declaration" => self.emit_consts(node, source, facts),
            "function_call_expression" => self.emit_define(node, source, facts),
            _ => self.emit_declaration(node, source, facts),
        }
    }
}

impl PhpVisitor {
    /// `namespace App\Models;` (rest-of-file scope) or
    /// `namespace App { ... }` (braced scope).
    fn emit_namespace(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(name_node) = child_by_field(node, "name") else {
            return; // global `namespace { ... }` block — no prefix.
        };
        let name = node_text(name_node, source).to_string();
        let end_byte = match child_by_field(node, "body") {
            Some(body) => body.end_byte(),
            None => source.len(),
        };

        let idx = facts.symbols.len();
        facts.symbols.push(SymbolFact {
            name: name.clone(),
            qualified: name.clone(),
            kind: SymbolKind::Namespace,
            signature: Some(format!("namespace {name}")),
            doc: extract_phpdoc(node, source),
            visibility: None,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start: child_by_field(node, "body").map(|b| b.start_byte()),
            parent_idx: None,
        });
        self.namespace = Some(NamespaceScope {
            prefix: name,
            end_byte,
            symbol_idx: idx,
        });
    }

    /// class / interface / trait / enum / anonymous class / function /
    /// method — the single-symbol declaration shapes.
    fn emit_declaration(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some((kind, name, body_start)) = match_php_item(node, source) else {
            return;
        };

        let qualified = self.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let visibility = php_visibility(node, source, &kind);
        let doc = extract_phpdoc(node, source);
        let parent_idx = self.current_parent();

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

    /// `private int $count = 0, $other;` — one Property per
    /// `property_element`. The symbol name drops the `$` sigil so it
    /// matches how member accesses spell the property (`$obj->count`).
    fn emit_properties(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let visibility = php_visibility(node, source, &SymbolKind::Property);
        let doc = extract_phpdoc(node, source);
        let signature = signature_slice(node, source, None);
        let mut cursor = node.walk();
        for element in node.named_children(&mut cursor) {
            if element.kind() != "property_element" {
                continue;
            }
            let Some(var) = child_by_field(element, "name") else {
                continue;
            };
            let name = node_text(var, source).trim_start_matches('$').to_string();
            if name.is_empty() {
                continue;
            }
            let qualified = self.qualified_for(&name, facts);
            facts.symbols.push(SymbolFact {
                name,
                qualified,
                kind: SymbolKind::Property,
                signature: signature.clone(),
                doc: doc.clone(),
                visibility,
                byte_range: element.byte_range(),
                line_range: line_of(element)..end_line_of(element),
                body_start: None,
                parent_idx: self.current_parent(),
            });
        }
    }

    /// `const A = 1, B = 2;` at file or class level — one Constant per
    /// `const_element`.
    fn emit_consts(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let visibility = php_visibility(node, source, &SymbolKind::Constant);
        let doc = extract_phpdoc(node, source);
        let mut cursor = node.walk();
        for element in node.named_children(&mut cursor) {
            if element.kind() != "const_element" {
                continue;
            }
            let Some(name_node) = element.named_child(0) else {
                continue;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = self.qualified_for(&name, facts);
            facts.symbols.push(SymbolFact {
                name,
                qualified,
                kind: SymbolKind::Constant,
                signature: signature_slice(element, source, None),
                doc: doc.clone(),
                visibility,
                byte_range: element.byte_range(),
                line_range: line_of(element)..end_line_of(element),
                body_start: None,
                parent_idx: self.current_parent(),
            });
        }
    }

    /// `define('NAME', value)` — PHP's runtime constant declaration.
    /// Emitted wherever it appears (conditional defines are common).
    fn emit_define(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        let Some(func) = child_by_field(node, "function") else {
            return;
        };
        if func.kind() != "name" || node_text(func, source) != "define" {
            return;
        }
        let Some(name) = first_string_argument(node, source) else {
            return;
        };
        let qualified = self.qualified_for(&name, facts);
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind: SymbolKind::Constant,
            signature: signature_slice(node, source, None),
            doc: None,
            visibility: None,
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start: None,
            parent_idx: self.current_parent(),
        });
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Class | SymbolKind::Interface | SymbolKind::Trait | SymbolKind::Enum
    )
}

fn match_php_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    let body_start = || child_by_field(node, "body").map(|n| n.start_byte());
    match node.kind() {
        "class_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Class,
                node_text(name, source).to_string(),
                body_start(),
            ))
        }
        "anonymous_class" => Some((
            SymbolKind::Class,
            ANONYMOUS_CLASS_NAME.to_string(),
            body_start(),
        )),
        "interface_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Interface,
                node_text(name, source).to_string(),
                body_start(),
            ))
        }
        "trait_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Trait,
                node_text(name, source).to_string(),
                body_start(),
            ))
        }
        "enum_declaration" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Enum,
                node_text(name, source).to_string(),
                body_start(),
            ))
        }
        "enum_case" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Constant,
                node_text(name, source).to_string(),
                None,
            ))
        }
        "function_definition" => {
            let name = child_by_field(node, "name")?;
            Some((
                SymbolKind::Function,
                node_text(name, source).to_string(),
                body_start(),
            ))
        }
        "method_declaration" => {
            let name = child_by_field(node, "name")?;
            let name = node_text(name, source).to_string();
            let kind = if name == "__construct" {
                SymbolKind::Constructor
            } else {
                SymbolKind::Method
            };
            Some((kind, name, body_start()))
        }
        _ => None,
    }
}

/// Visibility from an explicit `visibility_modifier`; class members
/// without one default to public (PHP semantics). `protected` maps to
/// [`Visibility::Crate`], matching the TypeScript backend's convention.
fn php_visibility(node: Node<'_>, source: &[u8], kind: &SymbolKind) -> Option<Visibility> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "visibility_modifier" {
            return match node_text(child, source) {
                "public" => Some(Visibility::Public),
                "private" => Some(Visibility::Private),
                "protected" => Some(Visibility::Crate),
                _ => None,
            };
        }
    }
    match kind {
        SymbolKind::Method | SymbolKind::Constructor | SymbolKind::Property => {
            Some(Visibility::Public)
        }
        _ => None,
    }
}

/// First string argument of a call — the constant name in
/// `define('NAME', ...)`.
fn first_string_argument(call: Node<'_>, source: &[u8]) -> Option<String> {
    let args = child_by_field(call, "arguments")?;
    let mut cursor = args.walk();
    let first = args
        .named_children(&mut cursor)
        .find(|c| c.kind() == "argument")?;
    let string = first.named_child(0)?;
    if string.kind() != "string" {
        return None;
    }
    let mut sc = string.walk();
    let content = string
        .named_children(&mut sc)
        .find(|c| c.kind() == "string_content")?;
    let text = node_text(content, source).to_string();
    (!text.is_empty()).then_some(text)
}

// ─── use statements → imports ──────────────────────────────────────────────

/// `use Foo\Bar;`, `use Foo\Bar as Baz;`, `use function Foo\baz;`,
/// `use Foo\{A, B as C};` — one [`ImportFact`] per imported name with
/// `to_module` holding the full path as written (Go-style; PHP has no
/// module/name split in a plain `use`).
fn emit_use_imports(node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
    let line = line_of(node);
    let mut group_prefix: Option<String> = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            // Group form: `use App\Traits\{...}` — the prefix sits
            // outside the braces as a bare namespace_name.
            "namespace_name" => group_prefix = Some(node_text(child, source).to_string()),
            "namespace_use_clause" => emit_use_clause(child, source, None, line, facts),
            "namespace_use_group" => {
                let mut gc = child.walk();
                for clause in child.named_children(&mut gc) {
                    if clause.kind() == "namespace_use_clause" {
                        emit_use_clause(clause, source, group_prefix.as_deref(), line, facts);
                    }
                }
            }
            _ => {}
        }
    }
}

fn emit_use_clause(
    clause: Node<'_>,
    source: &[u8],
    prefix: Option<&str>,
    line: u32,
    facts: &mut SyntacticFacts,
) {
    let alias = child_by_field(clause, "alias").map(|n| node_text(n, source).to_string());
    // The imported path is the first non-field name/qualified_name
    // child (the alias `name` carries the `alias` field and is skipped).
    let mut target: Option<String> = None;
    let mut cursor = clause.walk();
    if cursor.goto_first_child() {
        loop {
            let child = cursor.node();
            if cursor.field_name().is_none()
                && matches!(child.kind(), "name" | "qualified_name" | "namespace_name")
            {
                target = Some(node_text(child, source).to_string());
                break;
            }
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
    let Some(path) = target else { return };
    let to_module = match prefix {
        Some(p) => format!("{p}\\{path}"),
        None => path,
    };
    facts.imports.push(ImportFact {
        to_module,
        imported: None,
        alias,
        is_reexport: false,
        line,
    });
}

// ─── doc comments ──────────────────────────────────────────────────────────

/// PHPDoc blocks are `/** ... */` comments immediately preceding a
/// declaration, surfaced by the grammar as sibling `comment` nodes.
/// Same scan-back shape as the TypeScript backend's JSDoc extraction.
fn extract_phpdoc(node: Node<'_>, source: &[u8]) -> Option<String> {
    let parent = node.parent()?;
    let mut cursor = parent.walk();
    let mut last_doc: Option<String> = None;

    for sibling in parent.children(&mut cursor) {
        if sibling.start_byte() >= node.start_byte() {
            break;
        }
        if sibling.kind() == "comment" {
            let text = node_text(sibling, source);
            if text.trim_start().starts_with("/**") {
                last_doc = Some(strip_phpdoc_markers(text));
            } else {
                last_doc = None;
            }
        } else if !sibling.is_extra() {
            last_doc = None;
        }
    }

    last_doc.filter(|doc| !doc.is_empty())
}

fn strip_phpdoc_markers(text: &str) -> String {
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

    fn symbol<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("symbol {name} not found"))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(PhpBackend.parser_id(), "tree-sitter-php");
    }

    #[test]
    fn extracts_class_interface_trait_enum_function() {
        let src = br#"<?php
/** Widget doc. */
class Widget {
    public function render(): string { return ''; }
}
interface Renderable {}
trait Timestamps {}
enum Status: string {
    case Active = 'active';
}
function top_level(): bool { return true; }
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();

        assert_eq!(symbol(&facts, "Widget").kind, SymbolKind::Class);
        assert_eq!(symbol(&facts, "Widget").doc.as_deref(), Some("Widget doc."));
        assert_eq!(symbol(&facts, "render").kind, SymbolKind::Method);
        assert_eq!(symbol(&facts, "render").qualified, "Widget::render");
        assert_eq!(symbol(&facts, "Renderable").kind, SymbolKind::Interface);
        assert_eq!(symbol(&facts, "Timestamps").kind, SymbolKind::Trait);
        assert_eq!(symbol(&facts, "Status").kind, SymbolKind::Enum);
        assert_eq!(symbol(&facts, "Active").kind, SymbolKind::Constant);
        assert_eq!(symbol(&facts, "Active").qualified, "Status::Active");
        assert_eq!(symbol(&facts, "top_level").kind, SymbolKind::Function);
    }

    #[test]
    fn namespace_prefixes_qualified_names() {
        let src = br#"<?php
namespace App\Models;

class Widget {
    public function render(): void {}
}
function helper(): void {}
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();

        let ns = symbol(&facts, "App\\Models");
        assert_eq!(ns.kind, SymbolKind::Namespace);
        assert_eq!(symbol(&facts, "Widget").qualified, "App\\Models\\Widget");
        assert_eq!(
            symbol(&facts, "render").qualified,
            "App\\Models\\Widget::render"
        );
        assert_eq!(symbol(&facts, "helper").qualified, "App\\Models\\helper");
        // Top-level declarations parent under the namespace symbol.
        let widget = symbol(&facts, "Widget");
        assert_eq!(
            facts.symbols[widget.parent_idx.unwrap()].name,
            "App\\Models"
        );
    }

    #[test]
    fn visibility_from_modifiers_with_public_default() {
        let src = br#"<?php
class C {
    private int $count = 0;
    protected static ?string $label;
    var $legacy;
    public const STATUS = 'ok';
    function plain(): void {}
    private function hidden(): void {}
}
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();

        assert_eq!(
            symbol(&facts, "count").visibility,
            Some(Visibility::Private)
        );
        assert_eq!(symbol(&facts, "count").kind, SymbolKind::Property);
        assert_eq!(symbol(&facts, "label").visibility, Some(Visibility::Crate));
        assert_eq!(
            symbol(&facts, "legacy").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(
            symbol(&facts, "STATUS").visibility,
            Some(Visibility::Public)
        );
        assert_eq!(symbol(&facts, "STATUS").kind, SymbolKind::Constant);
        assert_eq!(symbol(&facts, "plain").visibility, Some(Visibility::Public));
        assert_eq!(
            symbol(&facts, "hidden").visibility,
            Some(Visibility::Private)
        );
    }

    #[test]
    fn constructor_kind_for_construct() {
        let facts = PhpBackend
            .extract_syntactic(b"<?php class C { public function __construct() {} }")
            .unwrap();
        assert_eq!(symbol(&facts, "__construct").kind, SymbolKind::Constructor);
    }

    #[test]
    fn define_and_top_level_const() {
        let src = br#"<?php
define('APP_VERSION', '1.0');
const TOP_LEVEL = 2;
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();

        assert_eq!(symbol(&facts, "APP_VERSION").kind, SymbolKind::Constant);
        assert_eq!(symbol(&facts, "TOP_LEVEL").kind, SymbolKind::Constant);
    }

    #[test]
    fn use_statements_become_imports() {
        let src = br#"<?php
use App\Contracts\Jsonable;
use App\Models\Widget as W;
use function App\Helpers\slugify;
use App\Traits\{Timestamps, SoftDeletes as SD};
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();

        assert_eq!(facts.imports.len(), 5);
        assert!(
            facts
                .imports
                .iter()
                .any(|i| { i.to_module == "App\\Contracts\\Jsonable" && i.alias.is_none() })
        );
        assert!(
            facts.imports.iter().any(|i| {
                i.to_module == "App\\Models\\Widget" && i.alias.as_deref() == Some("W")
            })
        );
        assert!(
            facts
                .imports
                .iter()
                .any(|i| i.to_module == "App\\Helpers\\slugify")
        );
        assert!(
            facts
                .imports
                .iter()
                .any(|i| i.to_module == "App\\Traits\\Timestamps" && i.alias.is_none())
        );
        assert!(facts.imports.iter().any(|i| {
            i.to_module == "App\\Traits\\SoftDeletes" && i.alias.as_deref() == Some("SD")
        }));
    }

    #[test]
    fn html_mixed_file_still_extracts_php_sections() {
        let src = br#"<html><body>
<?php function render_header(): string { return '<h1>'; } ?>
<div><?php echo render_header(); ?></div>
</body></html>
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();
        assert_eq!(symbol(&facts, "render_header").kind, SymbolKind::Function);
    }

    #[test]
    fn claims_phtml_templates() {
        assert!(PhpBackend.file_patterns().contains(&"*.phtml"));
    }

    #[test]
    fn anonymous_class_methods_nest_under_placeholder() {
        let src = br#"<?php
$x = new class {
    public function hi(): void {}
};
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();

        let anon = symbol(&facts, ANONYMOUS_CLASS_NAME);
        assert_eq!(anon.kind, SymbolKind::Class);
        let hi = symbol(&facts, "hi");
        assert_eq!(hi.qualified, "class@anonymous::hi");
        assert_eq!(
            facts.symbols[hi.parent_idx.unwrap()].name,
            ANONYMOUS_CLASS_NAME
        );
    }

    #[test]
    fn braced_namespace_scopes_prefix() {
        let src = br#"<?php
namespace First {
    function a(): void {}
}
namespace Second {
    function b(): void {}
}
"#;
        let facts = PhpBackend.extract_syntactic(src).unwrap();
        assert_eq!(symbol(&facts, "a").qualified, "First\\a");
        assert_eq!(symbol(&facts, "b").qualified, "Second\\b");
    }

    #[test]
    fn signature_excludes_body() {
        let facts = PhpBackend
            .extract_syntactic(b"<?php function f(int $x): bool { return $x > 0; }")
            .unwrap();
        let sig = symbol(&facts, "f").signature.as_deref().unwrap();
        assert!(sig.contains("function f(int $x): bool"));
        assert!(!sig.contains("return"));
    }
}
