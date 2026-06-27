//! `cairn-lang-java` — Java backend.
//!
//! Tier-1 (syntactic): walks a tree-sitter-java parse tree and emits
//! [`SymbolFact`]s for classes, interfaces (including `@interface`
//! annotation types), enums, records, methods, constructors, fields,
//! constants, and enum constants, plus [`ImportFact`]s for the four
//! import forms (plain, on-demand `*`, `static`, `static` on-demand).
//! Methods annotated `@Test` / `@ParameterizedTest` / `@RepeatedTest` /
//! `@TestFactory` are tagged [`SymbolKind::Test`].
//!
//! Kind mapping notes:
//! - `record` → [`SymbolKind::Struct`]: a record is a nominal data
//!   carrier, closer to a struct than to a behavior-bearing class.
//! - `@interface` → [`SymbolKind::Interface`]: the JLS defines an
//!   annotation type as a kind of interface.
//!
//! Tier-2 (semantic): the [`analyzer`] module re-walks the same grammar
//! for `extends` / `implements` inheritance edges and same-file call /
//! instantiation refs. Tier-3 (jdtls) is a future slice.

#![forbid(unsafe_code)]

mod analyzer;

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImportFact, LANGUAGE_BACKENDS, LanguageBackend, SymbolFact, SymbolKind,
    SymbolScope, SyntacticFacts, Visibility,
};
use cairn_lang_treesitter_generic::{
    DocCommentPart, NestingTracker, Visitor, child_by_field, collapse_ws, end_line_of, extract,
    extract_doc_above_node, line_of, node_text, signature_slice, truncate,
};
use linkme::distributed_slice;
use tree_sitter::Node;

/// Backend instance.
pub struct JavaBackend;

impl LanguageBackend for JavaBackend {
    fn name(&self) -> &'static str {
        "java"
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.java"]
    }

    fn parser_id(&self) -> &'static str {
        "tree-sitter-java"
    }

    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_java::LANGUAGE.into();
        extract(source, &language, JavaVisitor::new())
    }

    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        Some(analyzer::analyzer())
    }
}

#[distributed_slice(LANGUAGE_BACKENDS)]
static REGISTER_JAVA: fn() -> Box<dyn LanguageBackend> = || Box::new(JavaBackend);

// ─── visitor ───────────────────────────────────────────────────────────────

struct JavaVisitor {
    nesting: NestingTracker,
}

impl JavaVisitor {
    fn new() -> Self {
        Self {
            nesting: NestingTracker::new("."),
        }
    }
}

impl Visitor for JavaVisitor {
    fn visit_node(&mut self, node: Node<'_>, source: &[u8], facts: &mut SyntacticFacts) {
        self.nesting.pop_outside(node.start_byte());

        match node.kind() {
            "import_declaration" => {
                if let Some(import) = match_import(node, source) {
                    facts.imports.push(import);
                }
            }
            "field_declaration" | "constant_declaration" => {
                self.emit_field_declarators(node, source, facts);
            }
            "enum_constant" => {
                let Some(name) = child_by_field(node, "name") else {
                    return;
                };
                let name = node_text(name, source).to_string();
                let body_start = child_by_field(node, "body").map(|n| n.start_byte());
                // Enum constants are implicitly public static final.
                self.emit_symbol(
                    node,
                    source,
                    facts,
                    SymbolKind::Constant,
                    name,
                    body_start,
                    Visibility::Public,
                );
            }
            _ => {
                let Some((mut kind, name, body_start)) = match_java_item(node, source) else {
                    return;
                };
                if matches!(kind, SymbolKind::Method) && is_test_method(node, source) {
                    kind = SymbolKind::Test;
                }
                let visibility =
                    java_visibility(node, self.nesting.parent_kind(facts).cloned().as_ref());
                let is_container = is_container(&kind);
                let idx = self.emit_symbol(node, source, facts, kind, name, body_start, visibility);
                if is_container {
                    self.nesting.push(idx, node.end_byte());
                }
            }
        }
    }
}

impl JavaVisitor {
    /// `int a, b;` declares one symbol per `declarator` field. Each
    /// symbol spans the whole declaration node (Java has no per-name
    /// sub-range worth slicing out).
    fn emit_field_declarators(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
    ) {
        let parent_kind = self.nesting.parent_kind(facts).cloned();
        // `constant_declaration` only appears in interface bodies;
        // `static final` fields and interface fields are constants too.
        let kind = if node.kind() == "constant_declaration"
            || matches!(parent_kind, Some(SymbolKind::Interface))
            || has_static_final(node)
        {
            SymbolKind::Constant
        } else {
            SymbolKind::Field
        };
        let visibility = java_visibility(node, parent_kind.as_ref());

        let mut cursor = node.walk();
        let declarators: Vec<Node<'_>> = node
            .children_by_field_name("declarator", &mut cursor)
            .collect();
        for declarator in declarators {
            let Some(name) = child_by_field(declarator, "name") else {
                continue;
            };
            let name = node_text(name, source).to_string();
            self.emit_symbol(node, source, facts, kind.clone(), name, None, visibility);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_symbol(
        &mut self,
        node: Node<'_>,
        source: &[u8],
        facts: &mut SyntacticFacts,
        kind: SymbolKind,
        name: String,
        body_start: Option<usize>,
        visibility: Visibility,
    ) -> usize {
        let qualified = self.nesting.qualified_for(&name, facts);
        let signature = signature_slice(node, source, body_start);
        let doc = extract_javadoc(node, source);
        let parent_idx = self.nesting.current_parent();

        let idx = facts.symbols.len();
        facts.symbols.push(SymbolFact {
            name,
            qualified,
            kind,
            signature,
            doc,
            visibility: Some(visibility),
            byte_range: node.byte_range(),
            line_range: line_of(node)..end_line_of(node),
            body_start,
            parent_idx,
            scope: SymbolScope::TopLevel,
        });
        idx
    }
}

fn is_container(kind: &SymbolKind) -> bool {
    matches!(
        kind,
        SymbolKind::Class | SymbolKind::Interface | SymbolKind::Enum | SymbolKind::Struct
    )
}

fn match_java_item(node: Node<'_>, source: &[u8]) -> Option<(SymbolKind, String, Option<usize>)> {
    let kind = match node.kind() {
        "class_declaration" => SymbolKind::Class,
        // `@interface` is an interface per the JLS.
        "interface_declaration" | "annotation_type_declaration" => SymbolKind::Interface,
        "enum_declaration" => SymbolKind::Enum,
        // A record is a nominal data carrier — closest to a struct.
        "record_declaration" => SymbolKind::Struct,
        "method_declaration" => SymbolKind::Method,
        "constructor_declaration" => SymbolKind::Constructor,
        // `String value() default "";` inside an `@interface`.
        "annotation_type_element_declaration" => SymbolKind::Method,
        _ => return None,
    };
    let name = child_by_field(node, "name")?;
    let body_start = child_by_field(node, "body").map(|n| n.start_byte());
    Some((kind, node_text(name, source).to_string(), body_start))
}

/// The `modifiers` node groups annotations and keyword modifiers; the
/// keywords (`public`, `static`, ...) are anonymous tokens, so the scan
/// walks all children, not just named ones.
fn find_modifiers<'a>(node: Node<'a>) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor).find(|c| c.kind() == "modifiers")
}

/// Explicit access modifier, or the contextual default: interface (and
/// annotation-type) members are implicitly public; everything else is
/// package-private. Both `protected` and package-private map to
/// [`Visibility::Crate`] — cairn's middle tier — mirroring the
/// TypeScript backend's treatment of `protected`.
fn java_visibility(node: Node<'_>, parent_kind: Option<&SymbolKind>) -> Visibility {
    if let Some(modifiers) = find_modifiers(node) {
        let mut cursor = modifiers.walk();
        for child in modifiers.children(&mut cursor) {
            match child.kind() {
                "public" => return Visibility::Public,
                "private" => return Visibility::Private,
                "protected" => return Visibility::Crate,
                _ => {}
            }
        }
    }
    match parent_kind {
        Some(SymbolKind::Interface) => Visibility::Public,
        _ => Visibility::Crate,
    }
}

fn has_static_final(node: Node<'_>) -> bool {
    let Some(modifiers) = find_modifiers(node) else {
        return false;
    };
    let (mut is_static, mut is_final) = (false, false);
    let mut cursor = modifiers.walk();
    for child in modifiers.children(&mut cursor) {
        match child.kind() {
            "static" => is_static = true,
            "final" => is_final = true,
            _ => {}
        }
    }
    is_static && is_final
}

/// JUnit-style test detection: a method whose modifiers carry a
/// `@Test`-shaped annotation. Matches on the last segment so
/// `@org.junit.jupiter.api.Test` also hits.
fn is_test_method(node: Node<'_>, source: &[u8]) -> bool {
    let Some(modifiers) = find_modifiers(node) else {
        return false;
    };
    let mut cursor = modifiers.walk();
    for child in modifiers.children(&mut cursor) {
        if !matches!(child.kind(), "marker_annotation" | "annotation") {
            continue;
        }
        let Some(name) = child_by_field(child, "name") else {
            continue;
        };
        let name = node_text(name, source);
        let last = name.rsplit('.').next().unwrap_or(name);
        if matches!(
            last,
            "Test" | "ParameterizedTest" | "RepeatedTest" | "TestFactory"
        ) {
            return true;
        }
    }
    false
}

/// Java imports have no alias form, so `alias` is always `None`:
/// - `import java.util.List;`        → `to_module=java.util.List`, `imported=List`
/// - `import java.util.*;`           → `to_module=java.util`, `imported=*`
/// - `import static a.b.C.max;`      → `to_module=a.b.C.max`, `imported=max`
/// - `import static a.b.C.*;`        → `to_module=a.b.C`, `imported=*`
///
/// `to_module` is the dotted path as written (sans `.*`), matching the
/// Go backend's full-path convention; `imported` is the last segment.
fn match_import(node: Node<'_>, source: &[u8]) -> Option<ImportFact> {
    let mut path: Option<Node<'_>> = None;
    let mut wildcard = false;
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "scoped_identifier" | "identifier" => path = Some(child),
            "asterisk" => wildcard = true,
            _ => {}
        }
    }
    let to_module = collapse_ws(node_text(path?, source)).replace(' ', "");
    if to_module.is_empty() {
        return None;
    }
    let imported = if wildcard {
        Some("*".to_string())
    } else {
        to_module.rsplit('.').next().map(ToString::to_string)
    };
    Some(ImportFact {
        to_module,
        imported,
        alias: None,
        is_reexport: false,
        line: line_of(node),

        byte_range: None,
    })
}

/// Javadoc lives in a `/** ... */` block comment immediately preceding
/// the declaration (annotations are part of the declaration's
/// `modifiers` child, so the comment really is the preceding sibling).
fn extract_javadoc(node: Node<'_>, source: &[u8]) -> Option<String> {
    extract_doc_above_node(node, source, |sibling, text| match sibling.kind() {
        "block_comment" if text.trim_start().starts_with("/**") => {
            Some(DocCommentPart::Replace(strip_javadoc_markers(text)))
        }
        // Any other comment between a javadoc and the declaration
        // detaches the javadoc.
        "block_comment" | "line_comment" => Some(DocCommentPart::Reset),
        _ => None,
    })
    .filter(|doc| !doc.is_empty())
}

fn strip_javadoc_markers(text: &str) -> String {
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

    fn facts(src: &str) -> SyntacticFacts {
        JavaBackend.extract_syntactic(src.as_bytes()).unwrap()
    }

    fn symbol<'a>(facts: &'a SyntacticFacts, name: &str) -> &'a SymbolFact {
        facts
            .symbols
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("{name} missing"))
    }

    #[test]
    fn parser_id_is_stable() {
        assert_eq!(JavaBackend.parser_id(), "tree-sitter-java");
    }

    #[test]
    fn extracts_class_interface_enum_record_method_constructor_field_constant() {
        let f = facts(
            r#"
public class Widget extends Base {
    public static final int LIMIT = 10;
    private String label;

    public Widget(String label) { this.label = label; }

    public String render() { return label; }
}

interface Renderer {
    int FLAG = 1;
    void draw();
}

enum Color { RED, GREEN }

record Point(int x, int y) {
    public int sum() { return x + y; }
}
"#,
        );

        assert_eq!(symbol(&f, "Widget").kind, SymbolKind::Class);
        assert_eq!(symbol(&f, "LIMIT").kind, SymbolKind::Constant);
        assert_eq!(symbol(&f, "label").kind, SymbolKind::Field);
        let ctor = f
            .symbols
            .iter()
            .find(|s| s.kind == SymbolKind::Constructor)
            .expect("constructor missing");
        assert_eq!(ctor.name, "Widget");
        assert_eq!(ctor.qualified, "Widget.Widget");
        assert_eq!(symbol(&f, "render").kind, SymbolKind::Method);
        assert_eq!(symbol(&f, "render").qualified, "Widget.render");
        assert_eq!(symbol(&f, "Renderer").kind, SymbolKind::Interface);
        // Interface field is implicitly a constant.
        assert_eq!(symbol(&f, "FLAG").kind, SymbolKind::Constant);
        assert_eq!(symbol(&f, "draw").kind, SymbolKind::Method);
        assert_eq!(symbol(&f, "Color").kind, SymbolKind::Enum);
        assert_eq!(symbol(&f, "RED").kind, SymbolKind::Constant);
        assert_eq!(symbol(&f, "RED").qualified, "Color.RED");
        assert_eq!(symbol(&f, "Point").kind, SymbolKind::Struct);
        assert_eq!(symbol(&f, "sum").qualified, "Point.sum");
    }

    #[test]
    fn multi_declarator_field_emits_one_symbol_per_name() {
        let f = facts("class C { private int a, b; }\n");
        assert_eq!(symbol(&f, "a").kind, SymbolKind::Field);
        assert_eq!(symbol(&f, "b").kind, SymbolKind::Field);
        assert_eq!(symbol(&f, "b").qualified, "C.b");
    }

    #[test]
    fn visibility_from_modifiers_and_contextual_defaults() {
        let f = facts(
            r#"
public class V {
    public int pub;
    protected int prot;
    private int priv;
    int pkg;
    void pkgMethod() {}
}

class PackagePrivate {}

interface I {
    void implicitlyPublic();
}
"#,
        );
        assert_eq!(symbol(&f, "V").visibility, Some(Visibility::Public));
        assert_eq!(symbol(&f, "pub").visibility, Some(Visibility::Public));
        assert_eq!(symbol(&f, "prot").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "priv").visibility, Some(Visibility::Private));
        assert_eq!(symbol(&f, "pkg").visibility, Some(Visibility::Crate));
        assert_eq!(symbol(&f, "pkgMethod").visibility, Some(Visibility::Crate));
        assert_eq!(
            symbol(&f, "PackagePrivate").visibility,
            Some(Visibility::Crate)
        );
        assert_eq!(
            symbol(&f, "implicitlyPublic").visibility,
            Some(Visibility::Public)
        );
    }

    #[test]
    fn static_final_field_is_constant_but_plain_static_is_field() {
        let f = facts("class C { static final int K = 1; static int counter; }\n");
        assert_eq!(symbol(&f, "K").kind, SymbolKind::Constant);
        assert_eq!(symbol(&f, "counter").kind, SymbolKind::Field);
    }

    #[test]
    fn nested_class_and_interface_qualify_under_outer() {
        let f = facts(
            r#"
class Outer {
    class Inner {
        void deep() {}
    }
    interface Hook {
        default void fire() {}
    }
}
"#,
        );
        assert_eq!(symbol(&f, "Inner").qualified, "Outer.Inner");
        assert_eq!(symbol(&f, "deep").qualified, "Outer.Inner.deep");
        let inner = symbol(&f, "Inner");
        assert_eq!(f.symbols[inner.parent_idx.unwrap()].name, "Outer");
        // Interface default method: still a Method, implicitly public.
        let fire = symbol(&f, "fire");
        assert_eq!(fire.kind, SymbolKind::Method);
        assert_eq!(fire.qualified, "Outer.Hook.fire");
        assert_eq!(fire.visibility, Some(Visibility::Public));
    }

    #[test]
    fn generics_survive_in_signatures() {
        let f = facts(
            "class Box<T extends Comparable<T>> {\n    public <U> java.util.List<U> map(U seed) { return null; }\n}\n",
        );
        let class_sig = symbol(&f, "Box").signature.as_deref().unwrap();
        assert!(class_sig.contains("Box<T extends Comparable<T>>"));
        let method_sig = symbol(&f, "map").signature.as_deref().unwrap();
        assert!(method_sig.contains("<U> java.util.List<U> map(U seed)"));
        assert!(!method_sig.contains("return"));
    }

    #[test]
    fn annotation_type_and_annotated_members() {
        let f = facts(
            r#"
@interface Marker {
    String value() default "";
}

class Annotated {
    @Override
    public String toString() { return ""; }

    @Deprecated
    @SuppressWarnings("unchecked")
    void old() {}
}
"#,
        );
        assert_eq!(symbol(&f, "Marker").kind, SymbolKind::Interface);
        // Annotation element behaves like an interface method.
        assert_eq!(symbol(&f, "value").kind, SymbolKind::Method);
        assert_eq!(symbol(&f, "value").visibility, Some(Visibility::Public));
        assert_eq!(symbol(&f, "toString").kind, SymbolKind::Method);
        assert_eq!(symbol(&f, "old").kind, SymbolKind::Method);
        let sig = symbol(&f, "old").signature.as_deref().unwrap();
        assert!(sig.contains("@Deprecated"));
    }

    #[test]
    fn junit_test_annotation_tags_method_as_test() {
        let f = facts(
            r#"
class WidgetTest {
    @Test
    void rendersLabel() {}

    @org.junit.jupiter.api.Test
    void qualifiedAnnotation() {}

    @ParameterizedTest
    void withParams(int x) {}

    void helper() {}
}
"#,
        );
        assert_eq!(symbol(&f, "rendersLabel").kind, SymbolKind::Test);
        assert_eq!(symbol(&f, "qualifiedAnnotation").kind, SymbolKind::Test);
        assert_eq!(symbol(&f, "withParams").kind, SymbolKind::Test);
        assert_eq!(symbol(&f, "helper").kind, SymbolKind::Method);
    }

    #[test]
    fn captures_javadoc_not_plain_comments() {
        let f = facts(
            r#"
/**
 * Renders widgets.
 * Second line.
 */
class Documented {
    /** Field doc. */
    int field;

    // plain comment, not javadoc
    void undocumented() {}
}
"#,
        );
        let doc = symbol(&f, "Documented").doc.as_deref().unwrap();
        assert!(doc.contains("Renders widgets."));
        assert!(doc.contains("Second line."));
        assert_eq!(symbol(&f, "field").doc.as_deref(), Some("Field doc."));
        assert_eq!(symbol(&f, "undocumented").doc, None);
    }

    #[test]
    fn extracts_all_import_forms() {
        let f = facts(
            "import java.util.List;\nimport java.util.*;\nimport static java.lang.Math.max;\nimport static java.util.Map.*;\nclass C {}\n",
        );
        assert_eq!(f.imports.len(), 4);

        assert_eq!(f.imports[0].to_module, "java.util.List");
        assert_eq!(f.imports[0].imported.as_deref(), Some("List"));
        assert_eq!(f.imports[1].to_module, "java.util");
        assert_eq!(f.imports[1].imported.as_deref(), Some("*"));
        assert_eq!(f.imports[2].to_module, "java.lang.Math.max");
        assert_eq!(f.imports[2].imported.as_deref(), Some("max"));
        assert_eq!(f.imports[3].to_module, "java.util.Map");
        assert_eq!(f.imports[3].imported.as_deref(), Some("*"));
        assert!(f.imports.iter().all(|i| i.alias.is_none()));
        assert!(f.imports.iter().all(|i| !i.is_reexport));
    }

    #[test]
    fn ignores_local_variables() {
        let f = facts("class C { void m() { int local = 1; final int k = 2; } }\n");
        assert!(f.symbols.iter().all(|s| s.name != "local"));
        assert!(f.symbols.iter().all(|s| s.name != "k"));
    }

    #[test]
    fn signature_excludes_body() {
        let f = facts("class C { public int add(int a, int b) { return a + b; } }\n");
        let sig = symbol(&f, "add").signature.as_deref().unwrap();
        assert!(sig.contains("public int add(int a, int b)"));
        assert!(!sig.contains("return"));
    }
}
