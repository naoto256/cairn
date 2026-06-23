//! PHP Tier-2 analyzer (semantic enrichment over the same
//! tree-sitter-php parse the syntactic pass uses).
//!
//! Like the Python and TypeScript analyzers, this is *structural*
//! extraction, not type resolution:
//!
//! - **heritage edges** — `class Dog extends Animal` → [`ImplFact`]
//!   with `kind = "inherit"`; `class Dog implements Walker` →
//!   `kind = "implement"`; `use SomeTrait;` inside a class body →
//!   `kind = "mixin"`. Interface `extends` is `"inherit"`, enum
//!   `implements` is `"implement"`. Base names are stored as written
//!   (`\Countable` stays `\Countable`), matching how a user would
//!   query `find_subtypes` / `find_supertypes`.
//! - **refs** — call sites (`foo()`, `$obj->method()`, `Cls::stat()` →
//!   [`RefKind::Call`]) and instantiations (`new Widget()` →
//!   [`RefKind::Instantiate`]). Name-level: receiver types are unknown
//!   without deeper analysis. A post-walk pass fills `target_qualified`
//!   when the callee (function / method / constructor) or instantiated
//!   class is defined in the same file, expanding the bare
//!   `target_name` against the call site's namespace and container
//!   stack (`{ns}\{containers}::{name}`, `{ns}\{name}`, `{name}`). The
//!   pass runs after the full walk so calls that lexically precede the
//!   definition still resolve. Cross-file targets stay `None`, which
//!   hides them from `find_references`' default outgoing view (visible
//!   with `include_noise`).
//!
//! `use` imports are emitted by the Tier-1 pass ([`SyntacticFacts`]'
//! `imports`), so they are not duplicated here.
//!
//! Qualified names mirror the syntactic pass exactly: namespace
//! segments joined with `\`, class-like containers joined to members
//! with `::` (`App\Models\Widget::render`). Keeping the two passes
//! aligned lets the indexer resolve `ImplFact.type_qualified` and
//! `RefFact.enclosing_qualified` against the symbols table.

use std::collections::HashSet;
use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts, SyntacticKind,
};
use cairn_lang_treesitter_generic::{child_by_field, line_of, node_text};
use tree_sitter::{Node, Parser};

use crate::ANONYMOUS_CLASS_NAME;

/// PHP semantic analyzer. Re-parses with the mixed HTML/PHP grammar
/// (the same one the syntactic pass uses) and walks for heritage
/// edges and call / instantiation refs.
pub struct PhpAnalyzer;

impl Analyzer for PhpAnalyzer {
    fn name(&self) -> &'static str {
        "php-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_php::LANGUAGE_PHP.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut walker = PhpSemanticWalker {
            facts: SemanticFacts::default(),
            namespace: None,
            containers: Vec::new(),
            enclosing: None,
            defined: HashSet::new(),
            ref_sites: Vec::new(),
        };
        walker.walk(tree.root_node(), source);
        walker.resolve_same_file_callees();
        Ok(walker.facts)
    }
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(PhpAnalyzer)
}

struct PhpSemanticWalker {
    facts: SemanticFacts,
    /// Active `namespace Foo\Bar` prefix, if any.
    namespace: Option<String>,
    /// Enclosing class-like container names (class / interface / trait
    /// / enum), innermost last.
    containers: Vec<String>,
    /// Qualified name of the nearest enclosing function / method /
    /// container, attached to refs as `enclosing_qualified`.
    enclosing: Option<String>,
    /// Qualified names of every function / method / constructor / class
    /// declared in this file. Populated as the walk descends so that
    /// calls preceding the definition still resolve in the post-walk
    /// pass.
    defined: HashSet<String>,
    /// Active namespace + container stack captured at each ref site,
    /// parallel to `facts.refs`. Used to expand a ref's `target_name`
    /// into qualified candidates against `defined`.
    ref_sites: Vec<RefSite>,
}

struct RefSite {
    namespace: Option<String>,
    containers: Vec<String>,
}

impl PhpSemanticWalker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            "namespace_definition" => {
                let prefix = child_by_field(node, "name")
                    .map(|n| node_text(n, source).to_string())
                    .filter(|s| !s.is_empty());
                if let Some(body) = child_by_field(node, "body") {
                    // Braced form: the prefix scopes the block only.
                    let previous = self.namespace.take();
                    self.namespace = prefix;
                    self.walk_children(body, source);
                    self.namespace = previous;
                } else {
                    // Unbraced form: applies to the rest of the file.
                    self.namespace = prefix;
                }
            }
            "class_declaration"
            | "interface_declaration"
            | "trait_declaration"
            | "enum_declaration" => {
                let Some(name_node) = child_by_field(node, "name") else {
                    return;
                };
                let name = node_text(name_node, source).to_string();
                self.walk_container(node, source, name);
            }
            "anonymous_class" => {
                self.walk_container(node, source, ANONYMOUS_CLASS_NAME.to_string());
            }
            "use_declaration" => {
                // Trait mixin inside a class-like body:
                // `use Timestamps, SoftDeletes;`
                self.emit_trait_mixins(node, source);
            }
            "function_definition" | "method_declaration" => {
                let Some(name_node) = child_by_field(node, "name") else {
                    return;
                };
                let qualified = self.qualify(node_text(name_node, source));
                self.defined.insert(qualified.clone());
                let previous = self.enclosing.replace(qualified);
                self.walk_children(node, source);
                self.enclosing = previous;
            }
            "function_call_expression" => {
                self.emit_call(node, source);
                self.walk_children(node, source);
            }
            "member_call_expression"
            | "nullsafe_member_call_expression"
            | "scoped_call_expression" => {
                self.emit_method_call(node, source);
                self.walk_children(node, source);
            }
            "object_creation_expression" => {
                self.emit_instantiation(node, source);
                self.walk_children(node, source);
            }
            _ => self.walk_children(node, source),
        }
    }

    fn walk_children(&mut self, node: Node<'_>, source: &[u8]) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.walk(child, source);
        }
    }

    /// Enter a class-like container: emit its heritage edges, then walk
    /// the body with the container pushed on both the name stack and
    /// `enclosing` (class-level refs attach to the class).
    fn walk_container(&mut self, node: Node<'_>, source: &[u8], name: String) {
        let qualified = self.qualify(&name);
        self.defined.insert(qualified.clone());
        self.emit_heritage(node, source, &qualified);

        let previous_enclosing = self.enclosing.replace(qualified);
        self.containers.push(name);
        self.walk_children(node, source);
        self.containers.pop();
        self.enclosing = previous_enclosing;
    }

    /// `extends` (base_clause) → `"inherit"`, `implements`
    /// (class_interface_clause) → `"implement"`. The grammar reuses
    /// base_clause for interface extension, so the mapping holds for
    /// classes, interfaces, enums, and anonymous classes alike.
    fn emit_heritage(&mut self, node: Node<'_>, source: &[u8], type_qualified: &str) {
        let line = line_of(node);
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            let (kind, syntactic) = match child.kind() {
                "base_clause" => ("inherit", SyntacticKind::Extends),
                "class_interface_clause" => ("implement", SyntacticKind::Implements),
                _ => continue,
            };
            let mut names = child.walk();
            for base in child.named_children(&mut names) {
                if !matches!(base.kind(), "name" | "qualified_name") {
                    continue;
                }
                let interface = node_text(base, source).to_string();
                if interface.is_empty() {
                    continue;
                }
                let br = base.byte_range();
                self.facts.impls.push(ImplFact {
                    type_qualified: type_qualified.to_string(),
                    interface_qualified: Some(interface),
                    kind: kind.to_string(),
                    syntactic_kind: Some(syntactic),
                    line,
                    interface_byte_range: Some((br.start as u32, br.end as u32)),
                });
            }
        }
    }

    /// `use TraitA, TraitB;` inside a class body — one `"mixin"` edge
    /// per trait, anchored on the enclosing container.
    fn emit_trait_mixins(&mut self, node: Node<'_>, source: &[u8]) {
        if self.containers.is_empty() {
            return; // not inside a class-like body — not a mixin.
        }
        let type_qualified = self.qualify_container();
        let line = line_of(node);
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if !matches!(child.kind(), "name" | "qualified_name") {
                continue;
            }
            let interface = node_text(child, source).to_string();
            if interface.is_empty() {
                continue;
            }
            let cr = child.byte_range();
            self.facts.impls.push(ImplFact {
                type_qualified: type_qualified.clone(),
                interface_qualified: Some(interface),
                kind: "mixin".to_string(),
                syntactic_kind: Some(SyntacticKind::TraitUse),
                line,
                interface_byte_range: Some((cr.start as u32, cr.end as u32)),
            });
        }
    }

    /// `foo()` / `Foo\bar()` → `Call` ref on the bare last segment.
    /// Dynamic callees (`$fn()`, `($x)()`) are skipped.
    fn emit_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(func) = child_by_field(node, "function") else {
            return;
        };
        if !matches!(func.kind(), "name" | "qualified_name") {
            return;
        }
        self.push_ref(func, source, RefKind::Call);
    }

    /// `$obj->method()`, `$obj?->method()`, `Cls::method()` → `Call`
    /// ref on the method name; the receiver type is unknown at this
    /// tier so the ref stays name-level.
    fn emit_method_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name) = child_by_field(node, "name") else {
            return;
        };
        if name.kind() != "name" {
            return; // dynamic: `$obj->$method()`.
        }
        self.push_ref(name, source, RefKind::Call);
    }

    /// `new Widget(...)` → `Instantiate` ref on the class name.
    /// `new $class()` (dynamic) and `new class {}` (anonymous) emit
    /// nothing.
    fn emit_instantiation(&mut self, node: Node<'_>, source: &[u8]) {
        let mut cursor = node.walk();
        let Some(class) = node
            .named_children(&mut cursor)
            .find(|c| matches!(c.kind(), "name" | "qualified_name"))
        else {
            return;
        };
        self.push_ref(class, source, RefKind::Instantiate);
    }

    fn push_ref(&mut self, name_node: Node<'_>, source: &[u8], kind: RefKind) {
        let text = node_text(name_node, source);
        let target_name = last_segment(text).to_string();
        if target_name.is_empty() {
            return;
        }
        self.facts.refs.push(RefFact {
            target_name,
            target_qualified: None,
            kind,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: name_node.byte_range(),
            line: line_of(name_node),
        });
        self.ref_sites.push(RefSite {
            namespace: self.namespace.clone(),
            containers: self.containers.clone(),
        });
    }

    /// Fill `target_qualified` for Call / Instantiate refs whose
    /// callee is defined in this file. The lookup expands the bare
    /// `target_name` into qualified candidates using the call site's
    /// namespace and container stack and picks the first candidate
    /// that hits `defined`. Cross-file targets stay `None`.
    fn resolve_same_file_callees(&mut self) {
        let defined = &self.defined;
        let sites = &self.ref_sites;
        for (idx, r) in self.facts.refs.iter_mut().enumerate() {
            if r.target_qualified.is_some() {
                continue;
            }
            if !matches!(r.kind, RefKind::Call | RefKind::Instantiate) {
                continue;
            }
            let Some(site) = sites.get(idx) else {
                continue;
            };
            for candidate in candidates_for(&r.target_name, &r.kind, site) {
                if defined.contains(&candidate) {
                    r.target_qualified = Some(candidate);
                    break;
                }
            }
        }
    }

    /// Qualified name for `name` under the current namespace and
    /// container nesting (`App\Models\Widget::render`).
    fn qualify(&self, name: &str) -> String {
        let base = if self.containers.is_empty() {
            name.to_string()
        } else {
            format!("{}::{name}", self.containers.join("::"))
        };
        match &self.namespace {
            Some(ns) => format!("{ns}\\{base}"),
            None => base,
        }
    }

    /// Qualified name of the innermost container itself.
    fn qualify_container(&self) -> String {
        let base = self.containers.join("::");
        match &self.namespace {
            Some(ns) => format!("{ns}\\{base}"),
            None => base,
        }
    }
}

/// The last `\`-separated segment of a path (`Foo\Bar` → `Bar`,
/// `\Countable` → `Countable`). Used as the `target_name` so
/// `find_references symbol=Bar` matches name-level.
fn last_segment(path: &str) -> &str {
    path.rsplit('\\').next().unwrap_or(path)
}

/// Qualified-name candidates for a ref's `target_name`, given the call
/// site's namespace + container stack. `Call` expands against an
/// innermost-container member, a namespace-level function, and the
/// bare name; `Instantiate` only against namespace-level and bare
/// (PHP `new` resolves a class, not a member).
fn candidates_for(name: &str, kind: &RefKind, site: &RefSite) -> Vec<String> {
    let mut out = Vec::new();
    let ns = site.namespace.as_deref();
    match kind {
        RefKind::Call => {
            if !site.containers.is_empty() {
                let containers = site.containers.join("::");
                out.push(match ns {
                    Some(n) => format!("{n}\\{containers}::{name}"),
                    None => format!("{containers}::{name}"),
                });
            }
            if let Some(n) = ns {
                out.push(format!("{n}\\{name}"));
            }
            out.push(name.to_string());
        }
        RefKind::Instantiate => {
            if let Some(n) = ns {
                out.push(format!("{n}\\{name}"));
            }
            out.push(name.to_string());
        }
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        PhpAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn edges<'a>(f: &'a SemanticFacts, kind: &str) -> Vec<(&'a str, &'a str)> {
        f.impls
            .iter()
            .filter(|i| i.kind == kind)
            .map(|i| {
                (
                    i.type_qualified.as_str(),
                    i.interface_qualified.as_deref().unwrap_or(""),
                )
            })
            .collect()
    }

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    #[test]
    fn extends_and_implements_edges() {
        let f = semantic("<?php\nclass Dog extends Animal implements Walker, \\Countable {}\n");
        assert_eq!(edges(&f, "inherit"), [("Dog", "Animal")]);
        assert_eq!(
            edges(&f, "implement"),
            [("Dog", "Walker"), ("Dog", "\\Countable")]
        );
    }

    #[test]
    fn interface_extends_is_inherit() {
        let f = semantic("<?php\ninterface Big extends Small {}\n");
        assert_eq!(edges(&f, "inherit"), [("Big", "Small")]);
    }

    #[test]
    fn enum_implements_edge() {
        let f = semantic("<?php\nenum Status: string implements HasLabel {}\n");
        assert_eq!(edges(&f, "implement"), [("Status", "HasLabel")]);
    }

    #[test]
    fn trait_use_is_mixin_edge() {
        let f = semantic("<?php\nclass C {\n    use Timestamps, Soft\\Deletes;\n}\n");
        assert_eq!(
            edges(&f, "mixin"),
            [("C", "Timestamps"), ("C", "Soft\\Deletes")]
        );
    }

    #[test]
    fn namespace_reflected_in_type_qualified() {
        let f = semantic("<?php\nnamespace App\\Models;\nclass Dog extends Animal {}\n");
        assert_eq!(edges(&f, "inherit"), [("App\\Models\\Dog", "Animal")]);
    }

    #[test]
    fn top_level_use_is_not_a_mixin() {
        let f = semantic("<?php\nuse App\\Models\\Widget;\nclass C {}\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn function_call_ref_with_enclosing() {
        let f = semantic("<?php\nfunction caller() {\n    greet();\n}\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "greet")
            .expect("greet call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("caller"));
        assert_eq!(hit.target_qualified, None);
    }

    #[test]
    fn method_and_static_calls_are_name_level() {
        let src = "<?php\nclass W {\n    public function go() {\n        $this->render();\n        Helper::stat();\n        $x?->maybe();\n    }\n}\n";
        let f = semantic(src);
        for name in ["render", "stat", "maybe"] {
            let hit = calls(&f)
                .into_iter()
                .find(|r| r.target_name == name)
                .unwrap_or_else(|| panic!("{name} call missing"));
            assert_eq!(hit.target_qualified, None);
            assert_eq!(hit.enclosing_qualified.as_deref(), Some("W::go"));
        }
    }

    #[test]
    fn qualified_call_uses_last_segment() {
        let f = semantic("<?php\nApp\\Helpers\\slugify('x');\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "slugify")
            .expect("slugify call missing");
        assert_eq!(hit.enclosing_qualified, None);
    }

    #[test]
    fn new_expression_is_instantiate_ref() {
        let f =
            semantic("<?php\nclass Widget {}\nfunction make() {\n    return new Widget();\n}\n");
        let hit = f
            .refs
            .iter()
            .find(|r| r.kind == RefKind::Instantiate)
            .expect("instantiate ref missing");
        assert_eq!(hit.target_name, "Widget");
        // Class declared in the same file → resolved to its qualified.
        assert_eq!(hit.target_qualified.as_deref(), Some("Widget"));
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("make"));
    }

    #[test]
    fn same_file_callees_resolve_against_namespace_and_container() {
        let src = "<?php
namespace App\\Models;
function helper(): void {}
class Widget {
    public function go(): void {
        helper();
        $this->render();
        self::stat();
        new Widget();
    }
    public function render(): void {}
    public static function stat(): void {}
}
class External {
    public function unrelated(): void {
        Helper::across_file();
    }
}
";
        let f = semantic(src);
        let lookup = |name: &str, kind: RefKind| -> &RefFact {
            f.refs
                .iter()
                .find(|r| r.target_name == name && r.kind == kind)
                .unwrap_or_else(|| panic!("{name} ref missing"))
        };

        // Free function in current namespace.
        assert_eq!(
            lookup("helper", RefKind::Call).target_qualified.as_deref(),
            Some("App\\Models\\helper"),
        );
        // `$this->render()` — innermost-container member resolution.
        assert_eq!(
            lookup("render", RefKind::Call).target_qualified.as_deref(),
            Some("App\\Models\\Widget::render"),
        );
        // `self::stat()` — same container, scoped call.
        assert_eq!(
            lookup("stat", RefKind::Call).target_qualified.as_deref(),
            Some("App\\Models\\Widget::stat"),
        );
        // `new Widget()` — class defined in the same file.
        assert_eq!(
            lookup("Widget", RefKind::Instantiate)
                .target_qualified
                .as_deref(),
            Some("App\\Models\\Widget"),
        );
        // Cross-file callee (target_name has no matching same-file
        // definition) stays unresolved.
        assert_eq!(lookup("across_file", RefKind::Call).target_qualified, None,);
    }

    #[test]
    fn dynamic_callees_are_skipped() {
        let f = semantic("<?php\n$fn();\n$obj->$method();\nnew $class();\n");
        assert!(f.refs.is_empty());
    }

    #[test]
    fn method_call_enclosed_by_namespaced_method() {
        let f = semantic(
            "<?php\nnamespace App;\nclass W {\n    public function go() {\n        helper();\n    }\n}\n",
        );
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("App\\W::go"));
    }

    #[test]
    fn mixin_inside_namespace_uses_qualified_container() {
        let f = semantic("<?php\nnamespace App;\nclass C {\n    use Timestamps;\n}\n");
        assert_eq!(edges(&f, "mixin"), [("App\\C", "Timestamps")]);
    }

    #[test]
    fn trait_use_emits_syntactic_trait_use() {
        let f = semantic("<?php\nclass C {\n    use TraitName;\n}\n");
        let mixin = f
            .impls
            .iter()
            .find(|i| i.kind == "mixin")
            .expect("trait use mixin missing");
        assert_eq!(mixin.syntactic_kind, Some(SyntacticKind::TraitUse));
    }
}
