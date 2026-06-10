//! C# Tier-2 analyzer (semantic enrichment over the same tree-sitter
//! parse the syntactic pass uses).
//!
//! Like the Python and TypeScript analyzers, this is *structural*
//! extraction, not type resolution. The facts emitted:
//!
//! - **base-list edges** — `class Dog : Animal, IBark` emits one
//!   [`ImplFact`] per base. C#'s `base_list` is syntactically blind to
//!   whether a base is a class or an interface (both are bare type
//!   names; the `I`-prefix is only a convention), so every edge uses
//!   `kind = "inherit"` — the same single-kind convention the Python
//!   analyzer settled on when the distinction is unresolvable without
//!   Tier-3. TypeScript keeps `"inherit"` / `"implement"` apart only
//!   because its grammar separates `extends` from `implements`.
//! - **using refs** — each `using` directive emits a
//!   [`RefKind::Import`] ref targeting the last path segment, with the
//!   full dotted path in `target_qualified`. Complements the Tier-1
//!   [`ImportFact`]s (which populate the imports table) by making
//!   `find_references` see using sites.
//! - **call / new refs** — `Foo()` / `obj.Bar()` →
//!   [`RefKind::Call`]; `new Widget(...)` → [`RefKind::Instantiate`]
//!   (the Rust analyzer's convention for construction sites). These
//!   are name-level only: a method call's receiver type is unknown
//!   without Tier-3, so `target_qualified` stays `None`.
//!
//! Qualified names are built from namespace + type nesting, matching
//! the syntactic pass's `NestingTracker` (which pushes namespaces and
//! type containers but not methods), so `ImplFact.type_qualified`
//! lines up with the `symbols.qualified` the indexer resolves against.

use std::sync::Arc;

use cairn_lang_api::{Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts};
use cairn_lang_treesitter_generic::{child_by_field, collapse_ws, line_of, node_text};
use tree_sitter::{Node, Parser};

/// C# semantic analyzer. Re-parses the source with tree-sitter-c-sharp
/// (the same grammar the syntactic pass uses) and walks for base-list
/// edges, using refs, and call / instantiation refs.
pub struct CsharpAnalyzer;

impl Analyzer for CsharpAnalyzer {
    fn name(&self) -> &'static str {
        "csharp-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_c_sharp::LANGUAGE.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut walker = CsSemanticWalker {
            facts: SemanticFacts::default(),
            containers: Vec::new(),
            enclosing: None,
        };
        walker.walk(tree.root_node(), source);
        Ok(walker.facts)
    }
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(CsharpAnalyzer)
}

struct CsSemanticWalker {
    facts: SemanticFacts,
    /// Namespace and type names enclosing the cursor; joined with `.`
    /// to build qualified names (mirrors the Tier-1 `NestingTracker`).
    containers: Vec<String>,
    /// Qualified name of the nearest enclosing method / constructor /
    /// property / type, attached to refs as `enclosing_qualified`.
    enclosing: Option<String>,
}

impl CsSemanticWalker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }

        match node.kind() {
            "using_directive" => {
                self.emit_using_ref(node, source);
            }
            "namespace_declaration" | "file_scoped_namespace_declaration" => {
                // A file-scoped namespace has no body node — its
                // declarations are siblings — but since nothing else
                // can follow it at top level, pushing for the rest of
                // this subtree walk is equivalent. (For the block form
                // the children genuinely nest.)
                let Some(name_node) = child_by_field(node, "name") else {
                    self.walk_children(node, source);
                    return;
                };
                let name = node_text(name_node, source).to_string();
                self.containers.push(name);
                if node.kind() == "file_scoped_namespace_declaration" {
                    // Siblings follow the node; walk them under this
                    // namespace by *not* popping — the parent
                    // compilation_unit loop continues with the pushed
                    // scope, and nothing legal can precede a
                    // file-scoped namespace that needs the old scope.
                    self.walk_children(node, source);
                } else {
                    self.walk_children(node, source);
                    self.containers.pop();
                }
            }
            "class_declaration"
            | "interface_declaration"
            | "struct_declaration"
            | "record_declaration"
            | "enum_declaration" => {
                self.walk_type_declaration(node, source);
            }
            "method_declaration" | "constructor_declaration" | "property_declaration" => {
                self.walk_member(node, source);
            }
            "invocation_expression" => {
                self.emit_call(node, source);
                self.walk_children(node, source);
            }
            "object_creation_expression" => {
                self.emit_new(node, source);
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

    fn qualify(&self, name: &str) -> String {
        if self.containers.is_empty() {
            name.to_string()
        } else {
            format!("{}.{name}", self.containers.join("."))
        }
    }

    fn walk_type_declaration(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name_node) = child_by_field(node, "name") else {
            self.walk_children(node, source);
            return;
        };
        let name = node_text(name_node, source).to_string();
        let qualified = self.qualify(&name);
        self.emit_base_list(node, source, &qualified);

        let previous_enclosing = self.enclosing.replace(qualified);
        self.containers.push(name);
        self.walk_children(node, source);
        self.containers.pop();
        self.enclosing = previous_enclosing;
    }

    fn walk_member(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(name_node) = child_by_field(node, "name") else {
            self.walk_children(node, source);
            return;
        };
        let qualified = self.qualify(node_text(name_node, source));
        let previous_enclosing = self.enclosing.replace(qualified);
        self.walk_children(node, source);
        self.enclosing = previous_enclosing;
    }

    /// `class C : Base, IFoo` — one edge per base-list entry. A record's
    /// primary-constructor base (`record D(...) : B(...)`) arrives as a
    /// `primary_constructor_base_type` wrapping the type.
    fn emit_base_list(&mut self, node: Node<'_>, source: &[u8], type_qualified: &str) {
        let Some(base_list) = first_child_of_kind(node, "base_list") else {
            return;
        };
        let mut cursor = base_list.walk();
        for child in base_list.named_children(&mut cursor) {
            let base_node = if child.kind() == "primary_constructor_base_type" {
                let Some(ty) = child.named_child(0) else {
                    continue;
                };
                ty
            } else if child.kind() == "argument_list" {
                continue;
            } else {
                child
            };
            let base = collapse_ws(node_text(base_node, source));
            if base.is_empty() {
                continue;
            }
            self.facts.impls.push(ImplFact {
                type_qualified: type_qualified.to_string(),
                interface_qualified: Some(base),
                kind: "inherit".to_string(),
                line: line_of(base_node),
            });
        }
    }

    /// `using A.B.C;` → Import ref on `C` with the full path qualified.
    /// Alias and `using static` forms point at the aliased / static
    /// target the same way.
    fn emit_using_ref(&mut self, node: Node<'_>, source: &[u8]) {
        let alias_id = child_by_field(node, "name").map(|n| n.id());
        let mut cursor = node.walk();
        let mut target: Option<Node<'_>> = None;
        for child in node.named_children(&mut cursor) {
            if matches!(
                child.kind(),
                "identifier" | "qualified_name" | "generic_name" | "alias_qualified_name"
            ) && alias_id != Some(child.id())
            {
                target = Some(child);
            }
        }
        let Some(target) = target else {
            return;
        };
        let full = collapse_ws(node_text(target, source));
        if full.is_empty() {
            return;
        }
        let bare = last_segment(&full);
        self.facts.refs.push(RefFact {
            target_name: strip_generic_args(bare).to_string(),
            target_qualified: Some(full.clone()),
            kind: RefKind::Import,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: None,
            byte_range: target.byte_range(),
            line: line_of(target),
        });
    }

    /// `Foo()` → Call ref on `Foo`; `obj.Bar()` → Call ref on `Bar`.
    /// Receiver types are unknown without Tier-3, so `target_qualified`
    /// stays `None` (name-level, same as the Python analyzer).
    fn emit_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(function) = child_by_field(node, "function") else {
            return;
        };
        let name_node = match function.kind() {
            "identifier" => function,
            "generic_name" => match function.named_child(0) {
                Some(n) => n,
                None => return,
            },
            // `obj.Bar` / `obj.Bar<T>` — the `name` field holds the
            // member; unwrap a generic to its identifier.
            "member_access_expression" => match child_by_field(function, "name") {
                Some(n) if n.kind() == "generic_name" => match n.named_child(0) {
                    Some(id) => id,
                    None => return,
                },
                Some(n) => n,
                None => return,
            },
            _ => return,
        };
        let target_name = node_text(name_node, source).to_string();
        if target_name.is_empty() {
            return;
        }
        self.facts.refs.push(RefFact {
            target_name,
            target_qualified: None,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: name_node.byte_range(),
            line: line_of(name_node),
        });
    }

    /// `new Widget(...)` → Instantiate ref on `Widget` (the Rust
    /// analyzer's convention for construction sites). Name-level only.
    fn emit_new(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(ty) = child_by_field(node, "type") else {
            return;
        };
        let full = collapse_ws(node_text(ty, source));
        if full.is_empty() {
            return;
        }
        let bare = strip_generic_args(last_segment(&full));
        if bare.is_empty() {
            return;
        }
        self.facts.refs.push(RefFact {
            target_name: bare.to_string(),
            target_qualified: None,
            kind: RefKind::Instantiate,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: ty.byte_range(),
            line: line_of(ty),
        });
    }
}

/// The last dotted segment of a path (`A.B.C` → `C`).
fn last_segment(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

/// Drop a trailing generic argument list (`List<int>` → `List`).
fn strip_generic_args(name: &str) -> &str {
    name.split('<').next().unwrap_or(name).trim()
}

fn first_child_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).find(|c| c.kind() == kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        CsharpAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    // ─── base-list edges ───────────────────────────────────────────

    #[test]
    fn single_base_edge() {
        let f = semantic("class Dog : Animal {}");
        assert_eq!(f.impls.len(), 1);
        assert_eq!(f.impls[0].type_qualified, "Dog");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(f.impls[0].kind, "inherit");
    }

    #[test]
    fn class_and_interface_bases_share_inherit_kind() {
        let f = semantic("class Dog : Animal, IBark, IFetch {}");
        let bases: Vec<&str> = f
            .impls
            .iter()
            .filter_map(|i| i.interface_qualified.as_deref())
            .collect();
        assert_eq!(bases, &["Animal", "IBark", "IFetch"]);
        assert!(f.impls.iter().all(|i| i.kind == "inherit"));
    }

    #[test]
    fn interface_extends_interface() {
        let f = semantic("interface IDerived : IBase {}");
        assert_eq!(f.impls[0].type_qualified, "IDerived");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("IBase"));
    }

    #[test]
    fn record_primary_constructor_base() {
        let f = semantic("record Derived(int X) : Base(X);");
        assert_eq!(f.impls.len(), 1);
        assert_eq!(f.impls[0].type_qualified, "Derived");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Base"));
    }

    #[test]
    fn generic_base_kept_verbatim() {
        let f = semantic("class Handler : EventHandler<ClickEvent> {}");
        assert_eq!(
            f.impls[0].interface_qualified.as_deref(),
            Some("EventHandler<ClickEvent>")
        );
    }

    #[test]
    fn base_edge_qualifies_under_namespace_and_outer_type() {
        let f = semantic("namespace App { class Outer { class Inner : Base {} } }");
        let edge = f
            .impls
            .iter()
            .find(|i| i.interface_qualified.as_deref() == Some("Base"))
            .expect("Inner : Base edge missing");
        assert_eq!(edge.type_qualified, "App.Outer.Inner");
    }

    #[test]
    fn file_scoped_namespace_qualifies_types() {
        let f = semantic("namespace App.Web;\nclass Handler : IHandler {}\n");
        assert_eq!(f.impls[0].type_qualified, "App.Web.Handler");
    }

    #[test]
    fn no_base_list_no_edge() {
        let f = semantic("class Plain {}");
        assert!(f.impls.is_empty());
    }

    // ─── using refs ────────────────────────────────────────────────

    #[test]
    fn using_emits_import_ref_with_full_path() {
        let f = semantic("using System.Collections.Generic;\n");
        let imp: Vec<&RefFact> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::Import)
            .collect();
        assert_eq!(imp.len(), 1);
        assert_eq!(imp[0].target_name, "Generic");
        assert_eq!(
            imp[0].target_qualified.as_deref(),
            Some("System.Collections.Generic")
        );
    }

    // ─── call / new refs ───────────────────────────────────────────

    #[test]
    fn method_call_inside_method_enclosed_by_qualified_method() {
        let f = semantic("class W { void Render() { Helper(); } }");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "Helper")
            .expect("Helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.Render"));
        assert_eq!(hit.target_qualified, None);
    }

    #[test]
    fn member_call_is_name_level_unresolved() {
        let f = semantic("class C { void M(object obj) { obj.Render(); } }");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "Render")
            .expect("Render call missing");
        assert_eq!(hit.target_qualified, None);
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("C.M"));
    }

    #[test]
    fn generic_call_unwraps_to_identifier() {
        let f = semantic("class C { void M(object s) { s.Parse<int>(); Make<string>(); } }");
        let names: Vec<&str> = calls(&f).iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"Parse"));
        assert!(names.contains(&"Make"));
    }

    #[test]
    fn new_emits_instantiate_ref() {
        let f = semantic("class C { object M() { return new Widget(1); } }");
        let inst: Vec<&RefFact> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::Instantiate)
            .collect();
        assert_eq!(inst.len(), 1);
        assert_eq!(inst[0].target_name, "Widget");
        assert_eq!(inst[0].target_qualified, None);
        assert_eq!(inst[0].enclosing_qualified.as_deref(), Some("C.M"));
    }

    #[test]
    fn new_generic_and_qualified_strips_to_bare_name() {
        let f = semantic(
            "class C { void M() { var x = new System.Collections.Generic.List<int>(); } }",
        );
        let inst = f
            .refs
            .iter()
            .find(|r| r.kind == RefKind::Instantiate)
            .expect("instantiate ref missing");
        assert_eq!(inst.target_name, "List");
    }

    #[test]
    fn constructor_body_call_enclosed_by_constructor() {
        let f = semantic("class C { C() { Init(); } }");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "Init")
            .expect("Init call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("C.C"));
    }

    #[test]
    fn property_body_call_enclosed_by_property() {
        let f = semantic("class C { int X => Compute(); }");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "Compute")
            .expect("Compute call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("C.X"));
    }

    #[test]
    fn nested_call_arguments_also_emit() {
        let f = semantic("class C { void M() { Outer(Inner()); } }");
        let names: Vec<&str> = calls(&f).iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"Outer"));
        assert!(names.contains(&"Inner"));
    }
}
