//! Python Tier-2 analyzer (semantic enrichment over the same
//! tree-sitter parse the syntactic pass uses).
//!
//! cairn's Tier-2 is *structural* extraction, not type resolution
//! (Rust's `syn` pass is name-level too; receiver-type resolution is
//! Tier-3 / rust-analyzer / pyright territory). For Python the
//! semantic facts that have a faithful, statically-extractable meaning
//! are:
//!
//! - **imports** — `import x`, `import x.y as z`, `from m import a, b`,
//!   `from . import c`, `from m import *`. Populates the `imports`
//!   table so `find_imports` works on Python repos.
//! - **inheritance edges** — `class Dog(Animal):` is Python's closest
//!   analog to Rust's `impl Trait for Type`. Emitted as an [`ImplFact`]
//!   with `type_qualified = Dog`, `interface_qualified = Animal`,
//!   `kind = "inherit"`. Populates the `implementations` table so
//!   `find_subtypes name=Animal` answers "what subclasses Animal".
//!
//! - **refs** — call sites (`foo()`, `obj.method()` → `RefKind::Call`)
//!   and signature type annotations (`def f(p: T) -> R` → `RefKind::Type`
//!   with `TypeRole::Param` / `Return`). Populates the `refs` table so
//!   `find_references` works on Python. These are name-level only and
//!   carry the same dynamic-dispatch limitation as Rust's syn pass: a
//!   method call's receiver type is unknown without Tier-3, so
//!   `target_qualified` stays `None`. Decorators and read/write variable
//!   refs are a possible later refinement.
//!
//! Qualified names are built from the **class** nesting only, matching
//! the syntactic pass: its `NestingTracker` pushes containers (Class /
//! Module) but not functions, so a class defined inside a function
//! qualifies under the enclosing class, not the function. Tracking the
//! same class-only scope here keeps `ImplFact.type_qualified` aligned
//! with the `symbols.qualified` the indexer resolves against.

use std::sync::Arc;

use cairn_lang_api::{
    Analyzer, ExtractError, ImplFact, ImportFact, RefFact, RefKind, SemanticFacts, SyntacticKind,
    TypeRole,
};
use cairn_lang_treesitter_generic::{child_by_field, collapse_ws, line_of, node_text};
use tree_sitter::{Node, Parser};

/// Python semantic analyzer. Re-parses the source with
/// tree-sitter-python (the same grammar the syntactic pass uses) and
/// walks for imports, inheritance edges, and refs (call sites +
/// signature type annotations).
pub struct PythonAnalyzer;

impl Analyzer for PythonAnalyzer {
    fn name(&self) -> &'static str {
        "python-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_python::LANGUAGE.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut facts = SemanticFacts::default();
        let mut class_stack: Vec<String> = Vec::new();
        walk(tree.root_node(), source, &mut class_stack, None, &mut facts);
        Ok(facts)
    }
}

/// Recursive walk maintaining:
/// - `class_stack`: the enclosing-class names (functions are not pushed,
///   matching the syntactic pass — see module docs), used to build
///   qualified names.
/// - `enclosing`: the qualified name of the nearest enclosing
///   function / method / class, or `None` at module level. Refs attach
///   this as `enclosing_qualified` so the indexer can resolve
///   `refs.enclosing_id` against the symbols table.
fn walk(
    node: Node<'_>,
    source: &[u8],
    class_stack: &mut Vec<String>,
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    match node.kind() {
        "import_statement" => {
            emit_import_statement(node, source, facts);
        }
        "import_from_statement" => {
            emit_from_import(node, source, facts);
        }
        "class_definition" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = qualify(class_stack, &name);
            emit_base_classes(node, source, &qualified, facts);
            class_stack.push(name);
            // Class-body refs (e.g. a class-level annotated assignment)
            // attach the class as enclosing.
            recurse(node, source, class_stack, Some(&qualified), facts);
            class_stack.pop();
            return;
        }
        "function_definition" => {
            let Some(name_node) = child_by_field(node, "name") else {
                return;
            };
            let name = node_text(name_node, source).to_string();
            let qualified = qualify(class_stack, &name);
            // Signature param / return annotations attach the function
            // itself as enclosing.
            emit_signature_annotations(node, source, &qualified, facts);
            // Body refs (calls, etc.) likewise nest under the function.
            recurse(node, source, class_stack, Some(&qualified), facts);
            return;
        }
        "call" => {
            emit_call(node, source, enclosing, facts);
            // fall through to recurse into the arguments (nested calls).
        }
        _ => {}
    }
    recurse(node, source, class_stack, enclosing, facts);
}

fn recurse(
    node: Node<'_>,
    source: &[u8],
    class_stack: &mut Vec<String>,
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            walk(cursor.node(), source, class_stack, enclosing, facts);
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

/// Join the enclosing class names with `.` to mirror the syntactic
/// pass's `NestingTracker` (which uses `"."` for Python).
fn qualify(class_stack: &[String], name: &str) -> String {
    if class_stack.is_empty() {
        name.to_string()
    } else {
        format!("{}.{name}", class_stack.join("."))
    }
}

/// `class Dog(Animal, Mixin):` → one [`ImplFact`] per positional base.
/// Keyword bases (`metaclass=…`) are skipped. The base text is stored
/// as written (`abc.ABC` stays `abc.ABC`), matching how a user would
/// query `find_subtypes name=…`.
fn emit_base_classes(
    class_node: Node<'_>,
    source: &[u8],
    type_qualified: &str,
    facts: &mut SemanticFacts,
) {
    let Some(supers) = child_by_field(class_node, "superclasses") else {
        return; // `class Foo:` with no bases — no inheritance edge.
    };
    let line = line_of(class_node);
    let mut cursor = supers.walk();
    for child in supers.named_children(&mut cursor) {
        // Positional bases are identifiers / attributes / subscripts
        // (e.g. `Generic[T]`). Skip keyword args (`metaclass=…`) and
        // comments.
        let base = match child.kind() {
            "identifier" | "attribute" | "subscript" => collapse_ws(node_text(child, source)),
            _ => continue,
        };
        if base.is_empty() {
            continue;
        }
        facts.impls.push(ImplFact {
            type_qualified: type_qualified.to_string(),
            interface_qualified: Some(base),
            kind: "inherit".to_string(),
            syntactic_kind: Some(SyntacticKind::BaseArg),
            line,
        });
    }
}

/// The last dotted segment of a path (`mod.pkg.Foo` → `Foo`, `Foo` →
/// `Foo`). Used as the `target_name` for attribute callees / dotted
/// type names so `find_references symbol=Foo` matches.
fn last_segment(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

/// A call expression. `foo()` → `Call` ref on `foo`; `obj.method()` →
/// `Call` ref on `method` (the receiver type is unknown without Tier-3,
/// so `target_qualified` stays `None` — name-level only, exactly like
/// Rust's syn pass). Callees that aren't a plain name / attribute
/// (e.g. `arr[0]()`, `f()()`) are skipped.
fn emit_call(
    call_node: Node<'_>,
    source: &[u8],
    enclosing: Option<&str>,
    facts: &mut SemanticFacts,
) {
    let Some(func) = child_by_field(call_node, "function") else {
        return;
    };
    let name_node = match func.kind() {
        "identifier" => func,
        // `obj.method` — the `attribute` field holds the method name.
        "attribute" => match child_by_field(func, "attribute") {
            Some(n) => n,
            None => return,
        },
        _ => return,
    };
    let target_name = node_text(name_node, source).to_string();
    if target_name.is_empty() {
        return;
    }
    facts.refs.push(RefFact {
        target_name,
        target_qualified: None,
        kind: RefKind::Call,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: enclosing.map(str::to_string),
        byte_range: name_node.byte_range(),
        line: line_of(name_node),
    });
}

/// Parameter and return-type annotations on a function/method signature.
/// `def f(p: T, q: U) -> R` emits `Type` refs for `T` / `U` (role
/// `Param`) and `R` (role `Return`), each enclosed by the function's
/// qualified name.
fn emit_signature_annotations(
    fn_node: Node<'_>,
    source: &[u8],
    fn_qualified: &str,
    facts: &mut SemanticFacts,
) {
    if let Some(params) = child_by_field(fn_node, "parameters") {
        let mut cursor = params.walk();
        for param in params.named_children(&mut cursor) {
            // `typed_parameter` (`p: T`) and `typed_default_parameter`
            // (`p: T = default`) carry a `type` field.
            if let Some(ty) = child_by_field(param, "type") {
                emit_type_expr(ty, source, TypeRole::Param, fn_qualified, facts);
            }
        }
    }
    if let Some(ret) = child_by_field(fn_node, "return_type") {
        emit_type_expr(ret, source, TypeRole::Return, fn_qualified, facts);
    }
}

/// Emit `Type` ref(s) for a type expression. A bare name / dotted name
/// becomes one ref; a subscript (`List[int]`, `Optional[Foo]`) emits the
/// container with `role` plus each parameter as `GenericArg`. Other
/// shapes (string forward-refs, `None`, tuples) are skipped.
fn emit_type_expr(
    ty: Node<'_>,
    source: &[u8],
    role: TypeRole,
    enclosing: &str,
    facts: &mut SemanticFacts,
) {
    match ty.kind() {
        // tree-sitter-python wraps an annotation in a `type` node
        // (`p: T` → `typed_parameter ... type: (type (identifier))`);
        // unwrap to the actual expression.
        "type" => {
            if let Some(inner) = ty.named_child(0) {
                emit_type_expr(inner, source, role, enclosing, facts);
            }
        }
        "identifier" | "attribute" => {
            let text = collapse_ws(node_text(ty, source));
            if text.is_empty() {
                return;
            }
            facts.refs.push(RefFact {
                target_name: last_segment(&text).to_string(),
                target_qualified: None,
                kind: RefKind::Type,
                type_role: Some(role),
                enclosing_idx: None,
                enclosing_qualified: Some(enclosing.to_string()),
                byte_range: ty.byte_range(),
                line: line_of(ty),
            });
        }
        // `List[int]` parses as `generic_type` (container + a
        // `type_parameter` holding the bracketed args).
        "generic_type" => {
            let mut cursor = ty.walk();
            let mut first = true;
            for child in ty.named_children(&mut cursor) {
                if first {
                    // The container (`List`) takes the outer role.
                    emit_type_expr(child, source, role, enclosing, facts);
                    first = false;
                } else if child.kind() == "type_parameter" {
                    // Each bracketed arg is a generic-arg type use.
                    let mut ac = child.walk();
                    for arg in child.named_children(&mut ac) {
                        emit_type_expr(arg, source, TypeRole::GenericArg, enclosing, facts);
                    }
                }
            }
        }
        // Some grammar contexts surface bracketed types as `subscript`
        // (`value` + bracketed args). Handle defensively.
        "subscript" => {
            if let Some(value) = child_by_field(ty, "value") {
                emit_type_expr(value, source, role, enclosing, facts);
            }
            let mut cursor = ty.walk();
            for child in ty.named_children(&mut cursor) {
                if child_by_field(ty, "value").map(|v| v.id()) == Some(child.id()) {
                    continue;
                }
                emit_type_expr(child, source, TypeRole::GenericArg, enclosing, facts);
            }
        }
        _ => {}
    }
}

/// `import a`, `import a.b`, `import a.b as c`, `import a, b`.
fn emit_import_statement(node: Node<'_>, source: &[u8], facts: &mut SemanticFacts) {
    let line = line_of(node);
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "dotted_name" => {
                facts.imports.push(ImportFact {
                    to_module: node_text(child, source).to_string(),
                    imported: None,
                    alias: None,
                    is_reexport: false,
                    line,
                });
            }
            "aliased_import" => {
                // `name` field = dotted_name, `alias` field = identifier.
                let module = child_by_field(child, "name")
                    .map(|n| node_text(n, source).to_string())
                    .unwrap_or_default();
                let alias =
                    child_by_field(child, "alias").map(|n| node_text(n, source).to_string());
                if !module.is_empty() {
                    facts.imports.push(ImportFact {
                        to_module: module,
                        imported: None,
                        alias,
                        is_reexport: false,
                        line,
                    });
                }
            }
            _ => {}
        }
    }
}

/// `from m import a, b`, `from m import a as x`, `from . import c`,
/// `from m import *`. One [`ImportFact`] per imported name.
fn emit_from_import(node: Node<'_>, source: &[u8], facts: &mut SemanticFacts) {
    let line = line_of(node);
    // `module_name` field is the source module (dotted_name or
    // relative_import like `.` / `..pkg`).
    let module = child_by_field(node, "module_name")
        .map(|n| node_text(n, source).to_string())
        .unwrap_or_default();

    // Manual cursor walk so we can read each child's field name
    // (`node.children(&mut cursor)` borrows the cursor for the whole
    // iteration, blocking `cursor.field_name()`).
    let mut emitted = false;
    let mut cursor = node.walk();
    if cursor.goto_first_child() {
        loop {
            let child = cursor.node();
            let field = cursor.field_name();
            // Wildcard: `from m import *` — one fact, no specific name.
            if child.kind() == "wildcard_import" {
                facts.imports.push(ImportFact {
                    to_module: module.clone(),
                    imported: None,
                    alias: None,
                    is_reexport: false,
                    line,
                });
                emitted = true;
            } else if field == Some("name") {
                // Imported names carry the `name` field (the module
                // carries `module_name`, disambiguating the two
                // dotted_names in `from a import b`).
                match child.kind() {
                    "dotted_name" | "identifier" => {
                        facts.imports.push(ImportFact {
                            to_module: module.clone(),
                            imported: Some(node_text(child, source).to_string()),
                            alias: None,
                            is_reexport: false,
                            line,
                        });
                        emitted = true;
                    }
                    "aliased_import" => {
                        let imported =
                            child_by_field(child, "name").map(|n| node_text(n, source).to_string());
                        let alias = child_by_field(child, "alias")
                            .map(|n| node_text(n, source).to_string());
                        facts.imports.push(ImportFact {
                            to_module: module.clone(),
                            imported,
                            alias,
                            is_reexport: false,
                            line,
                        });
                        emitted = true;
                    }
                    _ => {}
                }
            }
            if !cursor.goto_next_sibling() {
                break;
            }
        }
    }
    // Defensive: a from-import we couldn't parse the names of still
    // records the module dependency rather than vanishing.
    if !emitted && !module.is_empty() {
        facts.imports.push(ImportFact {
            to_module: module,
            imported: None,
            alias: None,
            is_reexport: false,
            line,
        });
    }
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(PythonAnalyzer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        PythonAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    #[test]
    fn plain_import() {
        let f = semantic("import os\n");
        assert_eq!(f.imports.len(), 1);
        assert_eq!(f.imports[0].to_module, "os");
        assert_eq!(f.imports[0].imported, None);
        assert_eq!(f.imports[0].alias, None);
    }

    #[test]
    fn dotted_and_aliased_import() {
        let f = semantic("import os.path as p\n");
        assert_eq!(f.imports[0].to_module, "os.path");
        assert_eq!(f.imports[0].alias.as_deref(), Some("p"));
        assert_eq!(f.imports[0].imported, None);
    }

    #[test]
    fn multi_import() {
        let f = semantic("import a, b\n");
        let mods: Vec<&str> = f.imports.iter().map(|i| i.to_module.as_str()).collect();
        assert_eq!(mods, &["a", "b"]);
    }

    #[test]
    fn from_import_names() {
        let f = semantic("from pkg import alpha, beta as b\n");
        assert_eq!(f.imports.len(), 2);
        assert_eq!(f.imports[0].to_module, "pkg");
        assert_eq!(f.imports[0].imported.as_deref(), Some("alpha"));
        assert_eq!(f.imports[1].imported.as_deref(), Some("beta"));
        assert_eq!(f.imports[1].alias.as_deref(), Some("b"));
    }

    #[test]
    fn relative_and_wildcard_import() {
        let rel = semantic("from . import mod\n");
        assert_eq!(rel.imports[0].to_module, ".");
        assert_eq!(rel.imports[0].imported.as_deref(), Some("mod"));

        let star = semantic("from pkg import *\n");
        assert_eq!(star.imports[0].to_module, "pkg");
        assert_eq!(star.imports[0].imported, None);
    }

    #[test]
    fn single_inheritance_edge() {
        let f = semantic("class Dog(Animal):\n    pass\n");
        assert_eq!(f.impls.len(), 1);
        assert_eq!(f.impls[0].type_qualified, "Dog");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(f.impls[0].kind, "inherit");
    }

    #[test]
    fn multiple_bases() {
        let f = semantic("class C(A, B):\n    pass\n");
        let ifaces: Vec<&str> = f
            .impls
            .iter()
            .filter_map(|i| i.interface_qualified.as_deref())
            .collect();
        assert_eq!(ifaces, &["A", "B"]);
        assert!(f.impls.iter().all(|i| i.type_qualified == "C"));
    }

    #[test]
    fn dotted_base_kept_verbatim() {
        let f = semantic("class E(abc.ABC):\n    pass\n");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("abc.ABC"));
    }

    #[test]
    fn no_bases_no_edge() {
        let f = semantic("class Plain:\n    pass\n");
        assert!(f.impls.is_empty());
    }

    #[test]
    fn nested_class_qualifies_under_enclosing_class_only() {
        // The inner class nests under Outer; the function `make` is NOT
        // part of the qualified path (matches the syntactic pass).
        let src = "\
class Outer:
    def make(self):
        class Inner(Base):
            pass
";
        let f = semantic(src);
        let edge = f
            .impls
            .iter()
            .find(|i| i.interface_qualified.as_deref() == Some("Base"))
            .expect("Inner(Base) edge missing");
        assert_eq!(edge.type_qualified, "Outer.Inner");
    }

    #[test]
    fn keyword_base_skipped() {
        // `metaclass=Meta` is a keyword argument, not a positional base.
        let f = semantic("class M(Base, metaclass=Meta):\n    pass\n");
        let ifaces: Vec<&str> = f
            .impls
            .iter()
            .filter_map(|i| i.interface_qualified.as_deref())
            .collect();
        assert_eq!(ifaces, &["Base"]);
    }

    // ─── refs: calls ───────────────────────────────────────────────

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    #[test]
    fn module_level_call_has_no_enclosing() {
        let f = semantic("greet()\n");
        let c = calls(&f);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].target_name, "greet");
        assert_eq!(c[0].target_qualified, None);
        assert_eq!(c[0].enclosing_qualified, None);
    }

    #[test]
    fn call_inside_function_enclosed_by_function() {
        let f = semantic("def caller():\n    greet()\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "greet")
            .expect("greet call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn method_call_is_name_level_unresolved() {
        let f = semantic("def run(obj):\n    obj.render()\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "render")
            .expect("render call missing");
        // receiver type unknown → name-level only.
        assert_eq!(hit.target_qualified, None);
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("run"));
    }

    #[test]
    fn call_inside_method_enclosed_by_qualified_method() {
        let f = semantic("class W:\n    def render(self):\n        helper()\n");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W.render"));
    }

    #[test]
    fn nested_function_call_enclosed_by_inner() {
        // `inner` qualifies to just "inner" (functions aren't part of
        // the path), matching the syntactic pass; the call nests under
        // it.
        let src = "def outer():\n    def inner():\n        foo()\n";
        let f = semantic(src);
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "foo")
            .expect("foo call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("inner"));
    }

    // ─── refs: type annotations ────────────────────────────────────

    fn type_refs(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Type).collect()
    }

    #[test]
    fn param_and_return_annotations() {
        let f = semantic("def f(p: Widget) -> Report:\n    pass\n");
        let t = type_refs(&f);
        let param = t.iter().find(|r| r.target_name == "Widget").unwrap();
        assert_eq!(param.type_role, Some(TypeRole::Param));
        assert_eq!(param.enclosing_qualified.as_deref(), Some("f"));
        let ret = t.iter().find(|r| r.target_name == "Report").unwrap();
        assert_eq!(ret.type_role, Some(TypeRole::Return));
        assert_eq!(ret.enclosing_qualified.as_deref(), Some("f"));
    }

    #[test]
    fn dotted_annotation_uses_last_segment() {
        let f = semantic("def f(p: mod.Widget):\n    pass\n");
        let hit = type_refs(&f)
            .into_iter()
            .find(|r| r.target_name == "Widget")
            .expect("Widget annotation missing");
        assert_eq!(hit.type_role, Some(TypeRole::Param));
    }

    #[test]
    fn subscript_annotation_emits_container_and_generic_arg() {
        let f = semantic("def f(p: List[Widget]) -> None:\n    pass\n");
        let t = type_refs(&f);
        let container = t.iter().find(|r| r.target_name == "List").unwrap();
        assert_eq!(container.type_role, Some(TypeRole::Param));
        let arg = t.iter().find(|r| r.target_name == "Widget").unwrap();
        assert_eq!(arg.type_role, Some(TypeRole::GenericArg));
        assert_eq!(arg.enclosing_qualified.as_deref(), Some("f"));
    }

    #[test]
    fn untyped_params_emit_no_type_refs() {
        let f = semantic("def f(x, y):\n    pass\n");
        assert!(type_refs(&f).is_empty());
    }
}
