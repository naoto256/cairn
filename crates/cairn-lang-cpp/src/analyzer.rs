//! C++ Tier-2 analyzer (semantic enrichment over the same tree-sitter
//! parse the syntactic pass uses).
//!
//! Like the C# / Java analyzers, this is *structural* extraction, not
//! type resolution. The emitted facts:
//!
//! - **inheritance edges** — `class Dog : public Animal, IBark` emits
//!   one [`ImplFact`] per base, all with `kind = "inherit"`. C++ has no
//!   syntactic distinction between an interface base and a base class
//!   (every base is just a type name), so the single-kind taxonomy used
//!   by the Python / C# analyzers fits naturally. The public / protected
//!   / private access modifier on the base is not represented in the
//!   kind — it is reserved for a future metadata channel.
//! - **call refs** — `helper()`, `obj.render()`, `this->draw()`,
//!   `Foo::bar()` → [`RefKind::Call`]. Receiver types are unknown
//!   without Tier-3, so member calls stay name-level: `target_qualified`
//!   is filled in only when the bare name uniquely matches a method,
//!   constructor, or free function declared in the *same* file.
//!   A `Foo::bar()`-shaped qualified call is resolved by looking up the
//!   full path (`enclosing_namespace::Foo::bar`) first, then falling
//!   back to a bare-name match.
//! - **`new` refs** — `new Widget(...)` → [`RefKind::Instantiate`]. The
//!   target name is the leaf type (`Widget`); same-file `Widget`
//!   declarations resolve `target_qualified` to the constructor's
//!   qualified name (`Foo::Widget::Widget`).
//!
//! Same-file callee resolution applies the cross-backend "ambiguous →
//! `None`" rule: if more than one same-file declaration shares a bare
//! name (overloads), the call ref stays unresolved rather than guessing.
//!
//! Qualified names are built from the namespace + class / struct / union
//! container stack, with `::` as the separator — matching the syntactic
//! pass's `NestingTracker`, so `ImplFact.type_qualified` and
//! `RefFact.enclosing_qualified` line up with `symbols.qualified` the
//! indexer resolves against. Out-of-class definitions
//! (`void Foo::bar() { ... }`) push the explicit scope segments onto the
//! container stack while walking the function body, so calls inside the
//! body attribute to `Foo::bar`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cairn_lang_api::{Analyzer, ExtractError, ImplFact, RefFact, RefKind, SemanticFacts};
use cairn_lang_treesitter_generic::{child_by_field, collapse_ws, line_of, node_text};
use tree_sitter::{Node, Parser};

/// C++ semantic analyzer. Re-parses the source with tree-sitter-cpp
/// (the same grammar the syntactic pass uses) and walks for base-clause
/// edges plus call / instantiation refs.
pub struct CppAnalyzer;

impl Analyzer for CppAnalyzer {
    fn name(&self) -> &'static str {
        "cpp-treesitter"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let language: tree_sitter::Language = tree_sitter_cpp::LANGUAGE.into();
        let mut parser = Parser::new();
        parser
            .set_language(&language)
            .map_err(|e| ExtractError::ParserFailure(format!("set_language: {e}")))?;
        let tree = parser
            .parse(source, None)
            .ok_or_else(|| ExtractError::ParserFailure("parse returned None".into()))?;

        let mut walker = Walker {
            facts: SemanticFacts::default(),
            containers: Vec::new(),
            enclosing: None,
            local_callables: HashMap::new(),
        };
        walker.walk(tree.root_node(), source);
        walker.resolve_same_file_callees();
        Ok(walker.facts)
    }
}

/// Construct the analyzer trait object the backend hands to the daemon.
#[must_use]
pub fn analyzer() -> Arc<dyn Analyzer> {
    Arc::new(CppAnalyzer)
}

struct Walker {
    facts: SemanticFacts,
    /// Namespace + type segments enclosing the cursor, joined with `::`
    /// to build qualified names. Mirrors the Tier-1 `NestingTracker`.
    containers: Vec<String>,
    /// Qualified name of the nearest enclosing method / constructor /
    /// function / type, attached to refs as `enclosing_qualified`.
    enclosing: Option<String>,
    /// For each callable lookup key (a bare name like `helper` or a
    /// full path like `Foo::bar` or `Foo::Widget`), the set of distinct
    /// declaration-site byte starts seen in this file and the qualified
    /// that was registered there. The resolver fills `target_qualified`
    /// only when exactly one declaration site is known: more than one
    /// declaration for the same bare name (overloads, multiple
    /// `partial`-like halves, in-class + out-of-class for the same
    /// method) is treated as ambiguous and left `None`.
    local_callables: HashMap<String, CallableSlot>,
}

struct CallableSlot {
    qualified: String,
    sites: HashSet<usize>,
}

impl Walker {
    fn walk(&mut self, node: Node<'_>, source: &[u8]) {
        if node.is_error() || node.is_missing() {
            return;
        }
        match node.kind() {
            "namespace_definition" => self.walk_namespace(node, source),
            "class_specifier" | "struct_specifier" | "union_specifier" => {
                self.walk_record(node, source);
            }
            "function_definition" => self.walk_function_definition(node, source),
            // `field_declaration` covers member fields and prototype
            // methods that have a return type; a `declaration` node
            // shows up for typeless constructor / destructor prototypes
            // (`Widget(int);` / `~Widget();`) and for namespace-scope
            // free-function prototypes.
            "field_declaration" | "declaration" => self.walk_field_declaration(node, source),
            "call_expression" => {
                self.emit_call(node, source);
                self.walk_children(node, source);
            }
            "new_expression" => {
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
            format!("{}::{name}", self.containers.join("::"))
        }
    }

    fn walk_namespace(&mut self, node: Node<'_>, source: &[u8]) {
        let name_node = child_by_field(node, "name");
        let Some(name_node) = name_node else {
            // Anonymous namespace — children walk under the current
            // scope, no segments pushed.
            self.walk_children(node, source);
            return;
        };
        let segments: Vec<String> = node_text(name_node, source)
            .split("::")
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let pushed = segments.len();
        for seg in &segments {
            self.containers.push(seg.clone());
        }
        self.walk_children(node, source);
        for _ in 0..pushed {
            self.containers.pop();
        }
    }

    fn walk_record(&mut self, node: Node<'_>, source: &[u8]) {
        let name_node = child_by_field(node, "name");
        let name = name_node.map(|n| node_text(n, source).trim().to_string());

        let qualified = name.as_deref().map(|n| self.qualify(n));
        if let (Some(name), Some(qualified)) = (name.as_deref(), qualified.as_deref()) {
            self.emit_base_edges(node, source, qualified);
            let prev_enclosing = self.enclosing.replace(qualified.to_string());
            self.containers.push(name.to_string());
            self.walk_children(node, source);
            self.containers.pop();
            self.enclosing = prev_enclosing;
        } else {
            // Anonymous struct/union — walk inside under current scope.
            self.walk_children(node, source);
        }
    }

    fn walk_function_definition(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(declarator) = child_by_field(node, "declarator") else {
            self.walk_children(node, source);
            return;
        };
        let Some(info) = analyze_declarator(declarator, source) else {
            self.walk_children(node, source);
            return;
        };
        if !info.is_function {
            self.walk_children(node, source);
            return;
        }

        // Build qualified + register as a same-file callable.
        let (qualified, pushed_segments) = self.qualified_for_function(&info);
        self.record_callable(&info.name_text, &qualified, node.start_byte());

        let prev_enclosing = self.enclosing.replace(qualified);
        for seg in &pushed_segments {
            self.containers.push(seg.clone());
        }
        self.walk_children(node, source);
        for _ in 0..pushed_segments.len() {
            self.containers.pop();
        }
        self.enclosing = prev_enclosing;
    }

    fn walk_field_declaration(&mut self, node: Node<'_>, source: &[u8]) {
        // Method / function prototype: register the callable so same-
        // file calls can resolve, but recurse into the declarators in
        // case default arguments contain calls or `new` expressions.
        let mut cursor = node.walk();
        let declarators: Vec<Node<'_>> = node
            .children_by_field_name("declarator", &mut cursor)
            .collect();
        for declarator in declarators {
            let Some(info) = analyze_declarator(declarator, source) else {
                continue;
            };
            if info.is_function {
                let (qualified, _) = self.qualified_for_function(&info);
                self.record_callable(&info.name_text, &qualified, node.start_byte());
            }
        }
        self.walk_children(node, source);
    }

    fn qualified_for_function(&self, info: &DeclaratorInfo) -> (String, Vec<String>) {
        if let Some(prefix) = &info.qualified_prefix {
            // `void Foo::bar()` — combine current namespace scope with
            // the explicit prefix, then push prefix segments so calls
            // inside the body attribute to `Foo::bar`.
            let prefix_segments: Vec<String> = prefix
                .split("::")
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            let mut all = self.containers.clone();
            all.extend(prefix_segments.iter().cloned());
            all.push(info.name_text.clone());
            (all.join("::"), prefix_segments)
        } else {
            (self.qualify(&info.name_text), Vec::new())
        }
    }

    fn record_callable(&mut self, name: &str, qualified: &str, byte_start: usize) {
        // Both the bare name and the fully-qualified form get
        // registered. The bare-name slot resolves unqualified call
        // sites; the qualified slot resolves `Foo::bar()`-style call
        // sites whose `qualified_identifier` already carries the path.
        Self::insert_site(&mut self.local_callables, name, qualified, byte_start);
        if name != qualified {
            Self::insert_site(&mut self.local_callables, qualified, qualified, byte_start);
        }
    }

    fn insert_site(
        map: &mut HashMap<String, CallableSlot>,
        key: &str,
        qualified: &str,
        byte_start: usize,
    ) {
        map.entry(key.to_string())
            .and_modify(|slot| {
                slot.sites.insert(byte_start);
            })
            .or_insert_with(|| {
                let mut sites = HashSet::new();
                sites.insert(byte_start);
                CallableSlot {
                    qualified: qualified.to_string(),
                    sites,
                }
            });
    }

    fn resolve_same_file_callees(&mut self) {
        // Resolve a call or `new` ref only when its key matches exactly
        // one same-file declaration site. Multiple sites are treated as
        // ambiguous and left `None`, covering overloads (`process(int)`
        // / `process(double)`) and the rare in-class + out-of-class
        // double declaration uniformly.
        let callables = &self.local_callables;
        for r in self.facts.refs.iter_mut() {
            if !matches!(r.kind, RefKind::Call | RefKind::Instantiate) {
                continue;
            }
            let lookup = |key: &str| -> Option<String> {
                let slot = callables.get(key)?;
                if slot.sites.len() == 1 {
                    Some(slot.qualified.clone())
                } else {
                    None
                }
            };
            if let Some(existing) = r.target_qualified.as_deref() {
                // Refine a pre-filled qualified path (`Foo::bar()`) when
                // we have a unique same-file match on that exact path.
                if let Some(resolved) = lookup(existing) {
                    r.target_qualified = Some(resolved);
                }
                continue;
            }
            if let Some(resolved) = lookup(&r.target_name) {
                r.target_qualified = Some(resolved);
            }
        }
    }

    /// `class Dog : public Animal, IBark` → one edge per base type, all
    /// `kind = "inherit"`.
    fn emit_base_edges(&mut self, node: Node<'_>, source: &[u8], type_qualified: &str) {
        let Some(clause) = first_child_of_kind(node, "base_class_clause") else {
            return;
        };
        let line = line_of(clause);
        let mut cursor = clause.walk();
        for child in clause.named_children(&mut cursor) {
            // Skip access specifier tokens (anonymous "public" etc.) and
            // any non-type bookkeeping; type references show up as
            // `type_identifier`, `qualified_identifier`, or
            // `template_type`.
            if !matches!(
                child.kind(),
                "type_identifier"
                    | "qualified_identifier"
                    | "template_type"
                    | "scoped_type_identifier"
            ) {
                continue;
            }
            let base = collapse_ws(node_text(child, source));
            if base.is_empty() {
                continue;
            }
            self.facts.impls.push(ImplFact {
                type_qualified: type_qualified.to_string(),
                interface_qualified: Some(base),
                kind: "inherit".to_string(),
                line,
            });
        }
    }

    fn emit_call(&mut self, node: Node<'_>, source: &[u8]) {
        let Some(function) = child_by_field(node, "function") else {
            return;
        };
        let Some(target) = call_target(function, source) else {
            return;
        };
        self.facts.refs.push(RefFact {
            target_name: target.bare,
            target_qualified: target.qualified,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: self.enclosing.clone(),
            byte_range: function.byte_range(),
            line: line_of(function),
        });
    }

    fn emit_new(&mut self, node: Node<'_>, source: &[u8]) {
        // `new_expression` carries the type either in the `type` field
        // or as the first named type child. The type's last `::` segment
        // (after stripping any `<...>`) is the bare name.
        let ty = child_by_field(node, "type")
            .or_else(|| first_type_child(node))
            .filter(|n| !node_text(*n, source).is_empty());
        let Some(ty) = ty else {
            return;
        };
        let full = collapse_ws(node_text(ty, source));
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

struct CallTarget {
    bare: String,
    /// Pre-filled when the call site already gives a qualified path
    /// (`Foo::bar()` or `Ns::free()`); same-file resolution will still
    /// try to overwrite it with a confirmed local qualified.
    qualified: Option<String>,
}

fn call_target(function: Node<'_>, source: &[u8]) -> Option<CallTarget> {
    match function.kind() {
        "identifier" => Some(CallTarget {
            bare: node_text(function, source).to_string(),
            qualified: None,
        }),
        "field_expression" => {
            // `obj.member` / `this->member` — receiver type unknown, so
            // bare-name only.
            let name = child_by_field(function, "field")?;
            Some(CallTarget {
                bare: node_text(name, source).to_string(),
                qualified: None,
            })
        }
        "qualified_identifier" => {
            let full = collapse_ws(node_text(function, source));
            let bare = last_segment(&full).to_string();
            Some(CallTarget {
                bare,
                qualified: Some(full),
            })
        }
        "template_function" => {
            // `make<T>(...)` — name is the identifier child.
            let name = child_by_field(function, "name").or_else(|| function.named_child(0))?;
            Some(CallTarget {
                bare: node_text(name, source).to_string(),
                qualified: None,
            })
        }
        "parenthesized_expression" => call_target(function.named_child(0)?, source),
        _ => None,
    }
}

struct DeclaratorInfo {
    name_text: String,
    qualified_prefix: Option<String>,
    is_function: bool,
}

fn analyze_declarator(node: Node<'_>, source: &[u8]) -> Option<DeclaratorInfo> {
    match node.kind() {
        "identifier" | "field_identifier" | "type_identifier" | "operator_name" => {
            Some(DeclaratorInfo {
                name_text: node_text(node, source).trim().to_string(),
                qualified_prefix: None,
                is_function: false,
            })
        }
        "destructor_name" => Some(DeclaratorInfo {
            name_text: collapse_ws(node_text(node, source)),
            qualified_prefix: None,
            is_function: false,
        }),
        "qualified_identifier" => {
            let full = collapse_ws(node_text(node, source));
            let (prefix, name) = match full.rfind("::") {
                Some(pos) => (Some(full[..pos].to_string()), full[pos + 2..].to_string()),
                None => (None, full.clone()),
            };
            Some(DeclaratorInfo {
                name_text: name,
                qualified_prefix: prefix,
                is_function: false,
            })
        }
        "init_declarator"
        | "pointer_declarator"
        | "reference_declarator"
        | "array_declarator"
        | "attributed_declarator"
        | "parenthesized_declarator" => {
            let inner = child_by_field(node, "declarator").or_else(|| node.named_child(0))?;
            analyze_declarator(inner, source)
        }
        "function_declarator" => {
            let inner = child_by_field(node, "declarator")?;
            let pointer_chain = inner.kind() == "parenthesized_declarator"
                && inner.named_child(0).is_some_and(|n| {
                    matches!(n.kind(), "pointer_declarator" | "reference_declarator")
                });
            let mut info = analyze_declarator(inner, source)?;
            if !pointer_chain {
                info.is_function = true;
            }
            Some(info)
        }
        _ => None,
    }
}

fn last_segment(path: &str) -> &str {
    path.rsplit("::").next().unwrap_or(path)
}

fn strip_generic_args(name: &str) -> &str {
    name.split('<').next().unwrap_or(name).trim()
}

fn first_child_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).find(|c| c.kind() == kind)
}

fn first_type_child(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).find(|c| {
        matches!(
            c.kind(),
            "type_identifier"
                | "qualified_identifier"
                | "template_type"
                | "scoped_type_identifier"
                | "primitive_type"
                | "sized_type_specifier"
        )
    })
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(src: &str) -> SemanticFacts {
        CppAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    fn calls(f: &SemanticFacts) -> Vec<&RefFact> {
        f.refs.iter().filter(|r| r.kind == RefKind::Call).collect()
    }

    // ─── inheritance edges ─────────────────────────────────────────

    #[test]
    fn single_base_inherit_edge() {
        let f = semantic("class Dog : public Animal {};");
        assert_eq!(f.impls.len(), 1);
        assert_eq!(f.impls[0].type_qualified, "Dog");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Animal"));
        assert_eq!(f.impls[0].kind, "inherit");
    }

    #[test]
    fn multiple_bases_emit_one_edge_per_base() {
        let f = semantic("class Dog : public Animal, IBark, private IFetch {};");
        let bases: Vec<&str> = f
            .impls
            .iter()
            .filter_map(|i| i.interface_qualified.as_deref())
            .collect();
        assert!(bases.contains(&"Animal"));
        assert!(bases.contains(&"IBark"));
        assert!(bases.contains(&"IFetch"));
        assert!(f.impls.iter().all(|i| i.kind == "inherit"));
    }

    #[test]
    fn base_under_namespace_qualifies_type() {
        let f = semantic("namespace app { class W : public Base {}; }");
        assert_eq!(f.impls[0].type_qualified, "app::W");
        assert_eq!(f.impls[0].interface_qualified.as_deref(), Some("Base"));
    }

    #[test]
    fn no_base_no_edge() {
        let f = semantic("class Plain {};");
        assert!(f.impls.is_empty());
    }

    // ─── call refs ─────────────────────────────────────────────────

    #[test]
    fn cross_file_callee_stays_unresolved() {
        let f = semantic("class W { public: void render() { helper(); } };");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.target_qualified, None);
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W::render"));
    }

    #[test]
    fn same_file_free_function_call_resolves() {
        let f = semantic("int helper(int x) { return x; } int caller(int x) { return helper(x); }");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("helper"));
    }

    #[test]
    fn same_file_method_call_resolves_via_bare_name() {
        let f = semantic("class W { public: void render() { helper(); } void helper() {} };");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("W::helper"));
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W::render"));
    }

    #[test]
    fn qualified_static_call_resolves_to_full_path() {
        let f = semantic(
            "class W { public: static int stat() { return 0; } }; int caller() { return W::stat(); }",
        );
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "stat")
            .expect("stat call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("W::stat"));
    }

    #[test]
    fn this_arrow_member_call_resolves_when_unique() {
        let f = semantic("class W { public: void render() { this->draw(); } void draw() {} };");
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "draw")
            .expect("draw call missing");
        assert_eq!(hit.target_qualified.as_deref(), Some("W::draw"));
    }

    #[test]
    fn overloaded_method_leaves_call_unresolved() {
        // Two methods share the bare name `process` — cairn's
        // "ambiguous → None" rule keeps the call ref unresolved.
        let f = semantic(
            r#"
class W {
public:
    void process(int);
    void process(double);
    void run() { process(1); }
};
"#,
        );
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "process")
            .expect("process call missing");
        assert_eq!(hit.target_qualified, None);
    }

    // ─── new refs ──────────────────────────────────────────────────

    #[test]
    fn new_emits_instantiate_ref_resolves_same_file() {
        let f = semantic(
            r#"
class Widget {
public:
    Widget(int x);
};

void make() { auto* w = new Widget(1); }
"#,
        );
        let inst = f
            .refs
            .iter()
            .find(|r| r.kind == RefKind::Instantiate)
            .expect("instantiate ref missing");
        assert_eq!(inst.target_name, "Widget");
        assert_eq!(inst.target_qualified.as_deref(), Some("Widget::Widget"));
    }

    #[test]
    fn new_cross_file_callee_stays_unresolved() {
        let f = semantic("void make() { auto* w = new Widget(1); }");
        let inst = f
            .refs
            .iter()
            .find(|r| r.kind == RefKind::Instantiate)
            .expect("instantiate ref missing");
        assert_eq!(inst.target_name, "Widget");
        assert_eq!(inst.target_qualified, None);
    }

    // ─── enclosing_qualified for out-of-class methods ──────────────

    #[test]
    fn out_of_class_method_body_attributes_calls_to_method_qualified() {
        let f = semantic(
            r#"
class W { public: void render(); };

void W::render() { helper(); }
void helper() {}
"#,
        );
        let hit = calls(&f)
            .into_iter()
            .find(|r| r.target_name == "helper")
            .expect("helper call missing");
        assert_eq!(hit.enclosing_qualified.as_deref(), Some("W::render"));
        assert_eq!(hit.target_qualified.as_deref(), Some("helper"));
    }
}
