//! Semantic enrichment for Rust source files via `syn`.
//!
//! Runs after the tree-sitter pass and emits facts the syntactic layer
//! cannot resolve cleanly:
//! - **`impls`**: every `impl Trait for Type` and inherent `impl Type`
//!   block with both names normalized to their dotted form.
//! - **`imports`**: every `use foo::bar::{Baz, Qux as Q}` statement
//!   flattened to one row per imported name, with `pub use` flagged
//!   as a re-export.
//! - **`doc_overrides`**: doc strings re-assembled from `///` /
//!   `//!` line clusters *and* `#[doc = "..."]` attribute lists, so
//!   items that use only the attribute form (codegen, conditional
//!   docs) still get their text into the index.
//! - **`refs`**: every call site inside a function body. ExprCall
//!   (`foo()` / `path::foo()`) and ExprMethodCall (`obj.bar()`)
//!   are emitted with the bare target name plus, for ExprCall, the
//!   full path. The enclosing function's qualified name is attached
//!   so `find_references` can answer "who calls foo" with the
//!   calling function included.
//!
//! syn does not do name resolution — `Display` in `impl Display for
//! Foo` is recorded as the token sequence we saw, not the fully-
//! qualified `std::fmt::Display`. That's by design: in-process
//! enrichment is the right cost/value point here. Global name
//! resolution lives outside this trait — it belongs to an external
//! analyzer (rust-analyzer adapter) plugged in through the separate
//! analyzer protocol.

use cairn_lang_api::{
    Analyzer, DocOverride, ExtractError, ImplFact, ImportFact, RefFact, RefKind, SemanticFacts,
    TypeRole,
};
use syn::spanned::Spanned;
use syn::visit::Visit;

pub struct RustAnalyzer;

impl Analyzer for RustAnalyzer {
    fn name(&self) -> &'static str {
        "rust-syn"
    }

    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError> {
        let text = std::str::from_utf8(source)?;
        let file = syn::parse_file(text)
            .map_err(|e| ExtractError::ParserFailure(format!("syn parse: {e}")))?;

        let mut facts = SemanticFacts::default();
        let mut visitor = Walker {
            facts: &mut facts,
            module_path: Vec::new(),
        };
        visitor.walk_items(&file.items);
        Ok(facts)
    }
}

/// Walks the `syn` AST, accumulating semantic facts. The current
/// module path is maintained on a stack so nested `mod foo { ... }`
/// contributes to the qualified names this analyzer emits.
struct Walker<'a> {
    facts: &'a mut SemanticFacts,
    module_path: Vec<String>,
}

impl<'a> Walker<'a> {
    fn walk_items(&mut self, items: &[syn::Item]) {
        for item in items {
            self.visit_item(item);
        }
    }

    fn visit_item(&mut self, item: &syn::Item) {
        match item {
            syn::Item::Impl(it) => self.visit_impl(it),
            syn::Item::Use(it) => self.visit_use(it),
            syn::Item::Mod(it) => self.visit_mod(it),
            // Doc overrides are emitted from every item kind that
            // carries `#[doc = "..."]` attributes the tree-sitter
            // pass would miss. Containerless items (fn / struct /
            // ... at the module level) get a one-line handler each.
            // Fn additionally has its body walked for call refs.
            syn::Item::Fn(it) => {
                self.collect_doc(it.sig.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.sig.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                emit_fn_signature_type_refs(&it.sig, &enclosing, self.facts);
                self.walk_fn_body(&enclosing, &it.block);
            }
            syn::Item::Struct(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                emit_generic_param_bounds(&it.generics, &enclosing, self.facts);
                emit_fields_type_refs(&it.fields, &enclosing, self.facts);
            }
            syn::Item::Enum(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                emit_generic_param_bounds(&it.generics, &enclosing, self.facts);
                for variant in &it.variants {
                    emit_fields_type_refs(&variant.fields, &enclosing, self.facts);
                }
            }
            syn::Item::Trait(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                emit_generic_param_bounds(&it.generics, &enclosing, self.facts);
                // Super-trait bounds (`trait Foo: Bar + Baz`) read
                // as `Bound` from the trait's own perspective.
                for bound in &it.supertraits {
                    if let syn::TypeParamBound::Trait(tb) = bound {
                        let line = u32::try_from(tb.path.span().start().line).unwrap_or(0);
                        emit_type_path_ref(&tb.path, TypeRole::Bound, &enclosing, self.facts, line);
                    }
                }
                for trait_item in &it.items {
                    if let syn::TraitItem::Fn(method) = trait_item {
                        let m_encl = format!("{enclosing}::{}", method.sig.ident);
                        emit_fn_signature_type_refs(&method.sig, &m_encl, self.facts);
                    }
                }
            }
            syn::Item::Union(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                emit_generic_param_bounds(&it.generics, &enclosing, self.facts);
                for field in &it.fields.named {
                    walk_type_for_refs(&field.ty, TypeRole::Field, &enclosing, self.facts);
                }
            }
            syn::Item::Const(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                walk_type_for_refs(&it.ty, TypeRole::Alias, &enclosing, self.facts);
            }
            syn::Item::Static(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                walk_type_for_refs(&it.ty, TypeRole::Alias, &enclosing, self.facts);
            }
            syn::Item::Type(it) => {
                self.collect_doc(it.ident.to_string(), &it.attrs);
                let enclosing = self.qualify(it.ident.to_string());
                emit_attribute_refs(&it.attrs, &enclosing, self.facts);
                emit_generic_param_bounds(&it.generics, &enclosing, self.facts);
                walk_type_for_refs(&it.ty, TypeRole::Alias, &enclosing, self.facts);
            }
            syn::Item::Macro(it) => {
                if let Some(name) = it.ident.as_ref() {
                    self.collect_doc(name.to_string(), &it.attrs);
                }
                // Item-position macro invocation (`lazy_static! { ... }`,
                // `define_table! { ... }` etc.). The enclosing is the
                // current module path; an invocation outside any
                // function still gets attribution.
                let enclosing = self.module_path.last().cloned().unwrap_or_default();
                emit_macro_invoke(&it.mac, &enclosing, self.facts);
            }
            _ => {}
        }
    }

    fn visit_impl(&mut self, it: &syn::ItemImpl) {
        let type_qualified = self.qualify(type_path_string(&it.self_ty));
        emit_attribute_refs(&it.attrs, &type_qualified, self.facts);
        let line = u32::try_from(it.span().start().line).unwrap_or(0);
        let (interface_qualified, kind) = match &it.trait_ {
            Some((_, path, _)) => (Some(path_to_string(path)), "trait".to_string()),
            None => (None, "inherent".to_string()),
        };
        self.facts.impls.push(ImplFact {
            type_qualified: type_qualified.clone(),
            interface_qualified,
            kind,
            line,
        });
        // Walk each method's body for call refs. Methods qualify
        // under the type they impl on (matching how the syntactic
        // pass builds `qualified` for `impl Foo { fn bar() }` →
        // `Foo::bar`). This means `find_references` against
        // `Foo::bar` resolves to the same symbol the outline emits.
        // Method signatures also contribute Type refs (param /
        // return / generic bound), tagged with the method's
        // qualified name as `enclosing`.
        emit_generic_param_bounds(&it.generics, &type_qualified, self.facts);
        for item in &it.items {
            match item {
                syn::ImplItem::Fn(method) => {
                    let enclosing = format!("{type_qualified}::{}", method.sig.ident);
                    emit_attribute_refs(&method.attrs, &enclosing, self.facts);
                    emit_fn_signature_type_refs(&method.sig, &enclosing, self.facts);
                    self.walk_fn_body(&enclosing, &method.block);
                }
                syn::ImplItem::Type(at) => {
                    let enclosing = format!("{type_qualified}::{}", at.ident);
                    walk_type_for_refs(&at.ty, TypeRole::Alias, &enclosing, self.facts);
                }
                syn::ImplItem::Const(c) => {
                    let enclosing = format!("{type_qualified}::{}", c.ident);
                    walk_type_for_refs(&c.ty, TypeRole::Alias, &enclosing, self.facts);
                }
                _ => {}
            }
        }
    }

    /// Walk a function body collecting call-site refs. Uses syn's
    /// `Visit` trait so nested expressions are reached automatically
    /// (closures, match arms, if-let bodies, etc.).
    fn walk_fn_body(&mut self, enclosing_qualified: &str, block: &syn::Block) {
        let mut visitor = BodyVisitor {
            facts: self.facts,
            enclosing_qualified,
        };
        visitor.visit_block(block);
    }

    fn visit_use(&mut self, it: &syn::ItemUse) {
        let line = u32::try_from(it.span().start().line).unwrap_or(0);
        let is_reexport = matches!(it.vis, syn::Visibility::Public(_));
        flatten_use_tree(&it.tree, &mut Vec::new(), is_reexport, line, self.facts);
    }

    fn visit_mod(&mut self, it: &syn::ItemMod) {
        self.collect_doc(it.ident.to_string(), &it.attrs);
        if let Some((_, items)) = &it.content {
            self.module_path.push(it.ident.to_string());
            for item in items {
                self.visit_item(item);
            }
            self.module_path.pop();
        }
    }

    /// Pull `#[doc = "..."]` attribute clusters off an item and emit
    /// a [`DocOverride`] when at least one was present. The
    /// tree-sitter pass already handles `///` / `//!` clusters; this
    /// path is what catches codegen-emitted docs (`include_str!`,
    /// macro expansions, cfg-gated docs).
    fn collect_doc(&mut self, name: String, attrs: &[syn::Attribute]) {
        let mut lines: Vec<String> = Vec::new();
        for attr in attrs {
            if !attr.path().is_ident("doc") {
                continue;
            }
            if let syn::Meta::NameValue(nv) = &attr.meta
                && let syn::Expr::Lit(lit) = &nv.value
                && let syn::Lit::Str(s) = &lit.lit
            {
                let value = s.value();
                // Trim a single leading space (rustdoc convention),
                // keep the rest verbatim so example bodies don't get
                // mangled.
                let trimmed = value.strip_prefix(' ').unwrap_or(&value).to_string();
                lines.push(trimmed);
            }
        }
        if lines.is_empty() {
            return;
        }
        let doc = lines.join("\n");
        let target_qualified = self.qualify(name);
        self.facts.doc_overrides.push(DocOverride {
            target_qualified,
            doc,
        });
    }

    fn qualify(&self, name: String) -> String {
        if self.module_path.is_empty() {
            name
        } else {
            let mut parts = self.module_path.clone();
            parts.push(name);
            parts.join("::")
        }
    }
}

/// Walks a function body collecting call-site references. One
/// instance per enclosing function so the `enclosing_qualified`
/// attribution stays correct across nested closures.
struct BodyVisitor<'a> {
    facts: &'a mut SemanticFacts,
    enclosing_qualified: &'a str,
}

impl<'ast> Visit<'ast> for BodyVisitor<'_> {
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        // `foo()` / `path::foo()` — receiver-less call.
        if let syn::Expr::Path(p) = node.func.as_ref() {
            let qualified = path_to_string(&p.path);
            let target_name = p
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap_or_else(|| qualified.clone());
            let line = u32::try_from(node.span().start().line).unwrap_or(0);
            self.facts.refs.push(RefFact {
                target_name,
                target_qualified: Some(qualified),
                kind: RefKind::Call,
                type_role: None,
                enclosing_idx: None,
                enclosing_qualified: Some(self.enclosing_qualified.to_string()),
                byte_range: 0..0,
                line,
            });
        }
        // Recurse so calls inside the arguments are reached too.
        syn::visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        // `obj.bar()` — receiver-type unknown without resolution, so
        // we only record the method name. `target_qualified` is
        // left None to make it obvious that the resolution stopped
        // short of a full path. Same-name false positives (a
        // common method like `.new()`) are the cost; future
        // rust-analyzer integration would tighten this.
        let line = u32::try_from(node.span().start().line).unwrap_or(0);
        self.facts.refs.push(RefFact {
            target_name: node.method.to_string(),
            target_qualified: None,
            kind: RefKind::Call,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: Some(self.enclosing_qualified.to_string()),
            byte_range: 0..0,
            line,
        });
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        // `let x: Foo = ...` — the type ascription is a Local
        // type ref. Without `: Foo` (the common case for inferred
        // bindings) `pat.ty` is absent and we emit nothing.
        if let syn::Pat::Type(pt) = &node.pat {
            walk_type_for_refs(
                &pt.ty,
                TypeRole::Local,
                self.enclosing_qualified,
                self.facts,
            );
        }
        syn::visit::visit_local(self, node);
    }

    fn visit_expr_macro(&mut self, node: &'ast syn::ExprMacro) {
        // `println!(...)`, `vec![...]`, `format!(...)` — expression-
        // position bang macros. The macro path identifies the
        // invocation target.
        emit_macro_invoke(&node.mac, self.enclosing_qualified, self.facts);
        syn::visit::visit_expr_macro(self, node);
    }

    fn visit_stmt_macro(&mut self, node: &'ast syn::StmtMacro) {
        // Statement-position bang macros (`assert!(...);` etc.).
        emit_macro_invoke(&node.mac, self.enclosing_qualified, self.facts);
        syn::visit::visit_stmt_macro(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        // `Foo { x: 1 }` / `MyEnum::Variant { x }` / `pkg::Foo { .. }`.
        // The path identifies the struct or enum-variant being
        // constructed; emit an Instantiate ref so consumers can
        // ask "where is Foo built?".
        let qualified = path_to_string(&node.path);
        let target_name = node
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_else(|| qualified.clone());
        let line = u32::try_from(node.span().start().line).unwrap_or(0);
        self.facts.refs.push(RefFact {
            target_name,
            target_qualified: Some(qualified),
            kind: RefKind::Instantiate,
            type_role: None,
            enclosing_idx: None,
            enclosing_qualified: Some(self.enclosing_qualified.to_string()),
            byte_range: 0..0,
            line,
        });
        syn::visit::visit_expr_struct(self, node);
    }

    fn visit_expr_cast(&mut self, node: &'ast syn::ExprCast) {
        // `expr as Foo` — destination type is a Cast ref.
        walk_type_for_refs(
            &node.ty,
            TypeRole::Cast,
            self.enclosing_qualified,
            self.facts,
        );
        syn::visit::visit_expr_cast(self, node);
    }
}

fn path_to_string(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

/// Emit a `RefKind::Type` row for one `syn::Path` appearing in a
/// type position. Recurses into angle-bracketed generic arguments
/// so `Vec<HashMap<K, V>>` produces refs for `Vec`, `HashMap`,
/// `K`, and `V` — the outer one carrying `role`, the nested ones
/// tagged `GenericArg` so callers can distinguish "uses Foo
/// directly" from "uses Bar<Foo>".
fn emit_type_path_ref(
    path: &syn::Path,
    role: TypeRole,
    enclosing: &str,
    facts: &mut SemanticFacts,
    line: u32,
) {
    let qualified = path_to_string(path);
    let target_name = path
        .segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_else(|| qualified.clone());
    facts.refs.push(RefFact {
        target_name,
        target_qualified: Some(qualified),
        kind: RefKind::Type,
        type_role: Some(role),
        enclosing_idx: None,
        enclosing_qualified: Some(enclosing.to_string()),
        byte_range: 0..0,
        line,
    });
    // Recurse into generic arguments anywhere along the path
    // (`a::b::Foo<Bar>` puts `Bar` inside the last segment, but
    // associated paths can carry args mid-path too).
    for segment in &path.segments {
        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
            for arg in &args.args {
                if let syn::GenericArgument::Type(inner_ty) = arg {
                    walk_type_for_refs(inner_ty, TypeRole::GenericArg, enclosing, facts);
                }
            }
        }
    }
}

/// Walk a `syn::Type`, unwrapping the structural wrappers
/// (`&T` / `(T, U)` / `[T]` / `[T; N]` / `*const T` / `(T)` /
/// `impl Trait` / `dyn Trait` / `fn(T) -> U`) and calling
/// [`emit_type_path_ref`] for every named-type leaf. Anything that
/// has no path leaf (`!`, `_`, macro types) is silently ignored —
/// they can't be the target of a ref anyway.
fn walk_type_for_refs(ty: &syn::Type, role: TypeRole, enclosing: &str, facts: &mut SemanticFacts) {
    let line = u32::try_from(ty.span().start().line).unwrap_or(0);
    match ty {
        syn::Type::Path(tp) => emit_type_path_ref(&tp.path, role, enclosing, facts, line),
        syn::Type::Reference(r) => walk_type_for_refs(&r.elem, role, enclosing, facts),
        syn::Type::Tuple(t) => {
            for elem in &t.elems {
                walk_type_for_refs(elem, role, enclosing, facts);
            }
        }
        syn::Type::Array(a) => walk_type_for_refs(&a.elem, role, enclosing, facts),
        syn::Type::Slice(s) => walk_type_for_refs(&s.elem, role, enclosing, facts),
        syn::Type::Ptr(p) => walk_type_for_refs(&p.elem, role, enclosing, facts),
        syn::Type::Paren(p) => walk_type_for_refs(&p.elem, role, enclosing, facts),
        syn::Type::Group(g) => walk_type_for_refs(&g.elem, role, enclosing, facts),
        // `impl Trait` / `dyn Trait` are themselves trait-bound
        // mentions; record each trait as `Bound` regardless of the
        // outer role (the bound binds the position, not the role).
        syn::Type::TraitObject(t) => {
            for bound in &t.bounds {
                if let syn::TypeParamBound::Trait(tb) = bound {
                    emit_type_path_ref(&tb.path, TypeRole::Bound, enclosing, facts, line);
                }
            }
        }
        syn::Type::ImplTrait(it) => {
            for bound in &it.bounds {
                if let syn::TypeParamBound::Trait(tb) = bound {
                    emit_type_path_ref(&tb.path, TypeRole::Bound, enclosing, facts, line);
                }
            }
        }
        syn::Type::BareFn(bf) => {
            for arg in &bf.inputs {
                walk_type_for_refs(&arg.ty, TypeRole::Param, enclosing, facts);
            }
            if let syn::ReturnType::Type(_, t) = &bf.output {
                walk_type_for_refs(t, TypeRole::Return, enclosing, facts);
            }
        }
        // Never, Infer, Macro, Verbatim — nothing to resolve.
        _ => {}
    }
}

/// Walk an item's attribute list, emitting `RefKind::Annotation`
/// for every named attribute and every path inside a `#[derive(...)]`
/// list. `#[doc = "..."]` is skipped — those round-trip through
/// `collect_doc` instead, and treating doc as an annotation would
/// spam the refs table.
fn emit_attribute_refs(attrs: &[syn::Attribute], enclosing: &str, facts: &mut SemanticFacts) {
    for attr in attrs {
        let path = attr.path();
        let last = path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default();
        if last == "doc" {
            continue;
        }
        if last == "derive" {
            // `#[derive(Foo, bar::Baz)]` — emit one Annotation per
            // derived trait path. Parse failures (esoteric macros
            // that fake derive syntax, etc.) are swallowed; the
            // attribute itself is already recorded as a fall-through
            // annotation below to avoid losing it entirely.
            let parsed = attr.parse_args_with(
                syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
            );
            match parsed {
                Ok(paths) => {
                    for p in &paths {
                        emit_annotation_path(p, enclosing, facts);
                    }
                    continue;
                }
                Err(_) => {
                    // Fall through to emitting the bare `derive`
                    // attribute so the row isn't completely lost.
                }
            }
        }
        emit_annotation_path(path, enclosing, facts);
    }
}

/// Emit a `RefKind::MacroInvoke` row for a `syn::Macro` (`name!(…)`
/// or `name![…]` or `name!{…}`). The macro's path goes into both
/// `target_name` (last segment) and `target_qualified` (joined
/// path), mirroring the call-site convention so consumers can use
/// the same field names across kinds.
fn emit_macro_invoke(mac: &syn::Macro, enclosing: &str, facts: &mut SemanticFacts) {
    let qualified = path_to_string(&mac.path);
    let target_name = mac
        .path
        .segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_else(|| qualified.clone());
    let line = u32::try_from(mac.span().start().line).unwrap_or(0);
    facts.refs.push(RefFact {
        target_name,
        target_qualified: Some(qualified),
        kind: RefKind::MacroInvoke,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: Some(enclosing.to_string()),
        byte_range: 0..0,
        line,
    });
}

fn emit_annotation_path(path: &syn::Path, enclosing: &str, facts: &mut SemanticFacts) {
    let qualified = path_to_string(path);
    let target_name = path
        .segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_else(|| qualified.clone());
    let line = u32::try_from(path.span().start().line).unwrap_or(0);
    facts.refs.push(RefFact {
        target_name,
        target_qualified: Some(qualified),
        kind: RefKind::Annotation,
        type_role: None,
        enclosing_idx: None,
        enclosing_qualified: Some(enclosing.to_string()),
        byte_range: 0..0,
        line,
    });
}

/// Emit `Field` type refs for every named or positional field on
/// a struct / enum variant / union. Lets `find_references` answer
/// "what structs have a `Foo` field?" without re-parsing each
/// definition.
fn emit_fields_type_refs(fields: &syn::Fields, enclosing: &str, facts: &mut SemanticFacts) {
    match fields {
        syn::Fields::Named(named) => {
            for f in &named.named {
                walk_type_for_refs(&f.ty, TypeRole::Field, enclosing, facts);
            }
        }
        syn::Fields::Unnamed(unnamed) => {
            for f in &unnamed.unnamed {
                walk_type_for_refs(&f.ty, TypeRole::Field, enclosing, facts);
            }
        }
        syn::Fields::Unit => {}
    }
}

/// Pull every named-type reference out of a function signature,
/// tagged with the right [`TypeRole`]: arguments are `Param`,
/// returns are `Return`, trait bounds on generic parameters and in
/// `where` clauses are `Bound`. The receiver (`self` / `&self`) is
/// skipped because the impl block's `type_qualified` already
/// records what type the method belongs to.
fn emit_fn_signature_type_refs(sig: &syn::Signature, enclosing: &str, facts: &mut SemanticFacts) {
    for input in &sig.inputs {
        if let syn::FnArg::Typed(pt) = input {
            walk_type_for_refs(&pt.ty, TypeRole::Param, enclosing, facts);
        }
    }
    if let syn::ReturnType::Type(_, ret) = &sig.output {
        walk_type_for_refs(ret, TypeRole::Return, enclosing, facts);
    }
    emit_generic_param_bounds(&sig.generics, enclosing, facts);
}

/// Generic bounds on `<T: Foo + Bar>` and in `where` clauses are
/// `Bound` refs. Lifetime bounds carry no type identity, so they
/// drop out naturally — we only emit for `TypeParamBound::Trait`.
fn emit_generic_param_bounds(generics: &syn::Generics, enclosing: &str, facts: &mut SemanticFacts) {
    for param in &generics.params {
        if let syn::GenericParam::Type(tp) = param {
            for bound in &tp.bounds {
                if let syn::TypeParamBound::Trait(tb) = bound {
                    let line = u32::try_from(tb.path.span().start().line).unwrap_or(0);
                    emit_type_path_ref(&tb.path, TypeRole::Bound, enclosing, facts, line);
                }
            }
        }
    }
    if let Some(where_clause) = &generics.where_clause {
        for pred in &where_clause.predicates {
            if let syn::WherePredicate::Type(pt) = pred {
                // LHS of `Foo: Bar + Baz` — the type being
                // constrained. Tagged `Bound` too (no separate
                // "Constrained" role in the wire vocabulary) so a
                // `kind=type type_role=bound` query surfaces both
                // sides of the where-predicate.
                walk_type_for_refs(&pt.bounded_ty, TypeRole::Bound, enclosing, facts);
                for bound in &pt.bounds {
                    if let syn::TypeParamBound::Trait(tb) = bound {
                        let line = u32::try_from(tb.path.span().start().line).unwrap_or(0);
                        emit_type_path_ref(&tb.path, TypeRole::Bound, enclosing, facts, line);
                    }
                }
            }
        }
    }
}

/// Best-effort textual rendering of a type position. For named types
/// we return the path (`std::fmt::Display`); for everything else
/// (refs / tuples / impl-trait / fn pointers) we serialize the type
/// via `quote!` and collapse whitespace. The result is what shows up
/// in the `implementations.type_id`-resolution lookup, so a stable
/// textual form is enough — name resolution is left to the
/// external analyzer path (rust-analyzer adapter), not handled
/// here.
fn type_path_string(ty: &syn::Type) -> String {
    if let syn::Type::Path(p) = ty {
        return path_to_string(&p.path);
    }
    // Fallback: re-emit the tokens. This handles `&Foo<T>`,
    // `(Foo, Bar)`, etc. with reasonable readability.
    let tokens = quote_type(ty);
    collapse_whitespace(&tokens)
}

fn quote_type(ty: &syn::Type) -> String {
    // syn doesn't expose ToTokens without `quote`; we reach into the
    // source-text-equivalent form via `Spanned + ToTokens` semantics
    // by formatting through `quote::ToTokens` — but to avoid pulling
    // `quote` we use a tiny fallback.
    format!("{}", TypeDisplay(ty))
}

struct TypeDisplay<'a>(&'a syn::Type);

impl<'a> std::fmt::Display for TypeDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // syn's Type implements Debug; not ideal but stable enough
        // for the fallback (only reached for non-path types in the
        // self_ty of an impl, which is uncommon).
        write!(f, "{:?}", self.0)
    }
}

fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Flatten a `use foo::bar::{Baz, Qux as Q};` tree into one row per
/// imported name. `prefix` is the dotted path accumulated so far.
fn flatten_use_tree(
    tree: &syn::UseTree,
    prefix: &mut Vec<String>,
    is_reexport: bool,
    line: u32,
    facts: &mut SemanticFacts,
) {
    match tree {
        syn::UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            flatten_use_tree(&p.tree, prefix, is_reexport, line, facts);
            prefix.pop();
        }
        syn::UseTree::Name(n) => {
            let to_module = prefix.join("::");
            let imported = Some(n.ident.to_string());
            facts.imports.push(ImportFact {
                to_module,
                imported,
                alias: None,
                is_reexport,
                line,
            });
        }
        syn::UseTree::Rename(r) => {
            let to_module = prefix.join("::");
            facts.imports.push(ImportFact {
                to_module,
                imported: Some(r.ident.to_string()),
                alias: Some(r.rename.to_string()),
                is_reexport,
                line,
            });
        }
        syn::UseTree::Glob(_) => {
            let to_module = prefix.join("::");
            facts.imports.push(ImportFact {
                to_module,
                imported: Some("*".to_string()),
                alias: None,
                is_reexport,
                line,
            });
        }
        syn::UseTree::Group(g) => {
            for inner in &g.items {
                flatten_use_tree(inner, prefix, is_reexport, line, facts);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(src: &str) -> SemanticFacts {
        RustAnalyzer.extract_semantic(src.as_bytes()).unwrap()
    }

    #[test]
    fn inherent_impl_emits_one_fact() {
        let f = run("struct Foo; impl Foo { fn bar(&self) {} }");
        assert_eq!(f.impls.len(), 1);
        let i = &f.impls[0];
        assert_eq!(i.type_qualified, "Foo");
        assert!(i.interface_qualified.is_none());
        assert_eq!(i.kind, "inherent");
    }

    #[test]
    fn trait_impl_records_both_sides() {
        let f = run(
            "struct Foo; impl std::fmt::Display for Foo { fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{Ok(())} }",
        );
        assert_eq!(f.impls.len(), 1);
        let i = &f.impls[0];
        assert_eq!(i.type_qualified, "Foo");
        assert_eq!(i.interface_qualified.as_deref(), Some("std::fmt::Display"));
        assert_eq!(i.kind, "trait");
    }

    #[test]
    fn use_simple_name() {
        let f = run("use std::fmt::Display;");
        assert_eq!(f.imports.len(), 1);
        let im = &f.imports[0];
        assert_eq!(im.to_module, "std::fmt");
        assert_eq!(im.imported.as_deref(), Some("Display"));
        assert_eq!(im.alias, None);
        assert!(!im.is_reexport);
    }

    #[test]
    fn use_with_rename_and_group_and_glob() {
        let f = run("use std::fmt::{Display, Result as FmtRes, *};");
        // 3 entries: Display, Result-as-FmtRes, *.
        assert_eq!(f.imports.len(), 3);
        let names: Vec<_> = f
            .imports
            .iter()
            .map(|i| (i.imported.as_deref(), i.alias.as_deref()))
            .collect();
        assert!(names.contains(&(Some("Display"), None)));
        assert!(names.contains(&(Some("Result"), Some("FmtRes"))));
        assert!(names.contains(&(Some("*"), None)));
    }

    #[test]
    fn pub_use_marked_as_reexport() {
        let f = run("pub use foo::Bar;");
        assert!(f.imports[0].is_reexport);
    }

    #[test]
    fn doc_attribute_emits_override() {
        let src = "#[doc = \" hello\"]\n#[doc = \" world\"]\nfn f() {}";
        let f = run(src);
        assert_eq!(f.doc_overrides.len(), 1);
        let d = &f.doc_overrides[0];
        assert_eq!(d.target_qualified, "f");
        assert_eq!(d.doc, "hello\nworld");
    }

    #[test]
    fn nested_mod_qualifies_impl_target_and_doc() {
        let src = r#"
            mod outer {
                #[doc = " inner"]
                pub struct Foo;
                impl Foo { fn bar(&self) {} }
            }
        "#;
        let f = run(src);
        // The doc override is qualified through the module path.
        assert!(
            f.doc_overrides
                .iter()
                .any(|d| d.target_qualified == "outer::Foo" && d.doc == "inner")
        );
        // Impl block's type name is also qualified.
        assert!(
            f.impls
                .iter()
                .any(|i| i.type_qualified == "outer::Foo" && i.kind == "inherent")
        );
    }

    #[test]
    fn call_refs_capture_function_and_method_calls() {
        let src = r#"
            fn caller() {
                foo();
                bar::baz(1, 2);
                let x = std::collections::HashMap::new();
                x.insert("k", "v");
            }
            struct Foo;
            impl Foo {
                fn entry(&self) {
                    helper();
                    self.private();
                }
                fn private(&self) {}
            }
        "#;
        let f = run(src);
        let names: Vec<&str> = f.refs.iter().map(|r| r.target_name.as_str()).collect();
        // Free-fn calls inside `caller`.
        assert!(names.contains(&"foo"));
        assert!(names.contains(&"baz"));
        // Path-call resolves to the last segment as `target_name`
        // with the full path on `target_qualified`.
        let baz = f.refs.iter().find(|r| r.target_name == "baz").unwrap();
        assert_eq!(baz.target_qualified.as_deref(), Some("bar::baz"));
        // Method calls.
        assert!(names.contains(&"new")); // HashMap::new() is recorded as a path call
        assert!(names.contains(&"insert"));
        assert!(names.contains(&"private"));
        // Enclosing qualification.
        let helper_ref = f.refs.iter().find(|r| r.target_name == "helper").unwrap();
        assert_eq!(
            helper_ref.enclosing_qualified.as_deref(),
            Some("Foo::entry")
        );
        let foo_ref = f.refs.iter().find(|r| r.target_name == "foo").unwrap();
        assert_eq!(foo_ref.enclosing_qualified.as_deref(), Some("caller"));
    }

    #[test]
    fn fn_signature_types_are_emitted_with_roles() {
        let src = r#"
            use std::fmt::Display;
            fn render(items: Vec<Display>, n: usize) -> String where Display: Sized {
                String::new()
            }
        "#;
        let f = run(src);
        let type_refs: Vec<&RefFact> = f.refs.iter().filter(|r| r.kind == RefKind::Type).collect();
        // Param refs: Vec (outer), Display (generic arg), usize.
        let names: Vec<&str> = type_refs.iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"Vec"), "missing Vec in {names:?}");
        assert!(names.contains(&"usize"), "missing usize in {names:?}");
        assert!(names.contains(&"Display"), "missing Display in {names:?}");
        assert!(
            names.contains(&"String"),
            "missing String return in {names:?}"
        );

        // Roles match position.
        let vec_ref = type_refs.iter().find(|r| r.target_name == "Vec").unwrap();
        assert_eq!(vec_ref.type_role, Some(TypeRole::Param));
        let string_ref = type_refs
            .iter()
            .find(|r| r.target_name == "String")
            .unwrap();
        assert_eq!(string_ref.type_role, Some(TypeRole::Return));
        // Display appears twice: once as a generic arg (inside Vec)
        // and once as a where-clause bound.
        let display_roles: Vec<_> = type_refs
            .iter()
            .filter(|r| r.target_name == "Display")
            .filter_map(|r| r.type_role)
            .collect();
        assert!(display_roles.contains(&TypeRole::GenericArg));
        assert!(display_roles.contains(&TypeRole::Bound));

        // Enclosing is the fn name.
        assert!(
            type_refs
                .iter()
                .all(|r| r.enclosing_qualified.as_deref() == Some("render"))
        );
    }

    #[test]
    fn struct_field_types_emitted_with_field_role() {
        let src = "struct Foo { name: String, items: Vec<Bar> }";
        let f = run(src);
        let by_name: std::collections::HashMap<&str, &RefFact> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::Type)
            .map(|r| (r.target_name.as_str(), r))
            .collect();
        assert_eq!(
            by_name.get("String").and_then(|r| r.type_role),
            Some(TypeRole::Field)
        );
        assert_eq!(
            by_name.get("Vec").and_then(|r| r.type_role),
            Some(TypeRole::Field)
        );
        assert_eq!(
            by_name.get("Bar").and_then(|r| r.type_role),
            Some(TypeRole::GenericArg)
        );
        assert!(
            by_name
                .values()
                .all(|r| r.enclosing_qualified.as_deref() == Some("Foo"))
        );
    }

    #[test]
    fn impl_method_signature_types_attribute_to_method() {
        let src = r#"
            struct Foo;
            impl Foo {
                fn handle(&self, req: Request) -> Response { Response }
            }
        "#;
        let f = run(src);
        let req = f
            .refs
            .iter()
            .find(|r| r.target_name == "Request")
            .expect("Request type ref missing");
        assert_eq!(req.kind, RefKind::Type);
        assert_eq!(req.type_role, Some(TypeRole::Param));
        assert_eq!(req.enclosing_qualified.as_deref(), Some("Foo::handle"));
        let resp = f
            .refs
            .iter()
            .find(|r| r.target_name == "Response" && r.type_role == Some(TypeRole::Return))
            .expect("Response return-type ref missing");
        assert_eq!(resp.enclosing_qualified.as_deref(), Some("Foo::handle"));
    }

    #[test]
    fn body_level_type_refs_local_and_cast() {
        let src = r#"
            fn body() {
                let x: Foo = make();
                let n = 42 as u64;
            }
        "#;
        let f = run(src);
        let foo = f
            .refs
            .iter()
            .find(|r| r.target_name == "Foo")
            .expect("Foo local-binding type ref missing");
        assert_eq!(foo.kind, RefKind::Type);
        assert_eq!(foo.type_role, Some(TypeRole::Local));
        assert_eq!(foo.enclosing_qualified.as_deref(), Some("body"));

        let u64ref = f
            .refs
            .iter()
            .find(|r| r.target_name == "u64")
            .expect("u64 cast-destination ref missing");
        assert_eq!(u64ref.kind, RefKind::Type);
        assert_eq!(u64ref.type_role, Some(TypeRole::Cast));
    }

    #[test]
    fn struct_literal_emits_instantiate_ref() {
        let src = r#"
            struct Foo { x: i32 }
            enum Color { Rgb { r: u8, g: u8, b: u8 } }
            fn build() {
                let _ = Foo { x: 1 };
                let _ = Color::Rgb { r: 0, g: 0, b: 0 };
            }
        "#;
        let f = run(src);
        let inst: Vec<&RefFact> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::Instantiate)
            .collect();
        assert!(inst.iter().any(|r| r.target_name == "Foo"));
        let rgb = inst
            .iter()
            .find(|r| r.target_name == "Rgb")
            .expect("Rgb variant instantiate missing");
        assert_eq!(rgb.target_qualified.as_deref(), Some("Color::Rgb"));
        assert!(
            inst.iter()
                .all(|r| r.enclosing_qualified.as_deref() == Some("build")),
            "instantiates should attribute to enclosing fn"
        );
    }

    #[test]
    fn derive_and_attribute_macros_emit_annotation_refs() {
        let src = r#"
            #[derive(Debug, Clone, serde::Deserialize)]
            struct Foo { x: i32 }

            #[tokio::main]
            async fn main() {}
        "#;
        let f = run(src);
        let anno: Vec<&RefFact> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::Annotation)
            .collect();
        let names: Vec<&str> = anno.iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"Debug"));
        assert!(names.contains(&"Clone"));
        assert!(names.contains(&"Deserialize"));
        assert!(names.contains(&"main"));
        let deser = anno
            .iter()
            .find(|r| r.target_name == "Deserialize")
            .unwrap();
        assert_eq!(
            deser.target_qualified.as_deref(),
            Some("serde::Deserialize")
        );
        assert_eq!(deser.enclosing_qualified.as_deref(), Some("Foo"));
        let tokio_main = anno
            .iter()
            .find(|r| {
                r.target_name == "main" && r.target_qualified.as_deref() == Some("tokio::main")
            })
            .expect("tokio::main attribute missing");
        assert_eq!(tokio_main.enclosing_qualified.as_deref(), Some("main"));
    }

    #[test]
    fn doc_attribute_does_not_appear_as_annotation_ref() {
        let src = r#"
            #[doc = " greet"]
            fn hello() {}
        "#;
        let f = run(src);
        assert!(
            f.refs
                .iter()
                .filter(|r| r.kind == RefKind::Annotation)
                .all(|r| r.target_name != "doc"),
            "doc should not appear as Annotation"
        );
    }

    #[test]
    fn bang_macros_emit_macro_invoke_refs() {
        let src = r#"
            fn run() {
                println!("hi");
                let v = vec![1, 2, 3];
                tracing::info!("ok");
                assert_eq!(1, 1);
            }
        "#;
        let f = run(src);
        let mi: Vec<&RefFact> = f
            .refs
            .iter()
            .filter(|r| r.kind == RefKind::MacroInvoke)
            .collect();
        let names: Vec<&str> = mi.iter().map(|r| r.target_name.as_str()).collect();
        assert!(names.contains(&"println"));
        assert!(names.contains(&"vec"));
        assert!(names.contains(&"info"));
        assert!(names.contains(&"assert_eq"));
        let tracing_info = mi
            .iter()
            .find(|r| r.target_qualified.as_deref() == Some("tracing::info"))
            .expect("tracing::info qualified missing");
        assert_eq!(tracing_info.enclosing_qualified.as_deref(), Some("run"));
    }

    #[test]
    fn generic_impl_method_refs_use_stripped_enclosing() {
        // Companion to lib.rs's
        // `generic_impl_self_type_is_stripped_in_qualified`. The
        // syn analyzer emits `enclosing_qualified` for body-level
        // refs; that string is the lookup key the indexer joins
        // against the symbols table. Pre-fix the analyzer already
        // produced "Walker::method" (path_to_string strips args),
        // but the tree-sitter pass stored "Walker<'a>::method" in
        // symbols, so the join failed silently. We assert the
        // analyzer side stays stripped — combined with the
        // tree-sitter test, both halves now agree.
        let src = r#"
            impl<'a> Walker<'a> {
                fn visit_item(&mut self) {
                    foo();
                }
            }
        "#;
        let f = run(src);
        let call = f
            .refs
            .iter()
            .find(|r| r.target_name == "foo")
            .expect("foo() call ref missing");
        assert_eq!(
            call.enclosing_qualified.as_deref(),
            Some("Walker::visit_item"),
            "syn analyzer must emit generic-stripped enclosing so it matches the tree-sitter symbols.qualified key"
        );
    }

    #[test]
    fn no_facts_for_empty_file() {
        let f = run("");
        assert!(f.impls.is_empty());
        assert!(f.imports.is_empty());
        assert!(f.doc_overrides.is_empty());
    }
}
