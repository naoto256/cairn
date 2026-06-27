//! String forms of the proto-defined enum tags used inside the CAS
//! store.
//!
//! The CAS schema stores enum values as TEXT columns
//! (`symbols.kind`, `symbols.visibility`, `refs.kind`, `refs.type_role`)
//! rather than INTEGERs so the on-disk view stays readable. Both the
//! insert side (`cas::blob`) and the query side (`query`,
//! `data_rpc::methods::*`) translate, so the mapping lives here once.
//!
//! Forward functions (`*_to_str`) are total. Reverse functions
//! (`*_from_str`) fall back to a sensible default for unknown strings
//! — the schema enforces what we wrote, but historical rows from a
//! prior parser revision can still appear and we want a single
//! consistent recovery rather than an `Err` every caller has to
//! decide what to do with.

use cairn_lang_api::{SymbolScope, Visibility};
use cairn_proto::common::{RefKind, SymbolKind, TypeRole};

// ─── SymbolKind ────────────────────────────────────────────────────────────

#[must_use]
/// Converts a proto symbol kind into the TEXT value stored in CAS rows.
/// `Other` keeps its payload so older or extension-produced tags can
/// round-trip without expanding this crate's enum mapping first.
pub fn symbol_kind_to_str(kind: &SymbolKind) -> String {
    match kind {
        SymbolKind::Function => "function".into(),
        SymbolKind::Method => "method".into(),
        SymbolKind::Constructor => "constructor".into(),
        SymbolKind::Getter => "getter".into(),
        SymbolKind::Setter => "setter".into(),
        SymbolKind::Class => "class".into(),
        SymbolKind::Struct => "struct".into(),
        SymbolKind::Enum => "enum".into(),
        SymbolKind::Union => "union".into(),
        SymbolKind::Trait => "trait".into(),
        SymbolKind::Impl => "impl".into(),
        SymbolKind::Interface => "interface".into(),
        SymbolKind::TypeAlias => "type_alias".into(),
        SymbolKind::Field => "field".into(),
        SymbolKind::Property => "property".into(),
        SymbolKind::Constant => "constant".into(),
        SymbolKind::Variable => "variable".into(),
        SymbolKind::Parameter => "parameter".into(),
        SymbolKind::Module => "module".into(),
        SymbolKind::Namespace => "namespace".into(),
        SymbolKind::Package => "package".into(),
        SymbolKind::Macro => "macro".into(),
        SymbolKind::Test => "test".into(),
        SymbolKind::Section => "section".into(),
        SymbolKind::Other(s) => s.clone(),
    }
}

#[must_use]
/// Rehydrates a CAS symbol-kind tag. Unknown tags become
/// [`SymbolKind::Other`] instead of failing so historical rows remain
/// queryable after parser revisions.
pub fn symbol_kind_from_str(s: &str) -> SymbolKind {
    match s {
        "function" => SymbolKind::Function,
        "method" => SymbolKind::Method,
        "constructor" => SymbolKind::Constructor,
        "getter" => SymbolKind::Getter,
        "setter" => SymbolKind::Setter,
        "class" => SymbolKind::Class,
        "struct" => SymbolKind::Struct,
        "enum" => SymbolKind::Enum,
        "union" => SymbolKind::Union,
        "trait" => SymbolKind::Trait,
        "impl" => SymbolKind::Impl,
        "interface" => SymbolKind::Interface,
        "type_alias" => SymbolKind::TypeAlias,
        "field" => SymbolKind::Field,
        "property" => SymbolKind::Property,
        "constant" => SymbolKind::Constant,
        "variable" => SymbolKind::Variable,
        "parameter" => SymbolKind::Parameter,
        "module" => SymbolKind::Module,
        "namespace" => SymbolKind::Namespace,
        "package" => SymbolKind::Package,
        "macro" => SymbolKind::Macro,
        "test" => SymbolKind::Test,
        "section" => SymbolKind::Section,
        other => SymbolKind::Other(other.to_string()),
    }
}

// ─── SymbolScope ──────────────────────────────────────────────────────────

#[must_use]
/// Converts a [`SymbolScope`] into the CAS TEXT tag stored on
/// `symbols.scope`. The default `TopLevel` corresponds to the schema
/// column default so backends that don't distinguish nested
/// declarations keep producing prior rows verbatim.
pub fn symbol_scope_to_str(scope: SymbolScope) -> &'static str {
    match scope {
        SymbolScope::TopLevel => "top_level",
        SymbolScope::Nested => "nested",
    }
}

#[must_use]
/// Rehydrates a `symbols.scope` TEXT value. Unknown tags fail closed
/// to `Nested` so that a future scope variant written by a newer
/// binary (e.g. `private`, `local`) stays hidden from
/// `find_symbols`' top-level workspace search instead of leaking
/// into it. The v12 schema's `DEFAULT 'top_level'` covers legacy
/// rows, so no live row will hit the unknown branch under normal
/// upgrade flow — this is purely a forward-compat safety belt.
pub fn symbol_scope_from_str(s: &str) -> SymbolScope {
    match s {
        "top_level" => SymbolScope::TopLevel,
        "nested" => SymbolScope::Nested,
        _ => SymbolScope::Nested,
    }
}

// ─── Visibility ───────────────────────────────────────────────────────────

#[must_use]
/// Converts visibility into the compact CAS TEXT tag used by symbol rows.
/// The mapping is intentionally stable because visibility is read by query
/// code long after the original analyzer run has finished.
pub fn visibility_to_str(v: Visibility) -> &'static str {
    match v {
        Visibility::Public => "public",
        Visibility::Crate => "crate",
        Visibility::Private => "private",
    }
}

#[must_use]
/// Rehydrates a CAS visibility tag. Unknown strings collapse to `Private`,
/// the most restrictive rendering hint; this is not a security boundary.
pub fn visibility_from_str(s: &str) -> Visibility {
    match s {
        "public" => Visibility::Public,
        "crate" => Visibility::Crate,
        _ => Visibility::Private,
    }
}

// ─── RefKind ──────────────────────────────────────────────────────────────

#[must_use]
/// Converts a proto reference kind into the CAS TEXT tag shared by
/// persistence and query filtering.
pub fn ref_kind_to_str(k: RefKind) -> &'static str {
    match k {
        RefKind::Call => "call",
        RefKind::Type => "type",
        RefKind::Import => "import",
        RefKind::Instantiate => "instantiate",
        RefKind::Read => "read",
        RefKind::Write => "write",
        RefKind::Override => "override",
        RefKind::MacroInvoke => "macro_invoke",
        RefKind::Annotation => "annotation",
    }
}

#[must_use]
/// Rehydrates a CAS reference-kind tag. Unknown strings collapse to
/// [`RefKind::Call`], the legacy default used when older callers cannot
/// distinguish a newer parser-emitted kind.
pub fn ref_kind_from_str(s: &str) -> RefKind {
    match s {
        "call" => RefKind::Call,
        "type" => RefKind::Type,
        "import" => RefKind::Import,
        "instantiate" => RefKind::Instantiate,
        "read" => RefKind::Read,
        "write" => RefKind::Write,
        "override" => RefKind::Override,
        "macro_invoke" => RefKind::MacroInvoke,
        "annotation" => RefKind::Annotation,
        _ => RefKind::Call,
    }
}

// ─── TypeRole ─────────────────────────────────────────────────────────────

#[must_use]
/// Converts a type-reference role into its CAS TEXT tag. These tags are
/// optional metadata on refs, so the forward mapping is total.
pub fn type_role_to_str(r: TypeRole) -> &'static str {
    match r {
        TypeRole::Param => "param",
        TypeRole::Return => "return",
        TypeRole::Field => "field",
        TypeRole::Local => "local",
        TypeRole::Bound => "bound",
        TypeRole::GenericArg => "generic_arg",
        TypeRole::Alias => "alias",
        TypeRole::Cast => "cast",
    }
}

#[must_use]
/// Rehydrates an optional type-reference role from CAS. Unknown tags return
/// `None` because callers can still use the underlying ref without role
/// metadata.
pub fn type_role_from_str(s: &str) -> Option<TypeRole> {
    Some(match s {
        "param" => TypeRole::Param,
        "return" => TypeRole::Return,
        "field" => TypeRole::Field,
        "local" => TypeRole::Local,
        "bound" => TypeRole::Bound,
        "generic_arg" => TypeRole::GenericArg,
        "alias" => TypeRole::Alias,
        "cast" => TypeRole::Cast,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_kind_roundtrips_known_variants() {
        for k in [
            SymbolKind::Function,
            SymbolKind::Class,
            SymbolKind::Impl,
            SymbolKind::TypeAlias,
            SymbolKind::Macro,
        ] {
            let s = symbol_kind_to_str(&k);
            assert_eq!(symbol_kind_from_str(&s), k, "{s}");
        }
    }

    #[test]
    fn symbol_kind_unknown_becomes_other() {
        match symbol_kind_from_str("future_kind") {
            SymbolKind::Other(s) => assert_eq!(s, "future_kind"),
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[test]
    fn symbol_scope_roundtrips_and_unknown_fails_closed() {
        assert_eq!(symbol_scope_from_str("top_level"), SymbolScope::TopLevel);
        assert_eq!(symbol_scope_from_str("nested"), SymbolScope::Nested);
        // Unknown future tag must fail closed to Nested so it stays
        // hidden from `find_symbols` top-level filter rather than
        // leaking into workspace search.
        assert_eq!(symbol_scope_from_str("future_scope"), SymbolScope::Nested);
        assert_eq!(symbol_scope_from_str(""), SymbolScope::Nested);
    }

    #[test]
    fn visibility_roundtrip_and_default() {
        for v in [Visibility::Public, Visibility::Crate, Visibility::Private] {
            let s = visibility_to_str(v);
            assert_eq!(visibility_from_str(s), v);
        }
        // Unknown collapses to Private.
        assert_eq!(visibility_from_str("future"), Visibility::Private);
    }

    #[test]
    fn ref_kind_roundtrips_and_falls_back_to_call() {
        for k in [
            RefKind::Call,
            RefKind::Type,
            RefKind::Import,
            RefKind::Instantiate,
            RefKind::MacroInvoke,
        ] {
            let s = ref_kind_to_str(k);
            assert_eq!(ref_kind_from_str(s), k);
        }
        assert_eq!(ref_kind_from_str("unknown"), RefKind::Call);
    }

    #[test]
    fn type_role_roundtrips_and_unknown_is_none() {
        for r in [TypeRole::Param, TypeRole::Return, TypeRole::GenericArg] {
            let s = type_role_to_str(r);
            assert_eq!(type_role_from_str(s), Some(r));
        }
        assert_eq!(type_role_from_str("unknown"), None);
    }
}
