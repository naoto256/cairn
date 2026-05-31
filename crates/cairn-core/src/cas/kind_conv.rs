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

use cairn_lang_api::Visibility;
use cairn_proto::common::{RefKind, SymbolKind, TypeRole};

// ─── SymbolKind ────────────────────────────────────────────────────────────

#[must_use]
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

// ─── Visibility ───────────────────────────────────────────────────────────

#[must_use]
pub fn visibility_to_str(v: Visibility) -> &'static str {
    match v {
        Visibility::Public => "public",
        Visibility::Crate => "crate",
        Visibility::Private => "private",
    }
}

/// Unknown visibility strings collapse to `Private` (the most
/// restrictive choice — a rendering hint, not a security boundary).
#[must_use]
pub fn visibility_from_str(s: &str) -> Visibility {
    match s {
        "public" => Visibility::Public,
        "crate" => Visibility::Crate,
        _ => Visibility::Private,
    }
}

// ─── RefKind ──────────────────────────────────────────────────────────────

#[must_use]
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

/// Unknown `RefKind` strings collapse to `Call` — the most common
/// kind and a safe default if a future parser revision drops a
/// variant we no longer recognise.
#[must_use]
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
