//! Resolution layer types (Tier-2.5 prep, Phase 1).
//!
//! These mirror the `resolutions` table introduced by schema migration
//! v6. The table is the semantic counterpart to the fact-layer tables
//! (`implementations`, `refs`, `imports`): a fact says "the grammar at
//! this site writes `extends Foo`", a resolution says "that `Foo` here
//! refers to symbol #42, as decided by `tier3-pyright-lsp`".
//!
//! Phase 1 ships definitions only. There is no reader, no writer, and
//! no query path that consults `resolutions` yet. Phase 2+ will add
//! persistence, and selected query paths (e.g. `find_subtypes`) will
//! migrate over once Tier-2.5 is producing resolutions for the
//! languages where the fact-layer `kind` is currently a heuristic
//! (Python `class Dog(Animal, Mixin)`, Swift `class Dog: Animal,
//! Protocol`, etc.).
//!
//! The `source` string must follow the `<tier-prefix>-<analyzer-id>`
//! convention enforced by
//! [`crate::workspace_analyzer::WORKSPACE_TIER_PREFIXES`], so existing
//! provenance ranking SQL keeps working once writers come online.

/// Site-class of a resolved token.
///
/// Extensible: new variants can be added without a schema change since
/// the column is `TEXT`. Keep [`ResolutionKind::as_str`] and the
/// [`std::str::FromStr`] implementation in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ResolutionKind {
    /// A type reference: `extends Foo`, `: Protocol`, `Animal` in
    /// `class Dog(Animal)`, etc.
    Type,
    /// A call site: `foo()`, `obj.method()`, JSX `<Foo />`.
    Call,
    /// An import binding: the name introduced by `from x import y`.
    Import,
}

impl ResolutionKind {
    /// String form persisted in `resolutions.kind`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Type => "type",
            Self::Call => "call",
            Self::Import => "import",
        }
    }
}

impl std::str::FromStr for ResolutionKind {
    type Err = ();

    /// Parse the stored string form. Returns `Err(())` for unknown
    /// values so callers can decide whether to ignore the row or treat
    /// it as a schema-drift error.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "type" => Ok(Self::Type),
            "call" => Ok(Self::Call),
            "import" => Ok(Self::Import),
            _ => Err(()),
        }
    }
}

/// Semantic flavour of a type-resolution edge. Only meaningful when the
/// outer [`Resolution::kind`] is [`ResolutionKind::Type`] and the edge
/// describes an inheritance / conformance relation; `None` otherwise.
///
/// This is the piece of the old `ImplFact.kind` that was a heuristic
/// in some backends. Lifting it onto a separately-sourced row lets the
/// fact layer keep recording what the grammar literally said while the
/// resolution layer records what it actually meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SemanticKind {
    /// Class extension / single-parent inheritance.
    Inherit,
    /// Interface / protocol / trait implementation.
    Implement,
    /// Ruby `include`, Python multiple-inheritance mixin, etc.
    Mixin,
    /// Swift `extension`, Kotlin extension fns, Rust inherent impl etc.
    Extension,
}

impl SemanticKind {
    /// String form persisted in `resolutions.semantic_kind`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::Implement => "implement",
            Self::Mixin => "mixin",
            Self::Extension => "extension",
        }
    }
}

impl std::str::FromStr for SemanticKind {
    type Err = ();

    /// Parse the stored string form. See [`ResolutionKind`]'s
    /// [`std::str::FromStr`] implementation for the unknown-value
    /// policy.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "inherit" => Ok(Self::Inherit),
            "implement" => Ok(Self::Implement),
            "mixin" => Ok(Self::Mixin),
            "extension" => Ok(Self::Extension),
            _ => Err(()),
        }
    }
}

/// One row of the `resolutions` table.
///
/// `target_symbol_id == None` encodes "site observed, target
/// unresolved" — useful for Tier-2.5 passes that want to record that
/// they looked without committing to a definition they couldn't pin.
///
/// Site addressing follows the CAS keying convention:
/// `(site_blob_sha, site_parser_id)` names one parse unit (a
/// composite FK to `blobs`, so rows cascade-delete with their owning
/// blob), and the byte offsets index into that blob's content,
/// matching the fact-layer span convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// `None` for unsaved rows; `Some` for rows read back from SQLite.
    pub id: Option<i64>,
    /// `blobs.blob_sha` of the blob containing the site token.
    pub site_blob_sha: String,
    /// `blobs.parser_id` paired with `site_blob_sha`.
    pub site_parser_id: String,
    /// Byte offset of the site token start, inclusive.
    pub site_byte_start: u32,
    /// Byte offset of the site token end, exclusive.
    pub site_byte_end: u32,
    /// Site class.
    pub kind: ResolutionKind,
    /// Inheritance / conformance flavour; only set for
    /// [`ResolutionKind::Type`] inheritance edges.
    pub semantic_kind: Option<SemanticKind>,
    /// FK into `symbols.id`. `None` means "unresolved".
    pub target_symbol_id: Option<i64>,
    /// Provenance string, e.g. `"tier3-pyright-lsp"`,
    /// `"tier25-py-resolver"`. Must follow the `<tier>-<analyzer>`
    /// convention recognised by
    /// [`crate::workspace_analyzer::WORKSPACE_TIER_PREFIXES`].
    pub source: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_kind_roundtrips() {
        for k in [
            ResolutionKind::Type,
            ResolutionKind::Call,
            ResolutionKind::Import,
        ] {
            assert_eq!(k.as_str().parse::<ResolutionKind>(), Ok(k));
        }
        assert_eq!("nope".parse::<ResolutionKind>(), Err(()));
    }

    #[test]
    fn semantic_kind_roundtrips() {
        for k in [
            SemanticKind::Inherit,
            SemanticKind::Implement,
            SemanticKind::Mixin,
            SemanticKind::Extension,
        ] {
            assert_eq!(k.as_str().parse::<SemanticKind>(), Ok(k));
        }
        assert_eq!("nope".parse::<SemanticKind>(), Err(()));
    }
}
