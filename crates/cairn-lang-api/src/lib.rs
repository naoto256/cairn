//! `cairn-lang-api` — the contract a language backend implements.
//!
//! A backend is two layers:
//! - [`LanguageBackend`] (required): synchronous syntactic extraction.
//!   Tree-sitter is the common floor but the trait is parser-agnostic.
//! - [`Analyzer`] (optional): synchronous semantic enrichment over the
//!   same source buffer. Produces facts that the daemon stores
//!   alongside syntactic ones — trait/impl edges, resolved imports,
//!   richer doc strings. Backends call into language-native machinery
//!   (e.g. `syn` for Rust) without relying on an external process.
//!   Heavier external analyzers (rust-analyzer, tsserver) plug in
//!   through the separate analyzer-protocol path, not this trait.
//!
//! Backends register themselves through the [`LANGUAGE_BACKENDS`]
//! distributed slice; static linking only.

#![forbid(unsafe_code)]

use std::ops::Range;
use std::sync::Arc;

use linkme::distributed_slice;
use serde::{Deserialize, Serialize};

/// Linker-time registry of language backends. Each backend crate
/// contributes one entry; the daemon collects them at startup and
/// dispatches by file extension.
#[distributed_slice]
pub static LANGUAGE_BACKENDS: [fn() -> Box<dyn LanguageBackend>] = [..];

/// What a backend produces from a single source buffer.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SyntacticFacts {
    pub symbols: Vec<SymbolFact>,
    pub refs: Vec<RefFact>,
    pub imports: Vec<ImportFact>,
}

/// A symbol declaration. `parent_idx` is an index into `SyntacticFacts.symbols`
/// (forward reference within the same fact bundle).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFact {
    pub name: String,
    /// Best-effort qualified name assembled from the lexical nesting
    /// path. Backends may revise this when an Analyzer later resolves
    /// modules; the syntactic value is a useful starting point.
    pub qualified: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub doc: Option<String>,
    pub visibility: Option<Visibility>,
    pub byte_range: Range<usize>,
    pub line_range: Range<u32>,
    /// Byte at which the body starts (e.g. `{` for Rust, `:` for
    /// Python). Used so the outline can return signature without
    /// reading the body.
    pub body_start: Option<usize>,
    pub parent_idx: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefFact {
    /// The bare name being referenced (e.g. `foo` for `foo()` or
    /// `bar` for `obj.bar()`). For module paths the last segment is
    /// stored here; the full path goes in `target_qualified`.
    pub target_name: String,
    /// Best-effort qualified form (`module::foo`, `Trait::method`,
    /// etc.) when the analyzer can resolve more than just the bare
    /// name. The indexer uses this for `target_id` lookup against
    /// the symbols table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_qualified: Option<String>,
    pub kind: RefKind,
    pub type_role: Option<TypeRole>,
    /// Index into the same `SyntacticFacts.symbols` vector. Set by
    /// tree-sitter backends that walk both definitions and refs in
    /// one pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_idx: Option<usize>,
    /// Qualified name of the symbol that contains this ref (e.g.
    /// `module::Foo::bar` when this ref lives inside `bar`'s body).
    /// Set by analyzers that don't share an index space with the
    /// syntactic pass; the indexer resolves it against the symbols
    /// table to populate `refs.enclosing_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_qualified: Option<String>,
    pub byte_range: Range<usize>,
    pub line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportFact {
    pub to_module: String,
    pub imported: Option<String>,
    pub alias: Option<String>,
    pub is_reexport: bool,
    pub line: u32,
    /// Byte range of the *import-site token* — for Ruby this is the
    /// argument string of `require` / `require_relative` (without the
    /// surrounding quotes), so a Tier-2.5 require-graph resolver can
    /// pin its `resolutions` row at the exact site the writer already
    /// saw. Mirrors [`ImplFact::interface_byte_range`]: persistence
    /// stores `None` as NULL, and `find_imports` LEFT JOINs on the
    /// `(blob, parser_id, byte_start, byte_end)` tuple, falling back to
    /// `tier2-fact` when no resolution row covers the site.
    ///
    /// Backends that have not yet wired the range (and the syntactic
    /// pass's `load` / `autoload` paths inside the Ruby backend) leave
    /// this as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_range: Option<(u32, u32)>,
}

/// Facts an [`Analyzer`] emits on top of the syntactic floor.
///
/// Each field is independently optional — an analyzer that only knows
/// about impls leaves the others empty. The daemon merges these with
/// syntactic facts at index time:
/// - `impls` populates the `implementations` table.
/// - `imports` populates the `imports` table (the syntactic layer
///   often leaves this empty since tree-sitter struggles with import
///   path resolution).
/// - `doc_overrides` updates the `doc` column on existing symbols
///   when the analyzer found a better source (e.g. `#[doc = "..."]`
///   attributes the tree-sitter pass missed).
/// - `refs` populates the `refs` table with call sites and other
///   references the syntactic pass did not enumerate. Each ref
///   names its target by qualified-name string; the indexer
///   best-effort resolves to symbol IDs within the same snapshot.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SemanticFacts {
    pub impls: Vec<ImplFact>,
    pub imports: Vec<ImportFact>,
    pub doc_overrides: Vec<DocOverride>,
    pub refs: Vec<RefFact>,
}

/// One `impl` block, either inherent (`impl Foo {}`) or trait-bearing
/// (`impl Trait for Foo {}`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImplFact {
    /// Qualified name of the type the impl block targets.
    pub type_qualified: String,
    /// Qualified name of the trait being implemented; `None` for an
    /// inherent impl.
    pub interface_qualified: Option<String>,
    /// Short kind label: `"trait"` for a trait impl, `"inherent"`
    /// for an inherent impl. Stored verbatim in the data DB's
    /// `implementations.kind` column.
    pub kind: String,
    /// Grammar-direct classification of the edge — the fact layer's
    /// "what the source literally says" (e.g. `extends` vs `implements`
    /// vs `: Base` vs `< Base`). The resolution layer (Tier-2.5+) will
    /// read this in Phase 3 to derive a `semantic_kind` per language
    /// rules. Wired in Phase 2; `None` is allowed for backends that
    /// have not yet been migrated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub syntactic_kind: Option<SyntacticKind>,
    pub line: u32,
    /// Byte range of the *base / interface token* (e.g. the `Foo` in
    /// `extends Foo`, the `Bar` in `class X < Bar`, the `Mod` in
    /// `include Mod`). Used by the resolution layer (Phase 3) as the
    /// `site_byte_start` / `site_byte_end` of the direct-translation
    /// `resolutions` row. `None` is allowed for backends or call
    /// sites that have not been migrated yet, or where no single base
    /// token applies; persistence treats `None` as "do not emit a
    /// direct-translation resolution".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_byte_range: Option<(u32, u32)>,
}

/// Grammar-shaped classification of an inheritance / conformance /
/// mixin edge. One variant per distinct syntactic form across the
/// supported languages; the resolution layer maps it down to a smaller
/// semantic vocabulary per language.
///
/// `#[non_exhaustive]` so new grammar shapes can be added without
/// breaking downstream matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SyntacticKind {
    /// Java / TypeScript / JavaScript / PHP `extends Foo`.
    Extends,
    /// Java / TypeScript / PHP `implements Foo`.
    Implements,
    /// Kotlin / Swift / C# `: Foo` heritage list.
    Colon,
    /// Ruby `class Dog < Animal`.
    LessThan,
    /// Ruby `include Mod`.
    Include,
    /// Ruby `extend Mod`.
    ExtendKw,
    /// Ruby `prepend Mod`.
    Prepend,
    /// PHP `use TraitName;` inside a class body.
    TraitUse,
    /// Python `class Foo(Base, Mixin):` positional base argument.
    BaseArg,
    /// Rust `impl Trait for Type` (and inherent `impl Type`).
    ImplFor,
    /// Rust `trait T: U` supertrait bound. Reserved — no emitter today
    /// (supertraits are surfaced through `Bound` refs in the syntactic
    /// pass, not as `ImplFact`s).
    Supertrait,
    /// C++ `class Dog : public Animal` base with `public` access.
    PublicBase,
    /// C++ `class Dog : private Animal` base with `private` access.
    PrivateBase,
    /// C++ `class Dog : protected Animal` base with `protected` access.
    ProtectedBase,
    /// Objective-C `@interface Foo : Bar` superclass.
    InterfaceColon,
    /// Objective-C `<Protocol, ...>` conformance list. Used for both
    /// class adoption and `@protocol Foo <Bar>` protocol-to-protocol
    /// inheritance — both share the same lexical shape.
    ProtocolList,
    /// Objective-C `@interface Foo (CategoryName)` category marker.
    Category,
    /// Swift `extension Foo { ... }` declaration self-edge with no
    /// conformance clause.
    Extension,
    /// Go struct embedding (`type S struct { T }`). Reserved — no Go
    /// analyzer ships impl edges today.
    Embed,
}

/// Upgrade the doc string of an already-emitted symbol. Used when the
/// analyzer can extract richer doc than the syntactic pass — e.g.
/// `#[doc = "..."]` attribute clusters.
///
/// In Rust the same qualified name can map to multiple symbol rows
/// (a `struct Foo`, plus any `impl Foo` and `impl SomeTrait for Foo`
/// blocks). The override carries `target_kind` and the writer scopes
/// its UPDATE by `(qualified, kind)` so the struct's doc doesn't
/// bleed onto the impl rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocOverride {
    pub target_qualified: String,
    pub target_kind: SymbolKind,
    pub doc: String,
}

// ─── re-exports from cairn-proto ───────────────────────────────────────────
//
// The kind / refkind / type-role enums are domain types that travel both
// in backend-emitted facts and in MCP wire payloads. Defining them once
// in `cairn-proto` and re-exporting here keeps the single source of
// truth without forcing backends to import the proto crate by name.

pub use cairn_proto::{RefKind, SymbolKind, TypeRole};

/// Visibility is a backend-only concept (proto doesn't surface it on
/// the wire yet, so it lives here).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    Public,
    Crate,
    Private,
}

// ─── trait surface ─────────────────────────────────────────────────────────

/// What the daemon needs to know about a language.
pub trait LanguageBackend: Send + Sync {
    /// Human-readable identifier (`"rust"`, `"python"`, ...). Used as
    /// the language tag in the data DB's `files.language` column.
    fn name(&self) -> &'static str;

    /// Filename glob patterns the backend claims. The daemon's
    /// dispatcher picks the first backend whose patterns match a file.
    fn file_patterns(&self) -> &'static [&'static str];

    /// Substrings the backend wants the daemon to look for in the
    /// shebang line of an extensionless executable. Returning a
    /// non-empty list opts the backend into shebang-based detection
    /// for files like `bin/foo` whose filename gives no language
    /// hint. The default is empty (extension-only matching).
    ///
    /// Convention: include the interpreter basename without leading
    /// slashes (e.g. `"python"`, `"python3"`, `"node"`). The
    /// dispatcher's shebang matcher is substring-based against the
    /// trimmed first line, so any of these will hit shebangs like
    /// `#!/usr/bin/env python3`, `#!/usr/bin/python`, etc.
    fn shebang_patterns(&self) -> &'static [&'static str] {
        &[]
    }

    /// Best-effort version string (e.g. tree-sitter grammar revision)
    /// recorded in `files.parser` so a stale snapshot is detectable.
    fn parser_id(&self) -> &'static str;

    /// Monotonic revision for this backend's syntactic output. Bump
    /// when the same input would produce different facts.
    fn parser_revision(&self) -> u32 {
        1
    }

    /// Extract syntactic facts from one source buffer. Synchronous; the
    /// daemon offloads to a parser pool / spawn_blocking as needed.
    ///
    /// # Errors
    /// Returns [`ExtractError`] if the input cannot be parsed at all.
    fn extract_syntactic(&self, source: &[u8]) -> Result<SyntacticFacts, ExtractError>;

    /// Optional semantic Analyzer. Returns `None` when the backend has
    /// nothing beyond the syntactic floor.
    fn analyzer(&self) -> Option<Arc<dyn Analyzer>> {
        None
    }
}

/// Semantic enrichment over the same source buffer the
/// [`LanguageBackend`] just parsed. Implementations call into
/// language-native machinery (e.g. `syn` for Rust) and return facts
/// the syntactic floor cannot reach (resolved impl edges, full
/// import paths, richer doc strings).
///
/// Analyzers are synchronous; the daemon offloads to a blocking
/// task. For heavier-than-in-process work (rust-analyzer, tsserver)
/// the separate analyzer protocol path is the right mechanism, not
/// this trait.
pub trait Analyzer: Send + Sync {
    fn name(&self) -> &'static str;

    /// Monotonic revision for this analyzer's semantic output. Bump
    /// when the same input would produce different semantic facts.
    fn revision(&self) -> u32 {
        1
    }

    /// Extract semantic facts from `source`. The same buffer was
    /// already handed to [`LanguageBackend::extract_syntactic`]; the
    /// analyzer re-parses it (or uses a different parser, like
    /// `syn`) to derive facts the syntactic pass cannot resolve.
    ///
    /// # Errors
    /// Returns [`ExtractError`] when the input cannot be parsed.
    /// A successful but empty result (no impls / imports / doc
    /// overrides) is reported by returning `Ok(SemanticFacts::default())`.
    fn extract_semantic(&self, source: &[u8]) -> Result<SemanticFacts, ExtractError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("invalid utf-8 in source: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("parser failed: {0}")]
    ParserFailure(String),
}

// ─── helpers ───────────────────────────────────────────────────────────────

/// Collect every registered backend. Convenience wrapper around the
/// `LANGUAGE_BACKENDS` distributed slice.
#[must_use]
pub fn all_backends() -> Vec<Box<dyn LanguageBackend>> {
    LANGUAGE_BACKENDS.iter().map(|ctor| ctor()).collect()
}

/// Match a path against the registered backends' patterns and return
/// the first match. The matcher implements the `*.ext` shape plus
/// literal basenames (`Gemfile`, `Rakefile`) — the only two pattern
/// shapes cairn currently issues.
#[must_use]
pub fn pick_backend_for_path<'a>(
    backends: &'a [Box<dyn LanguageBackend>],
    path: &str,
) -> Option<&'a dyn LanguageBackend> {
    backends.iter().find_map(|b| {
        b.file_patterns()
            .iter()
            .any(|pat| matches_glob(pat, path))
            .then(|| b.as_ref())
    })
}

/// Match a shebang line against the registered backends'
/// `shebang_patterns`. Used as a fallback when the filename gives no
/// hint (executables in `bin/` etc.). The caller is responsible for
/// having verified that `first_line` is a real shebang (starts with
/// `#!`); the substring match here is permissive on purpose so it
/// hits both `#!/usr/bin/python3` and `#!/usr/bin/env python3`.
#[must_use]
pub fn pick_backend_for_shebang<'a>(
    backends: &'a [Box<dyn LanguageBackend>],
    first_line: &str,
) -> Option<&'a dyn LanguageBackend> {
    backends.iter().find_map(|b| {
        b.shebang_patterns()
            .iter()
            .any(|pat| first_line.contains(pat))
            .then(|| b.as_ref())
    })
}

fn matches_glob(pattern: &str, path: &str) -> bool {
    if let Some(ext) = pattern.strip_prefix("*.") {
        path.rsplit('.').next() == Some(ext)
    } else {
        // A literal pattern matches the file's basename in any
        // directory (`Gemfile` claims `sub/dir/Gemfile`). Manifest
        // paths are git-style, so `/` is the only separator.
        path.rsplit('/').next() == Some(pattern)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matches_rs() {
        assert!(matches_glob("*.rs", "src/lib.rs"));
        assert!(!matches_glob("*.rs", "src/lib.py"));
    }

    #[test]
    fn literal_pattern_matches_basename_anywhere() {
        assert!(matches_glob("Gemfile", "Gemfile"));
        assert!(matches_glob("Gemfile", "engines/auth/Gemfile"));
        assert!(!matches_glob("Gemfile", "Gemfile.lock"));
        assert!(!matches_glob("Gemfile", "engines/auth/Gemfile.lock"));
    }

    #[test]
    fn empty_registry_returns_empty_vec() {
        // No backends linked in this crate's own test binary.
        let v = all_backends();
        assert!(v.is_empty());
    }
}
