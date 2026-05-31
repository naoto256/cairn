//! Types shared between the MCP and control protocols.

use serde::{Deserialize, Serialize};

/// A line:column position inside a file. Lines are 1-based (matching what
/// editors and `file:line` strings show); columns are 0-based UTF-8 byte
/// offsets within the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub line: u32,
    pub column: u32,
}

/// A half-open range `[start, end)` inside a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

/// How a fact in the index was produced. Clients use this to judge
/// confidence: tree-sitter heuristics are usually sufficient but a
/// semantic Analyzer (LSP, syn, ruff, …) yields type-resolved results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceTier {
    /// Produced by tree-sitter or another purely syntactic pass.
    Syntactic,
    /// Produced by an Analyzer with semantic understanding.
    Semantic,
}

/// Open-ended kind tag for a defined symbol. Backends are free to add
/// their own values; the strings below are the canonical names cairn
/// reasons about. This enum is the single source of truth — both wire
/// payloads (in `mcp`/`control`) and in-memory backend facts (in
/// `cairn-lang-api`) re-use it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolKind {
    Function,
    Method,
    Constructor,
    Getter,
    Setter,
    Class,
    Struct,
    Enum,
    Union,
    Trait,
    /// `impl <Trait> for <Type> { ... }` block in Rust. Acts as a
    /// container for the methods declared inside.
    Impl,
    Interface,
    TypeAlias,
    Field,
    Property,
    Constant,
    Variable,
    Parameter,
    Module,
    Namespace,
    Package,
    Macro,
    /// Documentation section. Produced by the markdown backend for an
    /// H1-H6 heading; the heading hierarchy maps to parent/child
    /// section relationships via the standard `parent_idx` field.
    Section,
    /// `#[test]`, `def test_*`, `it(...)`, `describe(...)`. Backend-detected.
    Test,
    /// Escape hatch for backend-specific kinds not in the canonical list.
    Other(String),
}

/// How a symbol is being used at a particular reference site. Splitting
/// these out is what lets `callers(foo)` and `who-types(Foo)` answer
/// different questions on the same underlying refs table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefKind {
    Call,
    Type,
    Import,
    Instantiate,
    Read,
    Write,
    Override,
    MacroInvoke,
    Annotation,
}

/// How complete a query's answer is at the moment it was produced.
///
/// Cairn is a **shortcut**, not a last resort — when a query arrives,
/// the daemon answers with whatever it has rather than blocking until
/// the underlying indexing / analysis is finished. `Completeness` lets
/// the wire surface that fact so consumers can decide whether to use
/// the partial answer, retry later, or fall back to grep.
///
/// The variants are intentionally minimal: methods that embed this
/// type document what `Partial` means *for that specific query* —
/// "Tier-2 not yet ready", "indexing in flight", etc. The shared type
/// only commits to "complete vs. not complete".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Completeness {
    /// Everything this query asks about was indexed and analyzed.
    Complete,
    /// Some inputs were not ready when the query ran. The result is
    /// still usable — just incomplete in the dimensions listed.
    Partial {
        /// Which tiers had not finished when the result was assembled.
        /// Empty when the partiality is at a finer granularity than
        /// the tier (e.g. one file out of many timed out).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        missing_tiers: Vec<MissingTier>,
        /// Machine-readable short tag for the cause. Kept open so
        /// methods can extend without bumping the proto.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl Completeness {
    /// Shorthand for the common "everything is ready" case.
    #[must_use]
    pub fn complete() -> Self {
        Self::Complete
    }

    /// Shorthand for "the result was capped at the caller's `limit`".
    /// The partiality is at a finer granularity than the tier (no tier
    /// is missing — we just stopped collecting), so `missing_tiers` is
    /// empty and the reason carries the cap.
    #[must_use]
    pub fn partial_truncated(reason: impl Into<String>) -> Self {
        Self::Partial {
            missing_tiers: Vec::new(),
            reason: Some(reason.into()),
        }
    }

    /// Shorthand for "the semantic (Tier-2) layer was not available for
    /// at least one snapshot this query touched". Used by Tier-2
    /// methods (`find_impls` / `find_references` / `find_imports`) when
    /// they run against a snapshot indexed at the syntactic tier only
    /// (e.g. a language without a Tier-2 analyzer yet, or enrichment
    /// still pending) — as well as by `find_symbols` when
    /// `include_inherited` walks a syntactic-only snapshot.
    /// `reason` carries a human-readable detail such as which branches
    /// were syntactic-only.
    #[must_use]
    pub fn partial_semantic(reason: impl Into<String>) -> Self {
        Self::Partial {
            missing_tiers: vec![MissingTier::Semantic],
            reason: Some(reason.into()),
        }
    }
}

/// Which tier of the indexer pipeline was not finished when a query
/// ran. The shared vocabulary keeps cross-method status reporting
/// consistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingTier {
    /// Tree-sitter / structural facts not yet indexed.
    Syntactic,
    /// Tier-2 analyzer (`syn` for Rust, future per-language
    /// analyzers) had not produced its output yet.
    Semantic,
}

/// When [`RefKind::Type`] applies, this further specifies where the type
/// is used. Lets queries like "what functions return Foo" succeed without
/// post-filtering the whole refs table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeRole {
    Param,
    Return,
    Field,
    Local,
    Bound,
    GenericArg,
    Alias,
    Cast,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completeness_complete_serializes_as_tagged_status() {
        let v = serde_json::to_value(Completeness::Complete).unwrap();
        assert_eq!(v, serde_json::json!({ "status": "complete" }));
    }

    #[test]
    fn completeness_partial_carries_tiers_and_reason() {
        let p = Completeness::Partial {
            missing_tiers: vec![MissingTier::Semantic],
            reason: Some("tier2_warming".into()),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "status": "partial",
                "missing_tiers": ["semantic"],
                "reason": "tier2_warming",
            })
        );
        // Round-trip.
        let back: Completeness = serde_json::from_value(v).unwrap();
        match back {
            Completeness::Partial {
                missing_tiers,
                reason,
            } => {
                assert_eq!(missing_tiers, vec![MissingTier::Semantic]);
                assert_eq!(reason.as_deref(), Some("tier2_warming"));
            }
            Completeness::Complete => panic!("expected Partial"),
        }
    }

    #[test]
    fn completeness_partial_omits_empty_optionals() {
        let p = Completeness::Partial {
            missing_tiers: vec![],
            reason: None,
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v, serde_json::json!({ "status": "partial" }));
    }
}
