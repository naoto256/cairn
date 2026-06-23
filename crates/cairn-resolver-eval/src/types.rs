//! Shared value types for the eval harness.
//!
//! Hits are compared structurally via [`ExpectedHit`] vs [`ActualHit`].
//! Line numbers are 1-based to match `cairn-proto`'s wire convention.

use serde::{Deserialize, Serialize};

/// The MCP-shaped tool a golden case targets. Mirrors the small subset
/// of cairn query verbs the harness exercises today; new variants are
/// additive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tool {
    FindCallers,
    FindCallees,
    FindSubtypes,
    FindSupertypes,
    FindSymbols,
}

/// Query payload. Each tool reads the fields it understands and
/// ignores the rest — this keeps `GoldenCase` flat at the cost of
/// optional fields. Validated by the runner per-tool.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    /// Symbol name or qualified name (callers / callees / subtypes /
    /// supertypes use this; `find_symbols` uses it as the `query`
    /// substring).
    pub symbol: Option<String>,
    /// Optional `kind` filter for `find_symbols` (e.g. `"class"`).
    pub kind: Option<String>,
    /// Result limit; defaults to 100 when unset.
    pub limit: Option<u32>,
}

/// Expected hit. Compared against the actual resolver output by
/// `(path, line, target_qualified)`. The qualified name is normalized
/// before comparison so callers can use either fully-qualified or
/// short forms — see [`crate::report::normalize_qualified`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExpectedHit {
    pub path: String,
    pub line: u32,
    pub target_qualified: String,
}

/// Concrete hit produced by the runner from a resolver result. Carries
/// the same key as `ExpectedHit` plus an optional parser id for
/// debugging which backend surfaced a row.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActualHit {
    pub path: String,
    pub line: u32,
    pub target_qualified: String,
    pub parser_id: String,
}

/// One row in the golden table. Static so cases can live in a `const`
/// or be embedded literally without lifetime gymnastics.
#[derive(Debug, Clone)]
pub struct GoldenCase {
    pub name: &'static str,
    pub language: &'static str,
    pub tool: Tool,
    pub query: Query,
    /// Hits the Tier-2 (syntactic) resolver is expected to return.
    pub tier2_expected: Vec<ExpectedHit>,
    /// Hits the Tier-3 (LSP) resolver is expected to return. Tier-3
    /// is currently not evaluated by the in-process runner (no LSPs
    /// in CI), so this is the future-baseline shape.
    pub tier3_expected: Vec<ExpectedHit>,
}
