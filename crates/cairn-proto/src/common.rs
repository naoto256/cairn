//! Types shared between the MCP and control protocols.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A line:column position inside a file. Lines are 1-based (matching what
/// editors and `file:line` strings show); columns are 0-based UTF-8 byte
/// offsets within the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    /// 1-based line number in the source file, matching `file:line`
    /// locations emitted by query hits.
    pub line: u32,
    /// 0-based UTF-8 byte offset within [`Self::line`].
    pub column: u32,
}

/// A half-open range `[start, end)` inside a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    /// Inclusive start position of the span.
    pub start: Position,
    /// Exclusive end position of the span.
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

/// Per-language enrichment status within one snapshot.
///
/// `tier` reports whether a matching analyzer run is recorded for any
/// blob in this `(snapshot, language)` slice; freshness against the
/// current analyzer revision is enforced separately on the next parse.
/// `tier=Syntactic && has_analyzer=true` therefore means Tier-2
/// capability exists but no matching analyzer run is recorded for this
/// snapshot's blob set — analyzer-ran-with-zero-facts already counts
/// as `Semantic`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanguageEnrichment {
    /// Short language tag, e.g. `"rust"`, `"python"`, `"markdown"`.
    pub language: String,
    /// Realized tier for this `(snapshot, language)` slice.
    pub tier: SourceTier,
    /// Whether the language's backend declares an analyzer at compile time.
    pub has_analyzer: bool,
}

/// Wall time the daemon spent producing a response.
///
/// `server_ms` is always present. Per-phase breakdowns are intentionally not
/// part of this v1 wire shape because the phase taxonomy is still unstable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timing {
    pub server_ms: u64,
}

/// Open-ended kind tag for a defined symbol. Backends are free to add
/// their own values; the strings below are the canonical names cairn
/// reasons about. This enum is the single source of truth — both wire
/// payloads (in `mcp`/`control`) and in-memory backend facts (in
/// `cairn-lang-api`) re-use it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolKind {
    /// Free function or top-level callable.
    Function,
    /// Method owned by a class, trait, impl, or equivalent container.
    Method,
    /// Constructor or initializer callable.
    Constructor,
    /// Property getter accessor.
    Getter,
    /// Property setter accessor.
    Setter,
    /// Class-like nominal type.
    Class,
    /// Rust `struct` or equivalent record type.
    Struct,
    /// Enum type or tagged union.
    Enum,
    /// Union type.
    Union,
    /// Trait, protocol, or interface-like behavior contract.
    Trait,
    /// `impl <Trait> for <Type> { ... }` block in Rust. Acts as a
    /// container for the methods declared inside.
    Impl,
    /// Interface type in languages that distinguish interfaces from classes.
    Interface,
    /// Type alias declaration.
    TypeAlias,
    /// Field declared on a struct, class, enum variant, or record.
    Field,
    /// Named property exposed by a type.
    Property,
    /// Compile-time or module-level constant.
    Constant,
    /// Local, module-level, or member variable.
    Variable,
    /// Function, method, or closure parameter.
    Parameter,
    /// Module declaration or file-backed module.
    Module,
    /// Namespace declaration.
    Namespace,
    /// Package-level symbol.
    Package,
    /// Macro definition.
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
    /// Function, method, constructor, or macro-like callable invocation.
    Call,
    /// Type name use, further classified by [`TypeRole`] when available.
    Type,
    /// Import or `use` edge.
    Import,
    /// Constructor or class instantiation site.
    Instantiate,
    /// Value read site.
    Read,
    /// Value write or assignment site.
    Write,
    /// Override relationship for methods or members.
    Override,
    /// Macro invocation distinct from a normal function call.
    MacroInvoke,
    /// Annotation, attribute, decorator, or type-hint metadata use.
    Annotation,
}

/// Wire state for one Tier-3 analyzer entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalyzerState {
    Ready,
    Queued,
    Running,
    Missing,
    Failed,
    Skipped,
    Stale,
    NotApplicable,
}

/// Machine-readable reason for a non-ready or intentionally skipped
/// Tier-3 analyzer entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    BinaryNotFound,
    NoMatchingFiles,
    WorkspaceUnsuitable,
    AnalyzerFailed,
    TimedOut,
    Stale,
    StaleRevision,
    NotApplicable,
    NotRecorded,
    NotScheduled,
    Unknown,
}

/// Machine-readable diagnostic emitted alongside query results.
///
/// Diagnostics describe facts about the result the daemon just produced.
/// They intentionally avoid prescriptive action so higher-level surfaces can
/// decide how to plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub severity: DiagnosticSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Stable diagnostic vocabulary for query response envelopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCode {
    AnalyzerNotRecorded,
    AnalyzerNotScheduled,
    AnalyzerFailed,
    AnalyzerStale,
    AnalyzerBinaryMissing,
    WorkspaceUnsuitable,
    QueryFailedPartial,
}

/// Severity for structured diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

/// Machine-readable next-step option emitted alongside query results.
///
/// Hints are options, not plans: callers choose whether and how to use them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hint {
    pub code: HintCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<HintAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drop_params: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// Stable hint vocabulary for query response envelopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HintCode {
    EmptyResultRelaxFilter,
    EmptyResultTryFuzzy,
    EmptyResultWidenScope,
    CappedIncreaseLimit,
    Tier3IndexingWait,
    Tier3UnavailableAlternative,
    TsxCallersUseInstantiate,
    ReindexViaCli,
}

/// Optional action category for a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HintAction {
    RelaxFilter,
    WidenScope,
    IncreaseLimit,
    WaitForIndex,
    TryAlternativeQuery,
}

/// One analyzer's readiness as exposed on query results.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Tier3AnalyzerStatus {
    /// Stable analyzer identifier, e.g. a workspace analyzer name. `None`
    /// means the language has no Tier-3 analyzer.
    pub id: Option<String>,
    /// Primary language this entry describes.
    pub language: String,
    /// Normalized wire state. Internal `succeeded` rows become `ready`.
    pub state: AnalyzerState,
    /// Machine-readable reason for non-ready states.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<ReasonCode>,
    /// Human-readable diagnostic text, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Tier3AnalyzerStatus {
    #[must_use]
    pub fn is_positive(&self) -> bool {
        matches!(
            self.state,
            AnalyzerState::Ready | AnalyzerState::Skipped | AnalyzerState::NotApplicable
        )
    }
}

/// Tier-3 workspace analyzer readiness body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tier3StatusBody {
    /// True when every Tier-3 analyzer relevant to the query reached a
    /// positive terminal state.
    pub ready: bool,
    /// Analyzer entries relevant to this view.
    pub analyzers: Vec<Tier3AnalyzerStatus>,
}

/// Tier-3 workspace analyzer readiness for the snapshots a query touched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tier3Status {
    /// Readiness for analyzers relevant to this query.
    pub this_query: Tier3StatusBody,
    /// Full repository readiness, included only when `verbose_tier3=true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_wide: Option<Tier3StatusBody>,
}

/// Tier-3 workspace analyzer readiness for a whole repository view.
///
/// Query results use `this_query`; inventory/status tools use `this_repo`
/// because their confidence is about the repository snapshot, not one query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tier3RepoStatus {
    /// Readiness for analyzers expected for this repository's current view.
    pub this_repo: Tier3StatusBody,
    /// Full repository readiness, included only when requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_wide: Option<Tier3StatusBody>,
}

impl Tier3Status {
    /// Build the default "Tier-3 was ready or not required" status.
    #[must_use]
    pub fn ready() -> Self {
        Self {
            this_query: Tier3StatusBody::ready(),
            repo_wide: None,
        }
    }

    #[must_use]
    pub fn from_body(this_query: Tier3StatusBody) -> Self {
        Self {
            this_query,
            repo_wide: None,
        }
    }

    #[must_use]
    pub fn with_repo_wide(mut self, repo_wide: Tier3StatusBody) -> Self {
        self.repo_wide = Some(repo_wide);
        self
    }
}

impl Tier3StatusBody {
    #[must_use]
    pub fn ready() -> Self {
        Self {
            ready: true,
            analyzers: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_analyzers(analyzers: Vec<Tier3AnalyzerStatus>) -> Self {
        Self {
            ready: analyzers.iter().all(Tier3AnalyzerStatus::is_positive),
            analyzers,
        }
    }
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
        /// Machine-readable short tag for the cause. Serialized as a
        /// snake_case string for wire compatibility.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<PartialReason>,
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
    pub fn partial_truncated(reason: impl Into<PartialReason>) -> Self {
        Self::Partial {
            missing_tiers: Vec::new(),
            reason: Some(reason.into()),
        }
    }

    /// Shorthand for "the semantic (Tier-2) layer was not available for
    /// at least one snapshot this query touched". Used by Tier-2
    /// methods (`find_subtypes` / `find_supertypes` / `find_references` / `find_callers` / `find_callees` / `find_imports`) when
    /// they run against a snapshot indexed at the syntactic tier only
    /// (e.g. a language without a Tier-2 analyzer yet, or enrichment
    /// still pending) — as well as by `find_symbols` when
    /// `include_inherited` walks a syntactic-only snapshot.
    /// `reason` carries the canonical machine-readable reason.
    #[must_use]
    pub fn partial_semantic(reason: impl Into<PartialReason>) -> Self {
        Self::Partial {
            missing_tiers: vec![MissingTier::Semantic],
            reason: Some(reason.into()),
        }
    }
}

/// Canonical machine-readable cause for a partial result.
///
/// The wire representation is intentionally a plain snake_case string
/// so existing clients that expect `"reason": "cap"` keep working.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartialReason {
    /// Caller `limit` was reached; raise `limit` to see more.
    Cap,
    /// Tier-2 semantic analyzer has not completed for this snapshot.
    Tier2Warming,
    /// Tier-3 workspace analyzer has not completed for this snapshot.
    Tier3Warming,
    /// Tier-3 binary or service was unavailable and the run was skipped.
    Tier3Unavailable,
    /// Analyzer execution failed for this snapshot.
    AnalyzerFailed,
    /// Forward-compatible backstop for reason strings from newer producers.
    Other(String),
}

impl PartialReason {
    /// Return the canonical snake_case wire string for this reason.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Cap => "cap",
            Self::Tier2Warming => "tier2_warming",
            Self::Tier3Warming => "tier3_warming",
            Self::Tier3Unavailable => "tier3_unavailable",
            Self::AnalyzerFailed => "analyzer_failed",
            Self::Other(value) => value.as_str(),
        }
    }
}

impl From<&str> for PartialReason {
    fn from(value: &str) -> Self {
        match value {
            "cap" => Self::Cap,
            "tier2_warming" => Self::Tier2Warming,
            "tier3_warming" => Self::Tier3Warming,
            "tier3_unavailable" => Self::Tier3Unavailable,
            "analyzer_failed" => Self::AnalyzerFailed,
            other => Self::Other(other.to_string()),
        }
    }
}

impl From<String> for PartialReason {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl Serialize for PartialReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PartialReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
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
    /// Type appears in a parameter position.
    Param,
    /// Type appears in a return position.
    Return,
    /// Type appears in a field declaration.
    Field,
    /// Type appears in a local binding or local annotation.
    Local,
    /// Type appears as a trait/type bound.
    Bound,
    /// Type appears as a generic argument.
    GenericArg,
    /// Type appears on the right-hand side of a type alias.
    Alias,
    /// Type appears in a cast or conversion annotation.
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
            reason: Some(PartialReason::Tier2Warming),
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
                assert_eq!(reason, Some(PartialReason::Tier2Warming));
            }
            Completeness::Complete => panic!("expected Partial"),
        }
    }

    #[test]
    fn partial_reason_round_trips_canonical_strings() {
        for reason in [
            PartialReason::Cap,
            PartialReason::Tier2Warming,
            PartialReason::Tier3Warming,
            PartialReason::Tier3Unavailable,
            PartialReason::AnalyzerFailed,
        ] {
            let value = serde_json::to_value(&reason).unwrap();
            assert_eq!(value, serde_json::json!(reason.as_str()));
            let back: PartialReason = serde_json::from_value(value).unwrap();
            assert_eq!(back, reason);
        }
    }

    #[test]
    fn partial_reason_preserves_unknown_strings() {
        let reason: PartialReason =
            serde_json::from_value(serde_json::json!("future_reason")).unwrap();
        assert_eq!(reason, PartialReason::Other("future_reason".into()));
        assert_eq!(
            serde_json::to_value(&reason).unwrap(),
            serde_json::json!("future_reason")
        );
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

    #[test]
    fn language_enrichment_round_trips() {
        let e = LanguageEnrichment {
            language: "rust".into(),
            tier: SourceTier::Semantic,
            has_analyzer: true,
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "language": "rust",
                "tier": "semantic",
                "has_analyzer": true,
            })
        );
        let back: LanguageEnrichment = serde_json::from_value(v).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn timing_struct_default_is_zero() {
        assert_eq!(Timing::default(), Timing { server_ms: 0 });
    }

    #[test]
    fn timing_serializes_server_ms() {
        let value = serde_json::to_value(Timing { server_ms: 42 }).unwrap();
        assert_eq!(value, serde_json::json!({ "server_ms": 42 }));
    }

    #[test]
    fn diagnostic_serializes_required_and_optional_fields() {
        let value = serde_json::to_value(Diagnostic {
            code: DiagnosticCode::AnalyzerBinaryMissing,
            severity: DiagnosticSeverity::Warning,
            message: "clangd binary not found".into(),
            language: Some("cpp".into()),
            analyzer_id: Some("clangd-cpp-lsp".into()),
            repo: Some("demo".into()),
            file: None,
            details: Some(serde_json::json!({ "reason_code": "binary_not_found" })),
        })
        .unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "code": "analyzer_binary_missing",
                "severity": "warning",
                "message": "clangd binary not found",
                "language": "cpp",
                "analyzer_id": "clangd-cpp-lsp",
                "repo": "demo",
                "details": { "reason_code": "binary_not_found" },
            })
        );
    }

    #[test]
    fn hint_serializes_required_and_optional_fields() {
        let value = serde_json::to_value(Hint {
            code: HintCode::EmptyResultTryFuzzy,
            message: "Try fuzzy search for this symbol name.".into(),
            action: Some(HintAction::TryAlternativeQuery),
            tool: Some("find_symbols".into()),
            params: Some(serde_json::json!({ "fuzzy": true })),
            drop_params: Vec::new(),
            target: None,
        })
        .unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "code": "empty_result_try_fuzzy",
                "message": "Try fuzzy search for this symbol name.",
                "action": "try_alternative_query",
                "tool": "find_symbols",
                "params": { "fuzzy": true },
            })
        );
    }

    #[test]
    fn diagnostic_code_enum_wire_is_snake_case() {
        assert_eq!(
            serde_json::to_value(DiagnosticCode::WorkspaceUnsuitable).unwrap(),
            serde_json::json!("workspace_unsuitable")
        );
    }

    #[test]
    fn hint_code_enum_wire_is_snake_case() {
        assert_eq!(
            serde_json::to_value(HintCode::Tier3UnavailableAlternative).unwrap(),
            serde_json::json!("tier3_unavailable_alternative")
        );
    }

    #[test]
    fn hint_action_enum_wire_is_snake_case() {
        assert_eq!(
            serde_json::to_value(HintAction::WaitForIndex).unwrap(),
            serde_json::json!("wait_for_index")
        );
    }

    #[test]
    fn reindex_via_cli_hint_omits_action_on_wire() {
        let value = serde_json::to_value(Hint {
            code: HintCode::ReindexViaCli,
            message: "Run `cairn ctl repo reindex demo` to refresh Tier-3 status.".into(),
            action: None,
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some("demo".into()),
        })
        .unwrap();

        assert!(value.get("action").is_none());
        assert_eq!(value["code"], serde_json::json!("reindex_via_cli"));
    }
}
