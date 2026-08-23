//! Diagnostics and actionable hints derived from query outcomes.

use cairn_proto::{
    AnalyzerState, Completeness, Diagnostic, DiagnosticCode, DiagnosticSeverity, Hint, HintAction,
    HintCode, PartialReason, ReasonCode, TierAnalyzerStatus, TierStatus,
};
#[cfg(test)]
use cairn_proto::{TierStatusBody, default_tier};
use serde_json::json;

use super::query::QueryFreshnessIssue;

/// Every input the diagnostic/hint builders need without carrying the
/// raw parameter struct of each tool. Callers assemble this once and
/// hand it to [`build_diagnostics`] / [`build_hints`] /
/// [`build_snapshot_aware_feedback`].
#[derive(Clone, Copy)]
pub(crate) struct EmissionContext<'a> {
    pub(crate) tool: QueryToolKind,
    pub(crate) items_empty: bool,
    pub(crate) completeness: &'a Completeness,
    pub(crate) tier3_status: &'a TierStatus,
    pub(crate) query_args: QueryArgsView<'a>,
}

/// Which query tool produced the result. Selects the copy and the
/// relax-drop candidate list in [`tool_metadata`], and gates a couple
/// of tool-specific hints (e.g. `GetOutline` for directory outlines).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryToolKind {
    FindSymbols,
    GetOutline,
    GetSymbolSource,
    FindReferences,
    FindCallers,
    FindCallees,
    FindSubtypes,
    FindSupertypes,
    FindImports,
}

/// The subset of caller arguments the hint builders inspect: whether a
/// given filter was set, and the string value of the identifier-like
/// filters (repo/container/path/file). Boolean fields flag "the caller
/// explicitly narrowed by this parameter"; the value itself is not
/// needed to advise dropping it.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QueryArgsView<'a> {
    pub(crate) repo: Option<&'a str>,
    pub(crate) fuzzy: bool,
    pub(crate) kind: bool,
    pub(crate) container: Option<&'a str>,
    pub(crate) path: Option<&'a str>,
    pub(crate) file: Option<&'a str>,
    pub(crate) max_depth: bool,
    pub(crate) direction: bool,
}

impl QueryArgsView<'_> {
    /// Which of the tool's declared relax-drop candidates the caller
    /// actually set. Only these can be suggested for removal — dropping
    /// a filter that was never set would be noise.
    fn filter_drop_params(&self, metadata: &ToolHintMetadata) -> Vec<String> {
        let mut params = Vec::new();
        for candidate in metadata.relax_drop_candidates {
            let set = match *candidate {
                "repo" => self.repo.is_some_and(|value| !value.is_empty()),
                "kind" => self.kind,
                "container" => self.container.is_some_and(|value| !value.is_empty()),
                "path" => self.path.is_some_and(|value| !value.is_empty()),
                "file" => self.file.is_some_and(|value| !value.is_empty()),
                "max_depth" => self.max_depth,
                "direction" => self.direction,
                "fuzzy" => self.fuzzy,
                _ => false,
            };
            if set {
                params.push((*candidate).to_string());
            }
        }
        params
    }

    fn has_relax_filters(&self, metadata: &ToolHintMetadata) -> bool {
        !self.filter_drop_params(metadata).is_empty()
    }

    /// `get_outline` with `path` set and `file` unset — a directory
    /// walk that gets a dedicated cap hint because narrowing by
    /// `max_depth` or `kind` is usually more helpful than raising
    /// `limit`.
    fn is_directory_outline(&self) -> bool {
        self.path.is_some_and(|value| !value.is_empty()) && self.file.is_none()
    }
}

/// Static copy per tool: the wire name, the noun to use when narrating
/// counts, and the ordered list of filters worth suggesting for
/// removal when the result is empty.
struct ToolHintMetadata {
    tool: &'static str,
    result_noun: &'static str,
    relax_drop_candidates: &'static [&'static str],
}

/// Per-tool hint copy and relax-drop candidate ordering. Kept in one
/// place so the wire strings stay consistent with the JSON-RPC method
/// names advertised by each [`DataMethod`].
fn tool_metadata(kind: QueryToolKind) -> ToolHintMetadata {
    match kind {
        QueryToolKind::FindSymbols => ToolHintMetadata {
            tool: "find_symbols",
            result_noun: "symbols",
            relax_drop_candidates: &["kind", "container", "path"],
        },
        QueryToolKind::GetOutline => ToolHintMetadata {
            tool: "get_outline",
            result_noun: "outline items",
            relax_drop_candidates: &["kind", "max_depth", "path", "file"],
        },
        QueryToolKind::GetSymbolSource => ToolHintMetadata {
            tool: "get_symbol_source",
            result_noun: "source results",
            relax_drop_candidates: &["file"],
        },
        QueryToolKind::FindReferences => ToolHintMetadata {
            tool: "find_references",
            result_noun: "references",
            relax_drop_candidates: &["kind", "direction"],
        },
        QueryToolKind::FindCallers => ToolHintMetadata {
            tool: "find_callers",
            result_noun: "callers",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindCallees => ToolHintMetadata {
            tool: "find_callees",
            result_noun: "callees",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindSubtypes => ToolHintMetadata {
            tool: "find_subtypes",
            result_noun: "subtypes",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindSupertypes => ToolHintMetadata {
            tool: "find_supertypes",
            result_noun: "supertypes",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindImports => ToolHintMetadata {
            tool: "find_imports",
            result_noun: "imports",
            relax_drop_candidates: &["file"],
        },
    }
}

/// Diagnostics that describe *why* a response is incomplete or degraded.
///
/// Partial completeness with a `Cap` reason is deliberately not emitted
/// as an error diagnostic — it is a normal outcome surfaced as a hint
/// instead. Every other partial reason becomes a `QueryFailedPartial`
/// error, and each analyzer status the caller's rows depend on is
/// mapped through [`diagnostic_for_analyzer`].
pub(crate) fn build_diagnostics(ctx: &EmissionContext<'_>) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if let Completeness::Partial { reason, .. } = ctx.completeness
        && !matches!(reason, Some(PartialReason::Cap))
    {
        diagnostics.push(Diagnostic {
            code: DiagnosticCode::QueryFailedPartial,
            severity: DiagnosticSeverity::Error,
            message: "query returned partial results".into(),
            language: None,
            analyzer_id: None,
            repo: ctx.query_args.repo.map(str::to_string),
            file: None,
            details: reason
                .as_ref()
                .map(|reason| json!({ "reason": reason.as_str() })),
        });
    }

    diagnostics.extend(
        ctx.tier3_status
            .this_query
            .analyzers
            .iter()
            .filter_map(diagnostic_for_analyzer),
    );
    diagnostics
}

/// Actionable next-step hints for the calling agent. Emits, in order:
/// cap-relief advice when truncation happened, an indexing-in-progress
/// notice while tier-3 analyzers are queued or running, empty-result
/// advice (relax filters, try fuzzy, widen repo scope) that the
/// snapshot-aware layer may later suppress, and a per-repo reindex
/// nudge when a tier-3 run was expected but not recorded.
pub(crate) fn build_hints(ctx: &EmissionContext<'_>) -> Vec<Hint> {
    let mut hints = Vec::new();
    let analyzers = &ctx.tier3_status.this_query.analyzers;
    let metadata = tool_metadata(ctx.tool);

    if matches!(
        ctx.completeness,
        Completeness::Partial {
            reason: Some(PartialReason::Cap),
            ..
        }
    ) {
        if ctx.tool == QueryToolKind::GetOutline && ctx.query_args.is_directory_outline() {
            hints.push(Hint {
                code: HintCode::CappedNarrowFilter,
                message:
                    "Outline result was capped. Try narrowing with kind=... or reducing max_depth."
                        .into(),
                action: None,
                tool: Some(metadata.tool.into()),
                params: Some(json!({ "narrow_candidates": ["kind", "max_depth"] })),
                drop_params: Vec::new(),
                target: None,
            });
        }
        hints.push(Hint {
            code: HintCode::CappedIncreaseLimit,
            message: format!("Increase `limit` to see more {}.", metadata.result_noun),
            action: Some(HintAction::IncreaseLimit),
            tool: Some(metadata.tool.into()),
            params: None,
            drop_params: Vec::new(),
            target: None,
        });
    }

    if analyzers.iter().any(|analyzer| {
        matches!(
            analyzer.state,
            AnalyzerState::Queued | AnalyzerState::Running
        )
    }) {
        hints.push(Hint {
            code: HintCode::Tier3IndexingWait,
            message: "Tier-3 indexing is still running for this query.".into(),
            action: Some(HintAction::WaitForIndex),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some("tier3".into()),
        });
    }

    if ctx.items_empty {
        let drop_params = ctx.query_args.filter_drop_params(&metadata);
        if ctx.query_args.has_relax_filters(&metadata) {
            let joined = drop_params.join(", ");
            hints.push(Hint {
                code: HintCode::EmptyResultRelaxFilter,
                message: format!(
                    "No {} matched. Try dropping {joined}.",
                    metadata.result_noun
                ),
                action: Some(HintAction::RelaxFilter),
                tool: Some(metadata.tool.into()),
                params: None,
                drop_params,
                target: None,
            });
        } else if !ctx.query_args.fuzzy {
            hints.push(Hint {
                code: HintCode::EmptyResultTryFuzzy,
                message: format!(
                    "No {} matched. Try fuzzy=true or a prefix wildcard.",
                    metadata.result_noun
                ),
                action: Some(HintAction::TryAlternativeQuery),
                tool: Some(metadata.tool.into()),
                params: Some(json!({ "fuzzy": true })),
                drop_params: Vec::new(),
                target: None,
            });
        }

        if ctx.query_args.repo.is_some() {
            hints.push(Hint {
                code: HintCode::EmptyResultWidenScope,
                message: format!(
                    "No {} matched. Try widening repo scope.",
                    metadata.result_noun
                ),
                action: Some(HintAction::WidenScope),
                tool: Some(metadata.tool.into()),
                params: None,
                drop_params: vec!["repo".into()],
                target: None,
            });
        }
    }

    if analyzers.iter().any(|analyzer| {
        analyzer.state == AnalyzerState::Missing
            && analyzer.reason_code == Some(ReasonCode::NotScheduled)
    }) {
        hints.push(Hint {
            code: HintCode::Tier3UnavailableAlternative,
            message: "Tier-3 data is unavailable for this query; use syntactic results or try a broader query.".into(),
            action: Some(HintAction::TryAlternativeQuery),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some("tier3".into()),
        });
    }

    if analyzers.iter().any(|analyzer| {
        analyzer.state == AnalyzerState::Missing
            && analyzer.reason_code == Some(ReasonCode::NotRecorded)
    }) && let Some(repo) = ctx.query_args.repo
    {
        hints.push(Hint {
            code: HintCode::ReindexViaCli,
            message: format!("Run `cairn ctl repo reindex {repo}` to refresh Tier-3 status."),
            action: None,
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some(repo.to_string()),
        });
    }

    hints
}

/// Build query feedback while giving snapshot uncertainty priority over
/// speculative empty-result advice. Cap and analyzer signals remain visible.
///
/// When `freshness_issues` is empty this returns the default builders
/// unchanged. Otherwise it re-shapes the output so a caller does not
/// see conflicting advice — e.g. "your snapshot is stale" alongside
/// "try dropping this filter, it might match more rows":
///
/// * The generic `QueryFailedPartial` diagnostic is replaced by one
///   `FileNotIndexedOrSnapshotStale` per issue, tagged with the repo
///   alias (omitted for the `*` aggregate).
/// * Empty-result hints (relax/fuzzy/widen) are dropped because rows
///   truly missing from an untrusted snapshot say nothing about the
///   real filter shape.
/// * Cap-relief hints are re-derived from a synthesised cap context
///   and appended after the surviving indexing/reconcile hints (the
///   freshness advisory is then inserted at the very front), so the
///   final order is freshness advisory -> indexing/reconcile hints
///   -> cap-relief, and the caller still learns the result was
///   truncated.
pub(crate) fn build_snapshot_aware_feedback(
    ctx: &EmissionContext<'_>,
    freshness_issues: &[QueryFreshnessIssue],
    capped: bool,
) -> (Vec<Diagnostic>, Vec<Hint>) {
    let mut diagnostics = build_diagnostics(ctx);
    let mut hints = build_hints(ctx);
    append_missing_cap_hints(ctx, capped, &mut hints);
    if freshness_issues.is_empty() {
        return (diagnostics, hints);
    }

    // The freshness advisory is the precise story; drop the generic
    // "partial" diagnostic that would otherwise repeat it in weaker form.
    diagnostics.retain(|diagnostic| diagnostic.code != DiagnosticCode::QueryFailedPartial);
    diagnostics.extend(freshness_issues.iter().map(|issue| Diagnostic {
        code: DiagnosticCode::FileNotIndexedOrSnapshotStale,
        severity: DiagnosticSeverity::Warning,
        message: "The query used a file-missing or freshness-unverified current snapshot.".into(),
        language: None,
        analyzer_id: None,
        repo: (issue.repo != "*").then(|| issue.repo.clone()),
        file: ctx.query_args.file.map(str::to_string),
        details: Some(json!({ "reason": issue.reason })),
    }));
    // Empty-result advice assumes the row set reflects the query.
    // Under an untrusted snapshot it may just mean "we could not see
    // the rows"; suppress the whole family.
    hints.retain(|hint| {
        !matches!(
            hint.code,
            HintCode::EmptyResultRelaxFilter
                | HintCode::EmptyResultTryFuzzy
                | HintCode::EmptyResultWidenScope
        )
    });
    // Lead with the freshness advisory so agents see the "wait or
    // reindex" step before anything else.
    hints.insert(
        0,
        Hint {
            code: HintCode::FileNotIndexedOrSnapshotStale,
            message: "Wait for reconciliation or run `cairn ctl repo reindex <alias>` before trusting an empty file query.".into(),
            action: Some(HintAction::WaitForIndex),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: ctx
                .query_args
                .repo
                .or(ctx.query_args.file)
                .map(str::to_string),
        },
    );
    hints.dedup_by_key(|hint| hint.code);
    (diagnostics, hints)
}

/// Preserve the independent truncation signal when another partial reason
/// owns the response's single completeness-reason slot.
fn append_missing_cap_hints(ctx: &EmissionContext<'_>, capped: bool, hints: &mut Vec<Hint>) {
    if !capped {
        return;
    }

    let cap = Completeness::partial_truncated(PartialReason::Cap);
    let cap_ctx = EmissionContext {
        completeness: &cap,
        ..*ctx
    };
    for hint in build_hints(&cap_ctx).into_iter().filter(|hint| {
        matches!(
            hint.code,
            HintCode::CappedIncreaseLimit | HintCode::CappedNarrowFilter
        )
    }) {
        if !hints.iter().any(|existing| existing.code == hint.code) {
            hints.push(hint);
        }
    }
}

/// Map one tier analyzer status onto a wire diagnostic, or `None` when
/// the analyzer is in a healthy state (`Ready`) or expresses no
/// problem the query should surface (`NotApplicable`, `Queued`,
/// `Running` are handled by hints instead). Falls through to a canned
/// message when the analyzer omitted its own free-form reason.
fn diagnostic_for_analyzer(analyzer: &TierAnalyzerStatus) -> Option<Diagnostic> {
    let (code, severity, fallback_message) = match (analyzer.state, analyzer.reason_code) {
        (AnalyzerState::Missing, Some(ReasonCode::NotRecorded)) => (
            DiagnosticCode::AnalyzerNotRecorded,
            DiagnosticSeverity::Warning,
            "analyzer run was not recorded",
        ),
        (AnalyzerState::Missing, Some(ReasonCode::NotScheduled)) => (
            DiagnosticCode::AnalyzerNotScheduled,
            DiagnosticSeverity::Warning,
            "expected analyzer was not scheduled",
        ),
        (AnalyzerState::Missing, Some(ReasonCode::AnalyzerFailed)) | (AnalyzerState::Failed, _) => {
            (
                DiagnosticCode::AnalyzerFailed,
                DiagnosticSeverity::Warning,
                "analyzer failed",
            )
        }
        (AnalyzerState::Missing, Some(ReasonCode::BinaryNotFound)) => (
            DiagnosticCode::AnalyzerBinaryMissing,
            DiagnosticSeverity::Warning,
            "analyzer binary is missing",
        ),
        (AnalyzerState::Stale, Some(ReasonCode::Stale | ReasonCode::StaleRevision))
        | (AnalyzerState::Stale, _) => (
            DiagnosticCode::AnalyzerStale,
            DiagnosticSeverity::Info,
            "analyzer result is stale",
        ),
        (AnalyzerState::Skipped, Some(ReasonCode::WorkspaceUnsuitable)) => (
            DiagnosticCode::WorkspaceUnsuitable,
            DiagnosticSeverity::Info,
            "workspace is unsuitable for this analyzer",
        ),
        _ => return None,
    };

    Some(Diagnostic {
        code,
        severity,
        message: analyzer
            .reason
            .clone()
            .unwrap_or_else(|| fallback_message.to_string()),
        language: Some(analyzer.language.clone()),
        analyzer_id: analyzer.id.clone(),
        repo: None,
        file: None,
        details: analyzer
            .reason_code
            .map(|reason_code| json!({ "reason_code": reason_code })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_hints_omits_all_when_happy_path() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Ready,
            reason_code: None,
            reason: None,
        }]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        assert!(build_hints(&ctx).is_empty());
        assert!(build_diagnostics(&ctx).is_empty());
    }

    #[test]
    fn build_hints_emits_relax_filter_when_filters_applied() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                kind: true,
                path: Some("src/"),
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultRelaxFilter);
        assert_eq!(hints[0].action, Some(HintAction::RelaxFilter));
        assert_eq!(hints[0].drop_params, vec!["kind", "path"]);
        assert_eq!(hints[0].tool.as_deref(), Some("find_symbols"));
        assert!(hints[0].message.contains("symbols"));
    }

    #[test]
    fn find_symbols_hint_uses_symbols_noun() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].tool.as_deref(), Some("find_symbols"));
        assert_eq!(hints[0].message, "Increase `limit` to see more symbols.");
    }

    #[test]
    fn get_outline_hint_uses_outline_items_noun_and_outline_tool() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                kind: true,
                path: Some("src/"),
                max_depth: true,
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        let increase = hints
            .iter()
            .find(|hint| hint.code == HintCode::CappedIncreaseLimit)
            .unwrap();
        assert_eq!(increase.tool.as_deref(), Some("get_outline"));
        assert!(increase.message.contains("outline items"));
        let relax = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultRelaxFilter)
            .unwrap();
        assert_eq!(relax.tool.as_deref(), Some("get_outline"));
        assert_eq!(relax.drop_params, vec!["kind", "max_depth", "path"]);
        assert!(relax.message.contains("outline items"));
    }

    #[test]
    fn find_imports_hint_uses_imports_noun_and_imports_tool() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindImports,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                file: Some("src/lib.rs"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultRelaxFilter);
        assert_eq!(hints[0].tool.as_deref(), Some("find_imports"));
        assert_eq!(hints[0].drop_params, vec!["file"]);
        assert!(hints[0].message.contains("imports"));
    }

    #[test]
    fn find_references_hint_can_drop_direction_filter() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                direction: true,
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultRelaxFilter);
        assert_eq!(hints[0].tool.as_deref(), Some("find_references"));
        assert_eq!(hints[0].drop_params, vec!["direction"]);
        assert!(hints[0].message.contains("references"));
    }

    #[test]
    fn relax_filter_drop_params_only_includes_actually_set_args() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindSymbols,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: Some("demo"),
                path: Some("src/"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        let relax = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultRelaxFilter)
            .unwrap();
        assert_eq!(relax.drop_params, vec!["path"]);
    }

    #[test]
    fn relax_filter_does_not_include_repo_in_drop_params() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: Some("demo"),
                kind: true,
                direction: true,
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        let relax = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultRelaxFilter)
            .unwrap();
        assert_eq!(relax.drop_params, vec!["kind", "direction"]);
        let widen = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultWidenScope)
            .unwrap();
        assert_eq!(widen.drop_params, vec!["repo"]);
    }

    #[test]
    fn build_hints_emits_try_fuzzy_when_exact_no_filter() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                fuzzy: false,
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultTryFuzzy);
        assert_eq!(hints[0].action, Some(HintAction::TryAlternativeQuery));
        assert_eq!(hints[0].params, Some(serde_json::json!({ "fuzzy": true })));
    }

    #[test]
    fn build_hints_emits_widen_scope_when_repo_specified() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                repo: Some("demo"),
                fuzzy: true,
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        let widen = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultWidenScope)
            .unwrap();
        assert_eq!(widen.drop_params, vec!["repo"]);
    }

    #[test]
    fn build_hints_emits_capped_increase_limit() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::CappedIncreaseLimit);
        assert_eq!(hints[0].action, Some(HintAction::IncreaseLimit));
    }

    #[test]
    fn directory_outline_cap_emits_capped_narrow_filter_first_then_capped_increase_limit() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: false,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                path: Some("src/"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::CappedNarrowFilter);
        assert_eq!(hints[0].tool.as_deref(), Some("get_outline"));
        assert_eq!(
            hints[0].params,
            Some(serde_json::json!({ "narrow_candidates": ["kind", "max_depth"] }))
        );
        assert_eq!(hints[1].code, HintCode::CappedIncreaseLimit);
    }

    #[test]
    fn file_mode_outline_cap_does_not_emit_capped_narrow_filter() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: false,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                file: Some("src/lib.rs"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert!(
            !hints
                .iter()
                .any(|hint| hint.code == HintCode::CappedNarrowFilter)
        );
        assert_eq!(hints[0].code, HintCode::CappedIncreaseLimit);
    }

    #[test]
    fn build_hints_emits_tier3_indexing_wait_when_running() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Running,
            reason_code: None,
            reason: None,
        }]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::Tier3IndexingWait);
        assert_eq!(hints[0].target.as_deref(), Some("tier3"));
    }

    #[test]
    fn build_hints_emits_reindex_via_cli_when_not_recorded_no_active_job() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Missing,
            reason_code: Some(ReasonCode::NotRecorded),
            reason: Some("analyzer run not recorded".into()),
        }]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView {
                repo: Some("demo"),
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        let hint = hints
            .iter()
            .find(|hint| hint.code == HintCode::ReindexViaCli)
            .unwrap();
        assert!(hint.message.contains("cairn ctl repo reindex demo"));
        assert!(hint.action.is_none());
    }

    #[test]
    fn build_diagnostics_from_tier3_analyzer_states() {
        let tier3_status = status_from_analyzers(vec![
            TierAnalyzerStatus {
                id: Some("missing-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Missing,
                reason_code: Some(ReasonCode::BinaryNotFound),
                reason: Some("binary missing".into()),
            },
            TierAnalyzerStatus {
                id: Some("stale-lsp".into()),
                language: "python".into(),
                tier: default_tier(),
                state: AnalyzerState::Stale,
                reason_code: Some(ReasonCode::StaleRevision),
                reason: Some("revision changed".into()),
            },
            TierAnalyzerStatus {
                id: Some("ruby-lsp".into()),
                language: "ruby".into(),
                tier: default_tier(),
                state: AnalyzerState::Skipped,
                reason_code: Some(ReasonCode::WorkspaceUnsuitable),
                reason: Some("Gemfile without Gemfile.lock".into()),
            },
        ]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let diagnostics = build_diagnostics(&ctx);
        let codes = diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                DiagnosticCode::AnalyzerBinaryMissing,
                DiagnosticCode::AnalyzerStale,
                DiagnosticCode::WorkspaceUnsuitable,
            ]
        );
        assert_eq!(diagnostics[0].severity, DiagnosticSeverity::Warning);
        assert_eq!(diagnostics[1].severity, DiagnosticSeverity::Info);
    }

    #[test]
    fn hints_priority_order_is_array_order() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Running,
            reason_code: None,
            reason: None,
        }]);
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                repo: Some("demo"),
                kind: true,
                ..QueryArgsView::default()
            },
        );

        let codes = build_hints(&ctx)
            .into_iter()
            .map(|hint| hint.code)
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                HintCode::CappedIncreaseLimit,
                HintCode::Tier3IndexingWait,
                HintCode::EmptyResultRelaxFilter,
                HintCode::EmptyResultWidenScope,
            ]
        );
    }

    fn status_from_analyzers(analyzers: Vec<TierAnalyzerStatus>) -> TierStatus {
        TierStatus::from_body(TierStatusBody::from_analyzers(analyzers))
    }

    fn emission_ctx<'a>(
        items_empty: bool,
        completeness: &'a Completeness,
        tier3_status: &'a TierStatus,
        query_args: QueryArgsView<'a>,
    ) -> EmissionContext<'a> {
        EmissionContext {
            tool: QueryToolKind::FindSymbols,
            items_empty,
            completeness,
            tier3_status,
            query_args,
        }
    }
}
