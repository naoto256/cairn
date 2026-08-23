//! Tier-3 analyzer status assembly for query and repository scopes.

use std::collections::BTreeSet;

use cairn_proto::{
    AnalyzerState, ReasonCode, TierAnalyzerStatus, TierStatus, TierStatusBody, default_tier,
};
use rusqlite::{OptionalExtension, params};

use crate::Result;
use crate::manifest::ManifestId;
use crate::workspace_analyzer::{
    WorkspaceAnalyzer, expected_analyzers_for_manifest, manifest_parser_ids,
};
pub(crate) fn parser_id_filter<I>(parser_ids: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = String>,
{
    parser_ids
        .into_iter()
        .filter(|parser_id| !parser_id.is_empty())
        .collect::<BTreeSet<_>>()
}

/// Repo-wide tier-3 status: every analyzer expected for the manifest,
/// regardless of whether any returned row depends on it. Used for the
/// `repo_wide` slice in verbose responses.
pub(crate) fn compute_tier_status(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<TierStatus> {
    Ok(TierStatus::from_body(
        compute_tier_status_body_with_analyzers(
            conn,
            manifest_id,
            expected_analyzers_for_manifest(conn, manifest_id)?,
            None,
        )?,
    ))
}

/// Query-scoped tier-3 status: only analyzers whose parser id appears
/// in `parser_ids`. Passing `None` widens back to every analyzer the
/// manifest expects, matching [`compute_tier_status`].
pub(crate) fn compute_tier_status_for_parser_ids(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    parser_ids: Option<&BTreeSet<String>>,
) -> Result<TierStatusBody> {
    compute_tier_status_body_with_analyzers(
        conn,
        manifest_id,
        expected_analyzers_for_manifest(conn, manifest_id)?,
        parser_ids,
    )
}

#[cfg(test)]
fn compute_tier_status_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
) -> Result<TierStatus> {
    Ok(TierStatus::from_body(
        compute_tier_status_body_with_analyzers(conn, manifest_id, analyzers, None)?,
    ))
}

/// Core tier-3 status assembler. Walks the expected analyzer list,
/// projects each run row into a [`TierAnalyzerStatus`], and back-fills
/// a `NotApplicable` entry for any relevant parser id the manifest
/// contains but no analyzer covers. The double intersection with
/// `manifest_parser_ids` and `relevant_parser_ids` is what limits
/// output to "expected here AND useful for this response".
fn compute_tier_status_body_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
    relevant_parser_ids: Option<&BTreeSet<String>>,
) -> Result<TierStatusBody> {
    let manifest_parser_ids = manifest_parser_ids(conn, manifest_id)?;
    let manifest_parser_ids_sorted = manifest_parser_ids.iter().cloned().collect::<BTreeSet<_>>();
    // `None` here is the "no query-scope filter" signal: fall back to
    // every parser id the manifest carries so repo-wide callers see
    // the full picture.
    let relevant_parser_ids = relevant_parser_ids.unwrap_or(&manifest_parser_ids_sorted);
    let mut described_parser_ids = BTreeSet::new();
    let mut statuses = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT status, error, analyzer_revision FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
    )?;

    for analyzer in analyzers {
        let parser_id = analyzer.parser_id();
        // Skip analyzers whose parser is absent from the manifest or
        // outside the query scope; we only describe analyzers the
        // caller can act on.
        if !manifest_parser_ids.contains(parser_id) || !relevant_parser_ids.contains(parser_id) {
            continue;
        }
        described_parser_ids.insert(parser_id.to_string());
        let row = stmt
            .query_row(params![manifest_id.0, analyzer.id()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .optional()?;
        statuses.push(analyzer_status_from_run(
            analyzer.id(),
            analyzer.language(),
            analyzer.revision(),
            row,
        ));
    }

    // Back-fill parser ids that appear in the manifest and matter for
    // this response but that no registered analyzer covers. Emitting
    // `NotApplicable` here is what lets callers tell "we have no
    // analyzer for this language" apart from "the analyzer ran but
    // reported nothing".
    for parser_id in relevant_parser_ids {
        if !manifest_parser_ids.contains(parser_id) || described_parser_ids.contains(parser_id) {
            continue;
        }
        statuses.push(TierAnalyzerStatus {
            id: None,
            language: language_from_parser_id(parser_id),
            tier: default_tier(),
            state: AnalyzerState::NotApplicable,
            reason_code: Some(ReasonCode::NotApplicable),
            reason: Some("no tier3 analyzer for language".into()),
        });
    }
    statuses.sort();
    statuses.dedup();
    Ok(TierStatusBody::from_analyzers(statuses))
}

/// Translate one `workspace_analysis_runs` row into a wire status,
/// applying two priority rules: an absent row is always
/// `Missing/NotScheduled`, and a revision mismatch always wins over
/// the row's status (a run that succeeded against an old analyzer
/// revision is treated as `Stale`, not `Ready`).
fn analyzer_status_from_run(
    analyzer_id: &str,
    language: &str,
    expected_revision: u32,
    row: Option<(String, Option<String>, i64)>,
) -> TierAnalyzerStatus {
    let Some((status, error, revision)) = row else {
        return TierAnalyzerStatus {
            id: Some(analyzer_id.into()),
            language: language.into(),
            tier: default_tier(),
            state: AnalyzerState::Missing,
            reason_code: Some(ReasonCode::NotScheduled),
            reason: Some("expected analyzer was not scheduled for this manifest".into()),
        };
    };
    // Revision precedes status: a `succeeded` row against an outdated
    // analyzer revision is not `Ready`, it is `Stale`.
    if revision != i64::from(expected_revision) {
        return TierAnalyzerStatus {
            id: Some(analyzer_id.into()),
            language: language.into(),
            tier: default_tier(),
            state: AnalyzerState::Stale,
            reason_code: Some(ReasonCode::Stale),
            reason: Some(format!(
                "analyzer revision changed from {revision} to {expected_revision}"
            )),
        };
    }
    let (state, reason_code) = match status.as_str() {
        "succeeded" => (AnalyzerState::Ready, None),
        "queued" => (AnalyzerState::Queued, None),
        "running" => (AnalyzerState::Running, None),
        "skipped" => (
            AnalyzerState::Skipped,
            reason_code_for_error(&status, error.as_deref()),
        ),
        "timed_out" => (AnalyzerState::Failed, Some(ReasonCode::TimedOut)),
        "failed" => (AnalyzerState::Failed, Some(ReasonCode::AnalyzerFailed)),
        _ => (AnalyzerState::Failed, Some(ReasonCode::Unknown)),
    };
    TierAnalyzerStatus {
        id: Some(analyzer_id.into()),
        language: language.into(),
        tier: default_tier(),
        state,
        reason_code,
        reason: error.or_else(|| (status == "cancelled").then(|| "cancelled".into())),
    }
}

/// Coarse pattern classifier that promotes free-form analyzer error
/// text into a structured `ReasonCode`. The substring checks are
/// case-insensitive and deliberately generous: an unrecognised message
/// falls back to `Unknown` (or `None` when the run actually succeeded
/// but no code was provided) so a new failure mode never crashes the
/// dispatch.
fn reason_code_for_error(status: &str, error: Option<&str>) -> Option<ReasonCode> {
    let Some(error) = error else {
        return (status != "succeeded").then_some(ReasonCode::Unknown);
    };
    let lower = error.to_ascii_lowercase();
    if lower.contains("binary") && (lower.contains("missing") || lower.contains("not available")) {
        Some(ReasonCode::BinaryNotFound)
    } else if lower.contains("no matching files") {
        Some(ReasonCode::NoMatchingFiles)
    } else if lower.contains("workspace unsuitable")
        || lower.contains("gemfile without gemfile.lock")
    {
        Some(ReasonCode::WorkspaceUnsuitable)
    } else if lower.contains("stalled") || lower.contains("timed out") {
        Some(ReasonCode::TimedOut)
    } else {
        Some(ReasonCode::Unknown)
    }
}

/// Best-effort human-readable language label for a `tree-sitter-*`
/// parser id: strip the `tree-sitter-` prefix, normalise the `md`
/// shortcut to `markdown`, and drop the `-ng` next-generation
/// grammar suffix. Purely cosmetic — parser ids remain the identity
/// used everywhere else.
fn language_from_parser_id(parser_id: &str) -> String {
    let language = parser_id.strip_prefix("tree-sitter-").unwrap_or(parser_id);
    if language == "md" {
        return "markdown".into();
    }
    language.strip_suffix("-ng").unwrap_or(language).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::anchor;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::workspace_analyzer::{AnalyzerProgress, WorkspaceFacts, WorkspaceFile};

    use super::super::test_support;

    struct TestAnalyzer {
        id: &'static str,
        parser_id: &'static str,
        language: &'static str,
    }

    impl WorkspaceAnalyzer for TestAnalyzer {
        fn id(&self) -> &'static str {
            self.id
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            self.language
        }

        fn parser_id(&self) -> &'static str {
            self.parser_id
        }

        fn analyze_workspace(
            &self,
            _repo_root: &Path,
            _manifest_id: ManifestId,
            _files: &[WorkspaceFile],
            _progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            Ok(WorkspaceFacts::default())
        }
    }

    #[test]
    fn tier3_status_is_ready_when_all_expected_analyzers_succeeded() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "demo-lsp", "succeeded");

        let status = compute_tier_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            })],
        )
        .unwrap();

        assert!(status.this_query.ready);
        assert_eq!(
            status.this_query.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("demo-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Ready,
                reason_code: None,
                reason: None,
            }]
        );
    }

    #[test]
    fn tier3_status_reports_running_analyzer() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "demo-lsp", "running");

        let status = compute_tier_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            })],
        )
        .unwrap();

        assert_eq!(
            status.this_query.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("demo-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Running,
                reason_code: None,
                reason: None,
            }]
        );
        assert!(!status.this_query.ready);
    }

    #[test]
    fn tier3_status_reports_not_applicable_when_no_analyzers_match_manifest() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);

        let status = compute_tier_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "not-present",
                language: "test",
            })],
        )
        .unwrap();

        assert!(status.this_query.ready);
        assert_eq!(
            status.this_query.analyzers,
            vec![TierAnalyzerStatus {
                id: None,
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::NotApplicable,
                reason_code: Some(ReasonCode::NotApplicable),
                reason: Some("no tier3 analyzer for language".into()),
            }]
        );
    }

    #[test]
    fn tier3_status_parser_filter_excludes_unrelated_language() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::from(["tree-sitter-rust".to_string()]);
        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("rust-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Running,
                reason_code: None,
                reason: None,
            }]
        );
    }

    #[test]
    fn tier3_status_empty_parser_filter_does_not_expand_to_repo_wide() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::new();
        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert!(status.ready);
        assert!(status.analyzers.is_empty());
    }

    #[test]
    fn tier3_status_parser_filter_keeps_multiple_touched_languages() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::from([
            "tree-sitter-python".to_string(),
            "tree-sitter-rust".to_string(),
        ]);
        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![
                TierAnalyzerStatus {
                    id: Some("python-lsp".into()),
                    language: "python".into(),
                    tier: default_tier(),
                    state: AnalyzerState::Running,
                    reason_code: None,
                    reason: None,
                },
                TierAnalyzerStatus {
                    id: Some("rust-lsp".into()),
                    language: "rust".into(),
                    tier: default_tier(),
                    state: AnalyzerState::Running,
                    reason_code: None,
                    reason: None,
                },
            ]
        );
    }

    #[test]
    fn tier3_status_response_includes_repo_wide_only_when_verbose() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::from(["tree-sitter-rust".to_string()]);
        let status = TierStatus::from_body(
            compute_tier_status_body_with_analyzers(
                &conn,
                manifest_id,
                multi_language_analyzers(),
                Some(&parser_ids),
            )
            .unwrap(),
        );
        assert!(status.repo_wide.is_none());

        let status = status.with_repo_wide(
            compute_tier_status_body_with_analyzers(
                &conn,
                manifest_id,
                multi_language_analyzers(),
                None,
            )
            .unwrap(),
        );
        assert_eq!(status.this_query.analyzers.len(), 1);
        assert_eq!(status.repo_wide.unwrap().analyzers.len(), 2);
    }

    #[test]
    fn tier3_status_not_applicable_for_markdown_only_query() {
        let fixture = test_support::registered_fixture_with_files(&[("README.md", "# Project\n")]);
        let (conn, manifest_id) = demo_store(&fixture);
        insert_manifest_parser(
            &conn,
            manifest_id,
            "README.md",
            "markdown-fixture-sha",
            "tree-sitter-md",
        );
        let parser_ids = BTreeSet::from(["tree-sitter-md".to_string()]);

        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            Vec::new(),
            Some(&parser_ids),
        )
        .unwrap();

        assert!(status.ready);
        assert_eq!(
            status.analyzers,
            vec![TierAnalyzerStatus {
                id: None,
                language: "markdown".into(),
                tier: default_tier(),
                state: AnalyzerState::NotApplicable,
                reason_code: Some(ReasonCode::NotApplicable),
                reason: Some("no tier3 analyzer for language".into()),
            }]
        );
    }

    #[test]
    fn expected_analyzers_matches_status_callsite() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_manifest_parser(
            &conn,
            manifest_id,
            "fake.rs",
            "fake-fixture-sha",
            "fake-parser",
        );

        let mut expected_ids = expected_analyzers_for_manifest(&conn, manifest_id)
            .unwrap()
            .into_iter()
            .map(|analyzer| analyzer.id().to_string())
            .collect::<Vec<_>>();
        expected_ids.sort();

        let mut status_ids = compute_tier_status(&conn, manifest_id)
            .unwrap()
            .this_query
            .analyzers
            .into_iter()
            .filter_map(|status| status.id)
            .collect::<Vec<_>>();
        status_ids.sort();

        assert_eq!(status_ids, expected_ids);
        assert!(status_ids.contains(&"fake-workspace".to_string()));
    }

    #[test]
    fn not_scheduled_when_expected_but_no_run_row() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);

        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "rust-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            })],
            None,
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("rust-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Missing,
                reason_code: Some(ReasonCode::NotScheduled),
                reason: Some("expected analyzer was not scheduled for this manifest".into()),
            }]
        );
    }

    fn multi_language_fixture() -> test_support::DataRpcFixture {
        test_support::registered_fixture_with_files(&[
            ("src/lib.rs", "pub fn rust_symbol() {}\n"),
            ("src/app.py", "def python_symbol():\n    pass\n"),
        ])
    }

    fn multi_language_analyzers() -> Vec<Box<dyn WorkspaceAnalyzer>> {
        vec![
            Box::new(TestAnalyzer {
                id: "rust-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            }),
            Box::new(TestAnalyzer {
                id: "python-lsp",
                parser_id: "tree-sitter-python",
                language: "python",
            }),
        ]
    }

    fn demo_store(fixture: &test_support::DataRpcFixture) -> (rusqlite::Connection, ManifestId) {
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        let conn =
            cas_store::open(&fixture.ctx.cas_data_dir.store_db_path(&entry.repo_hash)).unwrap();
        let manifest_id = anchor::resolve(&conn, &anchor::AnchorName::head())
            .unwrap()
            .unwrap();
        (conn, manifest_id)
    }

    fn insert_run(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        analyzer_id: &str,
        status: &str,
    ) {
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, ?2, 1, 'cfg', ?3, 0, 0, NULL, NULL, 0)",
            params![manifest_id.0, analyzer_id, status],
        )
        .unwrap();
    }

    fn insert_manifest_parser(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        path: &str,
        blob_sha: &str,
        parser_id: &str,
    ) {
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![blob_sha, parser_id],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, ?2, ?3)",
            params![manifest_id.0, path, blob_sha],
        )
        .unwrap();
    }
}
