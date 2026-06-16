//! Shared blocking helpers for data-RPC methods.

use std::collections::{BTreeSet, HashSet};

use cairn_proto::{
    AnalyzerState, Completeness, PartialReason, ReasonCode, Tier3AnalyzerStatus, Tier3Status,
    Tier3StatusBody,
};
use rusqlite::{OptionalExtension, params};

use crate::anchor;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::manifest::ManifestId;
use crate::workspace_analyzer::{WorkspaceAnalyzer, all_workspace_analyzers};
use crate::{Error, Result};

use super::DataCtx;

/// Open one requested repo store or every registered store, run the
/// per-store query, and apply the shared limit-probe semantics.
pub(crate) async fn with_one_or_all_stores<T, F, S>(
    ctx: &DataCtx,
    requested_repo: Option<String>,
    method_name: &'static str,
    effective_limit: u32,
    mut query_store: F,
    mut finalize: S,
) -> Result<(Vec<T>, bool)>
where
    T: Send + 'static,
    F: FnMut(&cas_registry::AliasEntry, &rusqlite::Connection) -> Result<Vec<T>> + Send + 'static,
    S: FnMut(&mut Vec<T>) + Send + 'static,
{
    let cas_data_dir = ctx.cas_data_dir.clone();
    tokio::task::spawn_blocking(move || -> Result<(Vec<T>, bool)> {
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        let aliases = match requested_repo.as_deref() {
            Some(name) => {
                let entry = cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
                    Error::RepoNotFound {
                        alias: name.to_string(),
                    }
                })?;
                vec![entry]
            }
            None => cas_registry::list_all(&index)?,
        };

        let mut out = Vec::new();
        let mut capped = false;
        for entry in aliases {
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            let mut hits = match query_store(&entry, &conn) {
                Ok(h) => h,
                Err(Error::AnchorNotFound { .. }) => continue,
                Err(other) => return Err(other),
            };
            capped |= trim_to_requested_limit(&mut hits, effective_limit);
            out.extend(hits);
        }

        finalize(&mut out);
        capped |= trim_to_requested_limit(&mut out, effective_limit);
        Ok((out, capped))
    })
    .await
    .map_err(|e| Error::internal_task_panic(method_name, e))?
}

pub(crate) fn limit_with_probe(effective_limit: u32) -> u32 {
    effective_limit.saturating_add(1)
}

pub(crate) fn trim_to_requested_limit<T>(rows: &mut Vec<T>, effective_limit: u32) -> bool {
    let requested = effective_limit as usize;
    if rows.len() > requested {
        rows.truncate(requested);
        true
    } else {
        false
    }
}

pub(crate) fn completeness_for_cap(capped: bool) -> Completeness {
    if capped {
        Completeness::partial_truncated(PartialReason::Cap)
    } else {
        Completeness::complete()
    }
}

pub(crate) fn parser_id_filter<I>(parser_ids: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = String>,
{
    parser_ids
        .into_iter()
        .filter(|parser_id| !parser_id.is_empty())
        .collect::<BTreeSet<_>>()
}

pub(crate) async fn tier3_status_for_query(
    ctx: &DataCtx,
    requested_repo: Option<String>,
    anchor_arg: Option<String>,
    branch_arg: Option<String>,
    relevant_parser_ids: BTreeSet<String>,
    verbose_tier3: bool,
    method_name: &'static str,
) -> Result<Tier3Status> {
    let cas_data_dir = ctx.cas_data_dir.clone();
    tokio::task::spawn_blocking(move || -> Result<Tier3Status> {
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        let aliases = match requested_repo.as_deref() {
            Some(name) => {
                let entry = cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
                    Error::RepoNotFound {
                        alias: name.to_string(),
                    }
                })?;
                vec![entry]
            }
            None => cas_registry::list_all(&index)?,
        };

        let mut analyzers = Vec::new();
        let mut repo_wide_analyzers = Vec::new();
        for entry in aliases {
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            let anchor = anchor::resolve_explicit_or_default(
                &conn,
                anchor_arg.as_deref(),
                branch_arg.as_deref(),
            )?;
            let Some(manifest_id) = anchor::resolve(&conn, &anchor)? else {
                continue;
            };
            analyzers.extend(
                compute_tier3_status_for_parser_ids(
                    &conn,
                    manifest_id,
                    Some(&relevant_parser_ids),
                )?
                .analyzers,
            );
            if verbose_tier3 {
                repo_wide_analyzers.extend(
                    compute_tier3_status(&conn, manifest_id)?
                        .this_query
                        .analyzers,
                );
            }
        }
        analyzers.sort();
        analyzers.dedup();
        let this_query = Tier3StatusBody::from_analyzers(analyzers);
        let status = Tier3Status::from_body(this_query);
        if verbose_tier3 {
            repo_wide_analyzers.sort();
            repo_wide_analyzers.dedup();
            Ok(status.with_repo_wide(Tier3StatusBody::from_analyzers(repo_wide_analyzers)))
        } else {
            Ok(status)
        }
    })
    .await
    .map_err(|e| Error::internal_task_panic(format!("{method_name} tier3 status"), e))?
}

pub(crate) fn compute_tier3_status(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<Tier3Status> {
    Ok(Tier3Status::from_body(
        compute_tier3_status_body_with_analyzers(
            conn,
            manifest_id,
            all_workspace_analyzers(),
            None,
        )?,
    ))
}

pub(crate) fn compute_tier3_status_response(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    parser_ids: Option<&BTreeSet<String>>,
    verbose_tier3: bool,
) -> Result<Tier3Status> {
    let status = Tier3Status::from_body(compute_tier3_status_for_parser_ids(
        conn,
        manifest_id,
        parser_ids,
    )?);
    if verbose_tier3 {
        Ok(status.with_repo_wide(compute_tier3_status(conn, manifest_id)?.this_query))
    } else {
        Ok(status)
    }
}

pub(crate) fn compute_tier3_status_for_parser_ids(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    parser_ids: Option<&BTreeSet<String>>,
) -> Result<Tier3StatusBody> {
    compute_tier3_status_body_with_analyzers(
        conn,
        manifest_id,
        all_workspace_analyzers(),
        parser_ids,
    )
}

#[cfg(test)]
fn compute_tier3_status_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
) -> Result<Tier3Status> {
    Ok(Tier3Status::from_body(
        compute_tier3_status_body_with_analyzers(conn, manifest_id, analyzers, None)?,
    ))
}

fn compute_tier3_status_body_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
    relevant_parser_ids: Option<&BTreeSet<String>>,
) -> Result<Tier3StatusBody> {
    let manifest_parser_ids = manifest_parser_ids(conn, manifest_id)?;
    let manifest_parser_ids_sorted = manifest_parser_ids.iter().cloned().collect::<BTreeSet<_>>();
    let relevant_parser_ids = relevant_parser_ids.unwrap_or(&manifest_parser_ids_sorted);
    let mut described_parser_ids = BTreeSet::new();
    let mut statuses = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT status, error, analyzer_revision FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
    )?;

    for analyzer in analyzers {
        let parser_id = analyzer.parser_id();
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

    for parser_id in relevant_parser_ids {
        if !manifest_parser_ids.contains(parser_id) || described_parser_ids.contains(parser_id) {
            continue;
        }
        statuses.push(Tier3AnalyzerStatus {
            id: None,
            language: language_from_parser_id(parser_id),
            state: AnalyzerState::NotApplicable,
            reason_code: Some(ReasonCode::NotApplicable),
            reason: Some("no tier3 analyzer for language".into()),
        });
    }
    statuses.sort();
    statuses.dedup();
    Ok(Tier3StatusBody::from_analyzers(statuses))
}

fn analyzer_status_from_run(
    analyzer_id: &str,
    language: &str,
    expected_revision: u32,
    row: Option<(String, Option<String>, i64)>,
) -> Tier3AnalyzerStatus {
    let Some((status, error, revision)) = row else {
        return Tier3AnalyzerStatus {
            id: Some(analyzer_id.into()),
            language: language.into(),
            state: AnalyzerState::Missing,
            reason_code: Some(ReasonCode::Unknown),
            reason: Some("analyzer run not recorded".into()),
        };
    };
    if revision != i64::from(expected_revision) {
        return Tier3AnalyzerStatus {
            id: Some(analyzer_id.into()),
            language: language.into(),
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
    Tier3AnalyzerStatus {
        id: Some(analyzer_id.into()),
        language: language.into(),
        state,
        reason_code,
        reason: error.or_else(|| (status == "cancelled").then(|| "cancelled".into())),
    }
}

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

fn language_from_parser_id(parser_id: &str) -> String {
    let language = parser_id.strip_prefix("tree-sitter-").unwrap_or(parser_id);
    if language == "md" {
        return "markdown".into();
    }
    language.strip_suffix("-ng").unwrap_or(language).to_string()
}

fn manifest_parser_ids(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT b.parser_id
           FROM blobs b
           JOIN manifest_entries me ON me.blob_sha = b.blob_sha
          WHERE me.manifest_id = ?1",
    )?;
    let parser_ids = stmt
        .query_map(params![manifest_id.0], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    Ok(parser_ids)
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use serde_json::Value;

    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    use super::DataCtx;
    use crate::data_rpc::DataMethod;

    pub(crate) struct DataRpcFixture {
        pub(crate) _repo: tempfile::TempDir,
        pub(crate) _data: tempfile::TempDir,
        pub(crate) ctx: DataCtx,
    }

    pub(crate) fn registered_fixture() -> DataRpcFixture {
        registered_fixture_with_files(&[(
            "src/lib.rs",
            "use std::fmt;\n\
             use std::fs;\n\
             use std::io;\n\
             pub trait Trait {}\n\
             pub struct A;\n\
             pub struct B;\n\
             pub struct C;\n\
             impl Trait for A {}\n\
             impl Trait for B {}\n\
             impl Trait for C {}\n\
             pub fn target() {}\n\
             pub fn caller_a() { target(); }\n\
             pub fn caller_b() { target(); }\n\
             pub fn caller_c() { target(); }\n",
        )])
    }

    pub(crate) fn registered_fixture_with_files(files: &[(&str, &str)]) -> DataRpcFixture {
        let (repo, _sha) = init_repo(files);
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        let canonical = std::fs::canonicalize(repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas.store_db_path(&repo_hash);
        let mut store = cas_store::open(&store_path).unwrap();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        register_repo(&mut store, &canonical, now_ns).unwrap();

        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "demo",
            &canonical.to_string_lossy(),
            &repo_hash,
            now_ns,
        )
        .unwrap();
        tx.commit().unwrap();

        DataRpcFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
            },
        }
    }

    pub(crate) async fn assert_limit_probe(
        method: &dyn DataMethod,
        exact_params: Value,
        over_params: Value,
    ) {
        let fixture = registered_fixture();

        let exact = method.dispatch(&fixture.ctx, exact_params).await.unwrap();
        assert_eq!(exact["items"].as_array().unwrap().len(), 3);
        assert_eq!(
            serde_json::from_value::<Completeness>(exact["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let over = method.dispatch(&fixture.ctx, over_params).await.unwrap();
        assert_eq!(over["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(over["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::workspace_analyzer::{AnalyzerProgress, WorkspaceFacts, WorkspaceFile};

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
    fn exact_limit_rows_are_complete() {
        let mut rows = vec![1, 2];
        assert!(!trim_to_requested_limit(&mut rows, 2));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn over_limit_rows_are_partial_and_truncated() {
        let mut rows = vec![1, 2, 3];
        assert!(trim_to_requested_limit(&mut rows, 2));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn probe_limit_adds_one() {
        assert_eq!(limit_with_probe(2), 3);
    }

    #[test]
    fn completeness_for_cap_marks_partial_with_cap_reason() {
        assert_eq!(completeness_for_cap(false), Completeness::Complete);
        assert_eq!(
            completeness_for_cap(true),
            Completeness::Partial {
                missing_tiers: Vec::new(),
                reason: Some(PartialReason::Cap),
            }
        );
    }

    #[test]
    fn tier3_status_is_ready_when_all_expected_analyzers_succeeded() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "demo-lsp", "succeeded");

        let status = compute_tier3_status_with_analyzers(
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
            vec![Tier3AnalyzerStatus {
                id: Some("demo-lsp".into()),
                language: "rust".into(),
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

        let status = compute_tier3_status_with_analyzers(
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
            vec![Tier3AnalyzerStatus {
                id: Some("demo-lsp".into()),
                language: "rust".into(),
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

        let status = compute_tier3_status_with_analyzers(
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
            vec![Tier3AnalyzerStatus {
                id: None,
                language: "rust".into(),
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
        let status = compute_tier3_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![Tier3AnalyzerStatus {
                id: Some("rust-lsp".into()),
                language: "rust".into(),
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
        let status = compute_tier3_status_body_with_analyzers(
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
        let status = compute_tier3_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![
                Tier3AnalyzerStatus {
                    id: Some("python-lsp".into()),
                    language: "python".into(),
                    state: AnalyzerState::Running,
                    reason_code: None,
                    reason: None,
                },
                Tier3AnalyzerStatus {
                    id: Some("rust-lsp".into()),
                    language: "rust".into(),
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
        let status = Tier3Status::from_body(
            compute_tier3_status_body_with_analyzers(
                &conn,
                manifest_id,
                multi_language_analyzers(),
                Some(&parser_ids),
            )
            .unwrap(),
        );
        assert!(status.repo_wide.is_none());

        let status = status.with_repo_wide(
            compute_tier3_status_body_with_analyzers(
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

        let status = compute_tier3_status_body_with_analyzers(
            &conn,
            manifest_id,
            Vec::new(),
            Some(&parser_ids),
        )
        .unwrap();

        assert!(status.ready);
        assert_eq!(
            status.analyzers,
            vec![Tier3AnalyzerStatus {
                id: None,
                language: "markdown".into(),
                state: AnalyzerState::NotApplicable,
                reason_code: Some(ReasonCode::NotApplicable),
                reason: Some("no tier3 analyzer for language".into()),
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
