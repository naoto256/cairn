//! Shared blocking helpers for data-RPC methods.

use std::collections::{BTreeSet, HashSet};

use cairn_proto::{Completeness, PartialReason, PendingAnalyzer, Tier3Status};
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

pub(crate) async fn tier3_status_for_query(
    ctx: &DataCtx,
    requested_repo: Option<String>,
    anchor_arg: Option<String>,
    branch_arg: Option<String>,
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

        let mut pending = BTreeSet::new();
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
            pending.extend(compute_tier3_status(&conn, manifest_id)?.pending_analyzers);
        }
        Ok(Tier3Status {
            ready: pending.is_empty(),
            pending_analyzers: pending.into_iter().collect(),
        })
    })
    .await
    .map_err(|e| Error::internal_task_panic(format!("{method_name} tier3 status"), e))?
}

pub(crate) fn compute_tier3_status(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<Tier3Status> {
    compute_tier3_status_with_analyzers(conn, manifest_id, all_workspace_analyzers())
}

fn compute_tier3_status_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
) -> Result<Tier3Status> {
    let parser_ids = manifest_parser_ids(conn, manifest_id)?;
    let mut expected_ids = analyzers
        .into_iter()
        .filter(|analyzer| parser_ids.contains(analyzer.parser_id()))
        .map(|analyzer| analyzer.id().to_string())
        .collect::<Vec<_>>();
    expected_ids.sort();
    expected_ids.dedup();

    if expected_ids.is_empty() {
        return Ok(Tier3Status::ready());
    }

    let mut stmt = conn.prepare(
        "SELECT status FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
    )?;
    let mut pending = Vec::new();
    for analyzer_id in expected_ids {
        let state = stmt
            .query_row(params![manifest_id.0, analyzer_id.as_str()], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
            .unwrap_or_else(|| "missing".to_string());
        if !matches!(state.as_str(), "succeeded" | "skipped") {
            pending.push(PendingAnalyzer { analyzer_id, state });
        }
    }
    Ok(Tier3Status {
        ready: pending.is_empty(),
        pending_analyzers: pending,
    })
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
        let (repo, _sha) = init_repo(&[(
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
        )]);
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
    }

    impl WorkspaceAnalyzer for TestAnalyzer {
        fn id(&self) -> &'static str {
            self.id
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            "test"
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
            })],
        )
        .unwrap();

        assert_eq!(status, Tier3Status::ready());
    }

    #[test]
    fn tier3_status_reports_running_pending_analyzer() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "demo-lsp", "running");

        let status = compute_tier3_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "tree-sitter-rust",
            })],
        )
        .unwrap();

        assert_eq!(
            status,
            Tier3Status {
                ready: false,
                pending_analyzers: vec![PendingAnalyzer {
                    analyzer_id: "demo-lsp".into(),
                    state: "running".into(),
                }],
            }
        );
    }

    #[test]
    fn tier3_status_is_ready_when_no_analyzers_match_manifest() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);

        let status = compute_tier3_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "not-present",
            })],
        )
        .unwrap();

        assert_eq!(status, Tier3Status::ready());
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
}
