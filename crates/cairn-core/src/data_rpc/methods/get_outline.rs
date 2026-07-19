//! `get_outline` — per-file symbol structure on top of the CAS store.

use cairn_proto::common::SourceTier;
use cairn_proto::methods::{OutlineArgs, OutlineItem, OutlineResult};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
};
use crate::query::{self, OutlineFilter, OutlineItem as QueryOutlineItem};
use crate::{Error, Result};

pub struct GetOutline;

#[async_trait::async_trait]
impl DataMethod for GetOutline {
    fn name(&self) -> &'static str {
        "get_outline"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: OutlineArgs = parse_params(params)?;
        if args.file.is_none() && args.path.is_none() {
            return Err(Error::InvalidArgument(
                "get_outline: one of `file` / `path` must be supplied".into(),
            ));
        }

        let repo_alias = args.scope.repo.clone();
        let file = args.file.clone();
        let exact_file = file.clone();
        let path = args.path.clone();
        let effective_limit = args.pagination.limit.unwrap_or(200).clamp(1, 1000);
        let kind_filter_set = args.kind.is_some();
        let filter = OutlineFilter {
            kind: args.kind,
            max_depth: args.max_depth,
        };

        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo: repo_alias,
                anchor: None,
                branch: None,
                method_name: "outline",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file,
            },
            move |_entry, conn, snapshot| -> Result<Vec<(OutlineItem, String)>> {
                if let Some(file) = file.as_deref() {
                    let raw = match query::get_outline(conn, &snapshot.anchor, file, None) {
                        Ok(r) => r,
                        Err(Error::AnchorNotFound { .. }) => Vec::new(),
                        Err(other) => return Err(other),
                    };
                    let filtered: Vec<_> = raw
                        .into_iter()
                        .filter(|i| filter.kind.as_ref().is_none_or(|k| &i.kind == k))
                        .map(|item| {
                            let parser_id = item.parser_id.clone();
                            (into_wire_item(item), parser_id)
                        })
                        .collect();
                    return Ok(filtered);
                }

                let path = path.as_deref().expect("validated path when file is absent");
                let raw = match query::get_outline_under_path(
                    conn,
                    &snapshot.anchor,
                    path,
                    None,
                    limit_with_probe(effective_limit),
                    &filter,
                ) {
                    Ok(r) => r,
                    Err(Error::AnchorNotFound { .. }) => Vec::new(),
                    Err(other) => return Err(other),
                };
                Ok(raw
                    .into_iter()
                    .map(|item| {
                        let parser_id = item.parser_id.clone();
                        (into_wire_item(item), parser_id)
                    })
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|(_, parser_id)| parser_id.clone())),
            |_out: &mut Vec<(OutlineItem, String)>| {},
        )
        .await?;
        let items: Vec<OutlineItem> = execution.items.into_iter().map(|(item, _)| item).collect();

        debug!(
            repo = ?args.scope.repo,
            file = ?args.file,
            path = ?args.path,
            count = items.len(),
            "outline served"
        );
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = completeness_for_snapshot_scan(
            execution.capped,
            execution.skipped_unavailable,
            &freshness_issues,
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: true,
                kind: kind_filter_set,
                container: None,
                path: args.path.as_deref(),
                file: args.file.as_deref(),
                max_depth: args.max_depth.is_some(),
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);
        Ok(serde_json::to_value(OutlineResult {
            items,
            completeness,
            tier3_status,
            diagnostics,
            hints,
            timing: cairn_proto::Timing::default(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetOutline);

fn into_wire_item(q: QueryOutlineItem) -> OutlineItem {
    OutlineItem {
        file: q.file,
        kind: q.kind,
        name: q.name,
        qualified: q.qualified,
        signature: q.signature,
        line: q.line,
        doc: q.doc,
        // CAS query layer doesn't yet round-trip per-fact source-tier;
        // mirror the find_symbols default until it does.
        source: SourceTier::Syntactic,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use rusqlite::params;
    use serde_json::json;

    use super::*;
    use crate::anchor;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[tokio::test]
    async fn directory_outline_caps_at_limit() {
        let fixture = outline_fixture();
        let result = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "a/", "limit": 2}),
            )
            .await
            .unwrap();

        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["file"], "a/bar.rs");
        assert_eq!(items[1]["file"], "a/foo.rs");
        assert_eq!(
            serde_json::from_value::<Completeness>(result["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
        assert_eq!(result["hints"][0]["code"], "capped_narrow_filter");
        assert_eq!(result["hints"][1]["code"], "capped_increase_limit");
    }

    #[tokio::test]
    async fn directory_outline_filters_by_kind() {
        let fixture = outline_fixture();
        let result = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "a/", "kind": "function"}),
            )
            .await
            .unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(!items.is_empty());
        assert!(result.get("diagnostics").is_none());
        assert!(result.get("hints").is_none());
        for item in items {
            assert_eq!(item["kind"], "function");
        }
    }

    #[tokio::test]
    async fn missing_exact_file_is_partial_without_speculative_empty_hints() {
        let fixture = outline_fixture();
        let result = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "file": "a/not-indexed.rs"}),
            )
            .await
            .unwrap();

        assert!(result["items"].as_array().unwrap().is_empty());
        assert_eq!(
            serde_json::from_value::<Completeness>(result["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("file_not_indexed_or_snapshot_stale")
        );
        assert_eq!(
            result["diagnostics"][0]["code"],
            "file_not_indexed_or_snapshot_stale"
        );
        assert!(
            result["hints"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hint| { hint["code"] == "file_not_indexed_or_snapshot_stale" })
        );
        assert!(result["hints"].as_array().unwrap().iter().all(|hint| {
            !matches!(
                hint["code"].as_str(),
                Some(
                    "empty_result_relax_filter"
                        | "empty_result_try_fuzzy"
                        | "empty_result_widen_scope"
                )
            )
        }));
    }

    #[tokio::test]
    async fn directory_outline_caps_depth() {
        let fixture = nested_outline_fixture();
        let shallow = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "src/", "max_depth": 1}),
            )
            .await
            .unwrap();
        let files: Vec<&str> = shallow["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["file"].as_str().unwrap())
            .collect();
        assert!(
            files
                .iter()
                .all(|f| !f.trim_start_matches("src/").contains('/'))
        );
        assert!(files.contains(&"src/top.rs"));

        let deep = GetOutline
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "path": "src/", "max_depth": 2}),
            )
            .await
            .unwrap();
        let deep_files: Vec<&str> = deep["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["file"].as_str().unwrap())
            .collect();
        assert!(deep_files.contains(&"src/nest/inner.rs"));
    }

    fn nested_outline_fixture() -> OutlineFixture {
        let (repo, _sha) = init_repo(&[
            ("src/top.rs", "pub fn top_one() {}\n"),
            ("src/nest/inner.rs", "pub fn inner_one() {}\n"),
        ]);
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
        let registration = register_repo(&mut store, &canonical, now_ns).unwrap();

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
        mark_fresh(&mut store, &index, &repo_hash, now_ns, &registration);

        OutlineFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    #[tokio::test]
    async fn rejects_when_neither_file_nor_path() {
        let fixture = outline_fixture();
        let err = GetOutline
            .dispatch(&fixture.ctx, json!({"repo": "demo"}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    struct OutlineFixture {
        _repo: tempfile::TempDir,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn outline_fixture() -> OutlineFixture {
        let (repo, _sha) = init_repo(&[
            ("a/foo.rs", "pub fn foo_one() {}\npub fn foo_two() {}\n"),
            ("a/bar.rs", "pub fn bar_one() {}\n"),
            ("b/baz.rs", "pub fn baz_one() {}\n"),
        ]);
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
        let registration = register_repo(&mut store, &canonical, now_ns).unwrap();

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
        mark_fresh(&mut store, &index, &repo_hash, now_ns, &registration);

        OutlineFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn mark_fresh(
        store: &mut rusqlite::Connection,
        index: &rusqlite::Connection,
        repo_hash: &str,
        now_ns: i64,
        registration: &crate::register::RegisterOutcome,
    ) {
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     applied_generation = 1,
                     last_success_ns = ?1,
                     watcher_state = 'active'
                 WHERE repo_hash = ?2",
                params![now_ns, repo_hash],
            )
            .unwrap();
        let tx = store.transaction().unwrap();
        anchor::set_reconciled(
            &tx,
            &anchor::AnchorName::tentative(registration.worktree_id),
            registration.tentative_manifest,
            now_ns,
            1,
        )
        .unwrap();
        tx.commit().unwrap();
    }
}
