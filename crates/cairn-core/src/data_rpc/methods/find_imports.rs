//! `find_imports` — `use` statements across an indexed repo. Reads
//! the CAS `imports` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::methods::{ImportHit, ImportsArgs, ImportsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::Result;
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
};
use crate::query::{self, FindImportsArgs as QueryArgs};

pub struct FindImports;

#[async_trait::async_trait]
impl DataMethod for FindImports {
    fn name(&self) -> &'static str {
        "find_imports"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ImportsArgs = parse_params(params)?;

        let effective_limit = args.pagination.limit.unwrap_or(200).max(1);
        let q = QueryArgs {
            file: args.file.clone(),
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();
        let exact_file = args.file.clone();

        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo,
                anchor: anchor_arg,
                branch: branch_arg,
                method_name: "find_imports",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file,
            },
            move |entry, conn, snapshot| {
                let anchor_label = snapshot.anchor.as_str().to_string();
                let hits = query::find_imports(conn, &snapshot.anchor, &q)?;
                Ok(hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id;
                        let location =
                            format!("{}:{}:{}:{}", entry.alias, anchor_label, h.file, h.line);
                        (
                            ImportHit {
                                file: h.file,
                                to_module: h.to_module,
                                imported: h.imported,
                                alias: h.alias,
                                is_reexport: h.is_reexport,
                                kind_source: h.kind_source,
                                target_path: h.target_path,
                                branch: anchor_label.clone(),
                                location,
                                line: h.line,
                            },
                            parser_id,
                        )
                    })
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|(_, parser_id)| parser_id.clone())),
            |_out: &mut Vec<(ImportHit, String)>| {},
        )
        .await?;
        let items: Vec<_> = execution.items.into_iter().map(|(item, _)| item).collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = completeness_for_snapshot_scan(
            execution.capped,
            execution.skipped_unavailable,
            &freshness_issues,
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindImports,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: true,
                kind: false,
                container: None,
                file: args.file.as_deref(),
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);

        Ok(serde_json::to_value(ImportsResult {
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
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImports);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::data_rpc::helpers::test_support::assert_limit_probe;
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[tokio::test]
    async fn exact_limit_is_complete_and_over_limit_is_partial() {
        assert_limit_probe(
            &FindImports,
            json!({"repo": "demo", "limit": 3}),
            json!({"repo": "demo", "limit": 2}),
        )
        .await;
    }

    #[tokio::test]
    async fn repo_none_searches_all_registered_repos_and_caps_accumulated_total() {
        let fixture = cross_repo_fixture();

        let all = FindImports
            .dispatch(&fixture.ctx, json!({"limit": 10, "anchor": "HEAD"}))
            .await
            .unwrap();
        let items = all["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert!(
            items
                .iter()
                .any(|h| h["location"].as_str().unwrap().starts_with("alpha:HEAD:"))
        );
        assert!(
            items
                .iter()
                .any(|h| h["location"].as_str().unwrap().starts_with("beta:HEAD:"))
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(all["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let capped = FindImports
            .dispatch(&fixture.ctx, json!({"limit": 2, "anchor": "HEAD"}))
            .await
            .unwrap();
        assert_eq!(capped["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(capped["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }

    struct CrossRepoFixture {
        _repos: Vec<tempfile::TempDir>,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn cross_repo_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "use std::fmt;\n\
             use std::fs;\n\
             pub fn alpha() {}\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "use std::io;\n\
             pub fn beta() {}\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn register_alias(cas: &CasDataDir, alias: &str, repo_path: &std::path::Path) {
        let canonical = std::fs::canonicalize(repo_path).unwrap();
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
        cas_registry::upsert(&tx, alias, &canonical.to_string_lossy(), &repo_hash, now_ns).unwrap();
        tx.commit().unwrap();
    }
}
