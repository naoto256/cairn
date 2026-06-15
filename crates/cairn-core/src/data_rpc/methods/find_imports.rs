//! `find_imports` — `use` statements across an indexed repo. Reads
//! the CAS `imports` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::methods::{ImportHit, ImportsArgs, ImportsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::Result;
use crate::data_rpc::helpers::{
    completeness_for_cap, limit_with_probe, tier3_status_for_query, with_one_or_all_stores,
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

        let (items, capped) = with_one_or_all_stores(
            ctx,
            requested_repo,
            "find_imports",
            effective_limit,
            move |entry, conn| {
                let anchor = crate::anchor::resolve_explicit_or_default(
                    conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let anchor_label = anchor.as_str().to_string();
                let hits = query::find_imports(conn, &anchor, &q)?;
                Ok(hits
                    .into_iter()
                    .map(|h| {
                        let location =
                            format!("{}:{}:{}:{}", entry.alias, anchor_label, h.file, h.line);
                        ImportHit {
                            file: h.file,
                            to_module: h.to_module,
                            imported: h.imported,
                            alias: h.alias,
                            is_reexport: h.is_reexport,
                            branch: anchor_label.clone(),
                            location,
                            line: h.line,
                        }
                    })
                    .collect())
            },
            |_out: &mut Vec<ImportHit>| {},
        )
        .await?;
        let tier3_status = tier3_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            "find_imports",
        )
        .await?;

        Ok(serde_json::to_value(ImportsResult {
            items,
            completeness: completeness_for_cap(capped),
            tier3_status,
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
