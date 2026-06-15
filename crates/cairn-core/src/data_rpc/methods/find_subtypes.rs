//! `find_subtypes` — "who implements / extends / mixes in `name`?"

use cairn_proto::methods::{FindSubtypesArgs, FindSubtypesResult, ImplHit};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    completeness_for_cap, limit_with_probe, tier3_status_for_query, with_one_or_all_stores,
};
use crate::query::{self, FindSubtypesArgs as QueryArgs, ImplHit as QueryHit};
use crate::{Error, Result};

pub struct FindSubtypes;

#[async_trait::async_trait]
impl DataMethod for FindSubtypes {
    fn name(&self) -> &'static str {
        "find_subtypes"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindSubtypesArgs = parse_params(params)?;
        if args.name.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_subtypes: `name` must be non-empty".into(),
            ));
        }

        let effective_limit = args.pagination.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            name: args.name.clone(),
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();

        let (items, capped) = with_one_or_all_stores(
            ctx,
            requested_repo,
            "find_subtypes",
            effective_limit,
            move |entry, conn| {
                let anchor = crate::anchor::resolve_explicit_or_default(
                    conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let anchor_label = anchor.as_str().to_string();
                let hits = query::find_subtypes(conn, &anchor, &q)?;
                Ok(hits
                    .into_iter()
                    .map(|hit| into_wire_hit(&entry.alias, &anchor_label, hit))
                    .collect())
            },
            |_out: &mut Vec<ImplHit>| {},
        )
        .await?;
        let tier3_status = tier3_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            "find_subtypes",
        )
        .await?;

        Ok(serde_json::to_value(FindSubtypesResult {
            items,
            completeness: completeness_for_cap(capped),
            tier3_status,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindSubtypes);

pub(super) fn into_wire_hit(repo: &str, anchor: &str, h: QueryHit) -> ImplHit {
    ImplHit {
        type_qualified: h.type_qualified,
        interface_qualified: h.interface_qualified,
        kind: h.kind,
        branch: anchor.to_string(),
        location: format!("{repo}:{anchor}:{}:{}", h.path, h.line),
    }
}

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
            &FindSubtypes,
            json!({"repo": "demo", "name": "Trait", "limit": 3}),
            json!({"repo": "demo", "name": "Trait", "limit": 2}),
        )
        .await;
    }

    #[tokio::test]
    async fn repo_none_searches_all_registered_repos_and_caps_accumulated_total() {
        let fixture = cross_repo_fixture();

        let all = FindSubtypes
            .dispatch(
                &fixture.ctx,
                json!({"name": "Trait", "limit": 10, "anchor": "HEAD"}),
            )
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

        let capped = FindSubtypes
            .dispatch(
                &fixture.ctx,
                json!({"name": "Trait", "limit": 2, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_eq!(capped["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(capped["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }

    #[tokio::test]
    async fn rejects_empty_name() {
        let fixture = cross_repo_fixture();
        let err = FindSubtypes
            .dispatch(&fixture.ctx, json!({"name": "", "anchor": "HEAD"}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    struct CrossRepoFixture {
        _repos: Vec<tempfile::TempDir>,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn cross_repo_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/lib.rs",
            "pub trait Trait {}\n\
             pub struct A;\n\
             pub struct B;\n\
             impl Trait for A {}\n\
             impl Trait for B {}\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/lib.rs",
            "pub trait Trait {}\n\
             pub struct C;\n\
             impl Trait for C {}\n",
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
