//! `find_supertypes` — "what does `name` extend / implement / mix in?"

use cairn_proto::methods::{FindSupertypesArgs, FindSupertypesResult, ImplHit};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::find_subtypes::into_wire_hit;
use crate::data_rpc::helpers::{
    completeness_for_cap, limit_with_probe, parser_id_filter, tier3_status_for_query,
    with_one_or_all_stores,
};
use crate::query::{self, FindSupertypesArgs as QueryArgs};
use crate::{Error, Result};

pub struct FindSupertypes;

#[async_trait::async_trait]
impl DataMethod for FindSupertypes {
    fn name(&self) -> &'static str {
        "find_supertypes"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindSupertypesArgs = parse_params(params)?;
        if args.name.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_supertypes: `name` must be non-empty".into(),
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

        let (hits, capped) = with_one_or_all_stores(
            ctx,
            requested_repo,
            "find_supertypes",
            effective_limit,
            move |entry, conn| {
                let anchor = crate::anchor::resolve_explicit_or_default(
                    conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let anchor_label = anchor.as_str().to_string();
                let hits = query::find_supertypes(conn, &anchor, &q)?;
                Ok(hits
                    .into_iter()
                    .map(|hit| {
                        let parser_id = hit.parser_id.clone();
                        (into_wire_hit(&entry.alias, &anchor_label, hit), parser_id)
                    })
                    .collect())
            },
            |_out: &mut Vec<(ImplHit, String)>| {},
        )
        .await?;
        let parser_ids = parser_id_filter(hits.iter().map(|(_, parser_id)| parser_id.clone()));
        let items = hits.into_iter().map(|(item, _)| item).collect();
        let tier3_status = tier3_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            parser_ids,
            args.tier3.verbose_tier3,
            "find_supertypes",
        )
        .await?;

        Ok(serde_json::to_value(FindSupertypesResult {
            items,
            completeness: completeness_for_cap(capped),
            tier3_status,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindSupertypes);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[tokio::test]
    async fn returns_base_traits_for_a_pinned_type() {
        let fixture = ts_fixture();
        let result = FindSupertypes
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "Dog", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(
            items
                .iter()
                .any(|h| h["type_qualified"] == "Dog" && h["interface_qualified"] == "Animal"),
            "supertypes of Dog should include Animal: {items:?}"
        );
    }

    #[tokio::test]
    async fn rejects_empty_name() {
        let fixture = ts_fixture();
        let err = FindSupertypes
            .dispatch(&fixture.ctx, json!({"repo": "demo", "name": ""}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    struct Fixture {
        _repo: tempfile::TempDir,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn ts_fixture() -> Fixture {
        let (repo, _sha) = init_repo(&[(
            "src/pets.ts",
            "class Animal {}\n\
             class Dog extends Animal {}\n",
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

        Fixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
            },
        }
    }
}
