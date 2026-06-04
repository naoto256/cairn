//! `find_impls` — trait/impl edges across an indexed repo. Reads the
//! CAS `implementations` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::methods::{ImplHit, ImplsArgs, ImplsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::data_rpc::helpers::{completeness_for_cap, limit_with_probe, trim_to_requested_limit};
use crate::query::{self, FindImplsArgs as QueryArgs, ImplHit as QueryHit};
use crate::{Error, Result};

pub struct FindImpls;

#[async_trait::async_trait]
impl DataMethod for FindImpls {
    fn name(&self) -> &'static str {
        "find_impls"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ImplsArgs = parse_params(params)?;
        if args.trait_.is_none() && args.type_.is_none() {
            return Err(Error::InvalidArgument(
                "find_impls: one of `trait` / `type` must be supplied".into(),
            ));
        }

        let effective_limit = args.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            interface_qualified: args.trait_.clone(),
            type_qualified: args.type_.clone(),
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let requested_repo = args.repo.clone();

        let cas_data_dir = ctx.cas_data_dir.clone();
        let (items, capped) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<ImplHit>, bool)> {
                let index = cas_registry::open(&cas_data_dir.index_db_path())?;
                let aliases = match requested_repo.as_deref() {
                    Some(name) => {
                        let entry =
                            cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
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
                    let mut hits = match query::find_impls(&conn, &anchor, &q) {
                        Ok(h) => h,
                        Err(Error::AnchorNotFound { .. }) => continue,
                        Err(other) => return Err(other),
                    };
                    capped |= trim_to_requested_limit(&mut hits, effective_limit);
                    for h in hits {
                        out.push(into_wire_hit(&entry.alias, anchor.as_str(), h));
                    }
                    capped |= trim_to_requested_limit(&mut out, effective_limit);
                }
                Ok((out, capped))
            })
            .await
            .map_err(|e| Error::InvalidArgument(format!("find_impls task panicked: {e}")))??;

        Ok(serde_json::to_value(ImplsResult {
            items,
            completeness: completeness_for_cap(capped),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImpls);

fn into_wire_hit(repo: &str, anchor: &str, h: QueryHit) -> ImplHit {
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
            &FindImpls,
            json!({"repo": "demo", "trait": "Trait", "limit": 3}),
            json!({"repo": "demo", "trait": "Trait", "limit": 2}),
        )
        .await;
    }

    #[tokio::test]
    async fn repo_none_searches_all_registered_repos_and_caps_accumulated_total() {
        let fixture = cross_repo_fixture();

        let all = FindImpls
            .dispatch(&fixture.ctx, json!({"trait": "Trait", "limit": 10}))
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

        let capped = FindImpls
            .dispatch(&fixture.ctx, json!({"trait": "Trait", "limit": 2}))
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
