//! `find_impls` — trait/impl edges across an indexed repo. Reads the
//! CAS `implementations` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::Completeness;
use cairn_proto::methods::{ImplHit, ImplsArgs, ImplsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::anchor::AnchorName;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::query::{self, FindImplsArgs as QueryArgs};
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

        let cas_data_dir = ctx.cas_data_dir.clone();
        let q = QueryArgs {
            interface_qualified: args.trait_.clone(),
            type_qualified: args.type_.clone(),
            limit: args.limit,
        };
        let anchor = args
            .branch
            .as_deref()
            .map_or_else(AnchorName::head, AnchorName::branch);
        let repo_alias = args.repo.clone();

        let items = tokio::task::spawn_blocking(move || -> Result<Vec<ImplHit>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &repo_alias)?.ok_or_else(|| {
                Error::InvalidArgument(format!("unknown repo alias: `{repo_alias}`"))
            })?;
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;

            let hits = match query::find_impls(&conn, &anchor, &q) {
                Ok(h) => h,
                Err(Error::InvalidArgument(msg)) if msg.contains("anchor not found") => {
                    Vec::new()
                }
                Err(other) => return Err(other),
            };

            let alias = entry.alias.clone();
            let branch = anchor.as_str().to_string();
            Ok(hits
                .into_iter()
                .map(|h| ImplHit {
                    type_qualified: h.type_qualified,
                    interface_qualified: h.interface_qualified,
                    kind: h.kind,
                    branch: branch.clone(),
                    location: format!("{}:{}:{}:{}", alias, branch, h.path, h.line),
                })
                .collect())
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("find_impls task panicked: {e}")))??;

        Ok(serde_json::to_value(ImplsResult {
            items,
            completeness: Completeness::complete(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImpls);
