//! `find_impls` — trait/impl edges across an indexed repo. Reads the
//! CAS `implementations` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::Completeness;
use cairn_proto::methods::{ImplHit, ImplsArgs, ImplsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::with_repo_conn;
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

        let q = QueryArgs {
            interface_qualified: args.trait_.clone(),
            type_qualified: args.type_.clone(),
            limit: args.limit,
        };
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let repo_alias = args.repo.clone();

        let items = with_repo_conn(ctx, &repo_alias, "find_impls", move |entry, conn| {
            let hits = match query::find_impls(&conn, &anchor, &q) {
                Ok(h) => h,
                Err(Error::InvalidArgument(msg)) if msg.contains("anchor not found") => Vec::new(),
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
        .await?;

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
