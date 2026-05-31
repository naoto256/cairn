//! `find_references` — either way: refs that target a symbol
//! (incoming, default) or refs inside a symbol's body (outgoing).
//! Reads the CAS `refs` table scoped by the resolved anchor.

use cairn_proto::Completeness;
use cairn_proto::methods::{FindReferenceHit, FindReferencesArgs, FindReferencesResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::anchor::AnchorName;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::{Error, Result};

pub struct FindReferences;

#[async_trait::async_trait]
impl DataMethod for FindReferences {
    fn name(&self) -> &'static str {
        "find_references"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindReferencesArgs = parse_params(params)?;
        if args.symbol.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_references: `symbol` must be non-empty".into(),
            ));
        }

        let cas_data_dir = ctx.cas_data_dir.clone();
        let q = QueryArgs {
            symbol: args.symbol.clone(),
            direction: args.direction,
            kind: args.kind,
            limit: Some(args.limit.unwrap_or(100)),
        };
        let anchor = args
            .branch
            .as_deref()
            .map_or_else(AnchorName::head, AnchorName::branch);
        let repo_alias = args.repo.clone();

        let items = tokio::task::spawn_blocking(move || -> Result<Vec<FindReferenceHit>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &repo_alias)?.ok_or_else(|| {
                Error::InvalidArgument(format!("unknown repo alias: `{repo_alias}`"))
            })?;
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;

            let hits = match query::find_references(&conn, &anchor, &q) {
                Ok(h) => h,
                Err(Error::InvalidArgument(msg)) if msg.contains("anchor not found") => {
                    // The requested branch doesn't exist as an anchor
                    // in this repo — return an empty result rather
                    // than failing the whole request.
                    Vec::new()
                }
                Err(other) => return Err(other),
            };

            Ok(hits
                .into_iter()
                .map(|h| into_wire_hit(&entry.alias, anchor.as_str(), h))
                .collect())
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("find_references task panicked: {e}")))??;

        Ok(serde_json::to_value(FindReferencesResult {
            items,
            completeness: Completeness::complete(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindReferences);

fn into_wire_hit(repo: &str, anchor: &str, h: ReferenceHit) -> FindReferenceHit {
    let location = format!("{repo}:{anchor}:{}:{}", h.path, h.line);
    FindReferenceHit {
        target_name: h.target_name,
        target_qualified: h.target_qualified,
        kind: h.kind,
        enclosing_qualified: h.enclosing_qualified,
        branch: anchor.to_string(),
        location,
    }
}
