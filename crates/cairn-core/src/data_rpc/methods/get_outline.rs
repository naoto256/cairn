//! `get_outline` — per-file symbol structure on top of the CAS store.

use cairn_proto::Completeness;
use cairn_proto::common::SourceTier;
use cairn_proto::methods::{OutlineArgs, OutlineItem, OutlineResult};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::anchor::AnchorName;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::query::{self, OutlineItem as QueryOutlineItem};
use crate::{Error, Result};

pub struct GetOutline;

#[async_trait::async_trait]
impl DataMethod for GetOutline {
    fn name(&self) -> &'static str {
        "get_outline"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: OutlineArgs = parse_params(params)?;
        let cas_data_dir = ctx.cas_data_dir.clone();
        let repo_alias = args.repo.clone();
        let file = args.file.clone();
        let anchor = AnchorName::head();

        let items = tokio::task::spawn_blocking(move || -> Result<Vec<OutlineItem>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &repo_alias)?.ok_or_else(|| {
                Error::InvalidArgument(format!("unknown repo alias: `{repo_alias}`"))
            })?;
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;

            let raw = match query::get_outline(&conn, &anchor, &file, None) {
                Ok(r) => r,
                Err(Error::InvalidArgument(msg)) if msg.contains("anchor not found") => {
                    Vec::new()
                }
                Err(other) => return Err(other),
            };
            Ok(raw.into_iter().map(into_wire_item).collect())
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("outline task panicked: {e}")))??;

        debug!(repo = %args.repo, file = %args.file, count = items.len(), "outline served");
        Ok(serde_json::to_value(OutlineResult {
            items,
            completeness: Completeness::complete(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetOutline);

fn into_wire_item(q: QueryOutlineItem) -> OutlineItem {
    OutlineItem {
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
