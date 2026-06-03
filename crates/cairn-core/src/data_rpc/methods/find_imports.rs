//! `find_imports` — `use` statements across an indexed repo. Reads
//! the CAS `imports` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::Completeness;
use cairn_proto::methods::{ImportHit, ImportsArgs, ImportsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::with_repo_conn;
use crate::query::{self, FindImportsArgs as QueryArgs};
use crate::{Error, Result};

pub struct FindImports;

#[async_trait::async_trait]
impl DataMethod for FindImports {
    fn name(&self) -> &'static str {
        "find_imports"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ImportsArgs = parse_params(params)?;

        let q = QueryArgs {
            file: args.file.clone(),
            limit: args.limit,
        };
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let repo_alias = args.repo.clone();

        let items = with_repo_conn(ctx, &repo_alias, "find_imports", move |_entry, conn| {
            let hits = match query::find_imports(&conn, &anchor, &q) {
                Ok(h) => h,
                Err(Error::InvalidArgument(msg)) if msg.contains("anchor not found") => Vec::new(),
                Err(other) => return Err(other),
            };

            Ok(hits
                .into_iter()
                .map(|h| ImportHit {
                    file: h.file,
                    to_module: h.to_module,
                    imported: h.imported,
                    alias: h.alias,
                    is_reexport: h.is_reexport,
                    branch: anchor.as_str().to_string(),
                    line: h.line,
                })
                .collect())
        })
        .await?;

        Ok(serde_json::to_value(ImportsResult {
            items,
            completeness: Completeness::complete(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImports);
