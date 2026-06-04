//! `find_imports` — `use` statements across an indexed repo. Reads
//! the CAS `imports` table joined against the requested anchor's
//! manifest entries.

use cairn_proto::methods::{ImportHit, ImportsArgs, ImportsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    completeness_for_cap, limit_with_probe, trim_to_requested_limit, with_repo_conn,
};
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

        let effective_limit = args.limit.unwrap_or(200).max(1);
        let q = QueryArgs {
            file: args.file.clone(),
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let repo_alias = args.repo.clone();

        let (items, capped) =
            with_repo_conn(ctx, &repo_alias, "find_imports", move |_entry, conn| {
                let mut hits = match query::find_imports(&conn, &anchor, &q) {
                    Ok(h) => h,
                    Err(Error::AnchorNotFound { .. }) => Vec::new(),
                    Err(other) => return Err(other),
                };
                let capped = trim_to_requested_limit(&mut hits, effective_limit);

                let items = hits
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
                    .collect();
                Ok((items, capped))
            })
            .await?;

        Ok(serde_json::to_value(ImportsResult {
            items,
            completeness: completeness_for_cap(capped),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImports);

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::data_rpc::helpers::test_support::assert_limit_probe;

    #[tokio::test]
    async fn exact_limit_is_complete_and_over_limit_is_partial() {
        assert_limit_probe(
            &FindImports,
            json!({"repo": "demo", "limit": 3}),
            json!({"repo": "demo", "limit": 2}),
        )
        .await;
    }
}
