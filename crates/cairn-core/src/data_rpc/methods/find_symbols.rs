//! `find_symbols` — anchor → manifest → symbols JOIN, scoped by the
//! caller's `repo` / `branch` / `query` / `kind` / `container` /
//! `path` filters. `repo = None` walks every registered alias.
//!
//! `include_inherited` is not honored yet on this path; it was a
//! Tier-2 layer in the legacy implementation and comes back in a
//! later iteration.

use cairn_proto::Completeness;
use cairn_proto::common::SourceTier;
use cairn_proto::methods::{FindSymbolArgs, FindSymbolHit, FindSymbolResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::cas::kind_conv::symbol_kind_to_str;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::query::{self, FindSymbolsArgs, SymbolHit};
use crate::{Error, Result};

pub struct FindSymbols;

#[async_trait::async_trait]
impl DataMethod for FindSymbols {
    fn name(&self) -> &'static str {
        "find_symbols"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindSymbolArgs = parse_params(params)?;
        validate(&args)?;

        let cas_data_dir = ctx.cas_data_dir.clone();
        let q = FindSymbolsArgs {
            query: args.query.clone(),
            fuzzy: args.fuzzy,
            kind: args.kind.as_ref().map(symbol_kind_to_str),
            container: args.container.clone(),
            path_prefix: args.path.clone(),
            limit: args.limit,
        };
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let requested_repo = args.repo.clone();
        let signature_only = args.signature_only;

        let items = tokio::task::spawn_blocking(move || -> Result<Vec<FindSymbolHit>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let aliases = match requested_repo.as_deref() {
                Some(name) => {
                    let entry = cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
                        Error::RepoNotFound {
                            alias: name.to_string(),
                        }
                    })?;
                    vec![entry]
                }
                None => cas_registry::list_all(&index)?,
            };

            let mut out = Vec::new();
            for entry in aliases {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let conn = cas_store::open(&store_path)?;
                let hits = match query::find_symbols(&conn, &anchor, &q) {
                    Ok(h) => h,
                    Err(Error::AnchorNotFound { .. }) => {
                        // The requested anchor doesn't exist in this
                        // store — skip rather than fail the whole
                        // query (= other repos may still have it).
                        continue;
                    }
                    Err(other) => return Err(other),
                };
                for h in hits {
                    out.push(into_wire_hit(
                        &entry.alias,
                        anchor.as_str(),
                        h,
                        signature_only,
                    ));
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("find_symbols task panicked: {e}")))??;

        Ok(serde_json::to_value(FindSymbolResult {
            items,
            completeness: Completeness::complete(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindSymbols);

fn validate(args: &FindSymbolArgs) -> Result<()> {
    let any = args.query.as_deref().is_some_and(|s| !s.is_empty())
        || args.kind.is_some()
        || args.container.as_deref().is_some_and(|s| !s.is_empty())
        || args.path.as_deref().is_some_and(|s| !s.is_empty());
    if any {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "find_symbols: at least one of `query`, `kind`, `container`, or `path` \
             must be supplied (an unfiltered enumeration would return every symbol)"
                .into(),
        ))
    }
}

fn into_wire_hit(repo: &str, anchor: &str, h: SymbolHit, signature_only: bool) -> FindSymbolHit {
    let location = format!("{repo}:{anchor}:{}:{}", h.path, h.line);
    FindSymbolHit {
        id: h.id,
        qualified: h.qualified,
        name: h.name,
        kind: h.kind,
        repo: repo.to_string(),
        branch: anchor.to_string(),
        location,
        // `signature_only=true` drops the heaviest field. The naming
        // mirrors `GetSymbolSourceArgs.signature_only`; here the
        // analogous "minimal navigation payload" is everything *but*
        // the signature.
        signature: if signature_only { None } else { h.signature },
        // The CAS query layer doesn't yet round-trip the per-fact
        // source-tier tag; default to Syntactic until it does.
        source: SourceTier::Syntactic,
    }
}
