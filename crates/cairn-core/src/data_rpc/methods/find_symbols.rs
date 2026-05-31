//! `find_symbols` — anchor → manifest → symbols JOIN, scoped by the
//! caller's `repo` / `branch` / `query` / `kind` / `container` /
//! `path` filters. `repo = None` walks every registered alias.
//!
//! `include_inherited` and `fuzzy` are not honored yet on this path;
//! they were Tier-2 / FTS layers in the legacy implementation and
//! come back in later iterations.

use cairn_proto::Completeness;
use cairn_proto::common::{SourceTier, SymbolKind};
use cairn_proto::methods::{FindSymbolArgs, FindSymbolHit, FindSymbolResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::anchor::AnchorName;
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
            kind: args.kind.as_ref().map(symbol_kind_to_db),
            container: args.container.clone(),
            path_prefix: args.path.clone(),
            limit: args.limit,
        };
        let anchor = args
            .branch
            .as_deref()
            .map_or_else(AnchorName::head, AnchorName::branch);
        let requested_repo = args.repo.clone();

        let items = tokio::task::spawn_blocking(move || -> Result<Vec<FindSymbolHit>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let aliases = match requested_repo.as_deref() {
                Some(name) => {
                    let entry = cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
                        Error::InvalidArgument(format!("unknown repo alias: `{name}`"))
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
                    Err(Error::InvalidArgument(_)) => {
                        // The requested anchor doesn't exist in this
                        // store — skip rather than fail the whole
                        // query (= other repos may still have it).
                        continue;
                    }
                    Err(other) => return Err(other),
                };
                for h in hits {
                    out.push(into_wire_hit(&entry.alias, anchor.as_str(), h));
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

fn into_wire_hit(repo: &str, anchor: &str, h: SymbolHit) -> FindSymbolHit {
    let location = format!("{repo}:{anchor}:{}:{}", h.path, h.line);
    FindSymbolHit {
        id: h.id,
        qualified: h.qualified,
        name: h.name,
        kind: h.kind,
        repo: repo.to_string(),
        branch: anchor.to_string(),
        location,
        signature: h.signature,
        // The CAS query layer doesn't yet round-trip the per-fact
        // source-tier tag; default to Syntactic until it does.
        source: SourceTier::Syntactic,
    }
}

fn symbol_kind_to_db(kind: &SymbolKind) -> String {
    match kind {
        SymbolKind::Function => "function".into(),
        SymbolKind::Method => "method".into(),
        SymbolKind::Constructor => "constructor".into(),
        SymbolKind::Getter => "getter".into(),
        SymbolKind::Setter => "setter".into(),
        SymbolKind::Class => "class".into(),
        SymbolKind::Struct => "struct".into(),
        SymbolKind::Enum => "enum".into(),
        SymbolKind::Union => "union".into(),
        SymbolKind::Trait => "trait".into(),
        SymbolKind::Impl => "impl".into(),
        SymbolKind::Interface => "interface".into(),
        SymbolKind::TypeAlias => "type_alias".into(),
        SymbolKind::Field => "field".into(),
        SymbolKind::Property => "property".into(),
        SymbolKind::Constant => "constant".into(),
        SymbolKind::Variable => "variable".into(),
        SymbolKind::Parameter => "parameter".into(),
        SymbolKind::Module => "module".into(),
        SymbolKind::Namespace => "namespace".into(),
        SymbolKind::Package => "package".into(),
        SymbolKind::Macro => "macro".into(),
        SymbolKind::Test => "test".into(),
        SymbolKind::Section => "section".into(),
        SymbolKind::Other(s) => s.clone(),
    }
}
