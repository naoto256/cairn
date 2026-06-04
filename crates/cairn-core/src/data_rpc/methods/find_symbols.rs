//! `find_symbols` — anchor → manifest → symbols JOIN, scoped by the
//! caller's `repo` / `branch` / `query` / `kind` / `container` /
//! `path` filters. `repo = None` walks every registered alias.
//!
//! `include_inherited` is not honored yet on this path; it was a
//! Tier-2 layer in the legacy implementation and comes back in a
//! later iteration.

use cairn_proto::methods::{FindSymbolArgs, FindSymbolHit, FindSymbolResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::cas::kind_conv::symbol_kind_to_str;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::data_rpc::helpers::{completeness_for_cap, limit_with_probe, trim_to_requested_limit};
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
        let effective_limit = args.limit.unwrap_or(50).max(1);
        let q = FindSymbolsArgs {
            query: args.query.clone(),
            fuzzy: args.fuzzy,
            kind: args.kind.as_ref().map(symbol_kind_to_str),
            container: args.container.clone(),
            path_prefix: args.path.clone(),
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let requested_repo = args.repo.clone();
        let signature_only = args.signature_only;

        let (items, capped) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<FindSymbolHit>, bool)> {
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

                let mut out: Vec<(String, SymbolHit)> = Vec::new();
                let mut capped = false;
                for entry in aliases {
                    let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                    let conn = cas_store::open(&store_path)?;
                    let mut hits = match query::find_symbols(&conn, &anchor, &q) {
                        Ok(h) => h,
                        Err(Error::AnchorNotFound { .. }) => {
                            // The requested anchor doesn't exist in this
                            // store — skip rather than fail the whole
                            // query (= other repos may still have it).
                            continue;
                        }
                        Err(other) => return Err(other),
                    };
                    capped |= trim_to_requested_limit(&mut hits, effective_limit);
                    for h in hits {
                        out.push((entry.alias.clone(), h));
                    }
                }
                out.sort_by(|(repo_a, a), (repo_b, b)| {
                    language_sort_key(a.language.as_deref())
                        .cmp(&language_sort_key(b.language.as_deref()))
                        .then_with(|| a.path.cmp(&b.path))
                        .then_with(|| a.line.cmp(&b.line))
                        .then_with(|| repo_a.cmp(repo_b))
                        .then_with(|| a.qualified.cmp(&b.qualified))
                });
                capped |= trim_to_requested_limit(&mut out, effective_limit);
                let items = out
                    .into_iter()
                    .map(|(repo, h)| into_wire_hit(&repo, anchor.as_str(), h, signature_only))
                    .collect();
                Ok((items, capped))
            })
            .await
            .map_err(|e| Error::InvalidArgument(format!("find_symbols task panicked: {e}")))??;

        Ok(serde_json::to_value(FindSymbolResult {
            items,
            completeness: completeness_for_cap(capped),
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
        language: h.language,
        // `signature_only=true` drops the heaviest field. The naming
        // mirrors `GetSymbolSourceArgs.signature_only`; here the
        // analogous "minimal navigation payload" is everything *but*
        // the signature.
        signature: if signature_only { None } else { h.signature },
        source: h.source_tier,
    }
}

fn language_sort_key(language: Option<&str>) -> (bool, &str) {
    match language {
        Some(lang) => (false, lang),
        None => (true, ""),
    }
}

#[cfg(test)]
mod tests {
    use cairn_proto::common::{SourceTier, SymbolKind};
    use serde_json::json;

    use super::*;
    use crate::data_rpc::helpers::test_support::assert_limit_probe;

    #[tokio::test]
    async fn exact_limit_is_complete_and_over_limit_is_partial() {
        assert_limit_probe(
            &FindSymbols,
            json!({"repo": "demo", "kind": "struct", "limit": 3}),
            json!({"repo": "demo", "kind": "struct", "limit": 2}),
        )
        .await;
    }

    #[test]
    fn wire_hit_preserves_query_source_tier() {
        let hit = SymbolHit {
            id: 1,
            name: "semantic_fn".into(),
            qualified: "semantic_fn".into(),
            kind: SymbolKind::Function,
            signature: None,
            visibility: None,
            path: "src/lib.rs".into(),
            line: 1,
            blob_sha: "sha".into(),
            language: Some("rust".into()),
            source_tier: SourceTier::Semantic,
        };

        let wire = into_wire_hit("demo", "HEAD", hit, false);
        assert_eq!(wire.source, SourceTier::Semantic);
        assert_eq!(wire.language.as_deref(), Some("rust"));
    }
}
