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
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, build_diagnostics, build_hints, completeness_for_cap,
    limit_with_probe, parser_id_filter, tier3_status_for_query, with_one_or_all_stores,
};
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

        let effective_limit = args.pagination.limit.unwrap_or(50).max(1);
        let q = FindSymbolsArgs {
            query: args.query.clone(),
            fuzzy: args.fuzzy,
            kind: args.kind.as_ref().map(symbol_kind_to_str),
            container: args.container.clone(),
            path_prefix: args.path.clone(),
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();
        let signature_only = args.signature_only;

        let (hits, capped) = with_one_or_all_stores(
            ctx,
            requested_repo,
            "find_symbols",
            effective_limit,
            move |entry, conn| {
                let anchor = crate::anchor::resolve_explicit_or_default(
                    conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let anchor_label = anchor.as_str().to_string();
                let hits = query::find_symbols(conn, &anchor, &q)?;
                Ok(hits
                    .into_iter()
                    .map(|hit| (entry.alias.clone(), anchor_label.clone(), hit))
                    .collect())
            },
            |out: &mut Vec<(String, String, SymbolHit)>| {
                out.sort_by(|(repo_a, _, a), (repo_b, _, b)| {
                    language_sort_key(a.language.as_deref())
                        .cmp(&language_sort_key(b.language.as_deref()))
                        .then_with(|| a.path.cmp(&b.path))
                        .then_with(|| a.line.cmp(&b.line))
                        .then_with(|| repo_a.cmp(repo_b))
                        .then_with(|| a.qualified.cmp(&b.qualified))
                });
            },
        )
        .await?;
        let parser_ids = parser_id_filter(hits.iter().map(|(_, _, h)| h.parser_id.clone()));
        let items: Vec<_> = hits
            .into_iter()
            .map(|(repo, anchor_label, h)| into_wire_hit(&repo, &anchor_label, h, signature_only))
            .collect();
        let tier3_status = tier3_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            parser_ids,
            args.tier3.verbose_tier3,
            "find_symbols",
        )
        .await?;
        let completeness = completeness_for_cap(capped);
        let emission_ctx = EmissionContext {
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: args.fuzzy,
                kind: args.kind.is_some(),
                container: args.container.as_deref(),
                path: args.path.as_deref(),
            },
        };
        let diagnostics = build_diagnostics(&emission_ctx);
        let hints = build_hints(&emission_ctx);

        Ok(serde_json::to_value(FindSymbolResult {
            items,
            completeness,
            tier3_status,
            diagnostics,
            hints,
            timing: cairn_proto::Timing::default(),
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
    use crate::data_rpc::helpers::test_support::{
        assert_limit_probe, registered_fixture_with_files,
    };

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
            parser_id: "tree-sitter-rust".into(),
            language: Some("rust".into()),
            source_tier: SourceTier::Semantic,
        };

        let wire = into_wire_hit("demo", "HEAD", hit, false);
        assert_eq!(wire.source, SourceTier::Semantic);
        assert_eq!(wire.language.as_deref(), Some("rust"));
    }

    #[tokio::test]
    async fn find_symbols_empty_result_includes_hints() {
        let fixture = registered_fixture_with_files(&[("README.md", "# Project\n")]);

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "query": "DefinitelyNoSuchSymbol",
                    "limit": 5,
                }),
            )
            .await
            .unwrap();

        assert_eq!(value["items"], json!([]));
        let hint_codes = value["hints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hint| hint["code"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            hint_codes,
            vec!["empty_result_try_fuzzy", "empty_result_widen_scope"]
        );
    }

    #[tokio::test]
    async fn find_symbols_happy_path_omits_envelope_optional_fields() {
        let fixture = registered_fixture_with_files(&[("README.md", "# Project\n")]);

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "query": "Project",
                    "limit": 5,
                }),
            )
            .await
            .unwrap();

        assert!(!value["items"].as_array().unwrap().is_empty());
        assert!(value.get("diagnostics").is_none());
        assert!(value.get("hints").is_none());
    }
}
