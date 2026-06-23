//! `find_callers` — "who calls `name`?" Thin shortcut over
//! `find_references` with `direction = Incoming` and `kind = Call`.

use std::path::PathBuf;

use cairn_proto::common::{Hint, HintAction, HintCode, RefKind};
use cairn_proto::methods::{CallHit, FindCallersArgs, FindCallersResult, ReferenceDirection};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::find_references::SnippetCache;
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, build_diagnostics, build_hints,
    completeness_for_cap, limit_with_probe, parser_id_filter, tier_status_for_query,
    with_one_or_all_stores,
};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::{Error, Result};

pub struct FindCallers;

#[async_trait::async_trait]
impl DataMethod for FindCallers {
    fn name(&self) -> &'static str {
        "find_callers"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindCallersArgs = parse_params(params)?;
        if args.name.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_callers: `name` must be non-empty".into(),
            ));
        }

        let effective_limit = args.pagination.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            symbol: args.name.clone(),
            direction: ReferenceDirection::Incoming,
            kind: Some(RefKind::Call),
            include_noise: false,
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();

        let (hits, capped) = with_one_or_all_stores(
            ctx,
            requested_repo,
            "find_callers",
            effective_limit,
            move |entry, conn| {
                let anchor = crate::anchor::resolve_explicit_or_default(
                    conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let anchor_label = anchor.as_str().to_string();
                let worktree_root = PathBuf::from(&entry.root_path);
                let hits = query::find_references(conn, &anchor, &q)?;
                let mut snippets = SnippetCache::new(worktree_root);
                Ok(hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id.clone();
                        (
                            into_call_hit(&entry.alias, &anchor_label, h, &mut snippets),
                            parser_id,
                        )
                    })
                    .collect())
            },
            |_out: &mut Vec<(CallHit, String)>| {},
        )
        .await?;
        let parser_ids = parser_id_filter(hits.iter().map(|(_, parser_id)| parser_id.clone()));
        let items: Vec<_> = hits.into_iter().map(|(item, _)| item).collect();
        let tier3_status = tier_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            parser_ids,
            args.tier3.verbose_tier3,
            "find_callers",
        )
        .await?;
        let completeness = completeness_for_cap(capped);
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindCallers,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: true,
                kind: true,
                container: None,
                path: None,
                ..QueryArgsView::default()
            },
        };
        let diagnostics = build_diagnostics(&emission_ctx);
        let mut hints = build_hints(&emission_ctx);
        if items.is_empty()
            && is_component_name(&args.name)
            && symbol_defined_in_jsx_file(
                ctx,
                args.scope.repo.clone(),
                args.scope.anchor.clone(),
                args.scope.branch.clone(),
                args.name.clone(),
            )
            .await?
        {
            hints.retain(|hint| {
                !matches!(
                    hint.code,
                    HintCode::EmptyResultRelaxFilter | HintCode::EmptyResultWidenScope
                )
            });
            hints.push(tsx_component_usage_hint());
        }

        Ok(serde_json::to_value(FindCallersResult {
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
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindCallers);

pub(super) fn into_call_hit(
    repo: &str,
    anchor: &str,
    h: ReferenceHit,
    snippets: &mut SnippetCache,
) -> CallHit {
    let location = format!("{repo}:{anchor}:{}:{}", h.path, h.line);
    let snippet = snippets.line_for(&h.blob_sha, &h.path, h.line);
    CallHit {
        target_name: h.target_name,
        target_qualified: h.target_qualified,
        enclosing_qualified: h.enclosing_qualified,
        branch: anchor.to_string(),
        location,
        snippet,
    }
}

fn is_component_name(name: &str) -> bool {
    name.chars().next().is_some_and(char::is_uppercase)
}

fn tsx_component_usage_hint() -> Hint {
    Hint {
        code: HintCode::TsxCallersUseInstantiate,
        message: "JSX component usage doesn't show in find_callers; use find_references kind=instantiate.".into(),
        action: Some(HintAction::TryAlternativeQuery),
        tool: Some("find_references".into()),
        params: Some(serde_json::json!({ "kind": "instantiate" })),
        drop_params: Vec::new(),
        target: None,
    }
}

async fn symbol_defined_in_jsx_file(
    ctx: &DataCtx,
    requested_repo: Option<String>,
    anchor_arg: Option<String>,
    branch_arg: Option<String>,
    name: String,
) -> Result<bool> {
    let q = crate::query::FindSymbolsArgs {
        query: Some(name.clone()),
        fuzzy: false,
        kind: None,
        container: None,
        path_prefix: None,
        limit: Some(20),
    };
    let (matches, _) = with_one_or_all_stores(
        ctx,
        requested_repo,
        "find_callers tsx hint",
        1,
        move |_entry, conn| {
            let anchor = crate::anchor::resolve_explicit_or_default(
                conn,
                anchor_arg.as_deref(),
                branch_arg.as_deref(),
            )?;
            let hits = query::find_symbols(conn, &anchor, &q)?;
            Ok(hits
                .into_iter()
                .filter(|hit| {
                    (hit.name == name || hit.qualified.rsplit("::").next() == Some(name.as_str()))
                        && (hit.path.ends_with(".tsx") || hit.path.ends_with(".jsx"))
                })
                .map(|_| ())
                .collect())
        },
        |_out: &mut Vec<()>| {},
    )
    .await?;
    Ok(!matches.is_empty())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[tokio::test]
    async fn returns_resolved_call_sites_only() {
        let fixture = call_graph_fixture();
        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "resolved", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let items = result["items"].as_array().unwrap();
        assert!(
            items
                .iter()
                .any(|h| h["enclosing_qualified"] == "caller" && h["target_name"] == "resolved"),
            "callers of resolved should include caller: {items:?}"
        );
        assert!(items.iter().all(|h| h["target_qualified"].is_string()));
    }

    #[tokio::test]
    async fn returns_empty_for_unknown_symbol() {
        let fixture = call_graph_fixture();
        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "does_not_exist", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert!(result["items"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn find_callers_emits_tsx_hint_when_symbol_is_tsx_component() {
        let fixture = fixture_from_files(&[(
            "src/App.tsx",
            "export function LineageFlow() { return <div />; }\n",
        )]);
        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "LineageFlow", "anchor": "HEAD"}),
            )
            .await
            .unwrap();

        assert!(result["items"].as_array().unwrap().is_empty());
        let hints = result["hints"].as_array().unwrap();
        assert!(hints.iter().any(|hint| {
            hint["code"] == "tsx_callers_use_instantiate"
                && hint["tool"] == "find_references"
                && hint["params"] == json!({"kind": "instantiate"})
        }));
        assert!(hints.iter().all(|hint| {
            hint["code"] != "empty_result_relax_filter"
                && hint["code"] != "empty_result_widen_scope"
        }));
    }

    #[tokio::test]
    async fn find_callers_does_not_emit_tsx_hint_when_lowercase_symbol() {
        let fixture = fixture_from_files(&[(
            "src/App.tsx",
            "export function lineWidget() { return <div />; }\n",
        )]);
        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "lineWidget", "anchor": "HEAD"}),
            )
            .await
            .unwrap();

        assert!(result["items"].as_array().unwrap().is_empty());
        let hints = result["hints"].as_array().unwrap();
        assert!(
            hints
                .iter()
                .all(|hint| hint["code"] != "tsx_callers_use_instantiate")
        );
    }

    #[tokio::test]
    async fn find_callers_does_not_emit_tsx_hint_when_definition_not_tsx() {
        let fixture = fixture_from_files(&[("src/app.ts", "export function LineageFlow() {}\n")]);
        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "LineageFlow", "anchor": "HEAD"}),
            )
            .await
            .unwrap();

        assert!(result["items"].as_array().unwrap().is_empty());
        let hints = result["hints"].as_array().unwrap();
        assert!(
            hints
                .iter()
                .all(|hint| hint["code"] != "tsx_callers_use_instantiate")
        );
    }

    #[tokio::test]
    async fn rejects_empty_name() {
        let fixture = call_graph_fixture();
        let err = FindCallers
            .dispatch(&fixture.ctx, json!({"repo": "demo", "name": ""}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    pub(super) struct Fixture {
        _repo: tempfile::TempDir,
        _data: tempfile::TempDir,
        pub(super) ctx: DataCtx,
    }

    pub(super) fn call_graph_fixture() -> Fixture {
        fixture_from_files(&[(
            "src/lib.rs",
            "pub fn resolved() {}\n\
             pub fn caller() { resolved(); }\n",
        )])
    }

    fn fixture_from_files(files: &[(&str, &str)]) -> Fixture {
        let (repo, _sha) = init_repo(files);
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        let canonical = std::fs::canonicalize(repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas.store_db_path(&repo_hash);
        let mut store = cas_store::open(&store_path).unwrap();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        register_repo(&mut store, &canonical, now_ns).unwrap();
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "demo",
            &canonical.to_string_lossy(),
            &repo_hash,
            now_ns,
        )
        .unwrap();
        tx.commit().unwrap();

        Fixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
            },
        }
    }
}
