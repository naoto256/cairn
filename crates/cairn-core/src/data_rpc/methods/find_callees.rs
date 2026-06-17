//! `find_callees` — "what does `name` call?" Thin shortcut over
//! `find_references` with `direction = Outgoing` and `kind = Call`.

use std::path::PathBuf;

use cairn_proto::common::RefKind;
use cairn_proto::methods::{CallHit, FindCalleesArgs, FindCalleesResult, ReferenceDirection};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::find_callers::into_call_hit;
use super::find_references::SnippetCache;
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, build_diagnostics, build_hints,
    completeness_for_cap, limit_with_probe, parser_id_filter, tier3_status_for_query,
    with_one_or_all_stores,
};
use crate::query::{self, FindReferencesArgs as QueryArgs};
use crate::{Error, Result};

pub struct FindCallees;

#[async_trait::async_trait]
impl DataMethod for FindCallees {
    fn name(&self) -> &'static str {
        "find_callees"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindCalleesArgs = parse_params(params)?;
        if args.name.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_callees: `name` must be non-empty".into(),
            ));
        }

        let effective_limit = args.pagination.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            symbol: args.name.clone(),
            direction: ReferenceDirection::Outgoing,
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
            "find_callees",
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
        let tier3_status = tier3_status_for_query(
            ctx,
            args.scope.repo.clone(),
            args.scope.anchor.clone(),
            args.scope.branch.clone(),
            parser_ids,
            args.tier3.verbose_tier3,
            "find_callees",
        )
        .await?;
        let completeness = completeness_for_cap(capped);
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindCallees,
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
        let hints = build_hints(&emission_ctx);

        Ok(serde_json::to_value(FindCalleesResult {
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
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindCallees);

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
    async fn returns_resolved_callees() {
        let fixture = call_graph_fixture();
        let result = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "caller", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0]["target_name"], "resolved");
        assert_eq!(items[0]["target_qualified"], "resolved");
        assert_eq!(items[0]["enclosing_qualified"], "caller");
    }

    #[tokio::test]
    async fn omits_unresolved_method_calls() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn resolved() {}\n\
             pub fn caller(arg: Widget) -> Widget {\n\
                 resolved();\n\
                 arg.render();\n\
                 arg\n\
             }\n",
        )]);
        let fixture = fixture_from_repo(_repo);
        let result = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "caller", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["target_name"], "resolved");
    }

    #[tokio::test]
    async fn rejects_empty_name() {
        let fixture = call_graph_fixture();
        let err = FindCallees
            .dispatch(&fixture.ctx, json!({"repo": "demo", "name": ""}))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    struct Fixture {
        _repo: tempfile::TempDir,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn call_graph_fixture() -> Fixture {
        let (repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub fn resolved() {}\n\
             pub fn caller() { resolved(); }\n",
        )]);
        fixture_from_repo(repo)
    }

    fn fixture_from_repo(repo: tempfile::TempDir) -> Fixture {
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
