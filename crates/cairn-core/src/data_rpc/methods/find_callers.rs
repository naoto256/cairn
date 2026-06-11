//! `find_callers` — "who calls `name`?" Thin shortcut over
//! `find_references` with `direction = Incoming` and `kind = Call`.

use std::path::PathBuf;

use cairn_proto::common::RefKind;
use cairn_proto::methods::{CallHit, FindCallersArgs, FindCallersResult, ReferenceDirection};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::find_references::SnippetCache;
use crate::data_rpc::helpers::{
    completeness_for_cap, limit_with_probe, tier3_status_for_query, with_one_or_all_stores,
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

        let effective_limit = args.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            symbol: args.name.clone(),
            direction: ReferenceDirection::Incoming,
            kind: Some(RefKind::Call),
            include_noise: false,
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.anchor.clone();
        let branch_arg = args.branch.clone();
        let requested_repo = args.repo.clone();

        let (items, capped) = with_one_or_all_stores(
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
                    .map(|h| into_call_hit(&entry.alias, &anchor_label, h, &mut snippets))
                    .collect())
            },
            |_out: &mut Vec<CallHit>| {},
        )
        .await?;
        let tier3_status = tier3_status_for_query(
            ctx,
            args.repo.clone(),
            args.anchor.clone(),
            args.branch.clone(),
            "find_callers",
        )
        .await?;

        Ok(serde_json::to_value(FindCallersResult {
            items,
            completeness: completeness_for_cap(capped),
            tier3_status,
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
        let (repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub fn resolved() {}\n\
             pub fn caller() { resolved(); }\n",
        )]);
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
