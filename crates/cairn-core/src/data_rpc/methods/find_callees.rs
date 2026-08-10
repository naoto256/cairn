//! `find_callees` — "what does `name` call?" Thin shortcut over
//! `find_references` with `direction = Outgoing` and `kind = Call`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cairn_proto::common::RefKind;
use cairn_proto::methods::{CallHit, FindCalleesArgs, FindCalleesResult, ReferenceDirection};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::find_callers::into_call_hit;
use super::find_references::{SnippetCache, call_graph_completeness};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
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
            // Drop unresolved call sites: a Call ref that failed to
            // resolve a target isn't a real callee. Covered by the
            // `omits_unresolved_method_calls` test.
            include_noise: false,
            limit: Some(limit_with_probe(effective_limit)),
        };
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();
        let any_omitted_unresolved_calls = Arc::new(AtomicBool::new(false));
        let query_omitted_unresolved_calls = Arc::clone(&any_omitted_unresolved_calls);

        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo,
                anchor: anchor_arg,
                branch: branch_arg,
                method_name: "find_callees",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file: None,
            },
            move |entry, conn, snapshot| {
                let anchor_label = snapshot.anchor.as_str().to_string();
                let worktree_root = PathBuf::from(&entry.root_path);
                let outcome = query::find_references_with_status(conn, &snapshot.anchor, &q)?;
                let omitted_unresolved_calls = outcome.omitted_unresolved_calls;
                query_omitted_unresolved_calls
                    .fetch_or(omitted_unresolved_calls, Ordering::Relaxed);
                let mut snippets = SnippetCache::new(entry.alias.clone(), worktree_root);
                Ok(outcome
                    .hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id.clone();
                        (
                            into_call_hit(&entry.alias, &anchor_label, h, &mut snippets),
                            parser_id,
                            omitted_unresolved_calls,
                        )
                    })
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|(_, parser_id, _)| parser_id.clone())),
            |_out: &mut Vec<(CallHit, String, bool)>| {},
        )
        .await?;
        // Evidence follows surviving rows across cross-repo final trimming.
        // An unresolved-only result has no row to carry provenance, so retain
        // the aggregate bit only when the final result itself is empty.
        let omitted_unresolved_calls = execution.items.iter().any(|(_, _, omitted)| *omitted)
            || (execution.items.is_empty() && any_omitted_unresolved_calls.load(Ordering::Relaxed));
        let items: Vec<_> = execution
            .items
            .into_iter()
            .map(|(item, _, _)| item)
            .collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = call_graph_completeness(
            completeness_for_snapshot_scan(
                execution.capped,
                execution.skipped_unavailable,
                &freshness_issues,
            ),
            omitted_unresolved_calls,
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindCallees,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                // `fuzzy: true` marks fuzzy as already engaged so the
                // shared empty-result hint doesn't suggest a fuzzy
                // toggle that this API doesn't expose. `kind` is inert
                // here (FindCallees' `relax_drop_candidates` is empty)
                // but is set for parity with sister methods.
                fuzzy: true,
                kind: true,
                container: None,
                path: None,
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);

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
    use crate::data_rpc::methods::find_references::FindReferences;
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
    async fn returns_distinct_zero_range_resolved_callees() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub fn first() {}\n\
             pub fn second() {}\n\
             pub fn third() {}\n\
             pub fn caller() { first(); second(); third(); }\n",
        )]);
        let fixture = fixture_from_repo(_repo);

        let result = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "caller", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let names: Vec<_> = result["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["target_name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["first", "second", "third"]);
        assert_eq!(result["completeness"]["status"], "complete");

        let references = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": "caller",
                    "direction": "outgoing",
                    "kind": "call",
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        let reference_names: Vec<_> = references["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["target_name"].as_str().unwrap())
            .collect();
        assert_eq!(reference_names, ["first", "second", "third"]);
        assert_eq!(references["completeness"]["status"], "complete");
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
    async fn omitted_unresolved_calls_make_the_public_call_graph_partial() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn resolved() {}\n\
             pub fn caller(arg: Widget) {\n\
                 resolved();\n\
                 arg.render();\n\
             }\n",
        )]);
        let fixture = fixture_from_repo(_repo);

        let callees = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "caller", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_eq!(callees["items"].as_array().unwrap().len(), 1);
        assert_eq!(callees["items"][0]["target_name"], "resolved");
        assert_eq!(callees["completeness"]["status"], "partial");
        assert_eq!(callees["completeness"]["reason"], "call_graph_unresolved");

        let outgoing_default = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": "caller",
                    "anchor": "HEAD",
                    "direction": "outgoing"
                }),
            )
            .await
            .unwrap();
        assert_eq!(outgoing_default["items"].as_array().unwrap().len(), 1);
        assert_eq!(outgoing_default["completeness"]["status"], "partial");
        assert_eq!(
            outgoing_default["completeness"]["reason"],
            "call_graph_unresolved"
        );

        let outgoing = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": "caller",
                    "anchor": "HEAD",
                    "direction": "outgoing",
                    "include_noise": true
                }),
            )
            .await
            .unwrap();
        assert_eq!(outgoing["completeness"]["status"], "complete");
        assert!(
            outgoing["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| { item["kind"] == "call" && item["target_name"] == "render" })
        );
    }

    #[tokio::test]
    async fn unresolved_only_call_graph_is_empty_and_partial() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn caller(arg: Widget) { arg.render(); }\n",
        )]);
        let fixture = fixture_from_repo(_repo);

        for result in [
            FindCallees
                .dispatch(
                    &fixture.ctx,
                    json!({"repo": "demo", "name": "caller", "anchor": "HEAD"}),
                )
                .await
                .unwrap(),
            FindReferences
                .dispatch(
                    &fixture.ctx,
                    json!({
                        "repo": "demo",
                        "symbol": "caller",
                        "anchor": "HEAD",
                        "direction": "outgoing"
                    }),
                )
                .await
                .unwrap(),
        ] {
            assert!(result["items"].as_array().unwrap().is_empty());
            assert_eq!(result["completeness"]["status"], "partial");
            assert_eq!(result["completeness"]["reason"], "call_graph_unresolved");
        }
    }

    #[tokio::test]
    async fn non_call_noise_does_not_make_the_call_graph_partial() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             pub fn caller(_arg: Widget) {}\n",
        )]);
        let fixture = fixture_from_repo(_repo);

        let result = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "caller", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert!(result["items"].as_array().unwrap().is_empty());
        assert_eq!(result["completeness"]["status"], "complete");
    }

    #[tokio::test]
    async fn unresolved_call_outranks_cap_but_keeps_one_cap_hint() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn first() {}\n\
             pub fn second() {}\n\
             pub fn caller(arg: Widget) { first(); second(); arg.render(); }\n",
        )]);
        let fixture = fixture_from_repo(_repo);

        for result in [
            FindCallees
                .dispatch(
                    &fixture.ctx,
                    json!({"repo": "demo", "name": "caller", "anchor": "HEAD", "limit": 1}),
                )
                .await
                .unwrap(),
            FindReferences
                .dispatch(
                    &fixture.ctx,
                    json!({
                        "repo": "demo",
                        "symbol": "caller",
                        "direction": "outgoing",
                        "kind": "call",
                        "anchor": "HEAD",
                        "limit": 1
                    }),
                )
                .await
                .unwrap(),
        ] {
            assert_eq!(result["items"].as_array().unwrap().len(), 1);
            assert_eq!(result["completeness"]["reason"], "call_graph_unresolved");
            assert_eq!(
                result["hints"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|hint| hint["code"] == "capped_increase_limit")
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn resolved_only_limit_remains_cap_partial() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub fn first() {}\n\
             pub fn second() {}\n\
             pub fn caller() { first(); second(); }\n",
        )]);
        let fixture = fixture_from_repo(_repo);

        let result = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "caller", "anchor": "HEAD", "limit": 1}),
            )
            .await
            .unwrap();
        assert_eq!(result["completeness"]["reason"], "cap");
        assert_eq!(
            result["hints"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|hint| hint["code"] == "capped_increase_limit")
                .count(),
            1
        );
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
                lifecycle: None,
            },
        }
    }
}
