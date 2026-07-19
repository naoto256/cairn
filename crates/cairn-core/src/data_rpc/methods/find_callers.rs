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
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::{Error, Result};

pub struct FindCallers;

enum CallerScanItem {
    Hit(Box<CallHit>, String),
    TsxDefinition,
}

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
        let component_name = is_component_name(&args.name).then(|| args.name.clone());

        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo,
                anchor: anchor_arg,
                branch: branch_arg,
                method_name: "find_callers",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file: None,
            },
            move |entry, conn, snapshot| {
                let anchor_label = snapshot.anchor.as_str().to_string();
                let worktree_root = PathBuf::from(&entry.root_path);
                let hits = query::find_references(conn, &snapshot.anchor, &q)?;
                let mut snippets = SnippetCache::new(worktree_root);
                let mut items = hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id.clone();
                        CallerScanItem::Hit(
                            Box::new(into_call_hit(&entry.alias, &anchor_label, h, &mut snippets)),
                            parser_id,
                        )
                    })
                    .collect::<Vec<_>>();
                if items.is_empty()
                    && let Some(name) = component_name.as_deref()
                    && symbol_defined_in_jsx_snapshot(conn, &snapshot.anchor, name)?
                {
                    items.push(CallerScanItem::TsxDefinition);
                }
                Ok(items)
            },
            |items| {
                parser_id_filter(items.iter().filter_map(|item| match item {
                    CallerScanItem::Hit(_, parser_id) => Some(parser_id.clone()),
                    CallerScanItem::TsxDefinition => None,
                }))
            },
            |items: &mut Vec<CallerScanItem>| {
                let mut saw_marker = false;
                items.retain(|item| match item {
                    CallerScanItem::TsxDefinition if saw_marker => false,
                    CallerScanItem::TsxDefinition => {
                        saw_marker = true;
                        true
                    }
                    CallerScanItem::Hit(_, _) => true,
                });
            },
        )
        .await?;
        let tsx_definition = execution
            .items
            .iter()
            .any(|item| matches!(item, CallerScanItem::TsxDefinition));
        let items: Vec<_> = execution
            .items
            .into_iter()
            .filter_map(|item| match item {
                CallerScanItem::Hit(hit, _) => Some(*hit),
                CallerScanItem::TsxDefinition => None,
            })
            .collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = completeness_for_snapshot_scan(
            execution.capped,
            execution.skipped_unavailable,
            &freshness_issues,
        );
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
        let (diagnostics, mut hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);
        if freshness_issues.is_empty() && items.is_empty() && tsx_definition {
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
        kind_source: h.kind_source,
        target_path: h.target_path,
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

fn symbol_defined_in_jsx_snapshot(
    conn: &rusqlite::Connection,
    anchor: &crate::anchor::AnchorName,
    name: &str,
) -> Result<bool> {
    let q = crate::query::FindSymbolsArgs {
        query: Some(name.to_string()),
        fuzzy: false,
        kind: None,
        container: None,
        path_prefix: None,
        limit: Some(20),
    };
    Ok(query::find_symbols(conn, anchor, &q)?
        .into_iter()
        .any(|hit| {
            (hit.name == name || hit.qualified.rsplit("::").next() == Some(name))
                && (hit.path.ends_with(".tsx") || hit.path.ends_with(".jsx"))
        }))
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
                lifecycle: None,
            },
        }
    }
}
