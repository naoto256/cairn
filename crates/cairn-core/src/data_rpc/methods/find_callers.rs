//! `find_callers` — "who calls `name`?" Thin shortcut over
//! `find_references` with `direction = Incoming` and `kind = Call`.

use std::path::PathBuf;

use cairn_proto::common::{Hint, HintAction, HintCode, RefKind};
use cairn_proto::methods::{CallHit, FindCallersArgs, FindCallersResult, ReferenceDirection};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::find_references::{SnippetCache, qualified_reference_completeness};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::{Error, Result};

pub struct FindCallers;

/// Per-snapshot scan output. `Hit` is a real call site (with its
/// parser id captured for tier-3 status filtering and a flag recording
/// whether the surviving row came from qualified-to-bare fallback).
/// Keeping that provenance on each row prevents a fallback trimmed by
/// the cross-repo limit from downgrading an otherwise exact response.
/// `TsxDefinition`
/// is a sentinel emitted once per snapshot when the queried name has
/// no callers *and* is defined in a `.tsx` / `.jsx` file — JSX
/// component usage (`<Foo />`) records as an `instantiate` ref, not
/// a `call`, so an empty caller result on a JSX component would
/// otherwise mislead. The sentinel survives the finalize dedup and
/// the tail turns it into the `TsxCallersUseInstantiate` hint.
enum CallerScanItem {
    Hit(Box<CallHit>, String, bool),
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
                let outcome = query::find_references_with_status(conn, &snapshot.anchor, &q)?;
                let qualified_fallback = outcome.qualified_fallback;
                let mut snippets = SnippetCache::new(entry.alias.clone(), worktree_root);
                let mut items = outcome
                    .hits
                    .into_iter()
                    .map(|h| {
                        let parser_id = h.parser_id.clone();
                        CallerScanItem::Hit(
                            Box::new(into_call_hit(&entry.alias, &anchor_label, h, &mut snippets)),
                            parser_id,
                            qualified_fallback,
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
                    CallerScanItem::Hit(_, parser_id, _) => Some(parser_id.clone()),
                    CallerScanItem::TsxDefinition => None,
                }))
            },
            |items: &mut Vec<CallerScanItem>| {
                // Collapse per-snapshot TsxDefinition markers so at
                // most one survives across all scanned repos — the
                // downstream hint fires on any positive signal and
                // the marker itself never appears in the wire result.
                let mut saw_marker = false;
                items.retain(|item| match item {
                    CallerScanItem::TsxDefinition if saw_marker => false,
                    CallerScanItem::TsxDefinition => {
                        saw_marker = true;
                        true
                    }
                    CallerScanItem::Hit(_, _, _) => true,
                });
            },
        )
        .await?;
        let tsx_definition = execution
            .items
            .iter()
            .any(|item| matches!(item, CallerScanItem::TsxDefinition));
        let qualified_fallback = execution.items.iter().any(|item| {
            matches!(item, CallerScanItem::Hit(_, _, qualified_fallback) if *qualified_fallback)
        });
        let items: Vec<_> = execution
            .items
            .into_iter()
            .filter_map(|item| match item {
                CallerScanItem::Hit(hit, _, _) => Some(*hit),
                CallerScanItem::TsxDefinition => None,
            })
            .collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = qualified_reference_completeness(
            completeness_for_snapshot_scan(
                execution.capped,
                execution.skipped_unavailable,
                &freshness_issues,
            ),
            qualified_fallback,
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
            // A JSX-component definition was actually found. Strip
            // the generic "relax filter" / "widen scope" advice —
            // both are red herrings for a JSX component — and steer
            // the caller to `find_references kind=instantiate` where
            // JSX usage does appear.
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

/// Uppercase-initial identifier check — the React/JSX convention
/// that distinguishes a component tag (`<Foo />`) from a lowercase
/// HTML element. Used only to gate the TSX-definition probe below;
/// non-JSX callers with uppercase names simply pay one extra lookup
/// that produces no rows.
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

const JSX_SYMBOL_EXISTS_SQL: &str = "SELECT EXISTS (
     SELECT 1
       FROM symbols s
       JOIN manifest_entries me
         ON me.manifest_id = ?1
        AND me.blob_sha = s.blob_sha
      WHERE s.scope = 'top_level'
        AND (
             s.name = ?2
          OR s.qualified = ?2
        )
        AND (
             substr(me.path, -4) = '.tsx'
          OR substr(me.path, -4) = '.jsx'
        )
 )";

/// Ask the pinned snapshot whether `name` is defined in a
/// `.tsx` / `.jsx` file by exact bare or qualified name. This is a
/// complete existence probe: the JSX-usage hint must not depend on
/// an arbitrary symbol-result page size.
fn symbol_defined_in_jsx_snapshot(
    conn: &rusqlite::Connection,
    anchor: &crate::anchor::AnchorName,
    name: &str,
) -> Result<bool> {
    let manifest_id =
        crate::anchor::resolve(conn, anchor)?.ok_or_else(|| Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;
    let exists = conn.query_row(
        JSX_SYMBOL_EXISTS_SQL,
        rusqlite::params![manifest_id.0, name],
        |row| row.get::<_, bool>(0),
    )?;
    Ok(exists)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::data_rpc::methods::find_references::FindReferences;
    use crate::data_rpc::methods::find_symbols::FindSymbols;
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
    async fn qualified_find_symbols_handoff_marks_bare_fallback_partial() {
        let fixture = fixture_from_files(&[
            (
                "src/hello.ts",
                "export class Hello { greet(): void {} }\nexport class Other { greet(): void {} use(): void { this.greet(); } }\n",
            ),
            (
                "src/lib.rs",
                "pub fn greet() {}\npub fn rust_user() { greet(); }\npub fn rust_user_again() { greet(); }\n",
            ),
        ]);

        let symbols = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "query": "greet",
                    "container": "Hello",
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        let qualified = symbols["items"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|item| item["qualified"].as_str())
            .unwrap();
        assert_eq!(qualified, "Hello.greet");

        let callers = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": qualified, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert!(
            !callers["items"].as_array().unwrap().is_empty(),
            "the baseline must prove that unrelated bare-name rows were used"
        );
        let fallback_locations = callers["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["location"].as_str())
            .collect::<Vec<_>>();
        assert!(
            fallback_locations
                .iter()
                .any(|location| location.contains("src/hello.ts")),
            "same-language Other.greet distractor must survive fallback: {callers:#}"
        );
        assert!(
            fallback_locations
                .iter()
                .any(|location| location.contains("src/lib.rs")),
            "cross-language Rust greet distractor must survive fallback: {callers:#}"
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(callers["completeness"].clone()).unwrap(),
            Completeness::partial_semantic("qualified_fallback")
        );

        let bare = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "greet", "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_eq!(bare["items"], callers["items"]);
        assert_eq!(
            serde_json::from_value::<Completeness>(bare["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let empty_qualified = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "name": "Missing.absent",
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert!(empty_qualified["items"].as_array().unwrap().is_empty());
        assert_eq!(
            serde_json::from_value::<Completeness>(empty_qualified["completeness"].clone())
                .unwrap(),
            Completeness::Complete
        );

        let references = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": qualified,
                    "direction": "incoming",
                    "kind": "call",
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert!(
            !references["items"].as_array().unwrap().is_empty(),
            "find_references must exercise the same qualified-to-bare seam"
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(references["completeness"].clone()).unwrap(),
            Completeness::partial_semantic("qualified_fallback")
        );
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
    async fn find_callers_tsx_hint_is_not_capped_by_non_jsx_definitions() {
        let mut owned_files = (0..20)
            .map(|index| {
                (
                    format!("src/decoy_{index:02}.js"),
                    "export function LineageFlow() {}\n".to_string(),
                )
            })
            .collect::<Vec<_>>();
        owned_files.push((
            "src/zz_target.tsx".to_string(),
            "export function LineageFlow() { return <div />; }\n".to_string(),
        ));
        let files = owned_files
            .iter()
            .map(|(path, source)| (path.as_str(), source.as_str()))
            .collect::<Vec<_>>();
        let fixture = fixture_from_files(&files);

        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({"repo": "demo", "name": "LineageFlow", "anchor": "HEAD"}),
            )
            .await
            .unwrap();

        assert!(result["items"].as_array().unwrap().is_empty());
        assert_eq!(result["completeness"]["status"], "complete");
        let hints = result["hints"].as_array().unwrap();
        assert!(
            hints
                .iter()
                .any(|hint| hint["code"] == "tsx_callers_use_instantiate"),
            "expected JSX definition hint after all non-JSX decoys: {hints:?}"
        );
    }

    #[tokio::test]
    async fn find_callers_emits_tsx_hint_for_jsx_definition() {
        let fixture = fixture_from_files(&[(
            "src/App.jsx",
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
        assert!(
            result["hints"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hint| hint["code"] == "tsx_callers_use_instantiate")
        );
    }

    #[tokio::test]
    async fn find_callers_emits_tsx_hint_for_fully_qualified_definition() {
        let fixture = fixture_from_files(&[(
            "src/App.tsx",
            "export class Widgets {\n\
                 static LineageFlow() { return <div />; }\n\
             }\n",
        )]);
        let result = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "name": "Widgets.LineageFlow",
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();

        assert!(result["items"].as_array().unwrap().is_empty());
        assert!(
            result["hints"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hint| hint["code"] == "tsx_callers_use_instantiate")
        );
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

    #[test]
    fn jsx_definition_probe_is_manifest_scoped_without_symbol_table_scan() {
        let data = tempfile::tempdir().unwrap();
        let conn = cas_store::open(&data.path().join("store.db")).unwrap();
        let plan = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {JSX_SYMBOL_EXISTS_SQL}"))
            .unwrap()
            .query_map(rusqlite::params![1_i64, "LineageFlow"], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let joined = plan.join(" | ");

        assert!(
            joined.contains("SEARCH me") && joined.contains("(manifest_id=?)"),
            "probe must start from the pinned manifest: {joined}"
        );
        assert!(
            joined.contains("idx_symbols_blob"),
            "probe must join symbols by manifest blob: {joined}"
        );
        assert!(
            plan.iter().all(|detail| !detail.contains("SCAN s")),
            "probe must not scan the global symbol table: {joined}"
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
