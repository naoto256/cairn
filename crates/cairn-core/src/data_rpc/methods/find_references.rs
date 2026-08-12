//! `find_references` — either way: refs that target a symbol
//! (incoming, default) or refs inside a symbol's body (outgoing).
//! Reads the CAS `refs` table scoped by the resolved anchor.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cairn_proto::Completeness;
use cairn_proto::common::PartialReason;
use cairn_proto::methods::{
    FindReferenceHit, FindReferencesArgs, FindReferencesResult, ReferenceDirection,
};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
};
use crate::query::{self, FindReferencesArgs as QueryArgs, ReferenceHit};
use crate::register::load_blob_or_verified_worktree;
use crate::{Error, Result};

pub struct FindReferences;

#[async_trait::async_trait]
impl DataMethod for FindReferences {
    fn name(&self) -> &'static str {
        "find_references"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindReferencesArgs = parse_params(params)?;
        if args.symbol.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_references: `symbol` must be non-empty".into(),
            ));
        }

        let effective_limit = args.pagination.limit.unwrap_or(100).max(1);
        let q = QueryArgs {
            symbol: args.symbol.clone(),
            direction: args.direction,
            kind: args.kind,
            include_noise: args.include_noise,
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
                method_name: "find_references",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file: None,
            },
            move |entry, conn, snapshot| {
                let anchor_label = snapshot.anchor.as_str().to_string();
                let worktree_root = PathBuf::from(&entry.root_path);
                let outcome = query::find_references_with_status(conn, &snapshot.anchor, &q)?;
                let qualified_fallback = outcome.qualified_fallback;
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
                            into_wire_hit(&entry.alias, &anchor_label, h, &mut snippets),
                            parser_id,
                            qualified_fallback,
                            omitted_unresolved_calls,
                        )
                    })
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|(_, parser_id, _, _)| parser_id.clone())),
            |_out: &mut Vec<(FindReferenceHit, String, bool, bool)>| {},
        )
        .await?;
        let qualified_fallback = execution
            .items
            .iter()
            .any(|(_, _, qualified_fallback, _)| *qualified_fallback);
        // Evidence follows surviving rows across cross-repo final trimming.
        // An unresolved-only result has no row to carry provenance, so retain
        // the aggregate bit only when the final result itself is empty.
        let omitted_unresolved_calls = execution.items.iter().any(|(_, _, _, omitted)| *omitted)
            || (execution.items.is_empty() && any_omitted_unresolved_calls.load(Ordering::Relaxed));
        let items: Vec<_> = execution
            .items
            .into_iter()
            .map(|(item, _, _, _)| item)
            .collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = call_graph_completeness(
            qualified_reference_completeness(
                completeness_for_snapshot_scan(
                    execution.capped,
                    execution.skipped_unavailable,
                    &freshness_issues,
                ),
                qualified_fallback,
            ),
            omitted_unresolved_calls,
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                // `fuzzy: true` suppresses the try-fuzzy empty-result
                // hint: `find_references` matches by symbol name
                // without a fuzzy toggle to flip. `direction` is
                // reported true only when the caller picked a
                // non-`Incoming` direction, which lets the shared
                // relax-filter hint suggest dropping it.
                fuzzy: true,
                kind: args.kind.is_some(),
                container: None,
                path: None,
                direction: args.direction != ReferenceDirection::Incoming,
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);

        Ok(serde_json::to_value(FindReferencesResult {
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

pub(super) fn qualified_reference_completeness(
    completeness: Completeness,
    qualified_fallback: bool,
) -> Completeness {
    if !qualified_fallback {
        return completeness;
    }

    // Snapshot freshness and unavailable-repository evidence remain stronger
    // than query-local uncertainty. A cap is weaker: the returned rows came
    // from a bare-name fallback, so callers must not mistake them for exact
    // qualified matches. The independent cap hint still reports truncation.
    if matches!(completeness, Completeness::Complete)
        || matches!(
            completeness,
            Completeness::Partial {
                reason: Some(PartialReason::Cap),
                ..
            }
        )
    {
        Completeness::partial_semantic("qualified_fallback")
    } else {
        completeness
    }
}

pub(super) fn call_graph_completeness(
    completeness: Completeness,
    omitted_unresolved_calls: bool,
) -> Completeness {
    if !omitted_unresolved_calls {
        return completeness;
    }

    // Snapshot freshness and unavailable-repository evidence outrank local
    // semantic uncertainty. An actual omitted call outranks a cap; the cap
    // remains independently visible through its exactly-once hint.
    if matches!(completeness, Completeness::Complete)
        || matches!(
            completeness,
            Completeness::Partial {
                reason: Some(PartialReason::Cap),
                ..
            }
        )
    {
        Completeness::partial_semantic("call_graph_unresolved")
    } else {
        completeness
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindReferences);

fn into_wire_hit(
    repo: &str,
    anchor: &str,
    h: ReferenceHit,
    snippets: &mut SnippetCache,
) -> FindReferenceHit {
    let location = format!("{repo}:{anchor}:{}:{}", h.path, h.line);
    let snippet = snippets.line_for(&h.blob_sha, &h.path, h.line);
    FindReferenceHit {
        target_name: h.target_name,
        target_qualified: h.target_qualified,
        kind: h.kind,
        kind_source: h.kind_source,
        target_path: h.target_path,
        enclosing_qualified: h.enclosing_qualified,
        branch: anchor.to_string(),
        location,
        snippet,
    }
}

#[cfg(test)]
mod phase_tests {
    #[cfg(unix)]
    use std::io::{BufRead, BufReader, Write};
    #[cfg(unix)]
    use std::os::unix::process::CommandExt;
    #[cfg(unix)]
    use std::process::{Command, Stdio};
    #[cfg(unix)]
    use std::sync::mpsc;
    #[cfg(unix)]
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use rusqlite::Connection;
    use serde_json::json;

    use super::{DataMethod, FindReferences};
    use crate::anchor::{self, AnchorName};
    use crate::cas::store as cas_store;
    use crate::data_rpc::helpers::{
        CapturedQueryKind, CapturedQuerySql, QueryPhase, QueryPhaseObserver, TestQueryVariant,
        test_support, with_query_phase_observer,
    };
    use crate::paths::path_hash;

    const RETURN_LIMIT: usize = 100;

    struct FixtureCardinality {
        input_refs: usize,
        strict_candidates: usize,
        bare_candidates: usize,
        dedupe_before: usize,
        dedupe_after: usize,
        db_bytes: u64,
        page_bytes: u64,
        schema_version: i64,
        manifest_id: i64,
    }

    fn repeated_call_source(count: usize) -> String {
        let mut source = String::from("pub fn target() {}\npub fn caller() {\n");
        for _ in 0..count {
            source.push_str("    crate::target();\n");
        }
        source.push_str("}\n");
        source
    }

    fn fixture_store(fixture: &test_support::DataRpcFixture) -> Connection {
        let canonical = std::fs::canonicalize(fixture._repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        cas_store::open_existing(&fixture.ctx.cas_data_dir.store_db_path(&repo_hash)).unwrap()
    }

    fn scalar_usize(conn: &Connection, sql: &str) -> usize {
        usize::try_from(conn.query_row(sql, [], |row| row.get::<_, i64>(0)).unwrap()).unwrap()
    }

    fn fixture_cardinality(
        fixture: &test_support::DataRpcFixture,
        conn: &Connection,
    ) -> FixtureCardinality {
        let input_refs = scalar_usize(conn, "SELECT COUNT(*) FROM refs");
        let strict_candidates = scalar_usize(
            conn,
            "SELECT COUNT(*) FROM refs WHERE target_qualified = 'crate::target'",
        );
        let bare_candidates = scalar_usize(
            conn,
            "SELECT COUNT(*) FROM refs WHERE target_name = 'target'",
        );
        let dedupe_before = bare_candidates;
        let dedupe_after = usize::try_from(
            conn.query_row(
                &crate::query::logical_site_distinct_count_sql(),
                ["target"],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        )
        .unwrap();
        let page_count = scalar_usize(conn, "PRAGMA page_count");
        let page_size = scalar_usize(conn, "PRAGMA page_size");
        let schema_version = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        let manifest_id = anchor::resolve(conn, &AnchorName::head())
            .unwrap()
            .expect("HEAD manifest missing")
            .0;
        let canonical = std::fs::canonicalize(fixture._repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let db_bytes = std::fs::metadata(fixture.ctx.cas_data_dir.store_db_path(&repo_hash))
            .unwrap()
            .len();
        FixtureCardinality {
            input_refs,
            strict_candidates,
            bare_candidates,
            dedupe_before,
            dedupe_after,
            db_bytes,
            page_bytes: u64::try_from(page_count * page_size).unwrap(),
            schema_version,
            manifest_id,
        }
    }

    async fn measured_dispatch(
        fixture: &test_support::DataRpcFixture,
        trace_id: &'static str,
        symbol: &str,
    ) -> (serde_json::Value, std::sync::Arc<QueryPhaseObserver>) {
        let observer = QueryPhaseObserver::new(trace_id, 0);
        let started = Instant::now();
        let result = with_query_phase_observer(
            observer.clone(),
            FindReferences.dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": symbol,
                    "direction": "incoming",
                    "kind": "call",
                    "anchor": "HEAD",
                    "limit": RETURN_LIMIT
                }),
            ),
        )
        .await
        .unwrap();
        let rows = result["items"].as_array().unwrap().len();
        observer.record(
            QueryPhase::DispatchTotal,
            started.elapsed(),
            Some(rows),
            Some(rows),
            None,
            true,
        );
        (result, observer)
    }

    async fn measured_dispatch_variant(
        fixture: &test_support::DataRpcFixture,
        trace_id: &'static str,
        symbol: &str,
        variant: TestQueryVariant,
    ) -> (serde_json::Value, std::sync::Arc<QueryPhaseObserver>) {
        let observer = QueryPhaseObserver::with_variant(trace_id, 0, variant);
        let started = Instant::now();
        let result = with_query_phase_observer(
            observer.clone(),
            FindReferences.dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": symbol,
                    "direction": "incoming",
                    "kind": "call",
                    "anchor": "HEAD",
                    "limit": RETURN_LIMIT
                }),
            ),
        )
        .await
        .unwrap();
        let rows = result["items"].as_array().unwrap().len();
        observer.record(
            QueryPhase::DispatchTotal,
            started.elapsed(),
            Some(rows),
            Some(rows),
            None,
            true,
        );
        (result, observer)
    }

    async fn run_cardinality_case(
        scale: &str,
        refs: usize,
        trace_id: &'static str,
        symbol: &str,
        expects_fallback: bool,
    ) {
        let source = repeated_call_source(refs);
        let build_started = Instant::now();
        let fixture = test_support::registered_fixture_with_files(&[("src/lib.rs", &source)]);
        let build_elapsed = build_started.elapsed();
        let conn = fixture_store(&fixture);
        let cardinality = fixture_cardinality(&fixture, &conn);
        assert_eq!(cardinality.strict_candidates, refs);
        assert_eq!(cardinality.bare_candidates, refs);
        assert_eq!(cardinality.dedupe_before, refs);
        assert_eq!(cardinality.dedupe_after, refs);

        let (result, observer) = measured_dispatch(&fixture, trace_id, symbol).await;
        let expected_output = refs.min(RETURN_LIMIT);
        let expected_sql_rows = refs.min(RETURN_LIMIT + 1);
        assert_eq!(result["items"].as_array().unwrap().len(), expected_output);
        let events = observer.events();
        if expects_fallback {
            assert!(
                events
                    .iter()
                    .any(|event| event.phase == QueryPhase::StrictSql && event.rows == Some(0))
            );
            assert!(events.iter().any(|event| {
                event.phase == QueryPhase::FallbackSql && event.rows == Some(expected_sql_rows)
            }));
        } else {
            assert!(events.iter().any(|event| {
                event.phase == QueryPhase::StrictSql && event.rows == Some(expected_sql_rows)
            }));
            assert!(
                !events
                    .iter()
                    .any(|event| event.phase == QueryPhase::FallbackSql)
            );
        }

        eprintln!(
            "CARDINALITY scale={scale} trace_id={trace_id} generated={refs} input_refs={} strict_candidates={} bare_candidates={} dedupe_before={} dedupe_after={} returned={expected_output} return_limit={RETURN_LIMIT} db_bytes={} page_bytes={} schema_version={} manifest_id={} fixture_build={build_elapsed:?}",
            cardinality.input_refs,
            cardinality.strict_candidates,
            cardinality.bare_candidates,
            cardinality.dedupe_before,
            cardinality.dedupe_after,
            cardinality.db_bytes,
            cardinality.page_bytes,
            cardinality.schema_version,
            cardinality.manifest_id,
        );
        for event in events {
            eprintln!("CARDINALITY_PHASE scale={scale} {event:?}");
        }
    }

    #[tokio::test]
    async fn actual_dispatch_records_single_query_phase_matrix() {
        let fixture = test_support::registered_fixture();
        let observer = QueryPhaseObserver::new("single-direct-find-references", 0);
        let dispatch_started = Instant::now();
        let result = with_query_phase_observer(
            observer.clone(),
            FindReferences.dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": "missing::target",
                    "direction": "incoming",
                    "kind": "call",
                    "anchor": "HEAD"
                }),
            ),
        )
        .await
        .unwrap();

        assert_eq!(result["items"].as_array().unwrap().len(), 3);
        observer.record(
            QueryPhase::DispatchTotal,
            dispatch_started.elapsed(),
            Some(3),
            Some(3),
            None,
            true,
        );
        let events = observer.events();
        for event in &events {
            eprintln!("{event:?}");
            assert_eq!(event.trace_id, "single-direct-find-references");
            assert_eq!(event.connection_ordinal, 0);
        }

        let strict = events
            .iter()
            .find(|event| event.phase == QueryPhase::StrictSql)
            .expect("strict SQL phase missing");
        assert_eq!(strict.rows, Some(0));
        let fallback = events
            .iter()
            .find(|event| event.phase == QueryPhase::FallbackSql)
            .expect("fallback SQL phase missing");
        assert_eq!(fallback.rows, Some(3));

        for required in [
            QueryPhase::IndexOpen,
            QueryPhase::AliasResolution,
            QueryPhase::LeaseAcquire,
            QueryPhase::SnapshotEvaluate,
            QueryPhase::Membership,
            QueryPhase::Commit,
            QueryPhase::Finalize,
            QueryPhase::LeaseRelease,
            QueryPhase::TierFreshnessSecondPass,
            QueryPhase::DispatchTotal,
        ] {
            assert!(
                events.iter().any(|event| event.phase == required),
                "phase missing: {required:?}"
            );
        }
    }

    #[tokio::test]
    async fn default_observer_matches_unobserved_production_carry() {
        let fixture = test_support::registered_fixture();
        let params = || {
            json!({
                "repo": "demo",
                "symbol": "missing::target",
                "direction": "incoming",
                "kind": "call",
                "anchor": "HEAD",
                "limit": RETURN_LIMIT
            })
        };
        let unobserved = FindReferences
            .dispatch(&fixture.ctx, params())
            .await
            .unwrap();
        let (default_result, default_observer) =
            measured_dispatch(&fixture, "default-observer-carry", "missing::target").await;
        let (explicit_result, explicit_observer) = measured_dispatch_variant(
            &fixture,
            "explicit-observer-carry",
            "missing::target",
            TestQueryVariant::ProductionCarry,
        )
        .await;

        assert_eq!(unobserved, default_result);
        assert_eq!(default_result, explicit_result);
        let default_sql = default_observer.captured_sql();
        let explicit_sql = explicit_observer.captured_sql();
        assert_eq!(default_sql, explicit_sql);
        assert_eq!(default_sql.len(), 2);
        for query in default_sql {
            assert_eq!(query.sql.matches("FROM refs t").count(), 1);
            assert_eq!(query.sql.matches("EXISTS (\n").count(), 1);
        }
    }

    #[tokio::test]
    #[ignore = "requires CAIRN_STAGE2_GOLDEN_DIR pre-refactor artifacts"]
    async fn baseline_sql_and_binds_match_parent_golden_bytes() {
        let golden_dir = std::env::var("CAIRN_STAGE2_GOLDEN_DIR")
            .expect("CAIRN_STAGE2_GOLDEN_DIR must name the mode-0700 capture root");
        let fixture = test_support::registered_fixture();
        let (_, observer) = measured_dispatch_variant(
            &fixture,
            "baseline-parent-golden",
            "missing::target",
            TestQueryVariant::RejoinOracle,
        )
        .await;
        let captured = observer.captured_sql();
        assert_eq!(captured.len(), 2);
        for query in captured {
            let stem = match query.kind {
                CapturedQueryKind::Strict => {
                    assert!(matches!(
                        query.params.as_slice(),
                        [
                            rusqlite::types::Value::Integer(_),
                            rusqlite::types::Value::Text(symbol),
                            rusqlite::types::Value::Text(kind)
                        ] if symbol == "missing::target" && kind == "call"
                    ));
                    "strict"
                }
                CapturedQueryKind::Fallback => {
                    assert!(matches!(
                        query.params.as_slice(),
                        [
                            rusqlite::types::Value::Integer(_),
                            rusqlite::types::Value::Text(symbol),
                            rusqlite::types::Value::Text(kind)
                        ] if symbol == "target" && kind == "call"
                    ));
                    "fallback"
                }
            };
            let golden_sql = std::fs::read(format!("{golden_dir}/{stem}.sql")).unwrap();
            let golden_binds = std::fs::read(format!("{golden_dir}/{stem}.binds")).unwrap();
            assert_eq!(query.sql.as_bytes(), golden_sql.as_slice());
            assert_eq!(
                format!("{:#?}\n", query.params).as_bytes(),
                golden_binds.as_slice()
            );
        }
    }

    #[tokio::test]
    #[ignore = "writes candidate SQL authority to CAIRN_STAGE2_CANDIDATE_DIR"]
    async fn capture_production_candidate_sql_and_binds() {
        let output_dir = std::env::var("CAIRN_STAGE2_CANDIDATE_DIR")
            .expect("CAIRN_STAGE2_CANDIDATE_DIR must name a mode-0700 task root");
        let fixture = test_support::registered_fixture();
        let (_, observer) = measured_dispatch_variant(
            &fixture,
            "production-candidate-authority",
            "missing::target",
            TestQueryVariant::ProductionCarry,
        )
        .await;
        let captured = observer.captured_sql();
        assert_eq!(captured.len(), 2);
        for query in captured {
            let stem = match query.kind {
                CapturedQueryKind::Strict => "strict",
                CapturedQueryKind::Fallback => "fallback",
            };
            assert_eq!(
                query.sql.matches("strict_refs (").count(),
                usize::from(matches!(query.kind, CapturedQueryKind::Strict))
            );
            assert_eq!(query.sql.matches("FROM refs t").count(), 1);
            std::fs::write(format!("{output_dir}/{stem}.sql"), query.sql.as_bytes()).unwrap();
            std::fs::write(
                format!("{output_dir}/{stem}.binds"),
                format!("{:#?}\n", query.params).as_bytes(),
            )
            .unwrap();
        }
    }

    #[tokio::test]
    async fn actual_dispatch_profiles_cardinality_scale_1() {
        run_cardinality_case("1", 1, "cardinality-1-strict", "crate::target", false).await;
        run_cardinality_case("1", 1, "cardinality-1-fallback", "missing::target", true).await;
    }

    #[tokio::test]
    async fn actual_dispatch_profiles_cardinality_scale_16_and_up() {
        for (scale, refs, strict_trace, fallback_trace) in [
            (
                "16",
                16usize,
                "cardinality-16-strict",
                "cardinality-16-fallback",
            ),
            (
                "256",
                256usize,
                "cardinality-256-strict",
                "cardinality-256-fallback",
            ),
            (
                "4096",
                4_096usize,
                "cardinality-4096-strict",
                "cardinality-4096-fallback",
            ),
            (
                "16384",
                16_384usize,
                "cardinality-16384-strict",
                "cardinality-16384-fallback",
            ),
        ] {
            run_cardinality_case(scale, refs, strict_trace, "crate::target", false).await;
            run_cardinality_case(scale, refs, fallback_trace, "missing::target", true).await;
        }
    }

    fn query_plan(conn: &Connection, captured: &CapturedQuerySql) -> Vec<(i64, i64, String)> {
        let sql = format!("EXPLAIN QUERY PLAN {}", captured.sql);
        conn.prepare(&sql)
            .unwrap()
            .query_map(rusqlite::params_from_iter(captured.params.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn explain_opcode_counts(
        conn: &Connection,
        captured: &CapturedQuerySql,
    ) -> std::collections::BTreeMap<String, usize> {
        let sql = format!("EXPLAIN {}", captured.sql);
        let opcodes = conn
            .prepare(&sql)
            .unwrap()
            .query_map(rusqlite::params_from_iter(captured.params.iter()), |row| {
                row.get::<_, String>(1)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let mut counts = std::collections::BTreeMap::new();
        for opcode in opcodes {
            *counts.entry(opcode).or_insert(0) += 1;
        }
        counts
    }

    #[derive(Debug)]
    struct ExplainOpcode {
        address: i64,
        opcode: String,
        p1: i64,
        p2: i64,
        p3: i64,
        p4: Option<String>,
        p5: i64,
    }

    fn explain_opcodes(conn: &Connection, captured: &CapturedQuerySql) -> Vec<ExplainOpcode> {
        let sql = format!("EXPLAIN {}", captured.sql);
        conn.prepare(&sql)
            .unwrap()
            .query_map(rusqlite::params_from_iter(captured.params.iter()), |row| {
                Ok(ExplainOpcode {
                    address: row.get(0)?,
                    opcode: row.get(1)?,
                    p1: row.get(2)?,
                    p2: row.get(3)?,
                    p3: row.get(4)?,
                    p4: row.get(5)?,
                    p5: row.get(6)?,
                })
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn cte_prefix(sql: &str) -> &str {
        let marker = "SELECT target_name, target_qualified, kind, enclosing,";
        let offset = sql
            .rfind(marker)
            .expect("production final SELECT marker missing");
        &sql[..offset]
    }

    fn cte_counts(conn: &Connection, captured: &CapturedQuerySql) -> Vec<i64> {
        let sql = format!(
            "{} SELECT
                 (SELECT COUNT(*) FROM best_resolution),
                 (SELECT COUNT(*) FROM ref_candidates),
                 (SELECT COUNT(*) FROM ranked_refs),
                 (SELECT COUNT(*) FROM ranked_refs WHERE dedup_rank = 1),
                 (SELECT COUNT(*) FROM ranked_refs
                   WHERE dedup_rank = 1
                     AND source_rank > 0
                     AND byte_start = 0 AND byte_end = 0
                     AND has_workspace_tier_same_line_target_name),
                 (SELECT COUNT(*) FROM surviving_refs)",
            cte_prefix(&captured.sql)
        );
        conn.query_row(
            &sql,
            rusqlite::params_from_iter(captured.params.iter()),
            |row| (0..6).map(|index| row.get(index)).collect(),
        )
        .unwrap()
    }

    async fn capture_strict_sql(
        fixture: &test_support::DataRpcFixture,
        trace_id: &'static str,
        variant: TestQueryVariant,
    ) -> CapturedQuerySql {
        let observer = QueryPhaseObserver::capture_sql_only(trace_id, 0, variant);
        let error = with_query_phase_observer(
            observer.clone(),
            FindReferences.dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": "crate::target",
                    "direction": "incoming",
                    "kind": "call",
                    "anchor": "HEAD",
                    "limit": RETURN_LIMIT
                }),
            ),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("test SQL capture only"));
        let captured = observer.captured_sql();
        assert_eq!(captured.len(), 1);
        let captured = captured.into_iter().next().unwrap();
        assert_eq!(captured.kind, CapturedQueryKind::Strict);
        assert_eq!(captured.params.len(), 3);
        captured
    }

    const V14_BOUNDARY_NAMES: [&str; 9] = [
        "best_resolution",
        "branch_a",
        "branch_b_with_best_resolution",
        "strict_union",
        "ref_candidates_pre_membership",
        "membership_projection_v14",
        "ranked_window",
        "surviving",
        "final_order_limit",
    ];

    struct BoundaryResult {
        rows: i64,
        sites: i64,
        query_elapsed: Duration,
        metadata_elapsed: Duration,
        plan: Vec<(i64, i64, String)>,
        opcode_count: usize,
    }

    fn v14_boundary_sql(
        captured: &CapturedQuerySql,
        scale: usize,
        label: &str,
    ) -> (String, i64, i64) {
        let prefix = cte_prefix(&captured.sql);
        let strict_marker = "         strict_refs AS (\n";
        let refs_marker = "         ref_candidates AS (\n";
        assert_eq!(prefix.matches(strict_marker).count(), 1);
        assert_eq!(prefix.matches(refs_marker).count(), 1);
        let strict_start = prefix.find(strict_marker).unwrap();
        let refs_start = prefix.find(refs_marker).unwrap();
        let strict_with = &prefix[..strict_start];
        let strict_body_with_tail = &prefix[strict_start + strict_marker.len()..refs_start];
        let strict_body = strict_body_with_tail
            .strip_suffix("),\n")
            .expect("strict_refs canonical close marker missing");
        let union_marker = "\n             UNION ALL\n";
        assert_eq!(strict_body.matches(union_marker).count(), 1);
        let (branch_a, branch_b) = strict_body.split_once(union_marker).unwrap();

        let n = i64::try_from(scale).unwrap();
        match label {
            "best_resolution" => (
                format!("{prefix} SELECT COUNT(*) FROM best_resolution"),
                n,
                0,
            ),
            "branch_a" => (
                format!("{strict_with} probe AS ({branch_a}) SELECT COUNT(*) FROM probe"),
                n,
                0,
            ),
            "branch_b_with_best_resolution" => (
                format!("{strict_with} probe AS ({branch_b}) SELECT COUNT(*) FROM probe"),
                n,
                0,
            ),
            "strict_union" => (
                format!("{prefix} SELECT COUNT(*) FROM strict_refs"),
                n * 2,
                0,
            ),
            "ref_candidates_pre_membership" => (
                format!("{prefix} SELECT COUNT(*) FROM ref_candidates"),
                n * 2,
                0,
            ),
            "membership_projection_v14" => (
                format!(
                    "{prefix} SELECT COALESCE(SUM(CASE WHEN has_workspace_tier_same_line_target_name THEN 1 ELSE 0 END), 0) FROM ref_candidates"
                ),
                n,
                n,
            ),
            "ranked_window" => (
                format!("{prefix} SELECT COUNT(*) FROM ranked_refs"),
                n * 2,
                0,
            ),
            "surviving" => (
                format!("{prefix} SELECT COUNT(*) FROM surviving_refs"),
                n * 2,
                0,
            ),
            "final_order_limit" => (
                format!(
                    "{prefix} SELECT COUNT(*) FROM (SELECT ref_id FROM surviving_refs WHERE target_qualified = ?2 ORDER BY path, line, byte_start, source_rank, ref_id LIMIT 101)"
                ),
                101,
                0,
            ),
            _ => panic!("unknown v14 boundary {label}"),
        }
    }

    fn run_v14_boundary_probe(
        conn: &Connection,
        captured: &CapturedQuerySql,
        scale: usize,
        label: &str,
    ) -> BoundaryResult {
        let (sql, expected, sites) = v14_boundary_sql(captured, scale, label);
        let mut stmt = conn.prepare(&sql).unwrap();
        let bind_count = stmt.parameter_count();
        assert!(bind_count <= captured.params.len());
        let params = &captured.params[..bind_count];
        let started = Instant::now();
        let rows: i64 = stmt
            .query_row(rusqlite::params_from_iter(params.iter()), |row| row.get(0))
            .unwrap();
        let query_elapsed = started.elapsed();
        assert_eq!(rows, expected, "boundary {label} cardinality drift");
        let metadata_started = Instant::now();
        let plan_sql = format!("EXPLAIN QUERY PLAN {sql}");
        let plan = conn
            .prepare(&plan_sql)
            .unwrap()
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let explain_sql = format!("EXPLAIN {sql}");
        let opcode_count = conn
            .prepare(&explain_sql)
            .unwrap()
            .query_map(rusqlite::params_from_iter(params.iter()), |_| Ok(()))
            .unwrap()
            .count();
        BoundaryResult {
            rows,
            sites,
            query_elapsed,
            metadata_elapsed: metadata_started.elapsed(),
            plan,
            opcode_count,
        }
    }

    fn plan_node_descends_from(
        plan: &[(i64, i64, String)],
        node_id: i64,
        ancestor_id: i64,
    ) -> bool {
        let mut current = node_id;
        while let Some((_, parent, _)) = plan.iter().find(|(id, _, _)| *id == current) {
            if *parent == ancestor_id {
                return true;
            }
            if *parent == current || *parent == 0 {
                return false;
            }
            current = *parent;
        }
        false
    }

    fn print_metadata(label: &str, fixture: &test_support::DataRpcFixture, conn: &Connection) {
        let sqlite_version: String = conn
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .unwrap();
        let compile_options = conn
            .prepare("PRAGMA compile_options")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let indexes = conn
            .prepare(
                "SELECT name, sql FROM sqlite_master
                  WHERE type = 'index' AND tbl_name IN ('refs','resolutions','symbols')
                  ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let stat1_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='sqlite_stat1')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let canonical = std::fs::canonicalize(fixture._repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let db_bytes = std::fs::metadata(fixture.ctx.cas_data_dir.store_db_path(&repo_hash))
            .unwrap()
            .len();
        eprintln!(
            "SQL_METADATA label={label} sqlite={sqlite_version} rusqlite={} schema_version={} db_bytes={db_bytes} stat1_exists={stat1_exists} compile_options={compile_options:?} indexes={indexes:?}",
            rusqlite::version(),
            scalar_usize(conn, "PRAGMA user_version"),
        );
    }

    async fn capture_baseline_sql(
        refs: usize,
        trace_id: &'static str,
        symbol: &str,
    ) -> (test_support::DataRpcFixture, CapturedQuerySql) {
        let source = repeated_call_source(refs);
        let fixture = test_support::registered_fixture_with_files(&[("src/lib.rs", &source)]);
        let (_, observer) = measured_dispatch(&fixture, trace_id, symbol).await;
        let captured = observer
            .captured_sql()
            .into_iter()
            .find(|captured| {
                matches!(
                    (captured.kind, symbol),
                    (CapturedQueryKind::Strict, "crate::target")
                        | (CapturedQueryKind::Fallback, "missing::target")
                )
            })
            .expect("expected production SQL capture missing");
        (fixture, captured)
    }

    #[tokio::test]
    async fn diagnose_strict_and_fallback_query_plan_and_cardinality() {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            let (strict_fixture, strict) =
                capture_baseline_sql(4_096, "diagnostic-4096-strict", "crate::target").await;
            let strict_conn = fixture_store(&strict_fixture);
            print_metadata("4096-strict", &strict_fixture, &strict_conn);
            eprintln!(
                "SQL_EQP label=4096-strict plan={:?}",
                query_plan(&strict_conn, &strict)
            );
            eprintln!(
                "SQL_EXPLAIN label=4096-strict opcodes={:?}",
                explain_opcode_counts(&strict_conn, &strict)
            );
            eprintln!(
                "SQL_CTE label=4096-strict counts={:?}",
                cte_counts(&strict_conn, &strict)
            );

            let (fallback_fixture, fallback) =
                capture_baseline_sql(4_096, "diagnostic-4096-fallback", "missing::target").await;
            let fallback_conn = fixture_store(&fallback_fixture);
            print_metadata("4096-fallback", &fallback_fixture, &fallback_conn);
            eprintln!(
                "SQL_EQP label=4096-fallback plan={:?}",
                query_plan(&fallback_conn, &fallback)
            );
            eprintln!(
                "SQL_EXPLAIN label=4096-fallback opcodes={:?}",
                explain_opcode_counts(&fallback_conn, &fallback)
            );
            eprintln!(
                "SQL_CTE label=4096-fallback counts={:?}",
                cte_counts(&fallback_conn, &fallback)
            );

            let source = repeated_call_source(16_384);
            let fixture = test_support::registered_fixture_with_files(&[("src/lib.rs", &source)]);
            let conn = fixture_store(&fixture);
            print_metadata("16384-plan-only", &fixture, &conn);
            eprintln!(
                "SQL_EQP label=16384-strict plan={:?}",
                query_plan(&conn, &strict)
            );
            eprintln!(
                "SQL_EQP label=16384-fallback plan={:?}",
                query_plan(&conn, &fallback)
            );
            let cardinality = fixture_cardinality(&fixture, &conn);
            eprintln!(
                "SQL_CHEAP_COUNTS label=16384 input={} strict={} bare={} logical={}",
                cardinality.input_refs,
                cardinality.strict_candidates,
                cardinality.bare_candidates,
                cardinality.dedupe_after
            );
        })
        .await
        .expect("SQL diagnostic exceeded 60 seconds");
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_truth_ref(
        conn: &Connection,
        blob_sha: &str,
        parser_id: &str,
        enclosing_id: Option<i64>,
        target_name: &str,
        target_qualified: &str,
        kind: &str,
        line: i64,
        source: &str,
    ) {
        insert_truth_ref_with_range(
            conn,
            blob_sha,
            parser_id,
            enclosing_id,
            target_name,
            target_qualified,
            kind,
            0,
            0,
            line,
            source,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_truth_ref_with_range(
        conn: &Connection,
        blob_sha: &str,
        parser_id: &str,
        enclosing_id: Option<i64>,
        target_name: &str,
        target_qualified: &str,
        kind: &str,
        byte_start: i64,
        byte_end: i64,
        line: i64,
        source: &str,
    ) {
        conn.execute(
            "INSERT INTO refs
                 (blob_sha, parser_id, enclosing_id, target_name, target_qualified,
                  kind, byte_start, byte_end, line, source)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params![
                blob_sha,
                parser_id,
                enclosing_id,
                target_name,
                target_qualified,
                kind,
                byte_start,
                byte_end,
                line,
                source
            ],
        )
        .unwrap();
    }

    type FixtureReadyBarrier =
        Box<dyn FnOnce(&std::path::Path, &CapturedQuerySql, usize, usize, usize, u64) -> bool>;

    async fn run_strict_branch_b_diagnostic(
        scale: usize,
        trace_id: &'static str,
        query_bound: Duration,
        inject_resolution_site_index: bool,
        ready_barrier: Option<FixtureReadyBarrier>,
    ) {
        run_strict_branch_b_diagnostic_variant(
            scale,
            trace_id,
            query_bound,
            inject_resolution_site_index,
            ready_barrier,
            TestQueryVariant::RejoinOracle,
        )
        .await;
    }

    async fn run_strict_branch_b_diagnostic_variant(
        scale: usize,
        trace_id: &'static str,
        query_bound: Duration,
        inject_resolution_site_index: bool,
        ready_barrier: Option<FixtureReadyBarrier>,
        variant: TestQueryVariant,
    ) {
        let fixture_started = Instant::now();
        let source = (0..(scale * 2 + 2))
            .map(|line| format!("// diagnostic line {line}\n"))
            .collect::<String>();
        let fixture = test_support::registered_fixture_with_files(&[("src/lib.rs", &source)]);
        let mut conn = fixture_store(&fixture);
        let (manifest_id, blob_sha, parser_id): (i64, String, String) = conn
            .query_row(
                "SELECT me.manifest_id, me.blob_sha, b.parser_id
                   FROM manifest_entries me
                   JOIN blobs b ON b.blob_sha = me.blob_sha
                  WHERE me.path = 'src/lib.rs' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        conn.execute("DELETE FROM resolutions", []).unwrap();
        conn.execute("DELETE FROM refs", []).unwrap();
        conn.execute("DELETE FROM symbols", []).unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, parent_id, name, qualified, kind, signature,
                visibility, doc, byte_start, byte_end, line_start, line_end,
                body_start, source)
             VALUES (?1, ?2, NULL, 'target', 'crate::target', 'function', NULL,
                     NULL, NULL, 0, 1, 1, 1, NULL, 'rust-syn')",
            rusqlite::params![blob_sha, parser_id],
        )
        .unwrap();
        let target_symbol_id = conn.last_insert_rowid();

        let tx = conn.transaction().unwrap();
        for ordinal in 0..scale {
            let a_line = i64::try_from(ordinal * 2 + 1).unwrap();
            let b_line = i64::try_from(ordinal * 2 + 2).unwrap();
            let a_start = i64::try_from(ordinal * 6 + 1).unwrap();
            let b_start = i64::try_from(ordinal * 6 + 3).unwrap();
            let ws_start = i64::try_from(ordinal * 6 + 5).unwrap();
            insert_truth_ref_with_range(
                &tx,
                &blob_sha,
                &parser_id,
                None,
                "target",
                "crate::target",
                "call",
                a_start,
                a_start + 1,
                a_line,
                "tier2-direct-branch-a",
            );
            insert_truth_ref_with_range(
                &tx,
                &blob_sha,
                &parser_id,
                None,
                "target",
                "syntactic::target",
                "call",
                b_start,
                b_start + 1,
                b_line,
                "tier2-direct-branch-b",
            );
            tx.execute(
                "INSERT INTO resolutions
                   (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                    kind, semantic_kind, target_symbol_id, source, target_path,
                    manifest_id)
                 VALUES (?1, ?2, ?3, ?4, 'call', NULL, ?5,
                         'tier25-branch-b-diagnostic', NULL, ?6)",
                rusqlite::params![
                    blob_sha,
                    parser_id,
                    b_start,
                    b_start + 1,
                    target_symbol_id,
                    manifest_id
                ],
            )
            .unwrap();
            insert_truth_ref_with_range(
                &tx,
                &blob_sha,
                &parser_id,
                None,
                "target",
                "workspace-only::target",
                "call",
                ws_start,
                ws_start + 1,
                b_line,
                "tier3-branch-b-seed",
            );
        }
        tx.commit().unwrap();

        let index_build = if inject_resolution_site_index {
            let pages: i64 = conn
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .unwrap();
            let canonical = std::fs::canonicalize(fixture._repo.path()).unwrap();
            let store_path = fixture
                .ctx
                .cas_data_dir
                .store_db_path(&path_hash(&canonical));
            let bytes = std::fs::metadata(&store_path).unwrap().len();
            let xinfo = conn
                .prepare("PRAGMA index_xinfo('idx_refs_resolution_site')")
                .unwrap()
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                xinfo,
                vec![
                    (0, 1, Some("blob_sha".into()), "BINARY".into(), 1),
                    (1, 2, Some("parser_id".into()), "BINARY".into(), 1),
                    (2, 8, Some("byte_start".into()), "BINARY".into(), 1),
                    (3, 9, Some("byte_end".into()), "BINARY".into(), 1),
                    (4, 6, Some("kind".into()), "BINARY".into(), 1),
                    (5, -1, None, "BINARY".into(), 0),
                ]
            );
            Some((Duration::ZERO, pages, pages, bytes, bytes))
        } else {
            None
        };

        let scalar = |sql: &str| -> usize {
            usize::try_from(conn.query_row(sql, [], |row| row.get::<_, i64>(0)).unwrap()).unwrap()
        };
        assert_eq!(
            scalar("SELECT COUNT(*) FROM refs WHERE target_qualified='crate::target'"),
            scale
        );
        assert_eq!(
            scalar(
                "SELECT COUNT(*) FROM refs r
                  JOIN resolutions res
                    ON res.site_blob_sha=r.blob_sha
                   AND res.site_parser_id=r.parser_id
                   AND res.site_byte_start=r.byte_start
                   AND res.site_byte_end=r.byte_end
                   AND res.kind=r.kind
                  JOIN symbols sym ON sym.id=res.target_symbol_id
                 WHERE sym.qualified='crate::target'
                   AND r.target_qualified<>'crate::target'"
            ),
            scale
        );
        assert_eq!(
            scalar(
                "SELECT COUNT(*) FROM refs r
                  WHERE r.source='tier3-branch-b-seed'
                    AND r.target_qualified='workspace-only::target'
                    AND NOT EXISTS (
                      SELECT 1 FROM resolutions res
                       WHERE res.site_blob_sha=r.blob_sha
                         AND res.site_parser_id=r.parser_id
                         AND res.site_byte_start=r.byte_start
                         AND res.site_byte_end=r.byte_end
                         AND res.kind=r.kind)"
            ),
            scale
        );
        let canonical = std::fs::canonicalize(fixture._repo.path()).unwrap();
        let store_path = fixture
            .ctx
            .cas_data_dir
            .store_db_path(&path_hash(&canonical));
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let db_bytes = std::fs::metadata(&store_path).unwrap().len();
        let fixture_build_elapsed = fixture_started.elapsed();
        if let Some(barrier) = ready_barrier {
            let captured = capture_strict_sql(&fixture, trace_id, variant).await;
            if !barrier(&store_path, &captured, scale, scale, scale, db_bytes) {
                return;
            }
        }

        let query_started = Instant::now();
        let (result, observer) = tokio::time::timeout(
            query_bound,
            measured_dispatch_variant(&fixture, trace_id, "crate::target", variant),
        )
        .await
        .expect("strict Branch-B diagnostic exceeded 60 seconds");
        let query_elapsed = query_started.elapsed();
        let strict_sql_elapsed = observer
            .events()
            .into_iter()
            .find(|event| event.phase == QueryPhase::StrictSql)
            .expect("strict Branch-B SQL phase missing")
            .elapsed;
        let captured = observer.captured_sql();
        assert_eq!(captured.len(), 1);
        let strict = &captured[0];
        assert_eq!(strict.kind, CapturedQueryKind::Strict);
        assert!(matches!(
            strict.params.as_slice(),
            [
                rusqlite::types::Value::Integer(id),
                rusqlite::types::Value::Text(symbol),
                rusqlite::types::Value::Text(kind)
            ] if *id == manifest_id && symbol == "crate::target" && kind == "call"
        ));
        let counts = cte_counts(&conn, strict);
        assert_eq!(usize::try_from(counts[1]).unwrap(), scale * 2);
        let branch_b_surviving_sql = format!(
            "{} SELECT COUNT(*) FROM surviving_refs
                WHERE source='tier2-direct-branch-b'",
            cte_prefix(&strict.sql)
        );
        let branch_b_surviving: i64 = conn
            .query_row(
                &branch_b_surviving_sql,
                rusqlite::params_from_iter(strict.params.iter()),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(branch_b_surviving, i64::try_from(scale).unwrap());
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), RETURN_LIMIT);
        assert!(items.iter().any(|item| {
            item["location"]
                .as_str()
                .and_then(|location| location.rsplit(':').next())
                .and_then(|line| line.parse::<usize>().ok())
                .is_some_and(|line| line % 2 == 0)
        }));
        assert!(items.iter().any(|item| {
            item["location"]
                .as_str()
                .and_then(|location| location.rsplit(':').next())
                .and_then(|line| line.parse::<usize>().ok())
                .is_some_and(|line| line % 2 == 1)
        }));

        let plan = query_plan(&conn, strict);
        emit_raw_eqp(trace_id, PerfSeries::StrictHit, &plan);
        let opcodes = explain_opcodes(&conn, strict);
        let opcode_rows = opcodes
            .iter()
            .map(|row| {
                format!(
                    "addr={} opcode={} p1={} p2={} p3={} p4={:?} p5={}",
                    row.address, row.opcode, row.p1, row.p2, row.p3, row.p4, row.p5
                )
            })
            .collect::<Vec<_>>();
        eprintln!(
            "BRANCH_B_EXPLAIN scale={scale} opcode_count={} classification=UNKNOWN opcodes={opcode_rows:?}",
            opcodes.len()
        );
        assert_v14_correlated_membership_plan(PerfSeries::StrictHit, &plan);
        if inject_resolution_site_index {
            if variant == TestQueryVariant::ProductionCarry {
                assert_strict_payload_carry_strict_index_plan(&plan);
            } else {
                assert_resolution_site_index_plan(&plan);
            }
        }
        eprintln!(
            "BRANCH_B_DIAGNOSTIC scale={scale} fixture_build={fixture_build_elapsed:?} strict_sql={strict_sql_elapsed:?} dispatch_total={query_elapsed:?} test_total={:?} strict_a={scale} strict_b={scale} workspace_seeds={scale} ref_candidates={} branch_b_surviving={branch_b_surviving} returned={}",
            fixture_started.elapsed(),
            counts[1],
            items.len()
        );
        if let Some((elapsed, before_pages, after_pages, before_bytes, after_bytes)) = index_build {
            eprintln!(
                "RESOLUTION_SITE_INDEX scale={scale} build={elapsed:?} pages_before={before_pages} pages_after={after_pages} bytes_before={before_bytes} bytes_after={after_bytes} integrity=ok"
            );
        }
    }

    #[tokio::test]
    async fn strict_branch_b_diagnostic_256() {
        run_strict_branch_b_diagnostic(
            256,
            "strict-branch-b-256",
            Duration::from_secs(60),
            false,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn strict_branch_b_diagnostic_4096() {
        run_strict_branch_b_diagnostic(
            4_096,
            "strict-branch-b-4096",
            Duration::from_secs(60),
            false,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn v14_operator_localization_full_256() {
        run_strict_branch_b_diagnostic(
            256,
            "v14-operator-full-256",
            Duration::from_secs(30),
            false,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn v14_operator_localization_full_512() {
        run_strict_branch_b_diagnostic(
            512,
            "v14-operator-full-512",
            Duration::from_secs(30),
            false,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn v14_operator_localization_full_1024() {
        run_strict_branch_b_diagnostic(
            1_024,
            "v14-operator-full-1024",
            Duration::from_secs(30),
            false,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn v14_operator_localization_full_2048() {
        run_strict_branch_b_diagnostic(
            2_048,
            "v14-operator-full-2048",
            Duration::from_secs(30),
            false,
            None,
        )
        .await;
    }

    async fn run_resolution_site_index_case(scale: usize, trace_id: &'static str, bound: u64) {
        run_strict_branch_b_diagnostic(scale, trace_id, Duration::from_secs(bound), true, None)
            .await;
    }

    #[tokio::test]
    async fn resolution_site_index_full_256() {
        run_resolution_site_index_case(256, "resolution-site-256", 30).await;
    }

    #[tokio::test]
    async fn resolution_site_index_full_512() {
        run_resolution_site_index_case(512, "resolution-site-512", 30).await;
    }

    #[tokio::test]
    async fn resolution_site_index_full_1024() {
        run_resolution_site_index_case(1_024, "resolution-site-1024", 30).await;
    }

    #[tokio::test]
    async fn resolution_site_index_full_2048() {
        run_resolution_site_index_case(2_048, "resolution-site-2048", 30).await;
    }

    #[tokio::test]
    async fn resolution_site_index_full_4096() {
        run_resolution_site_index_case(4_096, "resolution-site-4096", 60).await;
    }

    async fn run_strict_payload_carry_case(scale: usize, trace_id: &'static str, bound: u64) {
        run_strict_branch_b_diagnostic_variant(
            scale,
            trace_id,
            Duration::from_secs(bound),
            true,
            None,
            TestQueryVariant::ProductionCarry,
        )
        .await;
    }

    #[tokio::test]
    async fn strict_payload_carry_full_256() {
        run_strict_payload_carry_case(256, "strict-payload-carry-256", 30).await;
    }

    #[tokio::test]
    async fn strict_payload_carry_full_512() {
        run_strict_payload_carry_case(512, "strict-payload-carry-512", 30).await;
    }

    #[tokio::test]
    async fn strict_payload_carry_full_1024() {
        run_strict_payload_carry_case(1_024, "strict-payload-carry-1024", 30).await;
    }

    #[tokio::test]
    async fn strict_payload_carry_full_2048() {
        run_strict_payload_carry_case(2_048, "strict-payload-carry-2048", 30).await;
    }

    #[tokio::test]
    async fn strict_payload_carry_full_4096() {
        run_strict_payload_carry_case(4_096, "strict-payload-carry-4096", 60).await;
    }

    #[tokio::test]
    async fn strict_payload_carry_full_16384() {
        run_strict_payload_carry_case(16_384, "strict-payload-carry-16384", 60).await;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ExistingPathAuditSeries {
        BareFallback,
        Outgoing,
    }

    async fn run_existing_path_audit(
        series: ExistingPathAuditSeries,
        scale: usize,
        trace_id: &'static str,
        bound: Duration,
    ) {
        let fixture_started = Instant::now();
        let source = (0..(scale + 32))
            .map(|line| format!("// existing path audit {line}\n"))
            .collect::<String>();
        let fixture = test_support::registered_fixture_with_files(&[("src/lib.rs", &source)]);
        let mut conn = fixture_store(&fixture);
        let (manifest_id, blob_sha, parser_id): (i64, String, String) = conn
            .query_row(
                "SELECT me.manifest_id, me.blob_sha, b.parser_id
                   FROM manifest_entries me
                   JOIN blobs b ON b.blob_sha=me.blob_sha
                  WHERE me.path='src/lib.rs' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        conn.execute("DELETE FROM resolutions", []).unwrap();
        conn.execute("DELETE FROM refs", []).unwrap();
        conn.execute("DELETE FROM symbols", []).unwrap();
        let insert_symbol = |conn: &Connection, name: &str, qualified: &str, start: i64| {
            conn.execute(
                "INSERT INTO symbols
                   (blob_sha, parser_id, parent_id, name, qualified, kind, signature,
                    visibility, doc, byte_start, byte_end, line_start, line_end,
                    body_start, source)
                 VALUES (?1,?2,NULL,?3,?4,'function',NULL,NULL,NULL,?5,?6,1,1,NULL,'rust-syn')",
                rusqlite::params![blob_sha, parser_id, name, qualified, start, start + 1],
            )
            .unwrap();
            conn.last_insert_rowid()
        };
        let target_symbol_id = insert_symbol(&conn, "target", "crate::target", 0);
        let caller_symbol_id = insert_symbol(&conn, "caller", "crate::caller", 2);
        let tx = conn.transaction().unwrap();
        let insert_resolution_pair = |tx: &rusqlite::Transaction<'_>, start: i64| {
            for (source, path, scoped_manifest) in [
                ("tier3-audit-global", "global.rs", None),
                ("tier25-audit-manifest", "manifest.rs", Some(manifest_id)),
            ] {
                tx.execute(
                    "INSERT INTO resolutions
                       (site_blob_sha,site_parser_id,site_byte_start,site_byte_end,kind,
                        semantic_kind,target_symbol_id,source,target_path,manifest_id)
                     VALUES (?1,?2,?3,?4,'call',NULL,?5,?6,?7,?8)",
                    rusqlite::params![
                        blob_sha,
                        parser_id,
                        start,
                        start + 1,
                        target_symbol_id,
                        source,
                        path,
                        scoped_manifest
                    ],
                )
                .unwrap();
            }
        };
        match series {
            ExistingPathAuditSeries::BareFallback => {
                for ordinal in 0..scale {
                    let start = i64::try_from(ordinal * 2 + 10).unwrap();
                    insert_truth_ref_with_range(
                        &tx,
                        &blob_sha,
                        &parser_id,
                        None,
                        "target",
                        "crate::target",
                        "call",
                        start,
                        start + 1,
                        i64::try_from(ordinal + 1).unwrap(),
                        "tier2-fallback-audit",
                    );
                    if ordinal % 2 == 0 {
                        insert_resolution_pair(&tx, start);
                    }
                }
                for ordinal in 0..8 {
                    insert_truth_ref_with_range(
                        &tx,
                        &blob_sha,
                        &parser_id,
                        None,
                        "audit_noise",
                        "crate::audit_noise",
                        "call",
                        50_000 + ordinal,
                        50_001 + ordinal,
                        50_000 + ordinal,
                        "tier2-noise",
                    );
                }
            }
            ExistingPathAuditSeries::Outgoing => {
                for ordinal in 0..scale {
                    let start = i64::try_from(ordinal * 2 + 10).unwrap();
                    insert_truth_ref_with_range(
                        &tx,
                        &blob_sha,
                        &parser_id,
                        Some(caller_symbol_id),
                        "target",
                        "",
                        "call",
                        start,
                        start + 1,
                        i64::try_from(ordinal + 1).unwrap(),
                        "tier2-outgoing-audit",
                    );
                    insert_resolution_pair(&tx, start);
                }
                for ordinal in 0..8 {
                    let start = 60_000 + ordinal * 2;
                    insert_truth_ref_with_range(
                        &tx,
                        &blob_sha,
                        &parser_id,
                        Some(caller_symbol_id),
                        "unresolved",
                        "",
                        "call",
                        start,
                        start + 1,
                        60_000 + ordinal,
                        "tier2-unresolved-audit",
                    );
                    insert_truth_ref_with_range(
                        &tx,
                        &blob_sha,
                        &parser_id,
                        Some(caller_symbol_id),
                        "type_noise",
                        "crate::type_noise",
                        "type",
                        start + 1,
                        start + 2,
                        70_000 + ordinal,
                        "tier2-type-noise",
                    );
                }
            }
        }
        tx.commit().unwrap();
        let index_elapsed = Duration::ZERO;
        let relevant = match series {
            ExistingPathAuditSeries::BareFallback => conn
                .query_row(
                    "SELECT COUNT(*) FROM refs WHERE target_name='target' AND kind='call'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            ExistingPathAuditSeries::Outgoing => conn
                .query_row(
                    "SELECT COUNT(*) FROM refs WHERE enclosing_id=?1 AND kind='call' AND target_name='target'",
                    [caller_symbol_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
        };
        assert_eq!(usize::try_from(relevant).unwrap(), scale);
        let (resolved_sites, unresolved_calls, type_noise) = match series {
            ExistingPathAuditSeries::BareFallback => (
                conn.query_row(
                    "SELECT COUNT(DISTINCT site_byte_start) FROM resolutions",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                conn.query_row(
                    "SELECT COUNT(*) FROM refs r
                      WHERE r.target_name='target'
                        AND NOT EXISTS (
                          SELECT 1 FROM resolutions res
                           WHERE res.site_blob_sha=r.blob_sha
                             AND res.site_parser_id=r.parser_id
                             AND res.site_byte_start=r.byte_start
                             AND res.site_byte_end=r.byte_end
                             AND res.kind=r.kind)",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                0,
            ),
            ExistingPathAuditSeries::Outgoing => (
                conn.query_row(
                    "SELECT COUNT(DISTINCT site_byte_start) FROM resolutions",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                conn.query_row(
                    "SELECT COUNT(*) FROM refs
                      WHERE enclosing_id=?1 AND kind='call' AND target_name='unresolved'",
                    [caller_symbol_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                conn.query_row(
                    "SELECT COUNT(*) FROM refs
                      WHERE enclosing_id=?1 AND kind='type' AND target_name='type_noise'",
                    [caller_symbol_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            ),
        };
        match series {
            ExistingPathAuditSeries::BareFallback => {
                assert_eq!(usize::try_from(resolved_sites).unwrap(), scale.div_ceil(2));
                assert_eq!(usize::try_from(unresolved_calls).unwrap(), scale / 2);
                assert_eq!(type_noise, 0);
            }
            ExistingPathAuditSeries::Outgoing => {
                assert_eq!(usize::try_from(resolved_sites).unwrap(), scale);
                assert_eq!((unresolved_calls, type_noise), (8, 8));
            }
        }
        let fixture_elapsed = fixture_started.elapsed();
        drop(conn);

        let observer =
            QueryPhaseObserver::with_variant(trace_id, 0, TestQueryVariant::ProductionCarry);
        let params = match series {
            ExistingPathAuditSeries::BareFallback => json!({
                "repo":"demo", "symbol":"missing::target", "direction":"incoming",
                "kind":"call", "anchor":"HEAD", "include_noise":false,
                "limit":RETURN_LIMIT
            }),
            ExistingPathAuditSeries::Outgoing => json!({
                "repo":"demo", "symbol":"crate::caller", "direction":"outgoing",
                "anchor":"HEAD", "include_noise":false, "limit":RETURN_LIMIT
            }),
        };
        let query_started = Instant::now();
        let result = tokio::time::timeout(
            bound,
            with_query_phase_observer(
                observer.clone(),
                FindReferences.dispatch(&fixture.ctx, params),
            ),
        )
        .await
        .expect("existing-path audit query exceeded its statement bound")
        .unwrap();
        let query_elapsed = query_started.elapsed();
        let events = observer.events();
        let fallback = events
            .iter()
            .find(|event| event.phase == QueryPhase::FallbackSql)
            .expect("fallback SQL phase missing");
        assert_eq!(fallback.rows, Some(scale.min(RETURN_LIMIT + 1)));
        if series == ExistingPathAuditSeries::BareFallback {
            assert!(
                events
                    .iter()
                    .any(|event| { event.phase == QueryPhase::StrictSql && event.rows == Some(0) })
            );
            assert!(
                result["completeness"]
                    .to_string()
                    .contains("qualified_fallback")
            );
        } else {
            assert!(
                result["completeness"]
                    .to_string()
                    .contains("call_graph_unresolved")
            );
        }
        let items = result["items"].as_array().unwrap();
        assert_eq!(items.len(), scale.min(RETURN_LIMIT));
        assert_eq!(
            items[0]["location"].as_str().unwrap().rsplit(':').next(),
            Some("1")
        );
        assert_eq!(
            items.last().unwrap()["location"]
                .as_str()
                .unwrap()
                .rsplit(':')
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            scale.min(RETURN_LIMIT)
        );
        if series == ExistingPathAuditSeries::Outgoing {
            assert!(items.iter().all(|item| {
                item["kind_source"] == "tier25-audit-manifest"
                    && item["target_path"] == "manifest.rs"
                    && item["target_qualified"] == "crate::target"
            }));
        } else {
            let resolved = items
                .iter()
                .filter(|item| item["kind_source"] == "tier25-audit-manifest")
                .count();
            let unresolved = items
                .iter()
                .filter(|item| item["kind_source"] == "tier2-fact")
                .count();
            assert_eq!((resolved, unresolved), (50, 50));
        }
        let captured = observer.captured_sql();
        assert_eq!(
            captured.len(),
            if series == ExistingPathAuditSeries::BareFallback {
                2
            } else {
                1
            }
        );
        let query = captured.last().unwrap();
        assert_eq!(query.kind, CapturedQueryKind::Fallback);
        let plan_conn = fixture_store(&fixture);
        let plan = query_plan(&plan_conn, query);
        emit_raw_eqp(trace_id, PerfSeries::StrictEmptyFallback, &plan);
        assert_eq!(
            plan.iter()
                .filter(|(_, _, detail)| is_left_resolution_site_lookup(detail))
                .count(),
            1
        );
        assert!(
            !plan
                .iter()
                .any(|(_, _, detail)| detail == "SCAN res LEFT-JOIN")
        );
        assert_v14_correlated_membership_plan(PerfSeries::StrictEmptyFallback, &plan);
        let expected_binds = match series {
            ExistingPathAuditSeries::BareFallback => 3,
            ExistingPathAuditSeries::Outgoing => 2,
        };
        assert_eq!(query.params.len(), expected_binds);
        eprintln!(
            "EXISTING_PATH_AUDIT series={series:?} scale={scale} fixture={fixture_elapsed:?} index={index_elapsed:?} sql={:?} dispatch={query_elapsed:?} relevant={relevant} resolved_sites={resolved_sites} unresolved_calls={unresolved_calls} type_noise={type_noise} returned={} plan_nodes={}",
            fallback.elapsed,
            items.len(),
            plan.len()
        );
    }

    macro_rules! existing_path_audit_tests {
        ($(($name:ident, $series:ident, $scale:expr, $bound:expr)),+ $(,)?) => {$ (
            #[tokio::test]
            async fn $name() {
                run_existing_path_audit(
                    ExistingPathAuditSeries::$series,
                    $scale,
                    stringify!($name),
                    Duration::from_secs($bound),
                ).await;
            }
        )+};
    }

    existing_path_audit_tests!(
        (fallback_existing_path_256, BareFallback, 256, 30),
        (fallback_existing_path_512, BareFallback, 512, 30),
        (fallback_existing_path_1024, BareFallback, 1024, 30),
        (fallback_existing_path_2048, BareFallback, 2048, 30),
        (fallback_existing_path_4096, BareFallback, 4096, 60),
        (fallback_existing_path_16384, BareFallback, 16384, 60),
        (outgoing_existing_path_256, Outgoing, 256, 30),
        (outgoing_existing_path_512, Outgoing, 512, 30),
        (outgoing_existing_path_1024, Outgoing, 1024, 30),
        (outgoing_existing_path_2048, Outgoing, 2048, 30),
        (outgoing_existing_path_4096, Outgoing, 4096, 60),
        (outgoing_existing_path_16384, Outgoing, 16384, 60),
    );

    #[tokio::test]
    async fn strict_payload_carry_plan_only_256() {
        run_strict_branch_b_diagnostic_variant(
            256,
            "strict-payload-carry-plan-256",
            Duration::from_secs(30),
            true,
            Some(Box::new(|path, captured, a, b, workspace, _bytes| {
                assert_eq!((a, b, workspace), (256, 256, 256));
                assert_eq!(captured.kind, CapturedQueryKind::Strict);
                assert_eq!(captured.sql.matches("strict_refs (").count(), 1);
                assert_eq!(captured.sql.matches("JOIN best_resolution res").count(), 2);
                let ref_candidates = captured
                    .sql
                    .split_once("         ref_candidates AS (")
                    .expect("ref_candidates start missing")
                    .1
                    .split_once("         ranked_refs AS (")
                    .expect("ref_candidates end missing")
                    .0;
                assert_eq!(ref_candidates.matches("best_resolution").count(), 0);
                assert_eq!(
                    ref_candidates
                        .matches("sym.id = r.resolution_target_symbol_id")
                        .count(),
                    1
                );
                let conn = cas_store::open_existing(path).unwrap();
                let plan = query_plan(&conn, captured);
                emit_raw_eqp(
                    "strict-payload-carry-plan-256",
                    PerfSeries::StrictHit,
                    &plan,
                );
                assert_v14_correlated_membership_plan(PerfSeries::StrictHit, &plan);
                assert_strict_payload_carry_strict_index_plan(&plan);
                false
            })),
            TestQueryVariant::ProductionCarry,
        )
        .await;
    }

    #[tokio::test]
    #[cfg(unix)]
    #[ignore = "phase-isolation child; launched only by its controller"]
    async fn strict_branch_b_diagnostic_4096_phase_child() {
        run_strict_branch_b_diagnostic(
            4_096,
            "strict-branch-b-4096-phase-child",
            Duration::from_secs(60),
            false,
            Some(Box::new(|store_path, _, a, b, workspace, db_bytes| {
                println!(
                    "CAIRN_PHASE\tFIXTURE_READY\ta={a}\tb={b}\tws={workspace}\tdb_bytes={db_bytes}\tpath={}",
                    store_path.display()
                );
                std::io::stdout().flush().unwrap();
                let mut release = String::new();
                std::io::stdin().read_line(&mut release).unwrap();
                assert_eq!(release, "RELEASE_QUERY\n");
                println!("CAIRN_PHASE\tQUERY_STARTED");
                std::io::stdout().flush().unwrap();
                true
            })),
        )
        .await;
        println!("CAIRN_PHASE\tQUERY_TERMINAL");
        std::io::stdout().flush().unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn strict_branch_b_diagnostic_4096_phase_controller() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "data_rpc::methods::find_references::phase_tests::strict_branch_b_diagnostic_4096_phase_child",
                "--ignored",
                "--exact",
                "--nocapture",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                if line.starts_with("CAIRN_PHASE\t") {
                    tx.send(line).unwrap();
                }
            }
        });
        let ready = rx
            .recv_timeout(Duration::from_secs(60))
            .unwrap_or_else(|_| phase_child_stop(&mut child, pid, "setup timeout"));
        assert!(ready.starts_with("CAIRN_PHASE\tFIXTURE_READY\ta=4096\tb=4096\tws=4096\t"));
        let store_path = ready
            .split("\tpath=")
            .nth(1)
            .map(std::path::PathBuf::from)
            .expect("FIXTURE_READY path missing");
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"RELEASE_QUERY\n")
            .unwrap();
        child.stdin.as_mut().unwrap().flush().unwrap();
        let started = rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| phase_child_stop(&mut child, pid, "QUERY_STARTED missing"));
        assert_eq!(started, "CAIRN_PHASE\tQUERY_STARTED");
        let terminal = rx
            .recv_timeout(Duration::from_secs(60))
            .unwrap_or_else(|_| phase_child_stop(&mut child, pid, "query timeout"));
        assert_eq!(terminal, "CAIRN_PHASE\tQUERY_TERMINAL");
        let status = child.wait().unwrap();
        assert!(status.success());
        reader.join().unwrap();
        assert!(
            !store_path.exists(),
            "child temporary store path survived exit"
        );
        eprintln!("PHASE_CONTROLLER_TERMINAL pid={pid} residual_store=0");
    }

    #[cfg(unix)]
    fn boundary_command_loop(
        store_path: &std::path::Path,
        captured: &CapturedQuerySql,
        scale: usize,
        a: usize,
        b: usize,
        workspace: usize,
        db_bytes: u64,
    ) -> bool {
        println!(
            "CAIRN_BOUNDARY\tSETUP_READY\tscale={scale}\ta={a}\tb={b}\tws={workspace}\tdb_bytes={db_bytes}\tpath={}",
            store_path.display()
        );
        std::io::stdout().flush().unwrap();
        let stdin = std::io::stdin();
        let mut commands = stdin.lock().lines();
        for expected in V14_BOUNDARY_NAMES {
            let command = commands.next().unwrap().unwrap();
            assert_eq!(command, format!("RUN\t{expected}"));
            let conn = cas_store::open_existing(store_path).unwrap();
            println!("CAIRN_BOUNDARY\tPROBE_STARTED\t{expected}");
            std::io::stdout().flush().unwrap();
            let result = run_v14_boundary_probe(&conn, captured, scale, expected);
            eprintln!(
                "V14_BOUNDARY_RAW scale={scale} name={expected} rows={} sites={} query={:?} metadata={:?} plan={:?} opcodes={}",
                result.rows,
                result.sites,
                result.query_elapsed,
                result.metadata_elapsed,
                result.plan,
                result.opcode_count
            );
            std::io::stderr().flush().unwrap();
            println!(
                "CAIRN_BOUNDARY\tPROBE_DONE\t{expected}\tquery_ns={}\tmetadata_ns={}\trows={}\tsites={}\tplan_nodes={}\topcodes={}",
                result.query_elapsed.as_nanos(),
                result.metadata_elapsed.as_nanos(),
                result.rows,
                result.sites,
                result.plan.len(),
                result.opcode_count
            );
            std::io::stdout().flush().unwrap();
            drop(conn);
        }
        assert_eq!(commands.next().unwrap().unwrap(), "FINISH");
        false
    }

    #[cfg(unix)]
    async fn boundary_child(scale: usize, trace_id: &'static str) {
        run_strict_branch_b_diagnostic(
            scale,
            trace_id,
            Duration::from_secs(30),
            false,
            Some(Box::new(move |path, captured, a, b, ws, bytes| {
                boundary_command_loop(path, captured, scale, a, b, ws, bytes)
            })),
        )
        .await;
    }

    #[tokio::test]
    #[cfg(unix)]
    #[ignore = "boundary IPC child"]
    async fn v14_operator_boundaries_child_256() {
        boundary_child(256, "v14-boundaries-256").await;
    }

    #[tokio::test]
    #[cfg(unix)]
    #[ignore = "boundary IPC child"]
    async fn v14_operator_boundaries_child_512() {
        boundary_child(512, "v14-boundaries-512").await;
    }

    #[tokio::test]
    #[cfg(unix)]
    #[ignore = "boundary IPC child"]
    async fn v14_operator_boundaries_child_1024() {
        boundary_child(1_024, "v14-boundaries-1024").await;
    }

    #[tokio::test]
    #[cfg(unix)]
    #[ignore = "boundary IPC child"]
    async fn v14_operator_boundaries_child_2048() {
        boundary_child(2_048, "v14-boundaries-2048").await;
    }

    #[cfg(unix)]
    fn boundary_controller(scale: usize, child_test: &str) {
        let qualified = format!("data_rpc::methods::find_references::phase_tests::{child_test}");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([qualified.as_str(), "--ignored", "--exact", "--nocapture"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                if line.starts_with("CAIRN_BOUNDARY\t") {
                    tx.send(line).unwrap();
                }
            }
        });
        let ready = rx
            .recv_timeout(Duration::from_secs(60))
            .unwrap_or_else(|_| phase_child_stop(&mut child, pid, "boundary setup timeout"));
        assert!(ready.starts_with(&format!(
            "CAIRN_BOUNDARY\tSETUP_READY\tscale={scale}\ta={scale}\tb={scale}\tws={scale}\t"
        )));
        let store_path = ready
            .split("\tpath=")
            .nth(1)
            .map(std::path::PathBuf::from)
            .expect("SETUP_READY path missing");
        for name in V14_BOUNDARY_NAMES {
            let stdin = child.stdin.as_mut().unwrap();
            writeln!(stdin, "RUN\t{name}").unwrap();
            stdin.flush().unwrap();
            let started = rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| phase_child_stop(&mut child, pid, "PROBE_STARTED missing"));
            assert_eq!(started, format!("CAIRN_BOUNDARY\tPROBE_STARTED\t{name}"));
            let done = rx
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|_| phase_child_stop(&mut child, pid, name));
            assert!(done.starts_with(&format!("CAIRN_BOUNDARY\tPROBE_DONE\t{name}\t")));
            eprintln!("BOUNDARY_CONTROLLER scale={scale} {done}");
        }
        writeln!(child.stdin.as_mut().unwrap(), "FINISH").unwrap();
        child.stdin.as_mut().unwrap().flush().unwrap();
        let status = child.wait().unwrap();
        assert!(status.success());
        reader.join().unwrap();
        assert!(!store_path.exists(), "boundary child store survived exit");
        eprintln!("BOUNDARY_CONTROLLER_TERMINAL scale={scale} pid={pid} residual=0");
    }

    #[test]
    #[cfg(unix)]
    fn v14_operator_boundaries_controller_256() {
        boundary_controller(256, "v14_operator_boundaries_child_256");
    }

    #[test]
    #[cfg(unix)]
    fn v14_operator_boundaries_controller_512() {
        boundary_controller(512, "v14_operator_boundaries_child_512");
    }

    #[test]
    #[cfg(unix)]
    fn v14_operator_boundaries_controller_1024() {
        boundary_controller(1_024, "v14_operator_boundaries_child_1024");
    }

    #[test]
    #[cfg(unix)]
    fn v14_operator_boundaries_controller_2048() {
        boundary_controller(2_048, "v14_operator_boundaries_child_2048");
    }

    #[cfg(unix)]
    fn phase_child_stop(child: &mut std::process::Child, pid: u32, reason: &str) -> ! {
        let status = Command::new("kill")
            .args(["-INT", &format!("-{pid}")])
            .status()
            .expect("failed to signal phase child process group");
        assert!(
            status.success(),
            "failed to signal phase child process group"
        );
        let _ = child.wait();
        panic!("{reason}: phase child PID/PGID {pid} stopped");
    }

    async fn assert_payload_carry_case(
        fixture: &test_support::DataRpcFixture,
        name: &'static str,
        expected_rows: usize,
    ) {
        let symbol = format!("crate::{name}");
        let (baseline, baseline_observer) = measured_dispatch_variant(
            fixture,
            "payload-carry-inline-oracle",
            &symbol,
            TestQueryVariant::RejoinOracle,
        )
        .await;
        let (carry, carry_observer) = measured_dispatch_variant(
            fixture,
            "payload-carry-candidate",
            &symbol,
            TestQueryVariant::ProductionCarry,
        )
        .await;
        assert_eq!(baseline, carry, "payload carry diverged: {name}");
        assert_eq!(carry["items"].as_array().unwrap().len(), expected_rows);
        let baseline_sql = baseline_observer.captured_sql();
        let carry_sql = carry_observer.captured_sql();
        assert_eq!(baseline_sql.len(), 1);
        assert_eq!(carry_sql.len(), 1);
        assert_eq!(baseline_sql[0].kind, CapturedQueryKind::Strict);
        assert_eq!(baseline_sql[0].params, carry_sql[0].params);
        assert_eq!(carry_sql[0].sql.matches("strict_refs (").count(), 1);
        assert_eq!(carry_sql[0].sql.matches("SELECT r.*").count(), 0);
        assert_eq!(carry_sql[0].sql.matches("res.rn = 1").count(), 2);
        assert_eq!(carry_sql[0].sql.matches("best_resolution res").count(), 2);
        let ref_candidates = carry_sql[0]
            .sql
            .split_once("         ref_candidates AS (")
            .unwrap()
            .1;
        assert_eq!(ref_candidates.matches("best_resolution res").count(), 0);
        assert_eq!(
            ref_candidates
                .matches("sym.id = r.resolution_target_symbol_id")
                .count(),
            1
        );
        assert_eq!(carry_sql[0].sql.matches("LIMIT 101").count(), 1);
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PerfSeries {
        StrictHit,
        StrictEmptyFallback,
    }

    impl PerfSeries {
        fn symbol(self) -> &'static str {
            match self {
                Self::StrictHit => "crate::target",
                Self::StrictEmptyFallback => "missing::target",
            }
        }
    }

    fn assert_v14_correlated_membership_plan(series: PerfSeries, plan: &[(i64, i64, String)]) {
        let searches = plan
            .iter()
            .filter(|(_, _, detail)| {
                detail.starts_with("SEARCH t USING COVERING INDEX idx_refs_workspace_site_source ")
                    && detail.contains(
                        "(blob_sha=? AND line=? AND kind=? AND target_name=? AND enclosing_id=?)",
                    )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            searches.len(),
            match series {
                PerfSeries::StrictHit => 2,
                PerfSeries::StrictEmptyFallback => 1,
            }
        );
        let mut strict_left = 0;
        let mut strict_right = 0;
        for (search_id, _, _) in searches {
            let chain = eqp_parent_chain(plan, *search_id);
            let correlated_id = chain
                .iter()
                .find(|(_, detail)| detail.contains("CORRELATED SCALAR SUBQUERY"))
                .map(|(id, _)| *id)
                .expect("v14 membership lookup must belong to the correlated EXISTS");
            assert!(!plan.iter().any(|(id, _, detail)| {
                plan_node_descends_from(plan, *id, correlated_id)
                    && (detail == "SCAN t"
                        || detail.contains("idx_refs_kind")
                        || detail.starts_with("SEARCH t USING INDEX idx_refs_kind"))
            }));
            if series == PerfSeries::StrictHit {
                match chain.iter().find_map(|(_, detail)| {
                    matches!(detail.as_str(), "LEFT" | "RIGHT").then_some(detail.as_str())
                }) {
                    Some("LEFT") => strict_left += 1,
                    Some("RIGHT") => strict_right += 1,
                    None => panic!("strict v14 membership lookup lacks branch ancestry"),
                    Some(other) => unreachable!("filtered branch marker: {other}"),
                }
            }
        }
        if series == PerfSeries::StrictHit {
            assert_eq!((strict_left, strict_right), (1, 1));
        }
    }

    fn assert_resolution_site_index_plan(plan: &[(i64, i64, String)]) {
        let right_id = plan
            .iter()
            .find(|(_, _, detail)| detail == "RIGHT")
            .map(|(id, _, _)| *id)
            .expect("strict RIGHT branch missing");
        let right = plan
            .iter()
            .filter(|(id, _, _)| plan_node_descends_from(plan, *id, right_id))
            .collect::<Vec<_>>();
        assert_eq!(
            right
                .iter()
                .filter(|(_, _, detail)| {
                    detail.starts_with("SEARCH r USING INDEX idx_refs_resolution_site ")
                        && detail.contains("(blob_sha=? AND parser_id=? AND byte_start=? AND byte_end=? AND kind=?)")
                })
                .count(),
            1
        );
        assert_eq!(
            right
                .iter()
                .filter(|(_, _, detail)| is_best_resolution_driver_access(detail))
                .count(),
            1
        );
        assert!(!right.iter().any(|(_, _, detail)| {
            detail.starts_with("SEARCH r USING INDEX idx_refs_blob ")
                || detail.starts_with("SEARCH r USING INDEX idx_refs_kind ")
        }));
        assert!(right.iter().any(|(_, _, detail)| {
            detail.starts_with("SEARCH res USING AUTOMATIC ")
                || detail.starts_with("SEARCH res USING AUTOMATIC PARTIAL ")
        }));
    }

    fn is_best_resolution_driver_access(detail: &str) -> bool {
        detail == "SCAN res" || detail.starts_with("SEARCH res ")
    }

    #[test]
    fn best_resolution_driver_classifier_accepts_one_scan_or_search_only() {
        let count = |details: &[&str]| {
            details
                .iter()
                .filter(|detail| is_best_resolution_driver_access(detail))
                .count()
        };
        assert_eq!(count(&["SCAN res"]), 1);
        assert_eq!(
            count(&["SEARCH res USING AUTOMATIC PARTIAL COVERING INDEX (kind=? AND rn=?)"]),
            1
        );
        assert_eq!(
            count(&["SCAN r", "SEARCH sym USING INTEGER PRIMARY KEY"]),
            0
        );
        assert_eq!(
            count(&["SCAN res", "SEARCH res USING AUTOMATIC INDEX (rn=?)"]),
            2
        );
    }

    fn is_left_resolution_site_lookup(detail: &str) -> bool {
        detail
            == "SEARCH res USING AUTOMATIC PARTIAL COVERING INDEX \
(site_blob_sha=? AND site_parser_id=? AND site_byte_start=? AND site_byte_end=? AND kind=? AND rn=?) LEFT-JOIN"
    }

    #[test]
    fn left_resolution_site_lookup_classifier_requires_exact_plan_shape() {
        let exact = "SEARCH res USING AUTOMATIC PARTIAL COVERING INDEX \
(site_blob_sha=? AND site_parser_id=? AND site_byte_start=? AND site_byte_end=? AND kind=? AND rn=?) LEFT-JOIN";
        assert!(is_left_resolution_site_lookup(exact));
        assert!(!is_left_resolution_site_lookup(
            "SEARCH res USING AUTOMATIC COVERING INDEX (site_blob_sha=? AND site_parser_id=? AND site_byte_start=? AND site_byte_end=? AND kind=? AND rn=?) LEFT-JOIN"
        ));
        assert!(!is_left_resolution_site_lookup(
            "SEARCH res USING AUTOMATIC PARTIAL COVERING INDEX (site_blob_sha=? AND site_parser_id=? AND site_byte_start=? AND site_byte_end=? AND kind=?) LEFT-JOIN"
        ));
        assert!(!is_left_resolution_site_lookup(
            "SEARCH res USING AUTOMATIC PARTIAL COVERING INDEX (site_parser_id=? AND site_blob_sha=? AND site_byte_start=? AND site_byte_end=? AND kind=? AND rn=?) LEFT-JOIN"
        ));
        assert!(!is_left_resolution_site_lookup(
            "SEARCH res USING AUTOMATIC PARTIAL COVERING INDEX (site_blob_sha=? AND site_parser_id=? AND site_byte_start=? AND site_byte_end=? AND kind=? AND rn=?)"
        ));
    }

    fn assert_strict_payload_carry_strict_index_plan(plan: &[(i64, i64, String)]) {
        let left_id = plan
            .iter()
            .find(|(_, _, detail)| detail == "LEFT")
            .map(|(id, _, _)| *id)
            .expect("strict LEFT branch missing");
        let right_id = plan
            .iter()
            .find(|(_, _, detail)| detail == "RIGHT")
            .map(|(id, _, _)| *id)
            .expect("strict RIGHT branch missing");
        let left = plan
            .iter()
            .filter(|(id, _, _)| plan_node_descends_from(plan, *id, left_id))
            .collect::<Vec<_>>();
        let right = plan
            .iter()
            .filter(|(id, _, _)| plan_node_descends_from(plan, *id, right_id))
            .collect::<Vec<_>>();

        assert_eq!(
            left.iter()
                .filter(|(_, _, detail)| is_left_resolution_site_lookup(detail))
                .count(),
            1
        );
        assert_eq!(
            right
                .iter()
                .filter(|(_, _, detail)| {
                    detail.starts_with("SEARCH r USING INDEX idx_refs_resolution_site ")
                        && detail.contains(
                            "(blob_sha=? AND parser_id=? AND byte_start=? AND byte_end=? AND kind=?)",
                        )
                })
                .count(),
            1
        );
        assert_eq!(
            right
                .iter()
                .filter(|(_, _, detail)| is_best_resolution_driver_access(detail))
                .count(),
            1
        );
        assert!(!right.iter().any(|(_, _, detail)| {
            detail.starts_with("SEARCH r USING INDEX idx_refs_blob ")
                || detail.starts_with("SEARCH r USING INDEX idx_refs_kind ")
        }));
    }

    #[tokio::test]
    async fn production_correlated_membership_uses_v14_covering_index() {
        for series in [PerfSeries::StrictHit, PerfSeries::StrictEmptyFallback] {
            let (_, plan) = capture_eqp_only(
                "v14-production-plan",
                series,
                TestQueryVariant::ProductionCarry,
            )
            .await;
            emit_raw_eqp("v14-production-plan", series, &plan);
            assert_v14_correlated_membership_plan(series, &plan);
        }
    }

    async fn capture_eqp_only(
        step: &'static str,
        series: PerfSeries,
        variant: TestQueryVariant,
    ) -> (CapturedQuerySql, Vec<(i64, i64, String)>) {
        let source = repeated_call_source(256);
        let fixture = test_support::registered_fixture_with_files(&[("src/lib.rs", &source)]);
        let observer = QueryPhaseObserver::capture_sql_only(step, 0, variant);
        let capture_symbol = match series {
            PerfSeries::StrictHit => series.symbol(),
            PerfSeries::StrictEmptyFallback => "target",
        };
        let error = with_query_phase_observer(
            observer.clone(),
            FindReferences.dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "symbol": capture_symbol,
                    "direction": "incoming",
                    "kind": "call",
                    "anchor": "HEAD",
                    "limit": RETURN_LIMIT
                }),
            ),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("test SQL capture only"));
        let captured = observer.captured_sql();
        assert_eq!(captured.len(), 1);
        let captured = captured.into_iter().next().unwrap();
        let expected_kind = match series {
            PerfSeries::StrictHit => CapturedQueryKind::Strict,
            PerfSeries::StrictEmptyFallback => CapturedQueryKind::Fallback,
        };
        assert_eq!(captured.kind, expected_kind);
        let conn = fixture_store(&fixture);
        let plan = query_plan(&conn, &captured);
        (captured, plan)
    }

    fn eqp_parent_chain(plan: &[(i64, i64, String)], node_id: i64) -> Vec<(i64, String)> {
        let mut chain = Vec::new();
        let mut current = node_id;
        while let Some((_, parent, detail)) = plan.iter().find(|(id, _, _)| *id == current) {
            chain.push((current, detail.clone()));
            if *parent == current || *parent == 0 {
                break;
            }
            current = *parent;
        }
        chain
    }

    fn emit_raw_eqp(step: &str, series: PerfSeries, plan: &[(i64, i64, String)]) {
        for (id, parent, detail) in plan {
            let chain = eqp_parent_chain(plan, *id);
            let branch = match series {
                PerfSeries::StrictEmptyFallback => "bare-fallback",
                PerfSeries::StrictHit => match chain.iter().find_map(|(_, detail)| {
                    matches!(detail.as_str(), "LEFT" | "RIGHT").then_some(detail.as_str())
                }) {
                    Some("LEFT") => "strict-branch-a",
                    Some("RIGHT") => "strict-branch-b",
                    _ => "strict-shared-or-unknown",
                },
            };
            eprintln!(
                "RAW_EQP step={step} series={series:?} branch={branch} id={id} parent={parent} detail={detail:?} chain={chain:?}"
            );
        }
    }

    #[tokio::test]
    async fn production_carry_preserves_membership_truth_table() {
        let source = repeated_call_source(16);
        let fixture = test_support::registered_fixture_with_files(&[
            ("src/lib.rs", &source),
            ("src/other.rs", "pub fn other() {}\n"),
        ]);
        let conn = fixture_store(&fixture);
        let (blob_sha, parser_id): (String, String) = conn
            .query_row(
                "SELECT blob_sha, parser_id FROM refs WHERE target_name='target' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let other_blob: String = conn
            .query_row(
                "SELECT blob_sha FROM blobs
                  WHERE parser_id=?1 AND blob_sha<>?2 LIMIT 1",
                rusqlite::params![parser_id, blob_sha],
                |row| row.get(0),
            )
            .unwrap();
        let enclosing: i64 = conn
            .query_row("SELECT id FROM symbols LIMIT 1", [], |row| row.get(0))
            .unwrap();

        for source in ["truth-ordinary", "tier3-truth-a", "tier3-truth-b"] {
            insert_truth_ref(
                &conn,
                &blob_sha,
                &parser_id,
                None,
                "case_exact",
                "crate::case_exact",
                "call",
                50_000,
                source,
            );
        }
        for source in ["truth-ordinary", "tier3-truth"] {
            insert_truth_ref(
                &conn,
                &blob_sha,
                &parser_id,
                None,
                "case_null",
                "crate::case_null",
                "call",
                50_010,
                source,
            );
        }
        insert_truth_ref_with_range(
            &conn,
            &blob_sha,
            &parser_id,
            None,
            "case_null_asymmetric",
            "crate::case_null_asymmetric",
            "call",
            0,
            0,
            50_015,
            "truth-ordinary",
        );
        insert_truth_ref_with_range(
            &conn,
            &blob_sha,
            &parser_id,
            None,
            "case_null_asymmetric",
            "crate::case_null_asymmetric",
            "call",
            1,
            2,
            50_015,
            "tier3-truth",
        );

        let mismatch_cases = [
            (
                "case_line",
                &blob_sha,
                None,
                "call",
                50_020,
                &blob_sha,
                None,
                "call",
                50_021,
            ),
            (
                "case_kind",
                &blob_sha,
                None,
                "call",
                50_030,
                &blob_sha,
                None,
                "type",
                50_030,
            ),
            (
                "case_target",
                &blob_sha,
                None,
                "call",
                50_040,
                &blob_sha,
                None,
                "call",
                50_040,
            ),
            (
                "case_enclosing",
                &blob_sha,
                None,
                "call",
                50_050,
                &blob_sha,
                Some(enclosing),
                "call",
                50_050,
            ),
            (
                "case_blob",
                &blob_sha,
                None,
                "call",
                50_060,
                &other_blob,
                None,
                "call",
                50_060,
            ),
        ];
        for (
            name,
            ordinary_blob,
            ordinary_enclosing,
            ordinary_kind,
            ordinary_line,
            workspace_blob,
            workspace_enclosing,
            workspace_kind,
            workspace_line,
        ) in mismatch_cases
        {
            insert_truth_ref(
                &conn,
                ordinary_blob,
                &parser_id,
                ordinary_enclosing,
                name,
                &format!("crate::{name}"),
                ordinary_kind,
                ordinary_line,
                "truth-ordinary",
            );
            let workspace_name = if name == "case_target" {
                "case_target_mismatch"
            } else {
                name
            };
            insert_truth_ref(
                &conn,
                workspace_blob,
                &parser_id,
                workspace_enclosing,
                workspace_name,
                &format!("crate::{workspace_name}"),
                workspace_kind,
                workspace_line,
                "tier3-truth",
            );
        }
        drop(conn);

        assert_payload_carry_case(&fixture, "case_exact", 1).await;
        assert_payload_carry_case(&fixture, "case_null", 1).await;
        assert_payload_carry_case(&fixture, "case_null_asymmetric", 1).await;
        assert_payload_carry_case(&fixture, "case_line", 2).await;
        assert_payload_carry_case(&fixture, "case_kind", 1).await;
        assert_payload_carry_case(&fixture, "case_target", 1).await;
        assert_payload_carry_case(&fixture, "case_enclosing", 2).await;
        assert_payload_carry_case(&fixture, "case_blob", 2).await;
    }

    #[tokio::test]
    async fn strict_payload_carry_preserves_multi_resolution_winner_and_outgoing_consumer() {
        let fixture = test_support::registered_fixture_with_files(&[(
            "src/lib.rs",
            "fn caller() { target(); }\n",
        )]);
        let conn = fixture_store(&fixture);
        let (manifest_id, blob_sha, parser_id): (i64, String, String) = conn
            .query_row(
                "SELECT me.manifest_id, me.blob_sha, b.parser_id
                   FROM manifest_entries me
                   JOIN blobs b ON b.blob_sha = me.blob_sha
                  WHERE me.path = 'src/lib.rs' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        conn.execute("DELETE FROM resolutions", []).unwrap();
        conn.execute("DELETE FROM refs", []).unwrap();
        conn.execute("DELETE FROM symbols", []).unwrap();

        let insert_symbol = |name: &str, qualified: &str| {
            conn.execute(
                "INSERT INTO symbols
                   (blob_sha, parser_id, parent_id, name, qualified, kind, signature,
                    visibility, doc, byte_start, byte_end, line_start, line_end,
                    body_start, source)
                 VALUES (?1, ?2, NULL, ?3, ?4, 'function', NULL,
                         NULL, NULL, 0, 1, 1, 1, NULL, 'rust-syn')",
                rusqlite::params![blob_sha, parser_id, name, qualified],
            )
            .unwrap();
            conn.last_insert_rowid()
        };
        let enclosing_id = insert_symbol("caller", "crate::caller");
        let global_id = insert_symbol("global", "crate::global");
        let manifest_low_id = insert_symbol("low", "crate::low");
        let winner_id = insert_symbol("winner", "crate::winner");
        let same_tier_later_id = insert_symbol("later", "crate::later");
        let same_id = insert_symbol("same", "crate::same");
        insert_truth_ref_with_range(
            &conn,
            &blob_sha,
            &parser_id,
            Some(enclosing_id),
            "target",
            "syntactic::target",
            "call",
            4,
            10,
            1,
            "tier2-direct-tie",
        );
        for (target_symbol_id, source, target_path, selected_manifest) in [
            (global_id, "tier3-global", "global.rs", None),
            (
                manifest_low_id,
                "tier25-manifest",
                "low.rs",
                Some(manifest_id),
            ),
            (winner_id, "tier3-winner", "winner.rs", Some(manifest_id)),
            (
                same_tier_later_id,
                "tier3-winner",
                "later.rs",
                Some(manifest_id),
            ),
        ] {
            conn.execute(
                "INSERT INTO resolutions
                   (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                    kind, semantic_kind, target_symbol_id, source, target_path,
                    manifest_id)
                 VALUES (?1, ?2, 4, 10, 'call', NULL, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    blob_sha,
                    parser_id,
                    target_symbol_id,
                    source,
                    target_path,
                    selected_manifest
                ],
            )
            .unwrap();
        }
        for (name, qualified, start) in [
            ("same", "crate::same", 12_i64),
            ("override_query", "crate::override_query", 18_i64),
            ("nullpayload", "crate::nullpayload", 24_i64),
        ] {
            insert_truth_ref_with_range(
                &conn,
                &blob_sha,
                &parser_id,
                Some(enclosing_id),
                name,
                qualified,
                "call",
                start,
                start + 2,
                1,
                "tier2-direct-a",
            );
        }
        for (start, target_symbol_id, source, target_path) in [
            (12_i64, Some(same_id), "tier3-same", "same.rs"),
            (18_i64, Some(global_id), "tier3-override", "override.rs"),
            (24_i64, None, "tier3-null-payload", "null.rs"),
        ] {
            conn.execute(
                "INSERT INTO resolutions
                   (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                    kind, semantic_kind, target_symbol_id, source, target_path,
                    manifest_id)
                 VALUES (?1, ?2, ?3, ?4, 'call', NULL, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    blob_sha,
                    parser_id,
                    start,
                    start + 2,
                    target_symbol_id,
                    source,
                    target_path,
                    manifest_id
                ],
            )
            .unwrap();
        }
        drop(conn);

        let (inline, inline_observer) = measured_dispatch_variant(
            &fixture,
            "resolution-tie-inline",
            "crate::winner",
            TestQueryVariant::RejoinOracle,
        )
        .await;
        let (carry, carry_observer) = measured_dispatch_variant(
            &fixture,
            "resolution-tie-payload-carry",
            "crate::winner",
            TestQueryVariant::ProductionCarry,
        )
        .await;
        assert_eq!(inline, carry);
        let items = carry["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["target_qualified"], "crate::winner");
        assert_eq!(items[0]["kind_source"], "tier3-winner");
        assert_eq!(items[0]["target_path"], "winner.rs");
        let inline_sql = inline_observer.captured_sql();
        let carry_sql = carry_observer.captured_sql();
        assert_eq!(inline_sql.len(), 1);
        assert_eq!(carry_sql.len(), 1);
        assert_eq!(inline_sql[0].params, carry_sql[0].params);
        assert_eq!(inline_sql[0].sql.matches("res.rn = 1").count(), 2);
        assert_eq!(carry_sql[0].sql.matches("res.rn = 1").count(), 2);
        assert_eq!(carry_sql[0].sql.matches("SELECT r.*").count(), 0);
        let conn = fixture_store(&fixture);
        let counts_sql = format!(
            "{} SELECT
                 (SELECT COUNT(*) FROM best_resolution WHERE site_byte_start = 4),
                 (SELECT COUNT(*) FROM (
                    SELECT site_blob_sha, site_parser_id, site_byte_start,
                           site_byte_end, kind FROM best_resolution
                    WHERE site_byte_start = 4
                    GROUP BY site_blob_sha, site_parser_id, site_byte_start,
                             site_byte_end, kind
                  )),
                 (SELECT COUNT(*) FROM best_resolution WHERE site_byte_start = 4 AND rn = 1)",
            cte_prefix(&carry_sql[0].sql)
        );
        let counts: (i64, i64, i64) = conn
            .query_row(
                &counts_sql,
                rusqlite::params_from_iter(carry_sql[0].params.iter()),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(counts, (4, 1, 1));
        let winner: (i64, String, String) = conn
            .query_row(
                &format!(
                    "{} SELECT target_symbol_id, source, target_path FROM best_resolution WHERE site_byte_start = 4 AND rn = 1",
                    cte_prefix(&carry_sql[0].sql)
                ),
                rusqlite::params_from_iter(carry_sql[0].params.iter()),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            winner,
            (winner_id, "tier3-winner".into(), "winner.rs".into())
        );
        drop(conn);

        for (symbol, expected_strict_rows, expected_rows, expected_source, expected_path) in [
            ("crate::same", 1, 1, Some("tier3-same"), Some("same.rs")),
            (
                "crate::override_query",
                0,
                1,
                Some("tier3-override"),
                Some("override.rs"),
            ),
            (
                "crate::nullpayload",
                1,
                1,
                Some("tier3-null-payload"),
                Some("null.rs"),
            ),
        ] {
            let (oracle, oracle_observer) = measured_dispatch_variant(
                &fixture,
                "payload-a-oracle",
                symbol,
                TestQueryVariant::RejoinOracle,
            )
            .await;
            let (candidate, candidate_observer) = measured_dispatch_variant(
                &fixture,
                "payload-a-carry",
                symbol,
                TestQueryVariant::ProductionCarry,
            )
            .await;
            assert_eq!(oracle, candidate);
            for observer in [&oracle_observer, &candidate_observer] {
                let events = observer.events();
                let strict = events
                    .iter()
                    .find(|event| event.phase == QueryPhase::StrictSql)
                    .expect("strict phase missing");
                assert_eq!(strict.rows, Some(expected_strict_rows));
                let fallback = events
                    .iter()
                    .find(|event| event.phase == QueryPhase::FallbackSql);
                if expected_strict_rows == 0 {
                    let fallback = fallback.expect("fallback phase missing after strict mismatch");
                    assert!(fallback.executed);
                    assert_eq!(fallback.rows, Some(1));
                } else {
                    assert!(fallback.is_none());
                }
            }
            let oracle_sql = oracle_observer.captured_sql();
            let candidate_sql = candidate_observer.captured_sql();
            assert_eq!(oracle_sql.len(), candidate_sql.len());
            assert_eq!(
                oracle_sql.len(),
                if expected_strict_rows == 0 { 2 } else { 1 }
            );
            assert_eq!(
                oracle_sql
                    .iter()
                    .map(|query| (&query.kind, &query.params))
                    .collect::<Vec<_>>(),
                candidate_sql
                    .iter()
                    .map(|query| (&query.kind, &query.params))
                    .collect::<Vec<_>>()
            );
            if expected_strict_rows == 0 {
                let oracle_fallback = oracle_sql
                    .iter()
                    .find(|query| query.kind == CapturedQueryKind::Fallback)
                    .unwrap();
                let candidate_fallback = candidate_sql
                    .iter()
                    .find(|query| query.kind == CapturedQueryKind::Fallback)
                    .unwrap();
                assert_eq!(oracle_fallback, candidate_fallback);
            }
            let rows = candidate["items"].as_array().unwrap();
            assert_eq!(rows.len(), expected_rows);
            if let Some(source) = expected_source {
                assert_eq!(rows[0]["kind_source"], source);
            }
            if let Some(path) = expected_path {
                assert_eq!(rows[0]["target_path"], path);
            }
        }

        let outgoing_params = || {
            json!({
                "repo": "demo",
                "symbol": "crate::caller",
                "direction": "outgoing",
                "kind": "call",
                "anchor": "HEAD",
                "limit": RETURN_LIMIT
            })
        };
        let inline_outgoing_observer =
            QueryPhaseObserver::with_variant("outgoing-inline", 0, TestQueryVariant::RejoinOracle);
        let out_inline = with_query_phase_observer(
            inline_outgoing_observer.clone(),
            FindReferences.dispatch(&fixture.ctx, outgoing_params()),
        )
        .await
        .unwrap();
        let out_observer = QueryPhaseObserver::with_variant(
            "outgoing-carry-non-interference",
            0,
            TestQueryVariant::ProductionCarry,
        );
        let out_carry = with_query_phase_observer(
            out_observer.clone(),
            FindReferences.dispatch(&fixture.ctx, outgoing_params()),
        )
        .await
        .unwrap();
        assert_eq!(out_inline, out_carry);
        let stable_outgoing = out_carry["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                let location = item["location"].as_str().unwrap();
                let mut parts = location.splitn(4, ':');
                assert_eq!(parts.next(), Some("demo"));
                assert_eq!(parts.next(), Some("HEAD"));
                let path = parts.next().unwrap();
                let line = parts.next().unwrap().parse::<u32>().unwrap();
                json!({
                    "path": path,
                    "line": line,
                    "target_name": item["target_name"],
                    "target_qualified": item["target_qualified"],
                    "kind": item["kind"],
                    "enclosing_qualified": item["enclosing_qualified"],
                    "kind_source": item["kind_source"],
                    "target_path": item["target_path"]
                })
            })
            .collect::<Vec<_>>();
        let expected_outgoing = vec![
            json!({
                "path": "src/lib.rs", "line": 1, "target_name": "target",
                "target_qualified": "crate::winner", "kind": "call",
                "enclosing_qualified": "crate::caller",
                "kind_source": "tier3-winner", "target_path": "winner.rs"
            }),
            json!({
                "path": "src/lib.rs", "line": 1, "target_name": "same",
                "target_qualified": "crate::same", "kind": "call",
                "enclosing_qualified": "crate::caller",
                "kind_source": "tier3-same", "target_path": "same.rs"
            }),
            json!({
                "path": "src/lib.rs", "line": 1, "target_name": "override_query",
                "target_qualified": "crate::global", "kind": "call",
                "enclosing_qualified": "crate::caller",
                "kind_source": "tier3-override", "target_path": "override.rs"
            }),
            json!({
                "path": "src/lib.rs", "line": 1, "target_name": "nullpayload",
                "target_qualified": "crate::nullpayload", "kind": "call",
                "enclosing_qualified": "crate::caller",
                "kind_source": "tier3-null-payload", "target_path": "null.rs"
            }),
        ];
        assert_eq!(stable_outgoing.len(), expected_outgoing.len());
        assert_eq!(stable_outgoing, expected_outgoing);
        let winner = stable_outgoing
            .iter()
            .find(|row| row["target_name"] == "target")
            .unwrap();
        assert_eq!(winner["kind_source"], "tier3-winner");
        assert_eq!(winner["target_path"], "winner.rs");
        let outgoing_sql = out_observer.captured_sql();
        assert_eq!(outgoing_sql.len(), 1);
        assert_eq!(outgoing_sql[0].kind, CapturedQueryKind::Fallback);
        assert_eq!(outgoing_sql[0].sql.matches("res.rn = 1").count(), 1);
        assert_eq!(
            outgoing_sql[0].sql.matches("best_resolution res").count(),
            1
        );
        let inline_outgoing_sql = inline_outgoing_observer.captured_sql();
        assert_eq!(inline_outgoing_sql.len(), 1);
        assert_eq!(outgoing_sql[0], inline_outgoing_sql[0]);
    }
}

/// Lazily reads each touched blob at most once. Most `find_references`
/// result sets cluster hits onto a small number of files (one impl
/// block, one trait method), so the cache turns N hits into K blob
/// reads with K ≪ N.
pub(super) struct SnippetCache {
    repo: String,
    worktree_root: PathBuf,
    /// `blob_sha → file contents` once materialised. `None` means we
    /// already tried and the blob couldn't be loaded; we won't retry.
    blobs: HashMap<String, Option<Vec<u8>>>,
}

impl SnippetCache {
    pub(super) fn new(repo: String, worktree_root: PathBuf) -> Self {
        Self {
            repo,
            worktree_root,
            blobs: HashMap::new(),
        }
    }

    pub(super) fn line_for(&mut self, blob_sha: &str, path: &str, line: u32) -> Option<String> {
        let bytes = self.load(blob_sha, path);
        bytes
            .and_then(|b| extract_line(b, line))
            .map(|s| s.trim_end_matches(['\r', '\n']).to_string())
    }

    fn load(&mut self, blob_sha: &str, path: &str) -> Option<&[u8]> {
        if !self.blobs.contains_key(blob_sha) {
            let loaded = load_blob_or_verified_worktree(&self.worktree_root, blob_sha, path)
                .inspect_err(|_| {
                    debug!(
                        repo = self.repo,
                        path, "snippet omitted because indexed bytes are unavailable or changed"
                    );
                })
                .ok();
            self.blobs.insert(blob_sha.to_string(), loaded);
        }
        self.blobs.get(blob_sha).and_then(Option::as_deref)
    }
}

/// Returns the requested 1-indexed line as a UTF-8 lossy slice.
/// `None` when the file is shorter than the requested line.
fn extract_line(bytes: &[u8], line: u32) -> Option<String> {
    if line == 0 {
        return None;
    }
    let target = line as usize - 1;
    let mut current = 0;
    let mut start = 0;
    for (idx, &b) in bytes.iter().enumerate() {
        if current == target {
            // walk to the end of this line
            let end = bytes[idx..]
                .iter()
                .position(|&c| c == b'\n')
                .map_or(bytes.len(), |n| idx + n);
            return Some(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        }
        if b == b'\n' {
            current += 1;
            start = idx + 1;
        }
    }
    if current == target && start <= bytes.len() {
        return Some(String::from_utf8_lossy(&bytes[start..]).into_owned());
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use cairn_proto::common::PartialReason;
    use serde_json::json;

    use super::*;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::data_rpc::helpers::test_support::assert_limit_probe;
    use crate::data_rpc::methods::find_callees::FindCallees;
    use crate::data_rpc::methods::find_callers::FindCallers;
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[test]
    fn qualified_fallback_only_replaces_complete_or_cap_reason() {
        assert_eq!(
            qualified_reference_completeness(Completeness::Complete, true),
            Completeness::partial_semantic("qualified_fallback")
        );
        assert_eq!(
            qualified_reference_completeness(
                Completeness::partial_truncated(PartialReason::Cap),
                true,
            ),
            Completeness::partial_semantic("qualified_fallback")
        );

        let freshness = Completeness::partial_truncated("file_not_indexed_or_snapshot_stale");
        assert_eq!(
            qualified_reference_completeness(freshness.clone(), true),
            freshness
        );
        let tier = Completeness::partial_semantic(PartialReason::Tier2Warming);
        assert_eq!(qualified_reference_completeness(tier.clone(), true), tier);
        assert_eq!(
            qualified_reference_completeness(Completeness::Complete, false),
            Completeness::Complete
        );
    }

    #[test]
    fn unresolved_call_only_replaces_complete_or_cap_reason() {
        assert_eq!(
            call_graph_completeness(Completeness::Complete, true),
            Completeness::partial_semantic("call_graph_unresolved")
        );
        assert_eq!(
            call_graph_completeness(Completeness::partial_truncated(PartialReason::Cap), true),
            Completeness::partial_semantic("call_graph_unresolved")
        );

        let freshness = Completeness::partial_truncated("file_not_indexed_or_snapshot_stale");
        assert_eq!(call_graph_completeness(freshness.clone(), true), freshness);
        let tier = Completeness::partial_semantic(PartialReason::Tier2Warming);
        assert_eq!(call_graph_completeness(tier.clone(), true), tier);
        assert_eq!(
            call_graph_completeness(Completeness::Complete, false),
            Completeness::Complete
        );
    }

    #[tokio::test]
    async fn exact_limit_is_complete_and_over_limit_is_partial() {
        assert_limit_probe(
            &FindReferences,
            json!({"repo": "demo", "symbol": "target", "kind": "call", "include_noise": true, "limit": 3}),
            json!({"repo": "demo", "symbol": "target", "kind": "call", "include_noise": true, "limit": 2}),
        )
        .await;
    }

    #[tokio::test]
    async fn repo_none_searches_all_registered_repos_and_caps_accumulated_total() {
        let fixture = cross_repo_fixture();

        let all = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({"symbol": "target", "kind": "call", "include_noise": true, "limit": 10, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        let items = all["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert!(
            items
                .iter()
                .any(|h| h["location"].as_str().unwrap().starts_with("alpha:HEAD:"))
        );
        assert!(
            items
                .iter()
                .any(|h| h["location"].as_str().unwrap().starts_with("beta:HEAD:"))
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(all["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let capped = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({"symbol": "target", "kind": "call", "include_noise": true, "limit": 2, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_eq!(capped["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(capped["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }

    #[tokio::test]
    async fn capped_qualified_fallback_keeps_one_cap_hint() {
        let fixture = cross_repo_fixture();
        let capped = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "symbol": "Unrelated::target",
                    "kind": "call",
                    "include_noise": true,
                    "limit": 2,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();

        assert_eq!(capped["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(capped["completeness"].clone()).unwrap(),
            Completeness::partial_semantic("qualified_fallback")
        );
        assert_eq!(
            capped["hints"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|hint| hint["code"] == "capped_increase_limit")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn final_trim_drops_fallback_status_with_the_fallback_row() {
        let fixture = qualified_final_trim_fixture();

        let references = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "symbol": "Exact.target",
                    "kind": "call",
                    "limit": 1,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert_exact_survivor_with_cap(&references);

        let callers = FindCallers
            .dispatch(
                &fixture.ctx,
                json!({
                    "name": "Exact.target",
                    "limit": 1,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert_exact_survivor_with_cap(&callers);
    }

    #[tokio::test]
    async fn final_trim_drops_unresolved_status_with_the_unresolved_repo() {
        let fixture = call_graph_final_trim_fixture();

        let references = FindReferences
            .dispatch(
                &fixture.ctx,
                json!({
                    "symbol": "caller",
                    "direction": "outgoing",
                    "kind": "call",
                    "limit": 1,
                    "anchor": "HEAD"
                }),
            )
            .await
            .unwrap();
        assert_resolved_survivor_with_cap(&references);

        let callees = FindCallees
            .dispatch(
                &fixture.ctx,
                json!({"name": "caller", "limit": 1, "anchor": "HEAD"}),
            )
            .await
            .unwrap();
        assert_resolved_survivor_with_cap(&callees);
    }

    #[test]
    fn extract_line_returns_target_line_text() {
        let src = b"alpha\nbeta\ngamma\n";
        assert_eq!(extract_line(src, 1).as_deref(), Some("alpha"));
        assert_eq!(extract_line(src, 2).as_deref(), Some("beta"));
        assert_eq!(extract_line(src, 3).as_deref(), Some("gamma"));
    }

    #[test]
    fn extract_line_handles_trailing_no_newline() {
        let src = b"alpha\nbeta";
        assert_eq!(extract_line(src, 2).as_deref(), Some("beta"));
    }

    #[test]
    fn extract_line_returns_none_past_end() {
        let src = b"alpha\nbeta\n";
        assert_eq!(extract_line(src, 5), None);
        assert_eq!(extract_line(src, 0), None);
    }

    #[test]
    fn snippet_cache_omits_changed_worktree_fallback_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("source.rs"), b"changed\n").unwrap();
        let indexed_sha = crate::cas::hash::git_blob_sha(b"indexed\n");
        let mut cache = SnippetCache::new("demo".into(), tmp.path().to_path_buf());

        assert_eq!(cache.line_for(&indexed_sha, "source.rs", 1), None);
    }

    struct CrossRepoFixture {
        _repos: Vec<tempfile::TempDir>,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn cross_repo_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "pub fn target() {}\n\
             pub fn caller_a() { target(); }\n\
             pub fn caller_b() { target(); }\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "pub fn target() {}\n\
             pub fn caller_c() { target(); }\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn qualified_final_trim_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "pub fn target() {}\npub fn alpha_caller() { target(); }\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "pub fn target() {}\npub fn beta_caller() { target(); }\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());
        set_target_qualified(&cas, alpha.path(), Some("Exact.target"));
        set_target_qualified(&cas, beta.path(), None);

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn call_graph_final_trim_fixture() -> CrossRepoFixture {
        let alpha = init_repo(&[(
            "src/alpha.rs",
            "pub fn first() {}\n\
             pub fn second() {}\n\
             pub fn caller() { first(); second(); }\n",
        )])
        .0;
        let beta = init_repo(&[(
            "src/beta.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn caller(arg: Widget) { arg.render(); }\n",
        )])
        .0;
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        register_alias(&cas, "alpha", alpha.path());
        register_alias(&cas, "beta", beta.path());

        CrossRepoFixture {
            _repos: vec![alpha, beta],
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    fn set_target_qualified(cas: &CasDataDir, repo_path: &std::path::Path, value: Option<&str>) {
        let canonical = std::fs::canonicalize(repo_path).unwrap();
        let repo_hash = path_hash(&canonical);
        let conn = cas_store::open(&cas.store_db_path(&repo_hash)).unwrap();
        let changed = conn
            .execute(
                "UPDATE refs SET target_qualified = ?1
                  WHERE target_name = 'target' AND kind = 'call'",
                rusqlite::params![value],
            )
            .unwrap();
        assert_eq!(changed, 1, "fixture must contain exactly one target call");
    }

    fn assert_exact_survivor_with_cap(value: &Value) {
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{value:#}");
        assert!(
            items[0]["location"]
                .as_str()
                .unwrap()
                .starts_with("alpha:HEAD:"),
            "global final trim must retain the deterministic alpha exact row: {value:#}"
        );
        assert_eq!(items[0]["target_qualified"], "Exact.target");
        assert_eq!(
            serde_json::from_value::<Completeness>(value["completeness"].clone()).unwrap(),
            Completeness::partial_truncated(PartialReason::Cap)
        );
        let cap_hints = value["hints"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|hint| hint["code"] == "capped_increase_limit")
            .collect::<Vec<_>>();
        assert_eq!(cap_hints.len(), 1, "{value:#}");
        assert_eq!(cap_hints[0]["action"], "increase_limit");
    }

    fn assert_resolved_survivor_with_cap(value: &Value) {
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{value:#}");
        assert!(
            items[0]["location"]
                .as_str()
                .unwrap()
                .starts_with("alpha:HEAD:"),
            "global final trim must retain a deterministic resolved row: {value:#}"
        );
        assert_eq!(
            serde_json::from_value::<Completeness>(value["completeness"].clone()).unwrap(),
            Completeness::partial_truncated(PartialReason::Cap)
        );
        assert_eq!(
            value["hints"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|hint| hint["code"] == "capped_increase_limit")
                .count(),
            1,
            "{value:#}"
        );
    }

    fn register_alias(cas: &CasDataDir, alias: &str, repo_path: &std::path::Path) {
        let canonical = std::fs::canonicalize(repo_path).unwrap();
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
        cas_registry::upsert(&tx, alias, &canonical.to_string_lossy(), &repo_hash, now_ns).unwrap();
        tx.commit().unwrap();
    }
}
