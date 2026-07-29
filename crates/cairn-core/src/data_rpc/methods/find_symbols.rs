//! `find_symbols` — anchor → manifest → symbols JOIN, scoped by the
//! caller's `repo` / `branch` / `query` / `kind` / `container` /
//! `path` filters. `repo = None` walks every registered alias.
//!
//! With `include_inherited`, the manifest's resolved and uniquely
//! provable type-relation edges expand `container` before the ordinary
//! result filters and limit are applied.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cairn_proto::common::{Completeness, PartialReason};
use cairn_proto::methods::{FindSymbolArgs, FindSymbolHit, FindSymbolResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::cas::kind_conv::symbol_kind_to_str;
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, limit_with_probe,
    parser_id_filter, query_one_or_all_snapshots,
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
        let include_inherited = args.include_inherited && args.container.is_some();
        let inheritance_unresolved = Arc::new(AtomicBool::new(false));
        let tier2_warming = Arc::new(AtomicBool::new(false));
        let query_inheritance_unresolved = Arc::clone(&inheritance_unresolved);
        let query_tier2_warming = Arc::clone(&tier2_warming);

        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo,
                anchor: anchor_arg,
                branch: branch_arg,
                method_name: "find_symbols",
                effective_limit,
                verbose_tier3: args.tier3.verbose_tier3,
                exact_file: None,
            },
            move |entry, conn, snapshot| {
                let anchor_label = snapshot.anchor.as_str().to_string();
                let outcome =
                    query::find_symbols_with_status(conn, &snapshot.anchor, &q, include_inherited)?;
                query_inheritance_unresolved
                    .fetch_or(outcome.inheritance_unresolved, Ordering::Relaxed);
                query_tier2_warming.fetch_or(outcome.tier2_warming, Ordering::Relaxed);
                Ok(outcome
                    .hits
                    .into_iter()
                    .map(|hit| (entry.alias.clone(), anchor_label.clone(), hit))
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|(_, _, hit)| hit.parser_id.clone())),
            |out: &mut Vec<(String, String, SymbolHit)>| {
                // Deterministic cross-repo ordering: named languages
                // first (unknown sinks to the end), then path → line
                // → repo → qualified. Sorting by path before repo
                // keeps hits from the same file adjacent even when
                // several repos contribute overlapping paths.
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
        let items: Vec<_> = execution
            .items
            .into_iter()
            .map(|(repo, anchor_label, h)| into_wire_hit(&repo, &anchor_label, h, signature_only))
            .collect();
        let tier3_status = execution.tier3_status;
        let freshness_issues = execution.freshness_issues;
        let completeness = inherited_completeness(
            completeness_for_snapshot_scan(
                execution.capped,
                execution.skipped_unavailable,
                &freshness_issues,
            ),
            include_inherited,
            tier2_warming.load(Ordering::Relaxed),
            inheritance_unresolved.load(Ordering::Relaxed),
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::FindSymbols,
            items_empty: items.is_empty(),
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: args.fuzzy,
                kind: args.kind.is_some(),
                container: args.container.as_deref(),
                path: args.path.as_deref(),
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);

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

fn inherited_completeness(
    completeness: Completeness,
    include_inherited: bool,
    tier2_warming: bool,
    inheritance_unresolved: bool,
) -> Completeness {
    if !include_inherited || (!tier2_warming && !inheritance_unresolved) {
        return completeness;
    }

    // Snapshot freshness and analyzer failures remain stronger than
    // graph-local uncertainty. A cap is weaker: inherited resolution
    // can omit an entire branch, so its semantic reason takes priority.
    let replace = matches!(completeness, Completeness::Complete)
        || matches!(
            completeness,
            Completeness::Partial {
                reason: Some(PartialReason::Cap),
                ..
            }
        );
    if !replace {
        return completeness;
    }
    if tier2_warming {
        Completeness::partial_semantic(PartialReason::Tier2Warming)
    } else {
        Completeness::partial_semantic("inheritance_unresolved")
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
        file: h.path,
        line: h.line,
        language: h.language,
        // `signature_only=true` drops the heaviest field. The naming
        // mirrors `GetSymbolSourceArgs.signature_only`; here the
        // analogous "minimal navigation payload" is everything *but*
        // the signature.
        signature: if signature_only { None } else { h.signature },
        source: h.source_tier,
    }
}

/// Sort key that keeps named languages ahead of `None` (unknown /
/// unindexed language). The leading `bool` acts as the primary key
/// — `false < true` — so a language-tagged hit sorts before an
/// untagged one; secondary comparison is the language name itself.
fn language_sort_key(language: Option<&str>) -> (bool, &str) {
    match language {
        Some(lang) => (false, lang),
        None => (true, ""),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::common::{SourceTier, SymbolKind};
    use rusqlite::{Connection, params};
    use serde_json::json;

    use super::*;
    use crate::anchor;
    use crate::cas::{registry as cas_registry, store};
    use crate::data_rpc::helpers::test_support::{
        DataRpcFixture, assert_limit_probe, registered_fixture_with_files,
    };
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

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

    #[test]
    fn find_symbols_hit_includes_file_and_line_alongside_location() {
        let hit = SymbolHit {
            id: 1,
            name: "semantic_fn".into(),
            qualified: "semantic_fn".into(),
            kind: SymbolKind::Function,
            signature: None,
            visibility: None,
            path: "src/lib.rs".into(),
            line: 7,
            blob_sha: "sha".into(),
            parser_id: "tree-sitter-rust".into(),
            language: Some("rust".into()),
            source_tier: SourceTier::Semantic,
        };

        let wire = into_wire_hit("demo", "HEAD", hit, false);
        assert_eq!(wire.location, "demo:HEAD:src/lib.rs:7");
        assert_eq!(wire.file, "src/lib.rs");
        assert_eq!(wire.line, 7);
    }

    #[test]
    fn find_symbols_hit_file_matches_location_path_component() {
        let hit = SymbolHit {
            id: 1,
            name: "Intro".into(),
            qualified: "Intro".into(),
            kind: SymbolKind::Section,
            signature: None,
            visibility: None,
            path: "README.md".into(),
            line: 1,
            blob_sha: "sha".into(),
            parser_id: "tree-sitter-md".into(),
            language: Some("markdown".into()),
            source_tier: SourceTier::Syntactic,
        };

        let wire = into_wire_hit("demo", "HEAD", hit, false);
        let location_path = wire.location.rsplit_once(':').unwrap().0;
        assert!(location_path.ends_with(&wire.file));
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

    #[tokio::test]
    async fn include_inherited_adds_parent_declarations() {
        let fixture = registered_fixture_with_files(&[(
            "src/types.ts",
            "class Base { inherited() {} }\n\
             class Child extends Base { own() {} }\n",
        )]);

        let own_only = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": false,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        let with_inherited = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();

        let names = |value: &Value| {
            value["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["name"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&own_only), vec!["own"]);
        assert_eq!(names(&with_inherited), vec!["inherited", "own"]);
    }

    #[tokio::test]
    async fn inherited_union_is_transitive_additive_and_diamond_safe() {
        let fixture = registered_fixture_with_files(&[(
            "src/types.ts",
            "interface Root { rootOnly(): void; shared(): void; }\n\
             interface Left extends Root { same(): void; leftOnly(): void; }\n\
             interface Right extends Root { same(): void; rightOnly(): void; }\n\
             class Child implements Left, Right { same() {} own() {} }\n",
        )]);

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 20,
                }),
            )
            .await
            .unwrap();
        let names = item_names(&value);

        assert!(names.contains(&"rootOnly".to_string()), "{value:#}");
        assert!(names.contains(&"leftOnly".to_string()), "{value:#}");
        assert!(names.contains(&"rightOnly".to_string()), "{value:#}");
        assert!(names.contains(&"own".to_string()), "{value:#}");
        assert_eq!(
            names
                .iter()
                .filter(|name| name.as_str() == "shared")
                .count(),
            1,
            "the shared diamond ancestor is one physical declaration: {value:#}"
        );
        assert_eq!(
            names.iter().filter(|name| name.as_str() == "same").count(),
            3,
            "child and both parent declarations remain additive: {value:#}"
        );
    }

    #[tokio::test]
    async fn inherited_filters_and_limit_apply_after_graph_expansion() {
        let fixture = registered_fixture_with_files(&[
            (
                "src/a-base.ts",
                "class Base { inherited() {} another() {} }\n",
            ),
            (
                "src/z-child.ts",
                "class Child extends Base { own() {} ownTwo() {} }\n",
            ),
        ]);

        let limited = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 1,
                }),
            )
            .await
            .unwrap();
        assert_eq!(limited["items"].as_array().unwrap().len(), 1);
        assert_eq!(limited["items"][0]["file"], "src/a-base.ts");
        assert_ne!(limited["items"][0]["name"], "own");
        assert_eq!(limited["completeness"]["reason"], "cap");

        let exact = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "query": "inherited",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "path": "src/a-",
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&exact), vec!["inherited"]);

        let fuzzy = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "query": "inherit*",
                    "fuzzy": true,
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&fuzzy), vec!["inherited"]);
    }

    #[tokio::test]
    async fn inheritance_cycles_finish_and_extension_edges_are_excluded() {
        let fixture = registered_fixture_with_files(&[(
            "src/types.ts",
            "class A { fromA() {} }\n\
             class B { fromB() {} }\n\
             class Mixin { mixed() {} }\n\
             class Extension { extended() {} }\n\
             class Child { own() {} }\n",
        )]);
        let conn = demo_store(&fixture);
        insert_relation(&conn, "Child", "A", "inherit");
        insert_relation(&conn, "A", "B", "inherit");
        insert_relation(&conn, "B", "A", "inherit");
        insert_relation(&conn, "A", "A", "inherit");
        insert_relation(&conn, "Child", "Mixin", "mixin");
        insert_relation(&conn, "Child", "Extension", "extension");

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 20,
                }),
            )
            .await
            .unwrap();
        let names = item_names(&value);
        assert!(names.contains(&"fromA".to_string()), "{value:#}");
        assert!(names.contains(&"fromB".to_string()), "{value:#}");
        assert!(names.contains(&"mixed".to_string()), "{value:#}");
        assert!(!names.contains(&"extended".to_string()), "{value:#}");
        assert_eq!(value["completeness"]["status"], "complete");
    }

    #[tokio::test]
    async fn ambiguous_fact_only_parent_is_partial_and_fail_closed() {
        let fixture = registered_fixture_with_files(&[(
            "src/types.ts",
            "class Base { inherited() {} }\n\
             class Child extends Base { own() {} }\n",
        )]);
        let conn = demo_store(&fixture);
        duplicate_owner(&conn, "Base");

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&value), vec!["own"]);
        assert_eq!(value["completeness"]["status"], "partial");
        assert_eq!(value["completeness"]["reason"], "inheritance_unresolved");
    }

    #[tokio::test]
    async fn resolved_parent_identity_beats_ambiguous_fact_spelling() {
        let fixture = registered_fixture_with_files(&[(
            "src/types.ts",
            "class Base { inherited() {} }\n\
             class Child extends Base { own() {} }\n",
        )]);
        let conn = demo_store(&fixture);
        let base_id = conn
            .query_row(
                "SELECT id FROM symbols WHERE qualified = 'Base' ORDER BY id LIMIT 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        duplicate_owner(&conn, "Base");
        conn.execute(
            "UPDATE resolutions
                SET target_symbol_id = ?1
              WHERE kind = 'type'
                AND semantic_kind = 'inherit'",
            [base_id],
        )
        .unwrap();

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&value), vec!["inherited", "own"]);
        assert_eq!(value["completeness"]["status"], "complete");
    }

    #[tokio::test]
    async fn syntactic_only_container_reports_tier2_warming() {
        let fixture =
            registered_fixture_with_files(&[("src/types.ts", "class Child { own() {} }\n")]);
        let conn = demo_store(&fixture);
        conn.execute("UPDATE blobs SET analyzer_id = NULL", [])
            .unwrap();

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&value), vec!["own"]);
        assert_eq!(value["completeness"]["status"], "partial");
        assert_eq!(value["completeness"]["reason"], "tier2_warming");
    }

    #[tokio::test]
    async fn missing_external_parent_stops_without_partiality() {
        let fixture = registered_fixture_with_files(&[(
            "src/types.ts",
            "class Child extends External { own() {} }\n",
        )]);

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&value), vec!["own"]);
        assert_eq!(value["completeness"]["status"], "complete");
    }

    #[tokio::test]
    async fn fact_only_parent_proof_does_not_cross_language_families() {
        let fixture = registered_fixture_with_files(&[
            (
                "src/types.ts",
                "class Child extends CrossLanguage { own() {} }\n",
            ),
            (
                "src/types.py",
                "class CrossLanguage:\n    def foreign(self):\n        pass\n",
            ),
        ]);

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&value), vec!["own"]);
        assert_eq!(value["completeness"]["status"], "complete");
    }

    #[tokio::test]
    async fn inheritance_edges_are_scoped_to_the_selected_manifest() {
        let fixture = registered_fixture_with_files(&[
            ("src/child.ts", "class Child { own() {} }\n"),
            (
                "src/other-parent.ts",
                "class OtherParent { foreign() {} }\n",
            ),
        ]);
        let conn = demo_store(&fixture);
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (99, 'committed', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs
                (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('other-manifest-sha', 'tree-sitter-typescript', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (99, 'src/edge-only.ts', 'other-manifest-sha')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO implementations
                (blob_sha, parser_id, type_qualified, interface_qualified, kind, line)
             VALUES
                ('other-manifest-sha', 'tree-sitter-typescript',
                 'Child', 'OtherParent', 'inherit', 1)",
            [],
        )
        .unwrap();

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        assert_eq!(item_names(&value), vec!["own"]);
        assert_eq!(value["completeness"]["status"], "complete");
    }

    #[tokio::test]
    async fn inherited_union_keeps_repository_ownership_isolated() {
        let fixture = multi_repo_fixture(&[
            (
                "left",
                &[
                    ("src/base.ts", "class Base { leftInherited() {} }\n"),
                    (
                        "src/child.ts",
                        "class Child extends Base { leftOwn() {} }\n",
                    ),
                ],
            ),
            (
                "right",
                &[
                    ("src/base.ts", "class Base { rightInherited() {} }\n"),
                    (
                        "src/child.ts",
                        "class Child extends Base { rightOwn() {} }\n",
                    ),
                ],
            ),
        ]);

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 20,
                }),
            )
            .await
            .unwrap();
        let pairs = value["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item["repo"].as_str().unwrap().to_string(),
                    item["name"].as_str().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();
        assert!(pairs.contains(&("left".into(), "leftInherited".into())));
        assert!(pairs.contains(&("left".into(), "leftOwn".into())));
        assert!(pairs.contains(&("right".into(), "rightInherited".into())));
        assert!(pairs.contains(&("right".into(), "rightOwn".into())));
        assert!(!pairs.contains(&("left".into(), "rightInherited".into())));
        assert!(!pairs.contains(&("right".into(), "leftInherited".into())));
    }

    #[tokio::test]
    async fn inherited_union_deduplicates_one_physical_symbol_at_two_paths() {
        let fixture = registered_fixture_with_files(&[
            ("src/base.ts", "class Base { inherited() {} }\n"),
            ("src/child.ts", "class Child extends Base { own() {} }\n"),
        ]);
        let conn = demo_store(&fixture);
        let (manifest_id, base_blob) = conn
            .query_row(
                "SELECT a.manifest_id, me.blob_sha
                   FROM anchors a
                   JOIN manifest_entries me
                     ON me.manifest_id = a.manifest_id
                  WHERE a.anchor_name LIKE 'tentative/%'
                    AND me.path = 'src/base.ts'",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, 'src/base-alias.ts', ?2)",
            params![manifest_id, base_blob],
        )
        .unwrap();

        let value = FindSymbols
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "kind": "method",
                    "container": "Child",
                    "include_inherited": true,
                    "limit": 10,
                }),
            )
            .await
            .unwrap();
        let names = item_names(&value);
        assert_eq!(
            names
                .iter()
                .filter(|name| name.as_str() == "inherited")
                .count(),
            1,
            "{value:#}"
        );
        assert_eq!(
            names.iter().filter(|name| name.as_str() == "own").count(),
            1
        );
    }

    fn item_names(value: &Value) -> Vec<String> {
        value["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["name"].as_str().unwrap().to_string())
            .collect()
    }

    fn demo_store(fixture: &DataRpcFixture) -> Connection {
        let canonical = std::fs::canonicalize(fixture._repo.path()).unwrap();
        let cas = CasDataDir::with_root(fixture._data.path().to_path_buf());
        store::open(&cas.store_db_path(&path_hash(&canonical))).unwrap()
    }

    fn insert_relation(conn: &Connection, child: &str, parent: &str, kind: &str) {
        conn.execute(
            "INSERT INTO implementations
                (blob_sha, parser_id, type_qualified, interface_qualified, kind, line)
             SELECT blob_sha, parser_id, ?1, ?2, ?3, 1
               FROM symbols
              WHERE qualified = ?1
              ORDER BY id
              LIMIT 1",
            rusqlite::params![child, parent, kind],
        )
        .unwrap();
    }

    fn duplicate_owner(conn: &Connection, qualified: &str) {
        conn.execute(
            "INSERT INTO symbols
                (blob_sha, parser_id, parent_id, name, qualified, kind, signature,
                 visibility, doc, byte_start, byte_end, line_start, line_end,
                 body_start, source, scope)
             SELECT blob_sha, parser_id, parent_id, name, qualified, kind, signature,
                    visibility, doc, byte_start, byte_end, line_start, line_end,
                    body_start, source, scope
               FROM symbols
              WHERE qualified = ?1
              ORDER BY id
              LIMIT 1",
            [qualified],
        )
        .unwrap();
    }

    struct MultiRepoFixture {
        _repos: Vec<tempfile::TempDir>,
        _data: tempfile::TempDir,
        ctx: DataCtx,
    }

    fn multi_repo_fixture(specs: &[(&str, &[(&str, &str)])]) -> MultiRepoFixture {
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let mut repos = Vec::new();

        for (alias, files) in specs {
            let (repo, _sha) = init_repo(files);
            let canonical = std::fs::canonicalize(repo.path()).unwrap();
            let repo_hash = path_hash(&canonical);
            let mut repo_store = store::open(&cas.store_db_path(&repo_hash)).unwrap();
            let registration = register_repo(&mut repo_store, &canonical, now_ns).unwrap();

            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, alias, &canonical.to_string_lossy(), &repo_hash, now_ns)
                .unwrap();
            tx.commit().unwrap();
            index
                .execute(
                    "UPDATE repo_reconcile_state
                     SET desired_generation = 1,
                         applied_generation = 1,
                         last_success_ns = ?1,
                         watcher_state = 'active'
                     WHERE repo_hash = ?2",
                    params![now_ns, repo_hash],
                )
                .unwrap();
            let tx = repo_store.transaction().unwrap();
            anchor::set_reconciled(
                &tx,
                &anchor::AnchorName::tentative(registration.worktree_id),
                registration.tentative_manifest,
                now_ns,
                1,
            )
            .unwrap();
            tx.commit().unwrap();
            repos.push(repo);
        }

        MultiRepoFixture {
            _repos: repos,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }
}
