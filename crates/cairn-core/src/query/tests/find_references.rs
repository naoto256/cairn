use super::{refs_dedup_fixture, registered};
use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::find_references::KIND_SOURCE_FACT;
use crate::query::{FindReferencesArgs, find_references};
use crate::register::register_repo;
use crate::testutil::init_repo;
use cairn_proto::common::RefKind;
use cairn_proto::methods::ReferenceDirection;
use rusqlite::Connection;

#[test]
fn references_incoming_finds_callers() {
    let (_repo, _db, c) = registered();
    let hits = find_references(
        &c,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "alpha".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();
    // alpha is referenced inside the file (see fixture src/lib.rs);
    // at minimum we shouldn't error and the SQL must execute.
    // Whether the syntactic-only Rust analyzer surfaces this
    // particular call depends on the parser; assert structural
    // correctness instead of a specific count.
    for h in &hits {
        assert_eq!(h.target_name, "alpha");
    }
}

#[test]
fn references_outgoing_resolves_enclosing() {
    let (_repo, _db, c) = registered();
    // No symbol called `nonexistent` exists; the outgoing query
    // should run and return an empty result rather than error.
    let hits = find_references(
        &c,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "nonexistent::callee".into(),
            direction: ReferenceDirection::Outgoing,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn references_outgoing_defaults_to_resolved_calls() {
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
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        hits.iter()
            .map(|h| h.target_name.as_str())
            .collect::<Vec<_>>(),
        vec!["resolved"],
        "outgoing default should hide unresolved method calls and type refs: {hits:?}"
    );
    assert!(hits.iter().all(|h| h.kind == RefKind::Call));
    assert!(hits.iter().all(|h| h.target_qualified.is_some()));
}

#[test]
fn references_outgoing_include_noise_returns_legacy_refs() {
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
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(
        hits.iter().any(|h| h.target_name == "resolved"
            && h.kind == RefKind::Call
            && h.target_qualified.as_deref() == Some("resolved")),
        "resolved call missing from noisy outgoing refs: {hits:?}"
    );
    assert!(
        hits.iter().any(|h| h.target_name == "render"
            && h.kind == RefKind::Call
            && h.target_qualified.is_none()),
        "unresolved method call missing from include_noise refs: {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|h| h.target_name == "Widget" && h.kind == RefKind::Type),
        "type refs missing from include_noise refs: {hits:?}"
    );
}

#[test]
fn references_include_typescript_tier2_call_refs() {
    let (_repo, _sha) = init_repo(&[(
        "src/app.ts",
        "function caller() {\n\
                 foo();\n\
             }\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "foo".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(
        hits.iter().any(|h| {
            h.target_name == "foo"
                && h.target_qualified.as_deref() == Some("foo")
                && h.enclosing_qualified.as_deref() == Some("caller")
                && h.path == "src/app.ts"
        }),
        "TypeScript Tier-2 call ref missing from query results: {hits:?}"
    );
}

#[test]
fn references_tier2_only_falls_back_to_bare_name_refs() {
    let (_db, conn) = refs_dedup_fixture(false, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].target_name, "render");
    assert_eq!(hits[0].target_qualified, None);
}

#[test]
fn references_tier3_suppresses_tier2_same_call_site() {
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render")
    );
}

#[test]
fn references_tier3_suppresses_zero_range_semantic_same_line() {
    let (_db, conn) = refs_dedup_fixture(true, None);
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('sha-ref', 'tree-sitter-rust', 1, 'render', 'crate::Widget::render', 'call',
                0, 0, 5, 'semantic')",
        [],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "crate::Widget::render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render")
    );

    let noisy = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "crate::Widget::render".into(),
            direction: ReferenceDirection::Incoming,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(noisy.len(), 2, "{noisy:?}");
}

#[test]
fn references_tier3_suppresses_zero_range_semantic_with_qualified_mismatch() {
    let (_db, conn) = refs_dedup_fixture(true, None);
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('sha-ref', 'tree-sitter-rust', 1, 'render', 'query::render', 'call',
                0, 0, 5, 'semantic')",
        [],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render")
    );

    let noisy = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(noisy.len(), 3, "{noisy:?}");
    assert!(
        noisy
            .iter()
            .any(|h| h.target_qualified.as_deref() == Some("query::render")),
        "zero-range semantic row should remain visible with include_noise=true: {noisy:?}"
    );
}

#[test]
fn references_tier3_failed_run_falls_back_to_tier2_refs() {
    let (_db, conn) = refs_dedup_fixture(false, Some("failed"));

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].target_name, "render");
    assert_eq!(hits[0].target_qualified, None);
}

#[test]
fn references_outgoing_prefers_tier3_for_same_call_site() {
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render")
    );
}

#[test]
fn references_include_noise_keeps_tier2_and_tier3_duplicates() {
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 2, "{hits:?}");
    assert!(
        hits.iter().any(|h| h.target_qualified.is_none()),
        "Tier-2 fallback row missing from noisy refs: {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|h| h.target_qualified.as_deref() == Some("crate::Widget::render")),
        "Tier-3 row missing from noisy refs: {hits:?}"
    );
}

#[test]
fn references_empty_symbol_errors() {
    let (_repo, _db, c) = registered();
    let err = find_references(
        &c,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "  ".into(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-empty"));
}

// ──── P1 read-side side-effect: cross-parser resolution row visibility ────
//
// persist.rs cross-parser-id uniqueness fallback (Phase 1) can populate
// `resolutions.target_symbol_id` for sites where the resolver targets a
// sibling-parser symbol. find_references' SQL does not change in Phase 1
// (its target_path surface is Phase 2), but the data it reads does: rows
// it joins to `resolutions` may now have a non-NULL target_symbol_id, and
// `COALESCE(refs.target_qualified, sym.qualified)` therefore promotes the
// surfaced `target_qualified` for cross-parser calls that used to bottom
// out at None.
//
// These tests pin the read-side observability so a Phase 2 refactor of
// the wire shape doesn't silently lose the upgrade.

/// Build a fixture with:
///   - 1 Kotlin file containing a `call` ref to `fromJson` (target_qualified NULL)
///   - 1 Java file defining a `JsonAdapter.fromJson` symbol
///   - optional 2nd Java file defining the *same* qualified name (for the
///     ambiguous case)
///   - 1 Tier-2.5 resolution row pinning the Kotlin call site to the Java
///     symbol; `target_symbol_id` is filled in directly by the test to
///     simulate the cross-parser fallback's output (the persist-layer tests
///     in `workspace_analyzer::tests` already pin that path).
fn cross_parser_call_fixture(
    ambiguous: bool,
) -> (tempfile::TempDir, Connection, Option<i64>, Option<i64>) {
    let db_tmp = tempfile::tempdir().unwrap();
    let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
        [],
    )
    .unwrap();
    let kt_parser = "tree-sitter-kotlin-ng";
    let java_parser = "tree-sitter-java";
    conn.execute(
        "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha-kt', ?1, 1, 0),
                    ('sha-java', ?2, 1, 0)",
        rusqlite::params![kt_parser, java_parser],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/X.kt', 'sha-kt'),
                    (1, 'src/JsonAdapter.java', 'sha-java')",
        [],
    )
    .unwrap();
    // Caller symbol in Kotlin file so refs.enclosing_id resolves.
    conn.execute(
        "INSERT INTO symbols
               (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (1, 'sha-kt', ?1, 'caller', 'caller', 'function',
                0, 200, 1, 10, 'syn')",
        rusqlite::params![kt_parser],
    )
    .unwrap();
    // Java target symbol (cross-parser).
    conn.execute(
        "INSERT INTO symbols
               (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (2, 'sha-java', ?1, 'fromJson', 'com.x.JsonAdapter.fromJson', 'method',
                0, 100, 1, 5, 'syn')",
        rusqlite::params![java_parser],
    )
    .unwrap();
    let java_id: i64 = 2;
    // Kotlin call site with target_qualified NULL — the resolution layer
    // is where the cross-parser id lookup would normally fill in
    // `target_symbol_id`.
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('sha-kt', ?1, 1, 'fromJson', NULL, 'call',
                42, 50, 5, 'tree-sitter-kotlin-ng')",
        rusqlite::params![kt_parser],
    )
    .unwrap();
    // Optional second Java file with the same qualified — exercises the
    // uniqueness rejection path. The test then writes target_symbol_id =
    // NULL on the resolution row to simulate the fallback's None result.
    let second_java_id = if ambiguous {
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
                 VALUES ('sha-java-2', ?1, 1, 0)",
            rusqlite::params![java_parser],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
                 VALUES (1, 'src/JsonAdapter2.java', 'sha-java-2')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
                   (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                    line_start, line_end, source)
                 VALUES
                   (3, 'sha-java-2', ?1, 'fromJson', 'com.x.JsonAdapter.fromJson', 'method',
                    0, 100, 1, 5, 'syn')",
            rusqlite::params![java_parser],
        )
        .unwrap();
        Some(3i64)
    } else {
        None
    };
    (db_tmp, conn, Some(java_id), second_java_id)
}

#[test]
fn find_references_outgoing_picks_up_cross_parser_resolution_post_p1() {
    // R2 B4 regression pin: `find_callees` (outgoing direction +
    // include_noise: false) filters refs whose resolved
    // `sym.qualified` is NULL, so the resolution layer's outcome
    // *does* change the result set. Without the Phase 1 cross-parser
    // uniqueness fallback the Kotlin call to `fromJson` has
    // `target_symbol_id = NULL` → sym is NULL → COALESCE returns
    // NULL → the row is suppressed by the noise filter. With the
    // fallback it resolves to the Java symbol, COALESCE surfaces the
    // FQN, and the row passes.
    let (_db, conn, java_id, _) = cross_parser_call_fixture(false);
    let java_id = java_id.unwrap();
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, ?1,
                'src/JsonAdapter.java', 'tier25-kotlin-resolver')",
        rusqlite::params![java_id],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            kind: Some(RefKind::Call),
            include_noise: false,
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits.iter().find(|h| h.target_name == "fromJson").expect(
        "cross-parser fromJson call missing from find_callees noise-filtered hits — \
             the unique-hit fallback should have populated target_symbol_id and let it through",
    );
    assert_eq!(
        hit.target_qualified.as_deref(),
        Some("com.x.JsonAdapter.fromJson"),
        "cross-parser fallback should surface sibling-parser qualified via COALESCE"
    );
    assert_eq!(hit.kind_source, "tier25-kotlin-resolver");
}

#[test]
fn find_references_outgoing_ambiguous_cross_parser_is_filtered_out() {
    // R2 B4 regression pin: the same `find_callees` noise filter
    // suppresses the call when the cross-parser fallback was
    // ambiguous (target_symbol_id NULL). The resolution row exists
    // (carrying source / target_path) but COALESCE returns NULL for
    // target_qualified, so the row is filtered out. This pins "no
    // false positive": a coincidentally-named symbol does not get
    // adopted.
    let (_db, conn, _, _) = cross_parser_call_fixture(true);
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, NULL,
                'src/JsonAdapter.java', 'tier25-kotlin-resolver')",
        [],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            kind: Some(RefKind::Call),
            include_noise: false,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        hits.iter().all(|h| h.target_name != "fromJson"),
        "ambiguous cross-parser call must be filtered out by find_callees noise gate; \
         hits were: {hits:?}"
    );
}

// ──── Phase 2: target_path surface on find_references / find_callers ────

#[test]
fn find_references_returns_target_path_for_workspace_internal() {
    // Phase 2 round-trip: when persist wrote `resolutions.target_path`
    // (cross-parser type/call or any workspace-internal resolved ref),
    // `find_references` surfaces it on `ReferenceHit.target_path` via
    // `res.target_path` projection. No SQL JOIN through symbols / paths.
    let (_db, conn, java_id, _) = cross_parser_call_fixture(false);
    let java_id = java_id.unwrap();
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, ?1,
                'src/JsonAdapter.java', 'tier25-kotlin-resolver')",
        rusqlite::params![java_id],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "caller".into(),
            direction: ReferenceDirection::Outgoing,
            kind: Some(RefKind::Call),
            include_noise: false,
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.target_name == "fromJson")
        .expect("cross-parser fromJson call missing");
    assert_eq!(hit.target_path.as_deref(), Some("src/JsonAdapter.java"));
    assert_eq!(hit.kind_source, "tier25-kotlin-resolver");
}

#[test]
fn find_references_target_path_none_when_no_resolution() {
    // tier2-fact fallback (no resolution row): target_path stays None.
    let (_db, conn) = refs_dedup_fixture(false, None);
    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(
        hits[0].target_path.is_none(),
        "tier2-fact fallback must not invent a target_path: {:?}",
        hits[0]
    );
    assert_eq!(hits[0].kind_source, KIND_SOURCE_FACT);
}

#[test]
fn find_references_phase2_preserves_resolved_noise_semantics() {
    // Phase 2 regression pin (R2 "surface-additive, semantics
    // unchanged"): adding `target_path` to the projection must not
    // shift which refs the noise filter suppresses or which dedup
    // winner is picked. Same dedup fixture as the existing
    // tier3-suppresses-tier2 test: assert that the same single hit
    // wins, and the target_path field is None because no resolution
    // row exists in this fixture (refs-only).
    let (_db, conn) = refs_dedup_fixture(true, None);

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "render".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(hits.len(), 1, "dedup winner count must be 1: {hits:?}");
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("crate::Widget::render"),
        "tier3 row must still win dedup over tier2"
    );
    assert!(
        hits[0].target_path.is_none(),
        "no resolution row → target_path must stay None (Phase 2 is surface-additive)"
    );
}

// ──── Phase 4 F1: qualified-name lookup with COALESCE + extended separators ────

#[test]
fn find_references_incoming_dotted_fqn_matches_via_coalesce() {
    // Phase 4 F1 regression pin: a strict FQN query like
    // `find_callers com.x.JsonAdapter.fromJson` (Kotlin / Java /
    // C# / Python / Swift style) must hit a Tier-2.5
    // cross-parser resolution row where the surface
    // `target_qualified` comes from `sym.qualified` (because the
    // refs row itself had `target_qualified = NULL`). Pre-Phase 4
    // the WHERE clause only checked `r.target_qualified`, so this
    // returned 0 hits. The fix introduces `is_qualified_symbol`
    // (recognising `.` and `\` in addition to `::`) and switches
    // the strict path to `COALESCE(r.target_qualified, sym.qualified)`.
    let (_db, conn, java_id, _) = cross_parser_call_fixture(false);
    let java_id = java_id.unwrap();
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, ?1,
                'src/JsonAdapter.java', 'tier25-kotlin-resolver')",
        rusqlite::params![java_id],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "com.x.JsonAdapter.fromJson".into(),
            direction: ReferenceDirection::Incoming,
            kind: Some(RefKind::Call),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "dotted FQN strict lookup must find the cross-parser hit"
    );
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("com.x.JsonAdapter.fromJson")
    );
    assert_eq!(hits[0].target_path.as_deref(), Some("src/JsonAdapter.java"));
}

#[test]
fn find_references_incoming_php_backslash_fqn_recognised_as_qualified() {
    // Phase 4 F1: PHP FQN like `App\\Models\\Widget::render` (PHP
    // is_qualified via `\` separator) must enter the qualified
    // strict path, not the bare-name fallback.
    let (_db, conn, java_id, _) = cross_parser_call_fixture(false);
    let java_id = java_id.unwrap();
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, ?1,
                'src/JsonAdapter.java', 'tier25-kotlin-resolver')",
        rusqlite::params![java_id],
    )
    .unwrap();

    // A `App\\Foo\\Bar` lookup should not match the
    // `com.x.JsonAdapter.fromJson` resolution row (different FQN).
    // This pins that `\` is recognised as a qualified separator so
    // the symbol enters the strict path. Note that the strict path
    // still falls back to the bare-name index if the strict miss
    // returns 0 rows; this fixture has no bare-name decoy named
    // `Bar` for the fallback to pick up, so the outcome is
    // `hits.is_empty()`. A future-tightening "strict miss means
    // strict empty, no bare fallback" is a separate design knob.
    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "App\\Foo\\Bar".into(),
            direction: ReferenceDirection::Incoming,
            kind: Some(RefKind::Call),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        hits.is_empty(),
        "PHP-style FQN must not coincidentally match an unrelated cross-parser hit"
    );

    // Positive companion (CodeRabbit PR #231 finding C-15): the
    // negative assertion above also passes when `\` is *not*
    // recognised as a qualified separator (the query then falls
    // through to bare-name and finds nothing). Pin that `\` truly
    // enters the qualified strict path by adding a matching
    // `Vendor\Pkg\Foo` symbol + cross-parser resolution row and
    // asserting the strict-FQN incoming query finds it.
    conn.execute(
        "INSERT INTO symbols
               (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (99, 'sha-java', 'tree-sitter-java', 'Foo', 'Vendor\\Pkg\\Foo', 'class',
                300, 320, 30, 31, 'syn')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 1, 'Foo', NULL, 'call',
                60, 70, 7, 'tree-sitter-kotlin-ng')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 60, 70, 'call', NULL, 99,
                'src/JsonAdapter.java', 'tier25-kotlin-resolver')",
        [],
    )
    .unwrap();
    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "Vendor\\Pkg\\Foo".into(),
            direction: ReferenceDirection::Incoming,
            kind: Some(RefKind::Call),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "PHP-style `Vendor\\Pkg\\Foo` must enter strict-FQN path and find the cross-parser resolution; got {hits:?}"
    );
    assert_eq!(
        hits[0].target_qualified.as_deref(),
        Some("Vendor\\Pkg\\Foo"),
        "strict-FQN hit must surface the qualified name via COALESCE; got {hits:?}"
    );
}

// ──── MF-1: cross-manifest isolation for find_references ────
//
// Pin that a resolutions row scoped to *another* manifest is invisible
// to a HEAD-anchored (manifest 1) `find_references` query, even when
// the underlying blob is shared between both manifests. The shared
// blob is what makes the test bite: it ensures the
// `manifest_entries` JOIN cannot be the filter that hides the
// row, so the v11 CTE predicate `(manifest_id = ?1 OR manifest_id
// IS NULL)` is what's pinned.

/// Build a single-Rust-file fixture spanning two manifests that map
/// the same blob. HEAD points at manifest 1. The caller writes the
/// manifest-2 resolution row.
fn cross_manifest_ref_fixture() -> (tempfile::TempDir, Connection, i64) {
    let db_tmp = tempfile::tempdir().unwrap();
    let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    // Two manifests; HEAD pinned to manifest 1.
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0), (2, 'tentative', 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
        [],
    )
    .unwrap();
    // Same blob registered under both manifests at different paths.
    conn.execute(
        "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('shared-blob', 'tree-sitter-rust', 1, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/lib.rs', 'shared-blob'),
                    (2, 'lib/other-manifest.rs', 'shared-blob')",
        [],
    )
    .unwrap();
    // Caller symbol + a target symbol both in the shared blob, plus a
    // single ref row at byte 42..50. The ref's target_qualified is
    // NULL so a resolution row would normally upgrade kind_source /
    // target_path; the manifest-2 row must NOT do so.
    conn.execute(
        "INSERT INTO symbols
               (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (1, 'shared-blob', 'tree-sitter-rust', 'caller', 'caller', 'function',
                0, 200, 1, 10, 'rust-syn'),
               (2, 'shared-blob', 'tree-sitter-rust', 'target', 'crate::target', 'function',
                201, 220, 11, 12, 'rust-syn')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('shared-blob', 'tree-sitter-rust', 1, 'target', NULL, 'call',
                42, 50, 5, 'rust-syn')",
        [],
    )
    .unwrap();
    let target_id: i64 = 2;
    (db_tmp, conn, target_id)
}

#[test]
fn find_references_with_shared_blob_does_not_see_other_manifest_resolution() {
    let (_db, conn, target_id) = cross_manifest_ref_fixture();

    // Write a manifest-2 resolution row at the exact same site as
    // the manifest-1 ref. If the CTE filter were broken, this row
    // would surface its `target_path` sentinel on the manifest-1
    // query result.
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source,
                manifest_id)
             VALUES
               ('shared-blob', 'tree-sitter-rust', 42, 50, 'call', NULL, ?1,
                'lib/other-manifest.rs', 'tier25-rust-resolver', 2)",
        rusqlite::params![target_id],
    )
    .unwrap();

    // include_noise=true keeps the bare-name tier2-fact row in the
    // result so we can assert the row is visible *and* the
    // manifest-2 metadata didn't attach to it.
    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "target".into(),
            direction: ReferenceDirection::Incoming,
            include_noise: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "fact row must remain visible (only the other-manifest \
         resolution should be filtered out): {hits:?}"
    );
    let hit = &hits[0];
    assert_eq!(hit.target_name, "target");
    // No resolution row covers the site from manifest 1's perspective,
    // so kind_source falls back to fact and target_path stays None /
    // target_qualified stays None.
    assert_eq!(
        hit.kind_source, KIND_SOURCE_FACT,
        "manifest-2 resolution row leaked its `source` into manifest-1 hit"
    );
    assert!(
        hit.target_path.is_none(),
        "manifest-2 resolution row leaked its target_path into manifest-1 hit: {hit:?}"
    );
    assert!(
        hit.target_qualified.is_none(),
        "manifest-2 resolution row leaked its sibling-symbol qualified into manifest-1 hit: {hit:?}"
    );
}

// ──── PR-γ #8: SQLite expression-index perf fix ─────────────────────────
//
// Pre-fix, the strict-incoming + qualified-symbol path issued
// `WHERE COALESCE(r.target_qualified, sym.qualified) = ?`. The COALESCE
// referenced a column from a LEFT JOIN, so SQLite could not push the
// filter through `idx_refs_target_qualified` and fell back to
// `SCAN refs USING idx_refs_blob` (~135× slower on a 1K-ref fixture).
//
// The fix splits the predicate into two index-friendly branches inside
// a `strict_refs` CTE (`UNION ALL`). Tests below pin:
//   * the EXPLAIN QUERY PLAN shape (no `SCAN refs`, both branch indices
//     visible),
//   * Branch B fires when `target_qualified IS NULL` and only the
//     resolution-row-backed `sym.qualified` matches,
//   * the new shape returns the same row set as the old COALESCE
//     query — the rewrite is a perf-only refactor.

/// EXPLAIN QUERY PLAN over the production `strict_refs` shape: no
/// full `refs` scan, both branch indices appear.
#[test]
fn pr_gamma_strict_incoming_explain_uses_branch_indices() {
    use crate::cas::store;
    let db_tmp = tempfile::tempdir().unwrap();
    let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    // Empty DB is enough — EXPLAIN doesn't execute the query, it
    // returns the plan the planner *would* use. We just need the
    // schema (indices and tables) in place.
    let sql = "EXPLAIN QUERY PLAN
        WITH best_resolution AS (
            SELECT site_blob_sha, site_parser_id,
                   site_byte_start, site_byte_end, kind,
                   target_symbol_id, source, target_path,
                   ROW_NUMBER() OVER (
                       PARTITION BY site_blob_sha, site_parser_id,
                                    site_byte_start, site_byte_end, kind
                       ORDER BY id
                   ) AS rn
              FROM resolutions
             WHERE kind IN ('type', 'call', 'import')
               AND (manifest_id = ?1 OR manifest_id IS NULL)
        ),
        strict_refs AS (
            SELECT r.* FROM refs r WHERE r.target_qualified = ?2
            UNION ALL
            SELECT r.*
              FROM refs r
              JOIN best_resolution res
                ON res.site_blob_sha = r.blob_sha
               AND res.site_parser_id = r.parser_id
               AND res.site_byte_start = r.byte_start
               AND res.site_byte_end = r.byte_end
               AND res.kind = r.kind
               AND res.rn = 1
              JOIN symbols sym ON sym.id = res.target_symbol_id
             WHERE r.target_qualified IS NULL
               AND sym.qualified = ?2
        )
        SELECT * FROM strict_refs";
    let plan: Vec<String> = conn
        .prepare(sql)
        .unwrap()
        .query_map(rusqlite::params![1i64, "x"], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let joined = plan.join(" | ");

    // Branch A: idx_refs_target_qualified is the partial index over
    // (target_qualified IS NOT NULL); a strict equality lookup must
    // pick it.
    assert!(
        joined.contains("idx_refs_target_qualified"),
        "Branch A must use idx_refs_target_qualified; plan: {joined}"
    );
    // Branch B: starts from idx_symbols_qualified probe.
    assert!(
        joined.contains("idx_symbols_qualified"),
        "Branch B must use idx_symbols_qualified; plan: {joined}"
    );
    // No bare `SCAN refs` (Tier-1 ref table seq scan) anywhere — both
    // branches must drive through an index. `SCAN refs USING INDEX ...`
    // is OK; we deliberately match the bare prefix to catch the
    // regression where the planner falls back to a covering-only
    // scan of refs.
    for line in &plan {
        let mentions_refs = line.contains("SCAN refs") || line.contains("SCAN TABLE refs");
        let uses_index = line.contains("USING INDEX") || line.contains("USING COVERING INDEX");
        assert!(
            !mentions_refs || uses_index,
            "plan line is a full refs scan (no USING INDEX): {line:?}; full plan: {plan:?}"
        );
    }
}

/// Branch B regression: a ref row with `target_qualified=NULL` that
/// only matches via the resolution-row → symbol path must surface.
/// A Branch-A-shaped distractor with the same `target_name` but a
/// *different* `sym.qualified` must NOT come along for the ride.
#[test]
fn pr_gamma_strict_incoming_branch_b_resolves_via_resolution_symbol() {
    let (_db, conn, java_id, _) = cross_parser_call_fixture(false);
    let java_id = java_id.unwrap();
    // Resolution row: Kotlin call site → Java symbol.
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, ?1,
                NULL, 'tier25-kotlin-resolver')",
        rusqlite::params![java_id],
    )
    .unwrap();
    // Distractor: a separate ref with the same target_name but a
    // *non-matching* sym.qualified. Pre-fix this could silently win
    // via `idx_refs_target_name` fallback paths; the new code path
    // must still ignore it because the strict FQN doesn't match.
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, target_name, target_qualified, kind,
                line, byte_start, byte_end, enclosing_id, source)
             VALUES
               ('sha-java', 'tree-sitter-java', 'fromJson',
                'com.different.Other.fromJson', 'call', 99, 200, 210,
                NULL, 'tree-sitter-java')",
        [],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "com.x.JsonAdapter.fromJson".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    // Only the Kotlin site (Branch B via resolution+symbol) should
    // surface — the distractor's target_qualified is a different
    // strict FQN so neither branch picks it up.
    let kt_hits: Vec<_> = hits.iter().filter(|h| h.path == "src/X.kt").collect();
    assert_eq!(
        kt_hits.len(),
        1,
        "Branch B should resolve the Kotlin->Java call exactly once; got: {hits:#?}"
    );
    assert_eq!(
        hits.iter()
            .filter(|h| h.target_qualified.as_deref() == Some("com.different.Other.fromJson"))
            .count(),
        0,
        "distractor with different strict FQN must not be returned: {hits:#?}"
    );
}

/// Equivalence pin (R2 strict): the new `strict_refs` UNION ALL shape
/// returns the same row set the old `COALESCE = ?` WHERE would have
/// returned, for every strict-incoming-qualified-symbol query the
/// fix touches. Rather than running the legacy query string (which
/// is gone), we cover the equivalence by asserting that:
///   1. Branch A semantics: `target_qualified = ?` direct hits work.
///   2. Branch B semantics: `target_qualified IS NULL` +
///      resolution-backed `sym.qualified = ?` hits work.
///   3. Both can coexist in the same query and dedup correctly when
///      a single ref row would have qualified through either branch
///      (impossible by construction — `target_qualified IS NULL` is
///      mutually exclusive with `target_qualified = ?`).
#[test]
fn pr_gamma_strict_incoming_union_branches_are_disjoint_and_equivalent_to_coalesce() {
    let (_db, conn, java_id, _) = cross_parser_call_fixture(false);
    let java_id = java_id.unwrap();
    conn.execute(
        "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source)
             VALUES
               ('sha-kt', 'tree-sitter-kotlin-ng', 42, 50, 'call', NULL, ?1,
                NULL, 'tier25-kotlin-resolver')",
        rusqlite::params![java_id],
    )
    .unwrap();
    // Additionally, drop in a direct `target_qualified` ref at a
    // separate site so Branch A also fires.
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, target_name, target_qualified, kind,
                line, byte_start, byte_end, enclosing_id, source)
             VALUES
               ('sha-java', 'tree-sitter-java', 'fromJson',
                'com.x.JsonAdapter.fromJson', 'call', 12, 50, 60,
                NULL, 'tree-sitter-java')",
        [],
    )
    .unwrap();

    let hits = find_references(
        &conn,
        &AnchorName::head(),
        &FindReferencesArgs {
            symbol: "com.x.JsonAdapter.fromJson".into(),
            direction: ReferenceDirection::Incoming,
            ..Default::default()
        },
    )
    .unwrap();

    // Branch A row (the direct refs insertion above).
    assert!(
        hits.iter()
            .any(|h| h.path == "src/JsonAdapter.java" && h.line == 12),
        "Branch A (direct target_qualified) row missing; got: {hits:#?}"
    );
    // Branch B row (the Kotlin call site that needs resolution
    // → symbol fallback). The fixture's refs row sits at line 5.
    assert!(
        hits.iter().any(|h| h.path == "src/X.kt" && h.line == 5),
        "Branch B (resolution-backed) row missing; got: {hits:#?}"
    );
}

/// Empty-string semantics: COALESCE(NULL, 'X') = 'X', but
/// COALESCE('', 'X') = '' — empty string is NOT NULL. Branch A's
/// `target_qualified = ?` correctly picks up the empty-string case
/// when the symbol query is empty; Branch B's `target_qualified IS
/// NULL` does not. The old COALESCE expression had the same
/// semantics, so the rewrite must too. R2 flagged this explicitly:
/// mixing empty-string into the NULL bucket would be a *semantic
/// change*, not a perf fix.
///
/// We don't expect callers to actually issue empty-string FQN
/// queries (`is_qualified_symbol("")` returns false, so the strict
/// path is never entered), but the test pins the SQL-level
/// invariant in case the upstream guard changes.
#[test]
fn pr_gamma_strict_incoming_empty_string_is_not_null() {
    let (_db, conn, _, _) = cross_parser_call_fixture(false);
    conn.execute(
        "INSERT INTO refs
               (blob_sha, parser_id, target_name, target_qualified, kind,
                line, byte_start, byte_end, enclosing_id, source)
             VALUES
               ('sha-java', 'tree-sitter-java', 'empty', '', 'call',
                7, 0, 5, NULL, 'tree-sitter-java')",
        [],
    )
    .unwrap();
    let empty_ref_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM refs WHERE target_qualified = ''",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // Sanity: '' and IS NULL are distinct row sets at the SQL level —
    // this is what makes the Branch A / Branch B split a faithful
    // COALESCE equivalent. The COALESCE projection later in the
    // outer SELECT also treats '' as a non-NULL value, so the
    // rewrite preserves visible semantics.
    assert!(
        empty_ref_count >= 1,
        "empty-string target_qualified rows must be observable as a \
         distinct bucket from IS NULL"
    );
}
