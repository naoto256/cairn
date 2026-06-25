//! Unit tests for the Stage 1 `find_imports` resolution-layer LEFT
//! JOIN: a Tier-2.5 resolver row covering the import site should
//! upgrade `kind_source` away from `tier2-fact`, and rows that pre-date
//! schema v9 (byte_range NULL) must keep the fact-layer fallback intact.
//!
//! Rows are inserted directly through SQL rather than the analyzer
//! pipeline so the tests don't depend on Ruby being one of the
//! `register_repo` paths.

use rusqlite::Connection;

use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{FindImportsArgs, find_imports};

/// Build an in-memory store with one manifest, one anchor (`HEAD`),
/// one blob, and one path entry. Returns `(blob_sha, parser_id)`.
fn fixture_store() -> (tempfile::TempDir, Connection, &'static str, &'static str) {
    let tmp = tempfile::tempdir().unwrap();
    let conn = store::open(&tmp.path().join("store.db")).unwrap();
    let blob_sha = "sha-imp";
    let parser_id = "tree-sitter-ruby";
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
    conn.execute(
        "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
         VALUES (?1, ?2, 3, 0)",
        rusqlite::params![blob_sha, parser_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
         VALUES (1, 'lib/widget.rb', ?1)",
        rusqlite::params![blob_sha],
    )
    .unwrap();
    (tmp, conn, blob_sha, parser_id)
}

fn insert_import(
    conn: &Connection,
    blob_sha: &str,
    parser_id: &str,
    to_module: &str,
    line: i64,
    byte_range: Option<(i64, i64)>,
) {
    let (bs, be) = match byte_range {
        Some((s, e)) => (Some(s), Some(e)),
        None => (None, None),
    };
    conn.execute(
        "INSERT INTO imports
           (blob_sha, parser_id, to_module, imported, alias, is_reexport, line,
            byte_start, byte_end)
         VALUES (?1, ?2, ?3, NULL, NULL, 0, ?4, ?5, ?6)",
        rusqlite::params![blob_sha, parser_id, to_module, line, bs, be],
    )
    .unwrap();
}

fn insert_import_resolution(
    conn: &Connection,
    blob_sha: &str,
    parser_id: &str,
    byte_start: i64,
    byte_end: i64,
    source: &str,
) {
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, source)
         VALUES (?1, ?2, ?3, ?4, 'import', NULL, NULL, ?5)",
        rusqlite::params![blob_sha, parser_id, byte_start, byte_end, source],
    )
    .unwrap();
}

#[test]
fn tier25_resolution_upgrades_kind_source() {
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    // Ruby `require "foo"` — the analyzer emits byte_range covering
    // the argument-string content `foo`.
    insert_import(&conn, blob_sha, parser_id, "foo", 1, Some((9, 12)));
    insert_import_resolution(&conn, blob_sha, parser_id, 9, 12, "tier25-ruby-resolver");

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].to_module, "foo");
    assert_eq!(hits[0].kind_source, "tier25-ruby-resolver");
}

#[test]
fn no_resolution_falls_back_to_tier2_fact() {
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    // byte_range present (v3 backend) but no resolution row covering
    // the site. LEFT JOIN keeps the import, kind_source falls back.
    insert_import(&conn, blob_sha, parser_id, "bar", 2, Some((20, 23)));

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].to_module, "bar");
    assert_eq!(hits[0].kind_source, "tier2-fact");
}

#[test]
fn legacy_null_byte_range_keeps_tier2_fact_fallback() {
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    // Pre-v9 row: byte_start / byte_end are NULL. Even if a Ruby
    // resolution row exists for *some* site, the LEFT JOIN on NULL
    // never matches, so the import shows up as `tier2-fact`. This is
    // the regression guard: we must not accidentally pick up an
    // unrelated resolution row.
    insert_import(&conn, blob_sha, parser_id, "legacy", 3, None);
    // Decoy resolution at a real byte range (does NOT match NULL).
    insert_import_resolution(&conn, blob_sha, parser_id, 100, 110, "tier25-ruby-resolver");

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    let hit = hits
        .iter()
        .find(|h| h.to_module == "legacy")
        .expect("legacy import missing");
    assert_eq!(hit.kind_source, "tier2-fact");
}

/// v10: helper that inserts a resolution row with `target_path Some`.
/// `insert_import_resolution` writes NULL by default (legacy column-list
/// pattern); this variant exercises the new column.
fn insert_import_resolution_with_target_path(
    conn: &Connection,
    blob_sha: &str,
    parser_id: &str,
    byte_start: i64,
    byte_end: i64,
    source: &str,
    target_path: &str,
) {
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, target_path, source)
         VALUES (?1, ?2, ?3, ?4, 'import', NULL, NULL, ?5, ?6)",
        rusqlite::params![
            blob_sha,
            parser_id,
            byte_start,
            byte_end,
            target_path,
            source
        ],
    )
    .unwrap();
}

#[test]
fn find_imports_returns_target_path_for_workspace_internal() {
    // v10 round-trip: when the resolver pinned an import to a workspace
    // file by writing `resolutions.target_path = Some(...)`, the SELECT
    // surfaces it on `ImportHit.target_path` directly (no symbol chain).
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    insert_import(&conn, blob_sha, parser_id, "./db", 1, Some((9, 13)));
    insert_import_resolution_with_target_path(
        &conn,
        blob_sha,
        parser_id,
        9,
        13,
        "tier25-ruby-resolver",
        "lib/db.rb",
    );

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].to_module, "./db");
    assert_eq!(hits[0].kind_source, "tier25-ruby-resolver");
    assert_eq!(hits[0].target_path.as_deref(), Some("lib/db.rb"));
}

#[test]
fn find_imports_target_path_none_for_bare_specifier_fallback() {
    // Bare-specifier import that fell back to tier2-fact (no resolution
    // row). target_path must surface as None — regression pin that the
    // LEFT JOIN does not invent a path when no resolver claimed the site.
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    insert_import(&conn, blob_sha, parser_id, "rake", 2, Some((20, 24)));

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    let hit = hits
        .iter()
        .find(|h| h.to_module == "rake")
        .expect("rake import missing");
    assert_eq!(hit.kind_source, "tier2-fact");
    assert!(hit.target_path.is_none());
}

// ──── v11 manifest scoping ────

/// v11 row inserter that mirrors `persist_resolutions`'s shape with
/// explicit `manifest_id`. The existing `insert_import_resolution`
/// helper leaves `manifest_id NULL` (legacy/blob-scoped), which is
/// fine for the pre-v11 tests above but not for testing the scoping.
#[allow(clippy::too_many_arguments)]
fn insert_v11_import_resolution(
    conn: &Connection,
    blob_sha: &str,
    parser_id: &str,
    byte_start: i64,
    byte_end: i64,
    source: &str,
    target_path: Option<&str>,
    manifest_id: Option<i64>,
) {
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, target_path, source,
            manifest_id)
         VALUES (?1, ?2, ?3, ?4, 'import', NULL, NULL, ?5, ?6, ?7)",
        rusqlite::params![
            blob_sha,
            parser_id,
            byte_start,
            byte_end,
            target_path,
            source,
            manifest_id
        ],
    )
    .unwrap();
}

#[test]
fn find_imports_picks_manifest_specific_row_when_both_visible() {
    // v11 Layer 3 regression pin: even if a legacy NULL row coexists
    // with a manifest-specific row for the same site, the ROW_NUMBER
    // `ORDER BY CASE WHEN manifest_id = ?1 THEN 0 ELSE 1 END` clause
    // picks the manifest-specific row.
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    insert_import(&conn, blob_sha, parser_id, "./db", 1, Some((9, 13)));

    // Both rows present: legacy NULL and manifest 1.
    insert_v11_import_resolution(
        &conn,
        blob_sha,
        parser_id,
        9,
        13,
        "tier25-ruby-resolver",
        Some("lib/legacy-db.rb"),
        None, // legacy NULL
    );
    insert_v11_import_resolution(
        &conn,
        blob_sha,
        parser_id,
        9,
        13,
        "tier25-ruby-resolver",
        Some("lib/manifest1-db.rb"),
        Some(1),
    );

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    let hit = hits
        .iter()
        .find(|h| h.to_module == "./db")
        .expect("./db import missing");
    assert_eq!(
        hit.target_path.as_deref(),
        Some("lib/manifest1-db.rb"),
        "manifest-specific row must win over legacy NULL via Layer 3 ORDER precedence"
    );
}

#[test]
fn find_imports_does_not_see_other_manifest_resolution() {
    // v11 isolation: a resolution row scoped to manifest 2 must not
    // surface in queries scoped to manifest 1.
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    // Create manifest 2 so the FK on resolutions.manifest_id is
    // satisfied for the manifest-2 row below.
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (2, 'tentative', 0)",
        [],
    )
    .unwrap();
    insert_import(&conn, blob_sha, parser_id, "./db", 1, Some((9, 13)));
    insert_v11_import_resolution(
        &conn,
        blob_sha,
        parser_id,
        9,
        13,
        "tier25-ruby-resolver",
        Some("lib/manifest2-db.rb"),
        Some(2),
    );

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    let hit = hits
        .iter()
        .find(|h| h.to_module == "./db")
        .expect("./db import missing");
    // The hit falls back to tier2-fact because no resolution row
    // visible to manifest 1 covers the site (manifest 2's row is
    // filtered out by the CTE's manifest_id predicate).
    assert_eq!(hit.kind_source, "tier2-fact");
    assert!(hit.target_path.is_none());
}

#[test]
fn find_imports_picks_tier25_over_tier2_direct_when_both_present() {
    // v11 regression pin: the source_rank precedence (tier25 over
    // tier2-direct) must survive the new manifest_id filter and
    // ORDER clause. Both rows share the same site and pass the CTE
    // filter (manifest 1 row + blob-scoped NULL Tier-2 direct row).
    let (_tmp, conn, blob_sha, parser_id) = fixture_store();
    insert_import(&conn, blob_sha, parser_id, "./db", 1, Some((9, 13)));

    // Tier-2 direct (blob-scoped NULL).
    insert_v11_import_resolution(
        &conn,
        blob_sha,
        parser_id,
        9,
        13,
        "tier2-direct-ruby",
        None,
        None,
    );
    // Tier-2.5 manifest-specific.
    insert_v11_import_resolution(
        &conn,
        blob_sha,
        parser_id,
        9,
        13,
        "tier25-ruby-resolver",
        Some("lib/db.rb"),
        Some(1),
    );

    let hits = find_imports(&conn, &AnchorName::head(), &FindImportsArgs::default()).unwrap();
    let hit = hits
        .iter()
        .find(|h| h.to_module == "./db")
        .expect("./db import missing");
    assert_eq!(
        hit.kind_source, "tier25-ruby-resolver",
        "tier25 must outrank tier2-direct via source_rank precedence"
    );
    assert_eq!(hit.target_path.as_deref(), Some("lib/db.rb"));
}
