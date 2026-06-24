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
