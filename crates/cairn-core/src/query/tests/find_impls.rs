use rusqlite::Connection;

use crate::anchor::AnchorName;
use crate::cas::store;
use crate::query::{
    FindSubtypesArgs, FindSupertypesArgs, KIND_SOURCE_FACT, find_subtypes, find_supertypes,
};
use crate::register::register_repo;
use crate::testutil::init_repo;

#[test]
fn find_subtypes_includes_typescript_tier2_heritage_edges() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let implementors = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        implementors.iter().any(|h| {
            h.type_qualified == "Dog"
                && h.interface_qualified.as_deref() == Some("Animal")
                && h.kind == "inherit"
                && h.path == "src/pets.ts"
                // Phase 3 emits a Tier-2 direct-translation resolution
                // for TypeScript `extends`; Phase 4 surfaces it on
                // `kind_source`. If the resolution row is missing
                // (e.g. backend regression) the fallback string
                // `tier2-fact` would appear instead.
                && h.kind_source == "tier2-direct-typescript"
        }),
        "TypeScript inheritance edge missing by interface query: {implementors:?}"
    );
}

#[test]
fn find_supertypes_includes_typescript_tier2_heritage_edges() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let bases = find_supertypes(
        &conn,
        &AnchorName::head(),
        &FindSupertypesArgs {
            name: "Dog".into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        bases.iter().any(|h| {
            h.type_qualified == "Dog"
                && h.interface_qualified.as_deref() == Some("Animal")
                && h.kind == "inherit"
                && h.path == "src/pets.ts"
                && h.kind_source == "tier2-direct-typescript"
        }),
        "TypeScript inheritance edge missing by type query: {bases:?}"
    );
}

#[test]
fn find_subtypes_rejects_empty_name() {
    let (_repo, _sha) = init_repo(&[("src/lib.rs", "pub fn unused() {}\n")]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let err = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: " ".into(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-empty"));
}

#[test]
fn find_supertypes_rejects_empty_name() {
    let (_repo, _sha) = init_repo(&[("src/lib.rs", "pub fn unused() {}\n")]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let err = find_supertypes(
        &conn,
        &AnchorName::head(),
        &FindSupertypesArgs {
            name: String::new(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-empty"));
}

// ─── Phase 4: resolution-layer routing of `kind` + `kind_source` ─────

/// TypeScript `implements` is a Tier-2 direct-translation case Phase 3
/// emits resolutions for. The query layer should surface the
/// resolution-layer provenance via `kind_source`.
#[test]
fn find_subtypes_surfaces_tier2_direct_typescript_implements_kind_source() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "interface Walker {}\n\
             class Dog implements Walker {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Walker".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .unwrap_or_else(|| panic!("missing Dog→Walker TS implements edge: {hits:?}"));
    assert_eq!(hit.kind, "implement");
    assert_eq!(hit.kind_source, "tier2-direct-typescript");
}

/// Python `class Dog(Animal)` uses the ambiguous `BaseArg` syntactic
/// kind that Phase 3 deliberately skips; the query layer should fall
/// back to the fact-layer `implementations.kind`.
#[test]
fn find_subtypes_falls_back_to_fact_layer_for_python_base_arg() {
    let (_repo, _sha) = init_repo(&[(
        "pets.py",
        "class Animal:\n    pass\n\
             class Dog(Animal):\n    pass\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .unwrap_or_else(|| panic!("missing Dog→Animal Python edge: {hits:?}"));
    // `implementations.kind` is whatever the Python Tier-2 backend
    // chose; the precise label is not the point — only that no
    // resolution row covers the site, so kind_source is the fallback.
    assert_eq!(hit.kind_source, KIND_SOURCE_FACT);
}

/// C++ `class Dog : public Animal {}` is one of the Phase 3 direct
/// translations (public base → inherit). The query layer must surface
/// it via `kind_source = "tier2-direct-cpp"`.
#[test]
fn find_supertypes_cpp_public_base_reports_inherit_via_resolution() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.cpp",
        "class Animal {};\n\
             class Dog : public Animal {};\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let hits = find_supertypes(
        &conn,
        &AnchorName::head(),
        &FindSupertypesArgs {
            name: "Dog".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.interface_qualified.as_deref() == Some("Animal"))
        .unwrap_or_else(|| panic!("missing Dog→Animal C++ public-base edge: {hits:?}"));
    assert_eq!(hit.kind, "inherit");
    assert_eq!(hit.kind_source, "tier2-direct-cpp");
}

/// When multiple resolution rows cover the same site, the higher-tier
/// provenance wins via `source_rank_case_sql`. Phase 3 already wrote a
/// `tier2-direct-typescript` row for `class Dog extends Animal`; we
/// add a synthetic `tier25-...` row for the same byte range and the
/// query should pick the tier25 row.
#[test]
fn higher_tier_resolution_overrides_tier2_direct() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    inject_synthetic_resolution(&conn, "pets.ts", "mixin", "tier25-ts-resolver");

    let hits = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .unwrap_or_else(|| panic!("missing Dog→Animal TS edge: {hits:?}"));
    // tier25 outranks tier2-direct; semantic_kind comes from the
    // tier25 row even though Phase 3 also wrote a tier2-direct row.
    assert_eq!(hit.kind, "mixin");
    assert_eq!(hit.kind_source, "tier25-ts-resolver");
}

/// And tier3 beats tier25.
#[test]
fn tier3_resolution_beats_tier25() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    inject_synthetic_resolution(&conn, "pets.ts", "mixin", "tier25-ts-resolver");
    inject_synthetic_resolution(&conn, "pets.ts", "implement", "tier3-ts-lsp");

    let hits = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .unwrap_or_else(|| panic!("missing Dog→Animal TS edge: {hits:?}"));
    assert_eq!(hit.kind, "implement");
    assert_eq!(hit.kind_source, "tier3-ts-lsp");
}

/// A resolution row with `semantic_kind IS NULL` is valid as a
/// provenance upgrade: the higher-rank source wins `kind_source`, and
/// `kind` falls through to `implementations.kind` via COALESCE. Ruby
/// Tier-2.5 (and other in-process resolvers) emit Type resolutions
/// without semantic_kind because impl-edge classification was already
/// fixed by Tier-2's grammar-direct emission.
#[test]
fn null_semantic_kind_resolution_upgrades_kind_source_only() {
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    // Phase 3 already wrote a `tier2-direct-typescript` row with
    // `semantic_kind = "inherit"`. Insert a higher-tier row with
    // `semantic_kind = NULL`. The higher-rank row wins kind_source;
    // kind value falls through to the existing implementations.kind.
    let (blob_sha, parser_id, start, end) = impl_site(&conn, "pets.ts").expect("pets.ts impl row");
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, source)
         VALUES (?1, ?2, ?3, ?4, 'type', NULL, NULL, 'tier3-ts-lsp')",
        rusqlite::params![blob_sha, parser_id, start, end],
    )
    .unwrap();

    let hits = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .unwrap_or_else(|| panic!("missing Dog→Animal TS edge: {hits:?}"));
    assert_eq!(hit.kind, "inherit");
    assert_eq!(hit.kind_source, "tier3-ts-lsp");
}

/// Resolutions whose `kind` is not `"type"` must be ignored — the
/// table mixes call / import / type sites and only type-edges belong
/// in the impl-relation queries.
#[test]
fn non_type_resolution_kinds_are_ignored() {
    let (_repo, _sha) = init_repo(&[(
        "pets.py",
        "class Animal:\n    pass\n\
             class Dog(Animal):\n    pass\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let (blob_sha, parser_id, _start, _end) =
        impl_site(&conn, "pets.py").expect("pets.py impl row");
    // Force-write a resolutions row at byte 0..1 with kind='call' and
    // mark `interface_byte_start = 0` on the implementations row, so
    // the join would match if the kind='type' filter were missing.
    conn.execute(
        "UPDATE implementations
            SET interface_byte_start = 0, interface_byte_end = 1
          WHERE blob_sha = ?1 AND parser_id = ?2",
        rusqlite::params![blob_sha, parser_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, source)
         VALUES (?1, ?2, 0, 1, 'call', 'mixin', NULL, 'tier3-bogus')",
        rusqlite::params![blob_sha, parser_id],
    )
    .unwrap();

    let hits = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = hits
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .unwrap_or_else(|| panic!("missing Dog→Animal Python edge: {hits:?}"));
    // The call-kind resolution must not influence the type-edge query.
    assert_eq!(hit.kind_source, KIND_SOURCE_FACT);
}

// ─── Helpers ─────────────────────────────────────────────────────────

/// Read `(blob_sha, parser_id, interface_byte_start, interface_byte_end)`
/// from the first `implementations` row whose blob's manifest path
/// matches `path_suffix`. Returns `None` when no such row exists yet
/// — useful so tests fail clearly when the fixture is wrong.
fn impl_site(conn: &Connection, path_suffix: &str) -> Option<(String, String, i64, i64)> {
    conn.query_row(
        "SELECT i.blob_sha, i.parser_id, i.interface_byte_start, i.interface_byte_end
           FROM implementations i
           JOIN manifest_entries me ON me.blob_sha = i.blob_sha
          WHERE me.path LIKE '%' || ?1
          LIMIT 1",
        [path_suffix],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2).unwrap_or(0),
                r.get::<_, i64>(3).unwrap_or(0),
            ))
        },
    )
    .ok()
}

/// Insert a synthetic `resolutions` row covering the same site as the
/// `implementations` row for `path_suffix`. Used by precedence tests
/// to simulate higher-tier (tier25 / tier3) writers without standing
/// up a real analyzer. The (blob_sha, parser_id, byte_range) tuple
/// matches the existing impl row exactly so the query's LEFT JOIN
/// fires.
fn inject_synthetic_resolution(
    conn: &Connection,
    path_suffix: &str,
    semantic_kind: &str,
    source: &str,
) {
    let (blob_sha, parser_id, start, end) = impl_site(conn, path_suffix)
        .unwrap_or_else(|| panic!("missing impl row for {path_suffix}"));
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, source)
         VALUES (?1, ?2, ?3, ?4, 'type', ?5, NULL, ?6)",
        rusqlite::params![blob_sha, parser_id, start, end, semantic_kind, source],
    )
    .unwrap();
}

/// v10 variant: writes a resolution row that also pins
/// `target_path` directly (simulating a cross-parser-id type edge
/// where the resolver knows the workspace file the supertype lives
/// in, even if `target_symbol_id` is NULL because the symbol lookup
/// crosses parser_ids without a same-parser match).
fn inject_synthetic_resolution_with_target_path(
    conn: &Connection,
    path_suffix: &str,
    semantic_kind: &str,
    source: &str,
    target_path: &str,
) {
    let (blob_sha, parser_id, start, end) = impl_site(conn, path_suffix)
        .unwrap_or_else(|| panic!("missing impl row for {path_suffix}"));
    conn.execute(
        "INSERT INTO resolutions
           (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
            kind, semantic_kind, target_symbol_id, target_path, source)
         VALUES (?1, ?2, ?3, ?4, 'type', ?5, NULL, ?6, ?7)",
        rusqlite::params![
            blob_sha,
            parser_id,
            start,
            end,
            semantic_kind,
            target_path,
            source
        ],
    )
    .unwrap();
}

#[test]
fn find_subtypes_returns_target_path_for_cross_parser_resolution() {
    // v10 round-trip for find_impls: when a resolver writes a row
    // with `target_path Some` (cross-parser-id case), the SELECT
    // surfaces it on `ImplHit.target_path` directly without
    // chasing through `symbols`.
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    inject_synthetic_resolution_with_target_path(
        &conn,
        "pets.ts",
        "inherit",
        "tier25-typescript-resolver",
        "src/animals/animal.ts",
    );

    let implementors = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = implementors
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .expect("Dog subtype missing");
    assert_eq!(hit.kind_source, "tier25-typescript-resolver");
    assert_eq!(hit.target_path.as_deref(), Some("src/animals/animal.ts"));
}

#[test]
fn find_subtypes_target_path_none_for_tier2_direct_fallback() {
    // Tier-2 direct resolution (insert_direct_resolution in cas/blob.rs)
    // writes target_path NULL by construction. The wire field stays None
    // even when kind_source is tier2-direct-typescript.
    let (_repo, _sha) = init_repo(&[(
        "src/pets.ts",
        "class Animal {}\n\
             class Dog extends Animal {}\n",
    )]);
    let db_tmp = tempfile::tempdir().unwrap();
    let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
    register_repo(&mut conn, _repo.path(), 0).unwrap();

    let implementors = find_subtypes(
        &conn,
        &AnchorName::head(),
        &FindSubtypesArgs {
            name: "Animal".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let hit = implementors
        .iter()
        .find(|h| h.type_qualified == "Dog")
        .expect("Dog subtype missing");
    assert_eq!(hit.kind_source, "tier2-direct-typescript");
    assert!(hit.target_path.is_none());
}
