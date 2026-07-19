//! Per-repo store schema.
//!
//! One SQLite file per registered repo holds:
//!
//! 1. **CAS metadata** (`blobs`) — one row per `(blob_sha, parser_id)`
//!    the daemon has parsed.
//! 2. **Per-blob parsed data** (`symbols`, `refs`, `imports`,
//!    `implementations`) — keyed by `(blob_sha, parser_id)`, cascaded
//!    on blob delete. Cross-blob resolution is by string match
//!    (`target_qualified`, `interface_name`), not FK.
//! 3. **Manifests** (`manifests`, `manifest_entries`) — immutable
//!    snapshots of `(path → blob_sha)` at one point in time.
//! 4. **Anchors** (`anchors`) — named pointers to manifests
//!    (`branch/<name>`, `tag/<name>`, `HEAD`, `tentative/<id>`).
//! 5. **Worktree registry** (`worktrees`) — registered worktree paths.
//! 6. **FTS5** (`symbols_fts`) — fuzzy lookup over `name`/`qualified`/
//!    `doc`, content-synced via triggers.

use crate::migration::Migration;

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: r#"
-- CAS metadata
CREATE TABLE blobs (
    blob_sha        TEXT NOT NULL,
    parser_id       TEXT NOT NULL,
    parser_revision INTEGER NOT NULL,
    parsed_at_ns    INTEGER NOT NULL,
    PRIMARY KEY (blob_sha, parser_id)
) WITHOUT ROWID;

-- Per-blob parsed data
CREATE TABLE symbols (
    id          INTEGER PRIMARY KEY,
    blob_sha    TEXT NOT NULL,
    parser_id   TEXT NOT NULL,
    parent_id   INTEGER REFERENCES symbols(id),
    name        TEXT NOT NULL,
    qualified   TEXT NOT NULL,
    kind        TEXT NOT NULL,
    signature   TEXT,
    visibility  TEXT,
    doc         TEXT,
    byte_start  INTEGER NOT NULL,
    byte_end    INTEGER NOT NULL,
    line_start  INTEGER NOT NULL,
    line_end    INTEGER NOT NULL,
    body_start  INTEGER,
    source      TEXT NOT NULL,
    FOREIGN KEY (blob_sha, parser_id) REFERENCES blobs(blob_sha, parser_id) ON DELETE CASCADE
);

CREATE INDEX idx_symbols_blob ON symbols(blob_sha, parser_id);
CREATE INDEX idx_symbols_name ON symbols(name);
CREATE INDEX idx_symbols_qualified ON symbols(qualified);

CREATE TABLE refs (
    id               INTEGER PRIMARY KEY,
    blob_sha         TEXT NOT NULL,
    parser_id        TEXT NOT NULL,
    enclosing_id     INTEGER REFERENCES symbols(id) ON DELETE SET NULL,
    target_name      TEXT NOT NULL,
    target_qualified TEXT,
    kind             TEXT NOT NULL,
    type_role        TEXT,
    byte_start       INTEGER NOT NULL,
    byte_end         INTEGER NOT NULL,
    line             INTEGER NOT NULL,
    source           TEXT NOT NULL,
    FOREIGN KEY (blob_sha, parser_id) REFERENCES blobs(blob_sha, parser_id) ON DELETE CASCADE
);

CREATE INDEX idx_refs_blob ON refs(blob_sha, parser_id);
CREATE INDEX idx_refs_target_name ON refs(target_name);
CREATE INDEX idx_refs_target_qualified ON refs(target_qualified) WHERE target_qualified IS NOT NULL;
CREATE INDEX idx_refs_kind ON refs(kind);

CREATE TABLE imports (
    id           INTEGER PRIMARY KEY,
    blob_sha     TEXT NOT NULL,
    parser_id    TEXT NOT NULL,
    to_module    TEXT NOT NULL,
    imported     TEXT,
    alias        TEXT,
    is_reexport  INTEGER NOT NULL DEFAULT 0,
    line         INTEGER NOT NULL,
    FOREIGN KEY (blob_sha, parser_id) REFERENCES blobs(blob_sha, parser_id) ON DELETE CASCADE
);

CREATE INDEX idx_imports_blob ON imports(blob_sha, parser_id);
CREATE INDEX idx_imports_module ON imports(to_module);

CREATE TABLE implementations (
    id                  INTEGER PRIMARY KEY,
    blob_sha            TEXT NOT NULL,
    parser_id           TEXT NOT NULL,
    -- Implementations are semantic facts today; there is no query path
    -- that needs to distinguish syntactic vs semantic sources here.
    type_qualified      TEXT NOT NULL,
    interface_qualified TEXT,
    kind                TEXT NOT NULL,
    line                INTEGER NOT NULL,
    FOREIGN KEY (blob_sha, parser_id) REFERENCES blobs(blob_sha, parser_id) ON DELETE CASCADE
);

CREATE INDEX idx_impls_blob ON implementations(blob_sha, parser_id);
CREATE INDEX idx_impls_type ON implementations(type_qualified);
CREATE INDEX idx_impls_interface
    ON implementations(interface_qualified)
    WHERE interface_qualified IS NOT NULL;

-- Manifest layer
CREATE TABLE manifests (
    manifest_id INTEGER PRIMARY KEY,
    kind        TEXT NOT NULL CHECK (kind IN ('committed', 'tentative')),
    commit_sha  TEXT,
    built_at_ns INTEGER NOT NULL
);

CREATE INDEX idx_manifests_commit ON manifests(commit_sha) WHERE commit_sha IS NOT NULL;

CREATE TABLE manifest_entries (
    manifest_id INTEGER NOT NULL REFERENCES manifests(manifest_id) ON DELETE CASCADE,
    path        TEXT NOT NULL,
    blob_sha    TEXT NOT NULL,
    PRIMARY KEY (manifest_id, path)
);

CREATE INDEX idx_manifest_entries_blob ON manifest_entries(blob_sha);

-- Anchor layer. `manifest_id` uses RESTRICT so a manifest can't be
-- deleted while an anchor still points at it; GC removes the anchor
-- first, then the manifest.
CREATE TABLE anchors (
    anchor_name     TEXT PRIMARY KEY,
    manifest_id     INTEGER NOT NULL REFERENCES manifests(manifest_id) ON DELETE RESTRICT,
    last_updated_ns INTEGER NOT NULL
);

CREATE INDEX idx_anchors_manifest ON anchors(manifest_id);

-- Worktree registry
CREATE TABLE worktrees (
    worktree_id      INTEGER PRIMARY KEY,
    path             TEXT NOT NULL UNIQUE,
    registered_at_ns INTEGER NOT NULL
);

-- FTS5 over symbols, content-synced via triggers. Query semantics are
-- SQLite FTS5 defaults with unicode61: bare whitespace-separated tokens
-- are AND-ed, quoted text is an exact-order phrase, and prefix matching
-- requires an explicit trailing `*` in the MATCH query.
CREATE VIRTUAL TABLE symbols_fts USING fts5(
    name, qualified, doc,
    content='symbols', content_rowid='id',
    tokenize='unicode61 remove_diacritics 0'
);

CREATE TRIGGER symbols_ai AFTER INSERT ON symbols BEGIN
    INSERT INTO symbols_fts(rowid, name, qualified, doc)
    VALUES (new.id, new.name, new.qualified, new.doc);
END;

CREATE TRIGGER symbols_ad AFTER DELETE ON symbols BEGIN
    INSERT INTO symbols_fts(symbols_fts, rowid, name, qualified, doc)
    VALUES('delete', old.id, old.name, old.qualified, old.doc);
END;

CREATE TRIGGER symbols_au AFTER UPDATE ON symbols BEGIN
    INSERT INTO symbols_fts(symbols_fts, rowid, name, qualified, doc)
    VALUES('delete', old.id, old.name, old.qualified, old.doc);
    INSERT INTO symbols_fts(rowid, name, qualified, doc)
    VALUES (new.id, new.name, new.qualified, new.doc);
END;
"#,
    },
    Migration {
        version: 2,
        sql: r#"
ALTER TABLE blobs ADD COLUMN analyzer_id TEXT;
ALTER TABLE blobs ADD COLUMN analyzer_revision INTEGER;
"#,
    },
    Migration {
        version: 3,
        sql: r#"
-- Drop parser IDs that encoded the crate version
-- (`tree-sitter-X@<version>`). Stable parser IDs plus
-- LanguageBackend::parser_revision() now drive invalidation, so these
-- rows are guaranteed orphans after upgrade. CAS stores enable foreign
-- keys on open, so dependent rows cascade through the blobs foreign key.
DELETE FROM blobs WHERE parser_id LIKE '%@%';
"#,
    },
    Migration {
        version: 4,
        sql: r#"
CREATE TABLE workspace_analysis_runs (
    manifest_id       INTEGER NOT NULL REFERENCES manifests(manifest_id) ON DELETE CASCADE,
    analyzer_id       TEXT NOT NULL,
    analyzer_revision INTEGER NOT NULL,
    config_hash       TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'skipped')
    ),
    started_at_ns     INTEGER NOT NULL,
    finished_at_ns    INTEGER,
    error             TEXT,
    PRIMARY KEY (manifest_id, analyzer_id)
);

CREATE INDEX idx_workspace_analysis_runs_status
    ON workspace_analysis_runs(status);
"#,
    },
    Migration {
        version: 5,
        sql: r#"
CREATE TABLE workspace_analysis_runs_new (
    manifest_id       INTEGER NOT NULL REFERENCES manifests(manifest_id) ON DELETE CASCADE,
    analyzer_id       TEXT NOT NULL,
    analyzer_revision INTEGER NOT NULL,
    config_hash       TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled', 'timed_out')
    ),
    started_at_ns     INTEGER NOT NULL,
    finished_at_ns    INTEGER,
    error             TEXT,
    job_id            INTEGER,
    cancel_requested  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (manifest_id, analyzer_id)
);

INSERT INTO workspace_analysis_runs_new
    (manifest_id, analyzer_id, analyzer_revision, config_hash,
     status, started_at_ns, finished_at_ns, error, job_id, cancel_requested)
SELECT
    manifest_id,
    analyzer_id,
    analyzer_revision,
    config_hash,
    CASE
        WHEN status = 'pending' THEN 'queued'
        WHEN status = 'failed' AND error LIKE 'analyzer timed out%' THEN 'timed_out'
        ELSE status
    END,
    started_at_ns,
    finished_at_ns,
    error,
    NULL,
    0
FROM workspace_analysis_runs;

DROP TABLE workspace_analysis_runs;
ALTER TABLE workspace_analysis_runs_new RENAME TO workspace_analysis_runs;

CREATE INDEX idx_workspace_analysis_runs_status
    ON workspace_analysis_runs(status);
CREATE INDEX idx_workspace_analysis_runs_job_id
    ON workspace_analysis_runs(job_id);
"#,
    },
    Migration {
        version: 6,
        sql: r#"
-- Resolution layer (Tier-2.5 prep, Phase 1: schema only, empty operation).
--
-- Separates "what the language grammar literally says" (kept on
-- `implementations.kind` etc. — fact layer) from "what a name actually
-- resolves to" (this table — semantic layer). Phase 1 only installs the
-- schema; no reader or writer wires it up. Phase 2+ will populate it
-- from Tier-2.5 / Tier-3 passes and migrate selected query paths over.
--
-- Why no `files(id)` column: cairn has no file table. Paths live in
-- `manifest_entries`, and every per-blob artefact is keyed by
-- `(blob_sha, parser_id)`. We follow that convention so resolutions
-- cascade-delete with their owning blob, exactly like symbols/refs/
-- imports/implementations do.
CREATE TABLE resolutions (
    id                INTEGER PRIMARY KEY,
    site_blob_sha     TEXT NOT NULL,
    site_parser_id    TEXT NOT NULL,
    site_byte_start   INTEGER NOT NULL,
    site_byte_end     INTEGER NOT NULL,
    -- ResolutionKind: "type" | "call" | "import" (extensible).
    kind              TEXT NOT NULL,
    -- SemanticKind, only set when `kind = "type"` and the edge is an
    -- inheritance/conformance relation: "inherit" | "implement" |
    -- "mixin" | "extension". NULL otherwise.
    semantic_kind     TEXT,
    -- FK to symbols(id). NULL means "site observed, target unresolved"
    -- (e.g. Tier-2.5 saw the token but couldn't pin a definition).
    target_symbol_id  INTEGER REFERENCES symbols(id) ON DELETE SET NULL,
    -- Provenance, e.g. 'tier3-pyright-lsp', 'tier25-py-resolver',
    -- 'tier2-direct-java'. Must match the WORKSPACE_TIER_PREFIXES
    -- convention so the existing source-rank SQL keeps working.
    source            TEXT NOT NULL,
    FOREIGN KEY (site_blob_sha, site_parser_id)
        REFERENCES blobs(blob_sha, parser_id) ON DELETE CASCADE
);

-- "What's resolved at this token" — incremental rebuild lookup keyed by
-- the (blob, offset) tuple a writer naturally has.
CREATE INDEX idx_resolutions_site
    ON resolutions(site_blob_sha, site_byte_start);

-- "Who resolves to this symbol" — Phase 2 successor for find_subtypes /
-- find_callers when they switch off the fact layer.
CREATE INDEX idx_resolutions_target
    ON resolutions(target_symbol_id)
    WHERE target_symbol_id IS NOT NULL;

-- Provenance filter / dedup, mirroring the refs.source pattern.
CREATE INDEX idx_resolutions_source ON resolutions(source);
"#,
    },
    Migration {
        version: 7,
        sql: r#"
-- Tier-2.5 prep, Phase 2: grammar-direct classification for impl edges.
--
-- `implementations.kind` stays the semantic label ("inherit" /
-- "implement" / "mixin" / "extension") that the existing query layer
-- consumes; the new column carries the syntactic shape ("extends",
-- "implements", "colon", "less_than", "include", "trait_use",
-- "impl_for", "public_base", "interface_colon", "protocol_list",
-- "category", "extension", ...) the resolution layer will read in
-- Phase 3 to derive a per-language `semantic_kind`. NULL is allowed
-- for rows written before Phase 2 wired the field.
ALTER TABLE implementations ADD COLUMN syntactic_kind TEXT;
"#,
    },
    Migration {
        version: 8,
        sql: r#"
-- Tier-2.5 prep, Phase 4: carry the site byte range so query paths can
-- join `implementations` rows to `resolutions` rows written by the
-- direct-translation pass (and, eventually, Tier-2.5 / Tier-3).
--
-- Both columns are NULL for legacy rows and for backends that do not
-- yet ship a token range (e.g. Rust, where proc-macro2 exposes only
-- line/col). The query layer LEFT JOINs on
-- `(blob_sha, parser_id, interface_byte_start, interface_byte_end)`
-- and falls back to `implementations.kind` when no resolution exists
-- for the site.
ALTER TABLE implementations ADD COLUMN interface_byte_start INTEGER;
ALTER TABLE implementations ADD COLUMN interface_byte_end INTEGER;
"#,
    },
    Migration {
        version: 9,
        sql: r#"
-- Tier-2.5 Stage 1 1st wave (Ruby imports): carry the import-site byte
-- range so `find_imports` can LEFT JOIN against `resolutions` rows
-- written by the require-graph resolver on the shared
-- `(blob_sha, parser_id, byte_start, byte_end)` tuple — matching the
-- pattern already used for `implementations` in Phase 4.
--
-- Both columns are NULL for legacy rows and for backends that do not
-- yet ship a token range (only Ruby `require` / `require_relative`
-- emit it today; `load` / `autoload` and the other language backends
-- stay on the NULL fallback). The query layer's LEFT JOIN treats
-- NULL as "no resolution available" and falls back to the
-- `tier2-fact` provenance string, identical to find_impls.
ALTER TABLE imports ADD COLUMN byte_start INTEGER;
ALTER TABLE imports ADD COLUMN byte_end INTEGER;
"#,
    },
    Migration {
        version: 10,
        sql: r#"
-- Tier-2.5 resolution layer: target file path persistence.
--
-- Pre-v10 the only way to recover "which workspace file does this
-- edge resolve to?" from a `resolutions` row was to chase
-- `target_symbol_id -> symbols.blob_sha -> manifest_entries.path`.
-- That chain is structurally broken for two cases the resolver
-- already handles correctly internally:
--
--   1. Import edges (`require_relative './db'`, `import './foo'`).
--      The target is a *file*, not a symbol -- there is no symbol
--      row with `qualified = '<resolved path>'`, so the chain returns
--      NULL even when `WorkspaceResolution.target_path` is `Some`.
--
--   2. Cross-parser-id type/call edges (Kotlin extending a Java
--      class, Swift importing an Objective-C category, etc.). The
--      symbol exists in a sibling backend's `parser_id`, so the
--      single-parser symbol lookup in `persist_resolutions` misses
--      it and `target_symbol_id` is NULL.
--
-- v10 makes `target_path` the source of truth for "which workspace
-- file" by persisting it directly. `target_symbol_id` remains the
-- source of truth for "which symbol" -- both axes are orthogonal,
-- both may independently be NULL. The wire surface (`ImportHit`,
-- `ImplHit`, and -- in a follow-up Phase 2 -- `FindReferenceHit` /
-- `CallHit`) reads `target_path` directly instead of chasing through
-- `symbols`.
--
-- `target_qualified` is intentionally not added as a column in v10:
-- nothing in the query layer reads `resolutions.target_qualified`
-- today, so persisting it would be YAGNI. If a future call-graph
-- rewrite wants qualified-name info from `resolutions` directly,
-- that work can land in a v11 alongside `target_parser_id` and
-- `manifest_id` as a single coherent step (see "known limitation"
-- below).
--
-- The partial index supports future reverse lookups ("who resolves
-- to this file?") without paying for the NULL majority that bare
-- specifiers and unresolved sites produce.
--
-- Known limitation (not fixed in v10): `resolutions` rows are keyed
-- by `(site_blob_sha, site_parser_id, byte_range, source)` and do
-- not carry `manifest_id`. The same source blob shared across
-- branches/tags/manifests will see the last-writer-wins
-- `target_path` from whichever manifest most recently ran the
-- analyzer. Pre-v10 this issue already existed for
-- `target_symbol_id`; v10 exposes the same limitation for
-- `target_path`, so wrong-manifest reads can now surface
-- user-visible incorrect paths in addition to incorrect symbol IDs.
-- Follow-up: a separate PR (planned v11) will add
-- `resolutions.manifest_id` plus manifest-scoped query joins and
-- DELETE.
ALTER TABLE resolutions ADD COLUMN target_path TEXT;
CREATE INDEX idx_resolutions_target_path
    ON resolutions(target_path)
    WHERE target_path IS NOT NULL;
"#,
    },
    Migration {
        version: 11,
        sql: r#"
-- Tier-2.5 resolution layer: manifest-scoped resolutions.
--
-- v10 added `target_path` but left rows keyed by
-- `(site_blob_sha, site_parser_id, byte_range, source)` only — no
-- `manifest_id`. The same source blob shared across branches / tags
-- / manifests therefore reads the last-writer-wins target. Phase 4
-- (post-landing audit) confirmed this is observable at the wire:
-- `find_imports` / `find_impls` / `find_references` for a query
-- scoped to manifest B can surface a `target_path` that the
-- resolver wrote while indexing manifest A.
--
-- v11 introduces `resolutions.manifest_id` so workspace-aware rows
-- can be scoped to one manifest. `manifest_id` has *three* semantic
-- states; the wire layer treats them identically (Hit struct shape
-- is unchanged) but persistence rules distinguish them carefully:
--
--   Some(id)           Workspace-aware row written by a Tier-2.5+
--                      analyzer or by Tier-3 LSP pass. Visible only
--                      to queries scoped to manifest `id`.
--
--   NULL (new write)   Blob-scoped row written by Tier-2 direct
--                      (`cas/blob.rs::insert_direct_resolution`).
--                      Syntactic-only — derived from blob content
--                      alone, valid for every manifest containing
--                      the blob. Intentional NULL.
--
--   NULL (legacy v10)  Row written under v10 schema before this
--                      migration ran. Backward-compat fallback that
--                      surfaces to every manifest. v11 migration
--                      wholesale deletes these for workspace-aware
--                      sources (see DELETE below) so the only NULLs
--                      that survive are blob-scoped Tier-2 direct
--                      rows.
--
-- The two NULL states are not distinguishable from `manifest_id`
-- alone. They are distinguished by the `source` column: rows whose
-- source starts with any prefix in
-- `cairn_core::workspace_analyzer::WORKSPACE_TIER_PREFIXES`
-- (`tier3-*`, `tier25-*`) are workspace-aware; everything else
-- (specifically `tier2-direct-*`) is blob-scoped. The migration
-- DELETE below relies on this naming convention; the corresponding
-- invariant test in `cairn_core::cas::schema::tests`
-- (`migration_v11_cleanup_drops_only_workspace_tier_legacy_rows`)
-- pins the boundary so a new tier-prefix added to the constant
-- without updating this DELETE is caught at CI.
--
-- This migration is correctness-first, not permissive backward-
-- compat: cross-file resolved hits for Tier-2.5 / Tier-3 sources
-- disappear from existing repos until the user runs
-- `cairn ctl repo reindex <alias>`. CHANGELOG documents the upgrade
-- step. Wrong-manifest `target_path` / `target_symbol_id` leakage
-- stops at the migration point regardless of when the reindex runs.
--
-- Composite partial indexes mirror the query shape used by the
-- three workspace-aware query paths (find_imports / find_impls /
-- find_references). The CTE filters `(manifest_id = ?1 OR
-- manifest_id IS NULL)` so each branch must hit a covering index;
-- without them SQLite falls back to a `SCAN resolutions` over the
-- full table at every query. EXPLAIN QUERY PLAN is asserted in
-- tests (`explain_query_plan_production_shape_uses_both_partial_indexes`
-- plus per-branch `..._manifest_specific_branch_uses_manifest_site_index`
-- and `..._blob_scoped_branch_uses_blob_scoped_index`).
-- If a future query shape causes the planner to drop one of these
-- indexes, the fallback is to rewrite the affected query as
-- `UNION ALL` over the two branches.
ALTER TABLE resolutions
    ADD COLUMN manifest_id INTEGER
        REFERENCES manifests(manifest_id) ON DELETE CASCADE;

CREATE INDEX idx_resolutions_manifest_site
    ON resolutions(manifest_id, kind, site_blob_sha, site_parser_id,
                   site_byte_start, site_byte_end)
    WHERE manifest_id IS NOT NULL;

CREATE INDEX idx_resolutions_blob_scoped_site
    ON resolutions(kind, site_blob_sha, site_parser_id,
                   site_byte_start, site_byte_end)
    WHERE manifest_id IS NULL;

-- Migration-time wholesale cleanup of workspace-aware legacy NULL
-- rows. Tier-2 direct (`tier2-direct-*`) stays NULL on purpose.
-- The prefix list mirrors `WORKSPACE_TIER_PREFIXES` and is pinned
-- by the invariant test referenced above.
DELETE FROM resolutions
 WHERE manifest_id IS NULL
   AND (source LIKE 'tier3-%' OR source LIKE 'tier25-%');
"#,
    },
    Migration {
        version: 12,
        sql: r#"
-- Per-symbol "scope" distinction: 'top_level' (workspace-addressable)
-- vs 'nested' (file-local, declared inside a function body and not
-- meaningfully reachable as a workspace symbol). Wired by Tier-1
-- backends that have nested-function semantics worth distinguishing
-- (JS / TS / TSX today). All other backends keep emitting the
-- default, preserving prior behavior.
--
-- `find_symbols` filters to `scope = 'top_level'` so nested helpers
-- don't pollute workspace lookup; `get_outline` ignores `scope` so
-- the file-structure view stays complete.
ALTER TABLE symbols ADD COLUMN scope TEXT NOT NULL DEFAULT 'top_level';
"#,
    },
    Migration {
        version: 13,
        sql: r#"
-- Durable proof that a tentative anchor was published by one completed
-- reconcile attempt. Direct registrations and pre-v13 rows remain NULL;
-- query/status freshness treats those snapshots as unverified until the
-- startup reconcile stamps them.
ALTER TABLE anchors
    ADD COLUMN reconcile_generation INTEGER
        CHECK(reconcile_generation IS NULL OR reconcile_generation >= 0);
"#,
    },
];

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::MIGRATIONS;
    use crate::migration::{apply, apply_standard_pragmas};

    #[test]
    fn migration_v13_preserves_anchors_with_unverified_generation() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        let through_v12: Vec<_> = MIGRATIONS
            .iter()
            .filter(|migration| migration.version <= 12)
            .cloned()
            .collect();
        apply(&mut conn, &through_v12).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 10)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('tentative/1', 1, 20)",
            [],
        )
        .unwrap();

        apply(&mut conn, MIGRATIONS).unwrap();

        let row: (i64, i64, Option<i64>) = conn
            .query_row(
                "SELECT manifest_id, last_updated_ns, reconcile_generation
                 FROM anchors WHERE anchor_name = 'tentative/1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, (1, 20, None));
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 13);
    }

    /// Schema v6 should apply cleanly on top of all prior migrations
    /// and the `resolutions` table should accept a representative row
    /// (including the `target_symbol_id = NULL` "site observed, target
    /// unresolved" case) without touching any of the existing tables.
    #[test]
    fn resolutions_table_smoke() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        apply(&mut conn, MIGRATIONS).unwrap();

        // Migration applied to the latest version.
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 13);

        // Need a parent blob row because of the FK on
        // (site_blob_sha, site_parser_id).
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            ("deadbeef", "test-parser"),
        )
        .unwrap();

        // Resolved row.
        conn.execute(
            "INSERT INTO resolutions
             (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
              kind, semantic_kind, target_symbol_id, source)
             VALUES (?1, ?2, 10, 13, 'type', 'inherit', NULL, 'tier25-py-resolver')",
            ("deadbeef", "test-parser"),
        )
        .unwrap();

        // Unresolved row (target_symbol_id NULL, semantic_kind NULL).
        conn.execute(
            "INSERT INTO resolutions
             (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
              kind, semantic_kind, target_symbol_id, source)
             VALUES (?1, ?2, 42, 50, 'call', NULL, NULL, 'tier3-pyright-lsp')",
            ("deadbeef", "test-parser"),
        )
        .unwrap();

        // Read back through the site index.
        let (kind, sem, target, source): (String, Option<String>, Option<i64>, String) = conn
            .query_row(
                "SELECT kind, semantic_kind, target_symbol_id, source
                 FROM resolutions
                 WHERE site_blob_sha = ?1 AND site_byte_start = 10",
                ["deadbeef"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(kind, "type");
        assert_eq!(sem.as_deref(), Some("inherit"));
        assert!(target.is_none());
        assert_eq!(source, "tier25-py-resolver");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resolutions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);

        // Pre-existing tables are untouched and still functional.
        let symbols_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM symbols", [], |r| r.get(0))
            .unwrap();
        assert_eq!(symbols_count, 0);
        let impls_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM implementations", [], |r| r.get(0))
            .unwrap();
        assert_eq!(impls_count, 0);

        // Cascade-on-blob-delete keeps the new table consistent with
        // the rest of the per-blob artefacts.
        conn.execute("DELETE FROM blobs WHERE blob_sha = ?1", ["deadbeef"])
            .unwrap();
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM resolutions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(after, 0);
    }

    // ──── v11 migration invariants ────

    /// v11 wholesale cleanup deletes only workspace-aware
    /// (`WORKSPACE_TIER_PREFIXES`-matching) legacy NULL rows. The
    /// invariant is: if a new tier prefix joins
    /// `WORKSPACE_TIER_PREFIXES` and the migration's LIKE list isn't
    /// updated, this test fails so a CI reviewer notices.
    #[test]
    fn migration_v11_cleanup_drops_only_workspace_tier_legacy_rows() {
        use crate::workspace_analyzer::WORKSPACE_TIER_PREFIXES;

        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();

        // Apply migrations only up to v10 so we can seed legacy rows.
        let migrations_pre_v11: Vec<_> = MIGRATIONS
            .iter()
            .filter(|m| m.version <= 10)
            .cloned()
            .collect();
        apply(&mut conn, &migrations_pre_v11).unwrap();

        // Parent blob.
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('legacy-blob', 'test-parser', 1, 0)",
            [],
        )
        .unwrap();

        // Seed one workspace-aware legacy NULL row per prefix, plus
        // a Tier-2 direct NULL row that must survive.
        for (i, prefix) in WORKSPACE_TIER_PREFIXES.iter().enumerate() {
            let source = format!("{prefix}-fake-analyzer");
            let start = i as i64 * 10;
            conn.execute(
                "INSERT INTO resolutions
                   (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                    kind, semantic_kind, target_symbol_id, source)
                 VALUES ('legacy-blob', 'test-parser', ?1, ?2, 'type', 'inherit',
                         NULL, ?3)",
                rusqlite::params![start, start + 5, source],
            )
            .unwrap();
        }
        let blob_scoped_start = WORKSPACE_TIER_PREFIXES.len() as i64 * 10;
        conn.execute(
            "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, source)
             VALUES ('legacy-blob', 'test-parser', ?1, ?2, 'type', 'inherit',
                     NULL, 'tier2-direct-ruby')",
            rusqlite::params![blob_scoped_start, blob_scoped_start + 5],
        )
        .unwrap();

        // Now apply v11 migration on top.
        apply(&mut conn, MIGRATIONS).unwrap();

        // Workspace-aware legacy NULL rows must be gone.
        for prefix in WORKSPACE_TIER_PREFIXES.iter() {
            let pattern = format!("{prefix}-%");
            let cnt: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM resolutions
                     WHERE source LIKE ?1 AND manifest_id IS NULL",
                    rusqlite::params![pattern],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                cnt, 0,
                "v11 cleanup must drop legacy NULL rows for prefix {prefix}-*"
            );
        }

        // Tier-2 direct blob-scoped NULL row must survive (intentional NULL).
        let blob_scoped_cnt: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resolutions
                 WHERE source = 'tier2-direct-ruby' AND manifest_id IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            blob_scoped_cnt, 1,
            "Tier-2 direct blob-scoped NULL row must survive v11 cleanup"
        );
    }

    /// `ALTER TABLE` is not idempotent in SQLite (re-adding the same
    /// column fails). Verify that the migration runner detects v11
    /// is already applied and skips it on a second run.
    #[test]
    fn migration_v11_is_idempotent_on_second_apply() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        apply(&mut conn, MIGRATIONS).unwrap();
        // Second apply must be a no-op (migration runner reads
        // user_version and skips).
        apply(&mut conn, MIGRATIONS).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 13);
    }

    // ──── EXPLAIN QUERY PLAN: v11 composite partial indexes ────
    //
    // These three tests pin that:
    //   1. The full production CTE shape used by `find_imports` /
    //      `find_impls` / `find_references` avoids a full
    //      `resolutions` table scan and hits *both* partial indexes
    //      (one per branch of the `(manifest_id = ?1 OR manifest_id IS
    //      NULL)` predicate).
    //   2. The manifest-specific branch in isolation uses
    //      `idx_resolutions_manifest_site` and *not* the blob-scoped
    //      index.
    //   3. The blob-scoped branch in isolation uses
    //      `idx_resolutions_blob_scoped_site` and *not* the
    //      manifest-site index.
    //
    // The production-shape SQL below MUST MIRROR `find_imports.rs:74-95`'s
    // `best_resolution` CTE; the three query paths share the same
    // (kind filter + manifest_id OR predicate) shape with only the
    // selected columns and partition key differing. A shared SQL
    // builder was considered and rejected: the three CTEs diverge on
    // column list, kind filter, and PARTITION BY key enough that a
    // common builder would either be over-parameterized or fail to
    // capture the planner-relevant shape. When this raw copy ever
    // diverges from `find_imports.rs`, update both and re-run these
    // EXPLAIN tests.

    /// Read the `detail` column (index 3) from every row of an EXPLAIN
    /// QUERY PLAN result.
    fn explain_details(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> Vec<String> {
        conn.prepare(sql)
            .unwrap()
            .query_map(params, |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    /// Fail when any line scans `resolutions` *without* an index.
    /// Per-line judging is critical: `SCAN resolutions USING INDEX
    /// idx_...` is index-driven and acceptable; a bare `SCAN
    /// resolutions` / `SCAN TABLE resolutions` is the full-scan
    /// regression we're guarding against.
    fn assert_no_resolution_full_scan(plan: &[String]) {
        for line in plan {
            let mentions_resolutions =
                line.contains("SCAN resolutions") || line.contains("SCAN TABLE resolutions");
            let uses_index = line.contains("USING INDEX") || line.contains("USING COVERING INDEX");
            assert!(
                !mentions_resolutions || uses_index,
                "EXPLAIN QUERY PLAN line is a full `resolutions` scan \
                 (no USING INDEX clause): {line:?}; full plan: {plan:?}"
            );
        }
    }

    fn assert_uses_index(plan: &[String], index_name: &str) {
        assert!(
            plan.iter().any(|l| l.contains(index_name)),
            "expected EXPLAIN QUERY PLAN to mention index `{index_name}`; \
             full plan: {plan:?}"
        );
    }

    fn assert_not_uses_index(plan: &[String], index_name: &str) {
        assert!(
            plan.iter().all(|l| !l.contains(index_name)),
            "expected EXPLAIN QUERY PLAN to NOT mention index `{index_name}`; \
             full plan: {plan:?}"
        );
    }

    /// (1) Production CTE shape: both partial indexes must fire so the
    /// `(manifest_id = ?1 OR manifest_id IS NULL)` OR doesn't degrade
    /// to a full table scan.
    ///
    /// If this test fails because SQLite collapsed the plan to a single
    /// strategy (e.g. picked a UNION ALL rewrite or fell back to a
    /// b-tree scan that no longer mentions both partial indexes), treat
    /// it as a deliberate design change: update the test plus
    /// CHANGELOG, don't paper over it.
    #[test]
    fn explain_query_plan_production_shape_uses_both_partial_indexes() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        apply(&mut conn, MIGRATIONS).unwrap();

        // MUST MIRROR find_imports.rs:74-95 `best_resolution` CTE.
        // The `source_rank` CASE / `id` tie-break in the ORDER BY of
        // the ROW_NUMBER are not relevant to the planner's index
        // choice; the WHERE clause and partition shape are.
        let sql = "EXPLAIN QUERY PLAN
            WITH best_resolution AS (
                SELECT site_blob_sha, site_parser_id,
                       site_byte_start, site_byte_end,
                       source, target_path,
                       ROW_NUMBER() OVER (
                           PARTITION BY site_blob_sha, site_parser_id,
                                        site_byte_start, site_byte_end
                           ORDER BY
                               CASE WHEN manifest_id = ?1 THEN 0 ELSE 1 END,
                               id
                       ) AS rn
                  FROM resolutions
                 WHERE kind = 'import'
                   AND (manifest_id = ?1 OR manifest_id IS NULL)
            )
            SELECT site_blob_sha FROM best_resolution WHERE rn = 1";
        let plan = explain_details(&conn, sql, rusqlite::params![1i64]);

        assert_no_resolution_full_scan(&plan);
        let index_hit_count = plan
            .iter()
            .filter(|l| l.contains("idx_resolutions_"))
            .count();
        assert!(
            index_hit_count >= 2,
            "production CTE shape must engage both v11 partial indexes \
             (one per branch of the `manifest_id = ?1 OR manifest_id IS NULL` \
             predicate); index_hit_count={index_hit_count}, plan: {plan:?}"
        );
    }

    /// (2) Manifest-specific branch alone: must use
    /// `idx_resolutions_manifest_site` and must NOT touch
    /// `idx_resolutions_blob_scoped_site` (which is partial WHERE
    /// `manifest_id IS NULL`).
    #[test]
    fn explain_query_plan_manifest_specific_branch_uses_manifest_site_index() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        apply(&mut conn, MIGRATIONS).unwrap();

        let plan = explain_details(
            &conn,
            "EXPLAIN QUERY PLAN
             SELECT site_blob_sha FROM resolutions
              WHERE kind = 'import' AND manifest_id = ?1",
            rusqlite::params![1i64],
        );
        assert_no_resolution_full_scan(&plan);
        assert_uses_index(&plan, "idx_resolutions_manifest_site");
        assert_not_uses_index(&plan, "idx_resolutions_blob_scoped_site");
    }

    /// (3) Blob-scoped branch alone: must use
    /// `idx_resolutions_blob_scoped_site` and must NOT touch the
    /// manifest-site index (which is partial WHERE `manifest_id IS
    /// NOT NULL`).
    #[test]
    fn explain_query_plan_blob_scoped_branch_uses_blob_scoped_index() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        apply(&mut conn, MIGRATIONS).unwrap();

        let plan = explain_details(
            &conn,
            "EXPLAIN QUERY PLAN
             SELECT site_blob_sha FROM resolutions
              WHERE kind = 'import' AND manifest_id IS NULL",
            [],
        );
        assert_no_resolution_full_scan(&plan);
        assert_uses_index(&plan, "idx_resolutions_blob_scoped_site");
        assert_not_uses_index(&plan, "idx_resolutions_manifest_site");
    }

    /// FK cascade for v11: deleting a manifest must cascade-delete
    /// `manifest_id = Some(that)` resolutions, but Tier-2 direct
    /// blob-scoped rows (`manifest_id NULL`) must survive.
    #[test]
    fn delete_manifest_cascades_manifest_scoped_resolutions_but_preserves_blob_scoped() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply_standard_pragmas(&conn).unwrap();
        apply(&mut conn, MIGRATIONS).unwrap();

        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('blob-x', 'tree-sitter-ruby', 1, 0)",
            [],
        )
        .unwrap();
        // Two rows for the same site: one manifest-scoped, one blob-scoped.
        conn.execute(
            "INSERT INTO resolutions
               (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                kind, semantic_kind, target_symbol_id, target_path, source,
                manifest_id)
             VALUES
               ('blob-x', 'tree-sitter-ruby', 0, 5, 'type', 'inherit', NULL,
                NULL, 'tier25-ruby-resolver', 1),
               ('blob-x', 'tree-sitter-ruby', 0, 5, 'type', 'inherit', NULL,
                NULL, 'tier2-direct-ruby', NULL)",
            [],
        )
        .unwrap();

        // Drop the manifest. FK ON DELETE CASCADE must remove only the
        // manifest-scoped row.
        conn.execute("DELETE FROM manifests WHERE manifest_id = 1", [])
            .unwrap();
        let (manifest_scoped_left, blob_scoped_left): (i64, i64) = conn
            .query_row(
                "SELECT
                    SUM(CASE WHEN manifest_id IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN manifest_id IS NULL THEN 1 ELSE 0 END)
                 FROM resolutions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            manifest_scoped_left, 0,
            "manifest-scoped row must cascade-delete with the manifest"
        );
        assert_eq!(
            blob_scoped_left, 1,
            "blob-scoped (NULL manifest_id) row must survive manifest deletion"
        );
    }
}
