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
];

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::MIGRATIONS;
    use crate::migration::{apply, apply_standard_pragmas};

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
        assert_eq!(v, 10);

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
}
