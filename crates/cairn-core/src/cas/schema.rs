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
];
