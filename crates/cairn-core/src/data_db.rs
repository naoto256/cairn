//! Per-snapshot data DB (one file per `(worktree, branch)`).
//!
//! Holds the structural facts a backend extracts from one tree state:
//! files, symbols, references, imports, and implementation edges, plus
//! an FTS5 virtual table over symbol names for fuzzy lookup. Inserts
//! come from [`crate::indexer`]; this module only owns the schema and
//! the open/migrate ceremony.

use rusqlite::Connection;

use crate::Result;
use crate::migration::{Migration, open_with_migrations};

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: r#"
CREATE TABLE files (
    id           INTEGER PRIMARY KEY,
    path         TEXT NOT NULL UNIQUE,
    language     TEXT NOT NULL,
    blob_sha     TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    mtime_ns     INTEGER NOT NULL,
    parsed_at_ns INTEGER NOT NULL,
    parser       TEXT NOT NULL
);

CREATE TABLE symbols (
    id          INTEGER PRIMARY KEY,
    file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
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
    source      TEXT NOT NULL
);

CREATE INDEX idx_symbols_name ON symbols(name);
CREATE INDEX idx_symbols_qualified ON symbols(qualified);
CREATE INDEX idx_symbols_file ON symbols(file_id, line_start);

CREATE TABLE refs (
    id               INTEGER PRIMARY KEY,
    file_id          INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    enclosing_id     INTEGER REFERENCES symbols(id) ON DELETE SET NULL,
    target_id        INTEGER REFERENCES symbols(id) ON DELETE SET NULL,
    target_name      TEXT NOT NULL,
    target_qualified TEXT,
    kind             TEXT NOT NULL,
    type_role        TEXT,
    byte_start       INTEGER NOT NULL,
    byte_end         INTEGER NOT NULL,
    line             INTEGER NOT NULL,
    source           TEXT NOT NULL
);

CREATE INDEX idx_refs_target ON refs(target_id) WHERE target_id IS NOT NULL;
CREATE INDEX idx_refs_target_name ON refs(target_name);
CREATE INDEX idx_refs_file ON refs(file_id, line);
CREATE INDEX idx_refs_kind ON refs(kind, target_id);

CREATE TABLE imports (
    id           INTEGER PRIMARY KEY,
    from_file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    to_file_id   INTEGER REFERENCES files(id) ON DELETE SET NULL,
    to_module    TEXT NOT NULL,
    imported     TEXT,
    alias        TEXT,
    is_reexport  INTEGER NOT NULL DEFAULT 0,
    line         INTEGER NOT NULL
);

CREATE INDEX idx_imports_from ON imports(from_file_id);

CREATE TABLE implementations (
    id             INTEGER PRIMARY KEY,
    type_id        INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    interface_id   INTEGER REFERENCES symbols(id) ON DELETE SET NULL,
    interface_name TEXT NOT NULL,
    kind           TEXT NOT NULL
);

CREATE INDEX idx_impls_type ON implementations(type_id);
CREATE INDEX idx_impls_interface ON implementations(interface_id);

CREATE VIRTUAL TABLE symbols_fts USING fts5(
    name, qualified, doc,
    content='symbols', content_rowid='id',
    tokenize='unicode61 remove_diacritics 0'
);

-- Keep the FTS index in sync with the symbols table.
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
}];

/// Open (creating if necessary) a per-snapshot data DB at the given path.
///
/// # Errors
/// Filesystem or SQLite failures.
pub fn open(path: &std::path::Path) -> Result<Connection> {
    open_with_migrations(path, MIGRATIONS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn fresh() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("snap.db");
        let conn = open(&p).unwrap();
        (tmp, conn)
    }

    #[test]
    fn migrations_run_to_version_1() {
        let (_tmp, c) = fresh();
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 1);
    }

    #[test]
    fn fts_picks_up_inserted_symbol() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO files
               (path, language, blob_sha, size_bytes, mtime_ns, parsed_at_ns, parser)
             VALUES ('lib.rs', 'rust', 'sha', 1, 0, 0, 'tree-sitter:rust')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO symbols
               (file_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES (1, 'render', 'app::ui::View::render', 'method',
                     0, 10, 1, 5, 'syntactic')",
            [],
        )
        .unwrap();
        let hits: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM symbols_fts WHERE symbols_fts MATCH 'render'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hits, 1);
    }

    #[test]
    fn delete_propagates_through_fts() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO files
               (path, language, blob_sha, size_bytes, mtime_ns, parsed_at_ns, parser)
             VALUES ('a.rs', 'rust', 's', 1, 0, 0, 'tree-sitter:rust')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO symbols
               (file_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES (1, 'foo', 'foo', 'function', 0, 1, 1, 1, 'syntactic')",
            [],
        )
        .unwrap();
        c.execute("DELETE FROM symbols WHERE name = ?1", params!["foo"])
            .unwrap();
        let hits: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM symbols_fts WHERE symbols_fts MATCH 'foo'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hits, 0);
    }

    #[test]
    fn foreign_keys_enforced() {
        let (_tmp, c) = fresh();
        // No matching file_id — must fail.
        let res = c.execute(
            "INSERT INTO symbols
               (file_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES (999, 'x', 'x', 'function', 0, 1, 1, 1, 'syntactic')",
            [],
        );
        assert!(res.is_err());
    }
}
