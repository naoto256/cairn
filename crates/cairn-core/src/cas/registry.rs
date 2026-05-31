//! Top-level alias → store mapping for CAS-managed repos.
//!
//! Each registered repo gets one row keyed by the user-facing alias.
//! `repo_hash` names the per-repo store directory under `repos/`; the
//! daemon consults this index whenever a query references a repo by
//! alias.

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;
use crate::migration::{Migration, open_with_migrations};

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: r#"
CREATE TABLE aliases (
    alias            TEXT PRIMARY KEY,
    root_path        TEXT NOT NULL UNIQUE,
    repo_hash        TEXT NOT NULL,
    registered_at_ns INTEGER NOT NULL
);
"#,
    },
    Migration {
        // Drop the UNIQUE on root_path so multiple aliases can label
        // the same on-disk repo. Recreated via the rename-shuffle since
        // SQLite has no `DROP CONSTRAINT`.
        version: 2,
        sql: r#"
CREATE TABLE aliases_v2 (
    alias            TEXT PRIMARY KEY,
    root_path        TEXT NOT NULL,
    repo_hash        TEXT NOT NULL,
    registered_at_ns INTEGER NOT NULL
);
INSERT INTO aliases_v2 SELECT alias, root_path, repo_hash, registered_at_ns FROM aliases;
DROP TABLE aliases;
ALTER TABLE aliases_v2 RENAME TO aliases;
CREATE INDEX idx_aliases_repo_hash ON aliases(repo_hash);
"#,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasEntry {
    pub alias: String,
    pub root_path: String,
    pub repo_hash: String,
    pub registered_at_ns: i64,
}

/// Open (creating if necessary) the alias index DB at `path`.
///
/// # Errors
/// Filesystem or SQLite failures.
pub fn open(path: &std::path::Path) -> Result<Connection> {
    open_with_migrations(path, MIGRATIONS)
}

/// Insert or replace an alias mapping. Replaces only on conflict by
/// `alias`; other aliases pointing at the same `root_path` are left
/// alone so two labels can share one on-disk repo.
///
/// # Errors
/// SQLite failures.
pub fn upsert(
    tx: &Transaction<'_>,
    alias: &str,
    root_path: &str,
    repo_hash: &str,
    registered_at_ns: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO aliases (alias, root_path, repo_hash, registered_at_ns)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(alias) DO UPDATE SET
             root_path = excluded.root_path,
             repo_hash = excluded.repo_hash,
             registered_at_ns = excluded.registered_at_ns",
        params![alias, root_path, repo_hash, registered_at_ns],
    )?;
    Ok(())
}

/// Count how many aliases reference `repo_hash`. Used by
/// `remove_repo` to decide whether the per-repo store directory is
/// still in use by another label.
///
/// # Errors
/// SQLite failures.
pub fn count_aliases_for_repo(conn: &Connection, repo_hash: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM aliases WHERE repo_hash = ?1",
        params![repo_hash],
        |r| r.get(0),
    )?)
}

/// Look up one alias. Returns `Ok(None)` if absent.
///
/// # Errors
/// SQLite failures.
pub fn lookup_by_alias(conn: &Connection, alias: &str) -> Result<Option<AliasEntry>> {
    Ok(conn
        .query_row(
            "SELECT alias, root_path, repo_hash, registered_at_ns
             FROM aliases WHERE alias = ?1",
            params![alias],
            row_to_entry,
        )
        .optional()?)
}

/// All registered aliases ordered by alias.
///
/// # Errors
/// SQLite failures.
pub fn list_all(conn: &Connection) -> Result<Vec<AliasEntry>> {
    let mut stmt = conn.prepare(
        "SELECT alias, root_path, repo_hash, registered_at_ns
         FROM aliases ORDER BY alias",
    )?;
    let rows: rusqlite::Result<Vec<AliasEntry>> =
        stmt.query_map([], row_to_entry)?.collect();
    Ok(rows?)
}

/// Remove one alias by name. Returns `true` if a row was deleted.
///
/// # Errors
/// SQLite failures.
pub fn delete(tx: &Transaction<'_>, alias: &str) -> Result<bool> {
    let n = tx.execute("DELETE FROM aliases WHERE alias = ?1", params![alias])?;
    Ok(n > 0)
}

fn row_to_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<AliasEntry> {
    Ok(AliasEntry {
        alias: r.get(0)?,
        root_path: r.get(1)?,
        repo_hash: r.get(2)?,
        registered_at_ns: r.get(3)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = open(&tmp.path().join("index.db")).unwrap();
        (tmp, conn)
    }

    #[test]
    fn upsert_then_lookup_roundtrips() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/some/path", "h0", 1234).unwrap();
        tx.commit().unwrap();

        let entry = lookup_by_alias(&c, "demo").unwrap().unwrap();
        assert_eq!(entry.alias, "demo");
        assert_eq!(entry.root_path, "/some/path");
        assert_eq!(entry.repo_hash, "h0");
        assert_eq!(entry.registered_at_ns, 1234);
    }

    #[test]
    fn lookup_returns_none_for_missing() {
        let (_t, c) = fresh();
        assert!(lookup_by_alias(&c, "nope").unwrap().is_none());
    }

    #[test]
    fn upsert_replaces_same_alias_with_new_target() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/a", "h0", 1).unwrap();
        upsert(&tx, "demo", "/b", "h1", 2).unwrap();
        tx.commit().unwrap();
        let entry = lookup_by_alias(&c, "demo").unwrap().unwrap();
        assert_eq!(entry.root_path, "/b");
        assert_eq!(entry.repo_hash, "h1");
    }

    #[test]
    fn upsert_allows_multiple_aliases_for_same_path() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "first", "/p", "h", 1).unwrap();
        upsert(&tx, "second", "/p", "h", 2).unwrap();
        tx.commit().unwrap();
        // Both aliases survive; neither is silently dropped.
        assert_eq!(
            lookup_by_alias(&c, "first").unwrap().unwrap().root_path,
            "/p"
        );
        assert_eq!(
            lookup_by_alias(&c, "second").unwrap().unwrap().root_path,
            "/p"
        );
        assert_eq!(count_aliases_for_repo(&c, "h").unwrap(), 2);
    }

    #[test]
    fn count_aliases_for_repo_is_zero_for_unknown() {
        let (_t, c) = fresh();
        assert_eq!(count_aliases_for_repo(&c, "ghost").unwrap(), 0);
    }

    #[test]
    fn list_all_orders_by_alias() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "z", "/z", "hz", 0).unwrap();
        upsert(&tx, "a", "/a", "ha", 0).unwrap();
        upsert(&tx, "m", "/m", "hm", 0).unwrap();
        tx.commit().unwrap();
        let names: Vec<String> = list_all(&c).unwrap().into_iter().map(|e| e.alias).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn delete_removes_one_and_reports() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "demo", "/p", "h", 0).unwrap();
        assert!(delete(&tx, "demo").unwrap());
        assert!(!delete(&tx, "demo").unwrap());
        tx.commit().unwrap();
    }
}
