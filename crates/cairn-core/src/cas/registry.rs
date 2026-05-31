//! Top-level alias → store mapping for CAS-managed repos.
//!
//! Each registered repo gets one row keyed by the user-facing alias.
//! `repo_hash` names the per-repo store directory under `repos/`; the
//! daemon consults this index whenever a query references a repo by
//! alias.

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;
use crate::migration::{Migration, open_with_migrations};

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: r#"
CREATE TABLE aliases (
    alias            TEXT PRIMARY KEY,
    root_path        TEXT NOT NULL UNIQUE,
    repo_hash        TEXT NOT NULL,
    registered_at_ns INTEGER NOT NULL
);
"#,
}];

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

/// Insert or replace an alias mapping. Replaces on conflict by `alias`
/// (= same alias re-registered with possibly different path) and by
/// `root_path` (= same path re-registered under a different alias).
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
    // Drop any prior row keyed by root_path that uses a different
    // alias; the alias is the user-visible name and the latest wins.
    tx.execute(
        "DELETE FROM aliases WHERE root_path = ?1 AND alias <> ?2",
        params![root_path, alias],
    )?;
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
    fn upsert_rebinds_root_path_to_new_alias() {
        let (_t, mut c) = fresh();
        let tx = c.transaction().unwrap();
        upsert(&tx, "old", "/p", "h", 1).unwrap();
        upsert(&tx, "new", "/p", "h", 2).unwrap();
        tx.commit().unwrap();
        assert!(lookup_by_alias(&c, "old").unwrap().is_none());
        assert_eq!(
            lookup_by_alias(&c, "new").unwrap().unwrap().root_path,
            "/p"
        );
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
