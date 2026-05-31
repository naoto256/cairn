//! Schema migration framework.
//!
//! Each DB carries `PRAGMA user_version`. A migration set is a list of
//! `(version, sql)` pairs in ascending order; [`apply`] picks up where
//! the DB left off, executes the remaining steps in a single
//! transaction, and bumps `user_version` to match.
//!
//! The framework is intentionally minimal: no down-migrations, no
//! dependency tracking. Cairn's data is a derived cache — if a
//! migration goes badly, the worst case is deleting the file and
//! re-indexing.

use rusqlite::Connection;
use tracing::debug;

use crate::Result;

/// One forward migration step.
pub struct Migration {
    /// User-version this migration takes the DB to.
    pub version: u32,
    /// SQL statements executed inside a single transaction.
    pub sql: &'static str,
}

/// Apply any migrations whose version exceeds the DB's current
/// `user_version`. No-op if the DB is already at the latest version.
///
/// # Errors
/// Returns any SQLite error encountered while reading the pragma,
/// executing the statements, or committing the transaction.
///
/// # Panics
/// Panics if `migrations` is not sorted strictly ascending by `version`.
pub fn apply(conn: &mut Connection, migrations: &[Migration]) -> Result<()> {
    // Enforce ordering at the call site rather than silently doing the
    // wrong thing on a typo.
    for pair in migrations.windows(2) {
        assert!(
            pair[0].version < pair[1].version,
            "migrations must be sorted strictly ascending"
        );
    }

    let current: u32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let pending: Vec<&Migration> = migrations.iter().filter(|m| m.version > current).collect();

    if pending.is_empty() {
        debug!(current, "schema up to date");
        return Ok(());
    }

    let tx = conn.transaction()?;
    for m in &pending {
        debug!(version = m.version, "applying migration");
        tx.execute_batch(m.sql)?;
    }
    let final_version = pending.last().unwrap().version;
    // `pragma_update` doesn't accept user_version on older bindings;
    // fall back to executing the pragma as a statement.
    tx.execute_batch(&format!("PRAGMA user_version = {final_version}"))?;
    tx.commit()?;
    Ok(())
}

/// Common pragmas every DB cairn opens should have. WAL gives readers
/// and writers concurrency; foreign keys default to off in SQLite for
/// backwards compatibility, so we enable them explicitly.
pub fn apply_standard_pragmas(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    Ok(())
}

/// One-stop opener: ensure the parent directory exists, open (or
/// create) the SQLite file at `path`, apply the standard pragmas, and
/// run any pending migrations. This is the single correct procedure
/// for opening any cairn-managed DB; every per-DB module forwards to
/// it so a future change to "what cairn does on open" lands here.
///
/// # Errors
/// Filesystem or SQLite failures.
pub fn open_with_migrations(
    path: &std::path::Path,
    migrations: &[Migration],
) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut conn = Connection::open(path)?;
    apply_standard_pragmas(&conn)?;
    apply(&mut conn, migrations)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn applies_from_scratch() {
        let mut c = make_conn();
        apply(
            &mut c,
            &[Migration {
                version: 1,
                sql: "CREATE TABLE a (x INTEGER);",
            }],
        )
        .unwrap();
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 1);
        c.execute("INSERT INTO a VALUES (1)", []).unwrap();
    }

    #[test]
    fn skips_already_applied() {
        let mut c = make_conn();
        let ms = &[Migration {
            version: 1,
            sql: "CREATE TABLE a (x INTEGER);",
        }];
        apply(&mut c, ms).unwrap();
        // Second call must be a no-op (re-executing CREATE TABLE would fail).
        apply(&mut c, ms).unwrap();
    }

    #[test]
    fn applies_chain_in_order() {
        let mut c = make_conn();
        apply(
            &mut c,
            &[
                Migration {
                    version: 1,
                    sql: "CREATE TABLE a (x INTEGER);",
                },
                Migration {
                    version: 2,
                    sql: "ALTER TABLE a ADD COLUMN y INTEGER;",
                },
            ],
        )
        .unwrap();
        c.execute("INSERT INTO a (x, y) VALUES (1, 2)", []).unwrap();
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
    }

    #[test]
    #[should_panic(expected = "ascending")]
    fn panics_on_out_of_order_migrations() {
        let mut c = make_conn();
        apply(
            &mut c,
            &[
                Migration {
                    version: 2,
                    sql: "SELECT 1;",
                },
                Migration {
                    version: 1,
                    sql: "SELECT 1;",
                },
            ],
        )
        .ok();
    }

    #[test]
    fn standard_pragmas_set_wal() {
        let c = make_conn();
        apply_standard_pragmas(&c).unwrap();
        let mode: String = c
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        // In-memory DBs can report "memory" instead of WAL; on disk they'd be WAL.
        assert!(mode == "wal" || mode == "memory");
    }
}
