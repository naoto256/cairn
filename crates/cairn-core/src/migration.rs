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

use rusqlite::{Connection, TransactionBehavior};
use tracing::debug;

use crate::Result;

/// One forward migration step.
#[derive(Clone)]
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
    apply_with_pending_hook(conn, migrations, || {})
}

fn apply_with_pending_hook(
    conn: &mut Connection,
    migrations: &[Migration],
    before_lock: impl FnOnce(),
) -> Result<()> {
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

    before_lock();

    // Serialize migration planning with migration execution. Another
    // opener may have completed the same migrations after the optimistic
    // read above, so the pending set must be refreshed under the writer lock.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current: u32 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let pending: Vec<&Migration> = migrations.iter().filter(|m| m.version > current).collect();

    if pending.is_empty() {
        debug!(current, "schema brought up to date by concurrent opener");
        tx.commit()?;
        return Ok(());
    }

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
    // Set `busy_timeout` FIRST so every pragma that can fail
    // with SQLITE_BUSY (journal_mode = WAL is famously one
    // such: switching journal modes acquires an exclusive
    // lock, and a concurrent opener can race it) retries
    // instead of surfacing to the caller. Without this the
    // very first thread to bootstrap the WAL journal wins and
    // any other opener within a few ms racing it hits
    // `DatabaseBusy` even though nobody has done any real
    // work yet.
    //
    // 30s buys enough headroom for CPU-starved parallel test
    // runs and real production writer contention across
    // independent spawn_blocking tasks; production writes are
    // small and finish long before the timeout.
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
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
    use std::sync::{Arc, Barrier};

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

    #[test]
    fn concurrent_openers_refresh_pending_migrations_under_writer_lock() {
        const MIGRATIONS: &[Migration] = &[
            Migration {
                version: 1,
                sql: "CREATE TABLE a (x INTEGER);",
            },
            Migration {
                version: 2,
                sql: "ALTER TABLE a ADD COLUMN y INTEGER;",
            },
        ];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.db");
        let mut blocker = Connection::open(&path).unwrap();
        blocker
            .busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let blocker_tx = blocker
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();

        let observed = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let path = path.clone();
            let observed = Arc::clone(&observed);
            workers.push(std::thread::spawn(move || {
                let mut conn = Connection::open(path).unwrap();
                conn.busy_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                apply_with_pending_hook(&mut conn, MIGRATIONS, || {
                    observed.wait();
                })
            }));
        }

        // Both workers have observed user_version=0. Releasing the third
        // connection's lock lets them migrate serially from the same stale
        // observation; the second worker must refresh before executing SQL.
        observed.wait();
        blocker_tx.commit().unwrap();

        for worker in workers {
            worker.join().unwrap().unwrap();
        }

        let conn = Connection::open(path).unwrap();
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 2);
        let columns: Vec<String> = conn
            .prepare("PRAGMA table_info(a)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(columns, ["x", "y"]);
    }
}
