//! `registry.db` — the central inventory.
//!
//! Stores the list of registered repositories, their worktrees, and the
//! `(worktree, branch)` snapshots that have been (or are being) indexed.
//! Concrete file content for a snapshot lives in its own SQLite file
//! under `<data_dir>/indexes/...`; this DB just tracks where each one is
//! and what state it is in.

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};
use rusqlite::{Connection, OptionalExtension, ToSql, params};
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::migration::{Migration, open_with_migrations};

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: r#"
CREATE TABLE repos (
    id               INTEGER PRIMARY KEY,
    alias            TEXT UNIQUE NOT NULL,
    root_path        TEXT UNIQUE NOT NULL,
    repo_hash        TEXT UNIQUE NOT NULL,
    registered_at_ns INTEGER NOT NULL
);

CREATE TABLE worktrees (
    id                 INTEGER PRIMARY KEY,
    repo_id            INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    path               TEXT UNIQUE NOT NULL,
    worktree_hash      TEXT NOT NULL,
    current_branch     TEXT,
    current_head_sha   TEXT,
    UNIQUE(repo_id, worktree_hash)
);

CREATE INDEX idx_worktrees_repo ON worktrees(repo_id);

CREATE TABLE index_snapshots (
    id                INTEGER PRIMARY KEY,
    worktree_id       INTEGER NOT NULL REFERENCES worktrees(id) ON DELETE CASCADE,
    branch            TEXT NOT NULL,
    db_path           TEXT NOT NULL,
    status            TEXT NOT NULL,
    enrichment        TEXT NOT NULL,
    built_at_ns       INTEGER,
    last_accessed_ns  INTEGER NOT NULL,
    size_bytes        INTEGER,
    UNIQUE(worktree_id, branch)
);

CREATE INDEX idx_snapshots_worktree ON index_snapshots(worktree_id);
"#,
    },
    Migration {
        version: 2,
        // Stamp every snapshot with the extractor revision that
        // built it. Existing rows default to 0 so the startup check
        // (current `INDEXER_REVISION` is >= 1) schedules them for
        // background reindex — exactly the behaviour we want when a
        // user upgrades the daemon binary across an extractor
        // change without thinking about it.
        sql: r#"
ALTER TABLE index_snapshots
  ADD COLUMN indexer_revision INTEGER NOT NULL DEFAULT 0;
"#,
    },
];

/// Open (creating if necessary) `registry.db` at the given path.
///
/// # Errors
/// Filesystem or SQLite failures.
pub fn open(path: &std::path::Path) -> Result<Connection> {
    open_with_migrations(path, MIGRATIONS)
}

// ─── domain types ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repo {
    pub id: i64,
    pub alias: String,
    pub root_path: String,
    pub repo_hash: String,
    pub registered_at_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worktree {
    pub id: i64,
    pub repo_id: i64,
    pub path: String,
    pub worktree_hash: String,
    pub current_branch: Option<String>,
    pub current_head_sha: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    Building,
    Ready,
    Stale,
}

impl SnapshotStatus {
    /// Stable string form used both in the registry DB and when
    /// reporting status to callers (MCP / control / metrics).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Ready => "ready",
            Self::Stale => "stale",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "building" => Some(Self::Building),
            "ready" => Some(Self::Ready),
            "stale" => Some(Self::Stale),
            _ => None,
        }
    }
}

impl ToSql for SnapshotStatus {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}

impl FromSql for SnapshotStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let s = value.as_str()?;
        Self::parse(s)
            .ok_or_else(|| FromSqlError::Other(format!("unknown snapshot status: {s}").into()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotEnrichment {
    Syntactic,
    Semantic,
}

impl SnapshotEnrichment {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Syntactic => "syntactic",
            Self::Semantic => "semantic",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "syntactic" => Some(Self::Syntactic),
            "semantic" => Some(Self::Semantic),
            _ => None,
        }
    }
}

/// Bridge from the registry-internal enrichment value to the
/// wire-facing [`cairn_proto::SourceTier`]. Lives here because the
/// registry owns the source-of-truth values; mcp / ctl just consume.
impl From<SnapshotEnrichment> for cairn_proto::SourceTier {
    fn from(e: SnapshotEnrichment) -> Self {
        match e {
            SnapshotEnrichment::Syntactic => Self::Syntactic,
            SnapshotEnrichment::Semantic => Self::Semantic,
        }
    }
}

impl ToSql for SnapshotEnrichment {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}

impl FromSql for SnapshotEnrichment {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let s = value.as_str()?;
        Self::parse(s)
            .ok_or_else(|| FromSqlError::Other(format!("unknown snapshot enrichment: {s}").into()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: i64,
    pub worktree_id: i64,
    pub branch: String,
    pub db_path: String,
    pub status: SnapshotStatus,
    pub enrichment: SnapshotEnrichment,
    pub built_at_ns: Option<i64>,
    pub last_accessed_ns: i64,
    pub size_bytes: Option<i64>,
    /// Revision of the extractor pipeline that built this snapshot.
    /// Compared against [`crate::INDEXER_REVISION`] on daemon
    /// startup; lower values trigger a background `full_index`.
    /// Rows that predate migration v2 default to 0.
    pub indexer_revision: u32,
}

// ─── CRUD ───────────────────────────────────────────────────────────────────

/// Insert (or fail loudly) a new repository entry.
///
/// # Errors
/// Returns a SQLite uniqueness error if `alias` or `root_path` collides.
pub fn insert_repo(
    conn: &Connection,
    alias: &str,
    root_path: &str,
    repo_hash: &str,
    registered_at_ns: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO repos (alias, root_path, repo_hash, registered_at_ns)
         VALUES (?1, ?2, ?3, ?4)",
        params![alias, root_path, repo_hash, registered_at_ns],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Look up a repo by alias.
///
/// # Errors
/// Propagates SQLite errors.
pub fn find_repo_by_alias(conn: &Connection, alias: &str) -> Result<Option<Repo>> {
    conn.query_row(
        "SELECT id, alias, root_path, repo_hash, registered_at_ns
         FROM repos WHERE alias = ?1",
        params![alias],
        repo_from_row,
    )
    .optional()
    .map_err(Into::into)
}

/// List all registered repos, ordered by alias.
///
/// # Errors
/// Propagates SQLite errors.
pub fn list_repos(conn: &Connection) -> Result<Vec<Repo>> {
    let mut stmt = conn.prepare(
        "SELECT id, alias, root_path, repo_hash, registered_at_ns
         FROM repos ORDER BY alias",
    )?;
    let rows = stmt
        .query_map([], repo_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Delete a repo and cascade through worktrees / snapshots.
///
/// # Errors
/// Propagates SQLite errors.
pub fn delete_repo(conn: &Connection, alias: &str) -> Result<bool> {
    let affected = conn.execute("DELETE FROM repos WHERE alias = ?1", params![alias])?;
    Ok(affected > 0)
}

/// Upsert a worktree row keyed by (repo_id, path).
///
/// # Errors
/// Propagates SQLite errors.
pub fn upsert_worktree(
    conn: &Connection,
    repo_id: i64,
    path: &str,
    worktree_hash: &str,
    current_branch: Option<&str>,
    current_head_sha: Option<&str>,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO worktrees (repo_id, path, worktree_hash, current_branch, current_head_sha)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(path) DO UPDATE SET
           worktree_hash    = excluded.worktree_hash,
           current_branch   = excluded.current_branch,
           current_head_sha = excluded.current_head_sha",
        params![
            repo_id,
            path,
            worktree_hash,
            current_branch,
            current_head_sha
        ],
    )?;
    let id: i64 = conn.query_row(
        "SELECT id FROM worktrees WHERE path = ?1",
        params![path],
        |r| r.get(0),
    )?;
    Ok(id)
}

/// List worktrees registered under a repo.
///
/// # Errors
/// Propagates SQLite errors.
pub fn list_worktrees(conn: &Connection, repo_id: i64) -> Result<Vec<Worktree>> {
    let mut stmt = conn.prepare(
        "SELECT id, repo_id, path, worktree_hash, current_branch, current_head_sha
         FROM worktrees WHERE repo_id = ?1 ORDER BY path",
    )?;
    let rows = stmt
        .query_map(params![repo_id], worktree_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Upsert a snapshot entry keyed by (worktree_id, branch).
///
/// # Errors
/// Propagates SQLite errors.
#[allow(clippy::too_many_arguments)]
pub fn upsert_snapshot(
    conn: &Connection,
    worktree_id: i64,
    branch: &str,
    db_path: &str,
    status: SnapshotStatus,
    enrichment: SnapshotEnrichment,
    built_at_ns: Option<i64>,
    last_accessed_ns: i64,
    size_bytes: Option<i64>,
    indexer_revision: u32,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO index_snapshots
           (worktree_id, branch, db_path, status, enrichment,
            built_at_ns, last_accessed_ns, size_bytes, indexer_revision)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(worktree_id, branch) DO UPDATE SET
           db_path          = excluded.db_path,
           status           = excluded.status,
           enrichment       = excluded.enrichment,
           built_at_ns      = COALESCE(excluded.built_at_ns, index_snapshots.built_at_ns),
           last_accessed_ns = excluded.last_accessed_ns,
           size_bytes       = COALESCE(excluded.size_bytes, index_snapshots.size_bytes),
           indexer_revision = excluded.indexer_revision",
        params![
            worktree_id,
            branch,
            db_path,
            status,
            enrichment,
            built_at_ns,
            last_accessed_ns,
            size_bytes,
            indexer_revision,
        ],
    )?;
    let id: i64 = conn.query_row(
        "SELECT id FROM index_snapshots WHERE worktree_id = ?1 AND branch = ?2",
        params![worktree_id, branch],
        |r| r.get(0),
    )?;
    Ok(id)
}

/// Delete a snapshot row by (worktree_id, branch). Returns the
/// removed row's `db_path` if it existed, so the caller can also
/// remove the on-disk SQLite file. Returns `None` when no matching
/// row was found.
///
/// # Errors
/// Propagates SQLite errors.
pub fn delete_snapshot(
    conn: &Connection,
    worktree_id: i64,
    branch: &str,
) -> Result<Option<String>> {
    let path: Option<String> = conn
        .query_row(
            "SELECT db_path FROM index_snapshots WHERE worktree_id = ?1 AND branch = ?2",
            params![worktree_id, branch],
            |r| r.get(0),
        )
        .optional()?;
    if path.is_some() {
        conn.execute(
            "DELETE FROM index_snapshots WHERE worktree_id = ?1 AND branch = ?2",
            params![worktree_id, branch],
        )?;
    }
    Ok(path)
}

/// List all snapshots for a worktree.
///
/// # Errors
/// Propagates SQLite errors. Unknown enum values stored in `status` or
/// `enrichment` surface as `Error::SchemaCorruption` via the FromSql
/// implementations on the enums.
pub fn list_snapshots(conn: &Connection, worktree_id: i64) -> Result<Vec<Snapshot>> {
    let mut stmt = conn.prepare(
        "SELECT id, worktree_id, branch, db_path, status, enrichment,
                built_at_ns, last_accessed_ns, size_bytes, indexer_revision
         FROM index_snapshots WHERE worktree_id = ?1 ORDER BY branch",
    )?;
    let rows: Vec<Snapshot> = stmt
        .query_map(params![worktree_id], snapshot_from_row)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(into_schema_or_sql_error)?;
    Ok(rows)
}

/// Translate the rusqlite errors produced by FromSql failures into our
/// `SchemaCorruption` variant; pass everything else through as Sqlite.
fn into_schema_or_sql_error(e: rusqlite::Error) -> crate::Error {
    match &e {
        rusqlite::Error::FromSqlConversionFailure(_, _, inner) => {
            crate::Error::SchemaCorruption(inner.to_string())
        }
        _ => crate::Error::Sqlite(e),
    }
}

// ─── row mappers ────────────────────────────────────────────────────────────

fn repo_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Repo> {
    Ok(Repo {
        id: row.get(0)?,
        alias: row.get(1)?,
        root_path: row.get(2)?,
        repo_hash: row.get(3)?,
        registered_at_ns: row.get(4)?,
    })
}

fn worktree_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Worktree> {
    Ok(Worktree {
        id: row.get(0)?,
        repo_id: row.get(1)?,
        path: row.get(2)?,
        worktree_hash: row.get(3)?,
        current_branch: row.get(4)?,
        current_head_sha: row.get(5)?,
    })
}

fn snapshot_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Snapshot> {
    Ok(Snapshot {
        id: row.get(0)?,
        worktree_id: row.get(1)?,
        branch: row.get(2)?,
        db_path: row.get(3)?,
        status: row.get(4)?,
        enrichment: row.get(5)?,
        built_at_ns: row.get(6)?,
        last_accessed_ns: row.get(7)?,
        size_bytes: row.get(8)?,
        indexer_revision: row.get::<_, u32>(9)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> (tempfile::TempDir, Connection) {
        // Use a file-backed DB so WAL pragma can take effect (in-memory
        // DBs cannot use WAL); this exercises the same code path the
        // daemon takes at runtime.
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("registry.db");
        let conn = open(&p).unwrap();
        (tmp, conn)
    }

    #[test]
    fn empty_registry_has_no_repos() {
        let (_tmp, c) = fresh();
        assert!(list_repos(&c).unwrap().is_empty());
    }

    #[test]
    fn insert_and_find_repo() {
        let (_tmp, c) = fresh();
        let id = insert_repo(
            &c,
            "proj",
            "/Users/foo/proj",
            "abc123",
            1_700_000_000_000_000_000,
        )
        .unwrap();
        assert!(id > 0);
        let r = find_repo_by_alias(&c, "proj").unwrap().unwrap();
        assert_eq!(r.alias, "proj");
        assert_eq!(r.repo_hash, "abc123");
    }

    #[test]
    fn duplicate_alias_rejected() {
        let (_tmp, c) = fresh();
        insert_repo(&c, "a", "/p1", "h1", 1).unwrap();
        let dup = insert_repo(&c, "a", "/p2", "h2", 2);
        assert!(dup.is_err());
    }

    #[test]
    fn delete_repo_cascades_worktrees() {
        let (_tmp, c) = fresh();
        let rid = insert_repo(&c, "p", "/r", "h", 1).unwrap();
        upsert_worktree(&c, rid, "/r", "wh", Some("main"), Some("deadbee")).unwrap();
        assert_eq!(list_worktrees(&c, rid).unwrap().len(), 1);
        delete_repo(&c, "p").unwrap();
        assert!(find_repo_by_alias(&c, "p").unwrap().is_none());
        // worktrees row deleted via ON DELETE CASCADE
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM worktrees", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn upsert_worktree_keeps_id_stable() {
        let (_tmp, c) = fresh();
        let rid = insert_repo(&c, "p", "/r", "h", 1).unwrap();
        let id_a = upsert_worktree(&c, rid, "/r", "wh", Some("main"), Some("a")).unwrap();
        let id_b = upsert_worktree(&c, rid, "/r", "wh", Some("dev"), Some("b")).unwrap();
        assert_eq!(id_a, id_b);
        let w = list_worktrees(&c, rid).unwrap().pop().unwrap();
        assert_eq!(w.current_branch.as_deref(), Some("dev"));
    }

    #[test]
    fn upsert_snapshot_round_trip() {
        let (_tmp, c) = fresh();
        let rid = insert_repo(&c, "p", "/r", "h", 1).unwrap();
        let wid = upsert_worktree(&c, rid, "/r", "wh", Some("main"), None).unwrap();
        let sid = upsert_snapshot(
            &c,
            wid,
            "main",
            "/tmp/main.db",
            SnapshotStatus::Building,
            SnapshotEnrichment::Syntactic,
            None,
            1_000,
            None,
            crate::INDEXER_REVISION,
        )
        .unwrap();
        // Re-upsert should produce the same id and update mutable fields.
        let sid2 = upsert_snapshot(
            &c,
            wid,
            "main",
            "/tmp/main.db",
            SnapshotStatus::Ready,
            SnapshotEnrichment::Syntactic,
            Some(2_000),
            2_000,
            Some(4096),
            crate::INDEXER_REVISION,
        )
        .unwrap();
        assert_eq!(sid, sid2);
        let snap = list_snapshots(&c, wid).unwrap().pop().unwrap();
        assert_eq!(snap.status, SnapshotStatus::Ready);
        assert_eq!(snap.size_bytes, Some(4096));
        assert_eq!(snap.indexer_revision, crate::INDEXER_REVISION);
    }

    #[test]
    fn open_creates_file_and_runs_migrations() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("registry.db");
        let conn = open(&p).unwrap();
        let v: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
    }

    #[test]
    fn unknown_status_string_surfaces_as_schema_corruption() {
        // Bypass our typed inserts to plant an out-of-vocabulary value
        // (simulating either a schema migration drift or a downgrade).
        let (_tmp, c) = fresh();
        let rid = insert_repo(&c, "p", "/r", "h", 1).unwrap();
        let wid = upsert_worktree(&c, rid, "/r", "wh", None, None).unwrap();
        c.execute(
            "INSERT INTO index_snapshots
               (worktree_id, branch, db_path, status, enrichment,
                last_accessed_ns)
             VALUES (?1, 'main', '/tmp/x.db', 'garbage', 'syntactic', 0)",
            params![wid],
        )
        .unwrap();
        let err = list_snapshots(&c, wid).unwrap_err();
        assert!(
            matches!(err, crate::Error::SchemaCorruption(_)),
            "got {err:?}"
        );
    }
}
