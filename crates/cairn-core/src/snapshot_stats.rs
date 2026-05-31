//! Snapshot stats — the small read-only "how full is this index?"
//! summary that several callers (MCP `list_repos`, `ctl status`, future
//! metrics endpoints) want against an already-built data DB.
//!
//! The shape is intentionally narrow: file count, symbol count, on-disk
//! size, the distinct languages observed. Everything is best-effort: a
//! missing or unreadable DB yields a zeroed [`SnapshotStats`] instead
//! of an error, since stats are reporting data and the caller often
//! cannot do anything actionable with a failure.

use std::path::Path;

use rusqlite::Connection;

/// Read-only summary of one `(worktree, branch)` snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotStats {
    pub file_count: u64,
    pub symbol_count: u64,
    pub size_bytes: u64,
    pub languages: Vec<String>,
}

/// Open `db_path` read-only and compute the four-field summary.
///
/// Best-effort: any open / query failure produces a zeroed
/// [`SnapshotStats`]. Callers that need precise error reporting should
/// open the connection themselves.
#[must_use]
pub fn snapshot_stats(db_path: &Path) -> SnapshotStats {
    let Ok(conn) = Connection::open(db_path) else {
        return SnapshotStats::default();
    };
    let file_count = count(&conn, "SELECT COUNT(*) FROM files");
    let symbol_count = count(&conn, "SELECT COUNT(*) FROM symbols");
    let size_bytes = std::fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let languages = distinct_languages(&conn);
    SnapshotStats {
        file_count,
        symbol_count,
        size_bytes,
        languages,
    }
}

fn count(conn: &Connection, sql: &str) -> u64 {
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap_or(0);
    u64::try_from(n).unwrap_or(0)
}

fn distinct_languages(conn: &Connection) -> Vec<String> {
    let Ok(mut stmt) = conn.prepare("SELECT DISTINCT language FROM files ORDER BY language") else {
        return Vec::new();
    };
    let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) else {
        return Vec::new();
    };
    rows.flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_db;
    use rusqlite::params;

    fn populate(path: &Path) {
        let conn = data_db::open(path).unwrap();
        conn.execute(
            "INSERT INTO files (path, language, blob_sha, size_bytes, mtime_ns, parsed_at_ns, parser)
             VALUES ('a.rs', 'rust', 'sha', 1, 0, 0, 'tree-sitter:rust')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (path, language, blob_sha, size_bytes, mtime_ns, parsed_at_ns, parser)
             VALUES ('b.py', 'python', 'sha', 1, 0, 0, 'tree-sitter:python')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols (file_id, name, qualified, kind, byte_start, byte_end,
                                  line_start, line_end, source)
             VALUES (1, 'foo', 'foo', 'function', 0, 1, 1, 1, 'syntactic')",
            params![],
        )
        .unwrap();
    }

    #[test]
    fn counts_files_symbols_and_distinct_languages() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("snap.db");
        populate(&p);
        let s = snapshot_stats(&p);
        assert_eq!(s.file_count, 2);
        assert_eq!(s.symbol_count, 1);
        assert!(s.size_bytes > 0);
        assert_eq!(s.languages, vec!["python".to_string(), "rust".to_string()]);
    }

    #[test]
    fn missing_db_yields_zero_stats() {
        let s = snapshot_stats(Path::new("/nonexistent/snap.db"));
        assert_eq!(s, SnapshotStats::default());
    }
}
