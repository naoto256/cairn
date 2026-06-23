//! Per-repo store open ceremony.

use rusqlite::Connection;

use crate::Result;
use crate::cas::schema::MIGRATIONS;
use crate::migration::open_with_migrations;

/// Open (creating if necessary) the per-repo store at `path`. Applies
/// standard pragmas and runs any pending CAS-schema migrations.
///
/// # Errors
/// Filesystem or SQLite failures.
pub fn open(path: &std::path::Path) -> Result<Connection> {
    open_with_migrations(path, MIGRATIONS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("store.db");
        let conn = open(&p).unwrap();
        (tmp, conn)
    }

    #[test]
    fn migrations_run_to_latest_version() {
        let (_tmp, c) = fresh();
        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 6);
    }

    fn table_exists(c: &Connection, name: &str) -> bool {
        c.query_row(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?1",
            [name],
            |_| Ok(()),
        )
        .is_ok()
    }

    #[test]
    fn all_cas_tables_created() {
        let (_tmp, c) = fresh();
        for t in [
            "blobs",
            "symbols",
            "refs",
            "imports",
            "implementations",
            "manifests",
            "manifest_entries",
            "anchors",
            "worktrees",
            "workspace_analysis_runs",
            "resolutions",
            "symbols_fts",
        ] {
            assert!(table_exists(&c, t), "missing table: {t}");
        }
    }

    #[test]
    fn blob_insert_dedup_via_pk() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha1', 'rust', 1, 100)",
            [],
        )
        .unwrap();
        // Same (blob_sha, parser_id) → PK conflict.
        let err = c
            .execute(
                "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
                 VALUES ('sha1', 'rust', 1, 200)",
                [],
            )
            .unwrap_err();
        assert!(err.to_string().contains("UNIQUE") || err.to_string().contains("PRIMARY"));
        // Different parser_id is allowed for the same blob.
        c.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha1', 'rust-analyzer', 1, 300)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn v2_adds_nullable_analyzer_columns() {
        let mut c = Connection::open_in_memory().unwrap();
        crate::migration::apply(&mut c, &crate::cas::schema::MIGRATIONS[..1]).unwrap();
        c.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha1', 'rust', 1, 100)",
            [],
        )
        .unwrap();

        crate::migration::apply(&mut c, crate::cas::schema::MIGRATIONS).unwrap();
        let row: (Option<String>, Option<u32>) = c
            .query_row(
                "SELECT analyzer_id, analyzer_revision FROM blobs
                 WHERE blob_sha = 'sha1' AND parser_id = 'rust'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, (None, None));
    }

    #[test]
    fn v3_deletes_versioned_parser_ids_only() {
        let mut c = Connection::open_in_memory().unwrap();
        crate::migration::apply(&mut c, &crate::cas::schema::MIGRATIONS[..2]).unwrap();
        c.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('stable', 'tree-sitter-rust', 1, 0),
                    ('versioned', 'tree-sitter-rust@0.1.0-alpha.2', 1, 0)",
            [],
        )
        .unwrap();

        crate::migration::apply(&mut c, crate::cas::schema::MIGRATIONS).unwrap();
        let rows: Vec<(String, String)> = c
            .prepare("SELECT blob_sha, parser_id FROM blobs ORDER BY blob_sha")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![("stable".to_string(), "tree-sitter-rust".to_string())]
        );
    }

    #[test]
    fn migrations_run_to_latest_from_v3() {
        let mut c = Connection::open_in_memory().unwrap();
        crate::migration::apply(&mut c, &crate::cas::schema::MIGRATIONS[..2]).unwrap();
        c.execute_batch("PRAGMA user_version = 3").unwrap();

        crate::migration::apply(&mut c, crate::cas::schema::MIGRATIONS).unwrap();

        let v: u32 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 6);
        assert!(table_exists(&c, "workspace_analysis_runs"));
        assert!(table_exists(&c, "resolutions"));
    }

    #[test]
    fn v5_migrates_pending_and_timed_out_runs() {
        let mut c = Connection::open_in_memory().unwrap();
        crate::migration::apply(&mut c, &crate::cas::schema::MIGRATIONS[..4]).unwrap();
        c.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns, error)
             VALUES
               (1, 'queued-lsp', 1, 'cfg', 'pending', 10, NULL),
               (1, 'timeout-lsp', 1, 'cfg', 'failed', 10, 'analyzer timed out after 600s')",
            [],
        )
        .unwrap();

        crate::migration::apply(&mut c, crate::cas::schema::MIGRATIONS).unwrap();

        let rows: Vec<(String, String, Option<i64>, i64)> = c
            .prepare(
                "SELECT analyzer_id, status, job_id, cancel_requested
                 FROM workspace_analysis_runs ORDER BY analyzer_id",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("queued-lsp".into(), "queued".into(), None, 0),
                ("timeout-lsp".into(), "timed_out".into(), None, 0),
            ]
        );
    }

    #[test]
    fn workspace_analysis_runs_track_one_current_run_per_manifest_analyzer() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns)
             VALUES (1, 'rust-analyzer-lsp', 1, 'cfg-a', 'queued', 10)",
            [],
        )
        .unwrap();

        let err = c
            .execute(
                "INSERT INTO workspace_analysis_runs
                   (manifest_id, analyzer_id, analyzer_revision, config_hash,
                    status, started_at_ns)
                 VALUES (1, 'rust-analyzer-lsp', 2, 'cfg-b', 'queued', 20)",
                [],
            )
            .unwrap_err();
        assert!(err.to_string().contains("UNIQUE") || err.to_string().contains("PRIMARY"));
    }

    #[test]
    fn workspace_analysis_run_status_constraint_enforced() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();

        for status in [
            "queued",
            "running",
            "succeeded",
            "failed",
            "skipped",
            "cancelled",
            "timed_out",
        ] {
            c.execute(
                "INSERT INTO workspace_analysis_runs
                   (manifest_id, analyzer_id, analyzer_revision, config_hash,
                    status, started_at_ns)
                 VALUES (1, ?1, 1, 'cfg', ?2, 0)",
                [format!("analyzer-{status}"), status.to_string()],
            )
            .unwrap();
        }

        let err = c
            .execute(
                "INSERT INTO workspace_analysis_runs
                   (manifest_id, analyzer_id, analyzer_revision, config_hash,
                    status, started_at_ns)
                 VALUES (1, 'bad-analyzer', 1, 'cfg', 'unknown', 0)",
                [],
            )
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("check"));
    }

    #[test]
    fn workspace_analysis_runs_cascade_on_manifest_delete() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns)
             VALUES (1, 'rust-analyzer-lsp', 1, 'cfg', 'succeeded', 10)",
            [],
        )
        .unwrap();

        c.execute("DELETE FROM manifests WHERE manifest_id = 1", [])
            .unwrap();

        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM workspace_analysis_runs", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn symbol_cascades_with_blob_delete() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('shaA', 'rust', 1, 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind,
                byte_start, byte_end, line_start, line_end, source)
             VALUES ('shaA', 'rust', 'foo', 'foo', 'function',
                     0, 1, 1, 1, 'syntactic')",
            [],
        )
        .unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM symbols", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        c.execute(
            "DELETE FROM blobs WHERE blob_sha = 'shaA' AND parser_id = 'rust'",
            [],
        )
        .unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM symbols", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn fts_picks_up_inserted_symbol() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('shaB', 'rust', 1, 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind,
                byte_start, byte_end, line_start, line_end, source)
             VALUES ('shaB', 'rust', 'render', 'app::ui::View::render', 'method',
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
    fn manifest_kind_constraint_enforced() {
        let (_tmp, c) = fresh();
        // 'committed' and 'tentative' are accepted.
        c.execute(
            "INSERT INTO manifests (kind, commit_sha, built_at_ns)
             VALUES ('committed', 'abc', 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO manifests (kind, commit_sha, built_at_ns)
             VALUES ('tentative', NULL, 0)",
            [],
        )
        .unwrap();
        // Other kinds rejected by CHECK.
        let err = c
            .execute(
                "INSERT INTO manifests (kind, commit_sha, built_at_ns)
                 VALUES ('whatever', NULL, 0)",
                [],
            )
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("check"));
    }

    #[test]
    fn anchor_blocks_manifest_delete_while_referenced() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'committed', 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
            [],
        )
        .unwrap();
        // RESTRICT must block the delete.
        let err = c
            .execute("DELETE FROM manifests WHERE manifest_id = 1", [])
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("foreign key"));
        // Removing the anchor first frees the manifest.
        c.execute("DELETE FROM anchors WHERE anchor_name = 'HEAD'", [])
            .unwrap();
        c.execute("DELETE FROM manifests WHERE manifest_id = 1", [])
            .unwrap();
    }

    #[test]
    fn manifest_entries_cascade_on_manifest_delete() {
        let (_tmp, c) = fresh();
        c.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/lib.rs', 'sha1'), (1, 'src/main.rs', 'sha2')",
            [],
        )
        .unwrap();
        c.execute("DELETE FROM manifests WHERE manifest_id = 1", [])
            .unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM manifest_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
}
