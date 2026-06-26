use std::collections::HashMap;
use std::collections::HashSet;

use rusqlite::params;

use crate::Result;
use crate::manifest::ManifestId;

use super::{WorkspaceAnalyzer, all_workspace_analyzers};

/// Returns the Tier-3 analyzers that are expected to run for one manifest.
///
/// Reindex, doctor, and query readiness all use this same filter so their
/// answer to "which analyzers should exist for this snapshot?" cannot drift.
pub fn expected_analyzers_for_manifest(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<Vec<Box<dyn WorkspaceAnalyzer>>> {
    let parser_ids = manifest_parser_ids(conn, manifest_id)?;
    Ok(all_workspace_analyzers()
        .into_iter()
        .filter(|analyzer| parser_ids.contains(analyzer.parser_id()))
        .collect())
}

/// Returns `true` iff **every** expected analyzer for `manifest_id` has a
/// `workspace_analysis_runs` row whose `analyzer_revision` matches the
/// linked-in build's current `revision()` **and** whose `status` is
/// `succeeded`.
///
/// Used by `register_repo_inner` as one of three independent gates that
/// decide whether the workspace analyzer pass can be skipped on a
/// re-register where the tentative manifest is byte-identical. Queued /
/// running / failed / skipped / cancelled / timed_out rows all force a
/// re-run — only an outright `succeeded` row at the current revision
/// counts as "facts current", because any other state can leave the
/// resolutions table either empty or partially populated.
///
/// If `expected_analyzers_for_manifest` is empty (no language we analyze
/// at workspace tier appears in the manifest), the answer is trivially
/// `true` — there is no pass to run, so there is nothing to skip past.
pub(crate) fn check_workspace_analyzer_current_succeeded(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<bool> {
    let expected = expected_analyzers_for_manifest(conn, manifest_id)?;
    if expected.is_empty() {
        return Ok(true);
    }

    let mut existing: HashMap<String, (i64, String)> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT analyzer_id, analyzer_revision, status
               FROM workspace_analysis_runs
              WHERE manifest_id = ?1",
        )?;
        let rows = stmt.query_map(params![manifest_id.0], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (analyzer_id, revision, status) = row?;
            existing.insert(analyzer_id, (revision, status));
        }
    }

    for analyzer in &expected {
        let Some((revision, status)) = existing.get(analyzer.id()) else {
            return Ok(false);
        };
        if status != "succeeded" {
            return Ok(false);
        }
        let revision = u32::try_from(*revision).unwrap_or(u32::MAX);
        if revision != analyzer.revision() {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn manifest_parser_ids(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT b.parser_id
           FROM blobs b
           JOIN manifest_entries me ON me.blob_sha = b.blob_sha
          WHERE me.manifest_id = ?1",
    )?;
    let parser_ids = stmt
        .query_map(params![manifest_id.0], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    Ok(parser_ids)
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use crate::cas::store as cas_store;
    use crate::manifest::ManifestId;

    use super::*;

    #[test]
    fn expected_analyzers_for_manifest_filters_by_parser_id() {
        let tmp = tempfile::tempdir().unwrap();
        let conn = cas_store::open(&tmp.path().join("store.db")).unwrap();
        insert_manifest_parser(&conn, ManifestId(1), "fake-sha", "fake-parser");
        insert_manifest_parser(&conn, ManifestId(1), "unknown-sha", "unknown-parser");

        let mut analyzer_ids = expected_analyzers_for_manifest(&conn, ManifestId(1))
            .unwrap()
            .into_iter()
            .map(|analyzer| analyzer.id().to_string())
            .collect::<Vec<_>>();
        analyzer_ids.sort();

        assert_eq!(analyzer_ids, vec!["fake-workspace"]);
    }

    fn insert_run(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        analyzer_id: &str,
        analyzer_revision: u32,
        status: &str,
    ) {
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns)
             VALUES (?1, ?2, ?3, 'cfg', ?4, 10)",
            params![manifest_id.0, analyzer_id, analyzer_revision, status],
        )
        .unwrap();
    }

    /// Vacuous case: a manifest whose parsers don't match any
    /// registered analyzer (e.g. a pure rust manifest in this crate's
    /// test build, since no rust analyzer is linked here) returns
    /// `true` — there is no pass to skip past.
    #[test]
    fn check_workspace_analyzer_current_succeeded_is_vacuously_true_when_no_expected() {
        let tmp = tempfile::tempdir().unwrap();
        let conn = cas_store::open(&tmp.path().join("store.db")).unwrap();
        // unknown-parser has no registered analyzer.
        insert_manifest_parser(&conn, ManifestId(1), "u-sha", "unknown-parser");

        let ok = check_workspace_analyzer_current_succeeded(&conn, ManifestId(1)).unwrap();
        assert!(ok);
    }

    /// Happy path: every expected analyzer has a `succeeded` row at
    /// the current revision. Returns `true`.
    #[test]
    fn check_workspace_analyzer_current_succeeded_accepts_succeeded_at_current_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let conn = cas_store::open(&tmp.path().join("store.db")).unwrap();
        insert_manifest_parser(&conn, ManifestId(1), "fake-sha", "fake-parser");
        // fake-workspace.revision() == 7 in workspace_analyzer/mod.rs tests.
        insert_run(&conn, ManifestId(1), "fake-workspace", 7, "succeeded");

        let ok = check_workspace_analyzer_current_succeeded(&conn, ManifestId(1)).unwrap();
        assert!(ok);
    }

    /// Row absent: an expected analyzer with no run row returns
    /// `false`. The first-ever register on a manifest hits this arm.
    #[test]
    fn check_workspace_analyzer_current_succeeded_rejects_missing_row() {
        let tmp = tempfile::tempdir().unwrap();
        let conn = cas_store::open(&tmp.path().join("store.db")).unwrap();
        insert_manifest_parser(&conn, ManifestId(1), "fake-sha", "fake-parser");

        let ok = check_workspace_analyzer_current_succeeded(&conn, ManifestId(1)).unwrap();
        assert!(!ok);
    }

    /// R2 must-fix #2 pin: revision stale → `false`. Models the case
    /// where the analyzer's linked-in `revision()` bumped between runs
    /// (the targeted scenario for the v0.7.0 auto-reindex feature) and
    /// the existing `succeeded` row references the prior revision.
    #[test]
    fn check_workspace_analyzer_current_succeeded_rejects_stale_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let conn = cas_store::open(&tmp.path().join("store.db")).unwrap();
        insert_manifest_parser(&conn, ManifestId(1), "fake-sha", "fake-parser");
        // fake-workspace.revision() == 7; persisted row is at 6.
        insert_run(&conn, ManifestId(1), "fake-workspace", 6, "succeeded");

        let ok = check_workspace_analyzer_current_succeeded(&conn, ManifestId(1)).unwrap();
        assert!(!ok);
    }

    /// R2 must-fix #2 pin: queued / running / failed / skipped /
    /// cancelled / timed_out must all return `false` even when the
    /// revision matches. A half-finished pass leaves the resolutions
    /// table either empty or partially populated, so it must not
    /// masquerade as up-to-date.
    #[test]
    fn check_workspace_analyzer_current_succeeded_rejects_non_succeeded_status() {
        for status in [
            "queued",
            "running",
            "failed",
            "skipped",
            "cancelled",
            "timed_out",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let conn = cas_store::open(&tmp.path().join("store.db")).unwrap();
            insert_manifest_parser(&conn, ManifestId(1), "fake-sha", "fake-parser");
            insert_run(&conn, ManifestId(1), "fake-workspace", 7, status);

            let ok = check_workspace_analyzer_current_succeeded(&conn, ManifestId(1)).unwrap();
            assert!(!ok, "status {status:?} must NOT count as 'facts current'");
        }
    }

    fn insert_manifest_parser(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        blob_sha: &str,
        parser_id: &str,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (?1, 'tentative', 0)",
            params![manifest_id.0],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![blob_sha, parser_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, ?2, ?3)",
            params![manifest_id.0, format!("{parser_id}.rs"), blob_sha],
        )
        .unwrap();
    }
}
