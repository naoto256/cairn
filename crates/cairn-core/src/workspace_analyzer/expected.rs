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
