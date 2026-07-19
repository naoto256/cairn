use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use cairn_lang_api::{LanguageBackend, pick_backend_for_path, pick_backend_for_shebang};
use rusqlite::params;

use crate::Result;
use crate::manifest::{self, ManifestId};

use super::header_detect::pick_c_family_header_backend;
use super::{WorkspaceAnalyzer, all_workspace_analyzers};

pub(crate) use super::header_detect::is_c_family_header_path;

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

/// Shared backend-selection chain used by both the register hot path's
/// pre-publication parse and the daemon-startup parser-revision
/// staleness scanner.
///
/// Selection priority is: extension match → C-family header
/// disambiguation → shebang. The same chain produces the same
/// `(parser_id, parser_revision)` answer in both callers, so a future
/// drift between "what register reparses" and "what the scanner
/// considers stale" cannot grow from divergent selection logic.
pub(crate) fn pick_backend_with_fallbacks<'a>(
    backends: &'a [Box<dyn LanguageBackend>],
    path: &str,
    content: &[u8],
) -> Option<&'a dyn LanguageBackend> {
    if let Some(backend) = pick_backend_for_path(backends, path) {
        return Some(backend);
    }
    if let Some(backend) = pick_c_family_header_backend(backends, path, content) {
        return Some(backend);
    }
    let first_line = read_first_line(content)?;
    if !first_line.starts_with("#!") {
        return None;
    }
    pick_backend_for_shebang(backends, first_line)
}

fn read_first_line(content: &[u8]) -> Option<&str> {
    let window = &content[..content.len().min(256)];
    let end = window
        .iter()
        .position(|&b| b == b'\n')
        .unwrap_or(window.len());
    std::str::from_utf8(&window[..end]).ok()
}

/// A single `(blob_sha, parser_id, parser_revision)` triple that the
/// current backend set expects to be present in `blobs` for a given
/// manifest.
///
/// Computed by [`expected_parse_units`] starting from the manifest
/// entries and applying the same backend-selection chain
/// (`pick_backend_with_fallbacks`) that the register hot path uses.
/// "Expected" is the right framing for staleness detection: a row
/// missing from `blobs` is just as much a drift signal as a row whose
/// `parser_revision` mismatches the linked-in backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExpectedParseUnit {
    pub blob_sha: String,
    pub parser_id: String,
    pub parser_revision: u32,
}

/// Expected parse units for `manifest_id`, computed against the
/// current backend set.
///
/// Reads manifest entries from the store, runs each path through
/// [`pick_backend_with_fallbacks`] (lazily loading content from the
/// worktree only when the extension match misses), and returns the
/// distinct `(blob_sha, parser_id, parser_revision)` triples the
/// current backend set considers parseable.
///
/// Missing worktree files (entry referenced a file that has since
/// been deleted) and read errors during fallback are silently
/// skipped here because this startup diagnostic is best-effort. Registration's
/// full scan is stricter: any required read error rejects the attempt and
/// preserves the durable reconcile gap.
///
/// Caller is responsible for stopping at the first manifest that
/// matters; this function does not scan every manifest in the store.
pub(crate) fn expected_parse_units(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    repo_root: &Path,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<ExpectedParseUnit>> {
    let entries = manifest::get_entries(conn, manifest_id)?;
    let mut units = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for entry in &entries {
        if let Some(backend) = pick_backend_for_path(backends, &entry.path) {
            push_unit(&mut units, &mut seen, entry.blob_sha.clone(), backend);
            continue;
        }
        // Fallback paths need content. The worktree copy is the
        // ground truth at scan time; if the file has been deleted
        // we can only consult the extension match (which already
        // missed), so skip cleanly.
        let Ok(content) = std::fs::read(repo_root.join(entry.path.as_str())) else {
            continue;
        };
        if let Some(backend) = pick_backend_with_fallbacks(backends, &entry.path, &content) {
            push_unit(&mut units, &mut seen, entry.blob_sha.clone(), backend);
        }
    }
    Ok(units)
}

fn push_unit(
    units: &mut Vec<ExpectedParseUnit>,
    seen: &mut HashSet<(String, String)>,
    blob_sha: String,
    backend: &dyn LanguageBackend,
) {
    let parser_id = backend.parser_id().to_string();
    if seen.insert((blob_sha.clone(), parser_id.clone())) {
        units.push(ExpectedParseUnit {
            blob_sha,
            parser_id,
            parser_revision: backend.parser_revision(),
        });
    }
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
