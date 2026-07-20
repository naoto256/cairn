//! `status` — daemon health + every CAS-registered repo with the
//! anchors its store knows about.

use std::collections::BTreeMap;

use cairn_lang_api::{LanguageBackend, all_backends};
use cairn_proto::control::{
    DaemonInitializationStatus, JobSummary, RepoStatus, SnapshotStatus as ProtoSnapshotStatus,
    StatusReport,
};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::anchor;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::enrichment::collect_enrichment;
use crate::{Error, Result};

struct Status;

#[async_trait::async_trait]
impl ControlMethod for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn dispatch(&self, ctx: &CtlCtx, _params: Value) -> Result<Value> {
        let uptime = ctx.started_at.elapsed().as_secs();
        let version = ctx.version.to_string();
        let cas_data_dir = ctx.cas_data_dir.clone();
        let lifecycle = ctx.lifecycle.clone();

        let repos = tokio::task::spawn_blocking(move || -> Result<Vec<RepoStatus>> {
            let backends = all_backends();
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = cas_registry::list_all(&index)?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let _lease = match &lifecycle {
                    Some(lifecycle) => {
                        let Some(lease) = lifecycle.acquire_for_enumeration(&entry.repo_hash)?
                        else {
                            continue;
                        };
                        Some(lease)
                    }
                    None => None,
                };
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let store_bytes = std::fs::metadata(&store_path).map(|m| m.len()).unwrap_or(0);
                let conn = cas_store::open_existing(&store_path)?;
                let snapshots = collect_anchor_snapshots(&conn, store_bytes, &backends)?;
                let job_summary = collect_job_summary(&conn)?;
                // PR3 Phase 4: durable reconcile state per repo.
                // Two aliases with the same repo_hash both carry
                // the same object. Fail-closed on missing state
                // row when the repository exists — that shape
                // would indicate DB corruption (v4 seeds a
                // reconcile_state row alongside every repositories
                // row and FK ON DELETE CASCADE tears them down
                // together).
                let reconcile = match cas_registry::get_reconcile_state(&index, &entry.repo_hash)? {
                    Some(state) => {
                        let aliases = cas_registry::aliases_for_repo(&index, &entry.repo_hash)?;
                        Some(state.to_wire(&entry.repo_hash, aliases))
                    }
                    None => {
                        return Err(Error::Internal(format!(
                            "repo_reconcile_state row missing for repo_hash={} (alias={})",
                            entry.repo_hash, entry.alias
                        )));
                    }
                };
                let persistent = cas_registry::lookup_repository(&index, &entry.repo_hash)?
                    .ok_or_else(|| missing_status_repository(&entry.alias))?
                    .persistent;
                out.push(RepoStatus {
                    alias: entry.alias,
                    root: entry.root_path,
                    persistent,
                    snapshots,
                    job_summary,
                    jobs: Vec::new(),
                    reconcile,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::internal_task_panic("status", e))??;

        Ok(serde_json::to_value(StatusReport {
            daemon_version: version,
            uptime_secs: uptime,
            initialization: DaemonInitializationStatus::ready(),
            repos,
        })
        .unwrap())
    }
}

fn missing_status_repository(alias: &str) -> Error {
    Error::RepoNotFound {
        alias: alias.to_owned(),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Status);

struct SnapshotAcc {
    internal_names: Vec<String>,
}

fn collect_anchor_snapshots(
    conn: &rusqlite::Connection,
    store_bytes: u64,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<ProtoSnapshotStatus>> {
    let mut stmt =
        conn.prepare("SELECT anchor_name, manifest_id FROM anchors ORDER BY anchor_name")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut groups: BTreeMap<i64, SnapshotAcc> = BTreeMap::new();
    for (name, manifest_id) in rows {
        groups
            .entry(manifest_id)
            .or_insert(SnapshotAcc {
                internal_names: Vec::new(),
            })
            .internal_names
            .push(name);
    }

    let mut entries: Vec<(String, ProtoSnapshotStatus)> = Vec::with_capacity(groups.len());
    for (manifest_id, mut acc) in groups {
        acc.internal_names.sort_by_key(|a| anchor::order_key(a));
        let sort_internal = acc.internal_names.first().cloned().unwrap_or_default();
        let branches = acc
            .internal_names
            .iter()
            .map(|n| n.strip_prefix("branch/").unwrap_or(n).to_string())
            .collect();
        let file_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM manifest_entries WHERE manifest_id = ?1",
                params![manifest_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let symbol_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols s
                   JOIN manifest_entries me ON me.blob_sha = s.blob_sha
                  WHERE me.manifest_id = ?1",
                params![manifest_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        entries.push((
            sort_internal,
            ProtoSnapshotStatus {
                branches,
                status: "ready".into(),
                enrichment: collect_enrichment(conn, manifest_id, backends)?,
                file_count: u64::try_from(file_count).unwrap_or(0),
                symbol_count: u64::try_from(symbol_count).unwrap_or(0),
                // The CAS store is shared across anchors; reporting the
                // file's size on every anchor row matches the legacy
                // per-snapshot-DB wire shape closely enough for the
                // current dashboards.
                size_bytes: store_bytes,
            },
        ));
    }
    entries.sort_by_key(|(a, _)| anchor::order_key(a));
    Ok(entries.into_iter().map(|(_, e)| e).collect())
}

fn collect_job_summary(conn: &rusqlite::Connection) -> Result<JobSummary> {
    let mut stmt = conn.prepare(
        "SELECT status, COUNT(*)
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
           AND manifest_id IN (SELECT DISTINCT manifest_id FROM anchors)
         GROUP BY status",
    )?;
    let mut rows = stmt.query([])?;
    let mut summary = JobSummary::default();
    while let Some(row) = rows.next()? {
        let state: String = row.get(0)?;
        let count: u64 = row.get::<_, i64>(1).unwrap_or(0).try_into().unwrap_or(0);
        match state.as_str() {
            "queued" => summary.queued += count,
            "running" => summary.running += count,
            "succeeded" => summary.succeeded += count,
            "skipped" => summary.skipped += count,
            "failed" => summary.failed += count,
            "timed_out" => summary.timed_out += count,
            "cancelled" => summary.cancelled += count,
            _ => summary.other += count,
        }
        summary.total += count;
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_status_repository_reports_the_user_facing_alias() {
        let error = missing_status_repository("demo");

        assert!(matches!(
            error,
            Error::RepoNotFound { alias } if alias == "demo"
        ));
    }
    use cairn_lang_api::all_backends;
    use cairn_lang_markdown as _;
    use cairn_lang_python as _;
    use cairn_lang_rust as _;
    use cairn_proto::SourceTier;

    use crate::cas::store;
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[test]
    fn status_emits_per_language_enrichment_matrix() {
        let (repo, _sha) = init_repo(&[
            (
                "src/lib.rs",
                "pub trait T {}\npub struct S;\nimpl T for S {}\n",
            ),
            ("script.py", "def greet():\n    return 'hi'\n"),
            ("README.md", "# Hi\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 1000).unwrap();

        let backends = all_backends();
        let snapshots = collect_anchor_snapshots(&conn, 123, &backends).unwrap();
        let snapshot = snapshots.iter().find(|s| s.has_head()).unwrap();
        let languages: Vec<&str> = snapshot
            .enrichment
            .iter()
            .map(|e| e.language.as_str())
            .collect();
        assert_eq!(languages, vec!["markdown", "python", "rust"]);

        let rust = snapshot
            .enrichment
            .iter()
            .find(|e| e.language == "rust")
            .unwrap();
        assert!(rust.has_analyzer);
        assert_eq!(rust.tier, SourceTier::Semantic);

        let markdown = snapshot
            .enrichment
            .iter()
            .find(|e| e.language == "markdown")
            .unwrap();
        assert!(!markdown.has_analyzer);
        assert_eq!(markdown.tier, SourceTier::Syntactic);
        assert_eq!(snapshot.size_bytes, 123);
    }

    #[test]
    fn status_dedups_anchors_by_manifest_id() {
        let (repo, _sha) = init_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 1000).unwrap();

        let backends = all_backends();
        let snapshots = collect_anchor_snapshots(&conn, 123, &backends).unwrap();

        let head_main = snapshots.iter().find(|s| s.has_head()).unwrap();
        assert_eq!(head_main.branches, vec!["HEAD", "main"]);

        let tentative = snapshots
            .iter()
            .find(|s| s.branches.iter().any(|b| b.starts_with("tentative/")))
            .unwrap();
        assert_eq!(tentative.branches.len(), 1);
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots[0].has_head());
    }
}
