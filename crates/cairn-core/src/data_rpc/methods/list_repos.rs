//! `list_repos` — lightweight registered repository inventory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cairn_lang_api::{LanguageBackend, all_backends};
use cairn_proto::common::LanguageEnrichment;
use cairn_proto::methods::{
    ListReposArgs, ListReposResult, RepoAggregateStatus, RepoListEntry, RepoSnapshotEntry,
    RepoStatusCurrent, RepoStatusSummary,
};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::anchor;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::enrichment::collect_enrichment;
use crate::manifest::ManifestId;
use crate::{Error, Result};

pub struct ListRepos;

#[async_trait::async_trait]
impl DataMethod for ListRepos {
    fn name(&self) -> &'static str {
        "list_repos"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ListReposArgs = if params.is_null() {
            ListReposArgs::default()
        } else {
            parse_params(params)?
        };
        let cas_data_dir = ctx.cas_data_dir.clone();

        let (repos, capped) = tokio::task::spawn_blocking(move || -> Result<(Vec<_>, bool)> {
            let backends = all_backends();
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = cas_registry::list_all(&index)?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                if let Some(query) = args.query.as_deref()
                    && !entry.alias.contains(query)
                    && !entry.root_path.contains(query)
                {
                    continue;
                }
                let conn = cas_store::open(&cas_data_dir.store_db_path(&entry.repo_hash))?;
                let snapshot_summary = collect_repo_snapshot_summary(&conn, &backends)?;
                out.push(RepoListEntry {
                    alias: entry.alias,
                    root: entry.root_path,
                    languages: snapshot_summary.languages,
                    status: snapshot_summary.aggregate_status,
                    snapshot_count: snapshot_summary.summary.snapshot_count,
                    current_file_count: snapshot_summary.summary.current_file_count,
                    current_symbol_count: snapshot_summary.summary.current_symbol_count,
                });
            }
            let capped = if let Some(limit) = args.limit {
                let limit = limit as usize;
                if out.len() > limit {
                    out.truncate(limit);
                    true
                } else {
                    false
                }
            } else {
                false
            };
            Ok((out, capped))
        })
        .await
        .map_err(|e| Error::internal_task_panic("list_repos", e))??;

        Ok(serde_json::to_value(ListReposResult {
            repos,
            completeness: if capped {
                cairn_proto::Completeness::partial_truncated("cap")
            } else {
                cairn_proto::Completeness::complete()
            },
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(ListRepos);

#[derive(Debug, Clone)]
pub(super) struct RepoSnapshotSummary {
    pub(super) languages: Vec<String>,
    pub(super) summary: RepoStatusSummary,
    pub(super) current: RepoStatusCurrent,
    pub(super) aggregate_status: RepoAggregateStatus,
    pub(super) snapshots: Vec<RepoSnapshotEntry>,
    pub(super) current_manifest_id: Option<ManifestId>,
}

struct SnapshotAcc {
    internal_names: Vec<String>,
    last_updated_ns: i64,
}

struct SnapshotRecord {
    manifest_id: ManifestId,
    sort_internal: String,
    entry: RepoSnapshotEntry,
}

/// Build one snapshot summary per manifest. This remains the shared source for
/// inventory and repo_status so their aggregate counts cannot drift.
pub(super) fn collect_repo_snapshot_summary(
    conn: &rusqlite::Connection,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<RepoSnapshotSummary> {
    let records = collect_snapshot_records(conn, backends)?;
    let current_manifest_id = resolve_current_manifest(conn)?;
    let current = current_snapshot(&records, current_manifest_id);
    let snapshots = records
        .iter()
        .map(|record| record.entry.clone())
        .collect::<Vec<_>>();
    let languages = snapshots
        .iter()
        .flat_map(|snapshot| snapshot.enrichment.iter().map(|e| e.language.clone()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let ready_snapshot_count = snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "ready")
        .count() as u32;
    let stale_snapshot_count = snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "stale")
        .count() as u32;
    let active_jobs = count_active_jobs(conn)?;
    let aggregate_status = derive_aggregate_status(&snapshots, active_jobs);
    Ok(RepoSnapshotSummary {
        languages,
        summary: RepoStatusSummary {
            snapshot_count: snapshots.len() as u32,
            ready_snapshot_count,
            stale_snapshot_count,
            current_file_count: current
                .as_ref()
                .map(|snapshot| snapshot.file_count)
                .unwrap_or_default(),
            current_symbol_count: current
                .as_ref()
                .map(|snapshot| snapshot.symbol_count)
                .unwrap_or_default(),
        },
        current: RepoStatusCurrent {
            anchor: current
                .as_ref()
                .and_then(|snapshot| snapshot.primary_label())
                .unwrap_or("HEAD")
                .to_string(),
            status: current
                .as_ref()
                .map(|snapshot| snapshot.status.clone())
                .unwrap_or_else(|| "missing".into()),
        },
        aggregate_status,
        snapshots,
        current_manifest_id,
    })
}

pub(super) fn resolve_repo_by_path(
    index: &rusqlite::Connection,
    path: &Path,
) -> Result<Option<cas_registry::AliasEntry>> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut best: Option<cas_registry::AliasEntry> = None;
    for entry in cas_registry::list_all(index)? {
        let root = Path::new(&entry.root_path);
        if canonical.starts_with(root) {
            let replace = best
                .as_ref()
                .map(|current| entry.root_path.len() > current.root_path.len())
                .unwrap_or(true);
            if replace {
                best = Some(entry);
            }
        }
    }
    Ok(best)
}

fn collect_snapshot_records(
    conn: &rusqlite::Connection,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<SnapshotRecord>> {
    let mut stmt = conn.prepare(
        "SELECT anchor_name, manifest_id, last_updated_ns
           FROM anchors ORDER BY anchor_name",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut groups: BTreeMap<i64, SnapshotAcc> = BTreeMap::new();
    for (name, manifest_id, last_ns) in rows {
        let acc = groups.entry(manifest_id).or_insert(SnapshotAcc {
            internal_names: Vec::new(),
            last_updated_ns: last_ns,
        });
        acc.internal_names.push(name);
        if last_ns > acc.last_updated_ns {
            acc.last_updated_ns = last_ns;
        }
    }

    let mut entries: Vec<SnapshotRecord> = Vec::with_capacity(groups.len());
    for (manifest_id, mut acc) in groups {
        acc.internal_names.sort_by_key(|a| anchor::order_key(a));
        let sort_internal = acc.internal_names.first().cloned().unwrap_or_default();
        let branches = acc
            .internal_names
            .iter()
            .map(|n| n.strip_prefix("branch/").unwrap_or(n).to_string())
            .collect();
        let file_count = count_manifest_files(conn, manifest_id)?;
        let symbol_count = count_manifest_symbols(conn, manifest_id)?;
        let enrichment = collect_enrichment(conn, manifest_id, backends)?;
        let status = derive_status(file_count, symbol_count, &enrichment);
        entries.push(SnapshotRecord {
            manifest_id: ManifestId(manifest_id),
            sort_internal,
            entry: RepoSnapshotEntry {
                branches,
                status,
                enrichment,
                last_accessed: Some(crate::timefmt::ns_to_rfc3339_utc(acc.last_updated_ns)),
                file_count: u64::try_from(file_count).unwrap_or(0),
                symbol_count: u64::try_from(symbol_count).unwrap_or(0),
            },
        });
    }
    entries.sort_by_key(|record| anchor::order_key(&record.sort_internal));
    Ok(entries)
}

#[cfg(test)]
fn collect_snapshots(
    conn: &rusqlite::Connection,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<RepoSnapshotEntry>> {
    Ok(collect_snapshot_records(conn, backends)?
        .into_iter()
        .map(|record| record.entry)
        .collect())
}

fn current_snapshot(
    snapshots: &[SnapshotRecord],
    current_manifest_id: Option<ManifestId>,
) -> Option<RepoSnapshotEntry> {
    let current = current_manifest_id?;
    snapshots
        .iter()
        .find(|snapshot| snapshot.manifest_id == current)
        .map(|snapshot| snapshot.entry.clone())
}

fn resolve_current_manifest(conn: &rusqlite::Connection) -> Result<Option<ManifestId>> {
    let anchor = anchor::resolve_explicit_or_default(conn, None, None)?;
    anchor::resolve(conn, &anchor)
}

fn count_manifest_files(conn: &rusqlite::Connection, manifest_id: i64) -> Result<i64> {
    Ok(conn
        .query_row(
            "SELECT COUNT(*) FROM manifest_entries WHERE manifest_id = ?1",
            params![manifest_id],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

fn count_manifest_symbols(conn: &rusqlite::Connection, manifest_id: i64) -> Result<i64> {
    Ok(conn
        .query_row(
            "SELECT COUNT(*) FROM symbols s
               JOIN manifest_entries me ON me.blob_sha = s.blob_sha
              WHERE me.manifest_id = ?1",
            params![manifest_id],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

fn count_active_jobs(conn: &rusqlite::Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
           AND manifest_id IN (SELECT DISTINCT manifest_id FROM anchors)
           AND status IN ('queued', 'running')",
        [],
        |r| r.get(0),
    )?)
}

fn derive_aggregate_status(
    snapshots: &[RepoSnapshotEntry],
    active_jobs: i64,
) -> RepoAggregateStatus {
    if snapshots.is_empty()
        || snapshots
            .iter()
            .any(|snapshot| snapshot.status == "missing")
    {
        RepoAggregateStatus::Error
    } else if active_jobs > 0 {
        RepoAggregateStatus::Indexing
    } else if snapshots
        .iter()
        .any(|snapshot| snapshot.status == "stale" || snapshot.status == "no_analyzer")
    {
        RepoAggregateStatus::Partial
    } else {
        RepoAggregateStatus::Ready
    }
}

fn derive_status(file_count: i64, symbol_count: i64, enrichment: &[LanguageEnrichment]) -> String {
    if file_count == 0 {
        return "empty".into();
    }
    if symbol_count > 0 {
        return "ready".into();
    }
    if enrichment.iter().any(|e| e.has_analyzer) {
        "stale".into()
    } else {
        "no_analyzer".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_lang_markdown as _;
    use cairn_lang_python as _;
    use cairn_lang_rust as _;
    use cairn_proto::SourceTier;

    use crate::cas::store;
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[test]
    fn list_repos_emits_lightweight_inventory() {
        let (repo, _sha) = init_repo(&[
            ("src/lib.rs", "pub fn f() {}\n"),
            ("script.py", "def greet():\n    return 'hi'\n"),
            ("README.md", "# Hi\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 1000).unwrap();

        let summary = collect_repo_snapshot_summary(&conn, &all_backends()).unwrap();

        assert_eq!(summary.languages, vec!["markdown", "python", "rust"]);
        assert_eq!(summary.summary.snapshot_count, 2);
        assert!(summary.summary.current_file_count > 0);
        assert_eq!(summary.aggregate_status, RepoAggregateStatus::Ready);
    }

    #[test]
    fn list_repos_snapshot_summary_keeps_enrichment_matrix() {
        let (repo, _sha) = init_repo(&[
            ("src/lib.rs", "pub fn f() {}\n"),
            ("script.py", "def greet():\n    return 'hi'\n"),
            ("README.md", "# Hi\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 1000).unwrap();

        let snapshots = collect_snapshots(&conn, &all_backends()).unwrap();
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
    }
}
