//! `list_repos` — lightweight registered repository inventory.
//!
//! One row per registered alias, joined with per-repository
//! metadata (languages, snapshot counts, aggregate status). The
//! iteration walks [`cas_registry::list_all`] and does not
//! deduplicate by `repo_hash`: a repository with N aliases produces
//! N `RepoListEntry` rows with identical repo-level fields but the
//! alias-specific slot filled per row.
//!
//! Every call is treated as an enumeration — the optional `query`
//! is only a substring filter over `alias` / `root_path`, so the
//! lifecycle path always uses `acquire_for_enumeration` and drops
//! Removing owners with `partial_truncated("repo_unavailable")`
//! rather than raising. That contrasts with [`super::repo_status`],
//! which asks about exactly one repo and propagates
//! `RepositoryUnavailable` instead.
//!
//! The snapshot summary is computed in two passes: an inside-tx
//! read picks a candidate current snapshot and gathers per-manifest
//! counts, and a post-commit revalidation adjusts current-snapshot
//! status if the manifest went stale between reads (see
//! [`revalidate_status_snapshot`] and [`apply_current_freshness`]).

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
use crate::freshness::{self, EvaluatedSnapshot, SnapshotFreshness, SnapshotStaleReason};
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
        let lifecycle = ctx.lifecycle.clone();

        let (repos, capped, skipped_unavailable) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<_>, bool, bool)> {
                let backends = all_backends();
                let index = cas_registry::open(&cas_data_dir.index_db_path())?;
                let entries = cas_registry::list_all(&index)?;
                let mut out = Vec::with_capacity(entries.len());
                let mut skipped_unavailable = false;
                // `list_all` returns alias rows; a repository with
                // multiple aliases appears in the loop once per alias
                // and produces one `RepoListEntry` per pass. No
                // dedup by `repo_hash` happens on this path.
                for entry in entries {
                    // `query` is a plain substring filter over the
                    // alias and canonical root path — no glob, no
                    // regex, no case folding.
                    if let Some(query) = args.query.as_deref()
                        && !entry.alias.contains(query)
                        && !entry.root_path.contains(query)
                    {
                        continue;
                    }
                    let _lease = match &lifecycle {
                        Some(lifecycle) => {
                            let Some(lease) =
                                lifecycle.acquire_for_enumeration(&entry.repo_hash)?
                            else {
                                skipped_unavailable = true;
                                continue;
                            };
                            Some(lease)
                        }
                        None => None,
                    };
                    let mut conn =
                        cas_store::open_existing(&cas_data_dir.store_db_path(&entry.repo_hash))?;
                    let tx = conn.transaction()?;
                    // Pass 1: pick the current snapshot and derive
                    // per-manifest counts under one read tx so an
                    // anchor move cannot mix rows from two manifests.
                    let selected = select_status_snapshot(
                        &index,
                        &tx,
                        &entry.repo_hash,
                        crate::data_rpc::helpers::system_now_ns(),
                    )?;
                    let mut snapshot_summary = collect_repo_snapshot_summary(&tx, &backends)?;
                    tx.commit()?;
                    // Pass 2: re-validate against the reconcile state
                    // post-commit — if the manifest has since gone
                    // stale, `apply_current_freshness` overrides the
                    // in-tx status label and the aggregate.
                    let current_freshness = revalidate_status_snapshot(
                        &index,
                        &conn,
                        &entry.repo_hash,
                        selected.as_ref(),
                        crate::data_rpc::helpers::system_now_ns(),
                    )?;
                    apply_current_freshness(&mut snapshot_summary, current_freshness);
                    let owner = cas_registry::lookup_repository(&index, &entry.repo_hash)?
                        .ok_or_else(|| Error::RepoNotFound {
                            alias: entry.alias.clone(),
                        })?;
                    out.push(RepoListEntry {
                        alias: entry.alias,
                        root: entry.root_path,
                        persistent: owner.persistent,
                        languages: snapshot_summary.languages,
                        status: snapshot_summary.aggregate_status,
                        snapshot_count: snapshot_summary.summary.snapshot_count,
                        current_file_count: snapshot_summary.summary.current_file_count,
                        current_symbol_count: snapshot_summary.summary.current_symbol_count,
                    });
                }
                // `limit` is a post-collection cap only — no paging
                // cursor is emitted, so the truncated tail is lost
                // for this call.
                let capped = if let Some(limit) = args.pagination.limit {
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
                Ok((out, capped, skipped_unavailable))
            })
            .await
            .map_err(|e| Error::internal_task_panic("list_repos", e))??;

        Ok(serde_json::to_value(ListReposResult {
            repos,
            // Skip-unavailable outranks cap-truncation: an unknown
            // number of rows were dropped, which is a stronger claim
            // than "the tail past `limit` is missing".
            completeness: if skipped_unavailable {
                cairn_proto::Completeness::partial_truncated("repo_unavailable")
            } else if capped {
                cairn_proto::Completeness::partial_truncated("cap")
            } else {
                cairn_proto::Completeness::complete()
            },
            timing: cairn_proto::Timing::default(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(ListRepos);

/// Snapshot-level view of one repository — shared source of truth
/// for `list_repos` inventory rows and the fuller `repo_status`
/// response, so their aggregate counts cannot drift.
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
        .filter(|snapshot| matches!(snapshot.status.as_str(), "stale" | "reconciling"))
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

/// Resolve a filesystem path back to the innermost registered
/// alias whose canonical root is a prefix of `path`.
///
/// Canonicalization is best-effort — if `path` does not exist yet
/// (e.g. it names a file the user is about to create), the raw
/// path is used and the prefix check runs against it directly.
/// When several registered roots are prefixes (nested repos), the
/// longest match wins; for equal-length roots the strict `>`
/// comparison deterministically keeps the first row in `alias`
/// order returned by `list_all`. `Ok(None)` means no registered
/// root contains this path.
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

/// Group `anchors` rows by `manifest_id` and derive one snapshot
/// record per group.
///
/// Multiple anchor names can point at the same manifest (HEAD plus
/// a branch, or several branches at the same commit); their names
/// are folded into the snapshot's `branches` list. The record is
/// ordered by [`anchor::order_key`] of its first internal name so
/// HEAD-style snapshots sort ahead of branch-only ones
/// deterministically.
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

/// Count workspace-analyzer runs in `queued`/`running` state that
/// are still attached to a live anchor. Matches the filter shape
/// used by [`super::list_jobs`] so the two surfaces agree on what
/// counts as "an active job for this repo".
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

/// Collapse per-snapshot statuses plus active-job count into a
/// single aggregate label. Precedence, worst-first:
/// `Error` (no snapshots or any `missing`) > `Indexing` (active
/// job or any `reconciling`) > `Partial` (any `stale` /
/// `no_analyzer`) > `Ready`.
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
    } else if active_jobs > 0
        || snapshots
            .iter()
            .any(|snapshot| snapshot.status == "reconciling")
    {
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

/// Pick a candidate current snapshot for inventory / status.
///
/// Delegates to [`freshness::evaluate_snapshot`] with no explicit
/// anchor / branch, so the default anchor picked by
/// `anchor::resolve_explicit_or_default` is used. A repository
/// whose default anchor is not yet published returns `Ok(None)`
/// instead of the raw `AnchorNotFound`, so it still appears in the
/// inventory as "missing current snapshot".
pub(super) fn select_status_snapshot(
    index: &rusqlite::Connection,
    store: &rusqlite::Connection,
    repo_hash: &str,
    now_ns: i64,
) -> Result<Option<EvaluatedSnapshot>> {
    match freshness::evaluate_snapshot(index, store, repo_hash, None, None, now_ns) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(Error::AnchorNotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Post-commit freshness re-check for the snapshot chosen by
/// [`select_status_snapshot`]. When there was no candidate to
/// begin with, synthesise a stale verdict with
/// `MissingTentative` so the caller can still emit the
/// downgraded status.
pub(super) fn revalidate_status_snapshot(
    index: &rusqlite::Connection,
    store: &rusqlite::Connection,
    repo_hash: &str,
    selected: Option<&EvaluatedSnapshot>,
    now_ns: i64,
) -> Result<SnapshotFreshness> {
    match selected {
        Some(selected) => freshness::revalidate_snapshot(index, store, repo_hash, selected, now_ns),
        None => Ok(SnapshotFreshness::Stale(
            SnapshotStaleReason::MissingTentative,
        )),
    }
}

/// Downgrade the current-snapshot slot of a summary when its
/// post-commit freshness turned stale.
///
/// A non-stale verdict is a no-op. Otherwise the `current` label
/// is overwritten, the matching entry inside `snapshots` (looked
/// up by branch label, which is what `current.anchor` holds) is
/// updated in place, the ready/stale counts are recomputed, and
/// the aggregate is bumped down: any `reconciling` becomes
/// `Indexing`; a `stale` downgrades `Ready` to `Partial` but does
/// not touch existing `Indexing` / `Partial` / `Error` verdicts.
pub(super) fn apply_current_freshness(
    summary: &mut RepoSnapshotSummary,
    freshness: SnapshotFreshness,
) {
    let SnapshotFreshness::Stale(reason) = freshness else {
        return;
    };
    let status = reason.status_label();
    summary.current.status = status.into();
    if let Some(snapshot) = summary.snapshots.iter_mut().find(|snapshot| {
        snapshot
            .branches
            .iter()
            .any(|branch| branch == &summary.current.anchor)
    }) {
        snapshot.status = status.into();
    }
    summary.summary.ready_snapshot_count = summary
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "ready")
        .count() as u32;
    summary.summary.stale_snapshot_count = summary
        .snapshots
        .iter()
        .filter(|snapshot| matches!(snapshot.status.as_str(), "stale" | "reconciling"))
        .count() as u32;
    summary.aggregate_status = match (status, summary.aggregate_status) {
        ("reconciling", _) => RepoAggregateStatus::Indexing,
        ("stale", RepoAggregateStatus::Ready) => RepoAggregateStatus::Partial,
        (_, existing) => existing,
    };
}

/// Per-snapshot status label. In order of check:
/// `empty` (no files in the manifest), `ready` (has symbols),
/// `stale` (analyzer exists for some language but no symbols
/// landed yet), else `no_analyzer` (no analyzer registered for any
/// language present).
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
    use crate::data_rpc::helpers::test_support;
    use crate::lifecycle::RepoLifecycleManager;
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

    #[tokio::test]
    async fn list_repos_generation_gap_is_indexing_not_ready() {
        let fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = applied_generation + 1
                 WHERE repo_hash = ?1",
                params![entry.repo_hash],
            )
            .unwrap();

        let value = ListRepos
            .dispatch(&fixture.ctx, serde_json::Value::Null)
            .await
            .unwrap();
        let result: ListReposResult = serde_json::from_value(value).unwrap();

        assert_eq!(result.repos[0].status, RepoAggregateStatus::Indexing);
    }

    #[tokio::test]
    async fn list_repos_skips_removing_owner_and_marks_inventory_partial() {
        let mut fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        drop(index);
        let lifecycle = RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        lifecycle
            .begin_removal_and_wait(&entry.repo_hash)
            .await
            .unwrap();
        fixture.ctx.lifecycle = Some(lifecycle);

        let value = ListRepos
            .dispatch(&fixture.ctx, serde_json::Value::Null)
            .await
            .unwrap();
        let result: ListReposResult = serde_json::from_value(value).unwrap();
        assert!(result.repos.is_empty());
        assert_eq!(
            result.completeness,
            cairn_proto::Completeness::partial_truncated("repo_unavailable")
        );
    }
}
