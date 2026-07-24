//! Read-side job APIs: `jobs` (listing snapshots) and `prune_jobs`
//! (garbage collection of orphaned terminal runs).
//!
//! Both walk the per-store `workspace_analysis_runs` rows; listing
//! additionally decorates rows with process-local runtime metrics
//! (`jobs/metrics.rs`). Rows persisted with `job_id IS NULL`
//! (never externally named) are skipped by listing; pruning
//! deletes them like any other orphan terminal row, but only
//! non-NULL ids take part in the `JobIndex` cleanup.
use super::*;

impl JobManager {
    /// Snapshot jobs across registered repositories.
    ///
    /// Filters: `alias_filter` restricts to one alias (an unknown
    /// alias yields an empty list, not an error — contrast
    /// `prune_jobs`); `state_filter` keeps rows whose persisted
    /// state matches; `options.limit` truncates after the global
    /// sort (highest `job_id` first, i.e. newest first).
    ///
    /// The default view (`options.include_all == false`) keeps, per
    /// store: rows whose manifest a current anchor references, and
    /// within those every queued/running row plus the latest
    /// terminal row per analyzer (`latest_default_job_rows`).
    /// `include_all` returns every row that has a `job_id`.
    ///
    /// Lifecycle gating: in the unscoped enumeration a repository
    /// that is unavailable (Removing / Removed) is silently
    /// skipped; with `alias_filter` set, the unavailability error
    /// propagates instead. A bare `JobManager` without a lifecycle
    /// manager applies no gating.
    ///
    /// Runtime-metric fields (`scheduler_state`, `queued_ms`, ...)
    /// stay `None` for jobs this process never held in memory —
    /// e.g. terminal rows that predate the last daemon restart.
    pub(crate) fn jobs(
        &self,
        alias_filter: Option<&str>,
        state_filter: Option<RunStatus>,
        options: JobListOptions,
    ) -> Result<Vec<JobSnapshot>> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let mut out = Vec::new();
        let enumerate_all = alias_filter.is_none();
        for entry in cas_registry::list_all(&index)? {
            if let Some(alias) = alias_filter
                && entry.alias != alias
            {
                continue;
            }
            let _lease = self
                .lifecycle
                .as_ref()
                .map(|lifecycle| {
                    if enumerate_all {
                        lifecycle.acquire_for_enumeration(&entry.repo_hash)
                    } else {
                        lifecycle.acquire_by_repo_hash(&entry.repo_hash).map(Some)
                    }
                })
                .transpose()?
                .flatten();
            if self.lifecycle.is_some() && _lease.is_none() {
                continue;
            }
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open_existing(&store_path)?;
            let mut rows = if options.include_all {
                collect_all_job_rows(&conn, &entry.alias)?
            } else {
                collect_current_job_rows(&conn, &entry.alias)?
            };
            let observed_at_ns = now_ns();
            for row in &mut rows {
                self.runtime_metrics.decorate(row, observed_at_ns);
            }
            if !options.include_all {
                rows = latest_default_job_rows(rows);
            }
            out.extend(rows.into_iter().filter(|row| {
                state_filter
                    .map(|filter| row.state == filter.as_str())
                    .unwrap_or(true)
            }));
        }
        out.sort_by_key(|job| std::cmp::Reverse(job.job_id));
        if let Some(limit) = options.limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    /// Delete terminal runs whose manifest no current anchor
    /// references ("orphans"), store by store.
    ///
    /// * `repo_filter` — one alias (an unknown alias is
    ///   [`Error::RepoNotFound`], unlike `jobs`) or, when `None`,
    ///   every registered repository.
    /// * `dry_run` — report the would-be counts (rows matching the
    ///   predicate, plus how many of their ids currently sit in
    ///   the `JobIndex`) without deleting anything.
    ///
    /// Active (`queued`/`running`) orphans are deliberately
    /// preserved and logged instead of deleted, so a job for a
    /// superseded manifest is never hidden while it can still run.
    /// Lifecycle gating mirrors `jobs`: unscoped enumeration skips
    /// unavailable repositories, an explicit alias propagates the
    /// error.
    pub fn prune_jobs(&self, repo_filter: Option<&str>, dry_run: bool) -> Result<JobsPruneSummary> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let enumerate_all = repo_filter.is_none();
        let entries = match repo_filter {
            Some(alias) => {
                let entry = cas_registry::lookup_by_alias(&index, alias)?.ok_or_else(|| {
                    Error::RepoNotFound {
                        alias: alias.into(),
                    }
                })?;
                vec![entry]
            }
            None => cas_registry::list_all(&index)?,
        };

        let mut repos = Vec::with_capacity(entries.len());
        let mut total_deleted_runs = 0_u64;
        let mut total_deleted_index_entries = 0_u64;
        for entry in entries {
            let _lease = self
                .lifecycle
                .as_ref()
                .map(|lifecycle| {
                    if enumerate_all {
                        lifecycle.acquire_for_enumeration(&entry.repo_hash)
                    } else {
                        lifecycle.acquire_by_repo_hash(&entry.repo_hash).map(Some)
                    }
                })
                .transpose()?
                .flatten();
            if self.lifecycle.is_some() && _lease.is_none() {
                continue;
            }
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let mut conn = cas_store::open_existing(&store_path)?;
            let active_orphans = count_orphan_active_runs(&conn)?;
            if active_orphans > 0 {
                warn!(
                    alias = %entry.alias,
                    active_orphans,
                    "jobs prune preserved active jobs for manifests no current anchor references"
                );
            }
            let (deleted_runs_count, deleted_index_entries_count) =
                prune_jobs_in_store(&mut conn, &self.job_index, dry_run)?;
            total_deleted_runs = total_deleted_runs.saturating_add(deleted_runs_count);
            total_deleted_index_entries =
                total_deleted_index_entries.saturating_add(deleted_index_entries_count);
            repos.push(JobsPruneRepoSummary {
                alias: entry.alias,
                deleted_runs_count,
                deleted_index_entries_count,
            });
        }
        Ok(JobsPruneSummary {
            repos,
            total_deleted_runs,
            total_deleted_index_entries,
        })
    }
}

/// Every externally named row (`job_id IS NOT NULL`) in one store,
/// newest (highest `job_id`) first — the `include_all` scope.
fn collect_all_job_rows(conn: &Connection, alias: &str) -> Result<Vec<JobSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns, error
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
         ORDER BY job_id DESC",
    )?;
    collect_job_rows(&mut stmt, alias)
}

/// Like `collect_all_job_rows`, but restricted to rows whose
/// manifest at least one current anchor references — the default
/// `jobs list` scope.
fn collect_current_job_rows(conn: &Connection, alias: &str) -> Result<Vec<JobSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns, error
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
           AND manifest_id IN (SELECT DISTINCT manifest_id FROM anchors)
         ORDER BY job_id DESC",
    )?;
    collect_job_rows(&mut stmt, alias)
}

/// Shared WHERE fragment defining a prunable row: terminal status
/// AND its manifest is not referenced by any current anchor. The
/// status list must enumerate exactly the states
/// `RunStatus::is_terminal` accepts (it does today).
const ORPHAN_TERMINAL_RUNS_WHERE: &str = "
    manifest_id NOT IN (SELECT DISTINCT manifest_id FROM anchors)
    AND status IN ('succeeded', 'failed', 'cancelled', 'skipped', 'timed_out')
";

/// Delete one store's orphan terminal rows and drop their
/// `JobIndex` entries; returns `(deleted_runs,
/// deleted_index_entries)`. The non-NULL orphan ids are captured
/// before the IMMEDIATE transaction re-evaluates the predicate for
/// the DELETE, so the index cleanup is a best-effort cache sweep
/// over that pre-delete snapshot; dry-run reuses the same snapshot
/// to count would-be removals without mutating anything.
fn prune_jobs_in_store(
    conn: &mut Connection,
    job_index: &JobIndex,
    dry_run: bool,
) -> Result<(u64, u64)> {
    let job_ids = orphan_terminal_job_ids(conn)?;
    let deleted_runs_count = count_orphan_terminal_runs(conn)?;
    if dry_run {
        return Ok((deleted_runs_count, job_index.count_present(&job_ids)));
    }

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let deleted = tx.execute(
        &format!("DELETE FROM workspace_analysis_runs WHERE {ORPHAN_TERMINAL_RUNS_WHERE}"),
        [],
    )?;
    tx.commit()?;

    let deleted_runs_count = u64::try_from(deleted).unwrap_or(u64::MAX);
    let deleted_index_entries_count = job_index.remove_many(&job_ids);
    Ok((deleted_runs_count, deleted_index_entries_count))
}

fn count_orphan_terminal_runs(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM workspace_analysis_runs WHERE {ORPHAN_TERMINAL_RUNS_WHERE}"),
        [],
        |r| r.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(0))
}

/// Ids of rows matching the prune predicate — the `JobIndex`
/// cleanup / dry-run counting target.
fn orphan_terminal_job_ids(conn: &Connection) -> Result<Vec<JobId>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT job_id FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL AND {ORPHAN_TERMINAL_RUNS_WHERE}"
    ))?;
    let job_ids = stmt
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(job_ids)
}

/// Count queued/running rows on unreferenced manifests. These are
/// *not* pruned — the count only feeds the preservation warning in
/// `prune_jobs`.
fn count_orphan_active_runs(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM workspace_analysis_runs
         WHERE manifest_id NOT IN (SELECT DISTINCT manifest_id FROM anchors)
           AND status IN ('queued', 'running')",
        [],
        |r| r.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(0))
}

/// Map DB rows to `JobSnapshot`s. The store's `started_at_ns` is
/// stamped at queue time (see `queue_analyzer_run`), so it maps to
/// `created_at`. The runtime-only fields begin `None` and are
/// filled by `JobRuntimeMetricsStore::decorate` only for jobs this
/// process tracks; `started_at` itself has no producer on this
/// path and stays `None` (the actual run start is surfaced as
/// `run_started_at`).
fn collect_job_rows(stmt: &mut rusqlite::Statement<'_>, alias: &str) -> Result<Vec<JobSnapshot>> {
    let rows = stmt
        .query_map([], |r| {
            Ok(JobSnapshot {
                job_id: r.get(0)?,
                alias: alias.to_string(),
                analyzer_id: r.get(1)?,
                state: r.get(2)?,
                created_at: r.get(3)?,
                started_at: None,
                finished_at: r.get(4)?,
                error: r.get(5)?,
                pool_group: None,
                scheduler_state: None,
                enqueued_at: None,
                run_started_at: None,
                queued_ms: None,
                pool_wait_ms: None,
                run_ms: None,
                progress_ticks: None,
                last_progress_at: None,
                progress_per_minute: None,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Default-view filter for one store's rows, which arrive newest
/// (highest `job_id`) first: keep every queued/running row, only
/// the first-seen (= latest) terminal row per analyzer, and pass
/// through rows whose state string this build does not recognize
/// (persisted by an older or newer binary) rather than hiding
/// them.
pub(super) fn latest_default_job_rows(rows: Vec<JobSnapshot>) -> Vec<JobSnapshot> {
    let mut seen_terminal = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        match RunStatus::from_str(&row.state) {
            Some(RunStatus::Queued | RunStatus::Running) => out.push(row),
            Some(state) if state.is_terminal() => {
                if seen_terminal.insert(row.analyzer_id.clone()) {
                    out.push(row);
                }
            }
            _ => out.push(row),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::tests::*;

    #[test]
    fn default_job_rows_keep_active_and_latest_terminal_per_analyzer() {
        let rows = vec![
            job(7, "rust-analyzer", "running"),
            job(6, "pyright", "succeeded"),
            job(5, "pyright", "failed"),
            job(4, "gopls", "queued"),
            job(3, "gopls", "succeeded"),
            job(2, "rust-analyzer", "succeeded"),
            job(1, "unknown", "mystery"),
        ];

        let filtered = latest_default_job_rows(rows);
        let ids = filtered.iter().map(|job| job.job_id).collect::<Vec<_>>();

        assert_eq!(ids, vec![7, 6, 4, 3, 2, 1]);
    }

    #[test]
    fn prune_jobs_removes_orphan_terminal_rows_and_job_index_entries() {
        let (_data, _repo, manager, conn) = prune_test_manager();
        insert_manifest(&conn, 1);
        insert_manifest(&conn, 2);
        insert_anchor(&conn, "HEAD", 1);
        insert_job_run(&conn, 1, "current-lsp", "succeeded", Some(12));
        insert_job_run(&conn, 2, "orphan-terminal-lsp", "succeeded", Some(10));
        insert_job_run(&conn, 2, "orphan-active-lsp", "running", Some(11));
        manager.job_index.insert(10, "repo", "repo-hash");
        manager.job_index.insert(11, "repo", "repo-hash");
        manager.job_index.insert(12, "repo", "repo-hash");

        let result = manager.prune_jobs(Some("repo"), false).unwrap();

        assert_eq!(result.total_deleted_runs, 1);
        assert_eq!(result.total_deleted_index_entries, 1);
        assert_eq!(result.repos[0].deleted_runs_count, 1);
        assert_eq!(result.repos[0].deleted_index_entries_count, 1);
        assert_eq!(
            count_runs(&conn, 2, "orphan-terminal-lsp"),
            0,
            "orphan terminal row should be pruned"
        );
        assert_eq!(
            count_runs(&conn, 2, "orphan-active-lsp"),
            1,
            "active orphan rows stay visible instead of being hidden by GC"
        );
        assert_eq!(
            count_runs(&conn, 1, "current-lsp"),
            1,
            "current-anchor terminal rows are retained"
        );
        assert!(manager.job_index.get(10).is_none());
        assert!(manager.job_index.get(11).is_some());
        assert!(manager.job_index.get(12).is_some());
    }

    #[test]
    fn prune_jobs_dry_run_counts_without_deleting_rows_or_index_entries() {
        let (_data, _repo, manager, conn) = prune_test_manager();
        insert_manifest(&conn, 1);
        insert_manifest(&conn, 2);
        insert_anchor(&conn, "HEAD", 1);
        insert_job_run(&conn, 2, "orphan-terminal-lsp", "failed", Some(20));
        manager.job_index.insert(20, "repo", "repo-hash");

        let result = manager.prune_jobs(None, true).unwrap();

        assert_eq!(result.total_deleted_runs, 1);
        assert_eq!(result.total_deleted_index_entries, 1);
        assert_eq!(count_runs(&conn, 2, "orphan-terminal-lsp"), 1);
        assert!(manager.job_index.get(20).is_some());
    }
}
