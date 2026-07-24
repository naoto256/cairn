//! Daemon-restart recovery for persisted analyzer jobs.
//!
//! The durable job state is one `workspace_analysis_runs` row per
//! `(manifest_id, analyzer_id)` in each per-repo CAS store; the
//! scheduler queue, `JobIndex`, tracked keys and runtime metrics
//! are process-local and vanish with the daemon.
//! `restore_from_db` rebuilds the in-memory side in four phases:
//!
//! 1. Status recovery: flip `running` rows back to `queued`.
//! 2. Cross-store scan: group rows by `job_id`, seed the
//!    daemon-global id allocator above every historical id, every
//!    tombstoned id, and the wall clock, then assign fresh ids to
//!    queued rows persisted with `job_id IS NULL`.
//! 3. Collision recycle: rewrite every row of an ambiguous-id
//!    group to fresh ids, after durably tombstoning the old id in
//!    `index.db::ambiguous_job_ids` (the crash-safety anchor for
//!    the non-atomic per-store rewrites).
//! 4. Advertise dispatch-active (`queued`) rows to the memory
//!    queue; terminal rows remain history only.
//!
//! Nothing is deleted here — garbage collection of terminal rows
//! is `JobManager::prune_jobs`'s domain (`jobs/list.rs`).
use super::*;

/// A single row observed during `restore_from_db`, tagged with the
/// store it lives in and whether it is dispatch-active (queued or a
/// running row that was flipped back to queued). Used to build the
/// cross-store `job_id -> Vec<RestoreRow>` collision map before the
/// recycle rewrite in phase 3. `is_active == false` rows are
/// non-active (any terminal status) — they must still participate
/// in collision detection because `JobId` is an external identifier
/// that clients can still hold, but they are never advertised to
/// the memory queue in phase 4.
#[derive(Debug, Clone)]
struct RestoreRow {
    alias: String,
    repo_hash: String,
    store_path: PathBuf,
    repo_root: PathBuf,
    manifest_id: ManifestId,
    analyzer_id: String,
    is_active: bool,
}

impl JobManager {
    /// Rebuild in-memory job state from every registered store
    /// after a daemon restart. Called once at startup, before
    /// `start_workers` (see `daemon.rs::init_job_manager`), so no
    /// `running` row can belong to this process yet.
    ///
    /// Per-row contract:
    /// * `running` — flipped back to `queued` and re-queued for
    ///   dispatch; the writing daemon is dead, so the attempt is
    ///   treated as never started. `cancel_requested` survives the
    ///   flip, so a pre-restart cancel still lands when a worker
    ///   drains the job.
    /// * `queued` — re-advertised to the memory queue under its
    ///   persisted (or freshly assigned) `job_id`.
    /// * terminal — kept on disk as history and never
    ///   re-dispatched, but still scanned: its `job_id`
    ///   participates in cross-store collision detection and
    ///   raises the allocator floor.
    ///
    /// Failure mode: an error aborts the remaining work. The
    /// phases are per-store loops with no cross-store transaction,
    /// so a partial run can leave some stores rewritten and others
    /// not — the durable ambiguous-id tombstone (phase 3) keeps
    /// that window safe, and the next restart's restore finishes
    /// the rewrite. The daemon treats a restore error as fatal
    /// (fail-closed) rather than starting workers on unseeded
    /// state.
    pub fn restore_from_db(&self) -> Result<()> {
        let index_db_path = self.cas_data_dir.index_db_path();
        let mut index = cas_registry::open(&index_db_path)?;
        let entries = cas_registry::list_all(&index)?;
        // `list_all` returns *alias* rows, not stores. Two aliases
        // pointing at the same `repo_hash` would otherwise cause
        // every phase below to open the same store twice, doubling
        // every queued row into `active_by_id` and triggering a
        // false cross-store collision on rows that are actually the
        // same DB row. Dedupe by `repo_hash` up front and keep the
        // lexicographically-smallest alias as the representative —
        // deterministic because `list_all` is already `ORDER BY
        // alias`. Phase 4's memory-queue Job then carries a
        // canonical alias per store.
        let unique_entries: Vec<_> = {
            let mut seen: HashSet<String> = HashSet::new();
            entries
                .iter()
                .filter(|e| seen.insert(e.repo_hash.clone()))
                .cloned()
                .collect()
        };
        // Load every id previously retired as ambiguous. Any surviving
        // row that still carries one of these ids must be recycled
        // even if no sibling row is present this time — a prior
        // restart partially rewrote a collision group before crashing
        // and we would otherwise resolve `cancel(old_id)` to that last
        // row, silently targeting a still-live sibling of the
        // (already-rewritten) intended job.
        let existing_tombstones = cas_registry::all_ambiguous_job_ids(&index)?;

        // Phase 1: status recovery only. Flip `running` back to
        // `queued` (a `running` row can only have been produced by a
        // now-dead daemon). All `job_id` assignment — including
        // NULL-fill — happens in phase 2 through the daemon-global
        // allocator, strictly after `observed_max` seeding, so no
        // per-store id is ever written to disk.
        for entry in &unique_entries {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open_existing(&store_path)?;
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET status = 'queued', finished_at_ns = NULL, error = NULL
                 WHERE status = 'running'",
                [],
            )?;
        }

        // Phase 2: collect every row across every store — all
        // statuses, not just `queued`. `JobId` is an external
        // identifier (`jobs.list` / `jobs.cancel`), so a terminal
        // row that shares an id with a queued row in a sibling store
        // is a collision even though the terminal row is never
        // re-dispatched: a stale client's `cancel(old_id)` would
        // otherwise silently target the queued sibling. Rows with a
        // concrete `job_id` are grouped by id for collision
        // detection; rows with `job_id IS NULL` (queued only,
        // realistically) get fresh globally-unique ids assigned from
        // the allocator *after* seeding. `observed_max` spans every
        // historical row so the allocator floor clears the whole
        // history.
        let mut all_by_id: HashMap<JobId, Vec<RestoreRow>> = HashMap::new();
        let mut missing_id_rows: Vec<RestoreRow> = Vec::new();
        let mut observed_max: JobId = 0;
        // Tombstoned ids are permanently retired. Historical store
        // rows alone don't enforce that: a prune sweep or a
        // wall-clock rollback could drop the store's `MAX(job_id)`
        // below a tombstoned id, and the allocator would re-issue
        // it. Include the tombstone max in the floor as well.
        if let Some(&max_tomb) = existing_tombstones.iter().max() {
            observed_max = observed_max.max(max_tomb);
        }
        for entry in &unique_entries {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open_existing(&store_path)?;
            // Whole-store historical max — includes terminal rows.
            let store_max: Option<i64> = conn
                .query_row(
                    "SELECT MAX(job_id) FROM workspace_analysis_runs
                     WHERE job_id IS NOT NULL",
                    [],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            if let Some(m) = store_max {
                observed_max = observed_max.max(m);
            }
            let mut stmt = conn.prepare(
                "SELECT job_id, manifest_id, analyzer_id, status
                 FROM workspace_analysis_runs
                 ORDER BY started_at_ns ASC, analyzer_id ASC",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, Option<i64>>(0)?,
                        ManifestId(r.get::<_, i64>(1)?),
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (maybe_id, manifest_id, analyzer_id, status) in rows {
                let is_active = status == "queued";
                let row = RestoreRow {
                    alias: entry.alias.clone(),
                    repo_hash: entry.repo_hash.clone(),
                    store_path: store_path.clone(),
                    repo_root: PathBuf::from(&entry.root_path),
                    manifest_id,
                    analyzer_id,
                    is_active,
                };
                match maybe_id {
                    Some(id) => all_by_id.entry(id).or_default().push(row),
                    None => {
                        // A NULL `job_id` on a terminal row is
                        // meaningless (it was never externally
                        // named) — skip it. Only queued rows realistically
                        // land here.
                        if is_active {
                            missing_id_rows.push(row);
                        }
                    }
                }
            }
        }

        // Seed the daemon-global allocator so any post-restart
        // enqueue (including the NULL-fill and recycles below)
        // starts strictly after every observed active id and after
        // wall-clock `now_ns()` (preserving the monotonic-ish
        // property across restarts).
        self.seed_job_id_allocator_at_least(observed_max.max(now_ns()))?;

        // Assign fresh globally-unique ids to rows whose `job_id`
        // was persisted as NULL. This happens *after* the allocator
        // seed so the new ids clear every historical / tombstoned
        // id, and it uses `allocate_job_id()?` so an allocator at
        // `i64::MAX` fails closed rather than silently reissuing.
        for row in missing_id_rows {
            let new_id = self.allocate_job_id()?;
            let conn = cas_store::open_existing(&row.store_path)?;
            // Identity-only: assign the new `job_id` without
            // touching `cancel_requested`. A queued row whose
            // cancel had already been requested must remain
            // scheduled for cancellation.
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET job_id = ?1
                 WHERE manifest_id = ?2 AND analyzer_id = ?3",
                params![new_id, row.manifest_id.0, row.analyzer_id],
            )?;
            all_by_id.entry(new_id).or_default().push(row);
        }

        // Phase 3: rewrite collision groups. A group needs recycling
        // if its `old_id` was ambiguous *now* (`rows.len() > 1`) OR
        // *previously* (present in `existing_tombstones` from an
        // earlier partial rewrite). Every row in such a group gets a
        // fresh globally-unique `JobId`; truly-unique groups
        // (`rows.len() == 1` and not tombstoned) keep their original
        // id. Critically the pre-restart ambiguous id is **not**
        // reused by any row and is **not** inserted into `JobIndex`,
        // so a stale client holding it will hit the fallback scan
        // and receive `unknown job id`. Preserving one arbitrary row
        // in a collision group would be unsafe — it would resolve
        // the ambiguous id to that row and silently cancel the
        // wrong job.
        //
        // The per-store UPDATE loop is not cross-store atomic: if
        // store B's UPDATE fails after store A's succeeded, store A
        // holds the new id and store B still holds `old_id`, so
        // `cancel(old_id)` would silently target store B's row. The
        // fix is a durable tombstone in `index.db::ambiguous_job_ids`
        // committed **before** any per-store UPDATE. On partial
        // failure the tombstone survives, `cancel(old_id)` returns
        // unknown, and the next restart sees the surviving row's id
        // in `existing_tombstones` and recycles it.
        let mut new_ambiguous_ids: Vec<JobId> = Vec::new();
        for (old_id, rows) in &all_by_id {
            if rows.len() > 1 && !existing_tombstones.contains(old_id) {
                new_ambiguous_ids.push(*old_id);
            }
        }
        if !new_ambiguous_ids.is_empty() {
            let retired_at = now_ns();
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::insert_ambiguous_ids(&tx, &new_ambiguous_ids, retired_at)?;
            tx.commit()?;
        }
        let mut recycled: Vec<(JobId, JobId)> = Vec::new();
        let mut rows_after_recycle: Vec<(JobId, RestoreRow)> = Vec::new();
        for (old_id, rows) in all_by_id {
            let must_recycle = rows.len() > 1 || existing_tombstones.contains(&old_id);
            if !must_recycle {
                let row = rows.into_iter().next().expect("len checked");
                rows_after_recycle.push((old_id, row));
                continue;
            }
            for row in rows {
                let new_id = self.allocate_job_id()?;
                let conn = cas_store::open_existing(&row.store_path)?;
                // Identity-only rewrite: `cancel_requested` is a
                // scheduling flag, not part of the identity, so it
                // must survive the recycle. Otherwise a queued row
                // whose cancel was requested pre-restart would be
                // silently re-armed, and a terminal row's
                // historical `cancel_requested` value would be lost.
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET job_id = ?1
                     WHERE manifest_id = ?2 AND analyzer_id = ?3",
                    params![new_id, row.manifest_id.0, row.analyzer_id],
                )?;
                recycled.push((old_id, new_id));
                rows_after_recycle.push((new_id, row));
            }
        }
        if !recycled.is_empty() {
            debug!(
                collisions = recycled.len(),
                "restore: recycled ambiguous job ids to fresh globally-unique values",
            );
        }

        // Phase 4: advertise every *active* row to the memory queue.
        // Terminal rows are still part of `rows_after_recycle` for
        // collision purposes but must not be dispatched — they are
        // filtered out here by the `is_active` flag. `JobKey` carries
        // `repo_hash`, so `reserve_existing` cannot coalesce two
        // stores that happen to share `(manifest_id, analyzer_id)`.
        for (id, row) in rows_after_recycle {
            if !row.is_active {
                continue;
            }
            let key = JobKey {
                repo_hash: row.repo_hash.clone(),
                manifest_id: row.manifest_id,
                analyzer_id: row.analyzer_id.clone(),
            };
            if !self.tracked_keys.reserve_existing(key.clone()) {
                continue;
            }
            self.runtime_metrics.mark_enqueued(
                id,
                pool_group_for_analyzer_id(&row.analyzer_id).ok().flatten(),
                now_ns(),
            );
            let enqueued = self.enqueue_memory(Job {
                id,
                alias: row.alias.clone(),
                repo_hash: row.repo_hash.clone(),
                store_path: row.store_path.clone(),
                repo_root: row.repo_root.clone(),
                manifest_id: row.manifest_id,
                analyzer_id: row.analyzer_id.clone(),
            });
            if !enqueued {
                self.tracked_keys.release(&key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::tests::*;

    #[test]
    fn restore_recycles_all_rows_in_collision_group_preserves_unique() {
        // Cross-store collision: hash-a and hash-b both persisted
        // job_id=1 for (manifest_id=1, pyright-lsp). A third row on
        // hash-a at (manifest_id=1, ruby-lsp) has a unique job_id=2.
        // After `restore_from_db`, the collision group must have
        // *every* row rewritten (no row keeps job_id=1) and the
        // unique group must be preserved (job_id=2 unchanged). The
        // ambiguous id 1 must not appear on any surviving row.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_a, manifest_id.0, "ruby-lsp", "queued", Some(2));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        let a_pyright = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let b_pyright = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        let a_ruby = persisted_job_id(&conn_a, manifest_id.0, "ruby-lsp");
        assert_ne!(a_pyright, 1, "hash-a's colliding row must be recycled");
        assert_ne!(b_pyright, 1, "hash-b's colliding row must be recycled");
        assert_ne!(
            a_pyright, b_pyright,
            "both recycled ids must be unique across stores"
        );
        assert_eq!(a_ruby, 2, "unique-group row must keep its original id");
    }

    #[test]
    fn cancel_with_ambiguous_pre_restore_job_id_returns_not_found() {
        // A client that held the ambiguous pre-restart job_id=1
        // must not be able to cancel *any* store, because the id no
        // longer identifies a unique job. `cancel(1)` must return
        // `unknown job id` instead of silently cancelling whichever
        // store was scanned first.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        // Both stores now hold fresh ids != 1.
        assert_ne!(persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp"), 1);
        assert_ne!(persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp"), 1);

        // A client cancel with the stale ambiguous id must fail
        // rather than silently target one of the stores.
        let err = manager.cancel(1).unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("unknown job id"),
            "cancel(1) must return unknown job id after collision recycle, got {message}"
        );
    }

    #[test]
    fn two_stores_same_manifest_id_same_analyzer_id_both_enqueue() {
        // Both stores share `(manifest_id=1, pyright-lsp)` but have
        // distinct job_ids (so there is no collision to recycle).
        // `restore_from_db` must load *both* rows into the memory
        // queue — before `JobKey` carried `repo_hash`,
        // `reserve_existing` would have silently coalesced the
        // second.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(2));

        manager.restore_from_db().unwrap();

        // Both rows must be advertised to the memory queue; the
        // JobIndex is the observable proxy for that.
        assert!(
            manager.job_index.get(1).is_some(),
            "hash-a's job_id=1 must be enqueued"
        );
        assert!(
            manager.job_index.get(2).is_some(),
            "hash-b's job_id=2 must be enqueued"
        );
    }

    #[test]
    fn cancel_targets_only_correct_store() {
        // After restore, each store owns a unique job_id. `cancel`
        // must route via `JobIndex.get(job_id).repo_hash` to the
        // matching store and must not touch the sibling store's row.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(2));

        manager.restore_from_db().unwrap();

        manager.cancel(1).unwrap();

        let status_a: String = conn_a
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        let status_b: String = conn_b
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status_a, "cancelled", "hash-a's job must be cancelled");
        assert_eq!(
            status_b, "queued",
            "hash-b's job must be untouched by cancel(1)"
        );
    }

    #[test]
    fn job_index_no_collision_across_stores_after_restore() {
        // Two stores + two unique job_ids => JobIndex holds two
        // locators pointing to different repo_hashes. Neither should
        // overwrite the other.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(11));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(22));

        manager.restore_from_db().unwrap();

        let loc_a = manager.job_index.get(11).expect("job 11 registered");
        let loc_b = manager.job_index.get(22).expect("job 22 registered");
        assert_eq!(loc_a.repo_hash, "hash-a");
        assert_eq!(loc_b.repo_hash, "hash-b");
    }

    #[test]
    fn same_next_job_id_across_stores_does_not_collide_on_active_id() {
        // Both stores end up with `job_id=1` (the shape that
        // per-store `MAX(job_id)+1` produced). After restore, no
        // active row anywhere may still carry that ambiguous id, and
        // the allocator must be seeded above every observed id so
        // subsequent enqueues cannot reissue it.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        let id_a = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let id_b = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(id_a, id_b);
        assert!(id_a != 1 && id_b != 1);

        // A new allocation must land above every observed active id.
        let next = manager.allocate_job_id().unwrap();
        assert!(
            next > id_a && next > id_b,
            "post-restore allocation ({next}) must exceed recycled ids ({id_a}, {id_b})"
        );
    }

    #[test]
    fn allocator_floor_is_reflected_in_next_allocation() {
        // When a floor exceeds the allocator's current counter, the
        // counter must advance past the returned value so the next
        // allocation cannot reissue it. A plain
        // `allocate_job_id().max(floor)` returned `floor` but left
        // the counter at `current + 1`, so a second call with the
        // same floor would return the same id.
        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);
        let floor = 1_000_000i64;
        let first = manager.allocate_job_id_at_least(floor).unwrap();
        let second = manager.allocate_job_id_at_least(floor).unwrap();
        assert_eq!(first, floor, "first allocation must equal the floor");
        assert!(
            second > first,
            "second allocation with same floor must exceed first ({first}), got {second}"
        );
    }

    #[test]
    fn range_floor_advances_allocator_past_entire_range() {
        // Same invariant for the range variant: after handing out
        // `[first, first + count)`, the next single allocation must
        // start past the tail of the range.
        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);
        let floor = 500_000i64;
        let first = manager.allocate_job_id_range_at_least(4, floor).unwrap();
        assert_eq!(first, floor);
        let next = manager.allocate_job_id().unwrap();
        assert!(
            next >= first + 4,
            "single allocation ({next}) must exceed range tail (first={first}, count=4)"
        );
    }

    #[test]
    fn concurrent_allocations_with_same_floor_are_unique() {
        // The CAS loop in `allocate_job_id_at_least` must produce
        // distinct ids under contention, even when every thread
        // supplies the same floor.
        use std::collections::HashSet;
        use std::sync::Arc as StdArc;
        use std::thread;

        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);
        let floor = 100i64;
        let threads = 8;
        let per_thread = 200;
        let mgr = StdArc::new(manager);
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let m = StdArc::clone(&mgr);
                thread::spawn(move || {
                    let mut ids = Vec::with_capacity(per_thread);
                    for _ in 0..per_thread {
                        ids.push(m.allocate_job_id_at_least(floor).unwrap());
                    }
                    ids
                })
            })
            .collect();
        let mut all: HashSet<JobId> = HashSet::new();
        for h in handles {
            for id in h.join().unwrap() {
                assert!(id >= floor, "every id must respect the floor");
                assert!(all.insert(id), "id {id} was handed out twice");
            }
        }
        assert_eq!(all.len(), threads * per_thread);
    }

    #[test]
    fn restore_loads_both_stores_into_memory_queue() {
        // End-to-end: after `restore_from_db`, both stores' active
        // rows are visible in the JobIndex (proxy for the in-memory
        // queue's dispatch set). Without `repo_hash` in `JobKey`,
        // `reserve_existing` would have coalesced the second row and
        // left it stuck in DB.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(100));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(200));

        manager.restore_from_db().unwrap();

        assert!(manager.job_index.get(100).is_some());
        assert!(manager.job_index.get(200).is_some());
    }

    // ─── Durable ambiguous-id tombstone tests ────────────────────
    //
    // The four tests below pin the crash-safety invariant that the
    // collision-recycle in `restore_from_db` remains correct even if
    // per-store UPDATEs fail partway. Without the tombstone the
    // surviving row would keep the old ambiguous id and `cancel`
    // would silently target it.

    #[test]
    fn restore_writes_tombstone_for_collision_group() {
        // The ambiguous old id must appear in `ambiguous_job_ids`
        // *after* restore. This is the durable half of the
        // "cancel-of-ambiguous returns unknown" guarantee — the
        // in-memory `JobIndex` alone is not enough because it is
        // dropped on daemon restart.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));

        manager.restore_from_db().unwrap();

        let index_path = data.path().join("index.db");
        let index = cas_registry::open(&index_path).unwrap();
        assert!(
            cas_registry::is_ambiguous_job_id(&index, 1).unwrap(),
            "ambiguous job_id=1 must be tombstoned"
        );
    }

    #[test]
    fn partial_rewrite_recovery_recycles_surviving_row() {
        // Simulate a partial-rewrite crash: a prior restart
        // tombstoned old_id=1 and rewrote store-a's row to a fresh
        // id, but crashed before store-b's UPDATE landed. The
        // next restart sees only ONE row carrying job_id=1 (no
        // in-restart collision), but the tombstone says "this id was
        // ambiguous once — recycle any survivor." The row must be
        // rewritten to a fresh id.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, _conn_a, conn_b) = two_store_fixture(manifest_id);
        // Only store-b holds the ambiguous id now (store-a was
        // already rewritten by a hypothetical earlier restart).
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));
        // Seed the tombstone as if a prior restart had committed it.
        {
            let mut index = cas_registry::open(&data.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[1], 12345).unwrap();
            tx.commit().unwrap();
        }

        manager.restore_from_db().unwrap();

        let id_b = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(
            id_b, 1,
            "tombstoned surviving row must be recycled to a fresh id, got {id_b}"
        );
    }

    #[test]
    fn cancel_of_tombstoned_id_returns_unknown_without_scan() {
        // Even if a store still holds a row with the tombstoned id
        // (e.g. a mid-restart crash left it there), `cancel(old_id)`
        // must be rejected as `unknown job id` before the per-store
        // scan can hit that row. The tombstone check is
        // authoritative.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, _conn_a, conn_b) = two_store_fixture(manifest_id);
        // Manually seed both a tombstone and a still-live row that
        // carries the tombstoned id — simulating the partial-rewrite
        // window before the next restore_from_db runs.
        {
            let mut index = cas_registry::open(&data.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[42], 12345).unwrap();
            tx.commit().unwrap();
        }
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(42));

        // Bypass restore so the row still carries the tombstoned id.
        let err = manager.cancel(42).unwrap_err();
        assert!(
            format!("{err}").contains("unknown job id"),
            "cancel of tombstoned id must return unknown, got {err}"
        );
        // And the store row must be untouched.
        let status: String = conn_b
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "queued");
    }

    #[test]
    fn restore_seeds_allocator_above_terminal_job_ids() {
        // The allocator floor must clear every historical row's
        // job_id, not just currently-queued ones. `JobId` is an
        // external identifier (surfaced by `jobs list` etc.) so
        // re-issuing an id that already named a terminal run would
        // break identifier stability. Seed a `succeeded` row with a
        // job_id far above any queued row's and confirm the
        // post-restore allocation exceeds it.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        // Active queued row at a small id.
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(100));
        // Historical terminal row at a large id — must not be
        // re-issued. (Different analyzer_id so it doesn't collide
        // with the queued row's PK.)
        insert_job_run(&conn_b, manifest_id.0, "ruby-lsp", "succeeded", Some(5000));

        manager.restore_from_db().unwrap();

        let next = manager.allocate_job_id().unwrap();
        assert!(
            next > 5000,
            "allocator ({next}) must clear historical terminal job_id 5000"
        );
    }

    #[test]
    fn same_repo_hash_multiple_aliases_is_scanned_once() {
        // Two aliases pointing at the same repo_hash must be treated
        // as ONE store during restore. Without dedupe the loop
        // scanned the store twice, doubled every queued row into
        // `active_by_id`, and triggered a false cross-store
        // collision on rows that were actually a single DB row —
        // producing a spurious tombstone and a job_id / DB drift.
        let manifest_id = ManifestId(1);
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            // Both aliases point at the same physical store hash-a.
            cas_registry::upsert(&tx, "alias-a", repo.path().to_str().unwrap(), "hash-a", 1)
                .unwrap();
            cas_registry::upsert(&tx, "alias-b", repo.path().to_str().unwrap(), "hash-a", 2)
                .unwrap();
            tx.commit().unwrap();
        }
        let conn = cas_store::open(&cas_data_dir.store_db_path("hash-a")).unwrap();
        insert_manifest(&conn, manifest_id.0);
        insert_job_run(&conn, manifest_id.0, "pyright-lsp", "queued", Some(42));
        let manager = JobManager::new(cas_data_dir);

        manager.restore_from_db().unwrap();

        // The single physical row must keep its job_id.
        assert_eq!(persisted_job_id(&conn, manifest_id.0, "pyright-lsp"), 42);
        // No tombstone was written — this was not an ambiguous id.
        let index = cas_registry::open(&data.path().join("index.db")).unwrap();
        assert!(!cas_registry::is_ambiguous_job_id(&index, 42).unwrap());
        // Exactly one JobIndex entry (not double-registered).
        assert!(manager.job_index.get(42).is_some());
    }

    #[test]
    fn restore_seeds_allocator_above_tombstoned_ids() {
        // Tombstoned ids are permanently retired. Even if the store
        // row that carried one has since been pruned (so it no
        // longer appears in the whole-store `MAX(job_id)`), the
        // allocator floor must still clear it — otherwise a clock
        // rollback + prune sequence could re-issue a retired
        // ambiguous id.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, _conn_a, _conn_b) = two_store_fixture(manifest_id);
        // Seed a tombstone far above any store row (the fixture
        // stores are empty). No queued rows exist, so historical max
        // is 0. Only the tombstone should push the floor up.
        {
            let mut index = cas_registry::open(&data.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[9_999_999], 12345).unwrap();
            tx.commit().unwrap();
        }

        manager.restore_from_db().unwrap();

        let next = manager.allocate_job_id().unwrap();
        assert!(
            next > 9_999_999,
            "allocator ({next}) must clear tombstoned id 9_999_999"
        );
    }

    #[test]
    fn allocator_overflow_fails_closed() {
        // The allocator is the uniqueness invariant, so overflow
        // past `i64::MAX` must fail closed rather than silently
        // saturate or wrap. A `saturating_add(1)` at `i64::MAX`
        // returns `MAX` unchanged, the CAS commits `next == current`,
        // the returned value is `MAX`, and the very next call
        // returns `MAX` again — a duplicate id.
        let data = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager = JobManager::new(cas_data_dir);

        // Seed the counter to `MAX - 1` — the next single allocation
        // returns `MAX - 1` (advancing the counter to `MAX`), and any
        // further single allocation must error rather than reissue.
        manager
            .seed_job_id_allocator_at_least(JobId::MAX - 2)
            .unwrap();
        let last = manager.allocate_job_id().unwrap();
        assert_eq!(last, JobId::MAX - 1);
        assert!(
            manager.allocate_job_id().is_err(),
            "counter at MAX must fail closed on further single allocation"
        );
        assert!(
            manager.allocate_job_id_at_least(0).is_err(),
            "counter at MAX must fail closed on floored allocation too"
        );

        // A range that would trail past `i64::MAX` must fail closed
        // even from a fresh allocator.
        let data2 = tempfile::tempdir().unwrap();
        let m2 = JobManager::new(Arc::new(CasDataDir::with_root(data2.path().to_path_buf())));
        assert!(
            m2.allocate_job_id_range_at_least(2, JobId::MAX - 1)
                .is_err(),
            "range whose tail overflows must fail closed"
        );

        // Seeding at `i64::MAX` itself (which needs to bump to
        // `MAX + 1`) must also fail closed.
        assert!(
            m2.seed_job_id_allocator_at_least(JobId::MAX).is_err(),
            "seed at i64::MAX must fail closed"
        );
    }

    #[test]
    fn restore_assigns_global_unique_ids_to_null_jobs_across_stores() {
        // Rows whose `job_id` was persisted as NULL must be filled
        // from the daemon-global allocator, not per-store
        // `MAX(job_id)+1`. If two stores both had a NULL row, a
        // per-store filler would hand them the same id and rely on
        // the collision-repair path to fix it — but a Phase 2 error
        // in between would leave the per-store ids persisted. All
        // id assignment goes through the global allocator so no
        // per-store id ever hits disk.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        // Two stores, each with a queued row whose job_id is NULL.
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", None);
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", None);

        manager.restore_from_db().unwrap();

        let id_a = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let id_b = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(id_a, id_b, "the two NULL rows must get distinct ids");
        assert!(manager.job_index.get(id_a).is_some());
        assert!(manager.job_index.get(id_b).is_some());
    }

    #[test]
    fn restore_null_assignment_fails_closed_at_allocator_max() {
        // If the allocator is already at `i64::MAX`, filling a NULL
        // `job_id` row must propagate the allocator's fail-closed
        // error rather than silently reusing an id or writing a
        // bogus value.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, _conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", None);
        // Also seed a tombstone at `i64::MAX - 1` so the Phase 2
        // seed drives the allocator into overflow territory
        // (`observed_max = MAX - 1` -> seed bump to `MAX`, and the
        // subsequent single allocation must fail).
        let data_dir = manager.cas_data_dir();
        let mut index = cas_registry::open(&data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::insert_ambiguous_ids(&tx, &[JobId::MAX - 1], 1).unwrap();
        tx.commit().unwrap();

        let err = manager.restore_from_db().unwrap_err();
        assert!(
            format!("{err}").contains("job id allocator overflowed"),
            "restore must fail closed when allocator overflows, got {err}"
        );
    }

    #[test]
    fn restore_recycles_terminal_plus_queued_collision_and_cancel_cannot_target_active_with_old_id()
    {
        // Cross-store collision where one side is terminal and the
        // other is queued. `JobId` is an external identifier, so a
        // stale client that recorded "job 42 succeeded" for store A
        // must not be able to cancel store B's still-queued job 42
        // just because A's row is not on the dispatch path. Restore
        // must rewrite BOTH the terminal and the queued row to fresh
        // globally-unique ids, tombstone 42, and reject `cancel(42)`.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "succeeded", Some(42));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(42));

        manager.restore_from_db().unwrap();

        let a_id = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let b_id = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(a_id, 42, "terminal side must be recycled");
        assert_ne!(b_id, 42, "queued side must be recycled");
        assert_ne!(a_id, b_id, "both sides must land on distinct ids");

        // `cancel(42)` must return unknown, not silently target B.
        let err = manager.cancel(42).unwrap_err();
        assert!(format!("{err}").contains("unknown job id"));
        let status_b: String = conn_b
            .query_row(
                "SELECT status FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
                rusqlite::params![manifest_id.0],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status_b, "queued", "B's queued row must be untouched");
    }

    #[test]
    fn restore_recycles_terminal_terminal_collision_for_globally_unique_job_list() {
        // Two terminal rows sharing a `JobId` also constitute a
        // collision from the `jobs.list` / external-identifier
        // perspective: two distinct historical runs cannot share the
        // same id. Restore must rewrite both to fresh ids and record
        // the tombstone for the shared old id.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "succeeded", Some(99));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "failed", Some(99));

        manager.restore_from_db().unwrap();

        let a_id = persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp");
        let b_id = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(a_id, 99);
        assert_ne!(b_id, 99);
        assert_ne!(a_id, b_id);
        let index = cas_registry::open(&data.path().join("index.db")).unwrap();
        assert!(
            cas_registry::is_ambiguous_job_id(&index, 99).unwrap(),
            "the shared terminal id must be tombstoned"
        );
        // Neither terminal row is dispatchable — JobIndex must NOT
        // list them (Phase 4 filters by is_active).
        assert!(manager.job_index.get(a_id).is_none());
        assert!(manager.job_index.get(b_id).is_none());
    }

    #[test]
    fn pruning_terminal_collision_cannot_remove_other_store_active_job_index() {
        // Composed invariant: even when a terminal row and an active
        // row shared a `JobId` at persist time, a later `prune_jobs`
        // must not touch the active sibling's `JobIndex` entry.
        // Restore's terminal recycling is what makes this safe —
        // after rewrite the terminal side's id no longer matches
        // the active side's, so `prune_jobs_in_store`'s
        // `remove_many(&orphan_terminal_ids)` on store A cannot
        // touch store B's entry.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        // Terminal orphan in store A (no anchor references
        // manifest_id=1, so it satisfies the prune predicate).
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "succeeded", Some(7));
        // Queued row in store B with the same id.
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(7));

        manager.restore_from_db().unwrap();

        let b_id = persisted_job_id(&conn_b, manifest_id.0, "pyright-lsp");
        assert_ne!(b_id, 7);
        assert!(
            manager.job_index.get(b_id).is_some(),
            "B's active row must be registered under its fresh id"
        );

        // Prune terminal orphans across both stores.
        manager.prune_jobs(None, false).unwrap();

        // B's active JobIndex entry must survive — its id is fresh
        // and unrelated to A's terminal orphan.
        assert!(
            manager.job_index.get(b_id).is_some(),
            "prune must not evict B's active locator via the shared old id"
        );
    }

    #[test]
    fn collision_recycle_preserves_cancel_requested_on_queued_row() {
        // Identity-only rewrite: a queued row whose cancel was
        // requested pre-restart must NOT be silently re-armed by
        // the collision-recycle SQL. `cancel_requested = 1` must
        // survive the id rewrite so the worker still treats the
        // job as cancelled once it drains from the memory queue.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run_with_cancel(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(3), 1);
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(3));

        manager.restore_from_db().unwrap();

        // A's row was recycled to a fresh id but its cancel flag
        // must be intact.
        assert_ne!(persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp"), 3);
        assert_eq!(
            persisted_cancel_requested(&conn_a, manifest_id.0, "pyright-lsp"),
            1,
            "collision recycle must preserve cancel_requested = 1"
        );
        // B's row was untouched by the cancel flag.
        assert_eq!(
            persisted_cancel_requested(&conn_b, manifest_id.0, "pyright-lsp"),
            0
        );
    }

    #[test]
    fn collision_recycle_preserves_cancel_requested_on_terminal_row() {
        // Terminal rows also carry `cancel_requested` as historical
        // record. The recycle must not clobber it.
        let manifest_id = ManifestId(1);
        let (_data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run_with_cancel(
            &conn_a,
            manifest_id.0,
            "pyright-lsp",
            "cancelled",
            Some(8),
            1,
        );
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(8));

        manager.restore_from_db().unwrap();

        assert_ne!(persisted_job_id(&conn_a, manifest_id.0, "pyright-lsp"), 8);
        assert_eq!(
            persisted_cancel_requested(&conn_a, manifest_id.0, "pyright-lsp"),
            1,
            "terminal recycle must preserve cancel_requested = 1"
        );
    }

    #[test]
    fn tombstone_survives_across_manager_reinit() {
        // The tombstone must be durable across daemon restarts —
        // it lives in `index.db`, not in `JobManager` in-memory
        // state. A second `JobManager` opened against the same data
        // dir must still reject `cancel(old_id)`.
        let manifest_id = ManifestId(1);
        let (data, _repo_a, _repo_b, manager, conn_a, conn_b) = two_store_fixture(manifest_id);
        insert_job_run(&conn_a, manifest_id.0, "pyright-lsp", "queued", Some(1));
        insert_job_run(&conn_b, manifest_id.0, "pyright-lsp", "queued", Some(1));
        manager.restore_from_db().unwrap();
        drop(manager);

        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let manager2 = JobManager::new(cas_data_dir);
        // No restore call on manager2 — the tombstone alone must
        // suffice to reject the stale cancel.
        let err = manager2.cancel(1).unwrap_err();
        assert!(format!("{err}").contains("unknown job id"));
    }
}
