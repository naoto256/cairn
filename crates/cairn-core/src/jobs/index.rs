use super::*;

/// Routing hint mapping a `JobId` to the store that owns it, so
/// `cancel` can open the right store directly instead of scanning
/// every registered repository.
#[derive(Debug, Clone)]
pub(super) struct JobLocator {
    pub(super) alias: String,
    pub(super) repo_hash: String,
}

/// In-memory, best-effort `JobId -> JobLocator` cache; clones share
/// state through the inner `Arc`. It is not persisted:
/// `restore_from_db` rebuilds it from active rows at startup, and
/// an entry can go stale (its store row deleted or its persisted
/// status unrecognized), so `cancel` treats a hit whose store
/// lookup misses as stale, drops it, and falls back to the full
/// per-store scan.
#[derive(Debug, Clone, Default)]
pub(super) struct JobIndex {
    inner: Arc<Mutex<HashMap<JobId, JobLocator>>>,
}

impl JobIndex {
    pub(super) fn insert(&self, job_id: JobId, alias: &str, repo_hash: &str) {
        self.inner.lock().expect("job index lock poisoned").insert(
            job_id,
            JobLocator {
                alias: alias.to_string(),
                repo_hash: repo_hash.to_string(),
            },
        );
    }

    pub(super) fn get(&self, job_id: JobId) -> Option<JobLocator> {
        self.inner
            .lock()
            .expect("job index lock poisoned")
            .get(&job_id)
            .cloned()
    }

    pub(super) fn remove(&self, job_id: JobId) -> bool {
        self.inner
            .lock()
            .expect("job index lock poisoned")
            .remove(&job_id)
            .is_some()
    }

    /// Bulk-remove locators after a prune; returns how many of the
    /// given ids were actually present.
    pub(super) fn remove_many(&self, job_ids: &[JobId]) -> u64 {
        let mut index = self.inner.lock().expect("job index lock poisoned");
        job_ids
            .iter()
            .filter(|job_id| index.remove(job_id).is_some())
            .count() as u64
    }

    /// Dry-run counterpart of `remove_many`: count how many of the
    /// given ids currently have locators, without removing them.
    pub(super) fn count_present(&self, job_ids: &[JobId]) -> u64 {
        let index = self.inner.lock().expect("job index lock poisoned");
        job_ids
            .iter()
            .filter(|job_id| index.contains_key(job_id))
            .count() as u64
    }
}

/// De-dup reservation set over [`JobKey`]s: at most one queued or
/// running job per `(repo_hash, manifest_id, analyzer_id)` in the
/// in-memory pipeline. Shared between the enqueue path (reserve)
/// and the scheduler (release on finish / cancel / failed send).
#[derive(Debug, Clone, Default)]
pub(super) struct TrackedJobKeys {
    inner: Arc<Mutex<HashSet<JobKey>>>,
}

impl TrackedJobKeys {
    /// De-dup gate for fresh enqueues. The lock is held across the
    /// membership check, the durable row write, and the insert, so
    /// two concurrent identical enqueues serialize: the loser sees
    /// the key and gets `Ok(false)` without writing anything. If
    /// `write_current_row` fails, the key is *not* reserved, so a
    /// later retry stays possible.
    pub(super) fn reserve_after(
        &self,
        key: JobKey,
        write_current_row: impl FnOnce() -> Result<()>,
    ) -> Result<bool> {
        let mut keys = self.inner.lock().expect("tracked job key lock poisoned");
        if keys.contains(&key) {
            return Ok(false);
        }
        write_current_row()?;
        keys.insert(key);
        Ok(true)
    }

    /// Restore-path variant: the run row is already durable, so only
    /// the in-memory reservation is taken. Returns `false` when the
    /// key is already held (the row is coalesced, not re-advertised).
    pub(super) fn reserve_existing(&self, key: JobKey) -> bool {
        self.inner
            .lock()
            .expect("tracked job key lock poisoned")
            .insert(key)
    }

    /// Drop a reservation so the same `(store, manifest, analyzer)`
    /// can be enqueued again. Called when a job leaves the pipeline:
    /// worker finished, cancelled before dispatch, or the memory
    /// enqueue failed after the row write.
    pub(super) fn release(&self, key: &JobKey) {
        self.inner
            .lock()
            .expect("tracked job key lock poisoned")
            .remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_index_round_trips_and_removes_locator() {
        let index = JobIndex::default();
        index.insert(99, "repo", "hash");

        let locator = index.get(99).expect("job locator missing");
        assert_eq!(locator.alias, "repo");
        assert_eq!(locator.repo_hash, "hash");

        index.remove(99);
        assert!(index.get(99).is_none());
    }
}
