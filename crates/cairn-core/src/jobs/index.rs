use super::*;

#[derive(Debug, Clone)]
pub(super) struct JobLocator {
    pub(super) alias: String,
    pub(super) repo_hash: String,
}

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

    pub(super) fn remove_many(&self, job_ids: &[JobId]) -> u64 {
        let mut index = self.inner.lock().expect("job index lock poisoned");
        job_ids
            .iter()
            .filter(|job_id| index.remove(job_id).is_some())
            .count() as u64
    }

    pub(super) fn count_present(&self, job_ids: &[JobId]) -> u64 {
        let index = self.inner.lock().expect("job index lock poisoned");
        job_ids
            .iter()
            .filter(|job_id| index.contains_key(job_id))
            .count() as u64
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct TrackedJobKeys {
    inner: Arc<Mutex<HashSet<JobKey>>>,
}

impl TrackedJobKeys {
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

    pub(super) fn reserve_existing(&self, key: JobKey) -> bool {
        self.inner
            .lock()
            .expect("tracked job key lock poisoned")
            .insert(key)
    }

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
