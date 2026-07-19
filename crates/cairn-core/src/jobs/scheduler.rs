use super::*;

#[derive(Debug)]
pub(super) enum SchedulerMsg {
    Enqueue(Job),
    WorkerFinished {
        job_id: JobId,
        pool_group: Option<&'static str>,
        key: JobKey,
    },
    Cancel(JobId),
    Shutdown,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum GroupLane {
    Pooled(&'static str),
    Unpooled,
}

pub(super) async fn scheduler_loop(
    mut receiver: mpsc::UnboundedReceiver<SchedulerMsg>,
    worker_tx: mpsc::UnboundedSender<DispatchJob>,
    worker_capacity: usize,
    runtime_metrics: JobRuntimeMetricsStore,
    tracked_keys: TrackedJobKeys,
) {
    let mut scheduler =
        JobScheduler::new(worker_tx, worker_capacity, runtime_metrics, tracked_keys);
    while let Some(message) = receiver.recv().await {
        match message {
            SchedulerMsg::Enqueue(job) => scheduler.enqueue(job),
            SchedulerMsg::WorkerFinished {
                job_id,
                pool_group,
                key,
            } => scheduler.worker_finished(job_id, pool_group, key),
            SchedulerMsg::Cancel(job_id) => scheduler.cancel(job_id),
            SchedulerMsg::Shutdown => break,
        }
        scheduler.drain_runnable();
    }
}

pub(super) struct JobScheduler {
    worker_tx: mpsc::UnboundedSender<DispatchJob>,
    worker_capacity: usize,
    runtime_metrics: JobRuntimeMetricsStore,
    lanes: HashMap<GroupLane, VecDeque<Job>>,
    ready_order: VecDeque<GroupLane>,
    pub(super) active_groups: HashSet<&'static str>,
    pub(super) active_workers: usize,
    tracked_keys: TrackedJobKeys,
    cancelled_jobs: HashSet<JobId>,
}

impl JobScheduler {
    pub(super) fn new(
        worker_tx: mpsc::UnboundedSender<DispatchJob>,
        worker_capacity: usize,
        runtime_metrics: JobRuntimeMetricsStore,
        tracked_keys: TrackedJobKeys,
    ) -> Self {
        Self {
            worker_tx,
            worker_capacity,
            runtime_metrics,
            lanes: HashMap::new(),
            ready_order: VecDeque::new(),
            active_groups: HashSet::new(),
            active_workers: 0,
            tracked_keys,
            cancelled_jobs: HashSet::new(),
        }
    }

    pub(super) fn enqueue(&mut self, job: Job) {
        let lane = self.lane_for(&job);
        if let GroupLane::Pooled(group) = lane
            && self.active_groups.contains(group)
        {
            self.runtime_metrics.mark_waiting_pool_group(job.id, group);
        }
        let was_empty = self.lanes.get(&lane).is_none_or(VecDeque::is_empty);
        self.lanes.entry(lane).or_default().push_back(job);
        if was_empty && !self.ready_order.contains(&lane) {
            self.ready_order.push_back(lane);
        }
    }

    pub(super) fn cancel(&mut self, job_id: JobId) {
        self.cancelled_jobs.insert(job_id);
        self.remove_pending_job_id(job_id);
    }

    pub(super) fn worker_finished(
        &mut self,
        _job_id: JobId,
        pool_group: Option<&'static str>,
        _key: JobKey,
    ) {
        self.active_workers = self.active_workers.saturating_sub(1);
        if let Some(group) = pool_group {
            self.active_groups.remove(group);
            let lane = GroupLane::Pooled(group);
            if self.lanes.get(&lane).is_some_and(|jobs| !jobs.is_empty())
                && !self.ready_order.contains(&lane)
            {
                self.ready_order.push_back(lane);
            }
        }
        self.cancelled_jobs.remove(&_job_id);
        self.tracked_keys.release(&_key);
    }

    pub(super) fn drain_runnable(&mut self) {
        while self.active_workers < self.worker_capacity {
            let Some(lane) = self.ready_order.pop_front() else {
                break;
            };
            if matches!(lane, GroupLane::Pooled(group) if self.active_groups.contains(group)) {
                self.ready_order.push_back(lane);
                if self.ready_order.iter().all(|candidate| {
                    matches!(candidate, GroupLane::Pooled(group) if self.active_groups.contains(group))
                }) {
                    break;
                }
                continue;
            }
            let Some(job) = self.pop_next_job(lane) else {
                continue;
            };
            let pool_group = match lane {
                GroupLane::Pooled(group) => {
                    self.active_groups.insert(group);
                    Some(group)
                }
                GroupLane::Unpooled => None,
            };
            let key = JobKey::from_job(&job);
            self.runtime_metrics.mark_running(job.id);
            if self
                .worker_tx
                .send(DispatchJob {
                    job,
                    pool_group,
                    key: key.clone(),
                })
                .is_err()
            {
                if let Some(group) = pool_group {
                    self.active_groups.remove(group);
                }
                self.tracked_keys.release(&key);
                break;
            }
            self.active_workers += 1;
            if self.lanes.get(&lane).is_some_and(|jobs| !jobs.is_empty())
                && !self.ready_order.contains(&lane)
            {
                self.ready_order.push_back(lane);
            }
        }
    }

    fn pop_next_job(&mut self, lane: GroupLane) -> Option<Job> {
        loop {
            let jobs = self.lanes.get_mut(&lane)?;
            let job = jobs.pop_front()?;
            if self.cancelled_jobs.remove(&job.id) {
                self.tracked_keys.release(&JobKey::from_job(&job));
                continue;
            }
            return Some(job);
        }
    }

    fn remove_pending_job_id(&mut self, job_id: JobId) {
        for jobs in self.lanes.values_mut() {
            if let Some(index) = jobs.iter().position(|job| job.id == job_id) {
                if let Some(job) = jobs.remove(index) {
                    self.tracked_keys.release(&JobKey::from_job(&job));
                }
                return;
            }
        }
    }

    fn lane_for(&self, job: &Job) -> GroupLane {
        #[cfg(test)]
        if let Some(group) = test_pool_group_for_analyzer_id(&job.analyzer_id) {
            return group.map_or(GroupLane::Unpooled, GroupLane::Pooled);
        }

        match pool_group_for_analyzer_id(&job.analyzer_id) {
            Ok(Some(group)) => GroupLane::Pooled(group),
            Ok(None) => GroupLane::Unpooled,
            Err(err) => {
                warn!(
                    analyzer_id = %job.analyzer_id,
                    error = %err,
                    "unknown analyzer in scheduler; dispatching without pool group"
                );
                GroupLane::Unpooled
            }
        }
    }
}

#[cfg(test)]
fn test_pool_group_for_analyzer_id(analyzer_id: &str) -> Option<Option<&'static str>> {
    match analyzer_id {
        "clangd-c-lsp" | "clangd-cpp-lsp" | "clangd-objc-lsp" => Some(Some("clangd-lsp")),
        "typescript-language-server-ts-lsp"
        | "typescript-language-server-js-lsp"
        | "typescript-language-server-tsx-lsp" => Some(Some("typescript-language-server-lsp")),
        "pyright-lsp" | "gopls-lsp" | "ruby-lsp" => Some(None),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::jobs::tests::*;

    #[test]
    fn scheduler_serializes_same_pool_group() {
        let (mut scheduler, mut rx) = test_scheduler(2);
        scheduler.enqueue(test_job(1, 1, "clangd-c-lsp"));
        scheduler.enqueue(test_job(2, 2, "clangd-cpp-lsp"));
        scheduler.drain_runnable();

        let first = rx.try_recv().expect("first same-group job dispatched");
        assert_eq!(first.job.id, 1);
        assert!(rx.try_recv().is_err(), "second same-group job ran early");
        assert!(scheduler.active_groups.contains("clangd-lsp"));
    }

    #[test]
    fn scheduler_dispatches_different_group_while_one_waits() {
        let (mut scheduler, mut rx) = test_scheduler(2);
        scheduler.enqueue(test_job(1, 1, "clangd-c-lsp"));
        scheduler.enqueue(test_job(2, 2, "clangd-cpp-lsp"));
        scheduler.enqueue(test_job(3, 3, "typescript-language-server-ts-lsp"));
        scheduler.drain_runnable();

        let dispatched = drain_dispatched(&mut rx);
        let ids = dispatched.iter().map(|job| job.job.id).collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn scheduler_parallelizes_unpooled_jobs_up_to_capacity() {
        let (mut scheduler, mut rx) = test_scheduler(2);
        scheduler.enqueue(test_job(1, 1, "pyright-lsp"));
        scheduler.enqueue(test_job(2, 2, "ruby-lsp"));
        scheduler.enqueue(test_job(3, 3, "gopls"));
        scheduler.drain_runnable();

        let dispatched = drain_dispatched(&mut rx);
        assert_eq!(dispatched.len(), 2);
        assert_eq!(scheduler.active_workers, 2);
        assert!(scheduler.active_workers <= 2);
    }

    #[test]
    fn scheduler_dispatches_waiting_group_after_completion() {
        let (mut scheduler, mut rx) = test_scheduler(1);
        scheduler.enqueue(test_job(1, 1, "clangd-c-lsp"));
        scheduler.enqueue(test_job(2, 2, "clangd-cpp-lsp"));
        scheduler.drain_runnable();
        let first = rx.try_recv().expect("first job dispatched");

        scheduler.worker_finished(first.job.id, first.pool_group, first.key);
        scheduler.drain_runnable();

        let second = rx
            .try_recv()
            .expect("same group job dispatched after release");
        assert_eq!(second.job.id, 2);
    }

    #[test]
    fn scheduler_cancel_drops_pending_job_before_dispatch() {
        let (mut scheduler, mut rx) = test_scheduler(1);
        scheduler.enqueue(test_job(1, 1, "pyright-lsp"));
        scheduler.cancel(1);
        scheduler.drain_runnable();

        assert!(rx.try_recv().is_err());
    }
}
