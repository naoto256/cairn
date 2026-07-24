//! In-process analyzer job scheduler (single-task actor).
//!
//! `scheduler_loop` owns all scheduling state and consumes
//! [`SchedulerMsg`]s from one unbounded channel, so no locking is
//! needed: every state transition is serialized through the loop.
//! Dispatch hands jobs to the worker pool over a second unbounded
//! channel; `worker_capacity` equals the number of worker tasks
//! spawned by `start_workers`.
//!
//! Scheduling model — all guarantees are in-process only; a
//! restart rebuilds this state via `restore_from_db`:
//!
//! - Jobs are partitioned into lanes by analyzer pool group;
//!   within a lane, order is FIFO.
//! - Lanes take turns: `ready_order` rotates one dispatch per lane
//!   per pass, so a deep lane cannot starve other lanes while
//!   capacity is contended. There are no priorities and no aging
//!   beyond this rotation.
//! - At most one job of a pooled group is dispatched-and-
//!   unfinished at a time (`active_groups`); unpooled jobs compete
//!   only for global capacity.
//! - At most `worker_capacity` jobs are dispatched-and-unfinished
//!   at any moment (`active_workers`).
//!
//! "Dispatched" means handed to the worker channel — a job may sit
//! briefly in that channel before a worker task picks it up, but
//! it already counts against capacity and occupies its pool group.
use super::*;

/// Messages consumed by `scheduler_loop`. All scheduler-state
/// mutation happens by sending one of these; the producers are
/// `JobManager::enqueue_memory` (`Enqueue`), the worker loop
/// (`WorkerFinished`), `notify_cancelled_job` (`Cancel`), and
/// `JobManager::shutdown` (`Shutdown`).
#[derive(Debug)]
pub(super) enum SchedulerMsg {
    /// A new job to place at the tail of its lane.
    Enqueue(Job),
    /// A worker finished (successfully or not) a dispatched job:
    /// frees one capacity slot, the job's pool group, and its
    /// tracked key.
    WorkerFinished {
        job_id: JobId,
        pool_group: Option<&'static str>,
        key: JobKey,
    },
    /// Drop a not-yet-dispatched job. Already-dispatched jobs are
    /// unaffected here; the durable `cancel_requested` flag is what
    /// the running side observes.
    Cancel(JobId),
    /// Exit the scheduler loop. Pending in-memory jobs are dropped
    /// with the loop's state; their durable `queued` rows survive
    /// for the next restart's `restore_from_db`.
    Shutdown,
}

/// Lane key: jobs sharing a pooled analyzer group are serialized
/// through one `Pooled` lane; every group-less job rides the
/// single shared `Unpooled` lane.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum GroupLane {
    Pooled(&'static str),
    Unpooled,
}

/// Single-owner scheduler actor: applies each message to the
/// [`JobScheduler`] state and then re-drains, so every dispatch
/// decision sees the latest capacity / group occupancy. Exits when
/// the channel closes or on [`SchedulerMsg::Shutdown`].
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
    /// Global dispatch bound; equals the number of worker tasks.
    worker_capacity: usize,
    runtime_metrics: JobRuntimeMetricsStore,
    /// Pending jobs, FIFO per lane.
    lanes: HashMap<GroupLane, VecDeque<Job>>,
    /// Rotation of lanes that may have dispatchable work; a lane
    /// appears at most once (invariant kept by the `contains`
    /// checks before every push).
    ready_order: VecDeque<GroupLane>,
    /// Pooled groups with a dispatched-and-unfinished job — the
    /// per-group serialization guard.
    pub(super) active_groups: HashSet<&'static str>,
    /// Count of dispatched-and-unfinished jobs, bounded by
    /// `worker_capacity`.
    pub(super) active_workers: usize,
    tracked_keys: TrackedJobKeys,
    /// Cancelled ids that may still surface later. Covers the race
    /// where `Cancel` is processed before its `Enqueue`:
    /// `pop_next_job` drops such a job at dispatch time. Entries
    /// are cleared by `pop_next_job` or `worker_finished`; an id
    /// whose pending job was removed directly by `cancel` stays in
    /// the set (harmless for correctness — ids are never reused).
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

    /// Append the job to its lane and make the lane eligible for
    /// dispatch. If the pooled group is already occupied at this
    /// instant, the job is marked `waiting_pool_group` so its pool
    /// wait is measured from enqueue; a job whose group only
    /// becomes busy later (taken by a lane sibling) stays marked
    /// `queued` and accrues no pool-wait time.
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

    /// Pre-dispatch cancellation: drop the pending job if a lane
    /// still holds it, and tombstone the id in `cancelled_jobs` so
    /// an `Enqueue` processed after this message is still dropped
    /// at pop time. Jobs already handed to a worker are not
    /// touched here — their durable `cancel_requested` flag is the
    /// signal the running side observes.
    pub(super) fn cancel(&mut self, job_id: JobId) {
        self.cancelled_jobs.insert(job_id);
        self.remove_pending_job_id(job_id);
    }

    /// Return one capacity slot, free the job's pooled group
    /// (re-queueing its lane if work is waiting), clear any
    /// leftover cancel tombstone, and release the tracked key so
    /// the same `(repo_hash, manifest, analyzer)` can be enqueued
    /// again.
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

    /// Dispatch as many jobs as global capacity and pool-group
    /// serialization allow, taking one job per ready lane per
    /// rotation.
    pub(super) fn drain_runnable(&mut self) {
        while self.active_workers < self.worker_capacity {
            let Some(lane) = self.ready_order.pop_front() else {
                break;
            };
            // Lane blocked by its own active pool group: rotate it
            // to the back. If every remaining ready lane is blocked
            // the same way, stop instead of spinning —
            // `worker_finished` re-queues the lane once the group
            // frees up.
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
            // A send failure means the worker channel is closed
            // (shutdown in progress). Roll back the group
            // reservation and the tracked key and stop draining;
            // the job value is dropped, but its durable `queued`
            // row survives for the next restart's restore.
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
            // One dispatch per lane per turn: if the lane still has
            // work, rotate it to the back for round-robin fairness
            // across lanes.
            if self.lanes.get(&lane).is_some_and(|jobs| !jobs.is_empty())
                && !self.ready_order.contains(&lane)
            {
                self.ready_order.push_back(lane);
            }
        }
    }

    /// Pop the lane's next dispatchable job, dropping (and
    /// releasing the tracked key of) any job whose id sits in the
    /// `cancelled_jobs` tombstone set.
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

    /// Remove a not-yet-dispatched job from whichever lane holds
    /// it and release its tracked key. No-op when the id is not
    /// pending (already dispatched, finished, or never enqueued).
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

    /// Map a job to its lane via the linked-in analyzer registry.
    /// An analyzer id unknown to this build (e.g. a row persisted
    /// by an older binary) degrades to the unpooled lane with a
    /// warning rather than failing the job here.
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

/// Test-only lane table so scheduler unit tests do not depend on
/// which analyzers the production registry links in. Outer `None`
/// means "not a test id — fall through to the real registry";
/// inner `None` means "known but unpooled".
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
