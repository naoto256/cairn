//! Bounded ignore-matcher recovery scheduling.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::Sender;
use tracing::warn;

#[cfg(test)]
use crate::EventClassifier;
use crate::matcher::{GitMetadataPaths, RepoIgnoreMatcher};
use crate::{RescanReason, WatchEvent};

pub(super) const MATCHER_RETRY_IDLE: u8 = 0;
const MATCHER_RETRY_QUEUED: u8 = 1;
pub(super) const MATCHER_RETRY_RUNNING: u8 = 2;
pub(super) const MATCHER_RETRY_RUNNING_REQUESTED: u8 = 3;
pub(super) const MATCHER_BUILD_CONCURRENCY: usize = 2;
const MATCHER_RETRY_INITIAL: Duration = Duration::from_millis(100);
const MATCHER_RETRY_MAX: Duration = Duration::from_secs(2);
const MATCHER_RETRY_WARNING_WINDOW: Duration = Duration::from_secs(60);
const MATCHER_BUILD_PANIC_ERROR: &str = "ignore matcher build panicked; watcher remains fail-open";
// A nonempty queue rechecks closed receivers at this cadence. An empty queue
// waits indefinitely and relies on admission or shutdown to provide a wake.
const MATCHER_QUEUE_CLOSE_POLL: Duration = Duration::from_millis(50);
pub(super) static NEXT_MATCHER_TARGET_TOKEN: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
#[derive(Clone)]
pub(super) struct MatcherRetryHook {
    pub(super) reached: std::sync::mpsc::SyncSender<()>,
    pub(super) proceed: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

#[cfg(test)]
struct MatcherRollbackHook {
    expected_admission_id: AdmissionId,
    reached: std::sync::mpsc::SyncSender<()>,
    proceed: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

pub(super) struct MatcherBuildTarget {
    pub(super) token: u64,
    pub(super) repo_root: Arc<PathBuf>,
    pub(super) git_metadata: Arc<GitMetadataPaths>,
    pub(super) ignore: Arc<RwLock<Arc<RepoIgnoreMatcher>>>,
    pub(super) state: Arc<AtomicU8>,
    pub(super) desired_generation: AtomicU64,
    pub(super) commit: Mutex<MatcherCommitState>,
    pub(super) failure_log: Mutex<Option<MatcherRetryFailureWindow>>,
    pub(super) tx: Sender<WatchEvent>,
    pub(super) scheduler: Arc<MatcherBuildScheduler>,
    #[cfg(test)]
    pub(super) retry_attempt_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    pub(super) matcher_build_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    pub(super) retry_exit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    pub(super) retry_commit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    pub(super) retry_finish_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    pub(super) injected_attempt_panics: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    pub(super) generation_overflow_warning_count: std::sync::atomic::AtomicUsize,
}

pub(super) struct MatcherRetryFailureWindow {
    kind: std::io::ErrorKind,
    message: String,
    started_at: Instant,
    suppressed: u64,
}

pub(super) struct MatcherCommitState {
    pub(super) desired_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatcherCommitOutcome {
    Committed,
    Stale,
    Closed,
}

impl MatcherBuildTarget {
    fn log_matcher_retry_failure(&self, err: &std::io::Error) {
        let next = MatcherRetryFailureWindow {
            kind: err.kind(),
            message: err.to_string(),
            started_at: Instant::now(),
            suppressed: 0,
        };
        let previous = {
            let mut active = self
                .failure_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(current) = active.as_mut()
                && current.kind == next.kind
                && current.message == next.message
                && next
                    .started_at
                    .saturating_duration_since(current.started_at)
                    < MATCHER_RETRY_WARNING_WINDOW
            {
                current.suppressed = current.suppressed.saturating_add(1);
                return;
            }
            active.replace(next)
        };
        if let Some(previous) = previous {
            if previous.kind == err.kind() && previous.message == err.to_string() {
                warn!(
                    error = %err,
                    suppressed = previous.suppressed,
                    "ignore matcher retry failed"
                );
                return;
            }
            if previous.suppressed > 0 {
                warn!(
                    error_kind = ?previous.kind,
                    error = %previous.message,
                    suppressed = previous.suppressed,
                    "suppressed repeated ignore matcher retry failures"
                );
            }
        }
        warn!(error = %err, "ignore matcher retry failed");
    }

    fn log_matcher_retry_recovery(&self) {
        let previous = self
            .failure_log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(previous) = previous.filter(|previous| previous.suppressed > 0) {
            warn!(
                error_kind = ?previous.kind,
                error = %previous.message,
                suppressed = previous.suppressed,
                "ignore matcher retry recovered after suppressed failures"
            );
        }
    }

    fn commit_matcher(
        &self,
        matcher: RepoIgnoreMatcher,
        attempt_generation: u64,
    ) -> MatcherCommitOutcome {
        let commit = self
            .commit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.tx.is_closed() {
            return MatcherCommitOutcome::Closed;
        }
        if commit.desired_generation != attempt_generation {
            return MatcherCommitOutcome::Stale;
        }
        let mut current = self
            .ignore
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *current = Arc::new(matcher);
        drop(current);
        match self.tx.try_send(WatchEvent::Rescan {
            reason: RescanReason::MatcherRecovered,
        }) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                MatcherCommitOutcome::Committed
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => MatcherCommitOutcome::Closed,
        }
    }
}

struct ScheduledMatcherBuild {
    target: Weak<MatcherBuildTarget>,
    due: Instant,
    retry_delay: Duration,
    attempt_generation: u64,
    admission_id: AdmissionId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdmissionId(u64);

#[derive(Debug)]
struct CancelledMatcherBuild {
    token: u64,
    admission_id: AdmissionId,
}

#[derive(Debug)]
enum MatcherSchedulerWarning {
    AdmissionIdExhausted {
        token: u64,
    },
    WorkerSpawnFailed {
        error: std::io::Error,
    },
    NoLiveWorker {
        cancelled: Vec<CancelledMatcherBuild>,
    },
    QueueInvariant {
        mismatched_tokens: Vec<u64>,
    },
    LastWorkerExited {
        cancelled: Vec<CancelledMatcherBuild>,
    },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatcherSchedulerWarningKind {
    AdmissionIdExhausted,
    WorkerSpawnFailed,
    NoLiveWorker,
    QueueInvariant,
    LastWorkerExited,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SuccessorExhaustionPhase {
    BeforeRollback {
        admission_id: AdmissionId,
    },
    Popped {
        admission_id: AdmissionId,
        generation: u64,
    },
    CommitStale {
        attempt_generation: u64,
        latest_generation: u64,
    },
}

#[cfg(test)]
struct TestSchedulerObserver {
    tx: std::sync::mpsc::SyncSender<SuccessorExhaustionPhase>,
    full: std::sync::atomic::AtomicUsize,
    disconnected: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
type OptionalTestSchedulerObserver = Option<Arc<TestSchedulerObserver>>;
#[cfg(not(test))]
type OptionalTestSchedulerObserver = ();

#[cfg(test)]
type TestSchedulerWarningObserver = Arc<dyn Fn(MatcherSchedulerWarningKind) + Send + Sync>;

#[derive(Default)]
struct MatcherSchedulerState {
    jobs: HashMap<u64, ScheduledMatcherBuild>,
    next_admission_id: u64,
    live_workers: usize,
    starting_workers: usize,
    shutdown: bool,
    #[cfg(test)]
    queue_hwm: usize,
    #[cfg(test)]
    active_builds: usize,
    #[cfg(test)]
    warning_count: usize,
    #[cfg(test)]
    injected_spawn_failures: usize,
    #[cfg(test)]
    injected_worker_exits: usize,
}

impl MatcherSchedulerState {
    fn warning_count_for_test(&mut self) {
        #[cfg(test)]
        {
            self.warning_count += 1;
        }
    }
}

#[cfg(test)]
impl MatcherSchedulerWarning {
    fn kind(&self) -> MatcherSchedulerWarningKind {
        match self {
            Self::AdmissionIdExhausted { .. } => MatcherSchedulerWarningKind::AdmissionIdExhausted,
            Self::WorkerSpawnFailed { .. } => MatcherSchedulerWarningKind::WorkerSpawnFailed,
            Self::NoLiveWorker { .. } => MatcherSchedulerWarningKind::NoLiveWorker,
            Self::QueueInvariant { .. } => MatcherSchedulerWarningKind::QueueInvariant,
            Self::LastWorkerExited { .. } => MatcherSchedulerWarningKind::LastWorkerExited,
        }
    }
}

pub(super) struct MatcherBuildScheduler {
    state: Mutex<MatcherSchedulerState>,
    wake: Condvar,
    #[cfg(test)]
    rollback_hook: std::sync::Mutex<Option<MatcherRollbackHook>>,
    #[cfg(test)]
    before_next_job_hook: std::sync::Mutex<Option<MatcherRetryHook>>,
    #[cfg(test)]
    warning_observer: std::sync::Mutex<Option<TestSchedulerWarningObserver>>,
    #[cfg(test)]
    test_observer: Option<Arc<TestSchedulerObserver>>,
    #[cfg(test)]
    cancelled_for_test: std::sync::atomic::AtomicBool,
}

impl MatcherBuildScheduler {
    fn new() -> Arc<Self> {
        #[cfg(test)]
        {
            Self::new_with_optional_test_observer(None)
        }
        #[cfg(not(test))]
        {
            Self::new_with_optional_test_observer(())
        }
    }

    #[cfg(test)]
    fn new_with_test_observer(observer: Arc<TestSchedulerObserver>) -> Arc<Self> {
        Self::new_with_optional_test_observer(Some(observer))
    }

    fn new_with_optional_test_observer(_observer: OptionalTestSchedulerObserver) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(MatcherSchedulerState::default()),
            wake: Condvar::new(),
            #[cfg(test)]
            rollback_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_next_job_hook: std::sync::Mutex::new(None),
            #[cfg(test)]
            warning_observer: std::sync::Mutex::new(None),
            #[cfg(test)]
            test_observer: _observer,
            #[cfg(test)]
            cancelled_for_test: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(super) fn global() -> &'static Arc<Self> {
        static SCHEDULER: OnceLock<Arc<MatcherBuildScheduler>> = OnceLock::new();
        SCHEDULER.get_or_init(Self::new)
    }

    pub(super) fn request(self: &Arc<Self>, target: &Arc<MatcherBuildTarget>) {
        let mut local_admission = None;
        let allocation_exhausted = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if target.tx.is_closed() {
                // Prompt queued-close cleanup; correctness still comes from
                // the bounded close poll in `next_job` if this wake is missed.
                self.wake.notify_all();
                return;
            }
            if state.shutdown {
                target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                return;
            }
            match target.state.load(Ordering::SeqCst) {
                MATCHER_RETRY_IDLE => {
                    if let Some(admission_id) = Self::allocate_admission_id(&mut state) {
                        target.state.store(MATCHER_RETRY_QUEUED, Ordering::SeqCst);
                        local_admission = Some(admission_id);
                        let previous = state.jobs.insert(
                            target.token,
                            ScheduledMatcherBuild {
                                target: Arc::downgrade(target),
                                due: Instant::now() + MATCHER_RETRY_INITIAL,
                                retry_delay: MATCHER_RETRY_INITIAL,
                                attempt_generation: target
                                    .desired_generation
                                    .load(Ordering::SeqCst),
                                admission_id,
                            },
                        );
                        debug_assert!(
                            previous.is_none(),
                            "idle target retained a physical queue record"
                        );
                        #[cfg(test)]
                        {
                            state.queue_hwm = state.queue_hwm.max(state.jobs.len());
                        }
                        false
                    } else {
                        state.warning_count_for_test();
                        true
                    }
                }
                MATCHER_RETRY_QUEUED => {
                    debug_assert!(state.jobs.contains_key(&target.token));
                    false
                }
                MATCHER_RETRY_RUNNING => {
                    target
                        .state
                        .store(MATCHER_RETRY_RUNNING_REQUESTED, Ordering::SeqCst);
                    false
                }
                MATCHER_RETRY_RUNNING_REQUESTED => false,
                value => unreachable!("invalid matcher retry state: {value}"),
            }
        };
        if allocation_exhausted {
            self.emit_warning(MatcherSchedulerWarning::AdmissionIdExhausted {
                token: target.token,
            });
            return;
        }
        self.wake.notify_one();

        self.ensure_workers();

        #[cfg(test)]
        if let Some(admission_id) = local_admission {
            self.observe_successor_exhaustion(SuccessorExhaustionPhase::BeforeRollback {
                admission_id,
            });
            wait_on_matcher_rollback_hook(&self.rollback_hook, admission_id);
        }

        let warning = local_admission.and_then(|admission_id| {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let owns_queued_record = state.jobs.get(&target.token).is_some_and(|job| {
                job.admission_id == admission_id
                    && target.state.load(Ordering::SeqCst) == MATCHER_RETRY_QUEUED
            });
            if owns_queued_record && state.live_workers == 0 && state.starting_workers == 0 {
                state.jobs.remove(&target.token);
                target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                if !state.shutdown {
                    state.warning_count_for_test();
                    return Some(MatcherSchedulerWarning::NoLiveWorker {
                        cancelled: vec![CancelledMatcherBuild {
                            token: target.token,
                            admission_id,
                        }],
                    });
                }
            }
            None
        });
        if let Some(warning) = warning {
            self.emit_warning(warning);
        }
    }

    fn allocate_admission_id(state: &mut MatcherSchedulerState) -> Option<AdmissionId> {
        let admission_id = AdmissionId(state.next_admission_id);
        state.next_admission_id = state.next_admission_id.checked_add(1)?;
        Some(admission_id)
    }

    fn ensure_workers(self: &Arc<Self>) {
        let reservations = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.shutdown {
                return;
            }
            let reservations = MATCHER_BUILD_CONCURRENCY
                .saturating_sub(state.live_workers + state.starting_workers);
            state.starting_workers += reservations;
            reservations
        };
        for _ in 0..reservations {
            let _ = self.spawn_reserved_worker();
        }
    }

    fn spawn_reserved_worker(self: &Arc<Self>) -> bool {
        #[cfg(test)]
        let injected_failure = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.injected_spawn_failures > 0 {
                state.injected_spawn_failures -= 1;
                true
            } else {
                false
            }
        };
        #[cfg(not(test))]
        let injected_failure = false;
        if injected_failure {
            let warnings = self.resolve_failed_spawn(None);
            for warning in warnings {
                self.emit_warning(warning);
            }
            return false;
        }
        let scheduler = Arc::downgrade(self);
        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(0);
        let spawned = std::thread::Builder::new()
            .name("cairn-matcher-build".into())
            .spawn(move || {
                if start_rx.recv().is_ok() {
                    Self::worker_loop(scheduler);
                }
            });
        match spawned {
            Ok(_) => {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.starting_workers -= 1;
                state.live_workers += 1;
                drop(state);
                let _ = start_tx.send(());
                true
            }
            Err(err) => {
                let warnings = self.resolve_failed_spawn(Some(err));
                for warning in warnings {
                    self.emit_warning(warning);
                }
                false
            }
        }
    }

    fn resolve_failed_spawn(
        &self,
        spawn_error: Option<std::io::Error>,
    ) -> Vec<MatcherSchedulerWarning> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.starting_workers = state.starting_workers.saturating_sub(1);
        let mut warnings = Vec::new();
        if let Some(error) = spawn_error {
            state.warning_count_for_test();
            warnings.push(MatcherSchedulerWarning::WorkerSpawnFailed { error });
        }
        if state.live_workers == 0 && state.starting_workers == 0 && !state.jobs.is_empty() {
            let (cancelled, mismatched_tokens) = Self::cancel_all_queued(&mut state);
            state.warning_count_for_test();
            warnings.push(MatcherSchedulerWarning::NoLiveWorker { cancelled });
            if !mismatched_tokens.is_empty() {
                state.warning_count_for_test();
                warnings.push(MatcherSchedulerWarning::QueueInvariant { mismatched_tokens });
            }
        }
        warnings
    }

    fn cancel_all_queued(
        state: &mut MatcherSchedulerState,
    ) -> (Vec<CancelledMatcherBuild>, Vec<u64>) {
        let mut cancelled = Vec::with_capacity(state.jobs.len());
        let mut mismatched_tokens = Vec::new();
        for (token, job) in &state.jobs {
            let target = job.target.upgrade();
            if target.as_ref().is_some_and(|target| {
                target.token != *token
                    || target.state.load(Ordering::SeqCst) != MATCHER_RETRY_QUEUED
            }) {
                mismatched_tokens.push(*token);
            }
            if let Some(target) = target {
                // With no live or starting worker, the physical record is the
                // sole possible owner. Even a corrupted state must converge
                // fail-open instead of leaving a stranded queued target.
                target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            }
            cancelled.push(CancelledMatcherBuild {
                token: *token,
                admission_id: job.admission_id,
            });
        }
        state.jobs.clear();
        (cancelled, mismatched_tokens)
    }

    fn emit_warning(&self, warning: MatcherSchedulerWarning) {
        #[cfg(test)]
        let kind = warning.kind();
        match warning {
            MatcherSchedulerWarning::AdmissionIdExhausted { token } => {
                warn!(
                    token,
                    "matcher scheduler admission id exhausted; watcher remains fail-open"
                );
            }
            MatcherSchedulerWarning::WorkerSpawnFailed { error } => {
                warn!(error = %error, "matcher scheduler worker spawn failed");
            }
            MatcherSchedulerWarning::NoLiveWorker { cancelled } => {
                let cancelled = cancelled
                    .iter()
                    .map(|entry| (entry.token, entry.admission_id.0))
                    .collect::<Vec<_>>();
                warn!(
                    ?cancelled,
                    "matcher scheduler has no live worker; queued recovery was cancelled"
                );
            }
            MatcherSchedulerWarning::QueueInvariant { mismatched_tokens } => {
                warn!(
                    ?mismatched_tokens,
                    "matcher scheduler queue ownership invariant was repaired"
                );
            }
            MatcherSchedulerWarning::LastWorkerExited { cancelled } => {
                let cancelled = cancelled
                    .iter()
                    .map(|entry| (entry.token, entry.admission_id.0))
                    .collect::<Vec<_>>();
                warn!(
                    ?cancelled,
                    "last matcher scheduler worker exited; queued recovery was cancelled"
                );
            }
        }
        #[cfg(test)]
        let observer = self
            .warning_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned();
        #[cfg(test)]
        if let Some(observer) = observer {
            observer(kind);
        }
    }

    #[cfg(test)]
    fn observe_successor_exhaustion(&self, phase: SuccessorExhaustionPhase) {
        let Some(observer) = self.test_observer.clone() else {
            return;
        };
        match observer.tx.try_send(phase) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                observer.full.fetch_add(1, Ordering::SeqCst);
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                observer.disconnected.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn worker_loop(scheduler: Weak<Self>) {
        let mut guard = MatcherWorkerExitGuard::new(scheduler.clone());
        loop {
            let Some(owner) = scheduler.upgrade() else {
                guard.intentional = true;
                return;
            };
            #[cfg(test)]
            wait_on_matcher_retry_hook(&owner.before_next_job_hook);
            let Some((token, job, target)) = owner.next_job() else {
                guard.intentional = true;
                return;
            };
            guard.current = Some((Arc::downgrade(&target), job.admission_id));
            #[cfg(test)]
            owner.observe_successor_exhaustion(SuccessorExhaustionPhase::Popped {
                admission_id: job.admission_id,
                generation: job.attempt_generation,
            });
            #[cfg(test)]
            {
                let injected_exit = {
                    let mut state = owner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if state.injected_worker_exits > 0 {
                        state.injected_worker_exits -= 1;
                        true
                    } else {
                        false
                    }
                };
                if injected_exit {
                    return;
                }
            }
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(test)]
                wait_on_matcher_retry_hook(&target.retry_attempt_hook);
                #[cfg(test)]
                if target
                    .injected_attempt_panics
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    panic!("injected matcher build panic");
                }
                #[cfg(test)]
                wait_on_matcher_retry_hook(&target.matcher_build_hook);
                RepoIgnoreMatcher::build(&target.repo_root, &target.git_metadata.info_exclude)
            }));
            #[cfg(test)]
            wait_on_matcher_retry_hook(&target.retry_commit_hook);
            owner.finish_job(token, job, &target, outcome);
            guard.current = None;
            owner.ensure_workers();
        }
    }

    fn replace_one_worker(self: &Arc<Self>) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.shutdown
                || state.live_workers + state.starting_workers >= MATCHER_BUILD_CONCURRENCY
            {
                return;
            }
            state.starting_workers += 1;
        }
        let _ = self.spawn_reserved_worker();
    }

    fn next_job(&self) -> Option<(u64, ScheduledMatcherBuild, Arc<MatcherBuildTarget>)> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            #[cfg(test)]
            if self.cancelled_for_test.load(Ordering::SeqCst) {
                for (_, job) in state.jobs.drain() {
                    if let Some(target) = job.target.upgrade() {
                        target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                    }
                }
                return None;
            }
            if state.shutdown {
                return None;
            }
            let closed: Vec<u64> = state
                .jobs
                .iter()
                .filter_map(|(token, job)| {
                    job.target
                        .upgrade()
                        .is_none_or(|target| target.tx.is_closed())
                        .then_some(*token)
                })
                .collect();
            for token in closed {
                if let Some(job) = state.jobs.remove(&token)
                    && let Some(target) = job.target.upgrade()
                {
                    target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                }
            }
            let Some((&token, earliest)) = state.jobs.iter().min_by_key(|(_, job)| job.due) else {
                state = self
                    .wake
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            };
            let now = Instant::now();
            if earliest.due > now {
                let wait = (earliest.due - now).min(MATCHER_QUEUE_CLOSE_POLL);
                let (next, _) = self
                    .wake
                    .wait_timeout(state, wait)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state = next;
                continue;
            }
            let mut job = state
                .jobs
                .remove(&token)
                .expect("selected queue record exists");
            let Some(target) = job.target.upgrade() else {
                continue;
            };
            if target.tx.is_closed() {
                target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                continue;
            }
            if target
                .state
                .compare_exchange(
                    MATCHER_RETRY_QUEUED,
                    MATCHER_RETRY_RUNNING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                continue;
            }
            job.attempt_generation = target.desired_generation.load(Ordering::SeqCst);
            #[cfg(test)]
            {
                state.active_builds += 1;
            }
            return Some((token, job, target));
        }
    }

    fn finish_job(
        &self,
        token: u64,
        job: ScheduledMatcherBuild,
        target: &Arc<MatcherBuildTarget>,
        outcome: std::thread::Result<std::io::Result<RepoIgnoreMatcher>>,
    ) {
        if target.tx.is_closed() {
            #[cfg(test)]
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.active_builds = state.active_builds.saturating_sub(1);
            }
            target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            return;
        }
        let (commit_outcome, failed) = match outcome {
            Ok(Ok(matcher)) => {
                let commit_outcome = target.commit_matcher(matcher, job.attempt_generation);
                if commit_outcome == MatcherCommitOutcome::Committed {
                    target.log_matcher_retry_recovery();
                }
                (commit_outcome, false)
            }
            Ok(Err(err)) => {
                target.log_matcher_retry_failure(&err);
                (MatcherCommitOutcome::Stale, true)
            }
            Err(_) => {
                target.log_matcher_retry_failure(&std::io::Error::other(MATCHER_BUILD_PANIC_ERROR));
                (MatcherCommitOutcome::Stale, true)
            }
        };
        #[cfg(test)]
        if commit_outcome == MatcherCommitOutcome::Stale {
            self.observe_successor_exhaustion(SuccessorExhaustionPhase::CommitStale {
                attempt_generation: job.attempt_generation,
                latest_generation: target.desired_generation.load(Ordering::SeqCst),
            });
        }
        #[cfg(test)]
        wait_on_matcher_retry_hook(&target.retry_exit_hook);
        #[cfg(test)]
        wait_on_matcher_retry_hook(&target.retry_finish_hook);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        {
            state.active_builds = state.active_builds.saturating_sub(1);
        }
        if commit_outcome == MatcherCommitOutcome::Closed || target.tx.is_closed() {
            target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            return;
        }
        let requested = target.state.load(Ordering::SeqCst) == MATCHER_RETRY_RUNNING_REQUESTED
            || target.desired_generation.load(Ordering::SeqCst) != job.attempt_generation;
        if commit_outcome == MatcherCommitOutcome::Committed && !requested {
            target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            return;
        }
        let retry_delay = if failed {
            (job.retry_delay * 2).min(MATCHER_RETRY_MAX)
        } else {
            MATCHER_RETRY_INITIAL
        };
        let Some(admission_id) = Self::allocate_admission_id(&mut state) else {
            target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            state.warning_count_for_test();
            drop(state);
            self.emit_warning(MatcherSchedulerWarning::AdmissionIdExhausted { token });
            return;
        };
        target.state.store(MATCHER_RETRY_QUEUED, Ordering::SeqCst);
        let previous = state.jobs.insert(
            token,
            ScheduledMatcherBuild {
                target: Arc::downgrade(target),
                due: Instant::now() + retry_delay,
                retry_delay,
                attempt_generation: target.desired_generation.load(Ordering::SeqCst),
                admission_id,
            },
        );
        debug_assert!(previous.is_none());
        #[cfg(test)]
        {
            state.queue_hwm = state.queue_hwm.max(state.jobs.len());
        }
        self.wake.notify_one();
    }

    #[cfg(test)]
    fn shutdown_for_test(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.shutdown = true;
        for (_, job) in state.jobs.drain() {
            if let Some(target) = job.target.upgrade() {
                target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            }
        }
        self.wake.notify_all();
    }

    #[cfg(test)]
    fn wait_for_stopped(&self) {
        let started = Instant::now();
        loop {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.live_workers == 0 && state.starting_workers == 0 {
                assert!(state.jobs.is_empty());
                assert_eq!(state.active_builds, 0);
                return;
            }
            drop(state);
            assert!(started.elapsed() < Duration::from_secs(3));
            std::thread::yield_now();
        }
    }
}

struct MatcherWorkerExitGuard {
    scheduler: Weak<MatcherBuildScheduler>,
    current: Option<(Weak<MatcherBuildTarget>, AdmissionId)>,
    intentional: bool,
}

impl MatcherWorkerExitGuard {
    fn new(scheduler: Weak<MatcherBuildScheduler>) -> Self {
        Self {
            scheduler,
            current: None,
            intentional: false,
        }
    }
}

impl Drop for MatcherWorkerExitGuard {
    fn drop(&mut self) {
        let Some(scheduler) = self.scheduler.upgrade() else {
            return;
        };
        let (replace, warning) = {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.live_workers = state.live_workers.saturating_sub(1);
            if self.intentional || state.shutdown {
                return;
            }
            if let Some(target) = self
                .current
                .as_ref()
                .and_then(|(target, _)| target.upgrade())
            {
                #[cfg(test)]
                {
                    state.active_builds = state.active_builds.saturating_sub(1);
                }
                target.state.store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
            }
            let replace = state.live_workers > 0
                && state.live_workers + state.starting_workers < MATCHER_BUILD_CONCURRENCY;
            let warning = if state.live_workers == 0 && state.starting_workers == 0 {
                let mut cancelled = self
                    .current
                    .as_ref()
                    .map(|(target, admission_id)| CancelledMatcherBuild {
                        token: target.upgrade().map_or(0, |target| target.token),
                        admission_id: *admission_id,
                    })
                    .into_iter()
                    .collect::<Vec<_>>();
                let (queued, mismatched_tokens) =
                    MatcherBuildScheduler::cancel_all_queued(&mut state);
                cancelled.extend(queued);
                state.warning_count_for_test();
                if !mismatched_tokens.is_empty() {
                    state.warning_count_for_test();
                }
                Some((
                    MatcherSchedulerWarning::LastWorkerExited { cancelled },
                    (!mismatched_tokens.is_empty())
                        .then_some(MatcherSchedulerWarning::QueueInvariant { mismatched_tokens }),
                ))
            } else {
                None
            };
            scheduler.wake.notify_all();
            (replace, warning)
        };
        if let Some((warning, invariant_warning)) = warning {
            scheduler.emit_warning(warning);
            if let Some(invariant_warning) = invariant_warning {
                scheduler.emit_warning(invariant_warning);
            }
        }
        if replace {
            scheduler.replace_one_worker();
        }
    }
}

#[cfg(test)]
fn wait_on_matcher_retry_hook(hook_slot: &std::sync::Mutex<Option<MatcherRetryHook>>) -> bool {
    if let Some(hook) = hook_slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        && hook.reached.send(()).is_ok()
    {
        let _ = hook
            .proceed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        true
    } else {
        false
    }
}

#[cfg(test)]
fn wait_on_matcher_rollback_hook(
    hook_slot: &std::sync::Mutex<Option<MatcherRollbackHook>>,
    admission_id: AdmissionId,
) -> bool {
    let hook = {
        let mut slot = hook_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot
            .as_ref()
            .is_some_and(|hook| hook.expected_admission_id == admission_id)
        {
            slot.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook
        && hook.reached.send(()).is_ok()
    {
        let _ = hook
            .proceed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::backend::watch_repo_with_backend_mode_after_arm;
    use crate::matcher::resolve_git_metadata;
    use crate::{FileChange, WatchBackend};
    use notify::EventKind;
    use notify::event::ModifyKind;

    fn classifier_for(root: &Path) -> EventClassifier {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx)
    }

    fn install_matcher_retry_hook(
        hook_slot: &std::sync::Mutex<Option<MatcherRetryHook>>,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (proceed_tx, proceed_rx) = std::sync::mpsc::sync_channel(1);
        *hook_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(MatcherRetryHook {
            reached: reached_tx,
            proceed: Arc::new(std::sync::Mutex::new(proceed_rx)),
        });
        (reached_rx, proceed_tx)
    }

    fn install_matcher_rollback_hook(
        hook_slot: &std::sync::Mutex<Option<MatcherRollbackHook>>,
        expected_admission_id: AdmissionId,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (proceed_tx, proceed_rx) = std::sync::mpsc::sync_channel(1);
        *hook_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(MatcherRollbackHook {
            expected_admission_id,
            reached: reached_tx,
            proceed: Arc::new(std::sync::Mutex::new(proceed_rx)),
        });
        (reached_rx, proceed_tx)
    }

    struct SuccessorExhaustionCleanup {
        scheduler: Arc<MatcherBuildScheduler>,
        proceed: Option<std::sync::mpsc::SyncSender<()>>,
        completed: bool,
    }

    impl SuccessorExhaustionCleanup {
        fn release_proceed(&mut self) {
            if let Some(proceed) = self.proceed.take() {
                let _ = proceed.try_send(());
            }
        }

        fn complete(&mut self) {
            self.release_proceed();
            self.scheduler.shutdown_for_test();
            self.completed = true;
        }
    }

    impl Drop for SuccessorExhaustionCleanup {
        fn drop(&mut self) {
            self.release_proceed();
            if !self.completed {
                self.scheduler
                    .cancelled_for_test
                    .store(true, Ordering::SeqCst);
                self.scheduler.wake.notify_all();
            }
        }
    }

    struct MatcherRaceCleanup {
        scheduler: Arc<MatcherBuildScheduler>,
        next_job_proceed: Option<std::sync::mpsc::SyncSender<()>>,
        rollback_proceed: Option<std::sync::mpsc::SyncSender<()>>,
        build_proceed: Option<std::sync::mpsc::SyncSender<()>>,
        completed: bool,
    }

    impl MatcherRaceCleanup {
        fn release_next_job(&mut self) {
            if let Some(proceed) = self.next_job_proceed.take() {
                let _ = proceed.try_send(());
            }
        }

        fn release_rollback(&mut self) {
            if let Some(proceed) = self.rollback_proceed.take() {
                let _ = proceed.try_send(());
            }
        }

        fn release_build(&mut self) {
            if let Some(proceed) = self.build_proceed.take() {
                let _ = proceed.try_send(());
            }
        }

        fn complete(&mut self) {
            self.release_next_job();
            self.release_rollback();
            self.release_build();
            self.scheduler.shutdown_for_test();
            self.completed = true;
        }
    }

    impl Drop for MatcherRaceCleanup {
        fn drop(&mut self) {
            self.release_next_job();
            self.release_rollback();
            self.release_build();
            if !self.completed {
                self.scheduler
                    .cancelled_for_test
                    .store(true, Ordering::SeqCst);
                self.scheduler.wake.notify_all();
            }
        }
    }

    fn recv_successor_exhaustion_phase(
        rx: &std::sync::mpsc::Receiver<SuccessorExhaustionPhase>,
        scheduler: &MatcherBuildScheduler,
        expected: SuccessorExhaustionPhase,
    ) {
        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(actual) => {
                eprintln!("successor-exhaustion phase={actual:?}");
                assert_eq!(actual, expected);
            }
            Err(error) => {
                let observer = scheduler.test_observer.as_ref();
                let delivery = observer.map_or_else(
                    || "observer=None".to_string(),
                    |observer| {
                        format!(
                            "observer=Some(full={},disconnected={})",
                            observer.full.load(Ordering::SeqCst),
                            observer.disconnected.load(Ordering::SeqCst),
                        )
                    },
                );
                let snapshot = match scheduler.state.try_lock() {
                    Ok(state) => format!(
                        "Acquired(state_jobs={},active={},live={},starting={},warnings={})",
                        state.jobs.len(),
                        state.active_builds,
                        state.live_workers,
                        state.starting_workers,
                        state.warning_count,
                    ),
                    Err(std::sync::TryLockError::WouldBlock) => "Contended".to_string(),
                    Err(std::sync::TryLockError::Poisoned(state)) => format!(
                        "AcquiredPoisoned(state_jobs={},active={},live={},starting={},warnings={})",
                        state.get_ref().jobs.len(),
                        state.get_ref().active_builds,
                        state.get_ref().live_workers,
                        state.get_ref().starting_workers,
                        state.get_ref().warning_count,
                    ),
                };
                panic!(
                    "missing successor-exhaustion phase {expected:?}: {error}; {delivery}; snapshot={snapshot}"
                );
            }
        }
    }

    async fn wait_for_matcher_retry_idle(classifier: &EventClassifier) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if classifier.matcher_retry_state.load(Ordering::SeqCst) == MATCHER_RETRY_IDLE {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("matcher retry owner did not return to idle");
    }

    fn wait_for_matcher_retry_idle_sync(classifier: &EventClassifier) {
        let started = Instant::now();
        while classifier.matcher_retry_state.load(Ordering::SeqCst) != MATCHER_RETRY_IDLE {
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "matcher retry owner did not return to idle: state={}",
                classifier.matcher_retry_state.load(Ordering::SeqCst),
            );
            std::thread::yield_now();
        }
    }

    fn wait_for_test_scheduler_quiescent(
        scheduler: &MatcherBuildScheduler,
        target: &Arc<MatcherBuildTarget>,
    ) {
        let started = Instant::now();
        loop {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let target_state = target.state.load(Ordering::SeqCst);
            let target_queued = state.jobs.contains_key(&target.token);
            let quiescent = target_state == MATCHER_RETRY_IDLE
                && !target_queued
                && state.jobs.is_empty()
                && state.active_builds == 0
                && state.starting_workers == 0
                && state.live_workers == MATCHER_BUILD_CONCURRENCY;
            if quiescent {
                return;
            }
            if started.elapsed() >= Duration::from_secs(3) {
                panic!(
                    "test scheduler did not quiesce: target_state={target_state}, target_queued={target_queued}, jobs={}, active={}, starting={}, live={}, shutdown={}, warnings={}",
                    state.jobs.len(),
                    state.active_builds,
                    state.starting_workers,
                    state.live_workers,
                    state.shutdown,
                    state.warning_count,
                );
            }
            drop(state);
            std::thread::yield_now();
        }
    }

    fn isolated_deferred_classifier(
        scheduler: Arc<MatcherBuildScheduler>,
    ) -> (
        tempfile::TempDir,
        EventClassifier,
        tokio::sync::mpsc::Receiver<WatchEvent>,
    ) {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".gitignore"), "ignored.rs\n").unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new_deferred_with_scheduler(
            root.path(),
            resolve_git_metadata(root.path()).unwrap(),
            tx,
            scheduler,
        );
        (root, classifier, rx)
    }

    #[test]
    fn matcher_scheduler_coalesces_physical_records_without_advancing_due() {
        const REPOS: usize = 36;
        const REQUESTS: usize = 10_000;
        let scheduler = MatcherBuildScheduler::new();
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(2);
        let (proceed_tx, proceed_rx) = std::sync::mpsc::sync_channel(2);
        let proceed_rx = Arc::new(std::sync::Mutex::new(proceed_rx));
        let mut owned = Vec::with_capacity(REPOS);

        for ordinal in 0..REPOS {
            let (root, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
            if ordinal < MATCHER_BUILD_CONCURRENCY {
                *classifier
                    .matcher_build_hook
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(MatcherRetryHook {
                    reached: reached_tx.clone(),
                    proceed: proceed_rx.clone(),
                });
            }
            classifier.request_matcher_retry();
            owned.push((root, classifier, rx));
        }
        for _ in 0..MATCHER_BUILD_CONCURRENCY {
            reached_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        }
        let queued_token = owned[2].1.matcher_target.token;
        let original_due = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .jobs[&queued_token]
            .due;

        for ordinal in 0..REQUESTS {
            owned[ordinal % REPOS].1.request_matcher_retry();
        }

        for (ordinal, (_, classifier, _)) in owned.iter().enumerate() {
            let expected_generation =
                1 + (REQUESTS / REPOS) as u64 + u64::from(ordinal < REQUESTS % REPOS);
            assert_eq!(
                classifier
                    .matcher_target
                    .desired_generation
                    .load(Ordering::SeqCst),
                expected_generation
            );
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                if ordinal < MATCHER_BUILD_CONCURRENCY {
                    MATCHER_RETRY_RUNNING_REQUESTED
                } else {
                    MATCHER_RETRY_QUEUED
                }
            );
        }

        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.jobs.len() <= REPOS);
            assert_eq!(state.jobs.len(), REPOS - MATCHER_BUILD_CONCURRENCY);
            assert!(state.queue_hwm <= REPOS);
            assert_eq!(state.jobs[&queued_token].due, original_due);
        }
        drop(owned.drain(..).map(|(_, _, rx)| rx).collect::<Vec<_>>());
        for _ in 0..MATCHER_BUILD_CONCURRENCY {
            proceed_tx.send(()).unwrap();
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_scheduler_spawn_one_of_two_and_last_exit_are_honest() {
        let scheduler = MatcherBuildScheduler::new();
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.injected_spawn_failures = 1;
            state.injected_worker_exits = 1;
        }
        let (_root, classifier, _rx) = isolated_deferred_classifier(scheduler.clone());
        classifier.request_matcher_retry();

        let started = Instant::now();
        loop {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.live_workers == 0
                && state.starting_workers == 0
                && state.jobs.is_empty()
                && classifier.matcher_retry_state.load(Ordering::SeqCst) == MATCHER_RETRY_IDLE
            {
                assert_eq!(state.warning_count, 1);
                break;
            }
            drop(state);
            assert!(started.elapsed() < Duration::from_secs(3));
            std::thread::yield_now();
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_scheduler_admission_exhaustion_keeps_target_fail_open_and_idle() {
        let scheduler = MatcherBuildScheduler::new();
        let warning_kinds = Arc::new(std::sync::Mutex::new(Vec::new()));
        *scheduler
            .warning_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some({
            let scheduler = scheduler.clone();
            let warning_kinds = warning_kinds.clone();
            Arc::new(move |kind| {
                assert!(scheduler.state.try_lock().is_ok());
                warning_kinds
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(kind);
            })
        });
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_admission_id = u64::MAX;
        let (root, classifier, _rx) = isolated_deferred_classifier(scheduler.clone());

        classifier.request_matcher_retry();

        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.live_workers, 0);
        assert_eq!(state.starting_workers, 0);
        assert_eq!(state.warning_count, 1);
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert!(
            classifier
                .classify(
                    &root.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any)
                )
                .is_some()
        );
        drop(state);
        assert_eq!(
            *warning_kinds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![MatcherSchedulerWarningKind::AdmissionIdExhausted]
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn test_scheduler_observer_is_isolated_and_panic_cleanup_is_bounded() {
        let (phase_tx, phase_rx) = std::sync::mpsc::sync_channel(16);
        let observer = Arc::new(TestSchedulerObserver {
            tx: phase_tx,
            full: std::sync::atomic::AtomicUsize::new(0),
            disconnected: std::sync::atomic::AtomicUsize::new(0),
        });
        let observed_scheduler = MatcherBuildScheduler::new_with_test_observer(observer.clone());
        let unobserved_scheduler = MatcherBuildScheduler::new();
        assert!(observed_scheduler.test_observer.is_some());
        assert!(unobserved_scheduler.test_observer.is_none());
        assert!(MatcherBuildScheduler::global().test_observer.is_none());

        let (_observed_root, observed, _observed_rx) =
            isolated_deferred_classifier(observed_scheduler.clone());
        let (_other_root, other, mut other_rx) =
            isolated_deferred_classifier(unobserved_scheduler.clone());
        assert_ne!(observed.matcher_target.token, other.matcher_target.token);
        let (build_reached, release_build) =
            install_matcher_retry_hook(&observed.matcher_build_hook);
        let mut observed_cleanup = SuccessorExhaustionCleanup {
            scheduler: observed_scheduler.clone(),
            proceed: Some(release_build),
            completed: false,
        };
        observed.request_matcher_retry();
        build_reached.recv_timeout(Duration::from_secs(3)).unwrap();
        observed_scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_admission_id = u64::MAX;
        observed.request_matcher_retry();
        other.request_matcher_retry();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &observed_scheduler,
            SuccessorExhaustionPhase::BeforeRollback {
                admission_id: AdmissionId(0),
            },
        );
        recv_successor_exhaustion_phase(
            &phase_rx,
            &observed_scheduler,
            SuccessorExhaustionPhase::Popped {
                admission_id: AdmissionId(0),
                generation: 1,
            },
        );
        observed_cleanup.release_proceed();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &observed_scheduler,
            SuccessorExhaustionPhase::CommitStale {
                attempt_generation: 1,
                latest_generation: 2,
            },
        );
        wait_for_matcher_retry_idle_sync(&observed);
        let observed_state = observed_scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(observed_state.jobs.is_empty());
        assert_eq!(observed_state.active_builds, 0);
        assert_eq!(observed_state.warning_count, 1);
        drop(observed_state);
        assert_eq!(
            phase_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        wait_for_matcher_retry_idle_sync(&other);
        assert!(matches!(
            other_rx.try_recv(),
            Ok(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        ));
        assert_eq!(observer.full.load(Ordering::SeqCst), 0);
        assert_eq!(observer.disconnected.load(Ordering::SeqCst), 0);

        drop(phase_rx);
        let counters_before = (
            observer.full.load(Ordering::SeqCst),
            observer.disconnected.load(Ordering::SeqCst),
        );
        let (_other_root_2, other_2, _other_rx_2) =
            isolated_deferred_classifier(unobserved_scheduler.clone());
        other_2.request_matcher_retry();
        wait_for_matcher_retry_idle_sync(&other_2);
        let none_scheduler = MatcherBuildScheduler::new();
        let (_none_root, none_classifier, _none_rx) =
            isolated_deferred_classifier(none_scheduler.clone());
        none_classifier.request_matcher_retry();
        wait_for_matcher_retry_idle_sync(&none_classifier);
        assert_eq!(
            (
                observer.full.load(Ordering::SeqCst),
                observer.disconnected.load(Ordering::SeqCst),
            ),
            counters_before,
            "jobs on unobserved schedulers must not touch another scheduler's observer"
        );

        let (panic_phase_tx, _panic_phase_rx) = std::sync::mpsc::sync_channel(16);
        let panic_scheduler =
            MatcherBuildScheduler::new_with_test_observer(Arc::new(TestSchedulerObserver {
                tx: panic_phase_tx,
                full: std::sync::atomic::AtomicUsize::new(0),
                disconnected: std::sync::atomic::AtomicUsize::new(0),
            }));
        assert!(!Arc::ptr_eq(
            &panic_scheduler,
            MatcherBuildScheduler::global()
        ));
        let (_panic_root, panic_classifier, _panic_rx) =
            isolated_deferred_classifier(panic_scheduler.clone());
        let (panic_reached, panic_release) =
            install_matcher_retry_hook(&panic_classifier.matcher_build_hook);
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _cleanup = SuccessorExhaustionCleanup {
                scheduler: panic_scheduler.clone(),
                proceed: Some(panic_release),
                completed: false,
            };
            panic_classifier.request_matcher_retry();
            panic_reached.recv_timeout(Duration::from_secs(3)).unwrap();
            panic!("expected observer isolation cleanup panic");
        }));
        assert!(panic_result.is_err());
        panic_scheduler.wait_for_stopped();
        let panic_state = panic_scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(panic_state.jobs.is_empty());
        assert_eq!(panic_state.active_builds, 0);
        assert_eq!(panic_state.live_workers, 0);
        assert_eq!(panic_state.starting_workers, 0);
        drop(panic_state);
        assert_eq!(
            other.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );

        observed_cleanup.complete();
        observed_scheduler.wait_for_stopped();
        unobserved_scheduler.shutdown_for_test();
        unobserved_scheduler.wait_for_stopped();
        none_scheduler.shutdown_for_test();
        none_scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_scheduler_successor_admission_exhaustion_drops_stale_result() {
        let (phase_tx, phase_rx) = std::sync::mpsc::sync_channel(16);
        let observer = Arc::new(TestSchedulerObserver {
            tx: phase_tx,
            full: std::sync::atomic::AtomicUsize::new(0),
            disconnected: std::sync::atomic::AtomicUsize::new(0),
        });
        let scheduler = MatcherBuildScheduler::new_with_test_observer(observer.clone());
        assert!(!Arc::ptr_eq(&scheduler, MatcherBuildScheduler::global()));
        assert!(scheduler.test_observer.is_some());
        assert!(MatcherBuildScheduler::new().test_observer.is_none());
        assert!(MatcherBuildScheduler::global().test_observer.is_none());
        let (root, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let (rollback_reached, release_rollback) =
            install_matcher_rollback_hook(&scheduler.rollback_hook, AdmissionId(0));
        let (next_job_reached, release_next_job) =
            install_matcher_retry_hook(&scheduler.before_next_job_hook);
        let (build_reached, release_build) =
            install_matcher_retry_hook(&classifier.matcher_build_hook);
        let mut cleanup = MatcherRaceCleanup {
            scheduler: scheduler.clone(),
            next_job_proceed: Some(release_next_job),
            rollback_proceed: Some(release_rollback),
            build_proceed: Some(release_build),
            completed: false,
        };
        let request_classifier = classifier.clone();
        let request = std::thread::spawn(move || request_classifier.request_matcher_retry());
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::BeforeRollback {
                admission_id: AdmissionId(0),
            },
        );
        rollback_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("initial admission did not reach its rollback owner");
        next_job_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("worker did not pause before the initial admission");
        {
            let state = scheduler
                .state
                .try_lock()
                .unwrap_or_else(|_| panic!("scheduler state contended at queued checkpoint"));
            let queued = state
                .jobs
                .get(&classifier.matcher_target.token)
                .expect("initial physical admission must remain queued");
            assert_eq!(queued.admission_id, AdmissionId(0));
            assert_eq!(queued.attempt_generation, 1);
            assert_eq!(state.active_builds, 0);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_QUEUED
            );
        }
        cleanup.release_next_job();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::Popped {
                admission_id: AdmissionId(0),
                generation: 1,
            },
        );
        build_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("initial admission did not reach its build barrier");
        {
            let state = scheduler
                .state
                .try_lock()
                .unwrap_or_else(|_| panic!("scheduler state contended at running checkpoint"));
            assert!(state.jobs.is_empty());
            assert_eq!(state.active_builds, 1);
            assert!(state.live_workers >= 1);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_RUNNING
            );
        }
        cleanup.release_rollback();
        request.join().unwrap();
        {
            let state = scheduler
                .state
                .try_lock()
                .unwrap_or_else(|_| panic!("scheduler state contended after rollback terminal"));
            assert!(state.jobs.is_empty());
            assert_eq!(state.active_builds, 1);
            assert_eq!(state.warning_count, 0);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_RUNNING
            );
        }
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_admission_id = u64::MAX;
        classifier.reload_matcher();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            2
        );
        assert!(
            classifier
                .classify(
                    &root.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_some(),
            "semantic invalidation must leave the active generation fail-open"
        );
        let (next_loop_reached, release_next_loop) =
            install_matcher_retry_hook(&scheduler.before_next_job_hook);
        cleanup.next_job_proceed = Some(release_next_loop);
        cleanup.release_build();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::CommitStale {
                attempt_generation: 1,
                latest_generation: 2,
            },
        );
        next_loop_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("finishing worker did not return to the scheduler loop");
        wait_for_matcher_retry_idle_sync(&classifier);
        assert_eq!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
            "a stale attempt must not publish recovery when its successor cannot be admitted"
        );
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.jobs.is_empty());
            assert_eq!(state.warning_count, 1);
            assert_eq!(state.active_builds, 0);
            assert_eq!(state.live_workers, MATCHER_BUILD_CONCURRENCY);
            assert_eq!(state.starting_workers, 0);
            state.next_admission_id = 2;
        }
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            2
        );
        assert!(
            classifier
                .classify(
                    &root.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_some(),
            "successor admission exhaustion must leave the latest generation fail-open"
        );

        let (_foreign_root, foreign, mut foreign_rx) =
            isolated_deferred_classifier(scheduler.clone());
        foreign.request_matcher_retry();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::BeforeRollback {
                admission_id: AdmissionId(2),
            },
        );
        cleanup.release_next_job();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::Popped {
                admission_id: AdmissionId(2),
                generation: 1,
            },
        );
        wait_for_matcher_retry_idle_sync(&foreign);
        assert_eq!(
            foreign_rx.try_recv(),
            Ok(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        let final_state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(final_state.jobs.is_empty());
        assert_eq!(final_state.active_builds, 0);
        assert_eq!(final_state.live_workers, MATCHER_BUILD_CONCURRENCY);
        drop(final_state);
        assert_eq!(observer.full.load(Ordering::SeqCst), 0);
        assert_eq!(observer.disconnected.load(Ordering::SeqCst), 0);
        cleanup.complete();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn failed_attempt_with_requested_successor_exhaustion_returns_idle() {
        let scheduler = MatcherBuildScheduler::new();
        let (root, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let (build_reached, release_build) =
            install_matcher_retry_hook(&classifier.matcher_build_hook);
        let mut cleanup = SuccessorExhaustionCleanup {
            scheduler: scheduler.clone(),
            proceed: Some(release_build),
            completed: false,
        };
        classifier.request_matcher_retry();
        build_reached.recv_timeout(Duration::from_secs(3)).unwrap();
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_admission_id = u64::MAX;
        classifier.request_matcher_retry();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        std::fs::remove_dir_all(root.path()).unwrap();
        cleanup.release_proceed();

        wait_for_matcher_retry_idle_sync(&classifier);
        assert_eq!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.active_builds, 0);
        assert_eq!(state.warning_count, 1);
        assert_eq!(state.next_admission_id, u64::MAX);
        drop(state);
        cleanup.complete();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn closed_receiver_precedes_successor_admission_exhaustion() {
        let scheduler = MatcherBuildScheduler::new();
        let (_root, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
        let (build_reached, release_build) =
            install_matcher_retry_hook(&classifier.matcher_build_hook);
        let mut cleanup = SuccessorExhaustionCleanup {
            scheduler: scheduler.clone(),
            proceed: Some(release_build),
            completed: false,
        };
        classifier.request_matcher_retry();
        build_reached.recv_timeout(Duration::from_secs(3)).unwrap();
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_admission_id = u64::MAX;
        classifier.request_matcher_retry();
        drop(rx);
        cleanup.release_proceed();

        wait_for_matcher_retry_idle_sync(&classifier);
        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.active_builds, 0);
        assert_eq!(state.warning_count, 0);
        assert_eq!(state.next_admission_id, u64::MAX);
        drop(state);
        cleanup.complete();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn exhausted_admission_allocator_rejects_later_requests_without_workers() {
        const LATER_REQUESTS: usize = 1_000;
        let scheduler = MatcherBuildScheduler::new();
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_admission_id = u64::MAX;
        let (_root, classifier, _rx) = isolated_deferred_classifier(scheduler.clone());

        classifier.request_matcher_retry();
        for _ in 0..LATER_REQUESTS {
            classifier.request_matcher_retry();
        }

        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.live_workers, 0);
        assert_eq!(state.starting_workers, 0);
        assert_eq!(state.active_builds, 0);
        assert_eq!(state.next_admission_id, u64::MAX);
        assert_eq!(state.warning_count, LATER_REQUESTS + 1);
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            (LATER_REQUESTS + 1) as u64
        );
        drop(state);
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_scheduler_last_spawn_failure_drains_every_queued_admission() {
        const TARGETS: usize = 5;
        let scheduler = MatcherBuildScheduler::new();
        let warning_kinds = Arc::new(std::sync::Mutex::new(Vec::new()));
        *scheduler
            .warning_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some({
            let warning_kinds = warning_kinds.clone();
            Arc::new(move |kind| {
                warning_kinds
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(kind);
            })
        });
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.starting_workers = MATCHER_BUILD_CONCURRENCY;
            state.injected_spawn_failures = MATCHER_BUILD_CONCURRENCY;
        }
        let mut owned = Vec::new();
        for _ in 0..TARGETS {
            let owned_classifier = isolated_deferred_classifier(scheduler.clone());
            owned_classifier.1.request_matcher_retry();
            owned.push(owned_classifier);
        }
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(state.jobs.len(), TARGETS);
            assert_eq!(state.starting_workers, MATCHER_BUILD_CONCURRENCY);
        }

        assert!(!scheduler.spawn_reserved_worker());
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(state.jobs.len(), TARGETS);
            assert_eq!(state.starting_workers, 1);
        }
        assert!(!scheduler.spawn_reserved_worker());

        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.live_workers, 0);
        assert_eq!(state.starting_workers, 0);
        assert_eq!(state.warning_count, 1);
        for (_, classifier, _) in &owned {
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_IDLE
            );
        }
        drop(state);
        assert_eq!(
            *warning_kinds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![MatcherSchedulerWarningKind::NoLiveWorker]
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_scheduler_spawn_error_and_zero_worker_collapse_warn_separately() {
        let scheduler = MatcherBuildScheduler::new();
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.starting_workers = MATCHER_BUILD_CONCURRENCY;
        }
        let (_root, classifier, _rx) = isolated_deferred_classifier(scheduler.clone());
        classifier.request_matcher_retry();
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(state.jobs.len(), 1);
            // Model the last outstanding reservation returning an OS spawn
            // error. The error and the zero-worker collapse are distinct
            // warnings for the same incident.
            state.starting_workers = 1;
        }
        let warnings = scheduler
            .resolve_failed_spawn(Some(std::io::Error::other("injected OS spawn failure")));
        assert_eq!(warnings.len(), 2);
        assert_eq!(
            warnings
                .iter()
                .map(MatcherSchedulerWarning::kind)
                .collect::<Vec<_>>(),
            vec![
                MatcherSchedulerWarningKind::WorkerSpawnFailed,
                MatcherSchedulerWarningKind::NoLiveWorker,
            ]
        );
        for warning in warnings {
            scheduler.emit_warning(warning);
        }
        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.warning_count, 2);
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        drop(state);
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn rollback_hook_is_owned_by_matching_physical_admission_only() {
        let scheduler = MatcherBuildScheduler::new();
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = 1;
        let (_root, classifier, _rx) = isolated_deferred_classifier(scheduler.clone());
        let weak_target = Arc::downgrade(&classifier.matcher_target);
        let (next_job_reached, release_next_job) =
            install_matcher_retry_hook(&scheduler.before_next_job_hook);
        let (rollback_reached, release_rollback) =
            install_matcher_rollback_hook(&scheduler.rollback_hook, AdmissionId(0));
        let mut cleanup = MatcherRaceCleanup {
            scheduler: scheduler.clone(),
            next_job_proceed: Some(release_next_job),
            rollback_proceed: Some(release_rollback),
            build_proceed: None,
            completed: false,
        };

        assert!(!wait_on_matcher_rollback_hook(
            &scheduler.rollback_hook,
            AdmissionId(1),
        ));
        assert_eq!(
            scheduler
                .rollback_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|hook| hook.expected_admission_id),
            Some(AdmissionId(0)),
            "a mismatched admission must not steal the one-shot hook"
        );

        let t1_classifier = classifier.clone();
        let t1 = std::thread::spawn(move || t1_classifier.request_matcher_retry());
        rollback_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("the matching initial admission did not consume the rollback hook");
        next_job_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("the sole worker did not reach its pre-pop barrier");

        let (_unused_reached, unused_proceed) =
            install_matcher_rollback_hook(&scheduler.rollback_hook, AdmissionId(1));
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = 1;
        let (t2_done_tx, t2_done_rx) = std::sync::mpsc::sync_channel(1);
        let t2_scheduler = scheduler.clone();
        let t2_target = classifier.matcher_target.clone();
        let t2 = std::thread::spawn(move || {
            t2_scheduler.request(&t2_target);
            let _ = t2_done_tx.send(());
        });
        t2_done_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("a coalesced queued request blocked on the admission-owned hook");
        t2.join().unwrap();
        assert_eq!(
            scheduler
                .rollback_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|hook| hook.expected_admission_id),
            Some(AdmissionId(1)),
            "a queued request without a physical admission must not consume the hook"
        );
        drop(
            scheduler
                .rollback_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        drop(unused_proceed);

        cleanup.release_rollback();
        t1.join().unwrap();
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let queued = state
                .jobs
                .get(&classifier.matcher_target.token)
                .expect("a serviceable queued admission must survive local rollback");
            assert_eq!(queued.admission_id, AdmissionId(0));
            assert_eq!(state.live_workers, 1);
            assert_eq!(state.starting_workers, 0);
            assert_eq!(state.warning_count, 0);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_QUEUED
            );
        }
        cleanup.release_next_job();
        wait_for_test_scheduler_quiescent(&scheduler, &classifier.matcher_target);
        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.active_builds, 0);
        assert_eq!(state.starting_workers, 0);
        assert_eq!(state.live_workers, MATCHER_BUILD_CONCURRENCY);
        assert_eq!(state.warning_count, 0);
        drop(state);
        cleanup.complete();
        scheduler.wait_for_stopped();
        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.active_builds, 0);
        assert_eq!(state.starting_workers, 0);
        assert_eq!(state.live_workers, 0);
        drop(state);
        drop(classifier);
        assert!(weak_target.upgrade().is_none());
    }

    #[tokio::test]
    async fn failed_spawn_rollback_does_not_clobber_later_worker_ownership() {
        let (phase_tx, phase_rx) = std::sync::mpsc::sync_channel(32);
        let observer = Arc::new(TestSchedulerObserver {
            tx: phase_tx,
            full: std::sync::atomic::AtomicUsize::new(0),
            disconnected: std::sync::atomic::AtomicUsize::new(0),
        });
        let scheduler = MatcherBuildScheduler::new_with_test_observer(observer.clone());
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.injected_spawn_failures = 1;
        }
        let (_root, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let (rollback_reached, release_rollback) =
            install_matcher_rollback_hook(&scheduler.rollback_hook, AdmissionId(0));
        let (next_job_reached, release_next_job) =
            install_matcher_retry_hook(&scheduler.before_next_job_hook);
        let (build_reached, release_build) =
            install_matcher_retry_hook(&classifier.matcher_build_hook);
        let mut cleanup = MatcherRaceCleanup {
            scheduler: scheduler.clone(),
            next_job_proceed: Some(release_next_job),
            rollback_proceed: Some(release_rollback),
            build_proceed: Some(release_build),
            completed: false,
        };
        let request_classifier = classifier.clone();
        let request = std::thread::spawn(move || request_classifier.request_matcher_retry());
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::BeforeRollback {
                admission_id: AdmissionId(0),
            },
        );
        rollback_reached
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        next_job_reached
            .recv_timeout(Duration::from_secs(3))
            .unwrap();

        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = 1;
        let (t2_done_tx, t2_done_rx) = std::sync::mpsc::sync_channel(1);
        let t2_scheduler = scheduler.clone();
        let t2_target = classifier.matcher_target.clone();
        let t2 = std::thread::spawn(move || {
            t2_scheduler.request(&t2_target);
            let _ = t2_done_tx.send(());
        });
        if let Err(error) = t2_done_rx.recv_timeout(Duration::from_secs(3)) {
            let state = scheduler.state.try_lock().map_or_else(
                |_| "Contended".to_string(),
                |state| {
                    format!(
                        "Acquired(jobs={},active={},live={},starting={},target_state={})",
                        state.jobs.len(),
                        state.active_builds,
                        state.live_workers,
                        state.starting_workers,
                        classifier.matcher_retry_state.load(Ordering::SeqCst),
                    )
                },
            );
            let rollback_owner = scheduler.rollback_hook.try_lock().map_or_else(
                |_| "Contended".to_string(),
                |hook| {
                    format!(
                        "Acquired(expected={:?})",
                        hook.as_ref().map(|hook| hook.expected_admission_id)
                    )
                },
            );
            panic!(
                "same-target queued request did not terminate: {error}; state={state}; rollback_hook={rollback_owner}"
            );
        }
        t2.join().unwrap();
        cleanup.release_next_job();

        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::Popped {
                admission_id: AdmissionId(0),
                generation: 1,
            },
        );
        build_reached.recv_timeout(Duration::from_secs(3)).unwrap();
        {
            let state = scheduler.state.try_lock().unwrap_or_else(|_| {
                panic!("scheduler state was contended after worker pop acknowledgement")
            });
            assert!(state.jobs.is_empty());
            assert_eq!(state.live_workers, 1);
            assert_eq!(state.starting_workers, 0);
            assert_eq!(state.active_builds, 1);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_RUNNING
            );
        }

        cleanup.release_rollback();
        request.join().unwrap();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING
        );
        classifier.reload_matcher();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        assert!(
            scheduler
                .state
                .try_lock()
                .unwrap_or_else(|_| panic!("scheduler state was contended after rollback terminal"))
                .jobs
                .is_empty()
        );
        assert!(
            classifier
                .classify(
                    &_root.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_some(),
            "semantic invalidation must publish fail-open filtering immediately"
        );
        cleanup.release_build();

        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::CommitStale {
                attempt_generation: 1,
                latest_generation: 2,
            },
        );

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("successor matcher build did not terminate"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert!(
            classifier
                .classify(
                    &_root.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_none(),
            "latest matcher generation was not installed"
        );
        let state = scheduler
            .state
            .try_lock()
            .unwrap_or_else(|_| panic!("scheduler state was contended at race carrier terminal"));
        assert!(state.jobs.is_empty());
        assert_eq!(state.warning_count, 0);
        assert_eq!(state.active_builds, 0);
        drop(state);
        assert_eq!(observer.full.load(Ordering::SeqCst), 0);
        assert_eq!(observer.disconnected.load(Ordering::SeqCst), 0);
        cleanup.complete();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn stale_spawn_rollback_preserves_new_queued_admission() {
        let (phase_tx, phase_rx) = std::sync::mpsc::sync_channel(32);
        let observer = Arc::new(TestSchedulerObserver {
            tx: phase_tx,
            full: std::sync::atomic::AtomicUsize::new(0),
            disconnected: std::sync::atomic::AtomicUsize::new(0),
        });
        let scheduler = MatcherBuildScheduler::new_with_test_observer(observer.clone());
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = 1;
        let (_root, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let (rollback_reached, release_rollback) =
            install_matcher_rollback_hook(&scheduler.rollback_hook, AdmissionId(0));
        let (first_next_reached, release_first_next) =
            install_matcher_retry_hook(&scheduler.before_next_job_hook);
        let (build_reached, release_build) =
            install_matcher_retry_hook(&classifier.matcher_build_hook);
        let mut cleanup = MatcherRaceCleanup {
            scheduler: scheduler.clone(),
            next_job_proceed: Some(release_first_next),
            rollback_proceed: Some(release_rollback),
            build_proceed: Some(release_build),
            completed: false,
        };

        let t1_classifier = classifier.clone();
        let t1 = std::thread::spawn(move || t1_classifier.request_matcher_retry());
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::BeforeRollback {
                admission_id: AdmissionId(0),
            },
        );
        rollback_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("a1 rollback did not reach its owner barrier");
        first_next_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("worker did not reach the a1 pre-pop barrier");
        cleanup.release_next_job();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::Popped {
                admission_id: AdmissionId(0),
                generation: 1,
            },
        );
        build_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("a1 did not reach its build barrier");

        let (second_next_reached, release_second_next) =
            install_matcher_retry_hook(&scheduler.before_next_job_hook);
        cleanup.next_job_proceed = Some(release_second_next);
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = 1;
        cleanup.release_build();
        second_next_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("worker did not pause before polling for a2");
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(state.injected_spawn_failures, 0);
            assert_eq!(state.live_workers, 1);
            assert_eq!(state.starting_workers, 0);
            assert_eq!(state.warning_count, 0);
        }
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert_eq!(
            rx.try_recv(),
            Ok(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );

        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = 1;
        classifier.reload_matcher();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::BeforeRollback {
                admission_id: AdmissionId(1),
            },
        );
        assert!(
            scheduler
                .rollback_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none(),
            "a2 must not install or retain a rollback hook after the a1 owner took it"
        );
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let a2 = state
                .jobs
                .get(&classifier.matcher_target.token)
                .expect("a2 must remain physically queued while the worker is paused");
            assert_eq!(a2.admission_id, AdmissionId(1));
            assert_eq!(a2.attempt_generation, 2);
            assert_eq!(state.active_builds, 0);
            assert_eq!(state.live_workers, 1);
            assert_eq!(state.starting_workers, 0);
            assert_eq!(state.injected_spawn_failures, 0);
            assert_eq!(state.warning_count, 0);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_QUEUED
            );
        }

        cleanup.release_rollback();
        t1.join().unwrap();
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let a2 = state
                .jobs
                .get(&classifier.matcher_target.token)
                .expect("stale a1 rollback must not remove a2");
            assert_eq!(a2.admission_id, AdmissionId(1));
            assert_eq!(state.warning_count, 0);
            assert_eq!(
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                MATCHER_RETRY_QUEUED
            );
        }

        cleanup.release_next_job();
        recv_successor_exhaustion_phase(
            &phase_rx,
            &scheduler,
            SuccessorExhaustionPhase::Popped {
                admission_id: AdmissionId(1),
                generation: 2,
            },
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("a2 recovery timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
        let state = scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.jobs.is_empty());
        assert_eq!(state.active_builds, 0);
        assert_eq!(state.warning_count, 0);
        drop(state);
        assert_eq!(observer.full.load(Ordering::SeqCst), 0);
        assert_eq!(observer.disconnected.load(Ordering::SeqCst), 0);
        cleanup.complete();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_scheduler_prunes_dropped_weak_target_before_due() {
        let scheduler = MatcherBuildScheduler::new();
        let target = {
            let (_root, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
            let target = Arc::downgrade(&classifier.matcher_target);
            classifier.request_matcher_retry();
            drop(rx);
            target
        };

        let started = Instant::now();
        loop {
            let empty = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty();
            if empty && target.upgrade().is_none() {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(1));
            std::thread::yield_now();
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn closed_request_wakes_and_prunes_future_due_queue_record() {
        let scheduler = MatcherBuildScheduler::new();
        let (_root, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
        let weak_target = Arc::downgrade(&classifier.matcher_target);
        classifier.request_matcher_retry();
        {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let job = state
                .jobs
                .get_mut(&classifier.matcher_target.token)
                .expect("future-due request was popped before its admission was observable");
            job.due = Instant::now() + MATCHER_RETRY_MAX;
        }

        drop(rx);
        let started = Instant::now();
        classifier.reload_matcher();
        loop {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.jobs.is_empty()
                && classifier.matcher_retry_state.load(Ordering::SeqCst) == MATCHER_RETRY_IDLE
            {
                assert_eq!(state.active_builds, 0);
                assert_eq!(state.warning_count, 0);
                break;
            }
            drop(state);
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "closed request did not wake the future-due queue prune"
            );
            std::thread::yield_now();
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
        drop(classifier);
        assert!(weak_target.upgrade().is_none());
    }

    #[test]
    fn receiver_drop_alone_prunes_future_due_record_within_retry_bound() {
        let scheduler = MatcherBuildScheduler::new();
        let (_root, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
        let weak_target = Arc::downgrade(&classifier.matcher_target);
        classifier.request_matcher_retry();
        let future_due = {
            let mut state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let job = state
                .jobs
                .get_mut(&classifier.matcher_target.token)
                .expect("future-due request was popped before its admission was observable");
            job.due = Instant::now() + MATCHER_RETRY_MAX;
            job.due
        };

        let started = Instant::now();
        drop(rx);
        loop {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.jobs.is_empty()
                && classifier.matcher_retry_state.load(Ordering::SeqCst) == MATCHER_RETRY_IDLE
            {
                assert_eq!(state.active_builds, 0);
                assert_eq!(state.warning_count, 0);
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "drop-only close prune exceeded one second: state={} queue={} live={} due={future_due:?} poll={MATCHER_QUEUE_CLOSE_POLL:?}",
                classifier.matcher_retry_state.load(Ordering::SeqCst),
                state.jobs.len(),
                state.live_workers,
            );
            drop(state);
            std::thread::yield_now();
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
        drop(classifier);
        assert!(weak_target.upgrade().is_none());
    }

    #[test]
    fn close_request_and_queue_pop_race_converges_without_double_ownership() {
        let scheduler = MatcherBuildScheduler::new();
        for _ in 0..32 {
            let (_root, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
            let token = classifier.matcher_target.token;
            let weak_target = Arc::downgrade(&classifier.matcher_target);
            classifier.request_matcher_retry();
            drop(rx);
            classifier.reload_matcher();

            {
                let state = scheduler
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match classifier.matcher_retry_state.load(Ordering::SeqCst) {
                    MATCHER_RETRY_QUEUED => {
                        assert!(state.jobs.contains_key(&token));
                    }
                    MATCHER_RETRY_RUNNING => {
                        assert!(!state.jobs.contains_key(&token));
                        assert!(state.active_builds <= MATCHER_BUILD_CONCURRENCY);
                    }
                    MATCHER_RETRY_IDLE => {
                        assert!(!state.jobs.contains_key(&token));
                    }
                    MATCHER_RETRY_RUNNING_REQUESTED => {
                        panic!("closed request promoted a running owner to requested")
                    }
                    value => panic!("invalid matcher retry state: {value}"),
                }
            }

            let started = Instant::now();
            loop {
                let state = scheduler
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !state.jobs.contains_key(&token)
                    && classifier.matcher_retry_state.load(Ordering::SeqCst) == MATCHER_RETRY_IDLE
                {
                    break;
                }
                drop(state);
                assert!(started.elapsed() < Duration::from_secs(1));
                std::thread::yield_now();
            }
            drop(classifier);
            assert!(weak_target.upgrade().is_none());
        }
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.jobs.is_empty());
            assert_eq!(state.active_builds, 0);
            assert_eq!(state.warning_count, 0);
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[test]
    fn matcher_generation_overflow_stays_fail_open_and_does_not_queue() {
        let scheduler = MatcherBuildScheduler::new();
        let (root, classifier, _rx) = isolated_deferred_classifier(scheduler.clone());
        {
            let mut commit = classifier
                .matcher_target
                .commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            commit.desired_generation = u64::MAX;
            classifier
                .matcher_target
                .desired_generation
                .store(u64::MAX, Ordering::SeqCst);
        }

        classifier.reload_matcher();

        assert_eq!(
            classifier
                .matcher_target
                .generation_overflow_warning_count
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        assert!(
            classifier
                .classify(
                    &root.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_some()
        );
        classifier.reload_matcher();
        assert_eq!(
            classifier
                .matcher_target
                .generation_overflow_warning_count
                .load(Ordering::SeqCst),
            2,
            "later semantic requests remain fail-open and warn without wrapping"
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn matcher_scheduler_attempt_panic_keeps_workers_and_recovers() {
        let scheduler = MatcherBuildScheduler::new();
        let (_root, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        classifier
            .matcher_target
            .injected_attempt_panics
            .store(1, Ordering::SeqCst);
        classifier.request_matcher_retry();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("worker did not recover after an attempt panic"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert_eq!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .live_workers,
            MATCHER_BUILD_CONCURRENCY
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    fn run_git(root: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", root.join(".cairn-test-global"))
            .arg("-C")
            .arg(root)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git command failed: {args:?}");
    }

    fn init_git_repo(root: &Path) {
        std::fs::create_dir_all(root).unwrap();
        run_git(root, &["init", "-q"]);
    }

    fn debounced(event: notify::Event) -> notify_debouncer_full::DebouncedEvent {
        notify_debouncer_full::DebouncedEvent::new(event, std::time::Instant::now())
    }

    #[tokio::test]
    async fn ignore_change_publishes_transport_reason_before_async_matcher_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let ignored = root.join("sub/ignored.rs");
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );

        std::fs::write(root.join("sub/.gitignore"), "ignored.rs\n").unwrap();
        let (build_reached, release_build) =
            install_matcher_retry_hook(&classifier.retry_attempt_hook);
        let event = notify::Event::new(EventKind::Modify(ModifyKind::Any))
            .add_path(root.join("sub/.gitignore"));
        classifier.handle_batch(&[debounced(event)]);
        build_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("ignore callback synchronously performed the matcher build");

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("ignore transport reason timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            })
        );
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_some(),
            "callback must publish fail-open filtering before async recovery"
        );
        drop(rx);
        release_build.send(()).unwrap();
        wait_for_matcher_retry_idle(&classifier).await;
    }

    #[test]
    fn common_and_worktree_configs_are_ignore_controls() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("worktree");
        let common = tmp.path().join("main.git");
        let worktree_git_dir = common.join("worktrees/w1");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&worktree_git_dir).unwrap();
        std::fs::write(
            root.join(".git"),
            format!("gitdir: {}\n", worktree_git_dir.display()),
        )
        .unwrap();
        std::fs::write(worktree_git_dir.join("commondir"), "../..\n").unwrap();
        let metadata = resolve_git_metadata(&root).unwrap();
        let classifier = classifier_for(&root);

        assert!(classifier.is_ignore_control(&metadata.common_git_dir.join("config")));
        assert!(classifier.is_ignore_control(&metadata.worktree_git_dir.join("config.worktree")));
    }

    #[tokio::test]
    async fn local_core_excludes_config_change_reloads_classification() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        init_git_repo(root);
        std::fs::write(root.join("rules-a"), "/a.rs\n").unwrap();
        std::fs::write(root.join("rules-b"), "/b.rs\n").unwrap();
        std::fs::write(
            root.join(".git/config"),
            "[core]\n\texcludesFile = rules-a\n",
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let a = root.join("a.rs");
        let b = root.join("b.rs");
        assert!(
            classifier
                .classify(&a, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
        assert!(
            classifier
                .classify(&b, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );

        std::fs::write(
            root.join(".git/config"),
            "[core]\n\texcludesFile = rules-b\n",
        )
        .unwrap();
        let event = notify::Event::new(EventKind::Modify(ModifyKind::Any))
            .add_path(root.join(".git/config"));
        classifier.handle_batch(&[debounced(event)]);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("core.excludes transport reason timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            })
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("core.excludes matcher recovery timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        assert!(
            classifier
                .classify(&a, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );
        assert!(
            classifier
                .classify(&b, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
    }

    #[tokio::test]
    async fn linked_worktree_config_change_reloads_classification() {
        let tmp = tempfile::tempdir().unwrap();
        let main = tmp.path().join("main");
        let root = tmp.path().join("worktree");
        init_git_repo(&main);
        std::fs::write(main.join("tracked"), "").unwrap();
        run_git(&main, &["add", "tracked"]);
        run_git(
            &main,
            &[
                "-c",
                "user.name=Cairn Test",
                "-c",
                "user.email=cairn@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
        );
        run_git(
            &main,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "fixture-worktree",
                root.to_str().unwrap(),
            ],
        );
        run_git(&main, &["config", "extensions.worktreeConfig", "true"]);
        run_git(
            &root,
            &["config", "--worktree", "core.excludesFile", "rules-a"],
        );
        std::fs::write(root.join("rules-a"), "/a.rs\n").unwrap();
        std::fs::write(root.join("rules-b"), "/b.rs\n").unwrap();
        let metadata = resolve_git_metadata(&root).unwrap();
        let worktree_config = metadata.worktree_git_dir.join("config.worktree");
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root.as_path(), metadata, tx);
        let a = root.join("a.rs");
        let b = root.join("b.rs");
        assert!(classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(!classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));

        std::fs::write(&worktree_config, "[core]\n\texcludesFile = rules-b\n").unwrap();
        let event =
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(worktree_config);
        classifier.handle_batch(&[debounced(event)]);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("worktree config transport reason timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            })
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("worktree config matcher recovery timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        assert!(!classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));
    }

    /// Included config and selected excludes files are not watcher roots,
    /// wherever they live. Their current contents are sampled at matcher
    /// construction and become visible after a root config or
    /// `config.worktree` event, reload, reindex, or restart.
    #[tokio::test]
    async fn external_core_excludes_changes_only_on_explicit_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        let external = tmp.path().join("external-ignore");
        init_git_repo(&root);
        std::fs::write(&external, "/a.rs\n").unwrap();
        std::fs::write(
            root.join(".git/config"),
            format!("[core]\n\texcludesFile = {}\n", external.display()),
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(&root, resolve_git_metadata(&root).unwrap(), tx);
        let a = root.join("a.rs");
        let b = root.join("b.rs");
        assert!(classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(!classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));

        std::fs::write(&external, "/b.rs\n").unwrap();
        assert!(classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(!classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));

        classifier.reload_matcher();
        assert!(!classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(!classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("external excludes matcher recovery timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        assert!(!classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));
    }

    #[tokio::test]
    async fn malformed_ignore_reload_is_fail_open_then_recovers() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "ignored.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let ignored = root.join("ignored.rs");
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.handle_batch(&[debounced(
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(ignore_file.clone()),
        )]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            })
        );
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_some(),
            "a broken matcher must fail open"
        );

        std::fs::write(&ignore_file, "recovered.rs\n").unwrap();
        let recovered = root.join("recovered.rs");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if classifier
                    .classify(&recovered, EventKind::Modify(ModifyKind::Any))
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("matcher retry did not recover");
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
    }

    #[tokio::test]
    async fn deferred_matcher_is_fail_open_until_recovery_rescan() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
        let ignored = root.join("ignored.rs");
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier =
            EventClassifier::new_deferred(root, resolve_git_metadata(root).unwrap(), tx);
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_attempt_hook);

        classifier.begin_deferred_matcher_warmup();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("deferred matcher warm-up did not reach its build barrier");
        classifier.handle_batch(&[debounced(
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(ignored.clone()),
        )]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::File {
                path: ignored.clone(),
                change: FileChange::Touched,
            }),
            "fail-open classification must publish the event before recovery"
        );

        proceed_tx.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("matcher recovery edge was not published"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_none(),
            "the recovered matcher must resume ignore filtering"
        );
        wait_for_matcher_retry_idle(&classifier).await;
    }

    #[test]
    fn deferred_public_entry_arms_native_roots_before_warmup() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".git")).unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(4);
        let captured = Arc::new(std::sync::Mutex::new(None));
        let captured_after_arm = captured.clone();

        let handle = watch_repo_with_backend_mode_after_arm(
            tmp.path(),
            Duration::from_millis(20),
            tx,
            WatchBackend::Poll,
            true,
            move |classifier| {
                assert_eq!(
                    classifier.matcher_retry_state.load(Ordering::SeqCst),
                    MATCHER_RETRY_IDLE,
                    "warm-up must not own the retry state before native watch setup completes"
                );
                let (reached_rx, proceed_tx) =
                    install_matcher_retry_hook(&classifier.retry_attempt_hook);
                *captured_after_arm
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((classifier.clone(), reached_rx, proceed_tx));
            },
        )
        .unwrap();

        let (classifier, reached_rx, proceed_tx) = captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("after-arm observer did not run");
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("warm-up did not begin after native roots were armed");
        proceed_tx.send(()).unwrap();
        drop(handle);
        drop(_rx);
        let started = std::time::Instant::now();
        while classifier.matcher_retry_state.load(Ordering::SeqCst) != MATCHER_RETRY_IDLE {
            assert!(started.elapsed() < Duration::from_secs(3));
            std::thread::yield_now();
        }
    }

    #[test]
    fn native_setup_failure_does_not_begin_deferred_warmup() {
        let missing = tempfile::tempdir().unwrap().path().join("missing");
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let after_arm_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called = after_arm_called.clone();

        let result = watch_repo_with_backend_mode_after_arm(
            &missing,
            Duration::from_millis(20),
            tx,
            WatchBackend::Poll,
            true,
            move |_| {
                called.store(true, Ordering::SeqCst);
            },
        );

        assert!(result.is_err());
        assert!(!after_arm_called.load(Ordering::SeqCst));
    }

    #[test]
    fn eager_classifier_publishes_complete_matcher_before_return() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), "ignored.rs\n").unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        let classifier =
            EventClassifier::new(tmp.path(), resolve_git_metadata(tmp.path()).unwrap(), tx);

        assert!(
            classifier
                .classify(
                    &tmp.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_none(),
            "the eager constructor must not publish a fail-open matcher"
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
    }

    #[test]
    fn deferred_matcher_builds_hold_at_most_two_global_slots() {
        const REPOS: usize = 6;
        let scheduler = MatcherBuildScheduler::new();
        let mut roots = Vec::with_capacity(REPOS);
        let mut classifiers = Vec::with_capacity(REPOS);
        let mut receivers = Vec::with_capacity(REPOS);
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(REPOS);
        let (proceed_tx, proceed_rx) = std::sync::mpsc::sync_channel(REPOS);
        let shared_proceed = Arc::new(std::sync::Mutex::new(proceed_rx));

        for ordinal in 0..REPOS {
            let root = tempfile::tempdir().unwrap();
            std::fs::write(root.path().join(".gitignore"), "ignored.rs\n").unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            let classifier = EventClassifier::new_deferred_with_scheduler(
                root.path(),
                resolve_git_metadata(root.path()).unwrap(),
                tx,
                scheduler.clone(),
            );
            *classifier
                .matcher_build_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(MatcherRetryHook {
                reached: reached_tx.clone(),
                proceed: shared_proceed.clone(),
            });
            classifier.begin_deferred_matcher_warmup();
            roots.push(root);
            classifiers.push(classifier);
            receivers.push(rx);
            let _ = ordinal;
        }

        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("first matcher did not acquire a build slot");
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("second matcher did not acquire a build slot");
        assert_eq!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active_builds,
            MATCHER_BUILD_CONCURRENCY
        );
        assert!(
            reached_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "a third matcher entered the globally bounded build section"
        );

        for _ in 0..REPOS {
            proceed_tx.send(()).unwrap();
        }
        for (classifier, rx) in classifiers.iter().zip(receivers.iter_mut()) {
            assert_eq!(
                rx.blocking_recv(),
                Some(WatchEvent::Rescan {
                    reason: RescanReason::MatcherRecovered,
                })
            );
            let started = std::time::Instant::now();
            while classifier.matcher_retry_state.load(Ordering::SeqCst) != MATCHER_RETRY_IDLE {
                assert!(started.elapsed() < Duration::from_secs(3));
                std::thread::yield_now();
            }
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
        drop(roots);
    }

    #[tokio::test]
    async fn matcher_retry_hands_off_failure_arriving_before_owner_exit() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_exit_hook);

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        assert_ne!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );

        std::fs::write(&ignore_file, "first-recovery.rs\n").unwrap();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("first retry owner did not reach its exit handoff");
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        std::fs::write(&ignore_file, "second-recovery.rs\n").unwrap();
        proceed_tx.send(()).unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("the failure handed off at owner exit was lost"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
    }

    #[tokio::test]
    async fn matcher_retry_burst_coalesces_into_one_successor_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_exit_hook);

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        std::fs::write(&ignore_file, "first-recovery.rs\n").unwrap();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("first retry owner did not reach its exit handoff");
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );

        std::fs::write(&ignore_file, [0xff]).unwrap();
        for _ in 0..8 {
            classifier.reload_matcher();
        }
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        std::fs::write(&ignore_file, "second-recovery.rs\n").unwrap();
        proceed_tx.send(()).unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("coalesced successor did not recover"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn matcher_retry_failure_coalesces_ten_thousand_requests_into_one_backoff_record() {
        const REQUESTS: u64 = 10_000;
        let scheduler = MatcherBuildScheduler::new();
        let (tmp, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_exit_hook);

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("failed attempt did not reach its finish barrier");
        let repeated_error = {
            let failure_log = classifier
                .matcher_target
                .failure_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let failure = failure_log
                .as_ref()
                .expect("first matcher failure was not recorded");
            std::io::Error::new(failure.kind, failure.message.clone())
        };
        classifier
            .matcher_target
            .log_matcher_retry_failure(&repeated_error);
        assert_eq!(
            classifier
                .matcher_target
                .failure_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .expect("repeated matcher failure lost its window")
                .suppressed,
            1
        );
        for _ in 0..REQUESTS {
            classifier.reload_matcher();
        }
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            REQUESTS + 1
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty(),
            "running requests must not allocate a second physical record"
        );
        std::fs::write(&ignore_file, "recovered.rs\n").unwrap();
        proceed_tx.send(()).unwrap();

        let started = Instant::now();
        let original_due = loop {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(job) = state.jobs.get(&classifier.matcher_target.token) {
                assert_eq!(job.retry_delay, MATCHER_RETRY_INITIAL * 2);
                break job.due;
            }
            drop(state);
            assert!(started.elapsed() < Duration::from_secs(1));
            std::thread::yield_now();
        };
        classifier.reload_matcher();
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let after_due = state.jobs[&classifier.matcher_target.token].due;
            let after_generation = classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst);
            assert_eq!(state.jobs.len(), 1);
            assert_eq!(state.queue_hwm, 1);
            assert_eq!(after_generation, REQUESTS + 2);
            assert_eq!(
                after_due, original_due,
                "a queued semantic request must not advance failure backoff"
            );
            eprintln!(
                "carrier4 generation_after_10k={} generation_after_queued={} physical={} hwm={} retry_delay_ms={} due_baseline={original_due:?} due_after={after_due:?} due_equal={}",
                REQUESTS + 1,
                after_generation,
                state.jobs.len(),
                state.queue_hwm,
                state.jobs[&classifier.matcher_target.token]
                    .retry_delay
                    .as_millis(),
                after_due == original_due,
            );
        }

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("retry owner did not recover"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while classifier
                .matcher_target
                .failure_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successful matcher rebuild did not close the suppression window");
        assert!(
            classifier
                .matcher_target
                .failure_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none(),
            "successful matcher rebuild must close the suppression window"
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn matcher_retry_drops_pending_request_when_consumer_closes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_exit_hook);

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        std::fs::write(&ignore_file, "first-recovery.rs\n").unwrap();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("retry owner did not reach its exit handoff");
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        drop(rx);
        proceed_tx.send(()).unwrap();
        wait_for_matcher_retry_idle(&classifier).await;
    }

    #[tokio::test]
    async fn closed_reload_request_does_not_clear_active_retry_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let scheduler = MatcherBuildScheduler::new();
        let classifier = EventClassifier::new_deferred_with_scheduler(
            root,
            resolve_git_metadata(root).unwrap(),
            tx,
            scheduler.clone(),
        );
        let weak_target = Arc::downgrade(&classifier.matcher_target);
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_attempt_hook);

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("retry owner did not reach the controlled attempt");
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING
        );

        drop(rx);
        classifier.reload_matcher();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING,
            "a non-owner request must not clear active ownership"
        );
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty(),
            "a closed request must not insert a successor"
        );

        proceed_tx.send(()).unwrap();
        wait_for_matcher_retry_idle(&classifier).await;
        {
            let state = scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.jobs.is_empty());
            assert_eq!(state.active_builds, 0);
            assert_eq!(state.warning_count, 0);
        }
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
        drop(classifier);
        assert!(weak_target.upgrade().is_none());
    }

    #[tokio::test]
    async fn reload_is_immediately_fail_open_then_recovers_asynchronously() {
        let scheduler = MatcherBuildScheduler::new();
        let (tmp, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let ignored = tmp.path().join("ignored.rs");
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_attempt_hook);
        classifier.reload_matcher();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("reload callback synchronously performed the matcher build");
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING
        );
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_some(),
            "reload must publish fail-open filtering before asynchronous recovery"
        );
        proceed_tx.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn reload_after_build_before_commit_discards_stale_matcher_and_recovery() {
        let scheduler = MatcherBuildScheduler::new();
        let (tmp, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let old_ignored = tmp.path().join("ignored.rs");
        let latest_ignored = tmp.path().join("latest.rs");
        let (commit_reached, release_old) =
            install_matcher_retry_hook(&classifier.retry_commit_hook);
        classifier.request_matcher_retry();
        commit_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("old matcher did not reach its pre-commit barrier");

        std::fs::write(tmp.path().join(".gitignore"), "latest.rs\n").unwrap();
        let (successor_reached, release_successor) =
            install_matcher_retry_hook(&classifier.retry_attempt_hook);
        classifier.reload_matcher();
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        assert!(
            classifier
                .classify(&old_ignored, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );
        release_old.send(()).unwrap();
        successor_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("stale attempt did not hand off exactly one successor");
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            classifier
                .classify(&latest_ignored, EventKind::Modify(ModifyKind::Any))
                .is_some(),
            "stale matcher was installed while the successor was blocked"
        );

        release_successor.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        assert!(
            classifier
                .classify(&latest_ignored, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn reload_between_commit_and_scheduler_finish_converges_to_successor() {
        let scheduler = MatcherBuildScheduler::new();
        let (tmp, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        let latest_ignored = tmp.path().join("latest.rs");
        let (finish_reached, release_finish) =
            install_matcher_retry_hook(&classifier.retry_finish_hook);
        classifier.request_matcher_retry();
        finish_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("committed attempt did not reach its finish barrier");
        assert_eq!(
            rx.try_recv(),
            Ok(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );

        std::fs::write(tmp.path().join(".gitignore"), "latest.rs\n").unwrap();
        classifier.reload_matcher();
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        assert!(
            classifier
                .classify(&latest_ignored, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );
        release_finish.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        assert!(
            classifier
                .classify(&latest_ignored, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        let post_finish_ignored = tmp.path().join("post-finish.rs");
        std::fs::write(tmp.path().join(".gitignore"), "post-finish.rs\n").unwrap();
        let (post_finish_reached, release_post_finish) =
            install_matcher_retry_hook(&classifier.retry_attempt_hook);
        classifier.reload_matcher();
        post_finish_reached
            .recv_timeout(Duration::from_secs(3))
            .expect("post-finish reload did not admit one fresh attempt");
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            3
        );
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING
        );
        assert!(
            classifier
                .classify(&post_finish_ignored, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );
        release_post_finish.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered,
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert!(
            classifier
                .classify(&post_finish_ignored, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn recovery_commit_coalesces_full_channel_into_pending_dirty_edges() {
        let scheduler = MatcherBuildScheduler::new();
        let (tmp, classifier, mut rx) = isolated_deferred_classifier(scheduler.clone());
        for ordinal in 0..4 {
            classifier
                .tx
                .try_send(WatchEvent::File {
                    path: tmp.path().join(format!("pending-{ordinal}.rs")),
                    change: FileChange::Touched,
                })
                .unwrap();
        }
        classifier.request_matcher_retry();
        wait_for_matcher_retry_idle(&classifier).await;
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            1
        );
        assert!(
            classifier
                .classify(
                    &tmp.path().join("ignored.rs"),
                    EventKind::Modify(ModifyKind::Any),
                )
                .is_none(),
            "a full recovery edge must still commit the matcher"
        );
        for _ in 0..4 {
            assert!(matches!(rx.recv().await, Some(WatchEvent::File { .. })));
        }
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }

    #[tokio::test]
    async fn recovery_commit_closed_channel_does_not_publish_or_requeue() {
        let scheduler = MatcherBuildScheduler::new();
        let (_tmp, classifier, rx) = isolated_deferred_classifier(scheduler.clone());
        let weak_target = Arc::downgrade(&classifier.matcher_target);
        let (commit_reached, release_commit) =
            install_matcher_retry_hook(&classifier.retry_commit_hook);
        classifier.request_matcher_retry();
        assert_eq!(
            classifier
                .matcher_target
                .desired_generation
                .load(Ordering::SeqCst),
            1
        );
        commit_reached.recv_timeout(Duration::from_secs(3)).unwrap();
        drop(rx);
        release_commit.send(()).unwrap();
        wait_for_matcher_retry_idle(&classifier).await;
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .jobs
                .is_empty()
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
        drop(classifier);
        assert!(weak_target.upgrade().is_none());
    }

    #[tokio::test]
    async fn matcher_scheduler_spawn_failure_releases_owner_for_later_request() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let scheduler = MatcherBuildScheduler::new();
        scheduler
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .injected_spawn_failures = MATCHER_BUILD_CONCURRENCY;
        let classifier = EventClassifier::new_deferred_with_scheduler(
            root,
            resolve_git_metadata(root).unwrap(),
            tx,
            scheduler.clone(),
        );

        classifier.request_matcher_retry();

        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE,
            "a failed spawn must release retry ownership"
        );
        assert_eq!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .warning_count,
            1,
            "the failed spawn must emit exactly one server-side warning"
        );
        assert_eq!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
            "spawn failure must not claim matcher recovery"
        );

        classifier.request_matcher_retry();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("a later request did not retry after spawn failure"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
        assert_eq!(
            scheduler
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .warning_count,
            1
        );
        scheduler.shutdown_for_test();
        scheduler.wait_for_stopped();
    }
}
