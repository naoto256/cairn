//! `cairn-watch` — filesystem and git-ref watcher.
//!
//! One [`watch_repo`] call sets up a debounced, gitignore-aware watch
//! on a repository, classifies incoming events into [`WatchEvent`]s,
//! and forwards them on a tokio mpsc channel. The caller (typically
//! the daemon) is responsible for routing events to the indexer.
//!
//! Two event tracks share one underlying watcher:
//! - **file events** (any source file change under the repo root)
//! - **git ref events** (`.git/HEAD`, `.git/refs/heads/*`,
//!   `.git/packed-refs`, `.git/worktrees/*/HEAD`)
//!
//! Branch-rename SHA reconciliation is left to the consumer of these
//! events; the watcher only reports raw add / remove / modify for
//! ref-shaped paths.

#![forbid(unsafe_code)]

mod matcher;
pub mod scan;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use matcher::{
    GitMetadataPaths, RepoIgnoreMatcher, is_nested_git_marker_path, resolve_git_metadata,
};
use notify::event::{CreateKind, ModifyKind, RemoveKind};
use notify::{Config, EventKind, PollWatcher, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{
    DebounceEventResult, Debouncer, RecommendedCache, new_debouncer, new_debouncer_opt,
};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

/// Errors surfaced by the watcher setup. Runtime classification errors
/// are logged via `tracing` and do not stop the stream.
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error("notify: {0}")]
    Notify(#[from] notify::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// What the watcher pushes onto its outgoing channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// A file inside the working tree changed in a way that may
    /// require re-indexing.
    File { path: PathBuf, change: FileChange },
    /// A git ref-shaped path changed.
    Git(GitEvent),
    /// The watcher cannot safely reduce the change to one path.
    /// Consumers must reconcile the complete repository snapshot.
    Rescan { reason: RescanReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RescanReason {
    /// A repository-local ignore control file changed.
    IgnoreRulesChanged,
    /// A directory was created, removed, or renamed, so nested
    /// ignore-file discovery must run again.
    DirectoryTopologyChanged,
    /// The watcher backend reported that events may have been lost.
    BackendRequested,
    /// The watcher backend returned a runtime error.
    WatchError,
    /// A previously broken ignore matcher rebuilt successfully. At the commit
    /// linearization point the attempted generation was installed and this
    /// dirty edge was published, or coalesced into an already-pending edge.
    /// It does not guarantee that the consumer has observed the latest
    /// semantic generation.
    MatcherRecovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileChange {
    /// Created or modified. We collapse these because for tree-sitter
    /// re-parsing the response is identical.
    Touched,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitEvent {
    /// `.git/HEAD` changed — the active branch may have switched.
    HeadChanged,
    /// `.git/refs/heads/<name>` was created or modified (branch tip
    /// moved). The SHA is not read here; downstream is responsible.
    BranchTouched { name: String },
    /// `.git/refs/heads/<name>` was removed.
    BranchDeleted { name: String },
    /// `.git/packed-refs` changed; some branches may be packed/unpacked.
    PackedRefsChanged,
    /// A linked worktree's HEAD shifted
    /// (`.git/worktrees/<wt>/HEAD`).
    WorktreeHeadChanged { worktree: String },
}

/// Handle that keeps the watcher alive. Drop to stop watching.
#[allow(dead_code)] // fields kept only for their Drop side-effects
pub struct WatcherHandle {
    debouncer: WatcherDebouncer,
}

#[allow(clippy::large_enum_variant)]
enum WatcherDebouncer {
    // This enum is intentionally concrete: the production and test
    // backends both rely on Drop side effects from notify-debouncer,
    // and the extra enum size is paid once per watched repo.
    Recommended(Debouncer<RecommendedWatcher, RecommendedCache>),
    Poll(Debouncer<PollWatcher, RecommendedCache>),
}

/// Native watcher backend choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchBackend {
    /// Platform-recommended backend (`FSEvents` on macOS). This is
    /// the production default.
    Recommended,
    /// Polling backend. Used by macOS tempdir-based tests where the
    /// FSEvents stream can fail to deliver any callback.
    Poll,
}

/// Begin watching `repo_root` recursively. Events are debounced over
/// `debounce` and pushed on `tx`. The returned handle must be kept
/// alive; dropping it stops the watcher.
///
/// Gitignore filtering uses the same hierarchical matcher as the startup
/// scanner: the effective `core.excludesFile`, `.git/info/exclude`, and
/// repository-local `.gitignore` files. Included config and selected excludes
/// files are sampled when the matcher is built, wherever they live; only the
/// root config and `config.worktree` are live ignore controls.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend(repo_root, debounce, tx, WatchBackend::Recommended)
}

/// Variant of [`watch_repo`] with an explicit backend. Production
/// callers should prefer [`watch_repo`]; tests and diagnostics can use
/// this to avoid platform-specific native-watcher gaps.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo_with_backend(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend_mode(repo_root, debounce, tx, backend, false)
}

/// Arm a watcher without waiting for the recursive ignore-matcher walk.
///
/// Startup uses this after the durable repository inventory is known. The
/// watcher begins fail-open, so ignore filtering cannot hide filesystem events
/// while the matcher warms in the bounded recovery pool. At its commit
/// linearization point, a successful warm-up installs the matcher for the
/// attempted generation and publishes [`RescanReason::MatcherRecovered`], or
/// coalesces it into an already-pending dirty edge. That edge does not
/// guarantee that its consumer has observed the latest semantic generation.
/// If both fixed workers are permanently stalled, later warm-ups are starved
/// and their watchers remain fail-open. Dynamic registration continues to use
/// [`watch_repo_with_backend`] and therefore preserves its eager
/// matcher-publication contract.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo_with_startup_deferred_matcher(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend_mode(repo_root, debounce, tx, backend, true)
}

fn watch_repo_with_backend_mode(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
    defer_matcher: bool,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend_mode_after_arm(repo_root, debounce, tx, backend, defer_matcher, |_| {})
}

fn watch_repo_with_backend_mode_after_arm(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
    defer_matcher: bool,
    after_arm: impl FnOnce(&EventClassifier),
) -> Result<WatcherHandle, WatchError> {
    let repo_root = repo_root.canonicalize()?;
    let git_metadata = resolve_git_metadata(&repo_root).unwrap_or_else(|err| {
        warn!(
            root = %repo_root.display(),
            error = %err,
            "git metadata resolution failed; watcher is fail-open"
        );
        GitMetadataPaths::fail_open(&repo_root)
    });
    let classifier = if defer_matcher {
        EventClassifier::new_deferred(&repo_root, git_metadata.clone(), tx)
    } else {
        EventClassifier::new(&repo_root, git_metadata.clone(), tx)
    };

    let callback_classifier = classifier.clone();
    let event_handler = move |result: DebounceEventResult| {
        handle_debounce_result(&callback_classifier, result);
    };
    let mut debouncer = match backend {
        WatchBackend::Recommended => {
            WatcherDebouncer::Recommended(new_debouncer(debounce, None, event_handler)?)
        }
        WatchBackend::Poll => {
            WatcherDebouncer::Poll(new_debouncer_opt::<_, PollWatcher, RecommendedCache>(
                debounce,
                None,
                event_handler,
                RecommendedCache::new(),
                Config::default().with_poll_interval(debounce),
            )?)
        }
    };
    debouncer.watch(&repo_root, RecursiveMode::Recursive)?;
    // Linked worktrees keep HEAD in their worktree git dir and refs /
    // info/exclude in the common git dir. Watch both identities.
    for git_root in git_metadata.watch_roots() {
        if git_root.is_dir() {
            let _ = debouncer.watch(&git_root, RecursiveMode::Recursive);
        }
    }
    after_arm(&classifier);
    if defer_matcher {
        classifier.begin_deferred_matcher_warmup();
    }
    Ok(WatcherHandle { debouncer })
}

/// Bridge between the `notify-debouncer-full` callback and the
/// classifier. A successful batch is classified per event; an error
/// batch is collapsed to a single [`RescanReason::WatchError`] edge
/// after logging each individual error, so a burst of backend
/// errors does not translate into a burst of rescan events.
fn handle_debounce_result(classifier: &EventClassifier, result: DebounceEventResult) {
    match result {
        Ok(events) => classifier.handle_batch(&events),
        Err(errs) => {
            for err in &errs {
                warn!(?err, "notify error");
            }
            classifier.handle_watch_error_batch();
        }
    }
}

impl WatcherDebouncer {
    fn watch(
        &mut self,
        path: impl AsRef<Path>,
        recursive_mode: RecursiveMode,
    ) -> notify::Result<()> {
        match self {
            WatcherDebouncer::Recommended(debouncer) => debouncer.watch(path, recursive_mode),
            WatcherDebouncer::Poll(debouncer) => debouncer.watch(path, recursive_mode),
        }
    }
}

/// Turns a debounced batch of raw notify events into the coarser
/// [`WatchEvent`] stream this crate exposes.
///
/// Clones are cheap because most state sits behind `Arc`s — the
/// debouncer's callback and the matcher-build workers both need
/// their own handle. (`tx: Sender` is the exception: it does its
/// own cheap clone.) `ignore` is `Arc<RwLock<Arc<RepoIgnoreMatcher>>>`:
/// the read lock protects each matcher call independently, so a
/// writer waits for that guard, and a swap may occur between the
/// prune check and the gitignore check because the classify path
/// reacquires the lock rather than holding one snapshot across the
/// whole classify.
///
/// `matcher_retry_state` serializes matcher rebuild attempts after a
/// fail-open while retaining one coalesced request that arrives during
/// the active owner's final successful attempt — see
/// [`Self::request_matcher_retry`]. `tx` is the outbound event channel;
/// its `Closed` state is the only shutdown signal this classifier reacts
/// to.
#[derive(Clone)]
struct EventClassifier {
    repo_root: Arc<PathBuf>,
    git_metadata: Arc<GitMetadataPaths>,
    ignore: Arc<RwLock<Arc<RepoIgnoreMatcher>>>,
    #[cfg(test)]
    matcher_retry_state: Arc<AtomicU8>,
    #[cfg(test)]
    retry_attempt_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    matcher_build_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_exit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_commit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_finish_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    tx: Sender<WatchEvent>,
    matcher_target: Arc<MatcherBuildTarget>,
}

const MATCHER_RETRY_IDLE: u8 = 0;
const MATCHER_RETRY_QUEUED: u8 = 1;
const MATCHER_RETRY_RUNNING: u8 = 2;
const MATCHER_RETRY_RUNNING_REQUESTED: u8 = 3;
const MATCHER_BUILD_CONCURRENCY: usize = 2;
const MATCHER_RETRY_INITIAL: Duration = Duration::from_millis(100);
const MATCHER_RETRY_MAX: Duration = Duration::from_secs(2);
const MATCHER_RETRY_WARNING_WINDOW: Duration = Duration::from_secs(60);
const MATCHER_BUILD_PANIC_ERROR: &str = "ignore matcher build panicked; watcher remains fail-open";
// A nonempty queue rechecks closed receivers at this cadence. An empty queue
// waits indefinitely and relies on admission or shutdown to provide a wake.
const MATCHER_QUEUE_CLOSE_POLL: Duration = Duration::from_millis(50);
static NEXT_MATCHER_TARGET_TOKEN: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
#[derive(Clone)]
struct MatcherRetryHook {
    reached: std::sync::mpsc::SyncSender<()>,
    proceed: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

#[cfg(test)]
struct MatcherRollbackHook {
    expected_admission_id: AdmissionId,
    reached: std::sync::mpsc::SyncSender<()>,
    proceed: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

struct MatcherBuildTarget {
    token: u64,
    repo_root: Arc<PathBuf>,
    git_metadata: Arc<GitMetadataPaths>,
    ignore: Arc<RwLock<Arc<RepoIgnoreMatcher>>>,
    state: Arc<AtomicU8>,
    desired_generation: AtomicU64,
    commit: Mutex<MatcherCommitState>,
    failure_log: Mutex<Option<MatcherRetryFailureWindow>>,
    tx: Sender<WatchEvent>,
    scheduler: Arc<MatcherBuildScheduler>,
    #[cfg(test)]
    retry_attempt_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    matcher_build_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_exit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_commit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_finish_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    injected_attempt_panics: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    generation_overflow_warning_count: std::sync::atomic::AtomicUsize,
}

struct MatcherRetryFailureWindow {
    kind: std::io::ErrorKind,
    message: String,
    started_at: Instant,
    suppressed: u64,
}

struct MatcherCommitState {
    desired_generation: u64,
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

struct MatcherBuildScheduler {
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

    fn global() -> &'static Arc<Self> {
        static SCHEDULER: OnceLock<Arc<MatcherBuildScheduler>> = OnceLock::new();
        SCHEDULER.get_or_init(Self::new)
    }

    fn request(self: &Arc<Self>, target: &Arc<MatcherBuildTarget>) {
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
            EventClassifier::wait_on_matcher_rollback_hook(&self.rollback_hook, admission_id);
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
            EventClassifier::wait_on_matcher_retry_hook(&owner.before_next_job_hook);
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
                EventClassifier::wait_on_matcher_retry_hook(&target.retry_attempt_hook);
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
                EventClassifier::wait_on_matcher_retry_hook(&target.matcher_build_hook);
                RepoIgnoreMatcher::build(&target.repo_root, &target.git_metadata.info_exclude)
            }));
            #[cfg(test)]
            EventClassifier::wait_on_matcher_retry_hook(&target.retry_commit_hook);
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
        EventClassifier::wait_on_matcher_retry_hook(&target.retry_exit_hook);
        #[cfg(test)]
        EventClassifier::wait_on_matcher_retry_hook(&target.retry_finish_hook);
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

impl EventClassifier {
    fn with_matcher(
        repo_root: &Path,
        git_metadata: GitMetadataPaths,
        tx: Sender<WatchEvent>,
        matcher: RepoIgnoreMatcher,
    ) -> Self {
        Self::with_matcher_and_scheduler(
            repo_root,
            git_metadata,
            tx,
            matcher,
            MatcherBuildScheduler::global().clone(),
        )
    }

    fn with_matcher_and_scheduler(
        repo_root: &Path,
        git_metadata: GitMetadataPaths,
        tx: Sender<WatchEvent>,
        matcher: RepoIgnoreMatcher,
        scheduler: Arc<MatcherBuildScheduler>,
    ) -> Self {
        let repo_root = Arc::new(repo_root.to_path_buf());
        let git_metadata = Arc::new(git_metadata);
        let ignore = Arc::new(RwLock::new(Arc::new(matcher)));
        let matcher_retry_state = Arc::new(AtomicU8::new(MATCHER_RETRY_IDLE));
        #[cfg(test)]
        let retry_attempt_hook = Arc::new(std::sync::Mutex::new(None));
        #[cfg(test)]
        let matcher_build_hook = Arc::new(std::sync::Mutex::new(None));
        #[cfg(test)]
        let retry_exit_hook = Arc::new(std::sync::Mutex::new(None));
        #[cfg(test)]
        let retry_commit_hook = Arc::new(std::sync::Mutex::new(None));
        #[cfg(test)]
        let retry_finish_hook = Arc::new(std::sync::Mutex::new(None));
        #[cfg(test)]
        let injected_attempt_panics = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let token = NEXT_MATCHER_TARGET_TOKEN.fetch_add(1, Ordering::Relaxed);
        assert_ne!(token, 0, "matcher target token space exhausted");
        let matcher_target = Arc::new(MatcherBuildTarget {
            token,
            repo_root: repo_root.clone(),
            git_metadata: git_metadata.clone(),
            ignore: ignore.clone(),
            state: matcher_retry_state.clone(),
            desired_generation: AtomicU64::new(0),
            commit: Mutex::new(MatcherCommitState {
                desired_generation: 0,
            }),
            failure_log: Mutex::new(None),
            tx: tx.clone(),
            scheduler,
            #[cfg(test)]
            retry_attempt_hook: retry_attempt_hook.clone(),
            #[cfg(test)]
            matcher_build_hook: matcher_build_hook.clone(),
            #[cfg(test)]
            retry_exit_hook: retry_exit_hook.clone(),
            #[cfg(test)]
            retry_commit_hook: retry_commit_hook.clone(),
            #[cfg(test)]
            retry_finish_hook: retry_finish_hook.clone(),
            #[cfg(test)]
            injected_attempt_panics,
            #[cfg(test)]
            generation_overflow_warning_count: std::sync::atomic::AtomicUsize::new(0),
        });
        Self {
            repo_root,
            git_metadata,
            ignore,
            #[cfg(test)]
            matcher_retry_state,
            #[cfg(test)]
            retry_attempt_hook,
            #[cfg(test)]
            matcher_build_hook,
            #[cfg(test)]
            retry_exit_hook,
            #[cfg(test)]
            retry_commit_hook,
            #[cfg(test)]
            retry_finish_hook,
            tx,
            matcher_target,
        }
    }

    fn new(repo_root: &Path, git_metadata: GitMetadataPaths, tx: Sender<WatchEvent>) -> Self {
        let (initial, initial_failed) =
            match RepoIgnoreMatcher::build(repo_root, &git_metadata.info_exclude) {
                Ok(matcher) => (matcher, false),
                Err(err) => {
                    warn!(error = %err, "ignore matcher build failed; watcher is fail-open");
                    (RepoIgnoreMatcher::fail_open(repo_root), true)
                }
            };
        let classifier = Self::with_matcher(repo_root, git_metadata, tx, initial);
        if initial_failed {
            classifier.request_matcher_retry();
        }
        classifier
    }

    /// Publish a permissive classifier immediately and warm the complete
    /// hierarchical matcher outside the startup barrier. Fail-open is the
    /// conservative direction: it may enqueue extra work, but never hides a
    /// filesystem event. Matcher recovery emits a full-rescan edge before the
    /// watcher returns to filtered operation.
    fn new_deferred(
        repo_root: &Path,
        git_metadata: GitMetadataPaths,
        tx: Sender<WatchEvent>,
    ) -> Self {
        Self::with_matcher(
            repo_root,
            git_metadata,
            tx,
            RepoIgnoreMatcher::fail_open(repo_root),
        )
    }

    #[cfg(test)]
    fn new_deferred_with_scheduler(
        repo_root: &Path,
        git_metadata: GitMetadataPaths,
        tx: Sender<WatchEvent>,
        scheduler: Arc<MatcherBuildScheduler>,
    ) -> Self {
        Self::with_matcher_and_scheduler(
            repo_root,
            git_metadata,
            tx,
            RepoIgnoreMatcher::fail_open(repo_root),
            scheduler,
        )
    }

    /// Start the deferred matcher build after the native watcher owns all
    /// roots. This ordering closes the gap in which a warm-up could complete
    /// and emit its recovery rescan before filesystem observation begins.
    fn begin_deferred_matcher_warmup(&self) {
        self.request_matcher_retry();
    }

    /// Classify one debounced batch.
    ///
    /// Events whose paths are wholly inside a shared-pruned
    /// working-tree subtree are removed before topology and
    /// ignore-control checks, except the two exact Ruby composed-bundle
    /// inputs that must dirty reconcile without entering the manifest.
    /// Every remaining event is tested in
    /// order; the batch stops at the first event that produces a
    /// rescan reason, using this per-event precedence:
    ///
    /// 1. The backend requested a rescan (`need_rescan()` flag) —
    ///    [`RescanReason::BackendRequested`].
    /// 2. Any path is an ignore-control file (info/exclude, local Git
    ///    config, worktree config, or a working-tree `.gitignore`) —
    ///    [`RescanReason::IgnoreRulesChanged`].
    /// 3. A create/remove/rename touches a nested `.git` marker in
    ///    the working tree — [`RescanReason::DirectoryTopologyChanged`].
    /// 4. Any working-tree directory create/remove/rename — same
    ///    [`RescanReason::DirectoryTopologyChanged`].
    ///
    /// When any rescan reason fires, the matcher is reloaded and a
    /// single `Rescan` event is enqueued on a best-effort basis (a
    /// `Full` channel coalesces it into the pending edge, a closed
    /// channel drops it) — the per-event classification loop is
    /// skipped, since the consumer will re-read the whole snapshot
    /// anyway.
    fn handle_batch(&self, events: &[notify_debouncer_full::DebouncedEvent]) {
        let is_observable = |event: &notify_debouncer_full::DebouncedEvent| {
            event.paths.is_empty()
                || !event.paths.iter().all(|path| {
                    !self.is_ruby_lsp_config_control_path(path)
                        && self.is_always_pruned_working_tree_path(path)
                })
        };
        let reason = events
            .iter()
            .filter(|event| is_observable(event))
            .find_map(|event| {
                if event.need_rescan() {
                    return Some(RescanReason::BackendRequested);
                }
                if event.paths.iter().any(|path| self.is_ignore_control(path)) {
                    return Some(RescanReason::IgnoreRulesChanged);
                }
                if matches!(
                    event.kind,
                    EventKind::Create(_)
                        | EventKind::Remove(_)
                        | EventKind::Modify(ModifyKind::Name(_))
                ) && event.paths.iter().any(|path| {
                    self.is_working_tree_path(path)
                        && is_nested_git_marker_path(&self.repo_root, path)
                }) {
                    return Some(RescanReason::DirectoryTopologyChanged);
                }
                (is_directory_topology_change(event.kind)
                    && event.paths.iter().any(|path| {
                        self.is_working_tree_path(path)
                            && !self.is_ruby_lsp_config_control_path(path)
                            && !self.is_always_pruned_working_tree_path(path)
                    }))
                .then_some(RescanReason::DirectoryTopologyChanged)
            });
        if let Some(reason) = reason {
            self.reload_matcher();
            self.emit(WatchEvent::Rescan { reason });
            return;
        }

        for ev in events.iter().filter(|event| is_observable(event)) {
            for path in &ev.paths {
                if let Some(out) = self.classify(path, ev.kind) {
                    if !self.emit(out) {
                        return;
                    }
                }
            }
        }
    }

    /// Response to a batch of backend errors: reload the ignore
    /// matcher (the errors may have masked ignore-file writes) and
    /// attempt to emit a single [`RescanReason::WatchError`] edge
    /// (best-effort — coalesced into the pending edge when the
    /// channel is `Full`, dropped when the consumer is gone; this
    /// path ignores `emit`'s return value). The individual error
    /// messages are logged by [`handle_debounce_result`] before this
    /// is called.
    fn handle_watch_error_batch(&self) {
        self.reload_matcher();
        self.emit(WatchEvent::Rescan {
            reason: RescanReason::WatchError,
        });
    }

    /// Non-blocking send onto the outgoing channel.
    ///
    /// Returns `true` when the caller should keep processing more
    /// events from the current batch, and `false` when the consumer
    /// has dropped the receiver (`Closed`). A `Full` channel is
    /// treated as an already-pending edge and silently coalesced —
    /// this pairs with the daemon's capacity-1, edge-triggered
    /// consumer, where an outstanding item already means "the repo
    /// is dirty, dispatch again". Callers that ignore the return
    /// value (e.g. [`Self::handle_watch_error_batch`]) accept that
    /// this one event is dropped when the consumer is gone.
    fn emit(&self, event: WatchEvent) -> bool {
        match self.tx.try_send(event) {
            Ok(()) => true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                debug!("coalesced watcher event into pending edge");
                true
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Publish fail-open filtering and queue a matcher rebuild.
    ///
    /// Watch callbacks never recursively walk the repository. Ignore-control,
    /// topology, and backend-error edges coalesce into the same indexed
    /// recovery record while an attempt is queued or running.
    fn reload_matcher(&self) {
        self.request_semantic_matcher_rebuild(true);
    }

    /// Request recovery of a failed-open matcher through the global fixed
    /// worker pool. Each classifier owns at most one physical queue record;
    /// requests during a running attempt coalesce into one successor.
    ///
    /// Two permanently stalled builds can starve later recovery work. Watchers
    /// remain fail-open in that state, and event delivery continues without
    /// ignore filtering.
    fn request_matcher_retry(&self) {
        self.request_semantic_matcher_rebuild(false);
    }

    fn request_semantic_matcher_rebuild(&self, install_fail_open: bool) {
        let overflowed = {
            let mut commit = self
                .matcher_target
                .commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if install_fail_open {
                let mut current = self
                    .ignore
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *current = Arc::new(RepoIgnoreMatcher::fail_open(&self.repo_root));
            }
            if let Some(next) = commit.desired_generation.checked_add(1) {
                commit.desired_generation = next;
                self.matcher_target
                    .desired_generation
                    .store(next, Ordering::SeqCst);
                false
            } else {
                true
            }
        };
        if overflowed {
            warn!("matcher invalidation generation exhausted; watcher remains fail-open");
            #[cfg(test)]
            self.matcher_target
                .generation_overflow_warning_count
                .fetch_add(1, Ordering::SeqCst);
            return;
        }
        self.matcher_target.scheduler.request(&self.matcher_target);
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

    /// True when `path` is a file that changes the ignore rules themselves:
    /// the resolved `.git/info/exclude`, the repository's common config, the
    /// per-worktree config, or any `.gitignore` file inside the working tree.
    /// A `.gitignore`
    /// found *inside* a git dir (e.g. `.git/.gitignore`) is
    /// deliberately not treated as an ignore-control file because
    /// [`Self::is_working_tree_path`] excludes the git dirs.
    fn is_ignore_control(&self, path: &Path) -> bool {
        path == self.git_metadata.info_exclude
            || self
                .git_metadata
                .ignore_controls
                .iter()
                .any(|item| item == path)
            || (self.is_working_tree_path(path)
                && path.file_name().is_some_and(|name| name == ".gitignore"))
    }

    /// True when `path` lies inside the working tree but outside any
    /// of the git dirs (the per-worktree gitdir plus the shared
    /// common gitdir for a linked worktree). Used to keep ignore
    /// classification and directory-topology detection scoped to
    /// user-visible files.
    fn is_working_tree_path(&self, path: &Path) -> bool {
        path.starts_with(self.repo_root.as_path())
            && !self
                .git_metadata
                .watch_roots()
                .iter()
                .any(|git_root| path.starts_with(git_root))
    }

    /// True when a working-tree path is owned by the fixed shared
    /// prune policy. A nested `.git` marker remains observable when
    /// its parent is not fixed-pruned because that marker changes
    /// repository topology. Inside an already-pruned parent, the
    /// marker inherits the parent's ownership and is dropped here.
    fn is_always_pruned_working_tree_path(&self, path: &Path) -> bool {
        if !self.is_working_tree_path(path) {
            return false;
        }
        let matcher = self
            .ignore
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let topology_owned_marker = is_nested_git_marker_path(&self.repo_root, path)
            && path
                .parent()
                .is_some_and(|parent| !matcher.is_always_pruned_path(parent));
        !topology_owned_marker && matcher.is_always_pruned_path(path)
    }

    /// True only for Ruby LSP's two composed-bundle inputs at the repo root.
    ///
    /// Those files are still excluded from scans and manifests with the rest
    /// of `.ruby-lsp`, but their edges must reach reconcile so a successful
    /// first run that creates them can refresh its scheduler config snapshot.
    /// The Ruby analyzer independently declares the same two config inputs.
    /// This lower-level crate cannot import that declaration without creating
    /// a dependency cycle, so changes must keep both private lists aligned.
    fn is_ruby_lsp_config_control_path(&self, path: &Path) -> bool {
        path.strip_prefix(&*self.repo_root).is_ok_and(|relative| {
            relative == Path::new(".ruby-lsp/Gemfile")
                || relative == Path::new(".ruby-lsp/Gemfile.lock")
        })
    }

    /// Reduce one raw notify event into a single [`WatchEvent`], or
    /// `None` when it should be silently dropped.
    ///
    /// Classification order (first match wins):
    ///
    /// 1. Path inside any git dir → [`classify_git`] (either a git
    ///    event, or `None` for internal-only paths like `objects/`).
    /// 2. Path inside an always-pruned subtree or a nested
    ///    repository boundary → dropped, except the exact Ruby
    ///    composed-bundle inputs.
    /// 3. Path is gitignored → dropped, with the same exact exception.
    /// 4. Otherwise, map the raw `EventKind` to a [`WatchEvent::File`]:
    ///    Create and Modify collapse to `FileChange::Touched` (a
    ///    Tier-1 reparse is identical either way), Remove becomes
    ///    `FileChange::Deleted`, and any other kind is dropped.
    fn classify(&self, path: &Path, kind: EventKind) -> Option<WatchEvent> {
        for git_root in self.git_metadata.watch_roots() {
            if path.starts_with(&git_root) {
                return classify_git(path, kind, &git_root);
            }
        }
        let ruby_lsp_config_control = self.is_ruby_lsp_config_control_path(path);
        if !ruby_lsp_config_control
            && self
                .ignore
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_pruned_path(path)
        {
            debug!(?path, "skip (pruned subtree)");
            return None;
        }
        if !ruby_lsp_config_control && self.is_gitignored(path, kind) {
            debug!(?path, "skip (gitignored)");
            return None;
        }
        match kind {
            EventKind::Create(_) | EventKind::Modify(_) => Some(WatchEvent::File {
                path: path.to_path_buf(),
                change: FileChange::Touched,
            }),
            EventKind::Remove(_) => Some(WatchEvent::File {
                path: path.to_path_buf(),
                change: FileChange::Deleted,
            }),
            _ => None,
        }
    }

    /// Ask the matcher whether `path` is ignored, filling in the
    /// `is_dir` hint from what the filesystem or `kind` tell us.
    /// The `kind`-derived fallback matters for `Remove(Folder)` and
    /// `Create(Folder)`, where a `path.is_dir()` probe against a
    /// just-deleted entry (or one whose creation has not settled)
    /// would misreport the type.
    fn is_gitignored(&self, path: &Path, kind: EventKind) -> bool {
        let is_dir = path.is_dir()
            || matches!(kind, EventKind::Create(CreateKind::Folder))
            || matches!(kind, EventKind::Remove(RemoveKind::Folder));
        self.ignore
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_ignored(path, is_dir)
    }
}

/// True when a raw notify event is a directory-topology change:
/// a new folder appeared, a folder was removed, or a rename
/// happened (rename is reported by notify as `Modify(Name(_))`).
/// These change the set of on-disk `.gitignore` files the matcher
/// needs to consider, so the classifier reloads on any of them.
fn is_directory_topology_change(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(CreateKind::Folder)
            | EventKind::Remove(RemoveKind::Folder)
            | EventKind::Modify(ModifyKind::Name(_))
    )
}

/// Classify a raw notify event that fired inside `git_dir` into a
/// [`GitEvent`] variant, or `None` for any path the watcher does
/// not care about (loose objects, `logs/`, hooks, config, etc.).
///
/// Parsing is structural on the path components rather than string
/// matching, so `refs/heads/feature/x` correctly reads as branch
/// `feature/x` and `.git/HEAD/anything` is not misread as `HEAD`.
/// Empty components inside a branch name (`refs/heads//x`) yield
/// `None` rather than an empty-string branch name.
fn classify_git(path: &Path, kind: EventKind, git_dir: &Path) -> Option<WatchEvent> {
    let rel = path.strip_prefix(git_dir).ok()?;
    let components: Vec<&std::ffi::OsStr> = rel.iter().collect();

    // .git/HEAD
    if components == [std::ffi::OsStr::new("HEAD")] {
        return matches!(kind, EventKind::Modify(_) | EventKind::Create(_))
            .then_some(WatchEvent::Git(GitEvent::HeadChanged));
    }

    // .git/packed-refs
    if components == [std::ffi::OsStr::new("packed-refs")] {
        return matches!(kind, EventKind::Modify(_) | EventKind::Create(_))
            .then_some(WatchEvent::Git(GitEvent::PackedRefsChanged));
    }

    // .git/refs/heads/<name>[/<sub...>]
    if components.len() >= 3 && components[0] == "refs" && components[1] == "heads" {
        let tail: Vec<&str> = components[2..].iter().filter_map(|c| c.to_str()).collect();
        if tail.iter().any(|s| s.is_empty()) {
            return None;
        }
        let name = tail.join("/");
        return match kind {
            EventKind::Remove(_) => Some(WatchEvent::Git(GitEvent::BranchDeleted { name })),
            EventKind::Create(_) | EventKind::Modify(_) => {
                Some(WatchEvent::Git(GitEvent::BranchTouched { name }))
            }
            _ => None,
        };
    }

    // .git/worktrees/<wt>/HEAD
    if components.len() == 3 && components[0] == "worktrees" && components[2] == "HEAD" {
        let wt = components[1].to_str()?.to_string();
        return matches!(kind, EventKind::Modify(_) | EventKind::Create(_)).then_some(
            WatchEvent::Git(GitEvent::WorktreeHeadChanged { worktree: wt }),
        );
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, Flag, ModifyKind, RemoveKind, RenameMode};

    fn git(s: &str) -> PathBuf {
        PathBuf::from("/r/.git").join(s)
    }

    #[test]
    fn head_modify_yields_head_changed() {
        let ev = classify_git(
            &git("HEAD"),
            EventKind::Modify(ModifyKind::Any),
            &PathBuf::from("/r/.git"),
        );
        assert_eq!(ev, Some(WatchEvent::Git(GitEvent::HeadChanged)));
    }

    #[test]
    fn branch_create_yields_branch_touched() {
        let ev = classify_git(
            &git("refs/heads/main"),
            EventKind::Create(CreateKind::File),
            &PathBuf::from("/r/.git"),
        );
        assert_eq!(
            ev,
            Some(WatchEvent::Git(GitEvent::BranchTouched {
                name: "main".into()
            }))
        );
    }

    #[test]
    fn branch_delete_yields_branch_deleted() {
        let ev = classify_git(
            &git("refs/heads/feature/x"),
            EventKind::Remove(RemoveKind::File),
            &PathBuf::from("/r/.git"),
        );
        assert_eq!(
            ev,
            Some(WatchEvent::Git(GitEvent::BranchDeleted {
                name: "feature/x".into()
            }))
        );
    }

    #[test]
    fn worktree_head_change() {
        let ev = classify_git(
            &git("worktrees/wt1/HEAD"),
            EventKind::Modify(ModifyKind::Any),
            &PathBuf::from("/r/.git"),
        );
        assert_eq!(
            ev,
            Some(WatchEvent::Git(GitEvent::WorktreeHeadChanged {
                worktree: "wt1".into()
            }))
        );
    }

    #[test]
    fn packed_refs_modify() {
        let ev = classify_git(
            &git("packed-refs"),
            EventKind::Modify(ModifyKind::Any),
            &PathBuf::from("/r/.git"),
        );
        assert_eq!(ev, Some(WatchEvent::Git(GitEvent::PackedRefsChanged)));
    }

    #[test]
    fn random_internal_path_ignored() {
        let ev = classify_git(
            &git("objects/ab/cdef"),
            EventKind::Modify(ModifyKind::Any),
            &PathBuf::from("/r/.git"),
        );
        assert_eq!(ev, None);
    }

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

        assert!(!EventClassifier::wait_on_matcher_rollback_hook(
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

    /// Wait for the first touched edge of a session where the FSEvents
    /// stream may still be settling. The probe file
    /// is re-written on a fixed interval so that even if the very first
    /// few writes land inside the stream's initial dead zone (observed
    /// on /private/tmp under sandboxed runners), a later write still
    /// triggers a delivered event. The total wait budget is `total`;
    /// each retry write happens every `retry_every`.
    async fn wait_for_probe_with_retries(
        rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
        probe: &std::path::Path,
        total: Duration,
        retry_every: Duration,
    ) -> Option<WatchEvent> {
        let probe_name = probe.file_name()?.to_os_string();
        let deadline = tokio::time::Instant::now() + total;
        let mut attempt: u32 = 0;
        // Initial write — content varies per attempt so the debouncer
        // cannot dedupe a later retry against the first one.
        std::fs::write(probe, format!("probe-{attempt}")).ok()?;
        let mut last_write = tokio::time::Instant::now();
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return None;
            }
            let until_retry = (last_write + retry_every).saturating_duration_since(now);
            let until_deadline = deadline.saturating_duration_since(now);
            let wait = until_retry.min(until_deadline);
            match tokio::time::timeout(wait, rx.recv()).await {
                Ok(Some(WatchEvent::File {
                    path,
                    change: FileChange::Touched,
                })) if path.file_name() == Some(probe_name.as_os_str()) => {
                    return Some(WatchEvent::File {
                        path,
                        change: FileChange::Touched,
                    });
                }
                Ok(Some(_)) => continue,
                Ok(None) => return None,
                Err(_) => {
                    attempt += 1;
                    std::fs::write(probe, format!("probe-{attempt}")).ok()?;
                    last_write = tokio::time::Instant::now();
                }
            }
        }
    }

    async fn write_until_file_edge(
        rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
        path: &Path,
        contents: &str,
    ) -> FileChange {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            std::fs::write(path, contents).unwrap();
            let retry_at = tokio::time::Instant::now() + Duration::from_millis(250);
            loop {
                tokio::select! {
                    event = rx.recv() => match event {
                        Some(WatchEvent::File { path: event_path, change })
                            if event_path == path => return change,
                        Some(_) => {}
                        None => panic!("watch channel closed before Ruby config edge"),
                    },
                    () = tokio::time::sleep_until(retry_at) => break,
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Ruby config edge timed out for {}",
                path.display()
            );
        }
    }

    async fn assert_ruby_lsp_config_edges_for_backend(backend: WatchBackend) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join(".ruby-lsp/cache")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let handle =
            watch_repo_with_backend(&root, Duration::from_millis(50), tx, backend).unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        while rx.try_recv().is_ok() {}

        for relative in [".ruby-lsp/Gemfile", ".ruby-lsp/Gemfile.lock"] {
            let config_path = root.join(relative);
            for (contents, expected_change) in [
                (Some("first config snapshot\n"), FileChange::Touched),
                (Some("second config snapshot\n"), FileChange::Touched),
                (None, FileChange::Deleted),
            ] {
                while rx.try_recv().is_ok() {}
                if let Some(contents) = contents {
                    assert_eq!(
                        write_until_file_edge(&mut rx, &config_path, contents).await,
                        expected_change
                    );
                } else {
                    std::fs::remove_file(&config_path).unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            match rx.recv().await {
                                Some(WatchEvent::File { path, .. }) if path == config_path => break,
                                Some(_) => {}
                                None => panic!("watch channel closed before Ruby config edge"),
                            }
                        }
                    })
                    .await
                    .unwrap_or_else(|_| panic!("Ruby config delete edge timed out for {relative}"));
                }
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
        }

        while rx.try_recv().is_ok() {}
        std::fs::write(root.join(".ruby-lsp/cache/index"), "generated\n").unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            rx.try_recv().is_err(),
            "generated Ruby LSP artifacts must remain silent"
        );
        drop(handle);
    }

    #[test]
    fn classifier_skips_always_pruned_subtrees() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let classifier = classifier_for(root);
        for dir in ["target", "node_modules", ".claude", ".ruby-lsp"] {
            let path = root.join(dir).join("nested").join("file.rs");
            let ev = classifier.classify(&path, EventKind::Modify(notify::event::ModifyKind::Any));
            assert_eq!(ev, None, "expected {dir} subtree to be pruned");
        }
        // A regular file is not pruned.
        let path = root.join("src").join("lib.rs");
        let ev = classifier.classify(&path, EventKind::Modify(notify::event::ModifyKind::Any));
        assert!(matches!(ev, Some(WatchEvent::File { .. })));
    }

    #[test]
    fn ruby_lsp_composed_bundle_inputs_trigger_reconcile_without_exposing_artifacts() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".ruby-lsp")).unwrap();
        std::fs::write(root.join(".ruby-lsp/.gitignore"), "*\n").unwrap();
        let classifier = classifier_for(root);

        for relative in [".ruby-lsp/Gemfile", ".ruby-lsp/Gemfile.lock"] {
            let path = root.join(relative);
            for kind in [
                EventKind::Create(CreateKind::File),
                EventKind::Modify(ModifyKind::Any),
                EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
                EventKind::Remove(RemoveKind::File),
            ] {
                assert!(
                    matches!(
                        classifier.classify(&path, kind),
                        Some(WatchEvent::File { path: event_path, .. }) if event_path == path
                    ),
                    "composed-bundle input {relative} must request reconcile"
                );
            }
        }

        for relative in [
            ".ruby-lsp/freshness_hash",
            ".ruby-lsp/needs_update",
            ".ruby-lsp/bundle_is_composed",
            ".ruby-lsp/cache/index",
            ".ruby-lsp/server.log",
            ".ruby-lsp/nested/Gemfile",
            ".ruby-lsp/Gemfile.bak",
            ".ruby-lsp/Gemfile.lock.tmp",
        ] {
            let path = root.join(relative);
            assert_eq!(
                classifier.classify(&path, EventKind::Modify(ModifyKind::Any)),
                None,
                "generated Ruby LSP state must stay pruned: {relative}"
            );
        }
    }

    #[tokio::test]
    async fn ruby_lsp_config_batch_bypasses_prune_without_rescanning_artifacts() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".ruby-lsp/cache")).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let gemfile = root.join(".ruby-lsp/Gemfile");
        let artifact = root.join(".ruby-lsp/cache/index");

        classifier.handle_batch(&[
            debounced(
                notify::Event::new(EventKind::Create(CreateKind::File)).add_path(gemfile.clone()),
            ),
            debounced(
                notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(artifact.clone()),
            ),
        ]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("exact Ruby config batch edge timed out"),
            Some(WatchEvent::File {
                path: gemfile.clone(),
                change: FileChange::Touched,
            })
        );
        assert!(rx.try_recv().is_err(), "one exact input must emit one edge");

        classifier.handle_batch(&[debounced(
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(artifact),
        )]);
        assert!(
            rx.try_recv().is_err(),
            "artifact-only batches must remain silent"
        );

        classifier.handle_batch(&[debounced(
            notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Any)))
                .add_path(gemfile.clone()),
        )]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("Ruby config rename edge timed out"),
            Some(WatchEvent::File {
                path: gemfile,
                change: FileChange::Touched,
            })
        );
    }

    #[test]
    fn classifier_handles_claude_worktrees_layout() {
        // Concretely the Claude harness creates
        // .claude/worktrees/<id>/<full-repo-checkout>, which would
        // otherwise cause the entire repo to be re-indexed once per
        // sub-agent worktree.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let classifier = classifier_for(root);
        let nested = root
            .join(".claude")
            .join("worktrees")
            .join("agent-7")
            .join("crates")
            .join("foo")
            .join("src")
            .join("lib.rs");
        let ev = classifier.classify(&nested, EventKind::Modify(notify::event::ModifyKind::Any));
        assert_eq!(ev, None);
    }

    #[test]
    fn classifier_prunes_nested_git_boundaries_but_not_codex_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let git_dir_checkout = root.join("vendor/dir-checkout");
        std::fs::create_dir_all(git_dir_checkout.join(".git")).unwrap();
        let git_file_checkout = root.join(".codex/worktrees/w1/file-checkout");
        std::fs::create_dir_all(&git_file_checkout).unwrap();
        std::fs::write(git_file_checkout.join(".git"), "gitdir: elsewhere\n").unwrap();

        let classifier = classifier_for(root);
        for source in [
            git_dir_checkout.join("src/lib.rs"),
            git_file_checkout.join("src/lib.rs"),
        ] {
            assert_eq!(
                classifier.classify(&source, EventKind::Modify(ModifyKind::Any)),
                None
            );
        }
        assert!(matches!(
            classifier.classify(
                &root.join(".codex/settings.json"),
                EventKind::Modify(ModifyKind::Any)
            ),
            Some(WatchEvent::File { .. })
        ));
    }

    #[test]
    fn gitignored_file_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), "ignored.txt\n").unwrap();
        let classifier = classifier_for(tmp.path());
        let target = tmp.path().join("ignored.txt");
        assert!(classifier.is_gitignored(&target, EventKind::Modify(ModifyKind::Any)));
        let other = tmp.path().join("kept.txt");
        assert!(!classifier.is_gitignored(&other, EventKind::Modify(ModifyKind::Any)));
    }

    #[test]
    fn classifier_drops_gradle_generated_child() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), ".gradle\n").unwrap();
        let classifier = classifier_for(tmp.path());
        let generated = tmp
            .path()
            .join("gradle/plugins/.gradle/caches/junit/generated.bin");

        assert_eq!(
            classifier.classify(&generated, EventKind::Create(CreateKind::File)),
            None
        );
    }

    #[test]
    fn ignore_control_is_detected_before_parent_ignore_filter() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), ".gitignore\n").unwrap();
        let classifier = classifier_for(tmp.path());

        assert!(classifier.is_ignore_control(&tmp.path().join(".gitignore")));
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
    async fn backend_rescan_and_directory_topology_changes_force_full_reconcile() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        let backend_rescan = notify::Event::new(EventKind::Other)
            .set_flag(Flag::Rescan)
            .add_path(root.to_path_buf());
        classifier.handle_batch(&[debounced(backend_rescan)]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::BackendRequested
            })
        );

        let directory_create = notify::Event::new(EventKind::Create(CreateKind::Folder))
            .add_path(root.join("generated"));
        classifier.handle_batch(&[debounced(directory_create)]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("topology transport reason timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );
        let rename = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(root.join("old"))
            .add_path(root.join("new"));
        classifier.handle_batch(&[debounced(rename)]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );
    }

    #[tokio::test]
    async fn ruby_lsp_topology_and_ignore_events_are_pruned_before_rescan_checks() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let ruby_lsp = root.join(".ruby-lsp");

        for event in [
            notify::Event::new(EventKind::Create(CreateKind::Folder)).add_path(ruby_lsp.clone()),
            notify::Event::new(EventKind::Modify(ModifyKind::Any))
                .add_path(ruby_lsp.join("nested/.gitignore")),
            notify::Event::new(EventKind::Create(CreateKind::File))
                .add_path(ruby_lsp.join("nested/.git")),
            notify::Event::new(EventKind::Remove(RemoveKind::File))
                .add_path(ruby_lsp.join("nested/.git")),
            notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
                .add_path(ruby_lsp.join("old"))
                .add_path(ruby_lsp.join("new")),
            notify::Event::new(EventKind::Remove(RemoveKind::Folder))
                .add_path(ruby_lsp.join("nested")),
        ] {
            classifier.handle_batch(&[debounced(event)]);
        }

        assert!(
            rx.try_recv().is_err(),
            "events wholly inside an always-pruned subtree must not emit File or Rescan edges"
        );
    }

    #[tokio::test]
    async fn nested_git_marker_create_and_remove_force_topology_rescan() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let nested = root.join("tools/checkout");
        std::fs::create_dir_all(&nested).unwrap();
        let marker = nested.join(".git");
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        std::fs::write(&marker, "gitdir: elsewhere\n").unwrap();
        let create =
            notify::Event::new(EventKind::Create(CreateKind::File)).add_path(marker.clone());
        classifier.handle_batch(&[debounced(create)]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .ok()
                .flatten(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );

        std::fs::remove_file(&marker).unwrap();
        let remove =
            notify::Event::new(EventKind::Remove(RemoveKind::File)).add_path(marker.clone());
        classifier.handle_batch(&[debounced(remove)]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .ok()
                .flatten(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );

        std::fs::create_dir(&marker).unwrap();
        let create_directory =
            notify::Event::new(EventKind::Create(CreateKind::Folder)).add_path(marker.clone());
        classifier.handle_batch(&[debounced(create_directory)]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .ok()
                .flatten(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );

        std::fs::remove_dir(&marker).unwrap();
        let remove_directory =
            notify::Event::new(EventKind::Remove(RemoveKind::Folder)).add_path(marker);
        classifier.handle_batch(&[debounced(remove_directory)]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .ok()
                .flatten(),
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );
    }

    #[tokio::test]
    async fn nested_checkout_directory_rename_reloads_boundary_policy() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let old = root.join("old-checkout");
        let new = root.join("new-checkout");
        std::fs::create_dir_all(old.join(".git")).unwrap();
        std::fs::write(old.join("lib.rs"), "").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        std::fs::rename(&old, &new).unwrap();
        let rename = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(old)
            .add_path(new.clone());
        classifier.handle_batch(&[debounced(rename)]);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("directory topology transport reason timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .expect("directory topology matcher recovery timed out"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        assert_eq!(
            classifier.classify(&new.join("lib.rs"), EventKind::Modify(ModifyKind::Any)),
            None
        );
    }

    #[tokio::test]
    async fn git_internal_topology_change_does_not_force_full_reconcile() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".git/objects")).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        let object_directory = notify::Event::new(EventKind::Create(CreateKind::Folder))
            .add_path(root.join(".git/objects/ab"));
        classifier.handle_batch(&[debounced(object_directory)]);

        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "git object topology must not trigger a working-tree rescan"
        );

        let info_exclude = root.join(".git/info/exclude");
        std::fs::create_dir_all(info_exclude.parent().unwrap()).unwrap();
        std::fs::write(&info_exclude, "generated.rs\n").unwrap();
        let exclude_event =
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(info_exclude);
        classifier.handle_batch(&[debounced(exclude_event)]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            }),
            "the watched info/exclude file remains an explicit git-metadata exception"
        );
    }

    #[tokio::test]
    async fn notify_error_batch_reloads_and_rescans_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        handle_debounce_result(
            &classifier,
            Err(vec![
                notify::Error::generic("first injected watcher error"),
                notify::Error::generic("second injected watcher error"),
            ]),
        );

        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::WatchError
            })
        );
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "one notify error batch must emit exactly one rescan edge"
        );
    }

    #[test]
    fn malformed_git_file_keeps_watcher_fail_open() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join(".git"), "not-a-gitdir-file\n").unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        let watcher =
            watch_repo_with_backend(root, Duration::from_millis(50), tx, WatchBackend::Poll);

        if let Err(err) = watcher {
            panic!("malformed .git metadata must not leave the repository unwatched: {err}");
        }
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

    #[tokio::test]
    async fn end_to_end_file_event() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        // Initialize a fake repo so the .git watch path exists.
        std::fs::create_dir_all(root.join(".git")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let _handle =
            watch_repo_with_backend(&root, Duration::from_millis(150), tx, WatchBackend::Poll)
                .unwrap();

        let probe = root.join(".probe");
        // Use the polling backend here because macOS tempdir-backed
        // native watchers can fail to deliver any callback in this
        // isolated unit-test shape, even though production daemon
        // probes use the default recommended backend.
        let probe_event = wait_for_probe_with_retries(
            &mut rx,
            &probe,
            Duration::from_secs(10),
            Duration::from_millis(500),
        )
        .await;
        assert!(
            probe_event.is_some(),
            "watcher delivered no Touched event for .probe within 10s of retries"
        );
    }

    #[tokio::test]
    async fn polling_watcher_suppresses_ruby_lsp_subtree_but_keeps_ordinary_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join(".ruby-lsp/nested")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let handle =
            watch_repo_with_backend(&root, Duration::from_millis(50), tx, WatchBackend::Poll)
                .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        while rx.try_recv().is_ok() {}

        std::fs::write(root.join(".ruby-lsp/.gitignore"), "*\n").unwrap();
        std::fs::write(root.join(".ruby-lsp/nested/Gemfile.lock"), "generated\n").unwrap();
        std::fs::write(root.join(".ruby-lsp/nested/.git"), "gitdir: elsewhere\n").unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let leaked = rx.try_recv();
        assert!(
            matches!(leaked, Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "polling backend leaked a File or Rescan edge from .ruby-lsp: {leaked:?}"
        );

        std::fs::remove_file(root.join(".ruby-lsp/nested/.git")).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let leaked = rx.try_recv();
        assert!(
            matches!(leaked, Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "polling backend leaked a File or Rescan edge from removed .ruby-lsp marker: {leaked:?}"
        );

        let probe = root.join("ordinary.rs");
        let ordinary = wait_for_probe_with_retries(
            &mut rx,
            &probe,
            Duration::from_secs(5),
            Duration::from_millis(250),
        )
        .await;
        assert!(
            matches!(
                ordinary,
                Some(WatchEvent::File { path, .. })
                    if path.file_name() == probe.file_name()
            ),
            "ordinary working-tree files must remain observable"
        );
        drop(handle);
    }

    #[tokio::test]
    async fn polling_watcher_observes_exact_ruby_lsp_config_controls() {
        assert_ruby_lsp_config_edges_for_backend(WatchBackend::Poll).await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn native_watcher_observes_exact_ruby_lsp_config_controls() {
        assert_ruby_lsp_config_edges_for_backend(WatchBackend::Recommended).await;
    }
}
