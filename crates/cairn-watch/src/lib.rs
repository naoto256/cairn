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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
    /// A previously broken ignore matcher rebuilt successfully. The watcher
    /// was fail-open until then, so paths it let through may be ones the
    /// restored rules ignore.
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
    let repo_root = repo_root.canonicalize()?;
    let git_metadata = resolve_git_metadata(&repo_root).unwrap_or_else(|err| {
        warn!(
            root = %repo_root.display(),
            error = %err,
            "git metadata resolution failed; watcher is fail-open"
        );
        GitMetadataPaths::fail_open(&repo_root)
    });
    let classifier = EventClassifier::new(&repo_root, git_metadata.clone(), tx);

    let event_handler = move |result: DebounceEventResult| {
        handle_debounce_result(&classifier, result);
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
/// debouncer's callback and the matcher-retry thread both need
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
    matcher_retry_state: Arc<AtomicU8>,
    #[cfg(test)]
    retry_attempt_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_exit_hook: Arc<std::sync::Mutex<Option<MatcherRetryHook>>>,
    #[cfg(test)]
    retry_spawn_failure: Arc<std::sync::Mutex<Option<std::io::ErrorKind>>>,
    #[cfg(test)]
    retry_spawn_warning_count: Arc<std::sync::atomic::AtomicUsize>,
    tx: Sender<WatchEvent>,
}

const MATCHER_RETRY_IDLE: u8 = 0;
const MATCHER_RETRY_RUNNING: u8 = 1;
const MATCHER_RETRY_RUNNING_REQUESTED: u8 = 2;

#[cfg(test)]
#[derive(Clone)]
struct MatcherRetryHook {
    reached: std::sync::mpsc::SyncSender<()>,
    proceed: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl EventClassifier {
    fn new(repo_root: &Path, git_metadata: GitMetadataPaths, tx: Sender<WatchEvent>) -> Self {
        let (initial, initial_failed) =
            match RepoIgnoreMatcher::build(repo_root, &git_metadata.info_exclude) {
                Ok(matcher) => (matcher, false),
                Err(err) => {
                    warn!(error = %err, "ignore matcher build failed; watcher is fail-open");
                    (RepoIgnoreMatcher::fail_open(repo_root), true)
                }
            };
        let classifier = Self {
            repo_root: Arc::new(repo_root.to_path_buf()),
            git_metadata: Arc::new(git_metadata),
            ignore: Arc::new(RwLock::new(Arc::new(initial))),
            matcher_retry_state: Arc::new(AtomicU8::new(MATCHER_RETRY_IDLE)),
            #[cfg(test)]
            retry_attempt_hook: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            retry_exit_hook: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            retry_spawn_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            retry_spawn_warning_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            tx,
        };
        if initial_failed {
            classifier.request_matcher_retry();
        }
        classifier
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

    /// Rebuild the ignore matcher from the effective Git ignore snapshot and
    /// swap it into place. Included config and selected excludes files are
    /// sampled here but are not live watcher roots, wherever they live; a root
    /// config or `config.worktree` event, reload, reindex, or restart observes
    /// their latest contents. On
    /// failure the classifier installs a
    /// permissive [`RepoIgnoreMatcher::fail_open`] so events keep
    /// flowing, and hands off to [`Self::request_matcher_retry`] so a
    /// later rebuild can restore normal filtering — the fail-open
    /// path is not the terminal state.
    fn reload_matcher(&self) {
        match RepoIgnoreMatcher::build(&self.repo_root, &self.git_metadata.info_exclude) {
            Ok(matcher) => self.replace_matcher(matcher),
            Err(err) => {
                warn!(error = %err, "ignore matcher reload failed; watcher is fail-open");
                self.install_fail_open();
                self.request_matcher_retry();
            }
        }
    }

    /// Replace the current matcher with a permissive
    /// [`RepoIgnoreMatcher::fail_open`]. Nested-boundary and
    /// always-pruned pruning still works after this — only the
    /// gitignore rules go quiet.
    fn install_fail_open(&self) {
        self.replace_matcher(RepoIgnoreMatcher::fail_open(&self.repo_root));
    }

    /// Atomically swap the shared matcher pointer. A reader that
    /// is mid-matcher-call finishes it against the old matcher;
    /// the next read-lock acquire in the same classify path may
    /// observe the new one. A poisoned lock is unwrapped in place
    /// because writes here are unconditional pointer replacements.
    fn replace_matcher(&self, matcher: RepoIgnoreMatcher) {
        let mut current = self
            .ignore
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *current = Arc::new(matcher);
    }

    /// Request recovery of a failed-open matcher.
    ///
    /// One owner performs rebuild attempts. Requests arriving while it
    /// runs coalesce in `MATCHER_RETRY_RUNNING_REQUESTED`; the owner
    /// consumes requests present before an attempt and atomically hands a
    /// request that arrived during its successful attempt to the next
    /// attempt. This keeps ownership and pending work in one state word,
    /// without a clear-then-check window. The retry uses exponential
    /// backoff starting at 100ms and capped at 2s. A successful rebuild
    /// emits [`RescanReason::MatcherRecovered`]. A closed channel ends
    /// recovery: no new owner starts, and the active owner restores the idle
    /// state itself, so a request left pending at shutdown cannot strand
    /// ownership. Uses `std::thread` so no tokio runtime handle needs to
    /// reach into classifier setup.
    fn request_matcher_retry(&self) {
        if self.tx.is_closed() {
            return;
        }
        loop {
            match self.matcher_retry_state.load(Ordering::SeqCst) {
                MATCHER_RETRY_IDLE => {
                    if self
                        .matcher_retry_state
                        .compare_exchange(
                            MATCHER_RETRY_IDLE,
                            MATCHER_RETRY_RUNNING,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                MATCHER_RETRY_RUNNING => {
                    if self
                        .matcher_retry_state
                        .compare_exchange(
                            MATCHER_RETRY_RUNNING,
                            MATCHER_RETRY_RUNNING_REQUESTED,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                MATCHER_RETRY_RUNNING_REQUESTED => return,
                state => unreachable!("invalid matcher retry state: {state}"),
            }
        }
        let classifier = self.clone();
        let owner = move || {
            let mut delay = Duration::from_millis(100);
            'retry: loop {
                if classifier.tx.is_closed() {
                    classifier
                        .matcher_retry_state
                        .store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                    return;
                }
                std::thread::sleep(delay);
                #[cfg(test)]
                Self::wait_on_matcher_retry_hook(&classifier.retry_attempt_hook);
                let _ = classifier.matcher_retry_state.compare_exchange(
                    MATCHER_RETRY_RUNNING_REQUESTED,
                    MATCHER_RETRY_RUNNING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                match RepoIgnoreMatcher::build(
                    &classifier.repo_root,
                    &classifier.git_metadata.info_exclude,
                ) {
                    Ok(matcher) => {
                        classifier.replace_matcher(matcher);
                        classifier.emit(WatchEvent::Rescan {
                            reason: RescanReason::MatcherRecovered,
                        });
                        #[cfg(test)]
                        Self::wait_on_matcher_retry_hook(&classifier.retry_exit_hook);
                        loop {
                            if classifier.tx.is_closed() {
                                classifier
                                    .matcher_retry_state
                                    .store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
                                return;
                            }
                            match classifier.matcher_retry_state.compare_exchange(
                                MATCHER_RETRY_RUNNING,
                                MATCHER_RETRY_IDLE,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => return,
                                Err(MATCHER_RETRY_RUNNING_REQUESTED) => {
                                    if classifier
                                        .matcher_retry_state
                                        .compare_exchange(
                                            MATCHER_RETRY_RUNNING_REQUESTED,
                                            MATCHER_RETRY_RUNNING,
                                            Ordering::SeqCst,
                                            Ordering::SeqCst,
                                        )
                                        .is_ok()
                                    {
                                        delay = Duration::from_millis(100);
                                        continue 'retry;
                                    }
                                }
                                Err(MATCHER_RETRY_IDLE) => return,
                                Err(state) => {
                                    unreachable!("invalid matcher retry state: {state}")
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "ignore matcher retry failed");
                        delay = (delay * 2).min(Duration::from_secs(2));
                    }
                }
            }
        };
        #[cfg(test)]
        let spawned = match self
            .retry_spawn_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            Some(kind) => Err(std::io::Error::new(kind, "injected retry spawn failure")),
            None => std::thread::Builder::new().spawn(owner),
        };
        #[cfg(not(test))]
        let spawned = std::thread::Builder::new().spawn(owner);
        if let Err(err) = spawned {
            self.release_matcher_retry_after_spawn_failure(&err);
        }
    }

    /// Release retry ownership when the recovery thread could not be spawned.
    ///
    /// Nothing was rebuilt, so the matcher stays fail-open and no
    /// [`RescanReason::MatcherRecovered`] is emitted; the failure is recorded
    /// once in server-side diagnostics. Ownership returns to idle rather than
    /// retrying in place, because a spawn failure may reflect thread-resource
    /// exhaustion and an immediate respawn could hot-spin. A request coalesced
    /// into the state word is dropped with it; a later reload either recovers
    /// synchronously or, if it still fails, can claim a fresh retry owner.
    fn release_matcher_retry_after_spawn_failure(&self, err: &std::io::Error) {
        warn!(error = %err, "matcher retry thread spawn failed; releasing retry ownership");
        #[cfg(test)]
        self.retry_spawn_warning_count
            .fetch_add(1, Ordering::SeqCst);
        self.matcher_retry_state
            .store(MATCHER_RETRY_IDLE, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn wait_on_matcher_retry_hook(hook_slot: &std::sync::Mutex<Option<MatcherRetryHook>>) {
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
    async fn ignore_change_reloads_matcher_and_emits_one_rescan_edge() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let ignored = root.join("sub/ignored.rs");
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_some()
        );

        std::fs::write(root.join("sub/.gitignore"), "ignored.rs\n").unwrap();
        let event = notify::Event::new(EventKind::Modify(ModifyKind::Any))
            .add_path(root.join("sub/.gitignore"));
        classifier.handle_batch(&[debounced(event)]);

        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            })
        );
        assert!(
            classifier
                .classify(&ignored, EventKind::Modify(ModifyKind::Any))
                .is_none()
        );
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::IgnoreRulesChanged
            })
        );
        assert!(!classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));
    }

    /// Included config and selected excludes files are not watcher roots,
    /// wherever they live. Their current contents are sampled at matcher
    /// construction and become visible after a root config or
    /// `config.worktree` event, reload, reindex, or restart.
    #[test]
    fn external_core_excludes_changes_only_on_explicit_reload() {
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
        let classifier = classifier_for(&root);
        let a = root.join("a.rs");
        let b = root.join("b.rs");
        assert!(classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(!classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));

        std::fs::write(&external, "/b.rs\n").unwrap();
        assert!(classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(!classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));

        classifier.reload_matcher();
        assert!(!classifier.is_gitignored(&a, EventKind::Modify(ModifyKind::Any)));
        assert!(classifier.is_gitignored(&b, EventKind::Modify(ModifyKind::Any)));
    }

    #[tokio::test]
    async fn backend_rescan_and_directory_topology_changes_force_full_reconcile() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
            rx.recv().await,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        std::fs::rename(&old, &new).unwrap();
        let rename = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(old)
            .add_path(new.clone());
        classifier.handle_batch(&[debounced(rename)]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
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
    async fn matcher_retry_records_request_during_failed_attempt_backoff() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        let (reached_rx, proceed_tx) = install_matcher_retry_hook(&classifier.retry_attempt_hook);

        std::fs::write(&ignore_file, [0xff]).unwrap();
        classifier.reload_matcher();
        reached_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("retry owner did not reach the controlled attempt");
        classifier.reload_matcher();
        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_RUNNING_REQUESTED
        );
        std::fs::write(&ignore_file, "recovered.rs\n").unwrap();
        proceed_tx.send(()).unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("retry owner did not recover"),
            Some(WatchEvent::Rescan {
                reason: RescanReason::MatcherRecovered
            })
        );
        wait_for_matcher_retry_idle(&classifier).await;
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
    async fn closed_foreground_request_does_not_clear_active_retry_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let ignore_file = root.join(".gitignore");
        std::fs::write(&ignore_file, "initial.rs\n").unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
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
        assert_ne!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE,
            "a non-owner request must not clear active ownership"
        );

        proceed_tx.send(()).unwrap();
        wait_for_matcher_retry_idle(&classifier).await;
    }

    #[tokio::test]
    async fn successful_foreground_reload_does_not_arm_matcher_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);

        classifier.reload_matcher();

        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE
        );
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn matcher_retry_spawn_failure_releases_owner_for_later_request() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let classifier = EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx);
        *classifier
            .retry_spawn_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::io::ErrorKind::Other);

        classifier.request_matcher_retry();

        assert_eq!(
            classifier.matcher_retry_state.load(Ordering::SeqCst),
            MATCHER_RETRY_IDLE,
            "a failed spawn must release retry ownership"
        );
        assert_eq!(
            classifier.retry_spawn_warning_count.load(Ordering::SeqCst),
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
            classifier.retry_spawn_warning_count.load(Ordering::SeqCst),
            1
        );
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
