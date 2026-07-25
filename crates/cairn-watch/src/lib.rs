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
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// A previously broken ignore matcher rebuilt successfully.
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
/// Gitignore filtering uses the same hierarchical matcher as the
/// startup scanner: repository-local `.gitignore` files plus the
/// repository's `.git/info/exclude`.
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
/// `retrying_matcher` is a simple flag that serializes matcher
/// rebuild attempts after a fail-open — see
/// [`Self::start_matcher_retry`]. `tx` is the outbound event
/// channel; its `Closed` state is the only shutdown signal this
/// classifier reacts to.
#[derive(Clone)]
struct EventClassifier {
    repo_root: Arc<PathBuf>,
    git_metadata: Arc<GitMetadataPaths>,
    ignore: Arc<RwLock<Arc<RepoIgnoreMatcher>>>,
    retrying_matcher: Arc<AtomicBool>,
    tx: Sender<WatchEvent>,
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
            retrying_matcher: Arc::new(AtomicBool::new(false)),
            tx,
        };
        if initial_failed {
            classifier.start_matcher_retry();
        }
        classifier
    }

    /// Classify one debounced batch.
    ///
    /// Every event in the batch is tested in order; the batch stops
    /// at the first event that produces a rescan reason, using this
    /// per-event precedence:
    ///
    /// 1. The backend requested a rescan (`need_rescan()` flag) —
    ///    [`RescanReason::BackendRequested`].
    /// 2. Any path is an ignore-control file (info/exclude or a
    ///    working-tree `.gitignore`) — [`RescanReason::IgnoreRulesChanged`].
    /// 3. A create/remove/rename touches a nested `.git` marker in
    ///    the working tree — [`RescanReason::DirectoryTopologyChanged`].
    /// 4. Any working-tree directory create/remove/rename — same
    ///    [`RescanReason::DirectoryTopologyChanged`].
    ///
    /// When any rescan reason fires, the matcher is reloaded and
    /// exactly one `Rescan` event is emitted — the per-event
    /// classification loop is skipped, since the consumer will
    /// re-read the whole snapshot anyway.
    fn handle_batch(&self, events: &[notify_debouncer_full::DebouncedEvent]) {
        let reason = events.iter().find_map(|event| {
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
                self.is_working_tree_path(path) && is_nested_git_marker_path(&self.repo_root, path)
            }) {
                return Some(RescanReason::DirectoryTopologyChanged);
            }
            (is_directory_topology_change(event.kind)
                && event
                    .paths
                    .iter()
                    .any(|path| self.is_working_tree_path(path)))
            .then_some(RescanReason::DirectoryTopologyChanged)
        });
        if let Some(reason) = reason {
            self.reload_matcher();
            self.emit(WatchEvent::Rescan { reason });
            return;
        }

        for ev in events {
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
    /// emit exactly one [`RescanReason::WatchError`] edge. The
    /// individual error messages are logged by
    /// [`handle_debounce_result`] before this is called.
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

    /// Rebuild the ignore matcher from the on-disk `.gitignore` set
    /// and swap it into place. On failure the classifier installs a
    /// permissive [`RepoIgnoreMatcher::fail_open`] so events keep
    /// flowing, and hands off to [`Self::start_matcher_retry`] so a
    /// later rebuild can restore normal filtering — the fail-open
    /// path is not the terminal state.
    fn reload_matcher(&self) {
        match RepoIgnoreMatcher::build(&self.repo_root, &self.git_metadata.info_exclude) {
            Ok(matcher) => self.replace_matcher(matcher),
            Err(err) => {
                warn!(error = %err, "ignore matcher reload failed; watcher is fail-open");
                self.install_fail_open();
                self.start_matcher_retry();
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

    /// Start a background thread that keeps trying to rebuild a
    /// real matcher after a fail-open.
    ///
    /// At most one retry thread runs at a time — `retrying_matcher`
    /// gates the spawn via `compare_exchange` and is cleared once
    /// the thread exits. The retry uses exponential backoff
    /// starting at 100ms and capped at 2s, and terminates when
    /// either (a) the outgoing channel closed (the consumer went
    /// away), or (b) a rebuild succeeded, in which case a
    /// [`RescanReason::MatcherRecovered`] edge is emitted so the
    /// consumer knows to re-check anything it observed while the
    /// classifier was over-broad. Uses `std::thread` so no tokio
    /// runtime handle needs to reach into the classifier's setup.
    fn start_matcher_retry(&self) {
        if self
            .retrying_matcher
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let classifier = self.clone();
        std::thread::spawn(move || {
            let mut delay = Duration::from_millis(100);
            loop {
                if classifier.tx.is_closed() {
                    break;
                }
                std::thread::sleep(delay);
                match RepoIgnoreMatcher::build(
                    &classifier.repo_root,
                    &classifier.git_metadata.info_exclude,
                ) {
                    Ok(matcher) => {
                        classifier.replace_matcher(matcher);
                        classifier.emit(WatchEvent::Rescan {
                            reason: RescanReason::MatcherRecovered,
                        });
                        break;
                    }
                    Err(err) => {
                        warn!(error = %err, "ignore matcher retry failed");
                        delay = (delay * 2).min(Duration::from_secs(2));
                    }
                }
            }
            classifier.retrying_matcher.store(false, Ordering::SeqCst);
        });
    }

    /// True when `path` is a file that changes the ignore rules
    /// themselves: the resolved `.git/info/exclude`, or any
    /// `.gitignore` file inside the working tree. A `.gitignore`
    /// found *inside* a git dir (e.g. `.git/.gitignore`) is
    /// deliberately not treated as an ignore-control file because
    /// [`Self::is_working_tree_path`] excludes the git dirs.
    fn is_ignore_control(&self, path: &Path) -> bool {
        path == self.git_metadata.info_exclude
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

    /// Reduce one raw notify event into a single [`WatchEvent`], or
    /// `None` when it should be silently dropped.
    ///
    /// Classification order (first match wins):
    ///
    /// 1. Path inside any git dir → [`classify_git`] (either a git
    ///    event, or `None` for internal-only paths like `objects/`).
    /// 2. Path inside an always-pruned subtree or a nested
    ///    repository boundary → dropped.
    /// 3. Path is gitignored → dropped.
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
        if self
            .ignore
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_pruned_path(path)
        {
            debug!(?path, "skip (pruned subtree)");
            return None;
        }
        if self.is_gitignored(path, kind) {
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

    #[test]
    fn classifier_skips_always_pruned_subtrees() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let classifier = classifier_for(root);
        for dir in ["target", "node_modules", ".claude"] {
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
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );

        std::fs::remove_file(&marker).unwrap();
        let remove =
            notify::Event::new(EventKind::Remove(RemoveKind::File)).add_path(marker.clone());
        classifier.handle_batch(&[debounced(remove)]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );

        std::fs::create_dir(&marker).unwrap();
        let create_directory =
            notify::Event::new(EventKind::Create(CreateKind::Folder)).add_path(marker.clone());
        classifier.handle_batch(&[debounced(create_directory)]);
        assert_eq!(
            rx.recv().await,
            Some(WatchEvent::Rescan {
                reason: RescanReason::DirectoryTopologyChanged
            })
        );

        std::fs::remove_dir(&marker).unwrap();
        let remove_directory =
            notify::Event::new(EventKind::Remove(RemoveKind::Folder)).add_path(marker);
        classifier.handle_batch(&[debounced(remove_directory)]);
        assert_eq!(
            rx.recv().await,
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
}
