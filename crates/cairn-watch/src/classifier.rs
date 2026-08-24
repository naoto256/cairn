//! Classification of debounced filesystem and Git metadata events.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use crate::matcher::resolve_git_metadata;
use crate::matcher::{GitMetadataPaths, RepoIgnoreMatcher, is_nested_git_marker_path};
use crate::matcher_recovery::*;
use crate::{FileChange, GitEvent, RescanReason, WatchEvent};
use notify::EventKind;
use notify::event::{CreateKind, ModifyKind, RemoveKind};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

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
pub(super) struct EventClassifier {
    pub(super) repo_root: Arc<PathBuf>,
    pub(super) git_metadata: Arc<GitMetadataPaths>,
    pub(super) ignore: Arc<RwLock<Arc<RepoIgnoreMatcher>>>,
    #[cfg(test)]
    pub(super) matcher_retry_state: Arc<AtomicU8>,
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
    pub(super) tx: Sender<WatchEvent>,
    pub(super) matcher_target: Arc<MatcherBuildTarget>,
}

impl EventClassifier {
    pub(super) fn with_matcher(
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

    pub(super) fn with_matcher_and_scheduler(
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

    pub(super) fn new(
        repo_root: &Path,
        git_metadata: GitMetadataPaths,
        tx: Sender<WatchEvent>,
    ) -> Self {
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
    pub(super) fn new_deferred(
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
    pub(super) fn new_deferred_with_scheduler(
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
    pub(super) fn begin_deferred_matcher_warmup(&self) {
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
    pub(super) fn handle_batch(&self, events: &[notify_debouncer_full::DebouncedEvent]) {
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
    pub(super) fn handle_watch_error_batch(&self) {
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
    pub(super) fn emit(&self, event: WatchEvent) -> bool {
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
    pub(super) fn reload_matcher(&self) {
        self.request_semantic_matcher_rebuild(true);
    }

    /// Request recovery of a failed-open matcher through the global fixed
    /// worker pool. Each classifier owns at most one physical queue record;
    /// requests during a running attempt coalesce into one successor.
    ///
    /// Two permanently stalled builds can starve later recovery work. Watchers
    /// remain fail-open in that state, and event delivery continues without
    /// ignore filtering.
    pub(super) fn request_matcher_retry(&self) {
        self.request_semantic_matcher_rebuild(false);
    }

    pub(super) fn request_semantic_matcher_rebuild(&self, install_fail_open: bool) {
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

    /// True when `path` is a file that changes the ignore rules themselves:
    /// the resolved `.git/info/exclude`, the repository's common config, the
    /// per-worktree config, or any `.gitignore` file inside the working tree.
    /// A `.gitignore`
    /// found *inside* a git dir (e.g. `.git/.gitignore`) is
    /// deliberately not treated as an ignore-control file because
    /// [`Self::is_working_tree_path`] excludes the git dirs.
    pub(super) fn is_ignore_control(&self, path: &Path) -> bool {
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
    pub(super) fn is_working_tree_path(&self, path: &Path) -> bool {
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
    pub(super) fn is_always_pruned_working_tree_path(&self, path: &Path) -> bool {
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
    pub(super) fn is_ruby_lsp_config_control_path(&self, path: &Path) -> bool {
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
    pub(super) fn classify(&self, path: &Path, kind: EventKind) -> Option<WatchEvent> {
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
    pub(super) fn is_gitignored(&self, path: &Path, kind: EventKind) -> bool {
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
        let tail: Vec<&str> = components[2..]
            .iter()
            .map(|component| component.to_str())
            .collect::<Option<_>>()?;
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
    use crate::backend::handle_debounce_result;
    use crate::{WatchBackend, watch_repo_with_backend};
    use notify::event::{CreateKind, Flag, ModifyKind, RemoveKind, RenameMode};

    fn git(s: &str) -> PathBuf {
        PathBuf::from("/r/.git").join(s)
    }

    fn classifier_for(root: &Path) -> EventClassifier {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        EventClassifier::new(root, resolve_git_metadata(root).unwrap(), tx)
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

    #[cfg(unix)]
    #[test]
    fn non_utf8_branch_component_is_ignored() {
        use std::os::unix::ffi::OsStringExt as _;

        let git_dir = PathBuf::from("/r/.git");
        let path = git_dir
            .join("refs/heads")
            .join(std::ffi::OsString::from_vec(vec![0xff]))
            .join("x");

        assert_eq!(
            classify_git(&path, EventKind::Create(CreateKind::File), &git_dir),
            None
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

    fn debounced(event: notify::Event) -> notify_debouncer_full::DebouncedEvent {
        notify_debouncer_full::DebouncedEvent::new(event, std::time::Instant::now())
    }

    async fn recv_rescan_reason(
        rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
        expected: RescanReason,
    ) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match rx.recv().await {
                    Some(WatchEvent::Rescan { reason }) if reason == expected => return,
                    Some(WatchEvent::Rescan {
                        reason: RescanReason::MatcherRecovered,
                    }) => {}
                    other => panic!("unexpected watcher event: {other:?}"),
                }
            }
        })
        .await
        .expect("expected rescan reason timed out");
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
        recv_rescan_reason(&mut rx, RescanReason::DirectoryTopologyChanged).await;
        let rename = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(root.join("old"))
            .add_path(root.join("new"));
        classifier.handle_batch(&[debounced(rename)]);
        recv_rescan_reason(&mut rx, RescanReason::DirectoryTopologyChanged).await;
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
}
