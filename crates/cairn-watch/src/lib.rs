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

pub mod scan;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ignore::gitignore::Gitignore;
use notify::{EventKind, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer};
use tokio::sync::mpsc::UnboundedSender;
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
    debouncer: Debouncer<RecommendedWatcher, RecommendedCache>,
}

/// Begin watching `repo_root` recursively. Events are debounced over
/// `debounce` and pushed on `tx`. The returned handle must be kept
/// alive; dropping it stops the watcher.
///
/// gitignore filtering is best-effort using the repo's root
/// `.gitignore`. Per-directory `.gitignore` files are not yet
/// honored (planned for 0.2.0); the consumer is expected to filter
/// noisy paths anyway.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo(
    repo_root: &Path,
    debounce: Duration,
    tx: UnboundedSender<WatchEvent>,
) -> Result<WatcherHandle, WatchError> {
    let repo_root = repo_root.canonicalize()?;
    let git_dir = repo_root.join(".git");
    let ignore = load_root_gitignore(&repo_root);

    let classifier = EventClassifier {
        repo_root: Arc::new(repo_root.clone()),
        git_dir: Arc::new(git_dir.clone()),
        ignore: Arc::new(ignore),
        tx,
    };

    let mut debouncer =
        new_debouncer(
            debounce,
            None,
            move |result: DebounceEventResult| match result {
                Ok(events) => classifier.handle_batch(&events),
                Err(errs) => {
                    for e in errs {
                        warn!(?e, "notify error");
                    }
                }
            },
        )?;
    debouncer.watch(&repo_root, RecursiveMode::Recursive)?;
    // Watch .git separately even though it's under repo_root: this
    // means we still see ref events when the consumer asks us to
    // ignore the working tree (future flag).
    if git_dir.is_dir() {
        let _ = debouncer.watch(&git_dir, RecursiveMode::Recursive);
    }
    Ok(WatcherHandle { debouncer })
}

fn load_root_gitignore(repo_root: &Path) -> Gitignore {
    let mut builder = ignore::gitignore::GitignoreBuilder::new(repo_root);
    let candidate = repo_root.join(".gitignore");
    if candidate.exists() {
        if let Some(err) = builder.add(&candidate) {
            warn!(error = %err, path = %candidate.display(), "failed to load .gitignore");
        }
    }
    builder.build().unwrap_or_else(|err| {
        warn!(error = %err, "gitignore build failed, falling back to empty matcher");
        Gitignore::empty()
    })
}

#[derive(Clone)]
struct EventClassifier {
    repo_root: Arc<PathBuf>,
    git_dir: Arc<PathBuf>,
    ignore: Arc<Gitignore>,
    tx: UnboundedSender<WatchEvent>,
}

impl EventClassifier {
    fn handle_batch(&self, events: &[notify_debouncer_full::DebouncedEvent]) {
        for ev in events {
            for path in &ev.paths {
                if let Some(out) = self.classify(path, ev.kind) {
                    if self.tx.send(out).is_err() {
                        // Receiver dropped — no point continuing.
                        return;
                    }
                }
            }
        }
    }

    fn classify(&self, path: &Path, kind: EventKind) -> Option<WatchEvent> {
        if path.starts_with(self.git_dir.as_path()) {
            return classify_git(path, kind, &self.git_dir);
        }
        if self.is_in_pruned_subtree(path) {
            debug!(?path, "skip (always-pruned subtree)");
            return None;
        }
        if self.is_gitignored(path) {
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

    /// True if any path component falls under an
    /// [`scan::ALWAYS_PRUNED_DIR_NAMES`] entry. Sharing the list with
    /// the scan walker keeps the two surfaces consistent: an event
    /// the scanner would skip should never reach the indexer either.
    fn is_in_pruned_subtree(&self, path: &Path) -> bool {
        let Ok(rel) = path.strip_prefix(self.repo_root.as_path()) else {
            return false;
        };
        rel.components().any(|c| {
            c.as_os_str()
                .to_str()
                .is_some_and(|n| crate::scan::ALWAYS_PRUNED_DIR_NAMES.contains(&n))
        })
    }

    fn is_gitignored(&self, path: &Path) -> bool {
        // Gitignore::matched expects paths relative to the root.
        let Ok(rel) = path.strip_prefix(self.repo_root.as_path()) else {
            return false;
        };
        self.ignore.matched(rel, path.is_dir()).is_ignore()
    }
}

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
    use notify::event::{CreateKind, ModifyKind, RemoveKind};
    use std::ffi::OsStr;

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
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        EventClassifier {
            repo_root: Arc::new(root.to_path_buf()),
            git_dir: Arc::new(root.join(".git")),
            ignore: Arc::new(load_root_gitignore(root)),
            tx,
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
    fn gitignored_file_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), "ignored.txt\n").unwrap();
        let ignore = load_root_gitignore(tmp.path());
        // Note: `matched` needs paths relative to the root.
        let target = PathBuf::from("ignored.txt");
        assert!(ignore.matched(&target, false).is_ignore());
        let other = PathBuf::from("kept.txt");
        assert!(!ignore.matched(&other, false).is_ignore());
    }

    #[tokio::test]
    async fn end_to_end_file_event() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        // Initialize a fake repo so the .git watch path exists.
        std::fs::create_dir_all(root.join(".git")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _handle = watch_repo(&root, Duration::from_millis(150), tx).unwrap();

        // Give the watcher a moment to settle before we trigger events.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let target = root.join("hello.rs");
        std::fs::write(&target, "fn main() {}").unwrap();

        // Wait up to 3 seconds for a debounced event to surface.
        let timeout = Duration::from_secs(3);
        let event = tokio::time::timeout(timeout, async {
            while let Some(ev) = rx.recv().await {
                if let WatchEvent::File {
                    path,
                    change: FileChange::Touched,
                } = &ev
                {
                    if path.file_name() == Some(OsStr::new("hello.rs")) {
                        return Some(ev);
                    }
                }
            }
            None
        })
        .await
        .ok()
        .flatten();

        assert!(
            event.is_some(),
            "did not observe a Touched event for hello.rs"
        );
    }
}
