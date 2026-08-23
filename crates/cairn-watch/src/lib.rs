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

mod backend;
mod classifier;
use classifier::EventClassifier;
mod matcher;
pub use backend::{
    WatchBackend, WatcherHandle, watch_repo, watch_repo_with_backend,
    watch_repo_with_startup_deferred_matcher,
};
mod matcher_recovery;
pub mod scan;

use std::path::PathBuf;
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
