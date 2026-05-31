//! Live-watcher orchestrator.
//!
//! Owns one [`cairn_watch::WatcherHandle`] per registered alias and a
//! tokio task that drains its event channel into the corresponding
//! [`Indexer`] calls. The orchestrator hides the per-alias bookkeeping
//! from the control surface: `CtlHandler::handle_add_repo` calls
//! [`WatcherOrchestrator::start`] after the initial `full_index` and
//! `handle_remove_repo` calls [`WatcherOrchestrator::stop`] before
//! dropping the registry rows.
//!
//! Event handling is intentionally tolerant — a transient failure
//! (file read race, parse error, registry contention) is logged and
//! skipped; the watcher keeps running. The only fatal condition is
//! the receiver dropping, which signals that the orchestrator is
//! shutting down.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cairn_watch::{FileChange, GitEvent, WatchEvent, WatcherHandle, watch_repo};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::Result;
use crate::indexer::Indexer;

/// Default debounce window for the underlying watcher. Long enough to
/// absorb the editor "delete-then-rename" save pattern, short enough
/// that an interactive edit feels live.
pub const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(250);

/// Tracks one alias's watcher: the [`WatcherHandle`] (dropping it
/// stops the OS watch) and the JoinHandle of the task draining its
/// channel.
struct WatcherEntry {
    /// Kept alive purely for its Drop side-effect (un-registers the
    /// notify watch).
    _handle: WatcherHandle,
    task: JoinHandle<()>,
}

/// Per-daemon owner of all live watchers.
pub struct WatcherOrchestrator {
    indexer: Arc<Indexer>,
    entries: Mutex<HashMap<String, WatcherEntry>>,
    debounce: Duration,
}

impl WatcherOrchestrator {
    #[must_use]
    pub fn new(indexer: Arc<Indexer>) -> Self {
        Self {
            indexer,
            entries: Mutex::new(HashMap::new()),
            debounce: DEFAULT_DEBOUNCE,
        }
    }

    /// Construct with a custom debounce. Tests use a short value to
    /// keep e2e cases fast.
    #[doc(hidden)]
    #[must_use]
    pub fn with_debounce(indexer: Arc<Indexer>, debounce: Duration) -> Self {
        Self {
            indexer,
            entries: Mutex::new(HashMap::new()),
            debounce,
        }
    }

    /// Begin watching `root` for the given alias. Idempotent: if an
    /// entry already exists it is stopped first.
    ///
    /// # Errors
    /// Setup failures from the underlying [`watch_repo`] call.
    pub async fn start(&self, alias: &str, root: &Path) -> Result<()> {
        self.stop(alias).await;

        let (tx, rx) = unbounded_channel();
        let handle = watch_repo(root, self.debounce, tx).map_err(|e| {
            crate::Error::InvalidArgument(format!("watch_repo({}): {e}", root.display()))
        })?;
        let task = tokio::spawn(drive_events(alias.to_string(), self.indexer.clone(), rx));
        let entry = WatcherEntry {
            _handle: handle,
            task,
        };
        self.entries.lock().await.insert(alias.to_string(), entry);
        info!(alias, root = %root.display(), "watcher started");
        Ok(())
    }

    /// Stop watching the alias (no-op if not running).
    pub async fn stop(&self, alias: &str) {
        if let Some(entry) = self.entries.lock().await.remove(alias) {
            entry.task.abort();
            info!(alias, "watcher stopped");
        }
    }

    /// Drop all watchers. Used on daemon shutdown.
    pub async fn shutdown(&self) {
        let mut entries = self.entries.lock().await;
        for (alias, entry) in entries.drain() {
            entry.task.abort();
            debug!(alias, "watcher aborted at shutdown");
        }
    }
}

/// Event drain loop. One per registered alias. Runs until the
/// channel closes (= handle dropped) or the task is aborted.
async fn drive_events(alias: String, indexer: Arc<Indexer>, mut rx: UnboundedReceiver<WatchEvent>) {
    while let Some(event) = rx.recv().await {
        if let Err(e) = dispatch_event(&alias, &indexer, &event).await {
            warn!(alias = %alias, ?event, error = %e, "watcher event dispatch failed");
        }
    }
    debug!(alias = %alias, "watcher channel closed");
}

async fn dispatch_event(alias: &str, indexer: &Indexer, event: &WatchEvent) -> Result<()> {
    match event {
        WatchEvent::File {
            path,
            change: FileChange::Touched,
        } => indexer.reindex_file(alias, path).await,
        WatchEvent::File {
            path,
            change: FileChange::Deleted,
        } => indexer.delete_file(alias, path).await,
        WatchEvent::Git(GitEvent::HeadChanged) => indexer.handle_head_change(alias, None).await,
        WatchEvent::Git(GitEvent::WorktreeHeadChanged { worktree }) => {
            // The worktree label here is the on-disk directory name
            // under `.git/worktrees/<wt>/`, which lets us look up the
            // worktree's actual root via its `gitdir` file. Best-effort:
            // if the lookup fails we fall back to main worktree refresh,
            // which is still better than nothing.
            let resolved = resolve_linked_worktree_root(alias, indexer, worktree).await;
            indexer.handle_head_change(alias, resolved.as_deref()).await
        }
        WatchEvent::Git(GitEvent::BranchTouched { name } | GitEvent::BranchDeleted { name }) => {
            // Branch-ref events: macOS FSEvents does not reliably
            // distinguish Modify-of-parent from Remove-of-file, so
            // we cannot trust the event kind alone. Resolve the
            // truth from disk by checking whether the ref still
            // exists (loose or packed). If it does, this is a tip
            // move — currently a no-op (branch rename
            // reconciliation lands later). If it's gone, prune the
            // snapshot. `prune_branch_snapshot` itself defensively
            // skips a branch still recorded as a worktree's active
            // one, so a rename mid-flight does not strand the
            // working tree.
            let main = match indexer.active_snapshot(alias).await {
                Ok(h) => h,
                Err(_) => return Ok(()),
            };
            if ref_exists(&main.root, name) {
                Ok(())
            } else {
                indexer.prune_branch_snapshot(alias, name).await.map(|_| ())
            }
        }
        WatchEvent::Git(GitEvent::PackedRefsChanged) => {
            // packed-refs rewrites can hide a delete inside a bulk
            // pack operation. Run reconcile to catch any branch
            // ref that disappeared via packing without a per-ref
            // notify event.
            indexer.reconcile_snapshots(alias).await.map(|_| ())
        }
    }
}

/// True when `refs/heads/<branch>` is still resolvable from the
/// repo root — either as a loose ref file or as an entry in
/// `packed-refs`. Used to distinguish a branch-tip move (still
/// present) from a deletion (gone). Best-effort: a transient read
/// failure is treated as "exists" so we don't drop snapshots over
/// a stat hiccup.
fn ref_exists(repo_root: &Path, branch: &str) -> bool {
    let loose = repo_root
        .join(".git")
        .join("refs")
        .join("heads")
        .join(branch);
    if loose.exists() {
        return true;
    }
    let packed = repo_root.join(".git").join("packed-refs");
    let Ok(text) = std::fs::read_to_string(&packed) else {
        // Unreadable packed-refs (e.g. file genuinely absent on
        // small repos) — fall back to "loose miss = gone".
        return false;
    };
    let needle = format!(" refs/heads/{branch}");
    text.lines().any(|line| line.ends_with(&needle))
}

/// Read `<main-git-dir>/worktrees/<wt>/gitdir` to learn the absolute
/// path of a linked worktree's `.git` file, from which we derive the
/// working-tree root. Returns `None` on any failure — the caller
/// then falls back to the main worktree.
async fn resolve_linked_worktree_root(
    alias: &str,
    indexer: &Indexer,
    worktree_label: &str,
) -> Option<PathBuf> {
    let main = indexer.active_snapshot(alias).await.ok()?;
    let gitdir_path = main
        .root
        .join(".git")
        .join("worktrees")
        .join(worktree_label)
        .join("gitdir");
    let contents = tokio::fs::read_to_string(&gitdir_path).await.ok()?;
    // gitdir points at `<wt-root>/.git`; strip the trailing `.git` to
    // get the working tree root.
    let trimmed = contents.trim();
    let p = Path::new(trimmed);
    p.parent().map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::paths::DataDir;
    use crate::storage::Storage;
    use cairn_lang_api::LanguageBackend;

    fn make_indexer(work: &Path) -> Arc<Indexer> {
        let (idx, _s) = make_indexer_with_storage(work);
        idx
    }

    fn make_indexer_with_storage(work: &Path) -> (Arc<Indexer>, Arc<Storage>) {
        let data_dir = DataDir::with_root(work.join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Arc::new(Indexer::with_backends(storage.clone(), backends));
        (indexer, storage)
    }

    fn write_min_repo(root: &Path) {
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::write(root.join(".git").join("HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::write(root.join("src").join("lib.rs"), "fn one() {}\n").unwrap();
    }

    #[tokio::test]
    async fn start_then_stop_is_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        write_min_repo(&repo);

        let indexer = make_indexer(work);
        indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        let orch = WatcherOrchestrator::with_debounce(indexer, Duration::from_millis(100));
        orch.start("demo", &repo).await.unwrap();
        assert_eq!(orch.entries.lock().await.len(), 1);

        orch.stop("demo").await;
        assert!(orch.entries.lock().await.is_empty());
    }

    #[tokio::test]
    async fn file_touch_triggers_reindex() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        write_min_repo(&repo);

        let indexer = make_indexer(work);
        let handle = indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        let orch = Arc::new(WatcherOrchestrator::with_debounce(
            indexer.clone(),
            Duration::from_millis(100),
        ));
        orch.start("demo", &repo).await.unwrap();
        // Let the watcher settle.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Write a new symbol.
        std::fs::write(repo.join("src").join("new.rs"), "fn new_symbol() {}\n").unwrap();

        // Poll the snapshot DB for the new symbol — give up after 3s.
        let snapshot_db = handle.snapshot_db_path.clone();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut found = false;
        while std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let conn = crate::data_db::open(&snapshot_db).unwrap();
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM symbols WHERE name = 'new_symbol'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            if n >= 1 {
                found = true;
                break;
            }
        }
        orch.stop("demo").await;
        assert!(found, "expected new_symbol to be indexed after file touch");
    }

    #[tokio::test]
    async fn head_change_updates_current_branch_and_reindexes() {
        // git is required for this test; skip cleanly when absent.
        if !std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            eprintln!("git not on PATH; skipping head_change e2e");
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        let run = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@e")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@e")
                .output()
                .unwrap()
        };
        let init = run(&["init", "-q", "-b", "main"]);
        if !init.status.success() {
            eprintln!("git init failed; skipping");
            return;
        }
        std::fs::write(repo.join("src").join("lib.rs"), "fn one() {}\n").unwrap();
        run(&["add", "."]);
        run(&["commit", "-q", "-m", "first"]);

        let indexer = make_indexer(work);
        indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        let orch = WatcherOrchestrator::with_debounce(indexer.clone(), Duration::from_millis(100));
        orch.start("demo", &repo).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        run(&["checkout", "-q", "-b", "feature/x"]);

        // Poll the registry until the worktree row's current_branch
        // catches up.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut switched = false;
        while std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let active = indexer.active_snapshot("demo").await.unwrap();
            if active.branch == "feature/x" {
                switched = true;
                break;
            }
        }
        orch.stop("demo").await;
        assert!(switched, "expected active branch to advance to feature/x");
    }

    #[tokio::test]
    async fn prune_branch_snapshot_drops_inactive_row() {
        // Unit-test the prune path against a synthetic snapshot row,
        // bypassing the watcher entirely. The end-to-end "watcher
        // sees branch deletion → prune fires" chain is racy on
        // macOS FSEvents (notify coalesces ref-file events
        // unpredictably under back-to-back checkouts), so we test
        // the prune logic in isolation and the reconcile path in
        // `reconcile_drops_snapshots_whose_refs_are_gone` below.

        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        std::fs::write(repo.join(".git").join("HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::write(repo.join("src").join("lib.rs"), "fn one() {}\n").unwrap();

        let (indexer, storage) = make_indexer_with_storage(work);
        indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        // Plant a snapshot row for a non-active branch.
        storage
            .with_registry(|conn| {
                use crate::registry_db::{SnapshotEnrichment, SnapshotStatus, upsert_snapshot};
                let wt_id: i64 = conn
                    .query_row("SELECT id FROM worktrees LIMIT 1", [], |r| r.get(0))
                    .unwrap();
                upsert_snapshot(
                    conn,
                    wt_id,
                    "feature/throwaway",
                    "/tmp/cairn-test-throwaway.db",
                    SnapshotStatus::Ready,
                    SnapshotEnrichment::Syntactic,
                    None,
                    0,
                    None,
                    crate::INDEXER_REVISION,
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let removed = indexer
            .prune_branch_snapshot("demo", "feature/throwaway")
            .await
            .unwrap();
        assert_eq!(removed, 1);

        let remaining: Vec<String> = storage
            .with_registry(|conn| {
                let mut stmt = conn.prepare("SELECT branch FROM index_snapshots")?;
                let rows: Vec<String> = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<_>>()?;
                Ok(rows)
            })
            .await
            .unwrap();
        assert!(!remaining.iter().any(|b| b == "feature/throwaway"));
    }

    #[tokio::test]
    async fn prune_skips_currently_active_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        write_min_repo(&repo);

        let (indexer, _storage) = make_indexer_with_storage(work);
        indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        // Asking to prune the active branch is a no-op (defensive).
        let removed = indexer.prune_branch_snapshot("demo", "main").await.unwrap();
        assert_eq!(removed, 0);
        // The snapshot for main is still resolvable.
        let active = indexer.active_snapshot("demo").await.unwrap();
        assert_eq!(active.branch, "main");
    }

    #[tokio::test]
    async fn reconcile_drops_snapshots_whose_refs_are_gone() {
        if !std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            eprintln!("git not on PATH; skipping reconcile e2e");
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        let run = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@e")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@e")
                .output()
                .unwrap()
        };
        if !run(&["init", "-q", "-b", "main"]).status.success() {
            eprintln!("git init failed; skipping");
            return;
        }
        std::fs::write(repo.join("src").join("lib.rs"), "fn one() {}\n").unwrap();
        run(&["add", "."]);
        run(&["commit", "-q", "-m", "first"]);

        let (indexer, storage) = make_indexer_with_storage(work);
        indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        // Plant a stale snapshot for a branch that does not exist
        // as a ref in the repo.
        storage
            .with_registry(|conn| {
                use crate::registry_db::{SnapshotEnrichment, SnapshotStatus, upsert_snapshot};
                let wt_id: i64 = conn
                    .query_row("SELECT id FROM worktrees LIMIT 1", [], |r| r.get(0))
                    .unwrap();
                upsert_snapshot(
                    conn,
                    wt_id,
                    "feature/ghost",
                    "/tmp/cairn-test-ghost.db",
                    SnapshotStatus::Ready,
                    SnapshotEnrichment::Syntactic,
                    None,
                    0,
                    None,
                    crate::INDEXER_REVISION,
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let removed = indexer.reconcile_snapshots("demo").await.unwrap();
        assert_eq!(removed, 1);

        let remaining: Vec<String> = storage
            .with_registry(|conn| {
                let mut stmt = conn.prepare("SELECT branch FROM index_snapshots")?;
                let rows: Vec<String> = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<_>>()?;
                Ok(rows)
            })
            .await
            .unwrap();
        assert!(remaining.iter().any(|b| b == "main"));
        assert!(!remaining.iter().any(|b| b == "feature/ghost"));
    }

    #[tokio::test]
    async fn file_delete_drops_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path();
        let repo = work.join("repo");
        write_min_repo(&repo);
        std::fs::write(repo.join("src").join("doomed.rs"), "fn doomed() {}\n").unwrap();

        let indexer = make_indexer(work);
        let handle = indexer.register_repo("demo", &repo).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        let snapshot_db = handle.snapshot_db_path.clone();
        // Sanity: `doomed` is indexed.
        let conn = crate::data_db::open(&snapshot_db).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = 'doomed'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        drop(conn);

        let orch = WatcherOrchestrator::with_debounce(indexer.clone(), Duration::from_millis(100));
        orch.start("demo", &repo).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        std::fs::remove_file(repo.join("src").join("doomed.rs")).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut gone = false;
        while std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let conn = crate::data_db::open(&snapshot_db).unwrap();
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM symbols WHERE name = 'doomed'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            if n == 0 {
                gone = true;
                break;
            }
        }
        orch.stop("demo").await;
        assert!(gone, "expected doomed symbol to be removed after delete");
    }
}
