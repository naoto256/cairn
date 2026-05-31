//! Repository indexer.
//!
//! Walks a registered repository, dispatches each file to its
//! [`LanguageBackend`], and inserts the resulting [`SymbolFact`]s into
//! the matching `(worktree, branch)` data DB.
//!
//! Since 0.2.0 a registered repo can carry multiple worktrees
//! (discovered through `git worktree list`) and each worktree
//! maintains one snapshot per branch it has ever occupied.
//! [`Indexer::register_repo`] creates the registry rows and
//! synchronously builds the primary worktree's active-branch
//! snapshot; secondary snapshots are marked `Building` and are
//! populated by [`Indexer::full_index`] (covers every snapshot
//! associated with the alias) or by the watcher-driven incremental
//! path.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cairn_lang_api::{LanguageBackend, SymbolFact, SymbolKind, Visibility};
use cairn_watch::scan;
use rusqlite::{Connection, params};
use tracing::{debug, info, warn};

use crate::paths::{DataDir, path_hash};
use crate::registry_db::{self, SnapshotEnrichment, SnapshotStatus};
use crate::storage::Storage;
use crate::{Error, INDEXER_REVISION, Result};

/// One worktree discovered for a repository — either the main
/// checkout or a linked worktree created with `git worktree add`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredWorktree {
    /// Absolute, canonicalized path to the worktree on disk.
    pub path: PathBuf,
    /// Branch name (`main`, `feature/x`) or
    /// `detached@<short-sha>` for a detached HEAD.
    pub branch: String,
    /// Current commit SHA, when readable. May be `None` for a
    /// freshly-created worktree before its HEAD is set.
    pub head_sha: Option<String>,
}

/// What the indexer produces after a (re)index. Useful for `ctl
/// status` and for tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub files_indexed: usize,
    pub files_skipped: usize,
    pub symbols_inserted: usize,
    /// Count of Tier-2 semantic facts the optional analyzer
    /// produced across the snapshot — imports, doc overrides,
    /// impl edges, and ref sites. Used to decide whether the
    /// snapshot's enrichment level is `Syntactic` (Tier-1 only)
    /// or `Semantic` (Tier-1 + Tier-2).
    pub semantic_facts: usize,
}

pub struct Indexer {
    storage: Arc<Storage>,
    backends: Arc<Vec<Box<dyn LanguageBackend>>>,
}

impl Indexer {
    /// Construct an indexer that uses every backend present in the
    /// linkme-registered slice (typically: tree-sitter Rust + Python
    /// at 0.1.0).
    #[must_use]
    pub fn with_registered_backends(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            backends: Arc::new(cairn_lang_api::all_backends()),
        }
    }

    /// Test-only constructor: inject an explicit backend list.
    #[doc(hidden)]
    #[must_use]
    pub fn with_backends(storage: Arc<Storage>, backends: Vec<Box<dyn LanguageBackend>>) -> Self {
        Self {
            storage,
            backends: Arc::new(backends),
        }
    }

    /// Register a repository. Discovers every git worktree under the
    /// given root (the main checkout plus any `git worktree add`-ed
    /// linked worktrees), creates the registry rows, and seeds an
    /// empty `Building`-status snapshot for each worktree's currently
    /// checked-out branch. Idempotent on alias when called with the
    /// same path.
    ///
    /// The actual file content does not get indexed here — call
    /// [`Self::full_index`] (which now spans every snapshot owned by
    /// the alias) to populate them.
    ///
    /// # Errors
    /// Path canonicalization, registry insert, and DB-open failures.
    pub async fn register_repo(&self, alias: &str, root: &Path) -> Result<RegisteredRepo> {
        let root = root
            .canonicalize()
            .map_err(|e| Error::InvalidArgument(format!("{}: {e}", root.display())))?;
        let alias = alias.to_string();
        let now_ns = now_ns();
        let repo_hash = path_hash(&root);
        let data_dir = self.storage.data_dir.clone();

        // 1. Enumerate every worktree on disk. The main checkout
        //    always shows up as the first entry; secondary worktrees
        //    follow.
        let worktrees = detect_worktrees(&root);

        // 2. Insert registry rows in a single transaction. The first
        //    entry (= main worktree) drives the RegisteredRepo handle
        //    returned to the caller; subsequent worktrees create rows
        //    but their snapshots are populated lazily.
        let (repo_id, primary) = self
            .storage
            .with_registry({
                let alias = alias.clone();
                let root_str = root.to_string_lossy().to_string();
                let repo_hash = repo_hash.clone();
                let data_dir = data_dir.clone();
                let worktrees = worktrees.clone();
                move |conn| {
                    let repo_id = match registry_db::find_repo_by_alias(conn, &alias)? {
                        Some(r) if r.root_path == root_str => r.id,
                        Some(r) => {
                            return Err(Error::InvalidArgument(format!(
                                "alias `{alias}` already points to {}",
                                r.root_path
                            )));
                        }
                        None => {
                            registry_db::insert_repo(conn, &alias, &root_str, &repo_hash, now_ns)?
                        }
                    };

                    let mut primary: Option<RegisteredRepo> = None;
                    for wt in &worktrees {
                        let wt_path = wt.path.to_string_lossy().to_string();
                        let wt_hash = path_hash(&wt.path);
                        let worktree_id = registry_db::upsert_worktree(
                            conn,
                            repo_id,
                            &wt_path,
                            &wt_hash,
                            Some(&wt.branch),
                            wt.head_sha.as_deref(),
                        )?;
                        let snapshot_db_path =
                            data_dir.snapshot_db_path(&repo_hash, &wt_hash, &wt.branch);
                        let snapshot_id = registry_db::upsert_snapshot(
                            conn,
                            worktree_id,
                            &wt.branch,
                            &snapshot_db_path.to_string_lossy(),
                            SnapshotStatus::Building,
                            SnapshotEnrichment::Syntactic,
                            None,
                            now_ns,
                            None,
                            INDEXER_REVISION,
                        )?;
                        if wt.path == root && primary.is_none() {
                            primary = Some(RegisteredRepo {
                                alias: alias.clone(),
                                root: wt.path.clone(),
                                branch: wt.branch.clone(),
                                repo_id,
                                worktree_id,
                                snapshot_id,
                                snapshot_db_path,
                            });
                        }
                    }

                    let primary = primary.ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "register: no worktree matched the supplied root `{root_str}`"
                        ))
                    })?;
                    Ok((repo_id, primary))
                }
            })
            .await?;

        // 3. Touch each snapshot DB so the file exists with the
        //    schema applied. This makes `open` idempotent for callers
        //    that want to attach the snapshot before any data lands.
        for wt in &worktrees {
            let wt_hash = path_hash(&wt.path);
            let path = data_dir.snapshot_db_path(&repo_hash, &wt_hash, &wt.branch);
            let _ = self.storage.open_data_db(path).await?;
        }

        debug!(
            alias = %primary.alias,
            worktree_count = worktrees.len(),
            primary_branch = %primary.branch,
            "registered"
        );
        let _ = repo_id;
        Ok(primary)
    }

    /// Walk every worktree owned by `alias`, parse every supported
    /// file, and rewrite each snapshot DB from scratch. Returns the
    /// aggregate stats across all snapshots.
    ///
    /// The caller-facing primary stats (used to populate the
    /// `IndexResult` MCP payload) reflect the sum across worktrees,
    /// keyed by the alias's primary branch — the branch checked out
    /// in the main worktree.
    ///
    /// # Errors
    /// Filesystem, parse, or SQL failures of any snapshot DB.
    pub async fn full_index(&self, alias: &str) -> Result<IndexStats> {
        let plan = self.index_plan_for(alias).await?;
        let backends = self.backends.clone();

        // Each (worktree, branch) snapshot is indexed sequentially on
        // a blocking thread. We keep the indexer single-threaded for
        // now; introducing per-snapshot parallelism is a separate
        // optimisation that interacts with the writer-pool model.
        //
        // We track per-entry stats (rather than just the aggregate)
        // because each snapshot's enrichment level depends on
        // whether *its* analyzer produced facts — summing across
        // snapshots would lose that resolution.
        let plan_for_blocking = plan.clone();
        let per_entry: Vec<IndexStats> = tokio::task::spawn_blocking(move || {
            let mut out = Vec::with_capacity(plan_for_blocking.len());
            for entry in &plan_for_blocking {
                let stats =
                    index_repo_sync(&entry.root, &entry.snapshot_db_path, backends.as_slice())?;
                out.push(stats);
            }
            Ok::<_, Error>(out)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("indexer task panicked: {e}")))??;

        let total = per_entry
            .iter()
            .fold(IndexStats::default(), |a, b| IndexStats {
                files_indexed: a.files_indexed + b.files_indexed,
                files_skipped: a.files_skipped + b.files_skipped,
                symbols_inserted: a.symbols_inserted + b.symbols_inserted,
                semantic_facts: a.semantic_facts + b.semantic_facts,
            });

        // Mark every freshly-rebuilt snapshot as Ready and record
        // its on-disk size. The enrichment level is decided per
        // snapshot from whether Tier-2 produced anything during
        // this index pass — `Semantic` if it did, `Syntactic`
        // otherwise. Reading the snapshot's own DB would also work
        // but is more expensive; the indexer already knows.
        let now = now_ns();
        let entries_with_enrichment: Vec<(IndexPlanEntry, SnapshotEnrichment)> = plan
            .iter()
            .zip(per_entry.iter())
            .map(|(entry, stats)| {
                let enrichment = if stats.semantic_facts > 0 {
                    SnapshotEnrichment::Semantic
                } else {
                    SnapshotEnrichment::Syntactic
                };
                (entry.clone(), enrichment)
            })
            .collect();

        self.storage
            .with_registry(move |conn| {
                for (entry, enrichment) in &entries_with_enrichment {
                    let size_bytes = std::fs::metadata(&entry.snapshot_db_path)
                        .map(|m| i64::try_from(m.len()).unwrap_or(i64::MAX))
                        .ok();
                    registry_db::upsert_snapshot(
                        conn,
                        entry.worktree_id,
                        &entry.branch,
                        &entry.snapshot_db_path.to_string_lossy(),
                        SnapshotStatus::Ready,
                        *enrichment,
                        Some(now),
                        now,
                        size_bytes,
                        INDEXER_REVISION,
                    )?;
                }
                Ok(())
            })
            .await?;

        info!(alias = %alias, ?total, "indexing complete");
        Ok(total)
    }

    /// Build the per-snapshot work list for an alias: one entry per
    /// `(worktree, current_branch)` pair owned by the alias.
    async fn index_plan_for(&self, alias: &str) -> Result<Vec<IndexPlanEntry>> {
        let alias_owned = alias.to_string();
        let data_dir = self.storage.data_dir.clone();
        self.storage
            .with_registry(move |conn| build_index_plan(conn, &alias_owned, &data_dir))
            .await
    }

    /// Resolve the active snapshot handle for an alias. Returns the
    /// snapshot whose branch matches the main worktree's currently
    /// checked-out branch (= what `get_outline` / `find_symbols` should
    /// consult), falling back to the first registered worktree /
    /// snapshot when no exact match is found. Public so the MCP and
    /// control dispatchers share the "prefer main worktree + match
    /// `worktrees.current_branch`" picker without re-deriving it.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when no repo / worktree / snapshot
    /// matches.
    pub async fn active_snapshot(&self, alias: &str) -> Result<RegisteredRepo> {
        let alias_owned = alias.to_string();
        let data_dir = self.storage.data_dir.clone();
        self.storage
            .with_registry(move |conn| resolve_registered_repo(conn, &alias_owned, &data_dir))
            .await
    }

    /// Re-index a single file in the snapshot that owns it. Resolves
    /// the (worktree, branch) by longest-prefix match against the
    /// alias's worktrees, then replaces the file's row (cascading
    /// the old symbols away) and re-parses with the matching backend.
    ///
    /// A file outside every registered worktree, or one whose
    /// extension no backend claims, is silently ignored. Read /
    /// parse failures are logged but do not stop the daemon.
    ///
    /// # Errors
    /// SQL or filesystem failures that prevent the index from being
    /// updated at all.
    pub async fn reindex_file(&self, alias: &str, abs_path: &Path) -> Result<()> {
        let Some(target) = self.locate_file(alias, abs_path).await? else {
            return Ok(());
        };
        let backends = self.backends.clone();
        let abs_path = abs_path.to_path_buf();
        tokio::task::spawn_blocking(move || index_one_file_into(&target, &abs_path, &backends))
            .await
            .map_err(|e| Error::InvalidArgument(format!("reindex task panicked: {e}")))?
    }

    /// Drop a single file's rows from the snapshot that owns it.
    /// No-op when the file falls outside every registered worktree.
    ///
    /// # Errors
    /// SQL failures.
    pub async fn delete_file(&self, alias: &str, abs_path: &Path) -> Result<()> {
        let Some(target) = self.locate_file(alias, abs_path).await? else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || {
            let conn = crate::data_db::open(&target.snapshot_db_path)?;
            conn.execute(
                "DELETE FROM files WHERE path = ?1",
                rusqlite::params![target.rel_path.to_string_lossy()],
            )?;
            Ok::<_, Error>(())
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("delete task panicked: {e}")))??;
        Ok(())
    }

    /// Re-read `.git/HEAD` (or the linked worktree's HEAD file) for
    /// the given worktree, update `worktrees.current_branch` and
    /// `current_head_sha`, ensure a snapshot row exists for the new
    /// branch, and trigger a full re-index of just that worktree
    /// when the branch actually changed.
    ///
    /// Pass `None` for `worktree_path` to refresh the main worktree.
    ///
    /// # Errors
    /// SQL or filesystem failures.
    pub async fn handle_head_change(
        &self,
        alias: &str,
        worktree_path: Option<&Path>,
    ) -> Result<()> {
        let alias_owned = alias.to_string();
        let wt_path_owned = worktree_path.map(Path::to_path_buf);
        let data_dir = self.storage.data_dir.clone();
        let result = self
            .storage
            .with_registry(move |conn| {
                let repo = registry_db::find_repo_by_alias(conn, &alias_owned)?
                    .ok_or_else(|| Error::InvalidArgument(format!("no repo `{alias_owned}`")))?;
                let worktrees = registry_db::list_worktrees(conn, repo.id)?;
                let target_wt = match wt_path_owned.as_deref() {
                    Some(p) => {
                        let p_str = p.to_string_lossy();
                        worktrees.iter().find(|w| w.path == p_str).cloned()
                    }
                    None => worktrees.iter().find(|w| w.path == repo.root_path).cloned(),
                }
                .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "worktree {:?} not registered under `{alias_owned}`",
                        wt_path_owned.as_deref()
                    ))
                })?;
                let wt_root = PathBuf::from(&target_wt.path);
                let new_branch = detect_current_branch(&wt_root);
                let new_head = detect_head_sha(&wt_root);
                let changed = target_wt.current_branch.as_deref() != Some(new_branch.as_str());
                registry_db::upsert_worktree(
                    conn,
                    repo.id,
                    &target_wt.path,
                    &target_wt.worktree_hash,
                    Some(&new_branch),
                    new_head.as_deref(),
                )?;
                if changed {
                    let snapshot_db_path = data_dir.snapshot_db_path(
                        &repo.repo_hash,
                        &target_wt.worktree_hash,
                        &new_branch,
                    );
                    registry_db::upsert_snapshot(
                        conn,
                        target_wt.id,
                        &new_branch,
                        &snapshot_db_path.to_string_lossy(),
                        SnapshotStatus::Building,
                        SnapshotEnrichment::Syntactic,
                        None,
                        now_ns(),
                        None,
                        INDEXER_REVISION,
                    )?;
                }
                Ok((changed, wt_root, new_branch))
            })
            .await?;
        let (changed, wt_root, new_branch) = result;
        if changed {
            info!(alias, branch = %new_branch, worktree = %wt_root.display(), "branch switch; reindexing");
            self.full_index(alias).await?;
        }
        Ok(())
    }

    /// Re-run `detect_worktrees` for the alias and upsert any new
    /// worktrees (e.g. created by `git worktree add` after registration)
    /// with a fresh Building-status snapshot. Returns the number of
    /// worktrees added.
    ///
    /// # Errors
    /// SQL failures.
    pub async fn refresh_worktrees(&self, alias: &str) -> Result<usize> {
        let alias_owned = alias.to_string();
        let data_dir = self.storage.data_dir.clone();
        let added: usize = self
            .storage
            .with_registry(move |conn| {
                let repo = registry_db::find_repo_by_alias(conn, &alias_owned)?
                    .ok_or_else(|| Error::InvalidArgument(format!("no repo `{alias_owned}`")))?;
                let existing: std::collections::HashSet<String> =
                    registry_db::list_worktrees(conn, repo.id)?
                        .into_iter()
                        .map(|w| w.path)
                        .collect();
                let discovered = detect_worktrees(Path::new(&repo.root_path));
                let mut added = 0usize;
                let now = now_ns();
                for wt in &discovered {
                    let wt_path_str = wt.path.to_string_lossy().to_string();
                    if existing.contains(&wt_path_str) {
                        continue;
                    }
                    let wt_hash = path_hash(&wt.path);
                    let worktree_id = registry_db::upsert_worktree(
                        conn,
                        repo.id,
                        &wt_path_str,
                        &wt_hash,
                        Some(&wt.branch),
                        wt.head_sha.as_deref(),
                    )?;
                    let snapshot_db_path =
                        data_dir.snapshot_db_path(&repo.repo_hash, &wt_hash, &wt.branch);
                    registry_db::upsert_snapshot(
                        conn,
                        worktree_id,
                        &wt.branch,
                        &snapshot_db_path.to_string_lossy(),
                        SnapshotStatus::Building,
                        SnapshotEnrichment::Syntactic,
                        None,
                        now,
                        None,
                        INDEXER_REVISION,
                    )?;
                    added += 1;
                }
                Ok(added)
            })
            .await?;
        Ok(added)
    }

    /// Drop the snapshot for `branch` from every worktree owned by
    /// `alias`. Removes the registry row and the on-disk SQLite file.
    /// The branch that is currently checked out in a worktree is
    /// skipped defensively — a `BranchDeleted` notify event during a
    /// branch rename, for instance, can race the HEAD update and
    /// pruning the active snapshot would leave the worktree with no
    /// index to read from.
    ///
    /// Returns the number of snapshots removed.
    ///
    /// # Errors
    /// SQL or filesystem failures.
    pub async fn prune_branch_snapshot(&self, alias: &str, branch: &str) -> Result<usize> {
        let alias_owned = alias.to_string();
        let branch_owned = branch.to_string();
        let paths_to_drop: Vec<PathBuf> = self
            .storage
            .with_registry(move |conn| {
                let repo = registry_db::find_repo_by_alias(conn, &alias_owned)?
                    .ok_or_else(|| Error::InvalidArgument(format!("no repo `{alias_owned}`")))?;
                let mut to_drop = Vec::new();
                for wt in registry_db::list_worktrees(conn, repo.id)? {
                    if wt.current_branch.as_deref() == Some(branch_owned.as_str()) {
                        // Defensive skip — see doc comment.
                        debug!(
                            alias = %alias_owned,
                            branch = %branch_owned,
                            "skipping prune of currently-active branch"
                        );
                        continue;
                    }
                    if let Some(path) = registry_db::delete_snapshot(conn, wt.id, &branch_owned)? {
                        to_drop.push(PathBuf::from(path));
                    }
                }
                Ok(to_drop)
            })
            .await?;
        let removed = paths_to_drop.len();
        for path in &paths_to_drop {
            if let Err(e) = std::fs::remove_file(path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %path.display(), error = %e, "failed to remove snapshot file");
            }
        }
        if removed > 0 {
            info!(alias, branch, removed, "branch snapshot pruned");
        }
        Ok(removed)
    }

    /// Reconcile registry snapshots against the branches that still
    /// exist in the repo. Any snapshot whose branch ref is gone (no
    /// loose ref, no packed entry) is pruned. Idempotent; intended
    /// to run once at daemon startup so snapshots left behind from
    /// branches deleted while the daemon was down get cleaned up.
    ///
    /// The active branch of each worktree is preserved unconditionally
    /// (same safety reason as `prune_branch_snapshot`).
    ///
    /// Returns the number of snapshots removed.
    ///
    /// # Errors
    /// SQL or filesystem failures. Failure to enumerate refs (git
    /// missing, etc.) is logged and the reconcile pass is skipped —
    /// we'd rather keep stale snapshots than drop live ones.
    pub async fn reconcile_snapshots(&self, alias: &str) -> Result<usize> {
        let alias_owned = alias.to_string();
        let plan = self
            .storage
            .with_registry(move |conn| {
                let repo = registry_db::find_repo_by_alias(conn, &alias_owned)?
                    .ok_or_else(|| Error::InvalidArgument(format!("no repo `{alias_owned}`")))?;
                let mut snapshots = Vec::new();
                for wt in registry_db::list_worktrees(conn, repo.id)? {
                    for snap in registry_db::list_snapshots(conn, wt.id)? {
                        snapshots.push(SnapshotRecord {
                            branch: snap.branch,
                            worktree_current_branch: wt.current_branch.clone(),
                        });
                    }
                }
                Ok(ReconcilePlan {
                    repo_root: PathBuf::from(repo.root_path),
                    snapshots,
                })
            })
            .await?;
        let live_branches = match list_repo_branches(&plan.repo_root) {
            Some(b) => b,
            None => {
                warn!(alias, "could not enumerate branches; skipping reconcile");
                return Ok(0);
            }
        };
        let mut removed = 0usize;
        for snap in plan.snapshots {
            if snap.worktree_current_branch.as_deref() == Some(snap.branch.as_str()) {
                continue; // active branch — preserve
            }
            // Detached snapshots (branch label `detached@<sha>`) are
            // never enumerated by `git branch`; leave them alone.
            if snap.branch.starts_with("detached@") {
                continue;
            }
            if live_branches.contains(&snap.branch) {
                continue;
            }
            removed += self.prune_branch_snapshot(alias, &snap.branch).await?;
        }
        if removed > 0 {
            info!(alias, removed, "reconcile pruned stale snapshots");
        }
        Ok(removed)
    }

    /// Enumerate aliases that own at least one snapshot whose
    /// stored `indexer_revision` is older than the current
    /// [`INDEXER_REVISION`]. The daemon calls this on startup and
    /// kicks off a background `full_index` for each — so a binary
    /// upgrade that bumped the revision (new fact kinds, schema
    /// changes) transparently re-extracts the affected data
    /// without the operator having to remember to reindex.
    ///
    /// # Errors
    /// Propagates SQLite errors.
    pub async fn aliases_with_stale_revision(&self) -> Result<Vec<String>> {
        self.storage
            .with_registry(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT DISTINCT r.alias
                       FROM repos r
                       JOIN worktrees w ON w.repo_id = r.id
                       JOIN index_snapshots s ON s.worktree_id = w.id
                      WHERE s.indexer_revision < ?1
                      ORDER BY r.alias",
                )?;
                let rows: Vec<String> = stmt
                    .query_map(rusqlite::params![INDEXER_REVISION], |r| {
                        r.get::<_, String>(0)
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }

    /// Locate the snapshot DB and relative path for a file given an
    /// absolute working-tree path. Returns `None` when the path falls
    /// outside every worktree registered under `alias`.
    async fn locate_file(&self, alias: &str, abs_path: &Path) -> Result<Option<FileTarget>> {
        let alias_owned = alias.to_string();
        let abs_owned = abs_path.to_path_buf();
        let data_dir = self.storage.data_dir.clone();
        self.storage
            .with_registry(move |conn| {
                let repo = registry_db::find_repo_by_alias(conn, &alias_owned)?
                    .ok_or_else(|| Error::InvalidArgument(format!("no repo `{alias_owned}`")))?;
                let worktrees = registry_db::list_worktrees(conn, repo.id)?;
                // Longest-prefix match — handles linked worktrees that
                // happen to live inside the main worktree's path.
                let owning = worktrees
                    .iter()
                    .filter(|w| abs_owned.starts_with(&w.path))
                    .max_by_key(|w| w.path.len())
                    .cloned();
                let Some(wt) = owning else {
                    return Ok(None);
                };
                let Ok(rel) = abs_owned.strip_prefix(&wt.path) else {
                    return Ok(None);
                };
                let branch = wt
                    .current_branch
                    .clone()
                    .unwrap_or_else(|| "default".into());
                let snapshot_db_path =
                    data_dir.snapshot_db_path(&repo.repo_hash, &wt.worktree_hash, &branch);
                Ok(Some(FileTarget {
                    root: PathBuf::from(wt.path),
                    rel_path: rel.to_path_buf(),
                    snapshot_db_path,
                }))
            })
            .await
    }
}

/// Resolved snapshot / file location for an incremental update.
#[derive(Debug, Clone)]
struct FileTarget {
    root: PathBuf,
    rel_path: PathBuf,
    snapshot_db_path: PathBuf,
}

/// Re-parse one file and replace its rows in the snapshot DB. Used by
/// the incremental path. Picks the backend by filename; a no-backend
/// match is treated as a successful no-op (the file isn't indexable
/// language content).
fn index_one_file_into(
    target: &FileTarget,
    abs_path: &Path,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<()> {
    let name = match target.rel_path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return Ok(()),
    };
    let Some(backend) = pick_backend_with_shebang_fallback(backends, name, abs_path) else {
        return Ok(());
    };
    let meta = match std::fs::metadata(abs_path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // notify on macOS often surfaces a vanished file as a
            // Modify event of the parent (or as a different EventKind
            // entirely) rather than as a clean Remove. Treat the
            // missing-file case as a delete so the snapshot tracks
            // reality regardless of which path the event came in on.
            let conn = crate::data_db::open(&target.snapshot_db_path)?;
            conn.execute(
                "DELETE FROM files WHERE path = ?1",
                rusqlite::params![target.rel_path.to_string_lossy()],
            )?;
            return Ok(());
        }
        Err(e) => return Err(Error::from(e)),
    };
    let scanned = scan::ScannedFile {
        path: abs_path.to_path_buf(),
        mtime_ns: meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| i128::try_from(d.as_nanos()).unwrap_or(i128::MAX)),
        size_bytes: meta.len(),
    };

    let mut conn = crate::data_db::open(&target.snapshot_db_path)?;
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM files WHERE path = ?1",
        rusqlite::params![target.rel_path.to_string_lossy()],
    )?;
    let mut pending_impls: Vec<PendingImpl> = Vec::new();
    let mut pending_refs: Vec<PendingRef> = Vec::new();
    match index_one_file(
        &tx,
        &target.root,
        &target.rel_path,
        &scanned,
        backend,
        &mut pending_impls,
        &mut pending_refs,
    ) {
        Ok(_) => {
            // Re-resolve impl/ref edges so the watcher-driven path
            // also benefits from the Tier-2 enrichment. Same scope
            // rules as the full-index second pass: edges whose
            // target isn't visible inside this snapshot get the
            // target_id dropped (the row still lands with a NULL
            // target_id so name-based queries still work).
            apply_pending_impls(&tx, &pending_impls)?;
            apply_pending_refs(&tx, &pending_refs)?;
            tx.commit()?;
            Ok(())
        }
        Err(e) => {
            // Roll the tx back implicitly by dropping; report the
            // outer error so the daemon can log it but the snapshot
            // still has the old rows removed plus whatever survived.
            warn!(path = %target.rel_path.display(), error = %e, "incremental reindex failed");
            Err(e)
        }
    }
}

/// One unit of indexing work: a `(worktree, branch)` pair plus the
/// on-disk paths needed to perform the index pass.
#[derive(Debug, Clone)]
struct IndexPlanEntry {
    worktree_id: i64,
    root: PathBuf,
    branch: String,
    snapshot_db_path: PathBuf,
}

fn build_index_plan(
    conn: &Connection,
    alias: &str,
    data_dir: &DataDir,
) -> Result<Vec<IndexPlanEntry>> {
    let repo = registry_db::find_repo_by_alias(conn, alias)?
        .ok_or_else(|| Error::InvalidArgument(format!("no repo registered as `{alias}`")))?;
    let worktrees = registry_db::list_worktrees(conn, repo.id)?;
    if worktrees.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "repo `{alias}` has no worktree"
        )));
    }
    let mut plan = Vec::with_capacity(worktrees.len());
    for wt in &worktrees {
        // Use the worktree's current branch as the snapshot to
        // (re)build. Other historical branches are left untouched —
        // they remain whatever state the indexer last left them in.
        let branch = wt
            .current_branch
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let snapshot_db_path =
            data_dir.snapshot_db_path(&repo.repo_hash, &wt.worktree_hash, &branch);
        plan.push(IndexPlanEntry {
            worktree_id: wt.id,
            root: PathBuf::from(&wt.path),
            branch,
            snapshot_db_path,
        });
    }
    Ok(plan)
}

fn resolve_registered_repo(
    conn: &Connection,
    alias: &str,
    data_dir: &DataDir,
) -> Result<RegisteredRepo> {
    let repo = registry_db::find_repo_by_alias(conn, alias)?
        .ok_or_else(|| Error::InvalidArgument(format!("no repo registered as `{alias}`")))?;
    let worktrees = registry_db::list_worktrees(conn, repo.id)?;
    // Prefer the worktree at the repo's root_path (the main checkout)
    // when present; fall back to the first registered worktree.
    let worktree = worktrees
        .iter()
        .find(|w| w.path == repo.root_path)
        .cloned()
        .or_else(|| worktrees.into_iter().next())
        .ok_or_else(|| Error::InvalidArgument(format!("repo `{alias}` has no worktree")))?;
    let snapshots = registry_db::list_snapshots(conn, worktree.id)?;
    // Prefer the snapshot whose branch matches the worktree's
    // currently checked-out branch (= what's on disk right now).
    // Without that match the index would silently return data from a
    // stale branch.
    let snapshot = match worktree.current_branch.as_deref() {
        Some(b) => snapshots
            .iter()
            .find(|s| s.branch == b)
            .cloned()
            .or_else(|| snapshots.into_iter().next()),
        None => snapshots.into_iter().next(),
    }
    .ok_or_else(|| {
        Error::InvalidArgument(format!("repo `{alias}` has no snapshot — register first"))
    })?;
    Ok(RegisteredRepo {
        alias: repo.alias,
        root: PathBuf::from(repo.root_path),
        branch: snapshot.branch.clone(),
        repo_id: repo.id,
        worktree_id: worktree.id,
        snapshot_id: snapshot.id,
        snapshot_db_path: data_dir.snapshot_db_path(
            &repo.repo_hash,
            &worktree.worktree_hash,
            &snapshot.branch,
        ),
    })
}

fn index_repo_sync(
    root: &Path,
    snapshot_db: &Path,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<IndexStats> {
    // Start from a blank slate. For 0.1.0 we rewrite the whole DB on
    // every full_index call; incremental updates come later.
    if snapshot_db.exists() {
        std::fs::remove_file(snapshot_db)?;
    }
    let mut conn = crate::data_db::open(snapshot_db)?;
    let tx = conn.transaction()?;

    let mut stats = IndexStats::default();
    // Impl/ref edges have to be resolved against the *full* symbol
    // set, so we accumulate the facts while indexing each file and
    // run the resolution-and-insert pass at the end.
    let mut pending_impls: Vec<PendingImpl> = Vec::new();
    let mut pending_refs: Vec<PendingRef> = Vec::new();

    for scanned in scan::walk_repo(root) {
        let rel = match scanned.path.strip_prefix(root) {
            Ok(p) => p.to_path_buf(),
            Err(_) => continue,
        };
        let Some(name) = rel.file_name().and_then(|n| n.to_str()) else {
            stats.files_skipped += 1;
            continue;
        };
        let Some(backend) = pick_backend_with_shebang_fallback(backends, name, &scanned.path)
        else {
            stats.files_skipped += 1;
            continue;
        };
        match index_one_file(
            &tx,
            root,
            &rel,
            &scanned,
            backend,
            &mut pending_impls,
            &mut pending_refs,
        ) {
            Ok((n, semantic)) => {
                stats.files_indexed += 1;
                stats.symbols_inserted += n;
                stats.semantic_facts += semantic;
            }
            Err(e) => {
                warn!(path = %rel.display(), error = %e, "indexing file failed; skipping");
                stats.files_skipped += 1;
            }
        }
    }

    // Second pass: every symbol the syntactic layer was going to
    // produce is now in the DB, so resolution can run with full
    // snapshot visibility.
    // - impls: edges referring to a type that doesn't have a symbol
    //   in this snapshot are dropped (interface_name text is still
    //   carried so consumers can filter "what implements `Display`?"
    //   without us pretending we resolved the trait to a symbol id).
    // - refs: the row always lands; target_id and enclosing_id are
    //   best-effort qualified-name lookups against the symbols
    //   table, so callers can answer "who calls foo" by name even
    //   when the receiver type can't be resolved.
    apply_pending_impls(&tx, &pending_impls)?;
    apply_pending_refs(&tx, &pending_refs)?;

    tx.commit()?;
    Ok(stats)
}

/// One unresolved impl edge collected during the first indexing
/// pass. Resolved against the symbols table at the end of
/// [`index_repo_sync`].
/// One unresolved ref edge collected during the first indexing
/// pass. Resolved against the symbols table at the end of
/// [`index_repo_sync`] / [`index_one_file_into`].
struct PendingRef {
    file_id: i64,
    target_name: String,
    target_qualified: Option<String>,
    enclosing_qualified: Option<String>,
    kind: String,
    line: i64,
}

struct PendingImpl {
    type_qualified: String,
    interface_qualified: Option<String>,
    interface_name: String,
    kind: String,
}

fn apply_pending_impls(tx: &rusqlite::Transaction<'_>, pending: &[PendingImpl]) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut find = tx.prepare("SELECT id FROM symbols WHERE qualified = ?1 LIMIT 1")?;
    let mut inserted = 0usize;
    for p in pending {
        let type_id: Option<i64> = find
            .query_row(params![&p.type_qualified], |r| r.get(0))
            .ok();
        let Some(type_id) = type_id else {
            // Type is external to the snapshot — drop the edge.
            continue;
        };
        let interface_id: Option<i64> = match &p.interface_qualified {
            Some(q) => find.query_row(params![q], |r| r.get(0)).ok(),
            None => None,
        };
        tx.execute(
            "INSERT INTO implementations (type_id, interface_id, interface_name, kind)
             VALUES (?1, ?2, ?3, ?4)",
            params![type_id, interface_id, p.interface_name, p.kind],
        )?;
        inserted += 1;
    }
    debug!(impl_edges_inserted = inserted, "impls applied");
    Ok(())
}

/// Resolve each pending ref against the symbols table and insert one
/// row in the `refs` table. `enclosing_id` is looked up by
/// (file_id, qualified) — the calling fn lives in the same file as
/// the call site. `target_id` is looked up snapshot-wide by
/// qualified name, falling back to a bare-name match for method
/// calls where only the method name is known.
fn apply_pending_refs(tx: &rusqlite::Transaction<'_>, pending: &[PendingRef]) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut find_enclosing =
        tx.prepare("SELECT id FROM symbols WHERE file_id = ?1 AND qualified = ?2 LIMIT 1")?;
    let mut find_target_qualified =
        tx.prepare("SELECT id FROM symbols WHERE qualified = ?1 LIMIT 1")?;
    let mut find_target_name = tx.prepare("SELECT id FROM symbols WHERE name = ?1 LIMIT 1")?;

    let mut inserted = 0usize;
    for r in pending {
        let enclosing_id: Option<i64> = match &r.enclosing_qualified {
            Some(q) => find_enclosing
                .query_row(params![r.file_id, q], |row| row.get(0))
                .ok(),
            None => None,
        };
        let target_id: Option<i64> = match &r.target_qualified {
            Some(q) => find_target_qualified
                .query_row(params![q], |row| row.get(0))
                .ok()
                .or_else(|| {
                    find_target_name
                        .query_row(params![&r.target_name], |row| row.get(0))
                        .ok()
                }),
            None => find_target_name
                .query_row(params![&r.target_name], |row| row.get(0))
                .ok(),
        };
        tx.execute(
            "INSERT INTO refs (file_id, enclosing_id, target_id, target_name,
                               target_qualified, kind, type_role,
                               byte_start, byte_end, line, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 0, 0, ?7, ?8)",
            params![
                r.file_id,
                enclosing_id,
                target_id,
                r.target_name,
                r.target_qualified,
                r.kind,
                r.line,
                "semantic",
            ],
        )?;
        inserted += 1;
    }
    debug!(refs_inserted = inserted, "refs applied");
    Ok(())
}

/// Skip any single file larger than this. Cairn's value lies in
/// navigating handwritten source; multi-megabyte files are almost
/// always generated (lockfiles, vendored blobs, machine-emitted JSON)
/// where the symbol index doesn't help anyway. Cap also bounds peak
/// memory per parser invocation — tree-sitter can degrade super-linearly
/// on pathological inputs, so a hard ceiling protects the daemon.
const MAX_INDEXED_FILE_BYTES: u64 = 4 * 1024 * 1024;

#[allow(clippy::too_many_arguments)]
fn index_one_file(
    tx: &rusqlite::Transaction<'_>,
    root: &Path,
    rel_path: &Path,
    scanned: &scan::ScannedFile,
    backend: &dyn LanguageBackend,
    pending_impls: &mut Vec<PendingImpl>,
    pending_refs: &mut Vec<PendingRef>,
) -> Result<(usize, usize)> {
    if scanned.size_bytes > MAX_INDEXED_FILE_BYTES {
        warn!(
            path = %rel_path.display(),
            size_bytes = scanned.size_bytes,
            cap = MAX_INDEXED_FILE_BYTES,
            "skipping file exceeding size cap"
        );
        return Ok((0, 0));
    }
    let abs = root.join(rel_path);
    let source = std::fs::read(&abs)?;
    let facts = backend
        .extract_syntactic(&source)
        .map_err(|e| Error::InvalidArgument(format!("{}: {e}", rel_path.display())))?;

    // Insert file row.
    let blob_sha = sha256_hex(&source);
    let mtime_ns = i64::try_from(scanned.mtime_ns).unwrap_or(i64::MAX);
    let size_bytes = i64::try_from(scanned.size_bytes).unwrap_or(i64::MAX);
    let parsed_at = now_ns();
    let path_str = rel_path.to_string_lossy().to_string();
    tx.execute(
        "INSERT INTO files
           (path, language, blob_sha, size_bytes, mtime_ns, parsed_at_ns, parser)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            path_str,
            backend.name(),
            blob_sha,
            size_bytes,
            mtime_ns,
            parsed_at,
            backend.parser_id(),
        ],
    )?;
    let file_id = tx.last_insert_rowid();

    // Insert symbols. parent_id is a forward reference inside the
    // same SyntacticFacts vector, so we keep an idx -> rowid map and
    // resolve in-line.
    let mut idx_to_rowid: Vec<i64> = Vec::with_capacity(facts.symbols.len());
    let mut inserted = 0usize;
    for (idx, sym) in facts.symbols.iter().enumerate() {
        let parent_rowid = sym.parent_idx.and_then(|i| idx_to_rowid.get(i).copied());
        let rowid = insert_symbol(tx, file_id, parent_rowid, sym)?;
        idx_to_rowid.push(rowid);
        let _ = idx;
        inserted += 1;
    }

    // Insert any imports the syntactic layer emitted (most backends
    // leave this empty; the syn-based analyzer below produces them
    // for Rust).
    for imp in &facts.imports {
        insert_import(tx, file_id, imp)?;
    }

    // Run the optional semantic analyzer. Failures are logged and
    // dropped — they should not stop the syntactic facts from
    // landing. The returned `semantic` count lets the caller decide
    // whether this snapshot should be flagged `Semantic` (Tier-2
    // facts present) or `Syntactic` (Tier-1 only).
    let mut semantic = 0usize;
    if let Some(analyzer) = backend.analyzer() {
        match analyzer.extract_semantic(&source) {
            Ok(sem) => {
                semantic = apply_semantic_facts(tx, file_id, &sem, pending_impls, pending_refs)?;
            }
            Err(e) => warn!(
                path = %rel_path.display(),
                analyzer = analyzer.name(),
                error = %e,
                "semantic analyzer failed; syntactic facts kept"
            ),
        }
    }

    debug!(path = %rel_path.display(), inserted, semantic, "file indexed");
    Ok((inserted, semantic))
}

fn insert_import(
    tx: &rusqlite::Transaction<'_>,
    from_file_id: i64,
    imp: &cairn_lang_api::ImportFact,
) -> Result<()> {
    let line = i64::from(imp.line);
    tx.execute(
        "INSERT INTO imports
           (from_file_id, to_file_id, to_module, imported, alias, is_reexport, line)
         VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6)",
        params![
            from_file_id,
            imp.to_module,
            imp.imported,
            imp.alias,
            i64::from(imp.is_reexport),
            line,
        ],
    )?;
    Ok(())
}

/// Apply the per-file semantic facts an [`Analyzer`] produced:
/// - imports → insert directly,
/// - doc_overrides → UPDATE the matching symbol's `doc` column,
/// - impls → defer to the snapshot-wide resolution pass via
///   `pending_impls` (the target type may be defined in a file
///   we haven't indexed yet).
fn apply_semantic_facts(
    tx: &rusqlite::Transaction<'_>,
    file_id: i64,
    sem: &cairn_lang_api::SemanticFacts,
    pending_impls: &mut Vec<PendingImpl>,
    pending_refs: &mut Vec<PendingRef>,
) -> Result<usize> {
    // Tracked so the caller can attribute enrichment level
    // ("did Tier-2 actually produce anything?") without re-reading
    // the snapshot DB. Each fact kind counts once.
    let mut applied = 0usize;
    for imp in &sem.imports {
        insert_import(tx, file_id, imp)?;
        applied += 1;
    }
    for over in &sem.doc_overrides {
        // Scope the override to symbols belonging to this file so
        // a doc string on `outer::Foo` in one file does not
        // overwrite an unrelated `outer::Foo` in another.
        tx.execute(
            "UPDATE symbols SET doc = ?1 WHERE file_id = ?2 AND qualified = ?3",
            params![over.doc, file_id, over.target_qualified],
        )?;
        applied += 1;
    }
    for fact in &sem.impls {
        // `interface_name` always carries the human-readable name,
        // even when symbol id resolution later drops it.
        let interface_name = fact
            .interface_qualified
            .clone()
            .unwrap_or_else(|| "<inherent>".to_string());
        pending_impls.push(PendingImpl {
            type_qualified: fact.type_qualified.clone(),
            interface_qualified: fact.interface_qualified.clone(),
            interface_name,
            kind: fact.kind.clone(),
        });
        applied += 1;
    }
    for r in &sem.refs {
        pending_refs.push(PendingRef {
            file_id,
            target_name: r.target_name.clone(),
            target_qualified: r.target_qualified.clone(),
            enclosing_qualified: r.enclosing_qualified.clone(),
            kind: ref_kind_to_db(r.kind),
            line: i64::from(r.line),
        });
        applied += 1;
    }
    Ok(applied)
}

fn ref_kind_to_db(k: cairn_lang_api::RefKind) -> String {
    // The `RefKind` proto enum is sparse today; serialize via Debug
    // since the wire form is human-friendly snake-case via serde's
    // `rename_all`. Using its Display-equivalent serialization keeps
    // the DB value matching what the query layer expects.
    serde_json::to_string(&k)
        .ok()
        .and_then(|s| {
            s.strip_prefix('"')
                .map(|t| t.trim_end_matches('"').to_string())
        })
        .unwrap_or_else(|| "call".to_string())
}

fn insert_symbol(
    tx: &rusqlite::Transaction<'_>,
    file_id: i64,
    parent_id: Option<i64>,
    sym: &SymbolFact,
) -> Result<i64> {
    let kind_str = symbol_kind_to_db(&sym.kind);
    let visibility_str = sym.visibility.map(visibility_to_db);
    let line_start = i64::from(sym.line_range.start);
    let line_end = i64::from(sym.line_range.end);
    let byte_start = i64::try_from(sym.byte_range.start).unwrap_or(i64::MAX);
    let byte_end = i64::try_from(sym.byte_range.end).unwrap_or(i64::MAX);
    let body_start = sym.body_start.map(|b| i64::try_from(b).unwrap_or(i64::MAX));

    tx.execute(
        "INSERT INTO symbols
           (file_id, parent_id, name, qualified, kind, signature,
            visibility, doc, byte_start, byte_end, line_start, line_end,
            body_start, source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            file_id,
            parent_id,
            sym.name,
            sym.qualified,
            kind_str,
            sym.signature,
            visibility_str,
            sym.doc,
            byte_start,
            byte_end,
            line_start,
            line_end,
            body_start,
            "syntactic",
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

#[derive(Debug, Clone)]
pub struct RegisteredRepo {
    pub alias: String,
    pub root: PathBuf,
    pub branch: String,
    pub repo_id: i64,
    pub worktree_id: i64,
    pub snapshot_id: i64,
    pub snapshot_db_path: PathBuf,
}

// ─── helpers ────────────────────────────────────────────────────────────────

/// Enumerate every worktree associated with `repo_root` via
/// `git worktree list --porcelain`, falling back to a single
/// synthesised entry (the root with its `.git/HEAD`-derived branch)
/// when the command is unavailable or fails.
///
/// The fallback path matters in two cases:
/// - The directory is not a git repository at all (cairn still
///   supports indexing it as a plain tree).
/// - `git` is not on `$PATH` (uncommon, but the daemon should not
///   crash if so).
#[must_use]
pub fn detect_worktrees(repo_root: &Path) -> Vec<DiscoveredWorktree> {
    if let Some(parsed) = run_worktree_list(repo_root) {
        if !parsed.is_empty() {
            return parsed;
        }
    }
    vec![DiscoveredWorktree {
        path: repo_root.to_path_buf(),
        branch: detect_current_branch(repo_root),
        head_sha: detect_head_sha(repo_root),
    }]
}

fn run_worktree_list(repo_root: &Path) -> Option<Vec<DiscoveredWorktree>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("worktree")
        .arg("list")
        .arg("--porcelain")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    Some(parse_worktree_porcelain(&stdout))
}

/// Parse the `--porcelain` form. Each worktree is one record;
/// records are separated by blank lines and contain key-value
/// lines like:
/// ```text
/// worktree /path/to/repo
/// HEAD 0123abcd...
/// branch refs/heads/main
/// ```
/// A `detached` line replaces the `branch` line for detached HEADs.
/// Bare worktrees (no working tree on disk) are skipped — cairn
/// cannot index a tree that does not exist as files.
fn parse_worktree_porcelain(text: &str) -> Vec<DiscoveredWorktree> {
    let mut out = Vec::new();
    let mut cur_path: Option<PathBuf> = None;
    let mut cur_head: Option<String> = None;
    let mut cur_branch: Option<String> = None;
    let mut cur_detached = false;
    let mut cur_bare = false;

    let flush = |out: &mut Vec<DiscoveredWorktree>,
                 path: &mut Option<PathBuf>,
                 head: &mut Option<String>,
                 branch: &mut Option<String>,
                 detached: &mut bool,
                 bare: &mut bool| {
        if let Some(p) = path.take() {
            if !*bare {
                let final_branch = if let Some(b) = branch.take() {
                    b
                } else if *detached {
                    head.as_deref()
                        .filter(|h| h.len() >= 7)
                        .map(|h| format!("detached@{}", &h[..7]))
                        .unwrap_or_else(|| "default".to_string())
                } else {
                    "default".to_string()
                };
                out.push(DiscoveredWorktree {
                    path: p,
                    branch: final_branch,
                    head_sha: head.take(),
                });
            } else {
                head.take();
                branch.take();
            }
        }
        *detached = false;
        *bare = false;
    };

    for line in text.lines() {
        if line.is_empty() {
            flush(
                &mut out,
                &mut cur_path,
                &mut cur_head,
                &mut cur_branch,
                &mut cur_detached,
                &mut cur_bare,
            );
            continue;
        }
        if let Some(p) = line.strip_prefix("worktree ") {
            // A new record starting before a blank line — flush.
            flush(
                &mut out,
                &mut cur_path,
                &mut cur_head,
                &mut cur_branch,
                &mut cur_detached,
                &mut cur_bare,
            );
            cur_path = Some(PathBuf::from(p));
        } else if let Some(h) = line.strip_prefix("HEAD ") {
            cur_head = Some(h.to_string());
        } else if let Some(b) = line.strip_prefix("branch refs/heads/") {
            cur_branch = Some(b.to_string());
        } else if line == "detached" {
            cur_detached = true;
        } else if line == "bare" {
            cur_bare = true;
        }
        // Other lines (locked, prunable, ...) are ignored.
    }
    // Trailing record without a final blank line.
    flush(
        &mut out,
        &mut cur_path,
        &mut cur_head,
        &mut cur_branch,
        &mut cur_detached,
        &mut cur_bare,
    );
    out
}

/// Pick a backend for a file, falling back to shebang sniffing when
/// the filename pattern does not match. The shebang read is a single
/// best-effort `open` + first-256-bytes read; failures (binary file,
/// permission denied, race against deletion) are silently treated as
/// "no backend", matching the path-only behaviour.
///
/// Lives here rather than in `cairn-lang-api` because the API crate
/// is intentionally I/O-free.
fn pick_backend_with_shebang_fallback<'a>(
    backends: &'a [Box<dyn LanguageBackend>],
    name: &str,
    abs_path: &Path,
) -> Option<&'a dyn LanguageBackend> {
    if let Some(b) = cairn_lang_api::pick_backend_for_path(backends, name) {
        return Some(b);
    }
    let first_line = read_first_line(abs_path)?;
    if !first_line.starts_with("#!") {
        return None;
    }
    cairn_lang_api::pick_backend_for_shebang(backends, &first_line)
}

/// Read up to the first newline (or 256 bytes, whichever comes
/// first) from `path`. Returns `None` on any I/O failure or if the
/// file is empty / not valid UTF-8 up to the read window.
fn read_first_line(path: &Path) -> Option<String> {
    use std::io::Read;
    let mut buf = [0u8; 256];
    let mut file = std::fs::File::open(path).ok()?;
    let n = file.read(&mut buf).ok()?;
    if n == 0 {
        return None;
    }
    let slice = &buf[..n];
    let end = slice
        .iter()
        .position(|&b| b == b'\n')
        .unwrap_or(slice.len());
    std::str::from_utf8(&slice[..end]).ok().map(str::to_string)
}

/// Snapshot of a registry row that `reconcile_snapshots` needs to
/// decide whether to keep or drop. Plain data carrier — avoids the
/// `clippy::type_complexity` lint on a deeply-nested tuple.
struct SnapshotRecord {
    branch: String,
    worktree_current_branch: Option<String>,
}

/// Plan computed by `reconcile_snapshots` before it touches the
/// filesystem: where the repo lives, plus every (snapshot, owning
/// worktree's active branch) pair the reconcile loop needs to
/// consider.
struct ReconcilePlan {
    repo_root: PathBuf,
    snapshots: Vec<SnapshotRecord>,
}

/// Enumerate every branch known to the repo (loose refs + packed),
/// using `git for-each-ref` for correctness. Returns `None` when git
/// can't be reached — callers treat that as "don't reconcile" so a
/// transient git failure never drops live snapshots.
fn list_repo_branches(repo_root: &Path) -> Option<std::collections::HashSet<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("for-each-ref")
        .arg("--format=%(refname:short)")
        .arg("refs/heads/")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.lines().map(|s| s.trim().to_string()).collect())
}

fn detect_current_branch(repo_root: &Path) -> String {
    let head = repo_root.join(".git").join("HEAD");
    let Ok(text) = std::fs::read_to_string(&head) else {
        return "default".to_string();
    };
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("ref: refs/heads/") {
        rest.to_string()
    } else if text.len() >= 7 {
        format!("detached@{}", &text[..7])
    } else {
        "default".to_string()
    }
}

fn detect_head_sha(repo_root: &Path) -> Option<String> {
    let head = repo_root.join(".git").join("HEAD");
    let text = std::fs::read_to_string(&head).ok()?;
    let text = text.trim();
    if let Some(name) = text.strip_prefix("ref: refs/heads/") {
        let p = repo_root.join(".git").join("refs").join("heads").join(name);
        std::fs::read_to_string(&p)
            .ok()
            .map(|s| s.trim().to_string())
    } else {
        Some(text.to_string())
    }
}

fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn sha256_hex(bytes: &[u8]) -> String {
    // Simple FNV-1a 128-bit hash, hex-encoded. cairn does not need
    // collision-resistant content addressing at 0.1.0 — the hash
    // just helps the indexer detect when a file genuinely changed
    // versus a mtime-only touch.
    let mut h1: u128 = 0x6c62272e07bb014262b821756295c58d;
    for b in bytes {
        h1 ^= u128::from(*b);
        h1 = h1.wrapping_mul(0x0000_0000_0100_0000_0000_0000_013B);
    }
    format!("{h1:032x}")
}

pub(crate) fn symbol_kind_to_db(kind: &SymbolKind) -> String {
    match kind {
        SymbolKind::Function => "function".into(),
        SymbolKind::Method => "method".into(),
        SymbolKind::Constructor => "constructor".into(),
        SymbolKind::Getter => "getter".into(),
        SymbolKind::Setter => "setter".into(),
        SymbolKind::Class => "class".into(),
        SymbolKind::Struct => "struct".into(),
        SymbolKind::Enum => "enum".into(),
        SymbolKind::Union => "union".into(),
        SymbolKind::Trait => "trait".into(),
        SymbolKind::Impl => "impl".into(),
        SymbolKind::Interface => "interface".into(),
        SymbolKind::TypeAlias => "type_alias".into(),
        SymbolKind::Field => "field".into(),
        SymbolKind::Property => "property".into(),
        SymbolKind::Constant => "constant".into(),
        SymbolKind::Variable => "variable".into(),
        SymbolKind::Parameter => "parameter".into(),
        SymbolKind::Module => "module".into(),
        SymbolKind::Namespace => "namespace".into(),
        SymbolKind::Package => "package".into(),
        SymbolKind::Macro => "macro".into(),
        SymbolKind::Test => "test".into(),
        SymbolKind::Section => "section".into(),
        SymbolKind::Other(s) => s.clone(),
    }
}

pub(crate) fn symbol_kind_from_db(s: &str) -> SymbolKind {
    match s {
        "function" => SymbolKind::Function,
        "method" => SymbolKind::Method,
        "constructor" => SymbolKind::Constructor,
        "getter" => SymbolKind::Getter,
        "setter" => SymbolKind::Setter,
        "class" => SymbolKind::Class,
        "struct" => SymbolKind::Struct,
        "enum" => SymbolKind::Enum,
        "union" => SymbolKind::Union,
        "trait" => SymbolKind::Trait,
        "impl" => SymbolKind::Impl,
        "interface" => SymbolKind::Interface,
        "type_alias" => SymbolKind::TypeAlias,
        "field" => SymbolKind::Field,
        "property" => SymbolKind::Property,
        "constant" => SymbolKind::Constant,
        "variable" => SymbolKind::Variable,
        "parameter" => SymbolKind::Parameter,
        "module" => SymbolKind::Module,
        "namespace" => SymbolKind::Namespace,
        "package" => SymbolKind::Package,
        "macro" => SymbolKind::Macro,
        "test" => SymbolKind::Test,
        "section" => SymbolKind::Section,
        other => SymbolKind::Other(other.to_string()),
    }
}

fn visibility_to_db(v: Visibility) -> &'static str {
    match v {
        Visibility::Public => "public",
        Visibility::Crate => "crate",
        Visibility::Private => "private",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_lang_api::LanguageBackend;

    #[tokio::test]
    async fn indexes_extensionless_python_executable_via_shebang() {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("bin")).unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();
        // Extensionless executable with a Python shebang. Mirrors
        // `bin/uchgs` and similar real-world scripts.
        std::fs::write(
            repo_root.join("bin").join("uchgs"),
            "#!/usr/bin/env python3\n\ndef _walk_chain_to_known():\n    pass\n",
        )
        .unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> =
            vec![Box::new(cairn_lang_python::PythonBackend)];
        let indexer = Indexer::with_backends(storage.clone(), backends);

        indexer.register_repo("demo", &repo_root).await.unwrap();
        let stats = indexer.full_index("demo").await.unwrap();
        assert!(
            stats.files_indexed >= 1,
            "expected the extensionless python script to be indexed; got {stats:?}"
        );
        assert!(
            stats.symbols_inserted >= 1,
            "expected at least one symbol from the python shebang file"
        );

        // The symbol should be queryable via the snapshot DB.
        let handle = indexer.active_snapshot("demo").await.unwrap();
        let conn = crate::data_db::open(&handle.snapshot_db_path).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = '_walk_chain_to_known'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    /// Files above the size cap are skipped (logged + ignored) rather
    /// than read into memory. Catches a regression where a huge
    /// generated file would balloon parser memory.
    #[tokio::test]
    async fn skips_files_over_size_cap() {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();
        // One small file: indexed normally.
        std::fs::write(repo_root.join("src").join("ok.rs"), "fn small() {}\n").unwrap();
        // One file above the cap: header is real Rust, padded out past
        // MAX_INDEXED_FILE_BYTES with line comments. The cap is on
        // file metadata size, so the contents never get parsed.
        let big_path = repo_root.join("src").join("huge.rs");
        let header = "fn enormous() {}\n";
        let pad_len = (MAX_INDEXED_FILE_BYTES as usize) + 1024;
        let mut content = String::with_capacity(pad_len + header.len());
        content.push_str(header);
        content.push_str("// ");
        while content.len() < pad_len {
            content.push('x');
        }
        content.push('\n');
        std::fs::write(&big_path, &content).unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Indexer::with_backends(storage.clone(), backends);

        indexer.register_repo("demo", &repo_root).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        let handle = indexer.active_snapshot("demo").await.unwrap();
        let conn = crate::data_db::open(&handle.snapshot_db_path).unwrap();
        let small: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = 'small'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(small, 1, "small file should be indexed normally");
        let enormous: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = 'enormous'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(enormous, 0, "oversized file must be skipped, not parsed");
    }

    /// End-to-end: register a repo with two Rust files, full_index,
    /// then verify the data DB has the expected symbols.
    #[tokio::test]
    async fn registers_and_indexes_a_rust_repo() {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src").join("lib.rs"), "fn hello() {}\n").unwrap();
        std::fs::write(
            repo_root.join("src").join("util.rs"),
            "pub struct Foo;\nimpl Foo { fn bar(&self) {} }\n",
        )
        .unwrap();
        // Fake .git so detect_current_branch picks something sensible.
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());

        // Use only the rust backend in this test to keep the assertion stable.
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Indexer::with_backends(storage.clone(), backends);

        let registered = indexer.register_repo("demo", &repo_root).await.unwrap();
        assert_eq!(registered.branch, "main");

        let stats = indexer.full_index("demo").await.unwrap();
        assert_eq!(stats.files_indexed, 2);
        assert!(stats.symbols_inserted >= 3); // hello, Foo, impl Foo, bar
        // syn-based analyzer ran (`impl Foo` produces at least one
        // impl fact), so the indexer should have flagged this
        // snapshot's enrichment as Semantic — not stuck at Syntactic.
        assert!(
            stats.semantic_facts >= 1,
            "expected Tier-2 (syn) to produce at least one fact; got {stats:?}"
        );

        // Direct query on the snapshot DB confirms the rows landed.
        let conn = crate::data_db::open(&registered.snapshot_db_path).unwrap();
        let names: Vec<String> = conn
            .prepare("SELECT name FROM symbols ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(names.contains(&"hello".to_string()));
        assert!(names.contains(&"Foo".to_string()));
        assert!(names.contains(&"bar".to_string()));

        // The snapshot in the registry is marked Ready, and the
        // enrichment level reflects that Tier-2 actually ran.
        let snap = storage
            .with_registry(move |c| {
                let snaps = registry_db::list_snapshots(c, registered.worktree_id)?;
                Ok(snaps.into_iter().next().unwrap())
            })
            .await
            .unwrap();
        assert_eq!(snap.status, SnapshotStatus::Ready);
        assert_eq!(snap.enrichment, SnapshotEnrichment::Semantic);
    }

    /// Python-only snapshot should stay at `Syntactic` because the
    /// Python backend doesn't ship a Tier-2 analyzer yet. Guards the
    /// other direction of the enrichment flag — we shouldn't
    /// promote to Semantic just because indexing succeeded.
    #[tokio::test]
    async fn python_only_repo_stays_syntactic() {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(
            repo_root.join("src").join("a.py"),
            "def hello():\n    pass\n",
        )
        .unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> =
            vec![Box::new(cairn_lang_python::PythonBackend)];
        let indexer = Indexer::with_backends(storage.clone(), backends);

        let registered = indexer.register_repo("demo", &repo_root).await.unwrap();
        let stats = indexer.full_index("demo").await.unwrap();
        assert!(stats.files_indexed >= 1);
        assert_eq!(stats.semantic_facts, 0);

        let snap = storage
            .with_registry(move |c| {
                let snaps = registry_db::list_snapshots(c, registered.worktree_id)?;
                Ok(snaps.into_iter().next().unwrap())
            })
            .await
            .unwrap();
        assert_eq!(snap.enrichment, SnapshotEnrichment::Syntactic);
    }

    /// Snapshots whose stored `indexer_revision` is below the
    /// current constant should be listed as needing reindex; ones
    /// at the current revision should not. Simulates an upgraded
    /// daemon meeting an old snapshot.
    #[tokio::test]
    async fn aliases_with_stale_revision_picks_up_legacy_rows() {
        let work = tempfile::tempdir().unwrap();
        let repo_root = work.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src").join("lib.rs"), "fn hello() {}\n").unwrap();
        std::fs::create_dir(repo_root.join(".git")).unwrap();
        std::fs::write(
            repo_root.join(".git").join("HEAD"),
            "ref: refs/heads/main\n",
        )
        .unwrap();
        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Indexer::with_backends(storage.clone(), backends);
        let registered = indexer.register_repo("demo", &repo_root).await.unwrap();
        indexer.full_index("demo").await.unwrap();

        // Fresh full_index → snapshot stamped at current revision.
        assert!(
            indexer
                .aliases_with_stale_revision()
                .await
                .unwrap()
                .is_empty()
        );

        // Manually backdate the snapshot's revision (simulating a
        // row written by an older daemon) and assert it now shows up.
        let worktree_id = registered.worktree_id;
        storage
            .with_registry(move |conn| {
                conn.execute(
                    "UPDATE index_snapshots SET indexer_revision = 0 WHERE worktree_id = ?1",
                    rusqlite::params![worktree_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let stale = indexer.aliases_with_stale_revision().await.unwrap();
        assert_eq!(stale, vec!["demo".to_string()]);
    }

    #[test]
    fn detects_branch_from_head_file() {
        let tmp = tempfile::tempdir().unwrap();
        let git = tmp.path().join(".git");
        std::fs::create_dir(&git).unwrap();
        std::fs::write(git.join("HEAD"), "ref: refs/heads/feature/x\n").unwrap();
        assert_eq!(detect_current_branch(tmp.path()), "feature/x");
    }

    #[test]
    fn detects_detached_head() {
        let tmp = tempfile::tempdir().unwrap();
        let git = tmp.path().join(".git");
        std::fs::create_dir(&git).unwrap();
        std::fs::write(git.join("HEAD"), "abc1234567890def\n").unwrap();
        assert_eq!(detect_current_branch(tmp.path()), "detached@abc1234");
    }

    #[test]
    fn missing_git_falls_back_to_default() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(detect_current_branch(tmp.path()), "default");
    }

    #[test]
    fn symbol_kind_round_trips_through_db_string() {
        for k in [
            SymbolKind::Function,
            SymbolKind::Method,
            SymbolKind::Class,
            SymbolKind::Trait,
            SymbolKind::Impl,
            SymbolKind::Test,
            SymbolKind::Section,
            SymbolKind::Other("MyCustom".into()),
        ] {
            let s = symbol_kind_to_db(&k);
            let back = symbol_kind_from_db(&s);
            assert_eq!(back, k);
        }
    }

    #[test]
    fn parse_worktree_porcelain_two_branches() {
        let porcelain = "\
worktree /repo/main
HEAD 1111111111111111111111111111111111111111
branch refs/heads/main

worktree /repo/feature
HEAD 2222222222222222222222222222222222222222
branch refs/heads/feature/x

";
        let out = parse_worktree_porcelain(porcelain);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].path, PathBuf::from("/repo/main"));
        assert_eq!(out[0].branch, "main");
        assert_eq!(out[1].path, PathBuf::from("/repo/feature"));
        assert_eq!(out[1].branch, "feature/x");
    }

    #[test]
    fn parse_worktree_porcelain_detached_uses_short_sha() {
        let porcelain = "\
worktree /repo/main
HEAD abcdef1234567890abcdef1234567890abcdef12
branch refs/heads/main

worktree /repo/detached
HEAD deadbeef1234567890deadbeef1234567890deaa
detached

";
        let out = parse_worktree_porcelain(porcelain);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].branch, "detached@deadbee");
        assert_eq!(
            out[1].head_sha.as_deref(),
            Some("deadbeef1234567890deadbeef1234567890deaa")
        );
    }

    #[test]
    fn parse_worktree_porcelain_skips_bare() {
        let porcelain = "\
worktree /repo/main
HEAD abcdef1234567890abcdef1234567890abcdef12
branch refs/heads/main

worktree /repo/bare
bare

";
        let out = parse_worktree_porcelain(porcelain);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, PathBuf::from("/repo/main"));
    }

    #[test]
    fn detect_worktrees_fallback_when_no_git() {
        let tmp = tempfile::tempdir().unwrap();
        // No .git dir → `git worktree list` will fail; the fallback
        // should still produce a single entry pointing at the root.
        let out = detect_worktrees(tmp.path());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, tmp.path());
        assert_eq!(out[0].branch, "default");
    }

    /// Two-worktree end-to-end: a real `git init` + `git worktree add`
    /// pair gets registered, and indexing populates two snapshots.
    #[tokio::test]
    async fn registers_all_worktrees_and_indexes_each() {
        // Skip when `git` is unavailable; the rest of the suite has
        // already covered the parser unit-side.
        if std::process::Command::new("git")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        let work = tempfile::tempdir().unwrap();
        let main_root = work.path().join("repo");
        std::fs::create_dir_all(main_root.join("src")).unwrap();
        std::fs::write(main_root.join("src").join("lib.rs"), "fn one() {}\n").unwrap();

        // Initialise a real git repo so `git worktree add` works.
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .arg("-C")
                .arg(&main_root)
                .args(args)
                .output()
                .unwrap()
        };
        let _ = git(&["init", "-q", "-b", "main"]);
        let _ = git(&["config", "user.email", "t@example.com"]);
        let _ = git(&["config", "user.name", "t"]);
        let _ = git(&["add", "."]);
        let _ = git(&["commit", "-q", "-m", "initial"]);
        let _ = git(&["branch", "feature"]);
        let secondary = work.path().join("repo-feature");
        let wt_out = git(&["worktree", "add", secondary.to_str().unwrap(), "feature"]);
        // If `git worktree add` failed for any reason (e.g. an
        // unusual environment), skip — the parser unit tests above
        // already cover the porcelain shape.
        if !wt_out.status.success() {
            return;
        }

        let data_dir = DataDir::with_root(work.path().join("cc"));
        let storage = Arc::new(Storage::open(data_dir).unwrap());
        let backends: Vec<Box<dyn LanguageBackend>> = vec![Box::new(cairn_lang_rust::RustBackend)];
        let indexer = Indexer::with_backends(storage.clone(), backends);

        let registered = indexer.register_repo("demo", &main_root).await.unwrap();
        // Primary handle points at the main worktree.
        assert_eq!(registered.branch, "main");

        // Both worktrees are recorded in the registry.
        let worktrees = storage
            .with_registry({
                let repo_id = registered.repo_id;
                move |conn| registry_db::list_worktrees(conn, repo_id)
            })
            .await
            .unwrap();
        assert_eq!(worktrees.len(), 2);

        // full_index covers both snapshots.
        let stats = indexer.full_index("demo").await.unwrap();
        assert!(stats.files_indexed >= 2, "got {stats:?}");

        // active_snapshot resolves to the main worktree's branch.
        let handle = indexer.active_snapshot("demo").await.unwrap();
        assert_eq!(handle.branch, "main");
    }
}
