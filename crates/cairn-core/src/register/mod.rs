//! New `register_repo` orchestration over the CAS / manifest / anchor
//! layers.
//!
//! Stands alongside the original `indexer::register_repo` so the
//! daemon can flip over one entry point at a time. Given an open
//! store connection plus a worktree path, this function:
//!
//! 1. Inserts the worktree row (idempotent).
//! 2. Resolves the worktree's HEAD commit and branch name via git.
//! 3. Builds (or reuses) a `Committed` manifest for the HEAD commit.
//! 4. Builds a `Tentative` manifest from the current worktree.
//! 5. Parses every blob referenced by either manifest that lacks
//!    parsed_data for its inferred parser, reading committed-blob
//!    content via `git cat-file` and worktree-blob content from
//!    disk.
//! 6. Sets `HEAD`, `branch/<name>` (if on a branch), and
//!    `tentative/<worktree_id>` anchors to the right manifests.

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;

use cairn_lang_api::{LanguageBackend, all_backends, pick_backend_for_path};
use rusqlite::{Connection, OptionalExtension, params};
use tracing::debug;

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas;
use crate::jobs::{EnqueueReindex, JobManager, QueuedAnalyzerJob};
use crate::manifest::{
    self, ManifestEntry, ManifestId, ManifestKind, PathHint, WorktreeFilePayload,
};
use crate::workspace_analyzer::{
    check_workspace_analyzer_current_succeeded, is_c_family_header_path,
    pick_backend_with_fallbacks, run_registered_workspace_analyzers,
};

/// Outcome of a successful `register_repo` call.
#[derive(Debug, Clone)]
pub struct RegisterOutcome {
    pub worktree_id: i64,
    pub head_commit: String,
    pub branch: Option<String>,
    pub committed_manifest: ManifestId,
    pub tentative_manifest: ManifestId,
    /// `true` iff the workspace analyzer pass was skipped because the
    /// tentative manifest entries were byte-identical to the prior
    /// tentative, no blobs were re-parsed by the pre-publication parse pass,
    /// and every expected analyzer already has a `succeeded` run row
    /// at the current `analyzer_revision`. All three gates must hold
    /// — entries-unchanged alone is not enough to skip (it would let
    /// a parser_revision bump leave the resolutions table stale under
    /// a reused `manifest_id`).
    pub skip_analyzers_for_unchanged_manifest: bool,
    pub analyzer_jobs: Vec<QueuedAnalyzerJob>,
    /// Number of `(blob, parser)` pairs that were parsed fresh
    /// (= not reused from a prior call).
    pub blobs_parsed: usize,
    /// Present only when a reconcile attempt atomically stamped the
    /// tentative anchor with its durable generation proof.
    pub publication: Option<ReconcilePublicationReceipt>,
}

/// Durable store-side proof produced by one reconcile publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilePublicationReceipt {
    pub anchor: AnchorName,
    pub manifest_id: ManifestId,
    pub generation: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReconcileRegistration<'a> {
    pub alias: &'a str,
    pub repo_hash: &'a str,
    pub worktree_path: &'a Path,
    pub now_ns: i64,
    pub generation: i64,
    pub forced: bool,
}

#[derive(Debug, Clone, Copy)]
enum RegistrationPublication {
    Direct { dedupe_unchanged_tentative: bool },
    Reconcile { generation: i64, forced: bool },
}

impl RegistrationPublication {
    fn dedupe_unchanged_tentative(self) -> bool {
        match self {
            Self::Direct {
                dedupe_unchanged_tentative,
            } => dedupe_unchanged_tentative,
            Self::Reconcile { forced, .. } => !forced,
        }
    }

    fn generation(self) -> Option<i64> {
        match self {
            Self::Direct { .. } => None,
            Self::Reconcile { generation, .. } => Some(generation),
        }
    }
}

/// Register a worktree against the open CAS store.
///
/// `now_ns` lets tests pin a stable timestamp; live callers pass the
/// current wall-clock nanos.
///
/// # Errors
/// Returns [`crate::Error::InvalidArgument`] if git invocation fails
/// (not a repo, no commits yet); SQLite or IO errors otherwise.
pub fn register_repo(
    conn: &mut Connection,
    worktree_path: &Path,
    now_ns: i64,
) -> Result<RegisterOutcome> {
    register_repo_inner(
        conn,
        worktree_path,
        now_ns,
        true,
        |conn, repo_root, manifest_id, entries, now_ns| {
            let _inserted =
                run_registered_workspace_analyzers(conn, repo_root, manifest_id, entries, now_ns)?;
            Ok(Vec::new())
        },
    )
}

/// Force a full analyzer pass for an already-registered repo.
///
/// This backs the explicit `reindex_repo` control method; unlike the
/// watcher/default path, it does not short-circuit unchanged
/// tentative manifests.
pub(crate) fn register_repo_force_analyzers(
    conn: &mut Connection,
    worktree_path: &Path,
    now_ns: i64,
) -> Result<RegisterOutcome> {
    register_repo_inner(
        conn,
        worktree_path,
        now_ns,
        false,
        |conn, repo_root, manifest_id, entries, now_ns| {
            let _inserted =
                run_registered_workspace_analyzers(conn, repo_root, manifest_id, entries, now_ns)?;
            Ok(Vec::new())
        },
    )
}

pub(crate) fn register_repo_enqueue_analyzers(
    conn: &mut Connection,
    alias: &str,
    repo_hash: &str,
    worktree_path: &Path,
    now_ns: i64,
    job_manager: &JobManager,
) -> Result<RegisterOutcome> {
    register_repo_inner(
        conn,
        worktree_path,
        now_ns,
        true,
        |conn, repo_root, manifest_id, entries, now_ns| {
            job_manager.enqueue_reindex(EnqueueReindex {
                conn,
                alias,
                repo_hash,
                repo_root,
                manifest_id,
                entries,
                now_ns,
            })
        },
    )
}

pub(crate) fn register_repo_force_analyzers_enqueue(
    conn: &mut Connection,
    alias: &str,
    repo_hash: &str,
    worktree_path: &Path,
    now_ns: i64,
    job_manager: &JobManager,
) -> Result<RegisterOutcome> {
    register_repo_inner(
        conn,
        worktree_path,
        now_ns,
        false,
        |conn, repo_root, manifest_id, entries, now_ns| {
            job_manager.enqueue_reindex(EnqueueReindex {
                conn,
                alias,
                repo_hash,
                repo_root,
                manifest_id,
                entries,
                now_ns,
            })
        },
    )
}

pub(crate) fn register_repo_reconcile_enqueue_analyzers(
    conn: &mut Connection,
    request: ReconcileRegistration<'_>,
    job_manager: &JobManager,
) -> Result<RegisterOutcome> {
    register_repo_inner_with_publication(
        conn,
        request.worktree_path,
        request.now_ns,
        RegistrationPublication::Reconcile {
            generation: request.generation,
            forced: request.forced,
        },
        |conn, repo_root, manifest_id, entries, now_ns| {
            job_manager.enqueue_reindex(EnqueueReindex {
                conn,
                alias: request.alias,
                repo_hash: request.repo_hash,
                repo_root,
                manifest_id,
                entries,
                now_ns,
            })
        },
    )
}

pub(crate) fn register_repo_reconcile(
    conn: &mut Connection,
    worktree_path: &Path,
    now_ns: i64,
    generation: i64,
    forced: bool,
) -> Result<RegisterOutcome> {
    register_repo_inner_with_publication(
        conn,
        worktree_path,
        now_ns,
        RegistrationPublication::Reconcile { generation, forced },
        |conn, repo_root, manifest_id, entries, now_ns| {
            let _inserted =
                run_registered_workspace_analyzers(conn, repo_root, manifest_id, entries, now_ns)?;
            Ok(Vec::new())
        },
    )
}

fn register_repo_inner<F>(
    conn: &mut Connection,
    worktree_path: &Path,
    now_ns: i64,
    dedupe_unchanged_tentative: bool,
    run_analyzers: F,
) -> Result<RegisterOutcome>
where
    F: FnMut(
        &mut Connection,
        &Path,
        ManifestId,
        &[ManifestEntry],
        i64,
    ) -> Result<Vec<QueuedAnalyzerJob>>,
{
    register_repo_inner_with_publication(
        conn,
        worktree_path,
        now_ns,
        RegistrationPublication::Direct {
            dedupe_unchanged_tentative,
        },
        run_analyzers,
    )
}

fn register_repo_inner_with_publication<F>(
    conn: &mut Connection,
    worktree_path: &Path,
    now_ns: i64,
    publication: RegistrationPublication,
    mut run_analyzers: F,
) -> Result<RegisterOutcome>
where
    F: FnMut(
        &mut Connection,
        &Path,
        ManifestId,
        &[ManifestEntry],
        i64,
    ) -> Result<Vec<QueuedAnalyzerJob>>,
{
    let dedupe_unchanged_tentative = publication.dedupe_unchanged_tentative();
    let backends = all_backends();
    let include = |hint: &PathHint<'_>| {
        if pick_backend_for_path(&backends, hint.path).is_some() {
            return true;
        }
        if is_c_family_header_path(hint.path) {
            return true;
        }
        hint.is_executable
    };

    let head_commit = run_git_capture(worktree_path, &["rev-parse", "HEAD"])?;
    let branch = detect_branch(worktree_path);

    // Complete every source read and Tier-1 parse before opening the
    // manifest/anchor publication transaction. Parsed blobs are
    // content-addressed and may safely warm the cache if a later file fails;
    // no snapshot points at them until the transaction below commits.
    let existing_committed = manifest::lookup_by_commit_sha(conn, &head_commit)?;
    let committed_entries = match existing_committed {
        Some(id) => manifest::get_entries(conn, id)?,
        None => manifest::collect_git_tree(worktree_path, &head_commit, include)?,
    };
    let mut parse_progress = ParseProgress::default();
    let tentative_entries = manifest::capture_worktree(worktree_path, include, |payload| {
        parse_worktree_payload(conn, &backends, payload, now_ns, &mut parse_progress)
    })?;
    parse_committed_entries(
        conn,
        worktree_path,
        &backends,
        &committed_entries,
        now_ns,
        &mut parse_progress,
    )?;
    let blobs_parsed = parse_progress.fresh;

    let tx = conn.transaction()?;
    let worktree_id = upsert_worktree(&tx, worktree_path, now_ns)?;
    let tentative_anchor = AnchorName::tentative(worktree_id);
    let prior_tentative = anchor::resolve(&tx, &tentative_anchor)?;

    // Manifest construction — reuse a committed manifest if one
    // already exists for this commit.
    let committed = match existing_committed {
        Some(id) => id,
        None => manifest::persist_manifest(
            &tx,
            ManifestKind::Committed,
            Some(&head_commit),
            now_ns,
            &committed_entries,
        )?,
    };

    let built_tentative = manifest::persist_manifest(
        &tx,
        ManifestKind::Tentative,
        None,
        now_ns,
        &tentative_entries,
    )?;

    // Gate #1 (manifest reuse): tentative entries byte-identical to
    // the prior tentative manifest. When this holds we drop the
    // freshly-built tentative and keep the prior `manifest_id`. This
    // is *only* about not allocating a new manifest row — it does
    // NOT, on its own, mean facts under that manifest_id are
    // current. v11's `resolutions.manifest_id ON DELETE CASCADE`
    // turned a stale fact-set under a reused manifest_id into an
    // invisible-version-skew bug (Tier-2.5 facts lost on file touch
    // after parser_revision bump), so the analyzer-skip decision is
    // now gated separately below.
    let entries_unchanged = dedupe_unchanged_tentative
        && match prior_tentative {
            Some(prior) => manifest::get_entries(&tx, prior)? == tentative_entries,
            None => false,
        };
    let (tentative, tentative_entries) = if entries_unchanged {
        // Safe: entries_unchanged is only true if prior_tentative was Some.
        let prior = prior_tentative.expect("entries_unchanged implies prior_tentative is Some");
        manifest::delete_manifest(&tx, built_tentative)?;
        debug!(
            repo = %worktree_path.display(),
            manifest_id = prior.0,
            "tentative manifest unchanged; reusing prior manifest"
        );
        (prior, tentative_entries)
    } else {
        (built_tentative, tentative_entries)
    };

    // Anchors.
    anchor::set(&tx, &AnchorName::head(), committed, now_ns)?;
    if let Some(name) = &branch {
        anchor::set(&tx, &AnchorName::branch(name), committed, now_ns)?;
    }
    prune_stale_ref_anchors(
        &tx,
        "branch/",
        &live_ref_names(worktree_path, "refs/heads")?,
    )?;
    prune_stale_ref_anchors(&tx, "tag/", &live_ref_names(worktree_path, "refs/tags")?)?;
    let publication_receipt = match publication.generation() {
        Some(generation) => {
            anchor::set_reconciled(&tx, &tentative_anchor, tentative, now_ns, generation)?;
            Some(ReconcilePublicationReceipt {
                anchor: tentative_anchor.clone(),
                manifest_id: tentative,
                generation,
            })
        }
        None => {
            anchor::set(&tx, &tentative_anchor, tentative, now_ns)?;
            None
        }
    };

    tx.commit()?;

    // Gate #2 (blobs re-parsed): the truth-source for "Tier-1 facts
    // moved under this manifest" is the pre-publication parse pass. Any of
    // (a) a new blob, (b) a parser_revision bump on an existing
    // blob, or (c) a missing parsed_data row triggers `blobs_parsed
    // > 0`. We use that signal directly instead of re-deriving a
    // parser-drift check, so the two paths cannot drift apart.
    //
    // Order matters: parsing must finish before publication and before the
    // skip decision. Running it after would observe a post-skip state and let
    // drift slip through.
    // Gate #3 (analyzer revision current): every expected analyzer
    // has a `succeeded` workspace_analysis_runs row at the current
    // linked-in `revision()`. queued / running / failed / skipped /
    // cancelled / timed_out all count as "not current" so a
    // half-finished pass cannot masquerade as up-to-date — the
    // misleading-state symptom that motivated this fix.
    let analyzer_current = if entries_unchanged && blobs_parsed == 0 {
        check_workspace_analyzer_current_succeeded(conn, tentative)?
    } else {
        // Skip the DB probe when the cheap gates already disqualify
        // skipping. Saves the SELECT in the common churn path.
        false
    };

    let skip_analyzers_for_unchanged_manifest =
        entries_unchanged && blobs_parsed == 0 && analyzer_current;

    let analyzer_jobs = if skip_analyzers_for_unchanged_manifest {
        debug!(
            repo = %worktree_path.display(),
            manifest_id = tentative.0,
            "workspace analyzer pass skipped; facts current"
        );
        Vec::new()
    } else {
        debug!(
            repo = %worktree_path.display(),
            manifest_id = tentative.0,
            entries_unchanged,
            blobs_parsed,
            analyzer_current,
            "workspace analyzer pass forced"
        );
        run_analyzers(conn, worktree_path, tentative, &tentative_entries, now_ns)?
    };

    Ok(RegisterOutcome {
        worktree_id,
        head_commit,
        branch,
        committed_manifest: committed,
        tentative_manifest: tentative,
        skip_analyzers_for_unchanged_manifest,
        analyzer_jobs,
        blobs_parsed,
        publication: publication_receipt,
    })
}

// ─── helpers ───────────────────────────────────────────────────────────────

#[derive(Default)]
struct ParseProgress {
    seen: HashSet<(String, String)>,
    fresh: usize,
}

fn parse_worktree_payload(
    conn: &mut Connection,
    backends: &[Box<dyn LanguageBackend>],
    payload: WorktreeFilePayload<'_>,
    now_ns: i64,
    progress: &mut ParseProgress,
) -> Result<()> {
    let Some(backend) = pick_backend_for_path(backends, &payload.entry.path)
        .or_else(|| pick_backend_with_fallbacks(backends, &payload.entry.path, payload.bytes))
    else {
        return Ok(());
    };
    parse_borrowed(
        conn,
        payload.entry,
        backend,
        payload.bytes,
        now_ns,
        progress,
    )
}

fn parse_committed_entries(
    conn: &mut Connection,
    repo_root: &Path,
    backends: &[Box<dyn LanguageBackend>],
    entries: &[ManifestEntry],
    now_ns: i64,
    progress: &mut ParseProgress,
) -> Result<()> {
    for entry in entries {
        if let Some(backend) = pick_backend_for_path(backends, &entry.path) {
            let parser_id = backend.parser_id().to_string();
            if !progress
                .seen
                .insert((entry.blob_sha.clone(), parser_id.clone()))
            {
                continue;
            }
            let analyzer = backend.analyzer();
            let expected_analyzer = analyzer.as_deref().map(|a| (a.name(), a.revision()));
            let was_fresh = cas::blob::reuse_or_compute(
                conn,
                &entry.blob_sha,
                &parser_id,
                backend.parser_revision(),
                expected_analyzer,
                now_ns,
                || {
                    let bytes = git_cat_file(repo_root, &entry.blob_sha)?;
                    parse_bytes(backend, &bytes)
                },
            )?;
            progress.record_result(&entry.blob_sha, &parser_id, was_fresh);
            continue;
        }

        let bytes = git_cat_file(repo_root, &entry.blob_sha)?;
        let Some(backend) = pick_backend_with_fallbacks(backends, &entry.path, &bytes) else {
            continue;
        };
        parse_borrowed(conn, entry, backend, &bytes, now_ns, progress)?;
    }
    Ok(())
}

fn parse_borrowed(
    conn: &mut Connection,
    entry: &ManifestEntry,
    backend: &dyn LanguageBackend,
    bytes: &[u8],
    now_ns: i64,
    progress: &mut ParseProgress,
) -> Result<()> {
    let parser_id = backend.parser_id().to_string();
    if !progress
        .seen
        .insert((entry.blob_sha.clone(), parser_id.clone()))
    {
        return Ok(());
    }
    let analyzer = backend.analyzer();
    let expected_analyzer = analyzer.as_deref().map(|a| (a.name(), a.revision()));
    let was_fresh = cas::blob::reuse_or_compute(
        conn,
        &entry.blob_sha,
        &parser_id,
        backend.parser_revision(),
        expected_analyzer,
        now_ns,
        || parse_bytes(backend, bytes),
    )?;
    progress.record_result(&entry.blob_sha, &parser_id, was_fresh);
    Ok(())
}

fn parse_bytes(backend: &dyn LanguageBackend, bytes: &[u8]) -> Result<cas::blob::ParsedData> {
    cas::parse::parse(backend, bytes)
        .map_err(|err| crate::Error::InvalidArgument(format!("parse failed: {err}")))
}

impl ParseProgress {
    fn record_result(&mut self, blob_sha: &str, parser_id: &str, was_fresh: bool) {
        if was_fresh {
            self.fresh += 1;
        } else {
            debug!(blob_sha, parser_id, "reused parsed_data");
        }
    }
}

fn upsert_worktree(tx: &rusqlite::Transaction<'_>, path: &Path, now_ns: i64) -> Result<i64> {
    let path_str = path.to_string_lossy().to_string();
    if let Some(id) = tx
        .query_row(
            "SELECT worktree_id FROM worktrees WHERE path = ?1",
            params![path_str],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
    {
        return Ok(id);
    }
    tx.execute(
        "INSERT INTO worktrees (path, registered_at_ns) VALUES (?1, ?2)",
        params![path_str, now_ns],
    )?;
    Ok(tx.last_insert_rowid())
}

fn detect_branch(repo_root: &Path) -> Option<String> {
    let raw = run_git_capture(repo_root, &["symbolic-ref", "--quiet", "HEAD"]).ok()?;
    // raw looks like `refs/heads/main`
    raw.strip_prefix("refs/heads/").map(str::to_string)
}

fn live_ref_names(repo_root: &Path, namespace: &str) -> Result<HashSet<String>> {
    let refs = run_git_capture(
        repo_root,
        &["for-each-ref", "--format=%(refname:strip=2)", namespace],
    )?;
    Ok(refs
        .lines()
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect())
}

fn prune_stale_ref_anchors(
    tx: &rusqlite::Transaction<'_>,
    prefix: &str,
    live_refs: &HashSet<String>,
) -> Result<usize> {
    let mut pruned = 0;
    for anchor in anchor::list_prefix(tx, prefix)? {
        let Some(name) = anchor.name.as_str().strip_prefix(prefix) else {
            continue;
        };
        if !live_refs.contains(name) && anchor::delete(tx, &anchor.name)? {
            pruned += 1;
            debug!(anchor = %anchor.name.as_str(), "pruned stale git ref anchor");
        }
    }
    Ok(pruned)
}

fn run_git_capture(repo_root: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()
        .map_err(|e| crate::Error::InvalidArgument(format!("git invocation failed: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        debug!(args = ?args, stderr = %stderr.trim(), "git command non-zero");
        return Err(crate::Error::InvalidArgument(format!(
            "git {args:?}: {}",
            stderr.trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn is_git_sha1(blob_sha: &str) -> bool {
    blob_sha.len() == 40
        && blob_sha
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

pub(crate) fn git_cat_file(repo_root: &Path, blob_sha: &str) -> Result<Vec<u8>> {
    if !is_git_sha1(blob_sha) {
        // Manifest blob IDs are process input at the RPC/register
        // boundary. An allowlist keeps future git invocation changes
        // from turning object names into argument syntax.
        return Err(crate::Error::InvalidArgument(
            "invalid git blob sha: expected 40 hex characters".into(),
        ));
    }
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["cat-file", "-p", "--", blob_sha])
        .output()
        .map_err(crate::Error::Io)?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        debug!(blob_sha, stderr = %stderr.trim(), "git cat-file non-zero");
        return Err(crate::Error::InvalidArgument(format!(
            "git cat-file failed for blob {blob_sha}"
        )));
    }
    Ok(out.stdout)
}

/// Load the bytes the indexer parsed for `(blob_sha, path)`.
///
/// Tries `git cat-file <blob_sha>` first (= the authoritative blob the
/// index was built from). If that fails — typically because the blob
/// only exists in the worktree under a tentative anchor — falls back
/// to one immutable read of `worktree_root.join(path)` and verifies that
/// payload against `blob_sha` before returning it.
///
/// Wire methods that need source-line context (`get_symbol_source`,
/// `find_references` snippets) share this lookup; keeping it in one
/// place avoids the two callers' fallbacks drifting apart.
pub(crate) fn load_blob_or_verified_worktree(
    worktree_root: &Path,
    blob_sha: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    if !is_git_sha1(blob_sha) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid git blob sha: expected 40 hex characters",
        ));
    }
    if let Ok(bytes) = git_cat_file(worktree_root, blob_sha) {
        return Ok(bytes);
    }

    let bytes = std::fs::read(worktree_root.join(path))?;
    if crate::cas::hash::git_blob_sha(&bytes) != blob_sha {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "worktree content does not match indexed blob sha",
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use crate::testutil::{init_repo, run_git};
    use cairn_lang_c as _;
    use cairn_lang_cpp as _;
    use cairn_lang_objc as _;
    use cairn_lang_python::PythonBackend;
    use cairn_lang_rust as _;
    use std::cell::Cell;
    use std::fs;

    fn python_backends() -> Vec<Box<dyn LanguageBackend>> {
        vec![Box::new(PythonBackend)]
    }

    fn init_rust_repo(files: &[(&str, &str)]) -> tempfile::TempDir {
        let (tmp, _sha) = init_repo(files);
        tmp
    }

    #[test]
    fn verified_worktree_fallback_accepts_matching_immutable_payload() {
        let tmp = tempfile::tempdir().unwrap();
        let bytes = b"pub fn tentative() {}\n";
        fs::create_dir_all(tmp.path().join("src")).unwrap();
        fs::write(tmp.path().join("src/lib.rs"), bytes).unwrap();
        let blob_sha = crate::cas::hash::git_blob_sha(bytes);

        let loaded = load_blob_or_verified_worktree(tmp.path(), &blob_sha, "src/lib.rs").unwrap();

        assert_eq!(loaded, bytes);
    }

    #[test]
    fn verified_worktree_fallback_rejects_bytes_changed_after_indexing() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("src")).unwrap();
        fs::write(tmp.path().join("src/lib.rs"), b"pub fn changed() {}\n").unwrap();
        let indexed_sha = crate::cas::hash::git_blob_sha(b"pub fn indexed() {}\n");

        let err =
            load_blob_or_verified_worktree(tmp.path(), &indexed_sha, "src/lib.rs").unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn committed_blob_wins_over_changed_worktree_bytes() {
        let original = b"pub fn committed() {}\n";
        let repo = init_rust_repo(&[("src/lib.rs", std::str::from_utf8(original).unwrap())]);
        fs::write(repo.path().join("src/lib.rs"), b"pub fn changed() {}\n").unwrap();
        let blob_sha = crate::cas::hash::git_blob_sha(original);

        let loaded = load_blob_or_verified_worktree(repo.path(), &blob_sha, "src/lib.rs").unwrap();

        assert_eq!(loaded, original);
    }

    fn parser_ids_for_path(conn: &Connection, manifest_id: ManifestId, path: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(
                "SELECT b.parser_id
                 FROM manifest_entries me
                 JOIN blobs b ON b.blob_sha = me.blob_sha
                 WHERE me.manifest_id = ?1 AND me.path = ?2
                 ORDER BY b.parser_id",
            )
            .unwrap();
        stmt.query_map(params![manifest_id.0, path], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn end_to_end_register_persists_anchors_and_blobs() {
        let repo = init_rust_repo(&[(
            "src/lib.rs",
            "pub fn greet(name: &str) -> String { format!(\"hi {name}\") }\n",
        )]);

        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 1000).unwrap();
        assert_eq!(outcome.branch.as_deref(), Some("main"));
        assert!(outcome.blobs_parsed >= 1, "no blob parsed");

        // HEAD and branch/main anchors both point at the committed
        // manifest.
        let head = anchor::resolve(&conn, &AnchorName::head())
            .unwrap()
            .unwrap();
        let br = anchor::resolve(&conn, &AnchorName::branch("main"))
            .unwrap()
            .unwrap();
        assert_eq!(head, outcome.committed_manifest);
        assert_eq!(br, outcome.committed_manifest);

        let tent = anchor::resolve(&conn, &AnchorName::tentative(outcome.worktree_id))
            .unwrap()
            .unwrap();
        assert_eq!(tent, outcome.tentative_manifest);
        assert!(outcome.publication.is_none());
        assert_eq!(
            anchor::get(&conn, &AnchorName::tentative(outcome.worktree_id))
                .unwrap()
                .unwrap()
                .reconcile_generation,
            None
        );

        // A symbol named `greet` was indexed against some blob in the
        // committed manifest.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols
                 WHERE name = 'greet' AND parser_id = 'tree-sitter-rust'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(count >= 1, "greet symbol not indexed");
    }

    #[test]
    fn second_register_reuses_blobs() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let first = register_repo(&mut conn, repo.path(), 1).unwrap();
        let second = register_repo(&mut conn, repo.path(), 2).unwrap();
        assert!(first.blobs_parsed >= 1);
        assert_eq!(
            second.blobs_parsed, 0,
            "expected reuse on second call, got {} fresh",
            second.blobs_parsed
        );
        // Worktree row should be the same; upsert is idempotent.
        assert_eq!(first.worktree_id, second.worktree_id);
    }

    #[test]
    fn reconcile_register_atomically_stamps_tentative_generation() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo_reconcile(&mut conn, repo.path(), 10, 7, false).unwrap();
        let receipt = outcome.publication.unwrap();
        let durable = anchor::get(&conn, &receipt.anchor).unwrap().unwrap();

        assert_eq!(receipt.manifest_id, outcome.tentative_manifest);
        assert_eq!(receipt.generation, 7);
        assert_eq!(durable.manifest_id, receipt.manifest_id);
        assert_eq!(durable.reconcile_generation, Some(7));
    }

    #[test]
    fn unchanged_reconcile_reuses_manifest_but_advances_generation_receipt() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let first = register_repo_reconcile(&mut conn, repo.path(), 10, 3, false).unwrap();
        let second = register_repo_reconcile(&mut conn, repo.path(), 20, 4, false).unwrap();

        assert_eq!(first.tentative_manifest, second.tentative_manifest);
        let receipt = second.publication.unwrap();
        assert_eq!(receipt.generation, 4);
        assert_eq!(
            anchor::get(&conn, &receipt.anchor)
                .unwrap()
                .unwrap()
                .reconcile_generation,
            Some(4)
        );
    }

    #[test]
    fn analyzer_failure_after_publication_leaves_durable_receipt() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let err = register_repo_inner_with_publication(
            &mut conn,
            repo.path(),
            10,
            RegistrationPublication::Reconcile {
                generation: 8,
                forced: false,
            },
            |_, _, _, _, _| Err(crate::Error::Internal("enqueue failed".into())),
        )
        .unwrap_err();

        assert!(err.to_string().contains("enqueue failed"));
        let tentative = anchor::list_prefix(&conn, "tentative/").unwrap();
        assert_eq!(tentative.len(), 1);
        assert_eq!(tentative[0].reconcile_generation, Some(8));
    }

    #[test]
    fn incomplete_scan_preserves_published_anchors_and_skips_analyzers() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn old() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let analyzer_runs = Cell::new(0);

        let first = register_repo_inner(&mut conn, repo.path(), 1, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();
        let head_before = anchor::resolve(&conn, &AnchorName::head()).unwrap();
        let tentative_before =
            anchor::resolve(&conn, &AnchorName::tentative(first.worktree_id)).unwrap();
        let manifests_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM manifests", [], |row| row.get(0))
            .unwrap();

        fs::write(repo.path().join("src/lib.rs"), "pub fn new() {}\n").unwrap();
        fs::write(repo.path().join(".gitignore"), [0xff]).unwrap();
        let err = register_repo_inner(&mut conn, repo.path(), 2, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap_err();

        assert!(matches!(err, crate::Error::Scan(_)));
        assert_eq!(
            anchor::resolve(&conn, &AnchorName::head()).unwrap(),
            head_before
        );
        assert_eq!(
            anchor::resolve(&conn, &AnchorName::tentative(first.worktree_id)).unwrap(),
            tentative_before
        );
        let manifests_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM manifests", [], |row| row.get(0))
            .unwrap();
        assert_eq!(manifests_after, manifests_before);
        assert_eq!(analyzer_runs.get(), 1);
    }

    #[test]
    fn unchanged_tentative_manifest_skips_analyzer_pass_on_second_register() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let analyzer_runs = Cell::new(0);

        let first = register_repo_inner(&mut conn, repo.path(), 1, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();
        let second = register_repo_inner(&mut conn, repo.path(), 2, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();

        assert!(!first.skip_analyzers_for_unchanged_manifest);
        assert!(second.skip_analyzers_for_unchanged_manifest);
        assert_eq!(second.tentative_manifest, first.tentative_manifest);
        assert_eq!(analyzer_runs.get(), 1);
    }

    /// F-A regression: when blobs were re-parsed (e.g. parser_revision
    /// bumped on a freshly upgraded binary), the second register call
    /// must NOT skip the workspace analyzer pass even though the
    /// tentative manifest entries are byte-identical to the prior
    /// tentative.
    ///
    /// We simulate "blob re-parse fired" by deleting the parsed-blob
    /// rows between the two registers. The pre-publication parse pass will then
    /// re-compute them and return `blobs_parsed > 0`, which is the
    /// truth-source the skip gate watches. Manifest entries stay
    /// identical because they reference the same `blob_sha` values.
    #[test]
    fn register_repo_forces_analyzer_when_blobs_parsed_gt_zero() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let analyzer_runs = Cell::new(0);

        let first = register_repo_inner(&mut conn, repo.path(), 1, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();
        assert!(!first.skip_analyzers_for_unchanged_manifest);
        assert!(first.blobs_parsed >= 1);

        // Wipe the parsed-blob rows so the next register re-parses
        // everything. This is the test-double for "Tier-1 parser_revision
        // bumped on a binary upgrade" — same manifest entries, different
        // parser output → CAS invalidation → blobs_parsed > 0.
        conn.execute("DELETE FROM blobs", []).unwrap();

        let second = register_repo_inner(&mut conn, repo.path(), 2, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();

        assert!(
            !second.skip_analyzers_for_unchanged_manifest,
            "blobs_parsed > 0 must force the analyzer pass even when \
             entries are unchanged"
        );
        assert!(
            second.blobs_parsed >= 1,
            "expected re-parse on second register, got {} fresh",
            second.blobs_parsed
        );
        assert_eq!(
            analyzer_runs.get(),
            2,
            "analyzer must run twice when blobs were re-parsed"
        );
    }

    /// F-A direct regression pin: after a parser_revision bump
    /// (modeled by wiping `blobs` between calls), the second register
    /// must force the workspace analyzer pass so any resolutions /
    /// Tier-2.5 facts tied to the reused `manifest_id` get an
    /// opportunity to repopulate at the new revision.
    ///
    /// Before this fix, the dedup shortcut skipped the analyzer pass
    /// on the second register because tentative entries matched,
    /// leaving the resolutions table tied to the old fact-set version
    /// under the reused `manifest_id`. v11's `resolutions ON DELETE
    /// CASCADE` made the skew silent (no orphan detection in queries).
    ///
    /// We assert two things at the register-layer boundary: (1) the
    /// `tentative_manifest` is reused (same id as first call), so the
    /// dedup gate is still working; and (2) the analyzer is still
    /// forced to run. Together they pin the post-fix contract: dedup
    /// the manifest, don't dedup the analyzer pass.
    #[test]
    fn register_repo_forces_analyzer_after_revision_bump_f_a_regression() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let analyzer_runs = Cell::new(0);

        let first = register_repo_inner(&mut conn, repo.path(), 1, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();
        assert!(!first.skip_analyzers_for_unchanged_manifest);
        assert_eq!(analyzer_runs.get(), 1);

        // Simulate Tier-1 parser_revision bump: wipe parsed-blob rows.
        // The pre-publication parse pass on the next register will re-parse and
        // return `blobs_parsed > 0` — the truth-source the skip gate
        // watches. Manifest entries stay identical because the worktree
        // is unchanged.
        conn.execute("DELETE FROM blobs", []).unwrap();

        let second = register_repo_inner(&mut conn, repo.path(), 2, true, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();

        // Manifest dedup still works: same id reused.
        assert_eq!(
            second.tentative_manifest, first.tentative_manifest,
            "tentative manifest_id must be reused when entries match"
        );
        // Analyzer pass NOT skipped: the gate now picks up blobs_parsed.
        assert!(
            !second.skip_analyzers_for_unchanged_manifest,
            "blobs_parsed > 0 after revision bump must force the analyzer pass"
        );
        assert!(second.blobs_parsed >= 1);
        assert_eq!(
            analyzer_runs.get(),
            2,
            "F-A regression: analyzer must re-run when blobs were re-parsed \
             even though manifest_id is reused"
        );
    }

    #[test]
    fn force_reindex_does_not_dedupe_unchanged_tentative_manifest() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let analyzer_runs = Cell::new(0);

        let first = register_repo_inner(&mut conn, repo.path(), 1, false, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();
        let second = register_repo_inner(&mut conn, repo.path(), 2, false, |_, _, _, _, _| {
            analyzer_runs.set(analyzer_runs.get() + 1);
            Ok(Vec::new())
        })
        .unwrap();

        assert!(!first.skip_analyzers_for_unchanged_manifest);
        assert!(!second.skip_analyzers_for_unchanged_manifest);
        assert_ne!(second.tentative_manifest, first.tentative_manifest);
        assert_eq!(
            second.blobs_parsed, 0,
            "force reindex bypasses manifest/analyzer dedupe but retains the Tier-1 CAS"
        );
        assert_eq!(analyzer_runs.get(), 2);
    }

    #[test]
    fn re_register_prunes_deleted_branch_anchor() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        run_git(repo.path(), &["checkout", "-q", "-b", "gone"]);
        fs::write(
            repo.path().join("src/lib.rs"),
            "pub fn f() {}\npub fn g() {}\n",
        )
        .unwrap();
        run_git(repo.path(), &["add", "-A"]);
        run_git(repo.path(), &["commit", "-q", "-m", "probe branch"]);

        let branch_outcome = register_repo(&mut conn, repo.path(), 1).unwrap();
        assert_eq!(branch_outcome.branch.as_deref(), Some("gone"));
        assert!(
            anchor::resolve(&conn, &AnchorName::branch("gone"))
                .unwrap()
                .is_some()
        );
        run_git(repo.path(), &["tag", "v-stale"]);
        {
            let tx = conn.transaction().unwrap();
            anchor::set(
                &tx,
                &AnchorName::tag("v-stale"),
                branch_outcome.committed_manifest,
                1,
            )
            .unwrap();
            tx.commit().unwrap();
        }
        assert!(
            anchor::resolve(&conn, &AnchorName::tag("v-stale"))
                .unwrap()
                .is_some()
        );

        run_git(repo.path(), &["checkout", "-q", "main"]);
        run_git(repo.path(), &["branch", "-D", "gone"]);
        run_git(repo.path(), &["tag", "-d", "v-stale"]);

        let main_outcome = register_repo(&mut conn, repo.path(), 2).unwrap();
        assert_eq!(main_outcome.branch.as_deref(), Some("main"));
        assert!(
            anchor::resolve(&conn, &AnchorName::branch("gone"))
                .unwrap()
                .is_none()
        );
        assert!(
            anchor::resolve(&conn, &AnchorName::tag("v-stale"))
                .unwrap()
                .is_none()
        );
        assert!(
            anchor::resolve(&conn, &AnchorName::head())
                .unwrap()
                .is_some()
        );
        assert!(
            anchor::resolve(&conn, &AnchorName::branch("main"))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn worktree_only_file_lands_in_tentative_only() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        // Add an unstaged file before register.
        fs::write(repo.path().join("src/extra.rs"), "pub fn g() {}\n").unwrap();

        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        let committed = manifest::get_entries(&conn, outcome.committed_manifest).unwrap();
        let tentative = manifest::get_entries(&conn, outcome.tentative_manifest).unwrap();
        let comm_paths: Vec<&str> = committed.iter().map(|e| e.path.as_str()).collect();
        let tent_paths: Vec<&str> = tentative.iter().map(|e| e.path.as_str()).collect();
        assert!(comm_paths.contains(&"src/lib.rs"));
        assert!(!comm_paths.contains(&"src/extra.rs"));
        assert!(tent_paths.contains(&"src/extra.rs"));

        // The extra file's content was parsed too (= fresh blob).
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM symbols WHERE name = 'g'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(count >= 1);
    }

    #[test]
    fn dot_h_with_cpp_signals_routes_to_cpp_parser() {
        let repo = init_rust_repo(&[(
            "include/widget.h",
            "#include <vector>\nnamespace app { class Widget {}; }\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        assert_eq!(
            parser_ids_for_path(&conn, outcome.tentative_manifest, "include/widget.h"),
            vec!["tree-sitter-cpp"]
        );
    }

    #[test]
    fn dot_h_without_cpp_signals_routes_to_c_parser() {
        let repo = init_rust_repo(&[(
            "include/api.h",
            "typedef struct widget { int id; } widget_t;\nint widget_id(widget_t *w);\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        assert_eq!(
            parser_ids_for_path(&conn, outcome.tentative_manifest, "include/api.h"),
            vec!["tree-sitter-c"]
        );
    }

    #[test]
    fn dot_h_cpp_signals_in_comments_and_strings_do_not_route_to_cpp() {
        let repo = init_rust_repo(&[(
            "include/plain.h",
            "/* namespace app { class Widget {}; } */\n\
             const char *text = \"#include <vector>\";\n\
             typedef struct widget { int id; } widget_t;\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        assert_eq!(
            parser_ids_for_path(&conn, outcome.tentative_manifest, "include/plain.h"),
            vec!["tree-sitter-c"]
        );
    }

    #[test]
    fn dot_h_with_objc_interface_routes_to_objc_parser() {
        // AFNetworking-shaped header: ObjC `@interface` with protocol
        // conformance list. Must route to the objc backend, not cpp /
        // c, so the type-hierarchy edges are emitted.
        let repo = init_rust_repo(&[(
            "AFNetworking/AFHTTPSessionManager.h",
            "#import <Foundation/Foundation.h>\n\
             @interface AFHTTPSessionManager : AFURLSessionManager <NSSecureCoding, NSCopying>\n\
             @end\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        assert_eq!(
            parser_ids_for_path(
                &conn,
                outcome.tentative_manifest,
                "AFNetworking/AFHTTPSessionManager.h"
            ),
            vec!["tree-sitter-objc"]
        );
    }

    #[test]
    fn dot_h_with_objc_import_alone_routes_to_objc_parser() {
        // `#import "Local.h"` with no `@interface` is still ObjC-idiomatic
        // (a forward header that just re-exports), and pure C / C++ use
        // `#include`. Route to objc.
        let repo = init_rust_repo(&[(
            "include/forward.h",
            "#import \"OtherModule.h\"\n#import <UIKit/UIKit.h>\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        assert_eq!(
            parser_ids_for_path(&conn, outcome.tentative_manifest, "include/forward.h"),
            vec!["tree-sitter-objc"]
        );
    }

    #[test]
    fn dot_h_objc_priority_beats_cpp_signals() {
        // A header carrying both ObjC directives and incidental C++ish
        // identifiers (e.g. a project that uses `class` as a property
        // name or an inline doc snippet) must route to objc — the ObjC
        // signal is unambiguous and outranks the heuristic C++ check.
        let repo = init_rust_repo(&[(
            "include/Mixed.h",
            "@interface Foo : NSObject\n\
             @property (nonatomic, strong) NSString *operatorName;\n\
             @end\n\
             // namespace and template appear in comments only\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();

        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        assert_eq!(
            parser_ids_for_path(&conn, outcome.tentative_manifest, "include/Mixed.h"),
            vec!["tree-sitter-objc"]
        );
    }

    #[test]
    #[cfg(unix)]
    fn register_skips_worktree_symlink_to_outside_file() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("leak.rs"), "pub fn leaked() {}\n").unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("leak.rs"),
            repo.path().join("src/leak.rs"),
        )
        .unwrap();

        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        let tentative = manifest::get_entries(&conn, outcome.tentative_manifest).unwrap();
        assert!(
            !tentative.iter().any(|e| e.path == "src/leak.rs"),
            "worktree symlink must not become a tentative blob"
        );
        let leaked_symbols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = 'leaked'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leaked_symbols, 0);
    }

    #[test]
    fn git_cat_file_rejects_invalid_blob_sha_before_invoking_git() {
        let repo = init_rust_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        for sha in [
            "; rm -rf",
            "abc123",
            "01234567890123456789012345678901234567zz",
        ] {
            let err = git_cat_file(repo.path(), sha).unwrap_err();
            assert!(matches!(err, crate::Error::InvalidArgument(_)));
        }
    }

    #[test]
    fn shebang_fallback_picks_python_env_script() {
        let backends = python_backends();
        let backend = pick_backend_with_fallbacks(
            &backends,
            "bin/foo",
            b"#!/usr/bin/env python3\nprint('hi')\n",
        )
        .unwrap();
        assert_eq!(backend.parser_id(), "tree-sitter-python");
    }

    #[test]
    fn shebang_fallback_picks_uv_inline_script() {
        let backends = python_backends();
        let backend = pick_backend_with_fallbacks(
            &backends,
            "bin/foo",
            b"#!/usr/bin/env -S uv run --script\nprint('hi')\n",
        )
        .unwrap();
        assert_eq!(backend.parser_id(), "tree-sitter-python");
    }

    #[test]
    fn shebang_fallback_rejects_shell_script() {
        let backends = python_backends();
        assert!(
            pick_backend_with_fallbacks(&backends, "bin/foo", b"#!/bin/bash\necho hi\n").is_none()
        );
    }

    #[test]
    fn shebang_fallback_rejects_extensionless_without_shebang() {
        let backends = python_backends();
        assert!(pick_backend_with_fallbacks(&backends, "bin/foo", b"\x7fELF").is_none());
    }

    #[test]
    fn shebang_fallback_keeps_path_based_match() {
        let backends = python_backends();
        let backend = pick_backend_with_fallbacks(&backends, "foo.py", b"print('hi')\n").unwrap();
        assert_eq!(backend.parser_id(), "tree-sitter-python");
    }

    #[test]
    #[cfg(unix)]
    fn end_to_end_indexes_uv_inline_script() {
        use std::os::unix::fs::PermissionsExt;

        let (repo, _sha) = init_repo(&[(
            "bin/myscript",
            "#!/usr/bin/env -S uv run --script\n# /// script\n# requires-python = '>=3.11'\n# ///\ndef greet():\n    return 'hi'\n",
        )]);
        let path = repo.path().join("bin/myscript");
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();

        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let outcome = register_repo(&mut conn, repo.path(), 1000).unwrap();

        assert!(outcome.blobs_parsed >= 1);
        let entries = manifest::get_entries(&conn, outcome.tentative_manifest).unwrap();
        assert!(
            entries.iter().any(|e| e.path == "bin/myscript"),
            "bin/myscript missing from tentative manifest: {entries:#?}"
        );

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = 'greet'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(count >= 1);
    }

    #[test]
    #[cfg(unix)]
    fn end_to_end_skips_non_shebang_extensionless() {
        use std::os::unix::fs::PermissionsExt;

        let (repo, _sha) = init_repo(&[("bin/data", "not a script\n")]);
        let path = repo.path().join("bin/data");
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();

        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let outcome = register_repo(&mut conn, repo.path(), 1000).unwrap();

        assert_eq!(outcome.blobs_parsed, 0);
        let entries = manifest::get_entries(&conn, outcome.tentative_manifest).unwrap();
        assert!(entries.iter().any(|e| e.path == "bin/data"));
    }

    #[test]
    fn end_to_end_does_not_regress_python_py_files() {
        let (repo, _sha) = init_repo(&[("foo.py", "def greet():\n    return 'hi'\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let outcome = register_repo(&mut conn, repo.path(), 1000).unwrap();

        assert!(outcome.blobs_parsed >= 1);
        let entries = manifest::get_entries(&conn, outcome.committed_manifest).unwrap();
        assert!(entries.iter().any(|e| e.path == "foo.py"));

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE name = 'greet'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(count >= 1);
    }
}
