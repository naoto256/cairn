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

use std::path::{Path, PathBuf};
use std::process::Command;

use cairn_lang_api::{LanguageBackend, all_backends, pick_backend_for_path};
use rusqlite::{Connection, OptionalExtension, params};
use tracing::{debug, warn};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas;
use crate::manifest::{self, ManifestEntry, ManifestId};

/// Per-parser revision baseline. Bumped when a backend's output for
/// the same input changes in a way that should invalidate already-
/// stored blob entries. Per-`parser_id` so a Rust-parser bump does
/// not invalidate Python blobs.
pub const PARSER_REVISION: u32 = 1;

/// Outcome of a successful `register_repo` call.
#[derive(Debug, Clone)]
pub struct RegisterOutcome {
    pub worktree_id: i64,
    pub head_commit: String,
    pub branch: Option<String>,
    pub committed_manifest: ManifestId,
    pub tentative_manifest: ManifestId,
    /// Number of `(blob, parser)` pairs that were parsed fresh
    /// (= not reused from a prior call).
    pub blobs_parsed: usize,
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
    let backends = all_backends();
    let include = |path: &str| pick_backend_for_path(&backends, path).is_some();

    let head_commit = run_git_capture(worktree_path, &["rev-parse", "HEAD"])?;
    let branch = detect_branch(worktree_path);

    let tx = conn.transaction()?;

    let worktree_id = upsert_worktree(&tx, worktree_path, now_ns)?;

    // Manifest construction — reuse a committed manifest if one
    // already exists for this commit.
    let committed = match manifest::lookup_by_commit_sha(&tx, &head_commit)? {
        Some(id) => id,
        None => manifest::build_from_git_tree(&tx, worktree_path, &head_commit, now_ns, include)?,
    };

    let tentative = manifest::build_from_worktree(&tx, worktree_path, now_ns, include)?;

    // Anchors.
    anchor::set(&tx, &AnchorName::head(), committed, now_ns)?;
    if let Some(name) = &branch {
        anchor::set(&tx, &AnchorName::branch(name), committed, now_ns)?;
    }
    anchor::set(&tx, &AnchorName::tentative(worktree_id), tentative, now_ns)?;

    // Collect every (blob_sha, source) pair we need to ensure is
    // parsed. Source tells us where to fetch the content from:
    // committed blobs come from git, tentative-only blobs from the
    // worktree.
    let committed_entries = manifest::get_entries(&tx, committed)?;
    let tentative_entries = manifest::get_entries(&tx, tentative)?;
    tx.commit()?;

    let blobs_parsed = parse_pending_blobs(
        conn,
        worktree_path,
        &backends,
        &committed_entries,
        &tentative_entries,
        now_ns,
    )?;

    Ok(RegisterOutcome {
        worktree_id,
        head_commit,
        branch,
        committed_manifest: committed,
        tentative_manifest: tentative,
        blobs_parsed,
    })
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn parse_pending_blobs(
    conn: &mut Connection,
    worktree_path: &Path,
    backends: &[Box<dyn LanguageBackend>],
    committed: &[ManifestEntry],
    tentative: &[ManifestEntry],
    now_ns: i64,
) -> Result<usize> {
    // For each blob, prefer a worktree source when the same blob_sha
    // shows up in the tentative manifest (= file content is on
    // disk). Otherwise fall back to `git cat-file` for committed-
    // only blobs.
    use std::collections::HashMap;

    let mut tentative_path_by_blob: HashMap<&str, &Path> = HashMap::new();
    for e in tentative {
        tentative_path_by_blob
            .entry(e.blob_sha.as_str())
            .or_insert_with(|| Path::new(e.path.as_str()));
    }

    let mut work_units: Vec<(String, String, ContentSource)> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for entry in committed.iter().chain(tentative.iter()) {
        let Some(backend) = pick_backend_for_path(backends, &entry.path) else {
            continue;
        };
        let parser_id = backend.parser_id().to_string();
        if !seen.insert((entry.blob_sha.clone(), parser_id.clone())) {
            continue;
        }
        let source = match tentative_path_by_blob.get(entry.blob_sha.as_str()) {
            Some(rel) => ContentSource::Worktree(worktree_path.join(rel)),
            None => ContentSource::Git(entry.blob_sha.clone()),
        };
        work_units.push((entry.blob_sha.clone(), parser_id, source));
    }

    let mut fresh = 0;
    for (blob_sha, parser_id, source) in work_units {
        let backend = backends
            .iter()
            .find(|b| b.parser_id() == parser_id)
            .expect("parser_id was just produced by a registered backend")
            .as_ref();
        let was_fresh = cas::blob::reuse_or_compute(
            conn,
            &blob_sha,
            &parser_id,
            PARSER_REVISION,
            now_ns,
            || {
                let content = match &source {
                    ContentSource::Worktree(p) => std::fs::read(p)?,
                    ContentSource::Git(sha) => git_cat_file(worktree_path, sha)?,
                };
                cas::parse::parse(backend, &content)
                    .map_err(|e| crate::Error::InvalidArgument(format!("parse failed: {e}")))
            },
        )?;
        if was_fresh {
            fresh += 1;
        } else {
            debug!(blob_sha, parser_id, "reused parsed_data");
        }
    }
    Ok(fresh)
}

enum ContentSource {
    Worktree(PathBuf),
    Git(String),
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

fn run_git_capture(repo_root: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()
        .map_err(|e| crate::Error::InvalidArgument(format!("git invocation failed: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        warn!(args = ?args, stderr = %stderr.trim(), "git command non-zero");
        return Err(crate::Error::InvalidArgument(format!(
            "git {args:?}: {}",
            stderr.trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub(crate) fn git_cat_file(repo_root: &Path, blob_sha: &str) -> std::io::Result<Vec<u8>> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["cat-file", "-p", blob_sha])
        .output()?;
    if !out.status.success() {
        return Err(std::io::Error::other(format!(
            "git cat-file {blob_sha}: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(out.stdout)
}

/// Load the bytes the indexer parsed for `(blob_sha, path)`.
///
/// Tries `git cat-file <blob_sha>` first (= the authoritative blob the
/// index was built from). If that fails — typically because the blob
/// only exists in the worktree under a tentative anchor — falls back
/// to reading `worktree_root.join(path)` straight off disk.
///
/// Wire methods that need source-line context (`get_symbol_source`,
/// `find_references` snippets) share this lookup; keeping it in one
/// place avoids the two callers' fallbacks drifting apart.
pub(crate) fn load_blob_or_worktree(
    worktree_root: &Path,
    blob_sha: &str,
    path: &str,
) -> std::io::Result<Vec<u8>> {
    git_cat_file(worktree_root, blob_sha).or_else(|_| std::fs::read(worktree_root.join(path)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use crate::testutil::init_repo;
    use std::fs;

    fn init_rust_repo(files: &[(&str, &str)]) -> tempfile::TempDir {
        let (tmp, _sha) = init_repo(files);
        tmp
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

        // A symbol named `greet` was indexed against some blob in the
        // committed manifest.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols
                 WHERE name = 'greet' AND parser_id LIKE 'tree-sitter-rust@%'",
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
}
