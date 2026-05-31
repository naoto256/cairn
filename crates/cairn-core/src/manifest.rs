//! Manifest layer: `{ (path, blob_sha) }` snapshots that sit between
//! anchors and the blob-keyed parsed data.
//!
//! Two construction paths:
//! - `build_from_git_tree`: shell out to `git ls-tree -r -z <commit>`
//!   and persist `(path, blob_sha)` pairs that match the caller's
//!   inclusion predicate (= "this path is something a backend
//!   handles").
//! - `build_from_worktree`: walk the worktree honoring `.gitignore`
//!   via `cairn_watch::scan::walk_repo`, hash each file with
//!   `cas::hash::git_blob_sha`, and persist the pairs.
//!
//! Both produce a fresh `manifest_id`. Reuse across commits is the
//! caller's responsibility — `lookup_by_commit_sha` exposes the
//! check.

use std::path::Path;
use std::process::Command;

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;
use crate::cas::git_blob_sha;

/// Stable identity of a committed manifest row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ManifestId(pub i64);

/// Manifest variants. `Committed` corresponds to a git commit;
/// `Tentative` reflects the current worktree state and may diverge
/// from any commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestKind {
    Committed,
    Tentative,
}

impl ManifestKind {
    fn as_str(self) -> &'static str {
        match self {
            ManifestKind::Committed => "committed",
            ManifestKind::Tentative => "tentative",
        }
    }
}

/// One `(path, blob_sha)` pair from a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub path: String,
    pub blob_sha: String,
}

/// Look up an existing committed manifest for `commit_sha`. Returns
/// `Ok(None)` if none exists.
///
/// # Errors
/// SQLite failure.
pub fn lookup_by_commit_sha(conn: &Connection, commit_sha: &str) -> Result<Option<ManifestId>> {
    let id = conn
        .query_row(
            "SELECT manifest_id FROM manifests
             WHERE kind = 'committed' AND commit_sha = ?1
             ORDER BY built_at_ns DESC LIMIT 1",
            params![commit_sha],
            |r| r.get::<_, i64>(0).map(ManifestId),
        )
        .optional()?;
    Ok(id)
}

/// Build a `Committed` manifest from a git tree. Shells out to `git
/// ls-tree -r -z <commit>` in `repo_root`. Only paths for which
/// `include(path)` returns `true` are persisted.
///
/// # Errors
/// Returns [`crate::Error::InvalidArgument`] if the git command fails
/// (commit doesn't exist, not a git repo, etc.), and SQLite errors
/// otherwise.
pub fn build_from_git_tree<F>(
    tx: &Transaction<'_>,
    repo_root: &Path,
    commit_sha: &str,
    built_at_ns: i64,
    include: F,
) -> Result<ManifestId>
where
    F: Fn(&str) -> bool,
{
    let entries = list_git_tree(repo_root, commit_sha)?;
    let id = create_empty(tx, ManifestKind::Committed, Some(commit_sha), built_at_ns)?;
    insert_entries(tx, id, entries.into_iter().filter(|e| include(&e.path)))?;
    Ok(id)
}

/// Build a `Tentative` manifest from the current worktree contents.
/// Uses `cairn_watch::scan::walk_repo` (= `.gitignore`-aware) and
/// computes the git-style blob sha for each included file.
///
/// # Errors
/// Filesystem errors (file vanished between walk and read) propagate
/// as `crate::Error::Io`; SQLite errors propagate as
/// `crate::Error::Sqlite`.
pub fn build_from_worktree<F>(
    tx: &Transaction<'_>,
    worktree_path: &Path,
    built_at_ns: i64,
    include: F,
) -> Result<ManifestId>
where
    F: Fn(&str) -> bool,
{
    let id = create_empty(tx, ManifestKind::Tentative, None, built_at_ns)?;

    let entries: Vec<ManifestEntry> = cairn_watch::scan::walk_repo(worktree_path)
        .filter_map(|sf| {
            let rel = sf.path.strip_prefix(worktree_path).ok()?.to_owned();
            let rel_str = rel.to_string_lossy().into_owned();
            if !include(&rel_str) {
                return None;
            }
            // Slow path: read + hash. Worktree-only files (= not in
            // any git commit) get a freshly-computed blob_sha equal
            // to what `git add` would produce.
            let content = std::fs::read(&sf.path).ok()?;
            Some(ManifestEntry {
                path: rel_str,
                blob_sha: git_blob_sha(&content),
            })
        })
        .collect();
    insert_entries(tx, id, entries.into_iter())?;
    Ok(id)
}

/// Return the manifest's `(path, blob_sha)` pairs, sorted by path.
///
/// # Errors
/// SQLite failure.
pub fn get_entries(conn: &Connection, id: ManifestId) -> Result<Vec<ManifestEntry>> {
    let mut stmt = conn.prepare(
        "SELECT path, blob_sha FROM manifest_entries
         WHERE manifest_id = ?1 ORDER BY path",
    )?;
    let rows: rusqlite::Result<Vec<ManifestEntry>> = stmt
        .query_map(params![id.0], |r| {
            Ok(ManifestEntry {
                path: r.get(0)?,
                blob_sha: r.get(1)?,
            })
        })?
        .collect();
    Ok(rows?)
}

/// Look up the blob sha for a single path in this manifest.
///
/// # Errors
/// SQLite failure.
pub fn get_blob_for_path(conn: &Connection, id: ManifestId, path: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT blob_sha FROM manifest_entries
             WHERE manifest_id = ?1 AND path = ?2",
            params![id.0, path],
            |r| r.get::<_, String>(0),
        )
        .optional()?)
}

/// Upsert a single `(path, blob_sha)` entry. Used by the watcher on
/// tentative manifests when a file changes.
///
/// # Errors
/// SQLite failure.
pub fn upsert_path(tx: &Transaction<'_>, id: ManifestId, path: &str, blob_sha: &str) -> Result<()> {
    tx.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(manifest_id, path) DO UPDATE SET blob_sha = excluded.blob_sha",
        params![id.0, path, blob_sha],
    )?;
    Ok(())
}

/// Remove a single path from the manifest. Used by the watcher on
/// tentative manifests when a file is deleted.
///
/// # Errors
/// SQLite failure.
pub fn delete_path(tx: &Transaction<'_>, id: ManifestId, path: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM manifest_entries WHERE manifest_id = ?1 AND path = ?2",
        params![id.0, path],
    )?;
    Ok(())
}

/// Remove the manifest and its entries. The caller must remove any
/// anchors pointing at this manifest first (the FK is `RESTRICT`).
///
/// # Errors
/// SQLite failure including FK violation when anchors still reference.
pub fn delete_manifest(tx: &Transaction<'_>, id: ManifestId) -> Result<()> {
    tx.execute(
        "DELETE FROM manifests WHERE manifest_id = ?1",
        params![id.0],
    )?;
    Ok(())
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn create_empty(
    tx: &Transaction<'_>,
    kind: ManifestKind,
    commit_sha: Option<&str>,
    built_at_ns: i64,
) -> Result<ManifestId> {
    tx.execute(
        "INSERT INTO manifests (kind, commit_sha, built_at_ns)
         VALUES (?1, ?2, ?3)",
        params![kind.as_str(), commit_sha, built_at_ns],
    )?;
    Ok(ManifestId(tx.last_insert_rowid()))
}

fn insert_entries<I: IntoIterator<Item = ManifestEntry>>(
    tx: &Transaction<'_>,
    id: ManifestId,
    entries: I,
) -> Result<()> {
    let mut stmt = tx.prepare(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
         VALUES (?1, ?2, ?3)",
    )?;
    for e in entries {
        stmt.execute(params![id.0, e.path, e.blob_sha])?;
    }
    Ok(())
}

fn list_git_tree(repo_root: &Path, commit_sha: &str) -> Result<Vec<ManifestEntry>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("ls-tree")
        .arg("-r")
        .arg("-z")
        .arg(commit_sha)
        .output()
        .map_err(|e| crate::Error::InvalidArgument(format!("git ls-tree failed: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(crate::Error::InvalidArgument(format!(
            "git ls-tree {commit_sha}: {}",
            stderr.trim()
        )));
    }
    parse_ls_tree(&output.stdout)
}

fn parse_ls_tree(stdout: &[u8]) -> Result<Vec<ManifestEntry>> {
    let mut out = Vec::new();
    // Each record is `<mode> <type> <sha>\t<path>\0`.
    for record in stdout.split(|b| *b == 0) {
        if record.is_empty() {
            continue;
        }
        let record = std::str::from_utf8(record)
            .map_err(|e| crate::Error::InvalidArgument(format!("non-utf8 ls-tree output: {e}")))?;
        let Some((meta, path)) = record.split_once('\t') else {
            continue;
        };
        let mut parts = meta.split_whitespace();
        let Some(_mode) = parts.next() else { continue };
        let Some(obj_type) = parts.next() else {
            continue;
        };
        let Some(sha) = parts.next() else { continue };
        if obj_type != "blob" {
            // Skip submodules (type=commit). Tree entries don't
            // appear with -r.
            continue;
        }
        out.push(ManifestEntry {
            path: path.to_string(),
            blob_sha: sha.to_string(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use crate::testutil::init_repo;
    use std::fs;

    fn fresh_db() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&tmp.path().join("store.db")).unwrap();
        (tmp, conn)
    }

    #[test]
    fn parse_ls_tree_handles_records() {
        let stdout = b"100644 blob abc123\tsrc/lib.rs\x00100755 blob def456\tbin/run\x00";
        let parsed = parse_ls_tree(stdout).unwrap();
        assert_eq!(
            parsed,
            vec![
                ManifestEntry {
                    path: "src/lib.rs".into(),
                    blob_sha: "abc123".into()
                },
                ManifestEntry {
                    path: "bin/run".into(),
                    blob_sha: "def456".into()
                },
            ]
        );
    }

    #[test]
    fn parse_ls_tree_skips_submodules() {
        let stdout = b"160000 commit aaaa\tvendor/sub\x00100644 blob bbbb\tREADME\x00";
        let parsed = parse_ls_tree(stdout).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].path, "README");
    }

    #[test]
    fn build_from_git_tree_persists_filtered_entries() {
        let (_repo_tmp, commit) = init_repo(&[
            ("src/lib.rs", "fn x() {}\n"),
            ("README.md", "# title\n"),
            ("vendor/big.bin", "binary\n"),
        ]);
        let (_db_tmp, mut c) = fresh_db();

        let tx = c.transaction().unwrap();
        let id = build_from_git_tree(&tx, _repo_tmp.path(), &commit, 100, |p| {
            p.ends_with(".rs") || p.ends_with(".md")
        })
        .unwrap();
        tx.commit().unwrap();

        let entries = get_entries(&c, id).unwrap();
        assert_eq!(entries.len(), 2);
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["README.md", "src/lib.rs"]);
        // blob_sha must match git's own hash-object for the same content.
        let lib_entry = entries.iter().find(|e| e.path == "src/lib.rs").unwrap();
        assert_eq!(lib_entry.blob_sha, git_blob_sha(b"fn x() {}\n"));
    }

    #[test]
    fn build_from_git_tree_errors_on_bad_commit() {
        let (_repo_tmp, _commit) = init_repo(&[("a.rs", "x")]);
        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let err = build_from_git_tree(&tx, _repo_tmp.path(), "deadbeef", 0, |_| true).unwrap_err();
        let s = err.to_string();
        assert!(s.contains("git ls-tree"), "unexpected error: {s}");
    }

    #[test]
    fn lookup_by_commit_sha_finds_built_manifest() {
        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let id = create_empty(&tx, ManifestKind::Committed, Some("aaaa1111"), 0).unwrap();
        tx.commit().unwrap();
        assert_eq!(lookup_by_commit_sha(&c, "aaaa1111").unwrap(), Some(id));
        assert_eq!(lookup_by_commit_sha(&c, "ffff0000").unwrap(), None);
    }

    #[test]
    fn build_from_worktree_walks_and_hashes() {
        let tmp = tempfile::tempdir().unwrap();
        let wt = tmp.path();
        // Create a small worktree (no .git needed since walk_repo
        // sets require_git(false)).
        fs::write(wt.join("a.rs"), "fn a() {}\n").unwrap();
        fs::create_dir(wt.join("sub")).unwrap();
        fs::write(wt.join("sub/b.rs"), "fn b() {}\n").unwrap();
        fs::write(wt.join("ignore.txt"), "skip me\n").unwrap();

        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let id = build_from_worktree(&tx, wt, 0, |p| p.ends_with(".rs")).unwrap();
        tx.commit().unwrap();

        let entries = get_entries(&c, id).unwrap();
        assert_eq!(entries.len(), 2);
        let a_entry = entries.iter().find(|e| e.path == "a.rs").unwrap();
        assert_eq!(a_entry.blob_sha, git_blob_sha(b"fn a() {}\n"));
    }

    #[test]
    fn upsert_path_inserts_and_updates() {
        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let id = create_empty(&tx, ManifestKind::Tentative, None, 0).unwrap();
        upsert_path(&tx, id, "src/lib.rs", "sha1").unwrap();
        upsert_path(&tx, id, "src/lib.rs", "sha2").unwrap();
        tx.commit().unwrap();
        assert_eq!(
            get_blob_for_path(&c, id, "src/lib.rs").unwrap().unwrap(),
            "sha2"
        );
    }

    #[test]
    fn delete_path_removes_only_one_entry() {
        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let id = create_empty(&tx, ManifestKind::Tentative, None, 0).unwrap();
        upsert_path(&tx, id, "a", "sha1").unwrap();
        upsert_path(&tx, id, "b", "sha2").unwrap();
        delete_path(&tx, id, "a").unwrap();
        tx.commit().unwrap();
        let entries = get_entries(&c, id).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "b");
    }

    #[test]
    fn delete_manifest_cascades_entries() {
        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let id = create_empty(&tx, ManifestKind::Tentative, None, 0).unwrap();
        upsert_path(&tx, id, "a", "sha").unwrap();
        delete_manifest(&tx, id).unwrap();
        tx.commit().unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM manifest_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn get_blob_for_path_returns_none_for_missing() {
        let (_db_tmp, mut c) = fresh_db();
        let tx = c.transaction().unwrap();
        let id = create_empty(&tx, ManifestKind::Tentative, None, 0).unwrap();
        tx.commit().unwrap();
        assert_eq!(get_blob_for_path(&c, id, "nope").unwrap(), None);
    }
}
