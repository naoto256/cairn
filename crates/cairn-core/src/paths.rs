//! Storage layout on disk.
//!
//! Two layouts share the same root during the CAS rewrite: the
//! legacy per-snapshot tree
//! (`registry.db` + `indexes/<repo_hash>/<worktree_hash>/<branch>.db`)
//! served by [`DataDir`], and the content-addressed layout
//! (`repos/<repo_hash>/store.db`) served by [`CasDataDir`]. The
//! legacy layout is unused at runtime once everything flips; deleting
//! its files is safe and reclaims the space.
//!
//! ```text
//! <data_dir>/
//! ├── registry.db                  legacy
//! ├── indexes/                     legacy
//! │   └── <repo_hash>/<worktree_hash>/<branch>.db
//! └── repos/                       CAS
//!     └── <repo_hash>/store.db
//! ```
//!
//! The chosen `<data_dir>` follows the platform convention:
//! - macOS: `~/Library/Application Support/cairn/`
//! - Linux: `$XDG_DATA_HOME/cairn/` (or `~/.local/share/cairn/`)
//!
//! Hashes are stable derivations of canonicalized paths so a registered
//! repo's storage directory does not depend on a transient registry id.

use std::path::{Path, PathBuf};

use crate::{Error, Result};

const APP_NAME: &str = "cairn";

/// Resolved storage root. Constructed once at daemon startup and passed
/// down to anything that needs to open a DB.
#[derive(Debug, Clone)]
pub struct DataDir {
    root: PathBuf,
}

impl DataDir {
    /// Construct from the platform default location.
    ///
    /// # Errors
    /// Fails if the platform does not expose a user data directory.
    pub fn from_platform_default() -> Result<Self> {
        let base = dirs::data_dir()
            .ok_or_else(|| Error::InvalidArgument("platform has no user data directory".into()))?;
        Ok(Self {
            root: base.join(APP_NAME),
        })
    }

    /// Construct with an explicit root (useful for tests and overrides).
    #[must_use]
    pub fn with_root(root: PathBuf) -> Self {
        Self { root }
    }

    /// Ensure all top-level directories exist on disk.
    ///
    /// # Errors
    /// Propagates filesystem errors.
    pub fn ensure(&self) -> Result<()> {
        std::fs::create_dir_all(self.indexes_dir())?;
        Ok(())
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn registry_db_path(&self) -> PathBuf {
        self.root.join("registry.db")
    }

    #[must_use]
    pub fn indexes_dir(&self) -> PathBuf {
        self.root.join("indexes")
    }

    /// Directory housing a repository's snapshots.
    #[must_use]
    pub fn repo_dir(&self, repo_hash: &str) -> PathBuf {
        self.indexes_dir().join(repo_hash)
    }

    /// Directory for one worktree under a repository.
    #[must_use]
    pub fn worktree_dir(&self, repo_hash: &str, worktree_hash: &str) -> PathBuf {
        self.repo_dir(repo_hash).join(worktree_hash)
    }

    /// Path of the data DB for one `(worktree, branch)` snapshot.
    ///
    /// The branch name is sanitized so values like `feature/x` map to
    /// `feature__x.db` on disk; the original branch name is recorded in
    /// `registry.db` and is the source of truth.
    #[must_use]
    pub fn snapshot_db_path(&self, repo_hash: &str, worktree_hash: &str, branch: &str) -> PathBuf {
        let safe = sanitize_branch_for_filename(branch);
        self.worktree_dir(repo_hash, worktree_hash)
            .join(format!("{safe}.db"))
    }
}

/// Resolved storage root for the CAS layout (sibling of [`DataDir`];
/// they share the same root, so a single `cairn-ng` directory holds
/// both schemas).
#[derive(Debug, Clone)]
pub struct CasDataDir {
    root: PathBuf,
}

impl CasDataDir {
    /// Construct from the platform default location.
    ///
    /// # Errors
    /// Fails if the platform does not expose a user data directory.
    pub fn from_platform_default() -> Result<Self> {
        let base = dirs::data_dir()
            .ok_or_else(|| Error::InvalidArgument("platform has no user data directory".into()))?;
        Ok(Self {
            root: base.join(APP_NAME),
        })
    }

    /// Construct with an explicit root (useful for tests and overrides).
    #[must_use]
    pub fn with_root(root: PathBuf) -> Self {
        Self { root }
    }

    /// Ensure the `repos/` subdirectory exists.
    ///
    /// # Errors
    /// Propagates filesystem errors.
    pub fn ensure(&self) -> Result<()> {
        std::fs::create_dir_all(self.repos_dir())?;
        Ok(())
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn repos_dir(&self) -> PathBuf {
        self.root.join("repos")
    }

    /// Per-repo directory under `repos/`.
    #[must_use]
    pub fn repo_dir(&self, repo_hash: &str) -> PathBuf {
        self.repos_dir().join(repo_hash)
    }

    /// Per-repo CAS store DB.
    #[must_use]
    pub fn store_db_path(&self, repo_hash: &str) -> PathBuf {
        self.repo_dir(repo_hash).join("store.db")
    }

    /// Top-level alias → repo index DB (sits next to `repos/`, not
    /// under it).
    #[must_use]
    pub fn index_db_path(&self) -> PathBuf {
        self.root.join("index.db")
    }
}

/// Stable hash of an absolute, canonicalized path. The function is
/// deliberately not cryptographic — it just needs to be consistent across
/// runs and unlikely to collide for the ~hundreds of repositories one
/// user actually registers.
#[must_use]
pub fn path_hash(path: &Path) -> String {
    // 64-bit FNV-1a, hex-encoded. Pure Rust, deterministic, zero deps.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in path.as_os_str().as_encoded_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    format!("{hash:016x}")
}

fn sanitize_branch_for_filename(branch: &str) -> String {
    branch
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' | '@' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_compose_under_root() {
        let dir = DataDir::with_root(PathBuf::from("/tmp/c"));
        assert_eq!(dir.registry_db_path(), PathBuf::from("/tmp/c/registry.db"));
        assert_eq!(dir.indexes_dir(), PathBuf::from("/tmp/c/indexes"));
        assert_eq!(dir.repo_dir("abc"), PathBuf::from("/tmp/c/indexes/abc"));
        assert_eq!(
            dir.worktree_dir("abc", "wt"),
            PathBuf::from("/tmp/c/indexes/abc/wt")
        );
    }

    #[test]
    fn snapshot_db_path_sanitizes_branch() {
        let dir = DataDir::with_root(PathBuf::from("/tmp/c"));
        let path = dir.snapshot_db_path("repo", "wt", "feature/x");
        assert!(path.to_string_lossy().ends_with("feature_x.db"));
    }

    #[test]
    fn snapshot_db_path_keeps_safe_chars() {
        let dir = DataDir::with_root(PathBuf::from("/tmp/c"));
        let path = dir.snapshot_db_path("repo", "wt", "detached@a3f9c1d");
        assert!(path.to_string_lossy().ends_with("detached@a3f9c1d.db"));
    }

    #[test]
    fn path_hash_is_stable() {
        let a = path_hash(Path::new("/tmp/proj"));
        let b = path_hash(Path::new("/tmp/proj"));
        assert_eq!(a, b);
    }

    #[test]
    fn path_hash_distinguishes_different_paths() {
        let a = path_hash(Path::new("/tmp/proj"));
        let b = path_hash(Path::new("/tmp/other"));
        assert_ne!(a, b);
    }

    #[test]
    fn ensure_creates_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = DataDir::with_root(tmp.path().join("cairn"));
        dir.ensure().unwrap();
        assert!(dir.indexes_dir().is_dir());
    }

    #[test]
    fn cas_paths_compose_under_root() {
        let dir = CasDataDir::with_root(PathBuf::from("/tmp/c"));
        assert_eq!(dir.repos_dir(), PathBuf::from("/tmp/c/repos"));
        assert_eq!(dir.repo_dir("abc"), PathBuf::from("/tmp/c/repos/abc"));
        assert_eq!(
            dir.store_db_path("abc"),
            PathBuf::from("/tmp/c/repos/abc/store.db")
        );
    }

    #[test]
    fn cas_ensure_creates_repos_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = CasDataDir::with_root(tmp.path().join("cas"));
        dir.ensure().unwrap();
        assert!(dir.repos_dir().is_dir());
    }

    #[test]
    fn cas_and_legacy_share_root() {
        let root = PathBuf::from("/tmp/shared");
        let legacy = DataDir::with_root(root.clone());
        let cas = CasDataDir::with_root(root);
        assert_eq!(legacy.root(), cas.root());
    }
}
