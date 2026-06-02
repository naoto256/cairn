//! Startup full-scan reconciliation.
//!
//! Filesystem watchers — `FSEvents` in particular — drop events under
//! load. Re-running the entire `(path, mtime, size)` tuple set after
//! a watcher restart lets the daemon detect drift the watcher missed
//! while it was down. The output is intentionally a streaming
//! iterator so callers can process the tree without buffering
//! everything in memory.

use std::path::{Path, PathBuf};

use ignore::{DirEntry, WalkBuilder};

/// One observed file inside the repo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedFile {
    pub path: PathBuf,
    pub mtime_ns: i128,
    pub size_bytes: u64,
    pub is_executable: bool,
}

/// Directory names that cairn always prunes from the working-tree
/// walk, even when they are not present in any `.gitignore`. These
/// fall into three buckets:
///
/// - `.git` — tool metadata, never indexable as source.
/// - `target`, `node_modules` — build outputs / dependency caches
///   that would dominate the walk for zero indexing value.
/// - `.claude` — the Claude harness's per-project state, including
///   `.claude/worktrees/<id>/<full-repo-checkout>`. Walking into
///   those would re-index the whole repo once per sub-agent
///   worktree.
///
/// The watcher (`crate::EventClassifier`) uses the same list to
/// filter inbound notify events so the two surfaces stay consistent.
pub(crate) const ALWAYS_PRUNED_DIR_NAMES: &[&str] = &[".git", "target", "node_modules", ".claude"];

/// Walk `repo_root`, honoring `.gitignore` and `.git/info/exclude`,
/// and yield every regular file found. Directories listed in
/// [`ALWAYS_PRUNED_DIR_NAMES`] are skipped regardless of gitignore.
pub fn walk_repo(repo_root: &Path) -> impl Iterator<Item = ScannedFile> {
    let walker = WalkBuilder::new(repo_root)
        .hidden(false) // we want dotfiles like .env, but excluded set covers .git
        .git_ignore(true)
        .git_exclude(true)
        .git_global(false)
        // Honor .gitignore even without an initialized `.git` directory.
        // The repos cairn watches always have one, but tests can skip it.
        .require_git(false)
        .filter_entry(|e| !is_always_pruned(e))
        .build();

    walker.filter_map(Result::ok).filter_map(scanned_from_entry)
}

fn is_always_pruned(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_some_and(|n| ALWAYS_PRUNED_DIR_NAMES.contains(&n))
}

fn scanned_from_entry(entry: DirEntry) -> Option<ScannedFile> {
    if !entry.file_type().is_some_and(|t| t.is_file()) {
        return None;
    }
    let meta = entry.metadata().ok()?;
    let mtime_ns = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| i128::try_from(d.as_nanos()).unwrap_or(i128::MAX));
    Some(ScannedFile {
        path: entry.into_path(),
        mtime_ns,
        size_bytes: meta.len(),
        is_executable: is_executable(&meta),
    })
}

#[cfg(unix)]
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_meta: &std::fs::Metadata) -> bool {
    // TODO: Decide how Windows should surface executable script candidates.
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs;

    #[test]
    fn walks_simple_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.rs"), "fn a() {}").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub").join("b.rs"), "fn b() {}").unwrap();

        let names: HashSet<String> = walk_repo(root)
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains("a.rs"));
        assert!(names.contains("b.rs"));
    }

    #[test]
    fn respects_gitignore() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join(".gitignore"), "ignored.txt\n").unwrap();
        fs::write(root.join("kept.rs"), "").unwrap();
        fs::write(root.join("ignored.txt"), "secret").unwrap();

        let names: HashSet<String> = walk_repo(root)
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains("kept.rs"));
        assert!(!names.contains("ignored.txt"));
    }

    #[test]
    fn prunes_always_excluded_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Plant one file inside each always-pruned directory and one
        // file at the root that should be retained.
        for dir in [".git", "target", "node_modules", ".claude"] {
            fs::create_dir(root.join(dir)).unwrap();
            fs::write(root.join(dir).join("planted.rs"), "fn x() {}").unwrap();
        }
        // Simulate the Claude harness layout specifically.
        fs::create_dir_all(root.join(".claude").join("worktrees").join("agent-1")).unwrap();
        fs::write(
            root.join(".claude")
                .join("worktrees")
                .join("agent-1")
                .join("lib.rs"),
            "fn fake() {}",
        )
        .unwrap();
        fs::write(root.join("keep.rs"), "fn keep() {}").unwrap();

        let paths: Vec<PathBuf> = walk_repo(root).map(|f| f.path).collect();
        assert!(paths.iter().any(|p| p.ends_with("keep.rs")));
        for dir in [".git", "target", "node_modules", ".claude"] {
            assert!(
                paths.iter().all(|p| !p.iter().any(|c| c == dir)),
                "{dir} should be pruned but appeared in: {paths:?}"
            );
        }
    }
}
