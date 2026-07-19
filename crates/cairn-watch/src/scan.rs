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
use tracing::warn;

use crate::matcher::{RepoIgnoreMatcher, resolve_git_metadata};

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
    let info_exclude = resolve_git_metadata(repo_root)
        .map(|paths| paths.info_exclude)
        .unwrap_or_else(|_| repo_root.join(".git/info/exclude"));
    let matcher = RepoIgnoreMatcher::build(repo_root, &info_exclude).unwrap_or_else(|err| {
        warn!(
            root = %repo_root.display(),
            error = %err,
            "ignore matcher build failed during scan; scan is fail-open"
        );
        RepoIgnoreMatcher::fail_open(repo_root)
    });
    let walker = WalkBuilder::new(repo_root)
        .hidden(false) // we want dotfiles like .env, but excluded set covers .git
        // Ignore policy is owned by RepoIgnoreMatcher so the startup
        // scan and event classifier cannot drift. In particular, do
        // not inherit ripgrep-style `.ignore` files or `.gitignore`
        // files above the repository root.
        .ignore(false)
        .parents(false)
        .git_ignore(false)
        .git_exclude(false)
        .git_global(false)
        .require_git(false)
        .filter_entry(move |e| {
            !matcher.is_pruned_path(e.path())
                && !matcher.is_ignored(e.path(), e.file_type().is_some_and(|kind| kind.is_dir()))
        })
        .build();

    walker.filter_map(Result::ok).filter_map(scanned_from_entry)
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
    // Windows does not expose a Unix-style executable bit. Until cairn has a
    // Windows-specific script policy, extensionless shebang fallback stays
    // disabled on non-Unix scans.
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs;
    use std::process::Command;

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
    fn respects_nested_gitignore_with_directory_anchoring() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("sub/gen")).unwrap();
        fs::create_dir_all(root.join("gen")).unwrap();
        fs::write(root.join("sub/.gitignore"), "/gen/\n").unwrap();
        fs::write(root.join("sub/gen/ignored.rs"), "").unwrap();
        fs::write(root.join("gen/kept.rs"), "").unwrap();

        let paths = walk_repo(root).map(|file| file.path).collect::<Vec<_>>();
        assert!(paths.iter().any(|path| path.ends_with("gen/kept.rs")));
        assert!(
            paths
                .iter()
                .all(|path| !path.ends_with("sub/gen/ignored.rs"))
        );
    }

    #[test]
    fn ignores_git_exclude_but_not_dot_ignore_or_parent_gitignore() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("repo");
        fs::create_dir_all(root.join(".git/info")).unwrap();
        fs::write(parent.path().join(".gitignore"), "from-parent.rs\n").unwrap();
        fs::write(root.join(".ignore"), "from-dot-ignore.rs\n").unwrap();
        fs::write(root.join(".git/info/exclude"), "from-exclude.rs\n").unwrap();
        for name in ["from-parent.rs", "from-dot-ignore.rs", "from-exclude.rs"] {
            fs::write(root.join(name), "").unwrap();
        }

        let names = walk_repo(&root)
            .filter_map(|file| file.path.file_name().map(|name| name.to_os_string()))
            .collect::<HashSet<_>>();
        assert!(names.contains(std::ffi::OsStr::new("from-parent.rs")));
        assert!(names.contains(std::ffi::OsStr::new("from-dot-ignore.rs")));
        assert!(!names.contains(std::ffi::OsStr::new("from-exclude.rs")));
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

    #[test]
    fn prunes_nested_git_boundaries_without_pruning_harness_names() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join(".codex/worktrees/w1/repo")).unwrap();
        fs::write(
            root.join(".codex/worktrees/w1/repo/.git"),
            "gitdir: elsewhere\n",
        )
        .unwrap();
        fs::write(root.join(".codex/worktrees/w1/repo/lib.rs"), "").unwrap();
        fs::write(root.join(".codex/settings.json"), "{}").unwrap();

        fs::create_dir_all(root.join("vendor/nested/.git")).unwrap();
        fs::write(root.join("vendor/nested/lib.rs"), "").unwrap();
        fs::write(root.join("vendor/keep.rs"), "").unwrap();

        let paths = walk_repo(root).map(|file| file.path).collect::<Vec<_>>();
        assert!(
            paths
                .iter()
                .any(|path| path.ends_with(".codex/settings.json"))
        );
        assert!(paths.iter().any(|path| path.ends_with("vendor/keep.rs")));
        assert!(
            paths
                .iter()
                .all(|path| !path.ends_with(".codex/worktrees/w1/repo/lib.rs"))
        );
        assert!(
            paths
                .iter()
                .all(|path| !path.ends_with("vendor/nested/lib.rs"))
        );
    }

    #[test]
    fn tracked_gitlink_submodule_is_pruned_from_parent_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        run_git(root, &["init", "-q"]);
        run_git(root, &["config", "user.email", "cairn@example.invalid"]);
        run_git(root, &["config", "user.name", "Cairn Test"]);
        fs::write(root.join("keep.rs"), "").unwrap();
        run_git(root, &["add", "keep.rs"]);
        run_git(root, &["commit", "-qm", "fixture"]);
        let head = run_git(root, &["rev-parse", "HEAD"]);

        fs::create_dir_all(root.join("vendor/submodule")).unwrap();
        fs::write(
            root.join("vendor/submodule/.git"),
            "gitdir: ../../.git/modules/vendor/submodule\n",
        )
        .unwrap();
        fs::write(root.join("vendor/submodule/lib.rs"), "").unwrap();
        run_git(
            root,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                "160000",
                head.trim(),
                "vendor/submodule",
            ],
        );
        let index = run_git(root, &["ls-files", "--stage", "vendor/submodule"]);
        assert!(index.starts_with("160000 "), "expected gitlink: {index}");

        let paths = walk_repo(root).map(|file| file.path).collect::<Vec<_>>();
        assert!(paths.iter().any(|path| path.ends_with("keep.rs")));
        assert!(
            paths
                .iter()
                .all(|path| !path.ends_with("vendor/submodule/lib.rs"))
        );
    }

    fn run_git(root: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(["-c", "core.hooksPath=/dev/null"])
            .args(args)
            .current_dir(root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}
