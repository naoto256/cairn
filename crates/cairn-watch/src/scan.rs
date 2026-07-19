//! Startup full-scan reconciliation.
//!
//! Filesystem watchers — `FSEvents` in particular — drop events under
//! load. Re-running the entire `(path, mtime, size)` tuple set after
//! a watcher restart lets the daemon detect drift the watcher missed
//! while it was down. A scan keeps walking after individual I/O errors to
//! collect bounded diagnostics, but callers must reject the complete report
//! when any error was observed.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

const MAX_SCAN_ERROR_SAMPLES: usize = 64;
const MAX_SCAN_ERROR_MESSAGE_BYTES: usize = 1024;

/// Filesystem operation that failed during a full scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScanOperation {
    ResolveGitMetadata,
    BuildIgnoreMatcher,
    BoundaryMetadata,
    Walk,
    EntryMetadata,
    ModifiedTime,
}

/// One bounded diagnostic retained from an incomplete scan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScanErrorSample {
    pub operation: ScanOperation,
    pub path: PathBuf,
    pub message: String,
}

/// Error summary for a scan. `total` includes diagnostics omitted from
/// `samples` after the bounded retention limit is reached.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanErrors {
    pub total: usize,
    pub samples: Vec<ScanErrorSample>,
}

impl ScanErrors {
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    fn record(
        &mut self,
        operation: ScanOperation,
        path: impl Into<PathBuf>,
        message: impl AsRef<str>,
    ) {
        self.total = self.total.saturating_add(1);
        let sample = ScanErrorSample {
            operation,
            path: path.into(),
            message: truncate_utf8(message.as_ref(), MAX_SCAN_ERROR_MESSAGE_BYTES),
        };
        if self.samples.len() < MAX_SCAN_ERROR_SAMPLES {
            self.samples.push(sample);
            return;
        }
        let Some((largest_index, largest)) = self
            .samples
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.cmp(b))
        else {
            return;
        };
        if sample < *largest {
            self.samples[largest_index] = sample;
        }
    }

    fn finish(&mut self) {
        self.samples.sort();
    }
}

/// Complete result of walking a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanReport {
    pub root: PathBuf,
    pub entries: Vec<ScannedFile>,
    pub errors: ScanErrors,
}

impl ScanReport {
    /// Return entries only when every required filesystem operation succeeded.
    pub fn into_entries(self) -> Result<Vec<ScannedFile>, ScanFailure> {
        if self.errors.is_empty() {
            Ok(self.entries)
        } else {
            Err(ScanFailure {
                root: self.root,
                partial_entry_count: self.entries.len(),
                errors: self.errors,
            })
        }
    }
}

/// A full scan was incomplete and therefore cannot be published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanFailure {
    pub root: PathBuf,
    pub partial_entry_count: usize,
    pub errors: ScanErrors,
}

impl fmt::Display for ScanFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "repository scan of {} was incomplete: {} error(s), {} partial entries",
            self.root.display(),
            self.errors.total,
            self.partial_entry_count
        )?;
        if let Some(sample) = self.errors.samples.first() {
            write!(
                f,
                " (first: {:?} at {}: {})",
                sample.operation,
                sample.path.display(),
                sample.message
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ScanFailure {}

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
/// and collect every regular file found. Directories listed in
/// [`ALWAYS_PRUNED_DIR_NAMES`] are skipped regardless of gitignore.
pub fn walk_repo(repo_root: &Path) -> ScanReport {
    let errors = Arc::new(Mutex::new(ScanErrors::default()));
    let info_exclude = match resolve_git_metadata(repo_root) {
        Ok(paths) => paths.info_exclude,
        Err(err) => {
            record_error(
                &errors,
                ScanOperation::ResolveGitMetadata,
                repo_root.join(".git"),
                err,
            );
            repo_root.join(".git/info/exclude")
        }
    };
    let matcher = RepoIgnoreMatcher::build(repo_root, &info_exclude).unwrap_or_else(|err| {
        record_error(&errors, ScanOperation::BuildIgnoreMatcher, repo_root, &err);
        warn!(
            root = %repo_root.display(),
            error = %err,
            "ignore matcher build failed during scan; collecting diagnostics fail-open"
        );
        RepoIgnoreMatcher::fail_open(repo_root)
    });
    let filter_errors = Arc::clone(&errors);
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
            let is_dir = e.file_type().is_some_and(|kind| kind.is_dir());
            if matcher.is_ignored(e.path(), is_dir) {
                return false;
            }
            let pruned = matcher.try_is_pruned_path(e.path()).unwrap_or_else(|err| {
                record_error(
                    &filter_errors,
                    ScanOperation::BoundaryMetadata,
                    e.path(),
                    err,
                );
                false
            });
            !pruned
        })
        .build();

    let mut entries = Vec::new();
    for result in walker {
        match result {
            Ok(entry) => {
                if let Some(file) = scanned_from_entry(entry, &errors) {
                    entries.push(file);
                }
            }
            Err(err) => {
                let path = ignore_error_path(&err)
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| repo_root.to_path_buf());
                record_error(&errors, ScanOperation::Walk, path, err);
            }
        }
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let mut errors = take_errors(errors);
    errors.finish();
    ScanReport {
        root: repo_root.to_path_buf(),
        entries,
        errors,
    }
}

fn scanned_from_entry(entry: DirEntry, errors: &Arc<Mutex<ScanErrors>>) -> Option<ScannedFile> {
    if !entry.file_type().is_some_and(|t| t.is_file()) {
        return None;
    }
    let path = entry.path().to_path_buf();
    let meta = match entry.metadata() {
        Ok(meta) => meta,
        Err(err) => {
            record_error(errors, ScanOperation::EntryMetadata, &path, err);
            return None;
        }
    };
    let modified = match meta.modified() {
        Ok(modified) => modified,
        Err(err) => {
            record_error(errors, ScanOperation::ModifiedTime, &path, err);
            return None;
        }
    };
    let mtime_ns = match modified_time_ns(modified) {
        Ok(mtime_ns) => mtime_ns,
        Err(err) => {
            record_error(errors, ScanOperation::ModifiedTime, &path, err);
            return None;
        }
    };
    Some(ScannedFile {
        path,
        mtime_ns,
        size_bytes: meta.len(),
        is_executable: is_executable(&meta),
    })
}

fn modified_time_ns(modified: std::time::SystemTime) -> Result<i128, std::time::SystemTimeError> {
    let elapsed = modified.duration_since(std::time::UNIX_EPOCH)?;
    Ok(i128::try_from(elapsed.as_nanos()).unwrap_or(i128::MAX))
}

fn record_error(
    errors: &Arc<Mutex<ScanErrors>>,
    operation: ScanOperation,
    path: impl Into<PathBuf>,
    error: impl fmt::Display,
) {
    let mut guard = errors
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.record(operation, path, error.to_string());
}

fn take_errors(errors: Arc<Mutex<ScanErrors>>) -> ScanErrors {
    match Arc::try_unwrap(errors) {
        Ok(mutex) => mutex
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        Err(errors) => errors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
    }
}

fn ignore_error_path(error: &ignore::Error) -> Option<&Path> {
    match error {
        ignore::Error::Partial(errors) => errors.iter().find_map(ignore_error_path),
        ignore::Error::WithLineNumber { err, .. } | ignore::Error::WithDepth { err, .. } => {
            ignore_error_path(err)
        }
        ignore::Error::WithPath { path, .. } => Some(path),
        ignore::Error::Loop { child, .. } => Some(child),
        _ => None,
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
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

    fn scan_entries(root: &Path) -> Vec<ScannedFile> {
        let report = walk_repo(root);
        assert!(
            report.errors.is_empty(),
            "unexpected scan errors: {report:?}"
        );
        report.into_entries().unwrap()
    }

    #[test]
    fn walks_simple_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.rs"), "fn a() {}").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub").join("b.rs"), "fn b() {}").unwrap();

        let names: HashSet<String> = scan_entries(root)
            .into_iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains("a.rs"));
        assert!(names.contains("b.rs"));
    }

    #[test]
    fn successful_scan_report_has_no_errors_and_sorted_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join("z.rs"), "").unwrap();
        fs::write(root.join("a.rs"), "").unwrap();

        let report = walk_repo(root);
        assert!(report.errors.is_empty());
        let paths = report
            .into_entries()
            .unwrap()
            .into_iter()
            .map(|entry| entry.path.file_name().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(paths, ["a.rs", "z.rs"]);
    }

    #[test]
    fn incomplete_scan_report_rejects_partial_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("missing");

        let failure = walk_repo(&missing).into_entries().unwrap_err();
        assert!(failure.errors.total > 0);
        assert!(failure.errors.samples.iter().any(|sample| matches!(
            sample.operation,
            ScanOperation::BuildIgnoreMatcher | ScanOperation::Walk
        )));
    }

    #[test]
    fn scan_error_samples_are_bounded_and_utf8_safe() {
        let mut errors = ScanErrors::default();
        let message = "あ".repeat(MAX_SCAN_ERROR_MESSAGE_BYTES);
        for index in 0..(MAX_SCAN_ERROR_SAMPLES + 10) {
            errors.record(
                ScanOperation::EntryMetadata,
                format!("file-{index}"),
                &message,
            );
        }

        assert_eq!(errors.total, MAX_SCAN_ERROR_SAMPLES + 10);
        assert_eq!(errors.samples.len(), MAX_SCAN_ERROR_SAMPLES);
        assert!(
            errors
                .samples
                .iter()
                .all(|sample| sample.message.len() <= MAX_SCAN_ERROR_MESSAGE_BYTES)
        );
    }

    #[test]
    fn bounded_error_sample_selection_is_order_independent() {
        fn collect(indices: impl Iterator<Item = usize>) -> ScanErrors {
            let mut errors = ScanErrors::default();
            for index in indices {
                errors.record(
                    ScanOperation::EntryMetadata,
                    format!("file-{index:03}"),
                    "error",
                );
            }
            errors.finish();
            errors
        }

        let forward = collect(0..100);
        let reverse = collect((0..100).rev());
        assert_eq!(forward, reverse);
        assert_eq!(forward.total, 100);
        assert_eq!(forward.samples.len(), MAX_SCAN_ERROR_SAMPLES);
        assert_eq!(forward.samples.first().unwrap().path, Path::new("file-000"));
        assert_eq!(forward.samples.last().unwrap().path, Path::new("file-063"));
    }

    #[test]
    fn modified_time_before_epoch_is_not_silently_coerced_to_zero() {
        let before_epoch = std::time::UNIX_EPOCH - std::time::Duration::from_secs(1);
        assert!(modified_time_ns(before_epoch).is_err());
    }

    #[test]
    fn malformed_git_metadata_is_reported_instead_of_hidden() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join(".git"), "not a gitdir declaration\n").unwrap();
        fs::write(root.join("keep.rs"), "").unwrap();

        let report = walk_repo(root);
        assert!(
            report
                .entries
                .iter()
                .any(|entry| entry.path.ends_with("keep.rs"))
        );
        assert!(
            report
                .errors
                .samples
                .iter()
                .any(|sample| { sample.operation == ScanOperation::ResolveGitMetadata })
        );
        assert!(report.into_entries().is_err());
    }

    #[test]
    fn unreadable_ignore_rules_are_reported_instead_of_publishing_fail_open_walk() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join(".gitignore"), [0xff]).unwrap();
        fs::write(root.join("keep.rs"), "").unwrap();

        let report = walk_repo(root);
        assert!(
            report
                .entries
                .iter()
                .any(|entry| entry.path.ends_with("keep.rs"))
        );
        assert!(
            report
                .errors
                .samples
                .iter()
                .any(|sample| { sample.operation == ScanOperation::BuildIgnoreMatcher })
        );
        assert!(report.into_entries().is_err());
    }

    #[test]
    fn respects_gitignore() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::write(root.join(".gitignore"), "ignored.txt\n").unwrap();
        fs::write(root.join("kept.rs"), "").unwrap();
        fs::write(root.join("ignored.txt"), "secret").unwrap();

        let names: HashSet<String> = scan_entries(root)
            .into_iter()
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

        let paths = scan_entries(root)
            .into_iter()
            .map(|file| file.path)
            .collect::<Vec<_>>();
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

        let names = scan_entries(&root)
            .into_iter()
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

        let paths: Vec<PathBuf> = scan_entries(root).into_iter().map(|f| f.path).collect();
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

        let paths = scan_entries(root)
            .into_iter()
            .map(|file| file.path)
            .collect::<Vec<_>>();
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

        let paths = scan_entries(root)
            .into_iter()
            .map(|file| file.path)
            .collect::<Vec<_>>();
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
