//! Repository-scoped gitignore matcher and git metadata resolver.
//!
//! Both the startup [`crate::scan::walk_repo`] pass and the runtime
//! [`crate::EventClassifier`] rely on this module for two decisions:
//!
//! - Where does the repository's git metadata live? A plain checkout
//!   keeps `.git/` next to the working tree, but a linked worktree
//!   splits it into a per-worktree `gitdir` plus a shared `commondir`.
//!   [`resolve_git_metadata`] follows the `.git` file / `commondir`
//!   trail so local config controls, `info/exclude`, and the watched git
//!   roots point at the right on-disk locations.
//! - Is a path ignored, or does it fall in a subtree that never
//!   belongs to the parent repository? [`RepoIgnoreMatcher`] layers the
//!   effective `core.excludesFile`, `.git/info/exclude`, and every discovered
//!   `.gitignore` inside the working tree, and treats nested repository
//!   boundaries (a `.git` file or directory below the root) as always-pruned.
//!
//! Sharing one matcher between the scanner and the event classifier
//! keeps the startup snapshot and the live event stream from drifting
//! on which paths the parent repository owns.
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ignore::Match;
use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::scan::ALWAYS_PRUNED_DIR_NAMES;

/// Git-metadata locations the watcher needs to know about.
///
/// - `worktree_git_dir`: the per-worktree git dir. For a plain
///   checkout this is `<repo>/.git/`; for a linked worktree it is
///   `<common>/.git/worktrees/<name>/`. HEAD lives here.
/// - `common_git_dir`: the shared git dir that holds `refs/`,
///   `packed-refs`, and `info/exclude`. Equal to `worktree_git_dir`
///   for a plain checkout.
/// - `info_exclude`: the resolved absolute path of
///   `<common_git_dir>/info/exclude`. Kept precomputed because the
///   classifier compares against it on every event.
/// - `ignore_controls`: repository-local Git configuration files that
///   can change the effective ignore source. They are already below the
///   existing git-dir watch roots; no additional external path is watched.
#[derive(Debug, Clone)]
pub(crate) struct GitMetadataPaths {
    pub(crate) worktree_git_dir: PathBuf,
    pub(crate) common_git_dir: PathBuf,
    pub(crate) info_exclude: PathBuf,
    pub(crate) ignore_controls: Vec<PathBuf>,
}

impl GitMetadataPaths {
    /// Synthetic paths used when [`resolve_git_metadata`] fails. Every
    /// slot points into `<repo_root>/.git/...`, so downstream code
    /// keeps operating (fail-open) even if the real metadata layout
    /// cannot be read; the classifier stays permissive rather than
    /// silently going dark.
    pub(crate) fn fail_open(repo_root: &Path) -> Self {
        let dot_git = repo_root.join(".git");
        let ignore_controls = git_config_controls(&dot_git, &dot_git);
        Self {
            info_exclude: dot_git.join("info").join("exclude"),
            worktree_git_dir: dot_git.clone(),
            common_git_dir: dot_git,
            ignore_controls,
        }
    }

    /// Git dirs that should be handed to the OS watcher in addition
    /// to the working tree. For a plain checkout this is one entry
    /// (`.git/`); for a linked worktree it is two — the per-worktree
    /// git dir (for HEAD) and the shared common git dir (for refs,
    /// packed-refs, and info/exclude).
    pub(crate) fn watch_roots(&self) -> Vec<PathBuf> {
        let mut roots = vec![self.worktree_git_dir.clone()];
        if self.common_git_dir != self.worktree_git_dir {
            roots.push(self.common_git_dir.clone());
        }
        roots
    }
}

/// Resolve the git metadata layout for `repo_root`.
///
/// Handles three cases:
///
/// 1. A plain checkout with a `.git/` directory: `worktree_git_dir`
///    and `common_git_dir` both point at `<repo_root>/.git/`.
/// 2. A linked worktree, where `.git` is a file containing
///    `gitdir: <path-to-worktree-git-dir>`. That path is followed
///    (and canonicalized best-effort) to get the per-worktree git
///    dir. If a `commondir` file lives alongside it, that entry
///    resolves the shared common git dir; otherwise the worktree
///    and common dirs coincide.
/// 3. Anything malformed — a broken `.git` file, an unreadable
///    `commondir`, or an I/O error — is surfaced as `Err`. The two
///    callers handle it differently: the watcher setup falls back
///    to [`GitMetadataPaths::fail_open`] and continues, while the
///    scanner records the failure in `ScanErrors`, substitutes the
///    fixed `<repo_root>/.git/info/exclude` path, and builds a
///    fail-open matcher rather than a hard stop.
///
/// `info_exclude` is derived from the resolved common git dir since
/// that is where a linked worktree keeps the shared exclude file.
pub(crate) fn resolve_git_metadata(repo_root: &Path) -> io::Result<GitMetadataPaths> {
    let dot_git = repo_root.join(".git");
    let worktree_git_dir = if try_is_file(&dot_git)? {
        let contents = fs::read_to_string(&dot_git)?;
        let raw = contents
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("gitdir:"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid .git file"))?;
        absolutize(repo_root, Path::new(raw))
    } else {
        dot_git
    };

    let common_git_dir = match fs::read_to_string(worktree_git_dir.join("commondir")) {
        Ok(contents) => {
            let raw = contents.trim();
            if raw.is_empty() {
                worktree_git_dir.clone()
            } else {
                absolutize(&worktree_git_dir, Path::new(raw))
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => worktree_git_dir.clone(),
        Err(err) => return Err(err),
    };

    let ignore_controls = git_config_controls(&worktree_git_dir, &common_git_dir);
    Ok(GitMetadataPaths {
        info_exclude: common_git_dir.join("info").join("exclude"),
        worktree_git_dir,
        common_git_dir,
        ignore_controls,
    })
}

fn git_config_controls(worktree_git_dir: &Path, common_git_dir: &Path) -> Vec<PathBuf> {
    let mut controls = vec![
        common_git_dir.join("config"),
        worktree_git_dir.join("config.worktree"),
    ];
    controls.sort();
    controls.dedup();
    controls
}

/// Resolve `candidate` against `base` and canonicalize the result
/// best-effort. Canonicalization failures (broken symlink, missing
/// component) fall back to the un-canonicalized join so callers can
/// still record the intended path even when the target does not yet
/// exist on disk.
fn absolutize(base: &Path, candidate: &Path) -> PathBuf {
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base.join(candidate)
    };
    joined.canonicalize().unwrap_or(joined)
}

/// One `.gitignore` file discovered inside the working tree, keyed
/// by the directory it lives in. `matcher` interprets its patterns
/// with `directory` as the anchor so nested `/foo/` patterns stay
/// scoped to their own subtree.
#[derive(Debug)]
struct IgnoreLayer {
    directory: PathBuf,
    matcher: Gitignore,
}

/// Hierarchical gitignore matcher shared by the startup scanner and
/// the runtime event classifier.
///
/// Layers are evaluated from the effective `core.excludesFile` through
/// `info_exclude`, then shallow-to-deep working-tree `.gitignore` files.
/// Each later layer can override an earlier one, matching Git precedence.
/// Directory-scoped patterns anchor to the layer's own directory rather
/// than the repo root.
///
/// The set of layers is discovered eagerly by [`Self::build`] and
/// fixed for the lifetime of the matcher; the classifier hot-swaps
/// a whole matcher when a `.gitignore` or directory-topology event
/// arrives (see [`crate::EventClassifier::reload_matcher`]).
#[derive(Debug)]
pub(crate) struct RepoIgnoreMatcher {
    repo_root: PathBuf,
    core_excludes: Gitignore,
    info_exclude: Gitignore,
    layers: Vec<IgnoreLayer>,
}

impl RepoIgnoreMatcher {
    /// Build a matcher for `repo_root`, sampling the current effective
    /// `core.excludesFile`, then loading `info_exclude` and every discovered
    /// `.gitignore` in the working tree.
    ///
    /// Discovery walks directories eagerly and stops descending into
    /// nested repository boundaries and always-pruned subtrees, so a
    /// vendored checkout's own `.gitignore` never joins this matcher.
    /// Any I/O error while reading a `.gitignore`, listing a
    /// directory, or probing metadata is surfaced as `Err` and the
    /// caller decides whether to publish or fall back to
    /// [`Self::fail_open`].
    pub(crate) fn build(repo_root: &Path, info_exclude: &Path) -> io::Result<Self> {
        let repo_root = repo_root.to_path_buf();
        let core_excludes = load_core_excludes_matcher(&repo_root)?;
        let info_exclude = load_matcher(&repo_root, info_exclude)?;
        let mut matcher = Self {
            repo_root: repo_root.clone(),
            core_excludes,
            info_exclude,
            layers: Vec::new(),
        };
        matcher.discover_directory(&repo_root)?;
        Ok(matcher)
    }

    /// Permissive matcher: no `.gitignore` rules and no info/exclude
    /// rules are applied, so [`Self::is_ignored`] always returns
    /// `false`. Nested-boundary and always-pruned pruning still
    /// works because [`Self::is_pruned_path`] does not consult the
    /// gitignore layers.
    ///
    /// Used by the classifier when a matcher build/reload failed, so
    /// the watcher keeps delivering events (potentially over-broad)
    /// instead of going silent while the retry thread rebuilds.
    pub(crate) fn fail_open(repo_root: &Path) -> Self {
        Self {
            repo_root: repo_root.to_path_buf(),
            core_excludes: Gitignore::empty(),
            info_exclude: Gitignore::empty(),
            layers: Vec::new(),
        }
    }

    /// Return whether `path` belongs to a subtree that the parent
    /// repository never owns.
    ///
    /// In addition to the fixed always-pruned directory names, any
    /// directory below the registered root that contains a `.git`
    /// file or directory is a nested repository boundary. Metadata
    /// lookup errors intentionally fail open here. Full scans use
    /// [`Self::try_is_pruned_path`] so the same error rejects publication.
    pub(crate) fn is_pruned_path(&self, path: &Path) -> bool {
        self.try_is_pruned_path(path).unwrap_or_else(|err| {
            tracing::debug!(
                path = %path.display(),
                error = %err,
                "nested repository boundary lookup failed; classifier is fail-open"
            );
            false
        })
    }

    /// Return whether `path` contains one of the fixed directory names
    /// that the scanner and watcher always exclude. Nested repository
    /// boundaries are intentionally separate because topology events
    /// for their `.git` markers must still rebuild the matcher.
    pub(crate) fn is_always_pruned_path(&self, path: &Path) -> bool {
        let Ok(relative) = path.strip_prefix(&self.repo_root) else {
            return false;
        };
        !relative.as_os_str().is_empty()
            && relative.components().any(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .is_some_and(|name| ALWAYS_PRUNED_DIR_NAMES.contains(&name))
            })
    }

    /// Fallible form of [`Self::is_pruned_path`] for full-scan callers.
    pub(crate) fn try_is_pruned_path(&self, path: &Path) -> io::Result<bool> {
        if !path.starts_with(&self.repo_root) {
            return Ok(false);
        }
        if self.is_always_pruned_path(path) {
            return Ok(true);
        }

        for directory in path
            .ancestors()
            .take_while(|directory| *directory != self.repo_root)
            .take_while(|directory| directory.starts_with(&self.repo_root))
        {
            if try_has_git_marker(directory)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return whether `path` is gitignored under this repository's
    /// combined rules.
    ///
    /// Matching walks the ancestor chain from the repo root down to
    /// (but not including) `path`, so a directory ignored at any
    /// level ignores everything beneath it — matching git's own
    /// "an ignored directory hides its contents" rule. Paths that
    /// are not inside `repo_root` and the root itself both return
    /// `false`.
    pub(crate) fn is_ignored(&self, path: &Path, is_dir: bool) -> bool {
        let Ok(relative) = path.strip_prefix(&self.repo_root) else {
            return false;
        };
        if relative.as_os_str().is_empty() {
            return false;
        }

        let mut ancestor = self.repo_root.clone();
        let components = relative.components().collect::<Vec<_>>();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            ancestor.push(component.as_os_str());
            if self.direct_match(&ancestor, true) == MatchDecision::Ignore {
                return true;
            }
        }
        self.direct_match(path, is_dir) == MatchDecision::Ignore
    }

    /// Combine `core.excludesFile`, `info/exclude`, and every `.gitignore`
    /// layer whose directory covers `path`. Later layers override earlier
    /// ones only when they produce a non-`None` decision.
    fn direct_match(&self, path: &Path, is_dir: bool) -> MatchDecision {
        let mut result = decision(self.core_excludes.matched(path, is_dir));
        let info = decision(self.info_exclude.matched(path, is_dir));
        if info != MatchDecision::None {
            result = info;
        }
        for layer in &self.layers {
            if !path.starts_with(&layer.directory) {
                continue;
            }
            let current = decision(layer.matcher.matched(path, is_dir));
            if current != MatchDecision::None {
                result = current;
            }
        }
        result
    }

    /// Recursively load every `.gitignore` inside `directory`,
    /// pushing one [`IgnoreLayer`] per file found. Descent skips
    /// symlinks, entries already excluded by rules loaded so far,
    /// and nested repository boundaries — so a vendored subrepo's
    /// `.gitignore` never joins the parent's matcher.
    fn discover_directory(&mut self, directory: &Path) -> io::Result<()> {
        let ignore_file = directory.join(".gitignore");
        if try_is_file(&ignore_file)? {
            self.layers.push(IgnoreLayer {
                directory: directory.to_path_buf(),
                matcher: load_matcher(directory, &ignore_file)?,
            });
        }

        let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            let file_type = entry.file_type()?;
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let path = entry.path();
            if self.is_ignored(&path, true) || self.try_is_pruned_path(&path)? {
                continue;
            }
            self.discover_directory(&path)?;
        }
        Ok(())
    }
}

/// Three-valued outcome of matching a path against a single
/// gitignore layer. `None` means the layer had no opinion; because
/// [`RepoIgnoreMatcher::direct_match`] iterates layers shallow →
/// deep, this preserves whatever decision an already-consulted
/// shallower layer (or the shared `info/exclude`) has settled on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchDecision {
    None,
    Ignore,
    Whitelist,
}

fn decision(matched: Match<&ignore::gitignore::Glob>) -> MatchDecision {
    match matched {
        Match::None => MatchDecision::None,
        Match::Ignore(_) => MatchDecision::Ignore,
        Match::Whitelist(_) => MatchDecision::Whitelist,
    }
}

/// Effective `core.excludesFile` state. Git treats the three cases
/// differently, so they stay distinct: an unset key falls back to the XDG
/// default, while an explicitly empty value disables the global ignore file.
#[derive(Debug, PartialEq, Eq)]
enum CoreExcludesPath {
    Unset,
    Empty,
    Path(PathBuf),
}

/// Load the matcher for the effective `core.excludesFile`.
///
/// `Unset` defers to the same XDG default Git itself would read, so a
/// repository that configures nothing still honors the user's global ignore
/// file.
fn load_core_excludes_matcher(repo_root: &Path) -> io::Result<Gitignore> {
    match query_core_excludes_path(repo_root)? {
        CoreExcludesPath::Unset => {
            let (matcher, error) = GitignoreBuilder::new(repo_root).build_global();
            if error.is_some() {
                return Err(core_excludes_error(io::ErrorKind::InvalidData));
            }
            Ok(matcher)
        }
        CoreExcludesPath::Empty => Ok(Gitignore::empty()),
        CoreExcludesPath::Path(path) => load_matcher(repo_root, &path)
            .map_err(|_| core_excludes_error(io::ErrorKind::InvalidData)),
    }
}

fn query_core_excludes_path(repo_root: &Path) -> io::Result<CoreExcludesPath> {
    let output = query_core_excludes_output(repo_root)?;
    parse_core_excludes_output(
        repo_root,
        output.status.code(),
        &output.stdout,
        &output.stderr,
    )
}

fn query_core_excludes_output(repo_root: &Path) -> io::Result<Output> {
    Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["config", "--null", "--path", "--get", "core.excludesFile"])
        .output()
        .map_err(|_| core_excludes_error(io::ErrorKind::Other))
}

/// Interpret one `git config --null --path --get` result.
///
/// Only "key absent" is inferred, and only from the exact shape Git uses for
/// it: exit 1 with both streams empty. Any other non-zero exit, any stderr, a
/// value that is not NUL-terminated, or an embedded NUL — which would mean
/// more than one value and no unambiguous choice — is an error rather than a
/// guess, so a misread never silently widens or narrows what the watcher
/// ignores.
fn parse_core_excludes_output(
    repo_root: &Path,
    exit_code: Option<i32>,
    stdout: &[u8],
    stderr: &[u8],
) -> io::Result<CoreExcludesPath> {
    if exit_code == Some(1) && stdout.is_empty() && stderr.is_empty() {
        return Ok(CoreExcludesPath::Unset);
    }
    if exit_code != Some(0) || !stderr.is_empty() {
        return Err(core_excludes_error(io::ErrorKind::InvalidData));
    }
    let Some((&0, value)) = stdout.split_last() else {
        return Err(core_excludes_error(io::ErrorKind::InvalidData));
    };
    if value.contains(&0) {
        return Err(core_excludes_error(io::ErrorKind::InvalidData));
    }
    if value.is_empty() {
        return Ok(CoreExcludesPath::Empty);
    }

    #[cfg(unix)]
    let path = {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        PathBuf::from(OsString::from_vec(value.to_vec()))
    };
    #[cfg(not(unix))]
    let path = PathBuf::from(
        std::str::from_utf8(value).map_err(|_| core_excludes_error(io::ErrorKind::InvalidData))?,
    );

    Ok(if path.is_absolute() {
        CoreExcludesPath::Path(path)
    } else {
        CoreExcludesPath::Path(repo_root.join(path))
    })
}

/// One opaque error for every failure to determine the effective ignore
/// configuration. Callers only need "unavailable" to fail open, and a fixed
/// message keeps configured paths and values out of watcher diagnostics.
fn core_excludes_error(kind: io::ErrorKind) -> io::Error {
    io::Error::new(kind, "effective Git ignore configuration is unavailable")
}

/// Build a `Gitignore` matcher anchored at `root` from the patterns
/// in `source`. A missing `source` is not an error; it produces an
/// empty matcher. Parse and build errors are wrapped as
/// `io::ErrorKind::InvalidData` so callers can distinguish "file
/// unreadable" from "patterns malformed" only via the underlying
/// message — both fail the scan or classifier build.
fn load_matcher(root: &Path, source: &Path) -> io::Result<Gitignore> {
    if !try_is_file(source)? {
        return Ok(Gitignore::empty());
    }
    let mut builder = GitignoreBuilder::new(root);
    if let Some(err) = builder.add(source) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, err));
    }
    builder
        .build()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

/// Best-effort "is this an existing regular file?" probe. Swallows
/// `NotFound` and `NotADirectory` (the latter surfaces when an
/// intermediate path component is a file) as `Ok(false)`; other
/// errors propagate.
fn try_is_file(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

/// Return whether `directory` contains a `.git` file or directory.
/// Either shape (submodule gitfile or full `.git` directory) is a
/// nested repository boundary.
fn try_has_git_marker(directory: &Path) -> io::Result<bool> {
    let marker = directory.join(".git");
    match fs::metadata(marker) {
        Ok(metadata) => Ok(metadata.is_file() || metadata.is_dir()),
        // A file path can be the first ancestor examined. Appending `.git`
        // to it yields NotADirectory, which means "not a boundary" rather
        // than a failed metadata lookup.
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

/// True when `path` is a `.git` marker (file or directory) belonging
/// to a *nested* repository below `repo_root`.
///
/// The registered root's own `.git` marker is deliberately excluded:
/// changes there are ordinary git activity, not a nested-repo
/// boundary shift. The classifier uses this to force a rescan when a
/// nested `.git` appears or disappears, since the ignore matcher's
/// boundary set is now stale.
pub(crate) fn is_nested_git_marker_path(repo_root: &Path, path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == ".git")
        && path
            .parent()
            .is_some_and(|parent| parent != repo_root && parent.starts_with(repo_root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::process::{Command, Output};

    fn git_command(root: &Path) -> Command {
        let mut command = Command::new("git");
        command
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", root.join(".cairn-test-global"));
        command
    }

    fn run_git(root: &Path, args: &[&str]) -> Output {
        git_command(root)
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .unwrap()
    }

    fn configure_core_excludes(root: &Path, value: &std::ffi::OsStr) {
        let status = git_command(root)
            .arg("-C")
            .arg(root)
            .arg("config")
            .arg("core.excludesFile")
            .arg(value)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn core_excludes_query_contract_is_nul_exact_and_cwd_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        fs::create_dir(&root).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());

        configure_core_excludes(&root, std::ffi::OsStr::new("rules/with space\nline"));
        let output = query_core_excludes_output(&root).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"rules/with space\nline\0");
        assert!(output.stderr.is_empty());
        assert_eq!(
            parse_core_excludes_output(
                &root,
                output.status.code(),
                &output.stdout,
                &output.stderr,
            )
            .unwrap(),
            CoreExcludesPath::Path(root.join("rules/with space\nline"))
        );

        let absolute = tmp.path().join("absolute rules");
        configure_core_excludes(&root, absolute.as_os_str());
        let output = query_core_excludes_output(&root).unwrap();
        assert!(output.status.success());
        assert_eq!(
            parse_core_excludes_output(
                &root,
                output.status.code(),
                &output.stdout,
                &output.stderr,
            )
            .unwrap(),
            CoreExcludesPath::Path(absolute)
        );

        configure_core_excludes(&root, std::ffi::OsStr::new("~/rules"));
        let output = query_core_excludes_output(&root).unwrap();
        assert_eq!(
            parse_core_excludes_output(
                &root,
                output.status.code(),
                &output.stdout,
                &output.stderr,
            )
            .unwrap(),
            CoreExcludesPath::Path(
                PathBuf::from(std::env::var_os("HOME").unwrap()).join("rules")
            )
        );

        configure_core_excludes(&root, std::ffi::OsStr::new(""));
        let output = query_core_excludes_output(&root).unwrap();
        assert_eq!(output.stdout, b"\0");
        assert_eq!(
            parse_core_excludes_output(
                &root,
                output.status.code(),
                &output.stdout,
                &output.stderr,
            )
            .unwrap(),
            CoreExcludesPath::Empty
        );

        assert!(
            run_git(&root, &["config", "--unset", "core.excludesFile"])
                .status
                .success()
        );
        assert_eq!(
            parse_core_excludes_output(&root, Some(1), b"", b"").unwrap(),
            CoreExcludesPath::Unset
        );
    }

    #[test]
    fn core_excludes_query_contract_rejects_ambiguous_process_results() {
        let root = Path::new("/repo");
        for (code, stdout, stderr) in [
            (Some(0), b"not-terminated".as_slice(), b"".as_slice()),
            (Some(0), b"one\0two\0".as_slice(), b"".as_slice()),
            (Some(0), b"rules\0".as_slice(), b"warning".as_slice()),
            (Some(1), b"rules\0".as_slice(), b"".as_slice()),
            (Some(1), b"".as_slice(), b"error".as_slice()),
            (Some(2), b"".as_slice(), b"".as_slice()),
            (None, b"".as_slice(), b"".as_slice()),
        ] {
            assert!(
                parse_core_excludes_output(root, code, stdout, stderr).is_err(),
                "unexpectedly accepted code={code:?}, stdout={stdout:?}, stderr={stderr:?}"
            );
        }
    }

    #[test]
    fn git_ignore_authority_orders_core_then_info_then_nested_rules() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        fs::create_dir(&root).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("core-ignore"), "*.rs\n*.tmp\n").unwrap();
        fs::write(root.join(".git/info/exclude"), "!same.rs\n!info.tmp\n").unwrap();
        fs::write(root.join(".gitignore"), "same.rs\n").unwrap();
        fs::write(root.join("sub/.gitignore"), "!same.rs\n").unwrap();
        configure_core_excludes(&root, std::ffi::OsStr::new("core-ignore"));
        for path in ["same.rs", "sub/same.rs", "only-core.rs", "info.tmp"] {
            fs::write(root.join(path), "").unwrap();
        }

        assert!(
            run_git(&root, &["check-ignore", "-q", "same.rs"])
                .status
                .success()
        );
        assert!(
            !run_git(&root, &["check-ignore", "-q", "sub/same.rs"])
                .status
                .success()
        );
        assert!(
            run_git(&root, &["check-ignore", "-q", "only-core.rs"])
                .status
                .success()
        );
        assert!(
            !run_git(&root, &["check-ignore", "-q", "info.tmp"])
                .status
                .success()
        );

        let metadata = resolve_git_metadata(&root).unwrap();
        let matcher = RepoIgnoreMatcher::build(&root, &metadata.info_exclude).unwrap();
        assert!(matcher.is_ignored(&root.join("same.rs"), false));
        assert!(!matcher.is_ignored(&root.join("sub/same.rs"), false));
        assert!(matcher.is_ignored(&root.join("only-core.rs"), false));
        assert!(!matcher.is_ignored(&root.join("info.tmp"), false));
    }

    /// Only the root config and `config.worktree` are live ignore controls.
    /// Included config and selected excludes files are sampled at build time,
    /// wherever they live; changing either file alone leaves the current
    /// matcher unchanged until reload, reindex, restart, or another rebuild.
    #[test]
    fn git_config_authority_resolves_conditional_include() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        fs::create_dir(&root).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());
        let included = tmp.path().join("included.conf");
        fs::write(&included, "[core]\n\texcludesFile = rules-a\n").unwrap();
        fs::write(root.join("rules-a"), "/a.rs\n").unwrap();
        fs::write(root.join("rules-b"), "/b.rs\n").unwrap();
        let git_dir = run_git(&root, &["rev-parse", "--absolute-git-dir"]);
        assert!(git_dir.status.success());
        assert!(git_dir.stderr.is_empty());
        let git_dir = std::str::from_utf8(&git_dir.stdout)
            .unwrap()
            .strip_suffix('\n')
            .unwrap();
        let common_dir = run_git(
            &root,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        );
        assert!(common_dir.status.success());
        assert_eq!(common_dir.stdout, format!("{git_dir}\n").as_bytes());
        let condition = format!("gitdir:{git_dir}");
        let include_status = git_command(&root)
            .arg("-C")
            .arg(&root)
            .args(["config", "--add"])
            .arg(format!("includeIf.{condition}.path"))
            .arg(&included)
            .status()
            .unwrap();
        assert!(include_status.success());
        let output = query_core_excludes_output(&root).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"rules-a\0");
        assert!(output.stderr.is_empty());

        let metadata = resolve_git_metadata(&root).unwrap();
        let matcher = RepoIgnoreMatcher::build(&root, &metadata.info_exclude).unwrap();
        assert!(matcher.is_ignored(&root.join("a.rs"), false));
        assert!(!matcher.is_ignored(&root.join("b.rs"), false));

        fs::write(&included, "[core]\n\texcludesFile = rules-b\n").unwrap();
        assert!(matcher.is_ignored(&root.join("a.rs"), false));
        assert!(!matcher.is_ignored(&root.join("b.rs"), false));

        let rebuilt = RepoIgnoreMatcher::build(&root, &metadata.info_exclude).unwrap();
        assert!(!rebuilt.is_ignored(&root.join("a.rs"), false));
        assert!(rebuilt.is_ignored(&root.join("b.rs"), false));
    }

    #[test]
    fn git_config_authority_resolves_linked_worktree_value() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("main");
        let worktree = tmp.path().join("worktree");
        fs::create_dir(&root).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());
        fs::write(root.join("tracked"), "").unwrap();
        assert!(run_git(&root, &["add", "tracked"]).status.success());
        assert!(
            run_git(
                &root,
                &[
                    "-c",
                    "user.name=Cairn Test",
                    "-c",
                    "user.email=cairn@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                ],
            )
            .status
            .success()
        );
        assert!(
            run_git(
                &root,
                &[
                    "worktree",
                    "add",
                    "-q",
                    "-b",
                    "fixture-worktree",
                    worktree.to_str().unwrap(),
                ],
            )
            .status
            .success()
        );
        assert!(
            run_git(&root, &["config", "extensions.worktreeConfig", "true"])
                .status
                .success()
        );
        assert!(
            run_git(
                &worktree,
                &[
                    "config",
                    "--worktree",
                    "core.excludesFile",
                    "worktree.rules",
                ],
            )
            .status
            .success()
        );
        let output = query_core_excludes_output(&worktree).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"worktree.rules\0");
        let git_dir = run_git(&worktree, &["rev-parse", "--absolute-git-dir"]);
        let common_dir = run_git(
            &worktree,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        );
        assert!(git_dir.status.success());
        assert!(common_dir.status.success());
        assert_ne!(git_dir.stdout, common_dir.stdout);
        let git_dir = std::str::from_utf8(&git_dir.stdout)
            .unwrap()
            .strip_suffix('\n')
            .unwrap();
        assert!(Path::new(git_dir).join("config.worktree").is_file());
    }

    #[test]
    fn unset_core_excludes_adopts_xdg_default_in_repo_matcher() {
        const CHILD: &str = "CAIRN_XDG_MATCHER_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let root = PathBuf::from(std::env::var_os("CAIRN_XDG_MATCHER_ROOT").unwrap());
            let metadata = resolve_git_metadata(&root).unwrap();
            let result = RepoIgnoreMatcher::build(&root, &metadata.info_exclude);
            if std::env::var_os("CAIRN_XDG_MATCHER_EXPECT_ERROR").is_some() {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_ignored(&root.join("xdg.rs"), false));
            }
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        let home = tmp.path().join("home");
        let xdg = tmp.path().join("xdg");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&home).unwrap();
        fs::create_dir_all(xdg.join("git")).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());
        fs::write(xdg.join("git/ignore"), "/xdg.rs\n").unwrap();
        fs::write(root.join("xdg.rs"), "").unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "matcher::tests::unset_core_excludes_adopts_xdg_default_in_repo_matcher",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env("CAIRN_XDG_MATCHER_ROOT", &root)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &xdg)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                tmp.path().join("missing-global-config"),
            )
            .status()
            .unwrap();
        assert!(status.success());

        fs::write(xdg.join("git/ignore"), "[z-a]\n").unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "matcher::tests::unset_core_excludes_adopts_xdg_default_in_repo_matcher",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env("CAIRN_XDG_MATCHER_EXPECT_ERROR", "1")
            .env("CAIRN_XDG_MATCHER_ROOT", &root)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &xdg)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                tmp.path().join("missing-global-config"),
            )
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn missing_ignore_file_builds_an_empty_layer() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let matcher = load_matcher(root, &root.join("missing-ignore")).unwrap();

        assert_eq!(
            decision(matcher.matched(root.join("source.rs"), false)),
            MatchDecision::None
        );
    }

    #[test]
    #[cfg(unix)]
    fn core_excludes_query_preserves_non_utf8_without_shell_interpretation() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        fs::create_dir(&root).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());
        let raw = b"rules/\xff;$(touch SHOULD_NOT_EXIST)*.ignore".to_vec();
        configure_core_excludes(&root, &OsString::from_vec(raw.clone()));

        let output = query_core_excludes_output(&root).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, [raw.as_slice(), b"\0"].concat());
        let parsed =
            parse_core_excludes_output(&root, output.status.code(), &output.stdout, &output.stderr)
                .unwrap();
        let CoreExcludesPath::Path(parsed) = parsed else {
            panic!("configured non-UTF-8 path was not retained");
        };
        assert_eq!(
            parsed.as_os_str().as_bytes(),
            root.join(OsString::from_vec(raw)).as_os_str().as_bytes()
        );
        assert!(!root.join("SHOULD_NOT_EXIST").exists());
    }

    #[test]
    #[cfg(unix)]
    fn core_excludes_query_keeps_a_symlink_path_lexical() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("repo");
        fs::create_dir(&root).unwrap();
        assert!(run_git(&root, &["init", "-q"]).status.success());
        fs::write(root.join("actual-ignore"), "/generated.rs\n").unwrap();
        std::os::unix::fs::symlink("actual-ignore", root.join("ignore-link")).unwrap();
        configure_core_excludes(&root, std::ffi::OsStr::new("ignore-link"));

        let output = query_core_excludes_output(&root).unwrap();
        assert_eq!(output.stdout, b"ignore-link\0");
        assert_eq!(
            parse_core_excludes_output(
                &root,
                output.status.code(),
                &output.stdout,
                &output.stderr,
            )
            .unwrap(),
            CoreExcludesPath::Path(root.join("ignore-link"))
        );
        assert!(
            fs::symlink_metadata(root.join("ignore-link"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn nested_anchored_pattern_stays_scoped_to_its_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("sub/gen")).unwrap();
        fs::create_dir_all(root.join("gen")).unwrap();
        fs::write(root.join("sub/.gitignore"), "/gen/\n").unwrap();

        let matcher = RepoIgnoreMatcher::build(root, &root.join(".git/info/exclude")).unwrap();
        assert!(matcher.is_ignored(&root.join("sub/gen/file.rs"), false));
        assert!(!matcher.is_ignored(&root.join("gen/file.rs"), false));
    }

    #[test]
    fn nested_negation_overrides_file_rule_but_not_ignored_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("sub")).unwrap();
        fs::write(root.join(".gitignore"), "*.log\nblocked/\n").unwrap();
        fs::write(root.join("sub/.gitignore"), "!keep.log\n").unwrap();

        let matcher = RepoIgnoreMatcher::build(root, &root.join(".git/info/exclude")).unwrap();
        assert!(!matcher.is_ignored(&root.join("sub/keep.log"), false));
        assert!(matcher.is_ignored(&root.join("sub/drop.log"), false));
        assert!(matcher.is_ignored(&root.join("blocked/keep.log"), false));
    }

    #[test]
    fn nested_git_file_and_directory_boundaries_are_pruned() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("nested-dir/.git")).unwrap();
        fs::write(root.join("nested-dir/lib.rs"), "").unwrap();
        fs::create_dir_all(root.join("nested-file")).unwrap();
        fs::write(root.join("nested-file/.git"), "gitdir: elsewhere\n").unwrap();
        fs::write(root.join("nested-file/lib.rs"), "").unwrap();

        let matcher = RepoIgnoreMatcher::build(root, &root.join(".git/info/exclude")).unwrap();
        assert!(matcher.is_pruned_path(&root.join("nested-dir/lib.rs")));
        assert!(matcher.is_pruned_path(&root.join("nested-file/lib.rs")));
        assert!(!matcher.is_pruned_path(root));
        assert!(!matcher.is_pruned_path(&root.join("src/lib.rs")));
    }

    #[test]
    fn ordinary_codex_directory_is_not_pruned_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join(".codex")).unwrap();
        fs::write(root.join(".codex/settings.json"), "{}").unwrap();

        let matcher = RepoIgnoreMatcher::build(root, &root.join(".git/info/exclude")).unwrap();
        assert!(!matcher.is_pruned_path(&root.join(".codex/settings.json")));
    }

    #[test]
    fn matcher_discovery_stops_at_nested_repository_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let nested = root.join("vendor/nested");
        fs::create_dir_all(nested.join(".git")).unwrap();
        fs::write(nested.join(".gitignore"), "*.rs\n").unwrap();

        let matcher = RepoIgnoreMatcher::build(root, &root.join(".git/info/exclude")).unwrap();
        assert!(matcher.is_pruned_path(&nested.join("lib.rs")));
        assert!(matcher.layers.iter().all(|layer| layer.directory != nested));
    }

    #[test]
    fn fail_open_matcher_still_prunes_nested_repository_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("nested/.git")).unwrap();

        let matcher = RepoIgnoreMatcher::fail_open(root);
        assert!(matcher.is_pruned_path(&root.join("nested/src/lib.rs")));
    }

    #[test]
    #[cfg(unix)]
    fn strict_boundary_lookup_reports_metadata_error_while_classifier_fails_open() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::os::unix::fs::symlink("loop", root.join("loop")).unwrap();
        let matcher = RepoIgnoreMatcher::fail_open(root);
        let path = root.join("loop/source.rs");

        assert!(matcher.try_is_pruned_path(&path).is_err());
        assert!(!matcher.is_pruned_path(&path));
    }

    #[test]
    fn nested_git_marker_control_excludes_registered_root_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        assert!(!is_nested_git_marker_path(root, &root.join(".git")));
        assert!(is_nested_git_marker_path(
            root,
            &root.join("vendor/lib/.git")
        ));
        assert!(!is_nested_git_marker_path(
            root,
            &root.join("vendor/lib/.git/config")
        ));
    }

    #[test]
    fn linked_worktree_resolves_common_info_exclude() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("worktree");
        let common = tmp.path().join("main.git");
        let git_dir = common.join("worktrees/w1");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&git_dir).unwrap();
        fs::create_dir_all(common.join("info")).unwrap();
        fs::write(
            root.join(".git"),
            format!("gitdir: {}\n", git_dir.display()),
        )
        .unwrap();
        fs::write(git_dir.join("commondir"), "../..\n").unwrap();

        let paths = resolve_git_metadata(&root).unwrap();
        assert_eq!(paths.common_git_dir, common.canonicalize().unwrap());
        assert_eq!(
            paths.info_exclude,
            common.canonicalize().unwrap().join("info/exclude")
        );

        fs::write(&paths.info_exclude, "generated.rs\n").unwrap();
        let matcher = load_matcher(&root, &paths.info_exclude).unwrap();
        assert_eq!(
            decision(matcher.matched(root.join("generated.rs"), false)),
            MatchDecision::Ignore
        );
    }
}
