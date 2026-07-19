use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ignore::Match;
use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::scan::ALWAYS_PRUNED_DIR_NAMES;

#[derive(Debug, Clone)]
pub(crate) struct GitMetadataPaths {
    pub(crate) worktree_git_dir: PathBuf,
    pub(crate) common_git_dir: PathBuf,
    pub(crate) info_exclude: PathBuf,
}

impl GitMetadataPaths {
    pub(crate) fn fail_open(repo_root: &Path) -> Self {
        let dot_git = repo_root.join(".git");
        Self {
            info_exclude: dot_git.join("info").join("exclude"),
            worktree_git_dir: dot_git.clone(),
            common_git_dir: dot_git,
        }
    }

    pub(crate) fn watch_roots(&self) -> Vec<PathBuf> {
        let mut roots = vec![self.worktree_git_dir.clone()];
        if self.common_git_dir != self.worktree_git_dir {
            roots.push(self.common_git_dir.clone());
        }
        roots
    }
}

pub(crate) fn resolve_git_metadata(repo_root: &Path) -> io::Result<GitMetadataPaths> {
    let dot_git = repo_root.join(".git");
    let worktree_git_dir = if dot_git.is_file() {
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

    Ok(GitMetadataPaths {
        info_exclude: common_git_dir.join("info").join("exclude"),
        worktree_git_dir,
        common_git_dir,
    })
}

fn absolutize(base: &Path, candidate: &Path) -> PathBuf {
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base.join(candidate)
    };
    joined.canonicalize().unwrap_or(joined)
}

#[derive(Debug)]
struct IgnoreLayer {
    directory: PathBuf,
    matcher: Gitignore,
}

#[derive(Debug)]
pub(crate) struct RepoIgnoreMatcher {
    repo_root: PathBuf,
    info_exclude: Gitignore,
    layers: Vec<IgnoreLayer>,
}

impl RepoIgnoreMatcher {
    pub(crate) fn build(repo_root: &Path, info_exclude: &Path) -> io::Result<Self> {
        let repo_root = repo_root.to_path_buf();
        let info_exclude = load_matcher(&repo_root, info_exclude)?;
        let mut matcher = Self {
            repo_root: repo_root.clone(),
            info_exclude,
            layers: Vec::new(),
        };
        matcher.discover_directory(&repo_root)?;
        Ok(matcher)
    }

    pub(crate) fn fail_open(repo_root: &Path) -> Self {
        Self {
            repo_root: repo_root.to_path_buf(),
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
    /// lookup errors intentionally fail open here; the scanner's
    /// fallible-I/O contract is handled separately.
    pub(crate) fn is_pruned_path(&self, path: &Path) -> bool {
        let Ok(relative) = path.strip_prefix(&self.repo_root) else {
            return false;
        };
        if relative.as_os_str().is_empty() {
            return false;
        }
        if relative.components().any(|component| {
            component
                .as_os_str()
                .to_str()
                .is_some_and(|name| ALWAYS_PRUNED_DIR_NAMES.contains(&name))
        }) {
            return true;
        }

        path.ancestors()
            .take_while(|directory| *directory != self.repo_root)
            .take_while(|directory| directory.starts_with(&self.repo_root))
            .any(has_git_marker)
    }

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

    fn direct_match(&self, path: &Path, is_dir: bool) -> MatchDecision {
        let mut result = decision(self.info_exclude.matched(path, is_dir));
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

    fn discover_directory(&mut self, directory: &Path) -> io::Result<()> {
        let ignore_file = directory.join(".gitignore");
        if ignore_file.is_file() {
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
            if self.is_pruned_path(&path) || self.is_ignored(&path, true) {
                continue;
            }
            self.discover_directory(&path)?;
        }
        Ok(())
    }
}

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

fn load_matcher(root: &Path, source: &Path) -> io::Result<Gitignore> {
    if !source.is_file() {
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

fn has_git_marker(directory: &Path) -> bool {
    let marker = directory.join(".git");
    marker.is_file() || marker.is_dir()
}

pub(crate) fn is_nested_git_marker_path(repo_root: &Path, path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == ".git")
        && path
            .parent()
            .is_some_and(|parent| parent != repo_root && parent.starts_with(repo_root))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let matcher = RepoIgnoreMatcher::build(&root, &paths.info_exclude).unwrap();
        assert!(matcher.is_ignored(&root.join("generated.rs"), false));
    }
}
