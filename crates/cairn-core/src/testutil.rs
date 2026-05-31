//! Shared test fixtures.
//!
//! Each test that needs an on-disk git repo with a known initial
//! commit used to keep its own copy of `run_git` and `init_repo`;
//! this module is the single home.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Run `git` inside `repo` with the developer's global config
/// blanked out so a `core.hooksPath` pre-commit hook can't block the
/// test commit. Panics on non-zero exit (= every call site needs the
/// command to succeed).
pub fn run_git(repo: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@t")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@t")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Capture `git`'s stdout (trimmed) under the same isolation as
/// [`run_git`]. Panics on non-zero exit.
pub fn git_capture(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

/// Create a new tempdir, `git init` it, write `files`, commit them,
/// and return the tempdir + the resulting `HEAD` commit sha. Callers
/// that don't care about the sha can `let (tmp, _) = init_repo(...)`.
pub fn init_repo(files: &[(&str, &str)]) -> (tempfile::TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path();
    run_git(repo, &["init", "-q", "-b", "main"]);
    for (rel, content) in files {
        let p = repo.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(p, content).unwrap();
    }
    run_git(repo, &["add", "-A"]);
    run_git(repo, &["commit", "-q", "-m", "init"]);
    let sha = git_capture(repo, &["rev-parse", "HEAD"]);
    (tmp, sha)
}
