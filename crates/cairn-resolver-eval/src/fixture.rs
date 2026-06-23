//! Fixture loader.
//!
//! Each fixture lives at `crates/cairn-resolver-eval/fixtures/<lang>/`
//! as a tree of source files (no `.git`). At test time we copy the
//! tree into a tempdir, `git init` it, and commit so cairn can resolve
//! a HEAD anchor. The tempdir owns the lifetime; drop it to clean up.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

/// A live fixture: tempdir + the path to the working tree root.
pub struct LiveFixture {
    pub tempdir: tempfile::TempDir,
    pub repo_root: PathBuf,
}

/// Path to the on-disk fixture for `language` (relative to this
/// crate's manifest dir). Panics if the fixture tree is missing —
/// indicates a misconfigured test, not a runtime condition.
pub fn fixture_dir(language: &str) -> PathBuf {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(language);
    assert!(base.is_dir(), "fixture missing: {}", base.display());
    base
}

/// Copy the fixture tree for `language` into a fresh tempdir, then
/// `git init` + commit so cairn can register it.
pub fn build(language: &str) -> Result<LiveFixture> {
    let src = fixture_dir(language);
    let tempdir = tempfile::tempdir().context("create tempdir")?;
    let repo_root = tempdir.path().to_path_buf();
    copy_tree(&src, &repo_root)?;
    git_init_and_commit(&repo_root)?;
    Ok(LiveFixture { tempdir, repo_root })
}

fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    // Manual recursive copy — we avoid `fs::copy`'s parent-creation
    // semantics by walking entries ourselves. Symlinks are not
    // expected in fixtures; if a future fixture needs them, follow vs.
    // preserve becomes a design call.
    for entry in walk(src)? {
        let rel = entry.strip_prefix(src).unwrap();
        let target = dst.join(rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&target)
                .with_context(|| format!("mkdir {}", target.display()))?;
        } else if entry.is_file() {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("mkdir {}", parent.display()))?;
            }
            std::fs::copy(&entry, &target)
                .with_context(|| format!("copy {} -> {}", entry.display(), target.display()))?;
        }
    }
    Ok(())
}

fn walk(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        out.push(p.clone());
        if p.is_dir() {
            for entry in
                std::fs::read_dir(&p).with_context(|| format!("readdir {}", p.display()))?
            {
                stack.push(entry?.path());
            }
        }
    }
    Ok(out)
}

fn git_init_and_commit(repo: &Path) -> Result<()> {
    // Isolation matches `cairn-core::testutil::run_git`: blank out the
    // developer's global config so a `core.hooksPath` pre-commit hook
    // cannot block the fixture commit, and pin author/committer.
    let envs: &[(&str, &str)] = &[
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_SYSTEM", "/dev/null"),
        ("GIT_AUTHOR_NAME", "t"),
        ("GIT_AUTHOR_EMAIL", "t@t"),
        ("GIT_COMMITTER_NAME", "t"),
        ("GIT_COMMITTER_EMAIL", "t@t"),
    ];
    for args in [
        &["init", "-q", "-b", "main"][..],
        &["add", "-A"][..],
        &["commit", "-q", "-m", "fixture"][..],
    ] {
        let mut cmd = Command::new("git");
        cmd.arg("-C").arg(repo).args(args);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        let out = cmd.output().with_context(|| format!("git {args:?}"))?;
        if !out.status.success() {
            bail!(
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
    Ok(())
}
