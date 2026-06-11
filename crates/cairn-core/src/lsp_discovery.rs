//! Shared LSP binary discovery.
//!
//! Doctor checks and Tier-3 workers must resolve binaries the same way. The
//! daemon is often launched by launchd with a minimal PATH, so checking common
//! Homebrew prefixes before PATH keeps worker execution aligned with doctor.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Find an LSP binary by name.
///
/// Resolution order:
/// 1. explicit env var override, when provided
/// 2. common Homebrew prefix variants
/// 3. PATH search
pub fn discover_lsp_binary(bare_name: &str, env_override: Option<&str>) -> Option<PathBuf> {
    discover_lsp_binary_candidates(&[bare_name], env_override)
}

/// Find an LSP binary by trying multiple command names.
///
/// This is useful for servers whose wrapper name differs by installation
/// source, such as `phpantom_lsp` vs `phpantom-lsp`.
pub fn discover_lsp_binary_candidates(
    bare_names: &[&str],
    env_override: Option<&str>,
) -> Option<PathBuf> {
    discover_lsp_binary_candidates_with(
        bare_names,
        env_override.and_then(std::env::var_os),
        std::env::var_os("PATH"),
        &homebrew_prefixes(),
    )
}

/// Find sourcekit-lsp using SOURCEKIT_LSP, xcrun, Homebrew prefixes, then PATH.
pub fn discover_sourcekit_lsp() -> Option<PathBuf> {
    if let Some(path) = env_path(std::env::var_os("SOURCEKIT_LSP")) {
        return Some(path);
    }
    if let Some(path) = sourcekit_lsp_from_xcrun() {
        return Some(path);
    }
    discover_lsp_binary_candidates_with(
        &["sourcekit-lsp"],
        None,
        std::env::var_os("PATH"),
        &homebrew_prefixes(),
    )
}

fn discover_lsp_binary_candidates_with(
    bare_names: &[&str],
    env_value: Option<OsString>,
    path_value: Option<OsString>,
    homebrew_prefixes: &[PathBuf],
) -> Option<PathBuf> {
    if let Some(path) = env_path(env_value) {
        return Some(path);
    }
    for prefix in homebrew_prefixes {
        for bare_name in bare_names {
            if let Some(path) = canonical_file(prefix.join("bin").join(bare_name)) {
                return Some(path);
            }
        }
    }
    if let Some(paths) = path_value {
        for dir in std::env::split_paths(&paths) {
            for bare_name in bare_names {
                if let Some(path) = canonical_file(dir.join(bare_name)) {
                    return Some(path);
                }
            }
        }
    }
    None
}

fn env_path(value: Option<OsString>) -> Option<PathBuf> {
    value.and_then(canonical_file)
}

fn canonical_file(path: impl AsRef<Path>) -> Option<PathBuf> {
    let path = path.as_ref();
    path.is_file()
        .then(|| path.canonicalize().unwrap_or_else(|_| path.to_path_buf()))
}

fn homebrew_prefixes() -> Vec<PathBuf> {
    ["/opt/homebrew", "/usr/local"]
        .into_iter()
        .map(PathBuf::from)
        .collect()
}

fn sourcekit_lsp_from_xcrun() -> Option<PathBuf> {
    // macOS installs sourcekit-lsp inside the selected Xcode/Swift toolchain,
    // where PATH often does not include it. `xcrun --find` respects
    // xcode-select, while non-macOS Swift toolchains are handled by PATH below.
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("xcrun")
            .args(["--find", "sourcekit-lsp"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let path = String::from_utf8(output.stdout).ok()?;
        canonical_file(PathBuf::from(path.trim()))
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, b"").unwrap();
    }

    #[test]
    fn env_override_wins_over_homebrew_and_path() {
        let temp = tempfile::tempdir().unwrap();
        let env_binary = temp.path().join("env").join("server");
        let homebrew_binary = temp.path().join("homebrew").join("bin").join("server");
        let path_binary = temp.path().join("path").join("server");
        touch(&env_binary);
        touch(&homebrew_binary);
        touch(&path_binary);

        let resolved = discover_lsp_binary_candidates_with(
            &["server"],
            Some(env_binary.clone().into_os_string()),
            Some(temp.path().join("path").into_os_string()),
            &[temp.path().join("homebrew")],
        );

        assert_eq!(resolved, Some(env_binary.canonicalize().unwrap()));
    }

    #[test]
    fn homebrew_prefixes_win_over_path() {
        let temp = tempfile::tempdir().unwrap();
        let homebrew_binary = temp.path().join("homebrew").join("bin").join("server");
        let path_binary = temp.path().join("path").join("server");
        touch(&homebrew_binary);
        touch(&path_binary);

        let resolved = discover_lsp_binary_candidates_with(
            &["server"],
            None,
            Some(temp.path().join("path").into_os_string()),
            &[temp.path().join("homebrew")],
        );

        assert_eq!(resolved, Some(homebrew_binary.canonicalize().unwrap()));
    }

    #[test]
    fn candidates_allow_alternate_wrapper_names() {
        let temp = tempfile::tempdir().unwrap();
        let path_binary = temp.path().join("path").join("phpantom-lsp");
        touch(&path_binary);

        let resolved = discover_lsp_binary_candidates_with(
            &["phpantom_lsp", "phpantom-lsp"],
            None,
            Some(temp.path().join("path").into_os_string()),
            &[],
        );

        assert_eq!(resolved, Some(path_binary.canonicalize().unwrap()));
    }
}
