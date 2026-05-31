//! Unix domain socket paths and bind helpers.
//!
//! The daemon listens on two sockets in a per-user runtime directory:
//! - `cairn.sock` — newline-JSON / JSON-RPC frames consumed by AI agents
//! - `control.sock` — management commands from `cairn ctl`
//!
//! Path resolution prefers `$XDG_RUNTIME_DIR/cairn` on Linux and
//! `~/Library/Caches/cairn` on macOS, both with permission 0700 so
//! only the owning user can connect. We do not perform an explicit
//! per-connection peer-UID check; the parent directory's permissions
//! already restrict who can reach the sockets to the same uid that
//! launched the daemon.

use std::path::{Path, PathBuf};

use crate::{Error, Result};

const APP_NAME: &str = "cairn";

/// Resolved location of the two daemon sockets.
#[derive(Debug, Clone)]
pub struct SocketPaths {
    pub runtime_dir: PathBuf,
    pub cairn: PathBuf,
    pub control: PathBuf,
}

impl SocketPaths {
    /// Resolve from the platform conventions.
    ///
    /// # Errors
    /// Fails if no runtime / cache directory exists for the user.
    pub fn from_platform_default() -> Result<Self> {
        let base = runtime_base()?;
        let runtime_dir = base.join(APP_NAME);
        Ok(Self {
            cairn: runtime_dir.join("cairn.sock"),
            control: runtime_dir.join("control.sock"),
            runtime_dir,
        })
    }

    /// Test / override constructor.
    #[must_use]
    pub fn with_runtime_dir(runtime_dir: PathBuf) -> Self {
        Self {
            cairn: runtime_dir.join("cairn.sock"),
            control: runtime_dir.join("control.sock"),
            runtime_dir,
        }
    }

    /// Create the runtime directory if missing and tighten its
    /// permissions to 0700. Stale socket files from a previous run
    /// are removed.
    ///
    /// # Errors
    /// Filesystem failures.
    pub fn ensure(&self) -> Result<()> {
        std::fs::create_dir_all(&self.runtime_dir)?;
        tighten_dir_permissions(&self.runtime_dir)?;
        remove_if_exists(&self.cairn)?;
        remove_if_exists(&self.control)?;
        Ok(())
    }
}

fn runtime_base() -> Result<PathBuf> {
    // Linux: prefer XDG_RUNTIME_DIR (tmpfs, per-user, auto-cleaned on logout).
    if let Some(p) = dirs::runtime_dir() {
        return Ok(p);
    }
    // macOS / fallback: cache dir.
    if let Some(p) = dirs::cache_dir() {
        return Ok(p);
    }
    Err(Error::InvalidArgument(
        "platform has no runtime or cache directory".into(),
    ))
}

#[cfg(unix)]
fn tighten_dir_permissions(p: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(0o700);
    std::fs::set_permissions(p, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn tighten_dir_permissions(_p: &Path) -> Result<()> {
    // Non-Unix targets are out of scope for 0.1.0; UDS is Unix-only
    // anyway, so the daemon would not start on Windows.
    Ok(())
}

fn remove_if_exists(p: &Path) -> Result<()> {
    match std::fs::remove_file(p) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_creates_dir_and_removes_stale_sockets() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cc");
        let paths = SocketPaths::with_runtime_dir(dir.clone());
        // Plant a fake stale socket file.
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&paths.cairn, b"stale").unwrap();
        assert!(paths.cairn.exists());

        paths.ensure().unwrap();
        assert!(dir.is_dir());
        assert!(!paths.cairn.exists());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_tightens_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cc");
        let paths = SocketPaths::with_runtime_dir(dir.clone());
        paths.ensure().unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn paths_compose_under_runtime_dir() {
        let paths = SocketPaths::with_runtime_dir(PathBuf::from("/run/me"));
        assert_eq!(paths.cairn, PathBuf::from("/run/me/cairn.sock"));
        assert_eq!(paths.control, PathBuf::from("/run/me/control.sock"));
    }
}
