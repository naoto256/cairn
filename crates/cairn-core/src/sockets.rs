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
#[cfg(all(unix, not(target_os = "macos")))]
use std::sync::{Mutex, MutexGuard};

use tokio::net::UnixListener;

use crate::{Error, Result};

const APP_NAME: &str = "cairn";
// Owner-only permission bits: rwx for the runtime directory (it must
// be enterable), rw for the socket nodes themselves.
const RUNTIME_DIR_MODE: u32 = 0o700;
const SOCKET_FILE_MODE: u32 = 0o600;
// umask(2) is process-global, not per-thread: serialize bind-time
// umask swaps so concurrent binds (e.g. parallel tests) cannot
// observe or clobber each other's mask.
#[cfg(all(unix, not(target_os = "macos")))]
static UMASK_LOCK: Mutex<()> = Mutex::new(());

/// Resolved location of the two daemon sockets.
#[derive(Debug, Clone)]
pub struct SocketPaths {
    /// Parent directory of both sockets; the 0700 access boundary.
    pub runtime_dir: PathBuf,
    /// Data-plane socket (`cairn.sock`).
    pub cairn: PathBuf,
    /// Management socket (`control.sock`).
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

    /// Create the runtime directory if missing and verify that it is
    /// owned by the current uid with permissions 0700. Stale socket
    /// files from a previous run are removed.
    ///
    /// Any existing socket node is treated as stale and unlinked, so
    /// callers must have established that no live daemon is serving
    /// these paths before calling.
    ///
    /// # Errors
    /// Filesystem failures.
    pub fn ensure(&self) -> Result<()> {
        create_secure_runtime_dir(&self.runtime_dir)?;
        remove_if_exists(&self.cairn)?;
        remove_if_exists(&self.control)?;
        Ok(())
    }
}

/// Bind a Unix listener and force the socket node to 0600.
///
/// The runtime directory is the primary same-UID boundary; setting the
/// socket node immediately after bind adds defense in depth if an
/// ancestor's permissions are later weakened.
pub fn bind_socket_with_mode(path: &Path) -> Result<UnixListener> {
    let listener = bind_socket_private(path)?;
    set_socket_file_permissions(path)?;
    Ok(listener)
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
fn create_secure_runtime_dir(p: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    // The directory gates access in lieu of per-connection peer-UID
    // checks, so create it private from the first observable moment
    // and never "repair" an already-existing insecure directory.
    match std::fs::DirBuilder::new().mode(RUNTIME_DIR_MODE).create(p) {
        Ok(()) => {
            // DirBuilder's mode is filtered through the process umask;
            // the explicit chmod pins the fresh directory at exactly
            // 0700 regardless of the inherited mask.
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(RUNTIME_DIR_MODE))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    validate_runtime_dir_security(p, rustix::process::geteuid().as_raw())
}

/// Reject the runtime directory unless it is a real (non-symlink)
/// directory owned by `expected_uid` with mode exactly 0700.
///
/// `symlink_metadata` deliberately does not follow symlinks, so a
/// link planted at the path cannot redirect the check to a target
/// that would pass while the sockets land somewhere else.
#[cfg(unix)]
fn validate_runtime_dir_security(p: &Path, expected_uid: u32) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = std::fs::symlink_metadata(p)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "runtime socket path {} must be a directory",
            p.display()
        )));
    }

    let owner = metadata.uid();
    if owner != expected_uid {
        return Err(Error::InvalidArgument(format!(
            "runtime socket directory {} is owned by uid {owner}, expected uid {expected_uid}",
            p.display()
        )));
    }

    let mode = metadata.permissions().mode() & 0o777;
    if mode != RUNTIME_DIR_MODE {
        return Err(Error::InvalidArgument(format!(
            "runtime socket directory {} has mode {mode:o}, expected {RUNTIME_DIR_MODE:o}",
            p.display()
        )));
    }

    Ok(())
}

#[cfg(not(unix))]
fn create_secure_runtime_dir(p: &Path) -> Result<()> {
    // Non-Unix targets are out of scope for 0.1.0; UDS is Unix-only
    // anyway, so the daemon would not start on Windows.
    std::fs::create_dir_all(p)?;
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn bind_socket_private(path: &Path) -> Result<UnixListener> {
    // 0o077 — not 0o177 — so directories temporarily created by parallel
    // tests under this process-wide guard still get owner-execute and can
    // be entered; both values render the socket node 0o600.
    let guard = UmaskGuard::set(0o077);
    // UDS nodes inherit process umask at bind time; create them private
    // before any client can observe the path.
    let listener = UnixListener::bind(path)?;
    drop(guard);
    Ok(listener)
}

#[cfg(target_os = "macos")]
fn bind_socket_private(path: &Path) -> Result<UnixListener> {
    // No umask manipulation on macOS: socket-node modes cannot be
    // tightened reliably here anyway (see the EPERM handling in
    // `set_socket_file_permissions`), so the enforced 0700 runtime
    // directory is the effective same-UID boundary on this platform.
    Ok(UnixListener::bind(path)?)
}

#[cfg(not(unix))]
fn bind_socket_private(path: &Path) -> Result<UnixListener> {
    Ok(UnixListener::bind(path)?)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn set_socket_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_FILE_MODE))?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn set_socket_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    match std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_FILE_MODE)) {
        Ok(()) => {}
        Err(e)
            if e.kind() == std::io::ErrorKind::PermissionDenied || e.raw_os_error() == Some(1) =>
        {
            // Darwin reports EPERM for chmod on socket nodes. The
            // enforced 0700 runtime directory remains the effective
            // same-UID boundary on this platform.
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_socket_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

/// RAII guard that swaps the process umask and restores the previous
/// value on drop. It holds `UMASK_LOCK` for its whole lifetime
/// because the umask is shared by every thread in the process.
#[cfg(all(unix, not(target_os = "macos")))]
struct UmaskGuard {
    previous: rustix::fs::Mode,
    _lock: MutexGuard<'static, ()>,
}

#[cfg(all(unix, not(target_os = "macos")))]
impl UmaskGuard {
    fn set(mask: u32) -> Self {
        // A poisoned lock only means a previous holder panicked; that
        // guard's Drop already restored the prior mask during unwind,
        // so recovering the lock here is sound.
        let lock = UMASK_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self {
            previous: rustix::process::umask(rustix::fs::Mode::from(mask)),
            _lock: lock,
        }
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
impl Drop for UmaskGuard {
    fn drop(&mut self) {
        let _ = rustix::process::umask(self.previous);
    }
}

/// Unlink a stale socket path, tolerating its absence.
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
        std::fs::create_dir(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::write(&paths.cairn, b"stale").unwrap();
        assert!(paths.cairn.exists());

        paths.ensure().unwrap();
        assert!(dir.is_dir());
        assert!(!paths.cairn.exists());
    }

    #[cfg(unix)]
    #[test]
    fn ensure_creates_dir_with_secure_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cc");
        let paths = SocketPaths::with_runtime_dir(dir.clone());
        paths.ensure().unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn ensure_rejects_runtime_dir_with_loose_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cc");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        let paths = SocketPaths::with_runtime_dir(dir);
        let err = paths.ensure().expect_err("loose runtime dir must fail");
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[cfg(unix)]
    #[test]
    fn validate_runtime_dir_security_rejects_other_owner() {
        use std::os::unix::fs::MetadataExt;

        let tmp = tempfile::tempdir().unwrap();
        let actual_uid = std::fs::metadata(tmp.path()).unwrap().uid();
        let other_uid = actual_uid.wrapping_add(1);

        let err = validate_runtime_dir_security(tmp.path(), other_uid)
            .expect_err("other-owner runtime dir must fail");
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[tokio::test]
    async fn bind_socket_with_mode_sets_socket_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("cairn.sock");
        let listener = bind_socket_with_mode(&socket).unwrap();

        let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        drop(listener);
        std::fs::remove_file(socket).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn bind_socket_with_mode_tolerates_macos_socket_chmod_limitation() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("cairn.sock");
        let listener = bind_socket_with_mode(&socket).unwrap();

        assert!(socket.exists());

        drop(listener);
        std::fs::remove_file(socket).unwrap();
    }

    #[test]
    fn paths_compose_under_runtime_dir() {
        let paths = SocketPaths::with_runtime_dir(PathBuf::from("/run/me"));
        assert_eq!(paths.cairn, PathBuf::from("/run/me/cairn.sock"));
        assert_eq!(paths.control, PathBuf::from("/run/me/control.sock"));
    }
}
