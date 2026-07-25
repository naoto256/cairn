//! Version compatibility helpers shared by cairn front-ends.
//!
//! The daemon reports its own `CARGO_PKG_VERSION` on the control
//! `status` response (see `cairn_core::ctl::methods::status`). The
//! CLI and MCP front-ends compare it against their own compiled-in
//! `CARGO_PKG_VERSION` via [`pre_one_zero_compat`] before dispatching
//! real work. The comparison inspects only the SemVer core
//! (`major.minor.patch`) — build (`+…`) and prerelease (`-…`)
//! suffixes are stripped by `parse_version` and never participate
//! in the equality check.
//!
//! Downstream, `cairn/src/cmd/version_guard.rs` acts on the returned
//! variant as follows:
//! - [`VersionCompatibility::SamePatch`] and
//!   [`VersionCompatibility::PatchMismatch`] are silently accepted.
//! - [`VersionCompatibility::MinorMismatch`] emits a stderr warning
//!   and proceeds.
//! - [`VersionCompatibility::MajorMismatch`] is fatal for the
//!   interactive CLI; MCP downgrades it to a stderr warning so
//!   JSON-RPC initialization can still complete on the host side.
//! - [`VersionCompatibility::Unparseable`] emits a warning and
//!   proceeds regardless of mode, so a daemon reporting an
//!   unrecognised version string never blocks callers.
//!
//! The choice to treat `PatchMismatch` as silently compatible is what
//! encodes cairn's pre-1.0 SemVer 0.x policy (documented in the
//! top-level README): patch bumps are meant to preserve the wire
//! shape, and minor bumps are the earliest a wire-observable change
//! is permitted. That shape-level rule is enforced editorially by
//! reviewers, not by this module — the comparison here only decides
//! how loudly the guard reacts to a mismatch it observes at runtime.

/// Coarse compatibility level between a client binary and a daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCompatibility {
    /// Same `major.minor.patch`.
    SamePatch,
    /// Same major/minor, different patch.
    PatchMismatch,
    /// Same major, different minor.
    MinorMismatch,
    /// Different major.
    MajorMismatch,
    /// One side was not a `major.minor.patch` version.
    Unparseable,
}

/// SemVer core extracted from a version string, with the
/// prerelease (`-alpha.1`) and build (`+release`) metadata
/// discarded. Only the three `u64` components participate in the
/// compatibility comparison in [`pre_one_zero_compat`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

/// Compare daemon and client versions using cairn's pre-1.0 rule: patch
/// drift is compatible, minor drift is noteworthy, major drift is unsafe.
#[must_use]
pub fn pre_one_zero_compat(daemon: &str, client: &str) -> VersionCompatibility {
    let Some(daemon) = parse_version(daemon) else {
        return VersionCompatibility::Unparseable;
    };
    let Some(client) = parse_version(client) else {
        return VersionCompatibility::Unparseable;
    };
    if daemon.major != client.major {
        return VersionCompatibility::MajorMismatch;
    }
    if daemon.minor != client.minor {
        return VersionCompatibility::MinorMismatch;
    }
    if daemon.patch != client.patch {
        return VersionCompatibility::PatchMismatch;
    }
    VersionCompatibility::SamePatch
}

/// Parse a version string into its SemVer core numbers.
///
/// Returns `None` when the core does not have exactly three numeric
/// components: fewer components short-circuit on `?`, and a trailing
/// fourth component is rejected explicitly. Build metadata (after
/// `+`) is stripped first, then the prerelease tail (after `-`), so
/// inputs like `"0.4.2+release"` and `"0.4.2-alpha.1"` both reduce
/// to `ParsedVersion { major: 0, minor: 4, patch: 2 }`.
fn parse_version(version: &str) -> Option<ParsedVersion> {
    let core = version.split_once('+').map_or(version, |(core, _)| core);
    let core = core.split_once('-').map_or(core, |(core, _)| core);
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(ParsedVersion {
        major,
        minor,
        patch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_one_zero_compat_silent_for_same_patch() {
        assert_eq!(
            pre_one_zero_compat("0.4.2", "0.4.2"),
            VersionCompatibility::SamePatch
        );
    }

    #[test]
    fn pre_one_zero_compat_allows_patch_drift() {
        assert_eq!(
            pre_one_zero_compat("0.4.1", "0.4.2"),
            VersionCompatibility::PatchMismatch
        );
    }

    #[test]
    fn pre_one_zero_compat_warns_on_minor_drift() {
        assert_eq!(
            pre_one_zero_compat("0.3.0", "0.4.2"),
            VersionCompatibility::MinorMismatch
        );
    }

    #[test]
    fn pre_one_zero_compat_rejects_major_drift() {
        assert_eq!(
            pre_one_zero_compat("1.0.0", "0.4.2"),
            VersionCompatibility::MajorMismatch
        );
    }

    #[test]
    fn pre_one_zero_compat_accepts_build_and_prerelease_suffixes() {
        assert_eq!(
            pre_one_zero_compat("0.4.2+release", "0.4.2-alpha.1"),
            VersionCompatibility::SamePatch
        );
    }

    #[test]
    fn pre_one_zero_compat_marks_unparseable_versions() {
        assert_eq!(
            pre_one_zero_compat("0.4", "0.4.2"),
            VersionCompatibility::Unparseable
        );
        assert_eq!(
            pre_one_zero_compat("0.4.2", "dev"),
            VersionCompatibility::Unparseable
        );
    }
}
