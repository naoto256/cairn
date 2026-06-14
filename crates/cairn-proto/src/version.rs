//! Version compatibility helpers shared by cairn front-ends.

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
