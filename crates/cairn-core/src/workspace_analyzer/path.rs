//! URI → repo-relative path mapping shared by all Tier-3 passes.
//!
//! Targets that resolve outside the registered repository root (e.g.
//! definitions inside a dependency checkout or the standard library)
//! map to `None` and are skipped by persistence — guessing a manifest
//! path from a foreign URI risks colliding with a same-named file in
//! the repo.

use std::path::{Path, PathBuf};

use crate::lsp::Location;

/// Map an LSP definition target back to a path relative to
/// `repo_root`, or `None` when the target lives outside the repo.
pub(super) fn location_to_repo_path(repo_root: &Path, location: &Location) -> Option<String> {
    let path = file_uri_to_path(location.uri.as_str())?;
    let rel = path.strip_prefix(repo_root).ok()?;
    Some(rel.to_string_lossy().replace('\\', "/"))
}

fn file_uri_to_path(uri: &str) -> Option<PathBuf> {
    let path = uri.strip_prefix("file://")?;
    percent_decode(path).map(PathBuf::from)
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = *bytes.get(i + 1)?;
            let lo = *bytes.get(i + 2)?;
            out.push(hex_value(hi)? * 16 + hex_value(lo)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsp::{Position, Range, Url};

    fn location(uri: &str) -> Location {
        Location {
            uri: Url::from(uri),
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: 1,
                },
            },
        }
    }

    #[test]
    fn maps_in_repo_uri_to_relative_path() {
        let rel = location_to_repo_path(
            Path::new("/tmp/repo"),
            &location("file:///tmp/repo/crates/foo/src/lib.rs"),
        );
        assert_eq!(rel.as_deref(), Some("crates/foo/src/lib.rs"));
    }

    #[test]
    fn decodes_percent_encoded_uri() {
        let rel = location_to_repo_path(
            Path::new("/tmp/repo"),
            &location("file:///tmp/repo/src/sp%20ace.rs"),
        );
        assert_eq!(rel.as_deref(), Some("src/sp ace.rs"));
    }

    #[test]
    fn out_of_repo_target_maps_to_none() {
        // A dependency checkout containing `/src/` must not be guessed
        // into a repo-relative path (it would collide with the repo's
        // own `src/lib.rs`).
        let rel = location_to_repo_path(
            Path::new("/tmp/repo"),
            &location("file:///home/u/.cargo/registry/src/index/foo-1.0/src/lib.rs"),
        );
        assert_eq!(rel, None);
    }

    #[test]
    fn malformed_percent_escape_maps_to_none() {
        let rel = location_to_repo_path(Path::new("/tmp/repo"), &location("file:///tmp/repo/%zz"));
        assert_eq!(rel, None);
    }
}
