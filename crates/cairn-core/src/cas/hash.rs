//! Blob hashing in git-compatible format.
//!
//! `git hash-object` computes `sha1("blob " || ascii_len(content) ||
//! "\0" || content)`. Reproducing that format here lets a cairn blob
//! sha equal the git blob sha for the same bytes — opens the door to
//! zero-copy reuse from git's object store later.

use sha1::{Digest, Sha1};

/// Compute the git-style blob sha (40-hex) for `content`.
#[must_use]
pub fn git_blob_sha(content: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", content.len()).as_bytes());
    hasher.update(content);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known-good values from `git hash-object` invoked on the bytes
    // shown. These are stable across git versions because the format
    // is fixed by git's hash function.

    #[test]
    fn empty_blob() {
        // `printf '' | git hash-object --stdin`
        assert_eq!(
            git_blob_sha(b""),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
    }

    #[test]
    fn hello_newline() {
        // `printf 'hello\n' | git hash-object --stdin`
        assert_eq!(
            git_blob_sha(b"hello\n"),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
    }

    #[test]
    fn no_trailing_newline() {
        // `printf 'hello' | git hash-object --stdin`
        assert_eq!(
            git_blob_sha(b"hello"),
            "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0"
        );
    }

    #[test]
    fn deterministic() {
        let a = git_blob_sha(b"some content");
        let b = git_blob_sha(b"some content");
        assert_eq!(a, b);
    }

    #[test]
    fn distinguishes_content() {
        assert_ne!(git_blob_sha(b"a"), git_blob_sha(b"b"));
    }
}
