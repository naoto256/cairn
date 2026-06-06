use std::path::Path;

use serde::{Deserialize, Serialize};

use super::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Url(String);

impl Url {
    /// Build a `file://` URI from an absolute UTF-8 path.
    ///
    /// # Errors
    /// Returns [`Error::Protocol`] for relative or non-UTF-8 paths.
    pub fn from_file_path(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(Error::Protocol(format!(
                "file URI path must be absolute: {}",
                path.display()
            )));
        }
        let raw = path
            .to_str()
            .ok_or_else(|| Error::Protocol(format!("non-utf8 path: {}", path.display())))?;
        Ok(Self(format!("file://{}", percent_encode_path(raw))))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Url {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Url {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Zero-based LSP position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub line: u32,
    pub character: u32,
}

/// Zero-based, half-open LSP range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub uri: Url,
    pub range: Range,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LocationLink {
    pub(super) target_uri: Url,
    pub(super) target_range: Range,
    pub(super) target_selection_range: Option<Range>,
}

fn percent_encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for b in path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(char::from(b));
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
