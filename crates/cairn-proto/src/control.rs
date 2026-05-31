//! Control socket result payloads.
//!
//! The control socket speaks the same JSON-RPC 2.0 envelope as the
//! data socket (see [`crate::jsonrpc`]); only the set of methods
//! differs. This module just carries the result-payload shapes for
//! the admin verbs that the daemon's `ctl/methods/*` modules emit
//! and `cairn ctl` consumes.
//!
//! Method argument shapes for `register_repo` / `remove_repo` /
//! `reindex_repo` are in [`crate::methods`]. Verbs with no args
//! (`status`, `doctor`, `shutdown`) accept either `null` or `{}`.

use serde::{Deserialize, Serialize};

use crate::common::SourceTier;

// ─── status ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    pub daemon_version: String,
    pub uptime_secs: u64,
    pub repos: Vec<RepoStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatus {
    pub alias: String,
    pub root: String,
    pub languages: Vec<String>,
    pub snapshots: Vec<SnapshotStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStatus {
    pub branch: String,
    pub status: String,
    pub enrichment: SourceTier,
    pub file_count: u64,
    pub symbol_count: u64,
    pub size_bytes: u64,
}

// ─── doctor ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    pub checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorCheck {
    pub name: String,
    pub status: DoctorStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorStatus {
    Pass,
    Warn,
    Fail,
}

// ─── remove_repo ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveRepoArgs {
    pub alias: String,
}

/// Generic "operation accepted" payload returned by mutating methods
/// (`register_repo`, `remove_repo`, `reindex_repo`, `shutdown`) that
/// have nothing structured to say beyond "it worked". Callers that
/// only care about success can ignore the body and rely on the
/// JSON-RPC `result` vs `error` discriminator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl Ack {
    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            alias: None,
        }
    }

    #[must_use]
    pub fn with_alias(alias: impl Into<String>) -> Self {
        Self {
            ok: true,
            alias: Some(alias.into()),
        }
    }
}
