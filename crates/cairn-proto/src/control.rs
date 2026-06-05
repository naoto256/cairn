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

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::common::LanguageEnrichment;

// ─── prune ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneResult {
    pub repos: Vec<PruneRepoEntry>,
    pub total_deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneRepoEntry {
    pub alias: String,
    pub deleted_blob_count: u64,
}

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
    pub snapshots: Vec<SnapshotStatus>,
}

impl RepoStatus {
    #[must_use]
    pub fn languages(&self) -> BTreeSet<&str> {
        self.snapshots
            .iter()
            .flat_map(|s| s.enrichment.iter().map(|e| e.language.as_str()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStatus {
    pub branches: Vec<String>,
    pub status: String,
    pub enrichment: Vec<LanguageEnrichment>,
    pub file_count: u64,
    pub symbol_count: u64,
    pub size_bytes: u64,
}

impl SnapshotStatus {
    /// First branch label in `branches` ordering (`HEAD` if present).
    #[must_use]
    pub fn primary_label(&self) -> Option<&str> {
        self.branches.first().map(String::as_str)
    }

    /// Whether `HEAD` points at this snapshot's manifest.
    #[must_use]
    pub fn has_head(&self) -> bool {
        self.branches.iter().any(|b| b == "HEAD")
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;
    use crate::common::SourceTier;

    #[test]
    fn snapshot_status_serializes_enrichment_matrix() {
        let status = SnapshotStatus {
            branches: vec!["HEAD".into(), "main".into()],
            status: "ready".into(),
            enrichment: vec![LanguageEnrichment {
                language: "python".into(),
                tier: SourceTier::Syntactic,
                has_analyzer: true,
            }],
            file_count: 1,
            symbol_count: 2,
            size_bytes: 3,
        };
        let v = serde_json::to_value(&status).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "branches": ["HEAD", "main"],
                "status": "ready",
                "enrichment": [{
                    "language": "python",
                    "tier": "syntactic",
                    "has_analyzer": true
                }],
                "file_count": 1,
                "symbol_count": 2,
                "size_bytes": 3
            })
        );
        let back: SnapshotStatus = serde_json::from_value(v).unwrap();
        assert_eq!(back.enrichment[0].language, "python");
        assert_eq!(back.primary_label(), Some("HEAD"));
        assert!(back.has_head());
    }

    #[test]
    fn repo_status_derives_languages_from_snapshots() {
        let repo = RepoStatus {
            alias: "cairn".into(),
            root: "/tmp/cairn".into(),
            snapshots: vec![SnapshotStatus {
                branches: vec!["HEAD".into()],
                status: "ready".into(),
                enrichment: vec![LanguageEnrichment {
                    language: "rust".into(),
                    tier: SourceTier::Semantic,
                    has_analyzer: true,
                }],
                file_count: 1,
                symbol_count: 1,
                size_bytes: 1,
            }],
        };
        assert_eq!(
            repo.languages().into_iter().collect::<Vec<_>>(),
            vec!["rust"]
        );
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorStatus {
    Pass,
    Warn,
    Fail,
}

#[cfg(test)]
mod doctor_tests {
    use super::*;

    #[test]
    fn doctor_check_omits_absent_remediation_and_roundtrips_present_value() {
        let without_remediation = DoctorCheck {
            name: "data directory".into(),
            status: DoctorStatus::Pass,
            detail: Some("/tmp/cairn".into()),
            remediation: None,
        };
        let value = serde_json::to_value(&without_remediation).unwrap();
        assert!(value.get("remediation").is_none());
        let back: DoctorCheck = serde_json::from_value(value).unwrap();
        assert_eq!(back.remediation, None);

        let with_remediation: DoctorCheck = serde_json::from_value(serde_json::json!({
            "name": "repo `demo` root present",
            "status": "fail",
            "detail": "/tmp/missing",
            "remediation": "restore the directory"
        }))
        .unwrap();
        assert_eq!(
            with_remediation.remediation.as_deref(),
            Some("restore the directory")
        );
    }
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
    /// Mutating requests can succeed while a best-effort side effect
    /// fails. `register_repo` uses this when indexing and alias
    /// registration completed but the live filesystem watcher could
    /// not be installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watcher_failed: Option<String>,
}

impl Ack {
    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            alias: None,
            watcher_failed: None,
        }
    }

    #[must_use]
    pub fn with_alias(alias: impl Into<String>) -> Self {
        Self {
            ok: true,
            alias: Some(alias.into()),
            watcher_failed: None,
        }
    }

    #[must_use]
    pub fn with_alias_and_watcher_failed(alias: impl Into<String>, reason: String) -> Self {
        Self {
            ok: true,
            alias: Some(alias.into()),
            watcher_failed: Some(reason),
        }
    }
}
