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

/// Arguments to the `prune` control method.
///
/// `repo = None` prunes every registered repo; a value restricts the
/// operation to one alias.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneArgs {
    /// Repository alias to prune, or `None` for all repos. Omitted on the
    /// wire when pruning globally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

/// Result of `prune`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneResult {
    /// Per-repository deletion counts. Empty when no registered repo matched.
    pub repos: Vec<PruneRepoEntry>,
    /// Sum of [`PruneRepoEntry::deleted_blob_count`] across all entries.
    pub total_deleted: u64,
}

/// Deletion summary for one repository store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneRepoEntry {
    /// Repository alias that was pruned.
    pub alias: String,
    /// Number of unreachable blobs removed from that repo's CAS store.
    pub deleted_blob_count: u64,
}

// ─── jobs ─────────────────────────────────────────────────────────────────

/// Arguments to the `jobs.list` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsListArgs {
    /// Optional repository alias filter. `None` lists jobs across repos.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// Optional job-state filter, using the daemon's stored state strings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

/// Arguments to the `jobs.cancel` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsCancelArgs {
    /// Numeric job id returned by [`JobSnapshot::job_id`].
    pub job_id: i64,
}

/// Result of `jobs.list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsListResult {
    /// Matching jobs ordered by the daemon-side query.
    pub jobs: Vec<JobSnapshot>,
}

/// Snapshot of one analyzer job as stored by the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSnapshot {
    /// Daemon-assigned job id. Stable enough to pass to `jobs.cancel`.
    pub job_id: i64,
    /// Repository alias the analyzer job belongs to.
    pub alias: String,
    /// Analyzer backend id that owns this job.
    pub analyzer_id: String,
    /// Current job state string. Consumers should treat unknown states as
    /// non-terminal unless the daemon documents them otherwise.
    pub state: String,
    /// Creation timestamp in nanoseconds since the Unix epoch.
    pub created_at: i64,
    /// Start timestamp in nanoseconds since the Unix epoch. `None` means the
    /// job has not started or the source row does not record a start time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    /// Finish timestamp in nanoseconds since the Unix epoch. `None` means the
    /// job is still running, queued, or has not recorded a terminal time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    /// Terminal error text. `None` means no error has been recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of `jobs.cancel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsCancelResult {
    /// True when the daemon accepted the cancellation for the requested job.
    pub cancelled: bool,
    /// Human-readable outcome, including why no cancellation happened.
    pub reason: String,
}

// ─── status ────────────────────────────────────────────────────────────────

/// Result of the `status` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    /// Daemon version string reported by the running process.
    pub daemon_version: String,
    /// Daemon uptime in whole seconds.
    pub uptime_secs: u64,
    /// Registered repositories known to the daemon.
    pub repos: Vec<RepoStatus>,
}

/// Runtime status for one registered repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoStatus {
    /// Repository alias.
    pub alias: String,
    /// Repository root path as registered with the daemon.
    pub root: String,
    /// Snapshot manifests reachable through this repo's anchors.
    pub snapshots: Vec<SnapshotStatus>,
    /// Analyzer jobs for this repo. Empty lists are omitted to keep status
    /// output compact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobSnapshot>,
}

impl RepoStatus {
    /// Distinct language tags present in this repo's snapshot enrichment.
    #[must_use]
    pub fn languages(&self) -> BTreeSet<&str> {
        self.snapshots
            .iter()
            .flat_map(|s| s.enrichment.iter().map(|e| e.language.as_str()))
            .collect()
    }
}

/// Runtime status for one snapshot manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStatus {
    /// User-facing anchor labels pointing at this manifest. `branch/<name>`
    /// anchors are rendered as `<name>`; `HEAD` and `tentative/<id>` remain
    /// explicit.
    pub branches: Vec<String>,
    /// Snapshot readiness string emitted by the daemon, e.g. `ready` or a
    /// non-ready indexing status.
    pub status: String,
    /// Per-language analyzer tier matrix for this snapshot.
    pub enrichment: Vec<LanguageEnrichment>,
    /// Number of files in the snapshot manifest.
    pub file_count: u64,
    /// Number of symbols indexed for the snapshot.
    pub symbol_count: u64,
    /// Approximate on-disk size in bytes for this snapshot's indexed data.
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
            jobs: Vec::new(),
        };
        assert_eq!(
            repo.languages().into_iter().collect::<Vec<_>>(),
            vec!["rust"]
        );
    }
}

// ─── doctor ────────────────────────────────────────────────────────────────

/// Result of the `doctor` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    /// Ordered health checks evaluated by the daemon.
    pub checks: Vec<DoctorCheck>,
}

/// One daemon health check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorCheck {
    /// Stable, human-readable check name.
    pub name: String,
    /// Outcome severity.
    pub status: DoctorStatus,
    /// Optional observed value or diagnostic context. Omitted when the check
    /// has nothing useful to add.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Optional action the operator can take. Omitted for passing checks or
    /// warnings with no concrete fix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
}

/// Severity for one [`DoctorCheck`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorStatus {
    /// Check passed.
    Pass,
    /// Check found a degraded but non-fatal condition.
    Warn,
    /// Check found a condition that prevents normal operation.
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

/// Arguments to `remove_repo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveRepoArgs {
    /// Alias to remove from the daemon's registry.
    pub alias: String,
}

/// Generic "operation accepted" payload returned by mutating methods
/// (`register_repo`, `remove_repo`, `reindex_repo`, `shutdown`) that
/// have nothing structured to say beyond "it worked". Callers that
/// only care about success can ignore the body and rely on the
/// JSON-RPC `result` vs `error` discriminator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    /// Always true for successful JSON-RPC results using this payload.
    pub ok: bool,
    /// Alias affected by the operation. `None` for operations such as
    /// `shutdown` that are not scoped to a single repo.
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
    /// Build a successful acknowledgement with no repo alias.
    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            alias: None,
            watcher_failed: None,
        }
    }

    /// Build a successful acknowledgement for a repo-scoped operation.
    #[must_use]
    pub fn with_alias(alias: impl Into<String>) -> Self {
        Self {
            ok: true,
            alias: Some(alias.into()),
            watcher_failed: None,
        }
    }

    /// Build a successful `register_repo` acknowledgement when the repo was
    /// registered but the live watcher could not be installed.
    #[must_use]
    pub fn with_alias_and_watcher_failed(alias: impl Into<String>, reason: String) -> Self {
        Self {
            ok: true,
            alias: Some(alias.into()),
            watcher_failed: Some(reason),
        }
    }
}
