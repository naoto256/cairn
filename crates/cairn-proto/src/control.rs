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
    /// Include historical rows from manifests no current anchor points at.
    /// The default view keeps active jobs plus the latest terminal row per
    /// `(repo, analyzer)` for current manifests.
    #[serde(default)]
    pub all: bool,
    /// Maximum number of rows after filtering and global ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Arguments to the `jobs.cancel` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsCancelArgs {
    /// Numeric job id returned by [`JobSnapshot::job_id`].
    pub job_id: i64,
}

/// Arguments to the `jobs.prune` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsPruneArgs {
    /// Repository alias to prune, or `None` for all repos.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// When true, count rows that would be removed without deleting them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
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
    /// Pool group this analyzer waits on when it shares an LSP process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_group: Option<String>,
    /// Scheduler-side state such as `queued`, `waiting_pool_group`, or `running`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_state: Option<String>,
    /// Original enqueue timestamp in nanoseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enqueued_at: Option<i64>,
    /// Analyzer execution start timestamp in nanoseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_started_at: Option<i64>,
    /// Time spent queued before execution or completion, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_ms: Option<u64>,
    /// Time spent waiting for a shared pool-group slot, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_wait_ms: Option<u64>,
    /// Analyzer execution time since start, or total run time after completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_ms: Option<u64>,
    /// Analyzer progress ticks observed during this daemon lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_ticks: Option<u64>,
    /// Last observed progress timestamp in nanoseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_at: Option<i64>,
    /// Approximate progress ticks per minute while the job has been running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_per_minute: Option<f64>,
}

/// Result of `jobs.cancel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsCancelResult {
    /// True when the daemon accepted the cancellation for the requested job.
    pub cancelled: bool,
    /// Human-readable outcome, including why no cancellation happened.
    pub reason: String,
}

/// Result of `jobs.prune`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsPruneResult {
    /// Per-repository deletion counts.
    pub repos: Vec<JobsPruneRepoEntry>,
    /// Sum of [`JobsPruneRepoEntry::deleted_runs_count`] across all entries.
    pub total_deleted_runs: u64,
    /// Sum of [`JobsPruneRepoEntry::deleted_index_entries_count`] across all entries.
    pub total_deleted_index_entries: u64,
}

/// Deletion summary for one repository's analyzer job rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsPruneRepoEntry {
    /// Repository alias that was pruned.
    pub alias: String,
    /// Number of historical terminal `workspace_analysis_runs` rows removed.
    pub deleted_runs_count: u64,
    /// Number of runtime job-index entries removed for deleted job ids.
    pub deleted_index_entries_count: u64,
}

// ─── status ────────────────────────────────────────────────────────────────

/// Stable daemon-startup state exposed by [`StatusReport`].
///
/// Wire strings are the `snake_case` variant names (`"initializing"`,
/// `"ready"`). Older daemons (pre-0.8.0) bound the control socket only
/// after initialization completed, so the missing-field default on
/// [`StatusReport::initialization`] resolves to `Ready`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonInitializationState {
    /// Socket accepts `status` and `shutdown` while the daemon is still
    /// running through the ordered startup phases below.
    Initializing,
    /// All seven startup phases have completed and the daemon is
    /// serving the full control and data surface.
    Ready,
}

/// Ordered startup phase. The seven work phases are followed by the terminal
/// [`Self::Ready`] state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonInitializationPhase {
    SocketBound,
    RepositoryLifecycle,
    JobManager,
    ReconcileRecovery,
    WatcherBarrier,
    ReconcilePrime,
    PeriodicScheduler,
    Ready,
}

impl DaemonInitializationPhase {
    /// Number of ordered work phases the daemon runs before reaching
    /// [`Self::Ready`]. Held stable so consumers can render a fixed
    /// denominator (`completed / TOTAL_PHASES`) regardless of which
    /// phase is currently in flight.
    pub const TOTAL_PHASES: u8 = 7;

    /// Number of work phases completed before this phase became current.
    #[must_use]
    pub const fn completed_phases(self) -> u8 {
        match self {
            Self::SocketBound => 0,
            Self::RepositoryLifecycle => 1,
            Self::JobManager => 2,
            Self::ReconcileRecovery => 3,
            Self::WatcherBarrier => 4,
            Self::ReconcilePrime => 5,
            Self::PeriodicScheduler => 6,
            Self::Ready => Self::TOTAL_PHASES,
        }
    }

    /// Stable human label used by CLI progress output.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::SocketBound => "socket bound",
            Self::RepositoryLifecycle => "repository lifecycle",
            Self::JobManager => "job manager",
            Self::ReconcileRecovery => "reconcile recovery",
            Self::WatcherBarrier => "watcher barrier",
            Self::ReconcilePrime => "reconcile prime",
            Self::PeriodicScheduler => "periodic scheduler",
            Self::Ready => "ready",
        }
    }
}

/// Closed, path-free detail vocabulary for the current startup operation.
///
/// Keeping this as an enum prevents repository paths, backend errors, or other
/// free text from crossing the control-socket confidentiality boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonInitializationDetail {
    OpeningStorage,
    SweepingRepositories,
    RestoringJobs,
    StartingJobWorkers,
    RecoveringReconcileAttempts,
    BindingRuntimeManagers,
    ArmingRegisteredWatchers,
    RecordingStartupGenerations,
    StartingPeriodicReconcile,
}

impl DaemonInitializationDetail {
    /// Stable human label with no repository or error text.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::OpeningStorage => "opening storage",
            Self::SweepingRepositories => "sweeping repositories",
            Self::RestoringJobs => "restoring jobs",
            Self::StartingJobWorkers => "starting job workers",
            Self::RecoveringReconcileAttempts => "recovering reconcile attempts",
            Self::BindingRuntimeManagers => "binding runtime managers",
            Self::ArmingRegisteredWatchers => "arming registered watchers",
            Self::RecordingStartupGenerations => "recording startup generations",
            Self::StartingPeriodicReconcile => "starting periodic reconcile",
        }
    }
}

/// One monotonic daemon-startup observation.
///
/// Emitted inside [`StatusReport::initialization`]. Successive samples
/// have non-decreasing `phase` and `completed_phases`. `detail` is a
/// closed enum for confidentiality — see [`DaemonInitializationDetail`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonInitializationStatus {
    /// Coarse-grained bucket. Distinguishes `Initializing` from `Ready`
    /// without inspecting the phase.
    pub state: DaemonInitializationState,
    /// Ordered startup phase currently in progress, or `Ready` once
    /// startup has fully completed.
    pub phase: DaemonInitializationPhase,
    /// Number of prior phases already completed. Ranges over
    /// `0..=TOTAL_PHASES`.
    pub completed_phases: u8,
    /// Total number of ordered work phases. Currently equal to
    /// [`DaemonInitializationPhase::TOTAL_PHASES`], but carried on the
    /// wire so clients do not need to hard-code the constant.
    pub total_phases: u8,
    /// Closed-vocabulary label for the specific operation the daemon
    /// is executing inside the current phase. Omitted on the wire
    /// when the daemon has nothing narrower to report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<DaemonInitializationDetail>,
}

impl DaemonInitializationStatus {
    /// Build an `Initializing` observation at `phase` with an optional
    /// narrower `detail`. Panics if `phase` is `Ready` — use
    /// [`Self::ready`] for that terminal state instead.
    #[must_use]
    pub const fn initializing(
        phase: DaemonInitializationPhase,
        detail: Option<DaemonInitializationDetail>,
    ) -> Self {
        assert!(!matches!(phase, DaemonInitializationPhase::Ready));
        Self {
            state: DaemonInitializationState::Initializing,
            phase,
            completed_phases: phase.completed_phases(),
            total_phases: DaemonInitializationPhase::TOTAL_PHASES,
            detail,
        }
    }

    /// Terminal "startup finished" observation. All counters saturate
    /// at [`DaemonInitializationPhase::TOTAL_PHASES`] and `detail` is
    /// cleared.
    #[must_use]
    pub const fn ready() -> Self {
        Self {
            state: DaemonInitializationState::Ready,
            phase: DaemonInitializationPhase::Ready,
            completed_phases: DaemonInitializationPhase::TOTAL_PHASES,
            total_phases: DaemonInitializationPhase::TOTAL_PHASES,
            detail: None,
        }
    }

    /// True when the observation reports the terminal `Ready` state.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self.state, DaemonInitializationState::Ready)
    }
}

impl Default for DaemonInitializationStatus {
    fn default() -> Self {
        Self::ready()
    }
}

/// Result of the `status` control method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    /// Daemon version string reported by the running process.
    pub daemon_version: String,
    /// Daemon uptime in whole seconds.
    pub uptime_secs: u64,
    /// Startup progress. Missing on pre-0.8.0 daemon responses, which imply
    /// ready because those daemons bound sockets only after initialization.
    #[serde(default)]
    pub initialization: DaemonInitializationStatus,
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
    /// Whether a missing root is retained instead of auto-pruned.
    /// Reflects the daemon's removal-safe lifecycle policy for this
    /// alias. Absent on the wire from clients that predate the field;
    /// deserialized as `false` in that case.
    #[serde(default)]
    pub persistent: bool,
    /// Snapshot manifests reachable through this repo's anchors.
    pub snapshots: Vec<SnapshotStatus>,
    /// Compact analyzer-job state counts for manifests reachable through
    /// current anchors. Detailed history lives under `jobs.list`.
    #[serde(default, skip_serializing_if = "JobSummary::is_empty")]
    pub job_summary: JobSummary,
    /// Analyzer jobs for this repo. Kept for wire compatibility with older
    /// daemons/clients, but new `status` responses leave it empty to avoid
    /// dumping job history inline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobSnapshot>,
    /// PR3 Phase 4: durable reconcile state for the canonical
    /// repo. Additive; two alias rows sharing the same
    /// `reconcile.repo_hash` carry the same object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconcile: Option<crate::RepoReconcileStatus>,
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

/// State-count summary for analyzer jobs.
///
/// Zero-valued fields are omitted on the wire, so absent buckets
/// deserialize back as `0`. `total` is populated by the daemon and
/// is not automatically recomputed from the per-state counts on the
/// client side.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobSummary {
    /// Jobs enqueued but not yet running.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub queued: u64,
    /// Jobs the scheduler currently reports as executing.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub running: u64,
    /// Jobs that finished successfully — a straight count of rows
    /// whose persisted status is `succeeded`. There is no separate
    /// `ready` bucket in the wire shape; the daemon-side summarizer
    /// only tallies this field, it does not remap categories.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub succeeded: u64,
    /// Jobs the analyzer intentionally skipped (e.g. no matching
    /// files, workspace unsuitable).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skipped: u64,
    /// Jobs that finished with an error.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failed: u64,
    /// Jobs terminated by the timeout enforcement path.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub timed_out: u64,
    /// Jobs cancelled via `jobs.cancel` or a shutdown.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cancelled: u64,
    /// Catch-all bucket for daemon-side states that do not map to any
    /// of the named buckets above. Present so future producers can
    /// contribute new states without a wire break.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub other: u64,
    /// Total count reported by the daemon. Consumers should treat
    /// this as authoritative rather than summing the named buckets,
    /// since additional states may land in `other`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub total: u64,
}

impl JobSummary {
    /// True when the daemon reported no analyzer jobs for the enclosing
    /// scope. Used by [`RepoStatus`] as the `skip_serializing_if` guard
    /// to keep empty summaries off the wire.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
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
    fn daemon_initialization_progress_is_monotonic_with_fixed_total() {
        let phases = [
            DaemonInitializationPhase::SocketBound,
            DaemonInitializationPhase::RepositoryLifecycle,
            DaemonInitializationPhase::JobManager,
            DaemonInitializationPhase::ReconcileRecovery,
            DaemonInitializationPhase::WatcherBarrier,
            DaemonInitializationPhase::ReconcilePrime,
            DaemonInitializationPhase::PeriodicScheduler,
            DaemonInitializationPhase::Ready,
        ];

        let completed = phases
            .iter()
            .map(|phase| phase.completed_phases())
            .collect::<Vec<_>>();
        assert_eq!(completed, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(completed.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(DaemonInitializationPhase::TOTAL_PHASES, 7);

        let status = DaemonInitializationStatus::initializing(
            DaemonInitializationPhase::WatcherBarrier,
            Some(DaemonInitializationDetail::ArmingRegisteredWatchers),
        );
        assert!(!status.is_ready());
        assert_eq!(status.total_phases, 7);
        assert_eq!(status.completed_phases, 4);
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            serde_json::json!({
                "state": "initializing",
                "phase": "watcher_barrier",
                "completed_phases": 4,
                "total_phases": 7,
                "detail": "arming_registered_watchers"
            })
        );
    }

    #[test]
    fn legacy_status_payload_defaults_initialization_to_ready() {
        let report: StatusReport = serde_json::from_value(serde_json::json!({
            "daemon_version": "0.7.1",
            "uptime_secs": 3,
            "repos": []
        }))
        .unwrap();

        assert!(report.initialization.is_ready());
        assert_eq!(
            report.initialization.phase,
            DaemonInitializationPhase::Ready
        );
        assert_eq!(report.initialization.completed_phases, 7);
        assert_eq!(report.initialization.total_phases, 7);
        assert!(report.initialization.detail.is_none());
    }

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
            persistent: false,
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
            job_summary: JobSummary::default(),
            jobs: Vec::new(),
            reconcile: None,
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
///
/// Wire note: the `register_repo` and `reindex_repo` handlers splice
/// additional fields into the JSON `result` object after serializing
/// this struct — currently a `jobs` array of queued-job receipts
/// (`QueuedAnalyzerJob { job_id, analyzer_id, state }` today, where
/// `job_id` is a JSON number) for both verbs, and a `reconcile`
/// sub-object for `reindex_repo` when the request went through the
/// reconcile path. On the reconcile path `jobs` is emitted as an
/// empty array for wire compatibility; new consumers should read
/// `reconcile` on that path. These splice fields are not modeled on
/// this Rust type; consumers that only need the acknowledgement can
/// deserialize `Ack` and ignore the extras, and consumers that need
/// the extras should read the JSON value directly.
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
    ///
    /// Other mutating verbs (`remove_repo`, `reindex_repo`,
    /// `shutdown`) leave this `None`, and the field is omitted on
    /// the wire in that case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watcher_failed: Option<String>,
}

impl Ack {
    /// Build a successful acknowledgement with no repo alias.
    ///
    /// Used by non-repo-scoped verbs such as `shutdown`; leaves both
    /// `alias` and `watcher_failed` `None`, so both fields are
    /// omitted on the wire.
    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            alias: None,
            watcher_failed: None,
        }
    }

    /// Build a successful acknowledgement for a repo-scoped operation.
    ///
    /// Used by `remove_repo`, `reindex_repo`, and the
    /// success-without-watcher-failure branch of `register_repo`.
    /// Leaves `watcher_failed` `None`.
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
    ///
    /// Currently the only constructor that populates
    /// [`Ack::watcher_failed`].
    #[must_use]
    pub fn with_alias_and_watcher_failed(alias: impl Into<String>, reason: String) -> Self {
        Self {
            ok: true,
            alias: Some(alias.into()),
            watcher_failed: Some(reason),
        }
    }
}
