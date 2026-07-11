//! Wire type for the durable per-repo reconcile state.
//!
//! `RepoReconcileStatus` is the additive object embedded on both
//! the data-RPC `repo_status` entry and the control-socket
//! `status.repos[]` entry. It mirrors the fields of
//! `cairn_core::cas::registry::RepoReconcileState` verbatim, with
//! the wire-safety refinements the protocol layer requires:
//!
//! - `watcher_state` is a string, not an enum, so future variants
//!   don't fail deserialization on older clients.
//! - all optional durable columns serialize as `Option<T>` and skip
//!   when `None`, so an old client that ignores the field sees no
//!   noise on the wire.
//! - two derived boolean summaries (`pending`, `retry_scheduled`)
//!   are populated by the producer and treated as convenience —
//!   consumers that need the exact rule read the raw fields.
//!
//! Wire policy for PR3 Phase 4: strictly additive. This field is
//! `Option<RepoReconcileStatus>` on `RepoStatusEntry` / `RepoStatus`
//! with `#[serde(default, skip_serializing_if = "Option::is_none")]`,
//! so a daemon that has not populated it (older build) round-trips
//! cleanly through consumers that expect it, and a consumer that
//! doesn't know about it deserializes existing payloads unchanged.

use serde::{Deserialize, Serialize};

/// Per-repo durable reconcile state as surfaced on status
/// responses. All timestamps are nanoseconds since UNIX epoch.
///
/// The mapping to `repo_reconcile_state` columns is 1:1 except:
/// - `watcher_state` is the DB column value verbatim (`"starting"`,
///   `"active"`, `"failed"`, or `"stopped"`) but typed `String` on
///   the wire so future values don't break deserialization for
///   older clients.
/// - `aliases` is derived from `aliases_for_repo(repo_hash)` at
///   status-serve time; producers should populate it in sorted
///   order so consumers see a deterministic list.
/// - `pending` and `retry_scheduled` are derived booleans;
///   producers compute them from the raw columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepoReconcileStatus {
    /// Canonical repository identifier — same value for all
    /// aliases pointing at this on-disk root.
    pub repo_hash: String,
    /// Sorted list of aliases pointing at this repo_hash. Two
    /// alias entries in a status response with equal `repo_hash`
    /// carry the same `RepoReconcileStatus`; the `aliases` list
    /// lets a client tell they represent one canonical repo.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Highest generation the producer side (watcher / manual /
    /// drift scanner) has recorded as dirty. Monotone.
    pub desired_generation: i64,
    /// Highest generation the worker has completed successfully.
    /// `desired > applied` == a dirty gap the manager should
    /// close.
    pub applied_generation: i64,
    /// Highest generation for which a force request was recorded
    /// (manual reindex or parser-revision drift). Always `<=
    /// desired_generation`.
    pub force_generation: i64,
    /// Non-`None` when a worker attempt is currently in flight.
    /// Cleared to `None` on success/failure and by startup
    /// interrupted-attempt recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_generation: Option<i64>,
    /// First moment (ns) at which the durable gap opened. Cleared
    /// on success when `desired == applied`. Consumers can compute
    /// dirty age as `now_ns - dirty_since_ns`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_since_ns: Option<i64>,
    /// Last `mark_attempt_start` timestamp (ns), regardless of
    /// outcome. `None` until the first attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_ns: Option<i64>,
    /// Last `mark_attempt_success` timestamp (ns). `None` until
    /// the first success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_ns: Option<i64>,
    /// Consecutive failure counter — reset to 0 on the next
    /// success. Used to drive exponential backoff.
    pub consecutive_failures: i64,
    /// Wall clock (ns) at which the next retry attempt is
    /// scheduled to run. `None` if no retry is pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_ns: Option<i64>,
    /// Last error text (UTF-8-safe truncated at 4096 bytes at the
    /// storage layer). Cleared on the next success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Watcher lifecycle. Storage layer emits one of `"starting"`
    /// / `"active"` / `"failed"` / `"stopped"`; typed `String` so
    /// forward-compat future values don't break older clients.
    #[serde(default = "default_watcher_state")]
    pub watcher_state: String,
    /// Populated when `watcher_state == "failed"`. UTF-8-safe
    /// truncated at the storage layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watcher_error: Option<String>,
    /// Derived: `desired > applied || attempt_generation.is_some()`.
    /// Producers populate this; consumers may re-derive from the
    /// raw columns.
    #[serde(default)]
    pub pending: bool,
    /// Derived: `next_retry_at_ns.is_some()`. Producers populate;
    /// consumers may re-derive.
    #[serde(default)]
    pub retry_scheduled: bool,
}

fn default_watcher_state() -> String {
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TierRepoStatus;
    use crate::control::{JobSummary, RepoStatus};
    use crate::methods::{RepoStatusCurrent, RepoStatusEntry, RepoStatusSummary};

    fn baseline_entry() -> RepoStatusEntry {
        RepoStatusEntry {
            alias: "demo".into(),
            root: "/p".into(),
            languages: vec!["rust".into()],
            summary: RepoStatusSummary {
                snapshot_count: 1,
                ready_snapshot_count: 1,
                stale_snapshot_count: 0,
                current_file_count: 0,
                current_symbol_count: 0,
            },
            current: RepoStatusCurrent {
                anchor: "HEAD".into(),
                status: "ready".into(),
            },
            tier3_status: TierRepoStatus {
                this_repo: crate::TierStatusBody::ready(),
                repo_wide: None,
            },
            snapshots: Vec::new(),
            reconcile: None,
        }
    }

    fn baseline_control_status() -> RepoStatus {
        RepoStatus {
            alias: "demo".into(),
            root: "/p".into(),
            snapshots: Vec::new(),
            job_summary: JobSummary::default(),
            jobs: Vec::new(),
            reconcile: None,
        }
    }

    /// PR3 Phase 4 MF-1a: an existing JSON payload without a
    /// `reconcile` field must deserialize into the new struct
    /// with `reconcile == None`. Additive-only policy: old
    /// consumers keep working.
    #[test]
    fn repo_status_entry_deserializes_without_reconcile() {
        let json = serde_json::to_value(baseline_entry()).unwrap();
        // Ensure serializer omits `reconcile` when None.
        assert!(
            json.get("reconcile").is_none(),
            "None must skip; payload = {json}"
        );
        // And reading it back yields the same value.
        let back: RepoStatusEntry = serde_json::from_value(json).unwrap();
        assert!(back.reconcile.is_none());
    }

    /// PR3 Phase 4 MF-1b: a populated `RepoReconcileStatus`
    /// round-trips every nullable column faithfully.
    #[test]
    fn repo_reconcile_status_roundtrips_all_fields() {
        let full = RepoReconcileStatus {
            repo_hash: "h".into(),
            aliases: vec!["a".into(), "b".into()],
            desired_generation: 4,
            applied_generation: 2,
            force_generation: 1,
            attempt_generation: Some(3),
            dirty_since_ns: Some(100),
            last_attempt_ns: Some(200),
            last_success_ns: Some(150),
            consecutive_failures: 2,
            next_retry_at_ns: Some(500),
            last_error: Some("EMFILE".into()),
            watcher_state: "failed".into(),
            watcher_error: Some("git open failed".into()),
            pending: true,
            retry_scheduled: true,
        };
        let json = serde_json::to_string(&full).unwrap();
        let back: RepoReconcileStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(full, back);
    }

    /// PR3 Phase 4 MF-1c: same additive compat for control
    /// `RepoStatus`.
    #[test]
    fn control_repo_status_deserializes_without_reconcile() {
        let json = serde_json::to_value(baseline_control_status()).unwrap();
        assert!(
            json.get("reconcile").is_none(),
            "None must skip; payload = {json}"
        );
        let back: RepoStatus = serde_json::from_value(json).unwrap();
        assert!(back.reconcile.is_none());
    }

    /// PR3 Phase 4 MF-1d: an unknown future `watcher_state`
    /// deserializes without error — the field is a `String`, not
    /// a strict enum, so forward-compat variants don't break older
    /// clients.
    #[test]
    fn watcher_state_accepts_unknown_string() {
        let json = r#"{
            "repo_hash": "h",
            "desired_generation": 0,
            "applied_generation": 0,
            "force_generation": 0,
            "consecutive_failures": 0,
            "watcher_state": "future_variant_we_dont_know"
        }"#;
        let back: RepoReconcileStatus = serde_json::from_str(json).unwrap();
        assert_eq!(back.watcher_state, "future_variant_we_dont_know");
    }
}
