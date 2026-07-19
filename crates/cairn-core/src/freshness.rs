//! Snapshot freshness evaluation shared by query and status surfaces.
//!
//! Freshness is deliberately stricter than anchor existence: the default
//! worktree view is fresh only when the durable reconcile row is clean, its
//! watcher is healthy, and the tentative anchor carries the exact applied
//! generation receipt. Explicit historical anchors remain queryable without
//! being judged against current-worktree liveness.

use std::time::Duration;

use rusqlite::Connection;

use crate::anchor::{self, Anchor, AnchorKind, AnchorName};
use crate::cas::registry::{self as cas_registry, RepoReconcileState, WatcherState};
use crate::manifest::ManifestId;
use crate::{Error, Result};

/// Shared freshness horizon. The periodic reconciler uses the same duration,
/// so a healthy daemon schedules a full scan before a clean snapshot expires.
pub const MAX_CURRENT_SNAPSHOT_AGE: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotStaleReason {
    MissingTentative,
    MissingReconcileState,
    AttemptInProgress,
    GenerationGap,
    WatcherInactive,
    WatcherFailed,
    NeverReconciled,
    ScanTooOld,
    MissingPublicationReceipt,
    PublicationGenerationMismatch,
    ChangedDuringQuery,
}

impl SnapshotStaleReason {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MissingTentative => "missing_tentative_snapshot",
            Self::MissingReconcileState => "missing_reconcile_state",
            Self::AttemptInProgress => "reconcile_in_progress",
            Self::GenerationGap => "reconcile_generation_gap",
            Self::WatcherInactive => "watcher_inactive",
            Self::WatcherFailed => "watcher_failed",
            Self::NeverReconciled => "never_reconciled",
            Self::ScanTooOld => "snapshot_scan_too_old",
            Self::MissingPublicationReceipt => "missing_publication_receipt",
            Self::PublicationGenerationMismatch => "publication_generation_mismatch",
            Self::ChangedDuringQuery => "snapshot_changed_during_query",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotFreshness {
    Explicit,
    Fresh,
    Stale(SnapshotStaleReason),
}

impl SnapshotFreshness {
    #[must_use]
    pub(crate) fn is_stale(self) -> bool {
        matches!(self, Self::Stale(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FreshnessFingerprint {
    desired_generation: i64,
    applied_generation: i64,
    attempt_generation: Option<i64>,
    last_success_ns: Option<i64>,
    watcher_state: WatcherState,
    watcher_error: Option<String>,
    anchor_manifest_id: ManifestId,
    anchor_generation: Option<i64>,
}

/// One immutable snapshot selection used by a query response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvaluatedSnapshot {
    pub(crate) anchor: AnchorName,
    pub(crate) manifest_id: ManifestId,
    pub(crate) freshness: SnapshotFreshness,
    default_current: bool,
    fingerprint: Option<FreshnessFingerprint>,
}

/// Resolve one snapshot and evaluate its initial freshness.
///
/// The caller should invoke this inside the same SQLite read transaction used
/// by the query. That pins every anchor lookup performed by existing query
/// functions to the selected manifest for the lifetime of the transaction.
pub(crate) fn evaluate_snapshot(
    index: &Connection,
    store: &Connection,
    repo_hash: &str,
    anchor_arg: Option<&str>,
    branch_arg: Option<&str>,
    now_ns: i64,
) -> Result<EvaluatedSnapshot> {
    let default_current = anchor_arg.is_none() && branch_arg.is_none();
    let anchor_name = anchor::resolve_explicit_or_default(store, anchor_arg, branch_arg)?;
    let anchor = anchor::get(store, &anchor_name)?.ok_or_else(|| Error::AnchorNotFound {
        name: anchor_name.as_str().to_string(),
    })?;
    if !default_current {
        return Ok(EvaluatedSnapshot {
            anchor: anchor_name,
            manifest_id: anchor.manifest_id,
            freshness: SnapshotFreshness::Explicit,
            default_current: false,
            fingerprint: None,
        });
    }

    let state = cas_registry::get_reconcile_state(index, repo_hash)?;
    let freshness = evaluate_current(&anchor, state.as_ref(), now_ns);
    let fingerprint = state.map(|state| fingerprint(&anchor, state));
    Ok(EvaluatedSnapshot {
        anchor: anchor_name,
        manifest_id: anchor.manifest_id,
        freshness,
        default_current: true,
        fingerprint,
    })
}

/// Re-read the durable publication and reconcile state after query execution.
/// Freshness survives only when the complete fingerprint is unchanged.
pub(crate) fn revalidate_snapshot(
    index: &Connection,
    store: &Connection,
    repo_hash: &str,
    selected: &EvaluatedSnapshot,
    now_ns: i64,
) -> Result<SnapshotFreshness> {
    if !selected.default_current {
        return Ok(SnapshotFreshness::Explicit);
    }
    let Some(anchor) = anchor::get(store, &selected.anchor)? else {
        return Ok(SnapshotFreshness::Stale(
            SnapshotStaleReason::ChangedDuringQuery,
        ));
    };
    let Some(state) = cas_registry::get_reconcile_state(index, repo_hash)? else {
        return Ok(SnapshotFreshness::Stale(
            SnapshotStaleReason::MissingReconcileState,
        ));
    };
    let final_freshness = evaluate_current(&anchor, Some(&state), now_ns);
    let final_fingerprint = fingerprint(&anchor, state);
    if selected.fingerprint.as_ref() != Some(&final_fingerprint) {
        return Ok(SnapshotFreshness::Stale(
            SnapshotStaleReason::ChangedDuringQuery,
        ));
    }
    if selected.freshness.is_stale() {
        return Ok(selected.freshness);
    }
    Ok(final_freshness)
}

fn evaluate_current(
    anchor: &Anchor,
    state: Option<&RepoReconcileState>,
    now_ns: i64,
) -> SnapshotFreshness {
    if !matches!(anchor.name.kind(), Some(AnchorKind::Tentative(_))) {
        return SnapshotFreshness::Stale(SnapshotStaleReason::MissingTentative);
    }
    let Some(state) = state else {
        return SnapshotFreshness::Stale(SnapshotStaleReason::MissingReconcileState);
    };
    if state.attempt_generation.is_some() {
        return SnapshotFreshness::Stale(SnapshotStaleReason::AttemptInProgress);
    }
    if state.desired_generation != state.applied_generation {
        return SnapshotFreshness::Stale(SnapshotStaleReason::GenerationGap);
    }
    if state.watcher_error.is_some() || state.watcher_state == WatcherState::Failed {
        return SnapshotFreshness::Stale(SnapshotStaleReason::WatcherFailed);
    }
    if state.watcher_state != WatcherState::Active {
        return SnapshotFreshness::Stale(SnapshotStaleReason::WatcherInactive);
    }
    let Some(last_success_ns) = state.last_success_ns else {
        return SnapshotFreshness::Stale(SnapshotStaleReason::NeverReconciled);
    };
    let max_age_ns = i64::try_from(MAX_CURRENT_SNAPSHOT_AGE.as_nanos()).unwrap_or(i64::MAX);
    if now_ns.saturating_sub(last_success_ns) > max_age_ns {
        return SnapshotFreshness::Stale(SnapshotStaleReason::ScanTooOld);
    }
    let Some(generation) = anchor.reconcile_generation else {
        return SnapshotFreshness::Stale(SnapshotStaleReason::MissingPublicationReceipt);
    };
    if generation != state.applied_generation {
        return SnapshotFreshness::Stale(SnapshotStaleReason::PublicationGenerationMismatch);
    }
    SnapshotFreshness::Fresh
}

fn fingerprint(anchor: &Anchor, state: RepoReconcileState) -> FreshnessFingerprint {
    FreshnessFingerprint {
        desired_generation: state.desired_generation,
        applied_generation: state.applied_generation,
        attempt_generation: state.attempt_generation,
        last_success_ns: state.last_success_ns,
        watcher_state: state.watcher_state,
        watcher_error: state.watcher_error,
        anchor_manifest_id: anchor.manifest_id,
        anchor_generation: anchor.reconcile_generation,
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use super::*;
    use crate::cas::store as cas_store;

    struct Fixture {
        _tmp: tempfile::TempDir,
        index: Connection,
        store: Connection,
    }

    impl Fixture {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let mut index = cas_registry::open(&tmp.path().join("index.db")).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, "demo", "/repo", "hash", 1).unwrap();
            tx.commit().unwrap();
            index
                .execute(
                    "UPDATE repo_reconcile_state
                     SET desired_generation = 3,
                         applied_generation = 3,
                         last_success_ns = 100,
                         watcher_state = 'active'
                     WHERE repo_hash = 'hash'",
                    [],
                )
                .unwrap();

            let mut store = cas_store::open(&tmp.path().join("store.db")).unwrap();
            let tx = store.transaction().unwrap();
            tx.execute(
                "INSERT INTO manifests (manifest_id, kind, built_at_ns)
                 VALUES (1, 'tentative', 0), (2, 'committed', 0)",
                [],
            )
            .unwrap();
            anchor::set_reconciled(&tx, &AnchorName::tentative(1), ManifestId(1), 100, 3).unwrap();
            anchor::set(&tx, &AnchorName::head(), ManifestId(2), 100).unwrap();
            tx.commit().unwrap();
            Self {
                _tmp: tmp,
                index,
                store,
            }
        }

        fn evaluate(&self, anchor_arg: Option<&str>, now_ns: i64) -> EvaluatedSnapshot {
            evaluate_snapshot(&self.index, &self.store, "hash", anchor_arg, None, now_ns).unwrap()
        }
    }

    #[test]
    fn current_snapshot_is_fresh_only_with_matching_clean_receipt() {
        let fixture = Fixture::new();
        let snapshot = fixture.evaluate(None, 101);

        assert_eq!(snapshot.anchor, AnchorName::tentative(1));
        assert_eq!(snapshot.manifest_id, ManifestId(1));
        assert_eq!(snapshot.freshness, SnapshotFreshness::Fresh);
    }

    #[test]
    fn explicit_anchor_is_exempt_from_current_liveness() {
        let fixture = Fixture::new();
        fixture
            .index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 4, watcher_state = 'failed'",
                [],
            )
            .unwrap();

        let snapshot = fixture.evaluate(Some("HEAD"), i64::MAX);

        assert_eq!(snapshot.manifest_id, ManifestId(2));
        assert_eq!(snapshot.freshness, SnapshotFreshness::Explicit);
    }

    #[test]
    fn direct_anchor_write_cannot_inherit_a_fresh_receipt() {
        let mut fixture = Fixture::new();
        let tx = fixture.store.transaction().unwrap();
        anchor::set(&tx, &AnchorName::tentative(1), ManifestId(1), 101).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            fixture.evaluate(None, 101).freshness,
            SnapshotFreshness::Stale(SnapshotStaleReason::MissingPublicationReceipt)
        );
    }

    #[test]
    fn generation_change_during_query_invalidates_initial_freshness() {
        let fixture = Fixture::new();
        let selected = fixture.evaluate(None, 101);
        fixture
            .index
            .execute("UPDATE repo_reconcile_state SET desired_generation = 4", [])
            .unwrap();

        assert_eq!(
            revalidate_snapshot(&fixture.index, &fixture.store, "hash", &selected, 102).unwrap(),
            SnapshotFreshness::Stale(SnapshotStaleReason::ChangedDuringQuery)
        );
    }

    #[test]
    fn scan_older_than_periodic_horizon_is_stale() {
        let fixture = Fixture::new();
        let max_age_ns = i64::try_from(MAX_CURRENT_SNAPSHOT_AGE.as_nanos()).unwrap();

        assert_eq!(
            fixture.evaluate(None, 100 + max_age_ns + 1).freshness,
            SnapshotFreshness::Stale(SnapshotStaleReason::ScanTooOld)
        );
    }

    #[test]
    fn publication_generation_must_equal_applied_generation() {
        let fixture = Fixture::new();
        fixture
            .store
            .execute(
                "UPDATE anchors SET reconcile_generation = ?1
                 WHERE anchor_name = 'tentative/1'",
                params![2],
            )
            .unwrap();

        assert_eq!(
            fixture.evaluate(None, 101).freshness,
            SnapshotFreshness::Stale(SnapshotStaleReason::PublicationGenerationMismatch)
        );
    }
}
