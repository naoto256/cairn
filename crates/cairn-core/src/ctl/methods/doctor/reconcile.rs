use cairn_proto::control::{DoctorCheck, DoctorStatus};

use crate::Result;
use crate::cas::registry as cas_registry;
use crate::paths::CasDataDir;

use super::doctor_check;

// Reconcile-state doctor group.

/// A dirty gap older than this without any in-flight attempt or
/// scheduled retry is warned about — the manager should pick up
/// a gap within seconds; multi-minute idle dirty means it has
/// stalled.
const RECONCILE_DIRTY_GAP_WARN_NS: i64 = 5 * 60 * 1_000_000_000;

/// An in-flight attempt older than this is warned about — the
/// worker executes reindex in seconds to low minutes; multi-
/// minute in-flight typically means the worker is wedged.
const RECONCILE_STUCK_ATTEMPT_WARN_NS: i64 = 10 * 60 * 1_000_000_000;

/// Emits the reconcile-state doctor group (family 4). For every
/// repository row in index.db it looks up the `RepoReconcileState`
/// and delegates the label choice to [`classify_reconcile_state`],
/// then appends per-`repo_hash` `Warn`s for any incomplete removal
/// cleanups and, when the recent-completed-removals list is
/// non-empty, one summary `Pass` line.
///
/// Aliases sharing a `repo_hash` collapse to a single label via
/// [`format_repo_label`], so a repo with two aliases does not
/// produce duplicate warnings. A missing `RepoReconcileState` row
/// while a `repository` row exists is a `Fail` (index.db invariant
/// break).
pub(crate) fn reconcile_state_checks(cas_data_dir: &CasDataDir) -> Result<Vec<DoctorCheck>> {
    let index = cas_registry::open(&cas_data_dir.index_db_path())?;
    let repos = cas_registry::list_repositories(&index)?;
    let mut checks = Vec::new();
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX);
    for repo in &repos {
        let aliases = cas_registry::aliases_for_repo(&index, &repo.repo_hash)?;
        let state = cas_registry::get_reconcile_state(&index, &repo.repo_hash)?;
        let label = format_repo_label(&repo.repo_hash, &aliases);
        match state {
            None => checks.push(doctor_check(
                format!("reconcile state: {label}"),
                DoctorStatus::Fail,
                Some(format!(
                    "repository row exists for repo_hash={} but repo_reconcile_state row is missing",
                    repo.repo_hash
                )),
                Some(
                    "Index.db invariant broken. Inspect the DB; if unrecoverable, delete and re-register.".into(),
                ),
            )),
            Some(state) => {
                checks.extend(classify_reconcile_state(
                    &label,
                    &state,
                    repo.persistent,
                    now_ns,
                ));
            }
        }
    }
    let incomplete = cas_registry::list_incomplete_removals(&index)?;
    for event in incomplete {
        checks.push(doctor_check(
            format!("repository cleanup: {}", event.repo_hash),
            DoctorStatus::Warn,
            Some(format!(
                "{} cleanup is {:?}: {}",
                event.reason.as_db_str(),
                event.store_cleanup_state,
                event.cleanup_error.as_deref().unwrap_or("retry pending")
            )),
            Some("Restart the daemon to retry cleanup, then inspect the daemon log if it remains pending.".into()),
        ));
    }
    let completed = cas_registry::list_recent_completed_removals(&index, 10)?;
    if let Some(check) = completed_removal_history_check(&completed) {
        checks.push(check);
    }
    Ok(checks)
}

fn completed_removal_history_check(
    completed: &[cas_registry::RepositoryRemovalEvent],
) -> Option<DoctorCheck> {
    (!completed.is_empty()).then(|| {
        doctor_check(
            "recent repository removals",
            DoctorStatus::Pass,
            Some(
                completed
                    .iter()
                    .map(|event| format!("{} ({})", event.root_path, event.reason.as_db_str()))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            None,
        )
    })
}

fn format_repo_label(repo_hash: &str, aliases: &[String]) -> String {
    match aliases.split_first() {
        Some((first, [])) => first.clone(),
        Some((first, rest)) => {
            let mut all = vec![first.clone()];
            all.extend(rest.iter().cloned());
            format!("{first} (aliases: {})", all.join(", "))
        }
        None => format!("<{repo_hash}>"),
    }
}

/// Independent conditions — invariant violation, watcher failure,
/// stuck attempt, retry backoff, dirty gap — can all fire in the
/// same tick, so this returns zero or more checks rather than a
/// single label. An invariant violation short-circuits the rest
/// (the state is untrustworthy for the other predicates). The
/// non-violation branches gate on per-condition thresholds
/// ([`RECONCILE_STUCK_ATTEMPT_WARN_NS`] and
/// [`RECONCILE_DIRTY_GAP_WARN_NS`]) so a healthy in-flight attempt
/// or a fresh dirty gap does not raise noise.
fn classify_reconcile_state(
    label: &str,
    state: &cas_registry::RepoReconcileState,
    persistent: bool,
    now_ns: i64,
) -> Vec<DoctorCheck> {
    let mut out = Vec::new();

    // Fail closed on impossible invariant relationships. Mutation
    // predicates and affected-row checks should make these
    // unreachable, but doctor remains the operator's safety net.
    if let Some(violation) = state.invariant_violations().into_iter().next() {
        out.push(doctor_check(
            format!("reconcile invariants: {label}"),
            DoctorStatus::Fail,
            Some(violation.to_string()),
            Some("State machine invariant break. File a bug and restart the daemon.".into()),
        ));
        return out;
    }

    // Watcher failure (informational Warn — the reindex path can
    // still recover via manual reindex or startup wake, but
    // future file events are blind until restart).
    if state.watcher_failed() {
        out.push(doctor_check(
            format!("watcher lifecycle: {label}"),
            DoctorStatus::Warn,
            Some(format!(
                "watcher state = failed{}",
                state
                    .watcher_error
                    .as_deref()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            )),
            Some("Restart the daemon to re-open the watcher, or manual reindex until then.".into()),
        ));
    }

    if let Some(kind) = state.terminal_failure_kind {
        let (status, phase) = if state.quarantined_at_ns.is_some() {
            (DoctorStatus::Warn, "quarantined")
        } else {
            (DoctorStatus::Warn, "candidate")
        };
        let exemption = if persistent {
            "; persistent registration is exempt from automatic removal"
        } else {
            ""
        };
        out.push(doctor_check(
            format!("registration health: {label}"),
            status,
            Some(format!(
                "{phase}: kind={}, observations={}, first_seen_ns={:?}, quarantined_at_ns={:?}{exemption}",
                kind.as_db_str(),
                state.terminal_failure_count,
                state.terminal_failure_since_ns,
                state.quarantined_at_ns,
            )),
            Some(if state.quarantined_at_ns.is_some() {
                "Cairn is running low-frequency structural revalidation; restore the repository root or Git administration metadata.".into()
            } else {
                "Restore the repository root or Git administration metadata before the quarantine threshold is reached.".into()
            }),
        ));
    } else if state.quarantined_at_ns.is_some() {
        out.push(doctor_check(
            format!("registration health: {label}"),
            DoctorStatus::Warn,
            Some(format!(
                "quarantined history retained; current structural evidence is ambiguous{}",
                if persistent {
                    "; persistent registration is exempt from automatic removal"
                } else {
                    ""
                }
            )),
            Some(
                "Cairn will continue low-frequency structural probes until a healthy recovery reconcile succeeds."
                    .into(),
            ),
        ));
    }

    // Stuck attempt (worker held mark_attempt_start for too long).
    if let Some(attempt_age) = state.attempt_age_ns(now_ns) {
        if attempt_age > RECONCILE_STUCK_ATTEMPT_WARN_NS {
            out.push(doctor_check(
                format!("reconcile attempt: {label}"),
                DoctorStatus::Warn,
                Some(format!(
                    "in-flight attempt is {:.1}s old — worker may be wedged",
                    attempt_age as f64 / 1e9
                )),
                Some(
                    "Inspect daemon logs for `reconcile`. Consider `cairn ctl repo reindex` or daemon restart.".into(),
                ),
            ));
        }
    }

    // Retry / backoff still in progress.
    if state.retry_backoff_scheduled()
        && let Some(next) = state.next_retry_at_ns
    {
        out.push(doctor_check(
            format!("reconcile retry: {label}"),
            DoctorStatus::Warn,
            Some(format!(
                "consecutive_failures={}, last_error={:?}, next_retry_at_ns={next}",
                state.consecutive_failures, state.last_error,
            )),
            Some(
                "Check the last error. If persistent, `cairn ctl repo reindex` or restart the daemon.".into(),
            ),
        ));
    }

    // Dirty gap without attempt and without scheduled retry —
    // the manager did not pick it up. This is a stuck backlog.
    if let Some(dirty_age) = state.dirty_gap_ns(now_ns) {
        if dirty_age > RECONCILE_DIRTY_GAP_WARN_NS {
            out.push(doctor_check(
                format!("reconcile dirty gap: {label}"),
                DoctorStatus::Warn,
                Some(format!(
                    "desired={} applied={} dirty_for={:.1}s",
                    state.desired_generation,
                    state.applied_generation,
                    dirty_age as f64 / 1e9
                )),
                Some(
                    "Manager pickup stalled. Check daemon logs; `cairn ctl repo reindex` will force a wake.".into(),
                ),
            ));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn seeded_cas(alias_pairs: &[(&str, &str, &str)]) -> (tempfile::TempDir, Arc<CasDataDir>) {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(tmp.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        for (alias, root, hash) in alias_pairs {
            cas_registry::upsert(&tx, alias, root, hash, 1).unwrap();
        }
        tx.commit().unwrap();
        (tmp, cas)
    }

    fn set_state(cas: &CasDataDir, mutate: impl FnOnce(&rusqlite::Transaction<'_>)) {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        mutate(&tx);
        tx.commit().unwrap();
    }

    #[test]
    fn completed_removal_history_does_not_mislabel_explicit_removals_as_auto_prune() {
        let check = completed_removal_history_check(&[cas_registry::RepositoryRemovalEvent {
            event_id: 1,
            repo_hash: "old-hash".into(),
            root_path: "/repos/old".into(),
            removed_at_ns: 10,
            reason: cas_registry::RepositoryRemovalReason::AliasRetargeted,
            store_cleanup_state: cas_registry::StoreCleanupState::Complete,
            cleanup_error: None,
        }])
        .expect("completed removal should be reported");

        assert_eq!(check.name, "recent repository removals");
        assert_eq!(check.status, DoctorStatus::Pass);
        assert_eq!(
            check.detail.as_deref(),
            Some("/repos/old (alias_retargeted)")
        );
    }

    #[test]
    fn doctor_reports_live_quarantine_and_stale_git_removal_history() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repositories SET persistent = 1 WHERE repo_hash = 'h'",
                [],
            )
            .unwrap();
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET terminal_failure_kind = 'git_admin_missing',
                     terminal_failure_count = 3,
                     terminal_failure_since_ns = 10,
                     quarantined_at_ns = 20,
                     health_epoch = 4
                 WHERE repo_hash = 'h'",
                [],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO repository_removal_events
                 (repo_hash, root_path, removed_at_ns, reason, store_cleanup_state)
                 VALUES ('old', '/old', 30, 'stale_git_metadata', 'complete')",
                [],
            )
            .unwrap();
        });

        let checks = reconcile_state_checks(&cas).unwrap();
        let health = checks
            .iter()
            .find(|check| check.name == "registration health: demo")
            .expect("live quarantine must be visible");
        assert_eq!(health.status, DoctorStatus::Warn);
        let detail = health.detail.as_deref().unwrap();
        assert!(detail.contains("quarantined"));
        assert!(detail.contains("git_admin_missing"));
        assert!(detail.contains("persistent registration is exempt"));

        let history = checks
            .iter()
            .find(|check| check.name == "recent repository removals")
            .expect("post-delete stale history must remain visible");
        assert!(
            history
                .detail
                .as_deref()
                .unwrap()
                .contains("/old (stale_git_metadata)")
        );
    }

    /// A dirty generation gap with no attempt or retry is `Warn`
    /// after the age threshold.
    #[test]
    fn mf6_doctor_dirty_gap_old_since_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let ten_min_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - 10 * 60 * 1_000_000_000;
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1, dirty_since_ns = ?1
                 WHERE repo_hash = 'h'",
                rusqlite::params![ten_min_ago],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        let dirty = checks
            .iter()
            .find(|c| c.name.starts_with("reconcile dirty gap"))
            .expect("dirty gap check must fire");
        assert_eq!(dirty.status, DoctorStatus::Warn);
        assert!(dirty.detail.as_deref().unwrap_or("").contains("desired=1"));
    }

    /// A fresh dirty generation gap must not warn because the manager
    /// is expected to pick it up within seconds.
    #[test]
    fn mf6_doctor_dirty_gap_fresh_silent() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1, dirty_since_ns = ?1
                 WHERE repo_hash = 'h'",
                rusqlite::params![now],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        assert!(
            checks
                .iter()
                .all(|c| !c.name.starts_with("reconcile dirty gap")),
            "fresh dirty must not warn; checks = {checks:?}"
        );
    }

    /// A failed reconcile with a scheduled retry reports `Warn` and
    /// preserves the last error for the operator.
    #[test]
    fn mf7_doctor_retry_backoff_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     consecutive_failures = 3,
                     next_retry_at_ns = 9999999999,
                     last_error = 'EMFILE'
                 WHERE repo_hash = 'h'",
                [],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        let retry = checks
            .iter()
            .find(|c| c.name.starts_with("reconcile retry"))
            .expect("retry check must fire");
        assert_eq!(retry.status, DoctorStatus::Warn);
        assert!(retry.detail.as_deref().unwrap_or("").contains("EMFILE"));
    }

    /// An in-flight reconcile attempt older than the threshold reports
    /// `Warn`.
    #[test]
    fn mf8_doctor_stuck_attempt_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let long_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - 20 * 60 * 1_000_000_000;
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     attempt_generation = 1,
                     last_attempt_ns = ?1
                 WHERE repo_hash = 'h'",
                rusqlite::params![long_ago],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        let stuck = checks
            .iter()
            .find(|c| c.name.starts_with("reconcile attempt"))
            .expect("stuck attempt check must fire");
        assert_eq!(stuck.status, DoctorStatus::Warn);
        assert!(stuck.detail.as_deref().unwrap_or("").contains("wedged"));
    }

    /// A recently started reconcile attempt must not warn.
    #[test]
    fn mf8_doctor_fresh_attempt_silent() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     attempt_generation = 1,
                     last_attempt_ns = ?1
                 WHERE repo_hash = 'h'",
                rusqlite::params![now],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        assert!(
            checks
                .iter()
                .all(|c| !c.name.starts_with("reconcile attempt")),
            "fresh attempt must not warn; checks = {checks:?}"
        );
    }

    /// A failed watcher reports `Warn` with its persisted error.
    #[test]
    fn mf9_doctor_watcher_failed_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET watcher_state = 'failed', watcher_error = 'git open failed'
                 WHERE repo_hash = 'h'",
                [],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        let watcher = checks
            .iter()
            .find(|c| c.name.starts_with("watcher lifecycle"))
            .expect("watcher lifecycle check must fire");
        assert_eq!(watcher.status, DoctorStatus::Warn);
        assert!(
            watcher
                .detail
                .as_deref()
                .unwrap_or("")
                .contains("git open failed")
        );
    }

    /// An active watcher does not emit a warning.
    #[test]
    fn mf9_doctor_watcher_active_silent() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET watcher_state = 'active'
                 WHERE repo_hash = 'h'",
                [],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        assert!(
            checks
                .iter()
                .all(|c| !c.name.starts_with("watcher lifecycle")),
            "active watcher must not warn; checks = {checks:?}"
        );
    }

    /// Doctor fails closed when persisted generations violate
    /// `applied <= desired`.
    #[test]
    fn mf10_doctor_applied_over_desired_fails() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, |tx| {
            // CHECK constraints prevent this via UPDATE; simulate
            // corruption by writing directly to the underlying
            // column via a temporary CHECK-less path. Use SQLite
            // pragma to disable defer/enforce and rewrite.
            tx.execute("PRAGMA ignore_check_constraints = ON", [])
                .unwrap();
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1, applied_generation = 5
                 WHERE repo_hash = 'h'",
                [],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        let inv = checks
            .iter()
            .find(|c| c.name.starts_with("reconcile invariants"))
            .expect("invariants check must fire");
        assert_eq!(inv.status, DoctorStatus::Fail);
    }

    /// When aliases share one `repo_hash`, doctor emits one reconcile
    /// check per repo, not identical alias-level duplicates. The
    /// label lists both aliases.
    #[test]
    fn mf11_doctor_multi_alias_reconcile_dedup() {
        let (_t, cas) = seeded_cas(&[("a", "/p", "h"), ("b", "/p", "h")]);
        let long_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - 20 * 60 * 1_000_000_000;
        set_state(&cas, |tx| {
            tx.execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1, dirty_since_ns = ?1
                 WHERE repo_hash = 'h'",
                rusqlite::params![long_ago],
            )
            .unwrap();
        });
        let checks = reconcile_state_checks(&cas).unwrap();
        let hits: Vec<_> = checks
            .iter()
            .filter(|c| c.name.starts_with("reconcile dirty gap"))
            .collect();
        assert_eq!(hits.len(), 1, "must not duplicate per alias");
        assert!(
            hits[0].name.contains("aliases: a, b"),
            "label must list both aliases; got {}",
            hits[0].name
        );
    }
}
