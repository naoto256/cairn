use std::collections::{HashMap, HashSet};

use cairn_proto::control::{DoctorCheck, DoctorStatus};

use super::{AliasStoreProbe, AliasStoreState, STUCK_RUN_THRESHOLD, Tier3Run, doctor_check};
use crate::workspace_analyzer::StaleRevision;

/// Surface analyzer-revision drift as a doctor warning, even after the
/// startup hook has already enqueued reruns. This is the shadow-case
/// fallback: if `staleness::check_revision_staleness_and_enqueue`
/// failed at boot (DB error, JobManager full, etc.), the
/// `workspace_analysis_runs.analyzer_revision` column still records
/// the old value and the operator sees it here. Empty
/// `stale_revisions` means everything matches `expected_analyzer.revision()`
/// at probe time.
pub(super) fn revision_stale_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    probes
        .iter()
        .filter_map(|probe| {
            let state = probe.result.as_ref().ok()?;
            if state.stale_revisions.is_empty() {
                return None;
            }
            let detail = state
                .stale_revisions
                .iter()
                .map(|sr| {
                    let cur = sr
                        .current_rev
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| "none".to_string());
                    format!("{}: current={}, expected={}", sr.analyzer_id, cur, sr.expected_rev)
                })
                .collect::<Vec<_>>()
                .join("; ");
            Some(doctor_check(
                format!("repo `{}` analyzer revision drift", probe.alias),
                DoctorStatus::Warn,
                Some(detail),
                Some(format!(
                    "Run `cairn ctl repo reindex {}` to rerun the stale analyzers under the current build's revisions.",
                    probe.alias
                )),
            ))
        })
        .collect()
}

/// Surface `parser_revision` drift as a doctor warning. Same shadow-
/// case role as `revision_stale_checks` (analyzer revision), but for
/// the Tier-1 backend's `parser_revision()` vs. `blobs.parser_revision`.
/// Empty `stale_parser_revisions` means every expected parse unit has
/// a row whose persisted revision equals the linked-in backend's.
pub(super) fn parser_revision_stale_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    probes
        .iter()
        .filter_map(|probe| {
            let state = probe.result.as_ref().ok()?;
            if state.stale_parser_revisions.is_empty() {
                return None;
            }
            let detail = state
                .stale_parser_revisions
                .iter()
                .map(|psr| {
                    let cur = psr
                        .current_rev
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| "missing".to_string());
                    format!(
                        "{}: current={} ({} blob{}), expected={}",
                        psr.parser_id,
                        cur,
                        psr.affected_blob_count,
                        if psr.affected_blob_count == 1 { "" } else { "s" },
                        psr.expected_rev,
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            Some(doctor_check(
                format!("repo `{}` parser revision drift", probe.alias),
                DoctorStatus::Warn,
                Some(detail),
                Some(format!(
                    "Run `cairn ctl repo reindex {}` to reparse blobs at the current build's `parser_revision`.",
                    probe.alias
                )),
            ))
        })
        .collect()
}

/// Cross-references analyzer and parser drift against
/// `workspace_analysis_runs` for the alias's current tentative
/// manifest. This surfaces the post-enqueue lifecycle that an
/// operator would otherwise have to reconstruct from logs:
///
/// - **Case A (`Fail`, drift + run at expected revision succeeded)**:
///   the drift detector reports the alias as still stale, yet the
///   corresponding `workspace_analysis_runs` row is `succeeded` at
///   the current build's revision. For analyzer-revision drift this
///   contradicts the single-row `(manifest_id, analyzer_id)` PK and
///   should never fire while the persisted-state invariants hold.
///   Surfacing it defensively catches a future classifier regression.
///   For parser-revision drift, analyzer runs all succeeded at their
///   expected revision while `blobs.parser_revision` still mismatches.
///   That means the reindex chain
///   (`scanner` → `enqueue_full_repo_reindex` → `register_repo_inner`
///   → pre-publication Tier-1 parse) wrote the new analyzer rows without
///   updating the parser layer.
///
/// - **Case B (`Warn`, run at current revision failed/timed_out/cancelled)**:
///   the rerun reached the worker and terminated with a failure that
///   the operator should look at directly.
///
/// - **Case C (`Pass`, run is queued or running)**: a rerun is on the
///   way; surface as informational rather than a warning.
///
/// - **Case D (`Warn`, no run row at all)**: the rerun was never
///   enqueued, was coalesced or dropped at enqueue time, or was lost
///   before the worker picked it up (e.g. a daemon restart between
///   `enqueue` and `restore_from_db`).
///
/// - **Case E (silent)**: no drift on this alias → no rerun-health
///   check emitted. Doctor output noise is kept minimal.
///
/// Each emitted check carries explicit operator remediation. The
/// parser-drift evaluation walks every analyzer in
/// `expected_tier3_analyzer_ids` so a mixed state — for example,
/// analyzer A succeeded while analyzer B failed — surfaces the
/// failure instead of being classified from the succeeded row alone.
pub(super) fn analyzer_rerun_health_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    let mut out = Vec::new();
    for probe in probes {
        let Ok(state) = probe.result.as_ref() else {
            continue;
        };
        // Case E: no drift on this alias — emit nothing.
        if state.stale_revisions.is_empty() && state.stale_parser_revisions.is_empty() {
            continue;
        }
        // Per analyzer-revision-drift entry: one check.
        for sr in &state.stale_revisions {
            out.push(analyzer_drift_rerun_check(
                &probe.alias,
                sr,
                &state.tier3_runs,
            ));
        }
        // Per parser-revision-drift presence: one alias-level check
        // that evaluates every expected analyzer in aggregate.
        if !state.stale_parser_revisions.is_empty() {
            out.push(parser_drift_rerun_check(
                &probe.alias,
                &state.stale_parser_revisions,
                &state.expected_tier3_analyzer_ids,
                &state.expected_analyzer_revisions,
                &state.tier3_runs,
            ));
        }
    }
    out
}

/// Per-analyzer companion to the alias-wide
/// [`analyzer_rerun_health_checks`]. Matches the latest
/// `workspace_analysis_runs` row for `stale.analyzer_id` and
/// classifies:
///
/// - **No row** → `Warn` (rerun never enqueued or lost).
/// - **`succeeded` at the expected rev** → `Fail`. Contradicts the
///   drift detector; the `(manifest_id, analyzer_id)` PK plus the
///   classifier's revision stamp should keep this unreachable under
///   v0.7.0+ invariants — emitted defensively so a future refactor
///   that breaks the invariant still surfaces.
/// - **`succeeded` at an older rev** → `Warn` (rerun never landed).
/// - **`failed` / `timed_out` / `cancelled`** → `Warn` (the rerun
///   ran and errored; operator should look at the job).
/// - **`queued` / `running` at the expected rev** → `Pass` (current
///   rerun in flight).
/// - **`queued` / `running` at an older rev** → `Warn`.
///   [`crate::jobs::JobManager::enqueue_analyzer_run`] stamps the
///   current revision on enqueue, so a stale pending row is either
///   an old-binary residual, a `restore_from_db` relist, or a rerun
///   that was never stamped — not an in-flight current rerun.
/// - **Anything else** → `Warn` (unknown status; forward-compat
///   guard for a future status the operator's build doesn't know).
fn analyzer_drift_rerun_check(
    alias: &str,
    stale: &StaleRevision,
    runs: &[Tier3Run],
) -> DoctorCheck {
    let row = runs.iter().find(|r| r.analyzer_id == stale.analyzer_id);
    let name = format!(
        "repo `{alias}` analyzer `{}` rerun health",
        stale.analyzer_id
    );
    match row {
        None => doctor_check(
            name,
            DoctorStatus::Warn,
            Some(format!(
                "analyzer rerun was not enqueued, was dropped, or was lost before run \
                 (no `workspace_analysis_runs` row; expected revision {})",
                stale.expected_rev
            )),
            Some(format!(
                "Check the daemon log (e.g. `journalctl -u cairn` or your daemon log path) and grep for `{alias}` plus `staleness` to find scanner enqueue failures or coalesced jobs. Then run `cairn ctl repo reindex {alias}` for a manual recovery.",
            )),
        ),
        Some(run) => {
            let at_current = run.analyzer_revision == stale.expected_rev;
            match (run.status.as_str(), at_current) {
                ("succeeded", true) => doctor_check(
                    name,
                    DoctorStatus::Fail,
                    Some(format!(
                        "analyzer-revision drift reported but `workspace_analysis_runs` shows `succeeded` at the current revision ({}) — classifier / persist invariant broken",
                        stale.expected_rev
                    )),
                    Some(format!(
                        "Run `cairn ctl repo reindex {alias}` to rebuild persisted state at the current parser and analyzer revisions; older persisted state may need this one-time refresh. If this recurs after a fresh reindex with the current binary, it is a structural bug — please file an issue.",
                    )),
                ),
                ("succeeded", false) => doctor_check(
                    name,
                    DoctorStatus::Warn,
                    Some(format!(
                        "analyzer rerun was not enqueued, was dropped, or was lost before run \
                         (latest row at revision {} succeeded; expected revision {})",
                        run.analyzer_revision, stale.expected_rev
                    )),
                    Some(format!(
                        "Check the daemon log (e.g. `journalctl -u cairn` or your daemon log path) and grep for `{alias}` plus `staleness` to find scanner enqueue failures or coalesced jobs. Then run `cairn ctl repo reindex {alias}` for a manual recovery.",
                    )),
                ),
                ("failed" | "timed_out" | "cancelled", _) => doctor_check(
                    name,
                    DoctorStatus::Warn,
                    Some(format!(
                        "analyzer rerun failed at revision {} (status `{}`): {}",
                        run.analyzer_revision,
                        run.status,
                        run.error.as_deref().unwrap_or("<no error message>")
                    )),
                    Some(format!(
                        "Inspect `cairn ctl jobs list {alias}` for the failed job details, then `cairn ctl repo reindex {alias}` to retry.",
                    )),
                ),
                ("queued" | "running", true) => doctor_check(
                    name,
                    DoctorStatus::Pass,
                    Some(format!(
                        "analyzer rerun pending: `{}` at revision {}",
                        run.status, run.analyzer_revision
                    )),
                    Some(format!(
                        "Run `cairn ctl jobs list {alias}` to watch progress; the rerun will land on its own.",
                    )),
                ),
                // `JobManager::enqueue_analyzer_run` stamps the current
                // `analyzer_revision` when it enqueues a current rerun,
                // so a `queued` / `running` row at an OLDER revision is
                // NOT an in-flight current rerun — it is either an old
                // binary's row that remained, a `restore_from_db` of an
                // old `running` re-listed as `queued`, or a current
                // rerun that was never stamped / was coalesced. The
                // "rerun will land on its own" framing would be a lie.
                ("queued" | "running", false) => doctor_check(
                    name,
                    DoctorStatus::Warn,
                    Some(format!(
                        "analyzer rerun `{}` is at stale revision {} (expected {}); the current rerun has not landed — an old-binary row may be stuck or the rerun was never stamped",
                        run.status, run.analyzer_revision, stale.expected_rev
                    )),
                    Some(format!(
                        "Inspect `cairn ctl jobs list {alias}` for the stuck row, then `cairn ctl repo reindex {alias}` to force a current-revision rerun.",
                    )),
                ),
                _ => doctor_check(
                    name,
                    DoctorStatus::Warn,
                    Some(format!(
                        "analyzer rerun is in unexpected status `{}` at revision {} (expected {})",
                        run.status, run.analyzer_revision, stale.expected_rev
                    )),
                    Some(format!(
                        "Run `cairn ctl repo reindex {alias}` to retry the rerun.",
                    )),
                ),
            }
        }
    }
}

/// Aggregate the rerun state of every analyzer in
/// `expected_analyzer_ids` and surface the worst case so an alias
/// with a mixed picture (one analyzer succeeded, another failed) does
/// not get misclassified as Case A on the succeeded slice.
///
/// Selection order (first match wins):
///
/// - **Case A** (`Fail`) — every expected analyzer succeeded at its
///   current revision but parser drift remains. Silent-data-loss
///   safety net for the reindex chain.
/// - **Case B** (`Warn`) — at least one analyzer's rerun failed.
/// - **Case B-like** (`Warn`) — at least one rerun is
///   `queued` / `running` at a stale (or unknown-to-this-build)
///   revision; the current-revision rerun has not landed.
/// - **Case D** (`Warn`) — at least one expected analyzer has no
///   row. Ordered before Case C so a mixed
///   pending-current + missing-row state does not get masked by
///   the pending Pass.
/// - **Case D-like** (`Warn`) — at least one `succeeded` row is at
///   a revision older than the current build's (may coexist with a
///   pending-current row). Also ordered before Case C for the same
///   reason.
/// - **Case C** (`Pass`) — at least one analyzer pending at the
///   current revision and none of the above.
/// - **Fallback** (`Pass`) — the case matrix did not match. Two
///   distinct situations land here: (a) parser drift with no
///   expected analyzer to cross-reference (a Tier-1-only language
///   with no Tier-2.5/3 analyzer), and (b) an expected analyzer
///   whose only row carries a status string this build does not
///   recognize (the `_ =>` arm clears `every_succeeded_at_current`
///   without setting any warn flag, so an unknown status also
///   reaches the fallback). The emitted detail is worded for case
///   (a); in either case the plain [`parser_revision_stale_checks`]
///   `Warn` still carries the operator surface for the drift itself.
fn parser_drift_rerun_check(
    alias: &str,
    stale_parser: &[crate::workspace_analyzer::ParserStaleRevision],
    expected_analyzer_ids: &[String],
    expected_analyzer_revisions: &HashMap<String, u32>,
    runs: &[Tier3Run],
) -> DoctorCheck {
    let name = format!("repo `{alias}` parser drift rerun health");
    let mut any_failed = None;
    let mut any_pending_current = false;
    let mut any_pending_stale: Option<(String, String, u32, Option<u32>)> = None;
    let mut any_row_missing = false;
    let mut any_stale_succeeded: Option<(String, u32, u32)> = None;
    let mut any_unknown_status: Option<(String, String)> = None;
    let mut every_succeeded_at_current = !expected_analyzer_ids.is_empty();

    for analyzer_id in expected_analyzer_ids {
        let row = runs.iter().find(|r| r.analyzer_id == *analyzer_id);
        let expected_rev = expected_analyzer_revisions.get(analyzer_id).copied();
        match row {
            None => {
                any_row_missing = true;
                every_succeeded_at_current = false;
            }
            Some(run) => {
                match run.status.as_str() {
                    "succeeded" => {
                        // `succeeded` alone does not mean the current
                        // revision succeeded; the row must match the
                        // current build's expectation. A
                        // `succeeded` row at an older revision is the
                        // "rerun never landed" case (Case D-like) and
                        // must NOT count toward the Case A safety net.
                        match expected_rev {
                            Some(exp) if run.analyzer_revision == exp => {
                                // current succeeded — counts toward
                                // the every-succeeded-at-current pile.
                            }
                            Some(exp) => {
                                any_stale_succeeded.get_or_insert((
                                    analyzer_id.clone(),
                                    run.analyzer_revision,
                                    exp,
                                ));
                                every_succeeded_at_current = false;
                            }
                            None => {
                                // Analyzer is no longer in the
                                // linked-in registry for this manifest;
                                // observability is best-effort here.
                                // Don't count it toward Case A.
                                every_succeeded_at_current = false;
                            }
                        }
                    }
                    "failed" | "timed_out" | "cancelled" => {
                        any_failed.get_or_insert((
                            analyzer_id.clone(),
                            run.status.clone(),
                            run.error.clone(),
                        ));
                        every_succeeded_at_current = false;
                    }
                    "queued" | "running" => {
                        // A `queued` / `running` row at an older
                        // revision (or with no expected
                        // revision known) is NOT an in-flight current
                        // rerun — `JobManager::enqueue_analyzer_run`
                        // stamps the current `analyzer_revision` on
                        // enqueue. Treat stale pending rows as Warn,
                        // not Pass.
                        match expected_rev {
                            Some(exp) if run.analyzer_revision == exp => {
                                any_pending_current = true;
                            }
                            _ => {
                                any_pending_stale.get_or_insert((
                                    analyzer_id.clone(),
                                    run.status.clone(),
                                    run.analyzer_revision,
                                    expected_rev,
                                ));
                            }
                        }
                        every_succeeded_at_current = false;
                    }
                    _ => {
                        any_unknown_status.get_or_insert((analyzer_id.clone(), run.status.clone()));
                        every_succeeded_at_current = false;
                    }
                }
            }
        }
    }

    let parser_summary = stale_parser
        .iter()
        .map(|psr| {
            let cur = psr
                .current_rev
                .map(|r| r.to_string())
                .unwrap_or_else(|| "missing".to_string());
            format!(
                "{}: current={}, expected={}",
                psr.parser_id, cur, psr.expected_rev
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    // Case A — every expected analyzer succeeded at its current
    // revision, yet parser drift remains. This is the safety net for
    // a reindex chain that published analyzer output without bringing
    // the parser layer to the expected revision.
    if every_succeeded_at_current {
        return doctor_check(
            name,
            DoctorStatus::Fail,
            Some(format!(
                "every expected analyzer succeeded at its current revision but parser drift remains ({parser_summary}) — the parser-drift / full-reindex chain is broken (analyzer succeeded but parser_revision was not updated)",
            )),
            Some(format!(
                "Run `cairn ctl repo reindex {alias}` to rebuild persisted state at the current parser and analyzer revisions; older persisted state may need this one-time refresh. If this recurs after a fresh reindex with the current binary, it is a structural bug — please file an issue.",
            )),
        );
    }
    // Case B — at least one analyzer failed.
    if let Some((analyzer_id, status, error)) = any_failed {
        return doctor_check(
            name,
            DoctorStatus::Warn,
            Some(format!(
                "parser drift remains and at least one analyzer rerun failed: `{analyzer_id}` is `{status}` ({err}). Parser drift summary: {parser_summary}",
                err = error.as_deref().unwrap_or("<no error message>")
            )),
            Some(format!(
                "Inspect `cairn ctl jobs list {alias}` for the failed job details, then `cairn ctl repo reindex {alias}` to retry.",
            )),
        );
    }
    // Case B-like — at least one analyzer is `queued` / `running`
    // at a stale (or unknown) revision. This must be checked before
    // Case C because treating all pending rows as one flag without
    // checking `analyzer_revision` would mask this state. A stale
    // pending row is
    // NOT an in-flight current rerun (the JobManager stamps the
    // current revision on enqueue), so the operator must be warned.
    if let Some((analyzer_id, status, persisted, expected_rev)) = any_pending_stale {
        let exp_str = expected_rev
            .map(|r| r.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        return doctor_check(
            name,
            DoctorStatus::Warn,
            Some(format!(
                "parser drift remains and at least one analyzer rerun is `{status}` at stale revision: `{analyzer_id}` at revision {persisted}, expected {exp_str} — the current rerun has not landed. Parser drift summary: {parser_summary}",
            )),
            Some(format!(
                "Inspect `cairn ctl jobs list {alias}` for the stuck row, then `cairn ctl repo reindex {alias}` to force a current-revision rerun.",
            )),
        );
    }
    // Case D — at least one analyzer row missing. This must run
    // before Case C so a mixed (pending-current + missing-row) state
    // surfaces as Warn rather
    // than being masked by the pending Pass.
    if any_row_missing {
        return doctor_check(
            name,
            DoctorStatus::Warn,
            Some(format!(
                "parser drift remains and at least one expected analyzer has no rerun row — the rerun was not enqueued, was dropped, or was lost before run. Parser drift summary: {parser_summary}",
            )),
            Some(format!(
                "Check the daemon log (e.g. `journalctl -u cairn` or your daemon log path) and grep for `{alias}` plus `staleness` to find scanner enqueue failures or coalesced jobs. Then run `cairn ctl repo reindex {alias}` for a manual recovery.",
            )),
        );
    }
    // Case D-like — every row is `succeeded` but at least one is at
    // a revision older than what the current build expects. The
    // scanner did not enqueue the analyzer-revision rerun, or it
    // was dropped before the worker landed it. Checking the persisted
    // revision prevents all-`succeeded` rows from being mistaken for
    // the safety-net Case A. This must run before Case C so a mixed
    // pending-current + stale-succeeded state surfaces as Warn rather
    // than being masked by Pass.
    if let Some((stale_analyzer, persisted, expected_rev)) = any_stale_succeeded {
        return doctor_check(
            name,
            DoctorStatus::Warn,
            Some(format!(
                "parser drift remains and at least one analyzer rerun never landed at the current revision: `{stale_analyzer}` is `succeeded` at revision {persisted}, expected revision {expected_rev}. Parser drift summary: {parser_summary}",
            )),
            Some(format!(
                "Check the daemon log (e.g. `journalctl -u cairn` or your daemon log path) and grep for `{alias}` plus `staleness` to find scanner enqueue failures or coalesced jobs. Then run `cairn ctl repo reindex {alias}` for a manual recovery.",
            )),
        );
    }
    if let Some((analyzer_id, status)) = any_unknown_status {
        return doctor_check(
            name,
            DoctorStatus::Warn,
            Some(format!(
                "parser drift remains and analyzer `{analyzer_id}` reported unrecognized status `{status}`. Parser drift summary: {parser_summary}",
            )),
            Some(format!(
                "Run `cairn ctl repo reindex {alias}` and inspect the daemon log if the status persists.",
            )),
        );
    }
    // Case C — at least one analyzer pending at the current revision
    // (and no failed / stale-pending / missing / stale-succeeded).
    if any_pending_current {
        return doctor_check(
            name,
            DoctorStatus::Pass,
            Some(format!(
                "parser drift rerun pending (one or more expected analyzers queued/running at the current revision). Parser drift summary: {parser_summary}",
            )),
            Some(format!(
                "Run `cairn ctl jobs list {alias}` to watch progress; the rerun will land on its own.",
            )),
        );
    }
    // Fallback — parser drift exists with no expected analyzers (e.g.
    // a Tier-1-only language that has no Tier-2.5/3 analyzer); leave
    // the plain `parser_revision_stale_checks` Warn to carry the
    // operator surface and emit nothing here.
    doctor_check(
        name,
        DoctorStatus::Pass,
        Some(format!(
            "parser drift recorded but no expected analyzer to cross-reference ({parser_summary})",
        )),
        None,
    )
}

pub(super) fn tier3_run_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    probes
        .iter()
        .map(|probe| match &probe.result {
            Ok(state) => tier3_run_check(&probe.alias, state),
            Err(error) => doctor_check(
                format!("repo `{}` Tier-3 analyzer status", probe.alias),
                DoctorStatus::Fail,
                Some(error.clone()),
                Some(format!(
                    "Run `cairn ctl repo remove {}` then re-register, or restore the CAS file at {}.",
                    probe.alias,
                    probe.store_path.display()
                )),
            ),
        })
        .collect()
}

/// Emits exactly one check per alias. When several conditions hold
/// concurrently the highest-priority label wins; precedence (first
/// match returns) is:
///
/// 1. No rows but at least one expected analyzer is missing →
///    `Warn` (never indexed, or a fresh analyzer added to this
///    build).
/// 2. No rows and none expected → `Pass` (not applicable).
/// 3. A `queued` / `running` row whose `started_at_ns` is older
///    than [`STUCK_RUN_THRESHOLD`] → `Warn` (wedged).
/// 4. Any other `queued` / `running` row → `Warn` (indexing in
///    progress).
/// 5. Any `failed` row → `Warn`.
/// 6. Any `timed_out` / `cancelled` row → `Warn`.
/// 7. A row with a status this build doesn't recognize → `Warn`
///    (forward-compat guard).
/// 8. Any expected analyzer with no row → `Warn` (analyzer set
///    drift not yet reflected in the store).
/// 9. Otherwise → `Pass`.
///
/// The stuck check (3) intentionally runs before the plain
/// in-progress check (4) so a wedged worker does not hide behind
/// "indexing in progress" forever.
fn tier3_run_check(alias: &str, state: &AliasStoreState) -> DoctorCheck {
    if state.tier3_runs.is_empty() {
        let missing = missing_tier3_analyzer_ids(state);
        if !missing.is_empty() {
            return doctor_check(
                format!("repo `{alias}` Tier-3 analyzer status"),
                DoctorStatus::Warn,
                Some(tier3_runs_detail(state)),
                Some(format!(
                    "Trigger a reindex with `cairn ctl repo reindex {alias}` to record the current workspace analyzer set."
                )),
            );
        }

        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Pass,
            Some("not applicable (no workspace analyzers expected)".into()),
            None,
        );
    }

    let detail = tier3_runs_detail(state);

    // Stuck-run check: a `queued` or `running` row whose
    // `started_at_ns` is older than `STUCK_RUN_THRESHOLD` (6h) is
    // almost certainly wedged (worker hang, pool deadlock, daemon
    // crash that left the row mid-flight). Surface it loudly so the
    // operator can `reindex_repo` instead of staring at "indexing in
    // progress" forever. The `started_at_ns > 0` guard skips rows
    // with an unset (zero) `started_at_ns` — an age can't be computed
    // for them, so they never trip the stuck branch and fall through
    // to the plain in-progress check below.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let stuck_threshold_ns = STUCK_RUN_THRESHOLD.as_nanos() as i64;
    if let Some(run) = state.tier3_runs.iter().find(|run| {
        matches!(run.status.as_str(), "queued" | "running")
            && run.started_at_ns > 0
            && now_ns.saturating_sub(run.started_at_ns) > stuck_threshold_ns
    }) {
        let age_hours = (now_ns.saturating_sub(run.started_at_ns) / 1_000_000_000) / 3600;
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!(
                "{detail}; {} stuck in `{}` for ~{}h (threshold {}h)",
                run.analyzer_id,
                run.status,
                age_hours,
                STUCK_RUN_THRESHOLD.as_secs() / 3600,
            )),
            Some(format!(
                "Likely a wedged worker. Run `cairn ctl repo reindex {alias}` to clear and re-queue."
            )),
        );
    }

    if state
        .tier3_runs
        .iter()
        .any(|run| matches!(run.status.as_str(), "queued" | "running"))
    {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!("{detail}; indexing in progress")),
            Some(format!(
                "Track progress with `cairn ctl jobs list --alias {alias}`."
            )),
        );
    }

    if let Some(run) = state.tier3_runs.iter().find(|run| run.status == "failed") {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!(
                "{detail}; {} failed: {}",
                run.analyzer_id,
                run.error.as_deref().unwrap_or("unknown error")
            )),
            Some(format!(
                "Check daemon logs near manifest {}; transient failures usually recover on the next watcher tick. If persistent, try `cairn ctl repo reindex {alias}`.",
                run.manifest_id
            )),
        );
    }

    if let Some(run) = state
        .tier3_runs
        .iter()
        .find(|run| matches!(run.status.as_str(), "timed_out" | "cancelled"))
    {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!("{detail}; {} is {}", run.analyzer_id, run.status)),
            Some(format!(
                "Trigger a reindex with `cairn ctl repo reindex {alias}` when ready."
            )),
        );
    }

    if let Some(run) = state.tier3_runs.iter().find(|run| {
        !matches!(
            run.status.as_str(),
            "succeeded" | "skipped" | "queued" | "running" | "cancelled" | "timed_out"
        )
    }) {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(format!(
                "{detail}; {} reported status `{}` at manifest {} (not recognized by this doctor build)",
                run.analyzer_id, run.status, run.manifest_id
            )),
            Some(format!(
                "Trigger a reindex with `cairn ctl repo reindex {alias}` and check daemon logs if the status persists."
            )),
        );
    }

    let missing = missing_tier3_analyzer_ids(state);
    if !missing.is_empty() {
        return doctor_check(
            format!("repo `{alias}` Tier-3 analyzer status"),
            DoctorStatus::Warn,
            Some(detail),
            Some(format!(
                "Trigger a reindex with `cairn ctl repo reindex {alias}` to record the current workspace analyzer set."
            )),
        );
    }

    doctor_check(
        format!("repo `{alias}` Tier-3 analyzer status"),
        DoctorStatus::Pass,
        Some(detail),
        None,
    )
}

fn tier3_runs_detail(state: &AliasStoreState) -> String {
    let manifest_id = state
        .tier3_runs
        .iter()
        .map(|run| run.manifest_id)
        .min()
        .or(state.tentative_manifest_id)
        .unwrap_or_default();
    let mut statuses = state
        .tier3_runs
        .iter()
        .map(|run| {
            let status = tier3_status_label(run);
            format!("{}={status}", run.analyzer_id)
        })
        .collect::<Vec<_>>();
    statuses.extend(
        missing_tier3_analyzer_ids(state)
            .into_iter()
            .map(|analyzer_id| format!("{analyzer_id}=not yet recorded (run reindex)")),
    );
    let statuses = statuses.join(", ");
    format!("Tier-3 analyzer runs at manifest {manifest_id}: {statuses}")
}

fn missing_tier3_analyzer_ids(state: &AliasStoreState) -> Vec<String> {
    let recorded = state
        .tier3_runs
        .iter()
        .map(|run| run.analyzer_id.as_str())
        .collect::<HashSet<_>>();
    state
        .expected_tier3_analyzer_ids
        .iter()
        .filter(|analyzer_id| !recorded.contains(analyzer_id.as_str()))
        .cloned()
        .collect()
}

fn tier3_status_label(run: &Tier3Run) -> String {
    match (run.status.as_str(), run.error.as_deref()) {
        ("succeeded", _) => "succeeded".into(),
        ("skipped", Some(error)) => format!("skipped ({error})"),
        ("skipped", None) => "skipped".into(),
        ("queued", _) => "queued".into(),
        ("running", _) => "in progress".into(),
        ("timed_out", Some(error)) => format!("timed out ({error})"),
        ("timed_out", None) => "timed out".into(),
        ("cancelled", _) => "cancelled".into(),
        (status, _) => status.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{AliasStoreProbe, AliasStoreState, Tier3Run};
    use super::*;
    use crate::workspace_analyzer::{ParserStaleRevision, StaleRevision};
    use std::collections::HashMap;
    use std::path::PathBuf;

    const RUST_ANALYZER_ID: &str = "rust-analyzer-lsp";

    #[test]
    fn tier3_run_checks_map_statuses_to_actionable_results() {
        let succeeded = tier3_run_check(
            "ok",
            &AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 1,
                    status: "succeeded".into(),
                    error: None,
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        let skipped = tier3_run_check(
            "skip",
            &AliasStoreState {
                tentative_manifest_id: Some(2),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 2,
                    status: "skipped".into(),
                    error: Some("ContentModified".into()),
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        let pending = tier3_run_check(
            "queued",
            &AliasStoreState {
                tentative_manifest_id: Some(5),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 5,
                    status: "queued".into(),
                    error: None,
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        let running = tier3_run_check(
            "running",
            &AliasStoreState {
                tentative_manifest_id: Some(6),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 6,
                    status: "running".into(),
                    error: None,
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        let failed = tier3_run_check(
            "fail",
            &AliasStoreState {
                tentative_manifest_id: Some(3),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 3,
                    status: "failed".into(),
                    error: Some("boom".into()),
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        let not_applicable = tier3_run_check(
            "not-applicable",
            &AliasStoreState {
                tentative_manifest_id: Some(4),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );

        assert_eq!(succeeded.status, DoctorStatus::Pass);
        assert_eq!(skipped.status, DoctorStatus::Pass);
        assert!(
            skipped
                .detail
                .as_deref()
                .unwrap()
                .contains("ContentModified")
        );
        assert_eq!(pending.status, DoctorStatus::Warn);
        assert!(pending.detail.as_deref().unwrap().contains("queued"));
        assert!(
            pending
                .remediation
                .as_deref()
                .unwrap()
                .contains("jobs list")
        );
        assert_eq!(running.status, DoctorStatus::Warn);
        assert!(running.detail.as_deref().unwrap().contains("in progress"));
        assert!(
            running
                .remediation
                .as_deref()
                .unwrap()
                .contains("jobs list")
        );
        assert_eq!(failed.status, DoctorStatus::Warn);
        assert!(
            failed
                .remediation
                .as_deref()
                .unwrap()
                .contains("manifest 3")
        );
        assert_eq!(not_applicable.status, DoctorStatus::Pass);
        assert_eq!(
            not_applicable.detail.as_deref(),
            Some("not applicable (no workspace analyzers expected)")
        );
        assert!(not_applicable.remediation.is_none());
    }

    #[test]
    fn tier3_run_check_warns_when_expected_analyzer_has_no_run() {
        let check = tier3_run_check(
            "missing",
            &AliasStoreState {
                tentative_manifest_id: Some(4),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: vec!["demo-analyzer".into()],
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );

        assert_eq!(check.status, DoctorStatus::Warn);
        assert!(
            check
                .detail
                .as_deref()
                .unwrap()
                .contains("demo-analyzer=not yet recorded")
        );
        assert!(
            check
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex missing")
        );
    }

    #[test]
    fn tier3_run_check_reports_python_success_when_rust_skips() {
        let check = tier3_run_check(
            "py",
            &AliasStoreState {
                tentative_manifest_id: Some(9),
                tier3_runs: vec![
                    Tier3Run {
                        analyzer_id: "pyright-lsp".into(),
                        manifest_id: 9,
                        status: "succeeded".into(),
                        error: None,
                        started_at_ns: 0,
                        analyzer_revision: 1,
                    },
                    Tier3Run {
                        analyzer_id: RUST_ANALYZER_ID.into(),
                        manifest_id: 9,
                        status: "skipped".into(),
                        error: Some("no matching files".into()),
                        started_at_ns: 0,
                        analyzer_revision: 1,
                    },
                ],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );

        assert_eq!(check.status, DoctorStatus::Pass);
        let detail = check.detail.as_deref().unwrap();
        assert!(detail.contains("pyright-lsp=succeeded"));
        assert!(detail.contains("rust-analyzer-lsp=skipped (no matching files)"));
    }

    #[test]
    fn tier3_run_check_reports_expected_analyzer_without_run_record() {
        let check = tier3_run_check(
            "stale",
            &AliasStoreState {
                tentative_manifest_id: Some(10),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "old-analyzer".into(),
                    manifest_id: 10,
                    status: "succeeded".into(),
                    error: None,
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: vec!["new-analyzer".into(), "old-analyzer".into()],
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );

        assert_eq!(check.status, DoctorStatus::Warn);
        let detail = check.detail.as_deref().unwrap();
        assert!(detail.contains("old-analyzer=succeeded"));
        assert!(detail.contains("new-analyzer=not yet recorded (run reindex)"));
        assert!(
            check
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex stale")
        );
    }

    /// `revision_stale_checks` MUST emit one `Warn` per probe whose
    /// `stale_revisions` is non-empty, with the analyzer id, current
    /// revision, and expected revision surfaced in detail.
    #[test]
    fn revision_stale_checks_emits_warn_with_remediation() {
        let probes = vec![AliasStoreProbe {
            alias: "myrepo".into(),
            store_path: PathBuf::from("/tmp/myrepo/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: vec![StaleRevision {
                    analyzer_id: "demo-analyzer".into(),
                    current_rev: Some(3),
                    expected_rev: 4,
                }],
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            }),
        }];
        let checks = revision_stale_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("demo-analyzer")
                && detail.contains("current=3")
                && detail.contains("expected=4"),
            "expected detail to surface analyzer + revisions, got: {detail}"
        );
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(
            remediation.contains("cairn ctl repo reindex myrepo"),
            "expected remediation to suggest reindex, got: {remediation}"
        );
    }

    /// When every analyzer's recorded revision matches the linked-in
    /// build's `revision()`, `revision_stale_checks` returns no checks
    /// at all (silent pass — drift surfaces only when there is drift).
    #[test]
    fn revision_stale_checks_silent_when_no_drift() {
        let probes = vec![AliasStoreProbe {
            alias: "clean".into(),
            store_path: PathBuf::from("/tmp/clean/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 1,
                    status: "succeeded".into(),
                    error: None,
                    started_at_ns: 0,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: vec!["demo-analyzer".into()],
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            }),
        }];
        let checks = revision_stale_checks(&probes);
        assert!(
            checks.is_empty(),
            "no drift should produce no checks; got {checks:#?}"
        );
    }

    /// Doctor parser-revision drift: groups by `(parser_id,
    /// current_rev)`. The detail string carries the per-group blob
    /// count so the operator can tell "12 blobs at rev 3" apart from
    /// "1 blob at rev 2" within the same parser.
    #[test]
    fn parser_revision_stale_checks_groups_by_parser_and_revision() {
        let probes = vec![AliasStoreProbe {
            alias: "myrepo".into(),
            store_path: PathBuf::from("/tmp/myrepo/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: vec![
                    ParserStaleRevision {
                        parser_id: "tree-sitter-kotlin".into(),
                        current_rev: Some(2),
                        expected_rev: 4,
                        affected_blob_count: 4,
                    },
                    ParserStaleRevision {
                        parser_id: "tree-sitter-kotlin".into(),
                        current_rev: Some(3),
                        expected_rev: 4,
                        affected_blob_count: 12,
                    },
                ],
                expected_analyzer_revisions: HashMap::new(),
            }),
        }];
        let checks = parser_revision_stale_checks(&probes);
        assert_eq!(checks.len(), 1);
        let check = &checks[0];
        assert_eq!(check.status, DoctorStatus::Warn);
        assert_eq!(check.name, "repo `myrepo` parser revision drift");
        let detail = check.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("current=2 (4 blobs)") && detail.contains("current=3 (12 blobs)"),
            "expected per-group blob counts in detail, got: {detail}"
        );
        let remediation = check.remediation.as_deref().unwrap_or("");
        assert!(
            remediation.contains("cairn ctl repo reindex myrepo"),
            "expected reindex remediation, got: {remediation}"
        );
    }

    /// Doctor parser-revision drift: a missing parsed row surfaces
    /// as `current=missing` (not omitted). The recovery action — full
    /// reindex — is the same as for a revision mismatch, and hiding
    /// the missing case would leave the operator blind to a state
    /// the scanner already enqueued recovery for.
    #[test]
    fn parser_revision_stale_checks_surfaces_missing_row_as_current_missing() {
        let probes = vec![AliasStoreProbe {
            alias: "gappy".into(),
            store_path: PathBuf::from("/tmp/gappy/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: vec![ParserStaleRevision {
                    parser_id: "tree-sitter-rust".into(),
                    current_rev: None,
                    expected_rev: 3,
                    affected_blob_count: 1,
                }],
                expected_analyzer_revisions: HashMap::new(),
            }),
        }];
        let checks = parser_revision_stale_checks(&probes);
        assert_eq!(checks.len(), 1);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("current=missing"),
            "missing row must surface as 'current=missing', got: {detail}"
        );
        assert!(
            detail.contains("(1 blob)"),
            "expected singular '1 blob' form, got: {detail}"
        );
    }

    /// Doctor parser-revision drift: empty `stale_parser_revisions`
    /// produces no checks (the common case — every expected parse
    /// unit is up to date).
    #[test]
    fn parser_revision_stale_checks_silent_when_no_drift() {
        let probes = vec![AliasStoreProbe {
            alias: "clean".into(),
            store_path: PathBuf::from("/tmp/clean/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: Vec::new(),
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            }),
        }];
        let checks = parser_revision_stale_checks(&probes);
        assert!(
            checks.is_empty(),
            "no parser drift should produce no checks; got {checks:#?}"
        );
    }

    // Analyzer rerun health checks cross-reference analyzer and parser
    // drift with `workspace_analysis_runs`, including mixed states
    // where one analyzer succeeded and another never ran or failed.

    fn analyzer_drift_probe(
        alias: &str,
        analyzer_id: &str,
        expected_rev: u32,
        run: Option<Tier3Run>,
    ) -> AliasStoreProbe {
        AliasStoreProbe {
            alias: alias.into(),
            store_path: PathBuf::from(format!("/tmp/{alias}/store.db")),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: run.into_iter().collect(),
                expected_tier3_analyzer_ids: vec![analyzer_id.into()],
                stale_revisions: vec![StaleRevision {
                    analyzer_id: analyzer_id.into(),
                    current_rev: Some(expected_rev.saturating_sub(1)),
                    expected_rev,
                }],
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            }),
        }
    }

    fn run_row(analyzer_id: &str, revision: u32, status: &str, error: Option<&str>) -> Tier3Run {
        Tier3Run {
            analyzer_id: analyzer_id.into(),
            manifest_id: 1,
            status: status.into(),
            error: error.map(str::to_string),
            analyzer_revision: revision,
            started_at_ns: 0,
        }
    }

    fn parser_drift_probe(
        alias: &str,
        analyzers: &[(&str, u32)], // (analyzer_id, expected_revision)
        runs: Vec<Tier3Run>,
    ) -> AliasStoreProbe {
        AliasStoreProbe {
            alias: alias.into(),
            store_path: PathBuf::from(format!("/tmp/{alias}/store.db")),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: runs,
                expected_tier3_analyzer_ids: analyzers
                    .iter()
                    .map(|(id, _)| (*id).to_string())
                    .collect(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: vec![ParserStaleRevision {
                    parser_id: "tree-sitter-kotlin".into(),
                    current_rev: Some(1),
                    expected_rev: 4,
                    affected_blob_count: 99,
                }],
                expected_analyzer_revisions: analyzers
                    .iter()
                    .map(|(id, rev)| ((*id).to_string(), *rev))
                    .collect(),
            }),
        }
    }

    /// Test 1: analyzer drift + run row at the current (expected)
    /// revision with status=succeeded → Case A `Fail`. This state
    /// is structurally impossible under the v0.7.0 invariants
    /// (`(manifest_id, analyzer_id)` PK + the stale-revision
    /// detector compares the single persisted row), so surfacing
    /// catches a future refactor that breaks the classifier. It is
    /// the analyzer-side counterpart of the parser-drift safety net.
    #[test]
    fn analyzer_drift_succeeded_at_current_revision_is_fail() {
        let probes = vec![analyzer_drift_probe(
            "moshi",
            "kotlin-resolver",
            5,
            Some(run_row("kotlin-resolver", 5, "succeeded", None)),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Fail);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("classifier") && detail.contains("invariant"),
            "Case A detail must call out the invariant break: {detail}"
        );
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("cairn ctl repo reindex moshi"));
        assert!(
            remediation.contains("older persisted state"),
            "Case A remediation must describe older persisted state without overstating a structural failure: {remediation}"
        );
        assert!(
            remediation.contains("structural bug") && remediation.contains("file an issue"),
            "Case A remediation must add the conditional structural-bug call-out: {remediation}"
        );
    }

    /// Test 2: analyzer drift + run row failed at current revision
    /// → Case B `Warn` with the underlying error message echoed.
    #[test]
    fn analyzer_drift_failed_at_current_revision_is_warn_with_error() {
        let probes = vec![analyzer_drift_probe(
            "moshi",
            "kotlin-resolver",
            5,
            Some(run_row(
                "kotlin-resolver",
                5,
                "failed",
                Some("kotlin-language-server died"),
            )),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(detail.contains("kotlin-language-server died"));
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("cairn ctl jobs list moshi"));
        assert!(remediation.contains("cairn ctl repo reindex moshi"));
    }

    /// Test 3: analyzer drift + run row queued at current revision
    /// → Case C `Pass`. Pending reruns are surfaced as informational
    /// so doctor output does not noisy-warn the operator while a
    /// rerun is on its way.
    #[test]
    fn analyzer_drift_queued_is_pass_pending() {
        let probes = vec![analyzer_drift_probe(
            "moshi",
            "kotlin-resolver",
            5,
            Some(run_row("kotlin-resolver", 5, "queued", None)),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Pass);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(detail.contains("pending"));
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("cairn ctl jobs list moshi"));
    }

    /// Test 4: analyzer drift + no run row at all → Case D `Warn`
    /// with the "enqueued / dropped / lost" framing and a daemon-log
    /// grep hint.
    #[test]
    fn analyzer_drift_no_run_row_is_warn_lost_or_dropped() {
        let probes = vec![analyzer_drift_probe("moshi", "kotlin-resolver", 5, None)];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("not enqueued")
                && detail.contains("dropped")
                && detail.contains("lost"),
            "Case D detail must enumerate the three failure modes: {detail}"
        );
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(
            remediation.contains("daemon log")
                && remediation.contains("staleness")
                && remediation.contains("cairn ctl repo reindex moshi"),
            "Case D remediation must include daemon-log grep hint plus manual reindex: {remediation}"
        );
    }

    /// Test 5: parser drift + every expected analyzer succeeded at
    /// the current revision → Case A `Fail`. This is the observability
    /// safety net for an analyzer chain that is green while the parser
    /// layer remains stale, which means the full-reindex chain broke
    /// somewhere between
    /// `enqueue_full_repo_reindex` and the pre-publication Tier-1 parse.
    #[test]
    fn parser_drift_all_analyzers_succeeded_is_fail_chain_bug() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            vec![
                run_row("kotlin-resolver", 5, "succeeded", None),
                run_row("jdtls-lsp", 1, "succeeded", None),
            ],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Fail);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("parser drift remains") && detail.contains("not updated"),
            "Case A parser-drift detail must call out the broken reindex-chain framing: {detail}"
        );
        assert!(detail.contains("tree-sitter-kotlin"));
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("cairn ctl repo reindex moshi"));
        assert!(remediation.contains("older persisted state"));
    }

    /// Test 6: parser drift + mixed analyzer states (one succeeded,
    /// one failed) → Case B `Warn` on the failure, not Case A on
    /// the succeeded slice. The failed
    /// analyzer's status / error must surface so the operator gets a
    /// targeted lead.
    #[test]
    fn parser_drift_mixed_succeeded_and_failed_is_warn_on_failed() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            vec![
                run_row("kotlin-resolver", 5, "succeeded", None),
                run_row("jdtls-lsp", 1, "failed", Some("jdtls oom")),
            ],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "mixed succeeded + failed must NOT be misclassified as Case A Fail"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(detail.contains("jdtls-lsp"));
        assert!(detail.contains("jdtls oom"));
    }

    /// Test 7: parser drift + mixed analyzer states (one succeeded,
    /// one queued) → Case C `Pass` pending.
    /// Same anti-misclassification invariant as test 6, this time on
    /// the queued / running path.
    #[test]
    fn parser_drift_mixed_succeeded_and_queued_is_pass_pending() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            vec![
                run_row("kotlin-resolver", 5, "succeeded", None),
                run_row("jdtls-lsp", 1, "running", None),
            ],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Pass);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(detail.contains("pending"));
    }

    /// Test 8: parser drift + every expected analyzer's run row is
    /// missing → Case D `Warn` with the lost-or-not-enqueued framing
    /// at alias level.
    #[test]
    fn parser_drift_no_run_rows_is_warn_lost_or_dropped() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            Vec::new(),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("no rerun row")
                && (detail.contains("not enqueued")
                    || detail.contains("dropped")
                    || detail.contains("lost")),
            "Case D detail must surface the lost-or-not-enqueued framing: {detail}"
        );
    }

    /// Test 9 (Case E): no drift on this alias produces zero
    /// `analyzer-rerun health` checks. The
    /// noise-prevention invariant: doctor must not warn on every
    /// alias just because the cross-reference function exists.
    #[test]
    fn no_drift_emits_no_rerun_health_check() {
        let probes = vec![AliasStoreProbe {
            alias: "clean".into(),
            store_path: PathBuf::from("/tmp/clean/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: vec![run_row("kotlin-resolver", 5, "succeeded", None)],
                expected_tier3_analyzer_ids: vec!["kotlin-resolver".into()],
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            }),
        }];
        let checks = analyzer_rerun_health_checks(&probes);
        assert!(
            checks.is_empty(),
            "clean alias must produce zero rerun-health checks; got {checks:#?}"
        );
    }

    /// Test 10: analyzer drift + run row succeeded at an OLD
    /// revision (the rerun never landed at the current revision) →
    /// Case D-like `Warn`. The detail message must distinguish this
    /// from "no row at all" by mentioning the persisted revision so
    /// the operator can tell whether the scanner attempted at all.
    #[test]
    fn analyzer_drift_succeeded_at_old_revision_is_warn_rerun_never_landed() {
        let probes = vec![analyzer_drift_probe(
            "moshi",
            "kotlin-resolver",
            5,
            Some(run_row("kotlin-resolver", 4, "succeeded", None)),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("revision 4") && detail.contains("expected revision 5"),
            "Case D-like detail must surface persisted vs expected revision so the operator can tell the scanner attempted but the rerun never landed: {detail}"
        );
    }

    /// Parser drift plus every analyzer row `succeeded` at an older
    /// revision than the current
    /// build expects → MUST surface as `Warn` (rerun never landed),
    /// NOT as the safety-net Case A `Fail`. The Case A framing
    /// implies "the parser-drift / full-reindex chain is broken,"
    /// which would be a doctor observability lie when the simpler
    /// explanation is that the analyzer-revision rerun was not
    /// enqueued or was lost before the worker landed it.
    ///
    /// Fixture: parser drift on `tree-sitter-kotlin`, both
    /// `kotlin-resolver` and `jdtls-lsp` are `succeeded` at the
    /// **older** revision (4 vs current 5 for kotlin-resolver, and
    /// 0 vs current 1 for jdtls-lsp). The check must classify this
    /// as the Case D-like "rerun never landed" Warn, mention the
    /// stale analyzer's persisted-vs-expected revision in the
    /// detail, and steer the operator at the daemon log + manual
    /// reindex.
    #[test]
    fn parser_drift_all_succeeded_at_old_revision_is_warn_not_case_a_fail() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            vec![
                run_row("kotlin-resolver", 4, "succeeded", None),
                run_row("jdtls-lsp", 0, "succeeded", None),
            ],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "succeeded-at-old-revision must NOT be misclassified as Case A Fail"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("never landed"),
            "detail must use the 'rerun never landed' framing rather than the Case A chain-bug framing: {detail}"
        );
        assert!(
            detail.contains("revision 4") && detail.contains("expected revision 5"),
            "detail must surface the persisted-vs-expected revision so the operator can confirm the analyzer is stale: {detail}"
        );
        assert!(
            !detail.contains("chain is broken"),
            "Case A chain-bug language must NOT appear when the explanation is simply that the analyzer rerun has not landed yet: {detail}"
        );
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("daemon log") && remediation.contains("staleness"));
        assert!(remediation.contains("cairn ctl repo reindex moshi"));
    }

    /// The same probe carries `stale_revisions` (analyzer drift on
    /// `kotlin-resolver`) AND `stale_parser_revisions` (parser
    /// drift). The two checks the helper emits must classify
    /// correctly side-by-side:
    ///
    ///   - the analyzer-side check must surface Case D-like Warn
    ///     ("rerun never landed at current revision"),
    ///   - the parser-side check must surface Warn (NOT Case A
    ///     Fail), because the stale analyzer prevents the safety-net
    ///     framing from kicking in.
    #[test]
    fn cross_emission_analyzer_drift_and_parser_drift_both_warn() {
        let probes = vec![AliasStoreProbe {
            alias: "moshi".into(),
            store_path: PathBuf::from("/tmp/moshi/store.db"),
            worktree_registered: true,
            result: Ok(AliasStoreState {
                tentative_manifest_id: Some(1),
                tier3_runs: vec![
                    run_row("kotlin-resolver", 4, "succeeded", None),
                    run_row("jdtls-lsp", 1, "succeeded", None),
                ],
                expected_tier3_analyzer_ids: vec!["kotlin-resolver".into(), "jdtls-lsp".into()],
                stale_revisions: vec![StaleRevision {
                    analyzer_id: "kotlin-resolver".into(),
                    current_rev: Some(4),
                    expected_rev: 5,
                }],
                stale_parser_revisions: vec![ParserStaleRevision {
                    parser_id: "tree-sitter-kotlin".into(),
                    current_rev: Some(1),
                    expected_rev: 4,
                    affected_blob_count: 99,
                }],
                expected_analyzer_revisions: [
                    ("kotlin-resolver".to_string(), 5),
                    ("jdtls-lsp".to_string(), 1),
                ]
                .into_iter()
                .collect(),
            }),
        }];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(
            checks.len(),
            2,
            "one analyzer-side check + one parser-side check"
        );

        // Find the parser-drift check by name.
        let parser_check = checks
            .iter()
            .find(|c| c.name.contains("parser drift rerun health"))
            .expect("parser drift rerun health check must be emitted");
        assert_eq!(
            parser_check.status,
            DoctorStatus::Warn,
            "parser-drift side must NOT misclassify as Case A Fail when an analyzer is stale"
        );
        let parser_detail = parser_check.detail.as_deref().unwrap_or("");
        assert!(
            parser_detail.contains("never landed"),
            "parser-drift detail must use the 'never landed' framing, not the chain-bug framing: {parser_detail}"
        );
        assert!(
            !parser_detail.contains("chain is broken"),
            "Case A chain-bug language must NOT appear: {parser_detail}"
        );

        // Find the analyzer-side check by name.
        let analyzer_check = checks
            .iter()
            .find(|c| c.name.contains("analyzer `kotlin-resolver` rerun health"))
            .expect("analyzer-side rerun health check must be emitted");
        assert_eq!(analyzer_check.status, DoctorStatus::Warn);
        let analyzer_detail = analyzer_check.detail.as_deref().unwrap_or("");
        assert!(
            analyzer_detail.contains("revision 4")
                && analyzer_detail.contains("expected revision 5"),
            "analyzer-side detail must include the persisted-vs-expected revision: {analyzer_detail}"
        );
    }

    /// MA-1: a `running` row whose `started_at_ns` is older than
    /// `STUCK_RUN_THRESHOLD` (6h) MUST surface as `Warn` with an
    /// explicit "stuck" framing, not as the routine "indexing in
    /// progress" message. The remediation MUST nudge the operator
    /// toward `reindex_repo` (a wedged worker recovers via re-queue).
    #[test]
    fn tier3_run_check_warns_stuck_run_after_6h_running() {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        // 7 hours ago — well past the 6h threshold.
        let stuck_started_at = now_ns - (7 * 3600 * 1_000_000_000);
        let stuck = tier3_run_check(
            "wedged",
            &AliasStoreState {
                tentative_manifest_id: Some(9),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 9,
                    status: "running".into(),
                    error: None,
                    started_at_ns: stuck_started_at,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        assert_eq!(stuck.status, DoctorStatus::Warn);
        let detail = stuck.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("stuck") && detail.contains("running"),
            "expected stuck framing in detail, got: {detail}"
        );
        let remediation = stuck.remediation.as_deref().unwrap_or("");
        assert!(
            remediation.contains("reindex wedged"),
            "expected remediation to nudge reindex, got: {remediation}"
        );
    }

    /// MA-1 sibling: a `queued` row whose `started_at_ns` is older than
    /// `STUCK_RUN_THRESHOLD` MUST also surface as `Warn` with the
    /// "stuck" framing — the worker that picks up the row may be
    /// blocked behind a pool-group quota, deadlocked, or never wake
    /// up. queued and running share the same branch in
    /// `tier3_run_check`; this test pins that the queued status is
    /// not silently dropped by the threshold check.
    #[test]
    fn tier3_run_check_warns_stuck_run_after_6h_queued() {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let stuck_started_at = now_ns - (7 * 3600 * 1_000_000_000);
        let stuck = tier3_run_check(
            "wedged-queue",
            &AliasStoreState {
                tentative_manifest_id: Some(11),
                tier3_runs: vec![Tier3Run {
                    analyzer_id: "demo-analyzer".into(),
                    manifest_id: 11,
                    status: "queued".into(),
                    error: None,
                    started_at_ns: stuck_started_at,
                    analyzer_revision: 1,
                }],
                expected_tier3_analyzer_ids: Vec::new(),
                stale_revisions: Vec::new(),
                stale_parser_revisions: Vec::new(),
                expected_analyzer_revisions: HashMap::new(),
            },
        );
        assert_eq!(stuck.status, DoctorStatus::Warn);
        let detail = stuck.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("stuck") && detail.contains("queued"),
            "expected stuck framing for queued row, got: {detail}"
        );
    }

    // Stale-pending regression coverage.
    //
    // Treating every `queued` /
    // `running` row as an in-flight current rerun and returned Pass.
    // `JobManager::enqueue_analyzer_run` stamps the current
    // `analyzer_revision` on enqueue, so a pending row at an OLDER
    // revision is NOT an in-flight current rerun — it is a stuck
    // old-binary row, a `restore_from_db` artifact, or a coalesced
    // enqueue. These tests pin that stale analyzer and parser rows
    // surface Warn, and that mixed parser-drift states
    // (pending-current plus missing-row or stale-succeeded) are not
    // masked by the Case C Pass.

    /// Analyzer drift plus `queued` at a stale revision must be
    /// `Warn`. Treating `("queued" | "running", _)` as an
    /// unconditional Pass hides stuck old-binary rows.
    #[test]
    fn analyzer_drift_queued_at_stale_revision_is_warn_not_pass() {
        let probes = vec![analyzer_drift_probe(
            "moshi",
            "kotlin-resolver",
            5,
            Some(run_row("kotlin-resolver", 4, "queued", None)),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "queued at stale revision must NOT be Pass (rerun will land on its own would be a lie)"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("stale revision")
                && detail.contains("revision 4")
                && detail.contains("expected 5"),
            "detail must surface persisted-vs-expected revision so the operator can see the row is stuck at an old revision: {detail}"
        );
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("cairn ctl jobs list moshi"));
        assert!(remediation.contains("cairn ctl repo reindex moshi"));
    }

    /// Analyzer drift plus `running` at a stale revision must be
    /// `Warn` because queued and running share this branch.
    #[test]
    fn analyzer_drift_running_at_stale_revision_is_warn_not_pass() {
        let probes = vec![analyzer_drift_probe(
            "moshi",
            "kotlin-resolver",
            5,
            Some(run_row("kotlin-resolver", 4, "running", None)),
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "running at stale revision must NOT be Pass"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("running") && detail.contains("stale revision"),
            "detail must mark the running row as stale: {detail}"
        );
    }

    /// In the parser-drift cascade, one analyzer queued at the current
    /// revision plus another analyzer with no row must be
    /// `Warn`. The prior cascade returned Case C Pass on `any_pending`
    /// before checking `any_row_missing`, masking the missing-row
    /// failure mode.
    #[test]
    fn parser_drift_pending_plus_missing_row_is_warn_not_pass() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            vec![run_row("kotlin-resolver", 5, "queued", None)],
            // jdtls-lsp row intentionally absent.
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "pending-current + missing-row must NOT be masked by Case C Pass"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("no rerun row"),
            "detail must surface the missing-row mode rather than the pending Pass framing: {detail}"
        );
    }

    /// Pending-current plus another analyzer succeeded at an old
    /// revision must be `Warn`. A pending-first cascade would return
    /// Case C Pass on `any_pending` before checking
    /// `any_stale_succeeded`, masking the "rerun never landed" mode.
    #[test]
    fn parser_drift_pending_plus_stale_succeeded_is_warn_not_pass() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5), ("jdtls-lsp", 1)],
            vec![
                run_row("kotlin-resolver", 5, "queued", None),
                run_row("jdtls-lsp", 0, "succeeded", None),
            ],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "pending-current + stale-succeeded must NOT be masked by Case C Pass"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("never landed"),
            "detail must use the 'never landed' framing for the stale-succeeded analyzer: {detail}"
        );
        assert!(
            detail.contains("jdtls-lsp"),
            "detail must name the stale analyzer so the operator can target it: {detail}"
        );
    }

    /// A single analyzer `queued` at an old revision (with no other
    /// analyzers) must be `Warn`. A broad queued/running
    /// branch did not split on `analyzer_revision == expected_rev`
    /// and would have returned Case C Pass.
    #[test]
    fn parser_drift_queued_at_stale_revision_is_warn_not_pass() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5)],
            vec![run_row("kotlin-resolver", 4, "queued", None)],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].status,
            DoctorStatus::Warn,
            "queued at stale revision must NOT be Case C Pass"
        );
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("stale revision")
                && detail.contains("revision 4")
                && detail.contains("expected 5"),
            "detail must surface the stale pending row's persisted-vs-expected revision: {detail}"
        );
    }

    #[test]
    fn parser_drift_unknown_analyzer_status_is_warn_not_pass() {
        let probes = vec![parser_drift_probe(
            "moshi",
            &[("kotlin-resolver", 5)],
            vec![run_row("kotlin-resolver", 5, "paused", None)],
        )];
        let checks = analyzer_rerun_health_checks(&probes);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, DoctorStatus::Warn);
        let detail = checks[0].detail.as_deref().unwrap_or("");
        assert!(detail.contains("kotlin-resolver") && detail.contains("paused"));
    }
}
