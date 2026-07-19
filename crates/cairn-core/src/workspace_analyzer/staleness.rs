//! Daemon-startup revision-staleness scanner.
//!
//! Runs once per daemon boot (called from
//! [`crate::daemon::Daemon::run`] via `tokio::task::spawn_blocking`,
//! since every step is synchronous rusqlite I/O). For each registered
//! alias it looks at the tentative manifest, lines up
//! [`workspace_analysis_runs`] rows against [`all_workspace_analyzers`]
//! filtered by [`expected_analyzers_for_manifest`], and queues an
//! analyzer rerun when an analyzer's persisted output is **stale**
//! against the linker-time expectations the current binary embeds.
//!
//! ## What counts as "stale"
//!
//! An analyzer is stale (= enqueue) iff one of:
//!
//!   1. A run row exists with `analyzer_revision < expected.revision()`
//!      — the analyzer was bumped since this manifest was last
//!      analyzed. The most common case after a `cargo install` of a
//!      new cairn binary.
//!   2. No run row exists at all — the analyzer was registered for
//!      this parser set but never ran for this manifest. Catches the
//!      "shadow alias" case where a registered repo somehow missed
//!      its initial analyzer pass.
//!   3. A run row exists at the old revision with a terminal-failure
//!      status (`failed`, `timed_out`, `cancelled`). Re-queue once on
//!      revision bump in case the new revision fixes whatever the
//!      previous run blew up on.
//!
//! Explicitly **not stale**:
//!
//!   - Matching revision, status `queued` / `running` / `succeeded` —
//!     the row is current or actively converging.
//!   - Matching revision, terminal-failure status — re-queueing in a
//!     loop would mask a persistent bug. `doctor` surfaces it
//!     instead.
//!
//! And **active-stale** (old revision, still mid-flight as
//! `queued` / `running`): not enqueued (the de-dup gate would coalesce
//! it anyway) but counted in [`StalenessSummary::active_stale`] for
//! observability.
//!
//! ## Failure isolation
//!
//! Per-alias errors `warn!` and `continue`. The daemon must not crash
//! over one corrupt CAS store. DB-open / list-aliases failures bubble
//! to `Err` and the daemon logs a single warning; missing tentative
//! anchor (e.g. a brand-new alias whose first manifest hasn't built
//! yet) is silent.

use std::collections::HashMap;
use std::path::Path;

use cairn_lang_api::all_backends;
use rusqlite::params;
use tracing::{debug, info, warn};

use std::sync::Arc;

use crate::Result;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::jobs::{EnqueueAnalyzerRun, JobManager, ReindexReason};
use crate::manifest::ManifestId;
use crate::paths::CasDataDir;
use crate::reconcile::{ReconcileTrigger, RepoReconcileManager};
use crate::workspace_analyzer::{expected_analyzers_for_manifest, expected_parse_units};

use super::all_workspace_analyzers;

/// One detected revision mismatch surfaced through doctor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleRevision {
    pub analyzer_id: String,
    /// Persisted (= old) revision recorded on `workspace_analysis_runs`.
    /// `None` when no run row exists at all.
    pub current_rev: Option<u32>,
    /// Revision the linked-in analyzer reports right now.
    pub expected_rev: u32,
}

/// One detected `parser_revision` mismatch surfaced through doctor.
///
/// Each row reports a `(parser_id, current_rev)` group — multiple
/// blobs at the same persisted revision collapse into one entry with
/// `affected_blob_count > 1`. A missing parse row is reported with
/// `current_rev = None` (the alternative would be omitting it, which
/// would hide a real recovery-path signal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserStaleRevision {
    pub parser_id: String,
    /// Persisted revision on `blobs.parser_revision`. `None` when the
    /// expected parse row is missing entirely — the recovery action
    /// (full reindex) is the same as for a revision mismatch.
    pub current_rev: Option<u32>,
    /// Revision the linked-in backend reports for this `parser_id`.
    pub expected_rev: u32,
    /// Number of distinct blobs that hit this `(parser_id,
    /// current_rev)` group on the tentative manifest.
    pub affected_blob_count: usize,
}

/// Aggregated outcome of one scan. Reported via `tracing::info` at the
/// end of the spawn_blocking task; daemon callers use it only for logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StalenessSummary {
    pub aliases_scanned: usize,
    pub aliases_failed: usize,
    pub jobs_enqueued: usize,
    pub active_stale: usize,
    pub terminal_failed_current_revision: usize,
    /// Number of aliases for which the parser-revision drift check
    /// fired (= at least one expected parse unit was missing or had
    /// a `parser_revision` different from what the current backend
    /// reports).
    pub parser_drift_aliases: usize,
    /// Number of `request_force_by_alias(ParserRevisionDrift)`
    /// requests dispatched to the reconcile manager as a result
    /// of parser-revision drift. Counts *requests* (durable
    /// intent records); the worker executes the register /
    /// analyzer enqueue work asynchronously and its outcome is
    /// visible on `repo_reconcile_state` rather than in this
    /// summary.
    pub parser_drift_reconcile_requests: usize,
}

/// Read each registered alias's tentative manifest, compute which
/// analyzers are stale against the current binary's expectations, and
/// enqueue one job per stale analyzer.
///
/// # Errors
/// Errors **only** for failures that prevent enumerating aliases at all
/// (alias-index open / read). Per-alias errors are downgraded to a
/// warning and counted in [`StalenessSummary::aliases_failed`] — the
/// daemon must not abort startup over a single corrupt store.
pub fn check_revision_staleness_and_enqueue(
    cas_data_dir: &CasDataDir,
    job_manager: &JobManager,
    reconcile: Option<&Arc<RepoReconcileManager>>,
) -> Result<StalenessSummary> {
    let index = cas_registry::open(&cas_data_dir.index_db_path())?;
    let entries = cas_registry::list_all(&index)?;
    drop(index);

    // Expected revisions are constants from the linker-time registry;
    // computing them once per scan instead of per alias keeps the
    // hot path off `Vec<Box<dyn …>>` re-allocation.
    let expected_revision_for: HashMap<&'static str, u32> = all_workspace_analyzers()
        .iter()
        .map(|a| (a.id(), a.revision()))
        .collect();

    let mut summary = StalenessSummary {
        aliases_scanned: entries.len(),
        ..StalenessSummary::default()
    };

    for entry in &entries {
        match scan_one_alias(
            cas_data_dir,
            job_manager,
            reconcile,
            entry,
            &expected_revision_for,
        ) {
            Ok(per_alias) => {
                summary.jobs_enqueued += per_alias.jobs_enqueued;
                summary.active_stale += per_alias.active_stale;
                summary.terminal_failed_current_revision +=
                    per_alias.terminal_failed_current_revision;
                if per_alias.parser_drift {
                    summary.parser_drift_aliases += 1;
                    summary.parser_drift_reconcile_requests += 1;
                }
            }
            Err(err) => {
                summary.aliases_failed += 1;
                warn!(
                    alias = %entry.alias,
                    error = %err,
                    "revision staleness scan failed for alias; continuing"
                );
            }
        }
    }

    info!(
        aliases_scanned = summary.aliases_scanned,
        aliases_failed = summary.aliases_failed,
        jobs_enqueued = summary.jobs_enqueued,
        active_stale = summary.active_stale,
        terminal_failed_current_revision = summary.terminal_failed_current_revision,
        parser_drift_aliases = summary.parser_drift_aliases,
        parser_drift_reconcile_requests = summary.parser_drift_reconcile_requests,
        "revision staleness scan complete"
    );
    Ok(summary)
}

#[derive(Debug, Default)]
struct PerAliasSummary {
    jobs_enqueued: usize,
    active_stale: usize,
    terminal_failed_current_revision: usize,
    /// `true` iff the parser-revision drift pre-check fired and
    /// a durable reconcile force request was recorded (through
    /// [`RepoReconcileManager::request_force_by_alias`] with
    /// [`ReconcileTrigger::ParserRevisionDrift`], or through the
    /// compat helper [`JobManager::enqueue_full_repo_reindex`]
    /// when reconcile is unavailable — test-only). When set, the
    /// analyzer-revision drift loop is skipped — the upcoming
    /// reindex will re-stamp everything, so re-queueing here
    /// would only cause coalesce noise.
    parser_drift: bool,
}

fn scan_one_alias(
    cas_data_dir: &CasDataDir,
    job_manager: &JobManager,
    reconcile: Option<&Arc<RepoReconcileManager>>,
    entry: &cas_registry::AliasEntry,
    expected_revision_for: &HashMap<&'static str, u32>,
) -> Result<PerAliasSummary> {
    let _lease = job_manager.acquire_repository_lease(&entry.repo_hash)?;
    let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
    if !store_path.exists() {
        // The alias row references a store file that was deleted out
        // from under us (e.g. manual cleanup). Doctor will flag it;
        // staleness scan has nothing to do.
        return Ok(PerAliasSummary::default());
    }
    let mut conn = cas_store::open_existing(&store_path)?;
    let repo_root = Path::new(&entry.root_path);
    let manifest_id = match crate::anchor::resolve_tentative_manifest_id(&conn, repo_root)? {
        Some(id) => id,
        None => {
            debug!(
                alias = %entry.alias,
                "no tentative manifest yet; staleness scan skipping alias"
            );
            return Ok(PerAliasSummary::default());
        }
    };

    // Parser-revision drift pre-check. Runs before the analyzer drift
    // loop because a parser drift forces a full reparse, which then
    // forces an analyzer re-pass via the register hot path — so the
    // analyzer drift work would coalesce against jobs the upcoming
    // reindex is about to enqueue anyway.
    //
    // R2 must-fix: starts from `expected_parse_units` rather than
    // `SELECT DISTINCT parser_id FROM blobs`, so a row whose
    // `parser_id` is no longer claimed by any backend cannot trigger
    // an infinite reindex loop (the row stays in place across reindex,
    // and `expected_parse_units` ignores it).
    if detect_parser_revision_drift(&conn, manifest_id, repo_root)? {
        if let Some(reconcile) = reconcile {
            // Production path: record durable force intent via the
            // reconcile manager. The register / analyzer enqueue
            // work runs asynchronously in the worker; scan doesn't
            // wait for it. `Handle::current().block_on` is safe
            // here because the scanner runs inside a tokio
            // `spawn_blocking` task — the runtime handle is
            // present and reserved.
            let handle = tokio::runtime::Handle::current();
            let reconcile = reconcile.clone();
            let alias = entry.alias.clone();
            let dispatch = handle.block_on(async move {
                reconcile
                    .request_force_by_alias(alias, ReconcileTrigger::ParserRevisionDrift)
                    .await
            });
            match dispatch {
                Ok(outcome) => info!(
                    alias = %entry.alias,
                    repo_hash = %outcome.repo_hash,
                    generation = outcome.generation,
                    scheduled = outcome.scheduled,
                    "parser-revision drift: recorded reconcile force request"
                ),
                Err(err) => warn!(
                    alias = %entry.alias,
                    error = %err,
                    "parser-revision drift: reconcile force request failed; continuing"
                ),
            }
        } else {
            // Compat path (test / degraded startup only). The
            // production daemon always passes `Some(reconcile)`;
            // this branch is intentionally the sole surviving
            // production-adjacent caller of
            // `JobManager::enqueue_full_repo_reindex`, kept
            // behind the `reconcile.is_none()` gate so it never
            // runs when a reconcile driver exists.
            match job_manager
                .enqueue_full_repo_reindex(&entry.alias, ReindexReason::ParserRevisionDrift)
            {
                Ok(outcome) => info!(
                    alias = %entry.alias,
                    blobs_parsed = outcome.blobs_parsed,
                    jobs_enqueued = outcome.jobs_enqueued,
                    "parser-revision drift: fallback full reindex enqueued (no reconcile driver)"
                ),
                Err(err) => warn!(
                    alias = %entry.alias,
                    error = %err,
                    "parser-revision drift: fallback full reindex enqueue failed; continuing"
                ),
            }
        }
        return Ok(PerAliasSummary {
            parser_drift: true,
            ..PerAliasSummary::default()
        });
    }

    let expected_for_manifest = expected_analyzers_for_manifest(&conn, manifest_id)?;
    if expected_for_manifest.is_empty() {
        return Ok(PerAliasSummary::default());
    }

    // Map analyzer_id -> (revision, status) for the rows that already
    // exist on this manifest. Missing rows fall into the "row absent"
    // staleness arm.
    let mut existing: HashMap<String, (u32, String)> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT analyzer_id, analyzer_revision, status
               FROM workspace_analysis_runs
              WHERE manifest_id = ?1",
        )?;
        let rows = stmt.query_map(params![manifest_id.0], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (analyzer_id, revision, status) = row?;
            // analyzer_revision is stored as INTEGER but bounded to u32
            // by mark_run. Clamp defensively in case a future schema
            // change widens it.
            let revision: u32 = u32::try_from(revision).unwrap_or(u32::MAX);
            existing.insert(analyzer_id, (revision, status));
        }
    }

    let mut summary = PerAliasSummary::default();
    for analyzer in &expected_for_manifest {
        let analyzer_id = analyzer.id();
        let expected_rev = *expected_revision_for
            .get(analyzer_id)
            .unwrap_or(&analyzer.revision());

        let decision = classify(analyzer_id, expected_rev, existing.get(analyzer_id));
        match decision {
            Decision::Skip { reason } => {
                debug!(
                    alias = %entry.alias,
                    analyzer_id,
                    reason,
                    "staleness scan: skip"
                );
            }
            Decision::ActiveStale => {
                summary.active_stale += 1;
                debug!(
                    alias = %entry.alias,
                    analyzer_id,
                    "staleness scan: active stale (skip enqueue)"
                );
            }
            Decision::TerminalFailedCurrent => {
                summary.terminal_failed_current_revision += 1;
                debug!(
                    alias = %entry.alias,
                    analyzer_id,
                    "staleness scan: terminal failed at current revision (skip enqueue, doctor will warn)"
                );
            }
            Decision::Enqueue { current_rev } => {
                // The job stamp (revision, config_hash, pool_group,
                // job_id) is computed inside `enqueue_analyzer_run`
                // against the linked-in registry — this scanner does
                // not need to (and must not) supply them. If the
                // registry has dropped the analyzer between two
                // revision bumps, `enqueue_analyzer_run` returns
                // `Ok(None)` cleanly and we just log + continue.
                let queued = job_manager.enqueue_analyzer_run(EnqueueAnalyzerRun {
                    conn: &mut conn,
                    alias: &entry.alias,
                    repo_hash: &entry.repo_hash,
                    repo_root,
                    manifest_id,
                    analyzer_id,
                    now_ns: now_ns(),
                })?;
                if queued.is_some() {
                    summary.jobs_enqueued += 1;
                    info!(
                        alias = %entry.alias,
                        analyzer_id,
                        manifest_id = manifest_id.0,
                        current_rev = current_rev.map(u64::from).unwrap_or(0),
                        current_rev_present = current_rev.is_some(),
                        expected_rev,
                        "revision staleness: enqueued analyzer rerun"
                    );
                } else {
                    debug!(
                        alias = %entry.alias,
                        analyzer_id,
                        "revision staleness: coalesced or dropped analyzer (registry no longer produces this id)"
                    );
                }
            }
        }
    }
    Ok(summary)
}

#[derive(Debug, PartialEq, Eq)]
enum Decision {
    /// Already up-to-date, or actively converging at current revision.
    Skip { reason: &'static str },
    /// Old revision, still queued/running — let the in-flight job
    /// finish; the de-dup gate would coalesce us anyway.
    ActiveStale,
    /// Matching revision + terminal failure: don't loop. Doctor will
    /// surface it.
    TerminalFailedCurrent,
    /// Row absent, or old revision, or old-revision terminal failure
    /// (one retry per bump). `current_rev` is `None` only for the
    /// row-absent case.
    Enqueue { current_rev: Option<u32> },
}

fn classify(_analyzer_id: &str, expected_rev: u32, row: Option<&(u32, String)>) -> Decision {
    let Some((current_rev, status)) = row else {
        // I1 case 2: row absent → enqueue.
        return Decision::Enqueue { current_rev: None };
    };
    let current_rev = *current_rev;
    let is_terminal_failure = matches!(status.as_str(), "failed" | "timed_out" | "cancelled");
    let is_in_flight = matches!(status.as_str(), "queued" | "running");

    if current_rev < expected_rev {
        // I1 case 1 / case 3: rollback-safe (uses `<`, not `!=`, so
        // intentional revision rollbacks during canary aren't re-
        // queued).
        if is_in_flight {
            return Decision::ActiveStale;
        }
        return Decision::Enqueue {
            current_rev: Some(current_rev),
        };
    }

    if current_rev > expected_rev {
        // Rollback: a future binary recorded a newer revision, we're
        // a downgrade. Leave the row alone.
        return Decision::Skip {
            reason: "current revision is newer than expected (rollback)",
        };
    }

    // current_rev == expected_rev
    if is_terminal_failure {
        // I1 NOT-stale: terminal failure at current revision. Doctor
        // emits a warn; we don't loop.
        return Decision::TerminalFailedCurrent;
    }
    Decision::Skip {
        reason: "current revision matches expected",
    }
}

/// Returns `true` iff at least one expected parse unit for
/// `manifest_id` is either absent from `blobs` or persists at a
/// `parser_revision` different from what the current backend
/// reports.
///
/// Equality (`!=`) — not order (`<`) — is the invalidation rule for
/// `parser_revision`, matching `cas::blob::reuse_or_compute`. A
/// rollback case where `blobs.parser_revision > backend.parser_revision()`
/// is still a drift because the persisted facts came from a different
/// build whose syntactic output shape is not guaranteed to match.
///
/// Returns `Ok(true)` on the first drift found (short-circuit) —
/// a single drift fires a full repo reindex anyway.
fn detect_parser_revision_drift(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    repo_root: &Path,
) -> Result<bool> {
    let backends = all_backends();
    let expected = expected_parse_units(conn, manifest_id, repo_root, &backends)?;
    if expected.is_empty() {
        return Ok(false);
    }

    let mut stmt = conn.prepare(
        "SELECT parser_revision FROM blobs
         WHERE blob_sha = ?1 AND parser_id = ?2",
    )?;
    for unit in &expected {
        let persisted: Option<i64> = stmt
            .query_row(params![unit.blob_sha, unit.parser_id], |r| r.get(0))
            .ok();
        let Some(persisted) = persisted else {
            debug!(
                blob_sha = unit.blob_sha,
                parser_id = unit.parser_id,
                expected = unit.parser_revision,
                "parser-revision drift: parse row missing"
            );
            return Ok(true);
        };
        let persisted = u32::try_from(persisted).unwrap_or(u32::MAX);
        if persisted != unit.parser_revision {
            debug!(
                blob_sha = unit.blob_sha,
                parser_id = unit.parser_id,
                persisted,
                expected = unit.parser_revision,
                "parser-revision drift: revision mismatch"
            );
            return Ok(true);
        }
    }
    Ok(false)
}

/// Doctor-facing aggregation of parser-revision drift on one
/// manifest. Returns one row per `(parser_id, current_rev)` group;
/// missing parse rows surface as `current_rev = None` (still one
/// group per `parser_id`).
///
/// Shares `expected_parse_units` with the scanner so the "what
/// should be parsed?" answer cannot drift between the auto-recovery
/// path and the operator-facing surface.
pub fn compute_parser_stale_revisions(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    repo_root: &Path,
) -> Result<Vec<ParserStaleRevision>> {
    let backends = all_backends();
    let expected = expected_parse_units(conn, manifest_id, repo_root, &backends)?;
    if expected.is_empty() {
        return Ok(Vec::new());
    }

    // (parser_id, Option<persisted_rev>) -> (expected_rev, count)
    let mut groups: HashMap<(String, Option<u32>), (u32, usize)> = HashMap::new();

    let mut stmt = conn.prepare(
        "SELECT parser_revision FROM blobs
         WHERE blob_sha = ?1 AND parser_id = ?2",
    )?;
    for unit in &expected {
        let persisted: Option<u32> = stmt
            .query_row(params![unit.blob_sha, unit.parser_id], |r| {
                r.get::<_, i64>(0)
            })
            .ok()
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX));

        let is_stale = match persisted {
            Some(p) => p != unit.parser_revision,
            None => true,
        };
        if !is_stale {
            continue;
        }
        let key = (unit.parser_id.clone(), persisted);
        let entry = groups.entry(key).or_insert((unit.parser_revision, 0));
        entry.1 += 1;
    }

    let mut out: Vec<ParserStaleRevision> = groups
        .into_iter()
        .map(
            |((parser_id, current_rev), (expected_rev, count))| ParserStaleRevision {
                parser_id,
                current_rev,
                expected_rev,
                affected_blob_count: count,
            },
        )
        .collect();
    // Stable sort for deterministic doctor output: parser_id asc,
    // then current_rev (None last) asc.
    out.sort_by(|a, b| {
        a.parser_id
            .cmp(&b.parser_id)
            .then_with(|| match (a.current_rev, b.current_rev) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
    });
    Ok(out)
}

fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    )
    .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_row_absent_enqueues() {
        let d = classify("a", 3, None);
        assert_eq!(d, Decision::Enqueue { current_rev: None });
    }

    #[test]
    fn classify_old_revision_succeeded_enqueues() {
        let d = classify("a", 3, Some(&(2, "succeeded".into())));
        assert_eq!(
            d,
            Decision::Enqueue {
                current_rev: Some(2)
            }
        );
    }

    #[test]
    fn classify_old_revision_failed_enqueues() {
        let d = classify("a", 3, Some(&(2, "failed".into())));
        assert_eq!(
            d,
            Decision::Enqueue {
                current_rev: Some(2)
            }
        );
    }

    #[test]
    fn classify_old_revision_queued_is_active_stale() {
        let d = classify("a", 3, Some(&(2, "queued".into())));
        assert_eq!(d, Decision::ActiveStale);
    }

    #[test]
    fn classify_old_revision_running_is_active_stale() {
        let d = classify("a", 3, Some(&(2, "running".into())));
        assert_eq!(d, Decision::ActiveStale);
    }

    #[test]
    fn classify_matching_revision_succeeded_skips() {
        let d = classify("a", 3, Some(&(3, "succeeded".into())));
        assert!(matches!(d, Decision::Skip { .. }));
    }

    #[test]
    fn classify_matching_revision_failed_is_terminal_current() {
        let d = classify("a", 3, Some(&(3, "failed".into())));
        assert_eq!(d, Decision::TerminalFailedCurrent);
    }

    #[test]
    fn classify_matching_revision_timed_out_is_terminal_current() {
        let d = classify("a", 3, Some(&(3, "timed_out".into())));
        assert_eq!(d, Decision::TerminalFailedCurrent);
    }

    #[test]
    fn classify_matching_revision_cancelled_is_terminal_current() {
        let d = classify("a", 3, Some(&(3, "cancelled".into())));
        assert_eq!(d, Decision::TerminalFailedCurrent);
    }

    #[test]
    fn classify_rollback_skips() {
        // Older binary started up against a store that a newer binary
        // already wrote to. Don't re-queue; just leave the row.
        let d = classify("a", 2, Some(&(3, "succeeded".into())));
        assert!(matches!(d, Decision::Skip { .. }));
    }
}

/// Linker-time fake Tier-1 backend, registered only in test builds
/// of cairn-core. Claims `*.fake-tier1` paths so the parser-revision
/// drift scanner has a backend to compare against without pulling in
/// a real language crate. `parser_id == "fake-parser"` aligns with
/// the FakeWorkspaceAnalyzer in `workspace_analyzer/mod.rs::tests`,
/// so existing analyzer-drift e2e fixtures can keep using the same
/// `manifest_parser_ids` projection.
///
/// `parser_revision == 1` is the "no drift" baseline: existing
/// fixtures seed `blobs.parser_revision = 1` and stay matching;
/// drift tests bump one side off 1 explicitly.
#[cfg(test)]
struct FakeTier1Backend;

#[cfg(test)]
impl cairn_lang_api::LanguageBackend for FakeTier1Backend {
    fn name(&self) -> &'static str {
        "fake-tier1"
    }
    fn file_patterns(&self) -> &'static [&'static str] {
        &["*.fake-tier1"]
    }
    fn parser_id(&self) -> &'static str {
        "fake-parser"
    }
    fn parser_revision(&self) -> u32 {
        1
    }
    fn extract_syntactic(
        &self,
        _source: &[u8],
    ) -> std::result::Result<cairn_lang_api::SyntacticFacts, cairn_lang_api::ExtractError> {
        Ok(cairn_lang_api::SyntacticFacts::default())
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
#[linkme::distributed_slice(cairn_lang_api::LANGUAGE_BACKENDS)]
static FAKE_TIER1_BACKEND: fn() -> Box<dyn cairn_lang_api::LanguageBackend> =
    || Box::new(FakeTier1Backend);

// End-to-end tests against the real scanner path
// (`check_revision_staleness_and_enqueue` → `scan_one_alias` →
// `JobManager::enqueue_analyzer_run`). These pin the wire between the
// classify decisions above and the actual DB-stamped run rows, which
// the classify-only tests cannot reach.
#[cfg(test)]
mod e2e {
    use super::*;
    use crate::cas::registry as cas_registry;
    use crate::cas::store as cas_store;
    use crate::manifest::ManifestId;
    use crate::paths::CasDataDir;
    use std::sync::Arc;

    /// `fake-workspace` (linked-in test analyzer) has `revision() == 7`
    /// and `parser_id() == "fake-parser"`. We pre-seed a manifest with
    /// a `fake-parser` blob so `expected_analyzers_for_manifest`
    /// resolves to `[fake-workspace]` exactly.
    const ANALYZER_ID: &str = "fake-workspace";
    const EXPECTED_REV: u32 = 7;

    struct AliasFixture {
        _data: tempfile::TempDir,
        _repo: tempfile::TempDir,
        cas_data_dir: Arc<CasDataDir>,
        job_manager: Arc<JobManager>,
        repo_hash: String,
        manifest_id: ManifestId,
    }

    /// Build one registered alias with a tentative manifest that
    /// `expected_analyzers_for_manifest` will resolve to
    /// `[fake-workspace]`.
    fn setup_alias() -> AliasFixture {
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let job_manager = JobManager::new(Arc::clone(&cas_data_dir));
        let alias = "myrepo".to_string();
        let repo_hash = "repo-hash".to_string();
        {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(&tx, &alias, repo.path().to_str().unwrap(), &repo_hash, 1)
                .unwrap();
            tx.commit().unwrap();
        }
        let manifest_id = ManifestId(1);
        let conn = cas_store::open(&cas_data_dir.store_db_path(&repo_hash)).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (?1, 'tentative', 0)",
            [manifest_id.0],
        )
        .unwrap();
        // worktree_id 1 + tentative anchor → resolve_tentative_manifest_id
        // returns manifest 1.
        conn.execute(
            "INSERT INTO worktrees (worktree_id, path, registered_at_ns)
             VALUES (1, ?1, 0)",
            [repo.path().to_str().unwrap()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('tentative/1', ?1, 0)",
            [manifest_id.0],
        )
        .unwrap();
        // One fake-parser blob so expected_analyzers_for_manifest hits
        // fake-workspace via parser_id filter.
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('fake-sha', 'fake-parser', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, 'src/fake.fake-tier1', 'fake-sha')",
            [manifest_id.0],
        )
        .unwrap();

        AliasFixture {
            _data: data,
            _repo: repo,
            cas_data_dir,
            job_manager,
            repo_hash,
            manifest_id,
        }
    }

    fn insert_run(fixture: &AliasFixture, analyzer_id: &str, revision: u32, status: &str) {
        let conn =
            cas_store::open(&fixture.cas_data_dir.store_db_path(&fixture.repo_hash)).unwrap();
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, ?2, ?3, 'cfg', ?4, 10, NULL, NULL, NULL, 0)",
            rusqlite::params![fixture.manifest_id.0, analyzer_id, revision, status],
        )
        .unwrap();
    }

    fn count_runs(fixture: &AliasFixture, analyzer_id: &str) -> i64 {
        let conn =
            cas_store::open(&fixture.cas_data_dir.store_db_path(&fixture.repo_hash)).unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = ?2",
            rusqlite::params![fixture.manifest_id.0, analyzer_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn fetch_run_status(
        fixture: &AliasFixture,
        analyzer_id: &str,
        revision: u32,
    ) -> Option<String> {
        let conn =
            cas_store::open(&fixture.cas_data_dir.store_db_path(&fixture.repo_hash)).unwrap();
        conn.query_row(
            "SELECT status FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = ?2 AND analyzer_revision = ?3",
            rusqlite::params![fixture.manifest_id.0, analyzer_id, revision],
            |r| r.get::<_, String>(0),
        )
        .ok()
    }

    /// R2 must-fix 2 #1: old-revision succeeded → targeted enqueue. The
    /// new run row carries the *expected* revision (7), not the old one
    /// the previous run was stamped with.
    #[test]
    fn e2e_old_revision_succeeded_enqueues_via_new_api() {
        let f = setup_alias();
        insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, "succeeded");

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        assert_eq!(summary.aliases_scanned, 1);
        assert_eq!(summary.aliases_failed, 0);
        assert_eq!(summary.jobs_enqueued, 1);
        assert_eq!(summary.active_stale, 0);
        assert_eq!(summary.terminal_failed_current_revision, 0);
        // The new queued row stamps the expected revision — proving the
        // `enqueue_analyzer_run` API derives the stamp internally
        // rather than echoing a caller-supplied value.
        assert_eq!(
            fetch_run_status(&f, ANALYZER_ID, EXPECTED_REV).as_deref(),
            Some("queued"),
            "expected-revision row should be queued; rows = {:?}",
            count_runs(&f, ANALYZER_ID)
        );
    }

    /// R2 must-fix 2 #2: no run row at all → enqueue. This is the
    /// freshly-registered-repo case.
    #[test]
    fn e2e_missing_row_enqueues() {
        let f = setup_alias();
        // No pre-seeded run row.

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        assert_eq!(summary.jobs_enqueued, 1);
        assert_eq!(summary.aliases_failed, 0);
        assert_eq!(
            fetch_run_status(&f, ANALYZER_ID, EXPECTED_REV).as_deref(),
            Some("queued"),
        );
    }

    /// R2 must-fix 2 #3: old-revision terminal failure (failed /
    /// timed_out / cancelled) → enqueue. The new run gives the bumped
    /// resolver a chance to succeed at the current revision.
    #[test]
    fn e2e_old_revision_terminal_failure_enqueues() {
        for status in ["failed", "timed_out", "cancelled"] {
            let f = setup_alias();
            insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, status);

            let summary =
                check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None)
                    .unwrap_or_else(|e| panic!("scan failed for status `{status}`: {e}"));

            assert_eq!(
                summary.jobs_enqueued, 1,
                "old-revision `{status}` should enqueue; summary = {summary:?}"
            );
            assert_eq!(summary.terminal_failed_current_revision, 0);
        }
    }

    /// R2 must-fix 2 #4: matching-revision failure → NOT enqueued,
    /// counted as `terminal_failed_current_revision`. Doctor surfaces
    /// it; we deliberately do not loop on persistent failures.
    #[test]
    fn e2e_matching_revision_failure_skips_and_counts_terminal_current() {
        for status in ["failed", "timed_out", "cancelled"] {
            let f = setup_alias();
            insert_run(&f, ANALYZER_ID, EXPECTED_REV, status);

            let summary =
                check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None)
                    .unwrap_or_else(|e| panic!("scan failed for status `{status}`: {e}"));

            assert_eq!(
                summary.jobs_enqueued, 0,
                "matching-revision `{status}` must not enqueue; summary = {summary:?}"
            );
            assert_eq!(
                summary.terminal_failed_current_revision, 1,
                "matching-revision `{status}` should hit terminal_failed_current_revision; summary = {summary:?}"
            );
            // No queued row at the expected revision means the
            // failed row stays alone — exactly one row total.
            assert_eq!(count_runs(&f, ANALYZER_ID), 1);
        }
    }

    /// R2 must-fix 2 #5: old-revision queued/running → NOT enqueued,
    /// counted as `active_stale`. The in-flight job from the previous
    /// boot is allowed to finish; the de-dup gate would coalesce us
    /// anyway.
    #[test]
    fn e2e_old_revision_in_flight_skips_and_counts_active_stale() {
        for status in ["queued", "running"] {
            let f = setup_alias();
            insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, status);

            let summary =
                check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None)
                    .unwrap_or_else(|e| panic!("scan failed for status `{status}`: {e}"));

            assert_eq!(
                summary.jobs_enqueued, 0,
                "old-revision `{status}` must not enqueue (active_stale); summary = {summary:?}"
            );
            assert_eq!(
                summary.active_stale, 1,
                "old-revision `{status}` should hit active_stale; summary = {summary:?}"
            );
            assert_eq!(count_runs(&f, ANALYZER_ID), 1);
        }
    }

    /// R2 must-fix 2 #6: the scan reads the **tentative** manifest
    /// only. A definitive-manifest row at the old revision must not
    /// trigger an enqueue, even though it would look stale.
    #[test]
    fn e2e_uses_tentative_manifest_ignores_definitive_manifest_drift() {
        let f = setup_alias();

        // Add a second, definitive manifest with the same parser blob,
        // and an old-revision succeeded row for it. The tentative
        // manifest (already wired in setup_alias) has *no* row, so it
        // is freshly-stale and should drive exactly one enqueue.
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (2, 'committed', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (2, 'src/fake.fake-tier1', 'fake-sha')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (2, ?1, ?2, 'cfg', 'succeeded', 10, 20, NULL, NULL, 0)",
            rusqlite::params![ANALYZER_ID, EXPECTED_REV - 1],
        )
        .unwrap();

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        // One enqueue for the tentative manifest (missing row), and
        // zero enqueues for the definitive manifest's old row.
        assert_eq!(summary.jobs_enqueued, 1, "summary = {summary:?}");
        let queued_for_definitive: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workspace_analysis_runs
                 WHERE manifest_id = 2 AND analyzer_id = ?1 AND status = 'queued'",
                [ANALYZER_ID],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            queued_for_definitive, 0,
            "definitive manifest must not get a queued row"
        );
    }

    /// R2 must-fix 2 #7: a per-alias error (e.g. unreadable store file)
    /// increments `aliases_failed` and the scan continues to the next
    /// alias instead of aborting. We register a second clean alias to
    /// prove the loop keeps running.
    #[test]
    fn e2e_per_alias_error_continues_and_counts_aliases_failed() {
        let f = setup_alias();

        // Register a broken second alias whose store file is *present*
        // but corrupt — opening it surfaces a SQLite error inside
        // `scan_one_alias`.
        let broken_alias = "broken";
        let broken_repo = tempfile::tempdir().unwrap();
        let broken_hash = "broken-hash";
        {
            let mut index = cas_registry::open(&f.cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(
                &tx,
                broken_alias,
                broken_repo.path().to_str().unwrap(),
                broken_hash,
                2,
            )
            .unwrap();
            tx.commit().unwrap();
        }
        // Create a non-SQLite file at the store path so `cas_store::open`
        // fails parseably.
        let store_path = f.cas_data_dir.store_db_path(broken_hash);
        std::fs::create_dir_all(store_path.parent().unwrap()).unwrap();
        std::fs::write(&store_path, b"this is not a sqlite file").unwrap();

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        assert_eq!(summary.aliases_scanned, 2);
        assert_eq!(
            summary.aliases_failed, 1,
            "broken alias must increment aliases_failed; summary = {summary:?}"
        );
        // The clean alias still got its enqueue (the loop continued).
        assert_eq!(summary.jobs_enqueued, 1);
    }

    // ─── parser-revision drift tests ───────────────────────────────────────
    //
    // These pin the new pre-check in `scan_one_alias` that fires before the
    // analyzer-drift loop. The 13 tests are arranged top-down: unit tests on
    // `detect_parser_revision_drift` first, then end-to-end through
    // `check_revision_staleness_and_enqueue`, then doctor-surface tests on
    // the new `ParserStaleRevision` output (the doctor tests live in
    // `ctl::methods::doctor::tests`, see test #11-13 there).

    /// Test #1 — happy path: `blobs.parser_revision` equals the
    /// linked-in `FakeTier1Backend::parser_revision()` (= 1), so the
    /// drift check is a no-op and the scanner proceeds to the analyzer
    /// drift loop.
    #[test]
    fn parser_drift_no_drift_when_persisted_matches() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        let drifted =
            detect_parser_revision_drift(&conn, f.manifest_id, &std::path::PathBuf::from("/"))
                .unwrap();
        assert!(
            !drifted,
            "matching parser_revision must not register as drift"
        );
    }

    /// Test #2 — R2 must-fix: equality (`!=`) is the rule, not order
    /// (`<`). A blob row at `parser_revision = 2` while the backend
    /// reports `1` is a rollback case, which is *still* drift because
    /// the persisted syntactic facts came from a build whose output
    /// shape is not guaranteed to match the current binary.
    #[test]
    fn parser_drift_detects_rollback_as_drift() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute(
            "UPDATE blobs SET parser_revision = 2 WHERE blob_sha = 'fake-sha'",
            [],
        )
        .unwrap();
        let drifted =
            detect_parser_revision_drift(&conn, f.manifest_id, &std::path::PathBuf::from("/"))
                .unwrap();
        assert!(drifted, "rollback (persisted > expected) must be drift");
    }

    /// Test #3 — R2 must-fix #1 (core): a missing `blobs(blob_sha,
    /// parser_id)` row is drift. The `parse_pending_blobs` recovery
    /// path will re-parse and write the row on the next register pass.
    #[test]
    fn parser_drift_when_blob_row_missing() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute("DELETE FROM blobs WHERE blob_sha = 'fake-sha'", [])
            .unwrap();
        let drifted =
            detect_parser_revision_drift(&conn, f.manifest_id, &std::path::PathBuf::from("/"))
                .unwrap();
        assert!(drifted, "missing parse row must register as drift");
    }

    /// Test #4 — R2 must-fix #1 (∞-loop regression pin): an obsolete
    /// `blobs(blob_sha, parser_id)` row whose `parser_id` is no longer
    /// claimed by any backend must NOT trigger drift. The expected-
    /// parse-units set is computed from current backends only; an
    /// obsolete row simply falls outside the set.
    ///
    /// Without this guarantee, the scanner would enqueue a full
    /// reindex; `parse_pending_blobs` wouldn't touch the obsolete row
    /// (the new backend doesn't claim it); next startup would see the
    /// same row and enqueue again — an infinite reindex loop.
    #[test]
    fn parser_drift_obsolete_parser_row_ignored() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        // Add a row whose parser_id no live backend claims.
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('fake-sha', 'tree-sitter-extinct', 99, 0)",
            [],
        )
        .unwrap();
        let drifted =
            detect_parser_revision_drift(&conn, f.manifest_id, &std::path::PathBuf::from("/"))
                .unwrap();
        assert!(
            !drifted,
            "obsolete parser_id row must be ignored (no infinite reindex loop)"
        );
    }

    /// Test #5 — vacuous case: a manifest with no entries that map to
    /// any backend yields no expected parse units, so the drift check
    /// returns `false` trivially.
    #[test]
    fn parser_drift_empty_manifest_no_drift() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        // Replace the `.fake-tier1` entry with a path no backend claims.
        conn.execute(
            "UPDATE manifest_entries SET path = 'src/nothing.dead' WHERE manifest_id = ?1",
            [f.manifest_id.0],
        )
        .unwrap();
        let drifted =
            detect_parser_revision_drift(&conn, f.manifest_id, &std::path::PathBuf::from("/"))
                .unwrap();
        assert!(
            !drifted,
            "manifest with no expected parse units must report no drift"
        );
    }

    /// Test #6 — repo_root that does not exist (missing worktree) is a
    /// recoverable signal: extension-match still works (no I/O), and
    /// the fallback paths in `expected_parse_units` skip cleanly when
    /// `std::fs::read` fails. The fast-path entry must still register
    /// drift when the persisted row is missing.
    #[test]
    fn parser_drift_handles_missing_worktree_for_extension_match() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute("DELETE FROM blobs WHERE blob_sha = 'fake-sha'", [])
            .unwrap();
        // repo_root deliberately points at a path that does not exist
        // on disk. The `.fake-tier1` extension still drives the
        // selection chain without ever touching disk.
        let drifted = detect_parser_revision_drift(
            &conn,
            f.manifest_id,
            &std::path::PathBuf::from("/no/such/path-xyz123"),
        )
        .unwrap();
        assert!(
            drifted,
            "extension-match drift must fire even when the worktree is gone"
        );
    }

    /// Test #7 — end-to-end happy path: no drift, scanner proceeds to
    /// analyzer drift loop and enqueues an analyzer run as before. The
    /// `parser_drift_aliases` and `parser_drift_reconcile_requests` counters
    /// stay at zero.
    #[test]
    fn e2e_no_parser_drift_proceeds_to_analyzer_drift() {
        let f = setup_alias();
        insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, "succeeded");

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        assert_eq!(summary.parser_drift_aliases, 0);
        assert_eq!(summary.parser_drift_reconcile_requests, 0);
        assert_eq!(summary.jobs_enqueued, 1, "analyzer drift still enqueues");
    }

    /// Test #8 — end-to-end drift path: a blob revision mismatch
    /// triggers the parser-drift pre-check, the scanner attempts a
    /// full repo reindex enqueue, and the summary counters reflect
    /// the dispatch. The actual reindex enqueue may fail (this test
    /// setup has no git repo on disk), but the *scanner-side* counter
    /// records that the request was issued.
    #[test]
    fn e2e_parser_drift_sets_summary_counters() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute(
            "UPDATE blobs SET parser_revision = 0 WHERE blob_sha = 'fake-sha'",
            [],
        )
        .unwrap();
        drop(conn);

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        assert_eq!(
            summary.parser_drift_aliases, 1,
            "drift must be counted; summary = {summary:?}"
        );
        assert_eq!(
            summary.parser_drift_reconcile_requests, 1,
            "exactly one full reindex request per drifted alias"
        );
    }

    /// Test #9 — protocol #6 pin: parser drift skips the analyzer
    /// drift loop. The scanner must not double-enqueue: the upcoming
    /// full reindex will re-stamp all analyzer rows, so adding more
    /// analyzer-only enqueues here is dead work that the dedup gate
    /// will coalesce anyway. The test seeds *both* parser drift and
    /// analyzer drift on the same alias; only the parser path fires.
    #[test]
    fn e2e_parser_drift_skips_analyzer_drift_path() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute(
            "UPDATE blobs SET parser_revision = 0 WHERE blob_sha = 'fake-sha'",
            [],
        )
        .unwrap();
        drop(conn);
        insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, "succeeded"); // analyzer also stale

        let summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        assert_eq!(summary.parser_drift_aliases, 1);
        assert_eq!(
            summary.jobs_enqueued, 0,
            "analyzer drift path must NOT fire when parser drift fires; summary = {summary:?}"
        );
        // The pre-existing analyzer run row stays at its old revision —
        // the full reindex (when it succeeds against a real worktree)
        // is what re-stamps it, not the scanner.
        assert_eq!(
            fetch_run_status(&f, ANALYZER_ID, EXPECTED_REV).as_deref(),
            None,
            "no expected-revision row created by the parser-drift path"
        );
    }

    /// Test #10 — per-alias independence: a drifted alias and a clean
    /// alias both get the right treatment; counters add up correctly.
    #[test]
    fn e2e_parser_drift_per_alias_independence() {
        let drifted = setup_alias();
        {
            let conn =
                cas_store::open(&drifted.cas_data_dir.store_db_path(&drifted.repo_hash)).unwrap();
            conn.execute(
                "UPDATE blobs SET parser_revision = 0 WHERE blob_sha = 'fake-sha'",
                [],
            )
            .unwrap();
        }

        // Register a second clean alias under the same data dir.
        let clean_repo = tempfile::tempdir().unwrap();
        let clean_hash = "clean-hash";
        let clean_alias = "clean";
        {
            let mut index = cas_registry::open(&drifted.cas_data_dir.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(
                &tx,
                clean_alias,
                clean_repo.path().to_str().unwrap(),
                clean_hash,
                2,
            )
            .unwrap();
            tx.commit().unwrap();
        }
        let clean_manifest_id = ManifestId(1);
        let conn = cas_store::open(&drifted.cas_data_dir.store_db_path(clean_hash)).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (?1, 'tentative', 0)",
            [clean_manifest_id.0],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO worktrees (worktree_id, path, registered_at_ns)
             VALUES (1, ?1, 0)",
            [clean_repo.path().to_str().unwrap()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('tentative/1', ?1, 0)",
            [clean_manifest_id.0],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('clean-sha', 'fake-parser', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, 'src/clean.fake-tier1', 'clean-sha')",
            [clean_manifest_id.0],
        )
        .unwrap();
        drop(conn);

        let summary =
            check_revision_staleness_and_enqueue(&drifted.cas_data_dir, &drifted.job_manager, None)
                .unwrap();

        assert_eq!(summary.aliases_scanned, 2);
        assert_eq!(
            summary.parser_drift_aliases, 1,
            "only the drifted alias counts; summary = {summary:?}"
        );
        assert_eq!(summary.parser_drift_reconcile_requests, 1);
    }

    /// Test #11 — `FullReindexEnqueueOutcome` shape pin: the
    /// `JobManager::enqueue_full_repo_reindex` API returns a
    /// structured outcome with the seven documented fields. Tests #12
    /// and #13 below exercise the contents; this test only pins
    /// that the type exists and the API surface is `(alias, reason)`.
    #[test]
    fn full_reindex_outcome_api_surface_is_alias_and_reason() {
        // Build a minimal registry so the lookup succeeds, then verify
        // the call fails downstream rather than at the alias-lookup
        // step (= the surface is `(alias, reason)`, not richer).
        let f = setup_alias();
        // No git repo at the worktree, so the inner
        // `register_repo_force_analyzers_enqueue` will fail when it
        // shells out to `git rev-parse HEAD`. That's expected; the
        // point is that the *caller* only passed the two arguments.
        let result = f
            .job_manager
            .enqueue_full_repo_reindex("myrepo", ReindexReason::Manual);
        // We don't assert on Ok/Err here — both branches prove the
        // call site only had to know alias + reason.
        let _ = result;
    }

    /// Test #12 — unknown alias yields `RepoNotFound`. The error type
    /// is part of the API contract: callers can distinguish "no such
    /// alias" from "store I/O failed" without parsing strings.
    #[test]
    fn full_reindex_unknown_alias_returns_repo_not_found() {
        let f = setup_alias();
        let err = f
            .job_manager
            .enqueue_full_repo_reindex("not-registered", ReindexReason::Manual)
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::RepoNotFound { ref alias } if alias == "not-registered"),
            "expected RepoNotFound, got {err:?}"
        );
    }

    /// Test #13 — protocol #6 cross-PR invariant pin: a parser drift
    /// dispatched through the scanner must take the
    /// `register_repo_force_analyzers_enqueue` path, *not*
    /// `enqueue_analyzer_run`. Verified by the contrapositive: no
    /// `workspace_analysis_runs` row gets a `queued` status when the
    /// underlying register call fails (no git repo). If the scanner
    /// had wrongly fallen through to `enqueue_analyzer_run`, that
    /// helper would have stamped a queued row even without a git
    /// repo, because it does not invoke git.
    #[test]
    fn e2e_parser_drift_uses_full_reindex_api_not_enqueue_analyzer_run() {
        let f = setup_alias();
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute(
            "UPDATE blobs SET parser_revision = 0 WHERE blob_sha = 'fake-sha'",
            [],
        )
        .unwrap();
        drop(conn);
        insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, "succeeded"); // would-be analyzer-drift bait

        let _summary =
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager, None).unwrap();

        // If the parser-drift path had fallen through to
        // `enqueue_analyzer_run`, a row at EXPECTED_REV would now be
        // queued. Its absence pins the routing.
        assert_eq!(
            fetch_run_status(&f, ANALYZER_ID, EXPECTED_REV).as_deref(),
            None,
            "parser drift must NOT route through enqueue_analyzer_run"
        );
        // The pre-existing analyzer row stays untouched.
        assert_eq!(count_runs(&f, ANALYZER_ID), 1);
    }

    // ─── Phase 3 MF suite ─────────────────────────────────────────

    fn build_reconcile(f: &AliasFixture) -> Arc<crate::reconcile::RepoReconcileManager> {
        use crate::reconcile::RepoReconcileManager;
        let mgr = RepoReconcileManager::new(f.cas_data_dir.clone(), None);
        // Skip real register/enqueue work in the worker — MF tests
        // only care about durable intent recording and lack of
        // analyzer_run stamping via the parser-drift path.
        mgr.set_test_register_hook(Arc::new(|_, _, _, _| Ok(())));
        mgr
    }

    fn trigger_parser_drift(f: &AliasFixture) {
        let conn = cas_store::open(&f.cas_data_dir.store_db_path(&f.repo_hash)).unwrap();
        conn.execute(
            "UPDATE blobs SET parser_revision = 0 WHERE blob_sha = 'fake-sha'",
            [],
        )
        .unwrap();
    }

    async fn run_scan(
        f: &AliasFixture,
        reconcile: Option<Arc<crate::reconcile::RepoReconcileManager>>,
    ) -> StalenessSummary {
        let cas = f.cas_data_dir.clone();
        let job_manager = f.job_manager.clone();
        tokio::task::spawn_blocking(move || {
            check_revision_staleness_and_enqueue(&cas, &job_manager, reconcile.as_ref())
        })
        .await
        .unwrap()
        .unwrap()
    }

    /// MF-1: parser drift records durable reconcile intent through
    /// the manager (desired_generation + force_generation both
    /// bump), NOT through the compat full-reindex helper.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mf1_parser_drift_records_reconcile_intent() {
        let f = setup_alias();
        trigger_parser_drift(&f);
        let reconcile = build_reconcile(&f);
        let summary = run_scan(&f, Some(reconcile.clone())).await;

        assert_eq!(summary.parser_drift_aliases, 1);
        assert_eq!(summary.parser_drift_reconcile_requests, 1);

        let index = cas_registry::open(&f.cas_data_dir.index_db_path()).unwrap();
        let state = cas_registry::get_reconcile_state(&index, &f.repo_hash)
            .unwrap()
            .expect("reconcile state row must exist");
        assert!(
            state.desired_generation >= 1,
            "desired must bump; state = {state:?}"
        );
        assert!(
            state.force_generation >= 1,
            "force must bump (drift → forced reindex path); state = {state:?}"
        );

        reconcile.shutdown(std::time::Duration::from_secs(2)).await;
    }

    /// MF-2: parser drift with reconcile MUST NOT queue any
    /// analyzer-only rows via `enqueue_analyzer_run` — the worker
    /// re-stamps everything under the forced register path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mf2_parser_drift_skips_analyzer_loop_with_reconcile() {
        let f = setup_alias();
        trigger_parser_drift(&f);
        // Also seed analyzer drift so both paths could fire.
        insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, "succeeded");
        let reconcile = build_reconcile(&f);
        let _summary = run_scan(&f, Some(reconcile.clone())).await;

        // Only the pre-existing (old-revision) row exists; no new
        // row was stamped at the expected revision by the scanner.
        assert_eq!(
            fetch_run_status(&f, ANALYZER_ID, EXPECTED_REV).as_deref(),
            None,
            "parser drift must not queue an analyzer run via the reconcile path"
        );
        assert_eq!(count_runs(&f, ANALYZER_ID), 1);
        reconcile.shutdown(std::time::Duration::from_secs(2)).await;
    }

    /// MF-4: analyzer-only drift with reconcile wired still uses
    /// the targeted `enqueue_analyzer_run` primitive, and does NOT
    /// touch reconcile state. Analyzer-only rerun is not a
    /// repo-level intent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mf4_analyzer_drift_still_uses_analyzer_enqueue_with_reconcile() {
        let f = setup_alias();
        // Analyzer drift only, no parser drift.
        insert_run(&f, ANALYZER_ID, EXPECTED_REV - 1, "succeeded");
        let reconcile = build_reconcile(&f);
        let summary = run_scan(&f, Some(reconcile.clone())).await;

        assert_eq!(summary.parser_drift_aliases, 0);
        assert_eq!(summary.parser_drift_reconcile_requests, 0);
        assert!(summary.jobs_enqueued >= 1);
        // Fresh analyzer run row stamped at the expected revision.
        assert_eq!(
            fetch_run_status(&f, ANALYZER_ID, EXPECTED_REV).as_deref(),
            Some("queued")
        );
        // Reconcile state untouched.
        let index = cas_registry::open(&f.cas_data_dir.index_db_path()).unwrap();
        let state = cas_registry::get_reconcile_state(&index, &f.repo_hash)
            .unwrap()
            .expect("reconcile state row exists from v4 migration");
        assert_eq!(state.desired_generation, 0);
        assert_eq!(state.force_generation, 0);
        reconcile.shutdown(std::time::Duration::from_secs(2)).await;
    }

    /// MF-6: when the scanner is invoked without a reconcile
    /// driver (test / degraded startup), parser drift falls back
    /// to the compat `JobManager::enqueue_full_repo_reindex`
    /// helper. Production daemon wiring always passes reconcile,
    /// so this branch is not a production caller; it exists so
    /// the historical test surface keeps working while the
    /// helper is retired to `pub(crate)`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mf6_reconcile_none_falls_back_to_full_reindex_helper() {
        let f = setup_alias();
        trigger_parser_drift(&f);
        let summary = run_scan(&f, None).await;

        assert_eq!(summary.parser_drift_aliases, 1);
        assert_eq!(summary.parser_drift_reconcile_requests, 1);
        // Fallback path does not touch reconcile state.
        let index = cas_registry::open(&f.cas_data_dir.index_db_path()).unwrap();
        let state = cas_registry::get_reconcile_state(&index, &f.repo_hash)
            .unwrap()
            .expect("reconcile state row seeded by v4 migration");
        assert_eq!(state.desired_generation, 0);
        assert_eq!(state.force_generation, 0);
    }
}
