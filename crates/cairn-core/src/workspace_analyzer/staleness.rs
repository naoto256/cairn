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

use rusqlite::params;
use tracing::{debug, info, warn};

use crate::Result;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::jobs::{EnqueueAnalyzerRun, JobManager};
use crate::paths::CasDataDir;
use crate::workspace_analyzer::expected_analyzers_for_manifest;

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

/// Aggregated outcome of one scan. Reported via `tracing::info` at the
/// end of the spawn_blocking task; daemon callers use it only for logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StalenessSummary {
    pub aliases_scanned: usize,
    pub aliases_failed: usize,
    pub jobs_enqueued: usize,
    pub active_stale: usize,
    pub terminal_failed_current_revision: usize,
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
        match scan_one_alias(cas_data_dir, job_manager, entry, &expected_revision_for) {
            Ok(per_alias) => {
                summary.jobs_enqueued += per_alias.jobs_enqueued;
                summary.active_stale += per_alias.active_stale;
                summary.terminal_failed_current_revision +=
                    per_alias.terminal_failed_current_revision;
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
        "revision staleness scan complete"
    );
    Ok(summary)
}

#[derive(Debug, Default)]
struct PerAliasSummary {
    jobs_enqueued: usize,
    active_stale: usize,
    terminal_failed_current_revision: usize,
}

fn scan_one_alias(
    cas_data_dir: &CasDataDir,
    job_manager: &JobManager,
    entry: &cas_registry::AliasEntry,
    expected_revision_for: &HashMap<&'static str, u32>,
) -> Result<PerAliasSummary> {
    let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
    if !store_path.exists() {
        // The alias row references a store file that was deleted out
        // from under us (e.g. manual cleanup). Doctor will flag it;
        // staleness scan has nothing to do.
        return Ok(PerAliasSummary::default());
    }
    let mut conn = cas_store::open(&store_path)?;
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
             VALUES (?1, 'src/fake.rs', 'fake-sha')",
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
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager).unwrap();

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
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager).unwrap();

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

            let summary = check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager)
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

            let summary = check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager)
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

            let summary = check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager)
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
             VALUES (2, 'src/fake.rs', 'fake-sha')",
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
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager).unwrap();

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
            check_revision_staleness_and_enqueue(&f.cas_data_dir, &f.job_manager).unwrap();

        assert_eq!(summary.aliases_scanned, 2);
        assert_eq!(
            summary.aliases_failed, 1,
            "broken alias must increment aliases_failed; summary = {summary:?}"
        );
        // The clean alias still got its enqueue (the loop continued).
        assert_eq!(summary.jobs_enqueued, 1);
    }
}
