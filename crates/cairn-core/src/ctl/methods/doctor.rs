//! `doctor` — environment / dependency / registry sanity checks.
//!
//! The report is a flat vector of
//! [`cairn_proto::control::DoctorCheck`] items produced by four
//! families of probes. When a family emits, it does so in this order
//! so consumers see a stable prefix; families 3 and 4 are skipped
//! entirely when family 2 takes its empty-registry early-out (a
//! single `Warn`) or the alias listing itself errors, so the
//! reconcile-state group is not guaranteed to appear at all:
//!
//! 1. Environment coherence — linked language backends, workspace
//!    analyzer registration, data-directory writability, and Tier-3
//!    LSP binary discovery.
//! 2. Registered repositories — per-alias root-present and watcher
//!    checks (early-out with a single `Warn` when the registry is
//!    empty).
//! 3. Per-alias CAS store probes — tentative snapshot, analyzer /
//!    parser revision drift, post-drift rerun health, and current
//!    Tier-3 run status.
//! 4. Reconcile-state health (deduped by `repo_hash`) plus
//!    incomplete- and recent-removal history from index.db.
//!
//! Most non-`Pass` branches fill a remediation string keyed on
//! `alias` where applicable, so the CLI can print an actionable next
//! command without cross-referencing docs; a few checks intentionally
//! omit it (linked-language-backends `Fail`, backend-registration
//! coherence `Warn`, data-directory `Fail`, empty-registry `Warn`,
//! alias-index enumeration `Fail`) because no single command fixes
//! the condition (the coherence `Warn` instead names the missing
//! import symbol inline in its detail).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::manifest::ManifestId;
use crate::paths::CasDataDir;
use crate::workspace_analyzer::{
    ParserStaleRevision, StaleRevision, all_workspace_analyzers, compute_parser_stale_revisions,
    expected_analyzers_for_manifest,
};

mod analysis;
mod reconcile;
mod toolchain;

use analysis::{
    analyzer_rerun_health_checks, parser_revision_stale_checks, revision_stale_checks,
    tier3_run_checks,
};
use reconcile::reconcile_state_checks;
use toolchain::{backend_registration_coherence_check, tier3_binary_checks};

/// Wall-clock budget after which a `queued` / `running`
/// `workspace_analysis_runs` row is treated as wedged and surfaces a
/// `Warn` in doctor. 6 hours is long enough that an honest cold-start
/// LSP pass on a large repo finishes well under it, and short enough
/// that a stuck pool waiter from yesterday is obvious in the morning.
const STUCK_RUN_THRESHOLD: Duration = Duration::from_secs(6 * 3600);

include!(concat!(env!("OUT_DIR"), "/expected_backend_crates.rs"));

/// Distributed-slice registration marker for the `doctor` control
/// method. State-free; a fresh instance is constructed each time
/// the dispatcher initializes.
struct Doctor;

#[async_trait::async_trait]
impl ControlMethod for Doctor {
    fn name(&self) -> &'static str {
        "doctor"
    }

    /// Runs every check family in the order documented on the
    /// module. Every DB hop (alias listing, per-store probes, and
    /// reconcile-state read) runs under `spawn_blocking`; a
    /// `JoinError` from any hop maps to
    /// [`crate::Error::internal_task_panic`] and short-circuits.
    ///
    /// If the alias listing itself errors, families 2 / 3 / 4 all
    /// skip and the report carries only the environment checks plus
    /// a single `alias index readable` Fail. If the listing succeeds
    /// but is empty, the report carries the environment checks plus
    /// one `registered repositories` Warn and no per-alias or
    /// reconcile-state work runs.
    async fn dispatch(&self, ctx: &CtlCtx, _params: Value) -> Result<Value> {
        let mut checks: Vec<DoctorCheck> = Vec::new();

        let backend_names: Vec<&'static str> = cairn_lang_api::all_backends()
            .iter()
            .map(|b| b.name())
            .collect();
        checks.push(doctor_check(
            "language backends linked",
            if backend_names.is_empty() {
                DoctorStatus::Fail
            } else {
                DoctorStatus::Pass
            },
            Some(if backend_names.is_empty() {
                "none linked".into()
            } else {
                format!(
                    "{} backend(s): {}",
                    backend_names.len(),
                    backend_names.join(", ")
                )
            }),
            None,
        ));
        checks.push(backend_registration_coherence_check(
            &backend_names,
            &workspace_analyzer_ids(),
        ));

        let cas_root = ctx.cas_data_dir.root().to_path_buf();
        let writable = std::fs::metadata(&cas_root)
            .map(|m| !m.permissions().readonly())
            .unwrap_or(false);
        checks.push(doctor_check(
            "data directory",
            if writable {
                DoctorStatus::Pass
            } else {
                DoctorStatus::Fail
            },
            Some(if writable {
                cas_root.to_string_lossy().to_string()
            } else {
                format!("not writable: {}", cas_root.display())
            }),
            None,
        ));

        checks.extend(tier3_binary_checks());

        let cas_data_dir = ctx.cas_data_dir.clone();
        let aliases_result =
            tokio::task::spawn_blocking(move || -> Result<Vec<cas_registry::AliasEntry>> {
                let index = cas_registry::open(&cas_data_dir.index_db_path())?;
                cas_registry::list_all(&index)
            })
            .await
            .map_err(|e| crate::Error::internal_task_panic("doctor", e))?;

        match aliases_result {
            Ok(entries) if entries.is_empty() => checks.push(doctor_check(
                "registered repositories",
                DoctorStatus::Warn,
                Some("no repos registered yet".into()),
                None,
            )),
            Ok(entries) => {
                for entry in &entries {
                    checks.push(registered_repo_path_check(entry));
                }
                if let Some(watch_manager) = ctx.watch_manager.as_ref() {
                    checks.extend(alias_watcher_checks(&entries, watch_manager));
                }

                let store_data_dir = ctx.cas_data_dir.clone();
                let store_entries = entries.clone();
                let store_probes = tokio::task::spawn_blocking(move || {
                    probe_alias_stores(&store_data_dir, &store_entries)
                })
                .await
                .map_err(|e| crate::Error::internal_task_panic("doctor", e))?;
                checks.extend(tentative_snapshot_checks(&store_probes));
                checks.extend(revision_stale_checks(&store_probes));
                checks.extend(parser_revision_stale_checks(&store_probes));
                checks.extend(analyzer_rerun_health_checks(&store_probes));
                checks.extend(tier3_run_checks(&store_probes));
                // Reconcile health belongs to the canonical repository.
                // Deduping by repo_hash prevents aliases for the same repo
                // from producing identical warnings.
                let reconcile_data_dir = ctx.cas_data_dir.clone();
                let reconcile_res = tokio::task::spawn_blocking(move || {
                    reconcile_state_checks(&reconcile_data_dir)
                })
                .await
                .map_err(|e| crate::Error::internal_task_panic("doctor", e))?;
                match reconcile_res {
                    Ok(chks) => checks.extend(chks),
                    Err(err) => checks.push(doctor_check(
                        "reconcile state readable",
                        DoctorStatus::Fail,
                        Some(err.to_string()),
                        Some("Inspect daemon logs; index.db may be corrupt.".into()),
                    )),
                }
            }
            Err(e) => checks.push(doctor_check(
                "alias index readable",
                DoctorStatus::Fail,
                Some(e.to_string()),
                None,
            )),
        }

        Ok(serde_json::to_value(DoctorReport { checks }).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Doctor);

fn doctor_check(
    name: impl Into<String>,
    status: DoctorStatus,
    detail: Option<String>,
    remediation: Option<String>,
) -> DoctorCheck {
    DoctorCheck {
        name: name.into(),
        status,
        detail,
        remediation,
    }
}

fn workspace_analyzer_ids() -> Vec<&'static str> {
    all_workspace_analyzers()
        .iter()
        .map(|analyzer| analyzer.id())
        .collect()
}

/// Missing root is classified as `Fail`: the alias is durably
/// registered but any worktree-dependent operation (scan / reindex /
/// watcher arm) will fail — CAS-backed reads over already-indexed
/// snapshots continue to work. The remediation surfaces both
/// recovery paths (drop the alias vs. restore the directory) because
/// on-disk data survives when other aliases point at the same
/// `repo_hash`.
fn registered_repo_path_check(entry: &cas_registry::AliasEntry) -> DoctorCheck {
    let exists = Path::new(&entry.root_path).is_dir();
    doctor_check(
        format!("repo `{}` root present", entry.alias),
        if exists {
            DoctorStatus::Pass
        } else {
            DoctorStatus::Fail
        },
        Some(if exists {
            entry.root_path.clone()
        } else {
            format!("missing: {}", entry.root_path)
        }),
        (!exists).then(|| {
            format!(
                "Run `cairn ctl repo remove {}` to drop the alias entry (on-disk data is kept for any other aliases at the same path), or restore the directory at {}.",
                entry.alias, entry.root_path
            )
        }),
    )
}

/// One check per registered alias reflecting whether the
/// `WatchManager` currently holds a live FS watcher for it. Missing
/// coverage is `Warn`, not `Fail`: the reindex path can still
/// recover via manual `reindex_repo` or a daemon restart, but until
/// then future file events are blind. Existing tentative anchors
/// are unaffected — default reads keep resolving to the tentative
/// snapshot; only aliases with no tentative anchor at all fall back
/// to HEAD.
fn alias_watcher_checks(
    entries: &[cas_registry::AliasEntry],
    watch_manager: &crate::watcher::WatchManager,
) -> Vec<DoctorCheck> {
    entries
        .iter()
        .map(|entry| {
            let watching = watch_manager.is_watching_alias(&entry.alias);
            doctor_check(
                format!("repo `{}` watcher active", entry.alias),
                if watching {
                    DoctorStatus::Pass
                } else {
                    DoctorStatus::Warn
                },
                Some(if watching {
                    format!("watching {}", entry.root_path)
                } else {
                    "not watching (alias registered but no live FS watcher; tentative-default reads will fall back to HEAD until the next reindex_repo)".into()
                }),
                (!watching).then(|| {
                    format!(
                        "Run `cairn ctl repo remove {}` then `cairn ctl repo register --alias {} {}` to re-establish the FS watcher. Restarting the daemon is an alternative that re-installs every alias's watcher in one shot.",
                        entry.alias, entry.alias, entry.root_path
                    )
                }),
            )
        })
        .collect()
}

/// Result of probing one alias's CAS store, produced by
/// [`probe_alias_stores`]. `result` carries either a fully-populated
/// [`AliasStoreState`] or the string form of the first probe error;
/// the outer probe never short-circuits on a single alias's failure,
/// so downstream check families always cover every alias.
#[derive(Debug, Clone)]
struct AliasStoreProbe {
    alias: String,
    store_path: PathBuf,
    /// Whether `root_path` still has a worktree row in this store.
    /// Kept separately from the tentative anchor so doctor can name
    /// the missing ownership layer precisely.
    worktree_registered: bool,
    result: std::result::Result<AliasStoreState, String>,
}

/// Snapshot of the per-alias CAS store used by every family-3
/// check. Only the tentative-anchored manifest is inspected — other
/// anchors and older manifests are not part of the doctor surface.
/// Every `Vec` / `HashMap` field defaults to empty when the tentative
/// anchor is absent (fresh alias never indexed) so downstream checks
/// can treat them uniformly.
#[derive(Debug, Clone)]
struct AliasStoreState {
    tentative_manifest_id: Option<i64>,
    tier3_runs: Vec<Tier3Run>,
    expected_tier3_analyzer_ids: Vec<String>,
    /// Per-analyzer revision-mismatch evidence the doctor surfaces as a
    /// `Warn`. Populated from
    /// [`crate::workspace_analyzer::expected_analyzers_for_manifest`]'s
    /// `revision()` vs the persisted `analyzer_revision` column. Empty
    /// when nothing is stale (the common case).
    stale_revisions: Vec<StaleRevision>,
    /// Per-`(parser_id, current_rev)` `parser_revision` drift. Built
    /// from `compute_parser_stale_revisions`, which starts from the
    /// expected parse units rather than `SELECT DISTINCT parser_id
    /// FROM blobs`. A `current_rev = None` entry means a parse row
    /// is missing entirely (same recovery path as a mismatch).
    /// Empty in the common case.
    stale_parser_revisions: Vec<ParserStaleRevision>,
    /// `analyzer_id -> expected revision` for every analyzer the
    /// current build expects to run on this manifest. Lets
    /// `parser_drift_rerun_check` verify that a row whose
    /// `status = succeeded` is at the current revision. Inferring
    /// "current" from the absence of a `StaleRevision` entry is
    /// insufficient: a `succeeded` row at an older revision must not
    /// satisfy the parser-drift safety-net Case A.
    expected_analyzer_revisions: HashMap<String, u32>,
}

/// One row from `workspace_analysis_runs`, projected for the
/// checks that need it. Rows are collected per-analyzer; the
/// `(manifest_id, analyzer_id)` PRIMARY KEY guarantees at most one
/// row per analyzer per manifest.
#[derive(Debug, Clone)]
struct Tier3Run {
    analyzer_id: String,
    manifest_id: i64,
    status: String,
    error: Option<String>,
    /// Persisted `analyzer_revision`. Used by the new
    /// `analyzer_rerun_health_checks` to distinguish "succeeded at the
    /// expected revision" (a normal, current run) from "succeeded at
    /// an older revision" (the analyzer-revision-drift detector
    /// flagged it, but the rerun never landed). The
    /// `(manifest_id, analyzer_id)` PRIMARY KEY means there is at
    /// most one row per analyzer per manifest, so the persisted
    /// revision is the single source of truth.
    analyzer_revision: u32,
    /// `started_at_ns` from `workspace_analysis_runs`. Doctor uses it to
    /// detect rows that have been `queued`/`running` past
    /// [`STUCK_RUN_THRESHOLD`] — that level of pool-wait usually means
    /// the worker is wedged, not that indexing is genuinely slow.
    started_at_ns: i64,
}

fn probe_alias_stores(
    cas_data_dir: &CasDataDir,
    entries: &[cas_registry::AliasEntry],
) -> Vec<AliasStoreProbe> {
    entries
        .iter()
        .map(|entry| probe_alias_store(cas_data_dir, entry))
        .collect()
}

fn probe_alias_store(
    cas_data_dir: &CasDataDir,
    entry: &cas_registry::AliasEntry,
) -> AliasStoreProbe {
    let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
    let (worktree_registered, result) = match probe_alias_store_inner(&store_path, &entry.root_path)
    {
        Ok((worktree_registered, state)) => (worktree_registered, Ok(state)),
        Err(error) => (false, Err(error.to_string())),
    };
    AliasStoreProbe {
        alias: entry.alias.clone(),
        store_path,
        worktree_registered,
        result,
    }
}

/// Opens the alias's CAS store and materializes the fields the
/// family-3 checks read. Pipeline:
///
/// 1. Existence check on `store_path` (returns `Err` if missing so
///    the caller renders a probe-error `Fail`).
/// 2. Read-only open via `cas_store::open_existing`.
/// 3. Look up `worktree_id` by `root_path`. Absent → the alias has
///    never been indexed; the tentative manifest and every
///    downstream vector default to empty, and [`probe_manifest`] is
///    skipped.
/// 4. Otherwise resolve the `tentative/<worktree_id>` anchor and
///    delegate to [`probe_manifest`] for the tier3-run rows, drift
///    vectors, and expected-revision map.
fn probe_alias_store_inner(store_path: &Path, root_path: &str) -> Result<(bool, AliasStoreState)> {
    if !store_path.exists() {
        return Err(crate::Error::InvalidArgument(format!(
            "CAS store does not exist: {}",
            store_path.display()
        )));
    }
    let conn = cas_store::open_existing(store_path)?;
    let worktree_id = conn
        .query_row(
            "SELECT worktree_id FROM worktrees WHERE path = ?1",
            params![root_path],
            |r| r.get::<_, i64>(0),
        )
        .optional()?;
    let tentative_manifest_id = match worktree_id {
        Some(id) => conn
            .query_row(
                "SELECT manifest_id FROM anchors WHERE anchor_name = ?1",
                params![format!("tentative/{id}")],
                |r| r.get::<_, i64>(0),
            )
            .optional()?,
        None => None,
    };
    let (
        tier3_runs,
        expected_tier3_analyzer_ids,
        stale_revisions,
        stale_parser_revisions,
        expected_analyzer_revisions,
    ) = match tentative_manifest_id {
        Some(manifest_id) => probe_manifest(&conn, manifest_id, Path::new(root_path))?,
        None => (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            HashMap::new(),
        ),
    };
    Ok((
        worktree_id.is_some(),
        AliasStoreState {
            tentative_manifest_id,
            tier3_runs,
            expected_tier3_analyzer_ids,
            stale_revisions,
            stale_parser_revisions,
            expected_analyzer_revisions,
        },
    ))
}

/// Loads the current `workspace_analysis_runs` rows for
/// `manifest_id` and cross-references them against the linked-in
/// expected analyzer set to build the two drift vectors and the
/// per-analyzer expected-revision map used later by
/// [`parser_drift_rerun_check`].
///
/// A `StaleRevision` entry is pushed when either (a) the analyzer
/// has no row yet (`current_rev = None`) or (b) the persisted
/// revision is strictly less than the linked-in build's
/// `revision()`. A newer persisted revision (e.g. after a binary
/// downgrade) is not treated as stale here and is not surfaced.
#[allow(clippy::type_complexity)]
fn probe_manifest(
    conn: &rusqlite::Connection,
    manifest_id: i64,
    root_path: &Path,
) -> Result<(
    Vec<Tier3Run>,
    Vec<String>,
    Vec<StaleRevision>,
    Vec<ParserStaleRevision>,
    HashMap<String, u32>,
)> {
    let expected_analyzers = expected_analyzers_for_manifest(conn, ManifestId(manifest_id))?;
    let mut expected_tier3_analyzer_ids = expected_analyzers
        .iter()
        .map(|analyzer| analyzer.id().to_string())
        .collect::<Vec<_>>();
    expected_tier3_analyzer_ids.sort();

    let mut stmt = conn.prepare(
        "SELECT analyzer_id, manifest_id, status, error, analyzer_revision, started_at_ns
         FROM workspace_analysis_runs
         WHERE manifest_id = ?1
         ORDER BY analyzer_id",
    )?;
    let rows = stmt
        .query_map(params![manifest_id], |r| {
            let rev = r.get::<_, i64>(4)? as u32;
            Ok(Tier3Run {
                analyzer_id: r.get(0)?,
                manifest_id: r.get(1)?,
                status: r.get(2)?,
                error: r.get(3)?,
                analyzer_revision: rev,
                started_at_ns: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut persisted_revs: HashMap<String, u32> = HashMap::new();
    for run in &rows {
        persisted_revs.insert(run.analyzer_id.clone(), run.analyzer_revision);
    }
    let tier3_runs = rows;

    let mut stale_revisions = Vec::new();
    for analyzer in &expected_analyzers {
        let expected_rev = analyzer.revision();
        let current_rev = persisted_revs.get(analyzer.id()).copied();
        let is_mismatch = match current_rev {
            Some(cur) => cur < expected_rev,
            None => true,
        };
        if is_mismatch {
            stale_revisions.push(StaleRevision {
                analyzer_id: analyzer.id().to_string(),
                current_rev,
                expected_rev,
            });
        }
    }
    stale_revisions.sort_by(|a, b| a.analyzer_id.cmp(&b.analyzer_id));

    let stale_parser_revisions =
        compute_parser_stale_revisions(conn, ManifestId(manifest_id), root_path)?;

    // Capture every expected analyzer revision so the parser-drift
    // cross-reference can verify a `succeeded` row directly. The
    // absence of `stale_revisions` does not prove that a persisted
    // analyzer run used the current revision.
    let mut expected_analyzer_revisions: HashMap<String, u32> = HashMap::new();
    for analyzer in &expected_analyzers {
        expected_analyzer_revisions.insert(analyzer.id().to_string(), analyzer.revision());
    }

    Ok((
        tier3_runs,
        expected_tier3_analyzer_ids,
        stale_revisions,
        stale_parser_revisions,
        expected_analyzer_revisions,
    ))
}

/// Four outcomes per alias: `Pass` when a tentative anchor resolves
/// to a manifest; distinct `Warn`s when either the alias root has no
/// worktree registration or a registered worktree has no tentative
/// anchor; and `Fail` when any store-probe step errored. Only the
/// A missing per-worktree anchor does not by itself prove HEAD
/// fallback: default selection may use another tentative anchor in
/// the same store. Without the worktree row this probe cannot relate
/// the root to any surviving tentative anchor.
fn tentative_snapshot_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
    probes
        .iter()
        .map(|probe| match &probe.result {
            Ok(state) => match state.tentative_manifest_id {
                Some(manifest_id) => doctor_check(
                    format!("repo `{}` tentative snapshot present", probe.alias),
                    DoctorStatus::Pass,
                    Some(format!("tentative anchor -> manifest_id {manifest_id}")),
                    None,
                ),
                None if !probe.worktree_registered => doctor_check(
                    format!("repo `{}` tentative snapshot present", probe.alias),
                    DoctorStatus::Warn,
                    Some(
                        "no worktree registration for the alias root; root-to-tentative ownership cannot be resolved"
                            .into(),
                    ),
                    Some(format!(
                        "Run `cairn ctl repo reindex {}` to restore the alias root's worktree registration before relying on tentative ownership.",
                        probe.alias
                    )),
                ),
                None => doctor_check(
                    format!("repo `{}` tentative snapshot present", probe.alias),
                    DoctorStatus::Warn,
                    Some(
                        "no tentative anchor for this alias worktree; default reads may use another tentative anchor in this store, otherwise HEAD"
                            .into(),
                    ),
                    Some(format!(
                        "Run `cairn ctl repo reindex {}` to build the tentative snapshot.",
                        probe.alias
                    )),
                ),
            },
            Err(error) => doctor_check(
                format!("repo `{}` tentative snapshot present", probe.alias),
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

/// One [`binary_check`] per Tier-3 LSP the daemon can spawn, plus
/// the .NET SDK root probe that csharp-ls depends on. Each entry is
/// independent — a missing binary never blocks the others — and
/// each carries an install-hint remediation string so the operator
/// can act without consulting external docs.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::registry;
    use crate::paths::CasDataDir;
    use crate::watcher::WatchManager;
    use cairn_watch::WatchBackend;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Notify;

    const RUST_ANALYZER_ID: &str = "rust-analyzer-lsp";

    #[test]
    fn missing_repo_path_check_includes_remediation() {
        let entry = cas_registry::AliasEntry {
            alias: "gone".into(),
            root_path: "/definitely/missing/cairn/repo".into(),
            repo_hash: "hash".into(),
            registered_at_ns: 0,
        };

        let check = registered_repo_path_check(&entry);

        assert_eq!(check.status, DoctorStatus::Fail);
        assert_eq!(
            check.detail.as_deref(),
            Some("missing: /definitely/missing/cairn/repo")
        );
        let remediation = check.remediation.expect("remediation");
        assert!(remediation.contains("repo remove gone"));
        assert!(remediation.contains("/definitely/missing/cairn/repo"));
    }

    #[test]
    fn watcher_check_warns_with_remediation_when_alias_is_not_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = WatchManager::with_backend(
            Arc::new(CasDataDir::with_root(tmp.path().join("data"))),
            WatchBackend::Poll,
        );
        let entries = [cas_registry::AliasEntry {
            alias: "demo".into(),
            root_path: tmp.path().join("repo").to_string_lossy().to_string(),
            repo_hash: "hash".into(),
            registered_at_ns: 0,
        }];

        let checks = alias_watcher_checks(&entries, &manager);

        assert_eq!(checks[0].status, DoctorStatus::Warn);
        assert_eq!(
            checks[0].detail.as_deref(),
            Some(
                "not watching (alias registered but no live FS watcher; tentative-default reads will fall back to HEAD until the next reindex_repo)"
            )
        );
        assert!(
            checks[0]
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo register --alias demo")
        );
    }

    #[test]
    fn tentative_snapshot_checks_distinguish_missing_worktree_and_anchor() {
        let probes = vec![
            AliasStoreProbe {
                alias: "ok".into(),
                store_path: PathBuf::from("/tmp/ok/store.db"),
                worktree_registered: true,
                result: Ok(AliasStoreState {
                    tentative_manifest_id: Some(7),
                    tier3_runs: Vec::new(),
                    expected_tier3_analyzer_ids: Vec::new(),
                    stale_revisions: Vec::new(),
                    stale_parser_revisions: Vec::new(),
                    expected_analyzer_revisions: HashMap::new(),
                }),
            },
            AliasStoreProbe {
                alias: "missing-anchor".into(),
                store_path: PathBuf::from("/tmp/missing-anchor/store.db"),
                worktree_registered: true,
                result: Ok(AliasStoreState {
                    tentative_manifest_id: None,
                    tier3_runs: Vec::new(),
                    expected_tier3_analyzer_ids: Vec::new(),
                    stale_revisions: Vec::new(),
                    stale_parser_revisions: Vec::new(),
                    expected_analyzer_revisions: HashMap::new(),
                }),
            },
            AliasStoreProbe {
                alias: "missing-worktree".into(),
                store_path: PathBuf::from("/tmp/missing-worktree/store.db"),
                worktree_registered: false,
                result: Ok(AliasStoreState {
                    tentative_manifest_id: None,
                    tier3_runs: Vec::new(),
                    expected_tier3_analyzer_ids: Vec::new(),
                    stale_revisions: Vec::new(),
                    stale_parser_revisions: Vec::new(),
                    expected_analyzer_revisions: HashMap::new(),
                }),
            },
            AliasStoreProbe {
                alias: "bad".into(),
                store_path: PathBuf::from("/tmp/bad/store.db"),
                worktree_registered: false,
                result: Err("not a database".into()),
            },
        ];

        let checks = tentative_snapshot_checks(&probes);

        assert_eq!(checks[0].status, DoctorStatus::Pass);
        assert!(
            checks[0]
                .detail
                .as_deref()
                .unwrap()
                .contains("manifest_id 7")
        );
        assert_eq!(checks[1].status, DoctorStatus::Warn);
        assert_eq!(
            checks[1].detail.as_deref(),
            Some(
                "no tentative anchor for this alias worktree; default reads may use another tentative anchor in this store, otherwise HEAD"
            )
        );
        assert!(
            checks[1]
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex")
        );
        assert_eq!(checks[2].status, DoctorStatus::Warn);
        assert_eq!(
            checks[2].detail.as_deref(),
            Some(
                "no worktree registration for the alias root; root-to-tentative ownership cannot be resolved"
            )
        );
        assert!(
            checks[2]
                .remediation
                .as_deref()
                .unwrap()
                .contains("restore the alias root's worktree registration")
        );
        assert_ne!(checks[1].detail, checks[2].detail);
        assert_ne!(checks[1].remediation, checks[2].remediation);
        assert_eq!(checks[3].status, DoctorStatus::Fail);
        assert_eq!(checks[3].detail.as_deref(), Some("not a database"));
        assert!(
            checks[3]
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo remove")
        );
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_live_watcher_tentative_anchor_and_tier3_success() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", true, Some("succeeded"), None);
        fixture
            .watch_manager
            .watch_alias("demo".into(), fixture.repo_path("demo"))
            .unwrap();

        let report = fixture.run_doctor().await;

        let watcher = find_check(&report, "repo `demo` watcher active");
        assert_eq!(watcher.status, DoctorStatus::Pass);
        let tentative = find_check(&report, "repo `demo` tentative snapshot present");
        assert_eq!(tentative.status, DoctorStatus::Pass);
        let tier3 = find_check(&report, "repo `demo` Tier-3 analyzer status");
        assert_eq!(tier3.status, DoctorStatus::Pass);
        assert!(tier3.detail.as_deref().unwrap().contains("succeeded"));
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_per_analyzer_tier3_status_when_multiple_runs_present() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", true, None, None);
        fixture.seed_workspace_run("demo", "pyright-lsp", "succeeded", None);
        fixture.seed_workspace_run(
            "demo",
            RUST_ANALYZER_ID,
            "skipped",
            Some("no matching files"),
        );

        let report = fixture.run_doctor().await;

        let tier3 = find_check(&report, "repo `demo` Tier-3 analyzer status");
        assert_eq!(tier3.status, DoctorStatus::Pass);
        let detail = tier3.detail.as_deref().unwrap();
        assert!(detail.contains("pyright-lsp=succeeded"));
        assert!(detail.contains("rust-analyzer-lsp=skipped"));
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_registered_workspace_analyzer_without_run_record() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", true, None, None);
        fixture.seed_manifest_blob("demo", "sha-fake", "fake-parser");

        let report = fixture.run_doctor().await;

        let tier3 = find_check(&report, "repo `demo` Tier-3 analyzer status");
        assert_eq!(tier3.status, DoctorStatus::Warn);
        assert!(
            tier3
                .detail
                .as_deref()
                .unwrap()
                .contains("fake-workspace=not yet recorded (run reindex)")
        );
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_missing_watcher_and_tentative_with_remediation() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("demo", false, None, None);

        let report = fixture.run_doctor().await;

        let watcher = find_check(&report, "repo `demo` watcher active");
        assert_eq!(watcher.status, DoctorStatus::Warn);
        assert!(
            watcher
                .detail
                .as_deref()
                .unwrap()
                .starts_with("not watching")
        );
        assert!(
            watcher
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo register --alias demo")
        );
        let tentative = find_check(&report, "repo `demo` tentative snapshot present");
        assert_eq!(tentative.status, DoctorStatus::Warn);
        assert_eq!(
            tentative.detail.as_deref(),
            Some(
                "no tentative anchor for this alias worktree; default reads may use another tentative anchor in this store, otherwise HEAD"
            )
        );
        assert!(
            tentative
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex")
        );
    }

    #[tokio::test]
    async fn doctor_dispatch_reports_missing_worktree_separately_from_missing_anchor() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("missing-worktree", true, None, None);
        fixture.seed_alias("missing-anchor", false, None, None);
        let store =
            cas_store::open(&fixture.cas_data_dir.store_db_path("missing-worktree-hash")).unwrap();
        let worktree_id: i64 = store
            .query_row("SELECT worktree_id FROM worktrees", [], |row| row.get(0))
            .unwrap();
        store.execute("DELETE FROM worktrees", []).unwrap();
        let selected = crate::anchor::resolve_explicit_or_default(&store, None, None).unwrap();
        let missing_anchor_store =
            cas_store::open(&fixture.cas_data_dir.store_db_path("missing-anchor-hash")).unwrap();
        let head_selected =
            crate::anchor::resolve_explicit_or_default(&missing_anchor_store, None, None).unwrap();

        let report = fixture.run_doctor().await;
        let worktree = find_check(
            &report,
            "repo `missing-worktree` tentative snapshot present",
        );
        let anchor = find_check(&report, "repo `missing-anchor` tentative snapshot present");

        assert_eq!(worktree.status, DoctorStatus::Warn);
        assert_eq!(anchor.status, DoctorStatus::Warn);
        assert_eq!(
            selected.as_str(),
            format!("tentative/{worktree_id}"),
            "a surviving tentative anchor remains the default independently of the worktree row"
        );
        assert_eq!(
            worktree.detail.as_deref(),
            Some(
                "no worktree registration for the alias root; root-to-tentative ownership cannot be resolved"
            )
        );
        assert_eq!(
            anchor.detail.as_deref(),
            Some(
                "no tentative anchor for this alias worktree; default reads may use another tentative anchor in this store, otherwise HEAD"
            )
        );
        assert_eq!(
            head_selected.as_str(),
            "HEAD",
            "default selection falls back to HEAD when the store has no tentative anchors"
        );
        assert_ne!(worktree.remediation, anchor.remediation);
    }

    #[tokio::test]
    async fn doctor_missing_anchor_allows_another_worktree_tentative_default() {
        let fixture = DoctorFixture::new();
        fixture.seed_alias("target", true, None, None);

        let other_path = fixture.repo_path("other");
        std::fs::create_dir_all(&other_path).unwrap();

        let store = cas_store::open(&fixture.cas_data_dir.store_db_path("target-hash")).unwrap();
        let target_path = fixture.repo_path("target");
        let target_worktree_id: i64 = store
            .query_row(
                "SELECT worktree_id FROM worktrees WHERE path = ?1",
                params![target_path.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        store
            .execute(
                "INSERT INTO worktrees (path, registered_at_ns) VALUES (?1, 0)",
                params![other_path.to_string_lossy().as_ref()],
            )
            .unwrap();
        let other_worktree_id = store.last_insert_rowid();
        store
            .execute(
                "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
                 VALUES (?1, 1, 0)",
                params![format!("tentative/{other_worktree_id}")],
            )
            .unwrap();
        store
            .execute(
                "DELETE FROM anchors WHERE anchor_name = ?1",
                params![format!("tentative/{target_worktree_id}")],
            )
            .unwrap();

        let selected = crate::anchor::resolve_explicit_or_default(&store, None, None).unwrap();
        let report = fixture.run_doctor().await;
        let target = find_check(&report, "repo `target` tentative snapshot present");

        assert_eq!(selected.as_str(), format!("tentative/{other_worktree_id}"));
        assert_eq!(target.status, DoctorStatus::Warn);
        assert_eq!(
            target.detail.as_deref(),
            Some(
                "no tentative anchor for this alias worktree; default reads may use another tentative anchor in this store, otherwise HEAD"
            )
        );
        assert!(
            !target
                .detail
                .as_deref()
                .unwrap()
                .contains("reads will fall back to HEAD"),
            "the alias-local probe cannot prove that default reads select HEAD"
        );
    }

    struct DoctorFixture {
        _tmp: tempfile::TempDir,
        cas_data_dir: Arc<CasDataDir>,
        watch_manager: Arc<WatchManager>,
        repos_root: PathBuf,
    }

    impl DoctorFixture {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let cas_data_dir = Arc::new(CasDataDir::with_root(tmp.path().join("data")));
            cas_data_dir.ensure().unwrap();
            let watch_manager = Arc::new(WatchManager::with_backend(
                cas_data_dir.clone(),
                WatchBackend::Poll,
            ));
            let repos_root = tmp.path().join("repos");
            std::fs::create_dir_all(&repos_root).unwrap();
            Self {
                _tmp: tmp,
                cas_data_dir,
                watch_manager,
                repos_root,
            }
        }

        fn repo_path(&self, alias: &str) -> PathBuf {
            self.repos_root.join(alias)
        }

        fn seed_alias(
            &self,
            alias: &str,
            with_tentative: bool,
            tier3_status: Option<&str>,
            tier3_error: Option<&str>,
        ) {
            let repo_path = self.repo_path(alias);
            std::fs::create_dir_all(&repo_path).unwrap();
            let repo_hash = format!("{alias}-hash");
            let mut index = registry::open(&self.cas_data_dir.index_db_path()).unwrap();
            {
                let tx = index.transaction().unwrap();
                registry::upsert(&tx, alias, &repo_path.to_string_lossy(), &repo_hash, 0).unwrap();
                tx.commit().unwrap();
            }

            let store_path = self.cas_data_dir.store_db_path(&repo_hash);
            let store = cas_store::open(&store_path).unwrap();
            store
                .execute(
                    "INSERT INTO worktrees (path, registered_at_ns) VALUES (?1, 0)",
                    params![repo_path.to_string_lossy().as_ref()],
                )
                .unwrap();
            let worktree_id = store.last_insert_rowid();
            store
                .execute(
                    "INSERT INTO manifests (manifest_id, kind, built_at_ns)
                     VALUES (1, 'tentative', 0)",
                    [],
                )
                .unwrap();
            if with_tentative {
                store
                    .execute(
                        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
                         VALUES (?1, 1, 0)",
                        params![format!("tentative/{worktree_id}")],
                    )
                    .unwrap();
            }
            if let Some(status) = tier3_status {
                self.seed_workspace_run(alias, RUST_ANALYZER_ID, status, tier3_error);
            }
        }

        fn seed_workspace_run(
            &self,
            alias: &str,
            analyzer_id: &str,
            status: &str,
            error: Option<&str>,
        ) {
            let store_path = self.cas_data_dir.store_db_path(&format!("{alias}-hash"));
            let store = cas_store::open(&store_path).unwrap();
            store
                .execute(
                    "INSERT INTO workspace_analysis_runs
                       (manifest_id, analyzer_id, analyzer_revision, config_hash,
                        status, started_at_ns, finished_at_ns, error)
                     VALUES (1, ?1, 1, 'cfg', ?2, 0, 1, ?3)",
                    params![analyzer_id, status, error],
                )
                .unwrap();
        }

        fn seed_manifest_blob(&self, alias: &str, blob_sha: &str, parser_id: &str) {
            let store_path = self.cas_data_dir.store_db_path(&format!("{alias}-hash"));
            let store = cas_store::open(&store_path).unwrap();
            store
                .execute(
                    "INSERT INTO blobs
                       (blob_sha, parser_id, parser_revision, parsed_at_ns)
                     VALUES (?1, ?2, 1, 0)",
                    params![blob_sha, parser_id],
                )
                .unwrap();
            store
                .execute(
                    "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
                     VALUES (1, ?1, ?2)",
                    params![format!("src/{blob_sha}.fake"), blob_sha],
                )
                .unwrap();
        }

        async fn run_doctor(&self) -> DoctorReport {
            let ctx = CtlCtx {
                cas_data_dir: self.cas_data_dir.clone(),
                shutdown: Arc::new(Notify::new()),
                watch_manager: Some(self.watch_manager.clone()),
                job_manager: None,
                reconcile: None,
                lifecycle: None,
                version: "test",
                started_at: Instant::now(),
            };
            let value = Doctor.dispatch(&ctx, Value::Null).await.unwrap();
            serde_json::from_value(value).unwrap()
        }
    }

    fn find_check<'a>(report: &'a DoctorReport, name: &str) -> &'a DoctorCheck {
        report
            .checks
            .iter()
            .find(|check| check.name == name)
            .unwrap_or_else(|| panic!("missing check `{name}` in {:#?}", report.checks))
    }
}
