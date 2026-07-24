//! `doctor` — environment / dependency / registry sanity checks.
//!
//! The report is a flat vector of
//! [`cairn_proto::control::DoctorCheck`] items produced by four
//! families of probes, always emitted in this order so consumers see
//! a stable prefix:
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
//! omit it (linked-language-backends `Fail`, data-directory `Fail`,
//! empty-registry `Warn`, alias-index enumeration `Fail`) because no
//! single command fixes the condition.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_proto::control::{DoctorCheck, DoctorReport, DoctorStatus};
use linkme::distributed_slice;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::lsp_discovery::{
    discover_lsp_binary, discover_lsp_binary_candidates, discover_sourcekit_lsp,
};
use crate::manifest::ManifestId;
use crate::paths::CasDataDir;
use crate::workspace_analyzer::{
    ParserStaleRevision, StaleRevision, all_workspace_analyzers, compute_parser_stale_revisions,
    expected_analyzers_for_manifest,
};

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
                // PR3 Phase 4: durable reconcile-state health.
                // Deduped by repo_hash so a repo with two aliases
                // does not produce two identical warnings.
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
    /// `parser_drift_rerun_check` actually verify that a row whose
    /// `status = succeeded` is at the current revision (R2 catch:
    /// inferring "current" from the absence of a `StaleRevision`
    /// entry is fragile — the parser-drift check would otherwise
    /// declare a `succeeded` row at any revision as "current" and
    /// misclassify analyzer-stale rows as the D PR safety-net Case A
    /// Fail).
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
    let result = probe_alias_store_inner(&store_path, &entry.root_path).map_err(|e| e.to_string());
    AliasStoreProbe {
        alias: entry.alias.clone(),
        store_path,
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
fn probe_alias_store_inner(store_path: &Path, root_path: &str) -> Result<AliasStoreState> {
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
    Ok(AliasStoreState {
        tentative_manifest_id,
        tier3_runs,
        expected_tier3_analyzer_ids,
        stale_revisions,
        stale_parser_revisions,
        expected_analyzer_revisions,
    })
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

    // Capture the expected revision for every analyzer so the
    // parser-drift cross-reference can verify a `succeeded` row's
    // revision directly (R2 catch on PR #228 v1: relying on the
    // absence of `stale_revisions` to imply "current" would let
    // analyzer-stale rows masquerade as the D PR safety-net Case A).
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

/// Three outcomes per alias: `Pass` when a tentative anchor
/// resolves to a manifest, `Warn` when the store opened cleanly but
/// no tentative anchor exists yet (reads still work but fall back
/// to HEAD), and `Fail` when any store-probe step errored — store
/// file missing / DB open failure, or downstream anchor / manifest
/// / workspace-run / parser-revision probe error.
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
                None => doctor_check(
                    format!("repo `{}` tentative snapshot present", probe.alias),
                    DoctorStatus::Warn,
                    Some("no tentative anchor yet (reads will fall back to HEAD)".into()),
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
fn tier3_binary_checks() -> Vec<DoctorCheck> {
    vec![
        rust_analyzer_binary_check(),
        pyright_binary_check(),
        gopls_binary_check(),
        clangd_binary_check(),
        typescript_language_server_binary_check(),
        csharp_ls_binary_check(),
        csharp_dotnet_sdk_check(),
        phpantom_lsp_binary_check(),
        jdtls_binary_check(),
        kotlin_language_server_binary_check(),
        ruby_lsp_binary_check(),
        sourcekit_lsp_binary_check(),
    ]
}

fn rust_analyzer_binary_check() -> DoctorCheck {
    binary_check(
        "rust-analyzer binary discoverable",
        resolve_rust_analyzer(),
        "rust-analyzer not on PATH",
        "Install rust-analyzer (`rustup component add rust-analyzer`) and ensure it's on the daemon's PATH; Tier-3 (LSP) facts will not be available until then.",
    )
}

fn pyright_binary_check() -> DoctorCheck {
    binary_check(
        "pyright binary discoverable",
        resolve_pyright(),
        "pyright-langserver not on PATH",
        "Install pyright (`pip install pyright` or `npm i -g pyright`) and ensure pyright-langserver is on the daemon's PATH; Python Tier-3 (LSP) facts will not be available until then.",
    )
}

fn gopls_binary_check() -> DoctorCheck {
    binary_check(
        "gopls binary discoverable",
        resolve_gopls(),
        "gopls not on PATH",
        "Install gopls (`go install golang.org/x/tools/gopls@latest`) and ensure it's on the daemon's PATH; Go Tier-3 (LSP) facts will not be available until then.",
    )
}

fn clangd_binary_check() -> DoctorCheck {
    binary_check(
        "clangd binary discoverable",
        resolve_clangd(),
        "clangd not on PATH",
        "Install clangd (for example through LLVM / Xcode command line tools) and ensure it's on the daemon's PATH; C, C++, and Objective-C Tier-3 (LSP) facts will not be available until then.",
    )
}

fn typescript_language_server_binary_check() -> DoctorCheck {
    binary_check(
        "typescript-language-server binary discoverable",
        resolve_typescript_language_server(),
        "typescript-language-server not on PATH",
        "Install typescript-language-server (`npm i -g typescript typescript-language-server`) and ensure it's on the daemon's PATH; TypeScript, JavaScript, and TSX Tier-3 (LSP) facts will not be available until then.",
    )
}

fn csharp_ls_binary_check() -> DoctorCheck {
    binary_check(
        "csharp-ls binary discoverable",
        resolve_csharp_ls(),
        "csharp-ls not discoverable via CSHARP_LS or PATH",
        "Install csharp-ls (`dotnet tool install -g csharp-ls`) and ensure the .NET tools directory is on the daemon's PATH, or set CSHARP_LS; C# Tier-3 (LSP) facts will not be available until then.",
    )
}

fn csharp_dotnet_sdk_check() -> DoctorCheck {
    match dotnet_sdk_root(
        std::env::var_os("DOTNET_ROOT").map(PathBuf::from),
        standard_dotnet_roots(),
    ) {
        Some(root) => doctor_check(
            ".NET SDK root discoverable for csharp-ls",
            DoctorStatus::Pass,
            Some(root.display().to_string()),
            None,
        ),
        None => doctor_check(
            ".NET SDK root discoverable for csharp-ls",
            DoctorStatus::Warn,
            Some("DOTNET_ROOT unset and no SDK found in standard dotnet roots".into()),
            Some("Install the .NET SDK or set DOTNET_ROOT so csharp-ls can locate MSBuild under daemon launch environments.".into()),
        ),
    }
}

fn dotnet_sdk_root(
    dotnet_root: Option<PathBuf>,
    roots: impl IntoIterator<Item = PathBuf>,
) -> Option<PathBuf> {
    if let Some(root) = dotnet_root {
        return Some(root);
    }
    roots.into_iter().find(|root| root.join("sdk").is_dir())
}

fn standard_dotnet_roots() -> Vec<PathBuf> {
    let mut roots = vec![
        PathBuf::from("/usr/local/share/dotnet"),
        PathBuf::from("/opt/homebrew/share/dotnet"),
        PathBuf::from("/opt/homebrew/opt/dotnet/libexec"),
    ];
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".dotnet"));
    }
    roots
}

fn phpantom_lsp_binary_check() -> DoctorCheck {
    binary_check(
        "phpantom-lsp binary discoverable",
        resolve_phpantom_lsp(),
        "PHPantom LSP not discoverable via PHPANTOM_LSP or PATH",
        "Install PHPantom LSP (`brew install phpantom-lsp` or `cargo install phpantom_lsp --locked`) and ensure `phpantom_lsp` or `phpantom-lsp` is on the daemon's PATH, or set PHPANTOM_LSP; PHP Tier-3 (LSP) facts will not be available until then.",
    )
}

fn jdtls_binary_check() -> DoctorCheck {
    binary_check(
        "jdtls binary discoverable",
        resolve_jdtls(),
        "jdtls not on PATH",
        "Install an Eclipse JDT Language Server wrapper script named `jdtls`, or set JDTLS to that wrapper; Java Tier-3 (LSP) facts will not be available until then.",
    )
}

fn kotlin_language_server_binary_check() -> DoctorCheck {
    binary_check(
        "kotlin-language-server binary discoverable",
        resolve_kotlin_language_server(),
        "kotlin-language-server not discoverable via KOTLIN_LANGUAGE_SERVER or PATH",
        "Install kotlin-language-server (`brew install kotlin-language-server`, or download a release zip from https://github.com/fwcd/kotlin-language-server/releases) and ensure its wrapper script is on the daemon's PATH, or set KOTLIN_LANGUAGE_SERVER. JVM 11+ is required; Kotlin Tier-3 (LSP) facts will not be available until then.",
    )
}

fn ruby_lsp_binary_check() -> DoctorCheck {
    binary_check(
        "ruby-lsp binary discoverable",
        resolve_ruby_lsp(),
        "ruby-lsp not on PATH",
        "Install ruby-lsp (`gem install ruby-lsp`) and ensure it's on the daemon's PATH, or set RUBY_LSP; Ruby Tier-3 (LSP) facts will not be available until then.",
    )
}

fn sourcekit_lsp_binary_check() -> DoctorCheck {
    binary_check(
        "sourcekit-lsp binary discoverable",
        resolve_sourcekit_lsp(),
        "sourcekit-lsp not discoverable via SOURCEKIT_LSP, xcrun, or PATH",
        "Install Xcode command line tools (`xcode-select --install`) or a Swift toolchain that provides sourcekit-lsp, then ensure `xcrun --find sourcekit-lsp` or PATH can find it; Swift Tier-3 (LSP) facts will not be available until then.",
    )
}

/// Shared shape for Tier-3 binary probes. Resolved → `Pass` with
/// the resolved path as detail; not resolved → `Warn` (never
/// `Fail`). Missing Tier-3 support is a partial-capability state,
/// not a broken daemon: the daemon still serves Tier-1 / Tier-2
/// facts for the affected language, so promoting this to `Fail`
/// would be misleading.
fn binary_check(
    name: &str,
    resolved: Option<PathBuf>,
    missing_detail: &str,
    remediation: &str,
) -> DoctorCheck {
    match resolved {
        Some(path) => doctor_check(
            name,
            DoctorStatus::Pass,
            Some(path.to_string_lossy().to_string()),
            None,
        ),
        None => doctor_check(
            name,
            DoctorStatus::Warn,
            Some(missing_detail.into()),
            Some(remediation.into()),
        ),
    }
}

fn resolve_rust_analyzer() -> Option<PathBuf> {
    discover_lsp_binary("rust-analyzer", Some("RUST_ANALYZER"))
}

fn resolve_pyright() -> Option<PathBuf> {
    discover_lsp_binary("pyright-langserver", Some("PYRIGHT"))
}

fn resolve_gopls() -> Option<PathBuf> {
    discover_lsp_binary("gopls", Some("GOPLS"))
}

fn resolve_clangd() -> Option<PathBuf> {
    discover_lsp_binary("clangd", Some("CLANGD"))
}

fn resolve_typescript_language_server() -> Option<PathBuf> {
    discover_lsp_binary(
        "typescript-language-server",
        Some("TYPESCRIPT_LANGUAGE_SERVER"),
    )
}

fn resolve_csharp_ls() -> Option<PathBuf> {
    discover_lsp_binary("csharp-ls", Some("CSHARP_LS"))
}

fn resolve_phpantom_lsp() -> Option<PathBuf> {
    discover_lsp_binary_candidates(&["phpantom_lsp", "phpantom-lsp"], Some("PHPANTOM_LSP"))
}

fn resolve_jdtls() -> Option<PathBuf> {
    discover_lsp_binary("jdtls", Some("JDTLS"))
}

fn resolve_kotlin_language_server() -> Option<PathBuf> {
    discover_lsp_binary("kotlin-language-server", Some("KOTLIN_LANGUAGE_SERVER"))
}

fn resolve_ruby_lsp() -> Option<PathBuf> {
    discover_lsp_binary("ruby-lsp", Some("RUBY_LSP"))
}

fn resolve_sourcekit_lsp() -> Option<PathBuf> {
    discover_sourcekit_lsp()
}

/// Surface analyzer-revision drift as a doctor warning, even after the
/// startup hook has already enqueued reruns. This is the shadow-case
/// fallback: if `staleness::check_revision_staleness_and_enqueue`
/// failed at boot (DB error, JobManager full, etc.), the
/// `workspace_analysis_runs.analyzer_revision` column still records
/// the old value and the operator sees it here. Empty
/// `stale_revisions` means everything matches `expected_analyzer.revision()`
/// at probe time.
fn revision_stale_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
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
fn parser_revision_stale_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
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

/// v0.7.0 C PR — cross-references drift detection (`stale_revisions`
/// and `stale_parser_revisions`) against the actual state of
/// `workspace_analysis_runs` for the alias's current tentative
/// manifest. Surfaces the **post-enqueue lifecycle** the operator
/// otherwise has to reconstruct from logs:
///
/// - **Case A (`Fail`, drift + run at expected revision succeeded)**:
///   the drift detector reports the alias as still stale, yet the
///   corresponding `workspace_analysis_runs` row is `succeeded` at
///   the current build's revision. For analyzer-revision drift this
///   contradicts the single-row `(manifest_id, analyzer_id)` PK and
///   should never fire under v0.7.0 invariants — surfacing it
///   defensively catches a future refactor that breaks the
///   classifier. For parser-revision drift this is the D PR silent
///   data loss class's observability safety net: analyzer runs all
///   succeeded at their expected revision, yet `blobs.parser_revision`
///   still mismatches. The reindex chain
///   (`scanner` → `enqueue_full_repo_reindex` → `register_repo_inner`
///   → pre-publication Tier-1 parse) wrote the new analyzer rows without
///   updating the parser layer, which means a transiently-inaccessible
///   worktree (D PR's bug class) leaked through.
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
/// Each emitted check carries a remediation string with explicit
/// operator next-steps (R1 MF-3). The parser-drift evaluation walks
/// every analyzer in `expected_tier3_analyzer_ids` so a mixed
/// state — e.g. analyzer A succeeded, analyzer B failed — surfaces
/// the failure rather than misclassifying as Case A on the succeeded
/// half (R2 must-add detail B).
fn analyzer_rerun_health_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
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
                        "Run `cairn ctl repo reindex {alias}` to recover (legacy state from before v0.7.0 D PR ship is possible). If this recurs after a fresh reindex with the D PR binary, it is a structural bug — please file an issue.",
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
/// - **Fallback** (`Pass`) — the case matrix did not match, e.g.
///   parser drift with no expected analyzer to cross-reference
///   (Tier-1-only language) or a row with an unknown status string.
///   The plain [`parser_revision_stale_checks`] `Warn` still carries
///   the operator surface in the drift case.
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
                        // R2 catch (PR #228 v1): `succeeded` alone is
                        // not "current succeeded" — the revision must
                        // match the current build's expectation. A
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
                        // R2 v0.7.0 finding #5: a `queued` / `running`
                        // row at an OLDER revision (or with no expected
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
    // revision, yet parser drift remains. This is the D PR silent
    // data loss class observability safety net.
    if every_succeeded_at_current {
        return doctor_check(
            name,
            DoctorStatus::Fail,
            Some(format!(
                "every expected analyzer succeeded at its current revision but parser drift remains ({parser_summary}) — the parser-drift / full-reindex chain is broken (analyzer succeeded but parser_revision was not updated)",
            )),
            Some(format!(
                "Run `cairn ctl repo reindex {alias}` to recover (legacy state from before v0.7.0 D PR ship is possible). If this recurs after a fresh reindex with the D PR binary, it is a structural bug — please file an issue.",
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
    // at a stale (or unknown) revision. R2 v0.7.0 finding #5: this
    // would otherwise be masked by Case C Pass because the prior
    // implementation lumped all pending into a single flag without
    // checking the row's `analyzer_revision`. A stale pending row is
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
    // Case D — at least one analyzer row missing.
    // R2 v0.7.0 finding #5: must run BEFORE Case C Pass so a mixed
    // (pending-current + missing-row) state surfaces as Warn rather
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
    // was dropped before the worker landed it (R2 catch: this state
    // would otherwise be misclassified as the safety-net Case A
    // because all rows are `succeeded`). Must run BEFORE Case C Pass
    // (R2 v0.7.0 finding #5) so a mixed pending-current + stale-
    // succeeded state surfaces as Warn rather than masked by Pass.
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

fn tier3_run_checks(probes: &[AliasStoreProbe]) -> Vec<DoctorCheck> {
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
    // progress" forever.
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

/// Cross-references the build-time `EXPECTED_BACKEND_CRATES`
/// manifest (generated by build.rs from workspace Cargo metadata)
/// against the runtime-linked language backends and workspace
/// analyzers. Any expected crate whose runtime id is absent from
/// its target registry surfaces as `Warn`, not `Fail`: dev builds
/// legitimately omit backends (feature flags, custom `main.rs`),
/// and the remediation names the exact import symbol that is most
/// likely missing from `crates/cairn/src/main.rs`.
fn backend_registration_coherence_check(
    language_backend_names: &[&str],
    workspace_analyzer_ids: &[&str],
) -> DoctorCheck {
    let missing = EXPECTED_BACKEND_CRATES
        .iter()
        .filter(|expected| match expected.registry {
            ExpectedRegistry::LanguageBackend => {
                !language_backend_names.contains(&expected.runtime_id)
            }
            ExpectedRegistry::WorkspaceAnalyzer => {
                !workspace_analyzer_ids.contains(&expected.runtime_id)
            }
        })
        .collect::<Vec<_>>();

    if missing.is_empty() {
        return doctor_check(
            "backend registration coherence",
            DoctorStatus::Pass,
            Some(format!(
                "{} runtime backend crate(s) registered",
                EXPECTED_BACKEND_CRATES.len()
            )),
            None,
        );
    }

    doctor_check(
        "backend registration coherence",
        DoctorStatus::Warn,
        Some(
            missing
                .into_iter()
                .map(|expected| {
                    format!(
                        "{} is declared for runtime linking but `{}` is missing from {} - likely missing `{}` in crates/cairn/src/main.rs",
                        expected.crate_name,
                        expected.runtime_id,
                        expected.registry.label(),
                        expected.import_hint
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
        ),
        None,
    )
}

impl ExpectedRegistry {
    fn label(self) -> &'static str {
        match self {
            Self::LanguageBackend => "LANGUAGE_BACKENDS",
            Self::WorkspaceAnalyzer => "WORKSPACE_ANALYZERS",
        }
    }
}

// ─── PR3 Phase 4: reconcile-state doctor group ────────────────

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
                checks.extend(classify_reconcile_state(&label, &state, now_ns));
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
    now_ns: i64,
) -> Vec<DoctorCheck> {
    let mut out = Vec::new();

    // Fail-closed on impossible invariant relationships (PR3
    // Phase 1's helper WHERE + affected-rows contract should
    // make these unreachable, but doctor is the operator's
    // safety net).
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
    // the manager didn't pick it up. This is the "stuck backlog"
    // case; MF-6.
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
    use crate::cas::registry;
    use crate::paths::CasDataDir;
    use crate::watcher::WatchManager;
    use cairn_watch::WatchBackend;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Notify;

    const RUST_ANALYZER_ID: &str = "rust-analyzer-lsp";

    #[test]
    fn backend_registration_coherence_passes_when_expected_entries_are_registered() {
        let language_backends = [
            "rust",
            "python",
            "markdown",
            "ruby",
            "typescript",
            "go",
            "csharp",
            "php",
            "kotlin",
            "swift",
            "objc",
            "c",
            "cpp",
            "java",
        ];
        let workspace_analyzers = [
            "clangd-c-lsp",
            "clangd-cpp-lsp",
            "clangd-objc-lsp",
            "csharp-ls",
            "csharp-resolver",
            "gopls-lsp",
            "javascript-resolver",
            "jdtls-lsp",
            "kotlin-language-server",
            "kotlin-resolver",
            "php-resolver",
            "phpantom-lsp",
            "pyright-lsp",
            "python-resolver",
            "ruby-lsp",
            "ruby-resolver",
            "rust-analyzer-lsp",
            "sourcekit-lsp",
            "swift-resolver",
            "typescript-language-server-js-lsp",
            "typescript-language-server-ts-lsp",
            "typescript-language-server-tsx-lsp",
        ];

        let check = backend_registration_coherence_check(&language_backends, &workspace_analyzers);

        assert_eq!(check.status, DoctorStatus::Pass);
    }

    #[test]
    fn dotnet_sdk_root_respects_existing_dotnet_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");

        assert_eq!(
            dotnet_sdk_root(Some(root.clone()), std::iter::empty()),
            Some(root)
        );
    }

    #[test]
    fn dotnet_sdk_root_finds_first_standard_root_with_sdk() {
        let tmp = tempfile::tempdir().unwrap();
        let without_sdk = tmp.path().join("without-sdk");
        let with_sdk = tmp.path().join("with-sdk");
        std::fs::create_dir_all(&without_sdk).unwrap();
        std::fs::create_dir_all(with_sdk.join("sdk")).unwrap();

        assert_eq!(
            dotnet_sdk_root(None, [without_sdk, with_sdk.clone()]),
            Some(with_sdk)
        );
    }

    #[test]
    fn dotnet_sdk_root_is_none_without_env_or_standard_sdk() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dotnet");
        std::fs::create_dir_all(&root).unwrap();

        assert_eq!(dotnet_sdk_root(None, [root]), None);
    }

    #[test]
    fn backend_registration_coherence_warns_for_missing_runtime_entry() {
        let language_backends = [
            "rust", "python", "markdown", "ruby", "go", "csharp", "php", "kotlin", "swift", "objc",
            "c", "cpp", "java",
        ];
        let workspace_analyzers = [
            "clangd-c-lsp",
            "clangd-cpp-lsp",
            "clangd-objc-lsp",
            "gopls-lsp",
            "jdtls-lsp",
            "pyright-lsp",
            "ruby-lsp",
            "rust-analyzer-lsp",
            "sourcekit-lsp",
            "typescript-language-server-js-lsp",
            "typescript-language-server-ts-lsp",
            "typescript-language-server-tsx-lsp",
        ];

        let check = backend_registration_coherence_check(&language_backends, &workspace_analyzers);

        assert_eq!(check.status, DoctorStatus::Warn);
        let detail = check.detail.expect("warning detail");
        assert!(detail.contains("cairn-lang-typescript"));
        assert!(detail.contains("LANGUAGE_BACKENDS"));
        assert!(detail.contains("use cairn_lang_typescript as _;"));
    }

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
    fn tentative_snapshot_checks_cover_present_missing_and_store_error() {
        let probes = vec![
            AliasStoreProbe {
                alias: "ok".into(),
                store_path: PathBuf::from("/tmp/ok/store.db"),
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
                alias: "missing".into(),
                store_path: PathBuf::from("/tmp/missing/store.db"),
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
            Some("no tentative anchor yet (reads will fall back to HEAD)")
        );
        assert!(
            checks[1]
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex")
        );
        assert_eq!(checks[2].status, DoctorStatus::Fail);
        assert_eq!(checks[2].detail.as_deref(), Some("not a database"));
        assert!(
            checks[2]
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo remove")
        );
    }

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
            Some("no tentative anchor yet (reads will fall back to HEAD)")
        );
        assert!(
            tentative
                .remediation
                .as_deref()
                .unwrap()
                .contains("repo reindex")
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

    /// `revision_stale_checks` MUST emit one `Warn` per probe whose
    /// `stale_revisions` is non-empty, with the analyzer id, current
    /// revision, and expected revision surfaced in detail.
    #[test]
    fn revision_stale_checks_emits_warn_with_remediation() {
        let probes = vec![AliasStoreProbe {
            alias: "myrepo".into(),
            store_path: PathBuf::from("/tmp/myrepo/store.db"),
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

    // ───────────────────────────────────────────────────────────────
    // v0.7.0 C PR — analyzer_rerun_health_checks
    // ───────────────────────────────────────────────────────────────
    //
    // Cross-references drift detection (analyzer + parser) against
    // `workspace_analysis_runs` state and surfaces the post-enqueue
    // lifecycle. 10 tests: 4 analyzer-drift cases + 4 parser-drift
    // cases + Case E silence + Case A wording assertion (R1 / R2
    // catches).

    fn analyzer_drift_probe(
        alias: &str,
        analyzer_id: &str,
        expected_rev: u32,
        run: Option<Tier3Run>,
    ) -> AliasStoreProbe {
        AliasStoreProbe {
            alias: alias.into(),
            store_path: PathBuf::from(format!("/tmp/{alias}/store.db")),
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
    /// catches a future refactor that breaks the classifier — but it
    /// is also the D PR safety net analog on the analyzer side.
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
            remediation.contains("legacy state"),
            "Case A remediation must mention legacy-state framing (R1 MF-3 less-aggressive wording): {remediation}"
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
    /// with the "enqueued / dropped / lost" framing and the daemon-
    /// log grep hint (R1 MF-3).
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
    /// the current revision → Case A `Fail`. This is the D PR
    /// silent data loss class observability safety net: the analyzer
    /// chain is green but the parser layer is still stale, so the
    /// full-reindex chain broke somewhere between
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
            "Case A parser-drift detail must call out the chain-bug framing (R2 catch): {detail}"
        );
        assert!(detail.contains("tree-sitter-kotlin"));
        let remediation = checks[0].remediation.as_deref().unwrap_or("");
        assert!(remediation.contains("cairn ctl repo reindex moshi"));
        assert!(remediation.contains("legacy state"));
    }

    /// Test 6: parser drift + mixed analyzer states (one succeeded,
    /// one failed) → Case B `Warn` on the failure, NOT Case A on
    /// the succeeded slice (R2 must-add detail B). The failed
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
    /// one queued) → Case C `Pass` pending (R2 must-add detail B).
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

    /// Test 9 (R2 must-add detail A — Case E): no drift on this
    /// alias produces ZERO `analyzer-rerun health` checks. The
    /// noise-prevention invariant: doctor must not warn on every
    /// alias just because the cross-reference function exists.
    #[test]
    fn no_drift_emits_no_rerun_health_check() {
        let probes = vec![AliasStoreProbe {
            alias: "clean".into(),
            store_path: PathBuf::from("/tmp/clean/store.db"),
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

    /// **R2 must-fix on PR #228 v1**: parser drift + every analyzer
    /// row `succeeded` but at an **older** revision than the current
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

    /// **R2 must-fix on PR #228 v1, cross-emission pin**: the same
    /// probe carries `stale_revisions` (analyzer drift on
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

    // ───────────────────────────────────────────────────────────────
    // v0.7.0 R2 findings #4 / #5 — pin the stale-pending fixes
    // ───────────────────────────────────────────────────────────────
    //
    // The C PR's rerun-health framework treated any `queued` /
    // `running` row as an in-flight current rerun and returned Pass.
    // `JobManager::enqueue_analyzer_run` stamps the current
    // `analyzer_revision` on enqueue, so a pending row at an OLDER
    // revision is NOT an in-flight current rerun — it is a stuck
    // old-binary row, a `restore_from_db` artifact, or a coalesced
    // enqueue. These tests pin that the fixed code surfaces Warn for
    // those cases (analyzer-side: finding #4, parser-side: finding
    // #5) and that mixed parser-drift cases (pending-current plus
    // missing-row or stale-succeeded) are no longer masked by the
    // Case C Pass.

    /// Finding #4 — analyzer drift + `queued` at stale revision MUST
    /// be `Warn`. The prior code matched `("queued" | "running", _)`
    /// unconditionally Pass, hiding stuck old-binary rows.
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

    /// Finding #4 sibling — analyzer drift + `running` at stale
    /// revision MUST be `Warn` (queued/running share the branch).
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

    /// Finding #5 — parser drift cascade: one analyzer queued at the
    /// current revision PLUS another analyzer with no row MUST be
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

    /// Finding #5 — pending-current PLUS another analyzer succeeded
    /// at an OLD revision MUST be `Warn`. The prior cascade returned
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

    /// Finding #5 — single analyzer `queued` at an OLD revision (no
    /// other analyzers) MUST be `Warn`. The prior queued/running
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

    // ─── PR3 Phase 4 doctor reconcile-state suite ─────────────────

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

    fn set_state(
        cas: &CasDataDir,
        repo_hash: &str,
        mutate: impl FnOnce(&rusqlite::Transaction<'_>),
    ) {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        mutate(&tx);
        // Ensure the row exists as v4 always seeds it, but do
        // nothing more here.
        let _ = repo_hash;
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

    /// MF-6 (doctor dirty-gap classification): desired>applied,
    /// no attempt, no retry, old dirty_since -> Warn.
    #[test]
    fn mf6_doctor_dirty_gap_old_since_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let ten_min_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - 10 * 60 * 1_000_000_000;
        set_state(&cas, "h", |tx| {
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

    /// MF-6 fresh dirty gap must not warn — the manager is
    /// expected to pick it up within seconds.
    #[test]
    fn mf6_doctor_dirty_gap_fresh_silent() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        set_state(&cas, "h", |tx| {
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

    /// MF-7 doctor retry/backoff: consecutive_failures>0 +
    /// next_retry_at future + last_error -> Warn.
    #[test]
    fn mf7_doctor_retry_backoff_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, "h", |tx| {
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

    /// MF-8 doctor stuck attempt: attempt_generation Some +
    /// last_attempt_ns older than threshold -> Warn.
    #[test]
    fn mf8_doctor_stuck_attempt_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let long_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - 20 * 60 * 1_000_000_000;
        set_state(&cas, "h", |tx| {
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

    /// MF-8 fresh attempt: recent last_attempt_ns must not warn.
    #[test]
    fn mf8_doctor_fresh_attempt_silent() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        set_state(&cas, "h", |tx| {
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

    /// MF-9 doctor watcher failed -> Warn with watcher_error.
    #[test]
    fn mf9_doctor_watcher_failed_warns() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, "h", |tx| {
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

    /// MF-9 watcher active does not fire.
    #[test]
    fn mf9_doctor_watcher_active_silent() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, "h", |tx| {
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

    /// MF-10 corrupt-invariant fail-closed: applied > desired
    /// should never happen but doctor catches it.
    #[test]
    fn mf10_doctor_applied_over_desired_fails() {
        let (_t, cas) = seeded_cas(&[("demo", "/p", "h")]);
        set_state(&cas, "h", |tx| {
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

    /// MF-11 multi-alias dedup: two aliases share one
    /// `repo_hash`; doctor emits ONE reconcile check per repo,
    /// not two identical alias-level duplicates. The label lists
    /// both aliases.
    #[test]
    fn mf11_doctor_multi_alias_reconcile_dedup() {
        let (_t, cas) = seeded_cas(&[("a", "/p", "h"), ("b", "/p", "h")]);
        let long_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
            - 20 * 60 * 1_000_000_000;
        set_state(&cas, "h", |tx| {
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
