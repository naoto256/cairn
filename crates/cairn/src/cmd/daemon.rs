//! `cairn daemon` — long-lived index server.
//!
//! Brings the runtime sockets up with the real MCP and control
//! handlers and installs SIGINT / SIGTERM signal handling that
//! triggers a clean shutdown.
//!
//! # Role split with `cairn_core::daemon`
//!
//! This module is the CLI-side entry point. It resolves paths
//! from CLI flags or platform defaults, raises the file-descriptor
//! soft limit (Unix), installs the signal handler and shutdown
//! `Notify`, binds the UDS listeners via [`InitializingDaemon`],
//! and drives startup-side initialization in a separate task that
//! feeds resources into the [`StartupGate`].
//!
//! Everything inside the daemon — the accept loops, request
//! framing, teardown ordering, and periodic reconcile / staleness
//! machinery — lives in `cairn_core` (`cairn_core::daemon`,
//! `cairn_core::reconcile`, `cairn_core::jobs`,
//! `cairn_core::lifecycle`, ...). This file's job is to assemble
//! those pieces once and hand them to the core run loop.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use cairn_core::ctl::CtlHandler;
use cairn_core::daemon::{
    InitializingDaemon, shutdown_unpublished_resources, spawn_revision_staleness_scan,
};
use cairn_core::data_rpc::DataRpc;
use cairn_core::jobs::JobManager;
use cairn_core::lifecycle::RepoLifecycleManager;
use cairn_core::paths::CasDataDir;
use cairn_core::reconcile::{PeriodicReconcilePolicy, RepoReconcileManager};
use cairn_core::sockets::SocketPaths;
use cairn_core::startup::{ReadyDaemon, StartupGate};
use cairn_core::watcher::{WatchManager, WatchStartupReport};
use cairn_proto::control::{DaemonInitializationDetail, DaemonInitializationPhase};
use clap::Args as ClapArgs;
use tokio::sync::Notify;
use tracing::info;
#[cfg(unix)]
use tracing::warn;

/// Soft `RLIMIT_NOFILE` we try to reach on Unix before opening any
/// sockets or per-repo watchers. Chosen to cover the fanout of
/// UDS clients, LSP children, notify handles, and per-repo SQLite
/// store handles under a realistic multi-repo workload without
/// forcing an unbounded raise. Capped at the process hard limit
/// by [`nofile_raise_target`].
#[cfg(unix)]
const DAEMON_NOFILE_TARGET: u64 = 4096;

/// Clap flags accepted by `cairn daemon`. Both overrides feed the
/// path resolvers used to construct [`SocketPaths`] and
/// [`CasDataDir`]; leaving them `None` falls back to the platform
/// default lookup.
#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Override the runtime directory (otherwise picked from
    /// $XDG_RUNTIME_DIR / ~/Library/Caches).
    #[arg(long)]
    pub runtime_dir: Option<std::path::PathBuf>,

    /// Override the on-disk data directory (otherwise picked from
    /// $XDG_DATA_HOME / ~/Library/Application Support).
    #[arg(long)]
    pub data_dir: Option<std::path::PathBuf>,
}

/// Startup sequence for the daemon subcommand.
///
/// Resolves paths, raises the file-descriptor soft limit, installs
/// the signal handler, binds both UDS listeners via
/// `InitializingDaemon::bind` (so the bind step completes before
/// entering the `select!`; bind failures return from here, not
/// from the accept-loop arm), and spawns the slower runtime
/// bring-up (`initialize_runtime`) concurrently with the accept
/// loops. The `tokio::select!` below reconciles which of the two
/// completes first — either the accept loops stop (shutdown
/// notified) or initialization finishes. Initialization failure
/// signals shutdown to unwind the accept loops; initialization
/// success does not.
pub async fn run(args: Args) -> Result<()> {
    raise_nofile_soft_limit();
    let paths = match args.runtime_dir {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };
    let cas_data_dir = Arc::new(match args.data_dir {
        Some(p) => CasDataDir::with_root(p),
        None => CasDataDir::from_platform_default()?,
    });
    let shutdown = Arc::new(Notify::new());
    spawn_signal_handler(shutdown.clone());
    let gate = StartupGate::new(shutdown.clone(), env!("CARGO_PKG_VERSION"));
    let daemon = InitializingDaemon::bind(paths, gate.clone(), shutdown.clone())?;
    let mut initialization = tokio::spawn(initialize_runtime(cas_data_dir, shutdown.clone(), gate));
    let daemon_run = daemon.run();
    tokio::pin!(daemon_run);

    // Ordering:
    // - If the accept loops return first (shutdown), the
    //   initialization task gets up to 10s to observe shutdown and
    //   drain its own resources; on timeout we abort, and the outer
    //   runtime grace budget may still abandon any residual
    //   blocking work rather than reap it cleanly.
    // - If initialization returns first with an error, we notify
    //   shutdown so the accept loops unwind before we await them.
    //   Initialization success does not notify shutdown.
    let (daemon_result, initialization_result) = tokio::select! {
        daemon_result = &mut daemon_run => {
            let initialization_result = match tokio::time::timeout(
                Duration::from_secs(10),
                &mut initialization,
            ).await {
                Ok(result) => join_initialization(result),
                Err(_) => {
                    initialization.abort();
                    tracing::warn!("startup task did not stop within shutdown grace; runtime shutdown will abandon residual blocking work");
                    Ok(())
                }
            };
            (daemon_result, initialization_result)
        }
        initialization_result = &mut initialization => {
            let initialization_result = join_initialization(initialization_result);
            if initialization_result.is_err() {
                shutdown.notify_waiters();
            }
            (daemon_run.await, initialization_result)
        }
    };

    daemon_result?;
    initialization_result
}

/// Compute the effective soft `RLIMIT_NOFILE` we should attempt to
/// raise to. Returns `None` when no raise is necessary — either the
/// current soft limit already meets [`DAEMON_NOFILE_TARGET`] (or the
/// hard-capped target), or the current limit is unknown so we cannot
/// prove a raise is needed. When the hard limit is known, the
/// requested value is clamped to it to avoid `setrlimit` `EPERM`.
#[cfg(unix)]
fn nofile_raise_target(current: Option<u64>, maximum: Option<u64>) -> Option<u64> {
    let target = maximum.map_or(DAEMON_NOFILE_TARGET, |hard| hard.min(DAEMON_NOFILE_TARGET));
    match current {
        Some(current) if current < target => Some(target),
        Some(_) | None => None,
    }
}

/// Best-effort raise of the soft `RLIMIT_NOFILE` for this process
/// before any sockets or watchers open. Failure is logged and
/// swallowed: the daemon continues under the original limit, and
/// per-repo watcher / SQLite store code paths surface fd
/// exhaustion at their own boundaries.
#[cfg(unix)]
fn raise_nofile_soft_limit() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    let limit = getrlimit(Resource::Nofile);
    let Some(target) = nofile_raise_target(limit.current, limit.maximum) else {
        return;
    };
    match setrlimit(
        Resource::Nofile,
        Rlimit {
            current: Some(target),
            maximum: limit.maximum,
        },
    ) {
        Ok(()) => info!(
            previous = ?limit.current,
            current = target,
            hard = ?limit.maximum,
            "raised daemon file-descriptor soft limit"
        ),
        Err(err) => warn!(
            previous = ?limit.current,
            target,
            hard = ?limit.maximum,
            error = %err,
            "failed to raise daemon file-descriptor soft limit; continuing"
        ),
    }
}

/// No-op on non-Unix targets; there is no portable `RLIMIT_NOFILE`
/// equivalent to raise from process code.
#[cfg(not(unix))]
fn raise_nofile_soft_limit() {}

/// Flatten a joined initialization task result: a `JoinError`
/// (panic / cancellation) becomes a startup error, otherwise the
/// task's own `Result` is propagated unchanged.
fn join_initialization(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.map_err(|err| anyhow!("daemon initialization task failed: {err}"))?
}

/// Bring the daemon runtime up phase-by-phase and either publish
/// the [`ReadyDaemon`] bundle through `gate.publish_ready` or, on
/// an error after the bundle has been assembled, dismantle the
/// partially built resources with `shutdown_unpublished_resources`
/// before returning the error.
///
/// Each `gate.advance` call moves the reported initialization phase
/// forward so a `status` RPC arriving during startup can describe
/// what the daemon is doing. The order of construction is
/// load-bearing: `lifecycle` seeds the sweep and owns the intent
/// task, `job_manager` restores queued jobs before workers start,
/// `reconcile` and `watch_manager` bind against the lifecycle, and
/// only then does the ready bundle get published atomically.
async fn initialize_runtime(
    cas_data_dir: Arc<CasDataDir>,
    shutdown: Arc<Notify>,
    gate: Arc<StartupGate>,
) -> Result<()> {
    cas_data_dir.ensure()?;
    cairn_core::lsp::initialize_daemon_process_owner(cas_data_dir.root())?;
    info!(root = %cas_data_dir.root().display(), "storage open");
    gate.advance(
        DaemonInitializationPhase::RepositoryLifecycle,
        Some(DaemonInitializationDetail::SweepingRepositories),
    )?;
    let lifecycle = RepoLifecycleManager::new(cas_data_dir.clone());
    let sweep = lifecycle.startup_sweep().await?;
    info!(
        removed = sweep.repositories_removed.len(),
        active = sweep.repositories_active.len(),
        degraded = sweep.repositories_degraded.len(),
        cleanup_retried = sweep.cleanup_retried.len(),
        "repository lifecycle startup sweep complete"
    );

    gate.advance(
        DaemonInitializationPhase::JobManager,
        Some(DaemonInitializationDetail::RestoringJobs),
    )?;
    let job_manager = init_job_manager(cas_data_dir.clone(), lifecycle.clone())?;
    gate.advance(
        DaemonInitializationPhase::JobManager,
        Some(DaemonInitializationDetail::StartingJobWorkers),
    )?;
    job_manager.start_workers();
    let reconcile = RepoReconcileManager::new_with_lifecycle(
        cas_data_dir.clone(),
        Some(job_manager.clone()),
        lifecycle.clone(),
    );
    let watch_manager = Arc::new(WatchManager::with_reconcile(
        cas_data_dir.clone(),
        reconcile.clone(),
    ));
    let resources = ReadyDaemon {
        data_handler: Arc::new(DataRpc::with_lifecycle(
            cas_data_dir.clone(),
            Some(lifecycle.clone()),
        )),
        control_handler: Arc::new(CtlHandler::with_full_context(
            cas_data_dir,
            shutdown,
            env!("CARGO_PKG_VERSION"),
            Some(watch_manager.clone()),
            Some(job_manager.clone()),
            Some(reconcile.clone()),
            Some(lifecycle.clone()),
        )),
        job_manager: job_manager.clone(),
        reconcile: reconcile.clone(),
        lifecycle: lifecycle.clone(),
        watch_manager: watch_manager.clone(),
    };

    let initialized = async {
        gate.advance(
            DaemonInitializationPhase::ReconcileRecovery,
            Some(DaemonInitializationDetail::RecoveringReconcileAttempts),
        )?;
        let recovered = reconcile
            .recover_interrupted_attempts_without_wake()
            .await?;
        gate.advance(
            DaemonInitializationPhase::ReconcileRecovery,
            Some(DaemonInitializationDetail::BindingRuntimeManagers),
        )?;
        lifecycle.bind_runtime(
            Arc::downgrade(&job_manager),
            Arc::downgrade(&watch_manager),
            Arc::downgrade(&reconcile),
        )?;
        gate.advance(
            DaemonInitializationPhase::WatcherBarrier,
            Some(DaemonInitializationDetail::ArmingRegisteredWatchers),
        )?;
        start_registered_watchers(watch_manager).await?;
        gate.advance(
            DaemonInitializationPhase::ReconcilePrime,
            Some(DaemonInitializationDetail::RecordingStartupGenerations),
        )?;
        let startup = reconcile.prime_startup_reconcile(recovered).await?;
        info!(
            recovered = startup.recovered.len(),
            primed = startup.primed.len(),
            "startup full reconcile generations recorded"
        );
        gate.advance(
            DaemonInitializationPhase::PeriodicScheduler,
            Some(DaemonInitializationDetail::StartingPeriodicReconcile),
        )?;
        reconcile.start_periodic_reconcile(PeriodicReconcilePolicy::default())?;
        Result::<()>::Ok(())
    }
    .await;

    if let Err(err) = initialized {
        shutdown_unpublished_resources(resources)
            .await
            .context("cleaning up failed daemon initialization")?;
        return Err(err);
    }

    let staleness_jobs = job_manager;
    let staleness_reconcile = reconcile;
    match gate.publish_ready(resources) {
        Ok(()) => {
            spawn_revision_staleness_scan(staleness_jobs, Some(staleness_reconcile));
            info!("daemon initialization complete");
            Ok(())
        }
        Err(resources) => {
            shutdown_unpublished_resources(resources)
                .await
                .context("cleaning up daemon initialization after shutdown")?;
            Ok(())
        }
    }
}

/// Install a background task that watches for SIGINT / SIGTERM
/// and, on either signal, calls `notify_waiters` on the shared
/// shutdown `Notify`. Registration failures degrade to a warning
/// and no signal-driven shutdown — the operator can still stop the
/// daemon via `cairn ctl daemon shutdown` over the control socket.
fn spawn_signal_handler(shutdown: Arc<Notify>) {
    tokio::spawn(async move {
        let mut sigint =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to install SIGINT handler");
                    return;
                }
            };
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to install SIGTERM handler");
                    return;
                }
            };
        tokio::select! {
            _ = sigint.recv()  => info!("SIGINT received; shutting down"),
            _ = sigterm.recv() => info!("SIGTERM received; shutting down"),
        }
        shutdown.notify_waiters();
    });
}

/// Construct the `JobManager` and run `restore_from_db` up front.
/// `restore_from_db` is load-bearing: it seeds the daemon-global
/// `JobId` allocator above every store's historical + tombstoned
/// max, recycles cross-store collisions, and reserves tracked keys
/// / `JobIndex` for still-active rows. Continuing after a restore
/// failure would leave the allocator unseeded against persisted
/// ids and omit active rows from `JobIndex` / `TrackedJobKeys`, so
/// later enqueues or `cancel(job_id)` calls could collide with or
/// misroute onto a still-live sibling. Fail closed so the
/// supervisor (systemd / launchd / operator) surfaces the failure
/// and the DB state can be repaired before the daemon comes up.
fn init_job_manager(
    cas_data_dir: Arc<CasDataDir>,
    lifecycle: Arc<RepoLifecycleManager>,
) -> Result<Arc<JobManager>> {
    let job_manager = JobManager::with_lifecycle(cas_data_dir, lifecycle);
    job_manager
        .restore_from_db()
        .map_err(|e| anyhow::anyhow!("failed to restore queued analyzer jobs: {e}"))?;
    Ok(job_manager)
}

/// Arm watchers for every already-registered repository on a
/// blocking thread. `WatchManager::start_registered` opens the
/// registry DB and installs notify handles, both of which can
/// block on filesystem I/O, so it must not run on the async
/// runtime's worker threads. A join failure (panic / cancel) or
/// startup failure both surface as a startup error.
async fn start_registered_watchers(watch_manager: Arc<WatchManager>) -> Result<WatchStartupReport> {
    tokio::task::spawn_blocking(move || watch_manager.start_registered())
        .await
        .map_err(|err| anyhow::anyhow!("registered repo watcher startup task failed: {err}"))?
        .map_err(|err| anyhow::anyhow!("failed to start registered repo watchers: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::cas::registry as cas_registry;
    use cairn_core::paths::{CasDataDir, path_hash};

    #[cfg(unix)]
    #[test]
    fn nofile_target_raises_only_to_available_headroom() {
        assert_eq!(nofile_raise_target(Some(256), Some(8192)), Some(4096));
        assert_eq!(nofile_raise_target(Some(256), Some(1024)), Some(1024));
        assert_eq!(nofile_raise_target(Some(4096), Some(8192)), None);
        assert_eq!(nofile_raise_target(None, None), None);
    }

    #[test]
    fn init_job_manager_propagates_restore_failure() {
        // Fail-closed contract: if `restore_from_db` errors, daemon
        // startup must not construct a working `JobManager`.
        // Otherwise `start_workers` would run with an unseeded
        // allocator and an empty `JobIndex` / `TrackedJobKeys`,
        // breaking the global identity invariants restore is
        // responsible for establishing. Trigger the failure by
        // inserting a tombstone at `i64::MAX` — the allocator seed
        // bump then overflows and fails closed.
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        {
            let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::insert_ambiguous_ids(&tx, &[i64::MAX], 1).unwrap();
            tx.commit().unwrap();
        }
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let err = init_job_manager(cas, lifecycle)
            .err()
            .expect("restore must fail");
        assert!(
            format!("{err:#}").contains("failed to restore queued analyzer jobs"),
            "restore failure must be surfaced as a startup error, got {err:#}"
        );
    }

    #[test]
    fn init_job_manager_returns_ok_on_clean_data_dir() {
        // Baseline: on a fresh data dir with no aliases, restore
        // succeeds and `init_job_manager` returns the manager.
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        assert!(init_job_manager(cas, lifecycle).is_ok());
    }

    #[tokio::test]
    async fn start_registered_watchers_waits_until_alias_is_watched() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let canonical = repo.path().canonicalize().unwrap();
        let repo_hash = path_hash(&canonical);
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "demo", &canonical.to_string_lossy(), &repo_hash, 1).unwrap();
        tx.commit().unwrap();

        let watch_manager = Arc::new(WatchManager::new(cas));

        start_registered_watchers(watch_manager.clone())
            .await
            .unwrap();

        assert!(watch_manager.is_watching_alias("demo"));
    }

    #[tokio::test]
    async fn start_registered_watchers_propagates_registry_open_failure() {
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        std::fs::create_dir(cas.index_db_path()).unwrap();
        let watch_manager = Arc::new(WatchManager::new(cas));

        let err = start_registered_watchers(watch_manager)
            .await
            .expect_err("registry open failure must fail the startup barrier");

        assert!(format!("{err:#}").contains("failed to start registered repo watchers"));
    }

    #[tokio::test]
    async fn fresh_initialization_publishes_one_ready_bundle() {
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        let shutdown = Arc::new(Notify::new());
        let gate = StartupGate::new(shutdown.clone(), "test-version");

        initialize_runtime(cas, shutdown, gate.clone())
            .await
            .unwrap();

        assert!(gate.status().is_ready());
        let resources = gate
            .begin_shutdown()
            .expect("ready resources were not published");
        assert!(gate.begin_shutdown().is_none());
        shutdown_unpublished_resources(resources).await.unwrap();
    }
}
