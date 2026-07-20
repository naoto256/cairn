//! `cairn daemon` — long-lived index server.
//!
//! Brings the runtime sockets up with the real MCP and control
//! handlers and installs SIGINT / SIGTERM signal handling that
//! triggers a clean shutdown.

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

pub async fn run(args: Args) -> Result<()> {
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

fn join_initialization(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.map_err(|err| anyhow!("daemon initialization task failed: {err}"))?
}

async fn initialize_runtime(
    cas_data_dir: Arc<CasDataDir>,
    shutdown: Arc<Notify>,
    gate: Arc<StartupGate>,
) -> Result<()> {
    cas_data_dir.ensure()?;
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
        let watch_report = start_registered_watchers(watch_manager).await?;
        info!(
            armed = watch_report.armed.len(),
            failed = watch_report.failed.len(),
            "registered repository watcher barrier complete"
        );
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
