//! `cairn daemon` — long-lived index server.
//!
//! Brings the runtime sockets up with the real MCP and control
//! handlers and installs SIGINT / SIGTERM signal handling that
//! triggers a clean shutdown.

use std::sync::Arc;

use anyhow::Result;
use cairn_core::ctl::CtlHandler;
use cairn_core::daemon::Daemon;
use cairn_core::data_rpc::DataRpc;
use cairn_core::jobs::JobManager;
use cairn_core::paths::CasDataDir;
use cairn_core::reconcile::RepoReconcileManager;
use cairn_core::sockets::SocketPaths;
use cairn_core::watcher::WatchManager;
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
    cas_data_dir.ensure()?;
    info!(root = %cas_data_dir.root().display(), "storage open");
    let job_manager = init_job_manager(cas_data_dir.clone())?;
    job_manager.start_workers();
    let reconcile = RepoReconcileManager::new(cas_data_dir.clone(), Some(job_manager.clone()));
    // Clear any `attempt_generation` left set by a prior crash
    // BEFORE spawning workers or starting watchers — a stale
    // in-flight marker would otherwise block the first real
    // attempt.
    if let Err(err) = reconcile.recover_interrupted_attempts().await {
        tracing::warn!(error = %err, "reconcile: failed to recover interrupted attempts");
    }
    let watch_manager = Arc::new(WatchManager::with_reconcile(
        cas_data_dir.clone(),
        reconcile.clone(),
    ));
    start_registered_watchers(watch_manager.clone()).await;
    // Kick workers for repos whose dirty gap survived the last
    // shutdown so `desired > applied` catches up without waiting
    // for the next watcher event.
    if let Err(err) = reconcile.wake_dirty_repositories().await {
        tracing::warn!(error = %err, "reconcile: failed to wake dirty repositories at startup");
    }

    let shutdown = Arc::new(Notify::new());
    spawn_signal_handler(shutdown.clone());

    let daemon = Daemon {
        paths,
        data_handler: Arc::new(DataRpc::new(cas_data_dir.clone())),
        control_handler: Arc::new(CtlHandler::with_full_context(
            cas_data_dir,
            shutdown.clone(),
            env!("CARGO_PKG_VERSION"),
            Some(watch_manager),
            Some(job_manager.clone()),
            Some(reconcile.clone()),
        )),
        shutdown,
        job_manager: Some(job_manager),
    };
    // Retain the reconcile driver until daemon.run returns so
    // watcher requests keep landing on a live manager.
    let _reconcile_owner = reconcile;
    daemon.run().await?;
    Ok(())
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
fn init_job_manager(cas_data_dir: Arc<CasDataDir>) -> Result<Arc<JobManager>> {
    let job_manager = JobManager::new(cas_data_dir);
    job_manager
        .restore_from_db()
        .map_err(|e| anyhow::anyhow!("failed to restore queued analyzer jobs: {e}"))?;
    Ok(job_manager)
}

async fn start_registered_watchers(watch_manager: Arc<WatchManager>) {
    let result = tokio::task::spawn_blocking(move || {
        if let Err(err) = watch_manager.start_registered() {
            tracing::warn!(error = %err, "failed to start registered repo watchers");
        }
    })
    .await;
    if let Err(err) = result {
        tracing::warn!(error = %err, "registered repo watcher startup task failed");
    }
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
        let err = init_job_manager(cas).err().expect("restore must fail");
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
        assert!(init_job_manager(cas).is_ok());
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

        start_registered_watchers(watch_manager.clone()).await;

        assert!(watch_manager.is_watching_alias("demo"));
    }
}
