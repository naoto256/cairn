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
    let job_manager = JobManager::new(cas_data_dir.clone());
    if let Err(err) = job_manager.restore_from_db() {
        tracing::warn!(error = %err, "failed to restore queued analyzer jobs");
    }
    job_manager.start_workers();
    let watch_manager = Arc::new(WatchManager::with_jobs(
        cas_data_dir.clone(),
        job_manager.clone(),
    ));
    start_registered_watchers(watch_manager.clone()).await;

    let shutdown = Arc::new(Notify::new());
    spawn_signal_handler(shutdown.clone());

    let daemon = Daemon {
        paths,
        data_handler: Arc::new(DataRpc::new(cas_data_dir.clone())),
        control_handler: Arc::new(CtlHandler::with_watch_manager_and_jobs(
            cas_data_dir,
            shutdown.clone(),
            env!("CARGO_PKG_VERSION"),
            Some(watch_manager),
            Some(job_manager.clone()),
        )),
        shutdown,
        job_manager: Some(job_manager),
    };
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
