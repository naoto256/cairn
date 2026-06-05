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
    let watch_manager = Arc::new(WatchManager::new(cas_data_dir.clone()));
    spawn_registered_watchers(watch_manager.clone());

    let shutdown = Arc::new(Notify::new());
    spawn_signal_handler(shutdown.clone());

    let daemon = Daemon {
        paths,
        data_handler: Arc::new(DataRpc::new(cas_data_dir.clone())),
        control_handler: Arc::new(CtlHandler::with_watch_manager(
            cas_data_dir,
            shutdown.clone(),
            env!("CARGO_PKG_VERSION"),
            Some(watch_manager),
        )),
        shutdown,
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

fn spawn_registered_watchers(watch_manager: Arc<WatchManager>) {
    tokio::task::spawn_blocking(move || {
        if let Err(err) = watch_manager.start_registered() {
            tracing::warn!(error = %err, "failed to start registered repo watchers");
        }
    });
}
