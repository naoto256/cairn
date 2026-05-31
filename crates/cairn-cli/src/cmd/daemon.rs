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
use cairn_core::indexer::Indexer;
use cairn_core::paths::{CasDataDir, DataDir};
use cairn_core::sockets::SocketPaths;
use cairn_core::storage::Storage;
use cairn_core::watcher::WatcherOrchestrator;
use clap::Args as ClapArgs;
use tokio::sync::Notify;
use tracing::{info, warn};

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
    let data_dir = match args.data_dir.clone() {
        Some(p) => DataDir::with_root(p),
        None => DataDir::from_platform_default()?,
    };
    let cas_data_dir = Arc::new(match args.data_dir {
        Some(p) => CasDataDir::with_root(p),
        None => CasDataDir::from_platform_default()?,
    });

    let storage = Arc::new(Storage::open(data_dir)?);
    info!(root = %storage.data_dir.root().display(), "storage open");

    let indexer = Arc::new(Indexer::with_registered_backends(storage.clone()));
    let watcher = Arc::new(WatcherOrchestrator::new(indexer.clone()));
    resume_watchers(&storage, &indexer, &watcher).await;
    schedule_stale_revision_reindex(indexer.clone()).await;

    let shutdown = Arc::new(Notify::new());
    spawn_signal_handler(shutdown.clone());

    let daemon = Daemon {
        paths,
        data_handler: Arc::new(DataRpc::new(
            storage.clone(),
            indexer.clone(),
            cas_data_dir.clone(),
        )),
        control_handler: Arc::new(CtlHandler::new(
            indexer,
            storage.clone(),
            watcher.clone(),
            cas_data_dir,
            shutdown.clone(),
            env!("CARGO_PKG_VERSION"),
        )),
        shutdown,
    };
    let run_result = daemon.run().await;
    watcher.shutdown().await;
    run_result?;
    Ok(())
}

/// Re-attach watchers to every repo that survived the previous daemon
/// run. Lets the daemon restart pick up live changes without an
/// explicit re-`add-repo` round-trip. Also runs a one-shot snapshot
/// reconcile so branches that were deleted while the daemon was down
/// get their stale snapshots pruned now (the live watcher only sees
/// deletions that happen while it's running).
async fn resume_watchers(storage: &Storage, indexer: &Indexer, watcher: &WatcherOrchestrator) {
    let repos = match storage
        .with_registry(|conn| cairn_core::registry_db::list_repos(conn))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed to enumerate repos for watcher resume");
            return;
        }
    };
    for repo in repos {
        let path = std::path::PathBuf::from(&repo.root_path);
        if !path.is_dir() {
            warn!(alias = %repo.alias, root = %repo.root_path, "repo root missing; skipping watcher");
            continue;
        }
        if let Err(e) = indexer.reconcile_snapshots(&repo.alias).await {
            warn!(alias = %repo.alias, error = %e, "snapshot reconcile failed");
        }
        if let Err(e) = watcher.start(&repo.alias, &path).await {
            warn!(alias = %repo.alias, error = %e, "failed to resume watcher");
        }
    }
}

/// Find snapshots whose recorded `indexer_revision` is older than
/// the current `INDEXER_REVISION` constant and kick off a
/// background `full_index` for each owning alias. The daemon comes
/// up immediately; reindex runs in the background and queries
/// served in the meantime see the old (stale) data — that's
/// acceptable because the wire-level `completeness` field already
/// communicates "best-effort" semantics, and the alternative
/// (blocking startup on a full reindex) is worse for UX.
async fn schedule_stale_revision_reindex(indexer: Arc<cairn_core::indexer::Indexer>) {
    let aliases = match indexer.aliases_with_stale_revision().await {
        Ok(a) => a,
        Err(e) => {
            warn!(error = %e, "could not enumerate stale-revision aliases; skipping auto-reindex");
            return;
        }
    };
    if aliases.is_empty() {
        return;
    }
    info!(
        count = aliases.len(),
        revision = cairn_core::INDEXER_REVISION,
        "scheduling background reindex for snapshots below current indexer revision"
    );
    for alias in aliases {
        let indexer = indexer.clone();
        tokio::spawn(async move {
            info!(alias = %alias, "auto-reindex starting");
            match indexer.full_index(&alias).await {
                Ok(stats) => info!(alias = %alias, ?stats, "auto-reindex complete"),
                Err(e) => warn!(alias = %alias, error = %e, "auto-reindex failed"),
            }
        });
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
