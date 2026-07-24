//! `remove_repo` — drop the alias entry and delete the per-repo
//! CAS store on disk.
//!
//! Two paths coexist: production runs through
//! `RepoLifecycleManager::remove_alias`, which coordinates
//! displacement of any in-flight lease before the delete lands;
//! the legacy fallback is only taken when the daemon was built
//! without a lifecycle (unit tests, minimal setups) and performs
//! the deletion directly against the index. Both paths surface
//! `Error::RepoNotFound` — mapped to JSON-RPC `-32001` — when the
//! alias is unknown. Note that when multiple aliases share one
//! `repo_hash`, only the label row is removed; the on-disk store
//! is torn down solely when the alias being removed was the last
//! one pointing at it.

use cairn_proto::control::{Ack, RemoveRepoArgs};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::warn;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::cas::registry as cas_registry;
use crate::{Error, Result};

struct RemoveRepo;

#[async_trait::async_trait]
impl ControlMethod for RemoveRepo {
    fn name(&self) -> &'static str {
        "remove_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: RemoveRepoArgs = parse_params(params)?;
        if let Some(lifecycle) = &ctx.lifecycle {
            // Production path: the lifecycle manager owns the
            // transition mutex, the watcher unwind, and the
            // best-effort on-disk cleanup. It returns `Ok(false)`
            // only when the alias was not registered — the miss
            // must become an explicit `RepoNotFound` so the client
            // sees a stable JSON-RPC `-32001` rather than a silent
            // success.
            if !lifecycle.remove_alias(&args.alias).await? {
                return Err(Error::RepoNotFound {
                    alias: args.alias.clone(),
                });
            }
            return Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap());
        }
        let cas_data_dir = ctx.cas_data_dir.clone();
        let alias = args.alias.clone();

        // Legacy fallback: no lifecycle manager, so the alias row is
        // deleted directly under an Immediate transaction and the
        // per-repo directory is torn down only when this was the last
        // alias pointing at it.
        let removed = tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let Some(entry) = cas_registry::lookup_by_alias(&index, &alias)? else {
                return Ok(false);
            };
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::delete(&tx, &alias)?;
            tx.commit()?;

            // Only blow the on-disk store away when no other alias
            // still references this repo_hash — multiple labels can
            // share one CAS directory.
            let remaining = cas_registry::count_aliases_for_repo(&index, &entry.repo_hash)?;
            if remaining == 0 {
                let repo_dir = cas_data_dir.repo_dir(&entry.repo_hash);
                if let Err(e) = std::fs::remove_dir_all(&repo_dir)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(path = %repo_dir.display(), error = %e, "failed to remove repo dir");
                }
            }
            Ok(true)
        })
        .await
        .map_err(|e| Error::internal_task_panic("remove_repo", e))??;

        if !removed {
            return Err(Error::RepoNotFound {
                alias: args.alias.clone(),
            });
        }
        if let Some(watch_manager) = &ctx.watch_manager {
            watch_manager.unwatch_alias(&args.alias);
        }
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(RemoveRepo);
