//! `remove_repo` — stop the watcher, drop the registry rows, delete
//! the on-disk snapshot files.

use std::path::PathBuf;

use cairn_proto::control::{Ack, RemoveRepoArgs};
use linkme::distributed_slice;
use serde_json::Value;
use tracing::warn;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::{Error, Result, registry_db};

struct RemoveRepo;

#[async_trait::async_trait]
impl ControlMethod for RemoveRepo {
    fn name(&self) -> &'static str {
        "remove_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: RemoveRepoArgs = parse_params(params)?;
        // Stop the live watcher first — inflight events would
        // otherwise race the registry teardown.
        ctx.watcher.stop(&args.alias).await;

        let alias_owned = args.alias.clone();
        let data_dir = ctx.storage.data_dir.clone();
        let result = ctx
            .storage
            .with_registry(move |conn| {
                let Some(repo) = registry_db::find_repo_by_alias(conn, &alias_owned)? else {
                    return Ok::<_, Error>(None);
                };
                let mut snap_paths: Vec<PathBuf> = Vec::new();
                for wt in registry_db::list_worktrees(conn, repo.id)? {
                    for snap in registry_db::list_snapshots(conn, wt.id)? {
                        snap_paths.push(data_dir.snapshot_db_path(
                            &repo.repo_hash,
                            &wt.worktree_hash,
                            &snap.branch,
                        ));
                    }
                }
                registry_db::delete_repo(conn, &alias_owned)?;
                Ok(Some(snap_paths))
            })
            .await?;

        let Some(paths) = result else {
            return Err(Error::InvalidArgument(format!("no repo `{}`", args.alias)));
        };
        for p in paths {
            if let Err(e) = std::fs::remove_file(&p)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %p.display(), error = %e, "failed to remove snapshot file");
            }
        }
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(RemoveRepo);
