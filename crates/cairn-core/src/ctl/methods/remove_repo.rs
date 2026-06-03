//! `remove_repo` — drop the alias entry and delete the per-repo
//! CAS store on disk.

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
        let cas_data_dir = ctx.cas_data_dir.clone();
        let alias = args.alias.clone();

        let removed = tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let Some(entry) = cas_registry::lookup_by_alias(&index, &alias)? else {
                return Ok(false);
            };
            let tx = index.transaction()?;
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
        .map_err(|e| Error::InvalidArgument(format!("remove_repo task panicked: {e}")))??;

        if !removed {
            return Err(Error::RepoNotFound {
                alias: args.alias.clone(),
            });
        }
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(RemoveRepo);
