//! `reindex_repo` — re-run the register flow for an already-registered
//! repo so its store catches up with the current worktree / HEAD.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_proto::control::Ack;
use cairn_proto::methods::ReindexArgs;
use linkme::distributed_slice;
use serde_json::Value;
use tracing::info;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::register::register_repo_force_analyzers as cas_register;
use crate::{Error, Result};

struct ReindexRepo;

#[async_trait::async_trait]
impl ControlMethod for ReindexRepo {
    fn name(&self) -> &'static str {
        "reindex_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: ReindexArgs = parse_params(params)?;
        let cas_data_dir = ctx.cas_data_dir.clone();
        let alias = args.alias.clone();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::InvalidArgument(format!("clock: {e}")))?
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);

        let outcome = tokio::task::spawn_blocking(move || -> Result<_> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &alias)?.ok_or_else(|| {
                Error::RepoNotFound {
                    alias: alias.clone(),
                }
            })?;
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let mut conn = cas_store::open(&store_path)?;
            cas_register(&mut conn, &PathBuf::from(&entry.root_path), now_ns)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("reindex_repo task panicked: {e}")))??;

        info!(
            alias = %args.alias,
            head = %outcome.head_commit,
            blobs_parsed = outcome.blobs_parsed,
            "reindex_repo complete"
        );
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(ReindexRepo);
