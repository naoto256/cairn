//! `register_repo` — open the CAS store for a worktree, build its
//! initial committed + tentative manifests, parse any new blobs, and
//! seed the HEAD / branch / tentative anchors.

use std::time::{SystemTime, UNIX_EPOCH};

use cairn_proto::control::Ack;
use cairn_proto::methods::RegisterRepoArgs;
use linkme::distributed_slice;
use serde_json::Value;
use tracing::info;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::paths::path_hash;
use crate::register::register_repo as cas_register;
use crate::{Error, Result};

struct RegisterRepo;

#[async_trait::async_trait]
impl ControlMethod for RegisterRepo {
    fn name(&self) -> &'static str {
        "register_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: RegisterRepoArgs = parse_params(params)?;
        let path = std::path::PathBuf::from(&args.path);
        let canonical = std::fs::canonicalize(&path)
            .map_err(|e| Error::InvalidArgument(format!("canonicalize {}: {e}", args.path)))?;
        let repo_hash = path_hash(&canonical);

        let cas_data_dir = ctx.cas_data_dir.clone();
        cas_data_dir
            .ensure()
            .map_err(|e| Error::InvalidArgument(format!("cas data dir: {e}")))?;
        let store_path = cas_data_dir.store_db_path(&repo_hash);

        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::InvalidArgument(format!("clock: {e}")))?
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);

        let alias = args.alias.clone();
        let worktree = canonical.clone();
        let canonical_str = canonical.to_string_lossy().to_string();
        let repo_hash_for_index = repo_hash.clone();
        let index_path = cas_data_dir.index_db_path();

        let outcome = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut conn = cas_store::open(&store_path)?;
            let outcome = cas_register(&mut conn, &worktree, now_ns)?;

            // Update the top-level alias → repo index so queries can
            // resolve `repo=<alias>`.
            let mut idx = cas_registry::open(&index_path)?;
            let tx = idx.transaction()?;
            cas_registry::upsert(&tx, &alias, &canonical_str, &repo_hash_for_index, now_ns)?;
            tx.commit()?;
            Ok(outcome)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("register_repo task panicked: {e}")))??;

        info!(
            alias = %args.alias,
            head = %outcome.head_commit,
            branch = ?outcome.branch,
            blobs_parsed = outcome.blobs_parsed,
            "register_repo complete"
        );
        if let Some(watch_manager) = &ctx.watch_manager {
            watch_manager.watch_alias(args.alias.clone(), canonical)?;
        }
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(RegisterRepo);
