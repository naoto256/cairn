//! `register_repo` — open the CAS store for a worktree, build its
//! initial committed + tentative manifests, parse any new blobs, and
//! seed the HEAD / branch / tentative anchors.

use std::time::{SystemTime, UNIX_EPOCH};

use cairn_proto::control::Ack;
use cairn_proto::methods::RegisterRepoArgs;
use linkme::distributed_slice;
use serde_json::Value;
use tracing::{info, warn};

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::paths::path_hash;
use crate::register::{register_repo as cas_register, register_repo_enqueue_analyzers};
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
        if !canonical.is_dir() {
            return Err(Error::InvalidArgument(format!(
                "repo path is not a directory: {}",
                canonical.display()
            )));
        }
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
        let job_manager = ctx.job_manager.clone();
        let lifecycle = ctx.lifecycle.clone();
        let permit = match &lifecycle {
            Some(lifecycle) => {
                Some(lifecycle.begin_registration(repo_hash.clone(), canonical.clone(), now_ns)?)
            }
            None => None,
        };

        let work_result = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut conn = cas_store::open(&store_path)?;
            let outcome = match job_manager.as_deref() {
                Some(manager) => register_repo_enqueue_analyzers(
                    &mut conn,
                    &alias,
                    &repo_hash_for_index,
                    &worktree,
                    now_ns,
                    manager,
                )?,
                None => cas_register(&mut conn, &worktree, now_ns)?,
            };

            if lifecycle.is_none() {
                // Legacy constructor used by focused tests. Production
                // publishes aliases through RepoLifecycleManager.
                let mut idx = cas_registry::open(&index_path)?;
                let tx = idx.transaction()?;
                cas_registry::upsert(&tx, &alias, &canonical_str, &repo_hash_for_index, now_ns)?;
                tx.commit()?;
            }
            Ok(outcome)
        })
        .await
        .map_err(|e| Error::internal_task_panic("register_repo", e))?;

        let outcome = match work_result {
            Ok(outcome) => {
                if let (Some(lifecycle), Some(permit)) = (&ctx.lifecycle, permit) {
                    lifecycle.publish_registration(permit, &args.alias, args.persistent, now_ns)?;
                }
                outcome
            }
            Err(err) => {
                if let (Some(lifecycle), Some(permit)) = (&ctx.lifecycle, permit) {
                    lifecycle.abort_registration(permit).await?;
                }
                return Err(err);
            }
        };

        info!(
            alias = %args.alias,
            head = %outcome.head_commit,
            branch = ?outcome.branch,
            blobs_parsed = outcome.blobs_parsed,
            "register_repo complete"
        );
        let mut ack = Ack::with_alias(args.alias.clone());
        if let Some(watch_manager) = &ctx.watch_manager {
            if let Err(err) = watch_manager.watch_alias(args.alias.clone(), canonical) {
                warn!(
                    alias = %args.alias,
                    error = %err,
                    "register_repo completed but watcher start failed"
                );
                ack = Ack::with_alias_and_watcher_failed(args.alias.clone(), err.to_string());
            }
        }
        let mut value = serde_json::to_value(ack).unwrap();
        if let serde_json::Value::Object(obj) = &mut value {
            obj.insert(
                "jobs".into(),
                serde_json::to_value(&outcome.analyzer_jobs).unwrap(),
            );
        }
        Ok(value)
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(RegisterRepo);
