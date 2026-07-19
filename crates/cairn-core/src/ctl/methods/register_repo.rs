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
use crate::lifecycle::RegistrationReconcilePolicy;
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
        let mut watcher_error = None;
        let mut arm_receipt = None;
        if let Some(watch_manager) = &ctx.watch_manager {
            match watch_manager.arm_repository_if_absent(repo_hash.clone(), canonical.clone()) {
                Ok(receipt) => arm_receipt = Some(receipt),
                Err(err) => {
                    warn!(
                        alias = %args.alias,
                        error = %err,
                        "register_repo continuing after watcher arm failed"
                    );
                    watcher_error = Some(err.to_string());
                }
            }
        }

        let legacy_publish = lifecycle.is_none();
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

            if legacy_publish {
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
        .unwrap_or_else(|error| Err(Error::internal_task_panic("register_repo", error)));

        let outcome = match work_result {
            Ok(outcome) => {
                if let (Some(lifecycle), Some(permit)) = (&ctx.lifecycle, permit) {
                    let reconcile_policy = if ctx.reconcile.is_some() {
                        RegistrationReconcilePolicy::ImmediateCatchUp
                    } else {
                        RegistrationReconcilePolicy::None
                    };
                    let publication = match lifecycle.publish_registration(
                        permit,
                        &args.alias,
                        args.persistent,
                        now_ns,
                        reconcile_policy,
                    ) {
                        Ok(publication) => publication,
                        Err(error) => {
                            if let (Some(watch_manager), Some(receipt)) =
                                (&ctx.watch_manager, arm_receipt.take())
                            {
                                watch_manager.rollback_arm(receipt);
                            }
                            return Err(error);
                        }
                    };
                    if publication.catch_up_generation.is_some()
                        && let Some(reconcile) = &ctx.reconcile
                    {
                        reconcile.wake_recorded_generation(
                            &publication.repo_hash,
                            Some(args.alias.clone()),
                        );
                    }
                }
                if ctx.watch_manager.is_some()
                    && let Some(reconcile) = &ctx.reconcile
                {
                    let (state, error) = match &watcher_error {
                        Some(error) => (cas_registry::WatcherState::Failed, Some(error.clone())),
                        None => (cas_registry::WatcherState::Active, None),
                    };
                    reconcile
                        .set_watcher_state_by_repo_hash(repo_hash.clone(), state, error)
                        .await?;
                }
                outcome
            }
            Err(err) => {
                if let (Some(watch_manager), Some(receipt)) =
                    (&ctx.watch_manager, arm_receipt.take())
                {
                    watch_manager.rollback_arm(receipt);
                }
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
        let ack = match watcher_error {
            Some(error) => Ack::with_alias_and_watcher_failed(args.alias.clone(), error),
            None => Ack::with_alias(args.alias.clone()),
        };
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use cairn_watch::WatchBackend;
    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::ctl::CtlCtx;
    use crate::lifecycle::RepoLifecycleManager;
    use crate::paths::CasDataDir;
    use crate::reconcile::RepoReconcileManager;
    use crate::watcher::WatchManager;

    #[tokio::test]
    async fn failed_initial_scan_rolls_back_only_its_watcher_arm() {
        let root = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let reconcile =
            RepoReconcileManager::new_with_lifecycle(cas.clone(), None, lifecycle.clone());
        let watchers = Arc::new(WatchManager::with_backend_and_reconcile(
            cas.clone(),
            WatchBackend::Poll,
            reconcile.clone(),
        ));
        let ctx = CtlCtx {
            cas_data_dir: cas.clone(),
            shutdown: Arc::new(Notify::new()),
            watch_manager: Some(watchers.clone()),
            job_manager: None,
            reconcile: Some(reconcile),
            lifecycle: Some(lifecycle),
            version: env!("CARGO_PKG_VERSION"),
            started_at: Instant::now(),
        };
        let canonical = root.path().canonicalize().unwrap();
        let repo_hash = path_hash(&canonical);

        let error = RegisterRepo
            .dispatch(
                &ctx,
                json!({
                    "alias": "not-a-git-repository",
                    "path": canonical,
                }),
            )
            .await
            .expect_err("a directory without a Git repository must fail registration");

        assert!(error.to_string().contains("git"));
        assert!(!watchers.is_watching_repository(&repo_hash));
        let index = cas_registry::open(&cas.index_db_path()).unwrap();
        assert!(
            cas_registry::lookup_repository(&index, &repo_hash)
                .unwrap()
                .is_none(),
            "failed new registration must not leave a canonical owner"
        );
    }
}
