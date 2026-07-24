//! `register_repo` — open the CAS store for a worktree, build its
//! initial committed + tentative manifests, parse any new blobs, and
//! seed the HEAD / branch / tentative anchors.
//!
//! # Lifecycle contract
//!
//! With a `RepoLifecycleManager` wired (production path) the
//! handler acquires a `RegistrationPermit` before doing any work
//! and releases it either by publishing (success) or aborting
//! (failure). The permit fences the repo against `remove_repo` —
//! removal may close admission, but destructive cleanup waits for
//! this lease to drain — and does *not* serialise concurrent
//! `register_repo` calls for the same `repo_hash`; several leases
//! can coexist. If the repo is currently mid-removal or has a
//! pending cleanup, `begin_registration` fails fast with
//! `RepositoryUnavailable`.
//!
//! # Ordering
//!
//! The steps below are run in this order:
//!
//! 1. Canonicalise the input path and derive `repo_hash`.
//! 2. Acquire the lifecycle permit (or none, in fallback mode).
//! 3. Best-effort arm the watcher. A watcher failure is *not* a
//!    registration failure — the ack simply carries the error
//!    string via [`Ack::with_alias_and_watcher_failed`] so the
//!    client sees a degraded but usable repo. The receipt returned
//!    by `arm_repository_if_absent` is retained so a later abort
//!    can precisely roll back only *this* arm.
//! 4. On a blocking task, run the CAS scan and manifest build.
//! 5. On success, publish the alias through the lifecycle manager
//!    (or, in the legacy path, upsert directly), optionally wake
//!    the reconcile driver on the just-recorded catch-up
//!    generation, and persist the watcher state.
//! 6. On a *pre-publication* work failure, unwind the watcher arm
//!    first, then abort the registration permit;
//!    [`preserve_registration_error_after_cleanup`] keeps the
//!    original error so cleanup issues are only logged. Errors
//!    beyond that boundary do not roll back — a
//!    `set_watcher_state_by_repo_hash` failure after the alias is
//!    durably published returns the error without rolling back the
//!    already-published alias, and a publish failure whose
//!    lifecycle abort also fails collapses into an
//!    `Error::Internal` covering both.
//!
//! # Wire shape
//!
//! The reply is an `Ack` augmented with a `jobs` field that carries
//! the analyzer jobs enqueued during initial scan (numeric
//! `job_id`s). Callers that lack a `JobManager` see an empty list.
//!
//! [`Ack::with_alias_and_watcher_failed`]:
//!     cairn_proto::control::Ack::with_alias_and_watcher_failed

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

/// Run a cleanup future after a registration failure and always
/// return the *original* registration error — never the cleanup
/// error. A cleanup failure is logged with both errors in scope so
/// nothing is silently swallowed, but the client-facing error
/// must remain the one that describes why the registration itself
/// was rejected. See the paired unit test for this contract.
async fn preserve_registration_error_after_cleanup(
    registration_error: Error,
    cleanup: impl std::future::Future<Output = Result<()>>,
) -> Error {
    if let Err(cleanup_error) = cleanup.await {
        warn!(
            error = %registration_error,
            cleanup_error = %cleanup_error,
            "registration cleanup failed; preserving the primary registration error"
        );
    }
    registration_error
}

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

        // When no lifecycle manager is wired, the blocking task
        // must also perform the alias upsert itself — otherwise the
        // registration would succeed without ever making the alias
        // discoverable. Production paths pass this through
        // `RepoLifecycleManager::publish_registration` below.
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
                let tx = idx.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
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
                    // Only record an immediate catch-up generation
                    // when a reconcile driver is actually there to
                    // consume it — recording one without a worker
                    // would leave `desired > applied` durably
                    // pending until a later trigger picks it up.
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
                            // Publish failed after the scan
                            // succeeded — the on-disk store is
                            // committed but the alias never
                            // reached the index, so nothing yet
                            // observes the watcher. Roll it back
                            // before returning so a retry starts
                            // from a clean state.
                            if let (Some(watch_manager), Some(receipt)) =
                                (&ctx.watch_manager, arm_receipt.take())
                            {
                                watch_manager.rollback_arm(receipt);
                            }
                            return Err(error);
                        }
                    };
                    // `catch_up_generation` is only populated when
                    // the policy above was `ImmediateCatchUp`, i.e.
                    // when a reconcile driver exists. The recorded
                    // generation is already durable at this point;
                    // this wake is the sync in-process kick that
                    // causes the reconcile worker to notice it
                    // without waiting for the next trigger.
                    if publication.catch_up_generation.is_some()
                        && let Some(reconcile) = &ctx.reconcile
                    {
                        reconcile.wake_recorded_generation(
                            &publication.repo_hash,
                            Some(args.alias.clone()),
                        );
                    }
                }
                // Persist the observed watcher state onto the
                // reconcile row so `doctor` / `status` can report
                // it later. Uses the exact `watcher_error` captured
                // above so a partial arm failure is not silently
                // upgraded to `Active`.
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
                // Scan failed. Unwind the watcher arm (a plain,
                // in-process operation) before touching the
                // durable lifecycle path so a further cleanup
                // error cannot mask the original registration
                // failure.
                if let (Some(watch_manager), Some(receipt)) =
                    (&ctx.watch_manager, arm_receipt.take())
                {
                    watch_manager.rollback_arm(receipt);
                }
                let err = match (&ctx.lifecycle, permit) {
                    (Some(lifecycle), Some(permit)) => {
                        preserve_registration_error_after_cleanup(
                            err,
                            lifecycle.abort_registration(permit),
                        )
                        .await
                    }
                    _ => err,
                };
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
    async fn abort_cleanup_failure_does_not_mask_registration_error() {
        let error = preserve_registration_error_after_cleanup(
            Error::InvalidArgument("primary registration failure".into()),
            async { Err(Error::Internal("abort cleanup failure".into())) },
        )
        .await;

        assert!(matches!(
            error,
            Error::InvalidArgument(message) if message == "primary registration failure"
        ));
    }

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
