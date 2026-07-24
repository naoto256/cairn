//! `reindex_repo` — force a fresh reindex pass for a registered
//! repository.
//!
//! With a reconcile driver wired (production path), this handler
//! records durable force intent via
//! [`RepoReconcileManager::request_force_by_alias`] and returns
//! immediately with the accepted generation; the worker executes
//! the register/analyzer enqueue asynchronously. The old
//! synchronous inline path is retained as a fallback for
//! reconcile-less setups (tests / degraded startup).
//!
//! # Wire shape
//!
//! Both paths return an `Ack` augmented with two extra fields:
//!
//! - `jobs` — always present for backwards compatibility with
//!   pre-Phase-2 clients. On the production (reconcile-backed)
//!   path it is an empty array, because the worker enqueues the
//!   analyzer jobs after the ack has already gone out; on the
//!   inline fallback it carries the numeric `job_id`s enqueued
//!   during this call.
//! - `reconcile` — production only. Carries `repo_hash`, the
//!   post-bump `generation`, `forced: true`, and `scheduled` (true
//!   iff the wake actually kicked or spawned a worker for this
//!   generation).
//!
//! An unknown alias becomes `Error::RepoNotFound` (JSON-RPC
//! `-32001`) in both paths.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_proto::control::Ack;
use cairn_proto::methods::ReindexArgs;
use linkme::distributed_slice;
use serde_json::{Value, json};
use tracing::info;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::reconcile::ReconcileTrigger;
use crate::register::{
    register_repo_force_analyzers as cas_register, register_repo_force_analyzers_enqueue,
};
use crate::{Error, Result};

struct ReindexRepo;

#[async_trait::async_trait]
impl ControlMethod for ReindexRepo {
    fn name(&self) -> &'static str {
        "reindex_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: ReindexArgs = parse_params(params)?;
        let alias = args.alias.clone();

        if let Some(reconcile) = ctx.reconcile.clone() {
            let outcome = reconcile
                .request_force_by_alias(alias.clone(), ReconcileTrigger::ManualReindex)
                .await?;
            info!(
                alias = %alias,
                generation = outcome.generation,
                "reindex_repo scheduled via reconcile manager"
            );
            let mut value = serde_json::to_value(Ack::with_alias(args.alias)).unwrap();
            if let Value::Object(obj) = &mut value {
                obj.insert(
                    "reconcile".into(),
                    json!({
                        "repo_hash": outcome.repo_hash,
                        "generation": outcome.generation,
                        "forced": outcome.forced,
                        "scheduled": outcome.scheduled,
                    }),
                );
                // `jobs` was the pre-Phase-2 field name for the
                // analyzer job list. Preserve as an empty list
                // for wire compatibility; downstream consumers
                // will start reading `reconcile` in Phase 3+.
                obj.insert("jobs".into(), Value::Array(Vec::new()));
            }
            return Ok(value);
        }

        // Fallback path: no reconcile driver (test / degraded
        // startup). Run inline like the pre-Phase-2 behaviour.
        // The enqueue happens synchronously here, so — when a
        // `JobManager` is also present — the outcome carries the
        // enqueued job ids and the reply's `jobs` field is
        // populated. Without a `JobManager` the analyzer callback
        // runs inline and `jobs` degrades to an empty array.
        let cas_data_dir = ctx.cas_data_dir.clone();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::InvalidArgument(format!("clock: {e}")))?
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let job_manager = ctx.job_manager.clone();
        let alias_task = alias.clone();
        let outcome = tokio::task::spawn_blocking(move || -> Result<_> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &alias_task)?.ok_or_else(|| {
                Error::RepoNotFound {
                    alias: alias_task.clone(),
                }
            })?;
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let mut conn = cas_store::open_existing(&store_path)?;
            match job_manager.as_deref() {
                Some(manager) => register_repo_force_analyzers_enqueue(
                    &mut conn,
                    &alias_task,
                    &entry.repo_hash,
                    &PathBuf::from(&entry.root_path),
                    now_ns,
                    manager,
                ),
                None => cas_register(&mut conn, &PathBuf::from(&entry.root_path), now_ns),
            }
        })
        .await
        .map_err(|e| Error::internal_task_panic("reindex_repo", e))??;

        info!(
            alias = %args.alias,
            head = %outcome.head_commit,
            blobs_parsed = outcome.blobs_parsed,
            "reindex_repo complete (inline fallback)"
        );
        let mut value = serde_json::to_value(Ack::with_alias(args.alias)).unwrap();
        if let Value::Object(obj) = &mut value {
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
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(ReindexRepo);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::paths::CasDataDir;
    use crate::reconcile::{RepoReconcileManager, TestRegisterHookFn};

    #[tokio::test]
    async fn production_reindex_with_driver_never_uses_direct_writer_fallback() {
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();

        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "demo", "/not/a/worktree", "hash", 1).unwrap();
        tx.commit().unwrap();

        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        let hook: TestRegisterHookFn = Arc::new(|_, _, _, _| Ok(()));
        reconcile.set_test_register_hook(hook);
        let ctx = CtlCtx {
            cas_data_dir: cas.clone(),
            shutdown: Arc::new(Notify::new()),
            watch_manager: None,
            job_manager: None,
            reconcile: Some(reconcile.clone()),
            lifecycle: None,
            version: "test",
            started_at: Instant::now(),
        };

        let value = ReindexRepo
            .dispatch(&ctx, json!({ "alias": "demo" }))
            .await
            .unwrap();

        assert_eq!(value["reconcile"]["repo_hash"], "hash");
        assert_eq!(value["reconcile"]["forced"], true);
        assert_eq!(value["jobs"], json!([]));
        assert!(
            !cas.store_db_path("hash").exists(),
            "the inline fallback must not open or create a store when a driver exists"
        );

        reconcile.shutdown(Duration::from_secs(2)).await;
    }
}
