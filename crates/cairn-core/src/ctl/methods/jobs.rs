//! `jobs.list` / `jobs.cancel` / `jobs.prune` — manage background analyzer jobs.

use cairn_proto::control::{
    JobsCancelArgs, JobsCancelResult, JobsListArgs, JobsListResult, JobsPruneArgs,
    JobsPruneRepoEntry, JobsPruneResult,
};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::jobs::JobListOptions;
use crate::workspace_analyzer::RunStatus;
use crate::{Error, Result};

struct JobsList;
struct JobsCancel;
struct JobsPrune;

#[async_trait::async_trait]
impl ControlMethod for JobsList {
    fn name(&self) -> &'static str {
        "jobs.list"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: JobsListArgs = parse_params(params)?;
        let Some(manager) = &ctx.job_manager else {
            return Err(Error::InvalidArgument("job manager unavailable".into()));
        };
        let state = match args.state.as_deref() {
            Some(raw) => Some(
                RunStatus::from_str(raw)
                    .ok_or_else(|| Error::InvalidParams(format!("unknown job state: {raw}")))?,
            ),
            None => None,
        };
        let jobs = manager.jobs(
            args.alias.as_deref(),
            state,
            JobListOptions {
                include_all: args.all,
                limit: args.limit.map(|value| value as usize),
            },
        )?;
        Ok(serde_json::to_value(JobsListResult {
            jobs: jobs
                .into_iter()
                .map(|job| cairn_proto::control::JobSnapshot {
                    job_id: job.job_id,
                    alias: job.alias,
                    analyzer_id: job.analyzer_id,
                    state: job.state,
                    created_at: job.created_at,
                    started_at: job.started_at,
                    finished_at: job.finished_at,
                    error: job.error,
                    pool_group: job.pool_group,
                    scheduler_state: job.scheduler_state,
                    enqueued_at: job.enqueued_at,
                    run_started_at: job.run_started_at,
                    queued_ms: job.queued_ms,
                    pool_wait_ms: job.pool_wait_ms,
                    run_ms: job.run_ms,
                    progress_ticks: job.progress_ticks,
                    last_progress_at: job.last_progress_at,
                    progress_per_minute: job.progress_per_minute,
                })
                .collect(),
        })
        .unwrap())
    }
}

#[async_trait::async_trait]
impl ControlMethod for JobsCancel {
    fn name(&self) -> &'static str {
        "jobs.cancel"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: JobsCancelArgs = parse_params(params)?;
        let Some(manager) = &ctx.job_manager else {
            return Err(Error::InvalidArgument("job manager unavailable".into()));
        };
        let result = manager.cancel(args.job_id)?;
        Ok(serde_json::to_value(JobsCancelResult {
            cancelled: result.cancelled,
            reason: result.reason,
        })
        .unwrap())
    }
}

#[async_trait::async_trait]
impl ControlMethod for JobsPrune {
    fn name(&self) -> &'static str {
        "jobs.prune"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: JobsPruneArgs = parse_params(params)?;
        let Some(manager) = &ctx.job_manager else {
            return Err(Error::InvalidArgument("job manager unavailable".into()));
        };
        let manager = manager.clone();
        let result = tokio::task::spawn_blocking(move || {
            manager.prune_jobs(args.repo.as_deref(), args.dry_run.unwrap_or(false))
        })
        .await
        .map_err(|e| Error::internal_task_panic("jobs.prune", e))??;

        Ok(serde_json::to_value(JobsPruneResult {
            repos: result
                .repos
                .into_iter()
                .map(|repo| JobsPruneRepoEntry {
                    alias: repo.alias,
                    deleted_runs_count: repo.deleted_runs_count,
                    deleted_index_entries_count: repo.deleted_index_entries_count,
                })
                .collect(),
            total_deleted_runs: result.total_deleted_runs,
            total_deleted_index_entries: result.total_deleted_index_entries,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static JOBS_LIST: fn() -> Box<dyn ControlMethod> = || Box::new(JobsList);

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static JOBS_CANCEL: fn() -> Box<dyn ControlMethod> = || Box::new(JobsCancel);

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static JOBS_PRUNE: fn() -> Box<dyn ControlMethod> = || Box::new(JobsPrune);
