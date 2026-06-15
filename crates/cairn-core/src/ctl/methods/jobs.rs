//! `jobs.list` / `jobs.cancel` — inspect and cancel background analyzer jobs.

use cairn_proto::control::{JobsCancelArgs, JobsCancelResult, JobsListArgs, JobsListResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::jobs::JobListOptions;
use crate::workspace_analyzer::RunStatus;
use crate::{Error, Result};

struct JobsList;
struct JobsCancel;

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

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static JOBS_LIST: fn() -> Box<dyn ControlMethod> = || Box::new(JobsList);

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static JOBS_CANCEL: fn() -> Box<dyn ControlMethod> = || Box::new(JobsCancel);
