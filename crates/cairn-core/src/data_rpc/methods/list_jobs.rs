//! `list_jobs` — read-only background analyzer job inventory for MCP/data clients.

use cairn_proto::methods::{JobEntry, ListJobsArgs, ListJobsResult};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::{Error, Result};

pub struct ListJobs;

#[async_trait::async_trait]
impl DataMethod for ListJobs {
    fn name(&self) -> &'static str {
        "list_jobs"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ListJobsArgs = if params.is_null() {
            ListJobsArgs::default()
        } else {
            parse_params(params)?
        };
        let cas_data_dir = ctx.cas_data_dir.clone();

        let (jobs, capped) = tokio::task::spawn_blocking(move || -> Result<(Vec<_>, bool)> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = match args.repo.as_deref() {
                Some(alias) => {
                    let entry = cas_registry::lookup_by_alias(&index, alias)?.ok_or_else(|| {
                        Error::RepoNotFound {
                            alias: alias.to_string(),
                        }
                    })?;
                    vec![entry]
                }
                None => cas_registry::list_all(&index)?,
            };
            let mut out = Vec::new();
            for entry in entries {
                let conn = cas_store::open(&cas_data_dir.store_db_path(&entry.repo_hash))?;
                out.extend(collect_jobs(
                    &conn,
                    &entry.alias,
                    args.state.as_deref(),
                    args.include_terminal,
                )?);
            }
            out.sort_by_key(|job| std::cmp::Reverse(job.job_id));
            let capped = if let Some(limit) = args.limit {
                let limit = limit as usize;
                if out.len() > limit {
                    out.truncate(limit);
                    true
                } else {
                    false
                }
            } else {
                false
            };
            Ok((out, capped))
        })
        .await
        .map_err(|e| Error::internal_task_panic("list_jobs", e))??;

        Ok(serde_json::to_value(ListJobsResult {
            jobs,
            completeness: if capped {
                cairn_proto::Completeness::partial_truncated("cap")
            } else {
                cairn_proto::Completeness::complete()
            },
            timing: cairn_proto::Timing::default(),
        })
        .unwrap())
    }
}

fn collect_jobs(
    conn: &rusqlite::Connection,
    alias: &str,
    state: Option<&str>,
    include_terminal: bool,
) -> Result<Vec<JobEntry>> {
    let mut sql = String::from(
        "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
           AND manifest_id IN (SELECT DISTINCT manifest_id FROM anchors)",
    );
    if !include_terminal {
        sql.push_str(" AND status IN ('queued', 'running')");
    }
    if state.is_some() {
        sql.push_str(" AND status = ?1");
    }
    sql.push_str(" ORDER BY job_id DESC");
    let mut stmt = conn.prepare(&sql)?;
    match state {
        Some(state) => Ok(stmt
            .query_map(params![state], |r| job_entry(alias, r))?
            .collect::<rusqlite::Result<Vec<_>>>()?),
        None => Ok(stmt
            .query_map([], |r| job_entry(alias, r))?
            .collect::<rusqlite::Result<Vec<_>>>()?),
    }
}

fn job_entry(alias: &str, r: &rusqlite::Row<'_>) -> rusqlite::Result<JobEntry> {
    let status: String = r.get(2)?;
    let started_at: i64 = r.get(3)?;
    let finished_at: Option<i64> = r.get(4)?;
    let run_ms = finished_at
        .and_then(|finished| finished.checked_sub(started_at))
        .and_then(|delta| u64::try_from(delta / 1_000_000).ok())
        .unwrap_or_default();
    Ok(JobEntry {
        job_id: r.get(0)?,
        alias: alias.to_string(),
        analyzer_id: r.get(1)?,
        state: status.clone(),
        scheduler_state: status,
        pool_group: None,
        queued_ms: 0,
        pool_wait_ms: 0,
        run_ms,
        progress_ticks: 0,
        rate: None,
    })
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(ListJobs);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_rpc::helpers::test_support;

    #[tokio::test]
    async fn list_jobs_filters_by_state_and_repo() {
        let fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        let conn =
            cas_store::open(&fixture.ctx.cas_data_dir.store_db_path(&entry.repo_hash)).unwrap();
        let manifest_id: i64 = conn
            .query_row(
                "SELECT manifest_id FROM anchors WHERE anchor_name = 'HEAD'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "INSERT INTO workspace_analysis_runs
             (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
              started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, 'demo-lsp', 1, 'cfg', 'running', 1000, NULL, NULL, 42, 0)",
            params![manifest_id],
        )
        .unwrap();

        let value = ListJobs
            .dispatch(
                &fixture.ctx,
                serde_json::json!({"repo": "demo", "state": "running"}),
            )
            .await
            .unwrap();
        let result: ListJobsResult = serde_json::from_value(value).unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.jobs[0].job_id, 42);
        assert_eq!(result.jobs[0].state, "running");
        assert_eq!(result.jobs[0].scheduler_state, "running");
    }
}
