//! `cairn ctl` — management CLI.
//!
//! Talks to a running daemon's `control.sock`. Each invocation opens
//! one short-lived UDS connection, sends one newline JSON-RPC
//! request, reads one newline JSON-RPC reply, pretty-prints it, and
//! exits with code 0 on success or 1 on an error response.

use anyhow::{Context, Result, anyhow};
use cairn_core::sockets::SocketPaths;
use cairn_proto::control::{
    DoctorReport, DoctorStatus, JobSummary, JobsCancelResult, JobsListResult, PruneResult,
    StatusReport,
};
use cairn_proto::jsonrpc::{JsonRpcVersion, Request, RequestId, Response};
use clap::{Args as ClapArgs, Subcommand};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::version_guard::{VersionGuardMode, check_daemon_version};

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[command(subcommand)]
    command: CtlCommand,

    /// Override the runtime directory (otherwise picked from
    /// $XDG_RUNTIME_DIR / ~/Library/Caches).
    #[arg(long, global = true)]
    runtime_dir: Option<std::path::PathBuf>,
}

#[derive(Subcommand, Debug)]
enum CtlCommand {
    /// Register a repository so the daemon starts watching and
    /// indexing it.
    RegisterRepo {
        path: std::path::PathBuf,
        #[arg(long)]
        alias: String,
    },
    /// Drop a repository (and its indexes) from the registry.
    RemoveRepo { alias: String },
    /// Show daemon health, registered repos, and snapshot progress.
    Status,
    /// Force a full re-index of `alias`.
    ReindexRepo {
        alias: String,
        /// Wait until queued analyzer jobs reach terminal states.
        #[arg(long)]
        wait: bool,
        /// Maximum seconds to wait with `--wait`.
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// List or cancel background analyzer jobs.
    Jobs {
        /// Restrict listing to one repo alias.
        #[arg(long)]
        alias: Option<String>,
        /// Restrict listing to one state.
        #[arg(long)]
        state: Option<String>,
        /// Include historical jobs from old manifests.
        #[arg(long)]
        all: bool,
        /// Maximum number of rows to print.
        #[arg(long)]
        limit: Option<u32>,
        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
        /// Cancel a job by id.
        #[arg(long)]
        cancel: Option<i64>,
    },
    /// Delete cached blobs whose parser IDs no current backend owns.
    Prune {
        /// Restrict pruning to one registered repo alias.
        #[arg(long)]
        repo: Option<String>,
    },
    /// Diagnose dependencies, paths, and reachability.
    Doctor,
    /// Ask the daemon to shut down.
    Shutdown,
}

pub async fn run(args: Args) -> Result<()> {
    let paths = match args.runtime_dir {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };

    let check_version = !matches!(&args.command, CtlCommand::Shutdown);
    let (method, params, wait_after, json_output) = match args.command {
        CtlCommand::RegisterRepo { path, alias } => {
            let canon = path
                .canonicalize()
                .with_context(|| format!("canonicalize {}", path.display()))?;
            (
                "register_repo",
                json!({"path": canon.to_string_lossy(), "alias": alias}),
                None,
                false,
            )
        }
        CtlCommand::RemoveRepo { alias } => ("remove_repo", json!({"alias": alias}), None, false),
        CtlCommand::Status => ("status", Value::Null, None, false),
        CtlCommand::ReindexRepo {
            alias,
            wait,
            timeout,
        } => (
            "reindex_repo",
            json!({"alias": alias.clone()}),
            wait.then_some((alias, Duration::from_secs(timeout.unwrap_or(u64::MAX)))),
            false,
        ),
        CtlCommand::Jobs {
            alias,
            state,
            all,
            limit,
            json,
            cancel,
        } => match cancel {
            Some(job_id) => ("jobs.cancel", json!({"job_id": job_id}), None, json),
            None => (
                "jobs.list",
                json!({"alias": alias, "state": state, "all": all, "limit": limit}),
                None,
                json,
            ),
        },
        CtlCommand::Prune { repo } => ("prune", json!({"repo": repo}), None, false),
        CtlCommand::Doctor => ("doctor", Value::Null, None, false),
        CtlCommand::Shutdown => ("shutdown", Value::Null, None, false),
    };

    if check_version {
        check_daemon_version(&paths.control, VersionGuardMode::Cli).await?;
    }

    let resp = round_trip(&paths.control, method, params)
        .await
        .with_context(|| format!("talking to {}", paths.control.display()))?;
    if json_output {
        if method == "jobs.list"
            && let Some(value) = &resp.result
            && let Ok(report) = serde_json::from_value::<JobsListResult>(value.clone())
        {
            println!("{}", serde_json::to_string_pretty(&report.jobs).unwrap());
        } else {
            println!("{}", serde_json::to_string_pretty(&resp.result).unwrap());
        }
    } else {
        render(method, &resp);
    }

    if let Some(err) = resp.error {
        Err(anyhow!(err.message))
    } else if let Some((alias, timeout)) = wait_after {
        wait_for_jobs(&paths.control, &alias, timeout).await
    } else {
        Ok(())
    }
}

async fn round_trip(
    socket_path: &std::path::Path,
    method: &str,
    params: Value,
) -> Result<Response> {
    let req = Request {
        jsonrpc: JsonRpcVersion::V2,
        id: RequestId::Number(1),
        method: method.into(),
        params: Some(params),
    };
    let stream = UnixStream::connect(socket_path).await?;
    let (read, mut write) = stream.into_split();
    let mut line = serde_json::to_string(&req)?;
    line.push('\n');
    write.write_all(line.as_bytes()).await?;
    write.flush().await?;
    let mut reader = BufReader::new(read);
    let mut buf = String::new();
    let n = reader.read_line(&mut buf).await?;
    if n == 0 {
        return Err(anyhow!("daemon closed the connection without responding"));
    }
    let resp: Response = serde_json::from_str(buf.trim())
        .with_context(|| format!("parsing response: {}", buf.trim()))?;
    Ok(resp)
}

fn render(method: &str, resp: &Response) {
    if let Some(err) = &resp.error {
        eprintln!("error: {}", err.message);
        return;
    }
    let Some(value) = &resp.result else {
        println!("ok");
        return;
    };
    // Route the result based on which method we called — only
    // `status` and `doctor` have structured payloads worth
    // pretty-printing; everything else is the generic `Ack`.
    match method {
        "status" => {
            if let Ok(report) = serde_json::from_value::<StatusReport>(value.clone()) {
                render_status(&report);
                return;
            }
        }
        "doctor" => {
            if let Ok(report) = serde_json::from_value::<DoctorReport>(value.clone()) {
                render_doctor(&report);
                return;
            }
        }
        "prune" => {
            if let Ok(report) = serde_json::from_value::<PruneResult>(value.clone()) {
                render_prune(&report);
                return;
            }
        }
        "jobs.list" => {
            if let Ok(report) = serde_json::from_value::<JobsListResult>(value.clone()) {
                render_jobs(&report);
                return;
            }
        }
        "jobs.cancel" => {
            if let Ok(report) = serde_json::from_value::<JobsCancelResult>(value.clone()) {
                println!("cancelled={} {}", report.cancelled, report.reason);
                return;
            }
        }
        _ => {}
    }
    if let Some(jobs) = value.get("jobs").and_then(Value::as_array) {
        println!("queued {} analyzer job(s)", jobs.len());
        for job in jobs {
            let id = job
                .get("job_id")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let analyzer = job
                .get("analyzer_id")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            let state = job
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            println!("job {id}: {analyzer} -> {state}");
        }
        return;
    }
    println!("ok");
}

async fn wait_for_jobs(
    socket_path: &std::path::Path,
    alias: &str,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let resp = round_trip(socket_path, "jobs.list", json!({"alias": alias})).await?;
        if let Some(err) = resp.error {
            return Err(anyhow!(err.message));
        }
        let Some(value) = resp.result else {
            return Err(anyhow!("jobs.list returned no result"));
        };
        let report: JobsListResult = serde_json::from_value(value)?;
        let unfinished = report
            .jobs
            .iter()
            .filter(|job| matches!(job.state.as_str(), "queued" | "running"))
            .count();
        if unfinished == 0 {
            println!("all jobs reached terminal state");
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(anyhow!("timeout waiting for {unfinished} job(s)"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn render_jobs(r: &JobsListResult) {
    if r.jobs.is_empty() {
        println!("no jobs");
        return;
    }
    for job in &r.jobs {
        let finished = job
            .finished_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into());
        match &job.error {
            Some(error) => println!(
                "job {}: {} {} -> {} finished={} error={}",
                job.job_id, job.alias, job.analyzer_id, job.state, finished, error
            ),
            None => println!(
                "job {}: {} {} -> {} finished={}",
                job.job_id, job.alias, job.analyzer_id, job.state, finished
            ),
        }
    }
}

fn render_prune(r: &PruneResult) {
    println!("deleted {} blob(s)", r.total_deleted);
    for repo in &r.repos {
        println!("  - {}: {}", repo.alias, repo.deleted_blob_count);
    }
}

fn render_status(r: &StatusReport) {
    println!("cairn {} (uptime: {}s)", r.daemon_version, r.uptime_secs);
    if r.repos.is_empty() {
        println!("  (no repositories registered)");
        return;
    }
    for repo in &r.repos {
        println!("  - {} ({})", repo.alias, repo.root);
        let languages = repo.languages();
        if !languages.is_empty() {
            println!(
                "      languages: {}",
                languages.iter().copied().collect::<Vec<_>>().join(", ")
            );
        }
        for snap in &repo.snapshots {
            println!(
                "      [{}] status={} enrichment={:?} files={} symbols={} bytes={}",
                snap.branches.join("/"),
                snap.status,
                snap.enrichment,
                snap.file_count,
                snap.symbol_count,
                snap.size_bytes,
            );
        }
        if !repo.job_summary.is_empty() {
            println!("      jobs: {}", render_job_summary(&repo.job_summary));
        }
    }
}

fn render_job_summary(summary: &JobSummary) -> String {
    [
        ("queued", summary.queued),
        ("running", summary.running),
        ("succeeded", summary.succeeded),
        ("skipped", summary.skipped),
        ("failed", summary.failed),
        ("timed_out", summary.timed_out),
        ("cancelled", summary.cancelled),
        ("other", summary.other),
    ]
    .into_iter()
    .filter(|(_, count)| *count > 0)
    .map(|(name, count)| format!("{count} {name}"))
    .collect::<Vec<_>>()
    .join(", ")
}

fn render_doctor(r: &DoctorReport) {
    for c in &r.checks {
        let tag = match c.status {
            DoctorStatus::Pass => "[ok]  ",
            DoctorStatus::Warn => "[warn]",
            DoctorStatus::Fail => "[fail]",
        };
        match &c.detail {
            Some(d) => println!("{tag} {}: {}", c.name, d),
            None => println!("{tag} {}", c.name),
        }
        if let Some(remediation) = &c.remediation {
            println!("      fix: {remediation}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_proto::jsonrpc::ok_response;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn shutdown_skips_version_guard_and_reaches_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("control.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            let req: Request = serde_json::from_str(request.trim()).unwrap();
            assert_eq!(req.method, "shutdown");
            let mut line =
                serde_json::to_string(&ok_response(req.id, serde_json::json!(null))).unwrap();
            line.push('\n');
            write.write_all(line.as_bytes()).await.unwrap();
            write.flush().await.unwrap();
        });

        run(Args {
            command: CtlCommand::Shutdown,
            runtime_dir: Some(dir.path().to_path_buf()),
        })
        .await
        .unwrap();
        server.await.unwrap();
    }
}
