//! `cairn ctl` — management CLI.
//!
//! Talks to a running daemon's management sockets. Most commands use
//! `control.sock`; `repo list` reuses the read-only data socket's
//! `list_repos` method so the wire protocol stays unchanged. Each
//! invocation sends one newline JSON-RPC request, reads one reply,
//! pretty-prints it, and exits with code 0 on success or 1 on error.

use anyhow::{Context, Result, anyhow};
use cairn_core::sockets::SocketPaths;
use cairn_proto::control::{
    DoctorReport, DoctorStatus, JobSummary, JobsCancelArgs, JobsCancelResult, JobsListResult,
    JobsPruneResult, PruneResult, StatusReport,
};
use cairn_proto::jsonrpc::Response;
use cairn_proto::methods::ListReposResult;
use clap::{Args as ClapArgs, Subcommand};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::rpc_client;
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
    /// Repository lifecycle commands.
    Repo {
        #[command(subcommand)]
        command: RepoCommand,
    },
    /// Background analyzer job commands.
    Jobs {
        #[command(subcommand)]
        command: JobsCommand,
    },
    /// CAS blob maintenance commands.
    Blobs {
        #[command(subcommand)]
        command: BlobsCommand,
    },
    /// Daemon health and lifecycle commands.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Subcommand, Debug)]
enum RepoCommand {
    /// Register a repository so the daemon starts watching and indexing it.
    Register {
        path: PathBuf,
        #[arg(long)]
        alias: String,
        /// Retain the registration when its root is temporarily missing.
        #[arg(long, conflicts_with = "ephemeral")]
        persistent: bool,
        /// Explicitly restore the default missing-root auto-prune policy.
        #[arg(long, conflicts_with = "persistent")]
        ephemeral: bool,
    },
    /// Drop a repository alias from the registry.
    Remove { alias: String },
    /// Force a full re-index of `alias`.
    Reindex {
        alias: String,
        /// Wait until queued analyzer jobs reach terminal states.
        #[arg(long)]
        wait: bool,
        /// Maximum seconds to wait with `--wait`.
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// List registered repositories and snapshot summaries.
    List {
        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum JobsCommand {
    /// List background analyzer jobs.
    List {
        /// Restrict listing to one repo alias.
        #[arg(long, alias = "repo")]
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
    },
    /// Cancel a job by id.
    Cancel {
        job_id: i64,
        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
    /// Prune historical terminal job rows from old manifests.
    Prune {
        /// Restrict pruning to one registered repo alias.
        #[arg(long)]
        repo: Option<String>,
        /// Count rows that would be pruned without deleting them.
        #[arg(long)]
        dry_run: bool,
        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum BlobsCommand {
    /// Delete cached blobs whose parser IDs no current backend owns.
    Prune {
        /// Restrict pruning to one registered repo alias.
        #[arg(long)]
        repo: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum DaemonCommand {
    /// Show daemon health, registered repos, and snapshot progress.
    Status {
        /// Expand per-snapshot details instead of the default repo summary.
        #[arg(long)]
        snapshots: bool,
    },
    /// Diagnose dependencies, paths, and reachability.
    Doctor,
    /// Ask the daemon to shut down.
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CtlSocket {
    Control,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenderHint {
    Default,
    Status { snapshots: bool },
    JobsPrune { dry_run: bool },
}

#[derive(Debug)]
struct CtlInvocation {
    socket: CtlSocket,
    method: &'static str,
    params: Value,
    wait_after: Option<(String, Duration)>,
    json_output: bool,
    render_hint: RenderHint,
}

pub async fn run(args: Args) -> Result<()> {
    let paths = match args.runtime_dir {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };

    let check_version = should_check_daemon_version(&args.command);
    let invocation = route_ctl_command(args.command)?;

    if check_version {
        check_daemon_version(&paths.control, VersionGuardMode::Cli).await?;
    }

    let socket = match invocation.socket {
        CtlSocket::Control => &paths.control,
        CtlSocket::Data => &paths.cairn,
    };
    let resp = rpc_client::round_trip(socket, invocation.method, invocation.params)
        .await
        .with_context(|| format!("talking to {}", socket.display()))?;
    if invocation.json_output {
        if invocation.method == "jobs.list"
            && let Some(value) = &resp.result
            && let Ok(report) = serde_json::from_value::<JobsListResult>(value.clone())
        {
            println!("{}", serde_json::to_string_pretty(&report.jobs).unwrap());
        } else {
            println!("{}", serde_json::to_string_pretty(&resp.result).unwrap());
        }
    } else {
        render(invocation.method, &resp, invocation.render_hint);
    }

    if let Some(err) = resp.error {
        Err(anyhow!(err.message))
    } else if let Some((alias, timeout)) = invocation.wait_after {
        wait_for_jobs(&paths.control, &alias, timeout).await
    } else {
        Ok(())
    }
}

fn should_check_daemon_version(command: &CtlCommand) -> bool {
    // `daemon shutdown` is the remediation path for a mismatched daemon, so
    // it must stay available even when the guard would otherwise abort.
    !matches!(
        command,
        CtlCommand::Daemon {
            command: DaemonCommand::Shutdown
        }
    )
}

fn route_ctl_command(command: CtlCommand) -> Result<CtlInvocation> {
    match command {
        CtlCommand::Repo { command } => route_repo_command(command),
        CtlCommand::Jobs { command } => Ok(route_jobs_command(command)),
        CtlCommand::Blobs { command } => Ok(route_blobs_command(command)),
        CtlCommand::Daemon { command } => Ok(route_daemon_command(command)),
    }
}

fn route_repo_command(command: RepoCommand) -> Result<CtlInvocation> {
    match command {
        RepoCommand::Register {
            path,
            alias,
            persistent,
            ephemeral,
        } => {
            let canon = path
                .canonicalize()
                .with_context(|| format!("canonicalize {}", path.display()))?;
            Ok(control_invocation(
                "register_repo",
                json!({
                    "path": canon.to_string_lossy(),
                    "alias": alias,
                    "persistent": match (persistent, ephemeral) {
                        (true, false) => Some(true),
                        (false, true) => Some(false),
                        _ => None,
                    },
                }),
            ))
        }
        RepoCommand::Remove { alias } => {
            Ok(control_invocation("remove_repo", json!({"alias": alias})))
        }
        RepoCommand::Reindex {
            alias,
            wait,
            timeout,
        } => Ok(CtlInvocation {
            wait_after: wait.then_some((
                alias.clone(),
                Duration::from_secs(timeout.unwrap_or(u64::MAX)),
            )),
            ..control_invocation("reindex_repo", json!({"alias": alias}))
        }),
        RepoCommand::List { json } => Ok(CtlInvocation {
            socket: CtlSocket::Data,
            method: "list_repos",
            params: json!({"include_jobs": false}),
            wait_after: None,
            json_output: json,
            render_hint: RenderHint::Default,
        }),
    }
}

fn route_jobs_command(command: JobsCommand) -> CtlInvocation {
    match command {
        JobsCommand::List {
            alias,
            state,
            all,
            limit,
            json,
        } => CtlInvocation {
            json_output: json,
            ..control_invocation(
                "jobs.list",
                json!({"alias": alias, "state": state, "all": all, "limit": limit}),
            )
        },
        JobsCommand::Cancel { job_id, json } => CtlInvocation {
            json_output: json,
            ..control_invocation(
                "jobs.cancel",
                serde_json::to_value(JobsCancelArgs { job_id }).unwrap(),
            )
        },
        JobsCommand::Prune {
            repo,
            dry_run,
            json,
        } => CtlInvocation {
            json_output: json,
            render_hint: RenderHint::JobsPrune { dry_run },
            ..control_invocation("jobs.prune", json!({"repo": repo, "dry_run": dry_run}))
        },
    }
}

fn route_blobs_command(command: BlobsCommand) -> CtlInvocation {
    match command {
        BlobsCommand::Prune { repo } => control_invocation("prune", json!({"repo": repo})),
    }
}

fn route_daemon_command(command: DaemonCommand) -> CtlInvocation {
    match command {
        DaemonCommand::Status { snapshots } => {
            let mut invocation = control_invocation("status", Value::Null);
            invocation.render_hint = RenderHint::Status { snapshots };
            invocation
        }
        DaemonCommand::Doctor => control_invocation("doctor", Value::Null),
        DaemonCommand::Shutdown => control_invocation("shutdown", Value::Null),
    }
}

fn control_invocation(method: &'static str, params: Value) -> CtlInvocation {
    CtlInvocation {
        socket: CtlSocket::Control,
        method,
        params,
        wait_after: None,
        json_output: false,
        render_hint: RenderHint::Default,
    }
}

fn render(method: &str, resp: &Response, render_hint: RenderHint) {
    if let Some(err) = &resp.error {
        rpc_client::render_error(err);
        return;
    }
    let Some(value) = &resp.result else {
        println!("ok");
        return;
    };
    // Route the result based on the stable daemon method name; B11 changed
    // CLI shape only, not the control/data JSON-RPC methods.
    match method {
        "status" => {
            if let Ok(report) = serde_json::from_value::<StatusReport>(value.clone()) {
                let RenderHint::Status { snapshots } = render_hint else {
                    return;
                };
                render_status(&report, snapshots);
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
        "jobs.prune" => {
            if let Ok(report) = serde_json::from_value::<JobsPruneResult>(value.clone()) {
                let RenderHint::JobsPrune {
                    dry_run: dry_run_output,
                } = render_hint
                else {
                    return;
                };
                render_jobs_prune(&report, dry_run_output);
                return;
            }
        }
        "list_repos" => {
            if let Ok(report) = serde_json::from_value::<ListReposResult>(value.clone()) {
                render_repo_list(&report);
                return;
            }
        }
        _ => {}
    }
    if let Some(jobs) = value.get("jobs").and_then(Value::as_array) {
        println!("queued {} analyzer job(s)", jobs.len());
        for line in format_queued_job_lines(jobs) {
            println!("{line}");
        }
        return;
    }
    println!("ok");
}

fn format_queued_job_lines(jobs: &[Value]) -> Vec<String> {
    jobs.iter()
        .map(|job| {
            let id = job
                .get("job_id")
                .and_then(|value| match value {
                    Value::String(id) => Some(id.clone()),
                    Value::Number(id) => id.as_i64().map(|_| id.to_string()),
                    _ => None,
                })
                .unwrap_or_else(|| "0".into());
            let analyzer = job
                .get("analyzer_id")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            let state = job
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            format!("job {id}: {analyzer} -> {state}")
        })
        .collect()
}

async fn wait_for_jobs(
    socket_path: &std::path::Path,
    alias: &str,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let resp =
            rpc_client::round_trip(socket_path, "jobs.list", json!({"alias": alias})).await?;
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
        let metrics = render_job_metrics(job);
        match &job.error {
            Some(error) => println!(
                "job {}: {} {} -> {} finished={}{} error={}",
                job.job_id, job.alias, job.analyzer_id, job.state, finished, metrics, error
            ),
            None => println!(
                "job {}: {} {} -> {} finished={}{}",
                job.job_id, job.alias, job.analyzer_id, job.state, finished, metrics
            ),
        }
    }
}

fn render_job_metrics(job: &cairn_proto::control::JobSnapshot) -> String {
    let mut parts = Vec::new();
    if let Some(state) = &job.scheduler_state {
        parts.push(format!("scheduler={state}"));
    }
    if let Some(group) = &job.pool_group {
        parts.push(format!("group={group}"));
    }
    if let Some(ms) = job.queued_ms {
        parts.push(format!("queued={}ms", ms));
    }
    if let Some(ms) = job.pool_wait_ms {
        parts.push(format!("pool_wait={}ms", ms));
    }
    if let Some(ms) = job.run_ms {
        parts.push(format!("run={}ms", ms));
    }
    if let Some(ticks) = job.progress_ticks {
        parts.push(format!("progress={ticks}"));
    }
    if let Some(rate) = job.progress_per_minute {
        parts.push(format!("rate={rate:.1}/min"));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" {}", parts.join(" "))
    }
}

fn render_prune(r: &PruneResult) {
    println!("deleted {} blob(s)", r.total_deleted);
    for repo in &r.repos {
        println!("  - {}: {}", repo.alias, repo.deleted_blob_count);
    }
}

fn render_jobs_prune(r: &JobsPruneResult, dry_run: bool) {
    let verb = if dry_run { "would delete" } else { "deleted" };
    println!(
        "{verb} {} job run(s), {} job index entr(y/ies)",
        r.total_deleted_runs, r.total_deleted_index_entries
    );
    for repo in &r.repos {
        println!(
            "  - {}: {} run(s), {} index entr(y/ies)",
            repo.alias, repo.deleted_runs_count, repo.deleted_index_entries_count
        );
    }
}

fn render_repo_list(r: &ListReposResult) {
    if r.repos.is_empty() {
        println!("no repositories registered");
        return;
    }
    for line in format_repo_list_lines(r) {
        println!("{line}");
    }
}

fn format_repo_list_lines(r: &ListReposResult) -> Vec<String> {
    r.repos
        .iter()
        .map(|repo| {
            format!(
                "{}\t{}\t[{}]\tstatus={:?} snapshots={} files={} symbols={}",
                repo.alias,
                repo.root,
                repo.languages.join(","),
                repo.status,
                repo.snapshot_count,
                repo.current_file_count,
                repo.current_symbol_count
            )
        })
        .collect()
}

fn render_status(r: &StatusReport, snapshots: bool) {
    println!("cairn {} (uptime: {}s)", r.daemon_version, r.uptime_secs);
    if let Some(line) = initialization_status_line(r) {
        println!("  {line}");
        return;
    }
    if r.repos.is_empty() {
        println!("  (no repositories registered)");
        return;
    }
    for repo in &r.repos {
        println!("{}", render_status_repo_summary(repo));
        if snapshots {
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
        }
        if !repo.job_summary.is_empty() {
            println!("      jobs: {}", render_job_summary(&repo.job_summary));
        }
    }
}

fn initialization_status_line(report: &StatusReport) -> Option<String> {
    if report.initialization.is_ready() {
        return None;
    }
    let detail = report
        .initialization
        .detail
        .map(|detail| format!(" ({})", detail.label()))
        .unwrap_or_default();
    Some(format!(
        "initializing {}/{}: {}{}",
        report.initialization.completed_phases,
        report.initialization.total_phases,
        report.initialization.phase.label(),
        detail
    ))
}

fn render_status_repo_summary(repo: &cairn_proto::control::RepoStatus) -> String {
    let languages = repo.languages();
    let snapshots = repo.snapshots.len();
    let ready = repo
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "ready")
        .count();
    let stale = repo
        .snapshots
        .iter()
        .filter(|snapshot| snapshot.status == "stale")
        .count();
    let files = repo
        .snapshots
        .iter()
        .map(|snapshot| snapshot.file_count)
        .sum::<u64>();
    let symbols = repo
        .snapshots
        .iter()
        .map(|snapshot| snapshot.symbol_count)
        .sum::<u64>();
    format!(
        "  - {} ({}) [{}] snapshots={} ready={} stale={} files={} symbols={}",
        repo.alias,
        repo.root,
        languages.iter().copied().collect::<Vec<_>>().join(","),
        snapshots,
        ready,
        stale,
        files,
        symbols
    )
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
    use cairn_proto::jsonrpc::{Request, ok_response};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
            command: CtlCommand::Daemon {
                command: DaemonCommand::Shutdown,
            },
            runtime_dir: Some(dir.path().to_path_buf()),
        })
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[test]
    fn repo_list_routes_to_data_plane_list_repos_without_jobs() {
        let invocation = route_ctl_command(CtlCommand::Repo {
            command: RepoCommand::List { json: true },
        })
        .unwrap();

        assert_eq!(invocation.socket, CtlSocket::Data);
        assert_eq!(invocation.method, "list_repos");
        assert_eq!(
            invocation.params,
            serde_json::json!({"include_jobs": false})
        );
        assert!(invocation.json_output);
    }

    #[test]
    fn repo_list_render_hides_timing_from_human_output() {
        let report = ListReposResult {
            repos: vec![cairn_proto::methods::RepoListEntry {
                alias: "demo".into(),
                root: "/repo/demo".into(),
                persistent: false,
                languages: vec!["rust".into()],
                status: cairn_proto::methods::RepoAggregateStatus::Ready,
                snapshot_count: 1,
                current_file_count: 3,
                current_symbol_count: 10,
            }],
            completeness: cairn_proto::Completeness::complete(),
            timing: cairn_proto::Timing { server_ms: 123 },
        };

        let output = format_repo_list_lines(&report).join("\n");
        assert!(output.contains("demo"));
        assert!(!output.contains("timing"));
        assert!(!output.contains("server_ms"));
    }

    #[test]
    fn jobs_prune_routes_to_control_plane_with_dry_run_hint() {
        let invocation = route_ctl_command(CtlCommand::Jobs {
            command: JobsCommand::Prune {
                repo: Some("demo".into()),
                dry_run: true,
                json: false,
            },
        })
        .unwrap();

        assert_eq!(invocation.socket, CtlSocket::Control);
        assert_eq!(invocation.method, "jobs.prune");
        assert_eq!(
            invocation.params,
            serde_json::json!({"repo": "demo", "dry_run": true})
        );
        assert_eq!(
            invocation.render_hint,
            RenderHint::JobsPrune { dry_run: true }
        );
    }

    #[test]
    fn jobs_cancel_routes_job_id_as_decimal_string() {
        let invocation = route_ctl_command(CtlCommand::Jobs {
            command: JobsCommand::Cancel {
                job_id: 1_784_679_083_389_822_001,
                json: false,
            },
        })
        .unwrap();

        assert_eq!(invocation.socket, CtlSocket::Control);
        assert_eq!(invocation.method, "jobs.cancel");
        assert_eq!(
            invocation.params,
            serde_json::json!({"job_id": "1784679083389822001"})
        );
    }

    #[test]
    fn queued_job_render_preserves_string_ids_and_accepts_legacy_numbers() {
        let lines = format_queued_job_lines(&[
            serde_json::json!({
                "job_id": "1784679083389822001",
                "analyzer_id": "rust-analyzer-lsp",
                "state": "queued"
            }),
            serde_json::json!({
                "job_id": 7,
                "analyzer_id": "pyright-lsp",
                "state": "queued"
            }),
        ]);

        assert_eq!(
            lines,
            [
                "job 1784679083389822001: rust-analyzer-lsp -> queued",
                "job 7: pyright-lsp -> queued"
            ]
        );
    }

    #[test]
    fn daemon_status_defaults_to_repo_summary() {
        let invocation = route_ctl_command(CtlCommand::Daemon {
            command: DaemonCommand::Status { snapshots: false },
        })
        .unwrap();

        assert_eq!(invocation.socket, CtlSocket::Control);
        assert_eq!(invocation.method, "status");
        assert_eq!(
            invocation.render_hint,
            RenderHint::Status { snapshots: false }
        );
    }

    #[test]
    fn daemon_status_snapshots_flag_preserves_expanded_rendering() {
        let invocation = route_ctl_command(CtlCommand::Daemon {
            command: DaemonCommand::Status { snapshots: true },
        })
        .unwrap();

        assert_eq!(invocation.socket, CtlSocket::Control);
        assert_eq!(invocation.method, "status");
        assert_eq!(
            invocation.render_hint,
            RenderHint::Status { snapshots: true }
        );
    }

    #[test]
    fn daemon_status_formats_initialization_progress_without_repo_placeholder() {
        let report = StatusReport {
            daemon_version: "0.8.0".into(),
            uptime_secs: 1,
            initialization: cairn_proto::control::DaemonInitializationStatus::initializing(
                cairn_proto::control::DaemonInitializationPhase::WatcherBarrier,
                Some(cairn_proto::control::DaemonInitializationDetail::ArmingRegisteredWatchers),
            ),
            repos: Vec::new(),
        };

        assert_eq!(
            initialization_status_line(&report).as_deref(),
            Some("initializing 4/7: watcher barrier (arming registered watchers)")
        );
    }

    #[test]
    fn status_repo_summary_collapses_snapshot_counts() {
        let repo = cairn_proto::control::RepoStatus {
            alias: "demo".into(),
            root: "/repo/demo".into(),
            persistent: false,
            snapshots: vec![
                cairn_proto::control::SnapshotStatus {
                    branches: vec!["HEAD".into()],
                    status: "ready".into(),
                    enrichment: vec![cairn_proto::common::LanguageEnrichment {
                        language: "rust".into(),
                        tier: cairn_proto::common::SourceTier::Semantic,
                        has_analyzer: true,
                    }],
                    file_count: 3,
                    symbol_count: 10,
                    size_bytes: 100,
                },
                cairn_proto::control::SnapshotStatus {
                    branches: vec!["feature".into()],
                    status: "stale".into(),
                    enrichment: vec![cairn_proto::common::LanguageEnrichment {
                        language: "python".into(),
                        tier: cairn_proto::common::SourceTier::Semantic,
                        has_analyzer: true,
                    }],
                    file_count: 5,
                    symbol_count: 20,
                    size_bytes: 200,
                },
            ],
            job_summary: JobSummary::default(),
            jobs: Vec::new(),
            reconcile: None,
        };

        assert_eq!(
            render_status_repo_summary(&repo),
            "  - demo (/repo/demo) [python,rust] snapshots=2 ready=1 stale=1 files=8 symbols=30"
        );
    }

    #[test]
    fn daemon_shutdown_is_only_ctl_command_that_skips_version_guard() {
        assert!(!should_check_daemon_version(&CtlCommand::Daemon {
            command: DaemonCommand::Shutdown,
        }));
        assert!(should_check_daemon_version(&CtlCommand::Daemon {
            command: DaemonCommand::Status { snapshots: false },
        }));
        assert!(should_check_daemon_version(&CtlCommand::Jobs {
            command: JobsCommand::List {
                alias: None,
                state: None,
                all: false,
                limit: None,
                json: false,
            },
        }));
    }
}
