//! `cairn ctl` — management CLI.
//!
//! Talks to a running daemon's `control.sock`. Each invocation opens
//! one short-lived UDS connection, sends one newline JSON-RPC
//! request, reads one newline JSON-RPC reply, pretty-prints it, and
//! exits with code 0 on success or 1 on an error response.

use anyhow::{Context, Result, anyhow};
use cairn_core::sockets::SocketPaths;
use cairn_proto::control::{DoctorReport, DoctorStatus, StatusReport};
use cairn_proto::jsonrpc::{JsonRpcVersion, Request, RequestId, Response};
use clap::{Args as ClapArgs, Subcommand};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

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
    ReindexRepo { alias: String },
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

    let (method, params) = match args.command {
        CtlCommand::RegisterRepo { path, alias } => {
            let canon = path
                .canonicalize()
                .with_context(|| format!("canonicalize {}", path.display()))?;
            (
                "register_repo",
                json!({"path": canon.to_string_lossy(), "alias": alias}),
            )
        }
        CtlCommand::RemoveRepo { alias } => ("remove_repo", json!({"alias": alias})),
        CtlCommand::Status => ("status", Value::Null),
        CtlCommand::ReindexRepo { alias } => ("reindex_repo", json!({"alias": alias})),
        CtlCommand::Doctor => ("doctor", Value::Null),
        CtlCommand::Shutdown => ("shutdown", Value::Null),
    };

    let resp = round_trip(&paths.control, method, params)
        .await
        .with_context(|| format!("talking to {}", paths.control.display()))?;
    render(method, &resp);

    if let Some(err) = resp.error {
        Err(anyhow!(err.message))
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
        _ => {}
    }
    println!("ok");
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
    }
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
    }
}
