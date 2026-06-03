//! `cairn` — entry point for the Cairn binary.

use anyhow::Result;
use clap::{Parser, Subcommand};

// Each language backend registers itself into `LANGUAGE_BACKENDS` via
// `#[distributed_slice]`. Those static items live in the backend's
// rlib; Rust's link model drops an rlib entirely if no symbol from it
// is referenced by the binary, which makes the registrations
// disappear. The `as _` imports below pull the crate names into scope
// (under `_`, so the binding is unusable) and that suffices to keep
// the rlib in the final link line. Adding a new language backend
// means adding one more `use ... as _;` line here.
use cairn_lang_go as _;
use cairn_lang_markdown as _;
use cairn_lang_python as _;
use cairn_lang_rust as _;

mod cmd;

#[derive(Parser, Debug)]
#[command(
    name = "cairn",
    version,
    about = "Cairn: a local code-intelligence index server speaking MCP."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the long-lived index daemon.
    Daemon(cmd::daemon::Args),
    /// Stdio MCP front-end. Spawned by an MCP client (Claude Code,
    /// etc.); translates MCP tool calls into requests against the
    /// running daemon's UDS. A future `cairn lsp` will sit in the
    /// same slot for LSP clients.
    Mcp(cmd::mcp::Args),
    /// Talk to a running daemon's control socket.
    Ctl(cmd::ctl::Args),
    /// Command-line search front-end. GNU `global`-style read-only
    /// queries (symbols, outline, source, impls, imports, refs)
    /// against the running daemon's data socket.
    Query(cmd::query::Args),
}

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        match cli.command {
            Command::Daemon(args) => cmd::daemon::run(args).await,
            Command::Mcp(args) => cmd::mcp::run(args).await,
            Command::Ctl(args) => cmd::ctl::run(args).await,
            Command::Query(args) => cmd::query::run(args).await,
        }
    })
}

fn init_tracing() {
    // `mcp` runs as a stdio relay; logging on stderr is fine and
    // won't pollute the MCP wire.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}
