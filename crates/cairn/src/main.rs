//! `cairn` — entry point for the Cairn binary.
//!
//! Dispatches one clap-parsed subcommand on a fresh Tokio runtime,
//! then shuts the runtime down with a bounded grace period. Every
//! subcommand handler lives under [`cmd`]; this file only assembles
//! the CLI surface, keeps the language-backend rlibs linked in, and
//! initializes tracing to stderr.

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::time::Duration;

#[cfg(target_os = "macos")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "macos")]
use std::io::{self, Write};
#[cfg(target_os = "macos")]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
#[cfg(target_os = "macos")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::sync::{Arc, Mutex};

// Each language backend registers itself into `LANGUAGE_BACKENDS` via
// `#[distributed_slice]`. Those static items live in the backend's
// rlib; Rust's link model drops an rlib entirely if no symbol from it
// is referenced by the binary, which makes the registrations
// disappear. The `as _` imports below pull the crate names into scope
// (under `_`, so the binding is unusable) and that suffices to keep
// the rlib in the final link line. Adding a new language backend
// means adding one more `use ... as _;` line here.
use cairn_lang_c as _;
use cairn_lang_clangd_tier3 as _;
use cairn_lang_cpp as _;
use cairn_lang_csharp as _;
use cairn_lang_csharp_tier3 as _;
use cairn_lang_csharp_tier25 as _;
use cairn_lang_go as _;
use cairn_lang_go_tier3 as _;
use cairn_lang_java as _;
use cairn_lang_java_tier3 as _;
use cairn_lang_javascript_tier25 as _;
use cairn_lang_kotlin as _;
use cairn_lang_kotlin_tier3 as _;
use cairn_lang_kotlin_tier25 as _;
use cairn_lang_markdown as _;
use cairn_lang_objc as _;
use cairn_lang_php as _;
use cairn_lang_php_tier3 as _;
use cairn_lang_php_tier25 as _;
use cairn_lang_python as _;
use cairn_lang_python_tier3 as _;
use cairn_lang_python_tier25 as _;
use cairn_lang_ruby as _;
use cairn_lang_ruby_tier3 as _;
use cairn_lang_ruby_tier25 as _;
use cairn_lang_rust as _;
use cairn_lang_rust_tier3 as _;
use cairn_lang_swift as _;
use cairn_lang_swift_tier3 as _;
use cairn_lang_swift_tier25 as _;
use cairn_lang_typescript as _;
use cairn_lang_typescript_tier3 as _;

mod cmd;

/// Top-level clap surface. `version` inherits `CARGO_PKG_VERSION`
/// so `cairn --version` prints the same string the version guard
/// (see `cmd::version_guard`) compares against the running daemon.
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

/// Upper bound for `Runtime::shutdown_timeout` after the async
/// subcommand returns. `Runtime::drop` would otherwise wait
/// indefinitely for any residual `spawn_blocking` work (see the
/// caller comment in `main`).
const RUNTIME_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

#[cfg(target_os = "macos")]
const DAEMON_LOG_FILE_NAME: &str = "daemon.log";
#[cfg(target_os = "macos")]
const DAEMON_LOG_MAX_BYTES: u64 = 20 * 1024 * 1024;
#[cfg(target_os = "macos")]
const DAEMON_LOG_FILE_COUNT: usize = 5;

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.command);
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        match cli.command {
            Command::Daemon(args) => cmd::daemon::run(args).await,
            Command::Mcp(args) => cmd::mcp::run(args).await,
            Command::Ctl(args) => cmd::ctl::run(args).await,
            Command::Query(args) => cmd::query::run(args).await,
        }
    });
    // `Runtime::drop` waits indefinitely for spawn_blocking work. Reconcile
    // registration is intentionally crash-safe and may still be inside a
    // blocking filesystem scan after async daemon teardown completed, so both
    // success and failure paths use Tokio's bounded runtime shutdown instead.
    rt.shutdown_timeout(RUNTIME_SHUTDOWN_GRACE);
    result
}

/// Install the process-wide tracing subscriber after parsing the
/// subcommand. The macOS daemon owns a bounded private log; every
/// other command and platform keeps the existing stderr writer so
/// the `mcp` stdout stream remains reserved for protocol framing.
fn init_tracing(command: &Command) {
    #[cfg(target_os = "macos")]
    if let Command::Daemon(args) = command {
        match daemon_log_writer(args) {
            Ok(writer) => {
                init_tracing_with_writer(writer);
                return;
            }
            Err(err) => {
                eprintln!("warning: failed to open daemon log; using stderr: {err}");
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    let _ = command;
    init_tracing_with_writer(std::io::stderr);
}

fn init_tracing_with_writer<W>(writer: W)
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

#[cfg(target_os = "macos")]
fn daemon_log_writer(args: &cmd::daemon::Args) -> Result<RotatingLogWriter> {
    let data_dir = match &args.data_dir {
        Some(root) => cairn_core::paths::CasDataDir::with_root(root.clone()),
        None => cairn_core::paths::CasDataDir::from_platform_default()?,
    };
    std::fs::create_dir_all(data_dir.root())?;
    Ok(RotatingLogWriter::open(
        data_dir.root().join(DAEMON_LOG_FILE_NAME),
    )?)
}

#[cfg(target_os = "macos")]
#[derive(Clone)]
struct RotatingLogWriter {
    state: Arc<Mutex<RotatingLogState>>,
}

#[cfg(target_os = "macos")]
impl RotatingLogWriter {
    fn open(path: PathBuf) -> io::Result<Self> {
        Self::open_with_limits(path, DAEMON_LOG_MAX_BYTES, DAEMON_LOG_FILE_COUNT)
    }

    fn open_with_limits(path: PathBuf, max_bytes: u64, file_count: usize) -> io::Result<Self> {
        debug_assert!(max_bytes > 0);
        debug_assert!(file_count > 1);
        let file = open_private_log(&path)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            state: Arc::new(Mutex::new(RotatingLogState {
                path,
                file,
                bytes,
                max_bytes,
                file_count,
                rotation_failed: false,
            })),
        })
    }
}

#[cfg(target_os = "macos")]
impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for RotatingLogWriter {
    type Writer = RotatingLogEvent;

    fn make_writer(&'writer self) -> Self::Writer {
        RotatingLogEvent {
            state: self.state.clone(),
            bytes: Vec::new(),
        }
    }
}

#[cfg(target_os = "macos")]
struct RotatingLogEvent {
    state: Arc<Mutex<RotatingLogState>>,
    bytes: Vec<u8>,
}

#[cfg(target_os = "macos")]
impl Write for RotatingLogEvent {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl Drop for RotatingLogEvent {
    fn drop(&mut self) {
        if self.bytes.is_empty() {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = state.write_event(&self.bytes);
    }
}

#[cfg(target_os = "macos")]
struct RotatingLogState {
    path: PathBuf,
    file: File,
    bytes: u64,
    max_bytes: u64,
    file_count: usize,
    rotation_failed: bool,
}

#[cfg(target_os = "macos")]
impl RotatingLogState {
    fn write_event(&mut self, event: &[u8]) -> io::Result<()> {
        let event_bytes = u64::try_from(event.len()).unwrap_or(u64::MAX);
        if !self.rotation_failed
            && self.bytes > 0
            && self.bytes.saturating_add(event_bytes) > self.max_bytes
            && self.rotate().is_err()
        {
            // The existing file handle remains writable even if the current
            // path was already renamed. Disable further rotation attempts and
            // preserve subsequent events in that usable fallback sink.
            self.rotation_failed = true;
        }
        self.file.write_all(event)?;
        self.bytes = self.bytes.saturating_add(event_bytes);
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;
        let oldest = archive_path(&self.path, self.file_count - 1);
        match std::fs::remove_file(oldest) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        for index in (1..self.file_count - 1).rev() {
            match std::fs::rename(
                archive_path(&self.path, index),
                archive_path(&self.path, index + 1),
            ) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        std::fs::rename(&self.path, archive_path(&self.path, 1))?;
        self.file = open_private_log(&self.path)?;
        self.bytes = 0;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn open_private_log(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

#[cfg(target_os = "macos")]
fn archive_path(path: &Path, index: usize) -> PathBuf {
    let mut archive = path.as_os_str().to_os_string();
    archive.push(format!(".{index}"));
    archive.into()
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[cfg(target_os = "macos")]
    use std::io::Write as _;
    #[cfg(target_os = "macos")]
    use std::os::unix::fs::PermissionsExt as _;
    #[cfg(target_os = "macos")]
    use tracing_subscriber::fmt::MakeWriter as _;

    #[cfg(target_os = "macos")]
    fn write_log_event(writer: &super::RotatingLogWriter, event: &str) {
        let mut event_writer = writer.make_writer();
        event_writer.write_all(event.as_bytes()).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn daemon_log_uses_the_data_dir_override() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("private-data");
        let args = crate::cmd::daemon::Args {
            runtime_dir: None,
            data_dir: Some(data_dir.clone()),
        };

        let writer = super::daemon_log_writer(&args).unwrap();

        assert_eq!(
            writer
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .path,
            data_dir.join(super::DAEMON_LOG_FILE_NAME)
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn daemon_log_rotates_whole_events_with_private_bounded_files() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("daemon.log");
        let writer = super::RotatingLogWriter::open_with_limits(path.clone(), 10, 5).unwrap();
        let events = (0..6)
            .map(|index| format!("event-{index}\n"))
            .collect::<Vec<_>>();

        for event in &events {
            write_log_event(&writer, event);
        }

        let mut stored = String::new();
        for index in (1..5).rev() {
            let archive = super::archive_path(&path, index);
            stored.push_str(&std::fs::read_to_string(&archive).unwrap());
            assert_eq!(
                std::fs::metadata(archive).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        stored.push_str(&std::fs::read_to_string(&path).unwrap());
        assert_eq!(stored, events[1..].concat());
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 5);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn daemon_log_rotation_failure_keeps_current_sink_usable() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("daemon.log");
        let writer = super::RotatingLogWriter::open_with_limits(path.clone(), 8, 5).unwrap();
        write_log_event(&writer, "first\n");
        let blocked_archive = super::archive_path(&path, 4);
        std::fs::create_dir(&blocked_archive).unwrap();
        std::fs::write(blocked_archive.join("occupied"), b"x").unwrap();

        write_log_event(&writer, "second\n");

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "first\nsecond\n");
        assert!(
            writer
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .rotation_failed
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn daemon_log_serializes_concurrent_events() {
        const THREADS: usize = 8;
        const EVENTS: usize = 50;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("daemon.log");
        let writer =
            super::RotatingLogWriter::open_with_limits(path.clone(), 1024 * 1024, 5).unwrap();
        let mut workers = Vec::new();
        for thread in 0..THREADS {
            let writer = writer.clone();
            workers.push(std::thread::spawn(move || {
                for event in 0..EVENTS {
                    write_log_event(&writer, &format!("{thread}:{event}\n"));
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        let mut actual = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut expected = (0..THREADS)
            .flat_map(|thread| (0..EVENTS).map(move |event| format!("{thread}:{event}")))
            .collect::<Vec<_>>();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn runtime_language_backend_registry_includes_cli_linked_backends() {
        let mut backend_names = cairn_lang_api::all_backends()
            .iter()
            .map(|backend| backend.name())
            .collect::<Vec<_>>();
        backend_names.sort_unstable();

        assert_eq!(
            backend_names,
            [
                "c",
                "cpp",
                "csharp",
                "go",
                "java",
                "javascript",
                "kotlin",
                "markdown",
                "objc",
                "php",
                "python",
                "ruby",
                "rust",
                "swift",
                "tsx",
                "typescript"
            ]
        );
    }

    #[test]
    fn runtime_shutdown_timeout_does_not_wait_for_residual_blocking_work() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        rt.spawn_blocking(move || {
            started_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking task did not start");

        let started = Instant::now();
        rt.shutdown_timeout(Duration::from_millis(50));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "runtime shutdown waited for residual blocking work: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn runtime_workspace_analyzer_registry_includes_cli_linked_analyzers() {
        let analyzers = cairn_core::workspace_analyzer::all_workspace_analyzers();
        let mut analyzer_ids = analyzers
            .iter()
            .map(|analyzer| analyzer.id())
            .collect::<Vec<_>>();
        analyzer_ids.sort_unstable();

        assert_eq!(
            analyzer_ids,
            [
                "clangd-c-lsp",
                "clangd-cpp-lsp",
                "clangd-objc-lsp",
                "csharp-ls",
                "csharp-resolver",
                "gopls-lsp",
                "javascript-resolver",
                "jdtls-lsp",
                "kotlin-language-server",
                "kotlin-resolver",
                "php-resolver",
                "phpantom-lsp",
                "pyright-lsp",
                "python-resolver",
                "ruby-lsp",
                "ruby-resolver",
                "rust-analyzer-lsp",
                "sourcekit-lsp",
                "swift-resolver",
                "typescript-language-server-js-lsp",
                "typescript-language-server-ts-lsp",
                "typescript-language-server-tsx-lsp"
            ]
        );

        let mut deferred = analyzers
            .iter()
            .filter(|analyzer| analyzer.defer_stall_watchdog_until_active_work())
            .map(|analyzer| analyzer.id())
            .collect::<Vec<_>>();
        deferred.sort_unstable();
        assert_eq!(
            deferred,
            [
                "clangd-c-lsp",
                "clangd-cpp-lsp",
                "clangd-objc-lsp",
                "csharp-ls",
                "gopls-lsp",
                "jdtls-lsp",
                "kotlin-language-server",
                "phpantom-lsp",
                "pyright-lsp",
                "ruby-lsp",
                "rust-analyzer-lsp",
                "sourcekit-lsp",
                "typescript-language-server-js-lsp",
                "typescript-language-server-ts-lsp",
                "typescript-language-server-tsx-lsp",
            ]
        );
        assert_eq!(analyzers.len() - deferred.len(), 7);
    }

    #[test]
    fn query_symbols_help_includes_zero_hit_recovery_hint() {
        let mut cmd = super::Cli::command();
        let query = cmd.find_subcommand_mut("query").unwrap();
        let symbols = query.find_subcommand_mut("symbols").unwrap();
        let mut help = Vec::new();
        symbols.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(help.contains("If results are empty"));
        assert!(help.contains("Analyzer*"));
        assert!(help.contains("--container / --path / --kind"));
    }

    #[test]
    fn query_source_help_describes_fail_closed_ambiguity_selection() {
        let mut cmd = super::Cli::command();
        let query = cmd.find_subcommand_mut("query").unwrap();
        let source = query.find_subcommand_mut("source").unwrap();
        let mut help = Vec::new();
        source.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(help.contains("Duplicate physical declarations return ambiguity candidates"));
        assert!(help.contains("use `--repo`, `--file`, or `--file` with `--line` to select one"));
        assert!(help.contains("If multiple declarations share the file, add `--line`"));
        assert!(!help.contains("first matching symbol wins"));
    }

    #[test]
    fn ctl_help_uses_object_action_surface_without_legacy_top_level_verbs() {
        let cmd = super::Cli::command();
        let ctl = cmd
            .get_subcommands()
            .find(|command| command.get_name() == "ctl")
            .expect("ctl subcommand");
        let top_level = ctl
            .get_subcommands()
            .map(|command| command.get_name())
            .collect::<Vec<_>>();

        assert!(top_level.contains(&"repo"));
        assert!(top_level.contains(&"jobs"));
        assert!(top_level.contains(&"blobs"));
        assert!(top_level.contains(&"daemon"));
        for legacy in [
            "register-repo",
            "remove-repo",
            "reindex-repo",
            "status",
            "doctor",
            "shutdown",
            "prune",
        ] {
            assert!(
                !top_level.contains(&legacy),
                "legacy command still exposed: {legacy}"
            );
        }
    }
}
