//! `cairn query` — command-line search front-end.
//!
//! Mirrors the GNU `global` / `cscope` ergonomics: short subcommands
//! that wrap each read-only data RPC method on `cairn.sock`. The MCP
//! front-end exposes the same methods to LLM agents; this CLI is the
//! equivalent for shell users who want to grep symbol / impl / call
//! data straight from a terminal.
//!
//! Each subcommand opens one short-lived UDS connection to
//! `cairn.sock`, sends one newline JSON-RPC request, reads one
//! newline JSON-RPC reply, renders it human-readably, and exits.
//! Pass `--json` to get the raw `result` payload — handy for piping
//! into `jq`.
//!
//! Subcommand naming mirrors the MCP tool surface: each subcommand
//! takes the search target as its first positional argument and the
//! repo (when relevant) as an optional `--repo` flag. Omitting
//! `--repo` searches every registered repository — the same default
//! as `find_symbols`.

use anyhow::{Context, Result, anyhow};
use cairn_core::sockets::SocketPaths;
use cairn_proto::Completeness;
use cairn_proto::methods::{
    FindCalleesResult, FindCallersResult, FindReferencesResult, FindSubtypesResult,
    FindSupertypesResult, FindSymbolResult, GetSymbolSourceResult, ImportsResult, ListReposResult,
    OutlineResult,
};
use clap::{Args as ClapArgs, Subcommand, ValueEnum};
use serde_json::{Value, json};

use super::rpc_client;
use super::version_guard::{VersionGuardMode, check_daemon_version};

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[command(subcommand)]
    command: QueryCommand,

    /// Override the runtime directory (otherwise picked from
    /// $XDG_RUNTIME_DIR / ~/Library/Caches).
    #[arg(long, global = true)]
    runtime_dir: Option<std::path::PathBuf>,

    /// Print the raw JSON-RPC `result` instead of the human-readable
    /// rendering. Useful for piping into `jq`.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Subcommand, Debug)]
enum QueryCommand {
    /// List registered repositories and their snapshots.
    Repos,
    /// Outline a file or directory (what's defined and where).
    Outline {
        /// File or directory path relative to the repo root.
        /// Include a trailing `/` to scope to a directory.
        file: String,
        /// Repository alias. Omit to search every registered repo.
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        /// Restrict outline items to one symbol kind.
        #[arg(long)]
        kind: Option<String>,
        /// Directory-depth cap relative to the path prefix.
        #[arg(long)]
        max_depth: Option<u32>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Look up a definition by name (function, struct, method, …).
    #[command(
        alias = "find-symbols",
        long_about = "Look up a definition by name (function, struct, method, ...).\n\nIf results are empty, relax one dimension at a time: try the exact name without --fuzzy, add a prefix wildcard such as \"Analyzer*\", or remove --container / --path / --kind filters."
    )]
    Symbols {
        /// Symbol query — exact name unless `--fuzzy`.
        query: String,
        /// Repository alias. Omit to search every registered repo.
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        /// Restrict to symbols of this kind.
        #[arg(long)]
        kind: Option<String>,
        /// File-path prefix scope relative to the repo root.
        #[arg(long)]
        path: Option<String>,
        /// Qualified-prefix scope (for example, methods under a type).
        #[arg(long)]
        container: Option<String>,
        /// FTS5 match over name, qualified, and doc instead of exact.
        /// Spaces are AND, quotes are phrase, `*` enables prefix matching.
        #[arg(long)]
        fuzzy: bool,
        #[arg(long)]
        limit: Option<u32>,
        /// Drop the `signature` field from each hit. Use for broad
        /// enumerations where the signature dominates wire / context
        /// cost.
        #[arg(long)]
        signature_only: bool,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Print the source of one symbol by qualified name.
    Source {
        /// Fully-qualified name (`crate::module::name` or just `name`
        /// when unambiguous).
        qualified: String,
        /// Repository alias. Omit to search every registered repo
        /// (first matching symbol wins).
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        /// File path (relative to repo root) to disambiguate the
        /// qualified name when it exists in multiple files.
        #[arg(long)]
        file: Option<String>,
        /// 1-indexed declaration start line. Requires `--file` and is sent
        /// without conversion.
        #[arg(long, requires = "file", value_parser = clap::value_parser!(u32).range(1..))]
        line: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Types that implement / extend / mix in the given name.
    Subtypes {
        /// Base type / trait / interface.
        name: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Types that the given name extends / implements / mixes in.
    Supertypes {
        /// Subtype name.
        name: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Functions that call the given symbol.
    ///
    /// For React/JSX component usage, prefer `refs --kind instantiate`.
    Callers {
        /// Callee symbol.
        name: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Resolved calls made from inside the given symbol.
    Callees {
        /// Caller (enclosing) symbol.
        name: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Import edges (`use` statements / ES imports).
    Imports {
        /// File path relative to repo root. Omit to list every
        /// import in the (filtered) snapshot — pass `-` to keep this
        /// position explicit when wrapping in shell pipelines.
        file: Option<String>,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
    /// Symmetric reference query — incoming or outgoing references.
    /// Use the dedicated `callers` / `callees` subcommands for the
    /// common call-graph questions; reach for `refs` when you need
    /// type refs, imports, reads / writes, annotations, or JSX
    /// component instantiations (`--kind instantiate`).
    Refs {
        /// Symbol name or qualified path.
        symbol: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        kind: Option<String>,
        /// Query direction: incoming = who references the symbol;
        /// outgoing = what the symbol references.
        #[arg(long, value_enum)]
        direction: Option<CliReferenceDirection>,
        /// For outgoing queries, include unresolved method calls,
        /// type refs, annotations, and duplicate Tier-2/Tier-3 rows.
        #[arg(long)]
        include_noise: bool,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Include repo-wide Tier-3 readiness in addition to this query's analyzers.
        #[arg(long)]
        verbose_tier3: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliReferenceDirection {
    Incoming,
    Outgoing,
}

impl CliReferenceDirection {
    fn as_wire(self) -> &'static str {
        match self {
            Self::Incoming => "incoming",
            Self::Outgoing => "outgoing",
        }
    }
}

pub async fn run(args: Args) -> Result<()> {
    let paths = match args.runtime_dir.clone() {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };

    let (method, params): (&str, Value) = match &args.command {
        QueryCommand::Repos => ("list_repos", Value::Null),
        QueryCommand::Outline {
            file,
            repo,
            branch,
            anchor,
            kind,
            max_depth,
            limit,
            verbose_tier3,
        } => {
            let mut p = serde_json::Map::new();
            if let Some(r) = repo {
                p.insert("repo".into(), Value::String(r.clone()));
            }
            // Treat trailing `/` as the directory-mode signal.
            if file.ends_with('/') {
                p.insert("path".into(), Value::String(file.clone()));
            } else {
                p.insert("file".into(), Value::String(file.clone()));
            }
            if let Some(b) = branch {
                p.insert("branch".into(), Value::String(b.clone()));
            }
            if let Some(a) = anchor {
                p.insert("anchor".into(), Value::String(a.clone()));
            }
            if let Some(k) = kind {
                p.insert("kind".into(), Value::String(k.clone()));
            }
            if let Some(d) = max_depth {
                p.insert("max_depth".into(), json!(d));
            }
            if let Some(l) = limit {
                p.insert("limit".into(), json!(l));
            }
            insert_verbose_tier3(&mut p, *verbose_tier3);
            ("get_outline", Value::Object(p))
        }
        QueryCommand::Symbols {
            query,
            repo,
            branch,
            anchor,
            kind,
            path,
            container,
            fuzzy,
            limit,
            signature_only,
            verbose_tier3,
        } => (
            "find_symbols",
            symbols_query(SymbolsQueryArgs {
                query,
                repo,
                branch,
                anchor,
                kind,
                path,
                container,
                fuzzy: *fuzzy,
                limit: *limit,
                signature_only: *signature_only,
                verbose_tier3: *verbose_tier3,
            }),
        ),
        QueryCommand::Source {
            qualified,
            repo,
            branch,
            anchor,
            file,
            line,
            verbose_tier3,
        } => {
            let mut p = serde_json::Map::new();
            if let Some(r) = repo {
                p.insert("repo".into(), Value::String(r.clone()));
            }
            p.insert("qualified".into(), Value::String(qualified.clone()));
            if let Some(b) = branch {
                p.insert("branch".into(), Value::String(b.clone()));
            }
            if let Some(a) = anchor {
                p.insert("anchor".into(), Value::String(a.clone()));
            }
            if let Some(f) = file {
                p.insert("file".into(), Value::String(f.clone()));
            }
            if let Some(line) = line {
                p.insert("line".into(), json!(line));
            }
            insert_verbose_tier3(&mut p, *verbose_tier3);
            ("get_symbol_source", Value::Object(p))
        }
        QueryCommand::Subtypes {
            name,
            repo,
            branch,
            anchor,
            limit,
            verbose_tier3,
        } => (
            "find_subtypes",
            name_query(name, repo, branch, anchor, *limit, *verbose_tier3),
        ),
        QueryCommand::Supertypes {
            name,
            repo,
            branch,
            anchor,
            limit,
            verbose_tier3,
        } => (
            "find_supertypes",
            name_query(name, repo, branch, anchor, *limit, *verbose_tier3),
        ),
        QueryCommand::Callers {
            name,
            repo,
            branch,
            anchor,
            limit,
            verbose_tier3,
        } => (
            "find_callers",
            name_query(name, repo, branch, anchor, *limit, *verbose_tier3),
        ),
        QueryCommand::Callees {
            name,
            repo,
            branch,
            anchor,
            limit,
            verbose_tier3,
        } => (
            "find_callees",
            name_query(name, repo, branch, anchor, *limit, *verbose_tier3),
        ),
        QueryCommand::Imports {
            file,
            repo,
            branch,
            anchor,
            limit,
            verbose_tier3,
        } => {
            let mut p = serde_json::Map::new();
            if let Some(repo) = repo {
                p.insert("repo".into(), Value::String(repo.clone()));
            }
            if let Some(f) = file
                && f != "-"
            {
                p.insert("file".into(), Value::String(f.clone()));
            }
            if let Some(b) = branch {
                p.insert("branch".into(), Value::String(b.clone()));
            }
            if let Some(a) = anchor {
                p.insert("anchor".into(), Value::String(a.clone()));
            }
            if let Some(l) = limit {
                p.insert("limit".into(), json!(l));
            }
            insert_verbose_tier3(&mut p, *verbose_tier3);
            ("find_imports", Value::Object(p))
        }
        QueryCommand::Refs {
            symbol,
            repo,
            kind,
            direction,
            include_noise,
            branch,
            anchor,
            limit,
            verbose_tier3,
        } => {
            let mut p = serde_json::Map::new();
            if let Some(repo) = repo {
                p.insert("repo".into(), Value::String(repo.clone()));
            }
            p.insert("symbol".into(), Value::String(symbol.clone()));
            if let Some(k) = kind {
                p.insert("kind".into(), Value::String(k.clone()));
            }
            if let Some(direction) = direction {
                p.insert(
                    "direction".into(),
                    Value::String(direction.as_wire().into()),
                );
            }
            if *include_noise {
                p.insert("include_noise".into(), Value::Bool(true));
            }
            if let Some(b) = branch {
                p.insert("branch".into(), Value::String(b.clone()));
            }
            if let Some(a) = anchor {
                p.insert("anchor".into(), Value::String(a.clone()));
            }
            if let Some(l) = limit {
                p.insert("limit".into(), json!(l));
            }
            insert_verbose_tier3(&mut p, *verbose_tier3);
            ("find_references", Value::Object(p))
        }
    };

    check_daemon_version(&paths.control, VersionGuardMode::Cli).await?;

    let resp = rpc_client::round_trip(&paths.cairn, method, params)
        .await
        .with_context(|| format!("talking to {}", paths.cairn.display()))?;

    if let Some(err) = &resp.error {
        eprintln!("error: {}", err.message);
        return Err(anyhow!(err.message.clone()));
    }
    let Some(value) = resp.result else {
        println!("(empty result)");
        return Ok(());
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return Ok(());
    }

    render(method, &value);
    Ok(())
}

fn name_query(
    name: &str,
    repo: &Option<String>,
    branch: &Option<String>,
    anchor: &Option<String>,
    limit: Option<u32>,
    verbose_tier3: bool,
) -> Value {
    let mut p = serde_json::Map::new();
    if let Some(repo) = repo {
        p.insert("repo".into(), Value::String(repo.clone()));
    }
    p.insert("name".into(), Value::String(name.to_string()));
    if let Some(b) = branch {
        p.insert("branch".into(), Value::String(b.clone()));
    }
    if let Some(a) = anchor {
        p.insert("anchor".into(), Value::String(a.clone()));
    }
    if let Some(l) = limit {
        p.insert("limit".into(), json!(l));
    }
    insert_verbose_tier3(&mut p, verbose_tier3);
    Value::Object(p)
}

struct SymbolsQueryArgs<'a> {
    query: &'a str,
    repo: &'a Option<String>,
    branch: &'a Option<String>,
    anchor: &'a Option<String>,
    kind: &'a Option<String>,
    path: &'a Option<String>,
    container: &'a Option<String>,
    fuzzy: bool,
    limit: Option<u32>,
    signature_only: bool,
    verbose_tier3: bool,
}

fn symbols_query(args: SymbolsQueryArgs<'_>) -> Value {
    let mut p = serde_json::Map::new();
    p.insert("query".into(), Value::String(args.query.to_string()));
    if let Some(repo) = args.repo {
        p.insert("repo".into(), Value::String(repo.clone()));
    }
    if let Some(b) = args.branch {
        p.insert("branch".into(), Value::String(b.clone()));
    }
    if let Some(a) = args.anchor {
        p.insert("anchor".into(), Value::String(a.clone()));
    }
    if let Some(k) = args.kind {
        p.insert("kind".into(), Value::String(k.clone()));
    }
    if let Some(path) = args.path {
        p.insert("path".into(), Value::String(path.clone()));
    }
    if let Some(container) = args.container {
        p.insert("container".into(), Value::String(container.clone()));
    }
    if args.fuzzy {
        p.insert("fuzzy".into(), Value::Bool(true));
    }
    if let Some(l) = args.limit {
        p.insert("limit".into(), json!(l));
    }
    if args.signature_only {
        p.insert("signature_only".into(), Value::Bool(true));
    }
    insert_verbose_tier3(&mut p, args.verbose_tier3);
    Value::Object(p)
}

fn insert_verbose_tier3(p: &mut serde_json::Map<String, Value>, verbose_tier3: bool) {
    if verbose_tier3 {
        p.insert("verbose_tier3".into(), Value::Bool(true));
    }
}

// ─── rendering ─────────────────────────────────────────────────────────────
//
// Output is line-oriented: one record per line where possible, columns
// separated by spaces (filenames are last so trailing whitespace is
// irrelevant). The intent is grep / awk-ability, not a TUI.

/// Format a `target_path` field for pretty stdout output. Returns
/// `"\ttarget=<path>"` when the path is set, empty string otherwise —
/// so call sites can append it unconditionally without breaking the
/// existing column layout for rows that have no workspace-internal
/// target. Mirrors the Tier-2.5 wire contract: only Tier-2.5+
/// resolutions populate `target_path`, so the suffix is the visible
/// signal that a row was pinned to a specific workspace file.
fn format_target_path(target_path: &Option<String>) -> String {
    target_path
        .as_deref()
        .map(|p| format!("\ttarget={p}"))
        .unwrap_or_default()
}

/// Emit one stderr line when a result advertises `Partial` so a shell
/// user spots the SLA caveat without it polluting the grep-friendly
/// stdout stream. Cairn answers immediately and reports what it has;
/// "partial" means the underlying index is still warming, not that
/// the query failed.
fn note_partial(c: &Completeness) {
    if let Completeness::Partial {
        missing_tiers,
        reason,
    } = c
    {
        let tiers = if missing_tiers.is_empty() {
            String::new()
        } else {
            let names: Vec<String> = missing_tiers
                .iter()
                .map(|t| format!("{t:?}").to_lowercase())
                .collect();
            format!(" missing={}", names.join(","))
        };
        let why = reason
            .as_ref()
            .map(|r| format!(" reason={}", r.as_str()))
            .unwrap_or_default();
        eprintln!("# partial result{tiers}{why}");
    }
}

fn render(method: &str, value: &Value) {
    match method {
        "list_repos" => {
            if let Ok(r) = serde_json::from_value::<ListReposResult>(value.clone()) {
                for repo in &r.repos {
                    println!(
                        "{}\t{}\t[{}]\t{:?}\tfiles={}\tsymbols={}",
                        repo.alias,
                        repo.root,
                        repo.languages.join(","),
                        repo.status,
                        repo.current_file_count,
                        repo.current_symbol_count
                    );
                }
                return;
            }
        }
        "get_outline" => {
            if let Ok(r) = serde_json::from_value::<OutlineResult>(value.clone()) {
                for it in &r.items {
                    let sig = it.signature.as_deref().unwrap_or("");
                    let file = it.file.as_deref().unwrap_or("-");
                    println!(
                        "{}:{}\t{:?}\t{}\t{}",
                        file, it.line, it.kind, it.qualified, sig
                    );
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_symbols" => {
            if let Ok(r) = serde_json::from_value::<FindSymbolResult>(value.clone()) {
                for h in &r.items {
                    let sig = h.signature.as_deref().unwrap_or("");
                    println!("{}\t{:?}\t{}\t{}", h.location, h.kind, h.qualified, sig);
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_subtypes" => {
            if let Ok(r) = serde_json::from_value::<FindSubtypesResult>(value.clone()) {
                for h in &r.items {
                    let iface = h.interface_qualified.as_deref().unwrap_or("-");
                    let target = format_target_path(&h.target_path);
                    println!(
                        "{}\t{}\t{}\t{}({}){}",
                        h.location, h.kind, h.type_qualified, h.kind, iface, target
                    );
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_supertypes" => {
            if let Ok(r) = serde_json::from_value::<FindSupertypesResult>(value.clone()) {
                for h in &r.items {
                    let iface = h.interface_qualified.as_deref().unwrap_or("-");
                    let target = format_target_path(&h.target_path);
                    println!(
                        "{}\t{}\t{} {} {}{}",
                        h.location, h.kind, h.type_qualified, h.kind, iface, target
                    );
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_callers" => {
            if let Ok(r) = serde_json::from_value::<FindCallersResult>(value.clone()) {
                for h in &r.items {
                    let enc = h.enclosing_qualified.as_deref().unwrap_or("-");
                    let snippet = h.snippet.as_deref().unwrap_or("");
                    let target = format_target_path(&h.target_path);
                    println!(
                        "{}\t{} -> {}\t{}{}",
                        h.location, enc, h.target_name, snippet, target
                    );
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_callees" => {
            if let Ok(r) = serde_json::from_value::<FindCalleesResult>(value.clone()) {
                for h in &r.items {
                    let target_name = h
                        .target_qualified
                        .as_deref()
                        .unwrap_or(h.target_name.as_str());
                    let snippet = h.snippet.as_deref().unwrap_or("");
                    let target = format_target_path(&h.target_path);
                    println!("{}\t-> {}\t{}{}", h.location, target_name, snippet, target);
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_imports" => {
            if let Ok(r) = serde_json::from_value::<ImportsResult>(value.clone()) {
                for h in &r.items {
                    let imp = h.imported.as_deref().unwrap_or("*");
                    let alias = h
                        .alias
                        .as_deref()
                        .map(|a| format!(" as {a}"))
                        .unwrap_or_default();
                    let reex = if h.is_reexport { " [pub]" } else { "" };
                    let target = format_target_path(&h.target_path);
                    println!(
                        "{}\t{}::{}{}{}{}",
                        h.location, h.to_module, imp, alias, reex, target
                    );
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "find_references" => {
            if let Ok(r) = serde_json::from_value::<FindReferencesResult>(value.clone()) {
                for h in &r.items {
                    let enc = h.enclosing_qualified.as_deref().unwrap_or("-");
                    let target = format_target_path(&h.target_path);
                    println!(
                        "{}\t{:?}\t{}\tin {}{}",
                        h.location, h.kind, h.target_name, enc, target
                    );
                }
                note_partial(&r.completeness);
                return;
            }
        }
        "get_symbol_source" => {
            if let Ok(r) = serde_json::from_value::<GetSymbolSourceResult>(value.clone()) {
                println!("// {} ({:?}) — {}", r.qualified, r.kind, r.location);
                println!("{}", r.source);
                return;
            }
        }
        _ => {}
    }
    // Fallback: dump as JSON if we couldn't decode into a known type.
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn source_line_is_one_indexed_and_forwarded_without_conversion() {
        let cli = crate::Cli::try_parse_from([
            "cairn",
            "query",
            "source",
            "crate::same",
            "--file",
            "src/lib.rs",
            "--line",
            "1",
        ])
        .unwrap();

        let crate::Command::Query(Args {
            command: QueryCommand::Source { file, line, .. },
            ..
        }) = cli.command
        else {
            panic!("expected query source command");
        };
        assert_eq!(file.as_deref(), Some("src/lib.rs"));
        assert_eq!(line, Some(1));
    }

    #[test]
    fn source_line_zero_is_rejected_by_cli() {
        let err = crate::Cli::try_parse_from([
            "cairn",
            "query",
            "source",
            "crate::same",
            "--file",
            "src/lib.rs",
            "--line",
            "0",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn outline_cli_accepts_snapshot_scope_without_transforming_values() {
        let cli = crate::Cli::try_parse_from([
            "cairn",
            "query",
            "outline",
            "src/lib.rs",
            "--repo",
            "demo",
            "--branch",
            "feature/scope",
            "--anchor",
            "HEAD",
        ])
        .unwrap();

        let crate::Command::Query(Args {
            command:
                QueryCommand::Outline {
                    repo,
                    branch,
                    anchor,
                    ..
                },
            ..
        }) = cli.command
        else {
            panic!("expected query outline command");
        };
        assert_eq!(repo.as_deref(), Some("demo"));
        assert_eq!(branch.as_deref(), Some("feature/scope"));
        assert_eq!(anchor.as_deref(), Some("HEAD"));
    }

    #[test]
    fn symbols_query_includes_path_and_container_filters() {
        let repo = Some("demo".to_string());
        let branch = None;
        let anchor = None;
        let kind = Some("function".to_string());
        let path = Some("src/api/".to_string());
        let container = Some("Client".to_string());

        let value = symbols_query(SymbolsQueryArgs {
            query: "fetchJson",
            repo: &repo,
            branch: &branch,
            anchor: &anchor,
            kind: &kind,
            path: &path,
            container: &container,
            fuzzy: true,
            limit: Some(5),
            signature_only: true,
            verbose_tier3: false,
        });

        assert_eq!(
            value,
            json!({
                "query": "fetchJson",
                "repo": "demo",
                "kind": "function",
                "path": "src/api/",
                "container": "Client",
                "fuzzy": true,
                "limit": 5,
                "signature_only": true
            })
        );
    }

    #[test]
    fn symbols_query_omits_unset_scope_filters() {
        let none = None;

        let value = symbols_query(SymbolsQueryArgs {
            query: "fetchJson",
            repo: &none,
            branch: &none,
            anchor: &none,
            kind: &none,
            path: &none,
            container: &none,
            fuzzy: false,
            limit: None,
            signature_only: false,
            verbose_tier3: false,
        });

        assert_eq!(value, json!({"query": "fetchJson"}));
    }

    #[test]
    fn symbols_query_includes_verbose_tier3_when_requested() {
        let none = None;

        let value = symbols_query(SymbolsQueryArgs {
            query: "fetchJson",
            repo: &none,
            branch: &none,
            anchor: &none,
            kind: &none,
            path: &none,
            container: &none,
            fuzzy: false,
            limit: None,
            signature_only: false,
            verbose_tier3: true,
        });

        assert_eq!(value, json!({"query": "fetchJson", "verbose_tier3": true}));
    }
}
