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

use anyhow::{Context, Result, anyhow};
use cairn_core::sockets::SocketPaths;
use cairn_proto::Completeness;
use cairn_proto::jsonrpc::{JsonRpcVersion, Request, RequestId, Response};
use cairn_proto::methods::{
    FindReferencesResult, FindSymbolResult, GetSymbolSourceResult, ImplsResult, ImportsResult,
    ListReposResult, OutlineResult,
};
use clap::{Args as ClapArgs, Subcommand};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

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
    /// Outline a single file (what's defined and where).
    Outline {
        /// Repository alias.
        repo: String,
        /// File path relative to repo root.
        file: String,
    },
    /// Look up a definition by name (function, struct, method, …).
    Find {
        /// Symbol query — exact name unless `--fuzzy`.
        query: String,
        /// Repository alias.
        #[arg(long)]
        repo: String,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        /// Restrict to symbols of this kind.
        #[arg(long)]
        kind: Option<String>,
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
    },
    /// Print the source of one symbol by qualified name.
    Source {
        /// Fully-qualified name (`crate::module::name` or just `name`
        /// when unambiguous).
        qualified: String,
        #[arg(long)]
        repo: String,
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
    },
    /// Trait/impl edges. Supply `--trait` for "what implements X?",
    /// `--type` for "what does Y implement?", or both.
    Impls {
        #[arg(long)]
        repo: String,
        /// Trait name to match the impl's trait side.
        #[arg(long = "trait", value_name = "TRAIT")]
        trait_: Option<String>,
        /// Type name to match the impl's type side.
        #[arg(long = "type", value_name = "TYPE")]
        type_: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Import edges (`use` statements).
    Imports {
        #[arg(long)]
        repo: String,
        /// File to list imports for. Omit to list every import in
        /// the (filtered) snapshot.
        #[arg(long)]
        file: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Call / use sites of a symbol.
    Refs {
        /// Symbol name or qualified path.
        symbol: String,
        #[arg(long)]
        repo: String,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        /// Raw anchor name (`HEAD`, `branch/<n>`, `tag/<n>`,
        /// `tentative/<id>`). Takes priority over `--branch`.
        #[arg(long)]
        anchor: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
}

pub async fn run(args: Args) -> Result<()> {
    let paths = match args.runtime_dir.clone() {
        Some(p) => SocketPaths::with_runtime_dir(p),
        None => SocketPaths::from_platform_default()?,
    };

    let (method, params): (&str, Value) = match &args.command {
        QueryCommand::Repos => ("list_repos", Value::Null),
        QueryCommand::Outline { repo, file } => {
            ("get_outline", json!({"repo": repo, "file": file}))
        }
        QueryCommand::Find {
            query,
            repo,
            branch,
            anchor,
            kind,
            fuzzy,
            limit,
            signature_only,
        } => {
            let mut p = serde_json::Map::new();
            p.insert("query".into(), Value::String(query.clone()));
            p.insert("repo".into(), Value::String(repo.clone()));
            if let Some(b) = branch {
                p.insert("branch".into(), Value::String(b.clone()));
            }
            if let Some(a) = anchor {
                p.insert("anchor".into(), Value::String(a.clone()));
            }
            if let Some(k) = kind {
                p.insert("kind".into(), Value::String(k.clone()));
            }
            if *fuzzy {
                p.insert("fuzzy".into(), Value::Bool(true));
            }
            if let Some(l) = limit {
                p.insert("limit".into(), json!(l));
            }
            if *signature_only {
                p.insert("signature_only".into(), Value::Bool(true));
            }
            ("find_symbols", Value::Object(p))
        }
        QueryCommand::Source {
            qualified,
            repo,
            branch,
            anchor,
            file,
        } => {
            let mut p = serde_json::Map::new();
            p.insert("repo".into(), Value::String(repo.clone()));
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
            ("get_symbol_source", Value::Object(p))
        }
        QueryCommand::Impls {
            repo,
            trait_,
            type_,
            branch,
            anchor,
            limit,
        } => {
            let mut p = serde_json::Map::new();
            p.insert("repo".into(), Value::String(repo.clone()));
            if let Some(t) = trait_ {
                p.insert("trait".into(), Value::String(t.clone()));
            }
            if let Some(t) = type_ {
                p.insert("type".into(), Value::String(t.clone()));
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
            ("find_impls", Value::Object(p))
        }
        QueryCommand::Imports {
            repo,
            file,
            branch,
            anchor,
            limit,
        } => {
            let mut p = serde_json::Map::new();
            p.insert("repo".into(), Value::String(repo.clone()));
            if let Some(f) = file {
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
            ("find_imports", Value::Object(p))
        }
        QueryCommand::Refs {
            symbol,
            repo,
            kind,
            branch,
            anchor,
            limit,
        } => {
            let mut p = serde_json::Map::new();
            p.insert("repo".into(), Value::String(repo.clone()));
            p.insert("symbol".into(), Value::String(symbol.clone()));
            if let Some(k) = kind {
                p.insert("kind".into(), Value::String(k.clone()));
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
            ("find_references", Value::Object(p))
        }
    };

    let resp = round_trip(&paths.cairn, method, params)
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

// ─── rendering ─────────────────────────────────────────────────────────────
//
// Output is line-oriented: one record per line where possible, columns
// separated by spaces (filenames are last so trailing whitespace is
// irrelevant). The intent is grep / awk-ability, not a TUI.

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
                    let languages = repo.languages();
                    println!(
                        "{}\t{}\t[{}]",
                        repo.alias,
                        repo.root,
                        languages.iter().copied().collect::<Vec<_>>().join(",")
                    );
                    for snap in &repo.snapshots {
                        println!(
                            "    {}\t{}\tfiles={}\tsymbols={}",
                            snap.branches.join("/"),
                            snap.status,
                            snap.file_count,
                            snap.symbol_count
                        );
                    }
                }
                return;
            }
        }
        "get_outline" => {
            if let Ok(r) = serde_json::from_value::<OutlineResult>(value.clone()) {
                for it in &r.items {
                    let sig = it.signature.as_deref().unwrap_or("");
                    println!("{}\t{:?}\t{}\t{}", it.line, it.kind, it.qualified, sig);
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
        "find_impls" => {
            if let Ok(r) = serde_json::from_value::<ImplsResult>(value.clone()) {
                for h in &r.items {
                    let iface = h.interface_qualified.as_deref().unwrap_or("-");
                    println!(
                        "{}\t{}\t{}\timpl({})",
                        h.location, h.kind, h.type_qualified, iface
                    );
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
                    println!(
                        "{}:{}\t{}::{}{}{}",
                        h.file, h.line, h.to_module, imp, alias, reex
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
                    println!(
                        "{}\t{:?}\t{}\tin {}",
                        h.location, h.kind, h.target_name, enc
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
