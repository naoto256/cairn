# cairn

Local, symbol-aware code index. A daemon-backed index for fast,
structural code search across the repos you've registered — definitions,
references, impls, imports, source bodies — with no external service.

Status: **0.1.0-alpha.1**. The on-disk format is current-state-of-the-day;
do not depend on it staying stable until 1.0.

## Why

Editors and AI agents need to look up code by structure (the function
`foo`, callers of `bar`, what `Display` is implemented for), not by
fuzzy text. Cairn keeps that index always-current on a local daemon so
each lookup is sub-100 ms — no IDE waking up, no network hop, no
service to subscribe to. The wire surface is JSON-RPC over a Unix
domain socket; a stdio MCP front-end ships in the same binary so
Claude Code and friends can use it directly.

## Architecture

Storage is content-addressed, modelled on git's object store:

| layer | identity | what it holds |
|---|---|---|
| **blob** | `(blob_sha, parser_id)` | parsed symbols / refs / imports / impls of one file's bytes |
| **manifest** | `manifest_id` | `{(path, blob_sha)}` at one point in time |
| **anchor** | name (`HEAD`, `branch/<n>`, `tag/<n>`, `tentative/<id>`) | named pointer to a manifest |

`blob_sha` is git's blob hash, so the same file content parses once
and is shared across every branch / tag / worktree that references
it. Switching branches re-binds anchors instead of accreting per-branch
databases. Detached HEAD checkouts don't create snapshot rows. Past
states stay queryable through the branch anchors that hold them.

Substrate sources:
[`anchor.rs`](crates/cairn-core/src/anchor.rs),
[`manifest.rs`](crates/cairn-core/src/manifest.rs),
[`cas/blob.rs`](crates/cairn-core/src/cas/blob.rs),
[`cas/schema.rs`](crates/cairn-core/src/cas/schema.rs).

## Install

```sh
cargo build --release
cp target/release/cairn ~/.cargo/bin/
```

Requires Rust 1.85+ and a working `git` on `PATH`.

## Use

### Daemon

```sh
cairn daemon
```

Runs in the foreground. Data dir defaults to
`~/Library/Application Support/cairn/` on macOS and
`$XDG_DATA_HOME/cairn/` on Linux. Sockets land in
`~/Library/Caches/cairn/` (macOS) or `$XDG_RUNTIME_DIR/cairn/` (Linux),
both clamped to mode 0700.

Sample `LaunchAgent` plist and `systemd` user unit live in
[`contrib/`](contrib).

### Register a repo

```sh
cairn ctl register-repo --alias my-proj /path/to/repo
cairn ctl status
```

A repo can carry more than one alias; removing one keeps the on-disk
store alive while any other label still references it.

### Query

```sh
cairn query find <name>             --repo <alias>   # symbol by name
cairn query refs <name>             --repo <alias>   # callers / use sites
cairn query source <qualified-name> --repo <alias>   # source body
cairn query outline <alias> <file>                   # per-file outline
cairn query impls --type <T>        --repo <alias>   # what T implements
cairn query impls --trait <T>       --repo <alias>   # what implements T
cairn query imports --file <path>   --repo <alias>   # use / import edges
cairn query repos                                    # registered repos
```

`--anchor <name>` selects a non-default state:
`HEAD` (default), `branch/<n>`, `tag/<n>`, `tentative/<id>`.
The plain `--branch <n>` shorthand is equivalent to
`--anchor branch/<n>`.

### MCP

`cairn mcp` is a stdio JSON-RPC front-end intended to be spawned by an
MCP client. Each MCP tool maps one-to-one onto the query / ctl methods
above.

## Languages

Tier-1 (syntactic, tree-sitter) for Rust / Python / Markdown plus a
generic tree-sitter fallback. Python additionally carries a Tier-2
analyzer (imports, inheritance, refs). Rust Tier-2 (via `syn`) is
shipped as the analyzer in `cairn-lang-rust`; Tier-3 (LSP-grade,
`rust-analyzer`-driven) is roadmapped for the 1.0 line.

## License

Dual-licensed under either [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.
