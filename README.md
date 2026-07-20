# cairn

[![CI](https://github.com/naoto256/cairn/actions/workflows/ci.yml/badge.svg)](https://github.com/naoto256/cairn/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/naoto256/cairn?display_name=tag&sort=semver)](https://github.com/naoto256/cairn/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue)](#license)
[![Homebrew](https://img.shields.io/badge/homebrew-naoto256%2Fcairn-orange)](https://github.com/naoto256/homebrew-cairn)

Local, symbol-aware code index for AI coding agents. Cairn keeps a
daemon-backed structural index of the repos you've registered —
definitions, references, impls, imports, source bodies — so agents can
ask precise code questions without waking a full IDE or scraping text.

Status: **0.8.0**. Wire schemas (JSON-RPC + MCP), on-disk format,
and CLI flags follow SemVer 0.x rules — minor releases may break
compatibility. 1.0 will tag once these surfaces stabilize.

## Why

AI coding agents need to answer structural questions about code: where
the function `foo` is defined, who calls `bar`, what `Display` is
implemented for, or what a handler calls next. Text search can find
tokens; Cairn gives agents a small, local, queryable model of the
codebase.

The long-lived daemon keeps registered repos indexed as they change.
The query surface is JSON-RPC over a Unix domain socket, and the same
binary ships a stdio MCP front-end so Claude Code, Codex, and other
agents can use those structural facts directly. Tier-3 enrichment can
call each language's local LSP listed in the Languages table; Cairn
does not require a hosted index or cloud service.

## Installation

### Binary

#### Homebrew (macOS)

```sh
brew tap naoto256/cairn
brew install cairn
```

#### Debian / Ubuntu

Download `cairn_<version>-1_amd64.deb` from the
[latest release](https://github.com/naoto256/cairn/releases/latest),
then:

```sh
sudo apt install ./cairn_*.deb
```

The Debian package includes the user systemd unit at
`/usr/lib/systemd/user/cairn.service`.

#### Prebuilt binary (any OS / target in the matrix)

Download the tarball for your target from
[Releases](https://github.com/naoto256/cairn/releases/latest), extract,
and put `cairn` on your `PATH`:

```sh
tar -xzf cairn-v<version>-<target>.tar.gz
install cairn-v<version>-<target>/cairn ~/.local/bin/
```

#### From source (Rust 1.85+, working `git`)

```sh
cargo install --git https://github.com/naoto256/cairn cairn
```

Optional runtime dependencies for Tier-3 cross-file resolution are the
local LSP binaries listed in the Languages table. Make sure they are
visible to the daemon; `cairn ctl daemon doctor` reports which Tier-3
analyzers are discoverable and how to fix missing tools.

### Daemon

All query and control commands talk to the running daemon. Homebrew and
Debian installs can run it as a user service; prebuilt / source
installs can either run `cairn daemon` in a terminal or install one of
the sample service files from [`contrib/`](contrib).

Homebrew registers the daemon as a user LaunchAgent:

```sh
brew services start cairn
```

Debian / Ubuntu installs include a user systemd unit:

```sh
systemctl --user enable --now cairn.service
```

### Claude Code and Codex plugin

Install the Cairn plugin in your agent host after the binary is on
`PATH` and the daemon is running. The plugin registers `cairn mcp`
and, via a `SessionStart` hook, injects
`plugin/SERVER_INSTRUCTIONS.md` into each session so the agent
reaches for cairn's structural tools (`find_symbols`, `get_outline`,
`find_references`, etc.) before grep / Read on structure questions.
The guidance is loaded once per session (matchers:
`startup|resume|clear|compact`), not per tool call.

Claude Code:

```sh
claude plugin marketplace add naoto256/cairn
claude plugin install cairn@naoto256-cairn
```

Codex:

```sh
codex plugin marketplace add naoto256/cairn
codex plugin add cairn@naoto256-cairn
```

For a local checkout, replace `naoto256/cairn` with the absolute path
to this repository. Restart the agent session after install so the
MCP server registration and the `SessionStart` hook take effect.

## Use

### MCP

After installing the plugin, you can ask your agent directly:

```text
Add this repo to cairn.
Understand this codebase's structure.
```

`cairn mcp` is a stdio JSON-RPC front-end intended to be spawned by an
MCP client. It gives AI coding agents structural tools over the repos
you register:

- list registered repos and snapshot health
- find symbols by exact name, fuzzy query, kind, container, or path
- read an outline of one file or a directory
- fetch the source body for a specific qualified symbol
- ask who calls a function (`find_callers`) and what it calls
  (`find_callees`)
- inspect type-relation edges from either side — subtypes / extenders
  / implementers (`find_subtypes`) and base types / traits / interfaces
  / mixins (`find_supertypes`)
- ask any other reference question (type / import / read / write /
  annotation) via the symmetric `find_references`
- list import / `use` edges (`find_imports`)

Agents can omit `repo` on every read-side tool — `find_symbols`,
`find_subtypes`, `find_supertypes`, `find_callers`, `find_callees`,
`find_references`, `find_imports`, plus `get_outline` and
`get_symbol_source`. Each hit carries a `repo:branch:file:line` location
prefix. Reads default to the daemon's tentative worktree snapshot, so
the agent can see uncommitted edits without asking you to commit first.

`find_callers` / `find_callees` are thin shortcuts over
`find_references` with `kind=call` and the default "resolved calls
only" filter — reach for them when you want the call graph, and for
`find_references` directly when you need type refs, imports, reads /
writes, annotations, or the noise toggle. `find_subtypes` /
`find_supertypes` walk the same `implementations` table from opposite
sides, so they cover Rust `impl`, TypeScript `extends` /
`implements`, Python inheritance, and ECMAScript mixins under one
shape.

### CLI

The `cairn query` commands mirror the MCP tool surface: each takes the
search target as its first positional argument, and `--repo` as an
optional flag (omit it to search every registered repo).

```sh
cairn query symbols <name>           [--repo <alias>]   # symbol by name (was: find)
cairn query outline <file-or-dir/>   [--repo <alias>] [--branch <name> | --anchor <name>]
cairn query source  <qualified-name> [--repo <alias>] [--file <path> [--line <1-based>]]
cairn query subtypes   <name>        [--repo <alias>]   # who implements / extends / mixes in <name>
cairn query supertypes <name>        [--repo <alias>]   # what <name> extends / implements / mixes in
cairn query callers <name>           [--repo <alias>]   # who calls <name>
cairn query callees <name>           [--repo <alias>]   # what <name> calls
cairn query imports <path>           [--repo <alias>]   # use / import edges in <path>
cairn query refs    <symbol>         [--repo <alias>]   # any reference (type / import / read / write / annotation)
cairn query repos                                       # registered repos
```

For example, these CLI calls mirror common MCP tool calls:

```sh
cairn query callers handle
cairn query callees crate::service::handle
cairn query subtypes Display
cairn query supertypes Dog
cairn query outline crates/cairn-core/src/
cairn query refs Widget --kind type
```

Omitting `--repo` searches every registered repo; each hit carries its
origin in a `repo:branch:file:line` location prefix. `source` requires one
physical declaration: multiple matches return `-32003 AmbiguousSource` with
bounded candidates instead of choosing an arbitrary row. Use `--repo` and
`--file`, then the optional 1-indexed `--line`, to disambiguate. `outline`
interprets a trailing `/` on the positional path as the directory-mode signal
— without it the argument is treated as a single file.

`--anchor <name>` selects a specific snapshot: `HEAD` (committed
only), `branch/<n>`, `tag/<n>`, or `tentative/<id>`. The plain
`--branch <n>` shorthand is equivalent to `--anchor branch/<n>`.
When both are omitted, reads default to the registered worktree's
`tentative/<id>` snapshot. Pass `--anchor HEAD` explicitly to scope
back to committed-only state.

### Daemon

```sh
cairn daemon
```

Runs in the foreground. Data dir defaults to
`~/Library/Application Support/cairn/` on macOS and
`$XDG_DATA_HOME/cairn/` on Linux. Sockets land in
`~/Library/Caches/cairn/` (macOS) or `$XDG_RUNTIME_DIR/cairn/` (Linux),
both clamped to mode 0700.

The sockets open before repository and analyzer initialization finishes.
`cairn ctl daemon status` remains available throughout startup and exits
successfully with closed progress such as `initializing 4/7: watcher barrier`.
Other control and query requests fail once with JSON-RPC `-32005`
(`DaemonInitializing`), including the current phase and a retry hint; clients
choose when to retry rather than entering an automatic retry loop. The daemon
publishes normal query access only after lifecycle recovery, watcher arming,
startup reconciliation, and the periodic scheduler are all ready.

**Environment variables** (all optional):

- `CAIRN_LSP_POOL_MAX_ENTRIES` — hard cap on the number of live
  LSP child processes the daemon keeps around. Default `16`.
  Valid range `1..=64`; values above 64 are clamped to `64` (with
  a warning). Non-numeric, `0`, or negative values fall back to
  the default (with a warning). When the pool is at capacity and
  no idle entry can be evicted, subsequent `with_lsp` calls fail
  closed with `PoolAtCapacity` and the affected analyzer job
  lands as `Failed` (retry after a lease is released, the current
  eviction completes, or via `cairn ctl repo reindex <alias>`).

`cairn ctl daemon shutdown` acknowledges the shutdown request immediately;
this also works while initialization is in progress. Process teardown continues
asynchronously. Cairn stops accepting requests,
cancels active analyzer work, reaps LSP children, and drains workers under a
10-second daemon deadline, followed by a 2-second runtime backstop for residual
blocking filesystem work.

### Register a repo

```sh
cairn ctl repo register --alias my-proj /path/to/repo
cairn ctl repo register --alias mounted --persistent /Volumes/team/repo
cairn ctl daemon status
```

A repo can carry more than one alias; removing one keeps the on-disk
store alive while any other label still references it.

Registrations are ephemeral by default: when a repository root is
definitively missing, the daemon removes its canonical registration and CAS
store. Use `--persistent` for roots that may be temporarily unavailable, such
as removable or network-mounted storage; use `--ephemeral` when re-registering
to restore the default policy. Persistence protects only against missing-root
auto-prune. Aliases are routing labels, so a repository with no aliases is
unreachable and is removed even when persistent. Retargeting the last alias
away from a persistent repository has the same result.

Workspace analyzers (Tier-2.5 and Tier-3) run in daemon background
jobs. Use `cairn ctl jobs list --alias my-proj` to inspect
queued/running/completed analyzer work, or `cairn ctl repo reindex
my-proj --wait` when a script needs to block until the current
jobs finish.

When a newly installed cairn binary first opens a previously
registered repo, the daemon's startup scanners detect
analyzer-revision and parser-revision drift and enqueue the
appropriate reruns (parser-revision drift triggers a full repo
reindex; analyzer-revision drift enqueues a targeted analyzer
rerun). The watcher comes up first so the scanner runs against
live state; no manual `cairn ctl repo reindex <alias>` is needed
in the common case. If a rerun is lost (e.g. the worktree was
inaccessible at scan time), `cairn ctl daemon doctor` surfaces
the gap as an `analyzer rerun health` check with operator-facing
remediation (failed-at-current, queued, missing-row, or a
chain-broken Fail if drift persists despite a successful
analyzer pass) and a concrete next-step.

Repo-level reindex intent (watcher events, `cairn ctl repo
reindex`, parser-revision drift recovery) is driven by a durable
per-`repo_hash` state machine — desired / applied / force
generation counters, in-flight attempt, retry backoff, watcher
lifecycle — visible via `repo_status.reconcile` (data-RPC) and
`status.repos[].reconcile` (control), and grouped in
`cairn ctl daemon doctor` as `reconcile dirty gap` / `reconcile
retry` / `reconcile attempt` / `watcher lifecycle` /
`reconcile invariants` checks. The wire object is repeated per
alias for a canonical repo but carries `aliases: [...]` so a
client can dedup by `repo_hash`. Older clients that ignore the
field see no wire-shape change: the new `reconcile` object itself
is an `Option` with `serde(default, skip_serializing_if =
"Option::is_none")` so absent-in / absent-out round-trips
cleanly, and inside `RepoReconcileStatus` every nullable durable
column is likewise `Option<T>` with skip-when-`None` — required
scalar fields (`repo_hash`, generation counters,
`consecutive_failures`, `watcher_state`) always serialize.

The default current view is reported as ready only when its tentative anchor
is backed by the latest applied reconcile generation, no reconcile attempt or
dirty gap remains, the watcher is active, and the last successful full scan is
recent. Queries still return rows from an older current snapshot when useful,
but mark the response partial with
`file_not_indexed_or_snapshot_stale`; exact file queries also verify that the
file belongs to the selected manifest. Explicit `--anchor` / `--branch`
queries are historical reads and are not judged against current-worktree
freshness. A stale or unindexed file passed to `get_symbol_source` is surfaced
as JSON-RPC `-32002` with structured recovery data instead of a silent miss.
A qualified miss without `file` returns `-32004 SnapshotStale` when current
freshness cannot be proven. Tentative source and reference-snippet fallbacks
also verify the worktree bytes against the indexed blob SHA; changed bytes are
never presented as belonging to the selected snapshot.

## Languages

| Language | Tier-1 (syntax) | Tier-2 (semantic) | Tier-2.5 (in-process) | Tier-3 (cross-file) |
|---|---|---|---|---|
| Rust | ✅ | ✅ | – | ✅ (rust-analyzer) |
| Python | ✅ | ✅ | ✅ | ✅ (pyright-langserver) |
| Go | ✅ | ✅ | – | ✅ (gopls) |
| TypeScript / TSX (`.ts` / `.mts` / `.cts` / `.tsx`) | ✅ | ✅ | – | ✅ (typescript-language-server) |
| JavaScript (`.js` / `.mjs` / `.cjs` / `.jsx`) | ✅ | ✅ | ✅ | ✅ (typescript-language-server) |
| Java (`.java`) | ✅ | ✅ | – | ✅ (jdtls) |
| C# (`.cs`) | ✅ | ✅ | ✅ | ✅ (csharp-ls) |
| Kotlin (`.kt` / `.kts`) | ✅ | ✅ | ✅ | ✅ (kotlin-language-server) |
| Swift (`.swift`) | ✅ | ✅ | ✅ | ✅ (sourcekit-lsp) |
| C (`.c` / `.h`) | ✅ | ✅ | – | ✅ (clangd) |
| C++ (`.cpp` / `.cc` / `.cxx` / `.hpp` / `.hxx` / `.hh` / `.h++` / `.C` / `.H`) | ✅ | ✅ | – | ✅ (clangd) |
| Objective-C (`.m`) | ✅ | ✅ | – | ✅ (clangd) |
| Ruby (`.rb` / `.rake` / `Gemfile` / `Rakefile`) | ✅ | ✅ | ✅ | ✅ (ruby-lsp) |
| PHP (`.php`) | ✅ | ✅ | ✅ | ✅ (phpantom-lsp) |
| Markdown | ✅ | – | – | – |

Tier-1 is the tree-sitter syntax floor: symbols, outlines, imports,
and other facts extractable from one file. Fourteen first-class
language backends ship with 0.8.0 (plus Markdown for outlines),
and a generic tree-sitter fallback covers additional grammars.

Tier-2 adds language-specific semantic facts from one file —
inheritance / interface / mixin / extension edges, call refs with
same-file callees resolved, and import edges. The four-label
taxonomy `inherit` / `implement` / `mixin` / `extension` is shared
across every backend so `find_subtypes` / `find_supertypes` compare
cleanly across languages.

Tier-2.5 is the in-process workspace analyzer layer for cross-file
resolution without an external LSP. The 0.8.0 scope is complete with
seven Tier-2.5 backends — Ruby, PHP, Python, Kotlin, Swift, C#, and
JavaScript. These analyzers build manifest-scoped resolutions and
attach `target_path` to imports, subtype / supertype edges,
references, callers, and callees, so a `find_subtypes` query
against an interface returns the workspace file each implementation
lives in directly, even when no Tier-3 LSP runs. Languages without
a Tier-2.5 backend (Rust, Go, TypeScript, Java, C, C++,
Objective-C) skip the layer and rely on Tier-3 for cross-file
resolution. Java and TypeScript remain supported at Tier-1, Tier-2,
and Tier-3, but neither is part of the 0.8.0 Tier-2.5 scope.

TypeScript currently relies on `typescript-language-server` / tsserver
for cross-file semantics. Without a usable local server, its Tier-1 and
Tier-2 facts remain available, but Cairn is not self-contained for
TypeScript cross-file queries. This is a known gap, not a permanent
endorsement of external-LSP quality as an architectural dependency;
the Tier-2.5 position will be reconsidered on the path to 1.0.

The runner reads each Tier-2.5 file's bytes once per pass and
hands them to the analyzer through the `WorkspaceFile::source_bytes`
contract; a transiently inaccessible worktree marks the run as
`Failed` and preserves prior facts rather than silently emitting an
empty result. Tier-3 analyzers are not pre-read on this path so the
LSP-driven workflow keeps its existing memory profile. Tier-2.5
jobs share the workspace analyzer scheduler with Tier-3.

Tier-3 runs local language servers once per snapshot when their
binaries are discoverable by the daemon. Every supported language
except Markdown has a Tier-3 analyzer; the LSP for each language is
listed in the table above. Missing binaries or unsuitable
workspaces are recorded as `Skipped`; Tier-1 / Tier-2 / Tier-2.5
facts remain available.

Files are picked by extension first (`*.py`, `*.rs`, `*.md`, `*.ts`,
`*.go`, ...). Extensionless executables (`bin/foo` with mode `0755+`)
fall back to shebang detection, including `#!/usr/bin/env python3`,
`#!/usr/bin/env -S uv run --script` PEP 723 scripts, `node` (for
JavaScript), and `ruby` (for Ruby). For C / C++ / Objective-C, `.h`
stays with the C backend (Objective-C `.mm` is not yet claimed —
Objective-C++ headers are ambiguous enough that mis-claiming them
hurts more than it helps).

## Architecture

Storage is content-addressed, modelled on git's object store:

| layer | identity | what it holds |
|---|---|---|
| **blob** | `(blob_sha, parser_id)` | parsed symbols / refs / imports / impls of one file's bytes |
| **manifest** | `manifest_id` | `{(path, blob_sha)}` at one point in time |
| **anchor** | name (`HEAD`, `branch/<n>`, `tag/<n>`, `tentative/<id>`) | named pointer to a manifest |

`blob_sha` is git's blob hash, so the same file content parses once
and is shared across every branch / tag / worktree that references it.
Switching branches re-binds anchors instead of accreting per-branch
databases. Detached HEAD checkouts don't create snapshot rows.

`branch/<n>` and `tag/<n>` anchors track live git refs: a ref that
disappears from `git for-each-ref` is pruned from the anchor table on
the next register / reindex pass, so `cairn ctl daemon status` and
`list_repos` don't keep stale labels. `HEAD` and `tentative/<id>` are
not subject to that prune.

Substrate sources:
[`anchor.rs`](crates/cairn-core/src/anchor.rs),
[`manifest.rs`](crates/cairn-core/src/manifest.rs),
[`cas/blob.rs`](crates/cairn-core/src/cas/blob.rs),
[`cas/schema.rs`](crates/cairn-core/src/cas/schema.rs).

## License

Dual-licensed under either [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.
