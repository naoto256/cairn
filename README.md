# cairn

Local, symbol-aware code index for AI coding agents. Cairn keeps a
daemon-backed structural index of the repos you've registered —
definitions, references, impls, imports, source bodies — so agents can
ask precise code questions without waking a full IDE or scraping text.

Status: **0.1.1**. Wire schemas (JSON-RPC + MCP), on-disk format,
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
call local language servers such as `rust-analyzer`, `pyright`, and
`gopls`; Cairn does not require a hosted index or cloud service.

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

Optional runtime dependencies for Tier-3 cross-file resolution:
`rust-analyzer`, `pyright-langserver`, `gopls` (see Languages
section). Make sure they are visible on the daemon's `PATH`; `cairn ctl
doctor` reports which Tier-3 analyzers are discoverable.

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
`PATH` and the daemon is running. The plugin registers `cairn mcp` and
adds a non-blocking hook that nudges broad text searches toward the
matching structural tool when the current directory belongs to a
registered repo.

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

For a local checkout, replace `naoto256/cairn` with the absolute path to
this repository. Restart the agent session after install so the MCP
server registration and hook take effect.

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
- ask who references a symbol, or what a function / method references
- inspect trait / impl relationships and import edges

Agents can omit `repo` for discovery-style tools such as symbol,
reference, impl, and import search; each hit carries a
`repo:branch:file:line` location prefix. Reads default to the daemon's
tentative worktree snapshot, so the agent can see uncommitted edits
without asking you to commit first.

`find_references` is symmetric. `direction=incoming` answers "who
references this symbol?" while `direction=outgoing` answers "what does
this symbol reference?" The default outgoing view returns resolved call
refs only, which makes it useful as a call-graph edge list. Set
`include_noise=true` when auditing raw analyzer output or looking for
unresolved / type / annotation refs.

### CLI

The `cairn query` commands expose the same read-only surface for shell
use and debugging:

```sh
cairn query find <name>             [--repo <alias>]   # symbol by name
cairn query refs <name>             [--repo <alias>]   # incoming / outgoing refs
cairn query source <qualified-name> --repo <alias>     # source body
cairn query outline <alias> <file>                     # per-file outline
cairn query impls --type <T>        [--repo <alias>]   # what T implements
cairn query impls --trait <T>       [--repo <alias>]   # what implements T
cairn query imports --file <path>   [--repo <alias>]   # use / import edges
cairn query repos                                      # registered repos
```

For example, these CLI calls mirror common MCP tool calls:

```sh
cairn query refs handle
cairn query refs crate::service::handle --direction outgoing
cairn query refs crate::service::handle --direction outgoing --include-noise --json
```

Omitting `--repo` searches every registered repo for the four
discovery commands (`find`, `refs`, `impls`, `imports`); each hit
carries its origin in a `repo:branch:file:line` location prefix.
`source` and `outline` still target a single repo.

`--anchor <name>` selects a specific snapshot: `HEAD` (committed
only), `branch/<n>`, `tag/<n>`, or `tentative/<id>`. The plain
`--branch <n>` shorthand is equivalent to `--anchor branch/<n>`.
When both are omitted, reads default to the registered worktree's
`tentative/<id>` snapshot. Pass `--anchor HEAD` explicitly to scope
back to committed-only state.

`outline` takes `<alias>` as positional because it browses a single
repo. `source` takes `--repo <alias>` as a flag because
fully-qualified symbol names can collide with file paths if positional.
This asymmetry is intentional.

### Daemon

```sh
cairn daemon
```

Runs in the foreground. Data dir defaults to
`~/Library/Application Support/cairn/` on macOS and
`$XDG_DATA_HOME/cairn/` on Linux. Sockets land in
`~/Library/Caches/cairn/` (macOS) or `$XDG_RUNTIME_DIR/cairn/` (Linux),
both clamped to mode 0700.

### Register a repo

```sh
cairn ctl register-repo --alias my-proj /path/to/repo
cairn ctl status
```

A repo can carry more than one alias; removing one keeps the on-disk
store alive while any other label still references it.

## Languages

| Language | Tier-1 (syntax) | Tier-2 (semantic) | Tier-3 (cross-file) |
|---|---|---|---|
| Rust | ✅ | ✅ | ✅ (rust-analyzer) |
| Python | ✅ | ✅ | ✅ (pyright-langserver) |
| TypeScript (`.ts` / `.mts` / `.cts`) | ✅ | ✅ | – |
| Go | ✅ | ✅ | ✅ (gopls) |
| Markdown | ✅ | – | – |

Tier-1 is the tree-sitter syntax floor: symbols, outlines, imports,
and other facts that can be extracted from one file. Rust, Python,
Markdown, TypeScript, and Go have first-class backends, with a generic
tree-sitter fallback for additional grammars.

Tier-2 adds language-specific semantic facts from one file. Rust uses
`syn`; Python extracts imports, inheritance, and refs; TypeScript emits
call refs, type-role refs, and class / interface inheritance edges. The
TypeScript backend covers `*.ts`, `*.mts`, and `*.cts`. `.tsx` uses a
separate upstream grammar and is intentionally left for a follow-up
backend. Member-expression calls without a resolved receiver type stay
unresolved in Tier-2.

Tier-3 runs local language servers once per snapshot when their
binaries are discoverable on the daemon's `PATH`. Rust uses
`rust-analyzer` (`source = tier3-rust-analyzer`), Python uses
`pyright-langserver` (`source = pyright-lsp`), and Go uses `gopls`
(`source = gopls-lsp`). Missing binaries are recorded as `Skipped`;
Tier-1 / Tier-2 facts remain available.

Go covers `*.go` functions, receiver-qualified methods, named types,
top-level constants / variables, and imports. Exported visibility
follows Go's first-letter capitalization convention.

Files are picked by extension first (`*.py`, `*.rs`, `*.md`, `*.ts`,
`*.go`, ...). Extensionless executables (`bin/foo` with mode `0755+`)
fall back to shebang detection, including `#!/usr/bin/env python3` and
`#!/usr/bin/env -S uv run --script` PEP 723 scripts.

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
the next register / reindex pass, so `cairn ctl status` and
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
