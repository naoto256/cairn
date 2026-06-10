# cairn

Local, symbol-aware code index for AI coding agents. Cairn keeps a
daemon-backed structural index of the repos you've registered —
definitions, references, impls, imports, source bodies — so agents can
ask precise code questions without waking a full IDE or scraping text.

Status: **0.2.0**. Wire schemas (JSON-RPC + MCP), on-disk format,
and CLI flags follow SemVer 0.x rules — minor releases may break
compatibility. 1.0 will tag once these surfaces stabilize.

Upgrading from 0.1.x: the Tier-3 rust reference `source` label
changed from the legacy `tier3-rust-analyzer` alias to the uniform
`tier3-rust-analyzer-lsp` (matching `tier3-pyright-lsp` and
`tier3-gopls-lsp`). Clients that match on that string need to update;
rows under the old label are cleared and re-stamped on the next
reindex. `list_repos` snapshot `status` also reports `empty` /
`no_analyzer` / `stale` alongside the previous `ready`, so treat
new values as informational rather than errors.

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
cairn query outline <file-or-dir/>   [--repo <alias>]   # file or directory outline
cairn query source  <qualified-name> [--repo <alias>]   # source body for a symbol
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
origin in a `repo:branch:file:line` location prefix. `source` returns
the first matching qualified name across the registry, which is usually
unambiguous; pass `--repo` to pin it. `outline` interprets a trailing
`/` on the positional path as the directory-mode signal — without it
the argument is treated as a single file.

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
| TypeScript / TSX (`.ts` / `.mts` / `.cts` / `.tsx`) | ✅ | ✅ | – |
| JavaScript (`.js` / `.mjs` / `.cjs` / `.jsx`) | ✅ | ✅ | – |
| Go | ✅ | ✅ | ✅ (gopls) |
| Markdown | ✅ | – | – |

Tier-1 is the tree-sitter syntax floor: symbols, outlines, imports,
and other facts that can be extracted from one file. Rust, Python,
Markdown, TypeScript, and Go have first-class backends, with a generic
tree-sitter fallback for additional grammars.

Tier-2 adds language-specific semantic facts from one file. Rust uses
`syn`; Python extracts imports, inheritance, and refs; TypeScript emits
call refs, type-role refs, and class / interface inheritance edges. The
TypeScript backend family covers `*.ts` / `*.mts` / `*.cts`, `*.tsx`
(via the upstream TSX grammar), and plain JavaScript (`*.js` / `*.mjs`
/ `*.cjs` / `*.jsx`, shebang `node`), sharing one visitor and analyzer;
JSX component usages are recorded as instantiate refs.
Member-expression calls without a resolved receiver type stay
unresolved in Tier-2.

Tier-3 runs local language servers once per snapshot when their
binaries are discoverable on the daemon's `PATH`. Rust uses
`rust-analyzer` (`source = tier3-rust-analyzer-lsp`), Python uses
`pyright-langserver` (`source = tier3-pyright-lsp`), and Go uses
`gopls` (`source = tier3-gopls-lsp`). Missing binaries are recorded
as `Skipped`; Tier-1 / Tier-2 facts remain available.

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
