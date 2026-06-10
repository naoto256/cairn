# Cairn Plugin

Cairn is a local symbol-aware code index served over MCP. This plugin
wires the `cairn` MCP server into the host (Claude Code or Codex) and
installs a `PreToolUse` Bash hook that nudges grep-first habits toward
the matching cairn tool when the current working directory belongs to
a cairn-registered repo.

## What it does

- **MCP server registration** (`.mcp.json`). Adds `cairn` over stdio so
  the host can call `list_repos` / `get_outline` / `find_symbols` /
  `find_subtypes` / `find_supertypes` / `find_callers` /
  `find_callees` / `find_references` / `find_imports` /
  `get_symbol_source` / `register_repo` / `reindex_repo` without any
  per-project setup.
- **`grep` nudge hook** (`hooks/hooks.json` →
  `tools/cairn-nudge.sh`). Inspects every Bash call. If the command
  starts with `grep` / `rg` / `ag` / `ack` / `egrep` / `fgrep` AND the
  `cwd` is a cairn-registered repo, the hook **lets the call run** and
  emits a `hookSpecificOutput.additionalContext` advisory that names
  the closest cairn tool (`find_symbols`, `find_subtypes` /
  `find_supertypes`, `find_callers` / `find_callees`,
  `find_imports`, or `find_references`) with a one-line explanation
  of what it returns. The advisory surfaces in the agent's next-turn
  context so the next call defaults to the index, but the current
  `grep` is not interrupted. Non-grep commands and non-registered
  cwds pass through silently. Any dependency / runtime failure
  (missing `cairn` or `jq` on `PATH`, daemon down, parse error) is a
  no-op so a broken hook never breaks a turn.

## Prerequisites

- `cairn daemon` is running. (`cairn daemon` is the long-lived index
  service the MCP server talks to.)
- `cairn` is available on `PATH`.
- `jq` is available on `PATH` for the nudge hook.

## Install

This plugin is shipped in-tree alongside the cairn binary so its
version matches the daemon it talks to. Both Claude Code and Codex
discover plugins through marketplace catalogs (`marketplace.json`),
not by scanning arbitrary directories — the repo root carries a
`.claude-plugin/marketplace.json` that points at this `plugin/`
subdir as the install source.

### Claude Code

From a published GitHub remote:

```sh
claude plugin marketplace add naoto256/cairn
claude plugin install cairn@naoto256-cairn
```

From a local checkout (handy during development):

```sh
claude plugin marketplace add /absolute/path/to/cairn
claude plugin install cairn@naoto256-cairn
```

After install, restart the Claude Code session so the MCP server
registration and hook take effect.

See https://code.claude.com/docs/en/discover-plugins for the full
plugin command reference.

### Codex

From a published GitHub remote:

```sh
codex plugin marketplace add naoto256/cairn
codex plugin add cairn@naoto256-cairn
```

From a local checkout (handy during development):

```sh
codex plugin marketplace add /absolute/path/to/cairn
codex plugin add cairn@naoto256-cairn
```

After install, restart the Codex session so the MCP server registration
and hook take effect.

Codex reads `.codex-plugin/plugin.json`, `.mcp.json`, and
`hooks/hooks.json` from the same `plugin/` directory. See
https://developers.openai.com/codex/hooks for the host-specific
hook contract.

The `${PLUGIN_ROOT:-$CLAUDE_PLUGIN_ROOT}` expansion in
`hooks/hooks.json` handles either host's plugin-path environment
variable, and `tools/cairn-nudge.sh` emits the same
`hookSpecificOutput.additionalContext` payload for both hosts —
non-blocking advisory on `PreToolUse`.

## Configuration

The MCP server is discovered as `cairn mcp` on `PATH`; the nudge hook
detects cairn-registered cwds by calling `cairn query repos --json` and
matching the cwd against `.repos[].root` as a directory prefix (the cwd
is either an exact root or starts with `root/`).

To silence the nudge, either uninstall the plugin:

```sh
/plugin uninstall cairn@naoto256-cairn
```

or set `CAIRN_NUDGE_DISABLED=1` in your shell before launching the host.
