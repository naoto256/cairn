# Cairn Plugin

Cairn is a local symbol-aware code index served over MCP. This plugin
wires the `cairn` MCP server into the host (Claude Code or Codex) and
installs a `PreToolUse` Bash hook that nudges grep-first habits toward
the matching cairn tool when the current working directory belongs to
a cairn-registered repo.

## What it does

- **MCP server registration** (`.mcp.json`). Adds `cairn` over stdio so
  the host can call `list_repos` / `get_outline` / `find_symbols` /
  `find_impls` / `find_imports` / `find_references` /
  `get_symbol_source` / `register_repo` / `reindex_repo` without any
  per-project setup.
- **`grep` nudge hook** (`hooks/hooks.json` →
  `tools/cairn-nudge.sh`). Inspects every Bash call. If the command
  starts with `grep` / `rg` / `ag` / `ack` / `egrep` / `fgrep` AND the
  `cwd` is a cairn-registered repo, the hook blocks the call and
  returns a short reason that names the closest cairn tool
  (`find_symbols`, `find_impls`, `find_imports`, or `find_references`)
  with a one-line explanation of what it returns. The agent re-decides
  with the index in mind. Non-grep commands and non-registered cwds
  pass through silently. Any dependency / runtime failure (missing
  `cairn` or `jq` on `PATH`, daemon down, parse error) is a no-op so a
  broken hook never breaks a turn.

The hook *blocks* rather than passes a soft hint because the failure
mode this plugin is trying to fix is "agent reaches for grep on
muscle memory and never re-reads the MCP server instructions". A
non-blocking advisory was tried first and did not change behaviour;
a blocking nudge with a clear reason gives the agent a single
explicit decision point per grep call. Use a non-grep command (`cat`,
piping through another tool, etc.) to bypass when you genuinely want
raw-text search inside symbol bodies or in files cairn does not
understand.

## Prerequisites

- `cairn daemon` is running. (`cairn daemon` is the long-lived index
  service the MCP server talks to.)
- `cairn` is available on `PATH`.
- `jq` is available on `PATH` for the nudge hook.

## Install

This plugin is shipped in-tree alongside the cairn binary so its
version matches the daemon it talks to. Both Claude Code and Codex
discover plugins under the standard plugin directories.

- **Claude Code**: copy or symlink this directory to
  `~/.claude/plugins/cairn/`, or install via your plugin marketplace
  flow. Claude Code reads `.claude-plugin/plugin.json`,
  `.mcp.json`, and `hooks/hooks.json`.
- **Codex**: same directory works. Codex reads
  `.codex-plugin/plugin.json`, `.mcp.json`, and `hooks/hooks.json`.
  See https://developers.openai.com/codex/hooks for the host-specific
  hook contract.

The `${PLUGIN_ROOT:-$CLAUDE_PLUGIN_ROOT}` expansion in
`hooks/hooks.json` handles either host's plugin-path environment
variable.

## Configuration

There is nothing to configure. The plugin reads no environment
variables of its own. The MCP server is discovered as `cairn mcp` on
`PATH`; the nudge hook detects cairn-registered cwds by calling
`cairn query list-repos` and substring-matching the cwd against the
listed roots.

There is no built-in opt-out switch yet. To silence the nudge
without uninstalling, remove the `PreToolUse` entry from
`hooks/hooks.json` or make `tools/cairn-nudge.sh` non-executable.
A first-class disable mechanism is a candidate follow-up.
