# Cairn Plugin

Cairn is a local symbol-aware code index served over MCP. This plugin
wires the `cairn` MCP server into the host (Claude Code or Codex) and
installs a `SessionStart` hook that injects cairn's structural-tool
guidance into the host's context at the start of every session.

## What it does

- **MCP server registration** (`.mcp.json`). Adds `cairn` over stdio so
  the host can call `list_repos` / `get_outline` / `find_symbols` /
  `find_subtypes` / `find_supertypes` / `find_callers` /
  `find_callees` / `find_references` / `find_imports` /
  `get_symbol_source` / `register_repo` / `reindex_repo` without any
  per-project setup.
- **`SessionStart` guidance injection** (`hooks/hooks.json` →
  `SERVER_INSTRUCTIONS.md`). On `startup`, `resume`, `clear`, and
  `compact` events, the host runs `cat
  "${PLUGIN_ROOT:-$CLAUDE_PLUGIN_ROOT}/SERVER_INSTRUCTIONS.md"` and the
  hook's stdout becomes additional context for the next agent turn.
  This is where the "reach for cairn before grep" framing, the
  structural-failure catalog, and the `tier3_status` / `completeness`
  / `hints` recovery guidance live. Before v0.7.0 the same text was
  shipped as MCP `serverInstructions`; in v0.7.0 it moved to the
  plugin's `SessionStart` hook so it survives Claude Code's
  `serverInstructions` size cap and reaches Codex hosts that ignore
  the field.

## Prerequisites

- `cairn daemon` is running. (`cairn daemon` is the long-lived index
  service the MCP server talks to.)
- `cairn` is available on `PATH`.

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
variable so the same `SERVER_INSTRUCTIONS.md` is loaded under both.

## Configuration

The MCP server is discovered as `cairn mcp` on `PATH`. The
`SessionStart` hook has no runtime dependencies; both Claude Code
and Codex honor it and load the guidance once per session.

To silence the guidance injection, uninstall the plugin:

```sh
/plugin uninstall cairn@naoto256-cairn
```
