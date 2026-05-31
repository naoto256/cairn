# Changelog

All notable changes to Cairn are recorded here.

The format roughly follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [Unreleased]

(Nothing yet — see the roadmap in the README.)

## [0.1.0] — 2026-05-24

First useful cut. A single daemon indexes registered repositories with
tree-sitter and answers three MCP tools (`list_repos`, `outline`,
`find_symbol`) over a Unix domain socket; the `ctl` CLI manages the
registry through a second UDS.

### Added

- `cairn daemon` long-lived process. Two UDS listeners (`cairn.sock` +
  `control.sock`) under `$XDG_RUNTIME_DIR/cairn/` or
  `~/Library/Caches/cairn/`, parent clamped to mode 0700.
- `cairn serve` stdio↔UDS relay for MCP clients.
- `cairn ctl {add-repo, remove-repo, status, reindex, doctor, shutdown}`.
- Indexer that walks a repository (honoring `.gitignore`) and persists
  files / symbols rows into one SQLite snapshot DB per `(worktree,
  branch)`. Single worktree, single branch at this version.
- MCP server speaking JSON-RPC 2.0: `initialize`, `tools/list`, and
  `tools/call` for the three tools above. FTS5 backs fuzzy lookup.
- Language backends register through a `linkme` distributed slice;
  shipping with `cairn-lang-rust` and `cairn-lang-python` (tree-sitter
  syntactic extraction only).
- Filesystem and git-ref watcher (`cairn-watch`) wired but not yet
  consumed by an incremental indexer — slated for 0.2.0.
- Sample `LaunchAgent` plist and `systemd` user unit in `contrib/`.

### Documented

- README quickstart, architecture overview, MCP-tool table, roadmap.
- DESIGN.md memo (private) covering the storage layout, three-tier
  analyzer policy (in-process Rust crate → small in-tree adapter →
  external process), and the `(worktree, branch)` namespace plan.

[Unreleased]: https://github.com/naoto256/cairn/compare/v0.1.0...HEAD
[0.1.0]:      https://github.com/naoto256/cairn/releases/tag/v0.1.0
