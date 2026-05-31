# Changelog

All notable changes to cairn are recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [0.1.0-alpha.1] — Unreleased

Initial line under the content-addressed architecture. The previous
0.x line on the same name is discarded; this is a fresh start with no
upgrade path. Re-register your repos.

### Added

- **Content-addressed storage** ([`cas`](crates/cairn-core/src/cas)).
  Per-repo SQLite store keyed by `(blob_sha, parser_id)`. The same
  file content parses once and is shared across every branch / tag /
  worktree that references it. `blob_sha` is git's own blob hash so
  the on-disk layout is interoperable with git's object format.
- **Manifest layer** ([`manifest.rs`](crates/cairn-core/src/manifest.rs)).
  `{(path, blob_sha)}` snapshots built from `git ls-tree` (committed)
  or by walking the worktree (tentative). Manifests are immutable
  once committed; the tentative one updates in place.
- **Anchor layer** ([`anchor.rs`](crates/cairn-core/src/anchor.rs)).
  Named pointers to manifests: `HEAD`, `branch/<n>`, `tag/<n>`,
  `tentative/<id>`. Switching branches re-binds anchors instead of
  spawning per-branch databases. Detached HEAD checkouts no longer
  accrete snapshot rows.
- **`cairn ctl register-repo`** ([`register.rs`](crates/cairn-core/src/register.rs))
  orchestrates the above end-to-end against the worktree's HEAD,
  reusing already-parsed blobs whenever the same `blob_sha` is
  already on disk.
- **CAS-backed read methods**: `find_symbols`, `find_references`,
  `find_impls`, `find_imports`, `get_outline`, `get_symbol_source`,
  `list_repos`. Each resolves the requested anchor to a manifest
  and joins per-blob facts via `manifest_entries`.
- **`--anchor`** parameter on read methods + `cairn query` CLI,
  accepting `HEAD` / `branch/<n>` / `tag/<n>` / `tentative/<id>`.
  The legacy `--branch <n>` (= sugar for `branch/<n>`) is still
  accepted.
- **`cas::registry`** alias index. Multiple aliases can label the
  same on-disk repo; `cairn ctl remove-repo` keeps the store alive
  while any label still references it.

### Removed

- The legacy per-`(worktree, branch)` snapshot DB pipeline:
  `Storage` / `Indexer` / `WatcherOrchestrator`, and the `data_db` /
  `registry_db` / `snapshot_stats` modules. The daemon-side watcher
  resume and stale-revision auto-reindex chains go with them.

### Not yet

- Live worktree-change watcher → tentative-manifest update is
  roadmapped; for now, `cairn ctl reindex-repo <alias>` is how the
  index catches up after worktree edits.
- Tier-3 (LSP-grade, `rust-analyzer`-driven) analyzers; current
  Tier-2 for Rust uses `syn` only.
- Cross-repo blob deduplication: each registered repo gets its own
  CAS store. Two repos with byte-identical files do not share blobs.
- `find_history` (= "where did this symbol exist before it got
  deleted?"). The branch anchors retain past manifests, so the
  substrate is in place; the query method is unwired.

[0.1.0-alpha.1]: https://github.com/naoto256/cairn
