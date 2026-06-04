# Changelog

All notable changes to cairn are recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [0.1.0-alpha.4] — Unreleased

### Changed (wire additive)

- **`cairn-proto::ImportsArgs.repo`** and **`FindReferencesArgs.repo`**
  become `Option<String>` so `find_imports` and `find_references`
  accept `repo=None` for workspace-wide search, matching
  `find_symbols` and `find_impls`. All four data-plane discovery
  tools are now symmetric. Existing clients that pass a `String`
  payload keep working unchanged.
- **`cairn-proto::ImportHit.location`** is added as a `String` with
  `serde(default)` so the wire shape matches `ImplHit` /
  `FindReferenceHit` / `FindSymbolHit`. The field carries the same
  `repo:branch:file:line` prefix the other three discovery hits
  already used; older clients ignore unknown fields.
- **CLI** `cairn query find / impls / imports / refs` `--repo`
  becomes optional (was required). The CLI delegate omits the
  `repo` key from the JSON-RPC params when the flag is absent.
  `find` already had a cross-repo-aware renderer (it prints
  `h.location` which carries `repo:branch:file:line`); the
  `imports` renderer is updated the same way. (`find` itself was
  missed by PR #38; this PR brings it to parity with the other
  three commands so the four CLI discovery surfaces match the
  MCP authority.)
- **MCP** tool descriptions for `find_imports` and `find_references`
  lead with "Omit `repo` to search every registered repo; each hit
  carries its repo in the `location` prefix" (mirroring the
  `find_impls` wording introduced in alpha.3). `repo` is removed
  from `input_schema.required[]`.

### Internal

- **`data_rpc::helpers::with_one_or_all_stores<T, F, S>`** extracts
  the previously-duplicated cross-repo dispatch shape (spawn_blocking
  + registry lookup / list_all + `AnchorNotFound continue-skip` +
  per-store probe + accumulated trim). `FindSymbols::dispatch` and
  `FindImpls::dispatch` migrate to it; `FindImports::dispatch` and
  `FindReferences::dispatch` use it for their newly-added cross-repo
  paths. A `finalize: FnMut(&mut Vec<T>)` callback slots between
  accumulation and the final trim so callers needing a global sort
  (`find_symbols`' language / path / line / repo / qualified ordering)
  apply it where the cap has been told the right truth. Double trim
  is intentional: per-store probe detects per-repo overflow,
  accumulated trim enforces the union cap.
- **`query::get_outline_under_path`** `LIMIT` is now parameter-bound
  (`LIMIT ?` plus `bound.push(Box::new(i64::from(limit)))`), matching
  the rest of `query.rs`. Style fix; the previous `format!`-built
  clause was non-exploitable because the dispatch clamps `limit` to
  `[1, 1000]` before reaching the query layer.

## [0.1.0-alpha.3] — 2026-06-05

Capabilities-and-correctness pass on top of alpha.2. The big lines:
the dispatcher now sniffs shebangs so extensionless `bin/foo` scripts
get indexed; `list_repos` carries a real per-language Tier-1/Tier-2
matrix; snapshots dedupe by manifest so `HEAD`+`main` collapse into a
single row; the JSON-RPC error path no longer reclassifies messages by
string-prefix; and the CAS records per-blob analyzer identity so a
revision bump (or analyzer swap, or disappearance) invalidates the
cached parse on the next pass.

### Added

- **Shebang fallback in the dispatcher** ([`register::pick_backend_with_shebang_fallback`](crates/cairn-core/src/register.rs)).
  Extensionless executable files (mode `0755+`) are sniffed by their
  first line and routed to the matching backend. PythonBackend's
  `shebang_patterns` now covers `#!/usr/bin/env -S uv run --script`
  (PEP 723 inline scripts) in addition to the existing
  `python`/`python3` shapes.
- **`Analyzer::revision()`** ([cairn-lang-api]) — monotonic semantic-
  output revision hook. Default `1`; bump per-analyzer when the same
  input would produce different semantic facts.
- **Per-blob analyzer execution recorded** (`blobs` schema v2). New
  nullable `analyzer_id TEXT` / `analyzer_revision INTEGER` columns.
  Existing rows are NULL until the next parse, which lazily backfills
  them. `reuse_or_compute` re-extracts when the recorded
  `(analyzer_id, analyzer_revision)` no longer matches the backend's
  current analyzer (drift, swap, or disappearance).
- **`SnapshotEntry::primary_label`** + **`has_head`** inherent helpers
  on both `SnapshotEntry` and `SnapshotStatus`. Consumer-side
  convenience after `branch: String` became `branches: Vec<String>`.
- **`RepoEntry::languages()`** + **`RepoStatus::languages()`** —
  derive the per-repo language union from `snapshots[].enrichment`
  (single source of truth), replacing the flat `languages` field.
- **`cairn-proto::jsonrpc` envelope helpers** — `ok_response`,
  `error_response`, `serialize_response`. Three sites (`data_rpc`,
  `ctl`, `mcp`) now import them under their previous local aliases.
- **`RequestId::Null`** in the JSON-RPC types so an error envelope
  whose request `id` is unparseable can be emitted as `id: null`.
- **Rust Tier-3 analyzer** (`cairn-lang-rust-tier3`) backed by
  `rust-analyzer` LSP. `register_repo` / `reindex_repo` runs the
  workspace analyzer once per snapshot, waits for the LSP server to
  reach progress-quiescence, walks tree-sitter-rust to find method-
  call sites, and writes resolved targets into `refs` under
  `source = "tier3-rust-analyzer"`. `find_references` picks them up
  through the existing read path. `WorkspaceAnalyzer` trait +
  `WORKSPACE_ANALYZERS` distributed slice + `workspace_analysis_runs`
  table (schema v4) provide the boundary; `cairn-core::lsp::LspClient`
  provides a minimal self-typed LSP subprocess client (initialize /
  initialized handshake, `$/progress` reader, `did_open`, `did_change`,
  `did_close`, `textDocument/definition`, graceful shutdown). Missing
  `rust-analyzer` binary surfaces as `Skipped`; Tier-1 / Tier-2 facts
  are unaffected on degrade.
- **Long-lived rust-analyzer pool** (`cairn-core::lsp::pool`) reuses
  one warmed LSP client per canonical repo root / analyzer / binary /
  config tuple across Rust Tier-3 workspace analysis runs. The pool
  owns a dedicated Tokio runtime, lazily spawns clients, synchronizes
  documents with full-text `didOpen` / `didChange`, and provides
  daemon shutdown cleanup. The design note lives at
  `crates/cairn-core/src/lsp/docs/long_lived_design.md`.
- **`cairn-proto::PartialReason`** — canonical taxonomy for
  `Completeness::Partial.reason`: `Cap` / `Tier2Warming` /
  `Tier3Warming` / `Tier3Unavailable` / `AnalyzerFailed`, plus an
  `Other(String)` backstop for forward-compatible strings from newer
  producers. Wire format is unchanged (manual `Serialize`/`Deserialize`
  emits a plain snake_case string), so existing payloads like
  `{"reason": "cap"}` continue to round-trip. The shared
  `COMPLETENESS_REASON_DESC` constant enumerates the five reasons in
  the `find_symbols` / `find_impls` / `find_imports` /
  `find_references` MCP tool descriptions so consumers know which
  remediation each reason implies (raise `limit`, wait, fall back to
  a lower tier, or check `cairn ctl status`).
- **`plugin/`** — in-tree plugin for Claude Code and Codex. Bundles
  the cairn MCP server registration (`plugin/.mcp.json`) and a
  `PreToolUse` hook (`plugin/tools/cairn-nudge.sh`) that, when the
  cwd belongs to a cairn-registered repo, lets `grep` / `rg` / `ag`
  / `ack` / `egrep` / `fgrep` calls execute as usual and emits a
  `hookSpecificOutput.additionalContext` advisory pointing at the
  closest cairn tool (`find_imports` for `^use`, `find_impls` for
  `impl X for`, `find_references` for `name(`, `find_symbols`
  otherwise). The advisory surfaces in the agent's next-turn
  context so the *next* call defaults to the index, but the current
  `grep` is not interrupted. Both Claude Code and Codex accept the
  same `hookSpecificOutput.additionalContext` shape on `PreToolUse`,
  so a single output path covers both hosts. Any dependency /
  runtime failure (`cairn` / `jq` missing, daemon down, parse error)
  is a no-op — hooks must never break a turn. `.claude-plugin/` and
  `.codex-plugin/` manifests are sibling directories under
  `plugin/` so the same bundle installs on both hosts, and the repo
  root carries `.claude-plugin/marketplace.json` (with
  `source: ./plugin` relative to the marketplace root) so the bundle
  is discovered via `/plugin marketplace add` + `/plugin install`
  on Claude Code and `codex plugin marketplace add` +
  `codex plugin add` on Codex — both local-path and
  `naoto256/cairn` remote registrations resolve through the same
  `./plugin` relative source.

### Changed (wire breaking)

- **`SnapshotEntry.enrichment`** went from a single `SourceTier` to
  `Vec<LanguageEnrichment>` (per-language `{ language, tier,
  has_analyzer }`). `tier=Syntactic && has_analyzer=true` now means
  "Tier-2 capable but no matching analyzer run is recorded for this
  snapshot's blob set" — analyzer-ran-with-zero-facts already counts
  as `Semantic`. Same change on `SnapshotStatus.enrichment` (ctl
  side).
- **`SnapshotEntry.branch: String`** → **`branches: Vec<String>`**
  (same on `SnapshotStatus`). Snapshots are now grouped by
  `manifest_id`, so anchors that resolve to the same manifest (e.g.
  `HEAD` and `branch/main`) collapse into one entry whose `branches`
  carries all the names that point at it. Sort: `HEAD` first, then
  bare branches alphabetically, then prefix-tagged anchors. `tentative/<id>`
  still has its own entry.
- **`RepoEntry.languages` / `RepoStatus.languages`** removed (use the
  new `languages()` inherent helpers). The flat field duplicated the
  per-snapshot enrichment matrix.
- **`Error::InvalidParams(String)`** / **`Error::RepoNotFound { alias
  }`** / **`Error::AnchorNotFound { name }`** added to
  `cairn_core::Error`. Internal callers that previously emitted
  `InvalidArgument("unknown repo alias: ...")` / `"anchor not
  found: ..."` / `"invalid params: ..."` now emit the typed variant,
  and JSON-RPC `error_from` pattern-matches instead of doing
  `starts_with` string reclassification. `InvalidArgument(String)`
  remains as the catch-all for method-specific validation.
- **`cairn-proto::jsonrpc::error_code::SNAPSHOT_NOT_READY`** removed
  (no producer remained after the related error-classifier arm was
  retired).

### Added (gate behaviour)

- **MCP stdio line cap** (`MCP_MAX_LINE_BYTES = 16 MiB`,
  symmetric with the UDS handler). Oversized input is drained
  through the newline, replied to with a JSON-RPC `INVALID_REQUEST`
  envelope (`id: null`), then accepting continues.

### Fixed

- **`error_from` drift** — the `data_rpc` / `ctl` classifier was
  matching the string prefix `"no repo "` while the actual emitter
  said `"unknown repo alias: ..."`. `REPO_NOT_FOUND` had been
  silently demoted to `INTERNAL_ERROR`. The new typed `RepoNotFound`
  variant fixes this by type. The stale `"has no snapshot"` arm was
  also removed (grep confirmed no producer).

### Docs

- **README "Languages" section** — documents the
  extension-first + shebang-sniffing file selection rule (PEP 723 /
  `uv run --script` included).
- **README status line** — broadened to call out wire schemas (JSON-
  RPC + MCP) and CLI flags alongside the on-disk format as
  current-state-of-the-day.
- **MCP tool `branch` descriptions** unified across `find_symbols` /
  `find_impls` / `find_references` / `find_imports` /
  `get_symbol_source`. Lifted to a single `BRANCH_PARAM_DESC` const
  so the next wording tweak edits one place. The descriptions also
  no longer claim `branch=None` searches every indexed branch —
  the actual behaviour is `branch=None` resolves to `HEAD`.
- **`LanguageEnrichment` Rustdoc** — restates that `tier` reflects
  recorded matching analyzer execution rather than emitted semantic
  facts, with freshness enforced on the parse path.

### Internal

- **`data_rpc::helpers::with_repo_conn`** consolidates the
  `index → alias → store → connection → task-panic` boilerplate that
  was duplicated across five data-RPC methods. `find_symbols`
  (`repo=None` all-repo iteration) and `list_repos` stay specialized.
- **`cairn-core::jsonrpc_errors::error_from`** — shared
  `Error -> JSON-RPC envelope` mapper. The plane-specific wrappers
  in `data_rpc` and `ctl` now delegate to it.
- **`crate::anchor::order_key`** — single sort-key function for
  anchor labels. `list_repos` and `status` share it instead of
  carrying private duplicates.

## [0.1.0-alpha.2] — 2026-06-02

Follow-up pass on the 0.1.0-alpha.1 wire surface, driven by dogfooding
feedback from a peer code-review session that used cairn live.

### Added

- **`FindReferenceHit.snippet`**: every `find_references` hit now
  carries the one-line source text at its `line`, so callers can see
  what each call site looks like without a separate
  `get_symbol_source` round-trip. The line is materialised via
  `git cat-file` (worktree fallback for tentative anchors) and a
  per-call cache deduplicates blob reads.
- **`FindSymbolArgs.signature_only`** (wire + CLI `--signature-only` +
  MCP `find_symbols` tool `input_schema`): drops the `signature`
  field per hit so broad enumerations (e.g. `kind="function"` over a
  directory) stay cheap in wire / context cost. Navigation fields
  (`id`, `qualified`, `name`, `kind`, `repo`, `branch`, `location`)
  always come through.

### Changed (wire breaking)

- **`SnapshotEntry.last_accessed`** is now an RFC 3339 UTC string
  (`"2026-06-01T18:00:00.123456789Z"`) instead of the raw nanosecond
  epoch (`"1780243103595899000"`). Inline formatter (Hinnant's
  `civil_from_days`); no new time-crate dependency.

### Fixed

- **`get_outline` doc duplication**: the syn-emitted `doc_override`
  used to `UPDATE symbols SET doc = ?` scoped by `qualified` alone,
  which fanned the struct's doc onto every sibling `impl Foo` and
  `impl Trait for Foo` row in the table (Rust admits multiple symbol
  rows for one qualified name). `DocOverride` now carries
  `target_kind` and the UPDATE filters by `(qualified, kind)`, so
  outline responses no longer repeat the same doc 3–5× per type.

### Docs

- **`FindSymbolArgs.path`** docstring (proto + MCP tool description)
  now spells out the byte-level string-prefix semantics — `path =
  "crates/foo/"` for a directory scope, `path = "crates/foo"` also
  matches sibling `crates/foo_bar/...`. Behavior unchanged.

### Internal

- `register::load_blob_or_worktree(root, blob_sha, path)` extracted
  from the inlined `git cat-file` + worktree fallback that both
  `find_references` (snippet load) and `get_symbol_source` already
  needed. Rule-of-Three prep work — keeps the lookup canonical when a
  third wire method needs source-line context.

## [0.1.0-alpha.1] — 2026-05-31

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
- Cross-repo blob deduplication: each registered repo gets its own
  CAS store. Two repos with byte-identical files do not share blobs.
- `find_history` (= "where did this symbol exist before it got
  deleted?"). The branch anchors retain past manifests, so the
  substrate is in place; the query method is unwired.

[0.1.0-alpha.4]: https://github.com/naoto256/cairn/compare/v0.1.0-alpha.3...HEAD
[0.1.0-alpha.3]: https://github.com/naoto256/cairn/releases/tag/v0.1.0-alpha.3
[0.1.0-alpha.2]: https://github.com/naoto256/cairn/releases/tag/v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/naoto256/cairn/releases/tag/v0.1.0-alpha.1
