# Changelog

All notable changes to cairn are recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [0.3.0] — 2026-06-10

### Breaking

- **`find_impls` removed; surface verbs are now in pairs.** The
  Rust-leaning `find_impls` MCP tool and its `cairn query impls
  --type T` / `--trait T` CLI flags are replaced by four discoverable
  tools — `find_subtypes`, `find_supertypes`, `find_callers`,
  `find_callees` — each taking a qualified `name` plus the standard
  repo / branch / anchor / limit options. Tool descriptions spell
  out the agent question ("who calls X", "what X calls", "who
  implements / extends X", "what X extends / implements / mixes in")
  so the LLM picks directly instead of composing `find_references`
  with a direction filter. `find_references` stays as the general
  multi-kind reference query.
- **CLI subcommands normalized.** `cairn query find <name>` is now
  `cairn query symbols <name>`; `query impls --type/--trait` splits
  into `query supertypes` / `query subtypes`; the new pairs above
  ship as `query callers` / `query callees`; `query imports --file
  <path>` takes the path positionally; `query outline` takes
  `<file>` positionally with `--repo` as an optional flag; `query
  source` no longer requires `--repo`. Every discovery subcommand
  now uses the same shape — first positional is the search target,
  `--repo` is an optional flag, no exceptions.
- **`OutlineArgs.repo` and `GetSymbolSourceArgs.repo` are optional.**
  Omitting `repo` aggregates outlines across registered repos or
  walks them for the first qualified-name match in `source`. The
  required-`repo` validation is gone.
- **Java impl-kind taxonomy aligned with the rest of the index.**
  The Java backend now emits `inherit` / `implement` instead of the
  Rust/Java-only `extends` / `implements`, matching the four-label
  vocabulary used by every other backend (`inherit`, `implement`,
  `mixin`, `extension`). Clients matching on the old strings need to
  update; the new values are documented in the proto types.

### Added

- **Async Tier-3 indexing jobs.** `cairn ctl reindex-repo <alias>`
  now enqueues workspace analyzer jobs and returns promptly instead
  of waiting for LSP cold starts. Track progress with
  `cairn ctl jobs`, cancel queued jobs with `--cancel`, and use
  `reindex-repo --wait` when a script needs synchronous behavior.
- **Ten new language backends — Tier-1 plus Tier-2.** TSX +
  JavaScript (extending the existing TypeScript backend), Ruby, C#,
  PHP, Kotlin, Swift, C, Java, C++, and Objective-C all ship with
  symbol/outline extraction, inheritance edges, and name-level
  call/instantiation refs. Combined with the existing Rust, Python,
  Go, TypeScript, and Markdown backends, cairn now covers the
  industry-mainstream lineup at Tier-1 + Tier-2.
- **Same-file callee resolution across every backend.** Every
  backend that emits call refs now runs a post-pass that fills in
  `target_qualified` for callees defined in the same file, so the
  default `find_references(outgoing)` and the new `find_callees`
  return a meaningful call graph without requiring agents to set
  `include_noise=true`. Cross-file callees stay unresolved by
  design and remain reachable via `include_noise`.
- **Unified impl-kind taxonomy across backends.** Class/interface/
  trait/protocol/mixin/extension relationships are reported using a
  four-label set — `inherit`, `implement`, `mixin`, `extension` —
  regardless of source language. `find_subtypes` and
  `find_supertypes` therefore return edges that compare cleanly
  across languages.

### Internal

- Versions bumped to 0.3.0 across the workspace, plugin manifests,
  and README. README CLI section, the plugin nudge hints, and the
  `find_impls` references in language analyzer doc comments are all
  updated to the new vocabulary.

## [0.2.0] — 2026-06-10

### Breaking

- **Tier-3 rust reference `source` label renamed.** Rust refs
  persisted by `rust-analyzer` now carry
  `source = tier3-rust-analyzer-lsp`, replacing the legacy
  `tier3-rust-analyzer` alias and matching the uniform
  `tier3-<analyzer>-lsp` scheme that Python (`tier3-pyright-lsp`) and
  Go (`tier3-gopls-lsp`) already used. Clients matching on the old
  string need to update; rows under the legacy label are cleared and
  re-stamped on the next reindex, so no duplicate facts are left
  behind.

### Added

- **`list_repos` snapshot `status` is now a real diagnostic.** The
  field reports `empty` (no files in the manifest), `no_analyzer`
  (only languages without a semantic backend), or `stale`
  (analyzer-capable files but zero indexed symbols — `reindex_repo`
  is the usual fix) instead of always `ready`. A registered repo
  that looks "complete but empty" can now be told apart from one
  that simply has nothing to index.
- **`get_outline` accepts `kind` and `max_depth` filters.** `kind`
  restricts items to a single symbol kind (mirroring
  `find_symbols.kind`); `max_depth` caps directory depth relative to
  `path`, so `max_depth = 1` yields a module-level summary of a
  crate or package root without burning the item cap on nested
  files. Both are optional and default to the previous behavior.

### Internal

- **Tier-3 LSP definition pass hoisted into a core substrate.** The
  per-language Tier-3 crates now share the definition-resolution
  pass instead of duplicating it per analyzer.

## [0.1.1] — 2026-06-08

### Fixed

- **CLI `cairn query refs` now mirrors the MCP reference-query
  surface.** The command accepts `--direction incoming|outgoing` and
  `--include-noise`, so shell users can ask both "who references this
  symbol?" and "what does this symbol reference?" without dropping to
  JSON-RPC. `incoming` remains the default behavior.

### Changed

- **README refreshed for the 0.1.1 release.** The docs now lead with
  Cairn's AI-agent use case, document Homebrew / Debian service
  registration, move Architecture behind the user-facing workflow,
  simplify the Languages section, and expand `cairn query refs`
  around incoming vs outgoing lookups and `--include-noise`.
- **In-tree Homebrew tap scaffolding removed.** The live formula is
  maintained in `naoto256/homebrew-cairn`; keeping a formula template
  and checksum bump script in this repository created a second source
  of truth that could drift from the tap.
- **Workspace crate versions bumped to `0.1.1`.** The binary
  `--version`, Debian package metadata, and release artifacts now
  report the patch release version.
- **Claude Code / Codex plugin manifests bumped to `0.1.1`.** The
  packaged plugin version now matches the daemon version it launches.

### Internal

- **`workspace_analyzer` split by concern.** The public trait/types stay
  in `mod.rs`; run orchestration, Tier-3 reference persistence, and
  file-URI path helpers now live in `run.rs`, `persist.rs`, and
  `path.rs`. This is a readability refactor with no intended behavior
  change.

## [0.1.0] — 2026-06-07

### Fixed

- **`rust-analyzer ContentModified` noise suppressed** (`#44`).
  The Tier-3 Rust pool used to surface `ContentModified` cancellations
  from `rust-analyzer` as analyzer failures, polluting `cairn ctl
  doctor` output during normal in-flight edits. The error is now
  reclassified as a benign retry signal and dropped from the
  user-visible failure stream.
- **Watcher dedupes tentative writes when content-identical**
  (`#45`). A `notify` event that resolves to bytes already hashed
  into the tentative snapshot no longer triggers a parse / CAS
  pass; the snapshot pointer is left alone. Fixes a feedback loop
  observed under editors that touch files (`mtime`-only updates)
  without changing content.
- **Tier-2 analyzer failure no longer poisons Tier-1 facts for the
  same blob** (`#48`). A semantic-pass panic / error now leaves the
  syntactic symbols, refs, imports, and impls already committed
  for that blob intact, so a single misbehaving Tier-2 analyzer
  degrades to "Tier-1 only" for the affected file rather than
  losing the file from the index outright.
- **Stale `branch/*` / `tag/*` anchors no longer linger in
  `cairn ctl status` after a local git ref deletion.** Surfaced by
  the closed-beta stress test against tokio (`#6`): a local branch
  deleted via `git branch -D` after its anchors were registered
  continued to appear in snapshot labels until the daemon was
  restarted with a fresh CAS. `register_repo` (which the file
  watcher calls on every reindex) now reconciles stored
  `branch/*` / `tag/*` anchors against `git for-each-ref` output
  inside the same transaction as the anchor upserts, deleting any
  whose suffix is no longer a live ref. `HEAD` and `tentative/*`
  are intentionally not pruned. As a consequence, deleted-ref
  history is no longer retained via the anchor table — see the
  `find_history` note at the bottom of this file for the
  implication on the roadmapped feature.

### Added

- **TypeScript Tier-2 analyzer** (`#47`, `#49`, `#51`). The
  `cairn-lang-typescript` crate gains a blob-scoped Tier-2 pass that
  resolves call references, type-role refs (parameters, return
  positions, fields, type aliases, generic bounds), and class /
  interface inheritance edges (`extends` / `implements`). Member-
  expression calls remain intentionally unresolved pending
  import-derived alias tracking, mirroring the Rust / Python
  receiver-type policy. Subsequent hardening (`#51`) tightens the
  reference-emission rules against pathological grammar shapes
  observed during the closed-beta corpus run.
- **Python Tier-3 analyzer via `pyright-langserver`** (`#50`,
  `#52`). New `cairn-lang-python-tier3` crate. When
  `pyright-langserver` is discoverable on `PATH`, `register_repo`
  runs it once per snapshot and emits resolved method-call refs
  under `source = pyright-lsp`; consumers see them through
  `find_references` automatically. If the binary is absent the
  analyzer logs `Skipped` and Tier-1 / Tier-2 facts are untouched.
  Import-resolution test coverage lands alongside the doctor
  per-analyzer Tier-3 surface (`#52`).
- **Go Tier-3 analyzer via `gopls`** (`#53`). New
  `cairn-lang-go-tier3` crate, same Skipped-on-missing-binary
  semantics as the Rust and Python Tier-3 paths. The same PR
  fixes a Tier-1 visibility miscall where exported-vs-unexported
  was inferred incorrectly for receiver-qualified method names.
- **`cairn ctl doctor` actionable hints + watcher / snapshot /
  Tier-3 checks** (`#46`, `#52`). `doctor` now produces actionable
  remediation strings (concrete `cairn ctl …` commands the user
  can paste), surfaces watcher install state per alias, reports
  snapshot freshness against the registered worktree, and runs a
  per-analyzer Tier-3 availability probe so a missing
  `rust-analyzer` / `pyright-langserver` / `gopls` is visible
  without spelunking through the daemon log. `#59` rewrites the
  failure-detail strings to read coherently as full sentences.
- **LSP pool generalization** (`#54`, `#55`). `cairn-core::lsp`
  factors out an `LspSpawnSpec` plus an `Availability` /
  `Readiness` strategy pair so the three Tier-3 analyzers
  (`rust-analyzer`, `pyright-langserver`, `gopls`) plug into the
  same pool without per-analyzer branching at the call site.
  Pool sizing, shutdown ordering, and error propagation all
  consolidate into one place.
- **LSP front-end handles server-initiated requests** (`#56`).
  The pool now responds to server-→client requests
  (`workspace/configuration`, registration capabilities, dynamic
  client capability negotiation) instead of treating any
  inbound request as a protocol violation, unblocking analyzers
  that refuse to serve diagnostics until their config probe is
  answered.
- **Daemon-managed live file watcher.** `cairn_watch::watch_repo`
  shipped fully implemented in earlier alphas but was never
  instantiated by the daemon, so the README / MCP
  `SERVER_INSTRUCTIONS` "always-current index" claim was aspirational
  — the index only refreshed on explicit `cairn ctl reindex-repo`.
  The new `cairn_core::watcher::WatchManager` owns one
  `WatcherHandle` per registered alias, coalesces bursts over 500ms,
  and calls the same `register_repo` code path used by `reindex_repo`
  to refresh both HEAD and the tentative snapshot. `register_repo`
  pre-validates the repo path as a directory before any CAS mutation
  and reports post-commit watcher install failures via a new
  `Ack.watcher_failed: Option<String>` (wire-additive) so callers
  see degraded-success rather than misleading errors after state has
  been committed. Replacement of an existing alias stops the prior
  watcher before installing the new one, so a failed re-install
  leaves the alias unwatched rather than stale-wrong-path-watched.
- **Default anchor resolves to `tentative/<id>` when unspecified.**
  Read methods (`find_symbols` / `find_impls` / `find_imports` /
  `find_references` / `get_outline` / `get_symbol_source`) that
  receive neither an `anchor` nor a `branch` arg now resolve to the
  registered worktree's tentative snapshot (= committed HEAD + every
  uncommitted edit the live watcher has picked up), falling back to
  `HEAD` when no tentative anchor exists yet for the store. Pair
  this with the new daemon-managed watcher and the
  "always-current" promise is finally honest at the working-tree
  level — an AI agent that just wrote a new function sees it on the
  next `find_symbols` call without needing to commit first. Explicit
  `anchor="HEAD"` or `branch="..."` callers are unaffected. New
  `cairn_core::anchor::resolve_explicit_or_default` lives next to
  the existing `resolve_wire`; the latter is kept for callers that
  genuinely want the old explicit-or-HEAD semantics.

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
  `find_impls` wording introduced in an earlier 0.1.0 pre-release).
  `repo` is removed from `input_schema.required[]`.

### Packaging

- **Release binary archives built in CI** (`#62`). Tagged releases
  produce `.tar.gz` archives for macOS (aarch64) and Linux (x86_64)
  attached directly to the GitHub Release. macOS x86_64 is
  intentionally out of the matrix — build from source for that target.
- **Homebrew tap** (`#63`). `naoto256/homebrew-cairn` ships the formula
  pointing at the CI-built archives, so `brew tap naoto256/cairn`
  followed by `brew install cairn` is the standard macOS install path.
- **Debian package metadata** (`#65`). `cargo deb`-buildable
  `.deb` is produced for the linux-x86_64 target and uploaded
  alongside the binary archives.
- **Cargo metadata threaded through every workspace crate**
  (`#61`). `description`, `license`, `repository`, `homepage`,
  `readme`, and `categories` are populated on every member crate
  so that downstream packagers (and any future crates.io publish
  attempt by a contributor) have the right manifest data.

### Renamed

- **Workspace crate `cairn-cli` renamed to `cairn`.** The binary
  is the multi-mode umbrella entry point (`cairn daemon`,
  `cairn ctl …`, MCP stdio server) — not just a CLI — and the
  crate name now reflects that. Source-from-git installs use
  `cargo install --git URL cairn` (was `cairn-cli`).

### Internal

- **`query.rs` split into per-family modules** (`#57`).
  `cairn-core::query` factors the 1.7k-LOC dispatcher into one
  module per query family (`symbols`, `refs`, `impls`,
  `imports`, `outline`, `source`) with a thin re-export layer.
  No behavioural change.
- **`wire-frontend` panic paths converted to `Error` returns**
  (`#58`). Every `unwrap` / `expect` reachable from a wire
  request is now a typed error mapped to the JSON-RPC error
  channel, so a malformed inbound frame no longer takes down
  the frontend task.
- **`lsp/mod.rs` split into per-concern modules** (`#60`).
  Pool, transport, capability negotiation, and request
  bookkeeping land in separate modules under
  `cairn-core::lsp`, matching the `query.rs` split above.
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
  is discovered via `claude plugin marketplace add` +
  `claude plugin install` on Claude Code and
  `codex plugin marketplace add` + `codex plugin add` on Codex —
  both local-path and `naoto256/cairn` remote registrations resolve
  through the same `./plugin` relative source.

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
  RPC + MCP), CLI flags, and on-disk format as SemVer 0.x surfaces
  that may still change before 1.0.
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
  deleted?"). Not yet substrate-complete: branch / tag anchors now
  track live git refs and are pruned when the underlying ref
  disappears (so stale labels don't accumulate in
  `cairn ctl status`), which means deleted-ref history is no longer
  retained via those anchors. A future implementation will need its
  own manifest-retention mechanism (e.g. an explicit history table
  or a reflog-style pin).

[0.3.0]: https://github.com/naoto256/cairn/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/naoto256/cairn/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/naoto256/cairn/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/naoto256/cairn/compare/v0.1.0-alpha.3...v0.1.0
[0.1.0-alpha.3]: https://github.com/naoto256/cairn/releases/tag/v0.1.0-alpha.3
[0.1.0-alpha.2]: https://github.com/naoto256/cairn/releases/tag/v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/naoto256/cairn/releases/tag/v0.1.0-alpha.1
