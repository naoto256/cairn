# Changelog

All notable changes to cairn are recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [0.7.0] — 2026-06-27

### Added

- **`cairn ctl doctor` cross-references drift with the rerun
  lifecycle (`analyzer rerun health` check).** Drift detection
  (`analyzer revision drift` and `parser revision drift`) tells the
  operator *that* the alias is stale; the rerun-health check now
  tells them *what happened post-enqueue*. Each (alias, drift)
  combination is classified against the latest
  `workspace_analysis_runs` row for the alias's current tentative
  manifest:

  - **Fail** when drift is reported but the analyzer run is
    `succeeded` **at the current revision** (the observability
    safety net for the Tier-2.5 materialization fix below — the
    analyzer chain says "all good" yet the parser layer says
    "still stale", meaning the full-reindex chain wrote analyzer
    rows without bumping `blobs.parser_revision`). Remediation
    framing is conservative: "run `cairn ctl repo reindex <alias>`
    to recover (legacy state from before v0.7.0 is possible); if
    it recurs after a fresh reindex, please file an issue."
  - **Warn** when the rerun ran and terminated with
    `failed`/`timed_out`/`cancelled`, with the row's error message
    surfaced inline and a `cairn ctl jobs list <alias>` hint.
  - **Pass** (informational) when the rerun is `queued`/`running` —
    no operator action needed, the rerun will land on its own.
  - **Warn** when no run row exists at all, or when the latest row
    is at an older revision than the current build expects (the
    "scanner enqueue lost/dropped or never ran" case). The
    remediation points operators at the daemon log with a concrete
    grep recipe (`alias name + staleness`) and a `cairn ctl repo
    reindex <alias>` manual-recovery fallback.

  Parser-drift evaluation walks every expected workspace analyzer
  for the manifest and surfaces the worst case, so a mixed picture
  — one analyzer succeeded, another failed — surfaces the failure
  rather than misclassifying as the Case A Fail on the succeeded
  slice. Aliases with no drift produce zero rerun-health checks;
  doctor noise stays minimal.

- **Automatic revision-staleness rerun at daemon startup.** When an
  analyzer's compiled-in `revision()` is higher than the value
  persisted in `workspace_analysis_runs.analyzer_revision`, the daemon
  enqueues a targeted rerun for that specific `(manifest_id,
  analyzer_id)` pair after the file watcher is up. No more manual
  `cairn ctl repo reindex <alias>` after every PR that bumps an
  analyzer revision. The scan runs in `tokio::task::spawn_blocking`
  so the daemon's start path is not blocked. On the first upgrade,
  expect an `auto-reindexing N stale aliases (revision bump detected)`
  info log — this is a documented behavior change, not silent UX
  drift. Failure modes (per-alias error, DB unavailable, JobManager
  full) log a `warn`/`error` and the daemon keeps running; doctor
  picks up the shadow case (see below).
- **`cairn ctl doctor` surfaces analyzer-revision drift.** A new
  per-alias `analyzer revision drift` check Warns whenever the
  persisted `analyzer_revision` is lower than the linked-in build's
  `revision()`, listing the analyzer ids and the version delta. Acts
  as a shadow-case fallback if the startup auto-rerun failed to
  enqueue (DB error, pool full); the operator sees the drift either
  way and can run `cairn ctl repo reindex <alias>` to resolve.
- **`cairn ctl doctor` detects wedged analyzer runs.** A `queued` or
  `running` `workspace_analysis_runs` row whose `started_at_ns` is
  older than `STUCK_RUN_THRESHOLD` (6h) now surfaces as a distinct
  "stuck in `running` for ~Xh" Warn with a `reindex_repo`
  remediation, instead of being misclassified as routine "indexing in
  progress". Catches worker-pool deadlocks and crash-mid-run rows
  that `restore_from_db` flipped back to `queued`/`running` and that
  would otherwise sit silently.
- **Coupling-invariant documentation for `parser_revision` and
  `analyzer_revision`.** New module-level doc on
  `crates/cairn-core/src/workspace_analyzer/mod.rs` records that the
  two revision signals are independent (Tier-1 lives on `blobs`,
  Tier-2.5/Tier-3 lives on `workspace_analysis_runs`), how staleness
  is detected for each, why `config_hash` is *not* a stale criterion
  in the auto-rerun path, and why the rollback case (`<` comparison)
  intentionally produces no spurious rerun.

- **CLI pretty output for `target_path` (Phase 4).** `cairn query
  imports / subtypes / supertypes / refs / callers / callees` now
  append `\ttarget=<path>` to each non-JSON row when the Tier-2.5
  resolver pinned the edge to a workspace file. JSON / MCP output
  already carried the field since Phase 1 / 2; this closes the
  drift between wire and human-readable surfaces.
- **JavaScript Tier-2.5 backend (Phase 3 integration of the
  Wave 2B canary).** In-process tree-sitter resolver for JavaScript:
  binding-form module imports (`const X = require(...)`,
  `import X from '...'`, named / default / namespace / destructured
  shapes), ES-class hierarchy, and static call dispatch within the
  workspace. Adds `cairn-lang-javascript-tier25` (analyzer id
  `javascript-resolver`), binding-form CommonJS `require()` import
  emission on the Tier-1 `cairn-lang-typescript` backend across all
  three dialects, and the same Phase 1 contract on import edges:
  `target_qualified = None`, `target_path` is the source of truth,
  persist.rs sanitises the path against the manifest, and the
  cross-parser-id symbol fallback adopts a sibling-parser symbol
  whenever a JS analyzer emits a unique qualified target. JS
  analyzer revision starts at 2 so the same
  `cairn ctl repo reindex <alias>` migration story applies to
  JavaScript repos that were registered before this release.

  Phase 3 originally shipped binding-form only; later in this
  release the JavaScript / TypeScript Tier-1 emitter (see the
  `require('./x')` entry above) was extended to statement-position,
  expression-position, and `module.exports = require('./x')`
  re-export shapes. The remaining limitations are the named
  re-export pattern `exports.X = require('./x')` and downstream
  re-export graph semantics (resolving an `import { X } from
  './outer'` through a re-export chain).
- **Tier-2.5 resolution `target_path` surface on refs / calls
  (Phase 2).** `find_references`, `find_callers` and `find_callees`
  now return `target_path: Option<String>` on `FindReferenceHit` /
  `CallHit`, sourced directly from `resolutions.target_path` via the
  same projection added to `find_imports` / `find_subtypes` /
  `find_supertypes` in Phase 1. Surface-additive: no SQL semantics
  change for the existing resolved-noise filter, dedup ordering, or
  `kind_source` precedence — the new column is appended to the
  `best_resolution` CTE and the inner SELECT, and the resolved /
  noise gates continue to read `sym.qualified` via `COALESCE`. No
  new schema migration in Phase 2; the column reads come from
  Phase 1's v10.
- **Tier-2.5 resolution `target_path` persistence (Phase 1).** New
  schema migration v10 adds `resolutions.target_path TEXT` plus a
  partial index. Workspace analyzers now persist the file path of the
  resolved target (import edges, cross-parser type/call edges) as a
  source of truth that is independent of `target_symbol_id`, so the
  resolver can pin "which workspace file" even when no symbol-level
  identity is recoverable. `ImportHit.target_path` (`find_imports`)
  and `ImplHit.target_path` (`find_subtypes` / `find_supertypes`)
  surface the new column on the wire as additive `Option<String>`
  fields. The persist layer additionally gains a cross-parser-id
  symbol-lookup fallback (unique-hit only, with manifest-wide
  qualified-only branch gated to analyzer-emitted
  `target_path = None`) so cross-language hierarchies (Kotlin
  extending Java, Swift importing Objective-C, etc.) now pin
  `target_symbol_id` whenever a single sibling-parser symbol
  matches.
- **Ruby require-graph contract fix.** `RequireEdge.target_qualified`
  for `require_relative` edges is now `None` (was a path string,
  which never matched `symbols.qualified` and silently dropped the
  lookup). `target_path` remains the source of truth for these
  edges.

### Changed

- **Plugin guidance moved from MCP `serverInstructions` /
  PreToolUse Bash nudge to a SessionStart hook.** The Cairn plugin
  now ships `plugin/SERVER_INSTRUCTIONS.md` and injects it via a
  `SessionStart` hook (matchers: `startup|resume|clear|compact`)
  that simply `cat`s the file. The previous PreToolUse
  `cairn-nudge.sh` (fired on every `Bash` tool use, required `jq`,
  produced one-line "consider find_symbols" nudges) is retired and
  the script removed. The MCP server's `serverInstructions` field
  is now always `None`; hosts that load the plugin receive the same
  guidance from the agent-host-side `SessionStart` hook instead, so
  guidance no longer depends on the MCP handshake being read by the
  model. Hosts that connect to `cairn mcp` *without* installing the
  plugin get no inline guidance; install the plugin (or paste
  `plugin/SERVER_INSTRUCTIONS.md` into the host's system prompt) to
  restore the structural-tool reflex. `CAIRN_NUDGE_DISABLED`
  and the `jq` prerequisite no longer apply.
- All seven Tier-2.5 analyzers had their revisions bumped during
  `release/0.7.0` development. The final shipped revisions are
  Ruby = 4, PHP / Python / Swift / C# = 3, Kotlin = 5, JavaScript
  = 4. JavaScript ships in the same release as part of the
  Tier-2.5 JavaScript backend integration, with the same
  `target_path` contract as the other Tier-2.5 backends. On
  upgrade the daemon's CAS treats existing resolutions rows as
  stale: cached Tier-2.5 facts for the bumped analyzers are
  dropped on the next analyzer run and rewritten with the new
  `target_path` column populated.

  **Normal upgrade path is now automatic**: the daemon's
  startup analyzer-revision and parser-revision drift scanners
  (see entries above) detect the bumps after the watcher comes
  up and enqueue the appropriate rerun or full repo reindex.
  `cairn ctl repo reindex <alias>` remains the canonical
  *recovery* path when auto-reindex cannot run — missing
  worktree root, failing LSP, pool / DB error, or legacy
  corrupted state from a previous bug. Newly-registered
  repositories get the current shape automatically on first
  index.
- `find_references` / `find_callers` / `find_callees` results for
  cross-parser-id codebases (Kotlin → Java, Swift → Objective-C,
  etc.) may surface previously-missed resolved hits as the
  cross-parser symbol fallback newly fills in `target_symbol_id`
  for sites the same-parser lookup used to miss. This is false-
  negative recovery, not a semantic change.

### Fixed

- **Tier-2.5 analyzer-only reruns no longer silently destroy prior
  resolutions when the worktree is transiently inaccessible.** The
  daemon-startup analyzer-revision-drift scanner enqueues a single
  analyzer rerun via `JobManager::enqueue_analyzer_run` (not a full
  reindex). The seven Tier-2.5 analyzers
  (`tier25-kotlin-resolver`, `tier25-swift-resolver`,
  `tier25-csharp-resolver`, `tier25-python-resolver`,
  `tier25-javascript-resolver`, `tier25-ruby-resolver`,
  `tier25-php-resolver`) each read their input by calling
  `std::fs::read(worktree_path)` directly. If the worktree was
  transiently inaccessible at that moment (a git checkout in
  flight, a file move, a permissions blip), every read returned
  `None`, the analyzer returned an empty `WorkspaceFacts`, and
  `persist_resolutions` committed the `DELETE` half of its
  delete-then-insert with zero `INSERT`s — deleting every prior
  `tier25-*` row under a `Succeeded` run status. `doctor` showed
  the run as current+succeeded and the operator had no signal that
  the workspace facts were gone.

  Fix: the runner is now the single source of truth for analyzer
  input I/O. `WorkspaceAnalyzer` gained
  `requires_materialized_files() -> bool` (default `false`; LSP
  analyzers stay on their own server-driven file I/O); the seven
  Tier-2.5 analyzers override it to `true`. When `true`, the runner
  reads every selected file into `WorkspaceFile::source_bytes:
  Option<Arc<[u8]>>` before invoking the analyzer. Any missing
  `worktree_path` or `std::fs::read` failure on any selected file
  becomes a single run-level `Failed` (not `Succeeded`, not
  `Skipped`) with an operator-facing error message
  (`"N of M workspace files unreadable: <path>: <os error>, … (showing first 3 of N)"`),
  and `persist_resolutions` is not called — prior rows stay intact.
  Partial unreadable is also `Failed`: an incomplete cross-file
  graph silently produces wrong fallbacks instead of correct partial
  truth. The Tier-2.5 analyzers now read from
  `file.source_bytes.as_deref()` exclusively; the race window
  between the runner's readiness check and the analyzer's
  `std::fs::read` is gone by construction.

  Tests pin: all unreadable / partial unreadable / read error all
  produce `Failed` with prior rows preserved; readable-but-
  legitimately-empty facts still produce `Succeeded` with prior
  rows deleted (preserving the analyzer-evolution case where a
  revision bump genuinely zeroes out a fact class); the LSP /
  default analyzers are NOT pre-read (so a 50k-blob monorepo does
  not pay a double-read tax); and the consumer-side migration is
  pinned by a Kotlin smoke test that fails if the analyzer regresses
  to reading from disk.

  Recovery for existing users: this fix prevents new workspace-
  analyzer runs from marking unreadable input as succeeded and
  deleting prior Tier-2.5 facts. It does NOT repair stores that
  already contain a current-revision succeeded run with empty
  `tier25-*` resolutions from the previous bug. Symptoms to look
  for after upgrading:

  - `cairn ctl daemon doctor` previously surfaced
    `analyzer revision drift` for an alias.
  - A `find_subtypes` / `find_callers` / `find_references` query
    that was returning `tier25-<lang>-resolver` with a `target_path`
    now returns `tier2-fact` fallback with `target_path = null`.
  - Particularly affected: Kotlin / Swift / C# / JavaScript /
    Python / Ruby / PHP Tier-2.5 in repos that registered before
    the upgrade.

  If any of those match, run `cairn ctl repo reindex <alias>` once
  per affected alias to rebuild the lost facts. New registrations
  and new revisions are protected structurally.

  Out-of-scope nit (deferred to a future perf epic): the runner
  materializes every selected Tier-2.5 file at once into an
  `Arc<[u8]>`, so a single-pass over a multi-GB monorepo allocates
  peak memory proportional to the parsed input. Acceptable for the
  v0.7.0 release-blocker fix scope; chunked / streaming
  materialization is the follow-up shape.

- **Manifest dedup shortcut no longer skips the workspace analyzer
  pass when blobs were re-parsed or analyzer revision drifted (F-A
  regression).** v11 introduced `resolutions.manifest_id ON DELETE
  CASCADE` to fix cross-manifest leakage. Combined with the existing
  `register_repo_inner` shortcut that reused the prior `manifest_id`
  when tentative entries matched, this created a fact-set version
  skew: blob re-parse fired (because the Tier-1 `parser_revision`
  bumped on a freshly upgraded binary, or because a new analyzer
  revision invalidated CAS rows), but the workspace analyzer pass was
  skipped, leaving Tier-2.5 resolutions rows stale (orphaned facts
  under the reused `manifest_id`).

  Dogfood symptom: after `touch Moshi.kt` (or a `cairn` binary
  upgrade with a `parser_revision` bump), Tier-2.5 subtypes /
  callers fell back to `tier2-fact`, status displayed `Ready,
  stale=0` misleadingly.

  Fix: separate the contract. `register_repo_inner` now treats
  *manifest reuse* (cheap: entries unchanged) and *analyzer skip*
  (entries unchanged AND `blobs_parsed == 0` AND every expected
  analyzer has a `succeeded` `workspace_analysis_runs` row at the
  current `analyzer_revision`) as two independent gates. The
  analyzer-skip decision is moved to AFTER `parse_pending_blobs`
  runs, so the blob-CAS invalidation result is the truth-source the
  gate watches — no duplicate `parser_revision` drift helper. `queued`
  / `running` / `failed` / `skipped` / `cancelled` / `timed_out` rows
  all force a re-run; only an outright `succeeded` row at the current
  revision counts as "facts current". The `RegisterOutcome` field
  formerly named `analyzers_skipped_due_to_dedup` is renamed to
  `skip_analyzers_for_unchanged_manifest` to reflect that the skip is
  not unconditional on entries-unchanged. A register-level
  integration test pins the F-A state transition (`blobs_parsed > 0`
  forces a re-run while the `manifest_id` stays reused), and unit
  tests on the new `check_workspace_analyzer_current_succeeded`
  helper pin the per-status gate behavior.

- **`find_references` strict-FQN incoming query plan recovered from
  `SCAN refs` to index-driven lookup.** Phase 4 introduced a
  `WHERE COALESCE(r.target_qualified, sym.qualified) = ?` predicate
  so cross-parser-resolved hits (where the surface
  `target_qualified` comes from `symbols.qualified` via
  `resolutions.target_symbol_id`) would also match a strict FQN
  query. The COALESCE referenced a LEFT-JOINed column, so SQLite
  could not push the filter through `idx_refs_target_qualified` and
  fell back to a `SCAN refs USING idx_refs_blob` — about **135×
  slower** than the index-driven path on a 1K-ref fixture (extrapolating to
  a noticeable interactive lag on large monorepos). The strict path
  now runs a `strict_refs AS (Branch A UNION ALL Branch B)` CTE:
  Branch A is the index-friendly `r.target_qualified = ?` lookup
  through `idx_refs_target_qualified`; Branch B is the
  resolution-row-backed fallback that probes `idx_symbols_qualified`
  first and rides the resolution uniqueness back to the ref. The
  two branches are mutually exclusive by construction
  (`r.target_qualified IS NULL` is a single-table predicate on
  `refs`), so `UNION ALL` is the right combinator — no cross-branch
  dedup needed. Empty-string `target_qualified` rows stay in
  Branch A, preserving the `COALESCE('', sym.qualified) = ''`
  semantics. The rewrite is intentionally a pure perf refactor:
  same row set, same downstream `dedup_rank` / noise-filter /
  projection pipeline, no analyzer or parser revision bump. The
  EXPLAIN QUERY PLAN is pinned by
  `pr_gamma_strict_incoming_explain_uses_branch_indices`; future
  changes that drop either branch's index use will fail at CI.

- **JavaScript/TypeScript Tier-1 now emits `require('./x')` as
  `ImportFact` in statement-position, expression-position, and
  re-export shapes.** Previously only the binding form
  `const X = require('./foo')` reached `extract_cjs_requires`; bare
  statement calls (`require('./setup');`), argument-nested calls
  (`app.use(require('./routes'))`), and `module.exports =
  require('./x')` re-exports were silently dropped on the floor, so
  `find_references` / `find_callers` / `find_callees` against the
  imported module fell through to tier2-fact fallback with no
  `target_path`. The Tier-1 emitter now visits every `call_expression`
  whose callee is the bare identifier `require` with a single static
  string-literal argument, with a shared `seen_require_sites` HashSet
  keyed on the call-site byte range so the binding-form path and the
  generic visitor cannot double-emit the same call. The Tier-2.5
  JavaScript backend (`cairn-lang-javascript-tier25`) consumes the
  same shapes as `ImportBinding`/`RequireEdge`, with statement /
  expression / re-export all mapped to `ImportKind::SideEffect`
  (edge-only — no `ResolvedBinding`, no `target_qualified`), so
  `require_graph` produces a real module edge with `target_path`
  populated for in-workspace targets and `resolutions.target_path` is
  finally written for these sites. The recognizer shape is kept
  bit-for-bit identical between Tier-1 and Tier-2.5 (`require.resolve`,
  dynamic specifiers, and template literals are rejected structurally
  on both sides). TypeScript `parser_revision` bumps 2 → 3 to
  invalidate the CAS-cached syntactic snapshot; JS Tier-2.5
  `ANALYZER_REVISION` bumps 3 → 4 to drive a `const_resolver` /
  `require_graph` rerun — the 2nd real use case of PR #220's
  analyzer-revision staleness scanner after Wave 2C's PR-α CJS
  binding-form expansion. **Known limitations**: `exports.X =
  require('./x')` named re-export pattern is out of scope (only the
  strict `module.exports = require(...)` shape is recognized);
  re-export graph semantics (resolving a downstream
  `import { X } from './outer'` through a re-export chain) are not
  modeled — the Tier-2.5 contract for `module.exports =
  require('./x')` is edge-only.
- **Kotlin Tier-1 emits enum entries and constructors.** `enum class
  Color { RED, GREEN }` now surfaces `Color.RED` / `Color.GREEN` as
  `SymbolKind::Constant` (mirroring the Java backend's
  `enum_constant` shape), and `class Foo(val x: Int)` /
  `class Foo { constructor(x: Int) { ... } }` surface as
  `SymbolKind::Constructor` at qualified `pkg.Foo.Foo` (parent class
  FQN + class short name, same shape Java already emits for
  `Foo::new`). Pre-fix Java↔Kotlin cross-parser lookups for
  `MyEnum.VALUE` and `new Foo(...)` missed and fell through to
  tier2-fact because the row simply didn't exist on the Kotlin side.
  Constructors are not pushed as nesting parents — `val`/`var`
  primary-constructor parameters continue to attach as properties of
  the enclosing class, not of the synthetic ctor symbol. Kotlin
  `parser_revision` 3 → 4 — the daemon's parser-revision drift
  scanner (see the dedicated entry above) detects the bump at
  startup and enqueues a full repo reindex automatically when the
  alias's worktree is readable; manual
  `cairn ctl repo reindex <alias>` remains the recovery path when
  the auto-reindex cannot run.
- **Kotlin Tier-2.5 absorbs the JVM `<File>Kt` synthetic class.**
  Java callers cross-calling Kotlin top-level functions write
  `FooKt.bar()` (or `com.x.FooKt.bar()`); the JVM compiler
  synthesizes `<File>Kt` to host top-level callables. A new
  Stage 7.5 in `resolve_dotted_call` strips a trailing `*Kt`
  segment and routes the call to
  `methods.get_package_callable(pkg, method)` so the edge resolves
  to e.g. `com.x.bar`. Discipline:
  literal workspace `class FooKt` / `object FooKt` always wins
  (verified via `lookup_unique`); the normalization never routes
  into `get_unique_by_name` (R2 contract from PR #219 — would
  re-introduce the same-short-name collision class that PR closed);
  the alias-bound terminal contract is preserved end-to-end
  (`import pkg.b.FooKt; FooKt.bar()` does not fall through to a
  same-package `bar` when `pkg.b` has no `bar`). Kotlin Tier-2.5
  `analyzer_revision` 4 → 5; the daemon's analyzer-revision drift
  scanner enqueues a targeted rerun automatically on startup.
- Known limitation: `@file:JvmName("Custom")` is not handled. The
  annotation rewrites the synthetic class name from `<File>Kt` to
  `Custom`, but tree-sitter-kotlin-ng does not surface that
  annotation reliably from Tier-1 syntax alone. Affected Java
  callers' `Custom.bar()` calls stay unresolved at Tier-2.5; a
  Tier-3 LSP cross-check is the intended fix.
- **Kotlin Tier-2.5 dispatch chain is path-aware.** `PackageIndex` and
  `RequireGraph` bindings now key by `(path, qualified)` instead of
  qualified-only. Cross-file same-name class collisions no longer
  collapse to a HashMap first-hit, and `dispatch::resolve_call` runs
  every candidate qualified through `lookup_in_file` (when path-
  bound) or `lookup_unique` (when path-agnostic) before adoption —
  the pre-fix `resolve_dotted_to_qualified` would return
  `package + parts` unchecked, killing later fallbacks and dropping
  call edges to tier2-fact. Continues v11
  (`resolutions.manifest_id`) toward cross-manifest dispatch
  consistency. `CallReceiver::This` / `Super` / `Bare` now all walk
  lexical-class MRO as first-class cases (bare calls inside a class
  body resolve through inherited methods before falling out to the
  alias / same-package / wildcard cascade). Kotlin analyzer
  revision 3→4; running `cairn ctl repo reindex <alias>` invokes
  the `workspace_analysis_runs` staleness mechanism to queue
  analyzer reruns (no automatic global reindex). JS Tier-2.5
  verified unchanged (analyzer revision and tests untouched).

- **Cross-manifest divergence of Tier-2.5 / Tier-3 resolutions
  (schema v11).** `resolutions` rows now carry an explicit
  `manifest_id` column. Workspace-aware rows (Tier-2.5 / Tier-3,
  written by `persist_resolutions`) tag the writing manifest;
  Tier-2 direct rows (syntactic-only, written by
  `cas/blob.rs::insert_direct_resolution`) keep `manifest_id NULL`
  to remain valid across every manifest that contains the blob. The
  three query paths (`find_imports` / `find_impls` /
  `find_references`) filter to `(manifest_id = ?1 OR manifest_id IS
  NULL)`, with a `CASE WHEN manifest_id = ?1 THEN 0 ELSE 1 END`
  tie-break in `ROW_NUMBER` so a manifest-specific row always
  outranks a blob-scoped one when both cover the same site. Closes
  the root cause behind the Phase 4 *cross-manifest leakage*
  Known Limitation: queries against manifest B no longer pick up
  `target_path` / `target_symbol_id` values that the resolver
  wrote while indexing manifest A. Also removes the v10
  *cascading-reindex* symptom where reindexing manifest A's shared
  blob would nuke manifest B's resolution rows; v11's
  `persist_resolutions` DELETE keys on `(source, manifest_id)`
  with an extra clause for straggler legacy NULL rows of the same
  source. Composite partial indexes
  (`idx_resolutions_manifest_site`,
  `idx_resolutions_blob_scoped_site`) cover both branches of the
  `OR` so query plans avoid a full table scan; verified via
  `EXPLAIN QUERY PLAN` in tests.

  This release is **correctness-first, not permissive-compat**: the
  v11 migration deletes every workspace-aware legacy NULL row
  (`tier3-*` / `tier25-*` sources where `manifest_id IS NULL`) at
  the moment it lands. Existing repositories will see fewer
  cross-file resolved hits and `target_path` values until the user
  runs `cairn ctl repo reindex <alias>`. The wrong-manifest
  leakage stops at the migration point regardless. Newly registered
  repositories are unaffected. All seven Tier-2.5 analyzers
  (Ruby 3→4; PHP / Python / Kotlin / Swift / C# / JavaScript 2→3)
  had their revisions bumped so, after
  `cairn ctl repo reindex <alias>` is invoked, the standard
  `workspace_analysis_runs` staleness mechanism queues the
  analyzer reruns.
- **Cross-parser-id qualified-name lookup for `find_references` /
  `find_callers` (Phase 4).** Strict qualified lookup now matches
  against `COALESCE(r.target_qualified, sym.qualified)` rather than
  `r.target_qualified` alone. Phase 1 cross-parser resolutions
  populate `target_symbol_id` and the surface `target_qualified`
  flows from `sym.qualified` via the inner-SELECT COALESCE; before
  Phase 4 the WHERE clause only checked the raw `refs.target_qualified`,
  so a strict FQN query (`find_callers com.x.JsonAdapter.fromJson`)
  returned zero hits for every cross-parser resolved call. The
  qualified-name detector also gains `.` and `\` separators so
  Python / Kotlin / Swift / C# / Java FQNs and PHP `App\Foo\Bar`
  namespaces enter the strict path instead of falling through to a
  bare-name fallback that picks up unrelated same-name symbols.
- **Manifest-wide qualified-only symbol fallback gated on
  `kind != Import` (Phase 4).** `resolve_resolution_target`'s
  third-stage rescue (which adopts a manifest-wide unique symbol
  when the analyzer emits `target_qualified = Some(...)` without a
  path) now refuses to run for import edges. Some backends (Kotlin,
  Swift, C#, sometimes PHP / Python) populate
  `RequireEdge.target_qualified` with the import's bare FQN for
  external / unresolved imports; without the gate the rescue could
  adopt a coincidentally-matching workspace symbol and back-derive
  a `target_path` via `path_for_symbol_id`, silently re-pointing an
  import edge at a specific symbol's file. Type / call edges still
  use the rescue and the cross-parser hierarchies they enable.
- **`find_references` resolutions CTE filters by relevant kinds
  (Phase 4).** Defensive `WHERE kind IN ('type', 'call', 'import')`
  added to the `best_resolution` CTE, mirroring the explicit
  `kind = 'import'` / `'type'` filters in `find_imports` /
  `find_impls`. Today the JOIN-time `res.kind = r.kind` predicate
  already discriminates, so this is a future-proof against new
  `resolutions.kind` values rather than a behaviour change.

### Migration notes

- **Schema v10** is applied automatically when the new binary opens
  an existing index (the migration is an additive `ALTER TABLE` +
  partial index). No data loss; legacy resolutions rows keep
  `target_path = NULL` until the analyzer that wrote them re-runs.
- **Schema v11** is applied automatically on first open and is a
  **correctness-first destructive migration**: every workspace-aware
  legacy row whose `manifest_id IS NULL` (`tier3-*` / `tier25-*`
  sources) is deleted so the wrong-manifest leakage stops at the
  migration point. Existing repositories will see fewer cross-file
  resolved hits and `target_path` values until the daemon's startup
  scanners (analyzer-revision + parser-revision drift) repopulate
  the workspace-aware rows. The repopulation is automatic on a
  readable worktree; `cairn ctl repo reindex <alias>` is the
  recovery fallback when the auto-reindex cannot run. Newly
  registered repositories are unaffected. Tier-2 direct rows
  (syntactic-only) keep `manifest_id NULL` by design and stay valid
  across every manifest that contains the blob.
- **Daemon startup now performs revision-drift detection
  automatically.** When the daemon opens an existing index built
  with a previous binary, the analyzer-revision and parser-revision
  drift scanners enqueue the appropriate reruns once the file
  watcher is up. In the common case there is no manual step on
  upgrade; `cairn ctl repo reindex <alias>` is reserved for
  recovery (the `cairn ctl doctor` `analyzer rerun health` check
  routes operators to this command when auto-reindex did not
  complete).

### Known limitations

- **Kotlin Tier-2.5 dispatch is static-receiver scope only.**
  `adapter.fromJson(json)` where `adapter` is a local variable bound
  to e.g. `JsonAdapter<T>` — or any polymorphic adapter chain (Moshi,
  Retrofit-shaped builders, runtime-selected backends) — is out of
  scope for the current `dispatch::resolve_call` path. Resolving
  these receivers requires type-flow propagation from the construction
  site through every reassignment, which is Tier-3 LSP territory and
  is queued as a future epic. PR #219 closed the `dispatch::resolve_call`
  `None`-return path that surfaced as a regression on static-receiver
  sites; the polymorphic-receiver case is a separate analysis layer
  queued for a future Tier-3 epic.

### Follow-up

- **Structured scanner-outcome persistence.** The startup
  analyzer-revision and parser-revision drift scanners now run on
  daemon boot and the doctor `analyzer rerun health` check
  cross-references each drift against the persisted
  `workspace_analysis_runs` state, so the operator-facing surface
  is in place. What is **not yet persisted** is a per-scan-pass
  history of "drift detected → enqueue attempted → enqueue outcome"
  (success / coalesced / unknown analyzer / scan-time error). A
  scheduled follow-up adds a `staleness_scan_passes` table (or a
  `latest_staleness_scan_alias_outcomes` snapshot) with a bounded
  retention policy so operators can audit a single scan pass after
  the fact instead of grepping the daemon log.
- **Chunked / streaming materialization for very large monorepos.**
  The Tier-2.5 runner materializes every selected file at once into
  an `Arc<[u8]>`, so a single-pass over a multi-GB monorepo
  allocates peak memory proportional to the parsed input. Chunked /
  streaming materialization is the future shape.

## [0.6.2] — 2026-06-24

Patch release laying the structural groundwork for Tier-2.5 (a
fact / resolution layer split) without changing existing wire
behaviour, plus two Objective-C fixes uncovered in dogfood that
were blocking the release.

The headline shape is a four-phase walk: a new `resolutions` table
in the index (Phase 1, empty), `syntactic_kind` emitted on every
`ImplFact` from all 14 backends (Phase 2), Tier-2 direct
resolution rows written for 8 languages (Phase 3), and queries
finally reading from the resolutions table so `find_subtypes` /
`find_supertypes` return `semantic_kind` ranked by source
precedence (Phase 4). Existing fields are unchanged; new fields
are additive with `#[serde(default)]` for old-client
compatibility.

### Added

- **Resolution table schema.** New `resolutions` table in the
  on-disk index reserves the storage shape for Tier-2.5; rows are
  not yet written by the production path. Phase 1 of the Tier-2.5
  rollout, kept as an empty-operation migration so the schema can
  ship ahead of the writers (#193).
- **`syntactic_kind` field on `ImplFact`.** Every Tier-2 fact now
  carries the grammar-direct token that produced it (`class`,
  `interface`, `trait`, `struct`, `protocol`, `enum`, …),
  emitted from all 14 language backends. This is the substrate
  the Phase 3/4 resolutions read; consumers that only need the
  existing `kind` field are unaffected (#194).
- **Tier-2 direct resolution rows for 8 languages.** Java, Ruby,
  PHP, Objective-C, TypeScript, JavaScript, C++, and Swift now
  emit `tier2-direct-<lang>` rows into the `resolutions` table
  alongside their facts. These cover the cases where a single
  file's grammar is enough to decide the semantic relation
  (`class Foo extends Bar`, `class Baz implements Qux`,
  `protocol P : Q`, …) without waiting on a Tier-3 resolver pass
  (#195).
- **`find_subtypes` / `find_supertypes` read from resolutions.**
  Phase 4 wires the query path through the new table: results now
  carry `semantic_kind` derived from resolutions, and the wire
  ships an additive `kind_source` field whose value distinguishes
  `tier2-fact` (existing behaviour), `tier2-direct-<lang>` (new
  Phase 3 rows), `tier25-*` (reserved), and `tier3-*` (resolver
  output). The legacy `kind` field is unchanged (#196).
- **`tier3_status.analyzers[].tier` field (additive).** Per-
  analyzer status now reports `"tier3"` today, with `"tier25"`
  reserved for the upcoming Tier-2.5 surface. `#[serde(default)]`
  keeps old clients reading the response (#191).

### Changed

- **Source provenance generalized from Tier-3 to tiered.** The
  internal `Tier3Status*` types are renamed to `TierStatus*` and
  the SQL is stripped of `tier3-` literal prefixes so the same
  ranking machinery can serve Tier-2.5 sources next. Old type
  names remain as `pub use` aliases to keep dependents compiling
  (#191).
- **`register.rs` split into a `register/` module.** The 1222-line
  monolith is broken into `register/mod.rs` and
  `register/header_detect.rs`; the C/C++ header detection that
  decides whether `.h` is C, C++, or Objective-C now lives in its
  own file. Behaviour is unchanged (#190).
- **`cairn-lang-typescript-tier3` `TsSiteCollector` split.** The
  983-line `src/lib.rs` is reduced to 535 lines by lifting the
  site collector into its own module. Behaviour is unchanged
  (#189).
- **`find_subtypes` / `find_supertypes` ranking.** When the same
  pair has rows at multiple tiers, results are now returned in
  source-precedence order: `tier3` > `tier25` > `tier2-direct` >
  `tier2-fact`. The first observed `kind_source` per (subject,
  relation, object) wins (#196).

### Fixed

- **`.h` headers routed to the Objective-C backend.** Header
  dispatch was previously fixed to C/C++ on extension alone, so
  Apple framework headers were parsed as C++ and surfaced no
  classes. `.h` is now content-classified, and ObjC `@interface`
  / `@protocol` declarations in a `.h` file reach the ObjC
  backend (#197).
- **Apple `NS_ASSUME_NONNULL_BEGIN` macros no longer suppress
  Objective-C class extraction.** The ObjC Tier-1 pre-process now
  neutralizes the common nullability / availability macros
  (`NS_ASSUME_NONNULL_BEGIN` / `_END`, `NS_SWIFT_NAME(...)`,
  `NS_DESIGNATED_INITIALIZER`, …) before the grammar runs, so
  `@interface` declarations in modern Apple headers parse and
  produce class symbols (#198).

### Tooling

- **`cairn-resolver-eval` golden harness.** A new internal crate
  exercises `find_subtypes` / `find_supertypes` against language-
  curated fixtures and a baseline JSON, so changes to Tier-2 and
  the forthcoming Tier-2.5 / Tier-3 paths can be regressed
  against a known-good resolution set (#192).

### Notable

- Public Objective-C class hierarchy is visible for the first
  time when reading modern Apple SDK headers (everything wrapped
  in `NS_ASSUME_NONNULL_BEGIN`).
- Cross-language type-relation queries can now distinguish a fact
  derived directly from the file's own grammar from one resolved
  by a Tier-3 analyzer, via the additive `kind_source` field on
  `ImplHit`.

## [0.6.1] — 2026-06-18

Patch release addressing hint polish from the 0.6.0 dogfood pass and
reshaping the MCP server instructions around the agent pains the new
surface was designed to relieve.

### Added

- **`hints[]` envelope on `repo_status` for degraded paths.** When a
  `repo_status` call resolves to a not-fully-clean state, the response
  now carries a `hints[]` array describing the recovery move:
  - `daemon_not_ready` — the daemon socket is missing or refusing
    connections (injected from the MCP transport layer; works even
    when the data RPC itself can't be reached).
  - `repo_not_registered` — `path=` resolves outside any registered
    repository; carried in the JSON-RPC error `data.hints[]`.
  - `snapshot_stale` — the current snapshot is stale relative to the
    working tree.
  - `tier3_indexing_wait` — Tier-3 analyzer is `queued` or `running`
    for the current view (existing hint code, now also emitted from
    `repo_status`).
  All four hints intentionally omit `action`: cairn describes the
  state, agents decide the next move (#185).
- **`capped_narrow_filter` hint on directory `get_outline`.** When a
  directory-mode `get_outline` is capped by `limit`, the response now
  emits `capped_narrow_filter` ahead of the existing
  `capped_increase_limit`, with
  `params={"narrow_candidates":["kind","max_depth"]}` so agents reach
  for a narrower query before reaching for a wider response (#185).
- **MCP server instructions rewritten around the agent pain the
  redesign relieves.** The `initialize.instructions` reply now opens
  with six concrete failure modes you hit when you reach for grep/Read
  on structural questions (50+ matches, file-named-differently,
  type-hierarchy invisible to text, zero-result-vs-index-not-ready,
  JSX-call-not-a-call, …), then closes with the recovery signal
  ("reading >2 files to locate one definition" or "concluding no such
  symbol without checking tier3 readiness"). Roughly 100 fewer tokens
  than the 0.6.0 cockpit-label form; per-tool intent lives in each
  tool's description instead (#186).

### Changed

- **`relax_filter.drop_params` no longer suggests dropping `repo`.**
  When a query hits zero results with filters and a repo scope set,
  the `empty_result_relax_filter` hint suggests dropping the filters
  only; widening the repo scope remains the job of the separate
  `empty_result_widen_scope` hint. This fixes the redundancy where
  `repo` appeared in both hints' `drop_params` arrays (#185).

### Fixed

- **Codex plugin loader rejected the bundled hooks config.** The
  cairn plugin's `plugin/hooks/hooks.json` carried a top-level
  `description` field used as documentation; Codex's plugin loader
  enforces a strict schema and rejected the file with
  `unknown field 'description', expected 'hooks'` at startup, which
  surfaced to the operator on every Codex.app launch in a
  cairn-installed environment. The field is dropped; the
  documentation moves to the maintainer comment in the source tree
  (#186).

## [0.6.0] — 2026-06-18

The MCP surface is redesigned around AI coding agents: smaller default
responses, structured confidence/diagnostics/hints, a cwd-aware
entrypoint, and a tighter Core/Surface split between the CLI/UDS
primitives and the agent-facing MCP layer. The design and migration
guides ship alongside the binary at `docs/mcp-redesign-0.6.0.md` and
`docs/migration-0.6.0-mcp.md`.

### Added

- **Inventory split: `list_repos` / `repo_status` / `list_jobs`.**
  `list_repos` is now a 1-line-per-repo inventory (alias, root,
  languages, aggregate status, snapshot/file/symbol counts) — a
  21-repo registry returns roughly 6 KB instead of the previous
  14 KB blob. New MCP tool `repo_status({ repo | path })` returns
  per-repo detail (summary, current snapshot,
  `tier3_status.this_repo`, optional snapshot list via
  `include_snapshots`). New MCP tool
  `list_jobs({ repo, state, include_terminal })` exposes background
  analyzer job lifecycle (`state`), scheduler position
  (`scheduler_state`), pool group, queued / pool-wait / run
  milliseconds, progress, and rate. `list_repos` aggregate `status`
  is one of `ready / indexing / partial / error` (priority
  `error > indexing > partial > ready`) (#177).
- **Common response envelope: `diagnostics[]` + `hints[]` +
  `timing`.** All MCP query tools now return a shared envelope.
  `diagnostics[]` reports facts that reduce confidence (each entry
  carries a stable `code`, `severity`, `message`, plus optional
  `language` / `analyzer_id` / `repo` / `file` / `details`).
  `hints[]` is an array of next-move options (each carries a stable
  `code`, `message`, optional `action` from `relax_filter` /
  `widen_scope` / `increase_limit` / `wait_for_index` /
  `try_alternative_query`, plus `tool` / `params` / `drop_params` /
  `target`). Hints are deliberately options, never instructions —
  `reindex_via_cli` omits its `action` field so agents never chain
  a reindex automatically. Both arrays are omitted from the wire
  when empty so happy-path responses stay small (#179, #180).
- **`tier3_status.reason_code` gains `not_scheduled`.** Distinguishes
  a routing gap (the scheduler never enqueued the expected
  analyzer) from indexing latency (`not_recorded`, the run hasn't
  completed yet). With `not_recorded` the agent's correct move is
  "wait or reindex"; with `not_scheduled` the correct move is
  "don't loop — this is a routing or coverage gap." Backed by a
  shared `expected_analyzers_for_manifest()` source of truth that
  query readiness, doctor remediation, and `enqueue_reindex` all
  consume, so the three paths can no longer drift (fixes the
  EVAL-001 remediation loop) (#176).
- **`timing.server_ms` on every MCP response.** Daemon-side wall
  time the data plane spent producing the response, measured at
  the dispatch layer immediately before the method body. Lets
  agents triage "is cairn slow or is the MCP bridge slow?" without
  external tooling — proved decisive for separating EVAL-002
  (Codex sandbox overhead) from genuine daemon latency. `phases`
  is reserved for a future stable per-phase breakdown (#178).
- **TSX caller hint.** `find_callers` for an uppercase symbol
  defined in a `.tsx`/`.jsx` file now emits the
  `tsx_callers_use_instantiate` hint with `tool=find_references`,
  `params={"kind":"instantiate"}`, so React/JSX component usage
  routes to the correct primitive in one read. When this hint
  fires, unrelated generic hints (`empty_result_relax_filter` /
  `empty_result_widen_scope`) are suppressed so the recovery path
  stays clear (#180, #182).
- **Zero-arg `repo_status({})` in MCP.** Omitting both `repo` and
  `path` resolves the path from the MCP server's current working
  directory and is expanded into `path` before the data RPC call.
  The data RPC contract remains the strict "exactly one of `repo`
  or `path`" — composition lives only at the MCP layer, per
  Core/Surface design principle #8 (#182).
- **MCP server instructions and tool descriptions rewritten as
  cockpit labels.** The MCP `initialize` reply now teaches agents
  via five short sections (First move / Core workflow / Retry
  rules / JSX caveat / Composition stance). Each tool description
  follows a `WHEN: … / NOT FOR: … / Recovery: …` cockpit format
  and is capped at roughly 60-120 tokens so the standing context
  cost stays predictable while still preserving the caveats agents
  actually need (#181).
- **`find_symbols` hits include split `file` / `line`.** Alongside
  the existing `location` string, `find_symbols` hits now carry
  separate `file` and `line` fields so agents can feed them
  straight into `get_symbol_source` without parsing a
  colon-separated string (#182).
- **0.6.0 design and migration guides.** `docs/mcp-redesign-0.6.0.md`
  and `docs/migration-0.6.0-mcp.md` ship in-tree as the canonical
  source for the redesign principles, schemas, diagnostic/hint
  taxonomy, dogfood acceptance gates, and the 0.5.x → 0.6.0
  migration path. `docs/design-philosophy.md` (project-agnostic)
  is referenced from the design doc as the lens on the
  Core/Surface separation and directional-refactor rule that drove
  this release.

### Changed

- **Breaking: MCP wire is redesigned for agents.** Inventory tools,
  query response envelopes, and most tool descriptions all change
  shape; details are in `docs/migration-0.6.0-mcp.md`. The data
  RPC layer keeps its primitive contracts (no composition pushed
  down into the core); MCP is now the surface where agent
  ergonomics live, with selective MCP-only composition allowed
  (Design principle #8). cairn has no production users, so
  backward-compatibility shims are deliberately not provided.
- **Breaking: `list_repos` no longer returns snapshots or jobs.**
  Per-snapshot detail moves to
  `repo_status(include_snapshots=true)`; job inventory moves to
  `list_jobs`. The pre-0.6.0 inventory-plus-detail-plus-jobs shape
  is gone.
- **`hints[]` are method-aware.** Each hint carries the invoking
  tool's name and a result-noun appropriate to that tool (e.g.
  `capped_increase_limit` from `get_outline` reports
  `tool="get_outline"` and "outline items", not the previous
  `find_symbols` / "matching symbols" default). `drop_params` lists
  only filters actually set on the call. This fixes the dogfood
  EVAL-0600-001 mis-routing where every method's hints pointed at
  `find_symbols` (#182).
- **`list_jobs` items carry both `state` and `scheduler_state`.**
  `state` is the analyzer-run lifecycle
  (`queued / running / succeeded / failed / cancelled / timed_out /
  skipped`); `scheduler_state` retains its
  scheduler-pool-position meaning. The two axes can legitimately
  differ during transients.
- **Composition non-goal locked.** 0.6.0 does not introduce broad
  multi-step composition tools (no `find_definition_with_source`,
  no `trace_chain`, no `explain_call_path`). Live dogfood with
  Claude and Codex on the redesigned surface showed the primary
  pain was response interpretation and retry planning, not the
  number of calls. The single thin composition shipped is
  `repo_status` accepting `path=` plus its MCP-only zero-arg form.

### Fixed

- **Expected-analyzer drift across query readiness / doctor /
  scheduler.** `tier3_status`, `cairn ctl daemon doctor`, and
  `enqueue_reindex` previously each computed "which analyzers does
  this manifest expect?" independently, so a manifest could be
  reported as missing an analyzer that the scheduler had silently
  skipped. They now share `expected_analyzers_for_manifest()`. As
  an observable side effect, `enqueue_reindex` only queues
  analyzers whose `parser_id` is actually present in the manifest,
  so `list_jobs` no longer surfaces "skipped" rows for unrelated
  analyzers and doctor no longer suggests reindex for analyzers
  the routing layer never intended to run (#176).
- **Daemon-side `server_ms` exposes MCP-bridge latency.** Where
  the previous evaluator saw 150-second MCP `find_symbols` calls,
  the same call now reports `server_ms ≈ 10 ms`, attributing the
  gap to the client-side MCP transport rather than the daemon —
  closing the EVAL-002 investigation that motivated this field
  (#178).

### Performance

- **Hint emission is allocation-conscious on the happy path.**
  Default-empty `diagnostics` and `hints` arrays are
  `skip_serializing_if = "Vec::is_empty"`, so the most frequent
  case (items non-empty, complete, Tier-3 ready) adds no envelope
  noise to the wire and no array allocations to ignore.

## [0.5.0] — 2026-06-16

### Added

- **`tier3_status` rebuilt for query-relevant confidence.** Query
  responses now carry an explicit `tier3_status.this_query` view
  scoped to analyzers whose parser_id touches the query result, so
  unrelated repo-wide analyzer failures no longer pollute the
  confidence signal an agent reads on every call. The body shape
  becomes `{ ready, analyzers[] }` where each entry exposes
  `id` (nullable), `language` (singular), `state`, optional
  machine-readable `reason_code`, and free-text `reason`. The
  state enum is `ready / queued / running / missing / failed /
  skipped / stale / not_applicable`; internal `succeeded` rows
  collapse to `ready`. Pass `verbose_tier3=true` (CLI:
  `--verbose-tier3`, MCP: `verbose_tier3` argument) to additionally
  receive a `repo_wide` body covering the full repo's Tier-3
  matrix for diagnostics (#172, #173).
- **Scheduler and analyzer-job observability.** `cairn ctl jobs
  list` now surfaces per-job `scheduler`, `group`, `queued_ms`,
  `pool_wait_ms`, `run_ms`, `progress_ticks`, and rate so operators
  and agents can tell a stalled run from a long but healthy one
  without tailing the daemon log.
- **`cairn ctl daemon status` repo summary view.** The default
  output now collapses each repo into a single
  `<alias> (<root>) [<langs>] snapshots=N ready=M stale=K files=Σ
  symbols=Σ` line so the command stays usable on many-snapshot
  registries; `--snapshots` restores the legacy per-snapshot
  expansion (#171).
- **`cairn ctl jobs prune`.** Historical terminal analyzer rows
  from old manifests can now be garbage-collected with a
  `--dry-run` preview, scoped optionally by `--repo`. Keeps the
  active anchor's history intact.
- **Content-aware C-family header routing.** Ambiguous
  `.h` / `.hpp` headers now route to the C, C++, or Objective-C
  backend based on file content rather than extension alone, which
  fixes symbol extraction for header-only C++ libraries and mixed
  Objective-C / C codebases.

### Changed

- **Breaking: `cairn ctl` admin surface reorganized by lifecycle.**
  Top-level subcommands are now `repo` (register / remove /
  reindex / list), `jobs` (list / cancel / prune), `blobs` (prune),
  and `daemon` (status / doctor / shutdown). Pre-0.5.0 flat
  commands (`register-repo`, `remove-repo`, `status`, `reindex-
  repo`, `jobs`, `prune`, `doctor`, `shutdown`) are removed without
  aliases.
- **Breaking: `tier3_status` wire shape replaced.** The legacy flat
  `tier3_status: { ready, pending_analyzers: [...] }` payload
  emitted at the response root is removed; clients must read
  `tier3_status.this_query.ready` and `tier3_status.this_query.
  analyzers`. The `pending_analyzers` field and the
  `PendingAnalyzer` struct are gone; `repo_wide` is opt-in via
  `verbose_tier3` (#173, supersedes #172).
- **MCP forwarding plumbing shared across tools.** All 12 MCP
  tools now route through a single forwarding helper instead of
  per-tool clients, which keeps daemon dispatch behavior consistent
  and makes future surface additions a one-line registration.

### Fixed

- **Tier-3 reference echoes from clangd suppressed.** The
  clangd-backed C / C++ / Objective-C backend no longer emits
  use-site definition rows that point back at the use itself,
  which produced false call edges where Tier-3 looked like it
  resolved an external API to the caller it appeared in (e.g.
  `wait_job(...)` calls binding to their enclosing `main`).
- **Rust duplicate references at zero-range Tier-2 / Tier-3
  joins.** `find_references` and `find_callers` no longer return
  paired `target_qualified=foo` and `target_qualified=bar::foo`
  rows when both a zero-range Tier-2 row and a Tier-3 row resolve
  the same call site; the lower-quality row is dropped on the
  default view. `include_noise=true` still returns both for
  diagnostics.
- **TypeScript Tier-3 skips local binding calls before LSP
  definition requests.** Skipping calls that resolve to a same-file
  local binding before issuing an LSP definition request stops
  spurious Tier-3 round trips and matches the resolution policy
  Rust and Python Tier-3 already use.
- **Daemon awaits watcher startup before serving.** The control
  and data sockets now appear only after every registered repo
  watcher has reported ready, so an early-arriving CLI no longer
  races a not-yet-loaded registry.

### Performance

- **Multi-kind LSP definition passes per document.** Tier-3 now
  bundles definition queries for every reference kind in a
  document into one LSP pass instead of one pass per kind, which
  removes a multiplier-of-N visit cost on large files.
- **Pool-aware analyzer scheduler.** Analyzer jobs that share a
  pooled LSP backend (e.g. clangd across C / C++ / Objective-C)
  are dispatched through a scheduler that serializes same-pool
  work and parallelizes across pools, so pool contention no longer
  blocks unrelated languages and stays observable through the new
  job metrics.

## [0.4.2] — 2026-06-14

### Fixed

- **Daemon / client version match guard.** The CLI and MCP front-ends
  did not verify the running daemon's version, so a stale daemon
  (e.g. left over from a previous install) silently produced
  confusing failures rather than an actionable diagnostic. The
  front-ends now fetch the daemon version through the existing
  `ctl status` response once per process and compare it against
  `CARGO_PKG_VERSION` using a pre-1.0 SemVer rule:
  - Same patch / patch drift: silent.
  - Minor mismatch: stderr warning, continues.
  - Major mismatch (CLI): actionable error, aborts.
  - Major mismatch (MCP): stderr warning, continues serving so the
    host can surface the diagnostic without breaking the JSON-RPC
    session.
  - `cairn ctl shutdown` deliberately bypasses the guard so it
    remains usable as the non-Homebrew remediation path. Warning
    text points users at `brew services restart cairn` or
    `cairn ctl shutdown` then `cairn daemon` (#150).

## [0.4.1] — 2026-06-14

### Fixed

- **Post-0.4.0 dogfood follow-ups.** A v0.4.0 evaluation pass found
  four follow-ups that landed together for 0.4.1.
  - `cairn ctl status` no longer inlines per-repo job history; a
    compact `JobSummary` of analyzer-job state counts replaces the
    previous list. The legacy `RepoStatus.jobs` field stays on the
    wire but is left empty so older clients keep parsing. Detailed
    history remains available through `cairn ctl jobs`, which now
    defaults to active jobs plus the latest terminal row per `(repo,
    analyzer)` for current anchors; `--all` restores full history,
    `--limit` caps row count, and `--json` prints a JSON array
    directly. `list_repos` similarly omits jobs unless
    `include_jobs=true` is set, with matching MCP schema (#146).
  - `cairn ctl doctor` reindex remediations now use the actual
    positional `cairn ctl reindex-repo <alias>` instead of the stale
    `--alias <ALIAS>` form (#146).
  - `find_references` and `find_callers` no longer return duplicate
    rows for the same logical call site when both a Tier-2 semantic
    row (zero byte range) and a Tier-3 row exist. Default view
    suppresses the zero-range non-Tier-3 row when a Tier-3 row covers
    the same blob, line, kind, target, and enclosing symbol;
    `include_noise=true` continues to return both rows (#147).
  - MCP `find_symbols` description now ends with a zero-hit recovery
    hint (try exact `fuzzy=false`, prefix wildcard, relax
    container/path/kind, broaden repo/anchor). MCP `find_callers` and
    `find_references` descriptions and the `cairn query callers /
    refs` CLI help point React/JSX component usage to
    `find_references kind=instantiate` (#146).

## [0.4.0] — 2026-06-14

### Added

- **Tier-3 cross-file resolution across every supported language.**
  Eight new LSP backends join the existing `rust-analyzer`,
  `pyright-langserver`, and `gopls` analyzers: `clangd` is shared
  across C, C++, and Objective-C; `typescript-language-server` is
  shared across TypeScript, JavaScript, and TSX; `jdtls`,
  `kotlin-language-server`, `sourcekit-lsp`, `csharp-ls`,
  `ruby-lsp`, and `phpantom-lsp` cover Java, Kotlin, Swift, C#,
  Ruby, and PHP. Tier-3 now resolves calls and type references to
  their cross-file definitions across the supported language set.
- **Async indexing.** `cairn ctl reindex-repo <alias>` now returns
  immediately instead of blocking on LSP cold starts. Per-analyzer
  work is tracked as jobs via `cairn ctl jobs --alias <alias>`,
  `--state`, `--json`, and `--cancel`; scripts that need
  synchronous behavior can use `reindex-repo --wait --timeout`.
  Query result envelopes carry `tier3_status`, so clients can
  distinguish a confident-empty answer from one that is still
  indexing. The async job state is persisted by CAS schema migration
  v5.

### Fixed

- **Tier-3 reliability hardening.** `clangd` and
  `typescript-language-server` now use initialize-response readiness
  for servers that emit no progress notifications; `jdtls`,
  `kotlin-language-server`, `sourcekit-lsp`, and `csharp-ls` use an
  executable availability probe instead of rejected `--version`
  flags; `csharp-ls` receives a dotnet environment so MSBuild
  discovery works under launchd. LSP discovery also searches
  standard per-user bin directories, analyzers sharing a pooled LSP
  are serialized, total analyzer timeout is replaced by
  progress-based stall detection, per-site definition requests are
  pipelined, `clangd` skips preprocessor pseudo-call-sites, and LSP
  stderr head + tail is surfaced in handshake and exit errors.

- **Workspace quality review.** Reviewed the entire workspace
  (four parallel sessions, 190 findings) and shipped 13 PRs
  (#129–#142) plus two hotfixes addressing every Critical and High
  finding plus 23 mechanical Medium / Low improvements.
  - Critical: `prune` no longer wipes every blob when no language
    backend is registered (#129); test fixtures no longer carry
    developer-shaped absolute paths (#130).
  - High: panic payloads from `spawn_blocking` joins are sanitized
    behind a typed `Error::Internal` instead of leaking through
    `INVALID_PARAMS` (#136); daemon shutdown now stops accepting,
    drains in-flight connections, halts the job manager, and shuts
    the LSP pool down in that order (#139); CAS `reuse_or_compute`
    holds an `IMMEDIATE` transaction across re-check and insert,
    manifest walks skip symlinks, and `git cat-file` argument
    handling rejects non-40-hex SHAs (#137); the socket runtime
    directory is atomically created at `0o700` with owner and mode
    validated on reuse, and UDS sockets land at `0o600` via a
    process-wide umask guard (#138, #141); watcher swaps are atomic
    so a failed restart cannot silently unwatch an alias (#134);
    Tier-1 same-name resolution leaves Ruby, Objective-C, and
    Kotlin collisions unresolved instead of picking by walk order,
    and Ruby grows a real visibility-section stack (#135); Tier-3
    LSP pool `config_hash` now folds in `clangd` fallback flags and
    expands `*.csproj` and `*.sln` globs so dialect drift and
    project edits invalidate stale facts (#132);
    `CAIRN_WORKER_CONCURRENCY` is clamped to the same ceiling as
    automatic sizing with a warn on clamp (#131); the four
    `cairn-proto` public type families gained rustdoc covering wire
    invariants and Cairn-specific error codes (#133).
  - Medium / Low: 23 docstring and comment improvements explaining
    liveness beacons, CAS conversion invariants, LSP timing
    constants, and PHP `.phtml` template inclusion (#140, #142).

### Changed

- **JSON-RPC error code semantics.** Server-side panics in blocking
  tasks now return `INTERNAL_ERROR` (-32603) with a sanitized
  `"internal error"` message instead of being misclassified as
  `INVALID_PARAMS` (-32602) and leaking the raw panic payload onto
  the wire; client argument validation failures correspondingly
  map to `INVALID_PARAMS` rather than falling through to the
  default. Server-side `tracing::error!` still records the full
  context for diagnosis.

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
