# Long-lived rust-analyzer design

Status: Phase 1 design only. This document intentionally does not prescribe
or include implementation changes.

## Context

The current Rust Tier-3 analyzer starts a fresh `rust-analyzer` process for
each workspace analysis run, sends a batch of definition requests, then shuts
the process down. That keeps ownership simple, but it pays the workspace
warmup cost on every reindex and makes rust-analyzer readiness handling the
dominant source of latency.

The target design is a daemon-scoped pool of long-lived rust-analyzer clients
that can be reused across workspace analysis runs while keeping the public
analyzer API small.

## Goals

- Reuse rust-analyzer warm state across repeated analyses of the same repo.
- Keep one process per repo/worktree by default.
- Keep `WorkspaceAnalyzer` trait changes out of the first implementation if
  possible.
- Preserve the lightweight self-typed LSP client boundary in
  `crates/cairn-core/src/lsp/mod.rs`; do not introduce `lsp-types` just for
  pooling.
- Provide predictable shutdown, restart, and status behavior for daemon use.

## Non-goals

- A generic multi-language LSP pool.
- Cross-repo sharing of one rust-analyzer process.
- Fine-grained interactive editor-style synchronization in the first PR.
- Changing the per-blob `Analyzer` API.

## Recommended lifecycle

Use lazy spawn rather than daemon startup spawn.

The pool should start a rust-analyzer process the first time a Rust workspace
analysis run needs one for a given repo key. Startup spawn would make daemon
boot slower, would require eagerly discovering which registered repos contain
Rust code, and would keep processes alive for repos that may not be queried in
the current daemon session.

Idle eviction should be part of the daemon-owned pool. A reasonable default is
15-20 minutes since last use. Eviction sends LSP shutdown/exit first, then kills
the child after a short timeout if it does not exit. The timeout should be a
constant near the pool implementation so tests can configure it.

This gives the common path warm reuse while bounding memory in multi-repo
sessions.

## Ownership and registry

The pool should live in `cairn-core`, close to the existing LSP client module,
for example:

```text
crates/cairn-core/src/lsp/pool.rs
```

The daemon should own the pool lifetime. The first implementation can expose a
small daemon-global accessor only if that is the lowest-risk way to avoid
changing the `WorkspaceAnalyzer` trait, but the pool still needs an explicit
`shutdown_all` hook called during daemon shutdown. A pure process-global pool
without daemon shutdown ownership would make tests and repeated daemon runs
harder to reason about.

Recommended lookup key:

```text
canonical_repo_root + analyzer_id + rust_analyzer_binary + config_hash
```

The canonical repo root is a better primary key than a user-visible alias
because aliases can change and multiple aliases can point at the same path.
The binary path and config hash belong in the key so a changed launch
configuration creates a new process instead of silently reusing stale state.

Thread safety should avoid holding the whole registry lock during spawn:

- The registry maps `PoolKey` to `Arc<PoolEntry>`.
- The registry itself is guarded by a `RwLock` or `Mutex`.
- Each entry owns its own state lock for spawn/restart/shutdown.
- Concurrent callers for the same key share the same entry and await the same
  spawn instead of launching duplicate rust-analyzer processes.
- Concurrent callers for different repos do not block each other except for a
  short registry lookup/insert.

`LspClient` already owns request IDs and pending response state. Pooling should
not add a second request protocol layer; it should only manage process lifetime
and access to a client.

## Runtime model

`WorkspaceAnalyzer::analyze_workspace` is synchronous today, while `LspClient`
is async and relies on a reader task. The current Rust Tier-3 analyzer creates
a short-lived Tokio runtime per analysis run, which is incompatible with a
long-lived client because dropping the runtime also drops the reader task.

Recommended first implementation: the pool owns a dedicated Tokio runtime
thread. Synchronous analyzers enter the pool through a blocking API that
executes async LSP work on that runtime. This keeps the `WorkspaceAnalyzer`
trait unchanged and keeps the reader tasks alive for the lifetime of the pool.

A later refactor can pass an explicit daemon runtime or workspace analysis
context, but that should not be required for the first long-lived
rust-analyzer PR.

## File change propagation

Use simple full-text synchronization first.

For each workspace analysis run, the Rust analyzer already receives the set of
Rust files from the current manifest entries and reads the current worktree
contents. Before sending definition requests for a file, the pool should ensure
rust-analyzer has the current text:

- If the document is not open in the pooled client, send `textDocument/didOpen`.
- If it is already open, send a full-text `textDocument/didChange` with a
  monotonically increasing version.

If implementing version tracking is too much for the first PR, `didClose`
followed by `didOpen` per touched file is acceptable as a short-term fallback.
It is less efficient but simple and correct for reindex-driven analysis.

`cairn-watch` does not need to push every filesystem event directly into the
LSP pool in the first implementation. It can continue to trigger reindexing;
the workspace analysis run then synchronizes the files it actually analyzes.
That preserves the existing dataflow and avoids introducing an editor-style
live document cache before it is needed.

## Crash, restart, and shutdown

`LspClient` already has restart-oriented state, but a pooled client needs a
clear ownership contract:

- Request failures caused by process exit or broken pipes mark the entry
  unhealthy.
- The next request for that entry attempts one restart, bounded by the existing
  max-restart policy.
- If restart fails, the workspace analysis run fails through the existing
  analyzer error path so `workspace_analysis_runs` records the failure.
- Restart should reinitialize rust-analyzer and re-open documents needed by
  the current analysis run.

The daemon shutdown path should call `pool.shutdown_all()`. Shutdown should:

1. Stop idle eviction tasks.
2. Send LSP `shutdown` and `exit` for each live client.
3. Wait for child exit with a short timeout.
4. Kill remaining children as a last resort.

The pool should not rely on process exit or object drop alone for cleanup.

## Multi-repo behavior

Run one rust-analyzer process per canonical repo root. Do not use one shared
process with dynamic workspace folders in the first implementation.

This matches rust-analyzer's workspace model, keeps failure isolation simple,
and avoids cross-repo cache invalidation ambiguity. The tradeoff is memory:
large Rust workspaces can consume hundreds of MB to more than 1 GB per
process. Lazy spawn plus idle eviction are therefore required parts of the
design, not optional polish.

## Launch control and status

The pool should only spawn rust-analyzer when a Rust workspace analysis run has
Rust files to analyze. Non-Rust repos should not create a process.

Launch configuration should remain explicit:

- Use the configured rust-analyzer binary when provided.
- Preserve the existing fallback to `rust-analyzer` on `PATH` unless the
  product decision is to require `RUST_ANALYZER` for Tier-3 enablement.
- Include the resolved binary identity in the pool key.
- Surface binary-not-found or startup-timeout failures through the existing
  workspace analysis failure/skipped path.

Useful status states for future CLI or daemon inspection:

- `not_started`
- `starting`
- `ready`
- `unhealthy`
- `evicting`
- `stopped`

Status surfacing can be a later PR if the first pooling PR keeps the failure
path observable through `workspace_analysis_runs`.

## API minimality

Avoid changing the `WorkspaceAnalyzer` trait in the first implementation.

The Rust Tier-3 analyzer can call a small `cairn-core` pool API internally:

```text
get_or_spawn_rust_analyzer(key, spawn_spec)
with_rust_analyzer(key, spawn_spec, |client| ...)
shutdown_all()
```

The exact names can follow local style, but the important boundary is that
`WorkspaceAnalyzer::analyze_workspace` still receives the same repo root,
manifest id, and file list. Pool access should be an implementation detail of
the Rust Tier-3 analyzer and `cairn-core::lsp`.

`LspClient::shutdown(self)` consumes the client today. Pool eviction may need
either:

- an owned `Option<LspClient>` inside `PoolEntry`, so eviction can take and
  consume it, or
- an additional by-reference shutdown method with careful idempotency.

Prefer the owned-entry approach first because it avoids making `LspClient`
shutdown semantics more complex than necessary.

## Proposed PR split

Recommended split:

1. LSP client readiness and document sync primitives.
   - Add or stabilize `didOpen`, full-text `didChange`, and readiness behavior.
   - Add tests with fake LSP IO.
   - Estimate: 150-250 LOC.
2. Long-lived rust-analyzer pool integration.
   - Add pool, keying, lazy spawn, dedicated runtime thread, and Rust Tier-3
     analyzer usage.
   - No idle eviction in this PR if shutdown is implemented.
   - Estimate: 250-400 LOC.
3. Eviction, status, and daemon shutdown polish.
   - Add idle eviction, status snapshots, and explicit daemon shutdown hook.
   - Estimate: 150-250 LOC.

A single PR is possible, but it is likely 550-900 LOC and mixes protocol
correctness, process ownership, and daemon lifecycle. The split above makes it
easier to review failures and preserve the existing analyzer boundary.

## Open questions

- Should Tier-3 require `RUST_ANALYZER`, or is the current `PATH` fallback part
  of the supported interface?
- What idle timeout should be the default: 15 minutes, 20 minutes, or a
  configurable value?
- Should status be exposed immediately through a CLI/debug endpoint, or is
  `workspace_analysis_runs` enough for the first implementation?
