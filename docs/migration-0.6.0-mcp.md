# cairn 0.6.0 — MCP Migration Guide

**Status:** Draft, applies when 0.6.0 ships.
**Companion design doc:** [`mcp-redesign-0.6.0.md`](./mcp-redesign-0.6.0.md)

---

## Why MCP changed

cairn's mission is to be a comfortable code-intelligence surface for AI coding agents. 0.5.x kept MCP and CLI perfectly symmetric, which served early development but pushed cognitive load onto the agent: oversized inventory responses, prose-shaped output that wastes context tokens, recovery hints scattered across docstrings instead of in the response itself.

0.6.0 reshapes the MCP surface around three principles:

1. **Default response is small and query-local.** Repo-wide context, snapshots, and jobs are opt-in or moved to a different tool.
2. **Structured output is the source of truth.** Top-level fields (`completeness`, `tier3_status`, `diagnostics`, `hints`, `timing`) carry confidence, failure facts, and next-move suggestions. Text content is a short signpost.
3. **Failures expose facts and next options.** `diagnostics` says what happened; `hints` suggests what to try next. cairn never decides for you.

This is a **breaking** change. Core query semantics are preserved; the wire envelope and inventory tools are redesigned.

---

## Tool mapping 0.5.x → 0.6.0

| 0.5.x tool | 0.6.0 tool(s) | Notes |
|---|---|---|
| `find_symbols` | `find_symbols` | Same name; envelope is new (§Common response below) |
| `get_outline` | `get_outline` | Same name; envelope is new |
| `get_symbol_source` | `get_symbol_source` | Same name; envelope is new |
| `find_references` | `find_references` | Same name; envelope is new |
| `find_callers` | `find_callers` | Same name; envelope is new |
| `find_callees` | `find_callees` | Same name; envelope is new |
| `find_imports` | `find_imports` | Same name; envelope is new |
| `find_subtypes` | `find_subtypes` | Same name; envelope is new |
| `find_supertypes` | `find_supertypes` | Same name; envelope is new |
| `list_repos` (inventory + per-snapshot + jobs) | **`list_repos`** (inventory only) | Lightweight: 1 line per repo |
| | **`repo_status(repo=…)`** (new) | Per-repo detail; default still skips snapshots |
| | **`list_jobs(repo=…)`** (new) | Background analyzer jobs |
| `register_repo` | `register_repo` | Unchanged |
| `reindex_repo` | `reindex_repo` | Unchanged |

---

## Common response envelope

Every MCP query tool returns:

```json
{
  "items": [...],
  "completeness": { "status": "complete", "reason": null },
  "tier3_status": {
    "this_query": { "ready": true, "analyzers": [...] }
  },
  "diagnostics": [...],   // omitted when empty
  "hints": [...],         // omitted when empty
  "timing": { "server_ms": 12 }
}
```

Inventory tools (`list_repos`, `list_jobs`) use the primary noun (`repos`, `jobs`) instead of `items`, but otherwise share the envelope.

### What's new in the envelope

- **`diagnostics[]`** — observed facts that reduce confidence (analyzer missing, run not scheduled, workspace unsuitable, etc.). Always carries a stable `code`. See [design doc §6](./mcp-redesign-0.6.0.md#6-diagnostic-taxonomy-v1).
- **`hints[]`** — possible next moves. Array order is priority. Always carries a stable `code` and `action` (with one exception, `reindex_via_cli`, which deliberately omits `action` so agents don't auto-loop). See [design doc §7](./mcp-redesign-0.6.0.md#7-hint-taxonomy-v1).
- **`timing.server_ms`** — wall time the daemon spent. Lets you triage "is cairn slow or is the MCP bridge slow?" without external tooling. After EVAL-002, where Codex sandbox overhead masqueraded as MCP latency, this is the single best addition for agent debugging.

### `tier3_status` evolution

The `tier3_status` shape introduced in 0.5.0 (B17) is kept:

```json
"tier3_status": {
  "this_query": {
    "ready": true,
    "analyzers": [
      { "id": "rust-analyzer-lsp", "language": "rust", "state": "ready" }
    ]
  }
}
```

`verbose_tier3=true` adds `repo_wide`. `analyzers[].state` enum is unchanged from 0.5.0 (`ready / queued / running / missing / failed / skipped / not_applicable / stale`).

`reason_code` gains one new value: **`not_scheduled`**. This distinguishes a routing gap (the scheduler never enqueued the expected analyzer) from indexing latency (it was enqueued but hasn't completed yet, `not_recorded`). With `not_recorded` an agent's correct move is "wait or reindex". With `not_scheduled` the correct move is "don't loop — file an issue or work around the gap". This fixes the EVAL-001 remediation loop.

---

## Inventory / status / jobs examples

### Pick a repo to query

**Before (0.5.x):** `list_repos` returned every repo × every snapshot × every enrichment matrix. A 21-repo registry returned a ~14 KB JSON blob just to answer "what aliases exist?"

**After (0.6.0):**

```json
> list_repos
{
  "repos": [
    { "alias": "cairn", "root": "/Users/.../cairn", "languages": ["markdown", "rust"], "status": "ready", "snapshot_count": 37, "current_file_count": 5479, "current_symbol_count": 117449 },
    { "alias": "loom", "root": "/Users/.../loom", "languages": ["javascript", "markdown", "rust", "tsx", "typescript"], "status": "ready", "snapshot_count": 13, "current_file_count": 517, "current_symbol_count": 9682 }
  ],
  "completeness": { "status": "complete" },
  "timing": { "server_ms": 8 }
}
```

One line per repo, summary counts only. `status` is an aggregate (`ready` / `indexing` / `partial` / `error`) computed across the repo's snapshots — enough to pick an alias.

### Verify one repo before querying

```json
> repo_status(repo="cairn")
{
  "repo": {
    "alias": "cairn",
    "root": "/Users/.../cairn",
    "languages": ["markdown", "rust"],
    "summary": {
      "snapshot_count": 37,
      "ready_snapshot_count": 37,
      "stale_snapshot_count": 0,
      "current_file_count": 5479,
      "current_symbol_count": 117449
    },
    "current": { "anchor": "HEAD", "status": "ready" },
    "tier3_status": {
      "this_repo": {
        "ready": true,
        "analyzers": [{ "id": "rust-analyzer-lsp", "language": "rust", "state": "ready" }]
      }
    }
  },
  "timing": { "server_ms": 12 }
}
```

`include_snapshots=true` adds the per-snapshot detail. `repo_status` does *not* return jobs — use `list_jobs(repo="cairn")`.

`repo_status` also accepts `path=` instead of `repo=` (mutually exclusive). Pass a working-directory path; the daemon resolves the registered repo.

### Diagnose Tier-3 analyzer status

```json
> list_jobs(repo="cairn", include_terminal=false)
{
  "jobs": [
    {
      "job_id": 12345,
      "alias": "cairn",
      "analyzer_id": "rust-analyzer-lsp",
      "scheduler_state": "running",
      "pool_group": "rust-pool",
      "queued_ms": 23,
      "pool_wait_ms": 0,
      "run_ms": 1245,
      "progress_ticks": 89,
      "rate": 4.3
    }
  ],
  "timing": { "server_ms": 5 }
}
```

`include_terminal=true` includes completed jobs (cost: response size).

---

## Query retry examples

### Empty result, filters were applied

```json
> find_symbols(repo="cairn", query="parse_args", path="crates/foo/", kind="function")
{
  "items": [],
  "completeness": { "status": "complete" },
  "tier3_status": { "this_query": { "ready": true, "analyzers": [...] } },
  "hints": [
    {
      "code": "empty_result_relax_filter",
      "action": "relax_filter",
      "params": ["path", "container", "kind"],
      "message": "Try without path/container/kind."
    }
  ],
  "timing": { "server_ms": 7 }
}
```

The hint enumerates which params to drop. An agent reads `action: relax_filter` and `params: ["path", ...]`, decides which to drop first, and re-calls.

### Empty result, exact-match was used

```json
> find_symbols(repo="cairn", query="parse_args_v2")
{
  "items": [],
  "completeness": { "status": "complete" },
  "hints": [
    {
      "code": "empty_result_try_fuzzy",
      "action": "try_alternative_query",
      "tool": "find_symbols",
      "params": { "fuzzy": true },
      "message": "No exact symbol matched. Try fuzzy=true or a prefix wildcard."
    }
  ],
  "timing": { "server_ms": 7 }
}
```

### Capped query

```json
{
  "items": [/* 500 hits */],
  "completeness": { "status": "partial_truncated", "reason": "cap" },
  "hints": [
    {
      "code": "capped_increase_limit",
      "action": "increase_limit",
      "message": "Result was capped at limit=500. Increase limit, or narrow the query with kind/container."
    }
  ],
  "timing": { "server_ms": 34 }
}
```

`completeness.reason="cap"` is the canonical fact. The hint is the actionable suggestion. cairn never produces both as separate diagnostics — one fact, one canonical place.

### Analyzer missing, distinguish "wait" vs "routing gap"

```json
{
  "items": [...],
  "completeness": { "status": "complete" },
  "tier3_status": {
    "this_query": {
      "ready": false,
      "analyzers": [
        {
          "id": "clangd-cpp-lsp",
          "language": "cpp",
          "state": "missing",
          "reason_code": "not_scheduled",
          "reason": "Analyzer was expected for cpp but no job was scheduled for this manifest."
        }
      ]
    }
  },
  "diagnostics": [
    {
      "code": "analyzer_not_scheduled",
      "severity": "warning",
      "language": "cpp",
      "analyzer_id": "clangd-cpp-lsp",
      "message": "clangd-cpp-lsp was expected for cpp but no analyzer job was scheduled for this manifest. Reindex may not fix this; check analyzer routing."
    }
  ],
  "timing": { "server_ms": 15 }
}
```

Compare to `reason_code: "not_recorded"`, which means "the job is expected and either is running or hasn't completed yet — wait or reindex". This is the EVAL-001 remediation-loop fix.

### Tier-3 indexing in progress

```json
{
  "items": [/* Tier-1/2 hits */],
  "completeness": { "status": "complete" },
  "tier3_status": {
    "this_query": {
      "ready": false,
      "analyzers": [
        { "id": "rust-analyzer-lsp", "language": "rust", "state": "running" }
      ]
    }
  },
  "hints": [
    {
      "code": "tier3_indexing_wait",
      "action": "wait_for_index",
      "target": "tier3",
      "analyzer_id": "rust-analyzer-lsp",
      "message": "Tier-3 analyzer is running. Use list_jobs to see progress, then re-query."
    }
  ],
  "timing": { "server_ms": 9 }
}
```

---

## Removed behavior

- **Default `list_repos` no longer includes per-snapshot branch lists, enrichment matrices, or job history.** Use `repo_status(repo=…, include_snapshots=true)` and `list_jobs(repo=…)`.
- **The `pending_analyzers` field name** (removed in 0.5.0 already, mentioned here for completeness).
- **Free-form prose-shaped MCP text** as the primary output. Text content is now a 1-3 line signpost; structured fields are authoritative.

---

## For agent and plugin authors

If you ship cairn integrations (Claude Code plugin, Codex plugin, IDE bridges):

- Update tool descriptions to the new MCP descriptions (the cockpit-label format: intent + WHEN + NOT FOR + recovery hint).
- If your bridge caches tool schemas, invalidate the cache on cairn upgrade.
- The `list_repos` → `repo_status` / `list_jobs` split is the largest agent-facing change. Agent muscle memory of "call list_repos for everything" will shift to "list_repos to choose, repo_status to inspect, list_jobs to diagnose".
- The `hints[]` field is structured. Wiring it to agent retry logic is optional but rewarding. The `action` enum is small (`relax_filter / widen_scope / increase_limit / wait_for_index / try_alternative_query`) — easy to dispatch.
- `timing.server_ms` is the right metric to surface in agent latency dashboards.

---

## Versioning

- 0.6.0 is **breaking** for MCP wire and tool surface.
- 0.6.x patches will refine `diagnostic` / `hint` coverage and `timing.phases` shape (the latter is marked unstable in v1).
- Core query tool names (`find_symbols`, `get_outline`, `find_references`, etc.) are preserved across 0.6 and into 1.0.
