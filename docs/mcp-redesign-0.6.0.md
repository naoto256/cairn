# cairn 0.6.0 — MCP Redesign for Agents

**Status:** Approved. Implementation plan in §11.
**Authors:** Claude (司令塔) × Codex (cairn-eval), 5-turn discussion converged 2026-06-17.
**Companion doc:** [`migration-0.6.0-mcp.md`](./migration-0.6.0-mcp.md)

---

## 0. Why this exists

Through 0.5.x, cairn's MCP and CLI surfaces have been kept perfectly symmetric. That symmetry served the early phases — one mental model, one wire, one set of tests. But the project's mission has always been:

> **Local, symbol-aware code index — for AI coding agents.**

With 0.5.0 shipping `tier3_status.this_query`, A2 scheduler observability, and content-aware routing, the next bottleneck is no longer correctness or performance. It is **agent cognitive load**:

- Reading a result and deciding "do I trust it?"
- Recovering from a zero-hit / partial / missing-analyzer state without spinning in a remediation loop
- Picking the right tool from a list whose intent is buried in flag combinations

0.6.0 reframes the MCP surface around those moments. The CLI keeps its human ergonomics; the MCP optimizes for an agent reading structured fields and deciding the next move.

This is a **breaking** redesign. cairn has no users to be backward-compatible with, so we use that freedom deliberately and stop carrying symmetric vestiges that hurt agents.

---

## 1. Design principles (locked, 7)

1. **Tool name encodes intent.** A reader of the tool list should not have to read flags to know what a tool is for.
2. **Default response is small and query-local.** Repo-wide context, snapshots, and jobs are opt-in or a different tool.
3. **Structured output is the source of truth.** Text content is a 1-3 line signpost; confidence, diagnostics, hints, and timing are top-level structured fields.
4. **Failures expose facts and next options.** `diagnostics` reports what happened; `hints` suggests possible next moves. cairn does not become a planner — the agent decides.
5. **High-frequency primitives stay stable.** Core query tools (`find_symbols`, `get_outline`, `find_references`, `find_callers`, etc.) keep their names. Only the response envelope and tool descriptions change.
6. **MCP and CLI may diverge at the surface, not at semantics.** The data RPC layer is shared; presentation, tool grouping, and default verbosity may differ.
7. **Descriptions teach selection, not document everything.** Each tool description is a cockpit label: intent, when to use, when *not* to use, recovery hint. Details live in schema property descriptions and response fields.

## 2. Non-goals

- cairn does not become a planner. `hints` is not a policy engine; it offers options, never instructions.
- No MCP spec extensions (no compact mode, no session-aware tool descriptions). Stay inside the spec; rely on concise descriptions instead.
- CLI is *not* required to mirror the MCP surface. `cairn ctl repo status` may exist alongside `repo_status` MCP, but their flags and defaults can diverge for ergonomics.
- No Windows-native support in 0.6.0. WSL2 + Linux binary is the supported path. Distribution polish is a separate axis.

## 3. Compatibility stance

- 0.6.0 MCP wire is **breaking**. Tool names, response shapes, and default verbosity change.
- Core query *semantics* are preserved. A `find_symbols` call still means the same thing it did in 0.5.x; only the wire envelope and the surrounding tools shift.
- Core query *tool names* are preserved: `find_symbols`, `get_outline`, `get_symbol_source`, `find_references`, `find_callers`, `find_callees`, `find_imports`, `find_subtypes`, `find_supertypes`.
- Renamed / split:
  - `list_repos` (was inventory + per-repo detail) → split into `list_repos` (inventory only) + `repo_status` (per-repo) + `list_jobs` (jobs).
- Removed:
  - Human-oriented verbose text as the primary MCP output.

---

## 4. Common response envelope

### Query tools

```json
{
  "items": [...],
  "completeness": { "status": "complete", "reason": null },
  "tier3_status": {
    "this_query": { "ready": true, "analyzers": [...] }
    // "repo_wide": {...}  // only when verbose_tier3=true
  },
  "diagnostics": [...],   // omit when empty
  "hints": [...],         // omit when empty
  "timing": { "server_ms": 12 }
}
```

`items` is the only required field on the happy path. `completeness` and `tier3_status` are always present. `diagnostics` and `hints` are omitted when empty. `timing.server_ms` is always present.

### Inventory / control tools

The primary collection uses a noun, not `items`. This makes the response self-describing.

```json
{
  "repos": [...],
  "completeness": {...},   // present when capped
  "timing": { "server_ms": 8 }
}
```

```json
{
  "jobs": [...],
  "completeness": {...},
  "timing": { "server_ms": 5 }
}
```

`repo_status` returns a single `repo` object (not a collection) plus optional `diagnostics` / `hints` / `timing`.

---

## 5. Inventory tools

### 5.1 `list_repos`

**Intent:** registered repo inventory, lightweight.

**Input**
```json
{
  "query": "cairn",     // optional, alias or root substring filter
  "limit": 100          // optional, default 100
}
```

**Output**
```json
{
  "repos": [
    {
      "alias": "cairn",
      "root": "/Users/.../cairn",
      "languages": ["markdown", "rust"],
      "status": "ready",
      "snapshot_count": 37,
      "current_file_count": 5479,
      "current_symbol_count": 117449
    }
  ],
  "completeness": { "status": "complete" },
  "timing": { "server_ms": 8 }
}
```

**`status` enum** (aggregate, priority error > indexing > partial > ready):

- `ready` — current snapshot queryable, no blocking diagnostics
- `indexing` — analyzer jobs active or current snapshot not yet ready
- `partial` — queryable but some stale snapshots or missing optional analyzers
- `error` — repo root missing, index unreadable, current snapshot unavailable

**Description (~85 tokens)**
```
List registered repos with alias, root, language coverage, and aggregate status.

WHEN: You need to know which repos cairn covers, or to pick a repo alias for query tools.
NOT FOR: Per-repo snapshot/job/analyzer detail — use repo_status.
```

### 5.2 `repo_status`

**Intent:** detail for one repo.

**Input**
```json
{
  "repo": "cairn",                  // exactly one of repo/path required
  "path": null,                     // exclusive with repo
  "include_snapshots": false,       // optional, default false
  "verbose_tier3": false            // optional, default false
}
```

Both `repo` and `path` set → error. Neither set → error.

**Output (default)**
```json
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
    "current": {
      "anchor": "HEAD",
      "status": "ready"
    },
    "tier3_status": {
      "this_repo": {
        "ready": true,
        "analyzers": [
          { "id": "rust-analyzer-lsp", "language": "rust", "state": "ready" }
        ]
      }
    }
  },
  "diagnostics": [],
  "hints": [],
  "timing": { "server_ms": 12 }
}
```

`include_snapshots=true` adds `repo.snapshots: [...]`. No `include_jobs` — use `list_jobs(repo=...)`.

**Description (~95 tokens)**
```
Status of one registered repo: language coverage, snapshot summary, current anchor, and Tier-3 analyzer readiness.

WHEN: You have a repo alias (or a path under one) and need to verify it's indexed before querying, or to diagnose missing analyzers.
NOT FOR: Multi-repo inventory — use list_repos. Job-level detail — use list_jobs.
```

### 5.3 `list_jobs`

**Intent:** background analyzer job status.

**Input**
```json
{
  "repo": "cairn",              // optional
  "state": "running",           // optional, job lifecycle filter
  "include_terminal": false,    // optional, default false
  "limit": 50                   // optional
}
```

**Output**
```json
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
  "completeness": { "status": "complete" },
  "timing": { "server_ms": 5 }
}
```

**Description (~75 tokens)**
```
Background analyzer job status with timing and progress metrics.

WHEN: You need to diagnose why Tier-3 analyzers are slow, stalled, or not running for a repo.
NOT FOR: One-shot "is the index ready" check — use repo_status.tier3_status.
```

---

## 6. Diagnostic taxonomy (v1)

Diagnostics report **facts**: what happened, why confidence is reduced. Required fields: `code`, `severity`, `message`. Optional: `language`, `analyzer_id`, `repo`, `file`, `details`.

| code | severity | trigger |
|---|---|---|
| `analyzer_not_recorded` | warning | reindex direct, no run reached the index yet |
| `analyzer_not_scheduled` | warning | expected analyzer was skipped by scheduler routing |
| `analyzer_failed` | warning | LSP crash / protocol error |
| `analyzer_stale` | info | ANALYZER_REVISION mismatch |
| `analyzer_binary_missing` | warning | precheck found binary absent |
| `workspace_unsuitable` | info | analyzer intentionally skipped (e.g., Gemfile.lock absent) |
| `query_failed_partial` | error | query itself partially failed |

Not in v1 taxonomy:
- `result_capped` — represented by `completeness.reason="cap"`.
- `partial_results` — represented by `completeness.status`.
- `tier3_not_applicable` — represented by `tier3_status.analyzers[].state="not_applicable"`.

(DRY: each fact has one canonical representation.)

---

## 7. Hint taxonomy (v1)

Hints suggest **possible next moves**. Required: `code`, `message`. Optional: `action`, `tool`, `params`, `drop_params`, `target`. When multiple hints are returned, **array order is priority**.

| code | action | trigger |
|---|---|---|
| `empty_result_relax_filter` | `relax_filter` | items empty + filters were applied |
| `empty_result_try_fuzzy` | `try_alternative_query` | items empty + exact query, no fuzzy yet |
| `empty_result_widen_scope` | `widen_scope` | items empty + repo/anchor scope can be widened |
| `capped_increase_limit` | `increase_limit` | `completeness.reason="cap"` |
| `tier3_indexing_wait` | `wait_for_index` | Tier-3 analyzer running/queued |
| `tier3_unavailable_alternative` | `try_alternative_query` | Tier-3 missing, agent should consider Tier-1/2 facts or source inspection |
| `tsx_callers_use_instantiate` | `try_alternative_query` | `find_callers` empty + queried symbol's definition is in TSX + name uppercase |
| `reindex_via_cli` | *(no action)* | `analyzer_not_recorded` and no active job — message includes `cairn ctl repo reindex <alias>` |

`reindex_via_cli` deliberately omits `action` to prevent agents from chaining a reindex automatically. cairn surfaces the option; the agent decides.

**Emission rule:** omit hints when items non-empty + completeness complete + tier3 ready. Cap → always include `capped_increase_limit`. Empty + no retry route left → omit. Tier-3 running/queued → include wait hint.

### Action enum v1 (locked, 5)

- `relax_filter`
- `widen_scope`
- `increase_limit`
- `wait_for_index`
- `try_alternative_query`

---

## 8. `tier3_status` and `reason_code` refinement

`state` enum (lifecycle, wire-level): `ready / queued / running / missing / failed / skipped / not_applicable / stale`. Internal `succeeded` collapses to `ready` on the wire.

`reason_code` enum (blocked / explained):

- `binary_not_found`
- `workspace_unsuitable`
- `not_scheduled` ← **new**, distinguishes scheduler routing mismatch from indexing latency
- `not_recorded`
- `stale_revision`
- `analyzer_failed`
- `timed_out`
- `no_matching_files`
- `not_applicable`
- `unknown`

`not_scheduled` vs `not_recorded` is the EVAL-001 fix. With `not_recorded` the agent's correct move is "wait or reindex". With `not_scheduled` the correct move is "don't loop — this is a routing or coverage gap".

The same expected-analyzer selection logic is used by query readiness, doctor remediation, and scheduler enqueue — see Phase 0 below.

---

## 9. `timing`

```json
"timing": {
  "server_ms": 87
}
```

`server_ms` is always present in MCP responses (query tools and inventory tools alike). It is the wall time the daemon spent producing the response.

`phases` is **optional and unstable** in v1:
```json
"timing": {
  "server_ms": 87,
  "phases": { "query_ms": 12, "tier3_status_ms": 5 }
}
```

`server_ms` is the safety net for triaging "is cairn slow or is the MCP bridge slow?" — important after EVAL-002, which turned out to be Codex sandbox overhead and would have been triaged immediately with `server_ms` visible.

---

## 10. Description policy

Each MCP tool description follows this layout:

1. **One sentence**: what intent does this serve?
2. **WHEN**: 1-2 lines on the agent intent that should reach for this tool.
3. **NOT FOR**: 1-2 lines on adjacent tools to deflect to.
4. **Recovery hint** (high-frequency tools only): a short line on what to try if results are empty / capped / tier3 not ready.

Target length: **60–120 tokens** per description. High-frequency tools (`find_symbols`, `find_references`, `find_callers`, `get_outline`) up to 120; low-frequency up to 70.

Property-level (schema) descriptions stay short — `kind`, `repo`, `branch`, `anchor`, `limit` are conventional and need only a line.

Caveats kept in descriptions:
- `find_callers` → React/JSX usage requires `find_references kind=instantiate`.
- `find_references` → outgoing default returns resolved calls only; `include_noise=true` for diagnostics.
- `find_symbols` → free-form body search is grep's job.
- `get_symbol_source` → qualified name; precede with `find_symbols`.
- `list_repos` → details live in `repo_status`.

---

## 11. Implementation phases

### Phase 0 — Single source of truth for expected analyzers (preflight)

Before any wire change, isolate one function:

```text
manifest → parser/language coverage → expected workspace analyzers
```

This is the **canonical** mapping consumed by:
- query `tier3_status` (which analyzers are *expected* to be ready for this query)
- `repo_status.tier3_status.this_repo`
- doctor remediation
- scheduler enqueue (which jobs to start on reindex)

Without P0, `not_scheduled` cannot be reported truthfully — the same drift that produced EVAL-001 would simply move to a new field.

### Phase 1 — Inventory split + `not_scheduled` + `timing.server_ms`

- Split MCP surface into `list_repos` / `repo_status` / `list_jobs` with shapes from §5.
- Implement `not_scheduled` reason_code; surface it in query `tier3_status` and `repo_status.tier3_status.this_repo`.
- Add `timing.server_ms` to every MCP response.
- CLI surface unchanged in this phase.

### Phase 2 — Common envelope + diagnostics + hints

- Implement `diagnostics` and `hints` arrays per §6, §7.
- Wire the emission rules.
- Start with `find_symbols` end-to-end, then propagate the same envelope across query tools.

### Phase 3 — Description rewrite

- Apply the description policy (§10) across every MCP tool.
- Trim verbose paragraphs from existing descriptions.
- Add `WHEN / NOT FOR / Recovery` structure.

P3 is last because the description must reflect the final wire shape.

---

## 12. Dogfood acceptance tests

Acceptance gating for 0.6.0 ship includes both **objective** measurements and **agent comfort** verdicts. The comfort axis is required: this project's mission is to be a comfortable coding environment for cc / codex. Pass-on-paper without comfort-on-feel is a fail.

### Objective gates

- `list_repos` default output is ≤ 1 KB per repo (no snapshots / jobs).
- `repo_status(repo="cairn")` default does not include `snapshots[]` or `jobs[]`.
- `list_jobs(repo="cairn", include_terminal=false)` returns active jobs only.
- On `probe-fresh-express` (JS-only repo), query expected analyzer set matches scheduled analyzer set. If a mismatch is real (routing gap), the diagnostic is `analyzer_not_scheduled`, not `analyzer_not_recorded`.
- On `probe-fresh-nlohmann-json` (header-only C++), either clangd-cpp is scheduled, or `analyzer_not_scheduled` is reported correctly.
- MCP `find_symbols` response includes `timing.server_ms` ≤ 100ms for sub-second queries.
- Empty exact query returns a `hints` entry with code `empty_result_try_fuzzy`.
- Capped query returns `completeness.reason="cap"` AND a `hints` entry with code `capped_increase_limit`.
- Happy path query (items non-empty, ready, complete) returns no `diagnostics` and no `hints`.

### Agent-comfort gates (UX-GATE-001 〜 005)

Both agents (Claude + Codex) must run these gates with **live MCP** access. Each verdict is part of the dogfood report. "Works on paper" without "comfort on feel" is a fail.

#### UX-GATE-001: `list_repos` default response size + 体感

- Run MCP `list_repos` (default args).
- Record: response bytes, approximate token count, repo count.
- Verdict: is the response sufficient for alias selection without snapshot/job noise? Is context residence cost acceptable?
- Target: default reads as inventory; deeper detail only via `repo_status`.

#### UX-GATE-002: description context cost

- Inspect the `tools/list` output (the descriptions actually loaded into agent context).
- Focus on high-frequency tools: `find_symbols`, `get_outline`, `find_references`, `repo_status`, `list_jobs`.
- Verdict: do the descriptions read as cockpit labels? Is tool selection unambiguous? Is the standing context cost reasonable?

#### UX-GATE-003: retry plan immediacy

- Provoke empty result, capped result, and analyzer-missing-or-not-scheduled via live MCP.
- Read `diagnostics` and `hints`. Decide the next action in one read.
- Verdict: can the agent build the retry plan without inferring beyond what the response provides?

#### UX-GATE-004: `timing.server_ms` usefulness

- Compare CLI-fast / MCP-slow cases, or run a deliberately heavy query.
- Use `server_ms` and outer wall time to attribute the latency: server-side vs bridge/client/sandbox.
- Verdict: does `server_ms` actually triage EVAL-002-class issues in practice?

#### UX-GATE-005: two-agent subjective verdict

- Claude and Codex each run the same representative workflow via MCP:
  1. Pick a repo.
  2. Find a symbol.
  3. Read its source.
  4. Recover from empty / missing / capped.
  5. Inspect jobs / status.
- Each agent records a per-step verdict: `comfortable / acceptable / still-frictional`, with one-line reasoning.

### Reporting

Dogfood eval reports from 0.6.0 onward include an explicit `MCP agent UX verdict` section, separate from architecture / correctness / wire / performance. UX gates are not passable by mechanical assertion — they require live MCP runs and named verdicts from both agents.

---

## 13. Open questions / deferred

- `timing.phases` shape is optional and unstable in v1. Field set may change in 0.6.x.
- TSX hint heuristic is v1 lightweight (file extension + uppercase). A more precise definition lookup could land in 0.6.x.
- diagnostic / hint coverage is staged: start with the EVAL-001 path (`analyzer_not_scheduled` family) and the common retry hints. Expand based on real-session findings in 0.6.x.
- `repo_status` accepting `path` is supported but not used by CLI in 0.6.0. CLI gains it only if a clear user need surfaces.
- Whether to deprecate or repurpose `cairn ctl doctor` MCP exposure is left for 0.6.x — it overlaps with `list_jobs` + `repo_status` once those land.

---

## 14. How this doc was produced

This document is the converged output of a 5-turn Claude × Codex design discussion on 2026-06-17. Both agents are the primary users of cairn (this is the project's stated mission), and both contributed equally to the principles, schemas, and taxonomies above.

User directive that shaped this doc:

> このプロジェクトは cc/codex にとって気持ちのいいコーディング環境の実現を目指しています。方針はかならず二人で握ること。また、評価は MCP の観点からはかならず二人が使って気持ちいいか、この軸を入れてください。

The agent-comfort gates in §12 exist to keep that axis explicit through implementation and dogfood.
