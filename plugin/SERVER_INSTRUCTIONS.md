# Use cairn before grep

If you're about to grep / find / Read for a code question, stop first
and ask: is this a STRUCTURE question (where is X defined, who calls X,
what extends X, what does this file contain) or a TEXT question (TODO
comment, error message, config key)?

STRUCTURE → cairn answers in 1 call:
- "where is X defined" → `find_symbols query=X` (+ `kind`, `container`, `path`, `fuzzy=true`)
- "who calls X" → `find_callers name=X`
- "what does X call" → `find_callees name=X`
- "any other ref" → `find_references symbol=X kind=<call|type|import|instantiate|read|write|override|macro_invoke|annotation>`
- "what extends/implements X" → `find_subtypes name=X`
- "what X extends/implements" → `find_supertypes name=X`
- "imports in this file" → `find_imports file=<path>` (outgoing edges)
- "this file's symbols" → `get_outline file=<path>` (directory tree: `path=<prefix>/`)
- "this symbol's body" → `get_symbol_source qualified=<...>`
- "is path indexed" → `repo_status path=<path>`
- "what repos are registered" → `list_repos`

TEXT → `grep` / `Read` is fine.

If you typed any of these, you missed a cairn call:
- `grep 'fn X|class X|struct X|def X|impl X'` → `find_symbols query=X`
- `grep ': X|impl.*for X|extends X|implements X'` → `find_subtypes name=X`
- `grep 'X('` to find callers → `find_callers name=X`
- `grep '<X'` for JSX/TSX usage → `find_references symbol=X kind=instantiate`
- `grep 'import X|require\(.X.\)'` to find who imports X → `find_references symbol=X kind=import`
- `Read` an 800-line file to find one function → `get_outline` then `get_symbol_source`
- `Read` a function body just to see what it calls → `get_outline` + `find_callees name=X`
- `Read` 3+ files just to locate a definition → `find_symbols`

Pass `repo=` explicitly. Most query tools search all registered repos
when `repo` is omitted. `repo_status` can auto-resolve from cwd when
neither `repo` nor `path` is supplied.

Don't conclude "no such symbol" from an empty result unless ALL hold:
- `tier3_status.this_query.ready` is `true`
- `completeness.status` is `complete` (partial reasons: `cap`,
  `tier2_warming`, `tier3_warming`, `tier3_unavailable`, `analyzer_failed`)
- `hints` is empty
- `repo` was scoped intentionally

If a cairn tool returns the wrong shape (missing kind, stale snapshot
after `register_repo`, analyzer stuck `queued`), surface it via
`repo_status` and the daemon log — don't silently route around it.
