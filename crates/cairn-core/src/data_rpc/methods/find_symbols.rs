//! `find_symbols` — the generalized symbol-query method.
//!
//! Every filter (`query`, `kind`, `container`, `path`, `repo`,
//! `branch`) is optional, but at least one of `query` / `kind` /
//! `container` / `path` must be supplied — they AND together. This
//! lets the same tool answer:
//!
//! - "where is `parse_args` defined" — `{query: "parse_args"}`
//! - "what classes exist" — `{kind: "class"}`
//! - "what methods does `Widget` provide" — `{container: "Widget", kind: "method"}`
//! - "what classes live in `crates/foo/`" — `{kind: "class", path: "crates/foo/"}`
//! - "what does `Widget` inherit" — `{container: "Widget", include_inherited: true}`
//!
//! Cross-branch by default (every snapshot owned by the alias);
//! `repo = None` additionally walks every registered repo so an agent
//! can locate a symbol without knowing which repo it lives in.

use std::collections::{BTreeSet, HashSet};

use cairn_proto::Completeness;
use cairn_proto::common::SymbolKind;
use cairn_proto::methods::{FindSymbolArgs, FindSymbolHit, FindSymbolResult};
use linkme::distributed_slice;
use rusqlite::ToSql;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params, parse_source_tier};
use crate::indexer::symbol_kind_from_db;
use crate::{Error, Result};

pub struct FindSymbols;

#[async_trait::async_trait]
impl DataMethod for FindSymbols {
    fn name(&self) -> &'static str {
        "find_symbols"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindSymbolArgs = parse_params(params)?;
        validate(&args)?;

        let targets = ctx
            .snapshot_targets_any_repo(args.repo.as_deref(), args.branch.as_deref())
            .await?;
        if targets.is_empty() {
            return Err(Error::InvalidArgument(no_match_message(&args)));
        }

        let limit = args.limit.unwrap_or(50);
        let kind_filter = args.kind.as_ref().map(|k| match k {
            SymbolKind::Other(s) => s.clone(),
            other => crate::indexer::symbol_kind_to_db(other),
        });
        let query = args.query.clone();
        let fuzzy = args.fuzzy;
        let container = args.container.clone();
        let include_inherited = args.include_inherited;
        let path = args.path.clone();

        // Move everything we need into the blocking task, including
        // the targets list (we read each snapshot DB synchronously).
        // The result carries (hits, encountered_syntactic_snapshot)
        // so the dispatcher can shape completeness.
        let (hits, semantic_gap): (Vec<FindSymbolHit>, bool) =
            tokio::task::spawn_blocking(move || {
                let mut all: Vec<FindSymbolHit> = Vec::new();
                let mut any_syntactic_when_inherited = false;
                for target in &targets {
                    // Each snapshot may resolve `container` into a
                    // different base chain (different impls table per
                    // branch), so walk it per-target.
                    let containers: Vec<String> = if let Some(c) = container.as_deref() {
                        if include_inherited {
                            let conn = crate::data_db::open(&target.db_path)?;
                            // If this snapshot's enrichment is below
                            // Tier-2, the impls table is empty and the
                            // base walk degenerates to the seed alone.
                            // Flag the gap for the response.
                            let chain = collect_inheritance_chain(&conn, c)?;
                            if chain.len() == 1 {
                                any_syntactic_when_inherited = true;
                            }
                            chain
                        } else {
                            vec![c.to_string()]
                        }
                    } else {
                        Vec::new()
                    };

                    // Fetch limit+1 per snapshot so we can detect
                    // truncation at the aggregation step. Without the
                    // +1 a result that exactly fills the cap looks
                    // indistinguishable from one that overflowed it.
                    let rows = find_symbols_in_db(
                        &target.db_path,
                        &target.repo_alias,
                        &target.branch,
                        query.as_deref(),
                        fuzzy,
                        kind_filter.as_deref(),
                        &containers,
                        path.as_deref(),
                        limit.saturating_add(1),
                    )?;
                    all.extend(rows);
                }
                // Stable order: by (repo, qualified, branch). Deduping
                // by id within a snapshot is implicit (SELECT DISTINCT
                // not needed — symbols.id is unique per snapshot).
                all.sort_by(|a, b| {
                    a.repo
                        .cmp(&b.repo)
                        .then_with(|| a.qualified.cmp(&b.qualified))
                        .then_with(|| a.branch.cmp(&b.branch))
                });
                Ok::<_, Error>((all, any_syntactic_when_inherited))
            })
            .await
            .map_err(|e| Error::InvalidArgument(format!("find_symbols task panicked: {e}")))??;

        // Capture pre-truncation count so the partial reason can name
        // it honestly ("…of N").
        let total = hits.len();
        let mut hits = hits;
        let truncated = hits.len() > limit as usize;
        hits.truncate(limit as usize);

        let completeness = if truncated {
            Completeness::partial_truncated(format!(
                "results capped at limit={limit} (had {total} matches; raise `limit` to see more)"
            ))
        } else if semantic_gap {
            Completeness::partial_semantic(
                "`include_inherited` walked a syntactic-only snapshot; the base-class chain \
                 may be incomplete (impl/inherit edges are Tier-2 facts)",
            )
        } else {
            Completeness::complete()
        };

        Ok(serde_json::to_value(FindSymbolResult {
            items: hits,
            completeness,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindSymbols);

/// At least one of `query` / `kind` / `container` / `path` must be set
/// — otherwise the call would dump the entire snapshot.
fn validate(args: &FindSymbolArgs) -> Result<()> {
    if args.query.as_deref().is_none_or(str::is_empty)
        && args.kind.is_none()
        && args.container.as_deref().is_none_or(str::is_empty)
        && args.path.as_deref().is_none_or(str::is_empty)
    {
        return Err(Error::InvalidArgument(
            "find_symbols: at least one of `query`, `kind`, `container`, or `path` \
             must be supplied (an unfiltered enumeration would return every symbol)"
                .into(),
        ));
    }
    Ok(())
}

fn no_match_message(args: &FindSymbolArgs) -> String {
    let mut parts = Vec::new();
    if let Some(r) = args.repo.as_deref() {
        parts.push(format!("repo=`{r}`"));
    } else {
        parts.push("no registered repos".to_string());
    }
    if let Some(b) = args.branch.as_deref() {
        parts.push(format!("branch=`{b}`"));
    }
    format!("no snapshot matches {}", parts.join(" "))
}

/// Walk the `implementations` table from `seed` upward, returning
/// every qualified name in the chain (seed first, then bases). The
/// walk follows `interface_name` strings because base types in other
/// crates / modules may not have a symbol row in this snapshot.
fn collect_inheritance_chain(conn: &rusqlite::Connection, seed: &str) -> Result<Vec<String>> {
    let mut result: Vec<String> = vec![seed.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(seed.to_string());
    let mut frontier: Vec<String> = vec![seed.to_string()];
    let mut stmt = conn.prepare(
        "SELECT i.interface_name
           FROM implementations i
           JOIN symbols s ON s.id = i.type_id
          WHERE s.qualified = ?1 AND i.interface_name <> ''",
    )?;
    while let Some(cur) = frontier.pop() {
        let bases: Vec<String> = stmt
            .query_map(rusqlite::params![cur], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for base in bases {
            if seen.insert(base.clone()) {
                frontier.push(base.clone());
                result.push(base);
            }
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn find_symbols_in_db(
    db_path: &std::path::Path,
    repo_alias: &str,
    branch: &str,
    query: Option<&str>,
    fuzzy: bool,
    kind_filter: Option<&str>,
    containers: &[String],
    path: Option<&str>,
    limit: u32,
) -> Result<Vec<FindSymbolHit>> {
    let conn = crate::data_db::open(db_path)?;
    let limit_clause = format!(" LIMIT {}", limit.max(1));

    // Build the SQL + bound-parameter list in lockstep.
    let mut sql = String::new();
    let mut bound: Vec<Box<dyn ToSql>> = Vec::new();

    let use_fts = fuzzy && query.is_some_and(|q| !q.is_empty());
    if use_fts {
        sql.push_str(
            "SELECT s.id, s.name, s.qualified, s.kind, s.signature, s.source, \
                    f.path, s.line_start \
             FROM symbols_fts ft \
             JOIN symbols s ON s.id = ft.rowid \
             JOIN files   f ON f.id = s.file_id \
             WHERE symbols_fts MATCH ?",
        );
        bound.push(Box::new(sanitize_fts_query(query.unwrap())));
    } else {
        sql.push_str(
            "SELECT s.id, s.name, s.qualified, s.kind, s.signature, s.source, \
                    f.path, s.line_start \
             FROM symbols s JOIN files f ON f.id = s.file_id \
             WHERE 1=1",
        );
    }

    if !use_fts
        && let Some(q) = query
        && !q.is_empty()
    {
        sql.push_str(" AND (s.name = ? OR s.qualified = ?)");
        bound.push(Box::new(q.to_string()));
        bound.push(Box::new(q.to_string()));
    }
    if let Some(k) = kind_filter {
        sql.push_str(" AND s.kind = ?");
        bound.push(Box::new(k.to_string()));
    }
    if let Some(p) = path
        && !p.is_empty()
    {
        sql.push_str(" AND f.path LIKE ?");
        bound.push(Box::new(format!("{p}%")));
    }
    if !containers.is_empty() {
        // `container = "Foo"` matches `Foo::*` (Rust) and `Foo.*`
        // (Python). When `include_inherited` expanded the seed to a
        // chain, OR every prefix together.
        sql.push_str(" AND (");
        for (i, c) in containers.iter().enumerate() {
            if i > 0 {
                sql.push_str(" OR ");
            }
            sql.push_str("s.qualified LIKE ? OR s.qualified LIKE ?");
            bound.push(Box::new(format!("{c}::%")));
            bound.push(Box::new(format!("{c}.%")));
        }
        sql.push(')');
    }

    sql.push_str(if use_fts {
        " ORDER BY rank"
    } else {
        " ORDER BY s.qualified"
    });
    sql.push_str(&limit_clause);

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let row_to_hit = |row: &rusqlite::Row<'_>| -> rusqlite::Result<FindSymbolHit> {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        let qualified: String = row.get(2)?;
        let kind_str: String = row.get(3)?;
        let signature: Option<String> = row.get(4)?;
        let source_str: String = row.get(5)?;
        let path: String = row.get(6)?;
        let line: i64 = row.get(7)?;
        Ok(FindSymbolHit {
            id,
            qualified,
            name,
            kind: symbol_kind_from_db(&kind_str),
            repo: repo_alias.to_string(),
            branch: branch.to_string(),
            location: format!("{repo_alias}:{branch}:{path}:{line}"),
            signature,
            source: parse_source_tier(&source_str),
        })
    };
    // De-dup within a snapshot when container chains share members
    // (rare; only meaningful when include_inherited returns a base
    // whose subclass also already lives in the snapshot).
    let mut seen_ids: BTreeSet<i64> = BTreeSet::new();
    let rows = stmt
        .query_map(param_refs.as_slice(), row_to_hit)?
        .collect::<rusqlite::Result<Vec<FindSymbolHit>>>()?
        .into_iter()
        .filter(|h| seen_ids.insert(h.id))
        .collect();
    Ok(rows)
}

/// Make a user-supplied string safe to pass to an FTS5 MATCH. FTS5
/// treats characters like `"`, `(`, `)`, `:`, `-` as operators; we
/// wrap each whitespace-separated token in quotes (escaping internal
/// quotes) and join with AND.
fn sanitize_fts_query(q: &str) -> String {
    let toks: Vec<String> = q
        .split_whitespace()
        .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
        .collect();
    if toks.is_empty() {
        "\"\"".to_string()
    } else {
        toks.join(" AND ")
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize_fts_query;

    #[test]
    fn sanitize_fts_query_quotes_tokens() {
        assert_eq!(sanitize_fts_query("hello world"), r#""hello" AND "world""#);
        assert_eq!(sanitize_fts_query(""), r#""""#);
        assert_eq!(sanitize_fts_query(r#"a"b"#), r#""a""b""#);
    }
}
