//! `find_references` — references either way: callers of a symbol
//! (`direction = incoming`, default) or what a symbol references
//! (`direction = outgoing`, "callees").
//!
//! Reads the Tier-2 `refs` table populated by the syn body-visitor.
//! For incoming, a `symbol` containing `::` is matched against
//! `target_qualified` first and falls back to `target_name`. For
//! outgoing, `symbol` is matched against the enclosing symbol's
//! qualified name (via `refs.enclosing_id → symbols.qualified`).
//! Cross-branch by default.

use cairn_proto::Completeness;
use cairn_proto::common::RefKind;
use cairn_proto::methods::{
    FindReferenceHit, FindReferencesArgs, FindReferencesResult, ReferenceDirection,
};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, completeness_from_targets, parse_params};
use crate::{Error, Result};

pub struct FindReferences;

#[async_trait::async_trait]
impl DataMethod for FindReferences {
    fn name(&self) -> &'static str {
        "find_references"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: FindReferencesArgs = parse_params(params)?;
        if args.symbol.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "find_references: `symbol` must be non-empty".into(),
            ));
        }
        let repo_alias = args.repo.clone();
        let targets = ctx
            .snapshot_targets(&repo_alias, args.branch.as_deref())
            .await?;
        if targets.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "no snapshot matches repo=`{repo_alias}`{}",
                args.branch
                    .as_deref()
                    .map(|b| format!(" branch=`{b}`"))
                    .unwrap_or_default()
            )));
        }
        // Semantic-tier baseline: any syntactic-only snapshot ⇒ Partial
        // (computed before `targets` is consumed by the blocking task).
        // The truncation axis is layered on top below. NOTE: a finer,
        // resolution-precision axis is still unwired — even on a
        // semantic snapshot, method-call hits (`foo.bar()`) carry a
        // null `target_qualified` because receiver-type resolution
        // needs Tier-3 (rust-analyzer, roadmap 0.4.0). Tagging those
        // as a distinct partial reason is deferred to that work
        // (MissingTier has no "resolved" variant yet).
        let semantic_gap = completeness_from_targets(&targets);
        let limit = args.limit.unwrap_or(100);
        let symbol = args.symbol.clone();
        let kind_filter = args.kind;
        let direction = args.direction;
        let repo_for_hits = repo_alias.clone();
        let hits = tokio::task::spawn_blocking(move || {
            let mut all = Vec::new();
            for t in &targets {
                let rows = references_in_db(
                    &t.db_path,
                    &repo_for_hits,
                    &t.branch,
                    &symbol,
                    kind_filter,
                    direction,
                    // Fetch limit+1 per snapshot so the dispatcher can
                    // detect truncation honestly (see find_symbols).
                    limit.saturating_add(1),
                )?;
                all.extend(rows);
            }
            Ok::<_, Error>(all)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("find_references task panicked: {e}")))??;

        // Two independent partial reasons:
        // 1. Truncation (`limit` was binding) — captured here from the
        //    pre-truncation total.
        // 2. Semantic-tier gap (a snapshot was syntactic-only, so
        //    references from it may be missing) — already computed by
        //    `completeness_from_targets` above into `semantic_gap`.
        // Truncation is the more actionable signal for the caller
        // (raise the cap), so it wins when both apply.
        let total = hits.len();
        let mut hits = hits;
        let truncated = hits.len() > limit as usize;
        hits.truncate(limit as usize);

        let completeness = if truncated {
            Completeness::partial_truncated(format!(
                "results capped at limit={limit} (had {total} matches; raise `limit` to see more)"
            ))
        } else {
            semantic_gap
        };

        Ok(serde_json::to_value(FindReferencesResult {
            items: hits,
            completeness,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindReferences);

fn references_in_db(
    db_path: &std::path::Path,
    repo_alias: &str,
    branch: &str,
    symbol: &str,
    kind_filter: Option<RefKind>,
    direction: ReferenceDirection,
    limit: u32,
) -> Result<Vec<FindReferenceHit>> {
    let conn = crate::data_db::open(db_path)?;
    let kind_str = kind_filter.map(ref_kind_to_db);

    let run = |column_path: &str, value: &str| -> Result<Vec<FindReferenceHit>> {
        // `column_path` is a fully-qualified SQL column reference
        // (`r.target_qualified` / `r.target_name` for incoming,
        // `enc.qualified` for outgoing). Encoding the SQL fragment at
        // the call site keeps the JOIN strategy uniform.
        let mut sql = String::from(
            "SELECT r.target_name, r.target_qualified, r.kind,
                    enc.qualified AS enclosing,
                    f.path, r.line
               FROM refs r
               JOIN files f ON f.id = r.file_id
               ",
        );
        // Outgoing matches against the enclosing symbol — INNER JOIN
        // so refs without enclosing_id are excluded. Incoming wants
        // every ref including top-level ones, so LEFT JOIN.
        let outgoing = matches!(direction, ReferenceDirection::Outgoing);
        sql.push_str(if outgoing {
            "JOIN symbols enc ON enc.id = r.enclosing_id\n"
        } else {
            "LEFT JOIN symbols enc ON enc.id = r.enclosing_id\n"
        });
        sql.push_str("              WHERE ");
        sql.push_str(column_path);
        sql.push_str(" = ?1");
        if kind_str.is_some() {
            sql.push_str(" AND r.kind = ?2");
        }
        sql.push_str(" ORDER BY f.path, r.line");
        sql.push_str(&format!(" LIMIT {}", limit.max(1)));

        let mut stmt = conn.prepare(&sql)?;
        let row_to_hit = |row: &rusqlite::Row<'_>| -> rusqlite::Result<FindReferenceHit> {
            let target_name: String = row.get(0)?;
            let target_qualified: Option<String> = row.get(1)?;
            let kind_db: String = row.get(2)?;
            let enclosing_qualified: Option<String> = row.get(3)?;
            let path: String = row.get(4)?;
            let line: i64 = row.get(5)?;
            Ok(FindReferenceHit {
                target_name,
                target_qualified,
                kind: db_to_ref_kind(&kind_db),
                enclosing_qualified,
                branch: branch.to_string(),
                location: format!("{repo_alias}:{branch}:{path}:{line}"),
            })
        };
        let rows: Vec<FindReferenceHit> = match kind_str {
            Some(k) => stmt
                .query_map(rusqlite::params![value, k], row_to_hit)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            None => stmt
                .query_map(rusqlite::params![value], row_to_hit)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        };
        Ok(rows)
    };

    match direction {
        ReferenceDirection::Outgoing => {
            // The anchor is the enclosing container. Match its
            // qualified form (callers typically know what they're
            // asking about).
            run("enc.qualified", symbol)
        }
        ReferenceDirection::Incoming => {
            // A `::`-bearing token is more specific; try qualified
            // first and fall back to bare-name when nothing matches.
            // A bare name skips straight to the `target_name` index.
            // This split keeps the common case ("who calls `foo`?")
            // on the cheap path while still letting
            // `crate::module::foo` disambiguate when the user knows
            // the qualifier.
            let is_qualified = symbol.contains("::");
            if is_qualified {
                let strict = run("r.target_qualified", symbol)?;
                if !strict.is_empty() {
                    return Ok(strict);
                }
                let bare = symbol.rsplit("::").next().unwrap_or(symbol);
                run("r.target_name", bare)
            } else {
                run("r.target_name", symbol)
            }
        }
    }
}

fn ref_kind_to_db(k: RefKind) -> &'static str {
    match k {
        RefKind::Call => "call",
        RefKind::Type => "type",
        RefKind::Import => "import",
        RefKind::Instantiate => "instantiate",
        RefKind::Read => "read",
        RefKind::Write => "write",
        RefKind::Override => "override",
        RefKind::MacroInvoke => "macro_invoke",
        RefKind::Annotation => "annotation",
    }
}

fn db_to_ref_kind(s: &str) -> RefKind {
    match s {
        "type" => RefKind::Type,
        "import" => RefKind::Import,
        "instantiate" => RefKind::Instantiate,
        "read" => RefKind::Read,
        "write" => RefKind::Write,
        "override" => RefKind::Override,
        "macro_invoke" => RefKind::MacroInvoke,
        "annotation" => RefKind::Annotation,
        // `call` and `method_call` both surface as `Call` on the wire
        // — the proto-level enum doesn't (yet) distinguish them and
        // most consumers want them merged anyway.
        _ => RefKind::Call,
    }
}
