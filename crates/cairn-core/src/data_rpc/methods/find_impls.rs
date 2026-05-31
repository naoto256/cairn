//! `find_impls` — trait/impl edges across an indexed repo.
//!
//! Either side of the relation may be the filter: `trait=Foo` answers
//! "what implements Foo?"; `type=Bar` answers "what does Bar
//! implement?". At least one must be supplied. Both sides may be
//! combined to ask "does X implement Y?".

use cairn_proto::methods::{ImplHit, ImplsArgs, ImplsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, completeness_from_targets, parse_params};
use crate::{Error, Result};

pub struct FindImpls;

#[async_trait::async_trait]
impl DataMethod for FindImpls {
    fn name(&self) -> &'static str {
        "find_impls"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ImplsArgs = parse_params(params)?;
        if args.trait_.is_none() && args.type_.is_none() {
            return Err(Error::InvalidArgument(
                "impls: at least one of `trait` or `type` must be supplied".into(),
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
        // Compute completeness before `targets` is moved into the
        // blocking task: any syntactic-only snapshot means impl edges
        // may be missing for that branch.
        let completeness = completeness_from_targets(&targets);
        let limit = args.limit.unwrap_or(50);
        let trait_filter = args.trait_.clone();
        let type_filter = args.type_.clone();
        let repo_for_hits = repo_alias.clone();
        let hits = tokio::task::spawn_blocking(move || {
            let mut all = Vec::new();
            for t in &targets {
                let rows = impls_in_db(
                    &t.db_path,
                    &repo_for_hits,
                    &t.branch,
                    trait_filter.as_deref(),
                    type_filter.as_deref(),
                    limit,
                )?;
                all.extend(rows);
            }
            all.truncate(limit as usize);
            Ok::<_, Error>(all)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("impls task panicked: {e}")))??;
        Ok(serde_json::to_value(ImplsResult {
            items: hits,
            completeness,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImpls);

fn impls_in_db(
    db_path: &std::path::Path,
    repo_alias: &str,
    branch: &str,
    trait_filter: Option<&str>,
    type_filter: Option<&str>,
    limit: u32,
) -> Result<Vec<ImplHit>> {
    let conn = crate::data_db::open(db_path)?;
    // The `implementations` table records the **type-side** symbol id
    // and a free-text `interface_name` (resolved via best-effort
    // qualified-name lookup at index time). Join symbols+files for
    // the type so the hit can carry a `repo:branch:file:line`
    // location.
    let mut sql = String::from(
        "SELECT i.kind, i.interface_name, s.qualified AS type_qualified,
                f.path, s.line_start
           FROM implementations i
           JOIN symbols s ON s.id = i.type_id
           JOIN files   f ON f.id = s.file_id
          WHERE 1=1",
    );
    let mut bound: Vec<String> = Vec::new();
    if let Some(t) = trait_filter {
        sql.push_str(&format!(" AND i.interface_name = ?{}", bound.len() + 1));
        bound.push(t.to_string());
    }
    if let Some(t) = type_filter {
        sql.push_str(&format!(" AND s.qualified = ?{}", bound.len() + 1));
        bound.push(t.to_string());
    }
    sql.push_str(&format!(" LIMIT {}", limit.max(1)));

    let mut stmt = conn.prepare(&sql)?;
    let row_to_hit = |row: &rusqlite::Row<'_>| -> rusqlite::Result<ImplHit> {
        let kind: String = row.get(0)?;
        let iface: String = row.get(1)?;
        let type_qualified: String = row.get(2)?;
        let path: String = row.get(3)?;
        let line: i64 = row.get(4)?;
        let interface_qualified = if kind == "inherent" {
            None
        } else {
            Some(iface)
        };
        Ok(ImplHit {
            type_qualified,
            interface_qualified,
            kind,
            branch: branch.to_string(),
            location: format!("{repo_alias}:{branch}:{path}:{line}"),
        })
    };
    let params_dyn: Vec<&dyn rusqlite::ToSql> =
        bound.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let rows: Vec<ImplHit> = stmt
        .query_map(params_dyn.as_slice(), row_to_hit)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
