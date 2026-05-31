//! `find_imports` — `use` statements across an indexed repo.
//!
//! Returns one hit per imported name. With `file = None` enumerates
//! every import in the (filtered) snapshot; pass `file` to narrow to
//! a single file's dependency surface.

use cairn_proto::methods::{ImportHit, ImportsArgs, ImportsResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, completeness_from_targets, parse_params};
use crate::{Error, Result};

pub struct FindImports;

#[async_trait::async_trait]
impl DataMethod for FindImports {
    fn name(&self) -> &'static str {
        "find_imports"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: ImportsArgs = parse_params(params)?;
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
        let completeness = completeness_from_targets(&targets);
        let limit = args.limit.unwrap_or(200);
        let file_filter = args.file.clone();
        let hits = tokio::task::spawn_blocking(move || {
            let mut all = Vec::new();
            for t in &targets {
                let rows = imports_in_db(&t.db_path, &t.branch, file_filter.as_deref(), limit)?;
                all.extend(rows);
            }
            all.truncate(limit as usize);
            Ok::<_, Error>(all)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("imports task panicked: {e}")))??;
        Ok(serde_json::to_value(ImportsResult {
            items: hits,
            completeness,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(FindImports);

fn imports_in_db(
    db_path: &std::path::Path,
    branch: &str,
    file_filter: Option<&str>,
    limit: u32,
) -> Result<Vec<ImportHit>> {
    let conn = crate::data_db::open(db_path)?;
    let mut sql = String::from(
        "SELECT f.path, im.to_module, im.imported, im.alias, im.is_reexport, im.line
           FROM imports im
           JOIN files   f ON f.id = im.from_file_id",
    );
    let mut bound: Vec<String> = Vec::new();
    if let Some(f) = file_filter {
        sql.push_str(" WHERE f.path = ?1");
        bound.push(f.to_string());
    }
    sql.push_str(" ORDER BY f.path, im.line");
    sql.push_str(&format!(" LIMIT {}", limit.max(1)));
    let mut stmt = conn.prepare(&sql)?;
    let row_to_hit = |row: &rusqlite::Row<'_>| -> rusqlite::Result<ImportHit> {
        let file: String = row.get(0)?;
        let to_module: String = row.get(1)?;
        let imported: Option<String> = row.get(2)?;
        let alias: Option<String> = row.get(3)?;
        let is_reexport: i64 = row.get(4)?;
        let line: i64 = row.get(5)?;
        Ok(ImportHit {
            file,
            to_module,
            imported,
            alias,
            is_reexport: is_reexport != 0,
            branch: branch.to_string(),
            line: u32::try_from(line).unwrap_or(0),
        })
    };
    let params_dyn: Vec<&dyn rusqlite::ToSql> =
        bound.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let rows: Vec<ImportHit> = stmt
        .query_map(params_dyn.as_slice(), row_to_hit)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
