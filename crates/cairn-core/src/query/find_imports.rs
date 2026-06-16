use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};

/// One import hit. Mirrors `cairn_proto::methods::ImportHit` minus
/// the wire envelope (repo / branch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportHit {
    pub file: String,
    pub to_module: String,
    pub imported: Option<String>,
    pub alias: Option<String>,
    pub is_reexport: bool,
    pub line: u32,
    pub parser_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct FindImportsArgs {
    /// File path (relative to repo root) to restrict to. `None`
    /// returns every import in the snapshot.
    pub file: Option<String>,
    pub limit: Option<u32>,
}

/// List the imports visible from `anchor`, optionally restricted to
/// one file.
///
/// # Errors
/// `Error::InvalidArgument` if the anchor doesn't resolve; SQLite
/// errors otherwise.
pub fn find_imports(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindImportsArgs,
) -> Result<Vec<ImportHit>> {
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;
    let limit = args.limit.unwrap_or(200).max(1);

    let mut sql = String::from(
        "SELECT me.path, i.to_module, i.imported, i.alias, i.is_reexport, i.line, i.parser_id
           FROM imports i
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = i.blob_sha
          WHERE 1=1",
    );
    let mut bound: Vec<Box<dyn ToSql>> = vec![Box::new(manifest_id.0)];
    if let Some(file) = args.file.as_deref()
        && !file.is_empty()
    {
        sql.push_str(" AND me.path = ?");
        bound.push(Box::new(file.to_string()));
    }
    sql.push_str(" ORDER BY me.path, i.line");
    sql.push_str(&format!(" LIMIT {limit}"));

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let rows: rusqlite::Result<Vec<ImportHit>> = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(ImportHit {
                file: row.get(0)?,
                to_module: row.get(1)?,
                imported: row.get(2)?,
                alias: row.get(3)?,
                is_reexport: row.get::<_, i64>(4)? != 0,
                line: u32::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
                parser_id: row.get(6)?,
            })
        })?
        .collect();
    Ok(rows?)
}
