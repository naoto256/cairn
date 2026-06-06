use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};

/// One impl-edge hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImplHit {
    pub type_qualified: String,
    pub interface_qualified: Option<String>,
    pub kind: String,
    pub path: String,
    pub line: u32,
}

/// Filters for `find_impls`. Either `interface_qualified` or
/// `type_qualified` must be set; the other side is the open end of
/// the query.
#[derive(Debug, Clone, Default)]
pub struct FindImplsArgs {
    /// `"What implements Display?"` — matches `interface_qualified`.
    pub interface_qualified: Option<String>,
    /// `"What does Foo implement?"` — matches `type_qualified`.
    pub type_qualified: Option<String>,
    pub limit: Option<u32>,
}

/// List impl edges visible from `anchor`, filtered by either the
/// trait side or the type side.
///
/// # Errors
/// `Error::InvalidArgument` when neither filter is set or the anchor
/// doesn't resolve. SQLite errors otherwise.
pub fn find_impls(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindImplsArgs,
) -> Result<Vec<ImplHit>> {
    let by_iface = args
        .interface_qualified
        .as_deref()
        .is_some_and(|s| !s.is_empty());
    let by_type = args
        .type_qualified
        .as_deref()
        .is_some_and(|s| !s.is_empty());
    if !by_iface && !by_type {
        return Err(crate::Error::InvalidArgument(
            "find_impls: one of `trait` / `type` must be supplied".into(),
        ));
    }
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;
    let limit = args.limit.unwrap_or(100).max(1);

    let mut sql = String::from(
        "SELECT i.type_qualified, i.interface_qualified, i.kind, me.path, i.line
           FROM implementations i
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = i.blob_sha
          WHERE 1=1",
    );
    let mut bound: Vec<Box<dyn ToSql>> = vec![Box::new(manifest_id.0)];
    if let Some(name) = args.interface_qualified.as_deref()
        && !name.is_empty()
    {
        sql.push_str(" AND i.interface_qualified = ?");
        bound.push(Box::new(name.to_string()));
    }
    if let Some(name) = args.type_qualified.as_deref()
        && !name.is_empty()
    {
        sql.push_str(" AND i.type_qualified = ?");
        bound.push(Box::new(name.to_string()));
    }
    sql.push_str(" ORDER BY me.path, i.line");
    sql.push_str(&format!(" LIMIT {limit}"));

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let rows: rusqlite::Result<Vec<ImplHit>> = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(ImplHit {
                type_qualified: row.get(0)?,
                interface_qualified: row.get(1)?,
                kind: row.get(2)?,
                path: row.get(3)?,
                line: u32::try_from(row.get::<_, i64>(4)?).unwrap_or(0),
            })
        })?
        .collect();
    Ok(rows?)
}
