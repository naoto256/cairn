//! Type-relation edge queries — `find_subtypes` (who implements /
//! extends `name`?) and `find_supertypes` (what does `name` extend
//! / implement?). Both walk the `implementations` table; they differ
//! only in which side of the edge they pin.

use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};

/// One impl-edge hit. Shared by both directions; only which side the
/// caller pinned changes between subtypes and supertypes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImplHit {
    pub type_qualified: String,
    pub interface_qualified: Option<String>,
    pub kind: String,
    pub path: String,
    pub line: u32,
}

/// Filters for `find_subtypes`.
#[derive(Debug, Clone, Default)]
pub struct FindSubtypesArgs {
    /// The supertype / trait / interface. Match returns rows whose
    /// `interface_qualified` equals this name — i.e. every type that
    /// implements / extends / mixes in `name`.
    pub name: String,
    pub limit: Option<u32>,
}

/// Filters for `find_supertypes`.
#[derive(Debug, Clone, Default)]
pub struct FindSupertypesArgs {
    /// The subtype. Match returns rows whose `type_qualified` equals
    /// this name — i.e. every base / trait / interface / mixin
    /// `name` extends or implements.
    pub name: String,
    pub limit: Option<u32>,
}

/// "Who implements / extends `name`?"
///
/// # Errors
/// `Error::InvalidArgument` when `name` is empty,
/// `Error::AnchorNotFound` when the anchor doesn't resolve.
pub fn find_subtypes(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindSubtypesArgs,
) -> Result<Vec<ImplHit>> {
    if args.name.trim().is_empty() {
        return Err(crate::Error::InvalidArgument(
            "find_subtypes: `name` must be non-empty".into(),
        ));
    }
    run(
        conn,
        anchor,
        "i.interface_qualified",
        &args.name,
        args.limit,
    )
}

/// "What does `name` extend / implement?"
///
/// # Errors
/// `Error::InvalidArgument` when `name` is empty,
/// `Error::AnchorNotFound` when the anchor doesn't resolve.
pub fn find_supertypes(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindSupertypesArgs,
) -> Result<Vec<ImplHit>> {
    if args.name.trim().is_empty() {
        return Err(crate::Error::InvalidArgument(
            "find_supertypes: `name` must be non-empty".into(),
        ));
    }
    run(conn, anchor, "i.type_qualified", &args.name, args.limit)
}

fn run(
    conn: &Connection,
    anchor: &AnchorName,
    where_col: &str,
    value: &str,
    limit: Option<u32>,
) -> Result<Vec<ImplHit>> {
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;
    let limit = limit.unwrap_or(100).max(1);

    let mut sql = String::from(
        "SELECT i.type_qualified, i.interface_qualified, i.kind, me.path, i.line
           FROM implementations i
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = i.blob_sha
          WHERE ",
    );
    sql.push_str(where_col);
    sql.push_str(" = ?2 ORDER BY me.path, i.line");
    sql.push_str(&format!(" LIMIT {limit}"));

    let bound: Vec<Box<dyn ToSql>> = vec![Box::new(manifest_id.0), Box::new(value.to_string())];
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
