use cairn_proto::common::SymbolKind;
use rusqlite::{Connection, OptionalExtension, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::symbol_kind_from_str;

/// Row data needed to render a `get_symbol_source` response. The
/// caller pulls the actual bytes from disk or git based on
/// `blob_sha` + `byte_start..byte_end`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolSourceRow {
    pub qualified: String,
    pub name: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub doc: Option<String>,
    pub path: String,
    pub blob_sha: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub line_start: u32,
    pub line_end: u32,
    pub parser_id: String,
}

/// Look up a symbol by its qualified name in the manifest at `anchor`
/// and return the metadata needed to materialise its source span.
/// `file_filter` constrains the search to one path. Returns `None`
/// when nothing matches.
///
/// # Errors
/// `Error::AnchorNotFound` when the anchor doesn't resolve; SQLite
/// errors otherwise.
pub fn get_symbol_source_row(
    conn: &Connection,
    anchor: &AnchorName,
    qualified: &str,
    file_filter: Option<&str>,
) -> Result<Option<SymbolSourceRow>> {
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;

    let mut sql = String::from(
        "SELECT s.name, s.kind, s.signature, s.doc,
                s.byte_start, s.byte_end, s.line_start, s.line_end,
                me.path, s.blob_sha, s.parser_id
           FROM symbols s
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = s.blob_sha
          WHERE s.qualified = ?2",
    );
    let mut bound: Vec<Box<dyn ToSql>> =
        vec![Box::new(manifest_id.0), Box::new(qualified.to_string())];
    if let Some(f) = file_filter {
        sql.push_str(" AND me.path = ?");
        bound.push(Box::new(f.to_string()));
    }
    sql.push_str(" LIMIT 1");

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let row = stmt
        .query_row(param_refs.as_slice(), |r| {
            Ok(SymbolSourceRow {
                qualified: qualified.to_string(),
                name: r.get(0)?,
                kind: symbol_kind_from_str(&r.get::<_, String>(1)?),
                signature: r.get(2)?,
                doc: r.get(3)?,
                byte_start: usize::try_from(r.get::<_, i64>(4)?).unwrap_or(0),
                byte_end: usize::try_from(r.get::<_, i64>(5)?).unwrap_or(0),
                line_start: u32::try_from(r.get::<_, i64>(6)?).unwrap_or(0),
                line_end: u32::try_from(r.get::<_, i64>(7)?).unwrap_or(0),
                path: r.get(8)?,
                blob_sha: r.get(9)?,
                parser_id: r.get(10)?,
            })
        })
        .optional()?;
    Ok(row)
}
