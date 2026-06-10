use cairn_proto::common::SymbolKind;
use rusqlite::{Connection, OptionalExtension, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::symbol_kind_from_str;

/// One outline entry for a single file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutlineItem {
    pub file: Option<String>,
    pub name: String,
    pub qualified: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub doc: Option<String>,
    pub line: u32,
}

/// Return every symbol in `file` from the manifest at `anchor`, in
/// line order. `parser_id` filters to one backend's output — typically
/// the daemon picks it from the file extension and passes it in.
///
/// # Errors
/// `Error::AnchorNotFound` when the anchor doesn't resolve; SQLite
/// errors otherwise.
pub fn get_outline(
    conn: &Connection,
    anchor: &AnchorName,
    file: &str,
    parser_id: Option<&str>,
) -> Result<Vec<OutlineItem>> {
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;

    let blob_sha: Option<String> = conn
        .query_row(
            "SELECT blob_sha FROM manifest_entries
             WHERE manifest_id = ?1 AND path = ?2",
            rusqlite::params![manifest_id.0, file],
            |r| r.get(0),
        )
        .optional()?;
    let Some(blob_sha) = blob_sha else {
        return Ok(Vec::new());
    };

    let mut sql = String::from(
        "SELECT name, qualified, kind, signature, doc, line_start
           FROM symbols
          WHERE blob_sha = ?1",
    );
    let mut bound: Vec<Box<dyn ToSql>> = vec![Box::new(blob_sha)];
    if let Some(pid) = parser_id {
        sql.push_str(" AND parser_id = ?");
        bound.push(Box::new(pid.to_string()));
    }
    sql.push_str(" ORDER BY line_start");

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let rows: rusqlite::Result<Vec<OutlineItem>> = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(OutlineItem {
                file: None,
                name: row.get(0)?,
                qualified: row.get(1)?,
                kind: symbol_kind_from_str(&row.get::<_, String>(2)?),
                signature: row.get(3)?,
                doc: row.get(4)?,
                line: u32::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
            })
        })?
        .collect();
    Ok(rows?)
}

/// Optional filters layered on top of `get_outline_under_path`'s
/// prefix scan. `kind` is byte-equal against `symbols.kind`;
/// `max_depth` caps the directory hops the file path may take past
/// `path_prefix` (depth 1 = files directly under the prefix).
#[derive(Debug, Clone, Default)]
pub struct OutlineFilter {
    pub kind: Option<SymbolKind>,
    pub max_depth: Option<u32>,
}

/// Return every symbol from files under `path_prefix`, sorted by
/// file then line. The prefix is byte-level and repo-root-relative,
/// matching `find_symbols.path` semantics.
///
/// # Errors
/// `Error::AnchorNotFound` when the anchor doesn't resolve; SQLite
/// errors otherwise.
pub fn get_outline_under_path(
    conn: &Connection,
    anchor: &AnchorName,
    path_prefix: &str,
    parser_id: Option<&str>,
    limit: u32,
    filter: &OutlineFilter,
) -> Result<Vec<OutlineItem>> {
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;
    let limit = limit.max(1);

    let mut sql = String::from(
        "SELECT me.path, s.name, s.qualified, s.kind, s.signature, s.doc, s.line_start
          FROM manifest_entries me
           JOIN symbols s
             ON s.blob_sha = me.blob_sha
          WHERE me.manifest_id = ?1
            AND substr(me.path, 1, length(?2)) = ?2",
    );
    let mut bound: Vec<Box<dyn ToSql>> =
        vec![Box::new(manifest_id.0), Box::new(path_prefix.to_string())];
    if let Some(pid) = parser_id {
        sql.push_str(" AND s.parser_id = ?");
        bound.push(Box::new(pid.to_string()));
    }
    if let Some(kind) = filter.kind.as_ref() {
        sql.push_str(" AND s.kind = ?");
        bound.push(Box::new(crate::cas::kind_conv::symbol_kind_to_str(kind)));
    }
    if let Some(depth) = filter.max_depth {
        // Slashes in the file path *after* the prefix must be <= depth - 1.
        // (depth = 1 → no further slashes; depth = 2 → at most one, etc.)
        // We compare strings rather than counting in SQL: substring length
        // minus its '/'-stripped length is the slash count.
        sql.push_str(
            " AND length(substr(me.path, length(?2) + 1))
               - length(replace(substr(me.path, length(?2) + 1), '/', '')) <= ?",
        );
        let allowed = depth.saturating_sub(1);
        bound.push(Box::new(i64::from(allowed)));
    }
    sql.push_str(" ORDER BY me.path, s.line_start");
    sql.push_str(" LIMIT ?");
    bound.push(Box::new(i64::from(limit)));

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let rows: rusqlite::Result<Vec<OutlineItem>> = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(OutlineItem {
                file: Some(row.get(0)?),
                name: row.get(1)?,
                qualified: row.get(2)?,
                kind: symbol_kind_from_str(&row.get::<_, String>(3)?),
                signature: row.get(4)?,
                doc: row.get(5)?,
                line: u32::try_from(row.get::<_, i64>(6)?).unwrap_or(0),
            })
        })?
        .collect();
    Ok(rows?)
}
