use cairn_lang_api::Visibility;
use cairn_proto::common::{SourceTier, SymbolKind};
use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::{symbol_kind_from_str, visibility_from_str};
use crate::manifest::ManifestId;

/// One symbol hit. Mirrors the public-fact subset of
/// `cairn_proto::methods::FindSymbolHit` but skips the wire-format
/// envelope (repo / branch / location) so callers compose them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolHit {
    pub id: i64,
    pub name: String,
    pub qualified: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub visibility: Option<Visibility>,
    pub path: String,
    pub line: u32,
    pub blob_sha: String,
    pub parser_id: String,
    pub language: Option<String>,
    pub source_tier: SourceTier,
}

/// Filters for `find_symbols`. All optional; the caller must supply
/// at least one of `query` / `kind` / `container` / `path_prefix` to
/// avoid dumping the whole index.
#[derive(Debug, Clone, Default)]
pub struct FindSymbolsArgs {
    pub query: Option<String>,
    pub fuzzy: bool,
    pub kind: Option<String>,
    pub container: Option<String>,
    pub path_prefix: Option<String>,
    pub limit: Option<u32>,
}

/// Query the symbols visible from `anchor`. `anchor` resolves to one
/// manifest; the join scopes hits to blobs that appear in that
/// manifest.
///
/// # Errors
/// Returns [`crate::Error::InvalidArgument`] when no filter is set or
/// the anchor does not resolve. SQLite errors otherwise.
pub fn find_symbols(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindSymbolsArgs,
) -> Result<Vec<SymbolHit>> {
    let any_filter = args.query.as_deref().is_some_and(|q| !q.is_empty())
        || args.kind.as_deref().is_some_and(|k| !k.is_empty())
        || args.container.as_deref().is_some_and(|c| !c.is_empty())
        || args.path_prefix.as_deref().is_some_and(|p| !p.is_empty());
    if !any_filter {
        return Err(crate::Error::InvalidArgument(
            "find_symbols: at least one of `query`, `kind`, `container`, or `path_prefix` \
             must be set"
                .to_string(),
        ));
    }

    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;

    run_find_symbols(conn, manifest_id, args)
}

fn run_find_symbols(
    conn: &Connection,
    manifest_id: ManifestId,
    args: &FindSymbolsArgs,
) -> Result<Vec<SymbolHit>> {
    let limit = args.limit.unwrap_or(50).max(1);

    // Base query: pull symbols whose blob_sha is in the manifest's
    // entry set, joined to manifest_entries so we can return the
    // file path the blob was mounted at.
    let mut sql = String::from(
        "SELECT s.id, s.name, s.qualified, s.kind, s.signature, s.visibility,
                 me.path, s.line_start, s.blob_sha, s.parser_id,
                 CASE
                   WHEN b.analyzer_id IS NULL THEN NULL
                   WHEN b.parser_id LIKE 'tree-sitter-%@%' THEN
                     substr(
                       substr(b.parser_id, 13),
                       1,
                       instr(substr(b.parser_id, 13), '@') - 1
                     )
                   WHEN b.parser_id LIKE 'tree-sitter-%' THEN substr(b.parser_id, 13)
                   ELSE b.parser_id
                 END AS language,
                 b.analyzer_id IS NOT NULL
           FROM symbols s
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = s.blob_sha
           JOIN blobs b
             ON b.blob_sha = s.blob_sha
          WHERE 1=1",
    );
    let mut bound: Vec<Box<dyn ToSql>> = vec![Box::new(manifest_id.0)];

    if let Some(q) = args.query.as_deref()
        && !q.is_empty()
    {
        if args.fuzzy {
            sql.push_str(
                " AND s.id IN (
                      SELECT rowid FROM symbols_fts
                       WHERE symbols_fts MATCH ?
                  )",
            );
            bound.push(Box::new(q.to_string()));
        } else {
            sql.push_str(" AND (s.name = ?  OR s.qualified = ?)");
            bound.push(Box::new(q.to_string()));
            bound.push(Box::new(q.to_string()));
        }
    }
    if let Some(k) = args.kind.as_deref()
        && !k.is_empty()
    {
        sql.push_str(" AND s.kind = ?");
        bound.push(Box::new(k.to_string()));
    }
    if let Some(c) = args.container.as_deref()
        && !c.is_empty()
    {
        sql.push_str(" AND (s.qualified LIKE ? OR s.qualified LIKE ?)");
        bound.push(Box::new(format!("{c}::%")));
        bound.push(Box::new(format!("{c}.%")));
    }
    if let Some(p) = args.path_prefix.as_deref()
        && !p.is_empty()
    {
        sql.push_str(" AND me.path LIKE ?");
        bound.push(Box::new(format!("{p}%")));
    }
    sql.push_str(" ORDER BY language IS NULL, language, me.path, s.line_start LIMIT ?");
    bound.push(Box::new(i64::from(limit)));

    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows: rusqlite::Result<Vec<SymbolHit>> =
        stmt.query_map(param_refs.as_slice(), row_to_hit)?.collect();
    Ok(rows?)
}

fn row_to_hit(row: &rusqlite::Row<'_>) -> rusqlite::Result<SymbolHit> {
    Ok(SymbolHit {
        id: row.get(0)?,
        name: row.get(1)?,
        qualified: row.get(2)?,
        kind: symbol_kind_from_str(&row.get::<_, String>(3)?),
        signature: row.get(4)?,
        visibility: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .map(visibility_from_str),
        path: row.get(6)?,
        line: u32::try_from(row.get::<_, i64>(7)?).unwrap_or(0),
        blob_sha: row.get(8)?,
        parser_id: row.get(9)?,
        language: row.get(10)?,
        source_tier: if row.get::<_, bool>(11)? {
            SourceTier::Semantic
        } else {
            SourceTier::Syntactic
        },
    })
}
