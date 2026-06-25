//! `find_imports` — list import edges visible from an anchor, with the
//! resolution-layer LEFT JOIN that Stage 1 of the Tier-2.5 imports work
//! introduced.
//!
//! Mirrors the find_impls Phase 4 pattern verbatim: the SQL LEFT JOINs
//! `imports` against `resolutions` on the shared
//! `(blob_sha, parser_id, byte_start, byte_end)` tuple, picks the
//! highest-ranked source via `ROW_NUMBER() OVER (PARTITION BY …)`
//! (there is no UNIQUE on `resolutions`, so multiple writers can land
//! on the same site), and surfaces a `kind_source` provenance string.
//! When no resolution covers the site — either because no resolver
//! has run, or because the row pre-dates schema v9 and `byte_start` /
//! `byte_end` are NULL — `kind_source` falls back to
//! [`crate::query::KIND_SOURCE_FACT`] (`"tier2-fact"`).

use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::query::KIND_SOURCE_FACT;
use crate::workspace_analyzer::source_rank_case_sql;

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
    /// Provenance for this import-site resolution. Either a
    /// resolution-layer `source` string (e.g.
    /// `"tier25-ruby-resolver"`) when a Tier-2.5+ resolver
    /// pinned the site, or [`KIND_SOURCE_FACT`] (`"tier2-fact"`) when
    /// the bare `imports` row was used as fallback.
    pub kind_source: String,
    /// Repo-relative path of the workspace file the import resolved
    /// to (v10+). `Some("src/db.js")` when `require_graph` /
    /// equivalent pinned the import to a workspace-internal file;
    /// `None` for bare specifiers (`require 'rake'`,
    /// `import 'lodash'`), node:builtin imports, externals, and any
    /// site that fell back to `tier2-fact`.
    pub target_path: Option<String>,
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

    // Best-resolution CTE mirrors find_impls.rs verbatim: rank
    // resolutions on the same site (no UNIQUE in the table, so multiple
    // writers can land on one (blob, parser, byte_range) tuple) by
    // `source_rank_case_sql`, then keep only `rn = 1`. The
    // `kind = 'import'` filter excludes the type / call sites that
    // share the same byte-range key space in `resolutions`.
    let source_rank = source_rank_case_sql("source");
    let mut sql = format!(
        "WITH best_resolution AS (
             SELECT site_blob_sha, site_parser_id,
                    site_byte_start, site_byte_end,
                    source, target_path,
                    ROW_NUMBER() OVER (
                        PARTITION BY site_blob_sha, site_parser_id,
                                     site_byte_start, site_byte_end
                        ORDER BY
                            CASE WHEN manifest_id = ?1 THEN 0 ELSE 1 END,
                            {source_rank}, id
                    ) AS rn
               FROM resolutions
              WHERE kind = 'import'
                AND (manifest_id = ?1 OR manifest_id IS NULL)
         )
         SELECT me.path, i.to_module, i.imported, i.alias, i.is_reexport,
                i.line, i.parser_id,
                CASE WHEN r.source IS NOT NULL THEN r.source ELSE '{KIND_SOURCE_FACT}' END
                    AS kind_source,
                r.target_path AS target_path
           FROM imports i
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = i.blob_sha
           LEFT JOIN best_resolution r
             ON r.site_blob_sha = i.blob_sha
            AND r.site_parser_id = i.parser_id
            AND r.site_byte_start = i.byte_start
            AND r.site_byte_end = i.byte_end
            AND r.rn = 1
          WHERE 1=1"
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
                kind_source: row.get(7)?,
                target_path: row.get(8)?,
            })
        })?
        .collect();
    Ok(rows?)
}
