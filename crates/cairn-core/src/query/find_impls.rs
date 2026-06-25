//! Type-relation edge queries — `find_subtypes` (who implements /
//! extends `name`?) and `find_supertypes` (what does `name` extend
//! / implement?). Both walk the `implementations` table; they differ
//! only in which side of the edge they pin.
//!
//! Phase 4 of the Tier-2.5 prep work routes the `kind` field through
//! the resolution layer when a matching row exists: the SQL LEFT JOINs
//! `implementations` against `resolutions` on the shared
//! `(blob, parser_id, byte_range)` tuple and surfaces
//! `COALESCE(resolutions.semantic_kind, implementations.kind)` plus a
//! `kind_source` provenance string. The wire `kind` value is unchanged
//! when there is no resolution row (fact-layer fallback,
//! `kind_source = "tier2-fact"`); when one exists the more
//! authoritative semantic label wins. Multiple resolutions on the same
//! site are ranked by [`source_rank_case_sql`] (tier3 > tier25 >
//! tier2-direct), keeping precedence stable across writers.

use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::workspace_analyzer::source_rank_case_sql;

/// Provenance string used in [`ImplHit::kind_source`] when no
/// resolution row covers the site, so the `kind` was read from the
/// fact layer (`implementations.kind`).
pub const KIND_SOURCE_FACT: &str = "tier2-fact";

/// One impl-edge hit. Shared by both directions; only which side the
/// caller pinned changes between subtypes and supertypes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImplHit {
    pub type_qualified: String,
    pub interface_qualified: Option<String>,
    pub kind: String,
    /// Provenance for [`Self::kind`]. Either a resolution-layer
    /// `source` string (e.g. `"tier2-direct-java"`,
    /// `"tier25-py-resolver"`, `"tier3-pyright-lsp"`) when the kind
    /// came through the `resolutions` table, or
    /// [`KIND_SOURCE_FACT`] (`"tier2-fact"`) when the Tier-2
    /// `implementations.kind` was used as fallback.
    pub kind_source: String,
    /// Repo-relative path of the workspace file the resolved
    /// supertype lives in (v10+). `Some("src/Foo.java")` when the
    /// resolver pinned the edge to a workspace-internal definition
    /// (same row that promoted `kind_source` off `tier2-fact`);
    /// `None` for unresolved sites and supertypes that live outside
    /// the indexed workspace.
    pub target_path: Option<String>,
    pub path: String,
    pub line: u32,
    pub parser_id: String,
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

    // The resolution-layer join is filtered to `kind = 'type'` because
    // `implementations` only ever stores type-relation edges; the
    // `resolutions` table mixes type / call / import sites under the
    // same `(blob, byte_range)` key space.
    //
    // When a single site has multiple resolution rows (e.g. tier3 + a
    // tier2-direct fallback from a stale rebuild), we pick the one
    // with the lowest source rank via ROW_NUMBER() in a CTE rather
    // than a correlated subquery so the JOIN stays index-friendly.
    let source_rank = source_rank_case_sql("source");
    let mut sql = format!(
        "WITH best_resolution AS (
             SELECT site_blob_sha, site_parser_id,
                    site_byte_start, site_byte_end,
                    semantic_kind, source, target_path,
                    ROW_NUMBER() OVER (
                        PARTITION BY site_blob_sha, site_parser_id,
                                     site_byte_start, site_byte_end
                        ORDER BY {source_rank}, id
                    ) AS rn
               FROM resolutions
              WHERE kind = 'type'
         )
         SELECT i.type_qualified,
                i.interface_qualified,
                COALESCE(r.semantic_kind, i.kind) AS kind,
                CASE WHEN r.source IS NOT NULL THEN r.source ELSE '{KIND_SOURCE_FACT}' END
                    AS kind_source,
                r.target_path AS target_path,
                me.path,
                i.line,
                i.parser_id
           FROM implementations i
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = i.blob_sha
           LEFT JOIN best_resolution r
             ON r.site_blob_sha = i.blob_sha
            AND r.site_parser_id = i.parser_id
            AND r.site_byte_start = i.interface_byte_start
            AND r.site_byte_end = i.interface_byte_end
            AND r.rn = 1
          WHERE "
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
                kind_source: row.get(3)?,
                target_path: row.get(4)?,
                path: row.get(5)?,
                line: u32::try_from(row.get::<_, i64>(6)?).unwrap_or(0),
                parser_id: row.get(7)?,
            })
        })?
        .collect();
    Ok(rows?)
}
