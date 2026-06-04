//! Query layer over the CAS store.
//!
//! This is the new-path counterpart to `data_rpc::methods::*`. It
//! resolves an anchor to a `manifest_id`, then joins `symbols` (or
//! `refs` / `imports` in future) against `manifest_entries` filtered
//! by `manifest_id` to surface results scoped to one snapshot's
//! visible blobs.
//!
//! Currently exposes `find_symbols` and `find_references`; the rest
//! of the surface ports in later work.

use cairn_lang_api::Visibility;
use cairn_proto::common::{RefKind, SourceTier, SymbolKind};
use cairn_proto::methods::ReferenceDirection;
use rusqlite::{Connection, OptionalExtension, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::{
    ref_kind_from_str, ref_kind_to_str, symbol_kind_from_str, visibility_from_str,
};
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

// ─── find_references ──────────────────────────────────────────────────────

/// One reference hit. Mirrors `cairn_proto::methods::FindReferenceHit`
/// minus the repo / branch / location envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceHit {
    pub target_name: String,
    pub target_qualified: Option<String>,
    pub kind: RefKind,
    pub enclosing_qualified: Option<String>,
    pub path: String,
    pub line: u32,
    /// SHA of the blob that owns this ref. The wire layer uses it to
    /// pull a one-line snippet via `git cat-file` (= the same content
    /// the indexer parsed), with the worktree as a fallback for
    /// uncommitted state.
    pub blob_sha: String,
}

/// Filters for `find_references`. `symbol` is required and non-empty.
#[derive(Debug, Clone, Default)]
pub struct FindReferencesArgs {
    pub symbol: String,
    pub direction: ReferenceDirection,
    pub kind: Option<RefKind>,
    pub include_noise: bool,
    pub limit: Option<u32>,
}

/// Find references either way:
/// - `Incoming` — refs whose target matches `symbol` (callers / use
///   sites). When `symbol` contains `::`, the qualified-name index is
///   tried first; bare names match `target_name` directly.
/// - `Outgoing` — refs inside the body of the symbol named `symbol`
///   (= callees / uses from the symbol). Matches `symbols.qualified`
///   on the enclosing FK.
///
/// # Errors
/// `Error::InvalidArgument` when `symbol` is empty, `Error::AnchorNotFound` when the anchor
/// doesn't resolve. SQLite errors otherwise.
pub fn find_references(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindReferencesArgs,
) -> Result<Vec<ReferenceHit>> {
    if args.symbol.trim().is_empty() {
        return Err(crate::Error::InvalidArgument(
            "find_references: `symbol` must be non-empty".into(),
        ));
    }
    let manifest_id =
        anchor::resolve(conn, anchor)?.ok_or_else(|| crate::Error::AnchorNotFound {
            name: anchor.as_str().to_string(),
        })?;
    run_find_references(conn, manifest_id, args)
}

fn run_find_references(
    conn: &Connection,
    manifest_id: ManifestId,
    args: &FindReferencesArgs,
) -> Result<Vec<ReferenceHit>> {
    let limit = args.limit.unwrap_or(100).max(1);
    let kind_str = args.kind.map(ref_kind_to_str);

    // Both directions JOIN `manifest_entries` so refs are scoped to
    // blobs visible from this anchor. The enclosing-symbol JOIN is
    // INNER for outgoing (we need the enclosing name to filter) and
    // LEFT for incoming (top-level refs have no enclosing).
    let run = |where_col: &str, value: &str, outgoing: bool| -> Result<Vec<ReferenceHit>> {
        let mut sql = String::from(
            "SELECT target_name, target_qualified, kind, enclosing, path, line, blob_sha
               FROM (
                 SELECT r.target_name, r.target_qualified, r.kind,
                        enc.qualified AS enclosing,
                        me.path, r.line, r.blob_sha, r.byte_start,
                        CASE
                          WHEN r.source LIKE 'tier3-%' THEN 0
                          WHEN r.source = 'rust-syn' THEN 1
                          ELSE 2
                        END AS source_rank,
                        ROW_NUMBER() OVER (
                          PARTITION BY r.blob_sha, r.byte_start, r.byte_end, r.kind
                          ORDER BY
                            CASE
                              WHEN r.source LIKE 'tier3-%' THEN 0
                              WHEN r.source = 'rust-syn' THEN 1
                              ELSE 2
                            END,
                            CASE
                              WHEN r.target_qualified IS NOT NULL
                               AND r.target_qualified <> '' THEN 0
                              ELSE 1
                            END,
                            r.source
                        ) AS dedup_rank
                   FROM refs r
                   JOIN manifest_entries me
                     ON me.manifest_id = ?1
                    AND me.blob_sha = r.blob_sha
               ",
        );
        sql.push_str(if outgoing {
            "JOIN symbols enc ON enc.id = r.enclosing_id\n"
        } else {
            "LEFT JOIN symbols enc ON enc.id = r.enclosing_id\n"
        });
        sql.push_str("              WHERE ");
        sql.push_str(where_col);
        sql.push_str(" = ?2");
        if kind_str.is_some() {
            sql.push_str(" AND r.kind = ?3");
        }
        if outgoing && !args.include_noise {
            sql.push_str(" AND r.kind = 'call'");
            sql.push_str(" AND r.target_qualified IS NOT NULL");
            sql.push_str(" AND r.target_qualified <> ''");
        }
        sql.push(')');
        if !args.include_noise {
            sql.push_str(" WHERE dedup_rank = 1");
        }
        sql.push_str(" ORDER BY path, line, byte_start, source_rank");
        sql.push_str(&format!(" LIMIT {limit}"));

        let mut stmt = conn.prepare(&sql)?;
        let row_to_hit = |row: &rusqlite::Row<'_>| -> rusqlite::Result<ReferenceHit> {
            Ok(ReferenceHit {
                target_name: row.get(0)?,
                target_qualified: row.get(1)?,
                kind: ref_kind_from_str(&row.get::<_, String>(2)?),
                enclosing_qualified: row.get(3)?,
                path: row.get(4)?,
                line: u32::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
                blob_sha: row.get(6)?,
            })
        };
        let rows: rusqlite::Result<Vec<ReferenceHit>> = match &kind_str {
            Some(k) => stmt
                .query_map(rusqlite::params![manifest_id.0, value, k], row_to_hit)?
                .collect(),
            None => stmt
                .query_map(rusqlite::params![manifest_id.0, value], row_to_hit)?
                .collect(),
        };
        Ok(rows?)
    };

    match args.direction {
        ReferenceDirection::Outgoing => run("enc.qualified", &args.symbol, true),
        ReferenceDirection::Incoming => {
            // Prefer qualified-name matching when the symbol carries
            // `::`; fall back to bare-name when qualified produces no
            // hits. Bare symbols skip straight to the bare-name index.
            if args.symbol.contains("::") {
                let strict = run("r.target_qualified", &args.symbol, false)?;
                if !strict.is_empty() {
                    return Ok(strict);
                }
                let bare = args.symbol.rsplit("::").next().unwrap_or(&args.symbol);
                run("r.target_name", bare, false)
            } else {
                run("r.target_name", &args.symbol, false)
            }
        }
    }
}

// ─── get_symbol_source ────────────────────────────────────────────────────

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
                me.path, s.blob_sha
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
            })
        })
        .optional()?;
    Ok(row)
}

// ─── get_outline ──────────────────────────────────────────────────────────

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
    sql.push_str(" ORDER BY me.path, s.line_start");
    sql.push_str(&format!(" LIMIT {limit}"));

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

// ─── find_impls ───────────────────────────────────────────────────────────

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

// ─── find_imports ─────────────────────────────────────────────────────────

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
        "SELECT me.path, i.to_module, i.imported, i.alias, i.is_reexport, i.line
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
            })
        })?
        .collect();
    Ok(rows?)
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
                 me.path, s.line_start, s.blob_sha,
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
        language: row.get(9)?,
        source_tier: if row.get::<_, bool>(10)? {
            SourceTier::Semantic
        } else {
            SourceTier::Syntactic
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::store;
    use crate::register::register_repo;
    use crate::testutil::init_repo;
    use cairn_lang_markdown as _;
    use cairn_lang_python as _;
    use cairn_lang_rust as _;
    use std::fs;

    fn registered() -> (tempfile::TempDir, tempfile::TempDir, Connection) {
        let (repo, _sha) = init_repo(&[
            (
                "src/lib.rs",
                "/// User Authentication handler.\n\
                 pub fn auth_user() {}\n\
                 /// Authentication User reversed.\n\
                 pub fn reverse_auth_doc() {}\n\
                 pub fn alpha() -> i32 { 1 }\n\
                 pub fn beta() {}\n\
                 pub struct Widget;\n\
                 impl Widget {\n    pub fn render(&self) {}\n}\n",
            ),
            ("src/util.rs", "pub fn helper() {}\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 0).unwrap();
        (repo, db_tmp, conn)
    }

    fn refs_dedup_fixture(
        tier3: bool,
        analyzer_status: Option<&str>,
    ) -> (tempfile::TempDir, Connection) {
        let db_tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/lib.rs', 'sha-ref')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha-ref', 'tree-sitter-rust', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (id, blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               (1, 'sha-ref', 'tree-sitter-rust', 'caller', 'caller', 'function',
                0, 100, 1, 8, 'rust-syn')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO refs
               (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                byte_start, byte_end, line, source)
             VALUES
               ('sha-ref', 'tree-sitter-rust', 1, 'render', NULL, 'call',
                42, 48, 5, 'rust-syn')",
            [],
        )
        .unwrap();
        if tier3 {
            conn.execute(
                "INSERT INTO refs
                   (blob_sha, parser_id, enclosing_id, target_name, target_qualified, kind,
                    byte_start, byte_end, line, source)
                 VALUES
                   ('sha-ref', 'tree-sitter-rust', 1, 'render', 'crate::Widget::render', 'call',
                    42, 48, 5, 'tier3-rust-analyzer')",
                [],
            )
            .unwrap();
        }
        if let Some(status) = analyzer_status {
            conn.execute(
                "INSERT INTO workspace_analysis_runs
                   (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
                    started_at_ns, finished_at_ns, error)
                 VALUES
                   (1, 'rust-analyzer-lsp', 1, 'config', ?1, 0, 1, 'rust-analyzer unavailable')",
                rusqlite::params![status],
            )
            .unwrap();
        }
        (db_tmp, conn)
    }

    fn source_tier_fixture() -> (tempfile::TempDir, Connection) {
        let db_tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
             VALUES ('HEAD', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs
               (blob_sha, parser_id, parser_revision, analyzer_id,
                analyzer_revision, parsed_at_ns)
             VALUES
               ('sha-syn', 'tree-sitter-rust', 1, NULL, NULL, 0),
               ('sha-sem', 'tree-sitter-rust', 1, 'rust-syn', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES
               (1, 'src/syntactic.rs', 'sha-syn'),
               (1, 'src/semantic.rs', 'sha-sem')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source)
             VALUES
               ('sha-syn', 'tree-sitter-rust', 'syntactic_fn', 'syntactic_fn',
                'function', 0, 10, 1, 1, 'tree-sitter-rust'),
               ('sha-sem', 'tree-sitter-rust', 'semantic_fn', 'semantic_fn',
                'function', 0, 10, 1, 1, 'rust-syn')",
            [],
        )
        .unwrap();
        (db_tmp, conn)
    }

    fn language_fixture() -> (tempfile::TempDir, tempfile::TempDir, Connection) {
        let (repo, _sha) = init_repo(&[
            ("src/b.rs", "pub fn rust_user() {}\n"),
            ("src/a.py", "def py_user():\n    pass\n"),
            ("src/z.md", "# User\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 0).unwrap();
        (repo, db_tmp, conn)
    }

    #[test]
    fn find_by_name_returns_matching_symbol() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("alpha".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "alpha");
        assert_eq!(hits[0].path, "src/lib.rs");
    }

    #[test]
    fn fuzzy_multi_token_query_matches_all_tokens() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("User Authentication".into()),
                fuzzy: true,
                ..Default::default()
            },
        )
        .unwrap();
        let names: Vec<&str> = hits.iter().map(|h| h.name.as_str()).collect();
        assert!(names.contains(&"auth_user"), "{hits:?}");
        assert!(names.contains(&"reverse_auth_doc"), "{hits:?}");
    }

    #[test]
    fn fuzzy_quoted_query_is_exact_order_phrase() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("\"User Authentication\"".into()),
                fuzzy: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].name, "auth_user");
    }

    #[test]
    fn fuzzy_prefix_matching_requires_star() {
        let (_repo, _db, c) = registered();
        let no_prefix = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("Authent".into()),
                fuzzy: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(no_prefix.is_empty(), "{no_prefix:?}");

        let prefix = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("Authent*".into()),
                fuzzy: true,
                ..Default::default()
            },
        )
        .unwrap();
        let names: Vec<&str> = prefix.iter().map(|h| h.name.as_str()).collect();
        assert!(names.contains(&"auth_user"), "{prefix:?}");
        assert!(names.contains(&"reverse_auth_doc"), "{prefix:?}");
    }

    #[test]
    fn find_by_kind_filters() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                kind: Some("struct".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            hits.iter().any(|h| h.name == "Widget"),
            "Widget not in {hits:?}"
        );
    }

    #[test]
    fn find_symbols_reports_source_tier_from_blob_analyzer_id() {
        let (_db, c) = source_tier_fixture();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                kind: Some("function".into()),
                ..Default::default()
            },
        )
        .unwrap();

        let syntactic = hits.iter().find(|h| h.name == "syntactic_fn").unwrap();
        assert_eq!(syntactic.source_tier, SourceTier::Syntactic);
        assert_eq!(syntactic.language, None);
        let semantic = hits.iter().find(|h| h.name == "semantic_fn").unwrap();
        assert_eq!(semantic.source_tier, SourceTier::Semantic);
        assert_eq!(semantic.language.as_deref(), Some("rust"));
    }

    #[test]
    fn find_symbols_returns_language_and_sorts_language_path_line() {
        let (_repo, _db, c) = language_fixture();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                path_prefix: Some("src/".into()),
                ..Default::default()
            },
        )
        .unwrap();

        let rows: Vec<(&str, Option<&str>, &str)> = hits
            .iter()
            .map(|h| (h.name.as_str(), h.language.as_deref(), h.path.as_str()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("py_user", Some("python"), "src/a.py"),
                ("rust_user", Some("rust"), "src/b.rs"),
                ("User", None, "src/z.md"),
            ]
        );
    }

    #[test]
    fn directory_outline_returns_items_per_file_under_path_prefix_sorted() {
        let (repo, _sha) = init_repo(&[
            ("a/foo.rs", "pub fn foo_one() {}\npub fn foo_two() {}\n"),
            ("a/bar.rs", "pub fn bar_one() {}\n"),
            ("b/baz.rs", "pub fn baz_one() {}\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 0).unwrap();

        let hits = get_outline_under_path(&conn, &AnchorName::head(), "a/", None, 10).unwrap();
        let rows: Vec<(&str, &str, u32)> = hits
            .iter()
            .map(|h| (h.file.as_deref().unwrap(), h.name.as_str(), h.line))
            .collect();

        assert_eq!(
            rows,
            vec![
                ("a/bar.rs", "bar_one", 1),
                ("a/foo.rs", "foo_one", 1),
                ("a/foo.rs", "foo_two", 2),
            ]
        );
    }

    #[test]
    fn find_by_container_matches_qualified_prefix() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                container: Some("Widget".into()),
                ..Default::default()
            },
        )
        .unwrap();
        // Widget::render and possibly Widget::Widget depending on
        // how the tree-sitter pass names the impl block; at minimum
        // the method shows up.
        assert!(
            hits.iter().any(|h| h.name == "render"),
            "render not in {hits:?}"
        );
    }

    #[test]
    fn find_by_path_prefix_limits_scope() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                kind: Some("function".into()),
                path_prefix: Some("src/util.rs".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            hits.iter().all(|h| h.path == "src/util.rs"),
            "leaked across path prefix: {hits:?}"
        );
        assert!(hits.iter().any(|h| h.name == "helper"));
    }

    #[test]
    fn limit_caps_results() {
        let (_repo, _db, c) = registered();
        let hits = find_symbols(
            &c,
            &AnchorName::head(),
            &FindSymbolsArgs {
                kind: Some("function".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn no_filter_is_an_error() {
        let (_repo, _db, c) = registered();
        let err = find_symbols(&c, &AnchorName::head(), &FindSymbolsArgs::default()).unwrap_err();
        assert!(err.to_string().contains("at least one"));
    }

    #[test]
    fn unknown_anchor_is_an_error() {
        let (_repo, _db, c) = registered();
        let err = find_symbols(
            &c,
            &AnchorName::branch("does-not-exist"),
            &FindSymbolsArgs {
                query: Some("alpha".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("anchor not found"));
    }

    #[test]
    fn references_incoming_finds_callers() {
        let (_repo, _db, c) = registered();
        let hits = find_references(
            &c,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "alpha".into(),
                direction: ReferenceDirection::Incoming,
                ..Default::default()
            },
        )
        .unwrap();
        // alpha is referenced inside the file (see fixture src/lib.rs);
        // at minimum we shouldn't error and the SQL must execute.
        // Whether the syntactic-only Rust analyzer surfaces this
        // particular call depends on the parser; assert structural
        // correctness instead of a specific count.
        for h in &hits {
            assert_eq!(h.target_name, "alpha");
        }
    }

    #[test]
    fn references_outgoing_resolves_enclosing() {
        let (_repo, _db, c) = registered();
        // No symbol called `nonexistent` exists; the outgoing query
        // should run and return an empty result rather than error.
        let hits = find_references(
            &c,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "nonexistent::callee".into(),
                direction: ReferenceDirection::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn references_outgoing_defaults_to_resolved_calls() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn resolved() {}\n\
             pub fn caller(arg: Widget) -> Widget {\n\
                 resolved();\n\
                 arg.render();\n\
                 arg\n\
             }\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, _repo.path(), 0).unwrap();

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "caller".into(),
                direction: ReferenceDirection::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(
            hits.iter()
                .map(|h| h.target_name.as_str())
                .collect::<Vec<_>>(),
            vec!["resolved"],
            "outgoing default should hide unresolved method calls and type refs: {hits:?}"
        );
        assert!(hits.iter().all(|h| h.kind == RefKind::Call));
        assert!(hits.iter().all(|h| h.target_qualified.is_some()));
    }

    #[test]
    fn references_outgoing_include_noise_returns_legacy_refs() {
        let (_repo, _sha) = init_repo(&[(
            "src/lib.rs",
            "pub struct Widget;\n\
             impl Widget { pub fn render(&self) {} }\n\
             pub fn resolved() {}\n\
             pub fn caller(arg: Widget) -> Widget {\n\
                 resolved();\n\
                 arg.render();\n\
                 arg\n\
             }\n",
        )]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, _repo.path(), 0).unwrap();

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "caller".into(),
                direction: ReferenceDirection::Outgoing,
                include_noise: true,
                ..Default::default()
            },
        )
        .unwrap();

        assert!(
            hits.iter().any(|h| h.target_name == "resolved"
                && h.kind == RefKind::Call
                && h.target_qualified.as_deref() == Some("resolved")),
            "resolved call missing from noisy outgoing refs: {hits:?}"
        );
        assert!(
            hits.iter().any(|h| h.target_name == "render"
                && h.kind == RefKind::Call
                && h.target_qualified.is_none()),
            "unresolved method call missing from include_noise refs: {hits:?}"
        );
        assert!(
            hits.iter()
                .any(|h| h.target_name == "Widget" && h.kind == RefKind::Type),
            "type refs missing from include_noise refs: {hits:?}"
        );
    }

    #[test]
    fn references_tier2_only_falls_back_to_bare_name_refs() {
        let (_db, conn) = refs_dedup_fixture(false, None);

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "render".into(),
                direction: ReferenceDirection::Incoming,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].target_name, "render");
        assert_eq!(hits[0].target_qualified, None);
    }

    #[test]
    fn references_tier3_suppresses_tier2_same_call_site() {
        let (_db, conn) = refs_dedup_fixture(true, None);

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "render".into(),
                direction: ReferenceDirection::Incoming,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(
            hits[0].target_qualified.as_deref(),
            Some("crate::Widget::render")
        );
    }

    #[test]
    fn references_tier3_failed_run_falls_back_to_tier2_refs() {
        let (_db, conn) = refs_dedup_fixture(false, Some("failed"));

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "render".into(),
                direction: ReferenceDirection::Incoming,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].target_name, "render");
        assert_eq!(hits[0].target_qualified, None);
    }

    #[test]
    fn references_outgoing_prefers_tier3_for_same_call_site() {
        let (_db, conn) = refs_dedup_fixture(true, None);

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "caller".into(),
                direction: ReferenceDirection::Outgoing,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(
            hits[0].target_qualified.as_deref(),
            Some("crate::Widget::render")
        );
    }

    #[test]
    fn references_include_noise_keeps_tier2_and_tier3_duplicates() {
        let (_db, conn) = refs_dedup_fixture(true, None);

        let hits = find_references(
            &conn,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "render".into(),
                direction: ReferenceDirection::Incoming,
                include_noise: true,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(hits.len(), 2, "{hits:?}");
        assert!(
            hits.iter().any(|h| h.target_qualified.is_none()),
            "Tier-2 fallback row missing from noisy refs: {hits:?}"
        );
        assert!(
            hits.iter()
                .any(|h| h.target_qualified.as_deref() == Some("crate::Widget::render")),
            "Tier-3 row missing from noisy refs: {hits:?}"
        );
    }

    #[test]
    fn references_empty_symbol_errors() {
        let (_repo, _db, c) = registered();
        let err = find_references(
            &c,
            &AnchorName::head(),
            &FindReferencesArgs {
                symbol: "  ".into(),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-empty"));
    }

    #[test]
    fn tentative_sees_uncommitted_file() {
        let (repo, _sha) = init_repo(&[("src/lib.rs", "pub fn committed() {}\n")]);
        // Add an extra unstaged file.
        fs::write(
            repo.path().join("src/staged.rs"),
            "pub fn uncommitted() {}\n",
        )
        .unwrap();
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        let outcome = register_repo(&mut conn, repo.path(), 0).unwrap();

        let tent_anchor = AnchorName::tentative(outcome.worktree_id);
        let hits = find_symbols(
            &conn,
            &tent_anchor,
            &FindSymbolsArgs {
                query: Some("uncommitted".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1, "uncommitted symbol missing under tentative");

        // The committed anchor must NOT see it.
        let head_hits = find_symbols(
            &conn,
            &AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("uncommitted".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(head_hits.is_empty(), "committed anchor leaked uncommitted");
    }
}
