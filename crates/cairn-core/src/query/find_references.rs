use cairn_proto::common::RefKind;
use cairn_proto::methods::ReferenceDirection;
use rusqlite::Connection;

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::{ref_kind_from_str, ref_kind_to_str};
use crate::manifest::ManifestId;
use crate::workspace_analyzer::{source_is_workspace_tier_sql, source_rank_case_sql};

/// Provenance string used in [`ReferenceHit::kind_source`] when no
/// resolution row covered the site, so the `target_qualified` / `kind`
/// values came directly from the Tier-2 `refs` row.
pub const KIND_SOURCE_FACT: &str = "tier2-fact";

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
    pub parser_id: String,
    /// Provenance for [`Self::target_qualified`] / [`Self::kind`].
    /// Either a resolution-layer `source` string (e.g.
    /// `"tier25-ruby-resolver"`, `"tier3-pyright-lsp"`) when a
    /// `resolutions`-table row covered this site and supplied the
    /// resolved target / kind, or [`KIND_SOURCE_FACT`] (`"tier2-fact"`)
    /// when only the Tier-2 `refs` row was available. Phase 4 of the
    /// Tier-2.5 prep work extends the find_impls.rs precedent to refs
    /// so callers can see when a Tier-2.5 resolver promoted a
    /// name-only Tier-2 ref into a resolved cross-file edge.
    pub kind_source: String,
    /// Repo-relative path of the workspace file the target lives in,
    /// pulled directly from `resolutions.target_path` (v10+, Phase 2).
    /// `Some("src/foo.rs")` whenever a Tier-2.5+ resolver pinned the
    /// site to a workspace-internal target; `None` for unresolved
    /// sites and for targets that resolved outside the indexed
    /// workspace. Independent of `target_symbol_id`: cross-parser
    /// type/call edges may carry `target_path = Some` even when no
    /// sibling-parser symbol could be uniquely identified, and import
    /// edges always carry `target_qualified = None` while still
    /// populating `target_path` for workspace-internal modules.
    pub target_path: Option<String>,
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
    //
    // Phase 4 of the Tier-2.5 prep work extends the find_impls.rs
    // precedent to the ref query path: a `best_resolution` CTE picks
    // one `resolutions` row per `(blob, parser_id, byte_start,
    // byte_end, kind)` tuple (ranked by `source_rank_case_sql` so
    // tier3 wins over tier25 wins over tier2-direct), the main query
    // LEFT JOINs it, and the projected `target_qualified` is
    // `COALESCE(refs.target_qualified, symbols.qualified)` where
    // `symbols.qualified` is pulled through `resolutions.target_symbol_id`.
    // `kind_source` carries the provenance: the resolution-layer
    // `source` string when a resolution covered the site, the
    // sentinel `tier2-fact` otherwise. The outgoing noise filter is
    // weakened correspondingly: a row passes when *either* the
    // Tier-2 row carries a qualified target *or* a Tier-2.5 / Tier-3
    // resolution supplied one — that is the whole point of running
    // the cross-file resolver.
    //
    // SQL fragments derived from the registered workspace-tier prefixes; they
    // expand when a new tier (e.g. Tier-2.5) joins WORKSPACE_TIER_PREFIXES.
    let source_rank_r = source_rank_case_sql("r.source");
    let resolution_source_rank = source_rank_case_sql("source");
    let workspace_tier_t = source_is_workspace_tier_sql("t.source");
    let run = |where_col: &str, value: &str, outgoing: bool| -> Result<Vec<ReferenceHit>> {
        let mut sql = String::from(
            "WITH best_resolution AS (
                 SELECT site_blob_sha, site_parser_id,
                        site_byte_start, site_byte_end, kind,
                        target_symbol_id, source, target_path,
                        ROW_NUMBER() OVER (
                            PARTITION BY site_blob_sha, site_parser_id,
                                         site_byte_start, site_byte_end, kind
                            ORDER BY
                                CASE WHEN manifest_id = ?1 THEN 0 ELSE 1 END,
                                ",
        );
        sql.push_str(&resolution_source_rank);
        sql.push_str(
            ", id
                        ) AS rn
                   FROM resolutions
                  WHERE kind IN ('type', 'call', 'import')
                    AND (manifest_id = ?1 OR manifest_id IS NULL)
             )
             SELECT target_name, target_qualified, kind, enclosing,
                    path, line, blob_sha, parser_id, kind_source,
                    target_path
               FROM (
                 SELECT r.target_name,
                        COALESCE(r.target_qualified, sym.qualified)
                            AS target_qualified,
                        r.kind,
                        enc.qualified AS enclosing,
                        me.path, r.line, r.blob_sha, r.parser_id,
                        r.byte_start, r.byte_end,
                        res.target_path AS target_path,
                        CASE WHEN res.source IS NOT NULL THEN res.source
                             ELSE '",
        );
        sql.push_str(KIND_SOURCE_FACT);
        sql.push_str("' END AS kind_source,\n                        ");
        sql.push_str(&source_rank_r);
        sql.push_str(" AS source_rank,\n");
        sql.push_str(&format!(
            "                        EXISTS (
                          SELECT 1
                            FROM refs t
                           WHERE t.blob_sha = r.blob_sha
                             AND ({workspace_tier_t})
                             AND t.line = r.line
                             AND t.kind = r.kind
                             AND t.target_name = r.target_name
                             AND t.enclosing_id IS r.enclosing_id
                        ) AS has_workspace_tier_same_line_target_name,
                        ROW_NUMBER() OVER (
                          PARTITION BY r.blob_sha, r.byte_start, r.byte_end, r.kind
                          ORDER BY
                            {source_rank_r},
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
                   LEFT JOIN best_resolution res
                     ON res.site_blob_sha = r.blob_sha
                    AND res.site_parser_id = r.parser_id
                    AND res.site_byte_start = r.byte_start
                    AND res.site_byte_end = r.byte_end
                    AND res.kind = r.kind
                    AND res.rn = 1
                   LEFT JOIN symbols sym
                     ON sym.id = res.target_symbol_id
               "
        ));
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
            // A row qualifies as a resolved callee when *either* the
            // Tier-2 ref already carries a qualified target *or* a
            // resolution-layer row pinned one (via sym.qualified).
            sql.push_str(" AND r.kind = 'call'");
            sql.push_str(
                " AND ((r.target_qualified IS NOT NULL AND r.target_qualified <> '')
                       OR sym.qualified IS NOT NULL)",
            );
        }
        sql.push(')');
        if !args.include_noise {
            sql.push_str(" WHERE dedup_rank = 1");
            sql.push_str(
                " AND NOT (
                    source_rank > 0
                    AND byte_start = 0
                    AND byte_end = 0
                    AND has_workspace_tier_same_line_target_name
                )",
            );
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
                parser_id: row.get(7)?,
                kind_source: row.get(8)?,
                target_path: row.get(9)?,
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
            // Prefer qualified-name matching when the symbol carries a
            // language-specific separator (`::` for Rust, `.` for
            // Python / Kotlin / Swift / C# / Java FQNs, `\` for PHP
            // namespaces). Bare symbols skip straight to the bare-name
            // index.
            //
            // The strict path matches against
            // `COALESCE(r.target_qualified, sym.qualified)` rather than
            // `r.target_qualified` alone so that cross-parser-id
            // resolutions — where the Tier-2.5 persist layer adopted a
            // sibling-parser symbol id and the surface `target_qualified`
            // comes from `sym.qualified` — also match a strict FQN query.
            // Pre-Phase 4 this only checked the raw `refs.target_qualified`
            // and missed every cross-parser resolved hit.
            if is_qualified_symbol(&args.symbol) {
                let strict = run_strict_incoming(
                    conn,
                    manifest_id,
                    &args.symbol,
                    kind_str,
                    args.include_noise,
                    limit,
                )?;
                if !strict.is_empty() {
                    return Ok(strict);
                }
                let bare = bare_name_from_qualified(&args.symbol);
                run("r.target_name", bare, false)
            } else {
                run("r.target_name", &args.symbol, false)
            }
        }
    }
}

/// Strict-FQN incoming reference lookup with index-friendly query
/// shape (PR-γ #8).
///
/// Pre-fix, this case ran through `run()` with
/// `WHERE COALESCE(r.target_qualified, sym.qualified) = ?`. The
/// COALESCE referenced a column from a LEFT JOIN (`symbols.qualified`
/// via `resolutions.target_symbol_id`), so SQLite could not push it
/// through `idx_refs_target_qualified` and fell back to `SCAN refs
/// USING idx_refs_blob`, an O(N) scan per query (measured ~135× the
/// index-driven path on a 1K-ref fixture).
///
/// The rewrite is a strict equivalence: `COALESCE(a, b) = ?` becomes
/// `a = ? OR (a IS NULL AND b = ?)`, but expressed as a `UNION ALL`
/// over two disjoint `strict_refs` branches so SQLite can pick an
/// index for each:
///
///   * **Branch A** — `refs.target_qualified = ?` hits the partial
///     index `idx_refs_target_qualified (target_qualified IS NOT NULL)`.
///   * **Branch B** — `refs.target_qualified IS NULL` + the join to
///     `best_resolution` + `symbols.qualified = ?` lets SQLite probe
///     `idx_symbols_qualified` first and ride the resolution-row
///     uniqueness back to the ref.
///
/// The two branches are mutually exclusive by construction
/// (`target_qualified IS NULL` is a single-table predicate on `refs`),
/// so `UNION ALL` is the right combinator: no dedup needed across
/// branches.
///
/// Critical invariants (see `pr_gamma_*` tests):
///   * Empty-string is NOT NULL — Branch A still selects
///     `target_qualified = ''` rows when that happens to match the
///     query value, matching the COALESCE semantics exactly.
///   * The downstream `dedup_rank` / noise filter / projection
///     COALESCE are unchanged — Branch B rows continue to carry
///     `target_qualified=NULL` and inherit `sym.qualified` through
///     the existing projection.
///   * Scope is limited to **incoming + qualified-symbol + strict**.
///     Bare-name fallback, outgoing, and non-qualified symbols still
///     go through `run()` and `idx_refs_target_name` as before.
#[allow(clippy::too_many_arguments)]
fn run_strict_incoming(
    conn: &Connection,
    manifest_id: ManifestId,
    symbol: &str,
    kind_str: Option<&'static str>,
    include_noise: bool,
    limit: u32,
) -> Result<Vec<ReferenceHit>> {
    let source_rank_r = source_rank_case_sql("r.source");
    let resolution_source_rank = source_rank_case_sql("source");
    let workspace_tier_t = source_is_workspace_tier_sql("t.source");

    let mut sql = String::from(
        "WITH best_resolution AS (
             SELECT site_blob_sha, site_parser_id,
                    site_byte_start, site_byte_end, kind,
                    target_symbol_id, source, target_path,
                    ROW_NUMBER() OVER (
                        PARTITION BY site_blob_sha, site_parser_id,
                                     site_byte_start, site_byte_end, kind
                        ORDER BY
                            CASE WHEN manifest_id = ?1 THEN 0 ELSE 1 END,
                            ",
    );
    sql.push_str(&resolution_source_rank);
    sql.push_str(
        ", id
                    ) AS rn
               FROM resolutions
              WHERE kind IN ('type', 'call', 'import')
                AND (manifest_id = ?1 OR manifest_id IS NULL)
         ),
         strict_refs AS (
             -- Branch A: r.target_qualified hits idx_refs_target_qualified.
             SELECT r.*
               FROM refs r
              WHERE r.target_qualified = ?2
             UNION ALL
             -- Branch B: cross-parser fallback. The Tier-2.5 persist
             -- layer adopted a sibling-parser symbol id (so
             -- `target_qualified` on the ref is NULL); the strict
             -- query reaches it via the resolution row + symbol
             -- table. Probes idx_symbols_qualified first.
             SELECT r.*
               FROM refs r
               JOIN best_resolution res
                 ON res.site_blob_sha = r.blob_sha
                AND res.site_parser_id = r.parser_id
                AND res.site_byte_start = r.byte_start
                AND res.site_byte_end = r.byte_end
                AND res.kind = r.kind
                AND res.rn = 1
               JOIN symbols sym ON sym.id = res.target_symbol_id
              WHERE r.target_qualified IS NULL
                AND sym.qualified = ?2
         )
         SELECT target_name, target_qualified, kind, enclosing,
                path, line, blob_sha, parser_id, kind_source,
                target_path
           FROM (
             SELECT r.target_name,
                    COALESCE(r.target_qualified, sym.qualified)
                        AS target_qualified,
                    r.kind,
                    enc.qualified AS enclosing,
                    me.path, r.line, r.blob_sha, r.parser_id,
                    r.byte_start, r.byte_end,
                    res.target_path AS target_path,
                    CASE WHEN res.source IS NOT NULL THEN res.source
                         ELSE '",
    );
    sql.push_str(KIND_SOURCE_FACT);
    sql.push_str("' END AS kind_source,\n                    ");
    sql.push_str(&source_rank_r);
    sql.push_str(" AS source_rank,\n");
    sql.push_str(&format!(
        "                    EXISTS (
                      SELECT 1
                        FROM refs t
                       WHERE t.blob_sha = r.blob_sha
                         AND ({workspace_tier_t})
                         AND t.line = r.line
                         AND t.kind = r.kind
                         AND t.target_name = r.target_name
                         AND t.enclosing_id IS r.enclosing_id
                    ) AS has_workspace_tier_same_line_target_name,
                    ROW_NUMBER() OVER (
                      PARTITION BY r.blob_sha, r.byte_start, r.byte_end, r.kind
                      ORDER BY
                        {source_rank_r},
                        CASE
                          WHEN r.target_qualified IS NOT NULL
                           AND r.target_qualified <> '' THEN 0
                          ELSE 1
                        END,
                        r.source
                    ) AS dedup_rank
               FROM strict_refs r
               JOIN manifest_entries me
                 ON me.manifest_id = ?1
                AND me.blob_sha = r.blob_sha
               LEFT JOIN best_resolution res
                 ON res.site_blob_sha = r.blob_sha
                AND res.site_parser_id = r.parser_id
                AND res.site_byte_start = r.byte_start
                AND res.site_byte_end = r.byte_end
                AND res.kind = r.kind
                AND res.rn = 1
               LEFT JOIN symbols sym ON sym.id = res.target_symbol_id
               LEFT JOIN symbols enc ON enc.id = r.enclosing_id
              WHERE 1=1"
    ));
    if kind_str.is_some() {
        sql.push_str(" AND r.kind = ?3");
    }
    sql.push(')');
    if !include_noise {
        sql.push_str(" WHERE dedup_rank = 1");
        sql.push_str(
            " AND NOT (
                source_rank > 0
                AND byte_start = 0
                AND byte_end = 0
                AND has_workspace_tier_same_line_target_name
            )",
        );
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
            parser_id: row.get(7)?,
            kind_source: row.get(8)?,
            target_path: row.get(9)?,
        })
    };
    let rows: rusqlite::Result<Vec<ReferenceHit>> = match kind_str {
        Some(k) => stmt
            .query_map(rusqlite::params![manifest_id.0, symbol, k], row_to_hit)?
            .collect(),
        None => stmt
            .query_map(rusqlite::params![manifest_id.0, symbol], row_to_hit)?
            .collect(),
    };
    Ok(rows?)
}

/// `true` when `symbol` looks like a fully-qualified name in any
/// language cairn currently indexes: Rust `::`, dotted FQNs (Python,
/// Kotlin, Swift, C#, Java, JS), or PHP-style `\` namespaces.
fn is_qualified_symbol(symbol: &str) -> bool {
    symbol.contains("::") || symbol.contains('.') || symbol.contains('\\')
}

/// Strip everything before the last qualified-name segment, so the
/// bare-name fallback can still try `r.target_name` when the strict
/// `COALESCE(...)` lookup returns nothing. Recognises the same
/// separators as [`is_qualified_symbol`].
fn bare_name_from_qualified(symbol: &str) -> &str {
    // Find the *rightmost* separator among the three we recognise.
    let mut last = 0usize;
    for (idx, _) in symbol.match_indices("::") {
        last = last.max(idx + 2);
    }
    for (idx, c) in symbol.char_indices() {
        if c == '.' || c == '\\' {
            last = last.max(idx + c.len_utf8());
        }
    }
    if last == 0 { symbol } else { &symbol[last..] }
}

#[cfg(test)]
mod qualified_helpers_tests {
    use super::{bare_name_from_qualified, is_qualified_symbol};

    #[test]
    fn is_qualified_recognises_rust_double_colon() {
        assert!(is_qualified_symbol("crate::foo::Bar"));
    }

    #[test]
    fn is_qualified_recognises_dotted_fqn() {
        assert!(is_qualified_symbol("com.example.app.User"));
        assert!(is_qualified_symbol("pkg.sub.Foo"));
    }

    #[test]
    fn is_qualified_recognises_php_backslash_namespace() {
        assert!(is_qualified_symbol("App\\Models\\Widget"));
    }

    #[test]
    fn is_qualified_rejects_bare_name() {
        assert!(!is_qualified_symbol("Widget"));
        assert!(!is_qualified_symbol("render"));
    }

    #[test]
    fn bare_name_strips_rightmost_separator() {
        assert_eq!(bare_name_from_qualified("crate::foo::Bar"), "Bar");
        assert_eq!(bare_name_from_qualified("com.example.app.User"), "User");
        assert_eq!(bare_name_from_qualified("App\\Models\\Widget"), "Widget");
    }

    #[test]
    fn bare_name_returns_input_when_no_separator() {
        assert_eq!(bare_name_from_qualified("Widget"), "Widget");
    }
}
