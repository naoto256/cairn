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

/// Internal query outcome used by the data-RPC layer to carry query-local
/// precision evidence alongside usable rows. The public query API intentionally
/// continues to return only hits.
#[derive(Debug)]
pub(crate) struct FindReferencesOutcome {
    pub(crate) hits: Vec<ReferenceHit>,
    pub(crate) qualified_fallback: bool,
    /// `true` when an outgoing default query observed at least one logical
    /// call site that survived tier de-duplication but had no qualified target.
    pub(crate) omitted_unresolved_calls: bool,
}

/// Find references either way:
/// - `Incoming` — refs whose target matches `symbol` (callers / use
///   sites). When `symbol` contains `::`, `.`, or `\`, the
///   qualified-name index is tried first; bare names match
///   `target_name` directly. The public query API preserves its
///   historical best-effort behavior by returning usable bare-name
///   rows after a strict miss; data-RPC consumers additionally report
///   that fallback as partial rather than claiming an exact match.
/// - `Outgoing` — refs inside the body of the symbol named `symbol`
///   (= callees / uses from the symbol). Matches `symbols.qualified`
///   on the enclosing FK. Default outgoing results contain resolved calls;
///   the private outcome records whether unresolved logical call sites were
///   omitted so data-RPC consumers do not claim a complete call graph.
///
/// # Errors
/// `Error::InvalidArgument` when `symbol` is empty, `Error::AnchorNotFound` when the anchor
/// doesn't resolve. SQLite errors otherwise.
pub fn find_references(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindReferencesArgs,
) -> Result<Vec<ReferenceHit>> {
    Ok(find_references_with_status(conn, anchor, args)?.hits)
}

pub(crate) fn find_references_with_status(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindReferencesArgs,
) -> Result<FindReferencesOutcome> {
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
) -> Result<FindReferencesOutcome> {
    // `None` picks the default page of 100. `.max(1)` guards against
    // `Some(0)` emitting `LIMIT 0` (SQLite: no rows).
    let limit = args.limit.unwrap_or(100).max(1);
    // Convert the wire-typed `RefKind` filter to the on-disk string
    // representation once so the closure below can bind it directly.
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
    // `COALESCE(symbols.qualified, refs.target_qualified)` where
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
    // Three separate `source_rank_case_sql` / `source_is_workspace_tier_sql`
    // expansions because the outer `refs` (`r.source`), the CTE
    // interior (`source`), and the noise-filter EXISTS subquery
    // (`t.source`) all live in different scopes with different column
    // aliases. Precomputing them keeps the format! templating readable.
    let source_rank_r = source_rank_case_sql("r.source");
    let resolution_source_rank = source_rank_case_sql("source");
    let workspace_tier_t = source_is_workspace_tier_sql("t.source");
    let logical_site_columns = logical_site_columns_sql("r");
    // Closure so incoming/outgoing share this SQL body — they differ
    // only in `where_col` (`enc.qualified` vs `r.target_name` /
    // `r.target_qualified`), the pinned `value`, the enclosing-symbol
    // JOIN semantics (INNER for outgoing, LEFT for incoming), and the
    // outgoing-only "resolved callee" noise filter below.
    let run = |where_col: &str, value: &str, outgoing: bool| -> Result<(Vec<ReferenceHit>, bool)> {
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
             ref_candidates AS (
                 SELECT r.target_name,
                        COALESCE(sym.qualified, r.target_qualified)
                            AS target_qualified,
                        r.kind,
                        enc.qualified AS enclosing,
                        me.path, r.line, r.blob_sha, r.parser_id,
                        r.byte_start, r.byte_end, r.id AS ref_id,
                        r.enclosing_id, r.source,
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
                        {logical_site_columns}
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
        // Two-part noise cut, staged as CTEs so the ranked set stays
        // reusable. `dedup_rank = 1` keeps one row per site; `kind` sits in
        // the partition so a `call` and a `type` on the same token stay
        // separate, and the logical-site columns give range-less rows their
        // own identity instead of collapsing distinct sites into one. The
        // `AND NOT` clause then drops a range-less lower-tier row only when a
        // workspace-tier row on the same `(line, kind, target_name,
        // enclosing_id)` tuple already supersedes it.
        sql.push_str(
            "),
             ranked_refs AS (
               SELECT *,
                      ROW_NUMBER() OVER (
                        PARTITION BY blob_sha, byte_start, byte_end, kind,
                                     logical_site_line, logical_site_target,
                                     logical_site_enclosing
                        ORDER BY source_rank,
                                 CASE WHEN target_qualified IS NOT NULL
                                           AND target_qualified <> ''
                                      THEN 0 ELSE 1 END,
                                 source
                      ) AS dedup_rank
                 FROM ref_candidates
             ),
             surviving_refs AS (
               SELECT *
                 FROM ranked_refs
                WHERE dedup_rank = 1
                  AND NOT (
                    source_rank > 0
                    AND byte_start = 0
                    AND byte_end = 0
                    AND has_workspace_tier_same_line_target_name
                  )
             ),
             presentation_refs AS (
               SELECT * FROM ",
        );
        sql.push_str(if args.include_noise {
            "ref_candidates"
        } else {
            "surviving_refs"
        });
        if outgoing && !args.include_noise {
            // The projected value is the sole resolved-target authority.
            // Resolution-row presence by itself never makes a call usable.
            sql.push_str(
                " WHERE kind = 'call'
                    AND target_qualified IS NOT NULL
                    AND target_qualified <> ''",
            );
        }
        sql.push_str(
            "),
             limited_hits AS (
               SELECT * FROM presentation_refs
                ORDER BY path, line, byte_start, source_rank, ref_id
                LIMIT ",
        );
        sql.push_str(&limit.to_string());
        // Evidence reads the pre-limit surviving set: a call the page cut off
        // is still an omitted call the caller has to know about.
        sql.push_str(
            "),
             evidence AS (
               SELECT ",
        );
        sql.push_str(if outgoing && !args.include_noise {
            "EXISTS(
                   SELECT 1 FROM surviving_refs
                    WHERE kind = 'call'
                      AND (target_qualified IS NULL OR target_qualified = '')
                 )"
        } else {
            "0"
        });
        // The metadata-only row carries that evidence when the page is empty:
        // an unresolved-only body leaves no surviving hit to attach it to, and
        // dropping it would report a complete call graph.
        sql.push_str(
            " AS omitted_unresolved_calls
             )
             SELECT target_name, target_qualified, kind, enclosing,
                    path, line, blob_sha, parser_id, kind_source,
                    target_path, ref_id, byte_start, source_rank,
                    0 AS is_metadata, omitted_unresolved_calls
               FROM limited_hits CROSS JOIN evidence
             UNION ALL
             SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, 1, omitted_unresolved_calls
               FROM evidence
              WHERE NOT EXISTS (SELECT 1 FROM limited_hits)
             ORDER BY is_metadata, path, line, byte_start, source_rank, ref_id",
        );

        let mut stmt = conn.prepare(&sql)?;
        let row_to_result = |row: &rusqlite::Row<'_>| -> rusqlite::Result<_> {
            let is_metadata = row.get::<_, bool>(13)?;
            let omitted_unresolved_calls = row.get::<_, bool>(14)?;
            if is_metadata {
                return Ok((None, omitted_unresolved_calls));
            }
            Ok((
                Some(ReferenceHit {
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
                }),
                omitted_unresolved_calls,
            ))
        };
        let rows: rusqlite::Result<Vec<(Option<ReferenceHit>, bool)>> = match &kind_str {
            Some(k) => stmt
                .query_map(rusqlite::params![manifest_id.0, value, k], row_to_result)?
                .collect(),
            None => stmt
                .query_map(rusqlite::params![manifest_id.0, value], row_to_result)?
                .collect(),
        };
        let rows = rows?;
        let omitted_unresolved_calls = rows
            .iter()
            .any(|(_, omitted_unresolved_calls)| *omitted_unresolved_calls);
        let hits = rows.into_iter().filter_map(|(hit, _)| hit).collect();
        Ok((hits, omitted_unresolved_calls))
    };

    match args.direction {
        ReferenceDirection::Outgoing => {
            let (hits, omitted_unresolved_calls) = run("enc.qualified", &args.symbol, true)?;
            Ok(FindReferencesOutcome {
                hits,
                qualified_fallback: false,
                omitted_unresolved_calls,
            })
        }
        ReferenceDirection::Incoming => {
            // Prefer qualified-name matching when the symbol carries a
            // language-specific separator (`::` for Rust, `.` for
            // Python / Kotlin / Swift / C# / Java FQNs, `\` for PHP
            // namespaces). Bare symbols skip straight to the bare-name
            // index.
            //
            // The strict path matches against
            // `COALESCE(sym.qualified, r.target_qualified)` rather than
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
                    return Ok(FindReferencesOutcome {
                        hits: strict,
                        qualified_fallback: false,
                        omitted_unresolved_calls: false,
                    });
                }
                let bare = bare_name_from_qualified(&args.symbol);
                let (hits, _) = run("r.target_name", bare, false)?;
                Ok(FindReferencesOutcome {
                    qualified_fallback: !hits.is_empty(),
                    hits,
                    omitted_unresolved_calls: false,
                })
            } else {
                let (hits, _) = run("r.target_name", &args.symbol, false)?;
                Ok(FindReferencesOutcome {
                    hits,
                    qualified_fallback: false,
                    omitted_unresolved_calls: false,
                })
            }
        }
    }
}

/// Strict-FQN incoming reference lookup with index-friendly query
/// shape (PR-γ #8).
///
/// Pre-fix, this case ran through `run()` with
/// `WHERE COALESCE(sym.qualified, r.target_qualified) = ?`. The
/// COALESCE referenced a column from a LEFT JOIN (`symbols.qualified`
/// via `resolutions.target_symbol_id`), so SQLite could not push it
/// through `idx_refs_target_qualified` and fell back to `SCAN refs
/// USING idx_refs_blob`, an O(N) scan per query (measured ~135× the
/// index-driven path on a 1K-ref fixture).
///
/// The rewrite is expressed as a `UNION ALL` over two disjoint
/// `strict_refs` branches so SQLite can pick an index for each:
///
///   * **Branch A** — `refs.target_qualified = ?` hits the partial
///     index `idx_refs_target_qualified (target_qualified IS NOT NULL)`.
///   * **Branch B** — the join to `best_resolution` plus
///     `symbols.qualified = ?` lets SQLite probe
///     `idx_symbols_qualified` first and ride the resolution-row
///     uniqueness back to the ref. It excludes rows already selected
///     by Branch A.
///
/// The outer projected-name predicate removes Branch A rows when a
/// higher-tier resolution supersedes their syntactic qualified value.
///
/// Critical invariants (see `pr_gamma_*` tests):
///   * Empty-string is NOT NULL — Branch A still selects
///     `target_qualified = ''` rows when that happens to match the
///     query value when no higher-tier symbol supersedes it.
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
    let logical_site_columns = logical_site_columns_sql("r");

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
             -- `target_qualified` on the ref may be absent or merely
             -- syntactic); the strict query reaches it via the resolution row + symbol
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
              WHERE sym.qualified = ?2
                AND (r.target_qualified IS NULL OR r.target_qualified <> ?2)
         ),
         ref_candidates AS (
             SELECT r.target_name,
                    COALESCE(sym.qualified, r.target_qualified)
                        AS target_qualified,
                    r.kind,
                    enc.qualified AS enclosing,
                    me.path, r.line, r.blob_sha, r.parser_id,
                    r.byte_start, r.byte_end, r.id AS ref_id,
                    r.enclosing_id, r.source,
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
                    {logical_site_columns}
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
    sql.push_str(
        "),
         ranked_refs AS (
           SELECT *,
                  ROW_NUMBER() OVER (
                    PARTITION BY blob_sha, byte_start, byte_end, kind,
                                 logical_site_line, logical_site_target,
                                 logical_site_enclosing
                    ORDER BY source_rank,
                             CASE WHEN target_qualified IS NOT NULL
                                       AND target_qualified <> ''
                                  THEN 0 ELSE 1 END,
                             source
                  ) AS dedup_rank
             FROM ref_candidates
         ),
         surviving_refs AS (
           SELECT *
             FROM ranked_refs
            WHERE dedup_rank = 1
              AND NOT (
                source_rank > 0
                AND byte_start = 0
                AND byte_end = 0
                AND has_workspace_tier_same_line_target_name
              )
         )
         SELECT target_name, target_qualified, kind, enclosing,
                path, line, blob_sha, parser_id, kind_source,
                target_path
           FROM ",
    );
    sql.push_str(if include_noise {
        "ref_candidates"
    } else {
        "surviving_refs"
    });
    sql.push_str(" WHERE target_qualified = ?2");
    sql.push_str(" ORDER BY path, line, byte_start, source_rank, ref_id");
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

/// SQL projection for the fallback identity of refs without byte ranges.
///
/// A non-zero range remains the physical-site authority. For a 0..0 range,
/// line + target + enclosing is the strongest fact the parser recorded; two
/// identical references on the same line are therefore indistinguishable.
fn logical_site_columns_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {alias}.byte_start = 0 AND {alias}.byte_end = 0
              THEN {alias}.line END AS logical_site_line,
         CASE WHEN {alias}.byte_start = 0 AND {alias}.byte_end = 0
              THEN {alias}.target_name END AS logical_site_target,
         CASE WHEN {alias}.byte_start = 0 AND {alias}.byte_end = 0
              THEN {alias}.enclosing_id END AS logical_site_enclosing"
    )
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
