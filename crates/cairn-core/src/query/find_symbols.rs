//! `find_symbols` — workspace-scoped symbol lookup by name / kind /
//! container / path prefix.
//!
//! Ordinary lookups read directly from Tier-1 `symbols`: this query
//! answers "where is X declared?" rather than "what does X resolve
//! to?". `include_inherited` is the narrow exception: resolution
//! authority selects the transitive owner set, while the returned rows
//! remain declarations. Rows are scoped to blobs visible from the
//! anchor's manifest via `manifest_entries`, and to
//! `scope = 'top_level'` so nested (function-local) declarations do
//! not surface as workspace-addressable hits — the file-structure
//! view in [`crate::query::get_outline`] keeps them.
//!
//! `SourceTier` (Syntactic vs Semantic) is derived from
//! `blobs.analyzer_id`: any non-NULL analyzer stamp on the parsed
//! blob promotes the row to Semantic, matching the Tier-2 native
//! enricher convention documented in
//! [`crate::workspace_analyzer`].
use std::collections::{HashMap, HashSet};

use cairn_lang_api::Visibility;
use cairn_proto::common::{SourceTier, SymbolKind};
use rusqlite::{Connection, ToSql};

use crate::Result;
use crate::anchor::{self, AnchorName};
use crate::cas::kind_conv::{symbol_kind_from_str, visibility_from_str};
use crate::manifest::ManifestId;
use crate::workspace_analyzer::source_rank_case_sql;

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

/// Fuzzy-result priority class. The variants are declared in priority order
/// and compared by that order, so reordering them silently changes which hits
/// lead a fuzzy page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FuzzyRank {
    ExactName,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RankedSymbolHit {
    pub(crate) hit: SymbolHit,
    rank: FuzzyRank,
}

impl RankedSymbolHit {
    pub(crate) fn rank_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank.cmp(&other.rank)
    }
}

/// Query rows plus semantic-partiality discovered while expanding an
/// inherited-member request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FindSymbolsOutcome {
    pub(crate) ranked_hits: Vec<RankedSymbolHit>,
    pub(crate) inheritance_unresolved: bool,
    pub(crate) tier2_warming: bool,
}

// FTS membership and exact-name priority intentionally share this one
// bytewise authority. Query syntax is passed to SQLite unchanged; only a raw
// `name` equality receives the leading class. The ordinary query reserves
// `?1` for its manifest; the inherited bulk lookup remaps this same expression
// to its first bind rather than maintaining a second rank definition.
const FUZZY_RANK_SQL: &str = "CASE WHEN s.name = ?2 COLLATE BINARY THEN 0 ELSE 1 END";

fn fuzzy_rank_from_sql(value: i64) -> FuzzyRank {
    if value == 0 {
        FuzzyRank::ExactName
    } else {
        FuzzyRank::Other
    }
}

/// Query the symbols visible from `anchor`. `anchor` resolves to one
/// manifest; the join scopes hits to blobs that appear in that
/// manifest.
///
/// A fuzzy query returns hits whose `name` equals the query ahead of the
/// remaining full-text matches. Every other query shape keeps the structural
/// language → path → line order unchanged.
///
/// # Errors
/// Returns [`crate::Error::InvalidArgument`] when no filter is set or
/// the anchor does not resolve. SQLite errors otherwise.
pub fn find_symbols(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindSymbolsArgs,
) -> Result<Vec<SymbolHit>> {
    Ok(find_symbols_with_status(conn, anchor, args, false)?
        .ranked_hits
        .into_iter()
        .map(|ranked| ranked.hit)
        .collect())
}

/// [`find_symbols`] with the semantic state needed by the public data
/// method to report a usable-but-partial inherited-member union.
pub(crate) fn find_symbols_with_status(
    conn: &Connection,
    anchor: &AnchorName,
    args: &FindSymbolsArgs,
    include_inherited: bool,
) -> Result<FindSymbolsOutcome> {
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

    if include_inherited
        && let Some(container) = args.container.as_deref()
        && !container.is_empty()
    {
        return run_find_symbols_inherited(conn, manifest_id, args, container);
    }

    Ok(FindSymbolsOutcome {
        ranked_hits: run_find_symbols(conn, manifest_id, args)?,
        inheritance_unresolved: false,
        tier2_warming: false,
    })
}

fn run_find_symbols(
    conn: &Connection,
    manifest_id: ManifestId,
    args: &FindSymbolsArgs,
) -> Result<Vec<RankedSymbolHit>> {
    // Default page of 50 (workspace-lookup pages are typically smaller
    // than reference / import listings). `.max(1)` guards against a
    // caller-supplied `Some(0)` becoming `LIMIT 0` (SQLite: no rows).
    let limit = args.limit.unwrap_or(50).max(1);

    // Base query: pull symbols whose blob_sha is in the manifest's
    // entry set, joined to manifest_entries so we can return the
    // file path the blob was mounted at.
    //
    // The `language` CASE reverse-engineers the human-readable
    // language name from `parser_id`:
    // `tree-sitter-<lang>[@<revision>]` produces `<lang>` (index 13
    // is one past the "tree-sitter-" prefix in SQLite's 1-indexed
    // `substr`); non-tree-sitter parser ids pass through unchanged;
    // and blobs with no analyzer stamp emit NULL so the ORDER BY
    // below can push them to the tail.
    //
    // The `s.scope = 'top_level'` filter is the workspace-vs-file
    // view split: nested (function-local) declarations are indexed
    // for outline / navigation but must not surface here.
    let fuzzy_query = args.fuzzy && args.query.as_deref().is_some_and(|query| !query.is_empty());
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
                 b.analyzer_id IS NOT NULL",
    );
    if fuzzy_query {
        sql.push_str(", ");
        sql.push_str(FUZZY_RANK_SQL);
        sql.push_str(" AS fuzzy_rank");
    }
    sql.push_str(
        "
           FROM symbols s
           JOIN manifest_entries me
             ON me.manifest_id = ?1
            AND me.blob_sha = s.blob_sha
           JOIN blobs b
             ON b.blob_sha = s.blob_sha
          WHERE 1=1
            AND s.scope = 'top_level'",
    );
    let mut bound: Vec<Box<dyn ToSql>> = vec![Box::new(manifest_id.0)];
    if fuzzy_query {
        bound.push(Box::new(args.query.clone().unwrap_or_default()));
    }

    if let Some(q) = args.query.as_deref()
        && !q.is_empty()
    {
        if args.fuzzy {
            // FTS5 path: `symbols_fts` is the content-synced virtual
            // table declared in `cas/schema.rs` (unicode61,
            // `remove_diacritics=0`). Bare whitespace tokens are
            // AND-ed, `"..."` quotes a phrase, and prefix matching
            // requires an explicit trailing `*` in `q` — none of
            // which we translate for the caller here; the raw string
            // reaches SQLite unchanged.
            sql.push_str(
                " AND s.id IN (
                      SELECT rowid FROM symbols_fts
                       WHERE symbols_fts MATCH ?3
                  )",
            );
            bound.push(Box::new(q.to_string()));
        } else {
            // Exact path: match on either the bare name or the fully
            // qualified name so callers can pass whichever form they
            // have without stringifying language-specific separators.
            // Both columns are indexed (`idx_symbols_name`,
            // `idx_symbols_qualified`).
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
        // Two-separator container filter: `::` covers Rust-style
        // qualified names, `.` covers dotted FQNs (Python / Java /
        // Kotlin / Swift / C# / JS). PHP `\` namespaces are not
        // handled by this filter today; a caller wanting members of
        // `App\Models` has to fall back to `query` (exact FQN) or
        // `path_prefix` (colocated namespaces).
        sql.push_str(" AND (s.qualified LIKE ? OR s.qualified LIKE ?)");
        bound.push(Box::new(format!("{c}::%")));
        bound.push(Box::new(format!("{c}.%")));
    }
    if let Some(p) = args.path_prefix.as_deref()
        && !p.is_empty()
    {
        // Treat the caller's prefix literally. `%`, `_`, and the
        // escape character itself otherwise broaden the SQLite LIKE
        // predicate beyond the requested filesystem path.
        sql.push_str(" AND me.path LIKE ? ESCAPE '\\'");
        bound.push(Box::new(format!("{}%", escape_like(p))));
    }
    // `language IS NULL` first so blobs without an analyzer stamp (in
    // practice: no language attribution) sort to the end rather than
    // ahead of alphabetically-earlier known languages. The fuzzy page also
    // breaks ties on `qualified, id`, because symbols declared at one path and
    // line are otherwise indistinguishable here: the local page has to select
    // the same rows every run for the cross-repo merge to trim a stable set.
    if fuzzy_query {
        sql.push_str(
            " ORDER BY fuzzy_rank, language IS NULL, language, me.path, s.line_start, s.qualified, s.id LIMIT ?",
        );
    } else {
        sql.push_str(" ORDER BY language IS NULL, language, me.path, s.line_start LIMIT ?");
    }
    bound.push(Box::new(i64::from(limit)));

    let param_refs: Vec<&dyn ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows: rusqlite::Result<Vec<RankedSymbolHit>> = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(RankedSymbolHit {
                hit: row_to_hit(row)?,
                rank: if fuzzy_query {
                    fuzzy_rank_from_sql(row.get(12)?)
                } else {
                    FuzzyRank::Other
                },
            })
        })?
        .collect();
    Ok(rows?)
}

#[derive(Debug, Clone)]
struct IndexedSymbol {
    hit: SymbolHit,
    raw_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OwnerIdentity {
    parser_id: String,
    qualified: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InheritanceEdge {
    child_qualified: String,
    parent_spelling: String,
    parser_id: String,
    target_symbol_id: Option<i64>,
}

const MANIFEST_INHERITANCE_EDGES_SQL: &str = "
    WITH manifest_blobs AS MATERIALIZED (
        SELECT DISTINCT me.blob_sha
          FROM manifest_entries me
         WHERE me.manifest_id = ?1
    ),
    manifest_sites AS MATERIALIZED (
        SELECT i.id, i.blob_sha, i.parser_id,
               i.type_qualified, i.interface_qualified, i.kind,
               i.interface_byte_start, i.interface_byte_end
          FROM manifest_blobs mb
          JOIN implementations i INDEXED BY idx_impls_blob
            ON i.blob_sha = mb.blob_sha
    ),
    resolved_sites AS MATERIALIZED (
        SELECT s.*,
               COALESCE(
                   (
                       SELECT r.id
                         FROM resolutions r
                              INDEXED BY idx_resolutions_manifest_site
                        WHERE r.manifest_id = ?1
                          AND r.kind = 'type'
                          AND r.site_blob_sha = s.blob_sha
                          AND r.site_parser_id = s.parser_id
                          AND r.site_byte_start = s.interface_byte_start
                          AND r.site_byte_end = s.interface_byte_end
                        ORDER BY {source_rank}, r.id
                        LIMIT 1
                   ),
                   (
                       SELECT r.id
                         FROM resolutions r
                              INDEXED BY idx_resolutions_blob_scoped_site
                        WHERE r.manifest_id IS NULL
                          AND r.kind = 'type'
                          AND r.site_blob_sha = s.blob_sha
                          AND r.site_parser_id = s.parser_id
                          AND r.site_byte_start = s.interface_byte_start
                          AND r.site_byte_end = s.interface_byte_end
                        ORDER BY {source_rank}, r.id
                        LIMIT 1
                   )
               ) AS resolution_id
          FROM manifest_sites s
    )
    SELECT s.type_qualified, s.interface_qualified, s.parser_id,
           r.target_symbol_id
      FROM resolved_sites s
      LEFT JOIN resolutions r
        ON r.id = s.resolution_id
     WHERE COALESCE(r.semantic_kind, s.kind)
           IN ('inherit', 'implement', 'mixin')";

fn manifest_inheritance_edges_sql() -> String {
    MANIFEST_INHERITANCE_EDGES_SQL.replace("{source_rank}", &source_rank_case_sql("r.source"))
}

/// Expand inherited-member ownership from one immutable manifest, then
/// apply the ordinary symbol filters to the complete owner union.
///
/// The two bulk reads below deliberately avoid per-ancestor queries.
/// Edge traversal only decides which already-loaded declaration rows
/// are eligible; query, kind, path, and limit remain result filters.
fn run_find_symbols_inherited(
    conn: &Connection,
    manifest_id: ManifestId,
    args: &FindSymbolsArgs,
    container: &str,
) -> Result<FindSymbolsOutcome> {
    let symbols = load_manifest_symbols(conn, manifest_id)?;
    let edges = load_manifest_inheritance_edges(conn, manifest_id)?;
    let fuzzy_ranks = if args.fuzzy
        && let Some(query) = args.query.as_deref()
        && !query.is_empty()
    {
        Some(load_fuzzy_symbol_ranks(conn, query)?)
    } else {
        None
    };

    let mut owner_symbols_by_id = HashMap::<i64, &IndexedSymbol>::new();
    let mut exact_owners = HashMap::<(String, String), HashMap<i64, &IndexedSymbol>>::new();
    let mut named_owners = HashMap::<(String, String), HashMap<i64, &IndexedSymbol>>::new();
    for symbol in &symbols {
        if !is_type_owner_kind(&symbol.raw_kind) {
            continue;
        }
        owner_symbols_by_id.insert(symbol.hit.id, symbol);
        let family = parser_family(&symbol.hit.parser_id).to_string();
        exact_owners
            .entry((family.clone(), symbol.hit.qualified.clone()))
            .or_default()
            .insert(symbol.hit.id, symbol);
        named_owners
            .entry((family, symbol.hit.name.clone()))
            .or_default()
            .insert(symbol.hit.id, symbol);
    }

    let mut edges_by_child = HashMap::<(String, String), Vec<&InheritanceEdge>>::new();
    for edge in &edges {
        edges_by_child
            .entry((
                parser_family(&edge.parser_id).to_string(),
                edge.child_qualified.clone(),
            ))
            .or_default()
            .push(edge);
    }

    // A container symbol is normally present, but prefix rows keep
    // ownership discoverable for backends that only publish members.
    let mut seeds = HashSet::<OwnerIdentity>::new();
    let mut seed_has_semantic_rows = HashMap::<OwnerIdentity, bool>::new();
    for symbol in &symbols {
        if !belongs_to_container(&symbol.hit.qualified, container)
            && symbol.hit.qualified != container
        {
            continue;
        }
        let seed = OwnerIdentity {
            parser_id: symbol.hit.parser_id.clone(),
            qualified: container.to_string(),
        };
        seed_has_semantic_rows
            .entry(seed.clone())
            .and_modify(|semantic| {
                *semantic |= symbol.hit.source_tier == SourceTier::Semantic;
            })
            .or_insert(symbol.hit.source_tier == SourceTier::Semantic);
        seeds.insert(seed);
    }
    for edge in &edges {
        if edge.child_qualified == container {
            seeds.insert(OwnerIdentity {
                parser_id: edge.parser_id.clone(),
                qualified: container.to_string(),
            });
        }
    }

    // No declaration or relation identifies a parser family. Preserve
    // the ordinary container lookup instead of inventing graph scope.
    if seeds.is_empty() {
        return Ok(FindSymbolsOutcome {
            ranked_hits: run_find_symbols(conn, manifest_id, args)?,
            inheritance_unresolved: false,
            tier2_warming: false,
        });
    }

    let mut selected_owners = HashSet::<OwnerIdentity>::new();
    let mut visited = HashSet::<OwnerIdentity>::new();
    let mut stack = seeds.iter().cloned().collect::<Vec<_>>();
    let mut inheritance_unresolved = false;
    while let Some(owner) = stack.pop() {
        if !visited.insert(owner.clone()) {
            continue;
        }
        selected_owners.insert(owner.clone());

        let family = parser_family(&owner.parser_id).to_string();
        let Some(outgoing) = edges_by_child.get(&(family.clone(), owner.qualified.clone())) else {
            continue;
        };
        for edge in outgoing {
            let target = if let Some(target_id) = edge.target_symbol_id {
                owner_symbols_by_id
                    .get(&target_id)
                    .copied()
                    .filter(|symbol| parser_family(&symbol.hit.parser_id) == family)
                    .map(|symbol| OwnerIdentity {
                        parser_id: symbol.hit.parser_id.clone(),
                        qualified: symbol.hit.qualified.clone(),
                    })
            } else {
                let exact_key = (family.clone(), edge.parent_spelling.clone());
                let candidate = if let Some(candidates) = exact_owners.get(&exact_key) {
                    unique_owner(candidates)
                } else if let Some(candidates) = named_owners.get(&exact_key) {
                    unique_owner(candidates)
                } else {
                    Ok(None)
                };
                match candidate {
                    Ok(Some(symbol)) => Some(OwnerIdentity {
                        parser_id: symbol.hit.parser_id.clone(),
                        qualified: symbol.hit.qualified.clone(),
                    }),
                    Ok(None) => None,
                    Err(()) => {
                        inheritance_unresolved = true;
                        None
                    }
                }
            };

            // A relation back to the same owner is not an ancestor. The
            // visited set independently makes longer cycles finite.
            if let Some(target) = target
                && target.qualified != owner.qualified
            {
                stack.push(target);
            }
        }
    }

    // When a syntactic-only seed has no usable relation row, absence of
    // an ancestor cannot be distinguished from Tier-2 still warming.
    let tier2_warming = seeds.iter().any(|seed| {
        !seed_has_semantic_rows.get(seed).copied().unwrap_or(false)
            && !edges_by_child.contains_key(&(
                parser_family(&seed.parser_id).to_string(),
                seed.qualified.clone(),
            ))
    });

    let mut ranked_hits = symbols
        .into_iter()
        .filter(|symbol| {
            selected_owners.iter().any(|owner| {
                owner.parser_id == symbol.hit.parser_id
                    && belongs_to_container(&symbol.hit.qualified, &owner.qualified)
            })
        })
        .filter(|symbol| matches_result_filters(symbol, args, fuzzy_ranks.as_ref()))
        .map(|symbol| RankedSymbolHit {
            rank: fuzzy_ranks
                .as_ref()
                .and_then(|ranks| ranks.get(&symbol.hit.id))
                .copied()
                .unwrap_or(FuzzyRank::Other),
            hit: symbol.hit,
        })
        .collect::<Vec<_>>();

    // One blob can appear at multiple paths in a manifest. Preserve the
    // existing tie order to choose one mount for each physical symbol, then
    // apply fuzzy rank and the caller's limit to the deduplicated union.
    ranked_hits.sort_by(|a, b| symbol_hit_cmp(&a.hit, &b.hit));
    let mut seen = HashSet::<i64>::new();
    ranked_hits.retain(|ranked| seen.insert(ranked.hit.id));
    ranked_hits.sort_by(ranked_symbol_hit_cmp);
    ranked_hits.truncate(args.limit.unwrap_or(50).max(1) as usize);

    Ok(FindSymbolsOutcome {
        ranked_hits,
        inheritance_unresolved,
        tier2_warming,
    })
}

fn load_manifest_symbols(conn: &Connection, manifest_id: ManifestId) -> Result<Vec<IndexedSymbol>> {
    let mut stmt = conn.prepare(
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
            AND b.parser_id = s.parser_id
          WHERE s.scope = 'top_level'",
    )?;
    let rows = stmt
        .query_map([manifest_id.0], |row| {
            let raw_kind = row.get::<_, String>(3)?;
            Ok(IndexedSymbol {
                hit: SymbolHit {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    qualified: row.get(2)?,
                    kind: symbol_kind_from_str(&raw_kind),
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
                },
                raw_kind,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_manifest_inheritance_edges(
    conn: &Connection,
    manifest_id: ManifestId,
) -> Result<Vec<InheritanceEdge>> {
    let sql = manifest_inheritance_edges_sql();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map([manifest_id.0], |row| {
            Ok(InheritanceEdge {
                child_qualified: row.get(0)?,
                parent_spelling: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                parser_id: row.get(2)?,
                target_symbol_id: row.get(3)?,
            })
        })?
        .filter_map(|row| match row {
            Ok(edge) if !edge.parent_spelling.is_empty() => Some(Ok(edge)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_fuzzy_symbol_ranks(conn: &Connection, query: &str) -> Result<HashMap<i64, FuzzyRank>> {
    let sql = fuzzy_rank_lookup_sql();
    let mut stmt = conn.prepare(&sql)?;
    Ok(stmt
        .query_map([query, query], |row| {
            Ok((row.get(0)?, fuzzy_rank_from_sql(row.get(1)?)))
        })?
        .collect::<rusqlite::Result<HashMap<_, _>>>()?)
}

fn fuzzy_rank_lookup_sql() -> String {
    let rank_sql = FUZZY_RANK_SQL.replace("?2", "?1");
    format!(
        "SELECT s.id, {rank_sql}
           FROM symbols s
          WHERE s.id IN (
                SELECT rowid FROM symbols_fts WHERE symbols_fts MATCH ?2
          )"
    )
}

fn unique_owner<'a>(
    candidates: &'a HashMap<i64, &'a IndexedSymbol>,
) -> std::result::Result<Option<&'a IndexedSymbol>, ()> {
    if candidates.len() == 1 {
        Ok(candidates.values().next().copied())
    } else {
        Err(())
    }
}

fn is_type_owner_kind(kind: &str) -> bool {
    matches!(
        kind,
        "class"
            | "struct"
            | "enum"
            | "union"
            | "trait"
            | "interface"
            | "type_alias"
            | "module"
            | "namespace"
            | "package"
    )
}

fn parser_family(parser_id: &str) -> &str {
    let parser_id = parser_id
        .split_once('@')
        .map_or(parser_id, |(base, _)| base);
    match parser_id {
        "tree-sitter-tsx" => "tree-sitter-typescript",
        "tree-sitter-jsx" => "tree-sitter-javascript",
        _ => parser_id,
    }
}

fn belongs_to_container(qualified: &str, container: &str) -> bool {
    qualified
        .strip_prefix(container)
        .is_some_and(|tail| tail.starts_with("::") || tail.starts_with('.'))
}

fn matches_result_filters(
    symbol: &IndexedSymbol,
    args: &FindSymbolsArgs,
    fuzzy_ranks: Option<&HashMap<i64, FuzzyRank>>,
) -> bool {
    if let Some(query) = args.query.as_deref()
        && !query.is_empty()
    {
        if args.fuzzy {
            if !fuzzy_ranks.is_some_and(|ranks| ranks.contains_key(&symbol.hit.id)) {
                return false;
            }
        } else if symbol.hit.name != query && symbol.hit.qualified != query {
            return false;
        }
    }
    if let Some(kind) = args.kind.as_deref()
        && !kind.is_empty()
        && symbol.raw_kind != kind
    {
        return false;
    }
    if let Some(path) = args.path_prefix.as_deref()
        && !path.is_empty()
        && !symbol.hit.path.starts_with(path)
    {
        return false;
    }
    true
}

fn symbol_hit_cmp(a: &SymbolHit, b: &SymbolHit) -> std::cmp::Ordering {
    language_sort_key(a.language.as_deref())
        .cmp(&language_sort_key(b.language.as_deref()))
        .then_with(|| a.path.cmp(&b.path))
        .then_with(|| a.line.cmp(&b.line))
        .then_with(|| a.qualified.cmp(&b.qualified))
        .then_with(|| a.id.cmp(&b.id))
}

fn ranked_symbol_hit_cmp(a: &RankedSymbolHit, b: &RankedSymbolHit) -> std::cmp::Ordering {
    a.rank_cmp(b).then_with(|| symbol_hit_cmp(&a.hit, &b.hit))
}

fn language_sort_key(language: Option<&str>) -> (bool, &str) {
    match language {
        Some(language) => (false, language),
        None => (true, ""),
    }
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
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

#[cfg(test)]
mod scope_filter_tests {
    use super::*;
    use crate::cas::store;
    use rusqlite::Connection;

    /// Build a minimal store with one manifest, one blob, and two
    /// symbols (one `top_level`, one `nested`) sharing the same name.
    fn fixture_with_top_level_and_nested() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&tmp.path().join("store.db")).unwrap();
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
             VALUES (1, 'src/app.js', 'sha-js')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('sha-js', 'tree-sitter-javascript', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols
               (blob_sha, parser_id, name, qualified, kind, byte_start, byte_end,
                line_start, line_end, source, scope)
             VALUES
               ('sha-js', 'tree-sitter-javascript', 'outer', 'outer', 'function',
                0, 100, 1, 5, 'syntactic', 'top_level'),
               ('sha-js', 'tree-sitter-javascript', 'helper', 'outer.helper', 'function',
                20, 60, 2, 4, 'syntactic', 'nested')",
            [],
        )
        .unwrap();
        (tmp, conn)
    }

    #[test]
    fn find_symbols_excludes_nested_scope_by_default() {
        let (_tmp, conn) = fixture_with_top_level_and_nested();
        let hits = find_symbols(
            &conn,
            &crate::anchor::AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("helper".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            hits.is_empty(),
            "nested helper must not surface as a workspace lookup hit, got {hits:?}"
        );

        // The top-level outer is still reachable.
        let hits = find_symbols(
            &conn,
            &crate::anchor::AnchorName::head(),
            &FindSymbolsArgs {
                query: Some("outer".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "outer");
    }

    /// Ranking the inherited lookup must not add a symbols-table scan: FTS
    /// membership still drives the query and symbols is reached by rowid.
    #[test]
    fn fuzzy_rank_lookup_uses_fts_membership_and_rowid_lookup() {
        let (_tmp, conn) = fixture_with_top_level_and_nested();
        let sql = format!("EXPLAIN QUERY PLAN {}", fuzzy_rank_lookup_sql());
        let mut stmt = conn.prepare(&sql).unwrap();
        let plan = stmt
            .query_map(["outer", "outer"], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        assert!(
            plan.iter()
                .any(|line| line.contains("SCAN symbols_fts VIRTUAL TABLE")),
            "{plan:#?}"
        );
        assert!(
            plan.iter()
                .any(|line| line.contains("SEARCH s USING INTEGER PRIMARY KEY")),
            "{plan:#?}"
        );
        assert!(!plan.iter().any(|line| line == "SCAN s"), "{plan:#?}");
    }
}

#[cfg(test)]
mod inheritance_plan_tests {
    use rusqlite::{Connection, params};

    use super::*;
    use crate::cas::store;

    #[test]
    fn inheritance_edge_plan_is_manifest_and_site_bounded() {
        let (_tmp, mut conn, manifest_id) = fixture_store();
        let expected = load_manifest_inheritance_edges(&conn, manifest_id).unwrap();
        assert_eq!(expected.len(), 1);

        let plan_before = explain_inheritance_plan(&conn, manifest_id);
        assert_manifest_bounded_plan(&plan_before);

        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (777, 'tentative', 0)",
            [],
        )
        .unwrap();
        for ordinal in 0..128 {
            let blob = format!("unrelated-{ordinal:03}");
            let path = format!("noise/{ordinal:03}.ts");
            tx.execute(
                "INSERT INTO blobs
                    (blob_sha, parser_id, parser_revision, parsed_at_ns)
                 VALUES (?1, 'tree-sitter-typescript', 1, 0)",
                [&blob],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
                 VALUES (777, ?1, ?2)",
                params![path, blob],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO implementations
                    (blob_sha, parser_id, type_qualified, interface_qualified,
                     kind, line, interface_byte_start, interface_byte_end)
                 VALUES
                    (?1, 'tree-sitter-typescript', ?2, ?3,
                     'inherit', 1, 10, 20)",
                params![
                    blob,
                    format!("NoiseChild{ordinal}"),
                    format!("NoiseParent{ordinal}")
                ],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO resolutions
                    (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                     kind, semantic_kind, target_symbol_id, source, manifest_id)
                 VALUES
                    (?1, 'tree-sitter-typescript', 10, 20,
                     'type', 'inherit', NULL, 'tier2-direct-noise', NULL)",
                [&blob],
            )
            .unwrap();
        }
        tx.commit().unwrap();

        assert_eq!(
            load_manifest_inheritance_edges(&conn, manifest_id).unwrap(),
            expected,
            "unrelated manifests must not change the selected edge set"
        );
        let plan_after = explain_inheritance_plan(&conn, manifest_id);
        assert_eq!(plan_after, plan_before);
        assert_manifest_bounded_plan(&plan_after);
    }

    fn fixture_store() -> (tempfile::TempDir, Connection, ManifestId) {
        let tmp = tempfile::tempdir().unwrap();
        let conn = store::open(&tmp.path().join("store.db")).unwrap();
        conn.execute(
            "INSERT INTO manifests (manifest_id, kind, built_at_ns)
             VALUES (1, 'tentative', 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO blobs
                (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES ('selected-child', 'tree-sitter-typescript', 1, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (1, 'src/child.ts', 'selected-child')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO implementations
                (blob_sha, parser_id, type_qualified, interface_qualified,
                 kind, line, interface_byte_start, interface_byte_end)
             VALUES
                ('selected-child', 'tree-sitter-typescript', 'Child', 'Base',
                 'inherit', 1, 10, 20)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO resolutions
                (site_blob_sha, site_parser_id, site_byte_start, site_byte_end,
                 kind, semantic_kind, target_symbol_id, source, manifest_id)
             VALUES
                ('selected-child', 'tree-sitter-typescript', 10, 20,
                 'type', 'inherit', NULL, 'tier25-test', 1),
                ('selected-child', 'tree-sitter-typescript', 10, 20,
                 'type', 'inherit', NULL, 'tier2-direct-test', NULL)",
            [],
        )
        .unwrap();
        (tmp, conn, ManifestId(1))
    }

    fn explain_inheritance_plan(conn: &Connection, manifest_id: ManifestId) -> Vec<String> {
        let sql = format!("EXPLAIN QUERY PLAN {}", manifest_inheritance_edges_sql());
        let mut stmt = conn.prepare(&sql).unwrap();
        stmt.query_map([manifest_id.0], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn assert_manifest_bounded_plan(plan: &[String]) {
        let rendered = plan.join("\n");
        assert!(
            plan.iter().any(|detail| {
                detail.contains("manifest_entries") && detail.contains("manifest_id=?")
            }),
            "{rendered}"
        );
        assert!(
            plan.iter()
                .any(|detail| detail.contains("SEARCH i USING INDEX idx_impls_blob")),
            "{rendered}"
        );
        assert!(
            plan.iter().any(|detail| {
                detail.contains("idx_resolutions_manifest_site")
                    && detail.contains("site_blob_sha=?")
            }),
            "{rendered}"
        );
        assert!(
            plan.iter().any(|detail| {
                detail.contains("idx_resolutions_blob_scoped_site")
                    && detail.contains("site_blob_sha=?")
            }),
            "{rendered}"
        );
        assert!(
            !plan.iter().any(|detail| detail.starts_with("SCAN i")),
            "{rendered}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("SCAN resolutions")),
            "{rendered}"
        );
    }
}
