//! Drives one `GoldenCase` end-to-end:
//!
//! 1. Build a live fixture (copy + `git init` + commit).
//! 2. Open a fresh CAS store (sqlite, in tempdir).
//! 3. `register_repo` — runs Tier-2 syntactic analyzers in-process and,
//!    via `run_registered_workspace_analyzers`, every registered
//!    Tier-2.5 workspace analyzer whose crate is linked into this
//!    binary. Their resolutions are persisted before the query runs.
//! 4. Execute the case's query against the public `cairn-core` API.
//! 5. Project results into [`ActualHit`] and score against
//!    `tier2_expected` / `tier25_expected` / `tier3_expected`.
//!
//! Tier-3 (LSP) is **not** driven in-process — see lib docs. The
//! `tier3` field on the returned report scores the empty actual set
//! against `tier3_expected` so a future runner upgrade can fill it
//! in without changing case definitions.
//!
//! Query verbs join the best covering `resolutions` row through the
//! `best_resolution` CTE and rank it with `source_rank_case_sql`.
//! `ReferenceHit` and `ImplHit` therefore expose `kind_source`, which
//! lets Tier-2.5 scoring retain only `tier25-*` rows (plus higher-rank
//! `tier3-*` rows) while Tier-2 scoring continues to observe all rows.

use anyhow::{Context, Result, bail};
use cairn_core::anchor::AnchorName;
use cairn_core::cas::store;
use cairn_core::query::{
    FindImportsArgs, FindReferencesArgs, FindSubtypesArgs, FindSupertypesArgs, FindSymbolsArgs,
    find_imports, find_references, find_subtypes, find_supertypes, find_symbols,
};
use cairn_core::register::register_repo;
use cairn_core::workspace_analyzer::all_workspace_analyzers;
use cairn_proto::methods::ReferenceDirection;

use crate::fixture;
use crate::report::{EvalReport, TierReport};
use crate::types::{ActualHit, GoldenCase, Tool};

/// Run one case and produce a full `EvalReport`. Errors bubble up
/// (build / register / sqlite); the caller decides whether a low
/// recall is a hard failure.
pub fn run_case(case: &GoldenCase) -> Result<EvalReport> {
    crate::force_link_tier25_analyzers();

    let tier25_analyzer_id = all_workspace_analyzers()
        .into_iter()
        .find(|analyzer| analyzer.language() == case.language)
        .map(|analyzer| analyzer.id());
    if !case.tier25_expected.is_empty() && tier25_analyzer_id.is_none() {
        bail!(
            "no Tier-2.5 workspace analyzer linked for {}",
            case.language
        );
    }

    let live = fixture::build(case.language).context("build fixture")?;
    let db_tmp = tempfile::tempdir().context("db tempdir")?;
    let mut conn = store::open(&db_tmp.path().join("store.db")).context("open CAS store")?;

    let outcome = register_repo(&mut conn, &live.repo_root, 0).context("register fixture repo")?;
    if outcome.blobs_parsed == 0 {
        bail!(
            "register_repo parsed zero blobs for {} — backend not linked?",
            case.language
        );
    }

    if let Some(analyzer_id) = tier25_analyzer_id {
        let (status, error): (String, Option<String>) = conn
            .query_row(
                "SELECT status, error FROM workspace_analysis_runs
                 WHERE manifest_id = ?1 AND analyzer_id = ?2",
                rusqlite::params![outcome.tentative_manifest.0, analyzer_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| format!("read {analyzer_id} run status"))?;
        if status != "succeeded" {
            bail!(
                "Tier-2.5 analyzer {analyzer_id} for {} finished as {status}: {}",
                case.language,
                error.as_deref().unwrap_or("no error recorded")
            );
        }

        if !case.tier25_expected.is_empty() {
            let source = format!("tier25-{analyzer_id}");
            let resolution_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM resolutions WHERE source = ?1",
                [&source],
                |row| row.get(0),
            )?;
            if resolution_count == 0 {
                bail!(
                    "Tier-2.5 analyzer {analyzer_id} for {} succeeded but persisted zero resolutions",
                    case.language
                );
            }
        }
    }

    let actual = run_tool(&conn, &AnchorName::tentative(outcome.worktree_id), case)?;
    let tier2 = TierReport::score(&case.tier2_expected, &actual);
    let tier25 = TierReport::score_tier25(&case.tier25_expected, &actual);
    // Tier-3 placeholder: no LSP-driven path yet, so score against an
    // empty actual set. When a Tier-3 runner lands, swap `&[]` for
    // its output here.
    let tier3 = TierReport::score(&case.tier3_expected, &[]);

    Ok(EvalReport {
        case: case.name,
        language: case.language,
        tier2,
        tier25,
        tier3,
    })
}

fn run_tool(
    conn: &rusqlite::Connection,
    anchor: &AnchorName,
    case: &GoldenCase,
) -> Result<Vec<ActualHit>> {
    let symbol = case.query.symbol.as_deref().unwrap_or_default().to_string();
    let limit = case.query.limit;

    let hits: Vec<ActualHit> = match case.tool {
        Tool::FindCallers => {
            let refs = find_references(
                conn,
                anchor,
                &FindReferencesArgs {
                    symbol,
                    direction: ReferenceDirection::Incoming,
                    kind: None,
                    include_noise: false,
                    limit,
                },
            )?;
            refs.into_iter()
                .map(|r| ActualHit {
                    path: r.path,
                    line: r.line,
                    target_qualified: r.target_qualified.unwrap_or_else(|| r.target_name.clone()),
                    parser_id: r.parser_id,
                    kind_source: Some(r.kind_source),
                })
                .collect()
        }
        Tool::FindCallees => {
            let refs = find_references(
                conn,
                anchor,
                &FindReferencesArgs {
                    symbol,
                    direction: ReferenceDirection::Outgoing,
                    kind: None,
                    include_noise: false,
                    limit,
                },
            )?;
            refs.into_iter()
                .map(|r| ActualHit {
                    path: r.path,
                    line: r.line,
                    target_qualified: r.target_qualified.unwrap_or_else(|| r.target_name.clone()),
                    parser_id: r.parser_id,
                    kind_source: Some(r.kind_source),
                })
                .collect()
        }
        Tool::FindImports => {
            let rows = find_imports(
                conn,
                anchor,
                &FindImportsArgs {
                    file: case.query.symbol.clone(),
                    limit,
                },
            )?;
            rows.into_iter()
                .map(|r| ActualHit {
                    path: r.file,
                    line: r.line,
                    target_qualified: r.target_path.unwrap_or(r.to_module),
                    parser_id: r.parser_id,
                    kind_source: Some(r.kind_source),
                })
                .collect()
        }
        Tool::FindSubtypes => {
            let rows = find_subtypes(
                conn,
                anchor,
                &FindSubtypesArgs {
                    name: symbol,
                    limit,
                },
            )?;
            rows.into_iter()
                .map(|r| ActualHit {
                    path: r.path,
                    line: r.line,
                    target_qualified: r.type_qualified,
                    parser_id: r.parser_id,
                    kind_source: Some(r.kind_source),
                })
                .collect()
        }
        Tool::FindSupertypes => {
            let rows = find_supertypes(
                conn,
                anchor,
                &FindSupertypesArgs {
                    name: symbol,
                    limit,
                },
            )?;
            rows.into_iter()
                .map(|r| ActualHit {
                    path: r.path,
                    line: r.line,
                    target_qualified: r
                        .interface_qualified
                        .unwrap_or_else(|| r.type_qualified.clone()),
                    parser_id: r.parser_id,
                    kind_source: Some(r.kind_source),
                })
                .collect()
        }
        Tool::FindSymbols => {
            let rows = find_symbols(
                conn,
                anchor,
                &FindSymbolsArgs {
                    query: case.query.symbol.clone(),
                    kind: case.query.kind.clone(),
                    fuzzy: false,
                    container: None,
                    path_prefix: None,
                    limit,
                },
            )?;
            rows.into_iter()
                .map(|r| ActualHit {
                    path: r.path,
                    line: r.line,
                    target_qualified: r.qualified,
                    parser_id: r.parser_id,
                    kind_source: None,
                })
                .collect()
        }
    };

    Ok(hits)
}
