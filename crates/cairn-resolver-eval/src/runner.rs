//! Drives one `GoldenCase` end-to-end:
//!
//! 1. Build a live fixture (copy + `git init` + commit).
//! 2. Open a fresh CAS store (sqlite, in tempdir).
//! 3. `register_repo` — runs Tier-2 syntactic analyzers in-process.
//! 4. Execute the case's query against the public `cairn-core` API.
//! 5. Project results into [`ActualHit`] and score against
//!    `tier2_expected` / `tier3_expected`.
//!
//! Tier-3 (LSP) is **not** driven in-process — see lib docs. The
//! `tier3` field on the returned report scores the empty actual set
//! against `tier3_expected` so a future runner upgrade can fill it
//! in without changing case definitions.

use anyhow::{Context, Result, bail};
use cairn_core::anchor::AnchorName;
use cairn_core::cas::store;
use cairn_core::query::{
    FindReferencesArgs, FindSubtypesArgs, FindSupertypesArgs, FindSymbolsArgs, find_references,
    find_subtypes, find_supertypes, find_symbols,
};
use cairn_core::register::register_repo;
use cairn_proto::methods::ReferenceDirection;

use crate::fixture;
use crate::report::{EvalReport, TierReport};
use crate::types::{ActualHit, GoldenCase, Tool};

/// Run one case and produce a full `EvalReport`. Errors bubble up
/// (build / register / sqlite); the caller decides whether a low
/// recall is a hard failure.
pub fn run_case(case: &GoldenCase) -> Result<EvalReport> {
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

    let actual = run_tool(&conn, case)?;
    let tier2 = TierReport::score(&case.tier2_expected, &actual);
    // Tier-2.5 placeholder: the cross-file syntactic resolver is not
    // wired into this in-process runner yet (Session B will land
    // `cairn-lang-ruby-tier25` and its peers). Score against an empty
    // actual set so cases that pin Tier-2.5 rows surface as `missing`
    // until the resolver attaches.
    let tier25 = TierReport::score(&case.tier25_expected, &[]);
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

fn run_tool(conn: &rusqlite::Connection, case: &GoldenCase) -> Result<Vec<ActualHit>> {
    let anchor = AnchorName::head();
    let symbol = case.query.symbol.as_deref().unwrap_or_default().to_string();
    let limit = case.query.limit;

    let hits: Vec<ActualHit> = match case.tool {
        Tool::FindCallers => {
            let refs = find_references(
                conn,
                &anchor,
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
                })
                .collect()
        }
        Tool::FindCallees => {
            let refs = find_references(
                conn,
                &anchor,
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
                })
                .collect()
        }
        Tool::FindSubtypes => {
            let rows = find_subtypes(
                conn,
                &anchor,
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
                })
                .collect()
        }
        Tool::FindSupertypes => {
            let rows = find_supertypes(
                conn,
                &anchor,
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
                })
                .collect()
        }
        Tool::FindSymbols => {
            let rows = find_symbols(
                conn,
                &anchor,
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
                })
                .collect()
        }
    };

    Ok(hits)
}
