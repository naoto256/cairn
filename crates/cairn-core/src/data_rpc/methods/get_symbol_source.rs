//! `get_symbol_source` — return the indexed source text of a symbol.
//!
//! Resolves by qualified name (what `find_symbols` / `get_outline`
//! hand back). The CAS path reads the file content via `git cat-file`
//! using the blob_sha the symbol was indexed against; the byte range
//! recorded at parse time always matches that blob.

use std::collections::BTreeSet;
use std::path::PathBuf;

use cairn_proto::common::SourceTier;
use cairn_proto::methods::{GetSymbolSourceArgs, GetSymbolSourceResult, SymbolSourceCandidate};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use crate::data_rpc::helpers::{
    EmissionContext, QueryArgsView, QueryToolKind, SnapshotQueryRequest,
    build_snapshot_aware_feedback, completeness_for_snapshot_scan, parser_id_filter,
    query_one_or_all_snapshots,
};
use crate::query::{self, SymbolSourceRow};
use crate::register::load_blob_or_worktree;
use crate::{Error, Result};

pub struct GetSymbolSource;

const AMBIGUITY_CANDIDATE_LIMIT: usize = 20;
const EXECUTION_CANDIDATE_LIMIT: u32 = (AMBIGUITY_CANDIDATE_LIMIT + 1) as u32;
const STORE_CANDIDATE_PROBE_LIMIT: usize = AMBIGUITY_CANDIDATE_LIMIT + 2;

struct SourceHit {
    repo: String,
    repo_hash: String,
    manifest_id: i64,
    anchor: String,
    worktree_root: PathBuf,
    row: SymbolSourceRow,
}

impl SourceHit {
    fn to_wire_candidate(&self) -> SymbolSourceCandidate {
        SymbolSourceCandidate {
            repo: self.repo.clone(),
            branch: self.anchor.clone(),
            file: self.row.path.clone(),
            line_start: self.row.line_start,
            line_end: self.row.line_end,
            kind: self.row.kind.clone(),
        }
    }
}

#[async_trait::async_trait]
impl DataMethod for GetSymbolSource {
    fn name(&self) -> &'static str {
        "get_symbol_source"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: GetSymbolSourceArgs = parse_params(params)?;
        if args.qualified.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "get_symbol_source: `qualified` must be non-empty".into(),
            ));
        }
        if matches!(args.line, Some(0)) {
            return Err(Error::InvalidParams(
                "get_symbol_source: `line` must be a 1-indexed value >= 1".into(),
            ));
        }
        if args.line.is_some() && args.file.is_none() {
            return Err(Error::InvalidParams(
                "get_symbol_source: `line` requires `file`".into(),
            ));
        }

        let qualified = args.qualified.clone();
        let file_filter = args.file.clone();
        let line_filter = args.line;
        let exact_file = file_filter.clone();
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();
        let verbose_tier3 = args.tier3.verbose_tier3;
        let execution = query_one_or_all_snapshots(
            ctx,
            SnapshotQueryRequest {
                requested_repo: requested_repo.clone(),
                anchor: anchor_arg,
                branch: branch_arg,
                method_name: "get_symbol_source",
                effective_limit: EXECUTION_CANDIDATE_LIMIT,
                verbose_tier3,
                exact_file,
            },
            move |entry, conn, snapshot| {
                let rows = query::get_symbol_source_rows(
                    conn,
                    &snapshot.anchor,
                    &qualified,
                    file_filter.as_deref(),
                    line_filter,
                    STORE_CANDIDATE_PROBE_LIMIT,
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| SourceHit {
                        repo: entry.alias.clone(),
                        repo_hash: entry.repo_hash.clone(),
                        manifest_id: snapshot.manifest_id.0,
                        anchor: snapshot.anchor.as_str().to_string(),
                        worktree_root: PathBuf::from(&entry.root_path),
                        row,
                    })
                    .collect())
            },
            |hits| parser_id_filter(hits.iter().map(|hit| hit.row.parser_id.clone())),
            finalize_source_hits,
        )
        .await?;

        let freshness_issues = execution.freshness_issues;
        let mut hits = execution.items;
        if hits.len() > 1 {
            let candidates_truncated = execution.capped || hits.len() > AMBIGUITY_CANDIDATE_LIMIT;
            hits.truncate(AMBIGUITY_CANDIDATE_LIMIT);
            return Err(Error::AmbiguousSource {
                qualified: args.qualified,
                candidates: hits.iter().map(SourceHit::to_wire_candidate).collect(),
                candidates_truncated,
            });
        }
        let Some(hit) = hits.pop() else {
            if let Some(issue) = freshness_issues.first() {
                let repo = (issue.repo != "*").then(|| issue.repo.clone());
                return match args.file.clone() {
                    Some(file) => Err(Error::FileNotIndexed {
                        repo,
                        file,
                        reason: issue.reason.into(),
                    }),
                    None => Err(Error::SnapshotStale {
                        repo,
                        reason: issue.reason.into(),
                    }),
                };
            }
            let scope = requested_repo
                .as_deref()
                .map(|name| format!("repo=`{name}`"))
                .unwrap_or_else(|| "registered repositories".into());
            return Err(Error::InvalidArgument(format!(
                "no symbol matches qualified=`{}` in {scope}",
                args.qualified
            )));
        };
        let tier3_status = execution.tier3_status;
        let completeness = completeness_for_snapshot_scan(
            execution.capped,
            execution.skipped_unavailable,
            &freshness_issues,
        );
        let emission_ctx = EmissionContext {
            tool: QueryToolKind::GetSymbolSource,
            items_empty: false,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: args.scope.repo.as_deref(),
                fuzzy: true,
                kind: false,
                container: None,
                file: args.file.as_deref(),
                ..QueryArgsView::default()
            },
        };
        let (diagnostics, hints) =
            build_snapshot_aware_feedback(&emission_ctx, &freshness_issues, execution.capped);
        let row = hit.row;
        let source = if args.signature_only {
            String::new()
        } else {
            materialise(&hit.worktree_root, &row)?
        };
        let result = GetSymbolSourceResult {
            qualified: row.qualified,
            name: row.name,
            kind: row.kind,
            branch: hit.anchor.clone(),
            location: format!(
                "{}:{}:{}:{}",
                hit.repo, hit.anchor, row.path, row.line_start
            ),
            line_start: row.line_start,
            line_end: row.line_end,
            source,
            signature: row.signature,
            doc: row.doc,
            // Source bytes come from the manifest blob, not one analyzer row.
            source_tier: SourceTier::Syntactic,
            completeness,
            tier3_status,
            diagnostics,
            hints,
            timing: cairn_proto::Timing::default(),
        };

        Ok(serde_json::to_value(result).unwrap())
    }
}

fn finalize_source_hits(hits: &mut Vec<SourceHit>) {
    hits.sort_by(|left, right| {
        (
            &left.repo,
            &left.anchor,
            &left.row.path,
            left.row.line_start,
            left.row.byte_start,
            left.row.byte_end,
            &left.row.blob_sha,
        )
            .cmp(&(
                &right.repo,
                &right.anchor,
                &right.row.path,
                right.row.line_start,
                right.row.byte_start,
                right.row.byte_end,
                &right.row.blob_sha,
            ))
    });
    let mut seen = BTreeSet::new();
    hits.retain(|hit| {
        seen.insert((
            hit.repo_hash.clone(),
            hit.manifest_id,
            hit.row.path.clone(),
            hit.row.blob_sha.clone(),
            hit.row.byte_start,
            hit.row.byte_end,
        ))
    });
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetSymbolSource);

/// Slice `row.byte_start..row.byte_end` out of the blob's content,
/// loaded via the shared `register::load_blob_or_worktree` helper (git
/// cat-file first, worktree fallback for tentative-anchor blobs).
fn materialise(worktree_root: &std::path::Path, row: &SymbolSourceRow) -> Result<String> {
    let bytes = load_blob_or_worktree(worktree_root, &row.blob_sha, &row.path).map_err(|e| {
        Error::InvalidArgument(format!(
            "get_symbol_source: blob {} not in git and {} unreadable: {e}",
            row.blob_sha,
            worktree_root.join(&row.path).display()
        ))
    })?;
    if row.byte_end > bytes.len() || row.byte_start > row.byte_end {
        return Err(Error::InvalidArgument(format!(
            "get_symbol_source: blob {} shorter than indexed byte range ({}..{} vs {} bytes); reindex needed",
            row.blob_sha,
            row.byte_start,
            row.byte_end,
            bytes.len()
        )));
    }
    Ok(String::from_utf8_lossy(&bytes[row.byte_start..row.byte_end]).into_owned())
}

#[cfg(test)]
mod tests {
    use rusqlite::params;
    use serde_json::json;

    use super::*;
    use crate::cas::registry as cas_registry;
    use crate::data_rpc::helpers::test_support;

    fn mark_fixture_stale(fixture: &test_support::DataRpcFixture) {
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = applied_generation + 1
                 WHERE repo_hash = ?1",
                params![entry.repo_hash],
            )
            .unwrap();
    }

    fn ambiguous_fixture() -> test_support::DataRpcFixture {
        test_support::registered_fixture_with_files(&[
            ("src/a.rs", "pub fn duplicate() { println!(\"a\"); }\n"),
            ("src/b.rs", "pub fn duplicate() { println!(\"b\"); }\n"),
        ])
    }

    #[tokio::test]
    async fn duplicate_qualified_declarations_return_typed_ambiguity_before_source_io() {
        let fixture = ambiguous_fixture();
        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "duplicate"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::AmbiguousSource {
                qualified,
                candidates,
                candidates_truncated: false,
            } if qualified == "duplicate"
                && candidates.len() == 2
                && candidates[0].file == "src/a.rs"
                && candidates[1].file == "src/b.rs"
        ));
    }

    #[tokio::test]
    async fn ambiguity_candidates_are_capped_without_weakening_cardinality() {
        let owned = (0..21)
            .map(|index| {
                (
                    format!("src/{index:02}.rs"),
                    "pub fn duplicate() {}\n".to_string(),
                )
            })
            .collect::<Vec<_>>();
        let files = owned
            .iter()
            .map(|(path, contents)| (path.as_str(), contents.as_str()))
            .collect::<Vec<_>>();
        let fixture = test_support::registered_fixture_with_files(&files);

        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "duplicate"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::AmbiguousSource {
                candidates,
                candidates_truncated: true,
                ..
            } if candidates.len() == AMBIGUITY_CANDIDATE_LIMIT
                && candidates[0].file == "src/00.rs"
                && candidates[19].file == "src/19.rs"
        ));
    }

    #[tokio::test]
    async fn aliases_for_one_repository_do_not_duplicate_physical_declarations() {
        let fixture = test_support::registered_fixture();
        let mut index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "demo-alt",
            &entry.root_path,
            &entry.repo_hash,
            entry.registered_at_ns,
        )
        .unwrap();
        tx.commit().unwrap();

        let value = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "qualified": "target"
                }),
            )
            .await
            .unwrap();

        assert_eq!(value["qualified"], "target");
    }

    #[tokio::test]
    async fn file_and_one_indexed_line_select_one_declaration() {
        let fixture = ambiguous_fixture();
        let value = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "duplicate",
                    "file": "src/a.rs",
                    "line": 1
                }),
            )
            .await
            .unwrap();

        assert_eq!(value["location"], "demo:tentative/1:src/a.rs:1");
        assert!(
            value["source"]
                .as_str()
                .unwrap()
                .contains("println!(\"a\")")
        );
    }

    #[tokio::test]
    async fn zero_line_is_rejected_as_invalid_params() {
        let fixture = ambiguous_fixture();
        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "duplicate",
                    "file": "src/a.rs",
                    "line": 0
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::InvalidParams(message) if message.contains("1-indexed")));
    }

    #[tokio::test]
    async fn line_without_file_is_rejected_as_invalid_params() {
        let fixture = ambiguous_fixture();
        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "duplicate",
                    "line": 1
                }),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::InvalidParams(message) if message.contains("requires `file`"))
        );
    }

    #[tokio::test]
    async fn explicit_missing_file_returns_typed_file_not_indexed() {
        let fixture = test_support::registered_fixture();
        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "crate::missing",
                    "file": "src/not-indexed.rs"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::FileNotIndexed {
                repo: Some(repo),
                file,
                reason
            } if repo == "demo"
                && file == "src/not-indexed.rs"
                && reason == "file_not_indexed"
        ));
    }

    #[tokio::test]
    async fn fresh_member_qualified_miss_keeps_existing_not_found_contract() {
        let fixture = test_support::registered_fixture();
        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "crate::missing",
                    "file": "src/lib.rs"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn fresh_qualified_miss_without_file_keeps_existing_not_found_contract() {
        let fixture = test_support::registered_fixture();
        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "crate::missing"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn stale_qualified_miss_without_file_returns_snapshot_stale() {
        let fixture = test_support::registered_fixture();
        mark_fixture_stale(&fixture);

        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "crate::missing"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::SnapshotStale {
                repo: Some(repo),
                reason
            } if repo == "demo" && reason == "reconcile_generation_gap"
        ));
    }

    #[tokio::test]
    async fn stale_qualified_miss_with_file_keeps_file_not_indexed_contract() {
        let fixture = test_support::registered_fixture();
        mark_fixture_stale(&fixture);

        let err = GetSymbolSource
            .dispatch(
                &fixture.ctx,
                json!({
                    "repo": "demo",
                    "qualified": "crate::missing",
                    "file": "src/lib.rs"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::FileNotIndexed {
                repo: Some(repo),
                file,
                reason
            } if repo == "demo"
                && file == "src/lib.rs"
                && reason == "reconcile_generation_gap"
        ));
    }
}
