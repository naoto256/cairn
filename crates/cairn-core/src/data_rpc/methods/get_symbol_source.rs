//! `get_symbol_source` — return the indexed source text of a symbol.
//!
//! Resolves by qualified name (what `find_symbols` / `get_outline`
//! hand back). The CAS path reads the file content via `git cat-file`
//! using the blob_sha the symbol was indexed against; the byte range
//! recorded at parse time always matches that blob.

use std::path::PathBuf;

use cairn_proto::common::SourceTier;
use cairn_proto::methods::{GetSymbolSourceArgs, GetSymbolSourceResult};
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

struct SourceHit {
    repo: String,
    anchor: String,
    row: SymbolSourceRow,
    source: String,
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

        let qualified = args.qualified.clone();
        let file_filter = args.file.clone();
        let exact_file = file_filter.clone();
        let signature_only = args.signature_only;
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
                effective_limit: 1,
                verbose_tier3,
                exact_file,
            },
            move |entry, conn, snapshot| {
                let row = match query::get_symbol_source_row(
                    conn,
                    &snapshot.anchor,
                    &qualified,
                    file_filter.as_deref(),
                )? {
                    Some(row) => row,
                    None => return Ok(Vec::new()),
                };
                let worktree_root = PathBuf::from(&entry.root_path);
                let source = if signature_only {
                    String::new()
                } else {
                    materialise(&worktree_root, &row)?
                };
                Ok(vec![SourceHit {
                    repo: entry.alias.clone(),
                    anchor: snapshot.anchor.as_str().to_string(),
                    row,
                    source,
                }])
            },
            |hits| parser_id_filter(hits.iter().map(|hit| hit.row.parser_id.clone())),
            |_hits: &mut Vec<SourceHit>| {},
        )
        .await?;

        let freshness_issues = execution.freshness_issues;
        let Some(hit) = execution.items.into_iter().next() else {
            if let Some(issue) = freshness_issues.first() {
                return Err(Error::FileNotIndexed {
                    repo: (issue.repo != "*").then(|| issue.repo.clone()),
                    file: args.file.clone().unwrap_or_else(|| "<unspecified>".into()),
                    reason: issue.reason.into(),
                });
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
            source: hit.source,
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
    use serde_json::json;

    use super::*;
    use crate::data_rpc::helpers::test_support;

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
}
