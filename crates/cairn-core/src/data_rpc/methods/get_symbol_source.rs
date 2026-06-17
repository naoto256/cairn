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
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::data_rpc::helpers::{compute_tier3_status_response, parser_id_filter};
use crate::query::{self, SymbolSourceRow};
use crate::register::load_blob_or_worktree;
use crate::{Error, Result};

pub struct GetSymbolSource;

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
        let signature_only = args.signature_only;
        let anchor_arg = args.scope.anchor.clone();
        let branch_arg = args.scope.branch.clone();
        let requested_repo = args.scope.repo.clone();
        let cas_data_dir = ctx.cas_data_dir.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<GetSymbolSourceResult> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let aliases = match requested_repo.as_deref() {
                Some(name) => {
                    let entry = cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
                        Error::RepoNotFound {
                            alias: name.to_string(),
                        }
                    })?;
                    vec![entry]
                }
                None => cas_registry::list_all(&index)?,
            };

            for entry in &aliases {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let conn = cas_store::open(&store_path)?;
                let anchor = crate::anchor::resolve_explicit_or_default(
                    &conn,
                    anchor_arg.as_deref(),
                    branch_arg.as_deref(),
                )?;
                let manifest_id = crate::anchor::resolve(&conn, &anchor)?.ok_or_else(|| {
                    Error::AnchorNotFound {
                        name: anchor.as_str().to_string(),
                    }
                })?;
                let row = match query::get_symbol_source_row(
                    &conn,
                    &anchor,
                    &qualified,
                    file_filter.as_deref(),
                )? {
                    Some(row) => row,
                    None => continue,
                };

                let worktree_root = PathBuf::from(&entry.root_path);
                let source = if signature_only {
                    String::new()
                } else {
                    materialise(&worktree_root, &row)?
                };
                let parser_ids = parser_id_filter(std::iter::once(row.parser_id.clone()));

                return Ok(GetSymbolSourceResult {
                    qualified: row.qualified,
                    name: row.name,
                    kind: row.kind,
                    branch: anchor.as_str().to_string(),
                    location: format!(
                        "{}:{}:{}:{}",
                        entry.alias,
                        anchor.as_str(),
                        row.path,
                        row.line_start
                    ),
                    line_start: row.line_start,
                    line_end: row.line_end,
                    source,
                    signature: row.signature,
                    doc: row.doc,
                    // `get_symbol_source` reads source bytes from the
                    // manifest blob rather than a specific analyzer row.
                    // Until rows carry their originating analyzer tier,
                    // report the source text itself as syntactic.
                    source_tier: SourceTier::Syntactic,
                    tier3_status: compute_tier3_status_response(
                        &conn,
                        manifest_id,
                        Some(&parser_ids),
                        args.tier3.verbose_tier3,
                    )?,
                    timing: cairn_proto::Timing::default(),
                });
            }

            let scope = match requested_repo.as_deref() {
                Some(name) => format!("repo=`{name}`"),
                None => format!("any of {} registered repos", aliases.len()),
            };
            Err(Error::InvalidArgument(format!(
                "no symbol matches qualified=`{qualified}` in {scope}"
            )))
        })
        .await
        .map_err(|e| Error::internal_task_panic("get_symbol_source", e))??;

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
