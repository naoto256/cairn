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
use crate::query::{self, SymbolSourceRow};
use crate::register::git_cat_file;
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

        let cas_data_dir = ctx.cas_data_dir.clone();
        let qualified = args.qualified.clone();
        let file_filter = args.file.clone();
        let signature_only = args.signature_only;
        let anchor = crate::anchor::resolve_wire(args.anchor.as_deref(), args.branch.as_deref());
        let repo_alias = args.repo.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<GetSymbolSourceResult> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry = cas_registry::lookup_by_alias(&index, &repo_alias)?.ok_or_else(|| {
                Error::InvalidArgument(format!("unknown repo alias: `{repo_alias}`"))
            })?;
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            let worktree_root = PathBuf::from(&entry.root_path);

            let row =
                query::get_symbol_source_row(&conn, &anchor, &qualified, file_filter.as_deref())?
                    .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "no symbol matches qualified=`{qualified}` in repo=`{repo_alias}`"
                    ))
                })?;

            let source = if signature_only {
                String::new()
            } else {
                materialise(&worktree_root, &row)?
            };

            Ok(GetSymbolSourceResult {
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
                source_tier: SourceTier::Syntactic,
            })
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("get_symbol_source task panicked: {e}")))??;

        Ok(serde_json::to_value(result).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetSymbolSource);

/// Slice `row.byte_start..row.byte_end` out of the blob's content.
/// Tries git first (= the indexed blob is the authoritative source);
/// if the blob is not in the object store (e.g. an uncommitted file
/// under a tentative anchor) falls back to reading the file from the
/// worktree.
fn materialise(worktree_root: &std::path::Path, row: &SymbolSourceRow) -> Result<String> {
    let bytes = match git_cat_file(worktree_root, &row.blob_sha) {
        Ok(b) => b,
        Err(_) => std::fs::read(worktree_root.join(&row.path)).map_err(|e| {
            Error::InvalidArgument(format!(
                "get_symbol_source: blob {} not in git and {} unreadable: {e}",
                row.blob_sha,
                worktree_root.join(&row.path).display()
            ))
        })?,
    };
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
