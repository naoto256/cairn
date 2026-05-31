//! `get_outline` — per-file symbol structure.
//!
//! Resolves to the alias's active snapshot (= main worktree's current
//! branch), opens that data DB, and emits every symbol attached to the
//! requested file in line order. Reads from the always-current cairn
//! index, not the filesystem.

use cairn_proto::Completeness;
use cairn_proto::methods::{OutlineArgs, OutlineItem, OutlineResult};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;
use tracing::debug;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params, parse_source_tier};
use crate::indexer::symbol_kind_from_db;
use crate::{Error, Result};

pub struct GetOutline;

#[async_trait::async_trait]
impl DataMethod for GetOutline {
    fn name(&self) -> &'static str {
        "get_outline"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: OutlineArgs = parse_params(params)?;
        let (db_path, repo_alias) = ctx.resolve_active_snapshot(&args.repo).await?;
        let file_arg = args.file.clone();

        let items = tokio::task::spawn_blocking(move || outline_in_db(&db_path, &file_arg))
            .await
            .map_err(|e| Error::InvalidArgument(format!("outline task panicked: {e}")))??;

        debug!(repo = %repo_alias, file = %args.file, count = items.len(), "outline served");
        // Tier-1 only: an outline is symbol definitions from the
        // tree-sitter pass. The semantic layer doesn't add or remove
        // outline entries, so this is always Complete regardless of the
        // snapshot's enrichment tier.
        Ok(serde_json::to_value(OutlineResult {
            items,
            completeness: Completeness::complete(),
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetOutline);

fn outline_in_db(db_path: &std::path::Path, file: &str) -> Result<Vec<OutlineItem>> {
    let conn = crate::data_db::open(db_path)?;
    let file_id: Option<i64> = conn
        .query_row("SELECT id FROM files WHERE path = ?1", params![file], |r| {
            r.get(0)
        })
        .ok();
    let Some(file_id) = file_id else {
        return Ok(Vec::new());
    };

    let mut stmt = conn.prepare(
        "SELECT name, qualified, kind, signature, line_start, doc, source
         FROM symbols WHERE file_id = ?1 ORDER BY line_start",
    )?;
    let rows = stmt
        .query_map(params![file_id], |row| {
            let name: String = row.get(0)?;
            let qualified: String = row.get(1)?;
            let kind_str: String = row.get(2)?;
            let signature: Option<String> = row.get(3)?;
            let line_start: i64 = row.get(4)?;
            let doc: Option<String> = row.get(5)?;
            let source_str: String = row.get(6)?;
            Ok(OutlineItem {
                kind: symbol_kind_from_db(&kind_str),
                name,
                qualified,
                signature,
                line: u32::try_from(line_start).unwrap_or(0),
                doc,
                source: parse_source_tier(&source_str),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
