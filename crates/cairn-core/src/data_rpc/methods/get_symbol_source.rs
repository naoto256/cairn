//! `get_symbol_source` — return the indexed source text of a symbol.
//!
//! Resolves by qualified name (what `find_symbols` / `get_outline`
//! hand back). The daemon looks up the byte range recorded at index
//! time and reads the file from disk; if the file moved or the
//! working tree drifted from the snapshot, the call surfaces an
//! error rather than guessing.

use std::path::PathBuf;

use cairn_proto::common::SymbolKind;
use cairn_proto::methods::{GetSymbolSourceArgs, GetSymbolSourceResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params, parse_source_tier};
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
        let repo_alias = args.repo.clone();
        let targets = ctx
            .snapshot_targets(&repo_alias, args.branch.as_deref())
            .await?;
        if targets.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "no snapshot matches repo=`{repo_alias}`{}",
                args.branch
                    .as_deref()
                    .map(|b| format!(" branch=`{b}`"))
                    .unwrap_or_default()
            )));
        }
        let qualified = args.qualified.clone();
        let file_filter = args.file.clone();
        let signature_only = args.signature_only;
        let repo_for_hits = repo_alias.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<GetSymbolSourceResult> {
            // Walk targets in order; first match wins. Cross-branch is
            // the default so we keep scanning until we hit a snapshot
            // that has this symbol.
            for t in &targets {
                if let Some(hit) = source_in_snapshot(
                    &t.db_path,
                    &t.worktree_root,
                    &repo_for_hits,
                    &t.branch,
                    &qualified,
                    file_filter.as_deref(),
                    signature_only,
                )? {
                    return Ok(hit);
                }
            }
            Err(Error::InvalidArgument(format!(
                "no symbol matches qualified=`{qualified}` in repo=`{repo_for_hits}`"
            )))
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("get_symbol_source task panicked: {e}")))??;
        Ok(serde_json::to_value(result).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(GetSymbolSource);

fn source_in_snapshot(
    db_path: &std::path::Path,
    worktree_root: &std::path::Path,
    repo_alias: &str,
    branch: &str,
    qualified: &str,
    file_filter: Option<&str>,
    signature_only: bool,
) -> Result<Option<GetSymbolSourceResult>> {
    let conn = crate::data_db::open(db_path)?;
    let mut sql = String::from(
        "SELECT s.name, s.kind, s.signature, s.doc, s.byte_start, s.byte_end,
                s.line_start, s.line_end, s.source, f.path
           FROM symbols s
           JOIN files f ON f.id = s.file_id
          WHERE s.qualified = ?1",
    );
    if file_filter.is_some() {
        sql.push_str(" AND f.path = ?2");
    }
    sql.push_str(" LIMIT 1");

    let mut stmt = conn.prepare(&sql)?;
    struct Row {
        name: String,
        kind_db: String,
        signature: Option<String>,
        doc: Option<String>,
        byte_start: i64,
        byte_end: i64,
        line_start: i64,
        line_end: i64,
        src: String,
        path: String,
    }
    let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Row> {
        Ok(Row {
            name: row.get(0)?,
            kind_db: row.get(1)?,
            signature: row.get(2)?,
            doc: row.get(3)?,
            byte_start: row.get(4)?,
            byte_end: row.get(5)?,
            line_start: row.get(6)?,
            line_end: row.get(7)?,
            src: row.get(8)?,
            path: row.get(9)?,
        })
    };
    let row_result: Option<Row> = match file_filter {
        Some(f) => stmt
            .query_row(rusqlite::params![qualified, f], map_row)
            .ok(),
        None => stmt.query_row(rusqlite::params![qualified], map_row).ok(),
    };
    let Some(row) = row_result else {
        return Ok(None);
    };
    let Row {
        name,
        kind_db,
        signature,
        doc,
        byte_start,
        byte_end,
        line_start,
        line_end,
        src,
        path,
    } = row;
    // Avoid the dead-store warning when signature_only short-circuits.
    let _ = (byte_start, byte_end);

    // `signature_only` short-circuits the file read entirely. The
    // signature + doc already live in the symbols table; consumers
    // wanting the full body call again without the flag. `source` is
    // returned as an empty string so the field stays present on the
    // wire but no body bytes are paid for.
    let source: String = if signature_only {
        String::new()
    } else {
        // Reading from the live worktree is intentional: the watcher
        // keeps the index ≤ ~250 ms behind disk so the byte range we
        // recorded should still address the same span. If the file
        // moved or shrank we report a clear error instead of silently
        // slicing unrelated bytes — guarding against truncation also
        // catches the race where a file is edited mid-call.
        let file_abs: PathBuf = worktree_root.join(&path);
        let bytes = std::fs::read(&file_abs).map_err(|e| {
            Error::InvalidArgument(format!(
                "get_symbol_source: failed to read {}: {e}",
                file_abs.display()
            ))
        })?;
        let bs = usize::try_from(byte_start).unwrap_or(0);
        let be = usize::try_from(byte_end).unwrap_or(0);
        if be > bytes.len() || bs > be {
            return Err(Error::InvalidArgument(format!(
                "get_symbol_source: file {} shorter than indexed byte range ({bs}..{be} vs {} bytes); reindex needed",
                file_abs.display(),
                bytes.len()
            )));
        }
        String::from_utf8_lossy(&bytes[bs..be]).into_owned()
    };

    let kind: SymbolKind = serde_json::from_value(serde_json::Value::String(kind_db.clone()))
        .unwrap_or(SymbolKind::Other(kind_db));

    Ok(Some(GetSymbolSourceResult {
        qualified: qualified.to_string(),
        name,
        kind,
        branch: branch.to_string(),
        location: format!("{repo_alias}:{branch}:{path}:{line_start}"),
        line_start: u32::try_from(line_start).unwrap_or(0),
        line_end: u32::try_from(line_end).unwrap_or(0),
        source,
        signature,
        doc,
        source_tier: parse_source_tier(&src),
    }))
}
