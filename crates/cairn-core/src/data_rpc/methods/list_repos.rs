//! `list_repos` — enumerate every registered repo with the anchors
//! its CAS store knows about.

use cairn_proto::common::SourceTier;
use cairn_proto::methods::{ListReposResult, RepoEntry, SnapshotEntry};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::{Error, Result};

pub struct ListRepos;

#[async_trait::async_trait]
impl DataMethod for ListRepos {
    fn name(&self) -> &'static str {
        "list_repos"
    }

    async fn dispatch(&self, ctx: &DataCtx, _params: Value) -> Result<Value> {
        let cas_data_dir = ctx.cas_data_dir.clone();

        let repos = tokio::task::spawn_blocking(move || -> Result<Vec<RepoEntry>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = cas_registry::list_all(&index)?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let conn = cas_store::open(&store_path)?;
                let snapshots = collect_snapshots(&conn)?;
                let languages = collect_languages(&conn)?;
                out.push(RepoEntry {
                    alias: entry.alias,
                    root: entry.root_path,
                    languages,
                    snapshots,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("list_repos task panicked: {e}")))??;

        Ok(serde_json::to_value(ListReposResult { repos }).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(ListRepos);

/// Build one `SnapshotEntry` per anchor that points at a committed or
/// tentative manifest. Branch-style names lose the `branch/` prefix to
/// match the old wire format (`branch: "main"`); `HEAD` and
/// `tentative/<id>` come through verbatim.
fn collect_snapshots(conn: &rusqlite::Connection) -> Result<Vec<SnapshotEntry>> {
    let mut stmt = conn.prepare(
        "SELECT anchor_name, manifest_id, last_updated_ns
           FROM anchors ORDER BY anchor_name",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::with_capacity(rows.len());
    for (name, manifest_id, last_ns) in rows {
        let file_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM manifest_entries WHERE manifest_id = ?1",
                params![manifest_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let symbol_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols s
                   JOIN manifest_entries me ON me.blob_sha = s.blob_sha
                  WHERE me.manifest_id = ?1",
                params![manifest_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let branch = name
            .strip_prefix("branch/")
            .map_or_else(|| name.clone(), str::to_string);
        out.push(SnapshotEntry {
            branch,
            status: "ready".into(),
            // The CAS path doesn't yet record per-blob source-tier;
            // surface Syntactic as the safe default. A later sweep
            // can derive Semantic when every blob has a semantic
            // analyzer entry.
            enrichment: SourceTier::Syntactic,
            last_accessed: Some(crate::timefmt::ns_to_rfc3339_utc(last_ns)),
            file_count: u64::try_from(file_count).unwrap_or(0),
            symbol_count: u64::try_from(symbol_count).unwrap_or(0),
        });
    }
    Ok(out)
}

/// Distinct languages from the blob parsers, stripped to their
/// short name (`tree-sitter-rust@0.23` → `rust`). Anything that
/// doesn't follow the `tree-sitter-<lang>@` shape comes through
/// verbatim.
fn collect_languages(conn: &rusqlite::Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT DISTINCT parser_id FROM blobs ORDER BY parser_id")?;
    let parser_ids = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(parser_ids
        .into_iter()
        .map(|p| {
            p.strip_prefix("tree-sitter-")
                .and_then(|rest| rest.split('@').next())
                .map_or(p.clone(), str::to_string)
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect())
}
