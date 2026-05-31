//! `status` — daemon health + every CAS-registered repo with the
//! anchors its store knows about.

use cairn_proto::common::SourceTier;
use cairn_proto::control::{RepoStatus, SnapshotStatus as ProtoSnapshotStatus, StatusReport};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::{Error, Result};

struct Status;

#[async_trait::async_trait]
impl ControlMethod for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn dispatch(&self, ctx: &CtlCtx, _params: Value) -> Result<Value> {
        let uptime = ctx.started_at.elapsed().as_secs();
        let version = ctx.version.to_string();
        let cas_data_dir = ctx.cas_data_dir.clone();

        let repos = tokio::task::spawn_blocking(move || -> Result<Vec<RepoStatus>> {
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = cas_registry::list_all(&index)?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let store_bytes = std::fs::metadata(&store_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                let conn = cas_store::open(&store_path)?;
                let snapshots = collect_anchor_snapshots(&conn, store_bytes)?;
                let languages = collect_languages(&conn)?;
                out.push(RepoStatus {
                    alias: entry.alias,
                    root: entry.root_path,
                    languages,
                    snapshots,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("status task panicked: {e}")))??;

        Ok(serde_json::to_value(StatusReport {
            daemon_version: version,
            uptime_secs: uptime,
            repos,
        })
        .unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Status);

fn collect_anchor_snapshots(
    conn: &rusqlite::Connection,
    store_bytes: u64,
) -> Result<Vec<ProtoSnapshotStatus>> {
    let mut stmt = conn.prepare(
        "SELECT anchor_name, manifest_id FROM anchors ORDER BY anchor_name",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::with_capacity(rows.len());
    for (name, manifest_id) in rows {
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
        out.push(ProtoSnapshotStatus {
            branch,
            status: "ready".into(),
            enrichment: SourceTier::Syntactic,
            file_count: u64::try_from(file_count).unwrap_or(0),
            symbol_count: u64::try_from(symbol_count).unwrap_or(0),
            // The CAS store is shared across anchors; reporting the
            // file's size on every anchor row matches the legacy
            // per-snapshot-DB wire shape closely enough for the
            // current dashboards.
            size_bytes: store_bytes,
        });
    }
    Ok(out)
}

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
