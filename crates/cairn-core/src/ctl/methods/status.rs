//! `status` — daemon health + every CAS-registered repo with the
//! anchors its store knows about.

use cairn_lang_api::{LanguageBackend, all_backends};
use cairn_proto::control::{RepoStatus, SnapshotStatus as ProtoSnapshotStatus, StatusReport};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::enrichment::collect_enrichment;
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
            let backends = all_backends();
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = cas_registry::list_all(&index)?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let store_bytes = std::fs::metadata(&store_path).map(|m| m.len()).unwrap_or(0);
                let conn = cas_store::open(&store_path)?;
                let snapshots = collect_anchor_snapshots(&conn, store_bytes, &backends)?;
                out.push(RepoStatus {
                    alias: entry.alias,
                    root: entry.root_path,
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
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<ProtoSnapshotStatus>> {
    let mut stmt =
        conn.prepare("SELECT anchor_name, manifest_id FROM anchors ORDER BY anchor_name")?;
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
            enrichment: collect_enrichment(conn, manifest_id, backends)?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_lang_api::all_backends;
    use cairn_lang_markdown as _;
    use cairn_lang_python as _;
    use cairn_lang_rust as _;
    use cairn_proto::SourceTier;

    use crate::cas::store;
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    #[test]
    fn status_emits_per_language_enrichment_matrix() {
        let (repo, _sha) = init_repo(&[
            (
                "src/lib.rs",
                "pub trait T {}\npub struct S;\nimpl T for S {}\n",
            ),
            ("script.py", "def greet():\n    return 'hi'\n"),
            ("README.md", "# Hi\n"),
        ]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 1000).unwrap();

        let backends = all_backends();
        let snapshots = collect_anchor_snapshots(&conn, 123, &backends).unwrap();
        let snapshot = snapshots.iter().find(|s| s.branch == "HEAD").unwrap();
        let languages: Vec<&str> = snapshot
            .enrichment
            .iter()
            .map(|e| e.language.as_str())
            .collect();
        assert_eq!(languages, vec!["markdown", "python", "rust"]);

        let rust = snapshot
            .enrichment
            .iter()
            .find(|e| e.language == "rust")
            .unwrap();
        assert!(rust.has_analyzer);
        assert_eq!(rust.tier, SourceTier::Semantic);

        let markdown = snapshot
            .enrichment
            .iter()
            .find(|e| e.language == "markdown")
            .unwrap();
        assert!(!markdown.has_analyzer);
        assert_eq!(markdown.tier, SourceTier::Syntactic);
        assert_eq!(snapshot.size_bytes, 123);
    }
}
