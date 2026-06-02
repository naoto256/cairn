//! `list_repos` — enumerate every registered repo with the anchors
//! its CAS store knows about.

use cairn_lang_api::{LanguageBackend, all_backends};
use cairn_proto::methods::{ListReposResult, RepoEntry, SnapshotEntry};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::enrichment::collect_enrichment;
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
            let backends = all_backends();
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entries = cas_registry::list_all(&index)?;
            let mut out = Vec::with_capacity(entries.len());
            for entry in entries {
                let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
                let conn = cas_store::open(&store_path)?;
                let snapshots = collect_snapshots(&conn, &backends)?;
                out.push(RepoEntry {
                    alias: entry.alias,
                    root: entry.root_path,
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
fn collect_snapshots(
    conn: &rusqlite::Connection,
    backends: &[Box<dyn LanguageBackend>],
) -> Result<Vec<SnapshotEntry>> {
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
            enrichment: collect_enrichment(conn, manifest_id, backends)?,
            last_accessed: Some(crate::timefmt::ns_to_rfc3339_utc(last_ns)),
            file_count: u64::try_from(file_count).unwrap_or(0),
            symbol_count: u64::try_from(symbol_count).unwrap_or(0),
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
    fn list_repos_emits_per_language_enrichment_matrix() {
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
        let snapshots = collect_snapshots(&conn, &backends).unwrap();
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

        let python = snapshot
            .enrichment
            .iter()
            .find(|e| e.language == "python")
            .unwrap();
        assert!(python.has_analyzer);

        let markdown = snapshot
            .enrichment
            .iter()
            .find(|e| e.language == "markdown")
            .unwrap();
        assert!(!markdown.has_analyzer);
        assert_eq!(markdown.tier, SourceTier::Syntactic);
    }
}
