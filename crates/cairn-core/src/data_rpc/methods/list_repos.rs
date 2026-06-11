//! `list_repos` — enumerate every registered repo with the anchors
//! its CAS store knows about.

use std::collections::BTreeMap;

use cairn_lang_api::{LanguageBackend, all_backends};
use cairn_proto::common::LanguageEnrichment;
use cairn_proto::control::JobSnapshot;
use cairn_proto::methods::{ListReposResult, RepoEntry, SnapshotEntry};
use linkme::distributed_slice;
use rusqlite::params;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod};
use crate::anchor;
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
                let jobs = collect_jobs(&conn, &entry.alias)?;
                out.push(RepoEntry {
                    alias: entry.alias,
                    root: entry.root_path,
                    snapshots,
                    jobs,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::InvalidArgument(format!("list_repos task panicked: {e}")))??;

        Ok(serde_json::to_value(ListReposResult { repos }).unwrap())
    }
}

fn collect_jobs(conn: &rusqlite::Connection, alias: &str) -> Result<Vec<JobSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns, error
         FROM workspace_analysis_runs
         WHERE job_id IS NOT NULL
         ORDER BY job_id DESC",
    )?;
    let jobs = stmt
        .query_map([], |r| {
            Ok(JobSnapshot {
                job_id: r.get(0)?,
                alias: alias.to_string(),
                analyzer_id: r.get(1)?,
                state: r.get(2)?,
                created_at: r.get(3)?,
                started_at: None,
                finished_at: r.get(4)?,
                error: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(jobs)
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(ListRepos);

struct SnapshotAcc {
    internal_names: Vec<String>,
    last_updated_ns: i64,
}

/// Build one `SnapshotEntry` per manifest. Anchor names become labels
/// in `branches`; branch-style names lose the `branch/` prefix while
/// `HEAD` and `tentative/<id>` come through verbatim.
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

    let mut groups: BTreeMap<i64, SnapshotAcc> = BTreeMap::new();
    for (name, manifest_id, last_ns) in rows {
        let acc = groups.entry(manifest_id).or_insert(SnapshotAcc {
            internal_names: Vec::new(),
            last_updated_ns: last_ns,
        });
        acc.internal_names.push(name);
        if last_ns > acc.last_updated_ns {
            acc.last_updated_ns = last_ns;
        }
    }

    let mut entries: Vec<(String, SnapshotEntry)> = Vec::with_capacity(groups.len());
    for (manifest_id, mut acc) in groups {
        acc.internal_names.sort_by_key(|a| anchor::order_key(a));
        let sort_internal = acc.internal_names.first().cloned().unwrap_or_default();
        let branches = acc
            .internal_names
            .iter()
            .map(|n| n.strip_prefix("branch/").unwrap_or(n).to_string())
            .collect();
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
        let enrichment = collect_enrichment(conn, manifest_id, backends)?;
        let status = derive_status(file_count, symbol_count, &enrichment);
        entries.push((
            sort_internal,
            SnapshotEntry {
                branches,
                status,
                enrichment,
                last_accessed: Some(crate::timefmt::ns_to_rfc3339_utc(acc.last_updated_ns)),
                file_count: u64::try_from(file_count).unwrap_or(0),
                symbol_count: u64::try_from(symbol_count).unwrap_or(0),
            },
        ));
    }
    entries.sort_by_key(|(a, _)| anchor::order_key(a));
    Ok(entries.into_iter().map(|(_, e)| e).collect())
}

/// Snapshot status derived from manifest + symbol counts.
///
/// Distinguishes the three "empty-looking" cases callers used to
/// conflate: `empty` (no files in the manifest), `no_analyzer` (only
/// languages without a semantic backend, e.g. all-markdown repo), and
/// `stale` (analyzer-capable files exist but produced zero symbols —
/// typically the index hasn't caught up yet and `reindex_repo` is the
/// fix). `ready` is the steady state.
fn derive_status(file_count: i64, symbol_count: i64, enrichment: &[LanguageEnrichment]) -> String {
    if file_count == 0 {
        return "empty".into();
    }
    if symbol_count > 0 {
        return "ready".into();
    }
    if enrichment.iter().any(|e| e.has_analyzer) {
        "stale".into()
    } else {
        "no_analyzer".into()
    }
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
        let snapshot = snapshots.iter().find(|s| s.has_head()).unwrap();
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

    #[test]
    fn derive_status_distinguishes_empty_stale_and_ready() {
        let none: Vec<LanguageEnrichment> = vec![];
        assert_eq!(derive_status(0, 0, &none), "empty");

        let md_only = vec![LanguageEnrichment {
            language: "markdown".into(),
            tier: SourceTier::Syntactic,
            has_analyzer: false,
        }];
        assert_eq!(derive_status(3, 0, &md_only), "no_analyzer");

        let rust = vec![LanguageEnrichment {
            language: "rust".into(),
            tier: SourceTier::Semantic,
            has_analyzer: true,
        }];
        assert_eq!(derive_status(3, 0, &rust), "stale");
        assert_eq!(derive_status(3, 7, &rust), "ready");
    }

    #[test]
    fn list_repos_dedups_anchors_by_manifest_id() {
        let (repo, _sha) = init_repo(&[("src/lib.rs", "pub fn f() {}\n")]);
        let db_tmp = tempfile::tempdir().unwrap();
        let mut conn = store::open(&db_tmp.path().join("store.db")).unwrap();
        register_repo(&mut conn, repo.path(), 1000).unwrap();

        let backends = all_backends();
        let snapshots = collect_snapshots(&conn, &backends).unwrap();

        let head_main = snapshots.iter().find(|s| s.has_head()).unwrap();
        assert_eq!(head_main.branches, vec!["HEAD", "main"]);

        let tentative = snapshots
            .iter()
            .find(|s| s.branches.iter().any(|b| b.starts_with("tentative/")))
            .unwrap();
        assert_eq!(tentative.branches.len(), 1);
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots[0].has_head());
    }
}
