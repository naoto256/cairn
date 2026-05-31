//! `list_repos` — enumerate every registered repo, its languages, and
//! the per-snapshot file / symbol counters.

use cairn_proto::methods::{ListReposResult, RepoEntry, SnapshotEntry};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod};
use crate::{Result, registry_db};

pub struct ListRepos;

#[async_trait::async_trait]
impl DataMethod for ListRepos {
    fn name(&self) -> &'static str {
        "list_repos"
    }

    async fn dispatch(&self, ctx: &DataCtx, _params: Value) -> Result<Value> {
        let repos = ctx
            .storage
            .with_registry(|conn| {
                let repos = registry_db::list_repos(conn)?;
                let mut out = Vec::with_capacity(repos.len());
                for r in repos {
                    let worktrees = registry_db::list_worktrees(conn, r.id)?;
                    let mut snapshot_entries = Vec::new();
                    let mut languages: std::collections::BTreeSet<String> =
                        std::collections::BTreeSet::new();
                    for wt in &worktrees {
                        for snap in registry_db::list_snapshots(conn, wt.id)? {
                            let stats = crate::snapshot_stats::snapshot_stats(
                                std::path::Path::new(&snap.db_path),
                            );
                            for lang in &stats.languages {
                                languages.insert(lang.clone());
                            }
                            snapshot_entries.push(SnapshotEntry {
                                branch: snap.branch.clone(),
                                status: snap.status.as_str().into(),
                                enrichment: snap.enrichment.into(),
                                last_accessed: Some(snap.last_accessed_ns.to_string()),
                                file_count: stats.file_count,
                                symbol_count: stats.symbol_count,
                            });
                        }
                    }
                    out.push(RepoEntry {
                        alias: r.alias,
                        root: r.root_path,
                        languages: languages.into_iter().collect(),
                        snapshots: snapshot_entries,
                    });
                }
                Ok(out)
            })
            .await?;
        Ok(serde_json::to_value(ListReposResult { repos }).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(ListRepos);
