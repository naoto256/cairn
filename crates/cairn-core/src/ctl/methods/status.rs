//! `status` — daemon health + registered repos + per-snapshot stats.

use cairn_proto::control::{RepoStatus, SnapshotStatus as ProtoSnapshotStatus, StatusReport};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::{Result, registry_db};

struct Status;

#[async_trait::async_trait]
impl ControlMethod for Status {
    fn name(&self) -> &'static str {
        "status"
    }

    async fn dispatch(&self, ctx: &CtlCtx, _params: Value) -> Result<Value> {
        let uptime = ctx.started_at.elapsed().as_secs();
        let version = ctx.version.to_string();
        let report = ctx
            .storage
            .with_registry(move |conn| {
                let mut repos: Vec<RepoStatus> = Vec::new();
                for r in registry_db::list_repos(conn)? {
                    let mut snapshots: Vec<ProtoSnapshotStatus> = Vec::new();
                    let mut languages: std::collections::BTreeSet<String> =
                        std::collections::BTreeSet::new();
                    for wt in registry_db::list_worktrees(conn, r.id)? {
                        for snap in registry_db::list_snapshots(conn, wt.id)? {
                            let stats = crate::snapshot_stats::snapshot_stats(
                                std::path::Path::new(&snap.db_path),
                            );
                            for lang in &stats.languages {
                                languages.insert(lang.clone());
                            }
                            snapshots.push(ProtoSnapshotStatus {
                                branch: snap.branch,
                                status: snap.status.as_str().to_string(),
                                enrichment: snap.enrichment.into(),
                                file_count: stats.file_count,
                                symbol_count: stats.symbol_count,
                                size_bytes: stats.size_bytes,
                            });
                        }
                    }
                    repos.push(RepoStatus {
                        alias: r.alias,
                        root: r.root_path,
                        languages: languages.into_iter().collect(),
                        snapshots,
                    });
                }
                Ok(StatusReport {
                    daemon_version: version,
                    uptime_secs: uptime,
                    repos,
                })
            })
            .await?;
        Ok(serde_json::to_value(report).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Status);
