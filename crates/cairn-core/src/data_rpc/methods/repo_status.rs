//! `repo_status` — detailed status for one registered repository.

use cairn_lang_api::all_backends;
use cairn_proto::common::{
    AnalyzerState, Diagnostic, Hint, HintAction, HintCode, TierRepoStatus, TierStatusBody,
};
use cairn_proto::methods::{RepoStatusArgs, RepoStatusEntry, RepoStatusResult};
use linkme::distributed_slice;
use serde_json::Value;

use super::super::{DATA_METHODS, DataCtx, DataMethod, parse_params};
use super::list_repos::{collect_repo_snapshot_summary, resolve_repo_by_path};
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::data_rpc::helpers::compute_tier_status;
use crate::{Error, Result};

pub struct RepoStatus;

#[async_trait::async_trait]
impl DataMethod for RepoStatus {
    fn name(&self) -> &'static str {
        "repo_status"
    }

    async fn dispatch(&self, ctx: &DataCtx, params: Value) -> Result<Value> {
        let args: RepoStatusArgs = parse_params(params)?;
        validate_repo_status_args(&args)?;
        let cas_data_dir = ctx.cas_data_dir.clone();

        let repo = tokio::task::spawn_blocking(move || -> Result<RepoStatusEntry> {
            let backends = all_backends();
            let index = cas_registry::open(&cas_data_dir.index_db_path())?;
            let entry =
                match (args.repo.as_deref(), args.path.as_deref()) {
                    (Some(alias), None) => cas_registry::lookup_by_alias(&index, alias)?
                        .ok_or_else(|| Error::RepoNotFound {
                            alias: alias.to_string(),
                        })?,
                    (None, Some(path)) => resolve_repo_by_path(&index, std::path::Path::new(path))?
                        .ok_or_else(|| Error::RepoNotFound {
                            alias: path.to_string(),
                        })?,
                    _ => unreachable!("validated above"),
                };
            let conn = cas_store::open(&cas_data_dir.store_db_path(&entry.repo_hash))?;
            let summary = collect_repo_snapshot_summary(&conn, &backends)?;
            let tier3_status = match summary.current_manifest_id {
                Some(manifest_id) => {
                    let status = compute_tier_status(&conn, manifest_id)?;
                    TierRepoStatus {
                        this_repo: status.this_query.clone(),
                        repo_wide: args.tier3.verbose_tier3.then_some(status.this_query),
                    }
                }
                None => TierRepoStatus {
                    this_repo: TierStatusBody::ready(),
                    repo_wide: args.tier3.verbose_tier3.then_some(TierStatusBody::ready()),
                },
            };
            Ok(RepoStatusEntry {
                alias: entry.alias,
                root: entry.root_path,
                languages: summary.languages,
                summary: summary.summary,
                current: summary.current,
                tier3_status,
                snapshots: if args.include_snapshots {
                    summary.snapshots
                } else {
                    Vec::new()
                },
            })
        })
        .await
        .map_err(|e| Error::internal_task_panic("repo_status", e))??;

        let hints = repo_status_hints(&repo);
        Ok(serde_json::to_value(RepoStatusResult {
            repo,
            diagnostics: Vec::<Diagnostic>::new(),
            hints,
            timing: cairn_proto::Timing::default(),
        })
        .unwrap())
    }
}

fn repo_status_hints(repo: &RepoStatusEntry) -> Vec<Hint> {
    let mut hints = Vec::new();
    if repo.current.status == "stale" {
        hints.push(Hint {
            code: HintCode::SnapshotStale,
            message: format!(
                "Current snapshot is stale. The watcher may be lagging; wait or run `cairn ctl repo reindex {}`.",
                repo.alias
            ),
            action: None,
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some(repo.alias.clone()),
        });
    }

    if repo
        .tier3_status
        .this_repo
        .analyzers
        .iter()
        .any(|analyzer| {
            matches!(
                analyzer.state,
                AnalyzerState::Queued | AnalyzerState::Running
            )
        })
    {
        hints.push(Hint {
            code: HintCode::Tier3IndexingWait,
            message: "Tier-3 indexing is still running for this repository.".into(),
            action: Some(HintAction::WaitForIndex),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some("tier3".into()),
        });
    }
    hints
}

fn validate_repo_status_args(args: &RepoStatusArgs) -> Result<()> {
    match (args.repo.as_ref(), args.path.as_ref()) {
        (Some(_), None) | (None, Some(_)) => Ok(()),
        (None, None) => Err(Error::InvalidParams(
            "repo_status requires exactly one of `repo` or `path`".into(),
        )),
        (Some(_), Some(_)) => Err(Error::InvalidParams(
            "repo_status accepts `repo` or `path`, not both".into(),
        )),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(DATA_METHODS)]
static REGISTER: fn() -> Box<dyn DataMethod> = || Box::new(RepoStatus);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_rpc::helpers::test_support;
    use cairn_proto::common::{TierAnalyzerStatus, default_tier};

    #[test]
    fn repo_status_requires_exactly_one_of_repo_or_path() {
        assert!(validate_repo_status_args(&RepoStatusArgs::default()).is_err());
        assert!(
            validate_repo_status_args(&RepoStatusArgs {
                repo: Some("demo".into()),
                path: Some("/tmp/demo".into()),
                ..RepoStatusArgs::default()
            })
            .is_err()
        );
        assert!(
            validate_repo_status_args(&RepoStatusArgs {
                repo: Some("demo".into()),
                ..RepoStatusArgs::default()
            })
            .is_ok()
        );
    }

    #[tokio::test]
    async fn repo_status_path_resolves_to_alias() {
        let fixture = test_support::registered_fixture();
        let repo_path = fixture._repo.path().join("src");
        let result = RepoStatus
            .dispatch(
                &fixture.ctx,
                serde_json::json!({"path": repo_path.to_string_lossy()}),
            )
            .await
            .unwrap();
        let result: RepoStatusResult = serde_json::from_value(result).unwrap();
        assert_eq!(result.repo.alias, "demo");
    }

    #[test]
    fn repo_status_emits_snapshot_stale_when_current_is_stale() {
        let repo = repo_status_entry("stale", TierStatusBody::ready());

        let hints = repo_status_hints(&repo);
        let hint = hints
            .iter()
            .find(|hint| hint.code == HintCode::SnapshotStale)
            .unwrap();
        assert!(hint.action.is_none());
        assert!(hint.message.contains("cairn ctl repo reindex demo"));
    }

    #[test]
    fn repo_status_emits_tier3_indexing_wait_when_analyzer_running() {
        let repo = repo_status_entry(
            "ready",
            TierStatusBody::from_analyzers(vec![TierAnalyzerStatus {
                id: Some("rust-analyzer-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Running,
                reason_code: None,
                reason: None,
            }]),
        );

        let hints = repo_status_hints(&repo);
        let hint = hints
            .iter()
            .find(|hint| hint.code == HintCode::Tier3IndexingWait)
            .unwrap();
        assert_eq!(hint.action, Some(HintAction::WaitForIndex));
        assert_eq!(hint.target.as_deref(), Some("tier3"));
    }

    #[tokio::test]
    async fn repo_status_returns_repo_not_found_for_path_outside_registry() {
        let fixture = test_support::registered_fixture();
        let err = RepoStatus
            .dispatch(
                &fixture.ctx,
                serde_json::json!({"path": "/tmp/cairn-definitely-not-registered"}),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, crate::Error::RepoNotFound { .. }));
    }

    fn repo_status_entry(current_status: &str, this_repo: TierStatusBody) -> RepoStatusEntry {
        RepoStatusEntry {
            alias: "demo".into(),
            root: "/tmp/demo".into(),
            languages: vec!["rust".into()],
            summary: cairn_proto::methods::RepoStatusSummary {
                snapshot_count: 1,
                ready_snapshot_count: u32::from(current_status == "ready"),
                stale_snapshot_count: u32::from(current_status == "stale"),
                current_file_count: 1,
                current_symbol_count: u64::from(current_status == "ready"),
            },
            current: cairn_proto::methods::RepoStatusCurrent {
                anchor: "HEAD".into(),
                status: current_status.into(),
            },
            tier3_status: TierRepoStatus {
                this_repo,
                repo_wide: None,
            },
            snapshots: Vec::new(),
        }
    }
}
