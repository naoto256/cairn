//! `repo_status` — detailed status for one registered repository.

use cairn_lang_api::all_backends;
use cairn_proto::RepoReconcileStatus;
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
                match (args.scope.repo.as_deref(), args.path.as_deref()) {
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
            // PR3 Phase 4: durable reconcile state. If the
            // `repositories` row exists (v4 always seeds a
            // reconcile_state row alongside it), we lift the row
            // into `RepoReconcileStatus`. If the row is somehow
            // missing (should be impossible under Phase 1's FK
            // cascade + seed), we fail-closed rather than
            // synthesising a clean state — the caller asked for
            // exactly one repo and missing state is DB corruption.
            let reconcile = match cas_registry::get_reconcile_state(&index, &entry.repo_hash)? {
                Some(state) => {
                    let aliases = cas_registry::aliases_for_repo(&index, &entry.repo_hash)?;
                    Some(reconcile_state_to_wire(&entry.repo_hash, aliases, &state))
                }
                None => {
                    return Err(Error::Internal(format!(
                        "repo_reconcile_state row missing for repo_hash={}",
                        entry.repo_hash
                    )));
                }
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
                reconcile,
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

    // PR3 Phase 4 hints from durable reconcile state.
    if let Some(r) = &repo.reconcile {
        if r.watcher_state == "failed" {
            hints.push(Hint {
                code: HintCode::ReconcileWatcherFailed,
                message: format!(
                    "Filesystem watcher for `{}` is in failed state: {}. Future file events for this repo will NOT be observed until the daemon restarts.",
                    repo.alias,
                    r.watcher_error.as_deref().unwrap_or("no error text"),
                ),
                action: None,
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: Some(repo.alias.clone()),
            });
        }
        if r.attempt_generation.is_some() {
            hints.push(Hint {
                code: HintCode::ReconcileAttemptInProgress,
                message: format!(
                    "Reconcile worker is running attempt for generation {} on `{}`.",
                    r.attempt_generation.unwrap_or_default(),
                    repo.alias
                ),
                action: Some(HintAction::WaitForIndex),
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: Some(repo.alias.clone()),
            });
        } else if r.pending {
            hints.push(Hint {
                code: HintCode::ReconcilePending,
                message: format!(
                    "Repository `{}` has a dirty gap (desired={} > applied={}) — the reconcile manager will pick it up on the next wake or startup.",
                    repo.alias, r.desired_generation, r.applied_generation
                ),
                action: Some(HintAction::WaitForIndex),
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: Some(repo.alias.clone()),
            });
        }
        if r.retry_scheduled && r.consecutive_failures > 0 {
            hints.push(Hint {
                code: HintCode::ReconcileRetryWait,
                message: format!(
                    "Last {} reconcile attempts failed on `{}`: {}. Next retry is scheduled.",
                    r.consecutive_failures,
                    repo.alias,
                    r.last_error.as_deref().unwrap_or("no error text"),
                ),
                action: Some(HintAction::WaitForIndex),
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: Some(repo.alias.clone()),
            });
        }
    }
    hints
}

/// Map a Phase 1 `RepoReconcileState` row into the wire type.
/// Shared by data-RPC and control status paths so both surfaces
/// produce identical objects for the same underlying row.
pub(crate) fn reconcile_state_to_wire(
    repo_hash: &str,
    aliases: Vec<String>,
    state: &cas_registry::RepoReconcileState,
) -> RepoReconcileStatus {
    let pending =
        state.desired_generation > state.applied_generation || state.attempt_generation.is_some();
    let retry_scheduled = state.next_retry_at_ns.is_some();
    RepoReconcileStatus {
        repo_hash: repo_hash.to_string(),
        aliases,
        desired_generation: state.desired_generation,
        applied_generation: state.applied_generation,
        force_generation: state.force_generation,
        attempt_generation: state.attempt_generation,
        dirty_since_ns: state.dirty_since_ns,
        last_attempt_ns: state.last_attempt_ns,
        last_success_ns: state.last_success_ns,
        consecutive_failures: state.consecutive_failures,
        next_retry_at_ns: state.next_retry_at_ns,
        last_error: state.last_error.clone(),
        watcher_state: state.watcher_state.as_db_str().to_string(),
        watcher_error: state.watcher_error.clone(),
        pending,
        retry_scheduled,
    }
}

fn validate_repo_status_args(args: &RepoStatusArgs) -> Result<()> {
    match (args.scope.repo.as_ref(), args.path.as_ref()) {
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
    use cairn_proto::methods::RepoScope;

    #[test]
    fn repo_status_requires_exactly_one_of_repo_or_path() {
        assert!(validate_repo_status_args(&RepoStatusArgs::default()).is_err());
        assert!(
            validate_repo_status_args(&RepoStatusArgs {
                scope: RepoScope {
                    repo: Some("demo".into()),
                },
                path: Some("/tmp/demo".into()),
                ..RepoStatusArgs::default()
            })
            .is_err()
        );
        assert!(
            validate_repo_status_args(&RepoStatusArgs {
                scope: RepoScope {
                    repo: Some("demo".into()),
                },
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
            reconcile: None,
        }
    }

    // ─── PR3 Phase 4 wire/data MF suite ───────────────────────────

    fn reconcile_entry_with_state(state: RepoReconcileStatus) -> RepoStatusEntry {
        let mut e = repo_status_entry("ready", TierStatusBody::ready());
        e.reconcile = Some(state);
        e
    }

    fn baseline_reconcile() -> RepoReconcileStatus {
        RepoReconcileStatus {
            repo_hash: "h".into(),
            aliases: vec!["demo".into()],
            desired_generation: 0,
            applied_generation: 0,
            force_generation: 0,
            attempt_generation: None,
            dirty_since_ns: None,
            last_attempt_ns: None,
            last_success_ns: None,
            consecutive_failures: 0,
            next_retry_at_ns: None,
            last_error: None,
            watcher_state: "active".into(),
            watcher_error: None,
            pending: false,
            retry_scheduled: false,
        }
    }

    /// MF-5a: pending reconcile state emits `reconcile_pending`
    /// hint.
    #[test]
    fn mf5_repo_status_emits_reconcile_pending_hint() {
        let repo = reconcile_entry_with_state(RepoReconcileStatus {
            desired_generation: 3,
            applied_generation: 1,
            pending: true,
            ..baseline_reconcile()
        });
        let hints = repo_status_hints(&repo);
        let hint = hints
            .iter()
            .find(|h| h.code == HintCode::ReconcilePending)
            .expect("pending hint must fire");
        assert!(hint.message.contains("desired=3"));
        assert!(hint.message.contains("applied=1"));
    }

    /// MF-5b: watcher_state == failed emits
    /// `reconcile_watcher_failed` hint with watcher_error.
    #[test]
    fn mf5_repo_status_emits_watcher_failed_hint() {
        let repo = reconcile_entry_with_state(RepoReconcileStatus {
            watcher_state: "failed".into(),
            watcher_error: Some("git open failed".into()),
            ..baseline_reconcile()
        });
        let hints = repo_status_hints(&repo);
        let hint = hints
            .iter()
            .find(|h| h.code == HintCode::ReconcileWatcherFailed)
            .expect("watcher failed hint must fire");
        assert!(hint.message.contains("git open failed"));
    }

    /// MF-5c: retry backoff emits `reconcile_retry_wait` with the
    /// error text.
    #[test]
    fn mf5_repo_status_emits_retry_wait_hint() {
        let repo = reconcile_entry_with_state(RepoReconcileStatus {
            desired_generation: 2,
            applied_generation: 1,
            pending: true,
            consecutive_failures: 3,
            next_retry_at_ns: Some(99_999),
            last_error: Some("EMFILE".into()),
            retry_scheduled: true,
            ..baseline_reconcile()
        });
        let hints = repo_status_hints(&repo);
        let hint = hints
            .iter()
            .find(|h| h.code == HintCode::ReconcileRetryWait)
            .expect("retry wait hint must fire");
        assert!(hint.message.contains("EMFILE"));
    }

    /// MF-5d: attempt in flight emits
    /// `reconcile_attempt_in_progress` (informational) and NOT
    /// the pending hint — the attempt is what pending would
    /// tell the user to wait for.
    #[test]
    fn mf5_repo_status_attempt_in_progress_hides_pending_hint() {
        let repo = reconcile_entry_with_state(RepoReconcileStatus {
            desired_generation: 5,
            applied_generation: 2,
            attempt_generation: Some(5),
            pending: true,
            ..baseline_reconcile()
        });
        let hints = repo_status_hints(&repo);
        assert!(
            hints
                .iter()
                .any(|h| h.code == HintCode::ReconcileAttemptInProgress)
        );
        assert!(
            hints.iter().all(|h| h.code != HintCode::ReconcilePending),
            "attempt in flight makes the pending hint redundant"
        );
    }
}
