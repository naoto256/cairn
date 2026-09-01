use super::*;

#[cfg(test)]
thread_local! {
    static AFTER_QUARANTINE_OBSERVATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
fn observe_after_quarantine_read_for_test() {
    AFTER_QUARANTINE_OBSERVATION_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

#[cfg(not(test))]
fn observe_after_quarantine_read_for_test() {}

impl JobManager {
    /// One worker task. All workers compete for dispatches on the
    /// shared receiver (serialized by the async mutex); the loop
    /// ends when the dispatch channel closes during shutdown. A
    /// failed run is logged and the loop continues — and
    /// `notify_worker_finished` is sent on success *and* failure so
    /// the scheduler frees the worker slot, pool-group slot, and
    /// tracked de-dup key in either outcome (the notify itself is
    /// best-effort and may be dropped during shutdown).
    pub(super) async fn worker_loop(self: Arc<Self>) {
        loop {
            let dispatch = {
                let mut receiver = self.worker_receiver.lock().await;
                receiver.recv().await
            };
            let Some(dispatch) = dispatch else {
                break;
            };
            let job_id = dispatch.job.id;
            let pool_group = dispatch.pool_group;
            let key = dispatch.key.clone();
            if let Err(err) = self.run_job(dispatch).await {
                warn!(
                    error = %err,
                    sqlite_code = ?err.sqlite_error_code(),
                    sqlite_extended_code = ?err.sqlite_extended_code(),
                    "analyzer job failed"
                );
            }
            self.notify_worker_finished(job_id, pool_group, key);
        }
    }

    async fn run_job(&self, dispatch: DispatchJob) -> Result<()> {
        // Hold a lifecycle lease (when wired) for the whole run so
        // repository removal cannot proceed underneath the analyzer;
        // the lease moves into the blocking task and is dropped when
        // the run returns.
        let lease = self
            .lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.acquire_by_repo_hash(&dispatch.job.repo_hash))
            .transpose()?;
        let runtime_metrics = self.runtime_metrics.clone();
        let job_id = dispatch.job.id;
        #[cfg(test)]
        let terminal_identity = (
            dispatch.job.repo_hash.clone(),
            dispatch.job.store_path.clone(),
            dispatch.job.manifest_id,
            dispatch.job.analyzer_id.clone(),
        );
        let progress_metrics = runtime_metrics.clone();
        let progress =
            crate::workspace_analyzer::AnalyzerProgress::with_observer(Arc::new(move |ticks| {
                progress_metrics.mark_progress(job_id, ticks);
            }));
        // Register the progress handle before the blocking run starts
        // so `begin_shutdown` / `cancel` can reach this job; if
        // admission is already closed, registration cancels the
        // handle immediately and the run below observes it at its
        // first cancellation check.
        self.register_active_progress(job_id, progress.clone());
        let joined = tokio::task::spawn_blocking(move || {
            run_job_blocking(dispatch.job, runtime_metrics, progress, lease)
        })
        .await;
        self.unregister_active_progress(job_id);
        #[cfg(not(test))]
        return finalize_joined_run(&self.runtime_metrics, job_id, joined);
        #[cfg(test)]
        {
            let result = finalize_joined_run(&self.runtime_metrics, job_id, joined);
            record_terminal_after_worker(terminal_identity, job_id, result.as_ref().err()).await;
            result
        }
    }

    /// Report a finished dispatch back to the scheduler so it frees
    /// the worker slot / pool group and releases the tracked key.
    /// Best-effort: during shutdown the sender may already be taken
    /// or the scheduler gone, and the send is dropped silently.
    fn notify_worker_finished(&self, job_id: JobId, pool_group: Option<&'static str>, key: JobKey) {
        let sender = self
            .scheduler_sender
            .lock()
            .expect("job scheduler sender lock poisoned");
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(SchedulerMsg::WorkerFinished {
                job_id,
                pool_group,
                key,
            });
        }
    }

    /// Tell the scheduler a queued job was cancelled so it is
    /// dropped from the pending lanes before dispatch. Best-effort,
    /// same as `notify_worker_finished`.
    pub(super) fn notify_cancelled_job(&self, job_id: JobId) {
        let sender = self
            .scheduler_sender
            .lock()
            .expect("job scheduler sender lock poisoned");
        if let Some(sender) = sender.as_ref() {
            let _ = sender.send(SchedulerMsg::Cancel(job_id));
        }
    }
}

#[cfg(test)]
async fn record_terminal_after_worker(
    identity: (
        String,
        std::path::PathBuf,
        crate::manifest::ManifestId,
        String,
    ),
    job_id: JobId,
    worker_error: Option<&Error>,
) {
    record_terminal_after_worker_with_probe(identity, job_id, worker_error.is_some(), || {}).await;
}

#[cfg(test)]
async fn record_terminal_after_worker_with_probe<F>(
    identity: (
        String,
        std::path::PathBuf,
        crate::manifest::ManifestId,
        String,
    ),
    job_id: JobId,
    worker_error: bool,
    probe: F,
) where
    F: FnOnce() + Send + 'static,
{
    let failure_identity = identity.clone();
    let durable = tokio::task::spawn_blocking(move || {
        probe();
        let (_, store_path, manifest_id, analyzer_id) = identity;
        cas_store::open_existing(&store_path).and_then(|conn| {
            conn.query_row(
                "SELECT status, error IS NOT NULL
             FROM workspace_analysis_runs
             WHERE job_id = ?1 AND manifest_id = ?2 AND analyzer_id = ?3",
                params![job_id, manifest_id.0, analyzer_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
            )
            .map_err(Error::from)
        })
    })
    .await;
    let (repo_hash, _, _, analyzer_id) = failure_identity;
    match durable {
        Ok(Ok((status, has_error))) => crate::churn_recorder::record_terminal(
            &repo_hash,
            &analyzer_id,
            job_id,
            &status,
            has_error.then_some("analyzer_failed"),
        ),
        Ok(Err(_)) | Err(_) => crate::churn_recorder::record_observation_failure(
            &repo_hash,
            crate::churn_recorder::ObservationFailureKind::TerminalRowUnavailable,
            None,
            Some(&analyzer_id),
            Some(job_id),
        ),
    }
    if worker_error {
        crate::churn_recorder::record_terminal(
            &repo_hash,
            &analyzer_id,
            job_id,
            "worker_error",
            Some("worker_error"),
        );
    }
}

/// Collapse the blocking worker's two failure channels into the
/// runtime-metrics terminal transition.
///
/// `run_job_blocking` reports ordinary failures through its inner
/// [`Result`], while panics surface as a [`tokio::task::JoinError`].
/// Both mark an unfinished run failed; a concurrent terminal state
/// such as cancellation remains authoritative.
fn finalize_joined_run(
    runtime_metrics: &JobRuntimeMetricsStore,
    job_id: JobId,
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match joined {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            runtime_metrics.mark_failed_if_unfinished(job_id);
            Err(err)
        }
        Err(err) => {
            runtime_metrics.mark_failed_if_unfinished(job_id);
            Err(Error::internal_task_panic("analyzer job", err))
        }
    }
}

/// Synchronous body of one analyzer run, executed on the blocking
/// pool. Re-reads the durable row before doing anything: the state
/// at dispatch time wins over the (possibly stale) memory-queue
/// entry. `_lease` pins the repository's lifecycle for the whole
/// run and is released on return.
fn run_job_blocking(
    job: Job,
    runtime_metrics: JobRuntimeMetricsStore,
    progress: crate::workspace_analyzer::AnalyzerProgress,
    _lease: Option<crate::lifecycle::RepoLease>,
) -> Result<()> {
    let mut conn = cas_store::open_existing(&job.store_path)?;
    // The IMMEDIATE transaction is the dispatch authority. Manifest currency,
    // expected membership, revision/config identity, cancellation, and the
    // exact queued row are checked under the same store write lock. Only the
    // unique Queued -> Running update grants permission to continue.
    let claim = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let durable: Option<(String, i64, i64, String)> = claim
        .query_row(
            "SELECT status, cancel_requested, analyzer_revision, config_hash
             FROM workspace_analysis_runs
             WHERE job_id = ?1 AND manifest_id = ?2 AND analyzer_id = ?3",
            params![job.id, job.manifest_id.0, job.analyzer_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((state, cancel_requested, stored_revision, stored_config_hash)) = durable else {
        claim.commit()?;
        runtime_metrics.mark_finished(job.id, "superseded");
        return Ok(());
    };
    if state != RunStatus::Queued.as_str() {
        claim.commit()?;
        runtime_metrics.mark_finished(job.id, "superseded");
        return Ok(());
    }

    // Resolve the linked analyzer only after proving that this dispatch still
    // owns the exact durable Queued row. A recovered job for an analyzer that
    // no longer exists must retire that row instead of returning an error and
    // leaving it queued for the next restart to advertise again.
    let Some(analyzer) = all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == job.analyzer_id)
    else {
        let reason = "linked analyzer is no longer available";
        let changed = claim.execute(
            "UPDATE workspace_analysis_runs
             SET status = 'skipped', finished_at_ns = ?1, error = ?2
             WHERE job_id = ?3 AND manifest_id = ?4 AND analyzer_id = ?5
               AND status = 'queued' AND cancel_requested = ?6
               AND analyzer_revision = ?7 AND config_hash = ?8",
            params![
                now_ns(),
                reason,
                job.id,
                job.manifest_id.0,
                job.analyzer_id,
                cancel_requested,
                stored_revision,
                stored_config_hash,
            ],
        )?;
        claim.commit()?;
        runtime_metrics.mark_finished(
            job.id,
            if changed == 1 {
                RunStatus::Skipped.as_str()
            } else {
                "superseded"
            },
        );
        return Ok(());
    };
    let current_config_hash = config_hash(&job.repo_root, analyzer.config_paths());

    // Quarantine is durable reconcile scheduling state, not a repository-wide
    // activity lock. Observe it immediately before the store claim: a state
    // already visible here suppresses this recovered attempt, while a
    // quarantine committed after admission may coexist with the current run.
    // Read failure is therefore not promoted into a new exclusion authority.
    let quarantined_before_claim = match cas_registry::open(&job.index_db_path)
        .and_then(|index| cas_registry::get_reconcile_state(&index, &job.repo_hash))
    {
        Ok(state) => state.is_some_and(|state| state.quarantined_at_ns.is_some()),
        Err(error) => {
            warn!(
                repo_hash = %job.repo_hash,
                error = %error,
                "could not observe quarantine before analyzer claim"
            );
            false
        }
    };
    observe_after_quarantine_read_for_test();
    if progress.is_cancelled() || cancel_requested != 0 {
        let changed = claim.execute(
            "UPDATE workspace_analysis_runs
             SET status = 'cancelled', finished_at_ns = ?1
             WHERE job_id = ?2 AND manifest_id = ?3 AND analyzer_id = ?4
               AND status = 'queued' AND cancel_requested = ?5
               AND analyzer_revision = ?6 AND config_hash = ?7",
            params![
                now_ns(),
                job.id,
                job.manifest_id.0,
                job.analyzer_id,
                cancel_requested,
                stored_revision,
                stored_config_hash,
            ],
        )?;
        claim.commit()?;
        runtime_metrics.mark_finished(
            job.id,
            if changed == 1 {
                RunStatus::Cancelled.as_str()
            } else {
                "superseded"
            },
        );
        return Ok(());
    }
    let current_manifest = crate::anchor::resolve_tentative_manifest_id(&claim, &job.repo_root)?;
    let current_and_expected = if current_manifest == Some(job.manifest_id) {
        expected_analyzers_for_manifest(&claim, job.manifest_id)?
            .iter()
            .any(|candidate| candidate.id() == job.analyzer_id)
    } else {
        false
    };
    let currency_reason = if quarantined_before_claim {
        Some("repository was quarantined before dispatch admission")
    } else if current_manifest != Some(job.manifest_id) {
        Some("current manifest changed before dispatch admission")
    } else if !current_and_expected {
        Some("analyzer is not expected for the current manifest")
    } else if u32::try_from(stored_revision).ok() != Some(analyzer.revision()) {
        Some("analyzer revision changed before dispatch admission")
    } else if stored_config_hash != current_config_hash {
        Some("analyzer configuration changed before dispatch admission")
    } else {
        None
    };
    if let Some(reason) = currency_reason {
        let changed = claim.execute(
            "UPDATE workspace_analysis_runs
             SET status = 'skipped', finished_at_ns = ?1, error = ?2
             WHERE job_id = ?3 AND manifest_id = ?4 AND analyzer_id = ?5
               AND status = 'queued' AND cancel_requested = 0
               AND analyzer_revision = ?6 AND config_hash = ?7",
            params![
                now_ns(),
                reason,
                job.id,
                job.manifest_id.0,
                job.analyzer_id,
                stored_revision,
                stored_config_hash,
            ],
        )?;
        claim.commit()?;
        runtime_metrics.mark_finished(
            job.id,
            if changed == 1 {
                RunStatus::Skipped.as_str()
            } else {
                "superseded"
            },
        );
        return Ok(());
    }
    let changed = claim.execute(
        "UPDATE workspace_analysis_runs
         SET status = 'running', started_at_ns = ?1,
             finished_at_ns = NULL, error = NULL
         WHERE job_id = ?2 AND manifest_id = ?3 AND analyzer_id = ?4
           AND status = 'queued' AND cancel_requested = 0
           AND analyzer_revision = ?5 AND config_hash = ?6",
        params![
            now_ns(),
            job.id,
            job.manifest_id.0,
            job.analyzer_id,
            analyzer.revision(),
            current_config_hash,
        ],
    )?;
    claim.commit()?;
    if changed != 1 {
        runtime_metrics.mark_finished(job.id, "superseded");
        return Ok(());
    }
    runtime_metrics.mark_running(job.id);

    // Entry materialization begins only after the exact claim. If it fails,
    // retire only the still-owned Running row; never overwrite a replacement.
    let entries = match manifest::get_entries(&conn, job.manifest_id) {
        Ok(entries) => entries,
        Err(error) => {
            let message = error.to_string();
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET status = 'failed', finished_at_ns = ?1, error = ?2
                 WHERE job_id = ?3 AND manifest_id = ?4 AND analyzer_id = ?5
                   AND status = 'running' AND cancel_requested = 0
                   AND analyzer_revision = ?6 AND config_hash = ?7",
                params![
                    now_ns(),
                    message,
                    job.id,
                    job.manifest_id.0,
                    job.analyzer_id,
                    analyzer.revision(),
                    current_config_hash,
                ],
            )?;
            runtime_metrics.mark_finished(job.id, RunStatus::Failed.as_str());
            return Err(error);
        }
    };
    let now = now_ns();
    debug!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        "analyzer job started"
    );
    // ANALYZER_STALL_TIMEOUT bounds progress *silence*, not total
    // run time: a long run that keeps ticking progress is allowed
    // (see the constant's rationale in workspace_analyzer::run).
    let outcome = run_one_workspace_analyzer_with_timeout(
        &mut conn,
        AnalyzerRunRequest {
            analyzer,
            repo_root: &job.repo_root,
            manifest_id: job.manifest_id,
            entries: &entries,
            now_ns: now,
            analyzer_stall_timeout: ANALYZER_STALL_TIMEOUT,
            job_id: Some(job.id),
            progress: Some(progress),
        },
    )?;
    runtime_metrics.mark_finished(job.id, outcome.status.as_str());
    debug!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        status = %outcome.status.as_str(),
        inserted_refs = outcome.inserted_refs,
        "analyzer job finished"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::tests::{insert_anchor, insert_manifest, insert_manifest_parser, job};

    struct WorkerFixture {
        _data: tempfile::TempDir,
        _repo: tempfile::TempDir,
        conn: rusqlite::Connection,
        job: Job,
    }

    fn worker_fixture(job_id: JobId, stored_config: Option<&str>) -> WorkerFixture {
        let data = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let cas_data_dir = CasDataDir::with_root(data.path().to_path_buf());
        let index_db_path = cas_data_dir.index_db_path();
        {
            let mut index = cas_registry::open(&index_db_path).unwrap();
            let tx = index.transaction().unwrap();
            cas_registry::upsert(
                &tx,
                "worker",
                repo.path().to_str().unwrap(),
                "worker-repo",
                1,
            )
            .unwrap();
            tx.commit().unwrap();
        }
        let store_path = cas_data_dir.store_db_path("worker-repo");
        let conn = cas_store::open(&store_path).unwrap();
        let manifest_id = ManifestId(7);
        insert_manifest(&conn, manifest_id.0);
        conn.execute(
            "INSERT INTO worktrees (worktree_id, path, registered_at_ns)
             VALUES (1, ?1, 0)",
            [repo.path().to_str().unwrap()],
        )
        .unwrap();
        insert_anchor(&conn, "tentative/1", manifest_id.0);
        insert_manifest_parser(
            &conn,
            manifest_id,
            "src/main.fake",
            "worker-blob",
            "fake-parser",
        );
        let analyzer = all_workspace_analyzers()
            .into_iter()
            .find(|analyzer| analyzer.id() == "fake-workspace")
            .unwrap();
        let current_config = config_hash(repo.path(), analyzer.config_paths());
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, 'fake-workspace', ?2, ?3,
                     'queued', 1, NULL, NULL, ?4, 0)",
            params![
                manifest_id.0,
                analyzer.revision(),
                stored_config.unwrap_or(&current_config),
                job_id,
            ],
        )
        .unwrap();
        let repo_root = repo.path().to_path_buf();
        WorkerFixture {
            _data: data,
            _repo: repo,
            conn,
            job: Job {
                id: job_id,
                alias: "worker".into(),
                repo_hash: "worker-repo".into(),
                store_path,
                index_db_path,
                repo_root,
                manifest_id,
                analyzer_id: "fake-workspace".into(),
            },
        }
    }

    fn run_fixture(fixture: &WorkerFixture) {
        run_job_blocking(
            fixture.job.clone(),
            JobRuntimeMetricsStore::default(),
            AnalyzerProgress::default(),
            None,
        )
        .unwrap();
    }

    fn enqueued_metrics(job_id: JobId) -> JobRuntimeMetricsStore {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(job_id, None, 1);
        metrics
    }

    fn metric_snapshot(metrics: &JobRuntimeMetricsStore, job_id: JobId) -> JobSnapshot {
        let mut snapshot = job(job_id, "fake-workspace", "queued");
        metrics.decorate(&mut snapshot, now_ns());
        snapshot
    }

    #[test]
    fn quarantined_before_preclaim_skips_exact_job_without_invocation() {
        let fixture = worker_fixture(41, None);
        let index = cas_registry::open(&fixture.job.index_db_path).unwrap();
        let changed = index
            .execute(
                "UPDATE repo_reconcile_state SET quarantined_at_ns = 10
                 WHERE repo_hash = 'worker-repo'",
                [],
            )
            .unwrap();
        assert_eq!(changed, 1);

        run_fixture(&fixture);

        let (status, error): (String, Option<String>) = fixture
            .conn
            .query_row(
                "SELECT status, error FROM workspace_analysis_runs WHERE job_id = 41",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Skipped.as_str());
        assert!(
            error
                .unwrap()
                .contains("quarantined before dispatch admission")
        );
    }

    #[test]
    fn quarantine_committed_after_observation_does_not_revoke_admitted_attempt() {
        let fixture = worker_fixture(45, None);
        let index_path = fixture.job.index_db_path.clone();
        AFTER_QUARANTINE_OBSERVATION_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let index = cas_registry::open(&index_path).unwrap();
                let changed = index
                    .execute(
                        "UPDATE repo_reconcile_state SET quarantined_at_ns = 10
                         WHERE repo_hash = 'worker-repo'",
                        [],
                    )
                    .unwrap();
                assert_eq!(changed, 1);
            }));
        });

        run_fixture(&fixture);

        let status: String = fixture
            .conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs WHERE job_id = 45",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Succeeded.as_str());
    }

    #[test]
    fn config_mismatch_before_preclaim_skips_without_running_analyzer() {
        let fixture = worker_fixture(42, Some("stale-config"));

        run_fixture(&fixture);

        let status: String = fixture
            .conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs WHERE job_id = 42",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Skipped.as_str());
    }

    #[test]
    fn replacement_job_id_is_not_claimed_or_clobbered() {
        let fixture = worker_fixture(43, None);
        fixture
            .conn
            .execute(
                "UPDATE workspace_analysis_runs SET job_id = 44 WHERE job_id = 43",
                [],
            )
            .unwrap();

        let metrics = enqueued_metrics(43);
        run_job_blocking(
            fixture.job.clone(),
            metrics.clone(),
            AnalyzerProgress::default(),
            None,
        )
        .unwrap();

        let (job_id, status): (i64, String) = fixture
            .conn
            .query_row(
                "SELECT job_id, status FROM workspace_analysis_runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((job_id, status), (44, RunStatus::Queued.as_str().into()));
        let runtime = metric_snapshot(&metrics, 43);
        assert_eq!(runtime.scheduler_state.as_deref(), Some("superseded"));
        assert!(runtime.run_started_at.is_none());
    }

    #[test]
    fn exact_claim_is_the_only_runtime_running_authority() {
        let fixture = worker_fixture(48, None);
        let metrics = enqueued_metrics(48);

        run_job_blocking(
            fixture.job.clone(),
            metrics.clone(),
            AnalyzerProgress::default(),
            None,
        )
        .unwrap();

        let status: String = fixture
            .conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs WHERE job_id = 48",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Succeeded.as_str());
        let runtime = metric_snapshot(&metrics, 48);
        assert_eq!(
            runtime.scheduler_state.as_deref(),
            Some(RunStatus::Succeeded.as_str())
        );
        assert!(runtime.run_started_at.is_some());
    }

    #[test]
    fn unknown_analyzer_retires_exact_queued_row_without_running_metrics() {
        let mut fixture = worker_fixture(49, None);
        fixture
            .conn
            .execute(
                "UPDATE workspace_analysis_runs
                 SET analyzer_id = 'retired-analyzer'
                 WHERE job_id = 49 AND status = 'queued'",
                [],
            )
            .unwrap();
        fixture.job.analyzer_id = "retired-analyzer".into();
        let metrics = enqueued_metrics(49);

        run_job_blocking(
            fixture.job.clone(),
            metrics.clone(),
            AnalyzerProgress::default(),
            None,
        )
        .unwrap();

        let (status, error): (String, Option<String>) = fixture
            .conn
            .query_row(
                "SELECT status, error FROM workspace_analysis_runs WHERE job_id = 49",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Skipped.as_str());
        assert!(error.is_some_and(|message| message.contains("no longer available")));
        let queued: i64 = fixture
            .conn
            .query_row(
                "SELECT COUNT(*) FROM workspace_analysis_runs
                 WHERE job_id = 49 AND status = 'queued'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(queued, 0, "restart restore must not re-advertise the row");
        let runtime = metric_snapshot(&metrics, 49);
        assert_eq!(
            runtime.scheduler_state.as_deref(),
            Some(RunStatus::Skipped.as_str())
        );
        assert!(runtime.run_started_at.is_none());
    }

    #[test]
    fn durable_cancel_before_claim_retires_without_invocation() {
        let fixture = worker_fixture(46, None);
        fixture
            .conn
            .execute(
                "UPDATE workspace_analysis_runs SET cancel_requested = 1 WHERE job_id = 46",
                [],
            )
            .unwrap();

        run_fixture(&fixture);

        let status: String = fixture
            .conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs WHERE job_id = 46",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Cancelled.as_str());
    }

    #[test]
    fn shutdown_token_before_claim_retires_without_invocation() {
        let fixture = worker_fixture(47, None);
        let progress = AnalyzerProgress::default();
        progress.cancel();

        run_job_blocking(
            fixture.job.clone(),
            JobRuntimeMetricsStore::default(),
            progress,
            None,
        )
        .unwrap();

        let status: String = fixture
            .conn
            .query_row(
                "SELECT status FROM workspace_analysis_runs WHERE job_id = 47",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, RunStatus::Cancelled.as_str());
    }

    fn scheduler_state(metrics: &JobRuntimeMetricsStore, job_id: JobId) -> String {
        let mut snapshot = job(job_id, "pyright-lsp", "failed");
        metrics.decorate(&mut snapshot, now_ns());
        snapshot.scheduler_state.unwrap()
    }

    fn running_metrics(job_id: JobId) -> JobRuntimeMetricsStore {
        let metrics = JobRuntimeMetricsStore::default();
        metrics.mark_enqueued(job_id, None, 1);
        metrics.mark_running(job_id);
        metrics
    }

    #[test]
    fn inner_worker_error_marks_unfinished_metrics_failed() {
        let metrics = running_metrics(1);

        let result = finalize_joined_run(
            &metrics,
            1,
            Ok(Err(Error::InvalidArgument("worker failed".into()))),
        );

        assert!(result.is_err());
        assert_eq!(scheduler_state(&metrics, 1), RunStatus::Failed.as_str());
    }

    #[tokio::test]
    async fn worker_panic_marks_unfinished_metrics_failed() {
        let metrics = running_metrics(2);
        let join_error = tokio::spawn(async {
            panic!("worker panic");
        })
        .await
        .unwrap_err();

        let result = finalize_joined_run(&metrics, 2, Err(join_error));

        assert!(result.is_err());
        assert_eq!(scheduler_state(&metrics, 2), RunStatus::Failed.as_str());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_observation_uses_blocking_pool_and_reports_missing_or_unreadable_row() {
        let tmp = tempfile::tempdir().unwrap();
        let (recorder, _guard) = crate::churn_recorder::install("repo", tmp.path());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let store_path = tmp.path().join("store.db");
        cas_store::open(&store_path).unwrap();
        let task = tokio::spawn(record_terminal_after_worker_with_probe(
            (
                "repo".into(),
                store_path,
                crate::manifest::ManifestId(7),
                "test-analyzer".into(),
            ),
            41,
            false,
            move || {
                let _ = started_tx.send(());
                release_rx.recv().unwrap();
            },
        ));

        started_rx.await.unwrap();
        // Reaching this point while the blocking probe is still held is the
        // responsiveness evidence; the counter below is only a sentinel that
        // the yield returned.
        let mut heartbeat = 0;
        tokio::task::yield_now().await;
        heartbeat += 1;
        assert_eq!(
            heartbeat, 1,
            "current-thread runtime must remain responsive"
        );
        release_tx.send(()).unwrap();
        task.await.unwrap();
        record_terminal_after_worker(
            (
                "repo".into(),
                tmp.path().join("unreadable.db"),
                crate::manifest::ManifestId(8),
                "test-analyzer".into(),
            ),
            42,
            None,
        )
        .await;

        assert_eq!(
            recorder.snapshot().observation_failures,
            [
                crate::churn_recorder::ObservationFailure {
                    kind: crate::churn_recorder::ObservationFailureKind::TerminalRowUnavailable,
                    repo_hash: "repo".into(),
                    generation: None,
                    analyzer_id: Some("test-analyzer".into()),
                    job_id: Some(41),
                },
                crate::churn_recorder::ObservationFailure {
                    kind: crate::churn_recorder::ObservationFailureKind::TerminalRowUnavailable,
                    repo_hash: "repo".into(),
                    generation: None,
                    analyzer_id: Some("test-analyzer".into()),
                    job_id: Some(42),
                },
            ]
        );
    }

    #[test]
    fn late_worker_failure_preserves_concurrent_cancellation() {
        let metrics = running_metrics(3);
        metrics.mark_finished(3, RunStatus::Cancelled.as_str());

        let result = finalize_joined_run(
            &metrics,
            3,
            Ok(Err(Error::InvalidArgument("late failure".into()))),
        );

        assert!(result.is_err());
        assert_eq!(scheduler_state(&metrics, 3), RunStatus::Cancelled.as_str());
    }
}
