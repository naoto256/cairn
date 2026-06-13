//! Background Tier-3 workspace analyzer jobs.
//!
//! The CAS store already keeps one current `workspace_analysis_runs`
//! row per `(manifest, analyzer)`. This module makes those rows the
//! daemon-visible job state and keeps the expensive LSP-backed work out
//! of short-lived control requests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::cas::{registry as cas_registry, store as cas_store};
use crate::manifest::{self, ManifestEntry, ManifestId};
use crate::paths::CasDataDir;
use crate::workspace_analyzer::{
    ANALYZER_STALL_TIMEOUT, AnalyzerRunRequest, RunRecord, RunStatus, all_workspace_analyzers,
    config_hash, mark_run, run_one_workspace_analyzer_with_timeout,
};
use crate::{Error, Result};

pub type JobId = i64;

const MAX_WORKER_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueuedAnalyzerJob {
    pub job_id: JobId,
    pub analyzer_id: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobSnapshot {
    pub job_id: JobId,
    pub alias: String,
    pub analyzer_id: String,
    pub state: String,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CancelResult {
    pub cancelled: bool,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct Job {
    id: JobId,
    alias: String,
    store_path: PathBuf,
    repo_root: PathBuf,
    manifest_id: ManifestId,
    analyzer_id: String,
}

pub struct JobManager {
    cas_data_dir: Arc<CasDataDir>,
    sender: Mutex<Option<mpsc::UnboundedSender<Job>>>,
    receiver: Arc<AsyncMutex<mpsc::UnboundedReceiver<Job>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    pool_group_locks: PoolGroupLocks,
}

pub struct EnqueueReindex<'a> {
    pub conn: &'a mut Connection,
    pub alias: &'a str,
    pub repo_hash: &'a str,
    pub repo_root: &'a Path,
    pub manifest_id: ManifestId,
    pub entries: &'a [ManifestEntry],
    pub now_ns: i64,
}

impl JobManager {
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Arc<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            cas_data_dir,
            sender: Mutex::new(Some(sender)),
            receiver: Arc::new(AsyncMutex::new(receiver)),
            workers: Mutex::new(Vec::new()),
            pool_group_locks: PoolGroupLocks::default(),
        })
    }

    pub fn start_workers(self: &Arc<Self>) {
        let workers = worker_concurrency();
        for _ in 0..workers {
            let manager = self.clone();
            let handle = tokio::spawn(async move {
                manager.worker_loop().await;
            });
            self.workers
                .lock()
                .expect("job worker lock poisoned")
                .push(handle);
        }
        info!(workers, "job workers started");
    }

    pub fn restore_from_db(&self) -> Result<()> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        for entry in cas_registry::list_all(&index)? {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET status = 'queued', finished_at_ns = NULL, error = NULL
                 WHERE status = 'running'",
                [],
            )?;
            let first_id = next_job_id(&conn)?.max(now_ns());
            let missing_job_ids = conn
                .prepare(
                    "SELECT manifest_id, analyzer_id
                     FROM workspace_analysis_runs
                     WHERE status = 'queued' AND job_id IS NULL
                     ORDER BY started_at_ns ASC, analyzer_id ASC",
                )?
                .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (next_id, (manifest_id, analyzer_id)) in (first_id..).zip(missing_job_ids) {
                conn.execute(
                    "UPDATE workspace_analysis_runs
                     SET job_id = ?1, cancel_requested = 0
                     WHERE manifest_id = ?2 AND analyzer_id = ?3",
                    params![next_id, manifest_id, analyzer_id],
                )?;
            }
            let mut stmt = conn.prepare(
                "SELECT job_id, manifest_id, analyzer_id
                 FROM workspace_analysis_runs
                 WHERE status = 'queued' AND job_id IS NOT NULL
                 ORDER BY started_at_ns ASC, job_id ASC",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        ManifestId(r.get::<_, i64>(1)?),
                        r.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (id, manifest_id, analyzer_id) in rows {
                self.enqueue_memory(Job {
                    id,
                    alias: entry.alias.clone(),
                    store_path: store_path.clone(),
                    repo_root: PathBuf::from(&entry.root_path),
                    manifest_id,
                    analyzer_id,
                });
            }
        }
        Ok(())
    }

    pub fn enqueue_reindex(&self, request: EnqueueReindex<'_>) -> Result<Vec<QueuedAnalyzerJob>> {
        let EnqueueReindex {
            conn,
            alias,
            repo_hash,
            repo_root,
            manifest_id,
            entries,
            now_ns,
        } = request;
        let first_id = next_job_id(conn)?.max(now_ns);
        let mut jobs = Vec::new();
        for (job_id, analyzer) in (first_id..).zip(all_workspace_analyzers()) {
            let analyzer_id = analyzer.id();
            let analyzer_revision = analyzer.revision();
            let cfg = config_hash(repo_root, analyzer.config_paths());
            mark_run(
                conn,
                RunRecord {
                    manifest_id,
                    analyzer_id,
                    analyzer_revision,
                    config_hash: &cfg,
                    status: RunStatus::Queued,
                    started_at_ns: now_ns,
                    finished_at_ns: now_ns,
                    error: None,
                    job_id: Some(job_id),
                },
            )?;
            conn.execute(
                "UPDATE workspace_analysis_runs
                 SET cancel_requested = 0
                 WHERE manifest_id = ?1 AND analyzer_id = ?2",
                params![manifest_id.0, analyzer_id],
            )?;
            self.enqueue_memory(Job {
                id: job_id,
                alias: alias.to_string(),
                store_path: self.cas_data_dir.store_db_path(repo_hash),
                repo_root: repo_root.to_path_buf(),
                manifest_id,
                analyzer_id: analyzer_id.to_string(),
            });
            jobs.push(QueuedAnalyzerJob {
                job_id,
                analyzer_id: analyzer_id.to_string(),
                state: RunStatus::Queued.as_str().to_string(),
            });
        }

        if entries.is_empty() {
            return Ok(jobs);
        }
        Ok(jobs)
    }

    pub(crate) fn jobs(
        &self,
        alias_filter: Option<&str>,
        state_filter: Option<RunStatus>,
    ) -> Result<Vec<JobSnapshot>> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let mut out = Vec::new();
        for entry in cas_registry::list_all(&index)? {
            if let Some(alias) = alias_filter
                && entry.alias != alias
            {
                continue;
            }
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            let mut stmt = conn.prepare(
                "SELECT job_id, analyzer_id, status, started_at_ns, finished_at_ns, error
                 FROM workspace_analysis_runs
                 WHERE job_id IS NOT NULL
                 ORDER BY job_id DESC",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(JobSnapshot {
                        job_id: r.get(0)?,
                        alias: entry.alias.clone(),
                        analyzer_id: r.get(1)?,
                        state: r.get(2)?,
                        created_at: r.get(3)?,
                        started_at: None,
                        finished_at: r.get(4)?,
                        error: r.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for row in rows {
                if let Some(filter) = state_filter
                    && row.state != filter.as_str()
                {
                    continue;
                }
                out.push(row);
            }
        }
        out.sort_by_key(|job| std::cmp::Reverse(job.job_id));
        Ok(out)
    }

    pub fn cancel(&self, job_id: JobId) -> Result<CancelResult> {
        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        for entry in cas_registry::list_all(&index)? {
            let store_path = self.cas_data_dir.store_db_path(&entry.repo_hash);
            let conn = cas_store::open(&store_path)?;
            let row: Option<(String, i64, String)> = conn
                .query_row(
                    "SELECT status, manifest_id, analyzer_id
                     FROM workspace_analysis_runs WHERE job_id = ?1",
                    [job_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let Some((state, manifest_id, analyzer_id)) = row else {
                continue;
            };
            match RunStatus::from_str(&state) {
                Some(RunStatus::Queued) => {
                    conn.execute(
                        "UPDATE workspace_analysis_runs
                         SET status = 'cancelled', cancel_requested = 1, finished_at_ns = ?1
                         WHERE job_id = ?2",
                        params![now_ns(), job_id],
                    )?;
                    return Ok(CancelResult {
                        cancelled: true,
                        reason: "queued job cancelled".into(),
                    });
                }
                Some(RunStatus::Running) => {
                    conn.execute(
                        "UPDATE workspace_analysis_runs
                         SET cancel_requested = 1
                         WHERE manifest_id = ?1 AND analyzer_id = ?2",
                        params![manifest_id, analyzer_id],
                    )?;
                    return Ok(CancelResult {
                        cancelled: false,
                        reason: "running job marked for cancellation; analyzer will finish current request".into(),
                    });
                }
                Some(state) if state.is_terminal() => {
                    return Ok(CancelResult {
                        cancelled: false,
                        reason: format!("job already {}", state.as_str()),
                    });
                }
                _ => {}
            }
        }
        Err(Error::InvalidArgument(format!("unknown job id: {job_id}")))
    }

    pub async fn shutdown(&self, drain_timeout: Duration) {
        {
            let mut sender = self.sender.lock().expect("job sender lock poisoned");
            sender.take();
        }
        let handles = {
            let mut workers = self.workers.lock().expect("job worker lock poisoned");
            std::mem::take(&mut *workers)
        };
        let _ = tokio::time::timeout(drain_timeout, async {
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
    }

    fn enqueue_memory(&self, job: Job) {
        let sender = self.sender.lock().expect("job sender lock poisoned");
        match sender.as_ref() {
            Some(sender) => {
                if let Err(err) = sender.send(job) {
                    warn!(error = %err, "failed to enqueue analyzer job");
                }
            }
            None => warn!("job manager is shutting down; analyzer job was not enqueued"),
        }
    }

    async fn worker_loop(self: Arc<Self>) {
        loop {
            let job = {
                let mut receiver = self.receiver.lock().await;
                receiver.recv().await
            };
            let Some(job) = job else {
                break;
            };
            if let Err(err) = self.run_job(job).await {
                warn!(error = %err, "analyzer job failed");
            }
        }
    }

    async fn run_job(&self, job: Job) -> Result<()> {
        let pool_group = pool_group_for_analyzer_id(&job.analyzer_id)?;
        let _pool_group_guard = if let Some(group) = pool_group {
            let wait_started = Instant::now();
            let guard = self.pool_group_locks.lock(group).await;
            let wait_elapsed = wait_started.elapsed();
            if wait_elapsed.as_millis() > 0 {
                debug!(
                    alias = %job.alias,
                    analyzer_id = %job.analyzer_id,
                    job_id = job.id,
                    pool_group = group,
                    wait_elapsed_ms = wait_elapsed.as_millis(),
                    "analyzer job waited for pool group"
                );
            }
            Some(guard)
        } else {
            None
        };
        tokio::task::spawn_blocking(move || run_job_blocking(job))
            .await
            .map_err(|e| Error::InvalidArgument(format!("analyzer job task panicked: {e}")))?
    }
}

#[derive(Default)]
struct PoolGroupLocks {
    groups: AsyncMutex<HashMap<&'static str, Arc<AsyncMutex<()>>>>,
}

impl PoolGroupLocks {
    async fn lock(&self, group: &'static str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut groups = self.groups.lock().await;
            Arc::clone(
                groups
                    .entry(group)
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        lock.lock_owned().await
    }
}

fn pool_group_for_analyzer_id(analyzer_id: &str) -> Result<Option<&'static str>> {
    all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == analyzer_id)
        .map(|a| a.pool_group())
        .ok_or_else(|| Error::InvalidArgument(format!("unknown analyzer: {analyzer_id}")))
}

fn run_job_blocking(job: Job) -> Result<()> {
    let mut conn = cas_store::open(&job.store_path)?;
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT status, cancel_requested
             FROM workspace_analysis_runs
             WHERE job_id = ?1 AND manifest_id = ?2 AND analyzer_id = ?3",
            params![job.id, job.manifest_id.0, job.analyzer_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((state, cancel_requested)) = row else {
        return Ok(());
    };
    if state == RunStatus::Cancelled.as_str()
        || (state == RunStatus::Queued.as_str() && cancel_requested != 0)
    {
        conn.execute(
            "UPDATE workspace_analysis_runs
             SET status = 'cancelled', finished_at_ns = ?1
             WHERE job_id = ?2",
            params![now_ns(), job.id],
        )?;
        return Ok(());
    }
    if state != RunStatus::Queued.as_str() && state != RunStatus::Running.as_str() {
        return Ok(());
    }

    let analyzer = all_workspace_analyzers()
        .into_iter()
        .find(|a| a.id() == job.analyzer_id)
        .ok_or_else(|| Error::InvalidArgument(format!("unknown analyzer: {}", job.analyzer_id)))?;
    let entries = manifest::get_entries(&conn, job.manifest_id)?;
    let now = now_ns();
    info!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        "analyzer job started"
    );
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
        },
    )?;
    info!(
        alias = %job.alias,
        analyzer_id = %job.analyzer_id,
        job_id = job.id,
        status = %outcome.status.as_str(),
        inserted_refs = outcome.inserted_refs,
        "analyzer job finished"
    );
    Ok(())
}

fn next_job_id(conn: &Connection) -> Result<JobId> {
    let max: Option<i64> =
        conn.query_row("SELECT MAX(job_id) FROM workspace_analysis_runs", [], |r| {
            r.get(0)
        })?;
    Ok(max.unwrap_or_else(now_ns) + 1)
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    )
    .unwrap_or(i64::MAX)
}

fn worker_concurrency() -> usize {
    let env_value = std::env::var("CAIRN_WORKER_CONCURRENCY").ok();
    worker_concurrency_from_env_value(env_value.as_deref(), fallback_worker_concurrency)
}

fn worker_concurrency_from_env_value(
    env_value: Option<&str>,
    fallback: impl FnOnce() -> usize,
) -> usize {
    if let Some(value) = env_value {
        match value.parse::<usize>() {
            Ok(parsed) if parsed > 0 => {
                // Keep env overrides within the same ceiling as automatic sizing to bound worker fan-out.
                let clamped = parsed.clamp(1, MAX_WORKER_CONCURRENCY);
                if clamped != parsed {
                    warn!(
                        requested = parsed,
                        clamped, "CAIRN_WORKER_CONCURRENCY clamped"
                    );
                }
                return clamped;
            }
            _ => {}
        }
    }
    fallback()
}

fn fallback_worker_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .map(|n| (n / 2).clamp(1, MAX_WORKER_CONCURRENCY))
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::subscriber::Interest;
    use tracing::{Event, Level, Metadata, Subscriber};

    use super::{MAX_WORKER_CONCURRENCY, PoolGroupLocks, worker_concurrency_from_env_value};

    #[derive(Default)]
    struct WarnCapture {
        fields: Mutex<Vec<Vec<(String, String)>>>,
    }

    impl WarnCapture {
        fn fields(&self) -> Vec<Vec<(String, String)>> {
            self.fields.lock().expect("warn capture poisoned").clone()
        }
    }

    struct WarnSubscriber(Arc<WarnCapture>);

    impl Subscriber for WarnSubscriber {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= &Level::WARN
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            if event.metadata().level() != &Level::WARN {
                return;
            }
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            self.0
                .fields
                .lock()
                .expect("warn capture poisoned")
                .push(visitor.fields);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}

        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if self.enabled(metadata) {
                Interest::always()
            } else {
                Interest::never()
            }
        }
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: Vec<(String, String)>,
    }

    impl FieldVisitor {
        fn push(&mut self, field: &Field, value: impl ToString) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.push(field, format!("{value:?}"));
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.push(field, value);
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.push(field, value);
        }
    }

    fn assert_no_worker_concurrency_warn(capture: &WarnCapture) {
        assert!(
            capture.fields().is_empty(),
            "unexpected worker concurrency warn"
        );
    }

    fn assert_worker_concurrency_warn(capture: &WarnCapture, requested: usize, clamped: usize) {
        let events = capture.fields();
        assert_eq!(events.len(), 1, "expected one worker concurrency warn");
        assert!(
            events[0]
                .iter()
                .any(|(name, value)| name == "requested" && value == &requested.to_string()),
            "warn did not include requested={requested}: {:?}",
            events[0]
        );
        assert!(
            events[0]
                .iter()
                .any(|(name, value)| name == "clamped" && value == &clamped.to_string()),
            "warn did not include clamped={clamped}: {:?}",
            events[0]
        );
    }

    fn with_warn_capture(f: impl FnOnce(&WarnCapture)) {
        static DISPATCH_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        let _guard = DISPATCH_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("tracing dispatch lock poisoned");
        let capture = Arc::new(WarnCapture::default());
        tracing::dispatcher::with_default(
            &tracing::Dispatch::new(WarnSubscriber(Arc::clone(&capture))),
            || {
                f(&capture);
            },
        );
    }

    #[test]
    fn worker_concurrency_uses_env_value_within_bounds() {
        with_warn_capture(|capture| {
            assert_eq!(worker_concurrency_from_env_value(Some("4"), || 2), 4);
            assert_no_worker_concurrency_warn(capture);
        });
    }

    #[test]
    fn worker_concurrency_clamps_env_value_above_ceiling_and_warns() {
        with_warn_capture(|capture| {
            assert_eq!(
                worker_concurrency_from_env_value(Some("128"), || 2),
                MAX_WORKER_CONCURRENCY
            );
            assert_worker_concurrency_warn(capture, 128, MAX_WORKER_CONCURRENCY);
        });
    }

    #[test]
    fn worker_concurrency_falls_back_for_zero_negative_or_invalid_env() {
        for value in ["0", "-4", "not-a-number"] {
            with_warn_capture(|capture| {
                assert_eq!(worker_concurrency_from_env_value(Some(value), || 3), 3);
                assert_no_worker_concurrency_warn(capture);
            });
        }
    }

    #[tokio::test]
    async fn same_pool_group_locks_serialize() {
        let locks = Arc::new(PoolGroupLocks::default());
        let first_guard = locks.lock("shared-lsp").await;
        let (acquired_tx, acquired_rx) = oneshot::channel();
        let waiting_locks = Arc::clone(&locks);
        let waiter = tokio::spawn(async move {
            let _guard = waiting_locks.lock("shared-lsp").await;
            let _ = acquired_tx.send(());
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), acquired_rx)
                .await
                .is_err(),
            "second same-group lock acquired while the first guard was held"
        );
        drop(first_guard);
        waiter.await.expect("same-group waiter task panicked");
    }

    #[tokio::test]
    async fn different_pool_groups_do_not_block_each_other() {
        let locks = Arc::new(PoolGroupLocks::default());
        let _first_guard = locks.lock("clangd-lsp").await;
        let (acquired_tx, acquired_rx) = oneshot::channel();
        let waiting_locks = Arc::clone(&locks);
        let waiter = tokio::spawn(async move {
            let _guard = waiting_locks.lock("typescript-language-server-lsp").await;
            let _ = acquired_tx.send(());
        });

        tokio::time::timeout(Duration::from_millis(25), acquired_rx)
            .await
            .expect("different-group lock was blocked")
            .expect("different-group waiter dropped before signaling");
        waiter.await.expect("different-group waiter task panicked");
    }
}
