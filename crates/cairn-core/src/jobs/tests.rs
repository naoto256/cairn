use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};

use crate::cas::{registry as cas_registry, store as cas_store};
use crate::manifest::ManifestId;
use crate::paths::CasDataDir;
use crate::workspace_analyzer::RunStatus;
use crate::workspace_analyzer::expected_analyzers_for_manifest;

use super::{
    DispatchJob, Job, JobId, JobListOptions, JobManager, JobRuntimeMetricsStore, JobScheduler,
    JobSnapshot, MAX_WORKER_CONCURRENCY, QueueAnalyzerRun, TrackedJobKeys,
    worker_concurrency_from_env_value,
};

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

#[test]
fn duplicate_enqueue_does_not_overwrite_current_job_row() {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    let manager = JobManager::new(Arc::clone(&cas_data_dir));
    let repo_hash = "repo-hash";
    let manifest_id = ManifestId(1);
    {
        let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "repo", repo.path().to_str().unwrap(), repo_hash, 1).unwrap();
        tx.commit().unwrap();
    }
    let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
         VALUES (?1, 'tentative', 0)",
        [manifest_id.0],
    )
    .unwrap();

    let first = manager
        .queue_analyzer_run(test_queue_request(
            &mut conn,
            repo.path(),
            repo_hash,
            manifest_id,
            1,
        ))
        .unwrap()
        .expect("first job should queue");
    assert_eq!(first.job_id, 1);

    let queued_duplicate = manager
        .queue_analyzer_run(test_queue_request(
            &mut conn,
            repo.path(),
            repo_hash,
            manifest_id,
            2,
        ))
        .unwrap();
    assert!(
        queued_duplicate.is_none(),
        "queued duplicate should coalesce before DB write"
    );
    let queued_row: (i64, String) = conn
        .query_row(
            "SELECT job_id, status
             FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
            [manifest_id.0],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(queued_row, (1, "queued".into()));

    conn.execute(
        "UPDATE workspace_analysis_runs
         SET status = 'running'
         WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
        [manifest_id.0],
    )
    .unwrap();

    let duplicate = manager
        .queue_analyzer_run(test_queue_request(
            &mut conn,
            repo.path(),
            repo_hash,
            manifest_id,
            3,
        ))
        .unwrap();
    assert!(duplicate.is_none(), "active duplicate should coalesce");
    let row: (i64, String, i64) = conn
        .query_row(
            "SELECT job_id, status, cancel_requested
             FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
            [manifest_id.0],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, (1, "running".into(), 0));

    let listed = manager
        .jobs(
            Some("repo"),
            None,
            JobListOptions {
                include_all: true,
                limit: None,
            },
        )
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].job_id, 1);

    let cancelled = manager.cancel(1).unwrap();
    assert!(!cancelled.cancelled);
    assert!(cancelled.reason.contains("running job marked"));
    let cancel_requested: i64 = conn
        .query_row(
            "SELECT cancel_requested FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = 'pyright-lsp'",
            [manifest_id.0],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cancel_requested, 1);
    assert!(
        manager.cancel(2).is_err(),
        "coalesced queued job id must not exist"
    );
    assert!(
        manager.cancel(3).is_err(),
        "coalesced active job id must not exist"
    );
}

#[test]
fn manual_cancel_of_running_job_cancels_active_handle() {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    let manager = JobManager::new(Arc::clone(&cas_data_dir));
    let repo_hash = "repo-hash";
    let manifest_id = ManifestId(1);
    {
        let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "repo", repo.path().to_str().unwrap(), repo_hash, 1).unwrap();
        tx.commit().unwrap();
    }
    let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
    insert_manifest(&conn, manifest_id.0);
    let job = manager
        .queue_analyzer_run(test_queue_request(
            &mut conn,
            repo.path(),
            repo_hash,
            manifest_id,
            1,
        ))
        .unwrap()
        .expect("job should queue");
    conn.execute(
        "UPDATE workspace_analysis_runs SET status = 'running' WHERE job_id = ?1",
        [job.job_id],
    )
    .unwrap();
    let progress = crate::workspace_analyzer::AnalyzerProgress::default();
    manager.register_active_progress(job.job_id, progress.clone());

    let cancelled = manager.cancel(job.job_id).unwrap();

    assert!(!cancelled.cancelled);
    assert!(progress.is_cancelled());
    let cancel_requested: i64 = conn
        .query_row(
            "SELECT cancel_requested FROM workspace_analysis_runs WHERE job_id = ?1",
            [job.job_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cancel_requested, 1);
}

#[test]
fn repository_removal_cancels_running_job_active_handle() {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    let manager = JobManager::new(Arc::clone(&cas_data_dir));
    let repo_hash = "repo-hash";
    let manifest_id = ManifestId(1);
    {
        let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "repo", repo.path().to_str().unwrap(), repo_hash, 1).unwrap();
        tx.commit().unwrap();
    }
    let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
    insert_manifest(&conn, manifest_id.0);
    let job = manager
        .queue_analyzer_run(test_queue_request(
            &mut conn,
            repo.path(),
            repo_hash,
            manifest_id,
            1,
        ))
        .unwrap()
        .expect("job should queue");
    conn.execute(
        "UPDATE workspace_analysis_runs SET status = 'running' WHERE job_id = ?1",
        [job.job_id],
    )
    .unwrap();
    let progress = crate::workspace_analyzer::AnalyzerProgress::default();
    manager.register_active_progress(job.job_id, progress.clone());

    assert_eq!(manager.cancel_repository(repo_hash).unwrap(), 1);

    assert!(progress.is_cancelled());
    let cancel_requested: i64 = conn
        .query_row(
            "SELECT cancel_requested FROM workspace_analysis_runs WHERE job_id = ?1",
            [job.job_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cancel_requested, 1);
}

#[test]
fn begin_shutdown_cancels_active_progress_and_rejects_new_admission() {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    let manager = JobManager::new(cas_data_dir);
    let progress = crate::workspace_analyzer::AnalyzerProgress::default();
    manager.register_active_progress(41, progress.clone());

    manager.begin_shutdown();

    assert!(progress.is_cancelled());
    let late_progress = crate::workspace_analyzer::AnalyzerProgress::default();
    manager.register_active_progress(42, late_progress.clone());
    assert!(
        late_progress.is_cancelled(),
        "a run racing after the shutdown linearization point must start cancelled"
    );

    let mut conn = cas_store::open(&manager.cas_data_dir.store_db_path("repo-hash")).unwrap();
    insert_manifest(&conn, 1);
    let err = manager
        .queue_analyzer_run(test_queue_request(
            &mut conn,
            repo.path(),
            "repo-hash",
            ManifestId(1),
            1,
        ))
        .unwrap_err();
    assert!(matches!(err, crate::Error::JobManagerShuttingDown));
    assert_eq!(count_runs(&conn, 1, "pyright-lsp"), 0);
}

#[test]
fn enqueue_reindex_queues_only_expected_manifest_analyzers() {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    let manager = JobManager::new(Arc::clone(&cas_data_dir));
    let repo_hash = "repo-hash";
    let manifest_id = ManifestId(1);
    let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
    insert_manifest(&conn, manifest_id.0);
    insert_manifest_parser(&conn, manifest_id, "src/fake.rs", "fake-sha", "fake-parser");
    insert_manifest_parser(
        &conn,
        manifest_id,
        "src/unknown.rs",
        "unknown-sha",
        "unknown-parser",
    );

    let expected_ids = expected_analyzers_for_manifest(&conn, manifest_id)
        .unwrap()
        .into_iter()
        .map(|analyzer| analyzer.id().to_string())
        .collect::<Vec<_>>();

    let jobs = manager
        .enqueue_reindex(super::EnqueueReindex {
            conn: &mut conn,
            alias: "repo",
            repo_hash,
            repo_root: repo.path(),
            manifest_id,
            entries: &[],
            now_ns: 1,
        })
        .unwrap();

    let queued_ids = jobs
        .iter()
        .map(|job| job.analyzer_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(queued_ids, expected_ids);
    assert_eq!(queued_ids, vec!["fake-workspace"]);

    let db_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM workspace_analysis_runs", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        db_count, 1,
        "reindex now creates jobs only for expected analyzers instead of recording no-match skips"
    );
}

pub(super) fn job(job_id: i64, analyzer_id: &str, state: &str) -> JobSnapshot {
    JobSnapshot {
        job_id,
        alias: "repo".into(),
        analyzer_id: analyzer_id.into(),
        state: state.into(),
        created_at: job_id,
        started_at: None,
        finished_at: None,
        error: None,
        pool_group: None,
        scheduler_state: None,
        enqueued_at: None,
        run_started_at: None,
        queued_ms: None,
        pool_wait_ms: None,
        run_ms: None,
        progress_ticks: None,
        last_progress_at: None,
        progress_per_minute: None,
    }
}

pub(super) fn test_scheduler(
    worker_capacity: usize,
) -> (JobScheduler, mpsc::UnboundedReceiver<DispatchJob>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        JobScheduler::new(
            tx,
            worker_capacity,
            JobRuntimeMetricsStore::default(),
            TrackedJobKeys::default(),
        ),
        rx,
    )
}

pub(super) fn drain_dispatched(rx: &mut mpsc::UnboundedReceiver<DispatchJob>) -> Vec<DispatchJob> {
    let mut out = Vec::new();
    while let Ok(job) = rx.try_recv() {
        out.push(job);
    }
    out
}

pub(super) fn test_job(id: JobId, manifest_id: i64, analyzer_id: &str) -> Job {
    Job {
        id,
        alias: "repo".into(),
        repo_hash: "repo-hash".into(),
        store_path: PathBuf::from("/tmp/store.db"),
        repo_root: PathBuf::from("/tmp/repo"),
        manifest_id: ManifestId(manifest_id),
        analyzer_id: analyzer_id.into(),
    }
}

/// `enqueue_analyzer_run` is the only public surface for a
/// **targeted** single-`(manifest, analyzer)` rerun. The stamp
/// (`analyzer_revision`, `config_hash`, `pool_group`, `job_id`) must
/// be derived inside JobManager — callers do not supply it — and
/// an unknown `analyzer_id` must return `Ok(None)` instead of
/// stamping a synthesized row. The de-dup gate must also hold:
/// a second call for the same `(manifest, analyzer)` while the
/// first is still `queued` coalesces.
#[test]
fn enqueue_analyzer_run_stamps_from_registry_and_dedupes() {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    let manager = JobManager::new(Arc::clone(&cas_data_dir));
    let repo_hash = "repo-hash";
    let manifest_id = ManifestId(1);
    let mut conn = cas_store::open(&cas_data_dir.store_db_path(repo_hash)).unwrap();
    insert_manifest(&conn, manifest_id.0);
    insert_manifest_parser(&conn, manifest_id, "src/fake.rs", "fake-sha", "fake-parser");

    // Unknown analyzer id → typed skip, no row written.
    let skip = manager
        .enqueue_analyzer_run(super::EnqueueAnalyzerRun {
            conn: &mut conn,
            alias: "repo",
            repo_hash,
            repo_root: repo.path(),
            manifest_id,
            analyzer_id: "not-in-registry",
            now_ns: 1,
        })
        .unwrap();
    assert!(
        skip.is_none(),
        "unknown analyzer id must skip cleanly without stamping a row"
    );
    let unknown_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM workspace_analysis_runs
             WHERE analyzer_id = 'not-in-registry'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unknown_rows, 0);

    // Known analyzer id (fake-workspace, registered via the
    // distributed_slice in workspace_analyzer/mod.rs test helpers)
    // → enqueue with the stamp derived from the analyzer instance,
    // not caller-supplied.
    let queued = manager
        .enqueue_analyzer_run(super::EnqueueAnalyzerRun {
            conn: &mut conn,
            alias: "repo",
            repo_hash,
            repo_root: repo.path(),
            manifest_id,
            analyzer_id: "fake-workspace",
            now_ns: 1,
        })
        .unwrap()
        .expect("first enqueue should queue");
    assert_eq!(queued.analyzer_id, "fake-workspace");

    // The stamped revision matches the linked-in registry value (7),
    // proving the API computed it internally rather than echoing a
    // caller value.
    let stamped_rev: i64 = conn
        .query_row(
            "SELECT analyzer_revision FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = 'fake-workspace'",
            [manifest_id.0],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stamped_rev, 7);

    // Second call for the same (manifest, analyzer) coalesces while
    // the first is still queued — only one row total.
    let dup = manager
        .enqueue_analyzer_run(super::EnqueueAnalyzerRun {
            conn: &mut conn,
            alias: "repo",
            repo_hash,
            repo_root: repo.path(),
            manifest_id,
            analyzer_id: "fake-workspace",
            now_ns: 2,
        })
        .unwrap();
    assert!(
        dup.is_none(),
        "duplicate enqueue while queued should coalesce"
    );
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM workspace_analysis_runs
             WHERE manifest_id = ?1 AND analyzer_id = 'fake-workspace'",
            [manifest_id.0],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rows, 1);
}

fn test_queue_request<'a>(
    conn: &'a mut rusqlite::Connection,
    repo_root: &'a Path,
    repo_hash: &'a str,
    manifest_id: ManifestId,
    job_id: JobId,
) -> QueueAnalyzerRun<'a> {
    QueueAnalyzerRun {
        conn,
        alias: "repo",
        repo_hash,
        repo_root,
        manifest_id,
        now_ns: job_id,
        job_id,
        analyzer_id: "pyright-lsp",
        analyzer_revision: 1,
        config_hash: "cfg",
        pool_group: None,
    }
}

pub(super) fn prune_test_manager() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Arc<JobManager>,
    rusqlite::Connection,
) {
    let data = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    {
        let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "repo", repo.path().to_str().unwrap(), "repo-hash", 1).unwrap();
        tx.commit().unwrap();
    }
    let conn = cas_store::open(&cas_data_dir.store_db_path("repo-hash")).unwrap();
    let manager = JobManager::new(cas_data_dir);
    (data, repo, manager, conn)
}

pub(super) fn insert_manifest(conn: &rusqlite::Connection, manifest_id: i64) {
    conn.execute(
        "INSERT INTO manifests (manifest_id, kind, built_at_ns)
         VALUES (?1, 'tentative', 0)",
        [manifest_id],
    )
    .unwrap();
}

pub(super) fn insert_anchor(conn: &rusqlite::Connection, anchor_name: &str, manifest_id: i64) {
    conn.execute(
        "INSERT INTO anchors (anchor_name, manifest_id, last_updated_ns)
         VALUES (?1, ?2, 0)",
        rusqlite::params![anchor_name, manifest_id],
    )
    .unwrap();
}

pub(super) fn insert_manifest_parser(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    path: &str,
    blob_sha: &str,
    parser_id: &str,
) {
    conn.execute(
        "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
         VALUES (?1, ?2, 1, 0)",
        rusqlite::params![blob_sha, parser_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO manifest_entries (manifest_id, path, blob_sha)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![manifest_id.0, path, blob_sha],
    )
    .unwrap();
}

pub(super) fn insert_job_run(
    conn: &rusqlite::Connection,
    manifest_id: i64,
    analyzer_id: &str,
    status: &str,
    job_id: Option<i64>,
) {
    insert_job_run_with_cancel(conn, manifest_id, analyzer_id, status, job_id, 0);
}

pub(super) fn insert_job_run_with_cancel(
    conn: &rusqlite::Connection,
    manifest_id: i64,
    analyzer_id: &str,
    status: &str,
    job_id: Option<i64>,
    cancel_requested: i64,
) {
    let finished_at_ns = RunStatus::from_str(status)
        .filter(|state| state.is_terminal())
        .map(|_| 20_i64);
    conn.execute(
        "INSERT INTO workspace_analysis_runs
           (manifest_id, analyzer_id, analyzer_revision, config_hash, status,
            started_at_ns, finished_at_ns, error, job_id, cancel_requested)
         VALUES (?1, ?2, 1, 'cfg', ?3, 10, ?4, NULL, ?5, ?6)",
        rusqlite::params![
            manifest_id,
            analyzer_id,
            status,
            finished_at_ns,
            job_id,
            cancel_requested
        ],
    )
    .unwrap();
}

pub(super) fn persisted_cancel_requested(
    conn: &rusqlite::Connection,
    manifest_id: i64,
    analyzer_id: &str,
) -> i64 {
    conn.query_row(
        "SELECT cancel_requested FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
        rusqlite::params![manifest_id, analyzer_id],
        |r| r.get(0),
    )
    .unwrap()
}

pub(super) fn count_runs(conn: &rusqlite::Connection, manifest_id: i64, analyzer_id: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
        rusqlite::params![manifest_id, analyzer_id],
        |r| r.get(0),
    )
    .unwrap()
}

// ─── Cross-store JobKey / JobId identity tests ───────────────
//
// These tests pin the invariant that daemon-global job
// scheduling does not conflate two stores that happen to share
// a `(manifest_id, analyzer_id)` pair. Without `repo_hash` in
// `JobKey`, a second store's rerun would be silently coalesced
// on top of the first store's queued job; without a
// daemon-global allocator, two stores could hand out the same
// `JobId` and `cancel` would route to whichever store was
// scanned first.

/// Test helper: build a two-store fixture with two aliases
/// (`repo-a` -> `hash-a`, `repo-b` -> `hash-b`), both carrying a
/// tentative manifest at `manifest_id`. Returns the temp dirs
/// (kept alive by the caller) plus the JobManager and both
/// store connections.
#[allow(clippy::type_complexity)]
pub(super) fn two_store_fixture(
    manifest_id: ManifestId,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
    Arc<JobManager>,
    rusqlite::Connection,
    rusqlite::Connection,
) {
    let data = tempfile::tempdir().unwrap();
    let repo_a = tempfile::tempdir().unwrap();
    let repo_b = tempfile::tempdir().unwrap();
    let cas_data_dir = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
    {
        let mut index = cas_registry::open(&cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(&tx, "repo-a", repo_a.path().to_str().unwrap(), "hash-a", 1).unwrap();
        cas_registry::upsert(&tx, "repo-b", repo_b.path().to_str().unwrap(), "hash-b", 1).unwrap();
        tx.commit().unwrap();
    }
    let conn_a = cas_store::open(&cas_data_dir.store_db_path("hash-a")).unwrap();
    let conn_b = cas_store::open(&cas_data_dir.store_db_path("hash-b")).unwrap();
    insert_manifest(&conn_a, manifest_id.0);
    insert_manifest(&conn_b, manifest_id.0);
    let manager = JobManager::new(cas_data_dir);
    (data, repo_a, repo_b, manager, conn_a, conn_b)
}

/// Read the `job_id` currently persisted for `(manifest_id,
/// analyzer_id)` in the given store.
pub(super) fn persisted_job_id(
    conn: &rusqlite::Connection,
    manifest_id: i64,
    analyzer_id: &str,
) -> JobId {
    conn.query_row(
        "SELECT job_id FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
        rusqlite::params![manifest_id, analyzer_id],
        |r| r.get(0),
    )
    .unwrap()
}
