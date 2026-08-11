//! Test-only observation of watcher → reconcile → analyzer churn.
//!
//! The recorder is test-only observation state. Production owners make their
//! decisions first; these hooks copy the resulting facts afterwards so tests
//! can separate event classification, manifest reuse, and enqueue policy
//! without changing their ordering. Publication and observation failures are
//! captured for assertions after the production owner has released its locks.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::manifest::{ManifestEntry, ManifestId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatchOutput {
    File { path: Vec<u8>, change: &'static str },
    Rescan { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchRecord {
    pub repo_hash: String,
    pub source_batch_ordinal: u64,
    pub output: WatchOutput,
    pub generation: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReconcileRecord {
    pub repo_hash: String,
    pub generation: i64,
    pub manifest_id: ManifestId,
    pub reused: bool,
    pub entry_count: usize,
    pub physical_fingerprint: u64,
    pub changed_paths: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueDecision {
    New,
    Coalesced,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnqueueRecord {
    pub repo_hash: String,
    pub generation: i64,
    pub analyzer_id: String,
    pub config_hash: String,
    pub revision: u32,
    pub decision: EnqueueDecision,
    pub terminal_status: Option<String>,
    pub failure_class: Option<&'static str>,
    pub job_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationCheck {
    pub repo_hash: String,
    pub generation: i64,
    pub analyzer_id: String,
    pub job_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservationFailureKind {
    MissingAdmissionBeforePublication,
    MissingActiveGeneration,
    TerminalAdmissionUnavailable,
    TerminalObservationTimedOut,
    TerminalRowUnavailable,
    PriorObservationAfterBegin,
    PriorManifestUnavailable,
    ManifestEntriesUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservationFailure {
    pub kind: ObservationFailureKind,
    pub repo_hash: String,
    pub generation: Option<i64>,
    pub analyzer_id: Option<String>,
    pub job_id: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChurnSnapshot {
    pub watch: Vec<WatchRecord>,
    pub reconcile: Vec<ReconcileRecord>,
    pub enqueue: Vec<EnqueueRecord>,
    pub publication_checks: Vec<PublicationCheck>,
    pub observation_failures: Vec<ObservationFailure>,
}

#[derive(Debug)]
pub(crate) struct ChurnRecorder {
    repo_hash: String,
    repo_root: PathBuf,
    canonical_repo_root: Option<PathBuf>,
    snapshot: Mutex<ChurnSnapshot>,
    active_generation: Mutex<Option<i64>>,
    changed_paths: Mutex<HashMap<i64, Vec<Vec<u8>>>>,
    fail_next_prior_manifest_observation: AtomicBool,
    fail_next_manifest_entries_observation: AtomicBool,
    wait_for_terminal_after_publication: AtomicBool,
    terminal_observed: Condvar,
}

impl ChurnRecorder {
    fn new(repo_hash: String, repo_root: PathBuf, canonical_repo_root: Option<PathBuf>) -> Self {
        Self {
            repo_hash,
            repo_root,
            canonical_repo_root,
            snapshot: Mutex::new(ChurnSnapshot::default()),
            active_generation: Mutex::new(None),
            changed_paths: Mutex::new(HashMap::new()),
            fail_next_prior_manifest_observation: AtomicBool::new(false),
            fail_next_manifest_entries_observation: AtomicBool::new(false),
            wait_for_terminal_after_publication: AtomicBool::new(false),
            terminal_observed: Condvar::new(),
        }
    }

    pub(crate) fn snapshot(&self) -> ChurnSnapshot {
        self.snapshot.lock().unwrap().clone()
    }

    fn matches(&self, repo_hash: &str) -> bool {
        self.repo_hash == repo_hash
    }

    pub(crate) fn active_generation(&self) -> Option<i64> {
        *self.active_generation.lock().unwrap()
    }

    pub(crate) fn fail_next_prior_manifest_observation(&self) {
        self.fail_next_prior_manifest_observation
            .store(true, Ordering::Release);
    }

    pub(crate) fn fail_next_manifest_entries_observation(&self) {
        self.fail_next_manifest_entries_observation
            .store(true, Ordering::Release);
    }

    /// Opt into a test-only wait for each published job's terminal observation.
    /// The publication path enters the wait only after releasing admission
    /// ownership; it is bounded at five seconds and records an
    /// `ObservationFailure` on timeout.
    pub(crate) fn wait_for_terminal_after_publication(&self) {
        self.wait_for_terminal_after_publication
            .store(true, Ordering::Release);
    }

    /// Relativize one observed path against the repository root.
    ///
    /// The raw root stays the registry/register identity; the canonical root
    /// is only a fallback for backend-reported watch paths that spell the same
    /// directory through an alias. The event leaf itself is never canonicalized,
    /// so after an accepted root is stripped, the remaining relative bytes
    /// preserve the watcher's spelling.
    fn relative_bytes(&self, path: &Path) -> Vec<u8> {
        if path.is_relative() {
            return path_bytes(path);
        }
        let relative = path.strip_prefix(&self.repo_root).ok().or_else(|| {
            self.canonical_repo_root
                .as_deref()
                .and_then(|root| path.strip_prefix(root).ok())
        });
        let relative = relative.unwrap_or(path);
        path_bytes(relative)
    }
}

static RECORDER: OnceLock<Mutex<Option<Arc<ChurnRecorder>>>> = OnceLock::new();
static RECORDER_TEST_LOCK: Mutex<()> = Mutex::new(());

pub(crate) struct ChurnRecorderGuard {
    _exclusive: MutexGuard<'static, ()>,
}

impl Drop for ChurnRecorderGuard {
    fn drop(&mut self) {
        *RECORDER.get_or_init(Default::default).lock().unwrap() = None;
    }
}

pub(crate) fn install(
    repo_hash: impl Into<String>,
    repo_root: impl Into<PathBuf>,
) -> (Arc<ChurnRecorder>, ChurnRecorderGuard) {
    let exclusive = RECORDER_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let repo_root = repo_root.into();
    let canonical_repo_root = repo_root.canonicalize().ok();
    let recorder = Arc::new(ChurnRecorder::new(
        repo_hash.into(),
        repo_root,
        canonical_repo_root,
    ));
    *RECORDER.get_or_init(Default::default).lock().unwrap() = Some(recorder.clone());
    (
        recorder,
        ChurnRecorderGuard {
            _exclusive: exclusive,
        },
    )
}

fn active() -> Option<Arc<ChurnRecorder>> {
    RECORDER
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .clone()
}

pub(crate) fn record_watch_batch(
    repo_hash: &str,
    source_batch_ordinal: u64,
    events: &[cairn_watch::WatchEvent],
) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    let mut snapshot = recorder.snapshot.lock().unwrap();
    for event in events {
        let output = match event {
            cairn_watch::WatchEvent::File { path, change } => WatchOutput::File {
                path: recorder.relative_bytes(path),
                change: match change {
                    cairn_watch::FileChange::Touched => "touched",
                    cairn_watch::FileChange::Deleted => "deleted",
                },
            },
            cairn_watch::WatchEvent::Rescan { reason } => WatchOutput::Rescan {
                reason: match reason {
                    cairn_watch::RescanReason::IgnoreRulesChanged => "ignore_rules_changed",
                    cairn_watch::RescanReason::DirectoryTopologyChanged => {
                        "directory_topology_changed"
                    }
                    cairn_watch::RescanReason::BackendRequested => "backend_requested",
                    cairn_watch::RescanReason::WatchError => "watch_error",
                    cairn_watch::RescanReason::MatcherRecovered => "matcher_recovered",
                },
            },
            cairn_watch::WatchEvent::Git(_) => continue,
        };
        snapshot.watch.push(WatchRecord {
            repo_hash: repo_hash.to_string(),
            source_batch_ordinal,
            output,
            generation: None,
        });
    }
}

pub(crate) fn correlate_watch_generation(
    repo_hash: &str,
    source_batch_ordinal: u64,
    generation: i64,
) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    // The dirty receipt is the sole authority joining a normalized source
    // batch to the generation that reconcile actually accepted.
    let mut changed_paths = Vec::new();
    let mut snapshot = recorder.snapshot.lock().unwrap();
    for record in snapshot.watch.iter_mut().filter(|record| {
        record.repo_hash == repo_hash && record.source_batch_ordinal == source_batch_ordinal
    }) {
        record.generation = Some(generation);
        if let WatchOutput::File { path, .. } = &record.output {
            changed_paths.push(path.clone());
        }
    }
    drop(snapshot);
    recorder
        .changed_paths
        .lock()
        .unwrap()
        .insert(generation, changed_paths);
}

pub(crate) fn begin_reconcile(repo_hash: &str, generation: i64) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    *recorder.active_generation.lock().unwrap() = Some(generation);
}

pub(crate) fn finish_reconcile(
    repo_hash: &str,
    generation: i64,
    manifest_id: ManifestId,
    reused: bool,
    entries: &[ManifestEntry],
) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    let changed_paths = recorder
        .changed_paths
        .lock()
        .unwrap()
        .remove(&generation)
        .unwrap_or_default();
    recorder
        .snapshot
        .lock()
        .unwrap()
        .reconcile
        .push(ReconcileRecord {
            repo_hash: repo_hash.to_string(),
            generation,
            manifest_id,
            reused,
            entry_count: entries.len(),
            physical_fingerprint: physical_fingerprint(entries),
            changed_paths,
        });
    *recorder.active_generation.lock().unwrap() = None;
}

pub(crate) fn abort_reconcile(repo_hash: &str) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    *recorder.active_generation.lock().unwrap() = None;
}

pub(crate) fn take_prior_manifest_observation_failure(repo_hash: &str) -> bool {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return false;
    };
    if !recorder
        .fail_next_prior_manifest_observation
        .swap(false, Ordering::AcqRel)
    {
        return false;
    }
    if let Some(generation) = recorder.active_generation() {
        record_observation_failure(
            repo_hash,
            ObservationFailureKind::PriorObservationAfterBegin,
            Some(generation),
            None,
            None,
        );
    }
    true
}

pub(crate) fn take_manifest_entries_observation_failure(repo_hash: &str) -> bool {
    active()
        .filter(|recorder| recorder.matches(repo_hash))
        .is_some_and(|recorder| {
            recorder
                .fail_next_manifest_entries_observation
                .swap(false, Ordering::AcqRel)
        })
}

pub(crate) fn record_observation_failure(
    repo_hash: &str,
    kind: ObservationFailureKind,
    generation: Option<i64>,
    analyzer_id: Option<&str>,
    job_id: Option<i64>,
) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    recorder
        .snapshot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .observation_failures
        .push(ObservationFailure {
            kind,
            repo_hash: repo_hash.to_string(),
            generation,
            analyzer_id: analyzer_id.map(str::to_string),
            job_id,
        });
    recorder.terminal_observed.notify_all();
}

pub(crate) fn record_enqueue(
    repo_hash: &str,
    analyzer_id: &str,
    config_hash: &str,
    revision: u32,
    decision: EnqueueDecision,
    job_id: Option<i64>,
) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    let Some(generation) = *recorder.active_generation.lock().unwrap() else {
        return;
    };
    recorder
        .snapshot
        .lock()
        .unwrap()
        .enqueue
        .push(EnqueueRecord {
            repo_hash: repo_hash.to_string(),
            generation,
            analyzer_id: analyzer_id.to_string(),
            config_hash: config_hash.to_string(),
            revision,
            decision,
            terminal_status: None,
            failure_class: None,
            job_id,
        });
}

pub(crate) fn record_existing_for_root(
    repo_root: &Path,
    analyzer_id: &str,
    config_hash: &str,
    revision: u32,
) {
    let Some(recorder) = active().filter(|recorder| recorder.repo_root == repo_root) else {
        return;
    };
    record_enqueue(
        &recorder.repo_hash,
        analyzer_id,
        config_hash,
        revision,
        EnqueueDecision::Existing,
        None,
    );
}

pub(crate) fn record_scheduler_publication(repo_hash: &str, analyzer_id: &str, job_id: i64) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    let Some(generation) = *recorder.active_generation.lock().unwrap() else {
        record_observation_failure(
            repo_hash,
            ObservationFailureKind::MissingActiveGeneration,
            None,
            Some(analyzer_id),
            Some(job_id),
        );
        return;
    };
    let mut snapshot = recorder.snapshot.lock().unwrap();
    let recorded = snapshot.enqueue.iter().any(|record| {
        record.repo_hash == repo_hash
            && record.generation == generation
            && record.analyzer_id == analyzer_id
            && record.decision == EnqueueDecision::New
            && record.job_id == Some(job_id)
    });
    if recorded {
        snapshot.publication_checks.push(PublicationCheck {
            repo_hash: repo_hash.to_string(),
            generation,
            analyzer_id: analyzer_id.to_string(),
            job_id,
        });
    } else {
        snapshot.observation_failures.push(ObservationFailure {
            kind: ObservationFailureKind::MissingAdmissionBeforePublication,
            repo_hash: repo_hash.to_string(),
            generation: Some(generation),
            analyzer_id: Some(analyzer_id.to_string()),
            job_id: Some(job_id),
        });
    }
}

pub(crate) fn await_terminal_after_scheduler_publication(repo_hash: &str, job_id: i64) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    if !recorder
        .wait_for_terminal_after_publication
        .load(Ordering::Acquire)
    {
        return;
    }
    let snapshot = recorder
        .snapshot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (mut snapshot, timeout) = recorder
        .terminal_observed
        .wait_timeout_while(snapshot, Duration::from_secs(5), |snapshot| {
            let terminal = snapshot
                .enqueue
                .iter()
                .any(|record| record.job_id == Some(job_id) && record.terminal_status.is_some());
            let failed = snapshot
                .observation_failures
                .iter()
                .any(|failure| failure.job_id == Some(job_id));
            !terminal && !failed
        })
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if timeout.timed_out() {
        snapshot.observation_failures.push(ObservationFailure {
            kind: ObservationFailureKind::TerminalObservationTimedOut,
            repo_hash: repo_hash.to_string(),
            generation: None,
            analyzer_id: None,
            job_id: Some(job_id),
        });
    }
}

pub(crate) fn record_terminal(
    repo_hash: &str,
    analyzer_id: &str,
    job_id: i64,
    status: &str,
    failure_class: Option<&'static str>,
) {
    let Some(recorder) = active().filter(|recorder| recorder.matches(repo_hash)) else {
        return;
    };
    let mut snapshot = recorder
        .snapshot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(record) = snapshot
        .enqueue
        .iter_mut()
        .find(|record| record.job_id == Some(job_id))
    {
        record.terminal_status = Some(status.to_string());
        record.failure_class = failure_class;
    } else {
        snapshot.observation_failures.push(ObservationFailure {
            kind: ObservationFailureKind::TerminalAdmissionUnavailable,
            repo_hash: repo_hash.to_string(),
            generation: None,
            analyzer_id: Some(analyzer_id.to_string()),
            job_id: Some(job_id),
        });
    }
    drop(snapshot);
    recorder.terminal_observed.notify_all();
}

pub(crate) fn physical_fingerprint(entries: &[ManifestEntry]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for entry in entries {
        for byte in entry
            .path
            .as_bytes()
            .iter()
            .chain(entry.blob_sha.as_bytes())
        {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().into_owned().into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_bytes_accepts_raw_canonical_and_relative_child_or_parent_paths() {
        let raw_root = PathBuf::from("/tmp/churn-recorder-alias");
        let canonical_root = PathBuf::from("/private/tmp/churn-recorder-real");
        let recorder = ChurnRecorder::new(
            "repo".to_string(),
            raw_root.clone(),
            Some(canonical_root.clone()),
        );

        for (path, expected) in [
            (
                raw_root.join("src/watch_probe.ts"),
                b"src/watch_probe.ts".as_slice(),
            ),
            (raw_root.join("src"), b"src".as_slice()),
            (
                canonical_root.join("src/watch_probe.ts"),
                b"src/watch_probe.ts".as_slice(),
            ),
            (canonical_root.join("src"), b"src".as_slice()),
            (
                PathBuf::from("src/watch_probe.ts"),
                b"src/watch_probe.ts".as_slice(),
            ),
            (PathBuf::from("src"), b"src".as_slice()),
        ] {
            assert_eq!(recorder.relative_bytes(&path), expected);
        }
    }

    #[test]
    fn relative_bytes_does_not_accept_an_outside_path_by_suffix() {
        let repo_root = PathBuf::from("/tmp/churn-recorder-repo");
        let outside = PathBuf::from("/tmp/churn-recorder-other/src");
        let recorder = ChurnRecorder::new("repo".to_string(), repo_root, None);

        assert_eq!(recorder.relative_bytes(&outside), path_bytes(&outside));
    }

    #[test]
    fn missing_admission_before_publication_is_recorded_without_poisoning_owner_lock() {
        let temp = tempfile::tempdir().unwrap();
        let (recorder, _guard) = install("repo", temp.path());
        begin_reconcile("repo", 9);
        let admission_lock = Mutex::new(());

        {
            let _owner = admission_lock.lock().unwrap();
            record_scheduler_publication("repo", "test-analyzer", 73);
        }
        assert!(
            admission_lock.lock().is_ok(),
            "owner lock must not be poisoned"
        );

        assert_eq!(
            recorder.snapshot().observation_failures,
            [ObservationFailure {
                kind: ObservationFailureKind::MissingAdmissionBeforePublication,
                repo_hash: "repo".into(),
                generation: Some(9),
                analyzer_id: Some("test-analyzer".into()),
                job_id: Some(73),
            }]
        );
    }

    #[cfg(unix)]
    #[test]
    fn relative_bytes_preserves_non_utf8_root_and_child_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let root = PathBuf::from(OsString::from_vec(b"/tmp/churn-recorder-\xff".to_vec()));
        let child = root.join(OsString::from_vec(b"src/child-\xfe.rs".to_vec()));
        let recorder = ChurnRecorder::new("repo".to_string(), root, None);

        assert_eq!(recorder.relative_bytes(&child), b"src/child-\xfe.rs");
    }

    #[cfg(unix)]
    #[test]
    fn install_relativizes_events_from_a_symlink_alias_canonical_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let real_root = temp.path().join("real");
        let alias_root = temp.path().join("alias");
        std::fs::create_dir(&real_root).unwrap();
        symlink(&real_root, &alias_root).unwrap();
        // The temporary directory itself may use an alias spelling, so canonicalize
        // the real root too.
        let canonical_real_root = real_root.canonicalize().unwrap();

        let (recorder, _guard) = install("repo", &alias_root);

        assert_eq!(
            recorder.relative_bytes(&canonical_real_root.join("src/watch_probe.ts")),
            b"src/watch_probe.ts"
        );
        assert_eq!(
            recorder.relative_bytes(&canonical_real_root.join("src")),
            b"src"
        );
    }
}
