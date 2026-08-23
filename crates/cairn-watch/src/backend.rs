//! Operating-system watcher ownership and callback delivery.

use std::path::Path;
use std::time::Duration;

use notify::{Config, PollWatcher, RecommendedWatcher, RecursiveMode};
#[cfg(not(target_os = "macos"))]
use notify_debouncer_full::new_debouncer;
use notify_debouncer_full::{
    DebounceEventHandler, DebounceEventResult, Debouncer, RecommendedCache, new_debouncer_opt,
};
use tokio::sync::mpsc::Sender;
use tracing::warn;

use crate::matcher::{GitMetadataPaths, resolve_git_metadata};
use crate::{EventClassifier, WatchError, WatchEvent};

/// Handle that keeps the watcher alive. Drop to stop watching.
#[allow(dead_code)] // fields kept only for their Drop side-effects
pub struct WatcherHandle {
    debouncer: WatcherDebouncer,
}

#[allow(clippy::large_enum_variant)]
enum WatcherDebouncer {
    // This enum is intentionally concrete: the production and test
    // backends both rely on Drop side effects from notify-debouncer,
    // and the extra enum size is paid once per watched repo.
    Recommended(Debouncer<RecommendedWatcher, NativeRecommendedCache>),
    Poll(Debouncer<PollWatcher, RecommendedCache>),
}

#[cfg(target_os = "macos")]
type NativeRecommendedCache = notify_debouncer_full::NoCache;
#[cfg(not(target_os = "macos"))]
type NativeRecommendedCache = RecommendedCache;

#[cfg(target_os = "macos")]
fn new_native_debouncer<F: DebounceEventHandler>(
    debounce: Duration,
    event_handler: F,
) -> notify::Result<Debouncer<RecommendedWatcher, NativeRecommendedCache>> {
    new_debouncer_opt::<_, RecommendedWatcher, NativeRecommendedCache>(
        debounce,
        None,
        event_handler,
        NativeRecommendedCache::new(),
        Config::default(),
    )
}

#[cfg(not(target_os = "macos"))]
fn new_native_debouncer<F: DebounceEventHandler>(
    debounce: Duration,
    event_handler: F,
) -> notify::Result<Debouncer<RecommendedWatcher, NativeRecommendedCache>> {
    new_debouncer(debounce, None, event_handler)
}

/// Native watcher backend choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchBackend {
    /// Platform-recommended backend (`FSEvents` on macOS). This is
    /// the production default.
    Recommended,
    /// Polling backend. Used by macOS tempdir-based tests where the
    /// FSEvents stream can fail to deliver any callback.
    Poll,
}

/// Begin watching `repo_root` recursively. Events are debounced over
/// `debounce` and pushed on `tx`. The returned handle must be kept
/// alive; dropping it stops the watcher.
///
/// Gitignore filtering uses the same hierarchical matcher as the startup
/// scanner: the effective `core.excludesFile`, `.git/info/exclude`, and
/// repository-local `.gitignore` files. Included config and selected excludes
/// files are sampled when the matcher is built, wherever they live; only the
/// root config and `config.worktree` are live ignore controls.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend(repo_root, debounce, tx, WatchBackend::Recommended)
}

/// Variant of [`watch_repo`] with an explicit backend. Production
/// callers should prefer [`watch_repo`]; tests and diagnostics can use
/// this to avoid platform-specific native-watcher gaps.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo_with_backend(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend_mode(repo_root, debounce, tx, backend, false)
}

/// Arm a watcher without waiting for the recursive ignore-matcher walk.
///
/// Startup uses this after the durable repository inventory is known. The
/// watcher begins fail-open, so ignore filtering cannot hide filesystem events
/// while the matcher warms in the bounded recovery pool. At its commit
/// linearization point, a successful warm-up installs the matcher for the
/// attempted generation and publishes [`RescanReason::MatcherRecovered`], or
/// coalesces it into an already-pending dirty edge. That edge does not
/// guarantee that its consumer has observed the latest semantic generation.
/// If both fixed workers are permanently stalled, later warm-ups are starved
/// and their watchers remain fail-open. Dynamic registration continues to use
/// [`watch_repo_with_backend`] and therefore preserves its eager
/// matcher-publication contract.
///
/// # Errors
/// Setup-time errors from `notify` or the filesystem.
pub fn watch_repo_with_startup_deferred_matcher(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend_mode(repo_root, debounce, tx, backend, true)
}

fn watch_repo_with_backend_mode(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
    defer_matcher: bool,
) -> Result<WatcherHandle, WatchError> {
    watch_repo_with_backend_mode_after_arm(repo_root, debounce, tx, backend, defer_matcher, |_| {})
}

pub(super) fn watch_repo_with_backend_mode_after_arm(
    repo_root: &Path,
    debounce: Duration,
    tx: Sender<WatchEvent>,
    backend: WatchBackend,
    defer_matcher: bool,
    after_arm: impl FnOnce(&EventClassifier),
) -> Result<WatcherHandle, WatchError> {
    let repo_root = repo_root.canonicalize()?;
    let git_metadata = resolve_git_metadata(&repo_root).unwrap_or_else(|err| {
        warn!(
            root = %repo_root.display(),
            error = %err,
            "git metadata resolution failed; watcher is fail-open"
        );
        GitMetadataPaths::fail_open(&repo_root)
    });
    let classifier = if defer_matcher {
        EventClassifier::new_deferred(&repo_root, git_metadata.clone(), tx)
    } else {
        EventClassifier::new(&repo_root, git_metadata.clone(), tx)
    };

    let callback_classifier = classifier.clone();
    let event_handler = move |result: DebounceEventResult| {
        handle_debounce_result(&callback_classifier, result);
    };
    let mut debouncer = match backend {
        WatchBackend::Recommended => {
            WatcherDebouncer::Recommended(new_native_debouncer(debounce, event_handler)?)
        }
        WatchBackend::Poll => {
            WatcherDebouncer::Poll(new_debouncer_opt::<_, PollWatcher, RecommendedCache>(
                debounce,
                None,
                event_handler,
                RecommendedCache::new(),
                Config::default().with_poll_interval(debounce),
            )?)
        }
    };
    debouncer.watch(&repo_root, RecursiveMode::Recursive)?;
    // Linked worktrees keep HEAD in their worktree git dir and refs /
    // info/exclude in the common git dir. Watch both identities.
    for git_root in git_metadata.watch_roots() {
        if git_root.is_dir() {
            let _ = debouncer.watch(&git_root, RecursiveMode::Recursive);
        }
    }
    after_arm(&classifier);
    if defer_matcher {
        classifier.begin_deferred_matcher_warmup();
    }
    Ok(WatcherHandle { debouncer })
}

/// Bridge between the `notify-debouncer-full` callback and the
/// classifier. A successful batch is classified per event; an error
/// batch is collapsed to a single [`RescanReason::WatchError`] edge
/// after logging each individual error, so a burst of backend
/// errors does not translate into a burst of rescan events.
pub(super) fn handle_debounce_result(classifier: &EventClassifier, result: DebounceEventResult) {
    match result {
        Ok(events) => classifier.handle_batch(&events),
        Err(errs) => {
            for err in &errs {
                warn!(?err, "notify error");
            }
            classifier.handle_watch_error_batch();
        }
    }
}

impl WatcherDebouncer {
    fn watch(
        &mut self,
        path: impl AsRef<Path>,
        recursive_mode: RecursiveMode,
    ) -> notify::Result<()> {
        match self {
            WatcherDebouncer::Recommended(debouncer) => debouncer.watch(path, recursive_mode),
            WatcherDebouncer::Poll(debouncer) => debouncer.watch(path, recursive_mode),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileChange;
    #[cfg(target_os = "macos")]
    use crate::RescanReason;

    /// Wait for the first touched edge of a session where the FSEvents
    /// stream may still be settling. The probe file
    /// is re-written on a fixed interval so that even if the very first
    /// few writes land inside the stream's initial dead zone (observed
    /// on /private/tmp under sandboxed runners), a later write still
    /// triggers a delivered event. The total wait budget is `total`;
    /// each retry write happens every `retry_every`.
    async fn wait_for_probe_with_retries(
        rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
        probe: &std::path::Path,
        total: Duration,
        retry_every: Duration,
    ) -> Option<WatchEvent> {
        let probe_name = probe.file_name()?.to_os_string();
        let deadline = tokio::time::Instant::now() + total;
        let mut attempt: u32 = 0;
        // Initial write — content varies per attempt so the debouncer
        // cannot dedupe a later retry against the first one.
        std::fs::write(probe, format!("probe-{attempt}")).ok()?;
        let mut last_write = tokio::time::Instant::now();
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return None;
            }
            let until_retry = (last_write + retry_every).saturating_duration_since(now);
            let until_deadline = deadline.saturating_duration_since(now);
            let wait = until_retry.min(until_deadline);
            match tokio::time::timeout(wait, rx.recv()).await {
                Ok(Some(WatchEvent::File {
                    path,
                    change: FileChange::Touched,
                })) if path.file_name() == Some(probe_name.as_os_str()) => {
                    return Some(WatchEvent::File {
                        path,
                        change: FileChange::Touched,
                    });
                }
                Ok(Some(_)) => continue,
                Ok(None) => return None,
                Err(_) => {
                    attempt += 1;
                    std::fs::write(probe, format!("probe-{attempt}")).ok()?;
                    last_write = tokio::time::Instant::now();
                }
            }
        }
    }

    async fn write_until_file_edge(
        rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
        path: &Path,
        contents: &str,
    ) -> FileChange {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            std::fs::write(path, contents).unwrap();
            let retry_at = tokio::time::Instant::now() + Duration::from_millis(250);
            loop {
                tokio::select! {
                    event = rx.recv() => match event {
                        Some(WatchEvent::File { path: event_path, change })
                            if event_path == path => return change,
                        Some(_) => {}
                        None => panic!("watch channel closed before Ruby config edge"),
                    },
                    () = tokio::time::sleep_until(retry_at) => break,
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Ruby config edge timed out for {}",
                path.display()
            );
        }
    }

    async fn assert_ruby_lsp_config_edges_for_backend(backend: WatchBackend) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join(".ruby-lsp/cache")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let handle =
            watch_repo_with_backend(&root, Duration::from_millis(50), tx, backend).unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        while rx.try_recv().is_ok() {}

        for relative in [".ruby-lsp/Gemfile", ".ruby-lsp/Gemfile.lock"] {
            let config_path = root.join(relative);
            for (contents, expected_change) in [
                (Some("first config snapshot\n"), FileChange::Touched),
                (Some("second config snapshot\n"), FileChange::Touched),
                (None, FileChange::Deleted),
            ] {
                while rx.try_recv().is_ok() {}
                if let Some(contents) = contents {
                    assert_eq!(
                        write_until_file_edge(&mut rx, &config_path, contents).await,
                        expected_change
                    );
                } else {
                    std::fs::remove_file(&config_path).unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            match rx.recv().await {
                                Some(WatchEvent::File { path, .. }) if path == config_path => break,
                                Some(_) => {}
                                None => panic!("watch channel closed before Ruby config edge"),
                            }
                        }
                    })
                    .await
                    .unwrap_or_else(|_| panic!("Ruby config delete edge timed out for {relative}"));
                }
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
        }

        while rx.try_recv().is_ok() {}
        std::fs::write(root.join(".ruby-lsp/cache/index"), "generated\n").unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            rx.try_recv().is_err(),
            "generated Ruby LSP artifacts must remain silent"
        );
        drop(handle);
    }

    #[tokio::test]
    async fn end_to_end_file_event() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        // Initialize a fake repo so the .git watch path exists.
        std::fs::create_dir_all(root.join(".git")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let _handle =
            watch_repo_with_backend(&root, Duration::from_millis(150), tx, WatchBackend::Poll)
                .unwrap();

        let probe = root.join(".probe");
        // Use the polling backend here because macOS tempdir-backed
        // native watchers can fail to deliver any callback in this
        // isolated unit-test shape, even though production daemon
        // probes use the default recommended backend.
        let probe_event = wait_for_probe_with_retries(
            &mut rx,
            &probe,
            Duration::from_secs(10),
            Duration::from_millis(500),
        )
        .await;
        assert!(
            probe_event.is_some(),
            "watcher delivered no Touched event for .probe within 10s of retries"
        );
    }

    #[tokio::test]
    async fn polling_watcher_suppresses_ruby_lsp_subtree_but_keeps_ordinary_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join(".ruby-lsp/nested")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let handle =
            watch_repo_with_backend(&root, Duration::from_millis(50), tx, WatchBackend::Poll)
                .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        while rx.try_recv().is_ok() {}

        std::fs::write(root.join(".ruby-lsp/.gitignore"), "*\n").unwrap();
        std::fs::write(root.join(".ruby-lsp/nested/Gemfile.lock"), "generated\n").unwrap();
        std::fs::write(root.join(".ruby-lsp/nested/.git"), "gitdir: elsewhere\n").unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let leaked = rx.try_recv();
        assert!(
            matches!(leaked, Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "polling backend leaked a File or Rescan edge from .ruby-lsp: {leaked:?}"
        );

        std::fs::remove_file(root.join(".ruby-lsp/nested/.git")).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let leaked = rx.try_recv();
        assert!(
            matches!(leaked, Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
            "polling backend leaked a File or Rescan edge from removed .ruby-lsp marker: {leaked:?}"
        );

        let probe = root.join("ordinary.rs");
        let ordinary = wait_for_probe_with_retries(
            &mut rx,
            &probe,
            Duration::from_secs(5),
            Duration::from_millis(250),
        )
        .await;
        assert!(
            matches!(
                ordinary,
                Some(WatchEvent::File { path, .. })
                    if path.file_name() == probe.file_name()
            ),
            "ordinary working-tree files must remain observable"
        );
        drop(handle);
    }

    #[tokio::test]
    async fn polling_watcher_observes_exact_ruby_lsp_config_controls() {
        assert_ruby_lsp_config_edges_for_backend(WatchBackend::Poll).await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn native_watcher_observes_exact_ruby_lsp_config_controls() {
        assert_ruby_lsp_config_edges_for_backend(WatchBackend::Recommended).await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn native_watcher_without_file_id_cache_preserves_dirty_edges() {
        async fn receive_matching(
            rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
            matches: impl Fn(&WatchEvent) -> bool,
        ) -> WatchEvent {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let event = rx.recv().await.expect("watch event channel closed");
                    if matches(&event) {
                        return event;
                    }
                }
            })
            .await
            .expect("native watcher delivered no matching dirty edge")
        }

        let tmp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(tmp.path()).unwrap();
        std::fs::create_dir_all(root.join(".git/refs/heads")).unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        let atomic_dst = root.join("atomic.rs");
        let old_file = root.join("rename-old.rs");
        let new_file = root.join("rename-new.rs");
        let old_dir = root.join("rename-old-dir");
        let new_dir = root.join("rename-new-dir");
        let deleted = root.join("deleted.rs");
        std::fs::write(&atomic_dst, "old").unwrap();
        std::fs::write(&old_file, "file").unwrap();
        std::fs::create_dir(&old_dir).unwrap();
        std::fs::write(old_dir.join("nested.rs"), "nested").unwrap();
        std::fs::write(&deleted, "delete me").unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let _handle = watch_repo_with_backend(
            &root,
            Duration::from_millis(50),
            tx,
            WatchBackend::Recommended,
        )
        .unwrap();
        let probe = root.join("probe.rs");
        assert!(
            wait_for_probe_with_retries(
                &mut rx,
                &probe,
                Duration::from_secs(5),
                Duration::from_millis(250),
            )
            .await
            .is_some(),
            "native watcher did not arm"
        );

        std::fs::write(&probe, "modified").unwrap();
        receive_matching(
            &mut rx,
            |event| matches!(event, WatchEvent::File { path, .. } if path == &probe),
        )
        .await;

        let atomic_tmp = root.join("atomic.tmp");
        std::fs::write(&atomic_tmp, "new").unwrap();
        std::fs::rename(&atomic_tmp, &atomic_dst).unwrap();
        receive_matching(&mut rx, |event| {
            matches!(event, WatchEvent::File { path, change: FileChange::Touched } if path == &atomic_dst)
                || matches!(event, WatchEvent::Rescan { .. })
        })
        .await;

        std::fs::rename(&old_file, &new_file).unwrap();
        receive_matching(&mut rx, |event| {
            matches!(event, WatchEvent::File { path, change: FileChange::Touched } if path == &old_file || path == &new_file)
                || matches!(event, WatchEvent::Rescan { .. })
        })
        .await;

        std::fs::rename(&old_dir, &new_dir).unwrap();
        receive_matching(&mut rx, |event| {
            matches!(
                event,
                WatchEvent::Rescan {
                    reason: RescanReason::DirectoryTopologyChanged
                }
            )
        })
        .await;

        let created = root.join("created.rs");
        std::fs::write(&created, "created").unwrap();
        receive_matching(&mut rx, |event| {
            matches!(event, WatchEvent::File { path, change: FileChange::Touched } if path == &created)
        })
        .await;
        std::fs::remove_file(&deleted).unwrap();
        receive_matching(&mut rx, |event| {
            matches!(event, WatchEvent::File { path, change: FileChange::Deleted | FileChange::Touched } if path == &deleted)
        })
        .await;

        std::fs::write(root.join(".gitignore"), "target/\n").unwrap();
        receive_matching(&mut rx, |event| {
            matches!(
                event,
                WatchEvent::Rescan {
                    reason: RescanReason::IgnoreRulesChanged
                }
            )
        })
        .await;
        receive_matching(&mut rx, |event| {
            matches!(
                event,
                WatchEvent::Rescan {
                    reason: RescanReason::MatcherRecovered
                }
            )
        })
        .await;
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/next\n").unwrap();
        receive_matching(&mut rx, |event| matches!(event, WatchEvent::Git(_))).await;

        let nested_git = root.join("nested/.git");
        std::fs::create_dir_all(nested_git.parent().unwrap()).unwrap();
        std::fs::write(&nested_git, "gitdir: elsewhere\n").unwrap();
        receive_matching(&mut rx, |event| {
            matches!(
                event,
                WatchEvent::Rescan {
                    reason: RescanReason::DirectoryTopologyChanged
                }
            )
        })
        .await;

        while rx.try_recv().is_ok() {}
        let ignored = root.join("target/ignored.rs");
        std::fs::create_dir_all(ignored.parent().unwrap()).unwrap();
        std::fs::write(&ignored, "ignored").unwrap();
        let leaked = tokio::time::timeout(Duration::from_millis(500), async {
            while let Some(event) = rx.recv().await {
                if matches!(event, WatchEvent::File { path, .. } if path == ignored) {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        assert!(!leaked, "native watcher leaked an ignored target edge");
    }
}
