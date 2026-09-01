use super::*;
use crate::lsp::Error;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_subscriber::fmt::MakeWriter;

#[cfg(unix)]
use crate::lsp::client::{
    TEST_FAKE_LSP_ENV, classify_group_signal, finish_owned_child_cleanup,
    finish_owned_child_termination, map_family_sweep_join, reject_unverified_child,
    validate_process_group_identity,
};
#[cfg(unix)]
use rustix::process::Pid;

#[derive(Clone, Default)]
struct CapturedLog {
    bytes: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl CapturedLog {
    fn contents(&self) -> String {
        String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
    }
}

impl Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLog {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_global_pool_initialization_runs_once() {
    static TEST_POOL: OnceLock<LspClientPool> = OnceLock::new();
    static TEST_INIT: StdMutex<()> = StdMutex::new(());

    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let initializations = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let barrier = Arc::clone(&barrier);
        let initializations = Arc::clone(&initializations);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            initialize_global_pool(&TEST_POOL, &TEST_INIT, || {
                initializations.fetch_add(1, Ordering::SeqCst);
                LspClientPool::with_config(NonZeroUsize::new(1).unwrap(), None)
            })
            .map(|pool| std::ptr::from_ref(pool) as usize)
        }));
    }

    let mut addresses = Vec::new();
    for task in tasks {
        addresses.push(
            task.await
                .expect("initializer task must not panic")
                .unwrap(),
        );
    }
    assert_eq!(initializations.load(Ordering::SeqCst), 1);
    assert!(addresses.iter().all(|address| *address == addresses[0]));
}

#[test]
fn failed_global_pool_initialization_can_retry() {
    let cell = OnceLock::new();
    let init_gate = StdMutex::new(());
    let initializations = AtomicUsize::new(0);

    let first = initialize_global_pool(&cell, &init_gate, || {
        initializations.fetch_add(1, Ordering::SeqCst);
        Err(Error::Protocol("transient runtime failure".into()))
    });
    let first = match first {
        Ok(_) => panic!("first initialization must fail"),
        Err(error) => error,
    };
    assert_eq!(
        first.to_string(),
        "LSP protocol error: transient runtime failure"
    );
    assert!(cell.get().is_none());

    let second = initialize_global_pool(&cell, &init_gate, || {
        initializations.fetch_add(1, Ordering::SeqCst);
        LspClientPool::with_config(NonZeroUsize::new(1).unwrap(), None)
    })
    .unwrap();
    let third = initialize_global_pool(&cell, &init_gate, || {
        initializations.fetch_add(1, Ordering::SeqCst);
        Err(Error::Protocol("must not run after success".into()))
    })
    .unwrap();

    assert!(std::ptr::eq(second, third));
    assert_eq!(initializations.load(Ordering::SeqCst), 2);
}

#[cfg(unix)]
struct FakeProbeBinary {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

#[cfg(unix)]
impl FakeProbeBinary {
    fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
fn fake_probe_binary(expected_arg: &'static str) -> FakeProbeBinary {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fake-lsp");
    fs::write(
            &path,
            format!(
                "#!/bin/sh\nif [ \"$#\" -eq 1 ] && [ \"$1\" = \"{expected_arg}\" ]; then exit 0; fi\nexit 1"
            ),
        )
        .unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).unwrap();
    FakeProbeBinary { _dir: dir, path }
}

#[test]
fn pool_key_uses_launch_configuration() {
    let repo = tempfile::tempdir().unwrap();
    let key_a = PoolKey::lsp(
        "rust",
        repo.path(),
        "rust-analyzer-lsp",
        Path::new("ra"),
        "cfg-a",
    )
    .unwrap();
    let key_b = PoolKey::lsp(
        "rust",
        repo.path(),
        "rust-analyzer-lsp",
        Path::new("ra"),
        "cfg-b",
    )
    .unwrap();
    let key_go = PoolKey::lsp("go", repo.path(), "gopls-lsp", Path::new("gopls"), "cfg-a").unwrap();

    assert_eq!(
        key_a.canonical_repo_root,
        std::fs::canonicalize(repo.path()).unwrap()
    );
    assert_ne!(key_a, key_b);
    assert_eq!(key_a.language, "rust");
    assert_eq!(key_go.analyzer_id, "gopls-lsp");
}

#[cfg(unix)]
#[test]
fn lsp_pool_runs_version_flag_availability_probe() {
    let binary = fake_probe_binary("--version");
    let runtime = Runtime::new().unwrap();

    runtime
        .block_on(check_lsp_available(
            binary.path(),
            &AvailabilityStrategy::VersionFlag,
            Duration::from_secs(5),
        ))
        .unwrap();
    assert!(matches!(
        runtime.block_on(check_lsp_available(
            binary.path(),
            &AvailabilityStrategy::VersionNoFlag,
            Duration::from_secs(5),
        )),
        Err(Error::BinaryMissing(_))
    ));
}

#[cfg(unix)]
#[test]
fn lsp_pool_runs_version_no_flag_availability_probe() {
    let binary = fake_probe_binary("version");
    let runtime = Runtime::new().unwrap();

    runtime
        .block_on(check_lsp_available(
            binary.path(),
            &AvailabilityStrategy::VersionNoFlag,
            Duration::from_secs(5),
        ))
        .unwrap();
    assert!(matches!(
        runtime.block_on(check_lsp_available(
            binary.path(),
            &AvailabilityStrategy::VersionFlag,
            Duration::from_secs(5),
        )),
        Err(Error::BinaryMissing(_))
    ));
}

#[test]
fn lsp_pool_checks_path_exists_executable_availability_without_spawning() {
    let binary = tempfile::NamedTempFile::new().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = binary.as_file().metadata().unwrap().permissions();
        perms.set_mode(0o755);
        binary.as_file().set_permissions(perms).unwrap();
    }
    let runtime = Runtime::new().unwrap();

    runtime
        .block_on(check_lsp_available(
            binary.path(),
            &AvailabilityStrategy::PathExistsExecutable,
            Duration::from_secs(1),
        ))
        .unwrap();

    let missing = binary.path().with_file_name("missing-lsp");
    assert!(matches!(
        runtime.block_on(check_lsp_available(
            &missing,
            &AvailabilityStrategy::PathExistsExecutable,
            Duration::from_secs(1),
        )),
        Err(Error::BinaryMissing(_))
    ));
}

#[test]
fn lsp_pool_dispatches_availability_strategy_per_server() {
    assert_eq!(
        availability_probe_args(&AvailabilityStrategy::VersionFlag),
        Some(&["--version"][..])
    );
    assert_eq!(
        availability_probe_args(&AvailabilityStrategy::VersionNoFlag),
        Some(&["version"][..])
    );
    assert_eq!(
        availability_probe_args(&AvailabilityStrategy::PathExistsExecutable),
        None
    );
}

#[test]
fn lsp_pool_dispatches_progress_quiescence_readiness_to_wait_hook() {
    let runtime = Runtime::new().unwrap();
    let timeout = Duration::from_secs(2);
    let mut waited = None;

    runtime
        .block_on(dispatch_readiness(
            &ReadinessStrategy::ProgressQuiescence { timeout },
            DefinitionReadiness::SpawnSpec,
            |wait| {
                waited = Some(wait);
                async { Ok(()) }
            },
        ))
        .unwrap();

    assert_eq!(waited, Some(ReadinessWait::Raw { timeout }));
}

#[test]
fn lsp_pool_dispatches_semantic_readiness_deadlines_to_wait_hook() {
    let runtime = Runtime::new().unwrap();
    let hard_timeout = Duration::from_secs(120);
    let stall_timeout = Duration::from_secs(90);
    let mut waited = None;

    runtime
        .block_on(dispatch_readiness(
            &ReadinessStrategy::ProgressQuiescence {
                timeout: Duration::from_secs(1),
            },
            DefinitionReadiness::Semantic {
                hard_timeout,
                stall_timeout,
            },
            |wait| {
                waited = Some(wait);
                async { Ok(()) }
            },
        ))
        .unwrap();

    assert_eq!(
        waited,
        Some(ReadinessWait::Semantic {
            hard_timeout,
            stall_timeout,
        })
    );
}

#[test]
fn lsp_pool_skips_wait_hook_for_initialize_response_readiness() {
    let runtime = Runtime::new().unwrap();
    let mut waited = false;

    runtime
        .block_on(dispatch_readiness(
            &ReadinessStrategy::InitializeResponseOnly,
            DefinitionReadiness::SpawnSpec,
            |wait| {
                let _ = wait;
                waited = true;
                async { Ok(()) }
            },
        ))
        .unwrap();

    assert!(!waited);
}

#[test]
fn lsp_pool_dispatches_readiness_strategy_per_server() {
    fn readiness_name(readiness: &ReadinessStrategy) -> &'static str {
        match readiness {
            ReadinessStrategy::ProgressQuiescence { .. } => "progress",
            ReadinessStrategy::InitializeResponseOnly => "initialize",
        }
    }

    // Keep this as a complete pre-0.8.5 public struct literal plus an
    // exhaustive two-variant match. Together they catch source-incompatible
    // public field/type or closed-enum changes.
    let rust = LspSpawnSpec {
        binary: PathBuf::from("rust-analyzer"),
        workspace_root: PathBuf::from("/tmp/repo"),
        config_hash: "cfg".into(),
        request_timeout: Duration::from_secs(1),
        availability: AvailabilityStrategy::VersionFlag,
        readiness: ReadinessStrategy::ProgressQuiescence {
            timeout: Duration::from_secs(2),
        },
        language_id: "rust",
        launch_args: Vec::new(),
        env: Vec::new(),
        initialization_options: serde_json::json!({
            "experimental": {
                "serverStatusNotification": true
            }
        }),
    };
    let pyright = LspSpawnSpec {
        readiness: ReadinessStrategy::InitializeResponseOnly,
        language_id: "python",
        launch_args: vec!["--stdio".to_string()],
        initialization_options: serde_json::json!({}),
        ..rust.clone()
    };

    assert!(matches!(
        rust.readiness,
        ReadinessStrategy::ProgressQuiescence { .. }
    ));
    assert!(matches!(
        pyright.readiness,
        ReadinessStrategy::InitializeResponseOnly
    ));
    assert_eq!(
        rust.initialization_options["experimental"]["serverStatusNotification"],
        true
    );
    assert_eq!(pyright.launch_args, vec!["--stdio"]);
    assert_eq!(pyright.initialization_options, serde_json::json!({}));
    assert_eq!(readiness_name(&rust.readiness), "progress");
    assert_eq!(readiness_name(&pyright.readiness), "initialize");
}

#[test]
fn empty_pool_shutdown_is_noop() {
    let pool = LspClientPool::new().unwrap();
    assert_eq!(pool.len(), 0);
    pool.shutdown_all().unwrap();
    assert_eq!(pool.len(), 0);
    // shutdown_all transitions to Stopped.
    assert_eq!(pool.mode(), PoolMode::Stopped);
}

// ─── Capacity parse contract (pure helper, no env mutation) ─

fn test_key(n: u32) -> PoolKey {
    // Registry-only tests don't need real repo roots; a bare
    // canonical path with a unique language suffix is enough
    // to make `PoolKey` distinct.
    PoolKey {
        canonical_repo_root: PathBuf::from(format!("/tmp/cairn-test-{n}")),
        language: format!("lang-{n}"),
        analyzer_id: format!("analyzer-{n}"),
        binary: PathBuf::from("bin"),
        config_hash: "cfg".into(),
    }
}

fn pool(capacity: usize) -> LspClientPool {
    LspClientPool::with_capacity(NonZeroUsize::new(capacity).unwrap()).unwrap()
}

// Since `acquire_lease` is async, tests drive it through the
// pool's own runtime — same path production uses.
fn acquire(pool: &LspClientPool, key: PoolKey) -> Result<PoolLease> {
    pool.runtime()
        .block_on(async { pool.acquire_lease(key).await })
}

fn cap(raw: Option<&str>) -> usize {
    capacity_from_env_value(raw).get()
}

#[test]
fn capacity_default_when_env_unset() {
    assert_eq!(cap(None), DEFAULT_POOL_CAPACITY);
}

#[test]
fn capacity_within_bounds_takes_effect() {
    assert_eq!(cap(Some("1")), 1);
    assert_eq!(cap(Some("8")), 8);
    assert_eq!(cap(Some(" 16 ")), 16); // trim
    assert_eq!(cap(Some(&MAX_POOL_CAPACITY.to_string())), MAX_POOL_CAPACITY);
}

#[test]
fn capacity_above_max_is_clamped() {
    assert_eq!(cap(Some("65")), MAX_POOL_CAPACITY);
    assert_eq!(cap(Some("999")), MAX_POOL_CAPACITY);
    // Positive numeric that overflows i64/u64 still clamps
    // (not falling into the invalid bucket).
    assert_eq!(cap(Some(&"9".repeat(40))), MAX_POOL_CAPACITY);
}

#[test]
fn capacity_zero_or_negative_falls_back_to_default() {
    assert_eq!(cap(Some("0")), DEFAULT_POOL_CAPACITY);
    assert_eq!(cap(Some("-1")), DEFAULT_POOL_CAPACITY);
    assert_eq!(cap(Some("-999")), DEFAULT_POOL_CAPACITY);
}

#[test]
fn capacity_invalid_string_falls_back_to_default() {
    assert_eq!(cap(Some("not-a-number")), DEFAULT_POOL_CAPACITY);
    assert_eq!(cap(Some("1.5")), DEFAULT_POOL_CAPACITY); // no float
    assert_eq!(cap(Some("")), DEFAULT_POOL_CAPACITY);
    assert_eq!(cap(Some("   ")), DEFAULT_POOL_CAPACITY);
    assert_eq!(cap(Some("-")), DEFAULT_POOL_CAPACITY);
    assert_eq!(cap(Some("-abc")), DEFAULT_POOL_CAPACITY);
}

#[test]
fn idle_ttl_defaults_overrides_and_zero_disables_sweeper() {
    assert_eq!(idle_ttl_from_env_value(None), Some(DEFAULT_IDLE_TTL));
    assert_eq!(
        idle_ttl_from_env_value(Some(" 30 ")),
        Some(Duration::from_secs(30))
    );
    assert_eq!(idle_ttl_from_env_value(Some("0")), None);
    assert_eq!(
        idle_ttl_from_env_value(Some("invalid")),
        Some(DEFAULT_IDLE_TTL)
    );

    let disabled = LspClientPool::with_config(NonZeroUsize::new(1).unwrap(), None).unwrap();
    assert!(
        disabled._idle_sweeper.is_none(),
        "TTL=0 must not spawn an idle sweep task"
    );
}

#[test]
fn capacity_env_warnings_do_not_log_raw_values() {
    let overflow = "9".repeat(40);
    let cases = [
        ("sensitive-capacity-token", "invalid", DEFAULT_POOL_CAPACITY),
        ("-0007", "out_of_range", DEFAULT_POOL_CAPACITY),
        ("000", "out_of_range", DEFAULT_POOL_CAPACITY),
        ("00065", "out_of_range", MAX_POOL_CAPACITY),
        (overflow.as_str(), "overflow", MAX_POOL_CAPACITY),
    ];

    for (raw, reason, expected) in cases {
        let output = CapturedLog::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(output.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            assert_eq!(cap(Some(raw)), expected);
        });

        let captured = output.contents();
        assert!(captured.contains(POOL_CAPACITY_ENV));
        assert!(captured.contains(&format!("reason=\"{reason}\"")));
        assert!(
            !captured.contains(raw),
            "capacity warning exposed raw value {raw:?}: {captured}"
        );
    }
}

#[test]
fn idle_ttl_env_warnings_do_not_log_raw_values() {
    let overflow = "9".repeat(40);
    let cases = [
        ("sensitive-ttl-token", "invalid"),
        (overflow.as_str(), "overflow"),
    ];

    for (raw, reason) in cases {
        let output = CapturedLog::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(output.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            assert_eq!(idle_ttl_from_env_value(Some(raw)), Some(DEFAULT_IDLE_TTL));
        });

        let captured = output.contents();
        assert!(captured.contains(IDLE_TTL_ENV));
        assert!(captured.contains(&format!("reason=\"{reason}\"")));
        assert!(
            !captured.contains(raw),
            "idle TTL warning exposed raw value {raw:?}: {captured}"
        );
    }
}

// ─── Force-shutdown outcome classifier ─────────────────────

#[test]
fn classify_force_shutdown_all_ok_yields_neither_flag() {
    let results = vec![
        CleanupOutcome::Terminal(CleanupTerminal::Proven, None),
        CleanupOutcome::Terminal(CleanupTerminal::Proven, None),
    ];
    let out = classify_force_shutdown_results(&results);
    assert!(out.first_os_residual.is_none());
    assert!(out.first_invariant_failure.is_none());
    assert!(!out.pending);
}

#[test]
fn classify_force_shutdown_preserves_os_residual_without_invariant() {
    let results = vec![CleanupOutcome::Terminal(
        CleanupTerminal::OsResidual("real".into()),
        None,
    )];
    let out = classify_force_shutdown_results(&results);
    assert_eq!(out.first_os_residual.as_deref(), Some("real"));
    assert!(out.first_invariant_failure.is_none());
    assert!(!out.pending);
}

#[test]
fn classify_force_shutdown_preserves_invariant_failure() {
    let results = vec![CleanupOutcome::Terminal(
        CleanupTerminal::InvariantFailure {
            message: "broken custody".into(),
            os_residual: None,
        },
        None,
    )];
    let out = classify_force_shutdown_results(&results);
    assert!(out.first_os_residual.is_none());
    assert_eq!(
        out.first_invariant_failure.as_deref(),
        Some("broken custody")
    );
    assert!(!out.pending);
}

#[test]
fn classify_force_shutdown_pending_is_distinct_from_terminal_facts() {
    let results = vec![
        CleanupOutcome::Pending,
        CleanupOutcome::Terminal(CleanupTerminal::Proven, None),
    ];
    let out = classify_force_shutdown_results(&results);
    assert!(out.pending);
    assert!(out.first_os_residual.is_none());
    assert!(out.first_invariant_failure.is_none());
}

#[test]
fn classify_force_shutdown_preserves_os_and_invariant_axes() {
    let results = vec![
        CleanupOutcome::Terminal(CleanupTerminal::OsResidual("os".into()), None),
        CleanupOutcome::Terminal(
            CleanupTerminal::InvariantFailure {
                message: "inv".into(),
                os_residual: None,
            },
            None,
        ),
    ];
    let out = classify_force_shutdown_results(&results);
    assert_eq!(out.first_os_residual.as_deref(), Some("os"));
    assert_eq!(out.first_invariant_failure.as_deref(), Some("inv"));
    assert!(
        out.public_error
            .as_ref()
            .is_some_and(Error::is_termination_unproven)
    );
}

fn error_contains_protocol(error: &Error) -> bool {
    match error {
        Error::Protocol(_) => true,
        Error::OperationWithCleanupFailure { original, cleanup } => {
            error_contains_protocol(original) || error_contains_protocol(cleanup)
        }
        _ => false,
    }
}

fn error_contains_pool_poisoned(error: &Error) -> bool {
    match error {
        Error::PoolPoisoned => true,
        Error::OperationWithCleanupFailure { original, cleanup } => {
            error_contains_pool_poisoned(original) || error_contains_pool_poisoned(cleanup)
        }
        _ => false,
    }
}

fn child_termination_fact_count(error: &Error, message: &str) -> usize {
    match error {
        Error::ChildTerminationFailed(candidate) => usize::from(candidate == message),
        Error::OperationWithCleanupFailure { original, cleanup } => {
            child_termination_fact_count(original, message)
                + child_termination_fact_count(cleanup, message)
        }
        _ => 0,
    }
}

fn outer_cleanup_residual(error: &Error) -> Option<&str> {
    match error {
        Error::ChildTerminationFailed(message) => Some(message),
        Error::OperationWithCleanupFailure { cleanup, .. } => match cleanup.as_ref() {
            Error::ChildTerminationFailed(message) => Some(message),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn force_classifier_uses_typed_first_residual_as_unique_outer_cleanup() {
    let residual_a = CleanupOutcome::Terminal(
        CleanupTerminal::OsResidual("group signal residual A".into()),
        None,
    );
    let residual_b = CleanupOutcome::Terminal(
        CleanupTerminal::OsResidual("leader wait residual B".into()),
        None,
    );
    let protocol = CleanupOutcome::Terminal(
        CleanupTerminal::Proven,
        Some(Error::Protocol("graceful shutdown failed".into())),
    );
    let invariant = CleanupOutcome::invariant("custody invariant", None);

    for (results, first, other) in [
        (
            vec![
                residual_a.clone(),
                protocol.clone(),
                invariant.clone(),
                residual_b.clone(),
            ],
            "group signal residual A",
            "leader wait residual B",
        ),
        (
            vec![residual_b, protocol.clone(), invariant, residual_a],
            "leader wait residual B",
            "group signal residual A",
        ),
    ] {
        let out = classify_force_shutdown_results(&results);
        assert_eq!(out.first_os_residual.as_deref(), Some(first));
        assert_eq!(
            out.first_invariant_failure.as_deref(),
            Some("custody invariant")
        );
        let public_error = out.public_error.as_ref().unwrap();
        assert!(public_error.is_termination_unproven());
        assert_eq!(outer_cleanup_residual(public_error), Some(first));
        assert_eq!(child_termination_fact_count(public_error, first), 1);
        assert_eq!(child_termination_fact_count(public_error, other), 1);
        assert!(error_contains_protocol(public_error));
        assert!(error_contains_pool_poisoned(public_error));
    }
}

#[test]
fn force_classifier_without_os_residual_is_not_termination_unproven() {
    let out = classify_force_shutdown_results(&[
        CleanupOutcome::Terminal(
            CleanupTerminal::Proven,
            Some(Error::Protocol("graceful shutdown failed".into())),
        ),
        CleanupOutcome::invariant("custody invariant", None),
    ]);

    assert!(out.first_os_residual.is_none());
    assert_eq!(
        out.first_invariant_failure.as_deref(),
        Some("custody invariant")
    );
    let public_error = out.public_error.as_ref().unwrap();
    assert!(!public_error.is_termination_unproven());
    assert!(error_contains_protocol(public_error));
    assert!(error_contains_pool_poisoned(public_error));
}

#[test]
fn force_classifier_typed_os_axis_repairs_a_nontermination_public_shape() {
    let out = classify_force_shutdown_results(&[CleanupOutcome::Terminal(
        CleanupTerminal::OsResidual("typed OS residual".into()),
        Some(Error::Protocol("graceful shutdown failed".into())),
    )]);

    assert_eq!(out.first_os_residual.as_deref(), Some("typed OS residual"));
    let public_error = out.public_error.as_ref().unwrap();
    assert!(public_error.is_termination_unproven());
    assert!(error_contains_protocol(public_error));
}

#[test]
fn pool_existing_key_reuse_does_not_grow() {
    let pool = pool(4);
    let key = test_key(1);
    let l1 = acquire(&pool, key.clone()).unwrap();
    assert_eq!(pool.len(), 1);
    age_record(&pool, &key, Duration::from_secs(10));
    let aged_at = pool.registry.lock().unwrap().entries[&key].last_used_at;
    let l2 = acquire(&pool, key.clone()).unwrap();
    // Two leases on the same key must share a single record.
    assert_eq!(pool.len(), 1);
    assert!(Arc::ptr_eq(&l1.entry, &l2.entry));
    assert_eq!(pool.active_leases(&key), Some(2));
    assert!(pool.registry.lock().unwrap().entries[&key].last_used_at > aged_at);
    age_record(&pool, &key, Duration::from_secs(10));
    let aged_at = pool.registry.lock().unwrap().entries[&key].last_used_at;
    drop(l1);
    assert_eq!(pool.active_leases(&key), Some(1));
    assert!(pool.registry.lock().unwrap().entries[&key].last_used_at > aged_at);
    drop(l2);
    assert_eq!(pool.active_leases(&key), Some(0));
}

fn age_record(pool: &LspClientPool, key: &PoolKey, age: Duration) {
    pool.registry
        .lock()
        .unwrap()
        .entries
        .get_mut(key)
        .unwrap()
        .last_used_at = Instant::now() - age;
}

fn sweep_idle(pool: &LspClientPool, ttl: Duration, timeout: Duration) -> Result<usize> {
    pool.runtime().block_on(LspClientPool::sweep_idle_once(
        &pool.registry,
        Instant::now(),
        ttl,
        timeout,
    ))
}

#[test]
fn idle_sweep_evicts_entry_older_than_ttl() {
    let pool = pool(2);
    let key = test_key(10);
    drop(acquire(&pool, key.clone()).unwrap());
    age_record(&pool, &key, Duration::from_secs(11));

    assert_eq!(
        sweep_idle(&pool, Duration::from_secs(10), Duration::from_millis(50)).unwrap(),
        1
    );
    assert_eq!(pool.len(), 0);
}

#[test]
fn idle_sweep_preserves_entry_with_active_lease() {
    let pool = pool(2);
    let key = test_key(11);
    let lease = acquire(&pool, key.clone()).unwrap();
    age_record(&pool, &key, Duration::from_secs(11));

    assert_eq!(
        sweep_idle(&pool, Duration::from_secs(10), Duration::from_millis(50)).unwrap(),
        0
    );
    assert_eq!(pool.active_leases(&key), Some(1));
    drop(lease);
}

#[test]
fn bounded_final_shutdown_bypasses_graceful_shutdown_gate() {
    let pool = Arc::new(pool(1));
    let key = test_key(12);
    drop(acquire(&pool, key.clone()).unwrap());
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();

    // Hold the graceful shutdown gate. Bounded final shutdown must
    // bypass it and use the independent process-control path.
    let gate = pool.runtime().block_on(entry.shutdown_gate.lock());
    pool.shutdown_all_bounded(Duration::from_millis(20))
        .expect("bounded final shutdown must not wait on graceful shutdown gate");
    assert_eq!(pool.mode(), PoolMode::Stopped);

    drop(gate);
}

#[test]
fn bounded_final_shutdown_cleans_owned_control_after_registry_poison() {
    let pool = pool(1);
    let key = test_key(123);
    drop(acquire(&pool, key.clone()).unwrap());
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let client = unstarted_client_for_cleanup();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    let registry = Arc::clone(&pool.registry);
    assert!(
        std::thread::spawn(move || {
            let _registry = registry.lock().unwrap();
            panic!("poison registry before final shutdown publication");
        })
        .join()
        .is_err()
    );

    let error = pool
        .shutdown_all_bounded(Duration::from_secs(1))
        .expect_err("registry invariant remains caller-visible");

    assert!(matches!(error, Error::PoolPoisoned));
    assert_eq!(client.cleanup_run_count_for_test(), 1);
    let mode = match pool.registry.lock() {
        Ok(registry) => registry.mode,
        Err(poisoned) => poisoned.into_inner().mode,
    };
    assert_eq!(mode, PoolMode::Stopped);
}

#[test]
fn pool_evicts_idle_lru_victim_before_inserting_new_key() {
    let pool = pool(2);
    let a = test_key(1);
    let b = test_key(2);
    let c = test_key(3);
    // Acquire and release A first — its last_used is smaller.
    drop(acquire(&pool, a.clone()).unwrap());
    // Then B — B's last_used is larger.
    drop(acquire(&pool, b.clone()).unwrap());
    assert_eq!(pool.len(), 2);
    // C forces eviction of the idle LRU (A, older last_used).
    let _lc = acquire(&pool, c.clone()).unwrap();
    assert_eq!(pool.len(), 2);
    assert!(pool.active_leases(&a).is_none(), "A must have been evicted");
    assert!(pool.active_leases(&b).is_some(), "B must remain");
    assert_eq!(pool.active_leases(&c), Some(1));
}

#[test]
fn pool_never_evicts_leased_entry() {
    let pool = pool(1);
    let _l1 = acquire(&pool, test_key(1)).unwrap();
    // Pool is full and the sole occupant is leased — new key
    // cannot evict it.
    let err = acquire(&pool, test_key(2)).unwrap_err();
    assert!(
        matches!(err, Error::PoolAtCapacity { capacity: 1 }),
        "expected PoolAtCapacity, got {err:?}"
    );
    assert_eq!(pool.len(), 1);
}

#[test]
fn pool_all_leased_returns_pool_at_capacity_with_stable_len() {
    let pool = pool(3);
    let _a = acquire(&pool, test_key(1)).unwrap();
    let _b = acquire(&pool, test_key(2)).unwrap();
    let _c = acquire(&pool, test_key(3)).unwrap();
    assert_eq!(pool.len(), 3);
    let err = acquire(&pool, test_key(4)).unwrap_err();
    assert!(matches!(err, Error::PoolAtCapacity { capacity: 3 }));
    // len must not have grown even by one transient slot.
    assert_eq!(pool.len(), 3);
}

#[test]
fn pool_drained_arc_drop_does_not_decrement_replacement() {
    // Simulate the sequence:
    // 1. acquire K → lease L1 with Arc A1
    // 2. force_shutdown_all evicts everything (drains record for K)
    // 3. re-acquire K → new lease L2 with a different Arc A2
    // 4. drop L1 → must NOT mutate A2's active_leases
    //
    // We can't run force_shutdown here (needs runtime + entries
    // that shut down cleanly), so we simulate step 2 manually
    // by pretending the drain happened.
    let pool = pool(2);
    let k = test_key(1);
    let l1 = acquire(&pool, k.clone()).unwrap();
    let old_arc = Arc::clone(&l1.entry);
    // Manually replace the entry Arc under the lock —
    // equivalent to what a Draining pass followed by a
    // fresh acquire would produce.
    {
        let mut reg = pool.registry.lock().unwrap();
        let record = reg.entries.get_mut(&k).unwrap();
        record.entry = Arc::new(PoolEntry::default());
        record.active_leases = 5; // sentinel — must not shrink
    }
    assert_eq!(pool.active_leases(&k), Some(5));
    drop(l1);
    // Even though we dropped L1 for key K, the record now
    // holds a different Arc, so ptr_eq guards the decrement.
    assert_eq!(
        pool.active_leases(&k),
        Some(5),
        "drained-Arc lease drop must not decrement replacement"
    );
    // Confirm old_arc really was different.
    let cur = { pool.registry.lock().unwrap().entries[&k].entry.clone() };
    assert!(!Arc::ptr_eq(&old_arc, &cur));
}

#[test]
fn pool_concurrent_distinct_keys_never_exceed_capacity() {
    // Drive concurrent acquires through std threads so the
    // pool's internal runtime handles the async work; using
    // `#[tokio::test]` here panics with "Cannot drop a
    // runtime in a context where blocking is not allowed"
    // when `LspClientPool` is dropped at the end.
    use std::sync::Barrier;
    let pool = Arc::new(pool(4));
    let n = 12usize;
    let barrier = Arc::new(Barrier::new(n));
    let mut handles = Vec::new();
    for i in 0..n {
        let p = Arc::clone(&pool);
        let b = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            b.wait();
            let p2 = Arc::clone(&p);
            p.runtime()
                .block_on(async move { p2.acquire_lease(test_key(i as u32)).await })
        }));
    }
    let mut leases: Vec<PoolLease> = Vec::new();
    let mut cap = 0usize;
    let mut other = 0usize;
    for h in handles {
        match h.join().unwrap() {
            Ok(lease) => leases.push(lease),
            Err(Error::PoolAtCapacity { .. }) => cap += 1,
            Err(_) => other += 1,
        }
    }
    assert_eq!(leases.len() + cap, n);
    assert_eq!(other, 0);
    assert!(
        leases.len() <= 4,
        "successful acquires ({}) must not exceed capacity 4",
        leases.len()
    );
    assert!(pool.len() <= 4, "pool len {} must not exceed 4", pool.len());
}

#[test]
fn pool_force_shutdown_rejects_concurrent_acquire() {
    let pool = pool(2);
    // Poke the mode to Draining and confirm acquire returns
    // PoolDraining without racing a real drain.
    {
        let mut reg = pool.registry.lock().unwrap();
        reg.mode = PoolMode::Draining;
    }
    let err = acquire(&pool, test_key(1)).unwrap_err();
    assert!(matches!(err, Error::PoolDraining));
    assert_eq!(pool.len(), 0);
}

#[test]
fn pool_force_shutdown_success_returns_running() {
    let pool = pool(2);
    // Empty pool: force drain instantly returns Ok, mode back
    // to Running.
    pool.force_shutdown_all(Duration::from_millis(50)).unwrap();
    assert_eq!(pool.mode(), PoolMode::Running);
}

#[test]
fn published_drain_remains_visible_to_bounded_shutdown() {
    let pool = pool(2);
    drop(acquire(&pool, test_key(31)).unwrap());
    drop(acquire(&pool, test_key(32)).unwrap());

    let mut registry = pool.registry.lock().unwrap();
    let drain = registry.publish_live_drain().unwrap();
    assert_eq!(drain.entries.len(), 2);
    assert!(
        registry.entries.is_empty(),
        "publish and live-map drain must be atomic under the registry lock"
    );
    assert_eq!(
        registry.draining_entries.get(&drain.id).map(Vec::len),
        Some(2),
        "bounded shutdown must observe entries after the live map is drained"
    );

    let bounded = registry.take_all_for_bounded_shutdown();
    assert_eq!(bounded.len(), 2);
    assert!(registry.draining_entries.is_empty());
    assert!(
        !registry.finish_published_drain(drain.id),
        "graceful finalize must not clobber a batch already taken by bounded shutdown"
    );
}

#[test]
fn pool_poisoned_mode_permanently_rejects_acquire() {
    let pool = pool(2);
    {
        let mut reg = pool.registry.lock().unwrap();
        reg.mode = PoolMode::Halted;
    }
    let err = acquire(&pool, test_key(1)).unwrap_err();
    assert!(matches!(err, Error::PoolPoisoned));
}

#[test]
fn pool_stopped_mode_rejects_acquire() {
    let pool = pool(2);
    pool.shutdown_all().unwrap();
    assert_eq!(pool.mode(), PoolMode::Stopped);
    let err = acquire(&pool, test_key(1)).unwrap_err();
    assert!(matches!(err, Error::PoolStopped));
}

// ─── Child lifecycle tests (real subprocess, Unix) ──────────

#[cfg(unix)]
struct HangBinary {
    _dir: tempfile::TempDir,
    path: PathBuf,
    pid_file: PathBuf,
}

#[cfg(unix)]
impl HangBinary {
    fn new() -> Self {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hang-lsp.py");
        let pid_file = dir.path().join("hang.pid");
        // Bake the pid file path directly into the script so
        // the test does not need to mutate a process-global
        // env var (which would race under parallel tests).
        let script = format!(
            "#!/usr/bin/env python3\n\
                 import os, time\n\
                 fd = os.open({p:?}, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)\n\
                 os.write(fd, str(os.getpid()).encode())\n\
                 os.fsync(fd)\n\
                 os.close(fd)\n\
                 time.sleep(300)\n",
            p = pid_file.display().to_string(),
        );
        fs::write(&path, script).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        Self {
            _dir: dir,
            path,
            pid_file,
        }
    }

    fn env(&self) -> Vec<(String, String)> {
        // Kept for callers that used the env-based approach;
        // the current script bakes the pid file path in, so
        // this env is ignored.
        vec![(
            "CAIRN_TEST_PID_FILE".into(),
            self.pid_file.display().to_string(),
        )]
    }
}

#[cfg(unix)]
fn read_pid(pid_file: &Path, deadline: std::time::Instant) -> Option<u32> {
    while std::time::Instant::now() < deadline {
        if let Ok(s) = std::fs::read_to_string(pid_file)
            && let Ok(pid) = s.trim().parse::<u32>()
        {
            return Some(pid);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // `kill -0 <pid>` returns exit 0 iff the process exists
    // and we can signal it. Portable POSIX check without a
    // libc dev-dep. Silence stderr — "No such process"
    // messages are the expected condition when polling for
    // reaped children and would otherwise pollute test
    // output.
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(unix)]
fn wait_until_dead(pid: u32, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if !pid_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

#[cfg(unix)]
#[test]
fn client_initialize_failure_kills_child_before_returning() {
    // A Python "hang" binary writes its PID at import time
    // (with `fsync`), then blocks on `time.sleep`. It never
    // speaks LSP, so `initialize` times out; `start_configured`
    // must then reap the child via the failure-path cleanup.
    //
    // Python startup is deterministic enough under parallel
    // test load that the PID write always beats the outer
    // `request_timeout`, so this test is not flaky.
    let hang = HangBinary::new();
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let err = rt.block_on(async {
        LspClient::start_configured(
            &hang.path,
            Vec::new(),
            hang.env(),
            workspace.path(),
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await
    });
    assert!(err.is_err(), "handshake must fail for a non-LSP binary");
    let pid = read_pid(
        &hang.pid_file,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .expect("hang script must have written its PID");
    assert!(
        wait_until_dead(pid, Duration::from_secs(3)),
        "child pid {pid} must be reaped after initialize failure"
    );
}

// ─── Real-subprocess Python fake LSP: covers initialize-
//     success + no-progress code paths for readiness / drop
//     / direct-force_terminate lifecycle assertions. ────────

/// Minimal LSP-speaking Python fake:
/// - writes its PID to `CAIRN_TEST_PID_FILE`
/// - responds to `initialize`
/// - accepts `initialized` notification (no response)
/// - responds to `shutdown` and exits on `exit`
/// - never sends `$/progress` (so `ProgressQuiescence` readiness
///   always times out — used to pin readiness cleanup)
#[cfg(unix)]
const FAKE_LSP_SCRIPT: &str = r#"import sys, os, json, subprocess, fcntl, time

spawn_count_file = os.environ.get("CAIRN_TEST_SPAWN_COUNT_FILE")
spawn_ordinal = 0
if spawn_count_file:
    try:
        with open(spawn_count_file) as f:
            spawn_ordinal = int(f.read() or "0")
    except FileNotFoundError:
        pass
    spawn_ordinal += 1
    with open(spawn_count_file, "w") as f:
        f.write(str(spawn_ordinal))
        f.flush()
        os.fsync(f.fileno())
fail_initialize_ordinal = int(os.environ.get("CAIRN_TEST_FAIL_INITIALIZE_ORDINAL", "-1"))

initialize_state_file = os.environ.get("CAIRN_TEST_INITIALIZE_STATE_FILE")
initialize_delay_ms = int(os.environ.get("CAIRN_TEST_INITIALIZE_DELAY_MS", "0"))
def update_initialize_state(delta):
    if not initialize_state_file:
        return
    with open(initialize_state_file, "a+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.seek(0)
        raw = f.read().strip()
        current, maximum = map(int, raw.split(",")) if raw else (0, 0)
        current += delta
        maximum = max(maximum, current)
        f.seek(0)
        f.truncate()
        f.write("{},{}".format(current, maximum))
        f.flush()
        os.fsync(f.fileno())
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)

pid_file = os.environ.get("CAIRN_TEST_PID_FILE")
if pid_file:
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

marker_file = os.environ.get("CAIRN_TEST_MARKER_FILE")
if marker_file:
    with open(marker_file, "a") as f:
        f.write("{}|{}\n".format(
            os.environ.get("CAIRN_LSP_OWNER", "<unset>"),
            os.environ.get("CAIRN_LSP_FAMILY", "<unset>")))
        f.flush()

grandchild_pid_file = os.environ.get("CAIRN_TEST_GRANDCHILD_PID_FILE")
if grandchild_pid_file:
    grandchild_env = None
    if os.environ.get("CAIRN_TEST_SCRUB_GRANDCHILD_MARKERS") == "1":
        grandchild_env = {
            key: value for key, value in os.environ.items()
            if key not in ("CAIRN_LSP_OWNER", "CAIRN_LSP_FAMILY")
        }
    grandchild = subprocess.Popen(["sleep", "300"], env=grandchild_env)
    with open(grandchild_pid_file, "w") as f:
        f.write(str(grandchild.pid))

def read_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line == b"\r\n":
            break
        k, _, v = line.decode().strip().partition(":")
        headers[k.strip()] = v.strip()
    length = int(headers.get("Content-Length", "0"))
    if length == 0:
        return None
    body = sys.stdin.buffer.read(length)
    return json.loads(body)

def write_message(msg):
    body = json.dumps(msg).encode()
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()

methods_file = os.environ.get("CAIRN_TEST_METHODS_FILE")
def log_method(m):
    if methods_file:
        with open(methods_file, "a") as f:
            f.write("{}:{}\n".format(os.getpid(), m))

while True:
    msg = read_message()
    if msg is None:
        break
    method = msg.get("method")
    if method:
        log_method(method)
    if method == "initialize" and spawn_ordinal != fail_initialize_ordinal:
        update_initialize_state(1)
        try:
            if initialize_delay_ms:
                time.sleep(initialize_delay_ms / 1000.0)
        finally:
            update_initialize_state(-1)
        write_message({"jsonrpc": "2.0", "id": msg["id"], "result": {"capabilities": {}}})
    elif method == "shutdown" and os.environ.get("CAIRN_TEST_RESPOND_SHUTDOWN", "1") == "1":
        write_message({"jsonrpc": "2.0", "id": msg["id"], "result": None})
    elif method == "exit" and os.environ.get("CAIRN_TEST_IGNORE_EXIT", "0") != "1":
        sys.exit(0)
    elif method == "textDocument/definition" and os.environ.get("CAIRN_TEST_CLOSE_STDOUT_ON_DEFINITION") == "1":
        os.close(sys.stdout.fileno())
    elif method == "textDocument/definition" and os.environ.get("CAIRN_TEST_EXIT_ON_DEFINITION") == "1":
        sys.stderr.write("test-only: exit after first definition\n")
        sys.stderr.flush()
        sys.exit(1)
    else:
        # Silently ignore didOpen / didChange / initialized / etc.
        # so ProgressQuiescence readiness never satisfies.
        pass
"#;

#[cfg(unix)]
struct FakeLspBinary {
    _dir: tempfile::TempDir,
    interpreter: PathBuf,
    script_path: PathBuf,
    pid_file: PathBuf,
}

#[cfg(unix)]
impl FakeLspBinary {
    fn new() -> Self {
        use std::fs;
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("fake-lsp.py");
        let pid_file = dir.path().join("fake-lsp.pid");
        fs::write(&script_path, FAKE_LSP_SCRIPT).unwrap();
        Self {
            _dir: dir,
            interpreter: stable_python_interpreter(),
            script_path,
            pid_file,
        }
    }

    fn binary(&self) -> &Path {
        &self.interpreter
    }

    fn launch_args(&self) -> Vec<String> {
        vec![self.script_path.to_string_lossy().into_owned()]
    }

    fn env(&self) -> Vec<(String, String)> {
        vec![
            (TEST_FAKE_LSP_ENV.into(), "1".into()),
            (
                "CAIRN_TEST_PID_FILE".into(),
                self.pid_file.display().to_string(),
            ),
        ]
    }

    fn env_with_grandchild(&self, grandchild_pid_file: &Path) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push((
            "CAIRN_TEST_GRANDCHILD_PID_FILE".into(),
            grandchild_pid_file.display().to_string(),
        ));
        env
    }

    fn env_with_marker_log(&self, marker_file: &Path) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push((
            "CAIRN_TEST_MARKER_FILE".into(),
            marker_file.display().to_string(),
        ));
        env
    }

    fn env_with_initialize_probe(&self, state_file: &Path) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push((
            "CAIRN_TEST_INITIALIZE_STATE_FILE".into(),
            state_file.display().to_string(),
        ));
        env.push(("CAIRN_TEST_INITIALIZE_DELAY_MS".into(), "250".into()));
        env
    }

    /// Env that also silences shutdown responses so graceful shutdown would
    /// stall. Force shutdown must bypass that protocol path and terminate the
    /// installed child through process control.
    fn env_stall_shutdown(&self) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push(("CAIRN_TEST_RESPOND_SHUTDOWN".into(), "0".into()));
        env
    }

    /// Env that responds to `shutdown` but ignores `exit`. Cleanup still uses
    /// the verified process group without polluting the result with a protocol
    /// error.
    fn env_shutdown_ok_but_no_exit(&self) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push(("CAIRN_TEST_IGNORE_EXIT".into(), "1".into()));
        env
    }

    /// Env that logs every method the fake receives to `path`
    /// (one line per method, prefixed by the fake's PID). Used by
    /// the `ServerExited` respawn test to observe that the
    /// second-spawn child receives `textDocument/didOpen` (not
    /// `didChange`) for the URI.
    fn env_with_methods_log(&self, path: &Path) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push(("CAIRN_TEST_METHODS_FILE".into(), path.display().to_string()));
        env
    }
}

#[cfg(unix)]
fn stable_python_interpreter() -> PathBuf {
    use std::os::unix::fs::PermissionsExt;

    static INTERPRETER: OnceLock<PathBuf> = OnceLock::new();
    INTERPRETER
        .get_or_init(|| {
            let search_path =
                std::env::var_os("PATH").expect("the fake LSP tests require python3 on PATH");
            std::env::split_paths(&search_path)
                .map(|directory| directory.join("python3"))
                .find(|candidate| {
                    std::fs::metadata(candidate)
                        .map(|metadata| {
                            metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                        })
                        .unwrap_or(false)
                })
                .and_then(|candidate| std::fs::canonicalize(candidate).ok())
                .expect("the fake LSP tests require an executable python3 on PATH")
        })
        .clone()
}

#[cfg(unix)]
fn spawn_spec(fake: &FakeLspBinary, request_timeout: Duration) -> LspSpawnSpec {
    LspSpawnSpec {
        binary: fake.interpreter.clone(),
        workspace_root: PathBuf::from("/tmp"),
        config_hash: "test".into(),
        request_timeout,
        availability: AvailabilityStrategy::PathExistsExecutable,
        readiness: ReadinessStrategy::InitializeResponseOnly,
        language_id: "python",
        launch_args: fake.launch_args(),
        env: fake.env(),
        initialization_options: serde_json::json!({}),
    }
}

#[cfg(unix)]
fn spawn_spec_stall(fake: &FakeLspBinary, request_timeout: Duration) -> LspSpawnSpec {
    LspSpawnSpec {
        env: fake.env_stall_shutdown(),
        ..spawn_spec(fake, request_timeout)
    }
}

fn unstarted_client_for_cleanup() -> LspClient {
    LspClient::configured(
        Path::new("test-only-lsp"),
        Vec::new(),
        Vec::new(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(1),
    )
}

#[cfg(unix)]
#[test]
fn pooled_spawns_receive_unique_markers_while_standalone_spawn_remains_unmarked() {
    use crate::lsp::process_sweep::ProcessOwnerContext;

    let fake = FakeLspBinary::new();
    let marker_file = fake.pid_file.with_file_name("markers.log");
    let owner = ProcessOwnerContext::for_test("owner-exact", [4; 16]);
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let pooled = LspClient::configured_for_pool_with_owner(
            fake.binary(),
            fake.launch_args(),
            fake.env_with_marker_log(&marker_file),
            Path::new("/tmp"),
            serde_json::json!({}),
            Duration::from_secs(10),
            owner,
        );
        pooled.start_process().await.unwrap();
        pooled.force_terminate().await.unwrap();
        pooled.start_process().await.unwrap();
        pooled.force_terminate().await.unwrap();

        let standalone = LspClient::configured(
            fake.binary(),
            fake.launch_args(),
            fake.env_with_marker_log(&marker_file),
            Path::new("/tmp"),
            serde_json::json!({}),
            Duration::from_secs(10),
        );
        standalone.start_process().await.unwrap();
        standalone.force_terminate().await.unwrap();
    });

    let lines: Vec<_> = std::fs::read_to_string(&marker_file)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].starts_with("owner-exact|cairn-family-v1:"));
    assert!(lines[1].starts_with("owner-exact|cairn-family-v1:"));
    assert_ne!(lines[0], lines[1], "each spawn must own a fresh family");
    assert_eq!(lines[2], "<unset>|<unset>");
}

#[cfg(unix)]
#[test]
fn pooled_spawn_without_required_owner_context_starts_no_process() {
    let fake = FakeLspBinary::new();
    let client = LspClient::configured_for_pool_without_owner_for_test(
        fake.binary(),
        fake.launch_args(),
        fake.env(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(3),
    );
    let error = Runtime::new()
        .unwrap()
        .block_on(client.start_process())
        .unwrap_err();
    assert!(matches!(error, Error::Protocol(_)));
    assert!(
        !fake.pid_file.exists(),
        "required owner context failure spawned a child"
    );
}

#[cfg(unix)]
#[test]
fn runtime_family_inspection_residual_is_diagnostic_and_allows_successor_admission() {
    use crate::lsp::process_sweep::ProcessOwnerContext;

    let fake = FakeLspBinary::new();
    let owner = ProcessOwnerContext::for_test_with_residual_sweep("owner", [8; 16]);
    let client = LspClient::configured_for_pool_with_owner(
        fake.binary(),
        fake.launch_args(),
        fake.env(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(3),
        owner,
    );
    Runtime::new().unwrap().block_on(async {
        client.start_process().await.unwrap();
        client.force_terminate().await.unwrap();
        let successor = LspClient::configured_for_pool_with_owner(
            fake.binary(),
            fake.launch_args(),
            fake.env(),
            Path::new("/tmp"),
            serde_json::json!({}),
            Duration::from_secs(3),
            ProcessOwnerContext::for_test_with_residual_sweep("owner", [8; 16]),
        );
        successor.start_process().await.unwrap();
        successor.force_terminate().await.unwrap();
    });

    let pool = pool(1);
    assert_eq!(pool.mode(), PoolMode::Running);
    drop(acquire(&pool, fake_pool_key(&fake, 90)).unwrap());
}

#[cfg(unix)]
#[test]
fn runtime_family_kill_failure_and_persistent_identity_allow_successor_admission() {
    use crate::lsp::process_sweep::ProcessOwnerContext;

    let owners = [
        ProcessOwnerContext::for_test_with_kill_failure_sweep("kill-owner", [9; 16]),
        ProcessOwnerContext::for_test_with_persistent_sweep("persistent-owner", [10; 16]),
    ];
    for (ordinal, owner) in owners.into_iter().enumerate() {
        let fake = FakeLspBinary::new();
        let client = LspClient::configured_for_pool_with_owner(
            fake.binary(),
            fake.launch_args(),
            fake.env(),
            Path::new("/tmp"),
            serde_json::json!({}),
            Duration::from_secs(3),
            owner,
        );
        Runtime::new().unwrap().block_on(async {
            client.start_process().await.unwrap();
            client.force_terminate().await.unwrap();
        });

        let pool = pool(1);
        assert_eq!(pool.mode(), PoolMode::Running);
        drop(
            acquire(
                &pool,
                fake_pool_key(&fake, u32::try_from(91 + ordinal).unwrap()),
            )
            .unwrap(),
        );
    }
}

#[cfg(unix)]
#[test]
fn group_and_wait_failures_remain_termination_unproven_and_compose_losslessly() {
    let group_only =
        finish_owned_child_termination(Some("group failure".into()), None).unwrap_err();
    assert!(group_only.is_termination_unproven());
    assert!(group_only.to_string().contains("group failure"));

    let wait_only =
        finish_owned_child_termination(None, Some(io::Error::other("wait failure"))).unwrap_err();
    assert!(wait_only.is_termination_unproven());
    assert!(
        wait_only
            .to_string()
            .contains("leader wait after kill: wait failure")
    );

    let error = finish_owned_child_termination(
        Some("group failure".into()),
        Some(io::Error::other("wait failure")),
    )
    .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("group failure"));
    assert!(text.contains("leader wait after kill: wait failure"));
    assert!(error.is_termination_unproven());
}

#[cfg(unix)]
#[test]
fn runtime_family_sweep_runs_off_async_worker_after_leader_wait_and_before_cleanup_terminal() {
    use crate::lsp::process_sweep::ProcessOwnerContext;

    let fake = FakeLspBinary::new();
    let (owner, control) =
        ProcessOwnerContext::for_test_with_blocking_sweep("blocking-owner", [11; 16]);
    let client = LspClient::configured_for_pool_with_owner(
        fake.binary(),
        fake.launch_args(),
        fake.env(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(3),
        owner,
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        client.start_process().await.unwrap();
        let process_control = client.process_control();
        let cleanup = client.force_terminate();
        tokio::pin!(cleanup);
        let release = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                release = control.started() => release,
                result = &mut cleanup => panic!("cleanup completed before sweep start: {result:?}"),
            }
        })
        .await
        .expect("blocking family sweep did not start");

        assert_eq!(process_control.leader_wait_count_for_test(), 1);
        let heartbeat = Arc::new(AtomicUsize::new(0));
        let heartbeat_task = Arc::clone(&heartbeat);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            heartbeat_task.store(1, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(heartbeat.load(Ordering::SeqCst), 1);
        tokio::select! {
            result = &mut cleanup => panic!("cleanup completed before sweep terminal: {result:?}"),
            () = tokio::task::yield_now() => {}
        }

        release.send(()).expect("release blocking family sweep");
        tokio::time::timeout(Duration::from_secs(3), &mut cleanup)
            .await
            .expect("cleanup did not terminate after sweep release")
            .unwrap();
    });
}

#[cfg(unix)]
#[test]
fn blocking_sweep_join_failures_are_not_silent_and_preserve_termination_facts() {
    use crate::lsp::process_sweep::ProcessOwnerContext;

    let fake = FakeLspBinary::new();
    let client = LspClient::configured_for_pool_with_owner(
        fake.binary(),
        fake.launch_args(),
        fake.env(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(3),
        ProcessOwnerContext::for_test_with_panicking_sweep("panic-owner", [12; 16]),
    );
    Runtime::new().unwrap().block_on(async {
        client.start_process().await.unwrap();
        let error = client.force_terminate().await.unwrap_err();
        assert!(matches!(error, Error::Protocol(_)));
        assert!(!error.is_termination_unproven());

        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        let join_error = cancelled.await.unwrap_err();
        let error = map_family_sweep_join(Err(join_error)).unwrap_err();
        assert!(matches!(error, Error::Protocol(_)));
    });

    let mixed = finish_owned_child_cleanup(
        Some("group failure".into()),
        Some(io::Error::other("wait failure")),
        Some(Error::Protocol("sweep task failure".into())),
    )
    .unwrap_err();
    let text = mixed.to_string();
    assert!(text.contains("sweep task failure"));
    assert!(text.contains("group failure"));
    assert!(text.contains("leader wait after kill: wait failure"));
    assert!(mixed.is_termination_unproven());
}

#[cfg(unix)]
fn fake_pool_key(fake: &FakeLspBinary, n: u32) -> PoolKey {
    PoolKey {
        canonical_repo_root: PathBuf::from(format!("/tmp/pool-key-{n}")),
        language: "python".into(),
        analyzer_id: format!("fake-{n}"),
        binary: fake.interpreter.clone(),
        config_hash: "test".into(),
    }
}

#[cfg(unix)]
#[test]
fn fake_initialize_admission_is_bounded_without_serializing_post_init_work() {
    let fake = FakeLspBinary::new();
    let initialize_state = fake.pid_file.with_file_name("initialize-state");
    let pool = Arc::new(pool(8));
    let start = Arc::new(std::sync::Barrier::new(8));
    let work = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();

    for ordinal in 0..8_u32 {
        let pool = Arc::clone(&pool);
        let start = Arc::clone(&start);
        let work = Arc::clone(&work);
        let key = fake_pool_key(&fake, 300 + ordinal);
        let spec = LspSpawnSpec {
            env: fake.env_with_initialize_probe(&initialize_state),
            ..spawn_spec(&fake, Duration::from_secs(3))
        };
        handles.push(std::thread::spawn(move || {
            start.wait();
            pool.with_lsp(key, spec, move |_lsp| {
                let work = Arc::clone(&work);
                Box::pin(async move {
                    timeout(Duration::from_secs(3), work.wait())
                        .await
                        .expect("post-initialize work was serialized behind admission");
                    Ok::<(), Error>(())
                })
            })
        }));
    }

    for handle in handles {
        handle.join().unwrap().unwrap();
    }
    let state = std::fs::read_to_string(&initialize_state).unwrap();
    let (current, maximum) = state.trim().split_once(',').unwrap();
    assert_eq!(current.parse::<usize>().unwrap(), 0);
    let maximum = maximum.parse::<usize>().unwrap();
    assert!(
        (1..=4).contains(&maximum),
        "fake initialize concurrency escaped its four-permit admission: {maximum}"
    );
    pool.force_shutdown_all(Duration::from_secs(3)).unwrap();
}

#[cfg(unix)]
#[test]
fn force_shutdown_all_with_active_entry_returns_running() {
    let fake = FakeLspBinary::new();
    let pool = pool(2);
    pool.with_lsp(
        fake_pool_key(&fake, 1),
        spawn_spec(&fake, Duration::from_secs(2)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    assert_eq!(pool.len(), 1);
    // Shutdown responds → force is clean → mode back to Running.
    pool.force_shutdown_all(Duration::from_secs(3)).unwrap();
    assert_eq!(pool.mode(), PoolMode::Running);
    assert_eq!(pool.len(), 0);
}

#[test]
fn force_timeout_stays_draining_until_published_owner_observes_terminal_cleanup() {
    let pool = Arc::new(pool(1));
    let key = test_key(122);
    drop(acquire(&pool, key.clone()).unwrap());
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let client = unstarted_client_for_cleanup();
    let (mut entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);

    let force_pool = Arc::clone(&pool);
    let force =
        std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_millis(10)));
    pool.runtime().block_on(async {
        timeout(Duration::from_secs(1), entered.changed())
            .await
            .expect("cleanup task did not start")
            .expect("cleanup entry signal disconnected");
    });
    let error = force
        .join()
        .unwrap()
        .expect_err("observation must time out");
    assert!(error.is_termination_unproven());
    assert_eq!(
        pool.mode(),
        PoolMode::Draining,
        "entry-local timeout/completion cannot reopen global admission"
    );

    release.send_replace(true);
    assert!(
        wait_for_mode(&pool, PoolMode::Running, Duration::from_secs(1)),
        "published drain owner must reopen admission after terminal cleanup"
    );
}

#[cfg(unix)]
#[test]
fn bounded_final_shutdown_reaps_child_while_pass_holds_state_mutex() {
    let fake = FakeLspBinary::new();
    let methods_log = fake.pid_file.with_file_name("bounded-shutdown-methods.log");
    let pool = Arc::new(pool(1));
    let key = fake_pool_key(&fake, 1);
    let spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(10))
    };
    let worker_pool = Arc::clone(&pool);
    let uri = Url::from("file:///tmp/pool-test/bounded-shutdown.py");
    let worker = std::thread::spawn(move || {
        worker_pool.with_lsp(key, spec, move |pooled| {
            Box::pin(async move {
                pooled.sync_document(&uri, "print('waiting')").await?;
                pooled
                    .definition(
                        &uri,
                        Position {
                            line: 0,
                            character: 0,
                        },
                    )
                    .await?;
                Ok::<(), Error>(())
            })
        })
    });

    assert!(
        wait_for_logged_method(
            &methods_log,
            "textDocument/definition",
            Duration::from_secs(5),
        ),
        "fake LSP must receive the pending definition request before shutdown"
    );
    let pid = read_pid(
        &fake.pid_file,
        std::time::Instant::now() + Duration::from_secs(2),
    )
    .expect("fake LSP must publish its PID");

    let started = std::time::Instant::now();
    pool.shutdown_all_bounded(Duration::from_secs(2))
        .expect("bounded shutdown must prove child termination");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "bounded shutdown exceeded its process-control budget"
    );
    assert_eq!(pool.mode(), PoolMode::Stopped);
    assert!(
        wait_until_dead(pid, Duration::from_secs(2)),
        "bounded shutdown must reap child pid {pid}"
    );

    let worker_result = worker
        .join()
        .expect("pass thread must unwind after child exit");
    assert!(
        matches!(
            worker_result,
            Err(Error::ServerExited(_)) | Err(Error::ServerExitedWithStderr { .. })
        ),
        "pending request must be released by child cleanup, got {worker_result:?}"
    );
}

#[cfg(unix)]
#[test]
fn force_shutdown_all_reaps_stalled_owner_without_poisoning_pool() {
    let fake = FakeLspBinary::new();
    let methods_log = fake
        .pid_file
        .with_file_name("force-stalled-owner-methods.log");
    let pool = Arc::new(pool(1));
    let key = fake_pool_key(&fake, 1);
    let spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(10))
    };
    let worker_pool = Arc::clone(&pool);
    let worker_key = key.clone();
    let uri = Url::from("file:///tmp/pool-test/force-stalled-owner.py");
    let (work_unblocked_tx, work_unblocked_rx) = std::sync::mpsc::channel();
    let (release_work_tx, release_work_rx) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        worker_pool.with_lsp(worker_key, spec, move |pooled| {
            Box::pin(async move {
                pooled.sync_document(&uri, "print('waiting')").await?;
                let definition = pooled
                    .definition(
                        &uri,
                        Position {
                            line: 0,
                            character: 0,
                        },
                    )
                    .await;
                work_unblocked_tx
                    .send(())
                    .expect("test owner must publish that child cleanup unblocked it");
                release_work_rx
                    .await
                    .expect("test must release the old owner");
                definition?;
                Ok::<(), Error>(())
            })
        })
    });

    assert!(wait_for_logged_method(
        &methods_log,
        "textDocument/definition",
        Duration::from_secs(5),
    ));
    let pid = read_pid(
        &fake.pid_file,
        std::time::Instant::now() + Duration::from_secs(2),
    )
    .expect("fake LSP must publish its PID");
    let old_entry = pool.registry.lock().unwrap().entries[&key].entry.clone();

    let force_result = pool.force_shutdown_all(Duration::from_millis(100));
    if force_result.is_err() {
        // Keep the baseline failure leak-free: the current graceful path times
        // out while the owner holds `state`, so use the already-proven control
        // plane solely to let the failing carrier unwind.
        pool.runtime()
            .block_on(old_entry.shutdown_bounded(Duration::from_secs(2)))
            .expect("baseline cleanup must reap the stalled child");
    }
    work_unblocked_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("child cleanup must unblock the stalled owner");
    assert!(
        wait_until_dead(pid, Duration::from_secs(2)),
        "force cleanup must kill and reap child pid {pid}"
    );

    let replacement = force_result
        .as_ref()
        .ok()
        .map(|_| acquire(&pool, key.clone()).expect("proven cleanup must keep the pool reusable"));
    release_work_tx
        .send(())
        .expect("old owner must still be waiting for release");
    let worker_result = worker.join().expect("stalled owner thread must unwind");
    assert!(
        matches!(
            worker_result,
            Err(Error::ServerExited(_)) | Err(Error::ServerExitedWithStderr { .. })
        ),
        "stalled owner must observe the reaped child, got {worker_result:?}"
    );

    force_result.expect("proven bounded cleanup must not poison the global pool");
    assert_eq!(pool.mode(), PoolMode::Running);
    let replacement = replacement.expect("successful force cleanup must install a replacement");
    assert_eq!(
        pool.active_leases(&key),
        Some(1),
        "late release from the old owner must not clobber its replacement"
    );
    assert!(!Arc::ptr_eq(&old_entry, &replacement.entry));
}

#[cfg(unix)]
#[test]
fn bounded_shutdown_reaps_entry_published_by_concurrent_force_drain() {
    let fake = FakeLspBinary::new();
    let methods_log = fake
        .pid_file
        .with_file_name("overlapping-shutdown-methods.log");
    let pool = Arc::new(pool(1));
    let key = fake_pool_key(&fake, 1);
    let spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(10))
    };
    let worker_pool = Arc::clone(&pool);
    let worker_key = key.clone();
    let uri = Url::from("file:///tmp/pool-test/overlapping-shutdown.py");
    let worker = std::thread::spawn(move || {
        worker_pool.with_lsp(worker_key, spec, move |pooled| {
            Box::pin(async move {
                pooled.sync_document(&uri, "print('waiting')").await?;
                pooled
                    .definition(
                        &uri,
                        Position {
                            line: 0,
                            character: 0,
                        },
                    )
                    .await?;
                Ok::<(), Error>(())
            })
        })
    });

    assert!(wait_for_logged_method(
        &methods_log,
        "textDocument/definition",
        Duration::from_secs(5),
    ));
    let pid = read_pid(
        &fake.pid_file,
        std::time::Instant::now() + Duration::from_secs(2),
    )
    .expect("fake LSP must publish its PID");
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let process_gate = entry.process_control.lock().unwrap();

    let force_pool = Arc::clone(&pool);
    let force = std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_secs(5)));
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force shutdown must publish Draining before final shutdown"
    );
    {
        let registry = pool.registry.lock().unwrap();
        assert!(registry.entries.is_empty());
        assert_eq!(
            registry
                .draining_entries
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            1,
            "force drain must publish the entry before removing it from the live map"
        );
    }

    let final_pool = Arc::clone(&pool);
    let final_shutdown =
        std::thread::spawn(move || final_pool.shutdown_all_bounded(Duration::from_secs(2)));
    assert!(
        wait_for_mode(&pool, PoolMode::Stopped, Duration::from_secs(2)),
        "final bounded shutdown must publish Stopped before entry cleanup"
    );
    drop(process_gate);
    final_shutdown
        .join()
        .expect("final bounded shutdown thread panicked")
        .expect("bounded path must reap the published draining entry");
    assert!(wait_until_dead(pid, Duration::from_secs(2)));
    assert!(pool.registry.lock().unwrap().draining_entries.is_empty());

    let worker_result = worker.join().expect("pass thread must unwind");
    assert!(matches!(
        worker_result,
        Err(Error::ServerExited(_)) | Err(Error::ServerExitedWithStderr { .. })
    ));
    force
        .join()
        .expect("force-shutdown thread panicked")
        .expect("overlapping bounded cleanup after reap must be idempotent");
    assert_eq!(
        pool.mode(),
        PoolMode::Stopped,
        "force finalize must not overwrite final bounded shutdown state"
    );
}

#[cfg(unix)]
#[test]
fn bounded_shutdown_reaps_entry_published_by_concurrent_graceful_drain() {
    let fake = FakeLspBinary::new();
    let methods_log = fake
        .pid_file
        .with_file_name("graceful-takeover-methods.log");
    let pool = Arc::new(pool(1));
    let key = fake_pool_key(&fake, 1);
    let spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(10))
    };
    let worker_pool = Arc::clone(&pool);
    let uri = Url::from("file:///tmp/pool-test/graceful-takeover.py");
    let worker = std::thread::spawn(move || {
        worker_pool.with_lsp(key, spec, move |pooled| {
            Box::pin(async move {
                pooled.sync_document(&uri, "print('waiting')").await?;
                pooled
                    .definition(
                        &uri,
                        Position {
                            line: 0,
                            character: 0,
                        },
                    )
                    .await?;
                Ok::<(), Error>(())
            })
        })
    });

    assert!(wait_for_logged_method(
        &methods_log,
        "textDocument/definition",
        Duration::from_secs(5),
    ));
    let pid = read_pid(
        &fake.pid_file,
        std::time::Instant::now() + Duration::from_secs(2),
    )
    .expect("fake LSP must publish its PID");

    let graceful_pool = Arc::clone(&pool);
    let graceful = std::thread::spawn(move || graceful_pool.shutdown_all());
    assert!(
        wait_for_mode(&pool, PoolMode::Stopped, Duration::from_secs(2)),
        "graceful shutdown must publish Stopped before waiting on the owner"
    );
    {
        let registry = pool.registry.lock().unwrap();
        assert!(registry.entries.is_empty());
        assert_eq!(
            registry
                .draining_entries
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            1,
            "graceful shutdown must publish its batch while the owner holds state"
        );
    }

    pool.shutdown_all_bounded(Duration::from_secs(2))
        .expect("bounded shutdown must take over and prove child termination");
    assert!(
        wait_until_dead(pid, Duration::from_secs(2)),
        "bounded takeover must kill and reap child pid {pid}"
    );
    assert!(pool.registry.lock().unwrap().draining_entries.is_empty());

    let worker_result = worker.join().expect("pass thread must unwind");
    assert!(matches!(
        worker_result,
        Err(Error::ServerExited(_)) | Err(Error::ServerExitedWithStderr { .. })
    ));
    graceful
        .join()
        .expect("graceful-shutdown thread panicked")
        .expect("graceful finalize after bounded takeover must be idempotent");
    assert_eq!(pool.mode(), PoolMode::Stopped);
    assert!(pool.registry.lock().unwrap().draining_entries.is_empty());
}

#[test]
fn bounded_final_shutdown_rejects_late_process_control_install() {
    let entry = Arc::new(PoolEntry::default());
    let runtime = Runtime::new().unwrap();
    runtime
        .block_on(entry.shutdown_bounded(Duration::from_millis(10)))
        .unwrap();

    let client = LspClient::configured(
        Path::new("unused-lsp"),
        Vec::new(),
        Vec::new(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(1),
    );
    assert!(matches!(
        entry.install_and_arm(client.process_control()),
        Err(Error::PoolStopped)
    ));
}

#[test]
fn force_shutdown_without_control_rejects_late_process_control_install() {
    let pool = pool(1);
    let key = test_key(41);
    drop(acquire(&pool, key.clone()).unwrap());
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();

    pool.force_shutdown_all(Duration::from_millis(20))
        .expect("an entry without installed control is already termination-proven");
    assert_eq!(pool.mode(), PoolMode::Running);

    let client = LspClient::configured(
        Path::new("unused-lsp"),
        Vec::new(),
        Vec::new(),
        Path::new("/tmp"),
        serde_json::json!({}),
        Duration::from_secs(1),
    );
    let install = entry.install_and_arm(client.process_control());
    assert!(
        matches!(install, Err(Error::PoolStopped)),
        "late process-control install must be rejected"
    );
}

#[test]
fn force_shutdown_process_control_failure_poisons_and_rejects_replacement() {
    let pool = pool(1);
    let key = test_key(42);
    drop(acquire(&pool, key.clone()).unwrap());
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let poison_entry = Arc::clone(&entry);
    assert!(
        std::thread::spawn(move || {
            let _slot = poison_entry.process_control.lock().unwrap();
            panic!("poison process-control slot for the failure-policy carrier");
        })
        .join()
        .is_err()
    );

    let err = pool
        .force_shutdown_all(Duration::from_millis(20))
        .expect_err("unreadable process control leaves termination unproven");
    assert!(
        matches!(err, Error::PoolPoisoned),
        "unproven process-control failure must poison, got {err:?}"
    );
    assert_eq!(pool.mode(), PoolMode::Halted);
    assert!(matches!(acquire(&pool, key), Err(Error::PoolPoisoned)));
}

#[test]
fn stopped_force_shutdown_surfaces_unproven_cleanup_error() {
    let pool = Arc::new(pool(2));
    let gated_key = test_key(43);
    let poisoned_key = test_key(44);
    drop(acquire(&pool, gated_key.clone()).unwrap());
    drop(acquire(&pool, poisoned_key.clone()).unwrap());
    let (gated_entry, residual_entry) = {
        let registry = pool.registry.lock().unwrap();
        (
            registry.entries[&gated_key].entry.clone(),
            registry.entries[&poisoned_key].entry.clone(),
        )
    };
    // Purpose-specific carriers keep the axes independent: one cleanup pauses
    // after Draining is published, while the other reports a typed OS
    // residual. No mutex poison is involved in this Stopped-precedence test.
    let gated_client = unstarted_client_for_cleanup();
    let (_entered, release) = gated_client.pause_cleanup_for_test();
    let mut gated_guard = gated_entry
        .install_and_arm(gated_client.process_control())
        .unwrap();
    gated_guard.armed = false;
    drop(gated_guard);
    let residual_client = unstarted_client_for_cleanup();
    residual_client
        .process_control()
        .force_os_residual_for_test("synthetic force-shutdown OS residual");
    let mut residual_guard = residual_entry
        .install_and_arm(residual_client.process_control())
        .unwrap();
    residual_guard.armed = false;
    drop(residual_guard);

    let force_pool = Arc::clone(&pool);
    let force = std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_secs(2)));
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force shutdown must publish Draining before final shutdown"
    );
    pool.shutdown_all()
        .expect("the already-published force batch leaves no graceful work");
    assert_eq!(pool.mode(), PoolMode::Stopped);
    release.send_replace(true);

    let err = force
        .join()
        .expect("force-shutdown thread panicked")
        .expect_err("Stopped precedence must not discard cleanup evidence");
    assert!(
        err.is_termination_unproven(),
        "force result must retain termination-unproven evidence, got {err:?}"
    );
    assert_eq!(pool.mode(), PoolMode::Stopped);
    assert!(matches!(
        acquire(&pool, test_key(45)),
        Err(Error::PoolStopped)
    ));
}

#[test]
fn bounded_entry_shutdown_timeout_message_is_context_neutral() {
    let err = bounded_shutdown_timeout_error(Duration::from_millis(1));
    assert_eq!(
        err.to_string(),
        "LSP child termination could not be proven: bounded LSP entry shutdown exceeded 1ms"
    );
}

#[cfg(unix)]
#[test]
fn force_shutdown_all_bypasses_stalled_graceful_shutdown() {
    let fake = FakeLspBinary::new();
    let pool = pool(2);
    pool.with_lsp(
        fake_pool_key(&fake, 1),
        spawn_spec_stall(&fake, Duration::from_secs(10)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    let pid = read_pid(
        &fake.pid_file,
        std::time::Instant::now() + Duration::from_secs(2),
    )
    .expect("fake LSP must publish its PID");

    pool.force_shutdown_all(Duration::from_secs(2)).unwrap();
    assert_eq!(pool.mode(), PoolMode::Running);
    assert_eq!(pool.len(), 0);
    assert!(wait_until_dead(pid, Duration::from_secs(2)));
    drop(acquire(&pool, fake_pool_key(&fake, 2)).unwrap());
}

// Concurrency around `Draining` publication is exercised by
// `acquire_during_drain_returns_pool_draining_deterministic` and
// `stopped_race_after_force_reaches_draining_stays_stopped`
// below, which use bounded-poll `wait_for_mode` on the actual
// Draining publication rather than any timing guess.

#[cfg(unix)]
#[test]
fn client_readiness_timeout_terminates_child_and_surfaces_both_errors() {
    // Fake LSP replies to `initialize` but never sends
    // `$/progress`. `wait_for_workspace_load(short_timeout)`
    // times out; `force_terminate` reaps the child. We assert
    // (a) the readiness Err is a `ReadinessTimeout`; (b) the
    // child PID is dead after `force_terminate`; (c) the
    // returned error from `force_terminate` is `Ok(())` since
    // the child was reap-able.
    let fake = FakeLspBinary::new();
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let client = LspClient::start_configured(
            fake.binary(),
            fake.launch_args(),
            fake.env(),
            workspace.path(),
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await
        .expect("initialize must succeed against fake LSP");
        let pid = read_pid(
            &fake.pid_file,
            std::time::Instant::now() + Duration::from_secs(2),
        )
        .expect("fake LSP must have written its PID");
        assert!(pid_alive(pid), "child must be alive after successful init");
        // Readiness: `wait_for_workspace_load` with a small
        // timeout must Err because the fake never sends
        // `$/progress`.
        let readiness = client
            .wait_for_workspace_load(Duration::from_millis(150))
            .await;
        assert!(readiness.is_err(), "readiness must time out");
        client
            .force_terminate()
            .await
            .expect("force_terminate must reap the fake LSP cleanly");
        assert!(
            wait_until_dead(pid, Duration::from_secs(2)),
            "child pid {pid} must be dead after force_terminate"
        );
    });
}

#[cfg(unix)]
#[test]
fn client_force_terminate_on_live_client_kills_pid() {
    // Direct force_terminate on a successfully-initialized
    // client → PID dead.
    let fake = FakeLspBinary::new();
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let client = LspClient::start_configured(
            fake.binary(),
            fake.launch_args(),
            fake.env(),
            workspace.path(),
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        let pid = read_pid(
            &fake.pid_file,
            std::time::Instant::now() + Duration::from_secs(2),
        )
        .expect("PID must be written");
        client
            .force_terminate()
            .await
            .expect("force_terminate must succeed");
        assert!(wait_until_dead(pid, Duration::from_secs(2)));
    });
}

#[cfg(unix)]
#[test]
fn unix_process_group_identity_and_signal_classification_are_fail_closed() {
    let leader = Pid::from_raw(41_000).unwrap();
    let other_group = Pid::from_raw(41_001).unwrap();
    let mismatch = validate_process_group_identity(leader, Ok(other_group))
        .expect_err("a non-leader process group must be rejected");
    assert!(mismatch.contains("process-group mismatch"));

    assert!(classify_group_signal(leader, Err(rustix::io::Errno::SRCH), "kill").is_none());
    let denied = classify_group_signal(leader, Err(rustix::io::Errno::PERM), "kill")
        .expect("EPERM must remain a containment fact");
    assert!(
        denied.contains("Operation not permitted") || denied.contains("operation not permitted")
    );
}

#[cfg(unix)]
#[test]
fn process_group_mismatch_reaps_unpublished_leader_and_never_claims_containment() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let mut child = tokio::process::Command::new("sleep")
            .arg("300")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().expect("sleep must publish a pid");
        let leader = Pid::from_raw(pid as i32).unwrap();
        let mismatched = Pid::from_raw(pid.checked_add(1).unwrap() as i32).unwrap();
        let reason = validate_process_group_identity(leader, Ok(mismatched))
            .expect_err("mismatch must fail identity validation");
        let error = reject_unverified_child(&mut child, &reason).await;
        assert!(error.is_termination_unproven());
        assert!(
            wait_until_dead(pid, Duration::from_secs(2)),
            "unpublished mismatched leader {pid} was not reaped"
        );
    });
}

#[cfg(unix)]
#[test]
fn verified_process_group_kill_reaps_leader_and_terminates_grandchild() {
    let fake = FakeLspBinary::new();
    let grandchild_pid_file = fake.pid_file.with_file_name("fake-lsp-grandchild.pid");
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let mut env = fake.env_with_grandchild(&grandchild_pid_file);
        env.push(("CAIRN_TEST_IGNORE_EXIT".into(), "1".into()));
        env.push(("CAIRN_LSP_OWNER".into(), "group-owner".into()));
        env.push(("CAIRN_LSP_FAMILY".into(), "group-family".into()));
        env.push(("CAIRN_TEST_SCRUB_GRANDCHILD_MARKERS".into(), "1".into()));
        let client = LspClient::start_configured(
            fake.binary(),
            fake.launch_args(),
            env,
            workspace.path(),
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        let leader = read_pid(
            &fake.pid_file,
            std::time::Instant::now() + Duration::from_secs(2),
        )
        .expect("leader pid must be written");
        let grandchild = read_pid(
            &grandchild_pid_file,
            std::time::Instant::now() + Duration::from_secs(2),
        )
        .expect("grandchild pid must be written");

        let control = client.process_control();
        let shutdown = timeout(Duration::from_secs(2), client.shutdown()).await;
        if shutdown.is_err() {
            // Mutation cleanup: a signal-after-wait or removed-signal variant
            // stalls while the still-owned leader keeps this PGID safe.
            let _ = rustix::process::kill_process_group(
                Pid::from_raw(leader as i32).unwrap(),
                rustix::process::Signal::KILL,
            );
        }
        shutdown
            .expect("graceful cleanup must signal before waiting")
            .expect("verified group kill and leader reap must succeed");
        assert_eq!(
            control.leader_wait_count_for_test(),
            1,
            "verified group termination must reap the leader exactly once"
        );
        assert!(
            !pid_alive(leader),
            "shutdown returned before reaping the signalled leader"
        );
        assert!(
            wait_until_dead(grandchild, Duration::from_secs(3)),
            "owned process-group descendant {grandchild} remained live"
        );
    });
}

#[test]
fn cleanup_timeout_keeps_one_task_running_and_vetoes_replacement_until_proven() {
    let runtime = Runtime::new().unwrap();
    let entry = Arc::new(PoolEntry::default());
    let client = unstarted_client_for_cleanup();
    let (mut entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);

    runtime.block_on(async {
        let first = entry.request_cleanup(false, None).unwrap().unwrap();
        timeout(Duration::from_secs(1), entered.changed())
            .await
            .expect("cleanup task did not start")
            .expect("cleanup entry signal disconnected");
        assert_eq!(client.cleanup_run_count_for_test(), 1);

        let second = entry.request_cleanup(false, None).unwrap().unwrap();
        assert_eq!(
            client.cleanup_run_count_for_test(),
            1,
            "cleanup must coalesce"
        );
        assert!(matches!(
            entry.install_and_arm(unstarted_client_for_cleanup().process_control()),
            Err(Error::PoolDraining)
        ));
        assert!(matches!(
            observe_cleanup_bounded(first, Duration::from_millis(1)).await,
            Err(Error::ChildTerminationFailed(_))
        ));

        release.send_replace(true);
        observe_cleanup(second)
            .await
            .expect("late cleanup must become proven");
        assert_eq!(client.cleanup_run_count_for_test(), 1);

        let mut replacement = entry
            .install_and_arm(unstarted_client_for_cleanup().process_control())
            .expect("late proof must release the epoch for replacement");
        replacement.armed = false;
    });
}

#[test]
fn capacity_lru_joins_pending_uncommitted_cleanup_before_opening_slot() {
    let pool = Arc::new(pool(1));
    let victim_key = test_key(123);
    let replacement_key = test_key(124);
    let client = unstarted_client_for_cleanup();
    let (mut entered, release) = client.pause_cleanup_for_test();

    pool.runtime().block_on(async {
        let lease = pool.acquire_lease(victim_key.clone()).await.unwrap();
        let guard = lease
            .entry
            .install_and_arm(client.process_control())
            .unwrap();
        // The uncommitted guard owns the only cleanup request; `state.client`
        // remains None throughout this carrier.
        drop(guard);
        drop(lease);
        timeout(Duration::from_secs(1), entered.changed())
            .await
            .expect("uncommitted cleanup did not start")
            .expect("cleanup entry signal disconnected");

        let acquiring_pool = Arc::clone(&pool);
        let acquire =
            tokio::spawn(async move { acquiring_pool.acquire_lease(replacement_key).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!acquire.is_finished(), "LRU treated Pending as Proven");
        {
            let registry = pool.registry.lock().unwrap();
            let victim = registry.entries.get(&victim_key).unwrap();
            assert_eq!(victim.state, RecordState::Evicting);
            assert_eq!(registry.entries.len(), 1, "replacement opened early");
        }

        release.send_replace(true);
        let replacement = acquire
            .await
            .expect("replacement acquire task panicked")
            .expect("replacement must acquire after exact terminal cleanup");
        assert_eq!(client.cleanup_run_count_for_test(), 1);
        drop(replacement);
    });
}

#[test]
fn cleanup_id_overflow_still_runs_owned_cleanup_and_halts_on_tracking_invariant() {
    let pool = pool(1);
    let key = test_key(120);
    drop(acquire(&pool, key.clone()).unwrap());
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let client = unstarted_client_for_cleanup();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    pool.registry.lock().unwrap().next_cleanup_id = u64::MAX;

    let error = pool.runtime().block_on(async {
        let receipt = entry.request_cleanup(false, None).unwrap().unwrap();
        observe_cleanup(receipt)
            .await
            .expect_err("tracking overflow is an invariant failure")
    });

    assert!(matches!(error, Error::PoolPoisoned));
    assert_eq!(client.cleanup_run_count_for_test(), 1);
    assert_eq!(pool.mode(), PoolMode::Halted);
}

#[test]
fn late_cleanup_completion_does_not_remove_same_key_successor() {
    let pool = pool(1);
    let key = test_key(121);
    drop(acquire(&pool, key.clone()).unwrap());
    let victim = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let client = unstarted_client_for_cleanup();
    let (mut entered, release) = client.pause_cleanup_for_test();
    let mut guard = victim.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    pool.registry
        .lock()
        .unwrap()
        .entries
        .get_mut(&key)
        .unwrap()
        .state = RecordState::Evicting;
    let receipt = pool.runtime().block_on(async {
        let receipt = victim.request_cleanup(false, None).unwrap().unwrap();
        timeout(Duration::from_secs(1), entered.changed())
            .await
            .expect("cleanup task did not start")
            .expect("cleanup entry signal disconnected");
        receipt
    });

    let successor = PoolEntry::new(key.clone(), &pool.registry);
    pool.registry.lock().unwrap().entries.insert(
        key.clone(),
        PoolRecord {
            entry: Arc::clone(&successor),
            active_leases: 0,
            last_used: 1,
            last_used_at: Instant::now(),
            state: RecordState::Ready,
        },
    );
    release.send_replace(true);
    pool.runtime().block_on(observe_cleanup(receipt)).unwrap();

    let registry = pool.registry.lock().unwrap();
    let current = registry.entries.get(&key).expect("successor must remain");
    assert!(Arc::ptr_eq(&current.entry, &successor));
    assert_eq!(current.state, RecordState::Ready);
}

#[test]
fn uncommitted_exit_guard_cleans_up_on_panic_and_future_drop() {
    let runtime = Runtime::new().unwrap();
    let entry = Arc::new(PoolEntry::default());

    runtime.block_on(async {
        let panic_client = unstarted_client_for_cleanup();
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            let _guard = entry
                .install_and_arm(panic_client.process_control())
                .unwrap();
            panic!("test-only uncommitted panic");
        }));
        assert!(panicked.is_err());
        let panic_receipt = entry
            .process_control
            .lock()
            .unwrap()
            .cleanup
            .as_ref()
            .unwrap()
            .receipt
            .clone();
        observe_cleanup(panic_receipt).await.unwrap();
        assert_eq!(panic_client.cleanup_run_count_for_test(), 1);

        let cancelled_client = unstarted_client_for_cleanup();
        let cancelled_entry = Arc::clone(&entry);
        let control = cancelled_client.process_control();
        let (armed_sender, armed_receiver) = tokio::sync::oneshot::channel();
        let pending = tokio::spawn(async move {
            let _guard = cancelled_entry.install_and_arm(control).unwrap();
            let _ = armed_sender.send(());
            std::future::pending::<()>().await;
        });
        armed_receiver
            .await
            .expect("guard must be armed before abort");
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());

        let cancelled_receipt = entry
            .process_control
            .lock()
            .unwrap()
            .cleanup
            .as_ref()
            .unwrap()
            .receipt
            .clone();
        observe_cleanup(cancelled_receipt).await.unwrap();
        assert_eq!(cancelled_client.cleanup_run_count_for_test(), 1);

        let error_client = unstarted_client_for_cleanup();
        let error_guard = entry
            .install_and_arm(error_client.process_control())
            .unwrap();
        let error = error_guard
            .finish_error(Error::Handshake("test-only start failure".into()))
            .await;
        assert!(matches!(error, Error::Handshake(_)));
        assert_eq!(error_client.cleanup_run_count_for_test(), 1);
    });
}

#[test]
fn pool_drop_waits_for_outstanding_cleanup_before_runtime_teardown() {
    let pool = pool(1);
    let key = PoolKey {
        canonical_repo_root: PathBuf::from("/tmp/drop-cleanup"),
        language: "rust".into(),
        analyzer_id: "drop-cleanup".into(),
        binary: PathBuf::from("test-only-lsp"),
        config_hash: "drop-cleanup".into(),
    };
    let lease = pool.runtime().block_on(pool.acquire_lease(key)).unwrap();
    let entry = Arc::clone(&lease.entry);
    let client = unstarted_client_for_cleanup();
    let (entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    drop(lease);

    let releaser = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !*entered.borrow() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(*entered.borrow(), "pool drop did not start cleanup");
        release.send_replace(true);
    });
    drop(pool);
    releaser.join().unwrap();
    assert_eq!(client.cleanup_run_count_for_test(), 1);
}

#[test]
fn pool_drop_collects_untracked_overflow_receipt_before_runtime_teardown() {
    let pool = pool(1);
    let key = test_key(125);
    let lease = pool.runtime().block_on(pool.acquire_lease(key)).unwrap();
    let entry = Arc::clone(&lease.entry);
    let client = unstarted_client_for_cleanup();
    let (entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    drop(lease);
    pool.registry.lock().unwrap().next_cleanup_id = u64::MAX;

    let releaser = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !*entered.borrow() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(*entered.borrow(), "drop did not start overflow cleanup");
        release.send_replace(true);
    });
    drop(pool);
    releaser.join().unwrap();
    assert_eq!(client.cleanup_run_count_for_test(), 1);
}

#[test]
fn pool_drop_recovers_poisoned_registry_and_observes_local_receipt() {
    let pool = pool(1);
    let key = test_key(126);
    let lease = pool.runtime().block_on(pool.acquire_lease(key)).unwrap();
    let entry = Arc::clone(&lease.entry);
    let client = unstarted_client_for_cleanup();
    let (entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    drop(lease);

    let registry = Arc::clone(&pool.registry);
    let _ = std::thread::spawn(move || {
        let _guard = registry.lock().unwrap();
        panic!("poison registry for drop carrier");
    })
    .join();
    assert!(pool.registry.is_poisoned());

    let releaser = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !*entered.borrow() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(*entered.borrow(), "drop did not recover poisoned custody");
        release.send_replace(true);
    });
    drop(pool);
    releaser.join().unwrap();
    assert_eq!(client.cleanup_run_count_for_test(), 1);
}

#[test]
fn cleanup_receipt_preserves_mixed_protocol_os_and_late_invariant_shape() {
    let process = ProcessCleanupCompletion {
        disposition: ProcessCleanupDisposition::InvariantFailure,
        error: Some(Error::OperationWithCleanupFailure {
            original: Box::new(Error::Protocol("family sweep join failed".into())),
            cleanup: Box::new(Error::ChildTerminationFailed(
                "group signal failed; leader wait failed".into(),
            )),
        }),
        os_residual: Some("group signal failed; leader wait failed".into()),
        invariant: Some("family sweep join failed".into()),
    };
    let outcome = add_cleanup_invariant(
        CleanupOutcome::from_process(process),
        "receipt registry ownership failed",
    );
    let (_sender, receipt) = watch::channel(outcome);
    let error = Runtime::new()
        .unwrap()
        .block_on(observe_cleanup(receipt))
        .unwrap_err();
    assert!(error.is_termination_unproven());
    let Error::OperationWithCleanupFailure { original, cleanup } = error else {
        panic!("late invariant must compose without flattening prior error");
    };
    assert!(matches!(*cleanup, Error::ChildTerminationFailed(_)));
    let Error::OperationWithCleanupFailure {
        original: protocol,
        cleanup: invariant,
    } = *original
    else {
        panic!("mixed process axes were not preserved");
    };
    assert!(matches!(*protocol, Error::Protocol(_)));
    assert!(matches!(*invariant, Error::PoolPoisoned));
}

#[test]
fn cleanup_receipt_invariant_without_termination_is_not_unproven() {
    let outcome = add_cleanup_invariant(CleanupOutcome::proven(), "late ownership failure");
    assert!(matches!(
        outcome,
        CleanupOutcome::Terminal(CleanupTerminal::InvariantFailure { .. }, _)
    ));
    let error = outcome.into_result().unwrap_err();
    assert!(matches!(error, Error::PoolPoisoned));
    assert!(!error.is_termination_unproven());
}

#[test]
fn cleanup_receipt_graceful_protocol_and_invariant_preserve_both_facts() {
    let outcome = add_cleanup_invariant(
        CleanupOutcome::from_process(ProcessCleanupCompletion {
            disposition: ProcessCleanupDisposition::Proven,
            error: Some(Error::Protocol("shutdown response was malformed".into())),
            os_residual: None,
            invariant: None,
        }),
        "receipt registry ownership failed",
    );
    let error = outcome.into_result().unwrap_err();
    assert!(!error.is_termination_unproven());
    let Error::OperationWithCleanupFailure { original, cleanup } = error else {
        panic!("graceful protocol and invariant facts must remain distinct");
    };
    assert!(matches!(*original, Error::Protocol(_)));
    assert!(matches!(*cleanup, Error::PoolPoisoned));
}

#[test]
fn cleanup_receipt_proven_disposition_preserves_graceful_protocol_error() {
    let outcome = CleanupOutcome::from_process(ProcessCleanupCompletion {
        disposition: ProcessCleanupDisposition::Proven,
        error: Some(Error::Protocol("shutdown response was malformed".into())),
        os_residual: None,
        invariant: None,
    });
    assert!(matches!(
        outcome,
        CleanupOutcome::Terminal(CleanupTerminal::Proven, Some(Error::Protocol(_)))
    ));
    let (_sender, receipt) = watch::channel(outcome);
    let error = Runtime::new()
        .unwrap()
        .block_on(observe_cleanup(receipt))
        .unwrap_err();
    assert!(matches!(error, Error::Protocol(_)));
}

#[test]
fn bounded_final_shutdown_observes_cleanup_no_longer_in_live_registry() {
    let pool = pool(1);
    let key = PoolKey {
        canonical_repo_root: PathBuf::from("/tmp/final-outstanding-cleanup"),
        language: "rust".into(),
        analyzer_id: "final-outstanding-cleanup".into(),
        binary: PathBuf::from("test-only-lsp"),
        config_hash: "final-outstanding-cleanup".into(),
    };
    let entry = PoolEntry::new(key, &pool.registry);
    let client = unstarted_client_for_cleanup();
    let (mut entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;

    let receipt = pool.runtime().block_on(async {
        let receipt = entry.request_cleanup(false, None).unwrap().unwrap();
        timeout(Duration::from_secs(1), entered.changed())
            .await
            .expect("cleanup task did not start")
            .expect("cleanup entry signal disconnected");
        receipt
    });

    assert!(matches!(
        pool.shutdown_all_bounded(Duration::from_millis(1)),
        Err(Error::ChildTerminationFailed(_))
    ));
    release.send_replace(true);
    pool.runtime()
        .block_on(observe_cleanup(receipt))
        .expect("outstanding cleanup must still finish after final observation timeout");
    assert_eq!(client.cleanup_run_count_for_test(), 1);
}

#[test]
fn final_shutdown_single_deadline_includes_inflight_cleanup_admission() {
    let pool = Arc::new(pool(1));
    let key = PoolKey {
        canonical_repo_root: PathBuf::from("/tmp/final-admission-race"),
        language: "rust".into(),
        analyzer_id: "final-admission-race".into(),
        binary: PathBuf::from("test-only-lsp"),
        config_hash: "final-admission-race".into(),
    };
    let entry = PoolEntry::new(key, &pool.registry);
    let client = unstarted_client_for_cleanup();
    let (mut entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;

    let slot = entry.process_control.lock().unwrap();
    let request_entry = Arc::clone(&entry);
    let runtime_handle = pool.runtime().handle().clone();
    let request = std::thread::spawn(move || {
        let _runtime_context = runtime_handle.enter();
        request_entry.request_cleanup(false, None)
    });
    let admission_deadline = std::time::Instant::now() + Duration::from_secs(1);
    while pool.registry.lock().unwrap().active_cleanup_admissions == 0
        && std::time::Instant::now() < admission_deadline
    {
        std::thread::yield_now();
    }
    assert_eq!(
        pool.registry.lock().unwrap().active_cleanup_admissions,
        1,
        "cleanup admission must linearize before final shutdown"
    );

    let bound = Duration::from_millis(500);
    let shutdown_pool = Arc::clone(&pool);
    let shutdown = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        let result = shutdown_pool.shutdown_all_bounded(bound);
        (result, started.elapsed())
    });
    let barrier_deadline = std::time::Instant::now() + Duration::from_secs(1);
    while pool.registry.lock().unwrap().mode != PoolMode::Stopped
        && std::time::Instant::now() < barrier_deadline
    {
        std::thread::yield_now();
    }
    assert_eq!(pool.registry.lock().unwrap().mode, PoolMode::Stopped);

    let late_key = PoolKey {
        canonical_repo_root: PathBuf::from("/tmp/final-admission-after-barrier"),
        language: "rust".into(),
        analyzer_id: "final-admission-after-barrier".into(),
        binary: PathBuf::from("test-only-lsp"),
        config_hash: "final-admission-after-barrier".into(),
    };
    let late_entry = PoolEntry::new(late_key, &pool.registry);
    let late_client = unstarted_client_for_cleanup();
    let (mut late_entered, late_release) = late_client.pause_cleanup_for_test();
    let mut late_guard = late_entry
        .install_and_arm(late_client.process_control())
        .unwrap();
    late_guard.armed = false;
    let late_receipt = {
        let runtime_handle = pool.runtime().handle().clone();
        let _runtime_context = runtime_handle.enter();
        late_entry
            .request_cleanup(false, None)
            .unwrap()
            .expect("post-barrier cleanup admission skipped its epoch")
    };

    drop(slot);
    let receipt = request
        .join()
        .expect("cleanup admission thread panicked")
        .expect("cleanup admission failed")
        .expect("cleanup admission skipped its epoch");
    let (shutdown_result, elapsed) = shutdown.join().expect("final shutdown thread panicked");
    assert!(matches!(
        shutdown_result,
        Err(Error::ChildTerminationFailed(_))
    ));
    assert!(
        elapsed < bound + Duration::from_millis(200),
        "final cleanup observation exceeded its single absolute deadline"
    );

    pool.runtime().block_on(async {
        timeout(Duration::from_secs(1), entered.changed())
            .await
            .expect("late cleanup task did not start")
            .expect("late cleanup entry signal disconnected");
        timeout(Duration::from_secs(1), late_entered.changed())
            .await
            .expect("post-barrier cleanup task did not start")
            .expect("post-barrier cleanup entry signal disconnected");
    });
    release.send_replace(true);
    late_release.send_replace(true);
    pool.runtime()
        .block_on(async {
            observe_cleanup(receipt).await?;
            observe_cleanup(late_receipt).await
        })
        .expect("timed-out observer must not cancel late cleanup");
    assert_eq!(client.cleanup_run_count_for_test(), 1);
    assert_eq!(late_client.cleanup_run_count_for_test(), 1);
}

#[test]
fn pool_drop_transfers_pending_cleanup_and_runtime_to_terminal_reaper() {
    let pool = pool(1);
    let lease = pool
        .runtime()
        .block_on(pool.acquire_lease(test_key(909)))
        .unwrap();
    let entry = Arc::clone(&lease.entry);
    let client = unstarted_client_for_cleanup();
    let (entered, release) = client.pause_cleanup_for_test();
    let mut guard = entry.install_and_arm(client.process_control()).unwrap();
    guard.armed = false;
    drop(guard);
    drop(lease);

    let started = std::time::Instant::now();
    drop(pool);
    assert!(
        started.elapsed() >= POOL_DROP_CLEANUP_TIMEOUT,
        "pool Drop returned before its bounded observation elapsed"
    );
    assert!(*entered.borrow(), "pool Drop did not start cleanup");
    let receipt = entry
        .process_control
        .lock()
        .unwrap()
        .cleanup
        .as_ref()
        .expect("pool Drop did not publish a cleanup receipt")
        .receipt
        .clone();
    assert_eq!(*receipt.borrow(), CleanupOutcome::Pending);

    release.send_replace(true);
    Runtime::new()
        .unwrap()
        .block_on(async { timeout(Duration::from_secs(2), observe_cleanup(receipt)).await })
        .expect("detached cleanup reaper dropped its runtime root")
        .expect("late cleanup must reach a terminal outcome");
    assert_eq!(client.cleanup_run_count_for_test(), 1);
}

#[cfg(unix)]
#[test]
fn client_drop_after_initialize_kills_pid_via_kill_on_drop() {
    // Drop a live, initialized client without calling
    // shutdown or force_terminate. `kill_on_drop(true)` is
    // the backstop — the child must still die.
    let fake = FakeLspBinary::new();
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let pid = rt.block_on(async {
        let client = LspClient::start_configured(
            fake.binary(),
            fake.launch_args(),
            fake.env(),
            workspace.path(),
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        let pid = read_pid(
            &fake.pid_file,
            std::time::Instant::now() + Duration::from_secs(2),
        )
        .expect("PID must be written");
        drop(client);
        pid
    });
    assert!(
        wait_until_dead(pid, Duration::from_secs(2)),
        "kill_on_drop must reap the child after drop"
    );
}

// ─── public termination diagnostic compatibility ───────────

#[test]
fn is_termination_unproven_covers_direct_and_nested_variants() {
    // Direct signal.
    assert!(Error::ChildTerminationFailed("boom".into()).is_termination_unproven());
    // Nested in OperationWithCleanupFailure via cleanup slot.
    let composed = Error::OperationWithCleanupFailure {
        original: Box::new(Error::ReadinessTimeout),
        cleanup: Box::new(Error::ChildTerminationFailed("nested".into())),
    };
    assert!(composed.is_termination_unproven());
    // Original slot alone must NOT flag — only `cleanup` is
    // the termination-proof channel.
    let original_only = Error::OperationWithCleanupFailure {
        original: Box::new(Error::ChildTerminationFailed("wrong slot".into())),
        cleanup: Box::new(Error::Protocol("proto".into())),
    };
    assert!(!original_only.is_termination_unproven());
    // Unrelated errors.
    assert!(!Error::ReadinessTimeout.is_termination_unproven());
    assert!(!Error::Protocol("p".into()).is_termination_unproven());
    assert!(!Error::PoolAtCapacity { capacity: 16 }.is_termination_unproven());
}

#[test]
fn with_lsp_result_termination_unproven_does_not_classify_pool_mode() {
    // The public diagnostic helper remains source-compatible, but pool mode
    // is driven only by the private typed cleanup disposition.
    let fake = FakeLspBinary::new();
    let pool = pool(4);
    let key = fake_pool_key(&fake, 1);
    let outcome = pool.with_lsp(
        key.clone(),
        spawn_spec(&fake, Duration::from_secs(3)),
        |_lsp| {
            Box::pin(async {
                Err::<(), Error>(Error::OperationWithCleanupFailure {
                    original: Box::new(Error::ServerExited(None.into())),
                    cleanup: Box::new(Error::ChildTerminationFailed("synthetic".into())),
                })
            })
        },
    );
    assert!(outcome.is_err());
    assert_eq!(pool.mode(), PoolMode::Running);
    drop(acquire(&pool, fake_pool_key(&fake, 2)).unwrap());
}

#[test]
fn stopped_mode_wins_over_late_invariant_failure() {
    let pool = pool(2);
    pool.shutdown_all().unwrap();
    assert_eq!(pool.mode(), PoolMode::Stopped);
    PoolEntry::new(test_key(1), &pool.registry).halt_registry("late invariant");
    assert_eq!(pool.mode(), PoolMode::Stopped);
}

// ─── Availability probe explicit wait → PID reaped ─────────

#[cfg(unix)]
#[test]
fn pool_availability_probe_timeout_reaps_hanging_binary() {
    // HangBinary bakes its pid file path into the script, so
    // no process-global env is required — safe under
    // parallel test execution.
    let hang = HangBinary::new();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // 3.5s is long enough for Python startup + PID write
        // under peak parallel test load (16 threads x mixed
        // subprocess tests). The child will otherwise block on
        // `time.sleep(300)` — the outer timeout terminates it.
        let err = crate::lsp::client::probe_binary(
            &hang.path,
            &["--version"],
            Duration::from_millis(3500),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::RequestTimeout | Error::OperationWithCleanupFailure { .. }
            ),
            "expected RequestTimeout, got {err:?}"
        );
        let pid = read_pid(
            &hang.pid_file,
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .expect("PID must be written before probe timed out");
        assert!(
            wait_until_dead(pid, Duration::from_secs(3)),
            "probe child pid {pid} must be reaped after timeout"
        );
    });
}

// ─── Barrier-based drain / Stopped race ────────────────────
//
// Overwrites the earlier "sleep 20ms then check" tests with
// deterministic barrier / bounded-poll variants that either
// observe the Draining state or fail explicitly instead of
// vacuously passing.

#[cfg(unix)]
fn wait_for_mode(pool: &Arc<LspClientPool>, target: PoolMode, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if pool.registry.lock().unwrap().mode == target {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

#[cfg(unix)]
#[test]
fn acquire_during_drain_returns_pool_draining_deterministic() {
    // Hold the process-control slot so bounded cleanup cannot complete before
    // the test observes the already-published Draining mode.
    use std::sync::Arc as StdArc;
    let fake = FakeLspBinary::new();
    let pool = StdArc::new(pool(2));
    let key = fake_pool_key(&fake, 1);
    pool.with_lsp(
        key.clone(),
        spawn_spec_stall(&fake, Duration::from_secs(10)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let process_gate = entry.process_control.lock().unwrap();
    let force_pool = StdArc::clone(&pool);
    let force_handle =
        std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_secs(5)));
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force_shutdown_all failed to publish Draining within 2s"
    );
    let err = acquire(&pool, fake_pool_key(&fake, 2)).unwrap_err();
    assert!(matches!(err, Error::PoolDraining));
    drop(process_gate);
    force_handle
        .join()
        .unwrap()
        .expect("bounded force cleanup must complete");
}

#[cfg(unix)]
#[test]
fn stopped_race_after_force_reaches_draining_stays_stopped() {
    // Force must have PUBLISHED Draining before shutdown_all
    // races in — otherwise we'd be testing "shutdown_all
    // before force starts" which is trivial.
    use std::sync::Arc as StdArc;
    let fake = FakeLspBinary::new();
    let pool = StdArc::new(pool(2));
    let key = fake_pool_key(&fake, 1);
    pool.with_lsp(
        key.clone(),
        spawn_spec_stall(&fake, Duration::from_secs(10)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let process_gate = entry.process_control.lock().unwrap();
    let force_pool = StdArc::clone(&pool);
    let force_handle =
        std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_secs(5)));
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force_shutdown_all failed to publish Draining"
    );
    // Now race shutdown_all in. Its Stopped write must not be
    // overwritten by force's finalize.
    pool.shutdown_all().ok();
    drop(process_gate);
    let force_result = force_handle.join().unwrap();
    assert_eq!(
        pool.mode(),
        PoolMode::Stopped,
        "final mode must be Stopped despite concurrent force finalize"
    );
    force_result.expect("bounded cleanup proved termination before force finalize");
}

// ─── Additional lifecycle invariants ───────────────────────

#[test]
fn lease_underflow_poisons_pool_fail_closed() {
    // Manually corrupt the lease counter under lock, then
    // drop a matching lease. The Drop's `checked_sub` must
    // detect underflow and transition to Poisoned rather
    // than silently clamp.
    let pool = pool(2);
    let key = test_key(1);
    let lease = acquire(&pool, key.clone()).unwrap();
    // Force underflow: set the record's counter to 0 while
    // the lease still exists.
    {
        let mut reg = pool.registry.lock().unwrap();
        reg.entries.get_mut(&key).unwrap().active_leases = 0;
    }
    drop(lease);
    assert_eq!(
        pool.mode(),
        PoolMode::Halted,
        "lease-counter underflow must poison the pool"
    );
}

#[cfg(unix)]
#[test]
fn readiness_failure_via_with_lsp_terminates_and_surfaces_error() {
    // Exercises the actual `PoolEntry::with_lsp_client`
    // readiness cleanup branch (rather than testing
    // `LspClient::wait_for_workspace_load` in isolation).
    // The fake LSP responds to initialize but never sends
    // `$/progress`, so the ProgressQuiescence readiness
    // strategy times out. We assert the pool call returns
    // Err and the child PID is dead.
    let fake = FakeLspBinary::new();
    let pool = pool(2);
    let key = fake_pool_key(&fake, 1);
    let spec = LspSpawnSpec {
        readiness: ReadinessStrategy::ProgressQuiescence {
            timeout: Duration::from_millis(150),
        },
        ..spawn_spec(&fake, Duration::from_secs(3))
    };
    let outcome = pool.with_lsp(key, spec, |_lsp| Box::pin(async { Ok::<(), Error>(()) }));
    assert!(outcome.is_err(), "readiness must time out");
    let pid = read_pid(
        &fake.pid_file,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .expect("PID must have been written");
    assert!(
        wait_until_dead(pid, Duration::from_secs(3)),
        "readiness-failure child pid {pid} must be reaped"
    );
}

// ─── Legacy availability probe (`LspClient::start_with_timeout`)

#[cfg(unix)]
#[test]
fn legacy_lsp_client_check_binary_probe_reaps_hanging_binary() {
    // `client.rs::check_binary_available` is exercised via
    // `LspClient::start_with_timeout`, which the pool path does
    // NOT touch. Hangs `--version`; expects RequestTimeout and a
    // reaped PID.
    let hang = HangBinary::new();
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let err = rt.block_on(async {
        LspClient::start_with_timeout(
            &hang.path,
            workspace.path(),
            "cfg",
            Duration::from_millis(3000),
        )
        .await
    });
    assert!(err.is_err(), "hanging probe must fail");
    let pid = read_pid(
        &hang.pid_file,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .expect("PID must be written before probe timed out");
    assert!(
        wait_until_dead(pid, Duration::from_secs(3)),
        "child pid {pid} must be reaped after probe timeout"
    );
}

// The manual-placeholder unit test that directly mutated
// `record.state = Evicting` and then removed the record was
// deleted: the real-path test
// `lru_eviction_real_path_placeholder_visible_and_replaced_on_termination_proof`
// exercises the same transitions through `acquire_lease`
// itself, and the manual test was implementation-lock-in with
// no distinct coverage.

#[cfg(unix)]
fn wait_for_record_state(
    pool: &Arc<LspClientPool>,
    key: &PoolKey,
    target: RecordState,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Some(record) = pool.registry.lock().unwrap().entries.get(key)
            && record.state == target
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

#[cfg(unix)]
#[test]
fn lru_eviction_real_path_placeholder_visible_and_replaced_on_termination_proof() {
    // Real end-to-end LRU eviction via `acquire_lease`:
    // 1. Populate V (Ready + idle) and hold its existing shutdown gate so the
    //    test can observe the `Evicting` placeholder without relying on a
    //    production grace-period delay.
    // 2. Spawn B's `with_lsp` on a background thread — it goes
    //    through the actual acquire path: marks V as Evicting,
    //    drops the registry lock, runs `V.shutdown()`, then on
    //    termination-proven completion removes V and inserts B.
    // 3. While V is Evicting the test asserts:
    //    - a new key acquire (C) sees the slot as reserved →
    //      `PoolAtCapacity`
    //    - a same-key acquire (V) is refused →`PoolDraining`
    //    - `entries.len()` stays at capacity (V placeholder counts)
    // 4. After B's thread completes, V is gone and B is Ready.
    use std::sync::Arc as StdArc;
    // Two independent fake binaries so V and B have distinct PID
    // files — this lets us pin the coexistence invariant: V's
    // child PID must be dead *before* B's replacement child
    // exists.
    let fake_v = FakeLspBinary::new();
    let fake_b = FakeLspBinary::new();
    let pool = StdArc::new(pool(1));
    let v_key = fake_pool_key(&fake_v, 1);
    let b_key = fake_pool_key(&fake_b, 2);
    let c_key = fake_pool_key(&fake_v, 3);
    let spec_slow_exit = LspSpawnSpec {
        env: fake_v.env_shutdown_ok_but_no_exit(),
        ..spawn_spec(&fake_v, Duration::from_secs(3))
    };
    pool.with_lsp(v_key.clone(), spec_slow_exit, |_lsp| {
        Box::pin(async { Ok::<(), Error>(()) })
    })
    .unwrap();
    assert_eq!(pool.len(), 1);
    let victim_pid = read_pid(
        &fake_v.pid_file,
        std::time::Instant::now() + Duration::from_secs(5),
    )
    .expect("V's child PID must have been written");
    assert!(
        pid_alive(victim_pid),
        "V's child must be alive after populate"
    );
    let victim_entry = pool.registry.lock().unwrap().entries[&v_key].entry.clone();
    let shutdown_gate = pool.runtime().block_on(victim_entry.shutdown_gate.lock());
    let b_pool = StdArc::clone(&pool);
    let b_key_for_thread = b_key.clone();
    let fake_b_interpreter = fake_b.interpreter.clone();
    let fake_b_launch_args = fake_b.launch_args();
    let fake_b_env = fake_b.env();
    let b_handle = std::thread::spawn(move || {
        let spec = LspSpawnSpec {
            binary: fake_b_interpreter,
            workspace_root: PathBuf::from("/tmp"),
            config_hash: "test".into(),
            request_timeout: Duration::from_secs(10),
            availability: AvailabilityStrategy::PathExistsExecutable,
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "python",
            launch_args: fake_b_launch_args,
            env: fake_b_env,
            initialization_options: serde_json::json!({}),
        };
        b_pool.with_lsp(b_key_for_thread, spec, |_lsp| {
            Box::pin(async { Ok::<(), Error>(()) })
        })
    });
    // Wait for V to enter Evicting.
    assert!(
        wait_for_record_state(&pool, &v_key, RecordState::Evicting, Duration::from_secs(5)),
        "V must enter Evicting within 5s"
    );
    // Placeholder-in-flight assertions.
    let err = acquire(&pool, c_key).unwrap_err();
    assert!(
        matches!(err, Error::PoolAtCapacity { capacity: 1 }),
        "new-key acquire during eviction must see PoolAtCapacity, got {err:?}"
    );
    let err = acquire(&pool, v_key.clone()).unwrap_err();
    assert!(
        matches!(err, Error::PoolDraining),
        "same-key acquire during eviction must see PoolDraining, got {err:?}"
    );
    assert_eq!(pool.len(), 1);
    drop(shutdown_gate);
    // Ordering invariant: V's child must be dead BEFORE B's
    // child comes into existence. Poll for B's pid_file to
    // appear (that's the earliest observable proof that B has
    // been spawned) and, in the SAME check, assert that
    // `victim_pid` is no longer alive. If a future refactor
    // reorders things so B is spawned while V is still alive,
    // this pin catches it — the previous "join then check"
    // pattern would miss the transient coexistence.
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    let b_pid = loop {
        if let Ok(s) = std::fs::read_to_string(&fake_b.pid_file)
            && let Ok(pid) = s.trim().parse::<u32>()
        {
            assert!(
                !pid_alive(victim_pid),
                "V's child pid {victim_pid} must be dead by the time B's child PID {pid} exists"
            );
            break pid;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "B's pid_file never appeared within {:?}",
            deadline.duration_since(std::time::Instant::now())
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    b_handle.join().unwrap().unwrap();
    assert_ne!(
        victim_pid, b_pid,
        "V's and B's children must be distinct processes"
    );
    let reg = pool.registry.lock().unwrap();
    assert!(!reg.entries.contains_key(&v_key), "V must be removed");
    assert!(reg.entries.contains_key(&b_key), "B must be inserted");
    assert_eq!(reg.entries.len(), 1);
}

#[cfg(unix)]
#[test]
fn same_key_concurrent_with_lsp_serializes_at_pool_entry_state() {
    // Contract: concurrent `with_lsp` calls for the SAME key
    // share a single `PoolEntry`, and their work closures never
    // overlap — the `PoolEntry.state` mutex serializes them so
    // at most one work closure holds the pooled client at a
    // time. Also verifies: only one `PoolRecord` for the key,
    // and all leases are released at the end.
    use std::sync::Arc as StdArc;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fake = FakeLspBinary::new();
    let pool = StdArc::new(pool(2));
    let key = fake_pool_key(&fake, 1);
    let n = 4usize;
    let counter = StdArc::new(AtomicUsize::new(0));
    let max_concurrent = StdArc::new(AtomicUsize::new(0));
    let barrier = StdArc::new(Barrier::new(n));
    let mut handles = Vec::new();
    for _ in 0..n {
        let p = StdArc::clone(&pool);
        let key = key.clone();
        let counter = StdArc::clone(&counter);
        let max_concurrent = StdArc::clone(&max_concurrent);
        let barrier = StdArc::clone(&barrier);
        let fake_interpreter = fake.interpreter.clone();
        let fake_launch_args = fake.launch_args();
        let fake_env = fake.env();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let spec = LspSpawnSpec {
                binary: fake_interpreter,
                workspace_root: PathBuf::from("/tmp"),
                config_hash: "test".into(),
                request_timeout: Duration::from_secs(3),
                availability: AvailabilityStrategy::PathExistsExecutable,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "python",
                launch_args: fake_launch_args,
                env: fake_env,
                initialization_options: serde_json::json!({}),
            };
            p.with_lsp(key, spec, |_lsp| {
                let counter = StdArc::clone(&counter);
                let max_concurrent = StdArc::clone(&max_concurrent);
                Box::pin(async move {
                    let cur = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    // Update max concurrent via CAS.
                    let mut best = max_concurrent.load(Ordering::Relaxed);
                    while cur > best
                        && let Err(observed) = max_concurrent.compare_exchange(
                            best,
                            cur,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                    {
                        best = observed;
                    }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    counter.fetch_sub(1, Ordering::Relaxed);
                    Ok::<(), Error>(())
                })
            })
        }));
    }
    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(
        max_concurrent.load(Ordering::Relaxed),
        1,
        "same-key concurrent with_lsp must serialize; max concurrent must be 1"
    );
    // Only one PoolRecord for the key.
    let reg = pool.registry.lock().unwrap();
    assert_eq!(reg.entries.len(), 1);
    assert!(reg.entries.contains_key(&key));
    // All leases released.
    assert_eq!(reg.entries[&key].active_leases, 0);
}

#[cfg(unix)]
#[test]
fn server_exit_clears_state_and_respawn_sends_did_open() {
    // Contract for the `PoolEntry::with_lsp_client` server-exit
    // cleanup branch: after the pooled work returns
    // `ServerExited(_)` or `ServerExitedWithStderr { .. }`, the
    // client is dropped, `opened_documents` is cleared, and the
    // NEXT `with_lsp` call spawns a fresh child that receives
    // `textDocument/didOpen` (not `didChange`) for the same URI.
    //
    // Verification uses a Python fake that logs every method it
    // receives to a shared file, prefixed with its PID. The test
    // then asserts:
    //   - two distinct PIDs are observed in the log
    //   - each PID's first `textDocument/*` method is `didOpen`
    //     (never `didChange`)
    let fake = FakeLspBinary::new();
    let methods_log = fake.pid_file.with_file_name("methods.log");
    let pool = pool(2);
    let key = fake_pool_key(&fake, 1);
    let uri = Url::from("file:///tmp/pool-test/hello.py");
    let first_spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(10))
    };
    let uri_clone = uri.clone();
    let first_methods_log = methods_log.clone();
    let first_result = pool.with_lsp(key.clone(), first_spec, move |pooled| {
        Box::pin(async move {
            pooled.sync_document(&uri_clone, "print('hello')").await?;
            assert!(
                wait_for_logged_method(
                    &first_methods_log,
                    "textDocument/didOpen",
                    Duration::from_secs(3),
                ),
                "first child must consume didOpen before cleanup begins"
            );
            pooled
                .client
                .process_control()
                .force_os_residual_for_test("synthetic process-group residual");
            // Inject the terminal transport fact after didOpen. The branch
            // under test owns process cleanup; depending on reader timing for
            // a subprocess exit would conflate that contract with request
            // timeout scheduling.
            Err::<(), Error>(Error::ServerExited(None.into()))
        })
    });
    assert!(
        first_result
            .as_ref()
            .is_err_and(Error::is_termination_unproven),
        "first work must retain its synthetic OS residual, got {first_result:?}"
    );
    assert_eq!(
        pool.mode(),
        PoolMode::Running,
        "an OS residual is a caller error, not an admission invariant"
    );
    // Second call — new spec, no exit-on-definition, work only
    // does the didOpen so it succeeds. Same key, same URI.
    let second_spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(10))
    };
    let uri_clone = uri.clone();
    pool.with_lsp(key.clone(), second_spec, move |pooled| {
        Box::pin(async move {
            pooled
                .sync_document(&uri_clone, "print('after respawn')")
                .await?;
            Ok::<(), Error>(())
        })
    })
    .expect("second call must succeed on a freshly spawned child");
    // Bounded poll: `did_open` is a notification (fire-and-
    // forget from LspClient), so the child may not have logged
    // it yet by the time `with_lsp` returns.
    let per_pid = poll_methods_log(&methods_log, 2, Duration::from_secs(3));
    assert!(
        per_pid.len() >= 2,
        "expected at least two distinct PIDs (respawn), got {}",
        per_pid.len()
    );
    for (pid, methods) in &per_pid {
        let first = methods
            .first()
            .unwrap_or_else(|| panic!("pid {pid} sent no textDocument/* methods"));
        assert!(
            first == "textDocument/didOpen",
            "pid {pid}: first textDocument/* method must be didOpen, got {first} (all: {methods:?})"
        );
    }
}

#[cfg(unix)]
#[test]
fn force_finalize_preserves_concurrent_halted_mode() {
    // If a concurrent invariant failure halts the pool
    // while `force_shutdown_all` is mid-drain, the force
    // finalizer must NOT regress the mode back to `Running`
    // just because its own local cleanup was clean.
    //
    // Reproduce by:
    // 1. Populate one entry and hold its process-control slot so bounded
    //    cleanup pauses after publishing the drain.
    // 2. Kick `force_shutdown_all` on a thread and observe `Draining`.
    // 3. Inject a typed internal invariant on the exact entry.
    // 4. Wait for the force thread to complete. Since its own
    //    drain is clean (no timeout / no unproven), it would
    //    naively finalize to `Running` — the finalize must
    //    instead preserve the `Poisoned` state.
    use std::sync::Arc as StdArc;
    let fake = FakeLspBinary::new();
    let pool = StdArc::new(pool(2));
    let key = fake_pool_key(&fake, 1);
    let spec_slow_exit = LspSpawnSpec {
        env: fake.env_shutdown_ok_but_no_exit(),
        ..spawn_spec(&fake, Duration::from_secs(3))
    };
    pool.with_lsp(key.clone(), spec_slow_exit, |_lsp| {
        Box::pin(async { Ok::<(), Error>(()) })
    })
    .unwrap();
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();
    let process_gate = entry.process_control.lock().unwrap();
    let force_pool = StdArc::clone(&pool);
    let force_handle =
        std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_secs(5)));
    // Wait for Draining publication so the race window is real.
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force_shutdown_all failed to publish Draining"
    );
    // Race in a private invariant failure.
    entry.halt_registry("synthetic invariant failure");
    drop(process_gate);
    // Force completes; finalize must NOT overwrite Poisoned.
    let force_result = force_handle.join().unwrap();
    assert_eq!(
        pool.mode(),
        PoolMode::Halted,
        "force finalize must preserve concurrent Poisoned, got {:?}",
        pool.mode()
    );
    // The force call's return value should surface PoolPoisoned.
    match force_result {
        Err(Error::PoolPoisoned) => {}
        other => panic!("expected Err(PoolPoisoned) from force, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn wait_for_logged_method_requires_the_exact_method() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("methods.log");
    std::fs::write(&path, "123:textDocument/didOpen\n").unwrap();

    assert!(!wait_for_logged_method(
        &path,
        "textDocument/definition",
        Duration::ZERO,
    ));

    let mut log = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(log, "123:textDocument/definition").unwrap();
    assert!(wait_for_logged_method(
        &path,
        "textDocument/definition",
        Duration::ZERO,
    ));
}

/// Wait until `required_method` itself is logged.
///
/// The previous PID-count wait released a carrier as soon as the child
/// recorded `textDocument/didOpen`, which precedes the definition request the
/// fixture actually needs in flight; Linux scheduling made that window
/// observable. The snapshot is read before the deadline check, so a zero
/// timeout still sees an already-logged method.
#[cfg(unix)]
fn wait_for_logged_method(path: &Path, required_method: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let found = read_methods_by_pid(path)
            .values()
            .any(|methods| methods.iter().any(|method| method == required_method));
        if found {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
#[test]
fn respawn_initialize_failure_reaps_child_and_allows_next_attempt() {
    let fake = FakeLspBinary::new();
    let spawn_count = fake.pid_file.with_file_name("spawn-count");
    let workspace = tempfile::tempdir().unwrap();
    let uri = Url::from("file:///tmp/pool-test/respawn.py");
    let mut env = fake.env();
    env.push((
        "CAIRN_TEST_SPAWN_COUNT_FILE".into(),
        spawn_count.display().to_string(),
    ));
    env.push(("CAIRN_TEST_FAIL_INITIALIZE_ORDINAL".into(), "2".into()));
    env.push(("CAIRN_TEST_CLOSE_STDOUT_ON_DEFINITION".into(), "1".into()));

    Runtime::new().unwrap().block_on(async {
        let client = LspClient::start_configured(
            fake.binary(),
            fake.launch_args(),
            env,
            workspace.path(),
            serde_json::json!({}),
            Duration::from_secs(3),
        )
        .await
        .expect("initial child must initialize");

        let first = client
            .definition(
                &uri,
                Position {
                    line: 0,
                    character: 0,
                },
            )
            .await;
        assert!(
            matches!(
                first,
                Err(Error::ServerExited(_)) | Err(Error::ServerExitedWithStderr { .. })
            ),
            "first child must exit before the respawn carrier: {first:?}"
        );
        timeout(Duration::from_secs(1), async {
            while client.transport_generation() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader must publish the first child exit");

        let failed_respawn = client
            .did_open(&uri, "python", 1, "print('failed respawn')")
            .await;
        assert!(
            failed_respawn.is_err(),
            "second child must fail its initialize exchange: {failed_respawn:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&spawn_count)
                .unwrap()
                .trim()
                .parse::<u32>()
                .unwrap(),
            2,
            "initialize failure must occur on the first respawn: {failed_respawn:?}"
        );
        let failed_pid = read_pid(
            &fake.pid_file,
            std::time::Instant::now() + Duration::from_secs(1),
        )
        .expect("failed respawn child must publish its pid");
        assert!(
            wait_until_dead(failed_pid, Duration::from_secs(2)),
            "failed respawn child {failed_pid} was not reaped"
        );

        client
            .did_open(&uri, "python", 2, "print('fresh respawn')")
            .await
            .expect("remaining restart budget must start a fresh child");
        let fresh_pid = read_pid(
            &fake.pid_file,
            std::time::Instant::now() + Duration::from_secs(1),
        )
        .expect("fresh respawn child must publish its pid");
        assert_ne!(fresh_pid, failed_pid, "next respawn must use a new child");
        assert_eq!(
            std::fs::read_to_string(&spawn_count)
                .unwrap()
                .trim()
                .parse::<u32>()
                .unwrap(),
            3,
            "failed initialize must consume one restart attempt"
        );
        client.shutdown().await.expect("fresh child shutdown");
    });
}

#[cfg(unix)]
fn poll_methods_log(
    path: &Path,
    min_pids_with_text_document: usize,
    timeout: Duration,
) -> std::collections::BTreeMap<u32, Vec<String>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let per_pid = read_methods_by_pid(path);
        if per_pid.len() >= min_pids_with_text_document || std::time::Instant::now() >= deadline {
            return per_pid;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
fn read_methods_by_pid(path: &Path) -> std::collections::BTreeMap<u32, Vec<String>> {
    let Ok(log) = std::fs::read_to_string(path) else {
        return std::collections::BTreeMap::new();
    };
    let mut per_pid: std::collections::BTreeMap<u32, Vec<String>> =
        std::collections::BTreeMap::new();
    for line in log.lines() {
        if let Some((pid_str, method)) = line.split_once(':')
            && let Ok(pid) = pid_str.parse::<u32>()
            && method.starts_with("textDocument/")
        {
            per_pid.entry(pid).or_default().push(method.to_string());
        }
    }
    per_pid
}
