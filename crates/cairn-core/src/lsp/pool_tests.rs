use super::*;
use crate::lsp::Error;
use std::io::{self, Write};
use tracing_subscriber::fmt::MakeWriter;

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
            |timeout| {
                waited = Some(timeout);
                async { Ok(()) }
            },
        ))
        .unwrap();

    assert_eq!(waited, Some(timeout));
}

#[test]
fn lsp_pool_skips_wait_hook_for_initialize_response_readiness() {
    let runtime = Runtime::new().unwrap();
    let mut waited = false;

    runtime
        .block_on(dispatch_readiness(
            &ReadinessStrategy::InitializeResponseOnly,
            |timeout| {
                let _ = timeout;
                waited = true;
                async { Ok(()) }
            },
        ))
        .unwrap();

    assert!(!waited);
}

#[test]
fn lsp_pool_dispatches_readiness_strategy_per_server() {
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
    pool.runtime
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
fn pool_env_warnings_do_not_log_raw_values() {
    let output = CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(output.clone())
        .finish();
    let sensitive_capacity = "sensitive-capacity-token";
    let sensitive_ttl = "sensitive-ttl-token";

    tracing::subscriber::with_default(subscriber, || {
        assert_eq!(cap(Some(sensitive_capacity)), DEFAULT_POOL_CAPACITY);
        assert_eq!(
            idle_ttl_from_env_value(Some(sensitive_ttl)),
            Some(DEFAULT_IDLE_TTL)
        );
    });

    let captured = output.contents();
    assert!(captured.contains(POOL_CAPACITY_ENV));
    assert!(captured.contains(IDLE_TTL_ENV));
    assert!(captured.contains("reason=\"invalid\""));
    assert!(!captured.contains(sensitive_capacity));
    assert!(!captured.contains(sensitive_ttl));
}

// ─── Force-shutdown outcome classifier ─────────────────────

fn elapsed_outcome() -> std::result::Result<Result<()>, tokio::time::error::Elapsed> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(1),
            std::future::pending::<Result<()>>(),
        )
        .await
    })
}

#[test]
fn classify_force_shutdown_all_ok_yields_neither_flag() {
    let results = vec![Ok(Ok::<(), Error>(())), Ok(Ok(()))];
    let out = classify_force_shutdown_results(results, Duration::from_millis(50));
    assert!(!out.termination_unproven());
    assert!(!out.timed_out);
    assert!(out.first_regular_err.is_none());
    assert!(out.first_unproven_err.is_none());
}

#[test]
fn classify_force_shutdown_actual_unproven_lands_in_unproven_slot() {
    // A real `ChildTerminationFailed` from `entry.shutdown()`
    // lands in the unproven slot and leaves `timed_out=false`
    // so the finalize does not fabricate an outer-timeout error.
    let results = vec![Ok(Err(Error::ChildTerminationFailed("real".into())))];
    let out = classify_force_shutdown_results(results, Duration::from_millis(50));
    assert!(out.termination_unproven());
    assert!(!out.timed_out);
    assert!(out.first_regular_err.is_none());
    match out.first_unproven_err {
        Some(Error::ChildTerminationFailed(msg)) => assert_eq!(msg, "real"),
        other => panic!("expected preserved ChildTerminationFailed, got {other:?}"),
    }
}

#[test]
fn classify_force_shutdown_outer_timeout_flags_timed_out_only() {
    // The outer `timeout(..)` firing sets `timed_out=true`; the
    // finalize will fabricate a synthetic ChildTerminationFailed.
    // No entry returned its own unproven Err.
    let results = vec![elapsed_outcome()];
    let out = classify_force_shutdown_results(results, Duration::from_millis(50));
    assert!(out.termination_unproven());
    assert!(out.timed_out);
    assert!(out.first_regular_err.is_none());
    assert!(out.first_unproven_err.is_none());
}

#[test]
fn classify_force_shutdown_non_unproven_err_only_flags_regular_slot() {
    // A protocol failure whose child was still reaped is NOT a
    // termination-unproven signal — mode stays Running / does
    // not poison; downstream sees the protocol err only.
    let results = vec![Ok(Err(Error::Protocol("proto".into())))];
    let out = classify_force_shutdown_results(results, Duration::from_millis(50));
    assert!(!out.termination_unproven());
    assert!(!out.timed_out);
    assert!(matches!(out.first_regular_err, Some(Error::Protocol(_))));
    assert!(out.first_unproven_err.is_none());
}

#[test]
fn classify_force_shutdown_mixed_regular_then_unproven_preserves_both() {
    // Order 1: regular error first, then unproven. Both must be
    // preserved in their respective slots — a naive single-slot
    // `first_err` would silently drop the unproven cause.
    let results = vec![
        Ok(Err(Error::Protocol("a".into()))),
        Ok(Err(Error::ChildTerminationFailed("b".into()))),
    ];
    let out = classify_force_shutdown_results(results, Duration::from_millis(50));
    assert!(out.termination_unproven());
    assert!(!out.timed_out);
    assert!(matches!(out.first_regular_err, Some(Error::Protocol(_))));
    assert!(matches!(
        out.first_unproven_err,
        Some(Error::ChildTerminationFailed(_))
    ));
}

#[test]
fn classify_force_shutdown_mixed_unproven_then_regular_preserves_both() {
    // Order 2: unproven first, then regular. Symmetric — the
    // classifier must not depend on order-of-arrival.
    let results = vec![
        Ok(Err(Error::ChildTerminationFailed("b".into()))),
        Ok(Err(Error::Protocol("a".into()))),
    ];
    let out = classify_force_shutdown_results(results, Duration::from_millis(50));
    assert!(out.termination_unproven());
    assert!(!out.timed_out);
    assert!(matches!(out.first_regular_err, Some(Error::Protocol(_))));
    assert!(matches!(
        out.first_unproven_err,
        Some(Error::ChildTerminationFailed(_))
    ));
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
    pool.runtime.block_on(LspClientPool::sweep_idle_once(
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
fn final_shutdown_does_not_run_entry_shutdown_concurrently_with_idle_sweep() {
    let pool = Arc::new(pool(1));
    let key = test_key(12);
    drop(acquire(&pool, key.clone()).unwrap());
    age_record(&pool, &key, Duration::from_secs(11));
    let entry = pool.registry.lock().unwrap().entries[&key].entry.clone();

    // Hold the per-entry shutdown gate so the sweep can reserve the
    // record but cannot begin process cleanup. Final shutdown must
    // wait behind the same gate and honor its own bound.
    let gate = pool.runtime.block_on(entry.shutdown_gate.lock());
    let sweep_pool = Arc::clone(&pool);
    let sweep = std::thread::spawn(move || {
        sweep_pool.runtime.block_on(LspClientPool::sweep_idle_once(
            &sweep_pool.registry,
            Instant::now(),
            Duration::from_secs(10),
            Duration::from_secs(1),
        ))
    });
    assert!(wait_for_record_state(
        &pool,
        &key,
        RecordState::Evicting,
        Duration::from_secs(1)
    ));

    let shutdown_err = pool
        .shutdown_all_bounded(Duration::from_millis(20))
        .unwrap_err();
    assert!(shutdown_err.is_termination_unproven());
    assert_eq!(pool.mode(), PoolMode::Stopped);

    drop(gate);
    assert_eq!(sweep.join().unwrap().unwrap(), 1);
    assert_eq!(pool.mode(), PoolMode::Stopped);
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
            p.runtime
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
fn pool_poisoned_mode_permanently_rejects_acquire() {
    let pool = pool(2);
    {
        let mut reg = pool.registry.lock().unwrap();
        reg.mode = PoolMode::Poisoned;
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
const FAKE_LSP_SCRIPT: &str = r#"#!/usr/bin/env python3
import sys, os, json

pid_file = os.environ.get("CAIRN_TEST_PID_FILE")
if pid_file:
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

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
    if method == "initialize":
        write_message({"jsonrpc": "2.0", "id": msg["id"], "result": {"capabilities": {}}})
    elif method == "shutdown" and os.environ.get("CAIRN_TEST_RESPOND_SHUTDOWN", "1") == "1":
        write_message({"jsonrpc": "2.0", "id": msg["id"], "result": None})
    elif method == "exit" and os.environ.get("CAIRN_TEST_IGNORE_EXIT", "0") != "1":
        sys.exit(0)
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
    path: PathBuf,
    pid_file: PathBuf,
}

#[cfg(unix)]
impl FakeLspBinary {
    fn new() -> Self {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake-lsp.py");
        let pid_file = dir.path().join("fake-lsp.pid");
        fs::write(&path, FAKE_LSP_SCRIPT).unwrap();
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
        vec![(
            "CAIRN_TEST_PID_FILE".into(),
            self.pid_file.display().to_string(),
        )]
    }

    /// Env that also silences shutdown responses so
    /// `LspClient::shutdown` sits in the request timeout + the
    /// graceful `wait` timeout — used to force `force_shutdown_all`'s
    /// outer timeout to fire.
    fn env_stall_shutdown(&self) -> Vec<(String, String)> {
        let mut env = self.env();
        env.push(("CAIRN_TEST_RESPOND_SHUTDOWN".into(), "0".into()));
        env
    }

    /// Env that responds to `shutdown` fast but ignores `exit`,
    /// so `LspClient::shutdown` gets a clean protocol response,
    /// then sits in `SHUTDOWN_TIMEOUT` graceful wait until
    /// `force_terminate` reaps. Total ~2s. Used to hold an
    /// `Evicting` placeholder without polluting the shutdown
    /// result with a `Handshake`/`RequestTimeout` protocol error.
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
fn spawn_spec(fake: &FakeLspBinary, request_timeout: Duration) -> LspSpawnSpec {
    LspSpawnSpec {
        binary: fake.path.clone(),
        workspace_root: PathBuf::from("/tmp"),
        config_hash: "test".into(),
        request_timeout,
        availability: AvailabilityStrategy::PathExistsExecutable,
        readiness: ReadinessStrategy::InitializeResponseOnly,
        language_id: "python",
        launch_args: Vec::new(),
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

#[cfg(unix)]
fn fake_pool_key(fake: &FakeLspBinary, n: u32) -> PoolKey {
    PoolKey {
        canonical_repo_root: PathBuf::from(format!("/tmp/pool-key-{n}")),
        language: "python".into(),
        analyzer_id: format!("fake-{n}"),
        binary: fake.path.clone(),
        config_hash: "test".into(),
    }
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

    let methods = poll_methods_log(&methods_log, 1, Duration::from_secs(5));
    assert!(
        methods.values().any(|methods| methods
            .iter()
            .any(|method| method == "textDocument/definition")),
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

#[test]
fn bounded_final_shutdown_rejects_late_process_control_install() {
    let entry = PoolEntry::default();
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
        entry.install_process_control(client.process_control()),
        Err(Error::PoolStopped)
    ));
}

#[cfg(unix)]
#[test]
fn force_shutdown_all_with_stalled_shutdown_transitions_to_poisoned() {
    let fake = FakeLspBinary::new();
    let pool = pool(2);
    pool.with_lsp(
        fake_pool_key(&fake, 1),
        spawn_spec_stall(&fake, Duration::from_secs(10)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    // Very small entry_timeout so the outer bound fires before
    // the stalled `shutdown` request / graceful wait completes.
    let err = pool
        .force_shutdown_all(Duration::from_millis(50))
        .unwrap_err();
    assert!(matches!(err, Error::PoolPoisoned));
    assert_eq!(pool.mode(), PoolMode::Poisoned);
    // Follow-up acquires must reject.
    let err = acquire(&pool, fake_pool_key(&fake, 2)).unwrap_err();
    assert!(matches!(err, Error::PoolPoisoned));
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
            &fake.path,
            Vec::new(),
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
            &fake.path,
            Vec::new(),
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
fn client_drop_after_initialize_kills_pid_via_kill_on_drop() {
    // Drop a live, initialized client without calling
    // shutdown or force_terminate. `kill_on_drop(true)` is
    // the backstop — the child must still die.
    let fake = FakeLspBinary::new();
    let rt = Runtime::new().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let pid = rt.block_on(async {
        let client = LspClient::start_configured(
            &fake.path,
            Vec::new(),
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

// ─── is_termination_unproven + central poison propagation ──

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
fn with_lsp_result_termination_unproven_poisons_pool() {
    // Central-poison invariant: any Err bubbling out of
    // `with_lsp` that satisfies `is_termination_unproven`
    // must transition the pool to `Poisoned`, so no
    // subsequent acquire (any key) spawns a replacement.
    //
    // We simulate the unproven signal via the `work` closure
    // rather than manipulating internal state — that's the
    // exact path a real ServerExited-with-cleanup-failure
    // would take.
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
    assert_eq!(pool.mode(), PoolMode::Poisoned);
    // Any subsequent acquire must reject.
    let err = acquire(&pool, fake_pool_key(&fake, 2)).unwrap_err();
    assert!(matches!(err, Error::PoolPoisoned));
}

#[test]
fn stopped_mode_wins_over_normal_path_poison() {
    // The central poison helper must never overwrite
    // `Stopped` — a completed daemon-final shutdown wins over
    // an in-flight unproven cleanup.
    let pool = pool(2);
    pool.shutdown_all().unwrap();
    assert_eq!(pool.mode(), PoolMode::Stopped);
    // Try to poison — helper is a no-op under Stopped.
    pool.poison_from_unproven_cleanup("test");
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
    // Deterministic replacement for the earlier sleep-based
    // test. We spawn a `force_shutdown_all` on a stalled
    // entry (which spends its whole time in the Draining
    // phase) and poll for Draining publication with a
    // bounded deadline — no conditional-pass; if we can't
    // observe Draining, the test fails.
    use std::sync::Arc as StdArc;
    let fake = FakeLspBinary::new();
    let pool = StdArc::new(pool(2));
    pool.with_lsp(
        fake_pool_key(&fake, 1),
        spawn_spec_stall(&fake, Duration::from_secs(10)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    let force_pool = StdArc::clone(&pool);
    let force_handle = std::thread::spawn(move || {
        // Longer timeout so Draining is observable throughout.
        let _ = force_pool.force_shutdown_all(Duration::from_secs(5));
    });
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force_shutdown_all failed to publish Draining within 2s"
    );
    let err = acquire(&pool, fake_pool_key(&fake, 2)).unwrap_err();
    assert!(matches!(err, Error::PoolDraining));
    force_handle.join().unwrap();
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
    pool.with_lsp(
        fake_pool_key(&fake, 1),
        spawn_spec_stall(&fake, Duration::from_secs(10)),
        |_lsp| Box::pin(async { Ok::<(), Error>(()) }),
    )
    .unwrap();
    let force_pool = StdArc::clone(&pool);
    let force_handle =
        std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_millis(50)));
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force_shutdown_all failed to publish Draining"
    );
    // Now race shutdown_all in. Its Stopped write must not be
    // overwritten by force's finalize.
    pool.shutdown_all().ok();
    let force_result = force_handle.join().unwrap();
    assert_eq!(
        pool.mode(),
        PoolMode::Stopped,
        "final mode must be Stopped despite concurrent force finalize"
    );
    // Even under Stopped, the force call must surface the
    // termination-unproven signal it observed (50 ms outer
    // timeout on a slow-shutdown entry). Dropping it would be an
    // observability lie — safe from a spawn-invariant standpoint
    // but misleading for callers.
    let err = force_result.expect_err("force must surface unproven signal");
    assert!(
        err.is_termination_unproven(),
        "force result must be termination-unproven, got {err:?}"
    );
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
        PoolMode::Poisoned,
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
    // 1. Populate V (Ready + idle) with a fake whose `shutdown`
    //    stalls long enough (~2s SHUTDOWN_TIMEOUT + kill window)
    //    that the test can observe the `Evicting` placeholder.
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
    let b_pool = StdArc::clone(&pool);
    let b_key_for_thread = b_key.clone();
    let fake_b_path = fake_b.path.clone();
    let fake_b_env = fake_b.env();
    let b_handle = std::thread::spawn(move || {
        let spec = LspSpawnSpec {
            binary: fake_b_path,
            workspace_root: PathBuf::from("/tmp"),
            config_hash: "test".into(),
            request_timeout: Duration::from_secs(10),
            availability: AvailabilityStrategy::PathExistsExecutable,
            readiness: ReadinessStrategy::InitializeResponseOnly,
            language_id: "python",
            launch_args: Vec::new(),
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
        let fake_path = fake.path.clone();
        let fake_env = fake.env();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let spec = LspSpawnSpec {
                binary: fake_path,
                workspace_root: PathBuf::from("/tmp"),
                config_hash: "test".into(),
                request_timeout: Duration::from_secs(3),
                availability: AvailabilityStrategy::PathExistsExecutable,
                readiness: ReadinessStrategy::InitializeResponseOnly,
                language_id: "python",
                launch_args: Vec::new(),
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
        env: {
            let mut env = fake.env_with_methods_log(&methods_log);
            env.push(("CAIRN_TEST_EXIT_ON_DEFINITION".into(), "1".into()));
            env
        },
        ..spawn_spec(&fake, Duration::from_secs(3))
    };
    let uri_clone = uri.clone();
    let first_result = pool.with_lsp(key.clone(), first_spec, move |pooled| {
        Box::pin(async move {
            pooled.sync_document(&uri_clone, "print('hello')").await?;
            // Any `definition` request triggers the fake's
            // configured exit-with-stderr, which surfaces as
            // `ServerExitedWithStderr` on the next `request` roundtrip.
            let _ = pooled
                .definition(
                    &uri_clone,
                    Position {
                        line: 0,
                        character: 0,
                    },
                )
                .await?;
            Ok::<(), Error>(())
        })
    });
    assert!(
        matches!(
            first_result,
            Err(Error::ServerExited(_)) | Err(Error::ServerExitedWithStderr { .. })
        ),
        "first work must surface a server-exit error, got {first_result:?}"
    );
    // Second call — new spec, no exit-on-definition, work only
    // does the didOpen so it succeeds. Same key, same URI.
    let second_spec = LspSpawnSpec {
        env: fake.env_with_methods_log(&methods_log),
        ..spawn_spec(&fake, Duration::from_secs(3))
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
fn force_finalize_preserves_concurrent_poisoned_mode() {
    // If a concurrent normal-path cleanup poisons the pool
    // while `force_shutdown_all` is mid-drain, the force
    // finalizer must NOT regress the mode back to `Running`
    // just because its own local cleanup was clean.
    //
    // Reproduce by:
    // 1. Populate one entry with a fake that ignores `exit`
    //    (~2s graceful wait before `force_terminate` reaps).
    // 2. Kick `force_shutdown_all` on a thread; poll for
    //    `Draining` publication.
    // 3. Directly call `poison_from_unproven_cleanup` on the
    //    pool (simulating a concurrent central-poison event
    //    from a different normal-path caller). This transitions
    //    mode to `Poisoned` mid-drain.
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
    pool.with_lsp(key, spec_slow_exit, |_lsp| {
        Box::pin(async { Ok::<(), Error>(()) })
    })
    .unwrap();
    let force_pool = StdArc::clone(&pool);
    let force_handle =
        std::thread::spawn(move || force_pool.force_shutdown_all(Duration::from_secs(5)));
    // Wait for Draining publication so the race window is real.
    assert!(
        wait_for_mode(&pool, PoolMode::Draining, Duration::from_secs(2)),
        "force_shutdown_all failed to publish Draining"
    );
    // Race in a normal-path poison.
    pool.poison_from_unproven_cleanup("synthetic normal-path unproven");
    // Force completes; finalize must NOT overwrite Poisoned.
    let force_result = force_handle.join().unwrap();
    assert_eq!(
        pool.mode(),
        PoolMode::Poisoned,
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
