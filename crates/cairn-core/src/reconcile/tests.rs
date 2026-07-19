//! `RepoReconcileManager` MF-1..MF-10 must-fix suite.
//!
//! Each test drives the manager through a specific behaviour
//! contract with a fake register hook — no real git worktree
//! required. See [`super`] for the concurrency contract that
//! these tests exercise.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::timeout;

use crate::cas::registry as cas_registry;
use crate::cas::store as cas_store;
use crate::lifecycle::{RegistrationReconcilePolicy, RepoLifecycleManager};
use crate::paths::CasDataDir;
use crate::reconcile::{
    Clock, PeriodicReconcilePolicy, ReconcileTrigger, RepoReconcileManager, RetryPolicy,
    TestRegisterHookFn,
};
use crate::testutil::init_repo;

// ─── Test-only clock / hook / helpers ─────────────────────────

#[derive(Debug)]
struct ManualClock {
    now_ns: AtomicI64,
}

impl ManualClock {
    fn new(start_ns: i64) -> Arc<Self> {
        Arc::new(Self {
            now_ns: AtomicI64::new(start_ns),
        })
    }
    #[allow(dead_code)]
    fn advance(&self, ns: i64) {
        self.now_ns.fetch_add(ns, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }
}

/// Records every register invocation and lets tests inject
/// per-call outcomes / gating.
#[derive(Default)]
struct FakeRegister {
    calls: Mutex<Vec<FakeCall>>,
    // If set to Some, the hook returns Err(msg) once and clears
    // the slot. Otherwise Ok.
    fail_next: Mutex<Option<String>>,
    // Optional gate: if set, the hook blocks the calling thread
    // on this notify until the test releases it. Used by MF-3
    // (event during in-flight attempt) and MF-9 (concurrent wake
    // race).
    gate: Mutex<Option<Arc<GateInner>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeCall {
    repo_hash: String,
    alias: String,
    generation: i64,
    forced: bool,
}

struct GateInner {
    entered: AtomicUsize,
    release: AtomicBool,
    notify_test: tokio::sync::Notify,
    notify_hook: std::sync::Condvar,
    hook_lock: std::sync::Mutex<()>,
}

impl FakeRegister {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn calls(&self) -> Vec<FakeCall> {
        self.calls.lock().unwrap().clone()
    }
    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
    fn set_fail_next(&self, msg: &str) {
        *self.fail_next.lock().unwrap() = Some(msg.to_string());
    }
    fn install_gate(&self) -> Arc<GateInner> {
        let gate = Arc::new(GateInner {
            entered: AtomicUsize::new(0),
            release: AtomicBool::new(false),
            notify_test: tokio::sync::Notify::new(),
            notify_hook: std::sync::Condvar::new(),
            hook_lock: std::sync::Mutex::new(()),
        });
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    fn as_hook(self: &Arc<Self>) -> TestRegisterHookFn {
        let this = self.clone();
        Arc::new(
            move |repo_hash: &str, alias: &str, generation: i64, forced: bool| {
                let call = FakeCall {
                    repo_hash: repo_hash.to_string(),
                    alias: alias.to_string(),
                    generation,
                    forced,
                };
                this.calls.lock().unwrap().push(call);
                if let Some(gate) = this.gate.lock().unwrap().clone() {
                    gate.entered.fetch_add(1, Ordering::SeqCst);
                    gate.notify_test.notify_waiters();
                    let mut guard = gate.hook_lock.lock().unwrap();
                    while !gate.release.load(Ordering::SeqCst) {
                        guard = gate.notify_hook.wait(guard).unwrap();
                    }
                    drop(guard);
                }
                if let Some(msg) = this.fail_next.lock().unwrap().take() {
                    return Err(crate::Error::Internal(msg));
                }
                Ok(())
            },
        )
    }
}

impl GateInner {
    async fn wait_for_entry(&self, timeout_ms: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        while self.entered.load(Ordering::SeqCst) == 0 {
            let notified = self.notify_test.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.entered.load(Ordering::SeqCst) > 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("gate never observed hook entry");
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }
    fn release(&self) {
        self.release.store(true, Ordering::SeqCst);
        let _guard = self.hook_lock.lock().unwrap();
        self.notify_hook.notify_all();
    }
}

fn fresh_cas() -> (tempfile::TempDir, Arc<CasDataDir>) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = Arc::new(CasDataDir::with_root(tmp.path().to_path_buf()));
    cas.ensure().unwrap();
    (tmp, cas)
}

fn seed_repo(cas: &CasDataDir, alias: &str, root: &str, repo_hash: &str) {
    let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
    let tx = index.transaction().unwrap();
    cas_registry::upsert(&tx, alias, root, repo_hash, 1).unwrap();
    tx.commit().unwrap();
}

fn seed_extra_alias(cas: &CasDataDir, alias: &str, root: &str, repo_hash: &str) {
    let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
    let tx = index.transaction().unwrap();
    cas_registry::upsert(&tx, alias, root, repo_hash, 2).unwrap();
    tx.commit().unwrap();
}

fn read_state(cas: &CasDataDir, repo_hash: &str) -> cas_registry::RepoReconcileState {
    let index = cas_registry::open(&cas.index_db_path()).unwrap();
    cas_registry::get_reconcile_state(&index, repo_hash)
        .unwrap()
        .expect("state row must exist")
}

fn build_manager(cas: Arc<CasDataDir>, clock: Arc<ManualClock>) -> Arc<RepoReconcileManager> {
    RepoReconcileManager::with_config(
        cas,
        None,
        clock,
        RetryPolicy {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        },
    )
}

async fn wait_for(mut cond: impl FnMut() -> bool, ms: u64, label: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {label}");
}

// ─── MF-1: durable-before-debounce ────────────────────────────

#[tokio::test]
async fn mf1_request_bumps_desired_before_worker_runs() {
    // Contract: the request path commits `desired_generation`
    // BEFORE spawning / notifying the worker, so a crash after
    // the request returns but before the worker completes still
    // leaves the durable gap visible to the next daemon startup.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    // Install a gate so the worker parks inside the hook — this
    // lets us assert desired_generation is durable BEFORE the
    // attempt finishes.
    let gate = register.install_gate();
    mgr.set_test_register_hook(register.as_hook());

    let outcome = mgr
        .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    assert_eq!(outcome.generation, 1);
    assert!(outcome.scheduled);

    // Even before the hook releases, the durable state must
    // reflect the intent.
    let state = read_state(&cas, "h");
    assert_eq!(state.desired_generation, 1);
    assert!(
        state.applied_generation == 0,
        "applied must not advance until the worker completes"
    );

    // Let the worker finish so shutdown does not race.
    gate.wait_for_entry(500).await;
    gate.release();
    mgr.shutdown(Duration::from_secs(2)).await;
}

// ─── MF-2: repo_hash dedup across aliases ─────────────────────

#[tokio::test]
async fn mf2_two_aliases_share_one_worker_and_one_attempt() {
    // Two aliases labelling the same on-disk repo must resolve
    // to a single repo_hash-owned runtime and a single attempt
    // per generation. The runtime map keys on repo_hash, so both
    // requests coalesce.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "primary", "/p", "h");
    seed_extra_alias(&cas, "secondary", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    mgr.set_test_register_hook(register.as_hook());

    let o1 = mgr
        .request_dirty_by_alias("primary".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    let o2 = mgr
        .request_dirty_by_alias("secondary".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    assert_eq!(o1.repo_hash, "h");
    assert_eq!(o2.repo_hash, "h");
    // Both requests bumped desired via the same repo row.
    assert!(o2.generation >= o1.generation);

    wait_for(
        || {
            let state = read_state(&cas, "h");
            state.applied_generation >= 1
        },
        2000,
        "worker to complete at least one attempt",
    )
    .await;

    mgr.shutdown(Duration::from_secs(2)).await;

    // At most one register call per generation observed. In the
    // common case both requests collapse into a single attempt
    // at the higher generation; even if the worker managed to
    // execute twice, no attempt may EVER run at a generation
    // already applied (Phase 1 mark_attempt_start contract).
    let calls = register.calls();
    assert!(!calls.is_empty(), "at least one attempt must run");
    let mut seen_gens = std::collections::HashSet::new();
    for c in &calls {
        assert_eq!(c.repo_hash, "h");
        assert!(seen_gens.insert(c.generation), "duplicate generation {c:?}");
    }
}

// ─── MF-3: event during in-flight attempt ─────────────────────

#[tokio::test]
async fn mf3_event_during_in_flight_attempt_re_runs_after_success() {
    // Attempt is in flight at generation N. A new request lands
    // while attempt_generation=N, bumping desired to N+1. When
    // the attempt completes, dirty gap must remain and worker
    // must run N+1 — the manager cannot declare the repo clean.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    let gate = register.install_gate();
    mgr.set_test_register_hook(register.as_hook());

    // Kick off attempt at gen=1.
    let o1 = mgr
        .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    assert_eq!(o1.generation, 1);
    gate.wait_for_entry(500).await;

    // Second request while attempt is parked in the hook.
    let o2 = mgr
        .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    assert_eq!(o2.generation, 2);
    let mid_state = read_state(&cas, "h");
    assert_eq!(mid_state.desired_generation, 2);
    assert_eq!(mid_state.attempt_generation, Some(1));

    // Release: attempt 1 completes success, worker should
    // observe desired > applied and re-loop at generation 2.
    gate.release();
    wait_for(
        || {
            let state = read_state(&cas, "h");
            state.applied_generation >= 2
        },
        2000,
        "second attempt to complete",
    )
    .await;

    mgr.shutdown(Duration::from_secs(2)).await;

    let calls = register.calls();
    let gens: Vec<i64> = calls.iter().map(|c| c.generation).collect();
    assert!(
        gens.contains(&1) && gens.contains(&2),
        "both generations must run, got {gens:?}"
    );
}

// ─── MF-4: force bypasses normal dedupe ───────────────────────

#[tokio::test]
async fn mf4_force_request_uses_forced_path() {
    // Manual reindex must invoke the register-hook with
    // `forced = true`; downstream that translates to
    // register_repo_force_analyzers_enqueue (dedupe disabled).
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    mgr.set_test_register_hook(register.as_hook());

    let outcome = mgr
        .request_force_by_alias("demo".into(), ReconcileTrigger::ManualReindex)
        .await
        .unwrap();
    assert!(outcome.forced);
    wait_for(
        || {
            let state = read_state(&cas, "h");
            state.applied_generation >= 1
        },
        2000,
        "forced attempt to complete",
    )
    .await;

    mgr.shutdown(Duration::from_secs(2)).await;
    let calls = register.calls();
    assert!(!calls.is_empty());
    assert!(
        calls.iter().any(|c| c.forced),
        "at least one call must carry forced=true, got {calls:?}"
    );
    let post = read_state(&cas, "h");
    assert!(post.force_generation >= 1);
}

// ─── MF-5: failure preserves gap and retries ──────────────────

#[tokio::test]
async fn mf5_failure_preserves_gap_and_bumps_failure_counter() {
    // Register hook fails once. Worker must:
    //   1. mark_attempt_failure with the error text
    //   2. leave applied_generation unchanged (durable gap
    //      preserved)
    //   3. bump consecutive_failures
    //   4. set next_retry_at_ns
    // A subsequent request after the retry deadline runs a
    // fresh attempt at the same generation.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    register.set_fail_next("register EMFILE");
    mgr.set_test_register_hook(register.as_hook());

    let _ = mgr
        .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();

    wait_for(
        || {
            let state = read_state(&cas, "h");
            state.consecutive_failures >= 1
        },
        2000,
        "failure to be recorded",
    )
    .await;
    let state = read_state(&cas, "h");
    assert_eq!(state.applied_generation, 0, "applied must not advance");
    assert_eq!(state.desired_generation, 1);
    assert!(state.attempt_generation.is_none());
    assert!(state.next_retry_at_ns.is_some());
    assert!(
        state
            .last_error
            .as_deref()
            .is_some_and(|s| s.contains("register EMFILE"))
    );

    mgr.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn incomplete_full_scan_preserves_durable_generation_gap() {
    let (repo, _commit) = init_repo(&[("src/lib.rs", "pub fn indexed() {}\n")]);
    std::fs::write(repo.path().join(".gitignore"), [0xff]).unwrap();
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", &repo.path().to_string_lossy(), "scan-failure");
    cas_store::open(&cas.store_db_path("scan-failure")).unwrap();
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock);

    mgr.request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    wait_for(
        || read_state(&cas, "scan-failure").consecutive_failures >= 1,
        2000,
        "incomplete scan failure to be recorded",
    )
    .await;

    let state = read_state(&cas, "scan-failure");
    assert_eq!(state.desired_generation, 1);
    assert_eq!(state.applied_generation, 0);
    assert!(state.attempt_generation.is_none());
    assert!(
        state
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("scan:"))
    );
    assert!(state.next_retry_at_ns.is_some());
    mgr.shutdown(Duration::from_secs(2)).await;
}

// ─── MF-6: interrupted attempt recovery ───────────────────────

#[tokio::test]
async fn mf6_recover_interrupted_attempts_waits_for_startup_prime_before_wake() {
    // Seed a state row with attempt_generation set (simulating a
    // crash mid-attempt). Recovery clears it but must not wake a worker until
    // the caller completes the watcher-arm barrier and startup priming.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    // Simulate crashed state.
    {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::increment_desired_generation(&tx, "h", 1_000_000).unwrap();
        cas_registry::mark_attempt_start(&tx, "h", 1, 2_000_000).unwrap();
        tx.commit().unwrap();
    }

    let clock = ManualClock::new(3_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    mgr.set_test_register_hook(register.as_hook());

    let hashes = mgr
        .recover_interrupted_attempts_without_wake()
        .await
        .unwrap();
    assert_eq!(hashes, vec!["h".to_string()]);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        register.call_count(),
        0,
        "recovery must not wake before arm"
    );

    let outcome = mgr.prime_startup_reconcile(hashes).await.unwrap();
    assert_eq!(outcome.recovered, vec!["h".to_string()]);
    assert_eq!(outcome.primed, vec![("h".to_string(), 2)]);

    wait_for(
        || {
            let state = read_state(&cas, "h");
            state.applied_generation >= 2
        },
        2000,
        "recovery-driven attempt to complete",
    )
    .await;

    mgr.shutdown(Duration::from_secs(2)).await;
    let state = read_state(&cas, "h");
    assert!(state.attempt_generation.is_none());
    // last_error carries the interrupted annotation.
    let index = cas_registry::open(&cas.index_db_path()).unwrap();
    let s = cas_registry::get_reconcile_state(&index, "h")
        .unwrap()
        .unwrap();
    // Success cleared the annotated error.
    assert!(s.last_error.is_none() || s.last_error.as_deref() == Some(""));
}

#[tokio::test]
async fn periodic_scheduler_delays_first_tick_and_stops_on_shutdown() {
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::increment_desired_generation(&tx, "h", 1).unwrap();
        cas_registry::mark_attempt_start(&tx, "h", 1, 2).unwrap();
        cas_registry::mark_attempt_success(&tx, "h", 1, 100).unwrap();
        tx.commit().unwrap();
    }
    let clock = ManualClock::new(1_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    mgr.set_test_register_hook(register.as_hook());
    mgr.start_periodic_reconcile(PeriodicReconcilePolicy {
        poll_interval: Duration::from_millis(100),
        max_clean_age: Duration::from_nanos(1),
    })
    .unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(register.call_count(), 0, "the first tick must be delayed");
    wait_for(|| register.call_count() == 1, 1_000, "periodic reconcile").await;
    wait_for(
        || read_state(&cas, "h").applied_generation == 2,
        1_000,
        "periodic generation apply",
    )
    .await;

    mgr.shutdown(Duration::from_secs(1)).await;
    let generation = read_state(&cas, "h").desired_generation;
    clock.advance(10_000);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(read_state(&cas, "h").desired_generation, generation);
}

#[tokio::test]
async fn periodic_scheduler_shutdown_is_observed_before_first_poll() {
    let (_t, cas) = fresh_cas();
    let mgr = build_manager(cas, ManualClock::new(1_000));
    mgr.start_periodic_reconcile(PeriodicReconcilePolicy {
        poll_interval: Duration::from_secs(60),
        max_clean_age: Duration::from_secs(60),
    })
    .unwrap();

    timeout(
        Duration::from_millis(250),
        mgr.shutdown(Duration::from_secs(1)),
    )
    .await
    .expect("periodic shutdown permit must wake a task before its first poll");
}

#[tokio::test]
async fn periodic_cycle_does_not_stack_on_a_dirty_repository() {
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::increment_desired_generation(&tx, "h", 1).unwrap();
        tx.commit().unwrap();
    }
    let clock = ManualClock::new(1_000);
    let mgr = build_manager(cas.clone(), clock);
    let register = FakeRegister::new();
    mgr.set_test_register_hook(register.as_hook());

    mgr.run_periodic_cycle(Duration::from_nanos(1)).await;

    assert_eq!(read_state(&cas, "h").desired_generation, 1);
    assert_eq!(register.call_count(), 0);
    mgr.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn periodic_cycle_skips_registering_owner_until_publication() {
    let root = tempfile::tempdir().unwrap();
    let (_t, cas) = fresh_cas();
    let lifecycle = RepoLifecycleManager::new(cas.clone());
    let permit = lifecycle
        .begin_registration("h".into(), root.path().to_path_buf(), 1)
        .unwrap();
    let mgr = RepoReconcileManager::new_with_lifecycle(cas.clone(), None, lifecycle.clone());
    let register = FakeRegister::new();
    mgr.set_test_register_hook(register.as_hook());

    mgr.run_periodic_cycle(Duration::from_nanos(1)).await;
    assert_eq!(read_state(&cas, "h").desired_generation, 0);
    assert_eq!(register.call_count(), 0);

    lifecycle
        .publish_registration(permit, "demo", None, 2, RegistrationReconcilePolicy::None)
        .unwrap();
    mgr.run_periodic_cycle(Duration::from_nanos(1)).await;
    wait_for(
        || register.call_count() == 1,
        1_000,
        "active periodic attempt",
    )
    .await;

    mgr.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn periodic_cycle_continues_after_one_repository_attempt_fails() {
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "a", "/a", "a");
    seed_repo(&cas, "b", "/b", "b");
    let clock = ManualClock::new(1_000);
    let mgr = build_manager(cas.clone(), clock);
    let register = FakeRegister::new();
    register.set_fail_next("first repo failed");
    mgr.set_test_register_hook(register.as_hook());

    mgr.run_periodic_cycle(Duration::from_nanos(1)).await;
    wait_for(
        || register.call_count() >= 2,
        1_000,
        "both periodic repository attempts",
    )
    .await;

    assert_eq!(read_state(&cas, "a").desired_generation, 1);
    assert_eq!(read_state(&cas, "b").desired_generation, 1);
    mgr.shutdown(Duration::from_secs(1)).await;
}

// ─── MF-7: watcher lifecycle persistence ──────────────────────

#[tokio::test]
async fn mf7_watcher_state_persists_and_rejects_missing_repo() {
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());

    mgr.set_watcher_state_by_repo_hash("h".into(), cas_registry::WatcherState::Active, None)
        .await
        .unwrap();
    let s = read_state(&cas, "h");
    assert_eq!(s.watcher_state, cas_registry::WatcherState::Active);
    assert!(s.watcher_error.is_none());

    mgr.set_watcher_state_by_repo_hash(
        "h".into(),
        cas_registry::WatcherState::Failed,
        Some("git open failed".into()),
    )
    .await
    .unwrap();
    let s = read_state(&cas, "h");
    assert_eq!(s.watcher_state, cas_registry::WatcherState::Failed);
    assert_eq!(s.watcher_error.as_deref(), Some("git open failed"));

    // Missing repo → Error::Internal (Phase 1 affected-rows == 1
    // contract propagates through the async wrapper).
    let err = mgr
        .set_watcher_state_by_repo_hash("ghost".into(), cas_registry::WatcherState::Active, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("transition rejected"));
}

// ─── MF-8: watcher path always goes through manager ───────────

#[tokio::test]
async fn mf8_watcher_request_bumps_generation_before_register_runs() {
    // The watcher path is `dispatch_events → reconcile
    // .request_dirty_by_repo_hash`. Assert that the durable
    // generation bump precedes the register call: the hook only
    // runs after the request has committed desired_generation.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    let gate = register.install_gate();
    mgr.set_test_register_hook(register.as_hook());

    let outcome = mgr
        .request_dirty_by_repo_hash("h".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    assert_eq!(outcome.generation, 1);
    // Durable state committed at this point — even if the
    // process died before gate.wait_for_entry returned, the
    // next startup would observe desired>applied.
    let mid = read_state(&cas, "h");
    assert_eq!(mid.desired_generation, 1);

    gate.wait_for_entry(500).await;
    // Only NOW is the hook running; the desired bump was
    // durable strictly before the register call.
    assert_eq!(register.call_count(), 1);
    gate.release();
    mgr.shutdown(Duration::from_secs(2)).await;
}

// ─── MF-9: concurrent wake race single attempt ────────────────

#[tokio::test]
async fn mf9_concurrent_wake_race_yields_at_most_one_attempt_per_generation() {
    // Two concurrent dirty requests race the wake_or_spawn
    // gate. Phase 1's mark_attempt_start affected-rows==1 plus
    // the runtime worker_running flag guarantee at most one
    // attempt per generation. A racing request that loses the
    // mutex is a no-op wake.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    let gate = register.install_gate();
    mgr.set_test_register_hook(register.as_hook());

    let mgr_a = mgr.clone();
    let mgr_b = mgr.clone();
    let (a, b) = tokio::join!(
        async move {
            mgr_a
                .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
                .await
                .unwrap()
        },
        async move {
            mgr_b
                .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
                .await
                .unwrap()
        }
    );
    assert_eq!(a.repo_hash, "h");
    assert_eq!(b.repo_hash, "h");

    gate.wait_for_entry(500).await;
    // Exactly one hook entry — the loser did not spawn a second
    // worker.
    assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
    gate.release();

    wait_for(
        || {
            let state = read_state(&cas, "h");
            state.applied_generation >= state.desired_generation
        },
        2000,
        "worker to drain",
    )
    .await;
    mgr.shutdown(Duration::from_secs(2)).await;

    // Every recorded call carries a distinct generation.
    let calls = register.calls();
    let gens: Vec<i64> = calls.iter().map(|c| c.generation).collect();
    let unique: std::collections::HashSet<_> = gens.iter().copied().collect();
    assert_eq!(gens.len(), unique.len(), "duplicate attempt per generation");
}

// ─── MF-10: repository deleted while scheduled ────────────────

#[tokio::test]
async fn mf10_repository_deleted_before_worker_runs_fails_cleanly() {
    // Schedule dirty request, delete the repo row before the
    // worker starts, expect the worker to exit cleanly without
    // recreating any rows. Because we drop the row after the
    // request has committed desired_generation, the FK cascade
    // wipes the state row too; the worker's next load_state
    // returns None and it exits.
    let (_t, cas) = fresh_cas();
    seed_repo(&cas, "demo", "/p", "h");
    let clock = ManualClock::new(1_000_000);
    let mgr = build_manager(cas.clone(), clock.clone());
    let register = FakeRegister::new();
    let gate = register.install_gate();
    mgr.set_test_register_hook(register.as_hook());

    // Kick a request, park in hook so we can delete the repo
    // between "attempt started on gen 1" and "hook returns".
    let _ = mgr
        .request_dirty_by_alias("demo".into(), ReconcileTrigger::WatchEvent)
        .await
        .unwrap();
    gate.wait_for_entry(500).await;

    // Delete the repository row now — because attempt_generation
    // is set, the next mark_attempt_success in Phase C will hit
    // FK cascade wiping the state row (via delete_repository).
    // The worker's finalize step will see 0-affected-rows and
    // surface Error::Internal, but must NOT panic and must NOT
    // recreate any rows.
    {
        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::delete_repository(&tx, "h").unwrap();
        tx.commit().unwrap();
    }
    gate.release();

    // Give the worker a moment to observe the deletion.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The repositories table is now empty, and no rows have
    // been re-created.
    let index = cas_registry::open(&cas.index_db_path()).unwrap();
    assert!(cas_registry::list_repositories(&index).unwrap().is_empty());
    assert!(
        cas_registry::get_reconcile_state(&index, "h")
            .unwrap()
            .is_none()
    );

    // Shutdown must complete promptly and not deadlock.
    timeout(Duration::from_secs(2), mgr.shutdown(Duration::from_secs(2)))
        .await
        .expect("shutdown must not hang after repository deletion");
}
