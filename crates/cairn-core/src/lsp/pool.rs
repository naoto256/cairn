//! Long-lived LSP client pool for workspace analyzers.
//!
//! The pool caps the number of live child processes at a hard
//! capacity (default 16, override via `CAIRN_LSP_POOL_MAX_ENTRIES`,
//! range `1..=64`; invalid/0 → default; >64 → clamp + warn), and
//! acquires each entry under a RAII lease that prevents eviction
//! while in use. Ready entries idle for 10 minutes are swept every
//! 60 seconds; `CAIRN_LSP_IDLE_TTL_SECS` overrides the TTL and `0`
//! disables time-based sweeping. Full lifecycle contract:
//!
//! - Acquire: existing Ready key → bump lease + LRU; existing
//!   Evicting key → [`Error::PoolDraining`] (same-key acquire may
//!   not join an in-flight eviction); new key + slot free →
//!   insert; full + a Ready idle victim → mark victim `Evicting`
//!   (record stays in registry so it still counts toward capacity)
//!   → shutdown outside the registry lock → on
//!   termination-proven completion (clean `Ok(())` OR
//!   termination-proven `Err`) remove the placeholder and retry;
//!   full + no Ready idle victim → [`Error::PoolAtCapacity`]. A
//!   termination-unproven shutdown keeps the placeholder AND
//!   poisons the pool globally — no other key can spawn a
//!   replacement while a possibly-live orphan may still hold
//!   resources.
//! - Idle sweep: a wall-clock timestamp is refreshed on acquire and
//!   lease release. Expired Ready entries with no active leases use
//!   the same `Evicting` reservation and fail-closed termination
//!   handling as capacity LRU.
//! - Force-shutdown: transitions `Running → Draining`, rejects new
//!   acquisitions, iterates cleanup with a per-entry timeout, and
//!   transitions back to `Running` on complete success; if any entry
//!   cleanup times out, the pool becomes `Poisoned` and permanently
//!   rejects new acquisitions until the daemon restarts (the daemon
//!   cannot silently spawn a replacement child alongside a
//!   possibly-still-live orphan).
//! - Final shutdown transitions to `Stopped` and rejects new
//!   acquisitions.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{debug, warn};

use super::client::LspProcessControl;
use super::{Error, LspClient, Position, Result, Url};

const DEFAULT_POOL_CAPACITY: usize = 16;
const MAX_POOL_CAPACITY: usize = 64;
const POOL_CAPACITY_ENV: &str = "CAIRN_LSP_POOL_MAX_ENTRIES";
const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(10 * 60);
const IDLE_TTL_ENV: &str = "CAIRN_LSP_IDLE_TTL_SECS";
const IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

type ClientWork<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

/// Registry key for one long-lived LSP server process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub canonical_repo_root: PathBuf,
    pub language: String,
    pub analyzer_id: String,
    pub binary: PathBuf,
    pub config_hash: String,
}

impl PoolKey {
    /// Build a key from the repo root and launch configuration.
    ///
    /// # Errors
    /// Returns an LSP protocol error when the repo root cannot be
    /// canonicalized.
    pub fn lsp(
        language: &str,
        repo_root: &Path,
        analyzer_id: &str,
        binary: &Path,
        config_hash: &str,
    ) -> Result<Self> {
        let canonical_repo_root = std::fs::canonicalize(repo_root).map_err(|e| {
            super::Error::Protocol(format!("canonicalize {}: {e}", repo_root.display()))
        })?;
        Ok(Self {
            canonical_repo_root,
            language: language.to_string(),
            analyzer_id: analyzer_id.to_string(),
            binary: binary.to_path_buf(),
            config_hash: config_hash.to_string(),
        })
    }
}

/// Strategy used to verify an LSP binary before spawning it.
#[derive(Debug, Clone)]
pub enum AvailabilityStrategy {
    /// `<binary> --version` returns exit 0.
    VersionFlag,
    /// `<binary> version` returns exit 0.
    VersionNoFlag,
    /// Path resolves to an executable file.
    PathExistsExecutable,
}

/// Strategy used to decide when an initialized LSP is ready for work.
#[derive(Debug, Clone)]
pub enum ReadinessStrategy {
    /// Wait for `$/progress` workspace-load quiescence.
    ProgressQuiescence { timeout: Duration },
    /// The initialize response is the readiness gate.
    InitializeResponseOnly,
}

/// Launch and readiness settings for a pooled LSP client.
#[derive(Debug, Clone)]
pub struct LspSpawnSpec {
    pub binary: PathBuf,
    pub workspace_root: PathBuf,
    pub config_hash: String,
    pub request_timeout: Duration,
    pub availability: AvailabilityStrategy,
    pub readiness: ReadinessStrategy,
    pub language_id: &'static str,
    pub launch_args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub initialization_options: Value,
}

/// Borrowed pooled client plus document synchronization state.
pub struct PooledLsp<'a> {
    client: &'a LspClient,
    opened_documents: &'a mut HashMap<String, i32>,
    language_id: &'static str,
}

impl PooledLsp<'_> {
    /// Open or fully replace a document.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP client.
    pub async fn sync_document(&mut self, uri: &Url, text: &str) -> Result<()> {
        if let Some(version) = self.opened_documents.get_mut(uri.as_str()) {
            *version = version.saturating_add(1);
            self.client.did_change(uri, *version, text).await
        } else {
            self.opened_documents.insert(uri.as_str().to_string(), 1);
            self.client.did_open(uri, self.language_id, 1, text).await
        }
    }

    /// Close a synced document and clear its local version state.
    ///
    /// # Errors
    /// Returns protocol/server errors from the underlying LSP client.
    pub async fn close_document(&mut self, uri: &Url) -> Result<()> {
        self.opened_documents.remove(uri.as_str());
        self.client.did_close(uri).await
    }

    /// Resolve the definition at `uri` + `position`.
    ///
    /// # Errors
    /// Returns timeout/protocol/server errors from the underlying LSP
    /// request.
    pub async fn definition(&self, uri: &Url, position: Position) -> Result<Vec<super::Location>> {
        self.client.definition(uri, position).await
    }
}

/// Daemon-scoped pool of long-lived LSP clients with a hard
/// capacity, LRU eviction, and fail-closed drain/poison states.
pub struct LspClientPool {
    runtime: Runtime,
    registry: Arc<StdMutex<PoolRegistry>>,
    capacity: NonZeroUsize,
    _idle_sweeper: Option<JoinHandle<()>>,
}

struct PoolRegistry {
    mode: PoolMode,
    entries: HashMap<PoolKey, PoolRecord>,
    /// Monotonic counter; every acquire bumps it and stamps the
    /// record's `last_used`. Overflow at `u64::MAX` is a
    /// (theoretical) fail-closed error rather than a wrap.
    access_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolMode {
    /// Normal operation. Acquire, evict, insert all permitted.
    Running,
    /// A `force_shutdown_all` is in flight. New acquires reject
    /// with `PoolDraining`; the drain iterator has already taken
    /// the entries out of the registry.
    Draining,
    /// A prior `force_shutdown_all` could not prove that at least
    /// one child terminated. All future acquires reject.
    Poisoned,
    /// Daemon-level final shutdown. All future acquires reject.
    Stopped,
}

struct PoolRecord {
    entry: Arc<PoolEntry>,
    active_leases: usize,
    last_used: u64,
    last_used_at: Instant,
    state: RecordState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordState {
    /// Normal entry — acquire may lease it, LRU may pick it as a
    /// victim when idle.
    Ready,
    /// LRU eviction reserved this record as a victim. It still
    /// counts toward `capacity`, so a concurrent `acquire` cannot
    /// spawn a replacement child in its place, but it is not
    /// leasable (same-key acquires reject with `PoolDraining` and
    /// LRU never picks another Evicting record). The eviction
    /// caller holds the `Arc<PoolEntry>` and runs `entry.shutdown()`
    /// outside the registry lock. On any **termination-proven
    /// completion** — clean `Ok(())` or an error whose
    /// `is_termination_unproven` is false — the record is removed
    /// from the registry so its capacity slot is freed. Only a
    /// termination-unproven error keeps the record in place AND
    /// poisons the pool globally, so the possibly-live orphan
    /// cannot be forgotten and no replacement child can be spawned
    /// anywhere until the daemon restarts.
    Evicting,
}

/// RAII lease held for the duration of one `with_lsp` call. The
/// `Arc<PoolEntry>` inside is stable across concurrent acquires of
/// the same key (they share this record). On drop the record's
/// `active_leases` is decremented, but only if the record we
/// registered against is still the one the registry holds — a
/// force-shutdown that evicted us and let a replacement be
/// installed must not have its lease counter mutated by our drop.
struct PoolLease {
    key: PoolKey,
    entry: Arc<PoolEntry>,
    registry: Arc<StdMutex<PoolRegistry>>,
}

impl std::fmt::Debug for PoolLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolLease")
            .field("key.language", &self.key.language)
            .field("key.analyzer_id", &self.key.analyzer_id)
            .finish_non_exhaustive()
    }
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        let mut reg = match self.registry.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(record) = reg.entries.get_mut(&self.key)
            && Arc::ptr_eq(&record.entry, &self.entry)
        {
            // Underflow is a lease-accounting bug (double-release
            // of the same lease), not a benign clamp: silently
            // saturating would let the counter drift below zero
            // and allow an incorrectly-tracked "idle" entry to be
            // evicted while its Arc is still in use. Fail-closed
            // by poisoning the pool so no further acquisitions
            // can proceed until the daemon restarts.
            match record.active_leases.checked_sub(1) {
                Some(new) => {
                    record.active_leases = new;
                    record.last_used_at = Instant::now();
                }
                None => {
                    warn!(
                        language = %self.key.language,
                        analyzer = %self.key.analyzer_id,
                        "lsp pool: lease counter underflow on drop; poisoning pool"
                    );
                    reg.mode = PoolMode::Poisoned;
                }
            }
        }
    }
}

/// Resolve `CAIRN_LSP_POOL_MAX_ENTRIES` to a capacity. Pure function
/// over the raw env value so tests can pin the parse contract
/// without mutating the process-global env. The contract:
///
/// | input                     | result                     |
/// |---------------------------|----------------------------|
/// | `None` (env unset)        | default (16)               |
/// | empty / whitespace-only   | default                    |
/// | non-numeric               | default + warn             |
/// | negative (`-N`)           | default + warn             |
/// | `0`                       | default + warn             |
/// | `1..=MAX_POOL_CAPACITY`   | value                      |
/// | positive numeric > MAX (including strings that overflow `u128`) | clamp to MAX + warn |
///
/// Values that would overflow `i64` (e.g. `"9" * 40`) are treated
/// as "positive numeric > MAX" and clamped — they do NOT fall into
/// the invalid-non-numeric bucket. This means "invalid string" and
/// "positive overflow" are semantically distinguished by whether
/// the input is all ASCII digits.
fn capacity_from_env_value(raw: Option<&str>) -> NonZeroUsize {
    let default = NonZeroUsize::new(DEFAULT_POOL_CAPACITY).expect("compile-time constant > 0");
    let max = NonZeroUsize::new(MAX_POOL_CAPACITY).expect("compile-time constant > 0");
    let Some(raw) = raw else {
        return default;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return default;
    }
    // Reject any leading `-` explicitly so `-5` doesn't parse as a
    // valid non-negative via later fall-through.
    if let Some(rest) = trimmed.strip_prefix('-') {
        // `-` alone or `-<non-digit>` → non-numeric; `-<digits>` →
        // negative. Both are user errors that fall back to default.
        let label = if rest.chars().all(|c| c.is_ascii_digit()) && !rest.is_empty() {
            "lsp pool capacity must be > 0; using default"
        } else {
            "invalid lsp pool capacity; using default"
        };
        warn!(env = POOL_CAPACITY_ENV, value = %raw, default = DEFAULT_POOL_CAPACITY, "{}", label);
        return default;
    }
    // Non-negative. Parse as `u128` so very large positive values
    // clamp to MAX rather than falling into the invalid bucket.
    // If the string is all ASCII digits but overflows u128
    // (>~10^38), still treat as "positive numeric > MAX" and clamp.
    let all_digits = trimmed.chars().all(|c| c.is_ascii_digit());
    let parsed: u128 = match trimmed.parse::<u128>() {
        Ok(v) => v,
        Err(_) if all_digits => {
            warn!(
                env = POOL_CAPACITY_ENV,
                value = %raw,
                max = MAX_POOL_CAPACITY,
                "lsp pool capacity exceeds max; clamping"
            );
            return max;
        }
        Err(_) => {
            warn!(
                env = POOL_CAPACITY_ENV,
                value = %raw,
                default = DEFAULT_POOL_CAPACITY,
                "invalid lsp pool capacity; using default"
            );
            return default;
        }
    };
    if parsed == 0 {
        warn!(
            env = POOL_CAPACITY_ENV,
            value = %raw,
            default = DEFAULT_POOL_CAPACITY,
            "lsp pool capacity must be > 0; using default"
        );
        return default;
    }
    if parsed > MAX_POOL_CAPACITY as u128 {
        warn!(
            env = POOL_CAPACITY_ENV,
            value = %raw,
            max = MAX_POOL_CAPACITY,
            "lsp pool capacity exceeds max; clamping"
        );
        return max;
    }
    // 1..=MAX_POOL_CAPACITY: safe to cast.
    NonZeroUsize::new(parsed as usize).unwrap_or(default)
}

fn parse_capacity_env() -> NonZeroUsize {
    capacity_from_env_value(std::env::var(POOL_CAPACITY_ENV).ok().as_deref())
}

/// Resolve the idle TTL environment override. `None` means the
/// sweeper is disabled, which is the explicit contract for zero.
fn idle_ttl_from_env_value(raw: Option<&str>) -> Option<Duration> {
    let Some(raw) = raw else {
        return Some(DEFAULT_IDLE_TTL);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Some(DEFAULT_IDLE_TTL);
    }
    match trimmed.parse::<u64>() {
        Ok(0) => None,
        Ok(seconds) => Some(Duration::from_secs(seconds)),
        Err(_) => {
            warn!(
                env = IDLE_TTL_ENV,
                value = %raw,
                default_secs = DEFAULT_IDLE_TTL.as_secs(),
                "invalid lsp pool idle TTL; using default"
            );
            Some(DEFAULT_IDLE_TTL)
        }
    }
}

fn parse_idle_ttl_env() -> Option<Duration> {
    idle_ttl_from_env_value(std::env::var(IDLE_TTL_ENV).ok().as_deref())
}

/// Aggregated per-entry outcomes for `force_shutdown_all`. Split
/// into regular-error and termination-unproven-error slots so an
/// unproven signal that arrived after a regular protocol error is
/// still surfaced — the safety-critical cause must not be lost to
/// order-of-arrival.
#[derive(Debug, Default)]
struct ForceShutdownOutcome {
    /// First non-termination-proof error observed (protocol
    /// failure, etc.). Termination-proven Errs whose child WAS
    /// reaped.
    first_regular_err: Option<Error>,
    /// First termination-unproven error observed. Preserved
    /// separately so its identity survives even when a regular
    /// error came first.
    first_unproven_err: Option<Error>,
    /// Set when the outer per-entry `timeout(..)` actually fired
    /// (as opposed to `entry.shutdown()` returning an unproven
    /// error under its own steam). Drives whether the finalize
    /// generates the synthetic "outer timeout" ChildTerminationFailed.
    timed_out: bool,
}

impl ForceShutdownOutcome {
    fn termination_unproven(&self) -> bool {
        self.first_unproven_err.is_some() || self.timed_out
    }
}

fn classify_force_shutdown_results(
    results: Vec<std::result::Result<Result<()>, tokio::time::error::Elapsed>>,
    entry_timeout: Duration,
) -> ForceShutdownOutcome {
    let mut out = ForceShutdownOutcome::default();
    for outcome in results {
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if e.is_termination_unproven() {
                    if out.first_unproven_err.is_none() {
                        out.first_unproven_err = Some(e);
                    }
                } else if out.first_regular_err.is_none() {
                    out.first_regular_err = Some(e);
                }
            }
            Err(_) => {
                out.timed_out = true;
                warn!(
                    timeout_ms = entry_timeout.as_millis(),
                    "timed out shutting down stalled LSP pool entry"
                );
            }
        }
    }
    out
}

impl LspClientPool {
    /// Create an empty pool sized from the environment.
    ///
    /// # Errors
    /// Returns an LSP protocol error if the dedicated Tokio runtime
    /// cannot be created.
    pub fn new() -> Result<Self> {
        Self::with_config(parse_capacity_env(), parse_idle_ttl_env())
    }

    #[cfg(test)]
    fn with_capacity(capacity: NonZeroUsize) -> Result<Self> {
        Self::with_config(capacity, Some(DEFAULT_IDLE_TTL))
    }

    fn with_config(capacity: NonZeroUsize, idle_ttl: Option<Duration>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("cairn-lsp-pool")
            .build()
            .map_err(|e| Error::Protocol(format!("lsp pool runtime: {e}")))?;
        let registry = Arc::new(StdMutex::new(PoolRegistry {
            mode: PoolMode::Running,
            entries: HashMap::new(),
            access_seq: 0,
        }));
        let idle_sweeper = idle_ttl.map(|idle_ttl| {
            let registry = Arc::clone(&registry);
            runtime.spawn(async move {
                let mut interval = interval(IDLE_SWEEP_INTERVAL);
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                // Tokio intervals tick immediately once. Consume that
                // tick so the first real sweep occurs after 60 seconds.
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(error) = Self::sweep_idle_once(
                        &registry,
                        Instant::now(),
                        idle_ttl,
                        IDLE_SHUTDOWN_TIMEOUT,
                    )
                    .await
                    {
                        warn!(%error, "lsp pool: idle sweep failed");
                    }
                }
            })
        });
        debug!(
            capacity = capacity.get(),
            idle_ttl_secs = idle_ttl.map(|ttl| ttl.as_secs()),
            "lsp pool initialized"
        );
        Ok(Self {
            runtime,
            registry,
            capacity,
            _idle_sweeper: idle_sweeper,
        })
    }

    /// Borrow a long-lived LSP client for `key`, lazily spawning it
    /// when needed according to `spawn_spec`.
    ///
    /// # Errors
    /// - [`Error::PoolAtCapacity`] if the pool is full and no idle
    ///   entry is available to evict.
    /// - [`Error::PoolDraining`] / [`Error::PoolPoisoned`] /
    ///   [`Error::PoolStopped`] if the pool is not accepting new
    ///   acquisitions.
    /// - LSP spawn/readiness/protocol errors from the pooled client.
    pub fn with_lsp<T, F>(&self, key: PoolKey, spawn_spec: LspSpawnSpec, work: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledLsp<'a>) -> ClientWork<'a, T>,
    {
        let lease = self
            .runtime
            .block_on(async { self.acquire_lease(key).await })?;
        let entry = Arc::clone(&lease.entry);
        let result = self
            .runtime
            .block_on(async move { entry.with_lsp_client(spawn_spec, work).await });
        drop(lease);
        // Central poison propagation: any termination-unproven
        // error observed on the normal path (spawn/initialize/
        // readiness/ServerExited cleanup) must poison the pool so
        // no replacement can spawn alongside a possibly-still-live
        // orphan. Same helper the LRU eviction path uses; both
        // preserve `Stopped`.
        if let Err(ref e) = result
            && e.is_termination_unproven()
        {
            self.poison_from_unproven_cleanup(&e.to_string());
        }
        result
    }

    fn poison_from_unproven_cleanup(&self, context: &str) {
        let Ok(mut reg) = self.registry.lock() else {
            return;
        };
        match reg.mode {
            PoolMode::Stopped => {}
            _ => {
                warn!(
                    context = context,
                    "lsp pool: termination unproven; poisoning pool"
                );
                reg.mode = PoolMode::Poisoned;
            }
        }
    }

    /// Acquire a lease on `key`, evicting an idle LRU victim outside
    /// the registry lock when the pool is at capacity. If the
    /// victim's shutdown fails, the current acquisition fails with
    /// that shutdown error rather than silently spawning a
    /// replacement.
    async fn acquire_lease(&self, key: PoolKey) -> Result<PoolLease> {
        loop {
            // Under the registry lock: either satisfy the acquire
            // directly (existing Ready key or free capacity slot)
            // and return, or reserve a victim for eviction. When
            // reserving a victim we set `state = Evicting` and keep
            // the record in the registry so it still counts toward
            // `capacity` — no other thread can spawn a replacement
            // child in its slot while the victim's shutdown is in
            // flight outside the lock.
            let (victim_key, victim_entry) = {
                let mut reg = self.lock_registry()?;
                match reg.mode {
                    PoolMode::Running => {}
                    PoolMode::Draining => return Err(Error::PoolDraining),
                    PoolMode::Poisoned => return Err(Error::PoolPoisoned),
                    PoolMode::Stopped => return Err(Error::PoolStopped),
                }
                // Existing key — Ready → bump lease; Evicting →
                // reject with `PoolDraining` (same-key concurrent
                // acquire cannot join an in-flight eviction, and
                // must not spawn a replacement while the victim
                // may still hold the child).
                if let Some(record) = reg.entries.get(&key) {
                    match record.state {
                        RecordState::Evicting => return Err(Error::PoolDraining),
                        RecordState::Ready => {}
                    }
                    let bumped_seq = reg
                        .access_seq
                        .checked_add(1)
                        .ok_or_else(|| Error::Protocol("pool access_seq overflow".into()))?;
                    reg.access_seq = bumped_seq;
                    let record = reg
                        .entries
                        .get_mut(&key)
                        .expect("contains_key just checked");
                    let bumped_leases = record
                        .active_leases
                        .checked_add(1)
                        .ok_or_else(|| Error::Protocol("pool lease counter overflow".into()))?;
                    record.active_leases = bumped_leases;
                    record.last_used = bumped_seq;
                    record.last_used_at = Instant::now();
                    return Ok(PoolLease {
                        key,
                        entry: Arc::clone(&record.entry),
                        registry: Arc::clone(&self.registry),
                    });
                }
                // New key — slot available? (Evicting records also
                // count toward `entries.len()`, so a live victim
                // holds its slot until its shutdown completes.)
                if reg.entries.len() < self.capacity.get() {
                    let bumped_seq = reg
                        .access_seq
                        .checked_add(1)
                        .ok_or_else(|| Error::Protocol("pool access_seq overflow".into()))?;
                    reg.access_seq = bumped_seq;
                    let entry = Arc::new(PoolEntry::default());
                    reg.entries.insert(
                        key.clone(),
                        PoolRecord {
                            entry: Arc::clone(&entry),
                            active_leases: 1,
                            last_used: bumped_seq,
                            last_used_at: Instant::now(),
                            state: RecordState::Ready,
                        },
                    );
                    return Ok(PoolLease {
                        key,
                        entry,
                        registry: Arc::clone(&self.registry),
                    });
                }
                // Full — find an idle LRU victim in the `Ready`
                // state. `Evicting` records are skipped: their
                // eviction is already reserved by another thread.
                let victim_key = reg
                    .entries
                    .iter()
                    .filter(|(_, r)| r.state == RecordState::Ready && r.active_leases == 0)
                    .min_by_key(|(_, r)| r.last_used)
                    .map(|(k, _)| k.clone());
                let Some(victim_key) = victim_key else {
                    return Err(Error::PoolAtCapacity {
                        capacity: self.capacity.get(),
                    });
                };
                let record = reg
                    .entries
                    .get_mut(&victim_key)
                    .expect("victim key just discovered");
                record.state = RecordState::Evicting;
                let victim_entry = Arc::clone(&record.entry);
                debug!(
                    language = %victim_key.language,
                    analyzer = %victim_key.analyzer_id,
                    "lsp pool: reserving idle LRU entry as eviction victim"
                );
                (victim_key, victim_entry)
            };
            // Shutdown victim OUTSIDE the registry lock. The
            // record stays in the registry as an `Evicting`
            // placeholder throughout — capacity is preserved so no
            // concurrent acquire can spawn a replacement.
            match victim_entry.shutdown().await {
                Ok(()) => {
                    // Termination proven. Safe to remove the
                    // placeholder and loop to re-acquire; the
                    // freed slot is now available for our key
                    // (unless another thread beat us to it, in
                    // which case we retry eviction).
                    let mut reg = self.lock_registry()?;
                    reg.entries.remove(&victim_key);
                }
                Err(e) if e.is_termination_unproven() => {
                    // Cannot prove the victim's child died.
                    // Preserve the `Evicting` placeholder so a
                    // replacement can never be spawned in its
                    // slot, and poison the pool globally — the
                    // possibly-live orphan may hold stdio / port /
                    // cache locks that collide with any new
                    // spawn.
                    self.poison_from_unproven_cleanup(&format!("LRU eviction: {e}"));
                    return Err(e);
                }
                Err(e) => {
                    // Termination-proven error (e.g. graceful
                    // protocol failure but the child was reaped).
                    // Safe to remove the placeholder before
                    // returning.
                    let mut reg = self.lock_registry()?;
                    reg.entries.remove(&victim_key);
                    return Err(e);
                }
            }
        }
    }

    /// Reserve every expired idle entry under the registry lock,
    /// then stop the reserved entries concurrently outside it.
    /// Keeping each record as `Evicting` until termination is proven
    /// preserves the same no-replacement invariant as capacity LRU.
    async fn sweep_idle_once(
        registry: &Arc<StdMutex<PoolRegistry>>,
        now: Instant,
        idle_ttl: Duration,
        entry_timeout: Duration,
    ) -> Result<usize> {
        let victims = {
            let mut reg = registry
                .lock()
                .map_err(|_| Error::Protocol("lsp pool registry poisoned".into()))?;
            if reg.mode != PoolMode::Running {
                return Ok(0);
            }
            let keys = reg
                .entries
                .iter()
                .filter(|(_, record)| {
                    record.state == RecordState::Ready
                        && record.active_leases == 0
                        && now.saturating_duration_since(record.last_used_at) > idle_ttl
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            keys.into_iter()
                .map(|key| {
                    let record = reg
                        .entries
                        .get_mut(&key)
                        .expect("idle sweep key just discovered");
                    record.state = RecordState::Evicting;
                    debug!(
                        language = %key.language,
                        analyzer = %key.analyzer_id,
                        idle_secs = now.saturating_duration_since(record.last_used_at).as_secs(),
                        "lsp pool: reserving expired idle entry for sweep"
                    );
                    (key, Arc::clone(&record.entry))
                })
                .collect::<Vec<_>>()
        };

        let victim_count = victims.len();
        let outcomes =
            futures::future::join_all(victims.into_iter().map(|(key, entry)| async move {
                let result = entry.shutdown_bounded(entry_timeout).await;
                (key, entry, result)
            }))
            .await;

        let mut first_err = None;
        let mut reg = registry
            .lock()
            .map_err(|_| Error::Protocol("lsp pool registry poisoned".into()))?;
        for (key, entry, result) in outcomes {
            let still_reserved = reg.entries.get(&key).is_some_and(|record| {
                record.state == RecordState::Evicting && Arc::ptr_eq(&record.entry, &entry)
            });
            match result {
                Ok(()) => {
                    if still_reserved {
                        reg.entries.remove(&key);
                    }
                }
                Err(error) if error.is_termination_unproven() => {
                    warn!(
                        language = %key.language,
                        analyzer = %key.analyzer_id,
                        %error,
                        "lsp pool: idle sweep could not prove child termination"
                    );
                    // Final daemon shutdown owns the terminal state.
                    // Otherwise keep the placeholder and fail closed.
                    if reg.mode != PoolMode::Stopped {
                        reg.mode = PoolMode::Poisoned;
                    }
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
                Err(error) => {
                    if still_reserved {
                        reg.entries.remove(&key);
                    }
                    warn!(
                        language = %key.language,
                        analyzer = %key.analyzer_id,
                        %error,
                        "lsp pool: idle sweep completed with shutdown error"
                    );
                    if first_err.is_none() {
                        first_err = Some(error);
                    }
                }
            }
        }
        match first_err {
            Some(error) => Err(error),
            None => Ok(victim_count),
        }
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, PoolRegistry>> {
        self.registry
            .lock()
            .map_err(|_| Error::Protocol("lsp pool registry poisoned".into()))
    }

    /// Gracefully stop all live clients and mark the pool `Stopped`.
    /// Further acquisitions will return [`Error::PoolStopped`].
    ///
    /// # Errors
    /// Returns the first LSP shutdown error observed after
    /// attempting every entry.
    pub fn shutdown_all(&self) -> Result<()> {
        let entries = {
            let mut reg = self.lock_registry()?;
            reg.mode = PoolMode::Stopped;
            reg.entries
                .drain()
                .map(|(_, r)| r.entry)
                .collect::<Vec<_>>()
        };
        self.runtime.block_on(async move {
            // Shutdown entries concurrently — same rationale as
            // `force_shutdown_all` (independent children, no
            // cross-entry contention).
            let results: Vec<_> = futures::future::join_all(
                entries
                    .into_iter()
                    .map(|entry| async move { entry.shutdown().await }),
            )
            .await;
            let mut first_err: Option<Error> = None;
            for r in results {
                if let Err(e) = r
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }

    /// Permanently stop the pool and force-terminate every child through its
    /// process-control handle, without waiting for an analyzer pass to release
    /// the entry's data-plane mutex.
    ///
    /// This is the daemon-final shutdown API. Unlike
    /// [`Self::force_shutdown_all`], it never returns the pool to `Running` and
    /// never permits replacement children after cleanup begins.
    ///
    /// # Errors
    /// Returns the first cleanup error after attempting every entry. A timeout
    /// is surfaced as [`Error::ChildTerminationFailed`] because child
    /// termination could not be proven.
    pub fn shutdown_all_bounded(&self, entry_timeout: Duration) -> Result<()> {
        let entries = {
            let mut reg = self.lock_registry()?;
            reg.mode = PoolMode::Stopped;
            reg.entries
                .drain()
                .map(|(_, record)| record.entry)
                .collect::<Vec<_>>()
        };
        self.runtime.block_on(async move {
            let results = futures::future::join_all(
                entries
                    .into_iter()
                    .map(|entry| async move { entry.shutdown_bounded(entry_timeout).await }),
            )
            .await;
            results
                .into_iter()
                .find_map(Result::err)
                .map_or(Ok(()), Err)
        })
    }

    /// Evict all live clients and give each entry a bounded grace
    /// period to shut down. Used after analyzer stall detection so
    /// the next analyzer run does not inherit a wedged pool key.
    ///
    /// New acquisitions are rejected with [`Error::PoolDraining`]
    /// while this call is in flight. Mode transitions on finalize:
    ///
    /// - Every entry clean (no timeout, no termination-unproven)
    ///   → pool returns to `Running`.
    /// - Any termination-unproven signal (either an outer timeout
    ///   or an entry that returned `ChildTerminationFailed`) →
    ///   pool becomes `Poisoned` and rejects all future
    ///   acquisitions until the daemon restarts.
    /// - A concurrent `shutdown_all` transitioned the pool to
    ///   `Stopped` while we were mid-drain → we preserve
    ///   `Stopped`; the pool is stopped, not poisoned.
    ///
    /// # Errors
    /// - [`Error::PoolPoisoned`] when the finalize mode is
    ///   `Poisoned` (whether via our own local outcome or a
    ///   concurrent path).
    /// - `ChildTerminationFailed` when the mode is `Stopped` /
    ///   `Running` but a termination-unproven signal was
    ///   observed. If a non-unproven `first_err` was also
    ///   present, the two are combined into an
    ///   `OperationWithCleanupFailure`.
    /// - The `first_err` alone (no cleanup err) when the only
    ///   signal was a clean protocol failure whose child was
    ///   still reaped.
    pub fn force_shutdown_all(&self, entry_timeout: Duration) -> Result<()> {
        let entries = {
            let mut reg = self.lock_registry()?;
            match reg.mode {
                PoolMode::Running => reg.mode = PoolMode::Draining,
                PoolMode::Draining => return Err(Error::PoolDraining),
                PoolMode::Poisoned => return Err(Error::PoolPoisoned),
                PoolMode::Stopped => return Err(Error::PoolStopped),
            }
            reg.entries
                .drain()
                .map(|(_, r)| r.entry)
                .collect::<Vec<_>>()
        };
        debug!(entries = entries.len(), "lsp pool: force-shutdown begin");
        // Cleanup entries CONCURRENTLY: the wall-clock cost of a
        // force-shutdown is bounded by ~one entry timeout rather
        // than `capacity × entry_timeout`. Each entry owns its own
        // child process; there is no cross-entry contention on
        // shutdown.
        // Classify each entry's outcome into three orthogonal
        // signals so we don't conflate "the actual shutdown
        // returned a termination-unproven error" with "the outer
        // per-entry timeout fired" — those are DIFFERENT causes
        // of `termination_unproven` and only the second one
        // warrants a synthetic "timeout" error message.
        let outcome = self.runtime.block_on(async move {
            let results: Vec<_> = futures::future::join_all(
                entries
                    .into_iter()
                    .map(|entry| async move { timeout(entry_timeout, entry.shutdown()).await }),
            )
            .await;
            classify_force_shutdown_results(results, entry_timeout)
        });
        let termination_unproven = outcome.termination_unproven();
        let ForceShutdownOutcome {
            first_regular_err,
            first_unproven_err,
            timed_out,
        } = outcome;
        // Finalize mode under lock. Preserve BOTH `Stopped` and
        // `Poisoned`: a concurrent `shutdown_all` may have raced
        // ahead of us (Stopped wins), OR a concurrent normal-path
        // cleanup (`with_lsp` central exit, LRU eviction) may have
        // observed a termination-unproven signal on a DIFFERENT
        // path while our own drain was clean (Poisoned must not
        // regress to Running). Only when the mode is still
        // `Draining` — which means nobody else transitioned it
        // during our drain — do we apply our local outcome.
        let final_mode = {
            let mut reg = self.lock_registry()?;
            match reg.mode {
                PoolMode::Stopped => PoolMode::Stopped,
                PoolMode::Poisoned => PoolMode::Poisoned,
                PoolMode::Draining => {
                    // Only the `termination_unproven` signal (from
                    // either an actual unproven error or an outer
                    // timeout) drives the mode transition. A
                    // clean `first_err` (e.g. protocol failure on
                    // an entry whose child was cleanly reaped) is
                    // NOT a safety hazard for future spawns.
                    reg.mode = if termination_unproven {
                        PoolMode::Poisoned
                    } else {
                        PoolMode::Running
                    };
                    reg.mode
                }
                PoolMode::Running => {
                    // Should not happen — we set Draining above and
                    // no other path writes Running. Preserve
                    // whatever the current state is defensively.
                    reg.mode
                }
            }
        };
        // Build the caller-visible error in two layers so that
        // the safety-critical termination-unproven cause is never
        // dropped by ordering.
        //
        // 1. `safety_cause` combines the first unproven error
        //    (from `entry.shutdown()`) with the synthetic outer
        //    timeout error (only fabricated when the outer
        //    `timeout(..)` actually fired).
        // 2. `combined` composes the first regular error (protocol
        //    etc.) with the safety cause. The safety cause is
        //    placed in the `cleanup` slot of
        //    `OperationWithCleanupFailure` so `is_termination_unproven`
        //    recursion still fires on the top-level err.
        let timeout_err = if timed_out {
            Some(Error::ChildTerminationFailed(
                "force-shutdown outer timeout — child termination could not be proven".into(),
            ))
        } else {
            None
        };
        let safety_cause = match (first_unproven_err, timeout_err) {
            (None, None) => None,
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(unproven), Some(timeout)) => Some(Error::OperationWithCleanupFailure {
                original: Box::new(unproven),
                cleanup: Box::new(timeout),
            }),
        };
        let combined = match (first_regular_err, safety_cause) {
            (None, None) => None,
            (Some(e), None) | (None, Some(e)) => Some(e),
            (Some(orig), Some(safety)) => Some(Error::OperationWithCleanupFailure {
                original: Box::new(orig),
                cleanup: Box::new(safety),
            }),
        };
        if final_mode == PoolMode::Poisoned {
            // Log the actual cause on the way out so operators
            // debugging a poisoned pool have the evidence
            // (previously we swallowed it).
            if let Some(e) = combined.as_ref() {
                warn!(
                    error = %e,
                    "lsp pool: force-shutdown finalize observed Poisoned mode; pool poisoned until daemon restart"
                );
            } else {
                warn!(
                    "lsp pool: force-shutdown finalize observed Poisoned mode; pool poisoned until daemon restart"
                );
            }
            return Err(Error::PoolPoisoned);
        }
        if final_mode == PoolMode::Stopped {
            // A concurrent `shutdown_all` won on the mode
            // transition. We still surface any accumulated
            // termination-unproven / first_err so the caller can
            // see the evidence.
            return match combined {
                Some(e) => Err(e),
                None => Ok(()),
            };
        }
        debug!("lsp pool: force-shutdown complete; pool running");
        match combined {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.registry.lock().unwrap().entries.len()
    }

    #[cfg(test)]
    fn mode(&self) -> PoolMode {
        self.registry.lock().unwrap().mode
    }

    #[cfg(test)]
    fn active_leases(&self, key: &PoolKey) -> Option<usize> {
        self.registry
            .lock()
            .unwrap()
            .entries
            .get(key)
            .map(|r| r.active_leases)
    }
}

#[derive(Default)]
struct PoolEntry {
    state: Mutex<PoolEntryState>,
    /// Serializes normal, idle-sweep, and final shutdown paths for
    /// this entry. Final shutdown may race a reserved eviction, but
    /// the child process must never receive concurrent stop attempts.
    shutdown_gate: Mutex<()>,
    /// Child-process control is deliberately independent from `state`. Normal
    /// work holds `state` across an analyzer pass, but final daemon shutdown
    /// must still be able to kill and reap the child to unblock that pass.
    process_control: StdMutex<ProcessControlSlot>,
}

#[derive(Default)]
struct ProcessControlSlot {
    stopping: bool,
    control: Option<LspProcessControl>,
}

#[derive(Default)]
struct PoolEntryState {
    client: Option<LspClient>,
    opened_documents: HashMap<String, i32>,
}

async fn check_lsp_available(
    binary_path: &Path,
    strategy: &AvailabilityStrategy,
    request_timeout: Duration,
) -> Result<()> {
    match strategy {
        AvailabilityStrategy::VersionFlag | AvailabilityStrategy::VersionNoFlag => {
            // Single source: the pool and `LspClient` both route
            // through `client::probe_binary`. Diverging the two
            // probe implementations previously produced silent
            // orphan children on one path but not the other, so
            // this consumer just dispatches the correct args.
            super::client::probe_binary(
                binary_path,
                availability_probe_args(strategy).unwrap_or(&[]),
                request_timeout,
            )
            .await
        }
        AvailabilityStrategy::PathExistsExecutable => check_path_exists_executable(binary_path),
    }
}

fn availability_probe_args(strategy: &AvailabilityStrategy) -> Option<&'static [&'static str]> {
    match strategy {
        AvailabilityStrategy::VersionFlag => Some(&["--version"]),
        AvailabilityStrategy::VersionNoFlag => Some(&["version"]),
        AvailabilityStrategy::PathExistsExecutable => None,
    }
}

fn check_path_exists_executable(binary_path: &Path) -> Result<()> {
    let resolved = resolve_executable(binary_path)
        .ok_or_else(|| super::Error::BinaryMissing(binary_path.to_path_buf()))?;
    if is_executable(&resolved) {
        Ok(())
    } else {
        Err(super::Error::BinaryMissing(binary_path.to_path_buf()))
    }
}

fn resolve_executable(binary_path: &Path) -> Option<PathBuf> {
    if has_path_separator(binary_path) {
        return binary_path.exists().then(|| binary_path.to_path_buf());
    }
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join(binary_path))
            .find(|candidate| candidate.exists())
    })
}

fn has_path_separator(path: &Path) -> bool {
    // Paths with separators are explicit filesystem references; only
    // bare names should be resolved through PATH.
    path.components().count() > 1
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

impl PoolEntry {
    async fn with_lsp_client<T, F>(&self, spec: LspSpawnSpec, work: F) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledLsp<'a>) -> ClientWork<'a, T>,
    {
        let mut state = self.state.lock().await;
        if state.client.is_none() {
            check_lsp_available(&spec.binary, &spec.availability, spec.request_timeout).await?;
            let client = LspClient::configured(
                &spec.binary,
                spec.launch_args.clone(),
                spec.env.clone(),
                &spec.workspace_root,
                spec.initialization_options.clone(),
                spec.request_timeout,
            );
            self.install_process_control(client.process_control())?;
            if let Err(err) = client.start_process().await {
                self.clear_process_control()?;
                return Err(err);
            }
            // Readiness check runs against a spawned + initialized
            // child. If it fails (timeout or `$/progress` error),
            // we must terminate the child before returning — the
            // child is not yet inside `state.client`, so nothing
            // else will reap it. Any cleanup error is surfaced
            // alongside the original readiness failure so the
            // caller / test can inspect both.
            if let Err(err) = dispatch_readiness(&spec.readiness, |timeout| {
                client.wait_for_workspace_load(timeout)
            })
            .await
            {
                let result = match client.force_terminate().await {
                    Ok(()) => err,
                    Err(cleanup) => Error::OperationWithCleanupFailure {
                        original: Box::new(err),
                        cleanup: Box::new(cleanup),
                    },
                };
                self.clear_process_control()?;
                return Err(result);
            }
            state.client = Some(client);
            state.opened_documents.clear();
        }

        let PoolEntryState {
            client,
            opened_documents,
        } = &mut *state;
        let client = client
            .as_ref()
            .ok_or_else(|| super::Error::ServerExited(None.into()))?;
        let mut pooled = PooledLsp {
            client,
            opened_documents,
            language_id: spec.language_id,
        };
        let result = work(&mut pooled).await;
        // Both `ServerExited` and `ServerExitedWithStderr` are
        // terminal server-exit signals — a live client cannot
        // recover from either. Take the client out of the state
        // (so the next `with_lsp_client` call spawns fresh) and
        // force-terminate the child; `opened_documents` is cleared
        // so the respawn starts from `didOpen` instead of
        // `didChange` against a document the new server never saw.
        // If the cleanup itself cannot prove the child terminated,
        // surface both errors via `OperationWithCleanupFailure` so
        // the central `with_lsp` poison path fires.
        if matches!(
            result,
            Err(super::Error::ServerExited(_)) | Err(super::Error::ServerExitedWithStderr { .. })
        ) {
            let client = state.client.take();
            state.opened_documents.clear();
            if let Some(client) = client
                && let Err(cleanup) = client.force_terminate().await
            {
                let original = result.err().expect("just matched Err above");
                self.clear_process_control()?;
                return Err(super::Error::OperationWithCleanupFailure {
                    original: Box::new(original),
                    cleanup: Box::new(cleanup),
                });
            }
            self.clear_process_control()?;
        }
        result
    }

    async fn shutdown(&self) -> Result<()> {
        let _shutdown_guard = self.shutdown_gate.lock().await;
        let mut state = self.state.lock().await;
        state.opened_documents.clear();
        let result = match state.client.take() {
            Some(client) => client.shutdown().await,
            None => Ok(()),
        };
        self.clear_process_control()?;
        result
    }

    /// Final-shutdown path that never waits for the data-plane state mutex.
    /// The independent process-control handle first disables respawn, then
    /// kills and reaps the child. Dropping the pool record later discards any
    /// document state still held by a pass that is unwinding.
    async fn shutdown_bounded(&self, entry_timeout: Duration) -> Result<()> {
        let shutdown = async {
            let _shutdown_guard = self.shutdown_gate.lock().await;
            let control = {
                let mut slot = self
                    .process_control
                    .lock()
                    .map_err(|_| Error::Protocol("lsp process-control slot poisoned".into()))?;
                slot.stopping = true;
                slot.control.clone()
            };
            let Some(control) = control else {
                return Ok(());
            };
            control.stop_and_terminate().await
        };
        match timeout(entry_timeout, shutdown).await {
            Ok(result) => result,
            Err(_) => Err(Error::ChildTerminationFailed(format!(
                "bounded final shutdown exceeded {}ms",
                entry_timeout.as_millis()
            ))),
        }
    }

    fn install_process_control(&self, control: LspProcessControl) -> Result<()> {
        let mut slot = self
            .process_control
            .lock()
            .map_err(|_| Error::Protocol("lsp process-control slot poisoned".into()))?;
        if slot.stopping {
            return Err(Error::PoolStopped);
        }
        slot.control = Some(control);
        Ok(())
    }

    fn clear_process_control(&self) -> Result<()> {
        self.process_control
            .lock()
            .map_err(|_| Error::Protocol("lsp process-control slot poisoned".into()))?
            .control = None;
        Ok(())
    }
}

async fn dispatch_readiness<F, Fut>(
    readiness: &ReadinessStrategy,
    wait_for_workspace_load: F,
) -> Result<()>
where
    F: FnOnce(Duration) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    match readiness {
        ReadinessStrategy::ProgressQuiescence { timeout } => {
            wait_for_workspace_load(*timeout).await
        }
        ReadinessStrategy::InitializeResponseOnly => Ok(()),
    }
}

static GLOBAL_POOL: OnceLock<LspClientPool> = OnceLock::new();

/// Return the daemon-global LSP pool.
///
/// # Errors
/// Returns an LSP protocol error if the pool runtime cannot be
/// initialized.
pub fn global() -> Result<&'static LspClientPool> {
    if let Some(pool) = GLOBAL_POOL.get() {
        return Ok(pool);
    }
    let pool = LspClientPool::new()?;
    Ok(GLOBAL_POOL.get_or_init(|| pool))
}

/// Shut down the daemon-global pool if it was initialized.
///
/// # Errors
/// Returns the first LSP shutdown error observed.
pub async fn shutdown_global_if_initialized() -> Result<()> {
    if let Some(pool) = GLOBAL_POOL.get() {
        tokio::task::spawn_blocking(move || pool.shutdown_all())
            .await
            .map_err(|e| super::Error::Protocol(format!("lsp pool shutdown task: {e}")))??;
    }
    Ok(())
}

/// Force-terminate the daemon-global pool through the final-shutdown control
/// plane. The blocking wrapper isolates the pool-owned runtime from the
/// caller's Tokio runtime.
///
/// # Errors
/// Returns process cleanup or join errors from bounded shutdown.
pub async fn shutdown_global_bounded_if_initialized(entry_timeout: Duration) -> Result<()> {
    if let Some(pool) = GLOBAL_POOL.get() {
        tokio::task::spawn_blocking(move || pool.shutdown_all_bounded(entry_timeout))
            .await
            .map_err(|err| super::Error::Protocol(format!("lsp pool shutdown task: {err}")))??;
    }
    Ok(())
}

/// Evict and shut down the daemon-global pool after a stalled analyzer.
///
/// # Errors
/// Returns the first LSP shutdown error observed before a timeout.
pub fn force_shutdown_global_if_initialized(entry_timeout: Duration) -> Result<()> {
    if let Some(pool) = GLOBAL_POOL.get() {
        pool.force_shutdown_all(entry_timeout)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod tests;
