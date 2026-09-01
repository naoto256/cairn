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
//!   OS cleanup residuals remain truthful caller errors but release the exact
//!   placeholder and keep admission available. Only a private ownership or
//!   accounting invariant retains custody and halts the pool.
//! - Idle sweep: a wall-clock timestamp is refreshed on acquire and
//!   lease release. Expired Ready entries with no active leases use
//!   the same `Evicting` reservation and fail-closed termination
//!   handling as capacity LRU.
//! - Published drain: entries enter a registry-owned draining batch before
//!   leaving the live map. This keeps independent process-control handles
//!   discoverable across graceful, force, and final cleanup paths.
//! - Force-shutdown: transitions `Running → Draining`, rejects new
//!   acquisitions, and gives each published entry one bounded
//!   process-control cleanup. The published drain owner returns the pool to
//!   `Running` after every terminal process outcome (including an OS residual);
//!   private invariant failures retain custody in `Halted`.
//! - Final bounded shutdown transitions to `Stopped`, takes both live
//!   and published draining entries, and drives process control
//!   without waiting on graceful cleanup. Concurrent kill/reap is
//!   idempotently serialized by each client's child mutex.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use std::{panic::AssertUnwindSafe, panic::catch_unwind};

use futures::FutureExt;
use serde_json::Value;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, timeout, timeout_at};
use tracing::{debug, warn};

use super::client::{LspProcessControl, ProcessCleanupCompletion, ProcessCleanupDisposition};
use super::{Error, LspClient, Position, Result, Url};

/// Cap on live child processes when the env override is unset.
const DEFAULT_POOL_CAPACITY: usize = 16;
/// Ceiling for the env override; larger values clamp here with a warn.
const MAX_POOL_CAPACITY: usize = 64;
const POOL_CAPACITY_ENV: &str = "CAIRN_LSP_POOL_MAX_ENTRIES";
/// Wall-clock idle TTL: Ready entries unused this long are swept.
const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(10 * 60);
const IDLE_TTL_ENV: &str = "CAIRN_LSP_IDLE_TTL_SECS";
/// Cadence of the background idle-sweeper task.
const IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Per-victim bound for the sweeper's `shutdown_bounded` call, so a
/// single wedged entry cannot stall a sweep pass indefinitely.
const IDLE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// Final bounded observation of cleanup tasks that outlived their initiating
/// call. The tasks themselves remain non-cancellable until runtime teardown.
const POOL_DROP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Boxed future produced by a `with_lsp` work closure; borrows the
/// pooled client for the lifetime of the lease.
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

/// Internal readiness routing for definition passes. Public spawn specs keep
/// their pre-0.8.5 shape; Rust's semantic policy is selected only by the
/// purpose-specific workspace-analyzer bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefinitionReadiness {
    SpawnSpec,
    Semantic {
        hard_timeout: Duration,
        stall_timeout: Duration,
    },
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
    /// Dedicated single-worker runtime that owns all pool futures and
    /// child I/O tasks, keeping `with_lsp`'s `block_on` independent
    /// of any caller runtime.
    runtime: Option<Runtime>,
    registry: Arc<StdMutex<PoolRegistry>>,
    capacity: NonZeroUsize,
    /// Handle of the background sweep task; `None` when the idle TTL
    /// override disables sweeping. Never awaited or aborted — the
    /// task stops when the pool's dedicated runtime is dropped.
    _idle_sweeper: Option<JoinHandle<()>>,
}

struct PoolRegistry {
    mode: PoolMode,
    entries: HashMap<PoolKey, PoolRecord>,
    /// Entries removed from the live map by an in-flight drain. Final bounded
    /// shutdown takes these published batches so process control remains
    /// reachable regardless of which cleanup path owns the batch.
    draining_entries: HashMap<u64, Vec<Arc<PoolEntry>>>,
    next_drain_id: u64,
    /// Cleanup tasks which outlived (or may outlive) their first observer.
    /// Receipts, rather than join handles, allow all callers to observe the
    /// same coalesced task without acquiring cancellation authority.
    outstanding_cleanups: HashMap<u64, watch::Receiver<CleanupOutcome>>,
    next_cleanup_id: u64,
    /// Cleanup admissions between selecting an epoch and publishing its
    /// receipt. Final shutdown waits for this to reach zero before its last
    /// receipt snapshot, closing the admission/snapshot race.
    active_cleanup_admissions: usize,
    /// Set at the final-shutdown fixed point under the same mutex as the
    /// admission count and receipt map. Later callers may join an existing
    /// cleanup generation, but cannot publish a new task past this barrier.
    cleanup_admissions_closed: bool,
    /// Monotonic counter; every acquire bumps it and stamps the
    /// record's `last_used`. Overflow at `u64::MAX` is a
    /// (theoretical) fail-closed error rather than a wrap.
    access_seq: u64,
}

struct PublishedDrain {
    id: u64,
    entries: Vec<Arc<PoolEntry>>,
}

fn lock_registry_state(
    registry: &StdMutex<PoolRegistry>,
) -> Result<std::sync::MutexGuard<'_, PoolRegistry>> {
    match registry.lock() {
        Ok(guard) => Ok(guard),
        Err(poisoned) => {
            let mut state = poisoned.into_inner();
            if state.mode != PoolMode::Stopped {
                state.mode = PoolMode::Halted;
            }
            Err(Error::PoolPoisoned)
        }
    }
}

impl PoolRegistry {
    /// Publish the entries to the bounded-shutdown control plane
    /// before removing them from the live map. Both steps occur
    /// under the registry mutex, so no observer can see an entry in
    /// neither collection.
    fn publish_live_drain(&mut self) -> Result<PublishedDrain> {
        let id = self.next_drain_id;
        let Some(next_drain_id) = self.next_drain_id.checked_add(1) else {
            if self.mode != PoolMode::Stopped {
                self.mode = PoolMode::Halted;
            }
            return Err(Error::PoolPoisoned);
        };
        self.next_drain_id = next_drain_id;
        let entries = self
            .entries
            .values()
            .map(|record| Arc::clone(&record.entry))
            .collect::<Vec<_>>();
        self.draining_entries.insert(id, entries.clone());
        self.entries.clear();
        Ok(PublishedDrain { id, entries })
    }

    /// Remove a published drain only when it is still owned by that path. A
    /// final bounded drain may already have taken it.
    fn finish_published_drain(&mut self, id: u64) -> bool {
        self.draining_entries.remove(&id).is_some()
    }

    /// Permanently take both live and already-published entries for
    /// daemon-final bounded process cleanup.
    fn take_all_for_bounded_shutdown(&mut self) -> Vec<Arc<PoolEntry>> {
        let mut entries = self
            .entries
            .drain()
            .map(|(_, record)| record.entry)
            .collect::<Vec<_>>();
        entries.extend(
            self.draining_entries
                .drain()
                .flat_map(|(_, entries)| entries),
        );
        entries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolMode {
    /// Normal operation. Acquire, evict, insert all permitted.
    Running,
    /// A `force_shutdown_all` is in flight. New acquires reject
    /// with `PoolDraining`; entries have moved from the live map to
    /// a published draining batch.
    Draining,
    /// Internal cleanup ownership or accounting became inconsistent. All
    /// future acquires reject until the daemon restarts.
    Halted,
    /// Daemon-level final shutdown. All future acquires reject.
    Stopped,
}

struct PoolRecord {
    entry: Arc<PoolEntry>,
    /// Count of outstanding `PoolLease`s. Non-zero blocks both LRU
    /// eviction and idle sweeping of this record.
    active_leases: usize,
    /// LRU ordinal (`access_seq` at the last acquire). Bumped on
    /// acquire only, not on lease release.
    last_used: u64,
    /// Wall-clock stamp for the idle TTL; refreshed on both acquire
    /// and lease release.
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
    /// outside the registry lock. Proven and OS-residual terminal outcomes
    /// release this exact record; only an internal invariant failure retains
    /// custody and halts admission.
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
        // Drop must not panic: on std-mutex poisoning, proceed with
        // the inner data so the lease count is still released.
        let mut reg = match self.registry.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                if state.mode != PoolMode::Stopped {
                    state.mode = PoolMode::Halted;
                }
                state
            }
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
                    reg.mode = PoolMode::Halted;
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
        let (label, reason) = if rest.chars().all(|c| c.is_ascii_digit()) && !rest.is_empty() {
            (
                "lsp pool capacity must be > 0; using default",
                "out_of_range",
            )
        } else {
            ("invalid lsp pool capacity; using default", "invalid")
        };
        warn!(
            env = POOL_CAPACITY_ENV,
            default = DEFAULT_POOL_CAPACITY,
            reason,
            "{}",
            label
        );
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
                max = MAX_POOL_CAPACITY,
                reason = "overflow",
                "lsp pool capacity exceeds max; clamping"
            );
            return max;
        }
        Err(_) => {
            warn!(
                env = POOL_CAPACITY_ENV,
                default = DEFAULT_POOL_CAPACITY,
                reason = "invalid",
                "invalid lsp pool capacity; using default"
            );
            return default;
        }
    };
    if parsed == 0 {
        warn!(
            env = POOL_CAPACITY_ENV,
            default = DEFAULT_POOL_CAPACITY,
            reason = "out_of_range",
            "lsp pool capacity must be > 0; using default"
        );
        return default;
    }
    if parsed > MAX_POOL_CAPACITY as u128 {
        warn!(
            env = POOL_CAPACITY_ENV,
            max = MAX_POOL_CAPACITY,
            reason = "out_of_range",
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
            let reason = if trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
                "overflow"
            } else {
                "invalid"
            };
            warn!(
                env = IDLE_TTL_ENV,
                default_secs = DEFAULT_IDLE_TTL.as_secs(),
                reason,
                "invalid lsp pool idle TTL; using default"
            );
            Some(DEFAULT_IDLE_TTL)
        }
    }
}

fn parse_idle_ttl_env() -> Option<Duration> {
    idle_ttl_from_env_value(std::env::var(IDLE_TTL_ENV).ok().as_deref())
}

/// Aggregated typed per-entry outcomes for `force_shutdown_all`.
#[derive(Debug, Default)]
struct ForceShutdownOutcome {
    first_os_residual: Option<String>,
    first_invariant_failure: Option<String>,
    public_error: Option<Error>,
    pending: bool,
}

fn classify_force_shutdown_results(results: &[CleanupOutcome]) -> ForceShutdownOutcome {
    let mut out = ForceShutdownOutcome::default();
    for outcome in results {
        if let Err(error) = outcome.clone().into_result() {
            out.public_error = Some(match out.public_error.take() {
                None => error,
                Some(prior) => Error::OperationWithCleanupFailure {
                    original: Box::new(prior),
                    cleanup: Box::new(error),
                },
            });
        }
        match outcome {
            CleanupOutcome::Pending => out.pending = true,
            CleanupOutcome::Terminal(CleanupTerminal::Proven, _) => {}
            CleanupOutcome::Terminal(CleanupTerminal::OsResidual(message), _) => {
                out.first_os_residual.get_or_insert_with(|| message.clone());
            }
            CleanupOutcome::Terminal(
                CleanupTerminal::InvariantFailure {
                    message,
                    os_residual,
                },
                _,
            ) => {
                out.first_invariant_failure
                    .get_or_insert_with(|| message.clone());
                if let Some(os_residual) = os_residual {
                    out.first_os_residual
                        .get_or_insert_with(|| os_residual.clone());
                }
            }
        }
    }
    if let Some(message) = out.first_os_residual.as_ref() {
        // The private typed disposition, rather than whichever public error
        // happened to be folded last, owns the caller's termination axis.
        // Remove copies of that exact fact from the aggregate before placing
        // it once at the outer cleanup position; distinct later residuals and
        // protocol/invariant facts remain on the original side.
        let original = out
            .public_error
            .take()
            .and_then(|error| remove_child_termination_fact(error, message));
        let residual = Error::ChildTerminationFailed(message.clone());
        out.public_error = Some(match original {
            Some(original) => Error::OperationWithCleanupFailure {
                original: Box::new(original),
                cleanup: Box::new(residual),
            },
            None => residual,
        });
    }
    out
}

fn remove_child_termination_fact(error: Error, message: &str) -> Option<Error> {
    match error {
        Error::ChildTerminationFailed(candidate) if candidate == message => None,
        Error::OperationWithCleanupFailure { original, cleanup } => {
            let original = remove_child_termination_fact(*original, message);
            let cleanup = remove_child_termination_fact(*cleanup, message);
            match (original, cleanup) {
                (Some(original), Some(cleanup)) => Some(Error::OperationWithCleanupFailure {
                    original: Box::new(original),
                    cleanup: Box::new(cleanup),
                }),
                (Some(error), None) | (None, Some(error)) => Some(error),
                (None, None) => None,
            }
        }
        other => Some(other),
    }
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
            draining_entries: HashMap::new(),
            next_drain_id: 1,
            outstanding_cleanups: HashMap::new(),
            next_cleanup_id: 1,
            active_cleanup_admissions: 0,
            cleanup_admissions_closed: false,
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
            runtime: Some(runtime),
            registry,
            capacity,
            _idle_sweeper: idle_sweeper,
        })
    }

    /// Borrow a long-lived LSP client for `key`, lazily spawning it
    /// when needed according to `spawn_spec`.
    ///
    /// Blocks the calling thread on the pool's dedicated runtime, so
    /// it must not be invoked from within an async context (Tokio's
    /// `block_on` panics there).
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
        self.with_lsp_readiness(key, spawn_spec, DefinitionReadiness::SpawnSpec, work)
    }

    pub(crate) fn with_lsp_readiness<T, F>(
        &self,
        key: PoolKey,
        spawn_spec: LspSpawnSpec,
        readiness: DefinitionReadiness,
        work: F,
    ) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledLsp<'a>) -> ClientWork<'a, T>,
    {
        let lease = self
            .runtime()
            .block_on(async { self.acquire_lease(key).await })?;
        let entry = Arc::clone(&lease.entry);
        let result = self
            .runtime()
            .block_on(async move { entry.with_lsp_client(spawn_spec, readiness, work).await });
        drop(lease);
        result
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
                    PoolMode::Halted => return Err(Error::PoolPoisoned),
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
                    let Some(bumped_seq) = reg.access_seq.checked_add(1) else {
                        reg.mode = PoolMode::Halted;
                        return Err(Error::PoolPoisoned);
                    };
                    reg.access_seq = bumped_seq;
                    let record = reg
                        .entries
                        .get_mut(&key)
                        .expect("contains_key just checked");
                    let Some(bumped_leases) = record.active_leases.checked_add(1) else {
                        reg.mode = PoolMode::Halted;
                        return Err(Error::PoolPoisoned);
                    };
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
                    let Some(bumped_seq) = reg.access_seq.checked_add(1) else {
                        reg.mode = PoolMode::Halted;
                        return Err(Error::PoolPoisoned);
                    };
                    reg.access_seq = bumped_seq;
                    let entry = PoolEntry::new(key.clone(), &self.registry);
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
            let completion = victim_entry.shutdown().await;
            match completion.terminal {
                CleanupTerminal::InvariantFailure { message, .. } => {
                    let mut registry = self.lock_registry()?;
                    if registry.mode != PoolMode::Stopped {
                        warn!(%message, "lsp pool: LRU cleanup invariant failure");
                        registry.mode = PoolMode::Halted;
                    }
                    return Err(Error::PoolPoisoned);
                }
                CleanupTerminal::Proven | CleanupTerminal::OsResidual(_) => {
                    // Terminal cleanup releases the exact placeholder. OS
                    // residuals remain a truthful caller error but do not
                    // halt unrelated pool admission.
                    let mut registry = self.lock_registry()?;
                    let still_reserved = registry.entries.get(&victim_key).is_some_and(|record| {
                        record.state == RecordState::Evicting
                            && Arc::ptr_eq(&record.entry, &victim_entry)
                    });
                    if still_reserved {
                        registry.entries.remove(&victim_key);
                    }
                    drop(registry);
                    completion.result?;
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
            let mut reg = lock_registry_state(registry)?;
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
        // Stop victims concurrently and with a per-victim bound —
        // unlike capacity LRU (unbounded `shutdown()`), the sweeper
        // task must not be stalled indefinitely by one wedged entry.
        let outcomes =
            futures::future::join_all(victims.into_iter().map(|(key, entry)| async move {
                let outcome = entry.shutdown_bounded_outcome(entry_timeout).await;
                (key, entry, outcome)
            }))
            .await;

        let mut first_err = None;
        let mut reg = lock_registry_state(registry)?;
        for (key, entry, outcome) in outcomes {
            // The registry may have changed while victims were being
            // stopped outside the lock: a drain (`shutdown_all` /
            // `force_shutdown_all` / `shutdown_all_bounded`) can have
            // removed the record, and after a drain that returned the
            // pool to Running a fresh record may exist under the same
            // key. Only remove the exact record we reserved (same
            // `Arc`, still Evicting) — never a newcomer.
            let still_reserved = reg.entries.get(&key).is_some_and(|record| {
                record.state == RecordState::Evicting && Arc::ptr_eq(&record.entry, &entry)
            });
            match outcome {
                CleanupOutcome::Terminal(CleanupTerminal::Proven, error) => {
                    if still_reserved {
                        reg.entries.remove(&key);
                    }
                    if first_err.is_none() {
                        first_err = error;
                    }
                }
                CleanupOutcome::Terminal(CleanupTerminal::OsResidual(message), error) => {
                    if still_reserved {
                        reg.entries.remove(&key);
                    }
                    warn!(
                        language = %key.language,
                        analyzer = %key.analyzer_id,
                        %message,
                        "lsp pool: idle sweep completed with OS cleanup residual"
                    );
                    if first_err.is_none() {
                        first_err = error.or(Some(Error::ChildTerminationFailed(message)));
                    }
                }
                CleanupOutcome::Terminal(
                    CleanupTerminal::InvariantFailure { message, .. },
                    error,
                ) => {
                    warn!(
                        language = %key.language,
                        analyzer = %key.analyzer_id,
                        %message,
                        "lsp pool: idle sweep hit cleanup invariant failure"
                    );
                    if reg.mode != PoolMode::Stopped {
                        reg.mode = PoolMode::Halted;
                    }
                    if first_err.is_none() {
                        first_err = error.or(Some(Error::PoolPoisoned));
                    }
                }
                CleanupOutcome::Pending => {
                    if first_err.is_none() {
                        first_err = Some(bounded_shutdown_timeout_error(entry_timeout));
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
        lock_registry_state(&self.registry)
    }

    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("LSP pool runtime is present until Drop")
    }

    /// Gracefully stop all live clients and mark the pool `Stopped`.
    /// Further acquisitions will return [`Error::PoolStopped`].
    ///
    /// # Errors
    /// Returns the first LSP shutdown error observed after
    /// attempting every entry.
    pub fn shutdown_all(&self) -> Result<()> {
        let (drain_id, entries, publication_error) = {
            let mut reg = match self.registry.lock() {
                Ok(registry) => registry,
                Err(poisoned) => {
                    let mut registry = poisoned.into_inner();
                    registry.mode = PoolMode::Stopped;
                    let entries = registry.take_all_for_bounded_shutdown();
                    drop(registry);
                    return self
                        .shutdown_entries_after_publication_failure(entries, Error::PoolPoisoned);
                }
            };
            reg.mode = PoolMode::Stopped;
            match reg.publish_live_drain() {
                Ok(drain) => (Some(drain.id), drain.entries, None),
                Err(error) => (None, reg.take_all_for_bounded_shutdown(), Some(error)),
            }
        };
        let result = self.runtime().block_on(async move {
            // Shutdown entries concurrently — same rationale as
            // `force_shutdown_all` (independent children, no
            // cross-entry contention).
            let completions = futures::future::join_all(
                entries
                    .into_iter()
                    .map(|entry| async move { entry.shutdown().await }),
            )
            .await;
            let mut first_err = publication_error;
            for completion in completions {
                if let Err(e) = completion.result
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        });
        if let Some(drain_id) = drain_id {
            match self.lock_registry() {
                Ok(mut reg) => {
                    reg.finish_published_drain(drain_id);
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        drain_id,
                        "lsp pool: could not finalize published shutdown drain"
                    );
                }
            }
        }
        result
    }

    fn shutdown_entries_after_publication_failure(
        &self,
        entries: Vec<Arc<PoolEntry>>,
        publication_error: Error,
    ) -> Result<()> {
        let cleanup_error = self.runtime().block_on(async move {
            futures::future::join_all(
                entries
                    .into_iter()
                    .map(|entry| async move { entry.shutdown().await }),
            )
            .await
            .into_iter()
            .find_map(|completion| completion.result.err())
        });
        match cleanup_error {
            Some(cleanup) => Err(Error::OperationWithCleanupFailure {
                original: Box::new(publication_error),
                cleanup: Box::new(cleanup),
            }),
            None => Err(publication_error),
        }
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
        let (entries, registry_invariant) = {
            match self.registry.lock() {
                Ok(mut reg) => {
                    reg.mode = PoolMode::Stopped;
                    (reg.take_all_for_bounded_shutdown(), false)
                }
                Err(poisoned) => {
                    let mut reg = poisoned.into_inner();
                    reg.mode = PoolMode::Stopped;
                    (reg.take_all_for_bounded_shutdown(), true)
                }
            }
        };
        let registry = Arc::clone(&self.registry);
        self.runtime().block_on(async move {
            let deadline = tokio::time::Instant::now() + entry_timeout;
            match timeout_at(deadline, async move {
                let mut receipts = Vec::new();
                let mut outstanding_receipts = HashMap::new();
                let mut first_error = registry_invariant.then_some(Error::PoolPoisoned);
                for entry in entries {
                    match entry.request_cleanup(true, None) {
                        Ok(Some(receipt)) => receipts.push(receipt),
                        Ok(None) => {}
                        Err(error) if first_error.is_none() => first_error = Some(error),
                        Err(_) => {}
                    }
                }

                loop {
                    let (active_admissions, outstanding, barrier_closed) = {
                        let mut state = match registry.lock() {
                            Ok(state) => state,
                            Err(poisoned) => {
                                if first_error.is_none() {
                                    first_error = Some(Error::PoolPoisoned);
                                }
                                poisoned.into_inner()
                            }
                        };
                        let active_admissions = state.active_cleanup_admissions;
                        let outstanding = state
                            .outstanding_cleanups
                            .iter()
                            .map(|(id, receipt)| (*id, receipt.clone()))
                            .collect::<Vec<_>>();
                        let barrier_closed = active_admissions == 0;
                        if barrier_closed {
                            state.cleanup_admissions_closed = true;
                        }
                        (active_admissions, outstanding, barrier_closed)
                    };
                    outstanding_receipts.extend(outstanding);
                    if barrier_closed {
                        break;
                    }
                    debug_assert_ne!(active_admissions, 0);
                    tokio::task::yield_now().await;
                }
                receipts.extend(outstanding_receipts.into_values());

                for result in
                    futures::future::join_all(receipts.into_iter().map(observe_cleanup)).await
                {
                    if let Err(error) = result
                        && first_error.is_none()
                    {
                        first_error = Some(error);
                    }
                }
                first_error.map_or(Ok(()), Err)
            })
            .await
            {
                Ok(result) => result,
                Err(_) if registry_invariant => Err(Error::OperationWithCleanupFailure {
                    original: Box::new(Error::PoolPoisoned),
                    cleanup: Box::new(bounded_shutdown_timeout_error(entry_timeout)),
                }),
                Err(_) => Err(bounded_shutdown_timeout_error(entry_timeout)),
            }
        })
    }

    /// Evict all live clients and give each entry a bounded period to terminate
    /// through its process-control handle. Used after analyzer stall detection
    /// so the next analyzer run does not inherit a wedged pool key.
    ///
    /// New acquisitions are rejected with [`Error::PoolDraining`]
    /// while this call is in flight. Mode transitions on finalize:
    ///
    /// - Every terminal process outcome (proven or OS residual) releases exact
    ///   custody; after all entries finish the drain owner returns to `Running`.
    /// - A private ownership/accounting invariant retains custody and moves the
    ///   pool to `Halted`.
    /// - A concurrent `shutdown_all` transitioned the pool to
    ///   `Stopped` while we were mid-drain → we preserve
    ///   `Stopped`; the pool is stopped, not poisoned.
    ///
    /// # Errors
    /// - [`Error::PoolPoisoned`] for a private invariant failure (combined with
    ///   any independently observed OS residual).
    /// - `ChildTerminationFailed` for an OS cleanup residual while the mode
    ///   remains `Stopped` / `Running`.
    /// - The `first_err` alone (no cleanup err) when the only
    ///   signal was a clean protocol failure whose child was
    ///   still reaped.
    pub fn force_shutdown_all(&self, entry_timeout: Duration) -> Result<()> {
        let drain = {
            let mut reg = self.lock_registry()?;
            match reg.mode {
                PoolMode::Running => reg.mode = PoolMode::Draining,
                PoolMode::Draining => return Err(Error::PoolDraining),
                PoolMode::Halted => return Err(Error::PoolPoisoned),
                PoolMode::Stopped => return Err(Error::PoolStopped),
            }
            reg.publish_live_drain()?
        };
        let drain_id = drain.id;
        debug!(
            entries = drain.entries.len(),
            "lsp pool: force-shutdown begin"
        );
        // Cleanup entries CONCURRENTLY: the wall-clock cost of a
        // force-shutdown is bounded by ~one entry timeout rather
        // than `capacity × entry_timeout`. Each entry owns its own
        // child process; there is no cross-entry contention on
        // shutdown.
        // `shutdown_bounded` owns the one per-entry timeout. Wrapping it in a
        // second timeout would create two competing authorities for whether
        // child termination was actually proven.
        let results = self.runtime().block_on(async move {
            futures::future::join_all(drain.entries.into_iter().map(|entry| async move {
                let receipt = match entry.request_cleanup(true, None) {
                    Ok(Some(receipt)) => Some(receipt),
                    Ok(None) => None,
                    Err(error) => {
                        return (
                            entry,
                            CleanupOutcome::invariant(error.to_string(), None),
                            None,
                        );
                    }
                };
                let outcome = match receipt.as_ref() {
                    Some(receipt) => {
                        observe_cleanup_bounded_outcome(receipt.clone(), entry_timeout).await
                    }
                    None => CleanupOutcome::proven(),
                };
                (entry, outcome, receipt)
            }))
            .await
        });
        let outcome = classify_force_shutdown_results(
            &results
                .iter()
                .map(|(_, outcome, _)| outcome.clone())
                .collect::<Vec<_>>(),
        );
        let final_mode = {
            let mut reg = self.lock_registry()?;
            let remove_drain = if let Some(entries) = reg.draining_entries.get_mut(&drain_id) {
                entries.retain(|entry| {
                    !results.iter().any(|(completed, outcome, _)| {
                        Arc::ptr_eq(entry, completed)
                            && matches!(
                                outcome,
                                CleanupOutcome::Terminal(
                                    CleanupTerminal::Proven | CleanupTerminal::OsResidual(_),
                                    _,
                                )
                            )
                    })
                });
                entries.is_empty()
            } else {
                false
            };
            if remove_drain {
                reg.draining_entries.remove(&drain_id);
            }
            match reg.mode {
                PoolMode::Stopped => PoolMode::Stopped,
                PoolMode::Halted => PoolMode::Halted,
                PoolMode::Draining => {
                    if outcome.first_invariant_failure.is_some() {
                        reg.mode = PoolMode::Halted;
                    } else if !outcome.pending && reg.draining_entries.is_empty() {
                        reg.mode = PoolMode::Running;
                    }
                    reg.mode
                }
                PoolMode::Running => PoolMode::Running,
            }
        };
        if final_mode == PoolMode::Draining && outcome.pending {
            let pending = results
                .iter()
                .filter_map(|(entry, outcome, receipt)| {
                    matches!(outcome, CleanupOutcome::Pending)
                        .then(|| (Arc::clone(entry), receipt.clone()))
                        .and_then(|(entry, receipt)| receipt.map(|receipt| (entry, receipt)))
                })
                .collect::<Vec<_>>();
            if !pending.is_empty() {
                let registry = Arc::clone(&self.registry);
                self.runtime().spawn(async move {
                    let terminals = futures::future::join_all(pending.into_iter().map(
                        |(entry, mut receipt)| async move {
                            let terminal = observe_cleanup_terminal(&mut receipt).await;
                            (entry, terminal)
                        },
                    ))
                    .await;
                    let mut reg = match registry.lock() {
                        Ok(reg) => reg,
                        Err(poisoned) => {
                            let mut reg = poisoned.into_inner();
                            if reg.mode != PoolMode::Stopped {
                                reg.mode = PoolMode::Halted;
                            }
                            return;
                        }
                    };
                    let invariant = terminals.iter().any(|(_, terminal)| {
                        matches!(
                            terminal,
                            CleanupOutcome::Terminal(CleanupTerminal::InvariantFailure { .. }, _,)
                        )
                    });
                    let remove_drain =
                        if let Some(entries) = reg.draining_entries.get_mut(&drain_id) {
                            entries.retain(|entry| {
                                !terminals.iter().any(|(completed, terminal)| {
                                    Arc::ptr_eq(entry, completed)
                                        && matches!(
                                            terminal,
                                            CleanupOutcome::Terminal(
                                                CleanupTerminal::Proven
                                                    | CleanupTerminal::OsResidual(_),
                                                _,
                                            )
                                        )
                                })
                            });
                            entries.is_empty()
                        } else {
                            true
                        };
                    if remove_drain {
                        reg.draining_entries.remove(&drain_id);
                    }
                    if reg.mode == PoolMode::Draining {
                        if invariant {
                            reg.mode = PoolMode::Halted;
                        } else if reg.draining_entries.is_empty() {
                            reg.mode = PoolMode::Running;
                        }
                    }
                });
            }
        }
        if let Some(reason) = outcome.first_invariant_failure.as_deref() {
            warn!(
                reason,
                "lsp pool: force-shutdown observed an internal invariant failure"
            );
            return Err(outcome
                .public_error
                .unwrap_or_else(|| match outcome.first_os_residual {
                    Some(message) => Error::OperationWithCleanupFailure {
                        original: Box::new(Error::PoolPoisoned),
                        cleanup: Box::new(Error::ChildTerminationFailed(message)),
                    },
                    None => Error::PoolPoisoned,
                }));
        }
        if final_mode == PoolMode::Halted {
            return Err(Error::PoolPoisoned);
        }
        if outcome.pending {
            return Err(bounded_shutdown_timeout_error(entry_timeout));
        }
        if let Some(error) = outcome.public_error {
            return Err(error);
        }
        if let Some(message) = outcome.first_os_residual {
            return Err(Error::ChildTerminationFailed(message));
        }
        debug!(mode = ?final_mode, "lsp pool: force-shutdown complete");
        Ok(())
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
    pub(crate) fn active_leases(&self, key: &PoolKey) -> Option<usize> {
        self.registry
            .lock()
            .unwrap()
            .entries
            .get(key)
            .map(|r| r.active_leases)
    }

    #[cfg(test)]
    pub(crate) fn is_running_for_test(&self) -> bool {
        self.registry.lock().unwrap().mode == PoolMode::Running
    }

    #[cfg(test)]
    pub(crate) fn capacity_for_test(&self) -> usize {
        self.capacity.get()
    }

    /// Remove one idle entry so a test leaves the shared pool as it found it.
    ///
    /// OS residual cleanup releases the exact record and returns the caller
    /// error; a private invariant retains it and halts admission.
    #[cfg(test)]
    pub(crate) fn remove_idle_test_entry(&self, key: &PoolKey) -> Result<()> {
        let entry = {
            let mut reg = self.lock_registry()?;
            let Some(record) = reg.entries.get_mut(key) else {
                return Ok(());
            };
            if record.state != RecordState::Ready || record.active_leases != 0 {
                return Err(Error::Protocol(
                    "test cleanup requires an idle ready pool entry".into(),
                ));
            }
            record.state = RecordState::Evicting;
            Arc::clone(&record.entry)
        };

        let completion = self.runtime().block_on(entry.shutdown());
        match completion.terminal {
            CleanupTerminal::InvariantFailure { message, .. } => {
                let mut registry = self.lock_registry()?;
                if registry.mode != PoolMode::Stopped {
                    registry.mode = PoolMode::Halted;
                }
                warn!(%message, "lsp pool: test cleanup invariant failure");
                Err(Error::PoolPoisoned)
            }
            CleanupTerminal::Proven | CleanupTerminal::OsResidual(_) => {
                let mut registry = self.lock_registry()?;
                let exact = registry.entries.get(key).is_some_and(|record| {
                    record.state == RecordState::Evicting && Arc::ptr_eq(&record.entry, &entry)
                });
                if exact {
                    registry.entries.remove(key);
                }
                completion.result
            }
        }
    }
}

impl Drop for LspClientPool {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        // Production uses the static GLOBAL_POOL, which is not dropped during
        // daemon final shutdown. This path is lifecycle hygiene for tests and
        // future non-static owners: runtime teardown must never cancel cleanup.
        let (entries, mut receipts) = {
            let registry = match self.registry.lock() {
                Ok(registry) => registry,
                Err(poisoned) => {
                    let mut registry = poisoned.into_inner();
                    if registry.mode != PoolMode::Stopped {
                        registry.mode = PoolMode::Halted;
                    }
                    registry
                }
            };
            let entries = registry
                .entries
                .values()
                .map(|record| Arc::clone(&record.entry))
                .chain(
                    registry
                        .draining_entries
                        .values()
                        .flat_map(|entries| entries.iter().cloned()),
                )
                .collect::<Vec<_>>();
            let receipts: Vec<_> = registry.outstanding_cleanups.values().cloned().collect();
            (entries, receipts)
        };
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _runtime_context = runtime.enter();
            for entry in entries {
                match entry.request_cleanup(true, None) {
                    Ok(Some(receipt)) => {
                        if !receipts.iter().any(|prior| prior.same_channel(&receipt)) {
                            receipts.push(receipt);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        warn!(%error, "lsp pool: could not schedule cleanup during drop");
                    }
                }
            }
        }));
        if receipts.is_empty() {
            return;
        }

        let observe = async {
            futures::future::join_all(receipts.iter().cloned().map(observe_cleanup)).await
        };
        let observed = catch_unwind(AssertUnwindSafe(|| {
            if Handle::try_current().is_ok() {
                std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            runtime.block_on(async {
                                timeout(POOL_DROP_CLEANUP_TIMEOUT, observe).await
                            })
                        })
                        .join()
                        .ok()
                })
            } else {
                Some(runtime.block_on(async { timeout(POOL_DROP_CLEANUP_TIMEOUT, observe).await }))
            }
        }))
        .ok()
        .flatten();
        if observed.is_some_and(|result| result.is_ok()) {
            return;
        }

        let pending = receipts
            .into_iter()
            .filter(|receipt| *receipt.borrow() == CleanupOutcome::Pending)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return;
        }

        let (runtime_sender, runtime_receiver) =
            std::sync::mpsc::sync_channel::<(Runtime, Vec<watch::Receiver<CleanupOutcome>>)>(1);
        let reaper_registry = Arc::clone(&self.registry);
        match std::thread::Builder::new()
            .name("cairn-lsp-cleanup-reaper".into())
            .spawn(move || {
                if let Ok((runtime, receipts)) = runtime_receiver.recv() {
                    let reaped = catch_unwind(AssertUnwindSafe(|| {
                        runtime.block_on(futures::future::join_all(
                            receipts.into_iter().map(observe_cleanup),
                        ));
                    }));
                    if reaped.is_err() {
                        if let Ok(mut registry) = reaper_registry.lock() {
                            registry.mode = PoolMode::Halted;
                        }
                        std::mem::forget(runtime);
                    }
                }
            }) {
            Ok(_) => {
                if let Err(error) = runtime_sender.send((runtime, pending)) {
                    warn!("lsp pool: cleanup reaper rejected runtime ownership");
                    std::mem::forget(error.0.0);
                    if let Ok(mut registry) = self.registry.lock() {
                        registry.mode = PoolMode::Halted;
                    }
                }
            }
            Err(error) => {
                warn!(%error, "lsp pool: could not start cleanup reaper");
                std::mem::forget(runtime);
                if let Ok(mut registry) = self.registry.lock() {
                    registry.mode = PoolMode::Halted;
                }
            }
        }
    }
}

struct PoolEntry {
    /// Data-plane state: the live client plus per-document version
    /// counters. Held for the whole of `with_lsp_client` — spawn,
    /// readiness wait, and the caller's work — so concurrent
    /// acquires of the same key serialize here, not in the registry.
    state: Mutex<PoolEntryState>,
    /// Serializes graceful shutdown paths for this entry. Bounded
    /// process-control cleanup deliberately bypasses this gate and is
    /// serialized by the child mutex instead.
    shutdown_gate: Mutex<()>,
    /// Child-process control is deliberately independent from `state`. Normal
    /// work holds `state` across an analyzer pass, but final daemon shutdown
    /// must still be able to kill and reap the child to unblock that pass.
    process_control: Arc<StdMutex<ProcessControlSlot>>,
    /// Identity used only when a timed-out cleanup later proves termination:
    /// release the exact `Evicting` record, never a same-key successor.
    registry: Weak<StdMutex<PoolRegistry>>,
    self_ref: Weak<PoolEntry>,
    key: Option<PoolKey>,
}

#[derive(Default)]
struct ProcessControlSlot {
    /// Set (and never cleared) by `shutdown_bounded` before it
    /// terminates the child. Once set, `install_and_arm` rejects new
    /// clients, so a racing spawn cannot install a fresh child after
    /// final shutdown has begun.
    stopping: bool,
    /// Monotonic identity for the installed control. Cleanup completion may
    /// mutate the slot only when this value still matches.
    epoch: u64,
    /// Control handle of the currently-installed client, if any.
    control: Option<LspProcessControl>,
    /// One coalesced cleanup task/receipt for the current epoch.
    cleanup: Option<CleanupTask>,
}

#[derive(Debug)]
enum CleanupOutcome {
    Pending,
    /// Private cleanup disposition plus the original typed public failure.
    /// The first axis controls custody/mode; the second is cloned into each
    /// observer's `Result` without reconstructing it from strings.
    Terminal(CleanupTerminal, Option<Error>),
}

impl Clone for CleanupOutcome {
    fn clone(&self) -> Self {
        match self {
            Self::Pending => Self::Pending,
            Self::Terminal(terminal, error) => Self::Terminal(
                terminal.clone(),
                error.as_ref().map(Error::clone_for_cleanup),
            ),
        }
    }
}

impl PartialEq for CleanupOutcome {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Pending, Self::Pending) => true,
            (Self::Terminal(left, _), Self::Terminal(right, _)) => left == right,
            _ => false,
        }
    }
}

impl Eq for CleanupOutcome {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CleanupTerminal {
    Proven,
    OsResidual(String),
    InvariantFailure {
        message: String,
        os_residual: Option<String>,
    },
}

#[derive(Debug)]
struct EntryCleanupCompletion {
    terminal: CleanupTerminal,
    result: Result<()>,
}

impl EntryCleanupCompletion {
    fn proven() -> Self {
        Self {
            terminal: CleanupTerminal::Proven,
            result: Ok(()),
        }
    }

    fn from_process(completion: ProcessCleanupCompletion) -> Self {
        let message = completion
            .error
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let terminal = match completion.disposition {
            ProcessCleanupDisposition::Proven => CleanupTerminal::Proven,
            ProcessCleanupDisposition::OsResidual => {
                CleanupTerminal::OsResidual(completion.os_residual.clone().unwrap_or(message))
            }
            ProcessCleanupDisposition::InvariantFailure => CleanupTerminal::InvariantFailure {
                message: completion.invariant.clone().unwrap_or(message),
                os_residual: completion.os_residual.clone(),
            },
        };
        Self {
            terminal,
            result: completion.into_result(),
        }
    }

    fn from_outcome(outcome: CleanupOutcome) -> Self {
        match outcome {
            CleanupOutcome::Pending => Self {
                terminal: CleanupTerminal::InvariantFailure {
                    message: "cleanup receipt remained pending after observation".into(),
                    os_residual: None,
                },
                result: Err(Error::PoolPoisoned),
            },
            CleanupOutcome::Terminal(terminal, error) => {
                let result = CleanupOutcome::Terminal(terminal.clone(), error).into_result();
                Self { terminal, result }
            }
        }
    }
}

impl CleanupOutcome {
    fn from_process(completion: ProcessCleanupCompletion) -> Self {
        let entry = EntryCleanupCompletion::from_process(completion);
        Self::Terminal(entry.terminal, entry.result.err())
    }

    fn proven() -> Self {
        Self::Terminal(CleanupTerminal::Proven, None)
    }

    fn invariant(message: impl Into<String>, os_residual: Option<String>) -> Self {
        let error = match os_residual.as_ref() {
            Some(residual) => Error::OperationWithCleanupFailure {
                original: Box::new(Error::PoolPoisoned),
                cleanup: Box::new(Error::ChildTerminationFailed(residual.clone())),
            },
            None => Error::PoolPoisoned,
        };
        Self::Terminal(
            CleanupTerminal::InvariantFailure {
                message: message.into(),
                os_residual,
            },
            Some(error),
        )
    }

    fn into_result(self) -> Result<()> {
        match self {
            Self::Pending => Err(Error::Protocol(
                "LSP cleanup receipt observed before completion".into(),
            )),
            Self::Terminal(_, Some(error)) => Err(error),
            Self::Terminal(CleanupTerminal::Proven, None) => Ok(()),
            Self::Terminal(CleanupTerminal::OsResidual(message), None) => {
                Err(Error::ChildTerminationFailed(message))
            }
            Self::Terminal(CleanupTerminal::InvariantFailure { os_residual, .. }, None) => {
                Self::invariant("cleanup invariant", os_residual).into_result()
            }
        }
    }
}

fn cleanup_os_residual(outcome: &CleanupOutcome) -> Option<String> {
    match outcome {
        CleanupOutcome::Terminal(CleanupTerminal::OsResidual(message), _) => Some(message.clone()),
        CleanupOutcome::Terminal(CleanupTerminal::InvariantFailure { os_residual, .. }, _) => {
            os_residual.clone()
        }
        CleanupOutcome::Pending | CleanupOutcome::Terminal(CleanupTerminal::Proven, _) => None,
    }
}

fn add_cleanup_invariant(outcome: CleanupOutcome, message: impl Into<String>) -> CleanupOutcome {
    let message = message.into();
    let os_residual = cleanup_os_residual(&outcome);
    let (message, prior_error) = match outcome {
        CleanupOutcome::Terminal(
            CleanupTerminal::InvariantFailure { message: prior, .. },
            error,
        ) => (format!("{prior}; {message}"), error),
        CleanupOutcome::Terminal(_, error) => (message, error),
        CleanupOutcome::Pending => (message, None),
    };
    let error = compose_cleanup_invariant_error(prior_error, os_residual.as_deref());
    CleanupOutcome::Terminal(
        CleanupTerminal::InvariantFailure {
            message,
            os_residual,
        },
        Some(error),
    )
}

fn compose_cleanup_invariant_error(prior_error: Option<Error>, os_residual: Option<&str>) -> Error {
    let Some(os_residual) = os_residual else {
        return match prior_error {
            Some(prior) => Error::OperationWithCleanupFailure {
                original: Box::new(prior),
                cleanup: Box::new(Error::PoolPoisoned),
            },
            None => Error::PoolPoisoned,
        };
    };

    match prior_error {
        Some(Error::ChildTerminationFailed(message)) => Error::OperationWithCleanupFailure {
            original: Box::new(Error::PoolPoisoned),
            cleanup: Box::new(Error::ChildTerminationFailed(message)),
        },
        Some(Error::OperationWithCleanupFailure { original, cleanup })
            if cleanup.is_termination_unproven() =>
        {
            Error::OperationWithCleanupFailure {
                original: Box::new(Error::OperationWithCleanupFailure {
                    original,
                    cleanup: Box::new(Error::PoolPoisoned),
                }),
                cleanup,
            }
        }
        Some(prior) => Error::OperationWithCleanupFailure {
            original: Box::new(Error::OperationWithCleanupFailure {
                original: Box::new(prior),
                cleanup: Box::new(Error::PoolPoisoned),
            }),
            cleanup: Box::new(Error::ChildTerminationFailed(os_residual.to_owned())),
        },
        None => Error::OperationWithCleanupFailure {
            original: Box::new(Error::PoolPoisoned),
            cleanup: Box::new(Error::ChildTerminationFailed(os_residual.to_owned())),
        },
    }
}

struct CleanupTask {
    epoch: u64,
    sender: watch::Sender<CleanupOutcome>,
    receipt: watch::Receiver<CleanupOutcome>,
    tracking_invariants: Arc<StdMutex<Vec<String>>>,
}

fn record_cleanup_tracking_invariant(
    tracking_invariants: &Arc<StdMutex<Vec<String>>>,
    message: impl Into<String>,
) -> Result<()> {
    tracking_invariants
        .lock()
        .map_err(|_| Error::PoolPoisoned)?
        .push(message.into());
    Ok(())
}

fn apply_cleanup_tracking_invariants(
    mut outcome: CleanupOutcome,
    tracking_invariants: &Arc<StdMutex<Vec<String>>>,
) -> CleanupOutcome {
    let messages = match tracking_invariants.lock() {
        Ok(mut messages) => std::mem::take(&mut *messages),
        Err(_) => vec!["LSP cleanup tracking-facts mutex poisoned".into()],
    };
    for message in messages {
        outcome = add_cleanup_invariant(outcome, message);
    }
    outcome
}

struct CleanupAdmission {
    registry: Option<Arc<StdMutex<PoolRegistry>>>,
}

impl CleanupAdmission {
    fn begin(registry: &Weak<StdMutex<PoolRegistry>>) -> Result<Option<Self>> {
        let Some(registry) = registry.upgrade() else {
            return Ok(Some(Self { registry: None }));
        };
        {
            let mut state = registry.lock().map_err(|_| Error::PoolPoisoned)?;
            if state.cleanup_admissions_closed {
                return Ok(None);
            }
            let Some(next) = state.active_cleanup_admissions.checked_add(1) else {
                state.mode = PoolMode::Halted;
                return Err(Error::PoolPoisoned);
            };
            state.active_cleanup_admissions = next;
        }
        Ok(Some(Self {
            registry: Some(registry),
        }))
    }
}

impl Drop for CleanupAdmission {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.as_ref() {
            let mut state = match registry.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    let mut state = poisoned.into_inner();
                    if state.mode != PoolMode::Stopped {
                        state.mode = PoolMode::Halted;
                    }
                    state
                }
            };
            match state.active_cleanup_admissions.checked_sub(1) {
                Some(next) => state.active_cleanup_admissions = next,
                None if state.mode != PoolMode::Stopped => state.mode = PoolMode::Halted,
                None => {}
            }
        }
    }
}

struct UncommittedExitGuard {
    entry: Arc<PoolEntry>,
    epoch: u64,
    armed: bool,
}

#[derive(Default)]
struct PoolEntryState {
    /// Live client; `None` until the first spawn and again after a
    /// terminal server exit is cleaned up.
    client: Option<LspClient>,
    /// URI → document version; decides `didOpen` vs `didChange` in
    /// `PooledLsp::sync_document`.
    opened_documents: HashMap<String, i32>,
}

impl Default for PoolEntry {
    fn default() -> Self {
        Self {
            state: Mutex::new(PoolEntryState::default()),
            shutdown_gate: Mutex::new(()),
            process_control: Arc::new(StdMutex::new(ProcessControlSlot::default())),
            registry: Weak::new(),
            self_ref: Weak::new(),
            key: None,
        }
    }
}

impl PoolEntry {
    fn new(key: PoolKey, registry: &Arc<StdMutex<PoolRegistry>>) -> Arc<Self> {
        Arc::new_cyclic(|self_ref| Self {
            state: Mutex::new(PoolEntryState::default()),
            shutdown_gate: Mutex::new(()),
            process_control: Arc::new(StdMutex::new(ProcessControlSlot::default())),
            registry: Arc::downgrade(registry),
            self_ref: self_ref.clone(),
            key: Some(key),
        })
    }
}

impl UncommittedExitGuard {
    /// Publish the ready client and disarm the exit guard in one helper. The
    /// assignment itself is the commit linearization point; no fallible work
    /// occurs between publication and disarm.
    fn commit(mut self, state: &mut PoolEntryState, client: LspClient) {
        state.client = Some(client);
        self.armed = false;
    }

    /// Normal error paths use the same cleanup task as `Drop`, but may observe
    /// its terminal receipt so primary and cleanup failures remain distinct.
    async fn finish_error(mut self, original: Error) -> Error {
        let receipt = self.entry.request_cleanup(false, Some(self.epoch));
        self.armed = false;
        let cleanup = match receipt {
            Ok(Some(receipt)) => observe_cleanup(receipt).await,
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        };
        match cleanup {
            Ok(()) => original,
            Err(cleanup) => Error::OperationWithCleanupFailure {
                original: Box::new(original),
                cleanup: Box::new(cleanup),
            },
        }
    }
}

impl Drop for UncommittedExitGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Err(error) = self.entry.request_cleanup(false, Some(self.epoch)) {
            self.entry.halt_registry(&error.to_string());
        }
    }
}

async fn observe_cleanup(mut receipt: watch::Receiver<CleanupOutcome>) -> Result<()> {
    observe_cleanup_terminal(&mut receipt).await.into_result()
}

async fn observe_cleanup_terminal(receipt: &mut watch::Receiver<CleanupOutcome>) -> CleanupOutcome {
    loop {
        let outcome = receipt.borrow().clone();
        if outcome != CleanupOutcome::Pending {
            return outcome;
        }
        if receipt.changed().await.is_err() {
            return CleanupOutcome::invariant(
                "LSP cleanup receipt disconnected before completion",
                None,
            );
        }
    }
}

async fn observe_cleanup_bounded_outcome(
    mut receipt: watch::Receiver<CleanupOutcome>,
    entry_timeout: Duration,
) -> CleanupOutcome {
    match timeout(entry_timeout, observe_cleanup_terminal(&mut receipt)).await {
        Ok(outcome) => outcome,
        Err(_) => CleanupOutcome::Pending,
    }
}

#[cfg(test)]
async fn observe_cleanup_bounded(
    receipt: watch::Receiver<CleanupOutcome>,
    entry_timeout: Duration,
) -> Result<()> {
    match observe_cleanup_bounded_outcome(receipt, entry_timeout).await {
        CleanupOutcome::Pending => Err(bounded_shutdown_timeout_error(entry_timeout)),
        outcome => outcome.into_result(),
    }
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
    /// Install the control and arm cleanup while holding the same slot lock.
    /// There is no published-control / unguarded-exit gap.
    fn install_and_arm(
        self: &Arc<Self>,
        control: LspProcessControl,
    ) -> Result<UncommittedExitGuard> {
        let mut slot = match self.process_control.lock() {
            Ok(slot) => slot,
            Err(_) => {
                self.halt_registry("lsp process-control slot poisoned during install");
                return Err(Error::PoolPoisoned);
            }
        };
        if slot.stopping {
            return Err(Error::PoolStopped);
        }
        let prior_cleanup = slot
            .cleanup
            .as_ref()
            .map(|cleanup| cleanup.receipt.borrow().clone());
        if let Some(prior_cleanup) = prior_cleanup {
            match prior_cleanup {
                CleanupOutcome::Pending => return Err(Error::PoolDraining),
                CleanupOutcome::Terminal(CleanupTerminal::InvariantFailure { .. }, _) => {
                    drop(slot);
                    self.halt_registry("prior cleanup ended with an internal invariant failure");
                    return Err(Error::PoolPoisoned);
                }
                CleanupOutcome::Terminal(
                    CleanupTerminal::Proven | CleanupTerminal::OsResidual(_),
                    _,
                ) => {
                    slot.control = None;
                    slot.cleanup = None;
                }
            }
        }
        if slot.control.is_some() {
            drop(slot);
            self.halt_registry("duplicate LSP process-control ownership");
            return Err(Error::PoolPoisoned);
        }
        let Some(epoch) = slot.epoch.checked_add(1) else {
            drop(slot);
            self.halt_registry("lsp process-control epoch overflow");
            return Err(Error::PoolPoisoned);
        };
        slot.epoch = epoch;
        slot.control = Some(control);
        Ok(UncommittedExitGuard {
            entry: Arc::clone(self),
            epoch,
            armed: true,
        })
    }

    /// Start or join the one cleanup task for the selected epoch. Task
    /// execution is owned by the pool runtime; dropping any observer only
    /// stops observation and cannot cancel kill/wait.
    fn request_cleanup(
        &self,
        final_stop: bool,
        expected_epoch: Option<u64>,
    ) -> Result<Option<watch::Receiver<CleanupOutcome>>> {
        let (admission, mut tracking_invariant) = match CleanupAdmission::begin(&self.registry) {
            Ok(admission) => (admission, None),
            Err(error) => (
                None,
                Some(format!("LSP cleanup admission bookkeeping failed: {error}")),
            ),
        };
        let mut slot = match self.process_control.lock() {
            Ok(slot) => slot,
            Err(_) => {
                self.halt_registry("lsp process-control slot poisoned during cleanup request");
                return Err(Error::PoolPoisoned);
            }
        };
        if final_stop {
            slot.stopping = true;
        }
        if expected_epoch.is_some_and(|expected| expected != slot.epoch) {
            return Ok(None);
        }
        let epoch = slot.epoch;
        if let Some(cleanup) = slot.cleanup.as_ref()
            && cleanup.epoch == epoch
        {
            if let Some(message) = tracking_invariant {
                record_cleanup_tracking_invariant(&cleanup.tracking_invariants, message)?;
                let current = cleanup.receipt.borrow().clone();
                if !matches!(current, CleanupOutcome::Pending) {
                    let current =
                        apply_cleanup_tracking_invariants(current, &cleanup.tracking_invariants);
                    cleanup.sender.send_replace(current);
                }
            }
            return Ok(Some(cleanup.receipt.clone()));
        }
        if admission.is_none() && tracking_invariant.is_none() {
            return Err(Error::PoolStopped);
        }
        let _admission = admission;

        let (sender, receiver) = watch::channel(CleanupOutcome::Pending);
        let tracking_invariants = Arc::new(StdMutex::new(Vec::new()));
        if let Some(message) = tracking_invariant.take() {
            record_cleanup_tracking_invariant(&tracking_invariants, message)?;
        }
        slot.cleanup = Some(CleanupTask {
            epoch,
            sender: sender.clone(),
            receipt: receiver.clone(),
            tracking_invariants: Arc::clone(&tracking_invariants),
        });
        let Some(control) = slot.control.clone() else {
            let outcome =
                apply_cleanup_tracking_invariants(CleanupOutcome::proven(), &tracking_invariants);
            sender.send_replace(outcome);
            return Ok(Some(receiver));
        };
        drop(slot);

        let cleanup_id = if let Some(registry) = self.registry.upgrade() {
            match registry.lock() {
                Ok(mut registry) => match registry.next_cleanup_id.checked_add(1) {
                    Some(next) => {
                        let id = registry.next_cleanup_id;
                        registry.next_cleanup_id = next;
                        registry.outstanding_cleanups.insert(id, receiver.clone());
                        Some(id)
                    }
                    None => {
                        if registry.mode != PoolMode::Stopped {
                            registry.mode = PoolMode::Halted;
                        }
                        record_cleanup_tracking_invariant(
                            &tracking_invariants,
                            "LSP cleanup id overflow",
                        )?;
                        None
                    }
                },
                Err(poisoned) => {
                    let mut registry = poisoned.into_inner();
                    if registry.mode != PoolMode::Stopped {
                        registry.mode = PoolMode::Halted;
                    }
                    record_cleanup_tracking_invariant(
                        &tracking_invariants,
                        "LSP pool registry mutex poisoned",
                    )?;
                    None
                }
            }
        } else {
            None
        };

        let process_control = Arc::clone(&self.process_control);
        let registry = self.registry.clone();
        let self_ref = self.self_ref.clone();
        let key = self.key.clone();
        let task = async move {
            let mut outcome = match AssertUnwindSafe(control.stop_and_terminate_completion())
                .catch_unwind()
                .await
            {
                Ok(completion) => CleanupOutcome::from_process(completion),
                Err(_) => CleanupOutcome::invariant("LSP cleanup task panicked", None),
            };
            match process_control.lock() {
                Ok(mut slot) if slot.epoch == epoch => {
                    if matches!(
                        &outcome,
                        CleanupOutcome::Terminal(
                            CleanupTerminal::Proven | CleanupTerminal::OsResidual(_),
                            _,
                        )
                    ) {
                        slot.control = None;
                    }
                }
                Ok(_) => {
                    outcome =
                        add_cleanup_invariant(outcome, "LSP cleanup epoch changed before finalize");
                }
                Err(_) => {
                    outcome = add_cleanup_invariant(
                        outcome,
                        "LSP process-control slot poisoned during cleanup finalize",
                    );
                }
            }

            let Some(registry) = registry.upgrade() else {
                sender.send_replace(apply_cleanup_tracking_invariants(
                    outcome,
                    &tracking_invariants,
                ));
                return;
            };
            let Ok(mut registry) = registry.lock() else {
                outcome = add_cleanup_invariant(
                    outcome,
                    "LSP pool registry mutex poisoned during cleanup finalize",
                );
                sender.send_replace(apply_cleanup_tracking_invariants(
                    outcome,
                    &tracking_invariants,
                ));
                return;
            };
            if let Some(cleanup_id) = cleanup_id {
                if registry.outstanding_cleanups.remove(&cleanup_id).is_none() {
                    outcome = add_cleanup_invariant(
                        outcome,
                        format!("LSP cleanup receipt custody lost for id {cleanup_id}"),
                    );
                }
            }
            match &outcome {
                CleanupOutcome::Terminal(
                    CleanupTerminal::Proven | CleanupTerminal::OsResidual(_),
                    _,
                ) => {
                    if let (Some(key), Some(entry)) = (key, self_ref.upgrade()) {
                        let release = registry.entries.get(&key).is_some_and(|record| {
                            record.state == RecordState::Evicting
                                && Arc::ptr_eq(&record.entry, &entry)
                        });
                        if release {
                            registry.entries.remove(&key);
                        }
                        for entries in registry.draining_entries.values_mut() {
                            entries.retain(|candidate| !Arc::ptr_eq(candidate, &entry));
                        }
                        registry
                            .draining_entries
                            .retain(|_, entries| !entries.is_empty());
                        // The published-drain owner is the sole authority for
                        // `Draining -> Running`. Entry-local completion may
                        // release exact custody, but must not reopen admission
                        // while `force_shutdown_all` is still aggregating.
                    }
                    if let CleanupOutcome::Terminal(CleanupTerminal::OsResidual(message), _) =
                        &outcome
                    {
                        warn!(%message, "lsp pool: cleanup completed with OS residual; admission continues");
                    }
                }
                CleanupOutcome::Terminal(CleanupTerminal::InvariantFailure { message, .. }, _) => {
                    if registry.mode != PoolMode::Stopped {
                        warn!(%message, "lsp pool: cleanup invariant failure; halting pool");
                        registry.mode = PoolMode::Halted;
                    }
                }
                CleanupOutcome::Pending => {}
            }
            sender.send_replace(apply_cleanup_tracking_invariants(
                outcome,
                &tracking_invariants,
            ));
        };

        let spawn_result = Handle::try_current().map_err(|_| ()).and_then(|handle| {
            catch_unwind(AssertUnwindSafe(|| handle.spawn(task))).map_err(|_| ())
        });
        if spawn_result.is_err() {
            let message = "LSP cleanup task could not be enqueued".to_string();
            // The sender moved into the task future, which is dropped when
            // spawning fails. Rebuild a terminal receipt in the slot so future
            // installs remain fail-closed.
            let (_terminal_sender, terminal_receiver) =
                watch::channel(CleanupOutcome::invariant(message.clone(), None));
            if let Ok(mut slot) = self.process_control.lock()
                && slot.epoch == epoch
            {
                slot.cleanup = Some(CleanupTask {
                    epoch,
                    sender: _terminal_sender,
                    receipt: terminal_receiver.clone(),
                    tracking_invariants: Arc::new(StdMutex::new(Vec::new())),
                });
            }
            if let Some(registry) = self.registry.upgrade()
                && let Ok(mut registry) = registry.lock()
                && let Some(cleanup_id) = cleanup_id
            {
                registry.outstanding_cleanups.remove(&cleanup_id);
            }
            self.halt_registry(&message);
            return Ok(Some(terminal_receiver));
        }
        Ok(Some(receiver))
    }

    fn halt_registry(&self, message: &str) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let Ok(mut registry) = registry.lock() else {
            return;
        };
        if registry.mode != PoolMode::Stopped {
            warn!(%message, "lsp pool: internal cleanup invariant failed; halting pool");
            registry.mode = PoolMode::Halted;
        }
    }

    async fn with_lsp_client<T, F>(
        self: &Arc<Self>,
        spec: LspSpawnSpec,
        readiness: DefinitionReadiness,
        work: F,
    ) -> Result<T>
    where
        F: for<'a> FnOnce(&'a mut PooledLsp<'a>) -> ClientWork<'a, T>,
    {
        let mut state = self.state.lock().await;
        if state.client.is_none() {
            check_lsp_available(&spec.binary, &spec.availability, spec.request_timeout).await?;
            let client = LspClient::configured_for_pool(
                &spec.binary,
                spec.launch_args.clone(),
                spec.env.clone(),
                &spec.workspace_root,
                spec.initialization_options.clone(),
                spec.request_timeout,
            );
            // Publish the control handle before spawning: if a
            // bounded final shutdown has already flagged `stopping`,
            // this rejects with `PoolStopped` and no child is
            // spawned.
            let guard = self.install_and_arm(client.process_control())?;
            if let Err(err) = client.start_process().await {
                return Err(guard.finish_error(err).await);
            }
            // Readiness check runs against a spawned + initialized
            // child. If it fails (timeout or `$/progress` error),
            // we must terminate the child before returning — the
            // child is not yet inside `state.client`, so nothing
            // else will reap it. Any cleanup error is surfaced
            // alongside the original readiness failure so the
            // caller / test can inspect both.
            if let Err(err) = dispatch_readiness(&spec.readiness, readiness, |wait| {
                let client = &client;
                async move {
                    match wait {
                        ReadinessWait::Raw { timeout } => {
                            client.wait_for_workspace_load(timeout).await
                        }
                        ReadinessWait::Semantic {
                            hard_timeout,
                            stall_timeout,
                        } => {
                            client
                                .wait_for_workspace_load_bounded(hard_timeout, stall_timeout)
                                .await
                        }
                    }
                }
            })
            .await
            {
                return Err(guard.finish_error(err).await);
            }
            guard.commit(&mut state, client);
            state.opened_documents.clear();
        }

        let PoolEntryState {
            client,
            opened_documents,
        } = &mut *state;
        // `client` is always `Some` here (pre-existing, or just
        // installed above); the error arm is defensive.
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
        // If cleanup reports an OS residual, surface both errors via
        // `OperationWithCleanupFailure`; the private cleanup disposition, not
        // this public error shape, independently decides pool admission.
        if matches!(
            result,
            Err(super::Error::ServerExited(_)) | Err(super::Error::ServerExitedWithStderr { .. })
        ) {
            let client = state.client.take();
            state.opened_documents.clear();
            drop(client);
            let cleanup = match self.request_cleanup(false, None) {
                Ok(Some(receipt)) => observe_cleanup(receipt).await,
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(cleanup) = cleanup {
                let original = result.err().expect("just matched Err above");
                return Err(super::Error::OperationWithCleanupFailure {
                    original: Box::new(original),
                    cleanup: Box::new(cleanup),
                });
            }
        }
        result
    }

    /// Graceful entry shutdown, used by capacity LRU eviction and
    /// `shutdown_all`. Waits without its own bound on the shutdown gate and the
    /// data-plane mutex, so an in-flight pass completes first. `Ok(())` means
    /// either no client was installed or `LspClient::shutdown` reaped the child
    /// — callers treat it as termination-proven.
    async fn shutdown(&self) -> EntryCleanupCompletion {
        let _shutdown_guard = self.shutdown_gate.lock().await;
        let mut state = self.state.lock().await;
        state.opened_documents.clear();
        let completion = match state.client.take() {
            Some(client) => {
                EntryCleanupCompletion::from_process(client.shutdown_completion().await)
            }
            None => match self.request_cleanup(false, None) {
                Ok(Some(mut receipt)) => EntryCleanupCompletion::from_outcome(
                    observe_cleanup_terminal(&mut receipt).await,
                ),
                Ok(None) => EntryCleanupCompletion::proven(),
                Err(error) => EntryCleanupCompletion {
                    terminal: CleanupTerminal::InvariantFailure {
                        message: error.to_string(),
                        os_residual: None,
                    },
                    result: Err(error),
                },
            },
        };
        if matches!(
            completion.terminal,
            CleanupTerminal::Proven | CleanupTerminal::OsResidual(_)
        ) && let Err(error) = self.clear_process_control()
        {
            self.halt_registry(&error.to_string());
            let result = match completion.result {
                Ok(()) => Err(Error::PoolPoisoned),
                Err(prior) => Err(Error::OperationWithCleanupFailure {
                    original: Box::new(prior),
                    cleanup: Box::new(Error::PoolPoisoned),
                }),
            };
            return EntryCleanupCompletion {
                terminal: CleanupTerminal::InvariantFailure {
                    message: error.to_string(),
                    os_residual: match completion.terminal {
                        CleanupTerminal::OsResidual(ref message) => Some(message.clone()),
                        CleanupTerminal::InvariantFailure {
                            ref os_residual, ..
                        } => os_residual.clone(),
                        CleanupTerminal::Proven => None,
                    },
                },
                result,
            };
        }
        completion
    }

    /// Bounded entry shutdown that never waits for the data-plane state mutex.
    /// Used by `shutdown_all_bounded` (daemon-final shutdown), by
    /// `force_shutdown_all`, and by the idle sweeper's per-victim eviction, so
    /// it is not final-shutdown-only.
    /// It also bypasses the graceful `shutdown_gate`: the independent
    /// process-control handle serializes kill/reap through the child mutex, so
    /// a concurrent graceful cleanup becomes an idempotent no-op after the
    /// bounded path reaps first. Dropping the pool record later discards any
    /// document state still held by a pass that is unwinding.
    ///
    /// This method owns the sole per-entry timeout classification for process
    /// termination; callers must not wrap it in a second timeout.
    #[cfg(test)]
    async fn shutdown_bounded(&self, entry_timeout: Duration) -> Result<()> {
        match self.shutdown_bounded_outcome(entry_timeout).await {
            CleanupOutcome::Pending => Err(bounded_shutdown_timeout_error(entry_timeout)),
            outcome => outcome.into_result(),
        }
    }

    async fn shutdown_bounded_outcome(&self, entry_timeout: Duration) -> CleanupOutcome {
        let receipt = match self.request_cleanup(true, None) {
            Ok(Some(receipt)) => receipt,
            Ok(None) => return CleanupOutcome::proven(),
            Err(error) => {
                return CleanupOutcome::Terminal(
                    CleanupTerminal::InvariantFailure {
                        message: error.to_string(),
                        os_residual: None,
                    },
                    Some(error),
                );
            }
        };
        // The timeout bounds observation only. `request_cleanup` transferred
        // kill/wait into a pool-runtime task which continues to completion.
        observe_cleanup_bounded_outcome(receipt, entry_timeout).await
    }

    /// Drop the control handle after the child is known terminated
    /// (or was never spawned). `stopping` is deliberately left
    /// untouched — the spawn veto is permanent for this entry.
    ///
    /// # Errors
    /// [`Error::PoolPoisoned`] when the slot mutex is poisoned.
    fn clear_process_control(&self) -> Result<()> {
        let mut slot = self
            .process_control
            .lock()
            .map_err(|_| Error::PoolPoisoned)?;
        let cleanup_pending = slot
            .cleanup
            .as_ref()
            .is_some_and(|cleanup| cleanup.receipt.borrow().clone() == CleanupOutcome::Pending);
        if !cleanup_pending {
            slot.control = None;
        }
        Ok(())
    }
}

/// Run the configured readiness gate against a freshly initialized
/// client; `InitializeResponseOnly` needs no extra wait.
async fn dispatch_readiness<F, Fut>(
    readiness: &ReadinessStrategy,
    dispatch: DefinitionReadiness,
    wait_for_workspace_load: F,
) -> Result<()>
where
    F: FnOnce(ReadinessWait) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let wait = match dispatch {
        DefinitionReadiness::SpawnSpec => match readiness {
            ReadinessStrategy::ProgressQuiescence { timeout } => {
                Some(ReadinessWait::Raw { timeout: *timeout })
            }
            ReadinessStrategy::InitializeResponseOnly => None,
        },
        DefinitionReadiness::Semantic {
            hard_timeout,
            stall_timeout,
        } => Some(ReadinessWait::Semantic {
            hard_timeout,
            stall_timeout,
        }),
    };
    match wait {
        Some(wait) => wait_for_workspace_load(wait).await,
        None => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadinessWait {
    Raw {
        timeout: Duration,
    },
    Semantic {
        hard_timeout: Duration,
        stall_timeout: Duration,
    },
}

fn bounded_shutdown_timeout_error(entry_timeout: Duration) -> Error {
    Error::ChildTerminationFailed(format!(
        "bounded LSP entry shutdown exceeded {}ms",
        entry_timeout.as_millis()
    ))
}

static GLOBAL_POOL: OnceLock<LspClientPool> = OnceLock::new();
static GLOBAL_POOL_INIT: StdMutex<()> = StdMutex::new(());

fn initialize_global_pool<'a, F>(
    cell: &'a OnceLock<LspClientPool>,
    init_gate: &StdMutex<()>,
    initialize: F,
) -> Result<&'a LspClientPool>
where
    F: FnOnce() -> Result<LspClientPool>,
{
    if let Some(pool) = cell.get() {
        return Ok(pool);
    }
    let _guard = match init_gate.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("lsp pool: recovering poisoned global initialization gate");
            poisoned.into_inner()
        }
    };
    if let Some(pool) = cell.get() {
        return Ok(pool);
    }
    let pool = initialize()?;
    if cell.set(pool).is_err() {
        return Err(Error::Protocol(
            "lsp pool initialized outside its synchronization gate".into(),
        ));
    }
    Ok(cell
        .get()
        .expect("global LSP pool must exist after successful initialization"))
}

/// Return the daemon-global LSP pool.
///
/// # Errors
/// Returns an LSP protocol error if the pool runtime cannot be
/// initialized.
pub fn global() -> Result<&'static LspClientPool> {
    initialize_global_pool(&GLOBAL_POOL, &GLOBAL_POOL_INIT, LspClientPool::new)
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
