//! Shared blocking helpers for data-RPC methods.
//!
//! Every snapshot-scoped symbol/navigation method (find_symbols,
//! get_outline, find_references, ...) routed through this helper
//! drives the same pipeline via [`query_one_or_all_snapshots`];
//! `list_repos` / `list_jobs` / `repo_status` build their own
//! per-repo pipelines instead:
//!
//! 1. Resolve one snapshot per repository via
//!    [`freshness::evaluate_snapshot`] inside the same SQLite read
//!    transaction that will run the query SQL, so an anchor move
//!    mid-query cannot mix rows from two manifests.
//! 2. Optionally verify that a required exact file is a member of the
//!    resolved manifest, run the caller's query closure against the
//!    pinned manifest, then commit the read transaction.
//! 3. Reopen the store on a fresh connection and call
//!    [`freshness::revalidate_snapshot`]; a changed durable
//!    fingerprint replaces the initial verdict.
//! 4. Build tier-3 status limited to the parser ids the returned rows
//!    actually touch, then compose diagnostics and hints so snapshot
//!    uncertainty outranks speculative empty-result advice.
//!
//! Multi-repository queries acquire per-repo leases individually so a
//! single `Removing` repository does not fail an unscoped scan;
//! explicitly requested repositories keep the strict "acquire or
//! error" contract of `acquire_by_repo_hash`.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
use std::future::Future;
#[cfg(test)]
use std::time::{Duration, Instant};

use cairn_proto::{
    AnalyzerState, Completeness, Diagnostic, DiagnosticCode, DiagnosticSeverity, Hint, HintAction,
    HintCode, PartialReason, ReasonCode, TierAnalyzerStatus, TierStatus, TierStatusBody,
    default_tier,
};
use rusqlite::{OptionalExtension, params};
use serde_json::json;

#[cfg(test)]
use crate::anchor;
use crate::cas::{registry as cas_registry, store as cas_store};
use crate::freshness::{self, EvaluatedSnapshot, SnapshotFreshness};
use crate::manifest::ManifestId;
use crate::workspace_analyzer::{
    WorkspaceAnalyzer, expected_analyzers_for_manifest, manifest_parser_ids,
};
use crate::{Error, Result};

use super::DataCtx;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryPhase {
    DispatchTotal,
    IndexOpen,
    IndexEnumeration,
    AliasResolution,
    LeaseAcquire,
    SnapshotEvaluate,
    Membership,
    StrictSql,
    FallbackSql,
    Commit,
    Finalize,
    LeaseRelease,
    TierFreshnessSecondPass,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryPhaseEvent {
    pub(crate) trace_id: &'static str,
    pub(crate) phase: QueryPhase,
    pub(crate) elapsed: Duration,
    pub(crate) rows: Option<usize>,
    pub(crate) cardinality: Option<usize>,
    pub(crate) connection_ordinal: usize,
    pub(crate) repo_ordinal: Option<usize>,
    pub(crate) executed: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapturedQueryKind {
    Strict,
    Fallback,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestQueryVariant {
    RejoinOracle,
    ProductionCarry,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CapturedQuerySql {
    pub(crate) kind: CapturedQueryKind,
    pub(crate) sql: String,
    pub(crate) params: Vec<rusqlite::types::Value>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryPhaseObserver {
    trace_id: &'static str,
    connection_ordinal: usize,
    events: Mutex<Vec<QueryPhaseEvent>>,
    sql: Mutex<Vec<CapturedQuerySql>>,
    variant: TestQueryVariant,
    capture_sql_only: bool,
}

#[cfg(test)]
impl QueryPhaseObserver {
    pub(crate) fn new(trace_id: &'static str, connection_ordinal: usize) -> Arc<Self> {
        Arc::new(Self {
            trace_id,
            connection_ordinal,
            events: Mutex::new(Vec::new()),
            sql: Mutex::new(Vec::new()),
            variant: TestQueryVariant::ProductionCarry,
            capture_sql_only: false,
        })
    }

    pub(crate) fn with_variant(
        trace_id: &'static str,
        connection_ordinal: usize,
        variant: TestQueryVariant,
    ) -> Arc<Self> {
        Arc::new(Self {
            trace_id,
            connection_ordinal,
            events: Mutex::new(Vec::new()),
            sql: Mutex::new(Vec::new()),
            variant,
            capture_sql_only: false,
        })
    }

    pub(crate) fn capture_sql_only(
        trace_id: &'static str,
        connection_ordinal: usize,
        variant: TestQueryVariant,
    ) -> Arc<Self> {
        Arc::new(Self {
            trace_id,
            connection_ordinal,
            events: Mutex::new(Vec::new()),
            sql: Mutex::new(Vec::new()),
            variant,
            capture_sql_only: true,
        })
    }

    pub(crate) fn events(&self) -> Vec<QueryPhaseEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn captured_sql(&self) -> Vec<CapturedQuerySql> {
        self.sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn record(
        &self,
        phase: QueryPhase,
        elapsed: Duration,
        rows: Option<usize>,
        cardinality: Option<usize>,
        repo_ordinal: Option<usize>,
        executed: bool,
    ) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(QueryPhaseEvent {
                trace_id: self.trace_id,
                phase,
                elapsed,
                rows,
                cardinality,
                connection_ordinal: self.connection_ordinal,
                repo_ordinal,
                executed,
            });
    }
}

#[cfg(test)]
tokio::task_local! {
    static QUERY_PHASE_OBSERVER: Arc<QueryPhaseObserver>;
}

#[cfg(test)]
thread_local! {
    static BLOCKING_QUERY_PHASE_OBSERVER: RefCell<Option<Arc<QueryPhaseObserver>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
pub(crate) async fn with_query_phase_observer<F, T>(
    observer: Arc<QueryPhaseObserver>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    QUERY_PHASE_OBSERVER.scope(observer, future).await
}

#[cfg(test)]
struct BlockingObserverGuard(Option<Arc<QueryPhaseObserver>>);

#[cfg(test)]
impl Drop for BlockingObserverGuard {
    fn drop(&mut self) {
        BLOCKING_QUERY_PHASE_OBSERVER.with(|slot| {
            *slot.borrow_mut() = self.0.take();
        });
    }
}

#[cfg(test)]
fn install_blocking_query_phase_observer(
    observer: Option<Arc<QueryPhaseObserver>>,
) -> BlockingObserverGuard {
    let previous = BLOCKING_QUERY_PHASE_OBSERVER.with(|slot| slot.replace(observer));
    BlockingObserverGuard(previous)
}

#[cfg(test)]
pub(crate) fn record_test_query_phase(
    phase: QueryPhase,
    started: Instant,
    rows: Option<usize>,
    cardinality: Option<usize>,
    repo_ordinal: Option<usize>,
    executed: bool,
) {
    BLOCKING_QUERY_PHASE_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow().as_ref() {
            observer.record(
                phase,
                started.elapsed(),
                rows,
                cardinality,
                repo_ordinal,
                executed,
            );
        }
    });
}

#[cfg(test)]
pub(crate) fn record_test_query_sql(
    kind: CapturedQueryKind,
    sql: &str,
    params: Vec<rusqlite::types::Value>,
) {
    BLOCKING_QUERY_PHASE_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow().as_ref() {
            observer
                .sql
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(CapturedQuerySql {
                    kind,
                    sql: sql.to_string(),
                    params,
                });
        }
    });
}

#[cfg(test)]
pub(crate) fn current_test_query_variant() -> TestQueryVariant {
    BLOCKING_QUERY_PHASE_OBSERVER.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(TestQueryVariant::ProductionCarry, |observer| {
                observer.variant
            })
    })
}

#[cfg(test)]
pub(crate) fn test_query_capture_only() -> bool {
    BLOCKING_QUERY_PHASE_OBSERVER.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|observer| observer.capture_sql_only)
    })
}

#[cfg(test)]
fn active_query_phase_observer() -> Option<Arc<QueryPhaseObserver>> {
    QUERY_PHASE_OBSERVER.try_with(Arc::clone).ok()
}

/// One repository-level reason a snapshot could not be trusted for the
/// current query. `repo` is the alias (or `"*"` for the aggregate
/// "not indexed by any known repository" marker); `reason` is either a
/// [`SnapshotStaleReason`](crate::freshness) label or the sentinel
/// `"file_not_indexed"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryFreshnessIssue {
    pub(crate) repo: String,
    pub(crate) reason: &'static str,
}

/// Aggregated outcome of a query fan-out over one or more repositories.
///
/// `items` is the caller-shaped rows, already trimmed to the requested
/// limit. `capped` reflects whether truncation happened at any layer,
/// `skipped_unavailable` whether an enumerating scan silently skipped a
/// repository that could not be leased, and `freshness_issues` collects
/// per-repository staleness / membership problems seen either at
/// evaluation or during post-query revalidation.
#[derive(Debug)]
pub(crate) struct SnapshotQueryResult<T> {
    pub(crate) items: Vec<T>,
    pub(crate) capped: bool,
    pub(crate) skipped_unavailable: bool,
    pub(crate) tier3_status: TierStatus,
    pub(crate) freshness_issues: Vec<QueryFreshnessIssue>,
}

/// Parameters shared by every snapshot-scoped RPC. `requested_repo`
/// unset means "enumerate every registered repository" and switches the
/// engine into lease-skipping mode; `anchor`/`branch` follow
/// [`freshness::evaluate_snapshot`]'s resolution rules.
pub(crate) struct SnapshotQueryRequest {
    pub(crate) requested_repo: Option<String>,
    pub(crate) anchor: Option<String>,
    pub(crate) branch: Option<String>,
    pub(crate) method_name: &'static str,
    pub(crate) effective_limit: u32,
    pub(crate) verbose_tier3: bool,
    /// Exact repo-relative path whose membership is required before query SQL.
    /// Prefix/path-filter queries leave this unset.
    pub(crate) exact_file: Option<String>,
}

/// Bookkeeping the query engine carries across its two passes: what
/// repository was successfully queried and against which manifest, so
/// tier-3 status assembly and freshness revalidation can find the same
/// store again on a fresh connection.
struct CapturedSnapshot {
    entry: cas_registry::AliasEntry,
    selected: EvaluatedSnapshot,
}

struct QueryInterruptState {
    next_id: u64,
    handles: Vec<(u64, Arc<rusqlite::InterruptHandle>)>,
}

struct QueryInterruptRegistry {
    cancelled: AtomicBool,
    request_cancellation: Option<Arc<crate::daemon::RequestCancellation>>,
    state: Mutex<QueryInterruptState>,
}

impl crate::daemon::RequestCancelTarget for QueryInterruptRegistry {
    fn cancel(&self) {
        self.cancel();
        #[cfg(test)]
        if let Some(cancellation) = self.request_cancellation.as_ref() {
            cancellation.mark_cooperative_cancel_started();
        }
    }
}

impl QueryInterruptRegistry {
    fn new(request_cancellation: Option<Arc<crate::daemon::RequestCancellation>>) -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            request_cancellation,
            state: Mutex::new(QueryInterruptState {
                next_id: 0,
                handles: Vec::new(),
            }),
        })
    }

    fn register(self: &Arc<Self>, connection: &rusqlite::Connection) -> RegisteredQueryInterrupt {
        let handle = Arc::new(connection.get_interrupt_handle());
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.is_cancelled() {
            drop(state);
            handle.interrupt();
            return RegisteredQueryInterrupt {
                registry: Arc::clone(self),
                id: None,
            };
        }
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.handles.push((id, handle));
        RegisteredQueryInterrupt {
            registry: Arc::clone(self),
            id: Some(id),
        }
    }

    fn cancel(&self) {
        let handles = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            self.cancelled.store(true, Ordering::Release);
            state
                .handles
                .iter()
                .map(|(_, handle)| Arc::clone(handle))
                .collect::<Vec<_>>()
        };
        for handle in handles {
            handle.interrupt();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || self
                .request_cancellation
                .as_ref()
                .is_some_and(|cancellation| cancellation.is_cancelled())
    }

    fn unregister(&self, id: u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.handles.retain(|(registered, _)| *registered != id);
    }
}

struct RegisteredQueryInterrupt {
    registry: Arc<QueryInterruptRegistry>,
    id: Option<u64>,
}

struct BlockingQueryCancellation {
    interrupts: Option<Arc<QueryInterruptRegistry>>,
    target_registration: Option<crate::daemon::RequestCancelTargetGuard>,
    drain: Option<crate::daemon::RequestDrainGuard>,
    #[cfg(test)]
    start_barrier: Option<crate::daemon::BlockingQueryStartBarrier>,
}

impl Drop for BlockingQueryCancellation {
    fn drop(&mut self) {
        // A blocking closure may unwind. Retire cancellation targeting only
        // after its SQLite resources have unwound, and retire the active drain
        // last so `drained()` always means resource ownership reached zero.
        drop(self.interrupts.take());
        drop(self.target_registration.take());
        drop(self.drain.take());
    }
}

impl Drop for RegisteredQueryInterrupt {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.registry.unregister(id);
        }
    }
}

fn ensure_query_not_cancelled(registry: &QueryInterruptRegistry) -> Result<()> {
    if registry.is_cancelled() {
        Err(Error::Internal("request cancelled".into()))
    } else {
        Ok(())
    }
}

/// Execute a query against one immutable manifest per repository.
///
/// Anchor resolution and the query itself share one SQLite read transaction,
/// so an anchor move cannot mix rows from two manifests. Tier status is then
/// evaluated against the captured manifest ids rather than resolving anchors
/// a second time. Finally, current-snapshot freshness is revalidated against
/// both databases before the response is assembled.
pub(crate) async fn query_one_or_all_snapshots<T, F, P, S>(
    ctx: &DataCtx,
    request: SnapshotQueryRequest,
    mut query_store: F,
    parser_ids_for_items: P,
    mut finalize: S,
) -> Result<SnapshotQueryResult<T>>
where
    T: Send + 'static,
    F: FnMut(
            &cas_registry::AliasEntry,
            &rusqlite::Connection,
            &EvaluatedSnapshot,
        ) -> Result<Vec<T>>
        + Send
        + 'static,
    P: Fn(&[T]) -> BTreeSet<String> + Send + 'static,
    S: FnMut(&mut Vec<T>) + Send + 'static,
{
    let cas_data_dir = ctx.cas_data_dir.clone();
    let lifecycle = ctx.lifecycle.clone();
    let method_name = request.method_name;
    #[cfg(test)]
    let phase_observer = active_query_phase_observer();
    let cancellation = crate::daemon::current_request_cancellation();
    let interrupts = QueryInterruptRegistry::new(cancellation.clone());
    let drain = match cancellation.as_ref() {
        Some(cancellation) => Some(
            cancellation
                .admit()
                .ok_or_else(|| Error::Internal("request cancelled".into()))?,
        ),
        None => None,
    };
    let target_registration = cancellation.as_ref().map(|cancellation| {
        let target: Arc<dyn crate::daemon::RequestCancelTarget> = interrupts.clone();
        cancellation.register_target(&target)
    });
    let ownership = BlockingQueryCancellation {
        interrupts: Some(interrupts),
        target_registration,
        drain,
        #[cfg(test)]
        start_barrier: cancellation
            .as_ref()
            .and_then(|cancellation| cancellation.take_blocking_query_start_barrier()),
    };
    let task = tokio::task::spawn_blocking(move || -> Result<SnapshotQueryResult<T>> {
        #[cfg(test)]
        let _phase_observer_guard = install_blocking_query_phase_observer(phase_observer);
        let ownership = ownership;
        #[cfg(test)]
        if let Some(barrier) = ownership.start_barrier.as_ref() {
            let _ = barrier.started.send(());
            let _ = barrier.release.recv();
        }
        let blocking_interrupts = Arc::clone(
            ownership
                .interrupts
                .as_ref()
                .expect("blocking cancellation ownership missing registry"),
        );
        // Locals declared after `ownership` unwind first. The explicit normal
        // path below mirrors that order; `BlockingQueryCancellation::drop`
        // then retires registration before the drain even on panic.
        ensure_query_not_cancelled(&blocking_interrupts)?;
        #[cfg(test)]
        let phase_started = Instant::now();
        let index = cas_registry::open(&cas_data_dir.index_db_path())?;
        #[cfg(test)]
        record_test_query_phase(
            QueryPhase::IndexOpen,
            phase_started,
            Some(1),
            Some(1),
            None,
            true,
        );
        let _index_interrupt = blocking_interrupts.register(&index);
        ensure_query_not_cancelled(&blocking_interrupts)?;
        // Enumeration mode makes lease acquisition, missing-anchor
        // handling and file-not-indexed reporting soft-fail per repo;
        // an explicit request keeps them hard.
        let enumerate_all = request.requested_repo.is_none();
        #[cfg(test)]
        let alias_started = Instant::now();
        let aliases = match request.requested_repo.as_deref() {
            Some(name) => {
                let entry = cas_registry::lookup_by_alias(&index, name)?.ok_or_else(|| {
                    Error::RepoNotFound {
                        alias: name.to_string(),
                    }
                })?;
                vec![entry]
            }
            None => cas_registry::list_all(&index)?,
        };
        #[cfg(test)]
        {
            record_test_query_phase(
                QueryPhase::IndexEnumeration,
                alias_started,
                request.requested_repo.is_none().then_some(aliases.len()),
                Some(aliases.len()),
                None,
                request.requested_repo.is_none(),
            );
            record_test_query_phase(
                QueryPhase::AliasResolution,
                alias_started,
                Some(aliases.len()),
                Some(aliases.len()),
                None,
                true,
            );
        }

        let mut out = Vec::new();
        let mut captured = Vec::new();
        let mut capped = false;
        let mut skipped_unavailable = false;
        let mut freshness_issues = Vec::new();
        let mut exact_member_found = false;
        // Test-only phase ordinals consume the enumerate index.
        #[cfg_attr(not(test), allow(clippy::unused_enumerate_index))]
        for (_repo_ordinal, entry) in aliases.into_iter().enumerate() {
            #[cfg(test)]
            let repo_ordinal = _repo_ordinal;
            ensure_query_not_cancelled(&blocking_interrupts)?;
            // Enumeration acquires per-repo so a single Removing owner
            // does not fail the whole scan; explicit requests keep the
            // typed RepositoryUnavailable error.
            #[cfg(test)]
            let phase_started = Instant::now();
            let lease = match &lifecycle {
                Some(lifecycle) if enumerate_all => {
                    let Some(lease) = lifecycle.acquire_for_enumeration(&entry.repo_hash)? else {
                        skipped_unavailable = true;
                        continue;
                    };
                    Some(lease)
                }
                Some(lifecycle) => Some(lifecycle.acquire_by_repo_hash(&entry.repo_hash)?),
                None => None,
            };
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::LeaseAcquire,
                phase_started,
                Some(usize::from(lease.is_some())),
                Some(1),
                Some(repo_ordinal),
                lifecycle.is_some(),
            );
            let store_path = cas_data_dir.store_db_path(&entry.repo_hash);
            let mut conn = cas_store::open_existing(&store_path)?;
            let _store_interrupt = blocking_interrupts.register(&conn);
            ensure_query_not_cancelled(&blocking_interrupts)?;
            // One read transaction covers snapshot selection, membership
            // check and the query SQL. That is what pins the response to
            // a single manifest even if an anchor moves mid-query.
            let tx = conn.transaction()?;
            #[cfg(test)]
            let phase_started = Instant::now();
            let selected = match freshness::evaluate_snapshot(
                &index,
                &tx,
                &entry.repo_hash,
                request.anchor.as_deref(),
                request.branch.as_deref(),
                system_now_ns(),
            ) {
                Ok(snapshot) => snapshot,
                // A repo that has no matching anchor (e.g. explicit
                // branch present in only some repos) is silently skipped
                // in enumeration mode; explicit requests bubble the
                // error to the caller.
                Err(Error::AnchorNotFound { .. }) if enumerate_all => continue,
                Err(error) => return Err(error),
            };
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::SnapshotEvaluate,
                phase_started,
                Some(1),
                Some(1),
                Some(repo_ordinal),
                true,
            );
            #[cfg(test)]
            let membership_started = Instant::now();
            if let Some(file) = request.exact_file.as_deref() {
                // Membership is answered inside the same read
                // transaction, so it cannot disagree with the SQL below.
                let member = tx.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM manifest_entries
                         WHERE manifest_id = ?1 AND path = ?2
                     )",
                    params![selected.manifest_id.0, file],
                    |row| row.get::<_, bool>(0),
                )?;
                if !member {
                    // For a specific repo, the miss is the query's
                    // answer. For an enumerating scan, a single miss is
                    // not evidence — defer to the aggregate check below.
                    if !enumerate_all {
                        freshness_issues.push(QueryFreshnessIssue {
                            repo: entry.alias.clone(),
                            reason: "file_not_indexed",
                        });
                    }
                    continue;
                }
                exact_member_found = true;
            }
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::Membership,
                membership_started,
                request
                    .exact_file
                    .as_ref()
                    .map(|_| usize::from(exact_member_found)),
                Some(1),
                Some(repo_ordinal),
                request.exact_file.is_some(),
            );
            let mut hits = match query_store(&entry, &tx, &selected) {
                Ok(hits) => hits,
                Err(Error::AnchorNotFound { .. }) => continue,
                Err(other) => return Err(other),
            };
            ensure_query_not_cancelled(&blocking_interrupts)?;
            #[cfg(test)]
            let phase_started = Instant::now();
            tx.commit()?;
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::Commit,
                phase_started,
                Some(hits.len()),
                Some(hits.len()),
                Some(repo_ordinal),
                true,
            );
            capped |= trim_to_requested_limit(&mut hits, request.effective_limit);
            out.extend(hits);
            captured.push(CapturedSnapshot { entry, selected });
            #[cfg(test)]
            let phase_started = Instant::now();
            drop(lease);
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::LeaseRelease,
                phase_started,
                Some(1),
                Some(1),
                Some(repo_ordinal),
                lifecycle.is_some(),
            );
        }
        // Aggregate marker: no repository in the fan-out carries this
        // path. Reported once with the `*` alias so callers can
        // distinguish it from a per-repo miss.
        if enumerate_all && request.exact_file.is_some() && !exact_member_found {
            freshness_issues.push(QueryFreshnessIssue {
                repo: "*".into(),
                reason: "file_not_indexed",
            });
        }

        // Caller-supplied merging / sorting / de-duplication may drop
        // rows; re-apply the request cap so the cross-repo total obeys
        // the same limit each per-repo pass already honoured.
        #[cfg(test)]
        let phase_started = Instant::now();
        #[cfg(test)]
        let before_finalize = out.len();
        finalize(&mut out);
        capped |= trim_to_requested_limit(&mut out, request.effective_limit);
        #[cfg(test)]
        record_test_query_phase(
            QueryPhase::Finalize,
            phase_started,
            Some(out.len()),
            Some(before_finalize),
            None,
            true,
        );
        // Only tier-3 analyzers whose parser touched a surviving row
        // matter for query-scoped status; the finalize step above may
        // have removed rows whose analyzers are otherwise irrelevant.
        let relevant_parser_ids = parser_ids_for_items(&out);

        let mut analyzers = Vec::new();
        let mut repo_wide_analyzers = Vec::new();
        // Test-only phase ordinals consume the enumerate index.
        #[cfg_attr(not(test), allow(clippy::unused_enumerate_index))]
        for (_repo_ordinal, mut snapshot) in captured.into_iter().enumerate() {
            #[cfg(test)]
            let repo_ordinal = _repo_ordinal;
            ensure_query_not_cancelled(&blocking_interrupts)?;
            // First-pass leases are released before result finalization. Reacquire here so an
            // all-repository query skips a repo that entered Removing between passes, while an
            // explicitly requested repo retains the typed unavailable error contract.
            #[cfg(test)]
            let phase_started = Instant::now();
            let lease = match &lifecycle {
                Some(lifecycle) if enumerate_all => {
                    let Some(lease) =
                        lifecycle.acquire_for_enumeration(&snapshot.entry.repo_hash)?
                    else {
                        skipped_unavailable = true;
                        continue;
                    };
                    Some(lease)
                }
                Some(lifecycle) => Some(lifecycle.acquire_by_repo_hash(&snapshot.entry.repo_hash)?),
                None => None,
            };
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::LeaseAcquire,
                phase_started,
                Some(usize::from(lease.is_some())),
                Some(1),
                Some(repo_ordinal),
                lifecycle.is_some(),
            );
            #[cfg(test)]
            let second_pass_started = Instant::now();
            let store_path = cas_data_dir.store_db_path(&snapshot.entry.repo_hash);
            let conn = cas_store::open_existing(&store_path)?;
            let _store_interrupt = blocking_interrupts.register(&conn);
            ensure_query_not_cancelled(&blocking_interrupts)?;
            analyzers.extend(
                compute_tier_status_for_parser_ids(
                    &conn,
                    snapshot.selected.manifest_id,
                    Some(&relevant_parser_ids),
                )?
                .analyzers,
            );
            if request.verbose_tier3 {
                repo_wide_analyzers.extend(
                    compute_tier_status(&conn, snapshot.selected.manifest_id)?
                        .this_query
                        .analyzers,
                );
            }
            // Phase 2 of the freshness two-phase protocol: reread the
            // durable fingerprint on a fresh connection. Only default
            // current-worktree snapshots participate; explicit anchors
            // short-circuit to `Explicit` inside `revalidate_snapshot`.
            snapshot.selected.freshness = freshness::revalidate_snapshot(
                &index,
                &conn,
                &snapshot.entry.repo_hash,
                &snapshot.selected,
                system_now_ns(),
            )?;
            ensure_query_not_cancelled(&blocking_interrupts)?;
            if let SnapshotFreshness::Stale(reason) = snapshot.selected.freshness {
                freshness_issues.push(QueryFreshnessIssue {
                    repo: snapshot.entry.alias,
                    reason: reason.as_str(),
                });
            }
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::TierFreshnessSecondPass,
                second_pass_started,
                Some(relevant_parser_ids.len()),
                Some(1),
                Some(repo_ordinal),
                true,
            );
            #[cfg(test)]
            let phase_started = Instant::now();
            drop(lease);
            #[cfg(test)]
            record_test_query_phase(
                QueryPhase::LeaseRelease,
                phase_started,
                Some(1),
                Some(1),
                Some(repo_ordinal),
                lifecycle.is_some(),
            );
        }
        analyzers.sort();
        analyzers.dedup();
        let mut tier3_status = TierStatus::from_body(TierStatusBody::from_analyzers(analyzers));
        if request.verbose_tier3 {
            repo_wide_analyzers.sort();
            repo_wide_analyzers.dedup();
            tier3_status =
                tier3_status.with_repo_wide(TierStatusBody::from_analyzers(repo_wide_analyzers));
        }
        // Any freshness issue — stale reason or missing file membership —
        // downgrades `ready` on both slices. Consumers reading tier3
        // readiness cannot conclude "index up to date" while a snapshot
        // guarantee has been lost.
        if !freshness_issues.is_empty() {
            tier3_status.this_query.ready = false;
            if let Some(repo_wide) = &mut tier3_status.repo_wide {
                repo_wide.ready = false;
            }
        }

        let result = SnapshotQueryResult {
            items: out,
            capped,
            skipped_unavailable,
            tier3_status,
            freshness_issues,
        };
        drop(_index_interrupt);
        drop(index);
        drop(blocking_interrupts);
        drop(ownership);
        Ok(result)
    });

    let outcome = task.await;
    let result = outcome.map_err(|error| Error::internal_task_panic(method_name, error))?;
    if cancellation
        .as_ref()
        .is_some_and(|cancellation| cancellation.is_cancelled())
    {
        return Err(Error::Internal("request cancelled".into()));
    }
    result
}

/// Wall-clock read used to time freshness evaluation and revalidation.
/// A clock jump before `UNIX_EPOCH` (impossible on a sane host) falls
/// back to `0`; an overflow past `i64::MAX` saturates. Both callers
/// use `saturating_sub` to tolerate the resulting non-monotonicity.
pub(crate) fn system_now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Fetch one extra row beyond the caller-requested limit so
/// [`trim_to_requested_limit`] can distinguish "exactly at the limit"
/// (complete) from "more available" (cap-truncated).
pub(crate) fn limit_with_probe(effective_limit: u32) -> u32 {
    effective_limit.saturating_add(1)
}

/// Truncate `rows` down to the caller-requested limit. Returns `true`
/// when at least one row was dropped, which the pipeline forwards as
/// the cap-truncated completeness signal.
pub(crate) fn trim_to_requested_limit<T>(rows: &mut Vec<T>, effective_limit: u32) -> bool {
    let requested = effective_limit as usize;
    if rows.len() > requested {
        rows.truncate(requested);
        true
    } else {
        false
    }
}

/// Completeness for tools that do not participate in freshness
/// revalidation. `repo_unavailable` takes priority over `cap` because
/// a skipped repository is a stronger uncertainty than a truncated but
/// complete-within-scope result.
pub(crate) fn completeness_for_scan(capped: bool, skipped_unavailable: bool) -> Completeness {
    if skipped_unavailable {
        Completeness::partial_truncated(PartialReason::from("repo_unavailable"))
    } else if capped {
        Completeness::partial_truncated(PartialReason::Cap)
    } else {
        Completeness::complete()
    }
}

/// Snapshot-aware variant of [`completeness_for_scan`]. Any freshness
/// issue — stale reason or missing file membership — outranks both
/// `repo_unavailable` and `cap`, since a snapshot guarantee is a
/// broader failure than a per-repo skip or a row-count cap.
pub(crate) fn completeness_for_snapshot_scan(
    capped: bool,
    skipped_unavailable: bool,
    freshness_issues: &[QueryFreshnessIssue],
) -> Completeness {
    if !freshness_issues.is_empty() {
        Completeness::partial_truncated("file_not_indexed_or_snapshot_stale")
    } else {
        completeness_for_scan(capped, skipped_unavailable)
    }
}

/// Every input the diagnostic/hint builders need without carrying the
/// raw parameter struct of each tool. Callers assemble this once and
/// hand it to [`build_diagnostics`] / [`build_hints`] /
/// [`build_snapshot_aware_feedback`].
#[derive(Clone, Copy)]
pub(crate) struct EmissionContext<'a> {
    pub(crate) tool: QueryToolKind,
    pub(crate) items_empty: bool,
    pub(crate) completeness: &'a Completeness,
    pub(crate) tier3_status: &'a TierStatus,
    pub(crate) query_args: QueryArgsView<'a>,
}

/// Which query tool produced the result. Selects the copy and the
/// relax-drop candidate list in [`tool_metadata`], and gates a couple
/// of tool-specific hints (e.g. `GetOutline` for directory outlines).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryToolKind {
    FindSymbols,
    GetOutline,
    GetSymbolSource,
    FindReferences,
    FindCallers,
    FindCallees,
    FindSubtypes,
    FindSupertypes,
    FindImports,
}

/// The subset of caller arguments the hint builders inspect: whether a
/// given filter was set, and the string value of the identifier-like
/// filters (repo/container/path/file). Boolean fields flag "the caller
/// explicitly narrowed by this parameter"; the value itself is not
/// needed to advise dropping it.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QueryArgsView<'a> {
    pub(crate) repo: Option<&'a str>,
    pub(crate) fuzzy: bool,
    pub(crate) kind: bool,
    pub(crate) container: Option<&'a str>,
    pub(crate) path: Option<&'a str>,
    pub(crate) file: Option<&'a str>,
    pub(crate) max_depth: bool,
    pub(crate) direction: bool,
}

impl QueryArgsView<'_> {
    /// Which of the tool's declared relax-drop candidates the caller
    /// actually set. Only these can be suggested for removal — dropping
    /// a filter that was never set would be noise.
    fn filter_drop_params(&self, metadata: &ToolHintMetadata) -> Vec<String> {
        let mut params = Vec::new();
        for candidate in metadata.relax_drop_candidates {
            let set = match *candidate {
                "repo" => self.repo.is_some_and(|value| !value.is_empty()),
                "kind" => self.kind,
                "container" => self.container.is_some_and(|value| !value.is_empty()),
                "path" => self.path.is_some_and(|value| !value.is_empty()),
                "file" => self.file.is_some_and(|value| !value.is_empty()),
                "max_depth" => self.max_depth,
                "direction" => self.direction,
                "fuzzy" => self.fuzzy,
                _ => false,
            };
            if set {
                params.push((*candidate).to_string());
            }
        }
        params
    }

    fn has_relax_filters(&self, metadata: &ToolHintMetadata) -> bool {
        !self.filter_drop_params(metadata).is_empty()
    }

    /// `get_outline` with `path` set and `file` unset — a directory
    /// walk that gets a dedicated cap hint because narrowing by
    /// `max_depth` or `kind` is usually more helpful than raising
    /// `limit`.
    fn is_directory_outline(&self) -> bool {
        self.path.is_some_and(|value| !value.is_empty()) && self.file.is_none()
    }
}

/// Static copy per tool: the wire name, the noun to use when narrating
/// counts, and the ordered list of filters worth suggesting for
/// removal when the result is empty.
struct ToolHintMetadata {
    tool: &'static str,
    result_noun: &'static str,
    relax_drop_candidates: &'static [&'static str],
}

/// Per-tool hint copy and relax-drop candidate ordering. Kept in one
/// place so the wire strings stay consistent with the JSON-RPC method
/// names advertised by each [`DataMethod`].
fn tool_metadata(kind: QueryToolKind) -> ToolHintMetadata {
    match kind {
        QueryToolKind::FindSymbols => ToolHintMetadata {
            tool: "find_symbols",
            result_noun: "symbols",
            relax_drop_candidates: &["kind", "container", "path"],
        },
        QueryToolKind::GetOutline => ToolHintMetadata {
            tool: "get_outline",
            result_noun: "outline items",
            relax_drop_candidates: &["kind", "max_depth", "path", "file"],
        },
        QueryToolKind::GetSymbolSource => ToolHintMetadata {
            tool: "get_symbol_source",
            result_noun: "source results",
            relax_drop_candidates: &["file"],
        },
        QueryToolKind::FindReferences => ToolHintMetadata {
            tool: "find_references",
            result_noun: "references",
            relax_drop_candidates: &["kind", "direction"],
        },
        QueryToolKind::FindCallers => ToolHintMetadata {
            tool: "find_callers",
            result_noun: "callers",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindCallees => ToolHintMetadata {
            tool: "find_callees",
            result_noun: "callees",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindSubtypes => ToolHintMetadata {
            tool: "find_subtypes",
            result_noun: "subtypes",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindSupertypes => ToolHintMetadata {
            tool: "find_supertypes",
            result_noun: "supertypes",
            relax_drop_candidates: &[],
        },
        QueryToolKind::FindImports => ToolHintMetadata {
            tool: "find_imports",
            result_noun: "imports",
            relax_drop_candidates: &["file"],
        },
    }
}

/// Diagnostics that describe *why* a response is incomplete or degraded.
///
/// Partial completeness with a `Cap` reason is deliberately not emitted
/// as an error diagnostic — it is a normal outcome surfaced as a hint
/// instead. Every other partial reason becomes a `QueryFailedPartial`
/// error, and each analyzer status the caller's rows depend on is
/// mapped through [`diagnostic_for_analyzer`].
pub(crate) fn build_diagnostics(ctx: &EmissionContext<'_>) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if let Completeness::Partial { reason, .. } = ctx.completeness
        && !matches!(reason, Some(PartialReason::Cap))
    {
        diagnostics.push(Diagnostic {
            code: DiagnosticCode::QueryFailedPartial,
            severity: DiagnosticSeverity::Error,
            message: "query returned partial results".into(),
            language: None,
            analyzer_id: None,
            repo: ctx.query_args.repo.map(str::to_string),
            file: None,
            details: reason
                .as_ref()
                .map(|reason| json!({ "reason": reason.as_str() })),
        });
    }

    diagnostics.extend(
        ctx.tier3_status
            .this_query
            .analyzers
            .iter()
            .filter_map(diagnostic_for_analyzer),
    );
    diagnostics
}

/// Actionable next-step hints for the calling agent. Emits, in order:
/// cap-relief advice when truncation happened, an indexing-in-progress
/// notice while tier-3 analyzers are queued or running, empty-result
/// advice (relax filters, try fuzzy, widen repo scope) that the
/// snapshot-aware layer may later suppress, and a per-repo reindex
/// nudge when a tier-3 run was expected but not recorded.
pub(crate) fn build_hints(ctx: &EmissionContext<'_>) -> Vec<Hint> {
    let mut hints = Vec::new();
    let analyzers = &ctx.tier3_status.this_query.analyzers;
    let metadata = tool_metadata(ctx.tool);

    if matches!(
        ctx.completeness,
        Completeness::Partial {
            reason: Some(PartialReason::Cap),
            ..
        }
    ) {
        if ctx.tool == QueryToolKind::GetOutline && ctx.query_args.is_directory_outline() {
            hints.push(Hint {
                code: HintCode::CappedNarrowFilter,
                message:
                    "Outline result was capped. Try narrowing with kind=... or reducing max_depth."
                        .into(),
                action: None,
                tool: Some(metadata.tool.into()),
                params: Some(json!({ "narrow_candidates": ["kind", "max_depth"] })),
                drop_params: Vec::new(),
                target: None,
            });
        }
        hints.push(Hint {
            code: HintCode::CappedIncreaseLimit,
            message: format!("Increase `limit` to see more {}.", metadata.result_noun),
            action: Some(HintAction::IncreaseLimit),
            tool: Some(metadata.tool.into()),
            params: None,
            drop_params: Vec::new(),
            target: None,
        });
    }

    if analyzers.iter().any(|analyzer| {
        matches!(
            analyzer.state,
            AnalyzerState::Queued | AnalyzerState::Running
        )
    }) {
        hints.push(Hint {
            code: HintCode::Tier3IndexingWait,
            message: "Tier-3 indexing is still running for this query.".into(),
            action: Some(HintAction::WaitForIndex),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some("tier3".into()),
        });
    }

    if ctx.items_empty {
        let drop_params = ctx.query_args.filter_drop_params(&metadata);
        if ctx.query_args.has_relax_filters(&metadata) {
            let joined = drop_params.join(", ");
            hints.push(Hint {
                code: HintCode::EmptyResultRelaxFilter,
                message: format!(
                    "No {} matched. Try dropping {joined}.",
                    metadata.result_noun
                ),
                action: Some(HintAction::RelaxFilter),
                tool: Some(metadata.tool.into()),
                params: None,
                drop_params,
                target: None,
            });
        } else if !ctx.query_args.fuzzy {
            hints.push(Hint {
                code: HintCode::EmptyResultTryFuzzy,
                message: format!(
                    "No {} matched. Try fuzzy=true or a prefix wildcard.",
                    metadata.result_noun
                ),
                action: Some(HintAction::TryAlternativeQuery),
                tool: Some(metadata.tool.into()),
                params: Some(json!({ "fuzzy": true })),
                drop_params: Vec::new(),
                target: None,
            });
        }

        if ctx.query_args.repo.is_some() {
            hints.push(Hint {
                code: HintCode::EmptyResultWidenScope,
                message: format!(
                    "No {} matched. Try widening repo scope.",
                    metadata.result_noun
                ),
                action: Some(HintAction::WidenScope),
                tool: Some(metadata.tool.into()),
                params: None,
                drop_params: vec!["repo".into()],
                target: None,
            });
        }
    }

    if analyzers.iter().any(|analyzer| {
        analyzer.state == AnalyzerState::Missing
            && analyzer.reason_code == Some(ReasonCode::NotScheduled)
    }) {
        hints.push(Hint {
            code: HintCode::Tier3UnavailableAlternative,
            message: "Tier-3 data is unavailable for this query; use syntactic results or try a broader query.".into(),
            action: Some(HintAction::TryAlternativeQuery),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some("tier3".into()),
        });
    }

    if analyzers.iter().any(|analyzer| {
        analyzer.state == AnalyzerState::Missing
            && analyzer.reason_code == Some(ReasonCode::NotRecorded)
    }) && let Some(repo) = ctx.query_args.repo
    {
        hints.push(Hint {
            code: HintCode::ReindexViaCli,
            message: format!("Run `cairn ctl repo reindex {repo}` to refresh Tier-3 status."),
            action: None,
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: Some(repo.to_string()),
        });
    }

    hints
}

/// Build query feedback while giving snapshot uncertainty priority over
/// speculative empty-result advice. Cap and analyzer signals remain visible.
///
/// When `freshness_issues` is empty this returns the default builders
/// unchanged. Otherwise it re-shapes the output so a caller does not
/// see conflicting advice — e.g. "your snapshot is stale" alongside
/// "try dropping this filter, it might match more rows":
///
/// * The generic `QueryFailedPartial` diagnostic is replaced by one
///   `FileNotIndexedOrSnapshotStale` per issue, tagged with the repo
///   alias (omitted for the `*` aggregate).
/// * Empty-result hints (relax/fuzzy/widen) are dropped because rows
///   truly missing from an untrusted snapshot say nothing about the
///   real filter shape.
/// * Cap-relief hints are re-derived from a synthesised cap context
///   and appended after the surviving indexing/reconcile hints (the
///   freshness advisory is then inserted at the very front), so the
///   final order is freshness advisory -> indexing/reconcile hints
///   -> cap-relief, and the caller still learns the result was
///   truncated.
pub(crate) fn build_snapshot_aware_feedback(
    ctx: &EmissionContext<'_>,
    freshness_issues: &[QueryFreshnessIssue],
    capped: bool,
) -> (Vec<Diagnostic>, Vec<Hint>) {
    let mut diagnostics = build_diagnostics(ctx);
    let mut hints = build_hints(ctx);
    append_missing_cap_hints(ctx, capped, &mut hints);
    if freshness_issues.is_empty() {
        return (diagnostics, hints);
    }

    // The freshness advisory is the precise story; drop the generic
    // "partial" diagnostic that would otherwise repeat it in weaker form.
    diagnostics.retain(|diagnostic| diagnostic.code != DiagnosticCode::QueryFailedPartial);
    diagnostics.extend(freshness_issues.iter().map(|issue| Diagnostic {
        code: DiagnosticCode::FileNotIndexedOrSnapshotStale,
        severity: DiagnosticSeverity::Warning,
        message: "The query used a file-missing or freshness-unverified current snapshot.".into(),
        language: None,
        analyzer_id: None,
        repo: (issue.repo != "*").then(|| issue.repo.clone()),
        file: ctx.query_args.file.map(str::to_string),
        details: Some(json!({ "reason": issue.reason })),
    }));
    // Empty-result advice assumes the row set reflects the query.
    // Under an untrusted snapshot it may just mean "we could not see
    // the rows"; suppress the whole family.
    hints.retain(|hint| {
        !matches!(
            hint.code,
            HintCode::EmptyResultRelaxFilter
                | HintCode::EmptyResultTryFuzzy
                | HintCode::EmptyResultWidenScope
        )
    });
    // Lead with the freshness advisory so agents see the "wait or
    // reindex" step before anything else.
    hints.insert(
        0,
        Hint {
            code: HintCode::FileNotIndexedOrSnapshotStale,
            message: "Wait for reconciliation or run `cairn ctl repo reindex <alias>` before trusting an empty file query.".into(),
            action: Some(HintAction::WaitForIndex),
            tool: None,
            params: None,
            drop_params: Vec::new(),
            target: ctx
                .query_args
                .repo
                .or(ctx.query_args.file)
                .map(str::to_string),
        },
    );
    hints.dedup_by_key(|hint| hint.code);
    (diagnostics, hints)
}

/// Preserve the independent truncation signal when another partial reason
/// owns the response's single completeness-reason slot.
fn append_missing_cap_hints(ctx: &EmissionContext<'_>, capped: bool, hints: &mut Vec<Hint>) {
    if !capped {
        return;
    }

    let cap = Completeness::partial_truncated(PartialReason::Cap);
    let cap_ctx = EmissionContext {
        completeness: &cap,
        ..*ctx
    };
    for hint in build_hints(&cap_ctx).into_iter().filter(|hint| {
        matches!(
            hint.code,
            HintCode::CappedIncreaseLimit | HintCode::CappedNarrowFilter
        )
    }) {
        if !hints.iter().any(|existing| existing.code == hint.code) {
            hints.push(hint);
        }
    }
}

/// Map one tier analyzer status onto a wire diagnostic, or `None` when
/// the analyzer is in a healthy state (`Ready`) or expresses no
/// problem the query should surface (`NotApplicable`, `Queued`,
/// `Running` are handled by hints instead). Falls through to a canned
/// message when the analyzer omitted its own free-form reason.
fn diagnostic_for_analyzer(analyzer: &TierAnalyzerStatus) -> Option<Diagnostic> {
    let (code, severity, fallback_message) = match (analyzer.state, analyzer.reason_code) {
        (AnalyzerState::Missing, Some(ReasonCode::NotRecorded)) => (
            DiagnosticCode::AnalyzerNotRecorded,
            DiagnosticSeverity::Warning,
            "analyzer run was not recorded",
        ),
        (AnalyzerState::Missing, Some(ReasonCode::NotScheduled)) => (
            DiagnosticCode::AnalyzerNotScheduled,
            DiagnosticSeverity::Warning,
            "expected analyzer was not scheduled",
        ),
        (AnalyzerState::Missing, Some(ReasonCode::AnalyzerFailed)) | (AnalyzerState::Failed, _) => {
            (
                DiagnosticCode::AnalyzerFailed,
                DiagnosticSeverity::Warning,
                "analyzer failed",
            )
        }
        (AnalyzerState::Missing, Some(ReasonCode::BinaryNotFound)) => (
            DiagnosticCode::AnalyzerBinaryMissing,
            DiagnosticSeverity::Warning,
            "analyzer binary is missing",
        ),
        (AnalyzerState::Stale, Some(ReasonCode::Stale | ReasonCode::StaleRevision))
        | (AnalyzerState::Stale, _) => (
            DiagnosticCode::AnalyzerStale,
            DiagnosticSeverity::Info,
            "analyzer result is stale",
        ),
        (AnalyzerState::Skipped, Some(ReasonCode::WorkspaceUnsuitable)) => (
            DiagnosticCode::WorkspaceUnsuitable,
            DiagnosticSeverity::Info,
            "workspace is unsuitable for this analyzer",
        ),
        _ => return None,
    };

    Some(Diagnostic {
        code,
        severity,
        message: analyzer
            .reason
            .clone()
            .unwrap_or_else(|| fallback_message.to_string()),
        language: Some(analyzer.language.clone()),
        analyzer_id: analyzer.id.clone(),
        repo: None,
        file: None,
        details: analyzer
            .reason_code
            .map(|reason_code| json!({ "reason_code": reason_code })),
    })
}

/// Collect parser ids into a sorted, unique set, discarding any empty
/// strings. Callers use it to distil "which analyzers actually matter
/// for this response" from a possibly noisy per-row iterator.
pub(crate) fn parser_id_filter<I>(parser_ids: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = String>,
{
    parser_ids
        .into_iter()
        .filter(|parser_id| !parser_id.is_empty())
        .collect::<BTreeSet<_>>()
}

/// Repo-wide tier-3 status: every analyzer expected for the manifest,
/// regardless of whether any returned row depends on it. Used for the
/// `repo_wide` slice in verbose responses.
pub(crate) fn compute_tier_status(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
) -> Result<TierStatus> {
    Ok(TierStatus::from_body(
        compute_tier_status_body_with_analyzers(
            conn,
            manifest_id,
            expected_analyzers_for_manifest(conn, manifest_id)?,
            None,
        )?,
    ))
}

/// Query-scoped tier-3 status: only analyzers whose parser id appears
/// in `parser_ids`. Passing `None` widens back to every analyzer the
/// manifest expects, matching [`compute_tier_status`].
pub(crate) fn compute_tier_status_for_parser_ids(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    parser_ids: Option<&BTreeSet<String>>,
) -> Result<TierStatusBody> {
    compute_tier_status_body_with_analyzers(
        conn,
        manifest_id,
        expected_analyzers_for_manifest(conn, manifest_id)?,
        parser_ids,
    )
}

#[cfg(test)]
fn compute_tier_status_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
) -> Result<TierStatus> {
    Ok(TierStatus::from_body(
        compute_tier_status_body_with_analyzers(conn, manifest_id, analyzers, None)?,
    ))
}

/// Core tier-3 status assembler. Walks the expected analyzer list,
/// projects each run row into a [`TierAnalyzerStatus`], and back-fills
/// a `NotApplicable` entry for any relevant parser id the manifest
/// contains but no analyzer covers. The double intersection with
/// `manifest_parser_ids` and `relevant_parser_ids` is what limits
/// output to "expected here AND useful for this response".
fn compute_tier_status_body_with_analyzers(
    conn: &rusqlite::Connection,
    manifest_id: ManifestId,
    analyzers: Vec<Box<dyn WorkspaceAnalyzer>>,
    relevant_parser_ids: Option<&BTreeSet<String>>,
) -> Result<TierStatusBody> {
    let manifest_parser_ids = manifest_parser_ids(conn, manifest_id)?;
    let manifest_parser_ids_sorted = manifest_parser_ids.iter().cloned().collect::<BTreeSet<_>>();
    // `None` here is the "no query-scope filter" signal: fall back to
    // every parser id the manifest carries so repo-wide callers see
    // the full picture.
    let relevant_parser_ids = relevant_parser_ids.unwrap_or(&manifest_parser_ids_sorted);
    let mut described_parser_ids = BTreeSet::new();
    let mut statuses = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT status, error, analyzer_revision FROM workspace_analysis_runs
         WHERE manifest_id = ?1 AND analyzer_id = ?2",
    )?;

    for analyzer in analyzers {
        let parser_id = analyzer.parser_id();
        // Skip analyzers whose parser is absent from the manifest or
        // outside the query scope; we only describe analyzers the
        // caller can act on.
        if !manifest_parser_ids.contains(parser_id) || !relevant_parser_ids.contains(parser_id) {
            continue;
        }
        described_parser_ids.insert(parser_id.to_string());
        let row = stmt
            .query_row(params![manifest_id.0, analyzer.id()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .optional()?;
        statuses.push(analyzer_status_from_run(
            analyzer.id(),
            analyzer.language(),
            analyzer.revision(),
            row,
        ));
    }

    // Back-fill parser ids that appear in the manifest and matter for
    // this response but that no registered analyzer covers. Emitting
    // `NotApplicable` here is what lets callers tell "we have no
    // analyzer for this language" apart from "the analyzer ran but
    // reported nothing".
    for parser_id in relevant_parser_ids {
        if !manifest_parser_ids.contains(parser_id) || described_parser_ids.contains(parser_id) {
            continue;
        }
        statuses.push(TierAnalyzerStatus {
            id: None,
            language: language_from_parser_id(parser_id),
            tier: default_tier(),
            state: AnalyzerState::NotApplicable,
            reason_code: Some(ReasonCode::NotApplicable),
            reason: Some("no tier3 analyzer for language".into()),
        });
    }
    statuses.sort();
    statuses.dedup();
    Ok(TierStatusBody::from_analyzers(statuses))
}

/// Translate one `workspace_analysis_runs` row into a wire status,
/// applying two priority rules: an absent row is always
/// `Missing/NotScheduled`, and a revision mismatch always wins over
/// the row's status (a run that succeeded against an old analyzer
/// revision is treated as `Stale`, not `Ready`).
fn analyzer_status_from_run(
    analyzer_id: &str,
    language: &str,
    expected_revision: u32,
    row: Option<(String, Option<String>, i64)>,
) -> TierAnalyzerStatus {
    let Some((status, error, revision)) = row else {
        return TierAnalyzerStatus {
            id: Some(analyzer_id.into()),
            language: language.into(),
            tier: default_tier(),
            state: AnalyzerState::Missing,
            reason_code: Some(ReasonCode::NotScheduled),
            reason: Some("expected analyzer was not scheduled for this manifest".into()),
        };
    };
    // Revision precedes status: a `succeeded` row against an outdated
    // analyzer revision is not `Ready`, it is `Stale`.
    if revision != i64::from(expected_revision) {
        return TierAnalyzerStatus {
            id: Some(analyzer_id.into()),
            language: language.into(),
            tier: default_tier(),
            state: AnalyzerState::Stale,
            reason_code: Some(ReasonCode::Stale),
            reason: Some(format!(
                "analyzer revision changed from {revision} to {expected_revision}"
            )),
        };
    }
    let (state, reason_code) = match status.as_str() {
        "succeeded" => (AnalyzerState::Ready, None),
        "queued" => (AnalyzerState::Queued, None),
        "running" => (AnalyzerState::Running, None),
        "skipped" => (
            AnalyzerState::Skipped,
            reason_code_for_error(&status, error.as_deref()),
        ),
        "timed_out" => (AnalyzerState::Failed, Some(ReasonCode::TimedOut)),
        "failed" => (AnalyzerState::Failed, Some(ReasonCode::AnalyzerFailed)),
        _ => (AnalyzerState::Failed, Some(ReasonCode::Unknown)),
    };
    TierAnalyzerStatus {
        id: Some(analyzer_id.into()),
        language: language.into(),
        tier: default_tier(),
        state,
        reason_code,
        reason: error.or_else(|| (status == "cancelled").then(|| "cancelled".into())),
    }
}

/// Coarse pattern classifier that promotes free-form analyzer error
/// text into a structured `ReasonCode`. The substring checks are
/// case-insensitive and deliberately generous: an unrecognised message
/// falls back to `Unknown` (or `None` when the run actually succeeded
/// but no code was provided) so a new failure mode never crashes the
/// dispatch.
fn reason_code_for_error(status: &str, error: Option<&str>) -> Option<ReasonCode> {
    let Some(error) = error else {
        return (status != "succeeded").then_some(ReasonCode::Unknown);
    };
    let lower = error.to_ascii_lowercase();
    if lower.contains("binary") && (lower.contains("missing") || lower.contains("not available")) {
        Some(ReasonCode::BinaryNotFound)
    } else if lower.contains("no matching files") {
        Some(ReasonCode::NoMatchingFiles)
    } else if lower.contains("workspace unsuitable")
        || lower.contains("gemfile without gemfile.lock")
    {
        Some(ReasonCode::WorkspaceUnsuitable)
    } else if lower.contains("stalled") || lower.contains("timed out") {
        Some(ReasonCode::TimedOut)
    } else {
        Some(ReasonCode::Unknown)
    }
}

/// Best-effort human-readable language label for a `tree-sitter-*`
/// parser id: strip the `tree-sitter-` prefix, normalise the `md`
/// shortcut to `markdown`, and drop the `-ng` next-generation
/// grammar suffix. Purely cosmetic — parser ids remain the identity
/// used everywhere else.
fn language_from_parser_id(parser_id: &str) -> String {
    let language = parser_id.strip_prefix("tree-sitter-").unwrap_or(parser_id);
    if language == "md" {
        return "markdown".into();
    }
    language.strip_suffix("-ng").unwrap_or(language).to_string()
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use cairn_proto::Completeness;
    use rusqlite::params;
    use serde_json::Value;

    use crate::anchor;
    use crate::cas::{registry as cas_registry, store as cas_store};
    use crate::paths::{CasDataDir, path_hash};
    use crate::register::register_repo;
    use crate::testutil::init_repo;

    use super::DataCtx;
    use crate::data_rpc::DataMethod;

    pub(crate) struct DataRpcFixture {
        pub(crate) _repo: tempfile::TempDir,
        pub(crate) _data: tempfile::TempDir,
        pub(crate) ctx: DataCtx,
    }

    pub(crate) fn registered_fixture() -> DataRpcFixture {
        registered_fixture_with_files(&[(
            "src/lib.rs",
            "use std::fmt;\n\
             use std::fs;\n\
             use std::io;\n\
             pub trait Trait {}\n\
             pub struct A;\n\
             pub struct B;\n\
             pub struct C;\n\
             impl Trait for A {}\n\
             impl Trait for B {}\n\
             impl Trait for C {}\n\
             pub fn target() {}\n\
             pub fn caller_a() { target(); }\n\
             pub fn caller_b() { target(); }\n\
             pub fn caller_c() { target(); }\n",
        )])
    }

    pub(crate) fn registered_fixture_with_files(files: &[(&str, &str)]) -> DataRpcFixture {
        let (repo, _sha) = init_repo(files);
        let data = tempfile::tempdir().unwrap();
        let cas = CasDataDir::with_root(data.path().to_path_buf());
        cas.ensure().unwrap();
        let canonical = std::fs::canonicalize(repo.path()).unwrap();
        let repo_hash = path_hash(&canonical);
        let store_path = cas.store_db_path(&repo_hash);
        let mut store = cas_store::open(&store_path).unwrap();
        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let registration = register_repo(&mut store, &canonical, now_ns).unwrap();

        let mut index = cas_registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "demo",
            &canonical.to_string_lossy(),
            &repo_hash,
            now_ns,
        )
        .unwrap();
        tx.commit().unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     applied_generation = 1,
                     last_success_ns = ?1,
                     watcher_state = 'active'
                 WHERE repo_hash = ?2",
                params![now_ns, repo_hash],
            )
            .unwrap();
        let tx = store.transaction().unwrap();
        anchor::set_reconciled(
            &tx,
            &anchor::AnchorName::tentative(registration.worktree_id),
            registration.tentative_manifest,
            now_ns,
            1,
        )
        .unwrap();
        tx.commit().unwrap();

        DataRpcFixture {
            _repo: repo,
            _data: data,
            ctx: DataCtx {
                cas_data_dir: Arc::new(cas),
                lifecycle: None,
            },
        }
    }

    pub(crate) async fn assert_limit_probe(
        method: &dyn DataMethod,
        exact_params: Value,
        over_params: Value,
    ) {
        let fixture = registered_fixture();

        let exact = method.dispatch(&fixture.ctx, exact_params).await.unwrap();
        assert_eq!(exact["items"].as_array().unwrap().len(), 3);
        assert_eq!(
            serde_json::from_value::<Completeness>(exact["completeness"].clone()).unwrap(),
            Completeness::Complete
        );

        let over = method.dispatch(&fixture.ctx, over_params).await.unwrap();
        assert_eq!(over["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            serde_json::from_value::<Completeness>(over["completeness"].clone()).unwrap(),
            Completeness::partial_truncated("cap")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::workspace_analyzer::{AnalyzerProgress, WorkspaceFacts, WorkspaceFile};

    struct ReleaseOnDrop(Option<mpsc::Sender<()>>);

    impl ReleaseOnDrop {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.release();
        }
    }

    struct NotifyTerminalOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for NotifyTerminalOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    struct TestAnalyzer {
        id: &'static str,
        parser_id: &'static str,
        language: &'static str,
    }

    impl WorkspaceAnalyzer for TestAnalyzer {
        fn id(&self) -> &'static str {
            self.id
        }

        fn revision(&self) -> u32 {
            1
        }

        fn language(&self) -> &'static str {
            self.language
        }

        fn parser_id(&self) -> &'static str {
            self.parser_id
        }

        fn analyze_workspace(
            &self,
            _repo_root: &Path,
            _manifest_id: ManifestId,
            _files: &[WorkspaceFile],
            _progress: &AnalyzerProgress,
        ) -> Result<WorkspaceFacts> {
            Ok(WorkspaceFacts::default())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_query_cancellation_interrupts_after_a_real_sqlite_step() {
        let fixture = test_support::registered_fixture();
        let ctx = fixture.ctx.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        let (error_tx, error_rx) = tokio::sync::oneshot::channel();
        let mut release_guard = ReleaseOnDrop(Some(release_tx));
        let cancellation = Arc::new(crate::daemon::RequestCancellation::new());
        let task_cancellation = Arc::clone(&cancellation);
        let query = tokio::spawn(async move {
            crate::daemon::with_request_cancellation(task_cancellation, async move {
                let mut started_tx = Some(started_tx);
                let mut terminal_tx = Some(terminal_tx);
                let mut error_tx = Some(error_tx);
                query_one_or_all_snapshots(
                    &ctx,
                    SnapshotQueryRequest {
                        requested_repo: Some("demo".into()),
                        anchor: None,
                        branch: None,
                        method_name: "interrupt registry test",
                        effective_limit: 10,
                        verbose_tier3: false,
                        exact_file: None,
                    },
                    move |_, connection, _| {
                        let _terminal_guard = NotifyTerminalOnDrop(terminal_tx.take());
                        let mut statement = connection.prepare(
                            "WITH RECURSIVE sequence(value) AS (\
                             SELECT 1 UNION ALL \
                             SELECT value + 1 FROM sequence WHERE value < 1000000000\
                             ) SELECT value FROM sequence",
                        )?;
                        let mut rows = statement.query([])?;
                        let first: i64 = rows.next()?.unwrap().get(0)?;
                        assert_eq!(first, 1, "the query must execute one SQLite step first");
                        started_tx.take().unwrap().send(()).unwrap();
                        release_rx.recv().unwrap();
                        let error = rows
                            .next()
                            .expect_err("the next SQLite step must observe cancellation");
                        if let Some(sender) = error_tx.take() {
                            let _ = sender.send(error.sqlite_error_code());
                        }
                        Err::<Vec<i64>, _>(error.into())
                    },
                    |_: &[i64]| BTreeSet::new(),
                    |_| {},
                )
                .await
            })
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("the SQLite query did not reach its first row")
            .unwrap();
        cancellation.cancel();
        tokio::time::timeout(
            Duration::from_secs(1),
            cancellation.cooperative_cancel_started(),
        )
        .await
        .expect("the snapshot driver did not issue SQLite interrupts");
        release_guard.release();

        let error = tokio::time::timeout(Duration::from_secs(1), error_rx)
            .await
            .expect("the cancelled SQLite step did not return")
            .unwrap();
        assert_eq!(error, Some(rusqlite::ErrorCode::OperationInterrupted));
        let result = tokio::time::timeout(Duration::from_secs(1), query)
            .await
            .expect("the cancelled snapshot query did not terminate")
            .unwrap();
        assert!(matches!(result, Err(Error::Internal(message)) if message == "request cancelled"));
        tokio::time::timeout(Duration::from_secs(1), terminal_rx)
            .await
            .expect("the blocking query did not release its SQLite resources")
            .unwrap();
    }

    #[test]
    fn cancellation_before_sqlite_registration_is_rechecked() {
        let registry = QueryInterruptRegistry::new(None);
        registry.cancel();
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        let _registration = registry.register(&connection);

        assert!(matches!(
            ensure_query_not_cancelled(&registry),
            Err(Error::Internal(message)) if message == "request cancelled"
        ));
    }

    #[test]
    fn cancelling_one_interrupt_registry_does_not_affect_another() {
        let first = QueryInterruptRegistry::new(None);
        let second = QueryInterruptRegistry::new(None);
        let first_connection = rusqlite::Connection::open_in_memory().unwrap();
        let second_connection = rusqlite::Connection::open_in_memory().unwrap();
        let _first_registration = first.register(&first_connection);
        let _second_registration = second.register(&second_connection);

        first.cancel();

        assert!(ensure_query_not_cancelled(&first).is_err());
        assert!(ensure_query_not_cancelled(&second).is_ok());
        assert_eq!(
            second_connection
                .query_row("SELECT 2", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn index_connection_registration_is_interrupted_without_cross_request_effect() {
        let fixture = test_support::registered_fixture();
        let index_path = fixture.ctx.cas_data_dir.index_db_path();
        let first_registry = QueryInterruptRegistry::new(None);
        let second_registry = QueryInterruptRegistry::new(None);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut release_guard = ReleaseOnDrop(Some(release_tx));
        let blocking_registry = Arc::clone(&first_registry);
        let query = tokio::task::spawn_blocking(move || {
            let index = cas_registry::open(&index_path).unwrap();
            let _registration = blocking_registry.register(&index);
            let mut statement = index
                .prepare(
                    "WITH RECURSIVE sequence(value) AS (\
                     SELECT 1 UNION ALL SELECT value + 1 FROM sequence \
                     WHERE value < 1000000000) SELECT value FROM sequence",
                )
                .unwrap();
            let mut rows = statement.query([]).unwrap();
            assert_eq!(rows.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            rows.next().expect_err("index step ignored its interrupt")
        });

        started_rx.await.unwrap();
        first_registry.cancel();
        release_guard.release();
        let error = query.await.unwrap();
        assert_eq!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::OperationInterrupted)
        );

        let second = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let _second_registration = second_registry.register(&second);
        assert_eq!(
            second
                .query_row("SELECT COUNT(*) FROM aliases", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn cancellation_between_repositories_prevents_the_second_query() {
        let fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        index
            .execute(
                "INSERT INTO aliases(alias, repo_hash, registered_at_ns) \
                 VALUES ('demo-two', ?1, ?2)",
                params![entry.repo_hash, system_now_ns()],
            )
            .unwrap();
        let cancellation = Arc::new(crate::daemon::RequestCancellation::new());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let closure_calls = Arc::clone(&calls);
        let closure_cancellation = Arc::clone(&cancellation);
        let result = crate::daemon::with_request_cancellation(
            Arc::clone(&cancellation),
            query_one_or_all_snapshots(
                &fixture.ctx,
                SnapshotQueryRequest {
                    requested_repo: None,
                    anchor: None,
                    branch: None,
                    method_name: "between repositories cancellation test",
                    effective_limit: 10,
                    verbose_tier3: false,
                    exact_file: None,
                },
                move |_, _, _| {
                    closure_calls.fetch_add(1, Ordering::SeqCst);
                    closure_cancellation.cancel();
                    Ok(vec![1_i64])
                },
                |_: &[i64]| BTreeSet::new(),
                |_| {},
            ),
        )
        .await;

        assert!(matches!(result, Err(Error::Internal(message)) if message == "request cancelled"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_snapshot_query_is_not_changed_by_late_cancellation() {
        let fixture = test_support::registered_fixture();
        let cancellation = Arc::new(crate::daemon::RequestCancellation::new());
        let result = crate::daemon::with_request_cancellation(
            Arc::clone(&cancellation),
            query_one_or_all_snapshots(
                &fixture.ctx,
                SnapshotQueryRequest {
                    requested_repo: Some("demo".into()),
                    anchor: None,
                    branch: None,
                    method_name: "late cancellation test",
                    effective_limit: 10,
                    verbose_tier3: false,
                    exact_file: None,
                },
                |_, _, snapshot| Ok(vec![snapshot.manifest_id.0]),
                |_: &[i64]| BTreeSet::new(),
                |_| {},
            ),
        )
        .await
        .unwrap();

        cancellation.cancel();
        assert_eq!(result.items.len(), 1);
    }

    #[test]
    fn exact_limit_rows_are_complete() {
        let mut rows = vec![1, 2];
        assert!(!trim_to_requested_limit(&mut rows, 2));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn over_limit_rows_are_partial_and_truncated() {
        let mut rows = vec![1, 2, 3];
        assert!(trim_to_requested_limit(&mut rows, 2));
        assert_eq!(rows, vec![1, 2]);
    }

    #[test]
    fn snapshot_feedback_suppresses_speculative_empty_hints_and_keeps_cap() {
        let completeness = Completeness::partial_truncated("file_not_indexed_or_snapshot_stale");
        let tier3_status = TierStatus::ready();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindSymbols,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: Some("demo"),
                fuzzy: false,
                ..QueryArgsView::default()
            },
        };
        let issues = vec![QueryFreshnessIssue {
            repo: "demo".into(),
            reason: "reconcile_generation_gap",
        }];

        let (diagnostics, hints) = build_snapshot_aware_feedback(&ctx, &issues, true);

        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiagnosticCode::FileNotIndexedOrSnapshotStale
        }));
        assert!(
            hints
                .iter()
                .any(|hint| hint.code == HintCode::FileNotIndexedOrSnapshotStale)
        );
        assert!(
            hints
                .iter()
                .any(|hint| hint.code == HintCode::CappedIncreaseLimit)
        );
        assert!(!hints.iter().any(|hint| {
            matches!(
                hint.code,
                HintCode::EmptyResultRelaxFilter
                    | HintCode::EmptyResultTryFuzzy
                    | HintCode::EmptyResultWidenScope
            )
        }));
    }

    #[tokio::test]
    async fn stale_snapshot_is_not_tier_ready_when_no_analyzers_apply() {
        let fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = applied_generation + 1
                 WHERE repo_hash = ?1",
                params![entry.repo_hash],
            )
            .unwrap();

        let result = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: Some("demo".into()),
                anchor: None,
                branch: None,
                method_name: "stale tier readiness test",
                effective_limit: 10,
                verbose_tier3: true,
                exact_file: None,
            },
            |_, _, _| Ok(Vec::<String>::new()),
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap();

        assert!(!result.tier3_status.this_query.ready);
        assert!(!result.tier3_status.repo_wide.unwrap().ready);
        assert_eq!(
            result.freshness_issues[0].reason,
            "reconcile_generation_gap"
        );
    }

    #[tokio::test]
    async fn snapshot_executor_pins_manifest_and_reports_concurrent_anchor_move() {
        let fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        let store_path = fixture.ctx.cas_data_dir.store_db_path(&entry.repo_hash);
        let mut store = cas_store::open_existing(&store_path).unwrap();
        let tentative = anchor::list_prefix(&store, "tentative/")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let original_manifest = tentative.manifest_id;
        let tx = store.transaction().unwrap();
        anchor::set_reconciled(&tx, &tentative.name, original_manifest, 100, 1).unwrap();
        tx.execute(
            "INSERT INTO manifests (kind, built_at_ns) VALUES ('tentative', 101)",
            [],
        )
        .unwrap();
        let moved_manifest = ManifestId(tx.last_insert_rowid());
        tx.commit().unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1,
                     applied_generation = 1,
                     last_success_ns = ?1,
                     watcher_state = 'active'
                 WHERE repo_hash = ?2",
                params![system_now_ns(), entry.repo_hash],
            )
            .unwrap();

        let (start_tx, start_rx) = mpsc::sync_channel(0);
        let (done_tx, done_rx) = mpsc::sync_channel(0);
        let writer_path = store_path.clone();
        let anchor_name = tentative.name.clone();
        let writer = std::thread::spawn(move || {
            start_rx.recv().unwrap();
            let mut writer_store = cas_store::open_existing(&writer_path).unwrap();
            let tx = writer_store.transaction().unwrap();
            anchor::set_reconciled(&tx, &anchor_name, moved_manifest, 101, 2).unwrap();
            tx.commit().unwrap();
            done_tx.send(()).unwrap();
        });

        let result = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: Some("demo".into()),
                anchor: None,
                branch: None,
                method_name: "snapshot pin test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            move |_, _, snapshot| {
                start_tx.send(()).unwrap();
                done_rx.recv().unwrap();
                Ok(vec![snapshot.manifest_id.0])
            },
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap();
        writer.join().unwrap();

        assert_eq!(result.items, vec![original_manifest.0]);
        assert_eq!(
            result.freshness_issues,
            vec![QueryFreshnessIssue {
                repo: "demo".into(),
                reason: "snapshot_changed_during_query",
            }]
        );
    }

    #[tokio::test]
    async fn all_repo_exact_file_skips_unrelated_repo_without_partiality() {
        let fixture = test_support::registered_fixture();
        let (other_repo, _) = crate::testutil::init_repo(&[("other.rs", "pub fn other() {}\n")]);
        let canonical = other_repo.path().canonicalize().unwrap();
        let repo_hash = crate::paths::path_hash(&canonical);
        let store_path = fixture.ctx.cas_data_dir.store_db_path(&repo_hash);
        let mut store = cas_store::open(&store_path).unwrap();
        let now_ns = system_now_ns();
        let registration = crate::register::register_repo(&mut store, &canonical, now_ns).unwrap();
        let mut index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "other",
            &canonical.to_string_lossy(),
            &repo_hash,
            now_ns,
        )
        .unwrap();
        tx.commit().unwrap();
        index
            .execute(
                "UPDATE repo_reconcile_state
                 SET desired_generation = 1, applied_generation = 1,
                     last_success_ns = ?1, watcher_state = 'active'
                 WHERE repo_hash = ?2",
                params![now_ns, repo_hash],
            )
            .unwrap();
        let tx = store.transaction().unwrap();
        anchor::set_reconciled(
            &tx,
            &anchor::AnchorName::tentative(registration.worktree_id),
            registration.tentative_manifest,
            now_ns,
            1,
        )
        .unwrap();
        tx.commit().unwrap();

        let result = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: None,
                anchor: None,
                branch: None,
                method_name: "all repo membership test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: Some("src/lib.rs".into()),
            },
            |entry, _, _| Ok(vec![entry.alias.clone()]),
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap();

        assert_eq!(result.items, vec!["demo"]);
        assert!(result.freshness_issues.is_empty());
    }

    #[tokio::test]
    async fn all_repo_query_skips_unpublished_repo_but_explicit_scope_errors() {
        let fixture = test_support::registered_fixture();
        let (other_repo, _) = crate::testutil::init_repo(&[("other.rs", "pub fn other() {}\n")]);
        let canonical = other_repo.path().canonicalize().unwrap();
        let repo_hash = crate::paths::path_hash(&canonical);
        let store_path = fixture.ctx.cas_data_dir.store_db_path(&repo_hash);
        let store = cas_store::open(&store_path).unwrap();
        store.execute("DELETE FROM anchors", []).unwrap();
        let mut index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        cas_registry::upsert(
            &tx,
            "unpublished",
            &canonical.to_string_lossy(),
            &repo_hash,
            system_now_ns(),
        )
        .unwrap();
        tx.commit().unwrap();

        let all = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: None,
                anchor: None,
                branch: None,
                method_name: "all repo unpublished skip test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            |entry, _, _| Ok(vec![entry.alias.clone()]),
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(all.items, vec!["demo"]);

        let explicit = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: Some("unpublished".into()),
                anchor: None,
                branch: None,
                method_name: "explicit unpublished error test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            |entry, _, _| Ok(vec![entry.alias.clone()]),
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap_err();
        assert!(matches!(explicit, Error::AnchorNotFound { .. }));
    }

    #[test]
    fn probe_limit_adds_one() {
        assert_eq!(limit_with_probe(2), 3);
    }

    #[test]
    fn completeness_for_scan_reports_cap_and_unavailable_repo() {
        assert_eq!(completeness_for_scan(false, false), Completeness::Complete);
        assert_eq!(
            completeness_for_scan(true, false),
            Completeness::Partial {
                missing_tiers: Vec::new(),
                reason: Some(PartialReason::Cap),
            }
        );
        assert_eq!(
            completeness_for_scan(false, true),
            Completeness::partial_truncated("repo_unavailable")
        );
    }

    #[tokio::test]
    async fn all_store_scan_skips_removing_repo_but_explicit_scope_errors() {
        let mut fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        drop(index);
        let lifecycle =
            crate::lifecycle::RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        lifecycle
            .begin_removal_and_wait(&entry.repo_hash)
            .await
            .unwrap();
        fixture.ctx.lifecycle = Some(lifecycle);

        let result = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: None,
                anchor: None,
                branch: None,
                method_name: "enumeration skip test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            |_entry, _conn, _snapshot| Ok(vec![1_u8]),
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap();
        assert!(result.items.is_empty());
        assert!(!result.capped);
        assert!(result.skipped_unavailable);

        let err = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: Some("demo".into()),
                anchor: None,
                branch: None,
                method_name: "explicit removing test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            |_entry, _conn, _snapshot| Ok(vec![1_u8]),
            |_| BTreeSet::new(),
            |_| {},
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::RepositoryUnavailable { .. }));
    }

    #[tokio::test]
    async fn second_pass_skips_repo_that_started_removal_after_capture() {
        let mut fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        drop(index);
        let lifecycle =
            crate::lifecycle::RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        fixture.ctx.lifecycle = Some(lifecycle.clone());

        let runtime = tokio::runtime::Handle::current();
        let repo_hash = entry.repo_hash.clone();
        let result = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: None,
                anchor: None,
                branch: None,
                method_name: "second-pass enumeration skip test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            |_entry, _conn, _snapshot| Ok(vec![1_u8]),
            |_| BTreeSet::new(),
            move |_| {
                runtime
                    .block_on(lifecycle.begin_removal_and_wait(&repo_hash))
                    .unwrap();
            },
        )
        .await
        .unwrap();

        assert_eq!(result.items, vec![1]);
        assert!(result.skipped_unavailable);
        assert!(result.tier3_status.this_query.analyzers.is_empty());
    }

    #[tokio::test]
    async fn second_pass_removal_keeps_explicit_repo_unavailable_error() {
        let mut fixture = test_support::registered_fixture();
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        drop(index);
        let lifecycle =
            crate::lifecycle::RepoLifecycleManager::new(fixture.ctx.cas_data_dir.clone());
        lifecycle.startup_sweep().await.unwrap();
        fixture.ctx.lifecycle = Some(lifecycle.clone());

        let runtime = tokio::runtime::Handle::current();
        let repo_hash = entry.repo_hash.clone();
        let error = query_one_or_all_snapshots(
            &fixture.ctx,
            SnapshotQueryRequest {
                requested_repo: Some("demo".into()),
                anchor: None,
                branch: None,
                method_name: "second-pass explicit removal test",
                effective_limit: 10,
                verbose_tier3: false,
                exact_file: None,
            },
            |_entry, _conn, _snapshot| Ok(vec![1_u8]),
            |_| BTreeSet::new(),
            move |_| {
                runtime
                    .block_on(lifecycle.begin_removal_and_wait(&repo_hash))
                    .unwrap();
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::RepositoryUnavailable { .. }));
    }

    #[test]
    fn tier3_status_is_ready_when_all_expected_analyzers_succeeded() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "demo-lsp", "succeeded");

        let status = compute_tier_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            })],
        )
        .unwrap();

        assert!(status.this_query.ready);
        assert_eq!(
            status.this_query.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("demo-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Ready,
                reason_code: None,
                reason: None,
            }]
        );
    }

    #[test]
    fn tier3_status_reports_running_analyzer() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "demo-lsp", "running");

        let status = compute_tier_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            })],
        )
        .unwrap();

        assert_eq!(
            status.this_query.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("demo-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Running,
                reason_code: None,
                reason: None,
            }]
        );
        assert!(!status.this_query.ready);
    }

    #[test]
    fn tier3_status_reports_not_applicable_when_no_analyzers_match_manifest() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);

        let status = compute_tier_status_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "demo-lsp",
                parser_id: "not-present",
                language: "test",
            })],
        )
        .unwrap();

        assert!(status.this_query.ready);
        assert_eq!(
            status.this_query.analyzers,
            vec![TierAnalyzerStatus {
                id: None,
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::NotApplicable,
                reason_code: Some(ReasonCode::NotApplicable),
                reason: Some("no tier3 analyzer for language".into()),
            }]
        );
    }

    #[test]
    fn tier3_status_parser_filter_excludes_unrelated_language() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::from(["tree-sitter-rust".to_string()]);
        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("rust-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Running,
                reason_code: None,
                reason: None,
            }]
        );
    }

    #[test]
    fn tier3_status_empty_parser_filter_does_not_expand_to_repo_wide() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::new();
        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert!(status.ready);
        assert!(status.analyzers.is_empty());
    }

    #[test]
    fn tier3_status_parser_filter_keeps_multiple_touched_languages() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::from([
            "tree-sitter-python".to_string(),
            "tree-sitter-rust".to_string(),
        ]);
        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            multi_language_analyzers(),
            Some(&parser_ids),
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![
                TierAnalyzerStatus {
                    id: Some("python-lsp".into()),
                    language: "python".into(),
                    tier: default_tier(),
                    state: AnalyzerState::Running,
                    reason_code: None,
                    reason: None,
                },
                TierAnalyzerStatus {
                    id: Some("rust-lsp".into()),
                    language: "rust".into(),
                    tier: default_tier(),
                    state: AnalyzerState::Running,
                    reason_code: None,
                    reason: None,
                },
            ]
        );
    }

    #[test]
    fn tier3_status_response_includes_repo_wide_only_when_verbose() {
        let fixture = multi_language_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_run(&conn, manifest_id, "rust-lsp", "running");
        insert_run(&conn, manifest_id, "python-lsp", "running");

        let parser_ids = BTreeSet::from(["tree-sitter-rust".to_string()]);
        let status = TierStatus::from_body(
            compute_tier_status_body_with_analyzers(
                &conn,
                manifest_id,
                multi_language_analyzers(),
                Some(&parser_ids),
            )
            .unwrap(),
        );
        assert!(status.repo_wide.is_none());

        let status = status.with_repo_wide(
            compute_tier_status_body_with_analyzers(
                &conn,
                manifest_id,
                multi_language_analyzers(),
                None,
            )
            .unwrap(),
        );
        assert_eq!(status.this_query.analyzers.len(), 1);
        assert_eq!(status.repo_wide.unwrap().analyzers.len(), 2);
    }

    #[test]
    fn tier3_status_not_applicable_for_markdown_only_query() {
        let fixture = test_support::registered_fixture_with_files(&[("README.md", "# Project\n")]);
        let (conn, manifest_id) = demo_store(&fixture);
        insert_manifest_parser(
            &conn,
            manifest_id,
            "README.md",
            "markdown-fixture-sha",
            "tree-sitter-md",
        );
        let parser_ids = BTreeSet::from(["tree-sitter-md".to_string()]);

        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            Vec::new(),
            Some(&parser_ids),
        )
        .unwrap();

        assert!(status.ready);
        assert_eq!(
            status.analyzers,
            vec![TierAnalyzerStatus {
                id: None,
                language: "markdown".into(),
                tier: default_tier(),
                state: AnalyzerState::NotApplicable,
                reason_code: Some(ReasonCode::NotApplicable),
                reason: Some("no tier3 analyzer for language".into()),
            }]
        );
    }

    #[test]
    fn expected_analyzers_matches_status_callsite() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);
        insert_manifest_parser(
            &conn,
            manifest_id,
            "fake.rs",
            "fake-fixture-sha",
            "fake-parser",
        );

        let mut expected_ids = expected_analyzers_for_manifest(&conn, manifest_id)
            .unwrap()
            .into_iter()
            .map(|analyzer| analyzer.id().to_string())
            .collect::<Vec<_>>();
        expected_ids.sort();

        let mut status_ids = compute_tier_status(&conn, manifest_id)
            .unwrap()
            .this_query
            .analyzers
            .into_iter()
            .filter_map(|status| status.id)
            .collect::<Vec<_>>();
        status_ids.sort();

        assert_eq!(status_ids, expected_ids);
        assert!(status_ids.contains(&"fake-workspace".to_string()));
    }

    #[test]
    fn not_scheduled_when_expected_but_no_run_row() {
        let fixture = test_support::registered_fixture();
        let (conn, manifest_id) = demo_store(&fixture);

        let status = compute_tier_status_body_with_analyzers(
            &conn,
            manifest_id,
            vec![Box::new(TestAnalyzer {
                id: "rust-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            })],
            None,
        )
        .unwrap();

        assert_eq!(
            status.analyzers,
            vec![TierAnalyzerStatus {
                id: Some("rust-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Missing,
                reason_code: Some(ReasonCode::NotScheduled),
                reason: Some("expected analyzer was not scheduled for this manifest".into()),
            }]
        );
    }

    #[test]
    fn build_hints_omits_all_when_happy_path() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Ready,
            reason_code: None,
            reason: None,
        }]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        assert!(build_hints(&ctx).is_empty());
        assert!(build_diagnostics(&ctx).is_empty());
    }

    #[test]
    fn build_hints_emits_relax_filter_when_filters_applied() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                kind: true,
                path: Some("src/"),
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultRelaxFilter);
        assert_eq!(hints[0].action, Some(HintAction::RelaxFilter));
        assert_eq!(hints[0].drop_params, vec!["kind", "path"]);
        assert_eq!(hints[0].tool.as_deref(), Some("find_symbols"));
        assert!(hints[0].message.contains("symbols"));
    }

    #[test]
    fn find_symbols_hint_uses_symbols_noun() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].tool.as_deref(), Some("find_symbols"));
        assert_eq!(hints[0].message, "Increase `limit` to see more symbols.");
    }

    #[test]
    fn get_outline_hint_uses_outline_items_noun_and_outline_tool() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                kind: true,
                path: Some("src/"),
                max_depth: true,
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        let increase = hints
            .iter()
            .find(|hint| hint.code == HintCode::CappedIncreaseLimit)
            .unwrap();
        assert_eq!(increase.tool.as_deref(), Some("get_outline"));
        assert!(increase.message.contains("outline items"));
        let relax = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultRelaxFilter)
            .unwrap();
        assert_eq!(relax.tool.as_deref(), Some("get_outline"));
        assert_eq!(relax.drop_params, vec!["kind", "max_depth", "path"]);
        assert!(relax.message.contains("outline items"));
    }

    #[test]
    fn find_imports_hint_uses_imports_noun_and_imports_tool() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindImports,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                file: Some("src/lib.rs"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultRelaxFilter);
        assert_eq!(hints[0].tool.as_deref(), Some("find_imports"));
        assert_eq!(hints[0].drop_params, vec!["file"]);
        assert!(hints[0].message.contains("imports"));
    }

    #[test]
    fn find_references_hint_can_drop_direction_filter() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                direction: true,
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultRelaxFilter);
        assert_eq!(hints[0].tool.as_deref(), Some("find_references"));
        assert_eq!(hints[0].drop_params, vec!["direction"]);
        assert!(hints[0].message.contains("references"));
    }

    #[test]
    fn relax_filter_drop_params_only_includes_actually_set_args() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindSymbols,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: Some("demo"),
                path: Some("src/"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        let relax = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultRelaxFilter)
            .unwrap();
        assert_eq!(relax.drop_params, vec!["path"]);
    }

    #[test]
    fn relax_filter_does_not_include_repo_in_drop_params() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = EmissionContext {
            tool: QueryToolKind::FindReferences,
            items_empty: true,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                repo: Some("demo"),
                kind: true,
                direction: true,
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        let relax = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultRelaxFilter)
            .unwrap();
        assert_eq!(relax.drop_params, vec!["kind", "direction"]);
        let widen = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultWidenScope)
            .unwrap();
        assert_eq!(widen.drop_params, vec!["repo"]);
    }

    #[test]
    fn build_hints_emits_try_fuzzy_when_exact_no_filter() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                fuzzy: false,
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::EmptyResultTryFuzzy);
        assert_eq!(hints[0].action, Some(HintAction::TryAlternativeQuery));
        assert_eq!(hints[0].params, Some(serde_json::json!({ "fuzzy": true })));
    }

    #[test]
    fn build_hints_emits_widen_scope_when_repo_specified() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                repo: Some("demo"),
                fuzzy: true,
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        let widen = hints
            .iter()
            .find(|hint| hint.code == HintCode::EmptyResultWidenScope)
            .unwrap();
        assert_eq!(widen.drop_params, vec!["repo"]);
    }

    #[test]
    fn build_hints_emits_capped_increase_limit() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::CappedIncreaseLimit);
        assert_eq!(hints[0].action, Some(HintAction::IncreaseLimit));
    }

    #[test]
    fn directory_outline_cap_emits_capped_narrow_filter_first_then_capped_increase_limit() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: false,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                path: Some("src/"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::CappedNarrowFilter);
        assert_eq!(hints[0].tool.as_deref(), Some("get_outline"));
        assert_eq!(
            hints[0].params,
            Some(serde_json::json!({ "narrow_candidates": ["kind", "max_depth"] }))
        );
        assert_eq!(hints[1].code, HintCode::CappedIncreaseLimit);
    }

    #[test]
    fn file_mode_outline_cap_does_not_emit_capped_narrow_filter() {
        let tier3_status = TierStatus::ready();
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = EmissionContext {
            tool: QueryToolKind::GetOutline,
            items_empty: false,
            completeness: &completeness,
            tier3_status: &tier3_status,
            query_args: QueryArgsView {
                file: Some("src/lib.rs"),
                ..QueryArgsView::default()
            },
        };

        let hints = build_hints(&ctx);
        assert!(
            !hints
                .iter()
                .any(|hint| hint.code == HintCode::CappedNarrowFilter)
        );
        assert_eq!(hints[0].code, HintCode::CappedIncreaseLimit);
    }

    #[test]
    fn build_hints_emits_tier3_indexing_wait_when_running() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Running,
            reason_code: None,
            reason: None,
        }]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let hints = build_hints(&ctx);
        assert_eq!(hints[0].code, HintCode::Tier3IndexingWait);
        assert_eq!(hints[0].target.as_deref(), Some("tier3"));
    }

    #[test]
    fn build_hints_emits_reindex_via_cli_when_not_recorded_no_active_job() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Missing,
            reason_code: Some(ReasonCode::NotRecorded),
            reason: Some("analyzer run not recorded".into()),
        }]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView {
                repo: Some("demo"),
                ..QueryArgsView::default()
            },
        );

        let hints = build_hints(&ctx);
        let hint = hints
            .iter()
            .find(|hint| hint.code == HintCode::ReindexViaCli)
            .unwrap();
        assert!(hint.message.contains("cairn ctl repo reindex demo"));
        assert!(hint.action.is_none());
    }

    #[test]
    fn build_diagnostics_from_tier3_analyzer_states() {
        let tier3_status = status_from_analyzers(vec![
            TierAnalyzerStatus {
                id: Some("missing-lsp".into()),
                language: "rust".into(),
                tier: default_tier(),
                state: AnalyzerState::Missing,
                reason_code: Some(ReasonCode::BinaryNotFound),
                reason: Some("binary missing".into()),
            },
            TierAnalyzerStatus {
                id: Some("stale-lsp".into()),
                language: "python".into(),
                tier: default_tier(),
                state: AnalyzerState::Stale,
                reason_code: Some(ReasonCode::StaleRevision),
                reason: Some("revision changed".into()),
            },
            TierAnalyzerStatus {
                id: Some("ruby-lsp".into()),
                language: "ruby".into(),
                tier: default_tier(),
                state: AnalyzerState::Skipped,
                reason_code: Some(ReasonCode::WorkspaceUnsuitable),
                reason: Some("Gemfile without Gemfile.lock".into()),
            },
        ]);
        let completeness = Completeness::complete();
        let ctx = emission_ctx(
            false,
            &completeness,
            &tier3_status,
            QueryArgsView::default(),
        );

        let diagnostics = build_diagnostics(&ctx);
        let codes = diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                DiagnosticCode::AnalyzerBinaryMissing,
                DiagnosticCode::AnalyzerStale,
                DiagnosticCode::WorkspaceUnsuitable,
            ]
        );
        assert_eq!(diagnostics[0].severity, DiagnosticSeverity::Warning);
        assert_eq!(diagnostics[1].severity, DiagnosticSeverity::Info);
    }

    #[test]
    fn hints_priority_order_is_array_order() {
        let tier3_status = status_from_analyzers(vec![TierAnalyzerStatus {
            id: Some("rust-lsp".into()),
            language: "rust".into(),
            tier: default_tier(),
            state: AnalyzerState::Running,
            reason_code: None,
            reason: None,
        }]);
        let completeness = Completeness::partial_truncated(PartialReason::Cap);
        let ctx = emission_ctx(
            true,
            &completeness,
            &tier3_status,
            QueryArgsView {
                repo: Some("demo"),
                kind: true,
                ..QueryArgsView::default()
            },
        );

        let codes = build_hints(&ctx)
            .into_iter()
            .map(|hint| hint.code)
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                HintCode::CappedIncreaseLimit,
                HintCode::Tier3IndexingWait,
                HintCode::EmptyResultRelaxFilter,
                HintCode::EmptyResultWidenScope,
            ]
        );
    }

    fn multi_language_fixture() -> test_support::DataRpcFixture {
        test_support::registered_fixture_with_files(&[
            ("src/lib.rs", "pub fn rust_symbol() {}\n"),
            ("src/app.py", "def python_symbol():\n    pass\n"),
        ])
    }

    fn multi_language_analyzers() -> Vec<Box<dyn WorkspaceAnalyzer>> {
        vec![
            Box::new(TestAnalyzer {
                id: "rust-lsp",
                parser_id: "tree-sitter-rust",
                language: "rust",
            }),
            Box::new(TestAnalyzer {
                id: "python-lsp",
                parser_id: "tree-sitter-python",
                language: "python",
            }),
        ]
    }

    fn demo_store(fixture: &test_support::DataRpcFixture) -> (rusqlite::Connection, ManifestId) {
        let index = cas_registry::open(&fixture.ctx.cas_data_dir.index_db_path()).unwrap();
        let entry = cas_registry::lookup_by_alias(&index, "demo")
            .unwrap()
            .unwrap();
        let conn =
            cas_store::open(&fixture.ctx.cas_data_dir.store_db_path(&entry.repo_hash)).unwrap();
        let manifest_id = anchor::resolve(&conn, &anchor::AnchorName::head())
            .unwrap()
            .unwrap();
        (conn, manifest_id)
    }

    fn insert_run(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        analyzer_id: &str,
        status: &str,
    ) {
        conn.execute(
            "INSERT INTO workspace_analysis_runs
               (manifest_id, analyzer_id, analyzer_revision, config_hash,
                status, started_at_ns, finished_at_ns, error, job_id, cancel_requested)
             VALUES (?1, ?2, 1, 'cfg', ?3, 0, 0, NULL, NULL, 0)",
            params![manifest_id.0, analyzer_id, status],
        )
        .unwrap();
    }

    fn insert_manifest_parser(
        conn: &rusqlite::Connection,
        manifest_id: ManifestId,
        path: &str,
        blob_sha: &str,
        parser_id: &str,
    ) {
        conn.execute(
            "INSERT INTO blobs (blob_sha, parser_id, parser_revision, parsed_at_ns)
             VALUES (?1, ?2, 1, 0)",
            params![blob_sha, parser_id],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO manifest_entries (manifest_id, path, blob_sha)
             VALUES (?1, ?2, ?3)",
            params![manifest_id.0, path, blob_sha],
        )
        .unwrap();
    }

    fn status_from_analyzers(analyzers: Vec<TierAnalyzerStatus>) -> TierStatus {
        TierStatus::from_body(TierStatusBody::from_analyzers(analyzers))
    }

    fn emission_ctx<'a>(
        items_empty: bool,
        completeness: &'a Completeness,
        tier3_status: &'a TierStatus,
        query_args: QueryArgsView<'a>,
    ) -> EmissionContext<'a> {
        EmissionContext {
            tool: QueryToolKind::FindSymbols,
            items_empty,
            completeness,
            tier3_status,
            query_args,
        }
    }
}
