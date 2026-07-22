//! Canonical repository lifecycle coordination.
//!
//! Repository identity belongs to `repo_hash`; aliases are routing labels.
//! This module is the only production writer for canonical registration and
//! removal transitions. A per-repository activity gate makes `Removing` the
//! linearization point after which no new store user can start.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tracing::{info, warn};

use crate::cas::registry::{self as cas_registry, RepositoryEntry, RepositoryRemovalReason};
use crate::jobs::JobManager;
use crate::paths::CasDataDir;
use crate::reconcile::RepoReconcileManager;
use crate::watcher::WatchManager;
use crate::{Error, Result};

const LEASE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoActivityState {
    Registering,
    Active,
    Removing,
    Removed,
}

impl RepoActivityState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Registering => "registering",
            Self::Active => "active",
            Self::Removing => "removing",
            Self::Removed => "removed",
        }
    }
}

#[derive(Debug)]
struct GateState {
    state: RepoActivityState,
    leases: usize,
}

/// Admission and quiescence state for one canonical repository.
#[derive(Debug)]
pub struct RepoActivityGate {
    repo_hash: String,
    inner: Mutex<GateState>,
    idle: Notify,
}

impl RepoActivityGate {
    fn new(repo_hash: String, state: RepoActivityState) -> Arc<Self> {
        Arc::new(Self {
            repo_hash,
            inner: Mutex::new(GateState { state, leases: 0 }),
            idle: Notify::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            warn!(repo_hash = %self.repo_hash, "repo activity gate mutex poisoned; recovering");
            poisoned.into_inner()
        })
    }

    fn acquire(self: &Arc<Self>) -> Result<RepoLease> {
        let mut inner = self.lock();
        match inner.state {
            RepoActivityState::Registering | RepoActivityState::Active => {
                inner.leases = inner.leases.checked_add(1).ok_or_else(|| {
                    Error::Internal(format!("repo lease counter overflow: {}", self.repo_hash))
                })?;
                Ok(RepoLease {
                    gate: Arc::clone(self),
                    released: false,
                })
            }
            state => Err(Error::RepositoryUnavailable {
                repo_hash: self.repo_hash.clone(),
                state: state.as_str(),
            }),
        }
    }

    fn acquire_active(self: &Arc<Self>) -> Result<RepoLease> {
        let mut inner = self.lock();
        match inner.state {
            RepoActivityState::Active => {
                inner.leases = inner.leases.checked_add(1).ok_or_else(|| {
                    Error::Internal(format!("repo lease counter overflow: {}", self.repo_hash))
                })?;
                Ok(RepoLease {
                    gate: Arc::clone(self),
                    released: false,
                })
            }
            state => Err(Error::RepositoryUnavailable {
                repo_hash: self.repo_hash.clone(),
                state: state.as_str(),
            }),
        }
    }

    fn set_active(&self) -> Result<()> {
        let mut inner = self.lock();
        match inner.state {
            RepoActivityState::Registering | RepoActivityState::Active => {
                inner.state = RepoActivityState::Active;
                Ok(())
            }
            state => Err(Error::RepositoryUnavailable {
                repo_hash: self.repo_hash.clone(),
                state: state.as_str(),
            }),
        }
    }

    fn ensure_publishable(&self) -> Result<()> {
        let inner = self.lock();
        match inner.state {
            RepoActivityState::Registering | RepoActivityState::Active => Ok(()),
            state => Err(Error::RepositoryUnavailable {
                repo_hash: self.repo_hash.clone(),
                state: state.as_str(),
            }),
        }
    }

    fn begin_removal(&self) -> Result<()> {
        let mut inner = self.lock();
        match inner.state {
            RepoActivityState::Registering | RepoActivityState::Active => {
                inner.state = RepoActivityState::Removing;
                Ok(())
            }
            RepoActivityState::Removing => Ok(()),
            RepoActivityState::Removed => Err(Error::RepositoryUnavailable {
                repo_hash: self.repo_hash.clone(),
                state: RepoActivityState::Removed.as_str(),
            }),
        }
    }

    async fn wait_idle(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock().leases == 0 {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(Error::Internal(format!(
                    "timed out waiting for repository activity to drain: {}",
                    self.repo_hash
                )));
            }
        }
    }

    fn mark_removed(&self) {
        let mut inner = self.lock();
        inner.state = RepoActivityState::Removed;
    }

    #[cfg(test)]
    fn snapshot(&self) -> (RepoActivityState, usize) {
        let inner = self.lock();
        (inner.state, inner.leases)
    }
}

/// RAII proof that one operation was admitted before removal linearized.
#[derive(Debug)]
pub struct RepoLease {
    gate: Arc<RepoActivityGate>,
    released: bool,
}

impl RepoLease {
    #[must_use]
    pub fn repo_hash(&self) -> &str {
        &self.gate.repo_hash
    }
}

impl Drop for RepoLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut inner = self.gate.lock();
        match inner.leases.checked_sub(1) {
            Some(leases) => {
                inner.leases = leases;
                if leases == 0 {
                    self.gate.idle.notify_waiters();
                }
            }
            None => {
                inner.state = RepoActivityState::Removing;
                warn!(repo_hash = %self.gate.repo_hash, "repo lease underflow; gate poisoned closed");
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StartupSweepReport {
    pub cleanup_retried: Vec<String>,
    pub repositories_removed: Vec<String>,
    pub repositories_active: Vec<String>,
    pub repositories_degraded: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovalIntent {
    MissingRoot { repo_hash: String },
    LastAliasRemoved { repo_hash: String },
    AliasRetargeted { repo_hash: String },
}

impl RemovalIntent {
    fn repo_hash(&self) -> &str {
        match self {
            Self::MissingRoot { repo_hash }
            | Self::LastAliasRemoved { repo_hash }
            | Self::AliasRetargeted { repo_hash } => repo_hash,
        }
    }

    fn reason(&self) -> RepositoryRemovalReason {
        match self {
            Self::MissingRoot { .. } => RepositoryRemovalReason::MissingRoot,
            Self::LastAliasRemoved { .. } => RepositoryRemovalReason::LastAliasRemoved,
            Self::AliasRetargeted { .. } => RepositoryRemovalReason::AliasRetargeted,
        }
    }
}

struct RuntimeBindings {
    jobs: std::sync::Weak<JobManager>,
    watchers: std::sync::Weak<WatchManager>,
    reconcile: std::sync::Weak<RepoReconcileManager>,
}

/// Proof that create-capable registration work owns the repository gate.
#[derive(Debug)]
pub struct RegistrationPermit {
    repo_hash: String,
    root_path: String,
    newly_created: bool,
    lease: Option<RepoLease>,
}

impl RegistrationPermit {
    #[must_use]
    pub fn repo_hash(&self) -> &str {
        &self.repo_hash
    }

    #[must_use]
    pub fn root_path(&self) -> &str {
        &self.root_path
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationReconcilePolicy {
    /// Focused legacy constructors without a reconcile manager.
    None,
    /// Atomically publish the alias and record an immediately runnable
    /// post-arm catch-up generation.
    ImmediateCatchUp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationPublication {
    pub repo_hash: String,
    pub catch_up_generation: Option<i64>,
}

/// Thin coordinator for canonical registry mutation and removal sequencing.
pub struct RepoLifecycleManager {
    cas_data_dir: Arc<CasDataDir>,
    gates: Mutex<HashMap<String, Arc<RepoActivityGate>>>,
    transition: Mutex<()>,
    runtime: Mutex<Option<RuntimeBindings>>,
    pending_intents: Mutex<HashMap<String, RemovalIntent>>,
    pending_notify: Arc<Notify>,
    owner_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutting_down: AtomicBool,
}

impl RepoLifecycleManager {
    #[must_use]
    pub fn new(cas_data_dir: Arc<CasDataDir>) -> Arc<Self> {
        Arc::new(Self {
            cas_data_dir,
            gates: Mutex::new(HashMap::new()),
            transition: Mutex::new(()),
            runtime: Mutex::new(None),
            pending_intents: Mutex::new(HashMap::new()),
            pending_notify: Arc::new(Notify::new()),
            owner_task: Mutex::new(None),
            shutting_down: AtomicBool::new(false),
        })
    }

    /// Bind daemon runtime owners exactly once and start the removal owner
    /// task. Weak references avoid a lifecycle cycle during shutdown.
    pub fn bind_runtime(
        self: &Arc<Self>,
        jobs: std::sync::Weak<JobManager>,
        watchers: std::sync::Weak<WatchManager>,
        reconcile: std::sync::Weak<RepoReconcileManager>,
    ) -> Result<()> {
        let mut runtime = self
            .runtime
            .lock()
            .map_err(|_| Error::Internal("repo lifecycle runtime binding mutex poisoned".into()))?;
        if runtime.is_some() {
            return Err(Error::Internal(
                "repo lifecycle runtime already bound".into(),
            ));
        }
        *runtime = Some(RuntimeBindings {
            jobs,
            watchers,
            reconcile,
        });
        drop(runtime);
        let manager = Arc::clone(self);
        let handle = tokio::spawn(async move { manager.owner_loop().await });
        *self
            .owner_task
            .lock()
            .map_err(|_| Error::Internal("repo lifecycle owner task mutex poisoned".into()))? =
            Some(handle);
        Ok(())
    }

    /// Coalesce a detector edge by repository. No removal or join runs on the
    /// detector task itself.
    pub fn request_removal(&self, intent: RemovalIntent) -> Result<()> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(Error::RepositoryUnavailable {
                repo_hash: intent.repo_hash().to_string(),
                state: "shutting_down",
            });
        }
        self.pending_intents
            .lock()
            .map_err(|_| Error::Internal("repo lifecycle intent map poisoned".into()))?
            .entry(intent.repo_hash().to_string())
            .or_insert(intent);
        self.pending_notify.notify_one();
        Ok(())
    }

    async fn owner_loop(self: Arc<Self>) {
        while !self.shutting_down.load(Ordering::SeqCst) {
            self.pending_notify.notified().await;
            loop {
                let next = self.pending_intents.lock().ok().and_then(|mut intents| {
                    let key = intents.keys().next()?.clone();
                    intents.remove(&key)
                });
                let Some(intent) = next else { break };
                if let Err(err) = self.process_runtime_removal(&intent).await {
                    warn!(
                        repo_hash = %intent.repo_hash(),
                        error = %err,
                        sqlite_code = ?err.sqlite_error_code(),
                        sqlite_extended_code = ?err.sqlite_extended_code(),
                        "repository removal deferred; durable request retained"
                    );
                    if self.shutting_down.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    if let Ok(mut intents) = self.pending_intents.lock() {
                        intents
                            .entry(intent.repo_hash().to_string())
                            .or_insert(intent);
                    }
                }
            }
        }
    }

    fn runtime_bindings(
        &self,
    ) -> Result<(
        Arc<JobManager>,
        Arc<WatchManager>,
        Arc<RepoReconcileManager>,
    )> {
        let runtime = self
            .runtime
            .lock()
            .map_err(|_| Error::Internal("repo lifecycle runtime binding mutex poisoned".into()))?;
        let runtime = runtime
            .as_ref()
            .ok_or_else(|| Error::Internal("repo lifecycle runtime is not bound".into()))?;
        let jobs = runtime.jobs.upgrade().ok_or_else(|| {
            Error::Internal("job manager dropped while repository removal was in flight".into())
        })?;
        let watchers = runtime.watchers.upgrade().ok_or_else(|| {
            Error::Internal("watch manager dropped while repository removal was in flight".into())
        })?;
        let reconcile = runtime.reconcile.upgrade().ok_or_else(|| {
            Error::Internal(
                "reconcile manager dropped while repository removal was in flight".into(),
            )
        })?;
        Ok((jobs, watchers, reconcile))
    }

    async fn process_runtime_removal(&self, intent: &RemovalIntent) -> Result<()> {
        let repo_hash = intent.repo_hash().to_string();
        {
            let _transition = self.transition.lock().map_err(|_| {
                Error::Internal("repository lifecycle transition mutex poisoned".into())
            })?;
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let Some(repo) = cas_registry::lookup_repository(&index, &repo_hash)? else {
                return Ok(());
            };
            if matches!(intent, RemovalIntent::MissingRoot { .. }) {
                if repo.persistent || !root_is_definitively_missing(Path::new(&repo.root_path))? {
                    return Ok(());
                }
            }
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::mark_removal_requested(&tx, &repo_hash, intent.reason(), now_ns())?;
            tx.commit()?;
            // Persist the intent before closing admission. If gate closure
            // itself fails, startup can still recover the durable request.
            // The transition mutex prevents registration publication from
            // interleaving between these two steps.
            self.ensure_gate(&repo_hash, RepoActivityState::Active)
                .begin_removal()?;
        }
        let gate = self.ensure_gate(&repo_hash, RepoActivityState::Removing);

        // Upgrade every runtime dependency before mutating external state. If
        // shutdown already dropped one, the durable request remains for the
        // next startup and registry/store deletion does not begin.
        let (jobs, watchers, reconcile) = self.runtime_bindings()?;
        watchers.unwatch_repository(&repo_hash);
        reconcile.quiesce_repository(&repo_hash);
        jobs.cancel_repository(&repo_hash)?;
        gate.wait_idle(LEASE_DRAIN_TIMEOUT).await?;

        let event_id = {
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let event_id = cas_registry::delete_repository_with_event(&tx, &repo_hash, now_ns())?
                .ok_or_else(|| {
                Error::Internal(format!("missing removal request for {repo_hash}"))
            })?;
            tx.commit()?;
            event_id
        };
        self.finish_store_cleanup(&repo_hash, event_id)?;
        gate.mark_removed();
        info!(repo_hash = %repo_hash, "repository lifecycle removal complete");
        Ok(())
    }

    fn finish_store_cleanup(&self, repo_hash: &str, event_id: i64) -> Result<()> {
        let repo_dir = self.cas_data_dir.repo_dir(repo_hash);
        let cleanup = std::fs::remove_dir_all(&repo_dir);
        let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        match cleanup {
            Ok(()) => {
                cas_registry::mark_store_cleanup_complete(&tx, event_id)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                cas_registry::mark_store_cleanup_complete(&tx, event_id)?;
            }
            Err(err) => {
                cas_registry::mark_store_cleanup_error(&tx, event_id, &err.to_string())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn lock_gates(&self) -> MutexGuard<'_, HashMap<String, Arc<RepoActivityGate>>> {
        self.gates.lock().unwrap_or_else(|poisoned| {
            warn!("repo lifecycle gate registry poisoned; recovering");
            poisoned.into_inner()
        })
    }

    fn gate(&self, repo_hash: &str) -> Option<Arc<RepoActivityGate>> {
        self.lock_gates().get(repo_hash).cloned()
    }

    fn ensure_gate(&self, repo_hash: &str, state: RepoActivityState) -> Arc<RepoActivityGate> {
        self.lock_gates()
            .entry(repo_hash.to_string())
            .or_insert_with(|| RepoActivityGate::new(repo_hash.to_string(), state))
            .clone()
    }

    fn registration_gate(&self, repo_hash: &str, newly_created: bool) -> Arc<RepoActivityGate> {
        let mut gates = self.lock_gates();
        if newly_created
            && gates
                .get(repo_hash)
                .is_some_and(|gate| gate.lock().state == RepoActivityState::Removed)
        {
            gates.remove(repo_hash);
        }
        gates
            .entry(repo_hash.to_string())
            .or_insert_with(|| {
                RepoActivityGate::new(
                    repo_hash.to_string(),
                    if newly_created {
                        RepoActivityState::Registering
                    } else {
                        RepoActivityState::Active
                    },
                )
            })
            .clone()
    }

    /// Acquire one store activity lease by canonical repository hash.
    pub fn acquire_by_repo_hash(&self, repo_hash: &str) -> Result<RepoLease> {
        self.gate(repo_hash)
            .ok_or_else(|| Error::RepositoryUnavailable {
                repo_hash: repo_hash.to_string(),
                state: RepoActivityState::Removed.as_str(),
            })?
            .acquire()
    }

    /// Acquire a lease only after registration publication has made the
    /// canonical owner Active. Event producers use this form so an edge
    /// observed while the initial scan is still Registering remains pending
    /// in the watcher dispatcher until publication completes.
    pub fn acquire_active_by_repo_hash(&self, repo_hash: &str) -> Result<RepoLease> {
        self.gate(repo_hash)
            .ok_or_else(|| Error::RepositoryUnavailable {
                repo_hash: repo_hash.to_string(),
                state: RepoActivityState::Removed.as_str(),
            })?
            .acquire_active()
    }

    /// Acquire a repository for an unscoped multi-repository scan. Lifecycle
    /// transitions are skipped so one Removing owner cannot fail the whole
    /// inventory; counter and internal failures still propagate.
    pub fn acquire_for_enumeration(&self, repo_hash: &str) -> Result<Option<RepoLease>> {
        match self.acquire_by_repo_hash(repo_hash) {
            Ok(lease) => Ok(Some(lease)),
            Err(Error::RepositoryUnavailable { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Resolve an alias and acquire its canonical gate, retrying if an alias
    /// retarget raced the first lookup.
    pub fn resolve_alias_and_acquire(&self, alias: &str) -> Result<(RepositoryEntry, RepoLease)> {
        for _ in 0..3 {
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let first = cas_registry::lookup_by_alias(&index, alias)?.ok_or_else(|| {
                Error::RepoNotFound {
                    alias: alias.to_string(),
                }
            })?;
            drop(index);
            let lease = self.acquire_by_repo_hash(&first.repo_hash)?;
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let Some(second) = cas_registry::lookup_by_alias(&index, alias)? else {
                continue;
            };
            if second.repo_hash == first.repo_hash {
                let repo = cas_registry::lookup_repository(&index, &first.repo_hash)?.ok_or_else(
                    || Error::RepoNotFound {
                        alias: alias.to_string(),
                    },
                )?;
                return Ok((repo, lease));
            }
        }
        Err(Error::Internal(format!(
            "alias kept changing while acquiring repository lease: {alias}"
        )))
    }

    /// Establish the canonical owner and Registering gate before any
    /// create-capable store open. Alias publication remains delayed until the
    /// existing registration work succeeds.
    pub fn begin_registration(
        &self,
        repo_hash: String,
        root_path: PathBuf,
        registered_at_ns: i64,
    ) -> Result<RegistrationPermit> {
        let _transition = self.transition.lock().map_err(|_| {
            Error::Internal("repository lifecycle transition mutex poisoned".into())
        })?;
        let root_path = root_path.to_string_lossy().to_string();
        {
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            if cas_registry::list_incomplete_removals(&index)?
                .iter()
                .any(|event| event.repo_hash == repo_hash)
            {
                return Err(Error::RepositoryUnavailable {
                    repo_hash,
                    state: "cleanup_pending",
                });
            }
        }
        let newly_created = {
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let existing = cas_registry::lookup_repository(&index, &repo_hash)?;
            if existing
                .as_ref()
                .is_some_and(|repo| repo.removal_request.is_some())
            {
                return Err(Error::RepositoryUnavailable {
                    repo_hash,
                    state: "removing",
                });
            }
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::upsert_repository(&tx, &repo_hash, &root_path, registered_at_ns)?;
            tx.commit()?;
            existing.is_none()
        };
        let gate = self.registration_gate(&repo_hash, newly_created);
        let lease = gate.acquire()?;
        Ok(RegistrationPermit {
            repo_hash,
            root_path,
            newly_created,
            lease: Some(lease),
        })
    }

    /// Publish a successfully indexed registration and apply the tri-state
    /// persistence policy. Alias retargeting durably requests cleanup of an
    /// old owner that becomes unreachable.
    pub fn publish_registration(
        &self,
        mut permit: RegistrationPermit,
        alias: &str,
        persistent: Option<bool>,
        registered_at_ns: i64,
        reconcile_policy: RegistrationReconcilePolicy,
    ) -> Result<RegistrationPublication> {
        let transition = self.transition.lock().map_err(|_| {
            Error::Internal("repository lifecycle transition mutex poisoned".into())
        })?;
        let publication = (|| -> Result<(Option<String>, Option<i64>)> {
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let old_hash = cas_registry::lookup_by_alias(&index, alias)?
                .map(|entry| entry.repo_hash)
                .filter(|hash| hash != &permit.repo_hash);
            let target = cas_registry::lookup_repository(&index, &permit.repo_hash)?
                .ok_or_else(|| Error::Internal("registration owner disappeared".into()))?;
            if target.removal_request.is_some() {
                return Err(Error::RepositoryUnavailable {
                    repo_hash: permit.repo_hash.clone(),
                    state: RepoActivityState::Removing.as_str(),
                });
            }
            self.gate(&permit.repo_hash)
                .ok_or_else(|| Error::RepositoryUnavailable {
                    repo_hash: permit.repo_hash.clone(),
                    state: RepoActivityState::Removed.as_str(),
                })?
                .ensure_publishable()?;
            let target_persistent = persistent.unwrap_or(if permit.newly_created {
                false
            } else {
                target.persistent
            });
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::upsert(
                &tx,
                alias,
                &permit.root_path,
                &permit.repo_hash,
                registered_at_ns,
            )?;
            if !cas_registry::set_repository_persistent(&tx, &permit.repo_hash, target_persistent)?
            {
                return Err(Error::Internal(format!(
                    "registration owner disappeared while setting persistence: {}",
                    permit.repo_hash
                )));
            }
            if let Some(old_hash) = &old_hash
                && cas_registry::count_aliases_for_repo(&tx, old_hash)? == 0
            {
                cas_registry::mark_removal_requested(
                    &tx,
                    old_hash,
                    RepositoryRemovalReason::AliasRetargeted,
                    now_ns(),
                )?;
            }
            let catch_up_generation = match reconcile_policy {
                RegistrationReconcilePolicy::None => None,
                RegistrationReconcilePolicy::ImmediateCatchUp => {
                    Some(cas_registry::increment_immediate_desired_generation(
                        &tx,
                        &permit.repo_hash,
                        registered_at_ns,
                    )?)
                }
            };
            tx.commit()?;
            Ok((old_hash, catch_up_generation))
        })();
        let (old_hash, catch_up_generation) = match publication {
            Ok(publication) => publication,
            Err(error) => {
                // The alias/catch-up transaction did not commit, so this permit
                // still owns an unpublished registration. Release the transition
                // lock before routing cleanup through the same canonical abort
                // path used by initial-scan failures.
                drop(transition);
                return match self.abort_registration_sync(permit) {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(Error::Internal(format!(
                        "registration publication failed: {error}; cleanup failed: {cleanup}"
                    ))),
                };
            }
        };
        if let Some(gate) = self.gate(&permit.repo_hash) {
            gate.set_active()?;
        }
        permit.lease.take();
        if let Some(repo_hash) = old_hash {
            // The alias retarget and durable removal request committed together above. A
            // runtime wake failure must not report the committed registration as rolled back;
            // startup recovery will resume the retained removal request.
            if let Err(error) = self.request_removal(RemovalIntent::AliasRetargeted {
                repo_hash: repo_hash.clone(),
            }) {
                warn!(
                    %repo_hash,
                    %error,
                    "alias retarget committed; runtime removal wake deferred"
                );
            }
        }
        Ok(RegistrationPublication {
            repo_hash: permit.repo_hash,
            catch_up_generation,
        })
    }

    /// Abort a failed new registration without exposing a partial canonical
    /// owner. Existing owners are left intact.
    pub async fn abort_registration(&self, permit: RegistrationPermit) -> Result<()> {
        self.abort_registration_sync(permit)
    }

    fn abort_registration_sync(&self, mut permit: RegistrationPermit) -> Result<()> {
        permit.lease.take();
        if !permit.newly_created {
            return Ok(());
        }
        {
            let _transition = self.transition.lock().map_err(|_| {
                Error::Internal("repository lifecycle transition mutex poisoned".into())
            })?;
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::mark_removal_requested(
                &tx,
                &permit.repo_hash,
                RepositoryRemovalReason::RegistrationAborted,
                now_ns(),
            )?;
            tx.commit()?;
        }
        // Registration can fail before runtime binding during startup/tests.
        // Use the startup-exclusive path because no alias was published and
        // the registration lease has already drained.
        let repo = {
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            cas_registry::lookup_repository(&index, &permit.repo_hash)?
        };
        if let Some(repo) = repo {
            self.remove_startup_exclusive(&repo, RepositoryRemovalReason::RegistrationAborted)?;
        }
        Ok(())
    }

    /// Remove one user-facing alias. A non-final alias is label-only; the
    /// final alias routes through canonical lifecycle removal.
    pub async fn remove_alias(&self, alias: &str) -> Result<bool> {
        let intent = {
            let _transition = self.transition.lock().map_err(|_| {
                Error::Internal("repository lifecycle transition mutex poisoned".into())
            })?;
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let Some(entry) = cas_registry::lookup_by_alias(&index, alias)? else {
                return Ok(false);
            };
            let remaining = cas_registry::count_aliases_for_repo(&index, &entry.repo_hash)?;
            drop(index);
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if remaining > 1 {
                cas_registry::delete(&tx, alias)?;
                tx.commit()?;
                return Ok(true);
            }
            cas_registry::mark_removal_requested(
                &tx,
                &entry.repo_hash,
                RepositoryRemovalReason::LastAliasRemoved,
                now_ns(),
            )?;
            tx.commit()?;
            RemovalIntent::LastAliasRemoved {
                repo_hash: entry.repo_hash,
            }
        };
        self.process_runtime_removal(&intent).await?;
        Ok(true)
    }

    /// Stop accepting detector intents and bound the lifecycle owner join.
    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.pending_notify.notify_waiters();
        let handle = self
            .owner_task
            .lock()
            .map_err(|_| Error::Internal("repo lifecycle owner task mutex poisoned".into()))?
            .take();
        if let Some(handle) = handle
            && tokio::time::timeout(timeout, handle).await.is_err()
        {
            return Err(Error::Internal(
                "timed out waiting for repo lifecycle owner shutdown".into(),
            ));
        }
        Ok(())
    }

    /// Run crash recovery before JobManager restore or any runtime worker.
    pub async fn startup_sweep(&self) -> Result<StartupSweepReport> {
        let mut report = StartupSweepReport::default();
        self.retry_incomplete_store_cleanup(&mut report)?;

        let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let repositories = cas_registry::list_repositories(&index)?;
        drop(index);

        for repo in repositories {
            let alias_count = {
                let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
                cas_registry::count_aliases_for_repo(&index, &repo.repo_hash)?
            };
            let requested_reason = repo.removal_request.as_ref().map(|request| request.reason);
            let reason = if let Some(reason) = requested_reason {
                Some(reason)
            } else if alias_count == 0 {
                Some(RepositoryRemovalReason::StartupAliasless)
            } else {
                match std::fs::metadata(&repo.root_path) {
                    Ok(metadata) if metadata.is_dir() => None,
                    Ok(_) => {
                        report.repositories_degraded.push(repo.repo_hash.clone());
                        None
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound && !repo.persistent => {
                        Some(RepositoryRemovalReason::MissingRoot)
                    }
                    Err(_) => {
                        report.repositories_degraded.push(repo.repo_hash.clone());
                        None
                    }
                }
            };

            if let Some(reason) = reason {
                self.remove_startup_exclusive(&repo, reason)?;
                report.repositories_removed.push(repo.repo_hash);
            } else {
                self.ensure_gate(&repo.repo_hash, RepoActivityState::Active)
                    .set_active()?;
                report.repositories_active.push(repo.repo_hash);
            }
        }
        let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        cas_registry::prune_completed_removal_events(&tx, 100)?;
        tx.commit()?;
        Ok(report)
    }

    fn retry_incomplete_store_cleanup(&self, report: &mut StartupSweepReport) -> Result<()> {
        let events = {
            let index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            cas_registry::list_incomplete_removals(&index)?
        };
        for event in events {
            let repo_dir = self.cas_data_dir.repo_dir(&event.repo_hash);
            let cleanup = std::fs::remove_dir_all(&repo_dir);
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            match cleanup {
                Ok(()) => {
                    cas_registry::mark_store_cleanup_complete(&tx, event.event_id)?;
                    report.cleanup_retried.push(event.repo_hash);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    cas_registry::mark_store_cleanup_complete(&tx, event.event_id)?;
                    report.cleanup_retried.push(event.repo_hash);
                }
                Err(err) => {
                    cas_registry::mark_store_cleanup_error(&tx, event.event_id, &err.to_string())?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    }

    fn remove_startup_exclusive(
        &self,
        repo: &RepositoryEntry,
        reason: RepositoryRemovalReason,
    ) -> Result<()> {
        let gate = self.ensure_gate(&repo.repo_hash, RepoActivityState::Removing);
        gate.begin_removal()?;
        let event_id = {
            let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
            let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            cas_registry::mark_removal_requested(&tx, &repo.repo_hash, reason, now_ns())?;
            let event_id =
                cas_registry::delete_repository_with_event(&tx, &repo.repo_hash, now_ns())?
                    .ok_or_else(|| {
                        Error::Internal(format!("missing removal request for {}", repo.repo_hash))
                    })?;
            tx.commit()?;
            event_id
        };
        let repo_dir = self.cas_data_dir.repo_dir(&repo.repo_hash);
        let cleanup = std::fs::remove_dir_all(&repo_dir);
        let mut index = cas_registry::open(&self.cas_data_dir.index_db_path())?;
        let tx = index.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        match cleanup {
            Ok(()) => {
                cas_registry::mark_store_cleanup_complete(&tx, event_id)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                cas_registry::mark_store_cleanup_complete(&tx, event_id)?;
            }
            Err(err) => {
                cas_registry::mark_store_cleanup_error(&tx, event_id, &err.to_string())?;
            }
        }
        tx.commit()?;
        gate.mark_removed();
        info!(repo_hash = %repo.repo_hash, ?reason, "repository removed during startup sweep");
        Ok(())
    }

    /// Test/support primitive used by runtime removal after it has stopped
    /// producers. It deliberately does not roll a Removing gate back on
    /// timeout.
    pub async fn begin_removal_and_wait(&self, repo_hash: &str) -> Result<()> {
        let gate = self
            .gate(repo_hash)
            .ok_or_else(|| Error::RepositoryUnavailable {
                repo_hash: repo_hash.to_string(),
                state: RepoActivityState::Removed.as_str(),
            })?;
        gate.begin_removal()?;
        gate.wait_idle(LEASE_DRAIN_TIMEOUT).await
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(i64::MAX)
}

fn root_is_definitively_missing(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(_) => Ok(false),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::registry;
    use crate::cas::registry::StoreCleanupState;
    use crate::jobs::JobManager;
    use crate::reconcile::{ReconcileTrigger, RepoReconcileManager};

    #[test]
    fn removing_linearization_rejects_new_leases() {
        let gate = RepoActivityGate::new("h".into(), RepoActivityState::Active);
        let lease = gate.acquire().unwrap();
        gate.begin_removal().unwrap();

        assert!(matches!(
            gate.acquire(),
            Err(Error::RepositoryUnavailable {
                state: "removing",
                ..
            })
        ));
        assert_eq!(gate.snapshot(), (RepoActivityState::Removing, 1));
        drop(lease);
        assert_eq!(gate.snapshot(), (RepoActivityState::Removing, 0));
    }

    #[test]
    fn active_only_lease_rejects_registering_gate_until_publication() {
        let gate = RepoActivityGate::new("h".into(), RepoActivityState::Registering);

        assert!(matches!(
            gate.acquire_active(),
            Err(Error::RepositoryUnavailable {
                state: "registering",
                ..
            })
        ));
        gate.set_active().unwrap();
        assert!(gate.acquire_active().is_ok());
    }

    #[tokio::test]
    async fn lease_drain_timeout_keeps_gate_fail_closed_for_retry() {
        let gate = RepoActivityGate::new("h".into(), RepoActivityState::Active);
        let lease = gate.acquire().unwrap();
        gate.begin_removal().unwrap();

        assert!(gate.wait_idle(Duration::from_millis(10)).await.is_err());
        assert_eq!(gate.snapshot(), (RepoActivityState::Removing, 1));
        assert!(gate.acquire().is_err());
        drop(lease);
        gate.wait_idle(Duration::from_millis(10)).await.unwrap();
    }

    #[tokio::test]
    async fn startup_sweep_removes_missing_ephemeral_and_preserves_persistent() {
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let missing_ephemeral = data.path().join("missing-ephemeral");
        let missing_persistent = data.path().join("missing-persistent");
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert(
            &tx,
            "ephemeral",
            &missing_ephemeral.to_string_lossy(),
            "ephemeral-hash",
            1,
        )
        .unwrap();
        registry::upsert(
            &tx,
            "persistent",
            &missing_persistent.to_string_lossy(),
            "persistent-hash",
            1,
        )
        .unwrap();
        registry::set_repository_persistent(&tx, "persistent-hash", true).unwrap();
        tx.commit().unwrap();

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let report = lifecycle.startup_sweep().await.unwrap();

        assert_eq!(report.repositories_removed, vec!["ephemeral-hash"]);
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "ephemeral-hash")
                .unwrap()
                .is_none()
        );
        assert!(
            registry::lookup_repository(&index, "persistent-hash")
                .unwrap()
                .is_some()
        );
        assert!(lifecycle.acquire_by_repo_hash("persistent-hash").is_ok());
    }

    #[tokio::test]
    async fn startup_sweep_resumes_explicit_request_while_root_exists() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert(&tx, "demo", &repo.path().to_string_lossy(), "hash", 1).unwrap();
        registry::mark_removal_requested(&tx, "hash", RepositoryRemovalReason::LastAliasRemoved, 2)
            .unwrap();
        tx.commit().unwrap();

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        lifecycle.startup_sweep().await.unwrap();

        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "hash")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            registry::list_recent_completed_removals(&index, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn startup_sweep_removes_aliasless_persistent_repository() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert_repository(&tx, "hash", &repo.path().to_string_lossy(), 1).unwrap();
        registry::set_repository_persistent(&tx, "hash", true).unwrap();
        tx.commit().unwrap();

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let report = lifecycle.startup_sweep().await.unwrap();

        assert_eq!(report.repositories_removed, vec!["hash"]);
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "hash")
                .unwrap()
                .is_none()
        );
        let events = registry::list_recent_completed_removals(&index, 10).unwrap();
        assert_eq!(events[0].reason, RepositoryRemovalReason::StartupAliasless);
    }

    #[test]
    fn registration_persistence_is_tri_state_and_alias_publish_is_delayed() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let hash = "hash".to_string();

        let permit = lifecycle
            .begin_registration(hash.clone(), repo.path().to_path_buf(), 1)
            .unwrap();
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(registry::lookup_by_alias(&index, "demo").unwrap().is_none());
        drop(index);
        lifecycle
            .publish_registration(
                permit,
                "demo",
                Some(true),
                2,
                RegistrationReconcilePolicy::None,
            )
            .unwrap();

        let permit = lifecycle
            .begin_registration(hash.clone(), repo.path().to_path_buf(), 3)
            .unwrap();
        lifecycle
            .publish_registration(permit, "demo", None, 4, RegistrationReconcilePolicy::None)
            .unwrap();
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, &hash)
                .unwrap()
                .unwrap()
                .persistent
        );
        drop(index);

        let permit = lifecycle
            .begin_registration(hash.clone(), repo.path().to_path_buf(), 5)
            .unwrap();
        lifecycle
            .publish_registration(
                permit,
                "demo",
                Some(false),
                6,
                RegistrationReconcilePolicy::None,
            )
            .unwrap();
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            !registry::lookup_repository(&index, &hash)
                .unwrap()
                .unwrap()
                .persistent
        );
    }

    #[test]
    fn registration_alias_and_catch_up_generation_publish_atomically() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());

        let permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 1)
            .unwrap();
        let publication = lifecycle
            .publish_registration(
                permit,
                "demo",
                None,
                2,
                RegistrationReconcilePolicy::ImmediateCatchUp,
            )
            .unwrap();

        assert_eq!(publication.repo_hash, "hash");
        assert_eq!(publication.catch_up_generation, Some(1));
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(registry::lookup_by_alias(&index, "demo").unwrap().is_some());
        assert_eq!(
            registry::get_reconcile_state(&index, "hash")
                .unwrap()
                .unwrap()
                .desired_generation,
            1
        );
    }

    #[test]
    fn catch_up_generation_failure_rolls_back_alias_publication() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 1)
            .unwrap();
        {
            let index = registry::open(&cas.index_db_path()).unwrap();
            index
                .execute(
                    "UPDATE repo_reconcile_state SET desired_generation = ?1
                     WHERE repo_hash = 'hash'",
                    rusqlite::params![i64::MAX],
                )
                .unwrap();
        }

        let err = lifecycle
            .publish_registration(
                permit,
                "demo",
                None,
                2,
                RegistrationReconcilePolicy::ImmediateCatchUp,
            )
            .unwrap_err();

        assert!(format!("{err}").contains("overflow"));
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_by_alias(&index, "demo").unwrap().is_none(),
            "alias publication must roll back with catch-up generation failure"
        );
    }

    #[tokio::test]
    async fn catch_up_failure_cleans_up_newly_created_owner_and_gate() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 1)
            .unwrap();
        let first_gate = lifecycle.gate("hash").unwrap();
        {
            let index = registry::open(&cas.index_db_path()).unwrap();
            index
                .execute(
                    "UPDATE repo_reconcile_state SET desired_generation = ?1
                     WHERE repo_hash = 'hash'",
                    rusqlite::params![i64::MAX],
                )
                .unwrap();
        }

        let err = lifecycle
            .publish_registration(
                permit,
                "demo",
                None,
                2,
                RegistrationReconcilePolicy::ImmediateCatchUp,
            )
            .unwrap_err();

        assert!(format!("{err}").contains("overflow"));
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "hash")
                .unwrap()
                .is_none()
        );
        assert!(registry::lookup_by_alias(&index, "demo").unwrap().is_none());
        assert_eq!(
            first_gate.snapshot(),
            (RepoActivityState::Removed, 0),
            "failed publication must release the permit and close its gate"
        );
        drop(index);

        let retry = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 3)
            .unwrap();
        let retry_gate = lifecycle.gate("hash").unwrap();
        assert!(!Arc::ptr_eq(&first_gate, &retry_gate));
        assert_eq!(retry_gate.snapshot(), (RepoActivityState::Registering, 1));
        lifecycle.abort_registration(retry).await.unwrap();
    }

    #[tokio::test]
    async fn failed_new_registration_is_removed_without_publishing_alias() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 1)
            .unwrap();

        lifecycle.abort_registration(permit).await.unwrap();

        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "hash")
                .unwrap()
                .is_none()
        );
        assert!(registry::lookup_by_alias(&index, "demo").unwrap().is_none());
        assert_eq!(
            registry::list_recent_completed_removals(&index, 10).unwrap()[0].reason,
            RepositoryRemovalReason::RegistrationAborted
        );
    }

    #[tokio::test]
    async fn dropped_runtime_binding_leaves_durable_removal_request() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert(&tx, "demo", &repo.path().to_string_lossy(), "hash", 1).unwrap();
        tx.commit().unwrap();

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        lifecycle.startup_sweep().await.unwrap();
        let jobs = JobManager::new(cas.clone());
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        let watchers = Arc::new(WatchManager::with_reconcile(cas.clone(), reconcile.clone()));
        lifecycle
            .bind_runtime(
                Arc::downgrade(&jobs),
                Arc::downgrade(&watchers),
                Arc::downgrade(&reconcile),
            )
            .unwrap();
        drop(jobs);

        let err = lifecycle
            .process_runtime_removal(&RemovalIntent::LastAliasRemoved {
                repo_hash: "hash".into(),
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("job manager dropped"));
        let index = registry::open(&cas.index_db_path()).unwrap();
        let owner = registry::lookup_repository(&index, "hash")
            .unwrap()
            .unwrap();
        assert_eq!(
            owner.removal_request.unwrap().reason,
            RepositoryRemovalReason::LastAliasRemoved
        );
        lifecycle.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn retargeting_final_alias_marks_old_persistent_owner_for_removal() {
        let old_root = tempfile::tempdir().unwrap();
        let new_root = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());

        let old = lifecycle
            .begin_registration("old".into(), old_root.path().to_path_buf(), 1)
            .unwrap();
        lifecycle
            .publish_registration(
                old,
                "demo",
                Some(true),
                2,
                RegistrationReconcilePolicy::None,
            )
            .unwrap();
        let new = lifecycle
            .begin_registration("new".into(), new_root.path().to_path_buf(), 3)
            .unwrap();
        lifecycle
            .publish_registration(new, "demo", None, 4, RegistrationReconcilePolicy::None)
            .unwrap();

        let index = registry::open(&cas.index_db_path()).unwrap();
        assert_eq!(
            registry::lookup_by_alias(&index, "demo")
                .unwrap()
                .unwrap()
                .repo_hash,
            "new"
        );
        let old = registry::lookup_repository(&index, "old").unwrap().unwrap();
        assert!(old.persistent);
        assert_eq!(
            old.removal_request.unwrap().reason,
            RepositoryRemovalReason::AliasRetargeted
        );
        assert_eq!(registry::count_aliases_for_repo(&index, "old").unwrap(), 0);
    }

    #[tokio::test]
    async fn committed_alias_retarget_survives_runtime_wake_failure() {
        let old_root = tempfile::tempdir().unwrap();
        let new_root = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());

        let old = lifecycle
            .begin_registration("old".into(), old_root.path().to_path_buf(), 1)
            .unwrap();
        lifecycle
            .publish_registration(old, "demo", None, 2, RegistrationReconcilePolicy::None)
            .unwrap();
        let new = lifecycle
            .begin_registration("new".into(), new_root.path().to_path_buf(), 3)
            .unwrap();
        lifecycle.shutdown(Duration::from_secs(1)).await.unwrap();

        let publication = lifecycle
            .publish_registration(new, "demo", None, 4, RegistrationReconcilePolicy::None)
            .unwrap();

        assert_eq!(publication.repo_hash, "new");
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert_eq!(
            registry::lookup_by_alias(&index, "demo")
                .unwrap()
                .unwrap()
                .repo_hash,
            "new"
        );
        assert_eq!(
            registry::lookup_repository(&index, "old")
                .unwrap()
                .unwrap()
                .removal_request
                .unwrap()
                .reason,
            RepositoryRemovalReason::AliasRetargeted
        );
    }

    #[tokio::test]
    async fn preexisting_registration_permit_cannot_revive_removing_owner() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let initial = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 1)
            .unwrap();
        lifecycle
            .publish_registration(initial, "demo", None, 2, RegistrationReconcilePolicy::None)
            .unwrap();
        let stale_permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 3)
            .unwrap();

        assert!(lifecycle.remove_alias("demo").await.is_err());
        let err = lifecycle
            .publish_registration(
                stale_permit,
                "demo",
                None,
                4,
                RegistrationReconcilePolicy::None,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            Error::RepositoryUnavailable {
                state: "removing",
                ..
            }
        ));
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "hash")
                .unwrap()
                .unwrap()
                .removal_request
                .is_some()
        );
    }

    #[tokio::test]
    async fn final_alias_removal_deletes_canonical_state_even_when_persistent() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 1)
            .unwrap();
        lifecycle
            .publish_registration(
                permit,
                "demo",
                Some(true),
                2,
                RegistrationReconcilePolicy::None,
            )
            .unwrap();
        let jobs = JobManager::new(cas.clone());
        let reconcile = RepoReconcileManager::new(cas.clone(), None);
        let watchers = Arc::new(WatchManager::with_reconcile(cas.clone(), reconcile.clone()));
        lifecycle
            .bind_runtime(
                Arc::downgrade(&jobs),
                Arc::downgrade(&watchers),
                Arc::downgrade(&reconcile),
            )
            .unwrap();

        assert!(lifecycle.remove_alias("demo").await.unwrap());

        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_repository(&index, "hash")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            registry::list_recent_completed_removals(&index, 10).unwrap()[0].reason,
            RepositoryRemovalReason::LastAliasRemoved
        );
        drop(index);

        let permit = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 3)
            .unwrap();
        lifecycle
            .publish_registration(
                permit,
                "demo-again",
                None,
                4,
                RegistrationReconcilePolicy::None,
            )
            .unwrap();
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert!(
            registry::lookup_by_alias(&index, "demo-again")
                .unwrap()
                .is_some()
        );
        lifecycle.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn startup_retries_pending_store_cleanup_before_repository_scan() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert(&tx, "demo", &repo.path().to_string_lossy(), "hash", 1).unwrap();
        registry::mark_removal_requested(&tx, "hash", RepositoryRemovalReason::LastAliasRemoved, 2)
            .unwrap();
        let event_id = registry::delete_repository_with_event(&tx, "hash", 3)
            .unwrap()
            .unwrap();
        tx.commit().unwrap();
        let repo_dir = cas.repo_dir("hash");
        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::write(repo_dir.join("leftover"), b"x").unwrap();

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let report = lifecycle.startup_sweep().await.unwrap();

        assert_eq!(report.cleanup_retried, vec!["hash"]);
        assert!(!repo_dir.exists());
        let index = registry::open(&cas.index_db_path()).unwrap();
        let event = registry::list_recent_completed_removals(&index, 10)
            .unwrap()
            .into_iter()
            .find(|event| event.event_id == event_id)
            .unwrap();
        assert_eq!(event.store_cleanup_state, StoreCleanupState::Complete);
    }

    #[test]
    fn registration_waits_for_pending_cleanup_of_same_hash() {
        let repo = tempfile::tempdir().unwrap();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert_repository(&tx, "hash", &repo.path().to_string_lossy(), 1).unwrap();
        registry::mark_removal_requested(
            &tx,
            "hash",
            RepositoryRemovalReason::RegistrationAborted,
            2,
        )
        .unwrap();
        registry::delete_repository_with_event(&tx, "hash", 3)
            .unwrap()
            .unwrap();
        tx.commit().unwrap();

        let lifecycle = RepoLifecycleManager::new(cas);
        let err = lifecycle
            .begin_registration("hash".into(), repo.path().to_path_buf(), 4)
            .unwrap_err();
        assert!(matches!(
            err,
            Error::RepositoryUnavailable {
                state: "cleanup_pending",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn missing_root_reconcile_routes_through_lifecycle_owner() {
        let repo = tempfile::tempdir().unwrap();
        let root = repo.path().to_path_buf();
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let mut index = registry::open(&cas.index_db_path()).unwrap();
        let tx = index.transaction().unwrap();
        registry::upsert(&tx, "demo", &root.to_string_lossy(), "hash", 1).unwrap();
        tx.commit().unwrap();
        drop(crate::cas::store::open(&cas.store_db_path("hash")).unwrap());

        let lifecycle = RepoLifecycleManager::new(cas.clone());
        lifecycle.startup_sweep().await.unwrap();
        let jobs = JobManager::with_lifecycle(cas.clone(), lifecycle.clone());
        let reconcile = RepoReconcileManager::new_with_lifecycle(
            cas.clone(),
            Some(jobs.clone()),
            lifecycle.clone(),
        );
        let watchers = Arc::new(WatchManager::with_reconcile(cas.clone(), reconcile.clone()));
        lifecycle
            .bind_runtime(
                Arc::downgrade(&jobs),
                Arc::downgrade(&watchers),
                Arc::downgrade(&reconcile),
            )
            .unwrap();
        repo.close().unwrap();

        reconcile
            .request_dirty_by_repo_hash("hash".into(), ReconcileTrigger::WatchEvent)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let index = registry::open(&cas.index_db_path()).unwrap();
            if registry::lookup_repository(&index, "hash")
                .unwrap()
                .is_none()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "missing ephemeral repository was not removed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(!cas.repo_dir("hash").exists());
        let index = registry::open(&cas.index_db_path()).unwrap();
        assert_eq!(
            registry::list_recent_completed_removals(&index, 10).unwrap()[0].reason,
            RepositoryRemovalReason::MissingRoot
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            reconcile.test_attempts_started(),
            1,
            "missing-root handoff must stop the worker instead of spinning on the dirty gap"
        );
        reconcile.shutdown(Duration::from_secs(1)).await;
        lifecycle.shutdown(Duration::from_secs(1)).await.unwrap();
    }
}
