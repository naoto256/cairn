//! Daemon-scoped ownership markers and bounded orphan-process cleanup.
//!
//! Only pooled, long-lived LSP children receive these markers. Availability
//! probes and standalone `LspClient` constructors intentionally remain outside
//! this contract. Runtime cleanup always signals the spawn-verified process
//! group and reaps its leader first; this module then kills exact marked
//! survivors by PID. Marker-derived process-group signalling is forbidden.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tracing::warn;

use super::{Error, Result};

pub(super) const OWNER_ENV: &str = "CAIRN_LSP_OWNER";
pub(super) const FAMILY_ENV: &str = "CAIRN_LSP_FAMILY";

const OWNER_PREFIX: &str = "cairn-owner-v1:";
const FAMILY_PREFIX: &str = "cairn-family-v1:";
const MAX_CANONICAL_ROOT_BYTES: usize = 4096;
const STARTUP_NONCE_BYTES: usize = 16;
const SWEEP_PASSES: usize = 3;
const SWEEP_RESCAN_DELAY: Duration = Duration::from_millis(25);
const MAX_RECORDED_RESIDUALS: usize = 16;

static OWNER_CONTEXT: OnceLock<Arc<ProcessOwnerContext>> = OnceLock::new();
static OWNER_INIT: Mutex<()> = Mutex::new(());
static SWEEP_DIAGNOSTICS: OnceLock<Mutex<SweepDiagnostics>> = OnceLock::new();

/// Exact marker identity installed into one pool-managed child.
#[derive(Clone)]
pub(super) struct ProcessMarker {
    owner: Arc<str>,
    family: Arc<str>,
    #[cfg(test)]
    backend: Option<Arc<dyn SweepBackend>>,
}

impl ProcessMarker {
    pub(super) fn owner(&self) -> &str {
        &self.owner
    }

    pub(super) fn family(&self) -> &str {
        &self.family
    }

    pub(super) fn env(&self) -> [(&'static str, &str); 2] {
        [(OWNER_ENV, self.owner()), (FAMILY_ENV, self.family())]
    }
}

/// Per-daemon marker authority. The nonce is regenerated after every daemon
/// lock acquisition and the checked sequence makes every child family unique.
pub(super) struct ProcessOwnerContext {
    owner: Arc<str>,
    nonce_hex: String,
    next_spawn_seq: AtomicU64,
    #[cfg(test)]
    backend: Option<Arc<dyn SweepBackend>>,
}

impl ProcessOwnerContext {
    fn new(owner: String, nonce: [u8; STARTUP_NONCE_BYTES]) -> Self {
        Self {
            owner: Arc::from(owner),
            nonce_hex: hex::encode(nonce),
            next_spawn_seq: AtomicU64::new(0),
            #[cfg(test)]
            backend: None,
        }
    }

    pub(super) fn marker_for_spawn(&self) -> Result<ProcessMarker> {
        let prior = self
            .next_spawn_seq
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| Error::Protocol("LSP ownership spawn sequence overflow".into()))?;
        let sequence = prior + 1;
        Ok(ProcessMarker {
            owner: Arc::clone(&self.owner),
            family: Arc::from(format!("{FAMILY_PREFIX}{}:{sequence:016x}", self.nonce_hex)),
            #[cfg(test)]
            backend: self.backend.clone(),
        })
    }

    #[cfg(test)]
    fn with_sequence_for_test(owner: String, nonce: [u8; STARTUP_NONCE_BYTES], value: u64) -> Self {
        let context = Self::new(owner, nonce);
        context.next_spawn_seq.store(value, Ordering::SeqCst);
        context
    }

    #[cfg(test)]
    pub(super) fn for_test(owner: &str, nonce: [u8; STARTUP_NONCE_BYTES]) -> Arc<Self> {
        Arc::new(Self::new(owner.into(), nonce))
    }

    #[cfg(test)]
    pub(super) fn for_test_with_residual_sweep(
        owner: &str,
        nonce: [u8; STARTUP_NONCE_BYTES],
    ) -> Arc<Self> {
        let mut context = Self::new(owner.into(), nonce);
        context.backend = Some(Arc::new(AlwaysResidualBackend));
        Arc::new(context)
    }

    #[cfg(test)]
    pub(super) fn for_test_with_kill_failure_sweep(
        owner: &str,
        nonce: [u8; STARTUP_NONCE_BYTES],
    ) -> Arc<Self> {
        Self::for_test_with_matching_backend(owner, nonce, InjectedKill::Failure)
    }

    #[cfg(test)]
    pub(super) fn for_test_with_persistent_sweep(
        owner: &str,
        nonce: [u8; STARTUP_NONCE_BYTES],
    ) -> Arc<Self> {
        Self::for_test_with_matching_backend(owner, nonce, InjectedKill::Persistent)
    }

    #[cfg(test)]
    fn for_test_with_matching_backend(
        owner: &str,
        nonce: [u8; STARTUP_NONCE_BYTES],
        kill: InjectedKill,
    ) -> Arc<Self> {
        let mut context = Self::new(owner.into(), nonce);
        let family = format!("{FAMILY_PREFIX}{}:{:016x}", hex::encode(nonce), 1_u64);
        context.backend = Some(Arc::new(MatchingInjectedBackend {
            owner: owner.into(),
            family,
            kill,
        }));
        Arc::new(context)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SweepDiagnostics {
    pub initialized: bool,
    pub last_scope: Option<&'static str>,
    pub examined: usize,
    pub matched: usize,
    pub signalled: usize,
    pub confirmed_execution_absent: usize,
    pub remaining: usize,
    pub residual_count: usize,
    pub residuals: Vec<String>,
}

#[derive(Debug, Default)]
struct SweepReport {
    scope: &'static str,
    examined: usize,
    matched: usize,
    signalled: usize,
    confirmed_execution_absent: usize,
    remaining: usize,
    residuals: Vec<String>,
}

impl SweepReport {
    fn residual_count(&self) -> usize {
        self.residuals.len()
    }
}

/// Initialize the daemon ownership context exactly once. On non-Linux/macOS
/// targets this is a compatibility no-op: those platforms retain their prior
/// leader-only process lifecycle and no marker is injected.
pub(super) fn initialize_daemon_process_owner(data_root: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let _guard = OWNER_INIT
            .lock()
            .map_err(|_| Error::Protocol("LSP ownership initializer poisoned".into()))?;
        if OWNER_CONTEXT.get().is_some() {
            return Err(Error::Protocol(
                "LSP daemon process ownership is already initialized".into(),
            ));
        }

        let canonical = std::fs::canonicalize(data_root).map_err(|error| {
            Error::Protocol(format!(
                "canonicalize LSP ownership data root {}: {error}",
                data_root.display()
            ))
        })?;
        let owner = owner_value_from_canonical_path(&canonical)?;
        let nonce = read_startup_nonce()?;
        let context = Arc::new(ProcessOwnerContext::new(owner, nonce));
        let report = publish_after_sweep(&OWNER_CONTEXT, Arc::clone(&context), |owner| {
            sweep_owner(owner)
        })?;
        record_report(&report, true);
        log_report(&report);
        Ok(())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = data_root;
        Ok(())
    }
}

fn publish_after_sweep(
    cell: &OnceLock<Arc<ProcessOwnerContext>>,
    context: Arc<ProcessOwnerContext>,
    sweep: impl FnOnce(&str) -> SweepReport,
) -> Result<SweepReport> {
    if cell.get().is_some() {
        return Err(Error::Protocol(
            "LSP daemon process ownership is already initialized".into(),
        ));
    }
    let report = sweep(&context.owner);
    cell.set(context)
        .map_err(|_| Error::Protocol("LSP daemon process ownership raced initialization".into()))?;
    Ok(report)
}

#[cfg(not(test))]
pub(super) fn daemon_process_owner() -> Option<Arc<ProcessOwnerContext>> {
    OWNER_CONTEXT.get().cloned()
}

pub(super) fn sweep_family(marker: &ProcessMarker) {
    let matcher = MarkerMatch::Family {
        owner: marker.owner(),
        family: marker.family(),
    };
    #[cfg(test)]
    let report = marker.backend.as_ref().map_or_else(
        || run_sweep(matcher),
        |backend| sweep_with_backend(backend.as_ref(), matcher),
    );
    #[cfg(not(test))]
    let report = run_sweep(matcher);
    record_report(&report, true);
    log_report(&report);
}

pub(crate) fn sweep_diagnostics() -> SweepDiagnostics {
    SWEEP_DIAGNOSTICS
        .get_or_init(|| Mutex::new(SweepDiagnostics::default()))
        .lock()
        .map_or_else(
            |_| SweepDiagnostics {
                initialized: true,
                last_scope: Some("diagnostic"),
                residuals: vec!["LSP orphan diagnostic state is unavailable".into()],
                residual_count: 1,
                ..SweepDiagnostics::default()
            },
            |diagnostics| diagnostics.clone(),
        )
}

fn read_startup_nonce() -> Result<[u8; STARTUP_NONCE_BYTES]> {
    let mut random = File::open("/dev/urandom")
        .map_err(|error| Error::Protocol(format!("open LSP ownership nonce source: {error}")))?;
    read_nonce_from(&mut random)
}

fn read_nonce_from(reader: &mut impl Read) -> Result<[u8; STARTUP_NONCE_BYTES]> {
    let mut nonce = [0_u8; STARTUP_NONCE_BYTES];
    reader
        .read_exact(&mut nonce)
        .map_err(|error| Error::Protocol(format!("read LSP ownership startup nonce: {error}")))?;
    Ok(nonce)
}

#[cfg(unix)]
fn owner_value_from_canonical_path(path: &Path) -> Result<String> {
    use std::os::unix::ffi::OsStrExt;

    owner_value_from_raw(path.as_os_str().as_bytes())
}

#[cfg(not(unix))]
fn owner_value_from_canonical_path(_path: &Path) -> Result<String> {
    Err(Error::Protocol(
        "LSP process ownership markers require Unix path bytes".into(),
    ))
}

fn owner_value_from_raw(raw: &[u8]) -> Result<String> {
    if raw.len() > MAX_CANONICAL_ROOT_BYTES {
        return Err(Error::Protocol(format!(
            "canonical LSP ownership data root exceeds {MAX_CANONICAL_ROOT_BYTES} bytes"
        )));
    }
    Ok(format!("{OWNER_PREFIX}{}", hex::encode(raw)))
}

fn sweep_owner(owner: &str) -> SweepReport {
    run_sweep(MarkerMatch::Owner(owner))
}

fn run_sweep(marker: MarkerMatch<'_>) -> SweepReport {
    #[cfg(target_os = "linux")]
    {
        sweep_with_backend(&linux::LinuxBackend, marker)
    }
    #[cfg(target_os = "macos")]
    {
        sweep_with_backend(&macos::MacOsBackend, marker)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = marker;
        SweepReport::default()
    }
}

fn record_report(report: &SweepReport, initialized: bool) {
    let diagnostics = SWEEP_DIAGNOSTICS.get_or_init(|| Mutex::new(SweepDiagnostics::default()));
    let Ok(mut diagnostics) = diagnostics.lock() else {
        warn!("lsp orphan sweep diagnostic mutex poisoned");
        return;
    };
    *diagnostics = SweepDiagnostics {
        initialized,
        last_scope: Some(report.scope),
        examined: report.examined,
        matched: report.matched,
        signalled: report.signalled,
        confirmed_execution_absent: report.confirmed_execution_absent,
        remaining: report.remaining,
        residual_count: report.residual_count(),
        residuals: report
            .residuals
            .iter()
            .take(MAX_RECORDED_RESIDUALS)
            .cloned()
            .collect(),
    };
}

fn log_report(report: &SweepReport) {
    if report.residuals.is_empty() {
        if report.matched != 0 {
            warn!(
                scope = report.scope,
                examined = report.examined,
                matched = report.matched,
                signalled = report.signalled,
                confirmed_execution_absent = report.confirmed_execution_absent,
                remaining = report.remaining,
                "removed marked LSP descendants; inherited helpers such as sccache may be included"
            );
        }
    } else {
        warn!(
            scope = report.scope,
            examined = report.examined,
            matched = report.matched,
            signalled = report.signalled,
            confirmed_execution_absent = report.confirmed_execution_absent,
            remaining = report.remaining,
            residuals = report.residuals.len(),
            details = ?report.residuals,
            "LSP orphan sweep left residual inspection or termination errors; admission continues"
        );
    }
}

#[derive(Clone, Copy)]
enum MarkerMatch<'a> {
    Owner(&'a str),
    Family { owner: &'a str, family: &'a str },
}

impl MarkerMatch<'_> {
    fn scope(self) -> &'static str {
        match self {
            Self::Owner(_) => "startup-owner",
            Self::Family { .. } => "runtime-family",
        }
    }

    fn matches(self, environment: &[Vec<u8>]) -> bool {
        let owner = match self {
            Self::Owner(owner) | Self::Family { owner, .. } => owner,
        };
        if !has_exact_env(environment, OWNER_ENV, owner) {
            return false;
        }
        match self {
            Self::Owner(_) => true,
            Self::Family { family, .. } => has_exact_env(environment, FAMILY_ENV, family),
        }
    }
}

fn has_exact_env(environment: &[Vec<u8>], key: &str, value: &str) -> bool {
    let mut expected = Vec::with_capacity(key.len() + value.len() + 1);
    expected.extend_from_slice(key.as_bytes());
    expected.push(b'=');
    expected.extend_from_slice(value.as_bytes());
    environment.iter().any(|entry| entry == &expected)
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ProcessIdentity {
    pid: i32,
    uid: u32,
    start: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionState {
    Executing,
    Zombie,
}

impl ExecutionState {
    fn is_absent(self) -> bool {
        matches!(self, Self::Zombie)
    }
}

struct ProcessSnapshot {
    identity: ProcessIdentity,
    execution_state: ExecutionState,
    environment: Vec<Vec<u8>>,
}

enum InspectFailure {
    Gone,
    Residual(String),
}

enum KillResult {
    Killed,
    Gone,
    IdentityChanged,
}

trait SweepBackend: Send + Sync {
    fn list_pids(&self) -> std::result::Result<PidListing, String>;
    fn inspect(&self, pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure>;
    fn kill_exact(&self, expected: &ProcessIdentity) -> std::result::Result<KillResult, String>;
}

#[derive(Default)]
struct PidListing {
    pids: Vec<i32>,
    residuals: Vec<String>,
}

#[cfg(test)]
struct AlwaysResidualBackend;

#[cfg(test)]
impl SweepBackend for AlwaysResidualBackend {
    fn list_pids(&self) -> std::result::Result<PidListing, String> {
        Ok(PidListing {
            pids: vec![i32::MAX - 1],
            residuals: Vec::new(),
        })
    }

    fn inspect(&self, _pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
        Err(InspectFailure::Residual(
            "test-injected marked process inspection failure".into(),
        ))
    }

    fn kill_exact(&self, _expected: &ProcessIdentity) -> std::result::Result<KillResult, String> {
        unreachable!("uninspectable process must not be signalled")
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum InjectedKill {
    Failure,
    Persistent,
}

#[cfg(test)]
struct MatchingInjectedBackend {
    owner: String,
    family: String,
    kill: InjectedKill,
}

#[cfg(test)]
impl SweepBackend for MatchingInjectedBackend {
    fn list_pids(&self) -> std::result::Result<PidListing, String> {
        Ok(PidListing {
            pids: vec![i32::MAX - 2],
            residuals: Vec::new(),
        })
    }

    fn inspect(&self, pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
        Ok(ProcessSnapshot {
            identity: ProcessIdentity {
                pid,
                uid: 1,
                start: 77,
            },
            execution_state: ExecutionState::Executing,
            environment: vec![
                format!("{OWNER_ENV}={}", self.owner).into_bytes(),
                format!("{FAMILY_ENV}={}", self.family).into_bytes(),
            ],
        })
    }

    fn kill_exact(&self, _expected: &ProcessIdentity) -> std::result::Result<KillResult, String> {
        match self.kill {
            InjectedKill::Failure => Err("test-injected marked process kill failure".into()),
            InjectedKill::Persistent => Ok(KillResult::Killed),
        }
    }
}

fn sweep_with_backend(
    backend: &(impl SweepBackend + ?Sized),
    marker: MarkerMatch<'_>,
) -> SweepReport {
    let mut examined = 0_usize;
    let mut matched = HashSet::new();
    let mut signalled_count = 0_usize;
    let mut confirmed_execution_absent = HashSet::new();
    let mut issues: BTreeMap<String, String> = BTreeMap::new();
    let self_pid = i32::try_from(std::process::id()).unwrap_or(i32::MAX);
    let mut signalled = HashMap::<i32, ProcessIdentity>::new();
    let mut retired_pids = HashSet::new();

    for pass in 0..SWEEP_PASSES {
        issues.retain(|key, _| !key.starts_with("enumerate-entry:"));
        let mut candidates: HashSet<i32> = signalled.keys().copied().collect();
        match backend.list_pids() {
            Ok(listing) => {
                issues.remove("enumerate");
                candidates.extend(listing.pids);
                for residual in listing.residuals {
                    issues.insert(format!("enumerate-entry:{residual}"), residual);
                }
            }
            Err(error) => {
                issues.insert("enumerate".into(), format!("enumerate processes: {error}"));
            }
        }
        let mut candidates: Vec<_> = candidates.into_iter().collect();
        candidates.sort_unstable();
        for pid in candidates {
            if pid <= 1 || pid == self_pid || retired_pids.contains(&pid) {
                continue;
            }
            examined = examined.saturating_add(1);
            let snapshot = match backend.inspect(pid) {
                Ok(snapshot) => {
                    issues.remove(&format!("inspect:{pid}"));
                    snapshot
                }
                Err(InspectFailure::Gone) => {
                    issues.remove(&format!("inspect:{pid}"));
                    issues.remove(&format!("kill:{pid}"));
                    if let Some(identity) = signalled.remove(&pid) {
                        confirmed_execution_absent.insert(identity);
                    }
                    continue;
                }
                Err(InspectFailure::Residual(error)) => {
                    issues.insert(
                        format!("inspect:{pid}"),
                        format!("inspect pid {pid}: {error}"),
                    );
                    continue;
                }
            };

            if let Some(expected) = signalled.get(&pid) {
                if snapshot.identity == *expected {
                    if snapshot.execution_state.is_absent() {
                        let expected = signalled.remove(&pid).expect("signalled identity exists");
                        confirmed_execution_absent.insert(expected);
                        issues.remove(&format!("kill:{pid}"));
                    }
                    continue;
                }
                let expected = signalled.remove(&pid).expect("signalled identity exists");
                confirmed_execution_absent.insert(expected);
                issues.remove(&format!("kill:{pid}"));
                retired_pids.insert(pid);
                continue;
            }

            if !marker.matches(&snapshot.environment) {
                issues.remove(&format!("kill:{pid}"));
                continue;
            }
            matched.insert(snapshot.identity.clone());
            match backend.kill_exact(&snapshot.identity) {
                Ok(KillResult::Killed) => {
                    issues.remove(&format!("kill:{pid}"));
                    signalled.insert(pid, snapshot.identity);
                    signalled_count = signalled_count.saturating_add(1);
                }
                Ok(KillResult::Gone) => {
                    issues.remove(&format!("kill:{pid}"));
                    confirmed_execution_absent.insert(snapshot.identity);
                }
                Ok(KillResult::IdentityChanged) => {
                    issues.remove(&format!("kill:{pid}"));
                    confirmed_execution_absent.insert(snapshot.identity);
                    retired_pids.insert(pid);
                }
                Err(error) => {
                    issues.insert(format!("kill:{pid}"), format!("kill pid {pid}: {error}"));
                }
            }
        }
        if pass + 1 != SWEEP_PASSES {
            std::thread::sleep(SWEEP_RESCAN_DELAY);
        }
    }
    let final_signalled: Vec<_> = signalled.values().cloned().collect();
    for expected in final_signalled {
        examined = examined.saturating_add(1);
        match backend.inspect(expected.pid) {
            Err(InspectFailure::Gone) => {
                issues.remove(&format!("inspect:{}", expected.pid));
                issues.remove(&format!("kill:{}", expected.pid));
                signalled.remove(&expected.pid);
                confirmed_execution_absent.insert(expected);
            }
            Ok(snapshot) if snapshot.identity != expected => {
                issues.remove(&format!("inspect:{}", expected.pid));
                issues.remove(&format!("kill:{}", expected.pid));
                signalled.remove(&expected.pid);
                confirmed_execution_absent.insert(expected);
            }
            Ok(snapshot) if snapshot.execution_state.is_absent() => {
                issues.remove(&format!("inspect:{}", expected.pid));
                issues.remove(&format!("kill:{}", expected.pid));
                signalled.remove(&expected.pid);
                confirmed_execution_absent.insert(expected);
            }
            Ok(_) => {
                issues.remove(&format!("inspect:{}", expected.pid));
            }
            Err(InspectFailure::Residual(error)) => {
                issues.insert(
                    format!("inspect:{}", expected.pid),
                    format!("final inspect pid {}: {error}", expected.pid),
                );
            }
        }
    }
    for identity in signalled.values() {
        issues.insert(
            format!("remaining:{}:{}", identity.pid, identity.start),
            format!(
                "pid {} with start identity {} remained after accepted signal",
                identity.pid, identity.start
            ),
        );
    }
    SweepReport {
        scope: marker.scope(),
        examined,
        matched: matched.len(),
        signalled: signalled_count,
        confirmed_execution_absent: confirmed_execution_absent.len(),
        remaining: signalled.len(),
        residuals: issues.into_values().collect(),
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::OsString;
    use std::fs;

    use rustix::process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal};

    use super::*;

    pub(super) struct LinuxBackend;

    impl SweepBackend for LinuxBackend {
        fn list_pids(&self) -> std::result::Result<PidListing, String> {
            let entries = fs::read_dir("/proc").map_err(|error| error.to_string())?;
            Ok(collect_numeric_pids(
                entries.map(|entry| entry.map(|entry| entry.file_name())),
            ))
        }

        fn inspect(&self, pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
            inspect_linux(pid)
        }

        fn kill_exact(
            &self,
            expected: &ProcessIdentity,
        ) -> std::result::Result<KillResult, String> {
            let Some(pid) = Pid::from_raw(expected.pid) else {
                return Ok(KillResult::IdentityChanged);
            };
            let pidfd = match pidfd_open(pid, PidfdFlags::empty()) {
                Ok(pidfd) => pidfd,
                Err(rustix::io::Errno::SRCH) => return Ok(KillResult::Gone),
                Err(error) => return Err(format!("pidfd_open: {error}")),
            };
            match inspect_linux(expected.pid) {
                Ok(snapshot) if snapshot.identity == *expected => {}
                Ok(_) => return Ok(KillResult::IdentityChanged),
                Err(InspectFailure::Gone) => return Ok(KillResult::Gone),
                Err(InspectFailure::Residual(error)) => {
                    return Err(format!("identity reread: {error}"));
                }
            }
            match pidfd_send_signal(pidfd, Signal::KILL) {
                Ok(()) => Ok(KillResult::Killed),
                Err(rustix::io::Errno::SRCH) => Ok(KillResult::Gone),
                Err(error) => Err(format!("pidfd_send_signal: {error}")),
            }
        }
    }

    fn collect_numeric_pids(entries: impl IntoIterator<Item = io::Result<OsString>>) -> PidListing {
        let mut listing = PidListing::default();
        for entry in entries {
            match entry {
                Ok(name) => {
                    if let Some(name) = name.to_str()
                        && let Ok(pid) = name.parse::<i32>()
                    {
                        listing.pids.push(pid);
                    }
                }
                Err(error) => listing
                    .residuals
                    .push(format!("read /proc directory entry: {error}")),
            }
        }
        listing
    }

    fn inspect_linux(pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
        inspect_linux_with(
            pid,
            || read_proc(pid, "status"),
            || read_proc(pid, "stat"),
            || read_proc(pid, "environ"),
        )
    }

    fn inspect_linux_with(
        pid: i32,
        read_status: impl FnOnce() -> std::result::Result<Vec<u8>, InspectFailure>,
        read_stat: impl FnOnce() -> std::result::Result<Vec<u8>, InspectFailure>,
        read_environment: impl FnOnce() -> std::result::Result<Vec<u8>, InspectFailure>,
    ) -> std::result::Result<ProcessSnapshot, InspectFailure> {
        let status = read_status()?;
        let uid = parse_effective_uid(&status)
            .ok_or_else(|| InspectFailure::Residual("missing effective uid".into()))?;
        if uid != rustix::process::geteuid().as_raw() {
            return Err(InspectFailure::Gone);
        }
        let stat = read_stat()?;
        let (execution_state, start) = parse_execution_state_and_start_time(&stat)
            .ok_or_else(|| InspectFailure::Residual("malformed stat state/start time".into()))?;
        let environment = if execution_state.is_absent() {
            Vec::new()
        } else {
            read_environment()?
                .split(|byte| *byte == 0)
                .filter(|entry| !entry.is_empty())
                .map(<[u8]>::to_vec)
                .collect()
        };
        Ok(ProcessSnapshot {
            identity: ProcessIdentity { pid, uid, start },
            execution_state,
            environment,
        })
    }

    fn read_proc(pid: i32, name: &str) -> std::result::Result<Vec<u8>, InspectFailure> {
        fs::read(format!("/proc/{pid}/{name}")).map_err(|error| match error.kind() {
            io::ErrorKind::NotFound => InspectFailure::Gone,
            _ => InspectFailure::Residual(format!("read {name}: {error}")),
        })
    }

    fn parse_effective_uid(status: &[u8]) -> Option<u32> {
        std::str::from_utf8(status).ok()?.lines().find_map(|line| {
            line.strip_prefix("Uid:")?
                .split_whitespace()
                .nth(1)?
                .parse()
                .ok()
        })
    }

    fn parse_execution_state_and_start_time(stat: &[u8]) -> Option<(ExecutionState, u128)> {
        let close = stat.iter().rposition(|byte| *byte == b')')?;
        let mut fields = std::str::from_utf8(stat.get(close + 1..)?)
            .ok()?
            .split_whitespace();
        let execution_state = match fields.next()? {
            "Z" => ExecutionState::Zombie,
            _ => ExecutionState::Executing,
        };
        let start = fields.nth(18)?.parse().ok()?;
        Some((execution_state, start))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn pidfd_open_esrch_is_gone_without_numeric_signal_fallback() {
            let expected = ProcessIdentity {
                pid: i32::MAX,
                uid: rustix::process::geteuid().as_raw(),
                start: 0,
            };
            assert!(matches!(
                LinuxBackend.kill_exact(&expected),
                Ok(KillResult::Gone)
            ));
        }

        #[test]
        fn proc_enumeration_keeps_entry_errors_and_defers_uid_to_inspection() {
            let listing = collect_numeric_pids([
                Ok(OsString::from("123")),
                Ok(OsString::from("not-a-pid")),
                Err(io::Error::from_raw_os_error(libc::EACCES)),
            ]);
            assert_eq!(listing.pids, vec![123]);
            assert_eq!(listing.residuals.len(), 1);
            assert!(listing.residuals[0].contains("Permission denied"));
        }

        #[test]
        fn proc_read_distinguishes_gone_from_permission_and_io_errors() {
            let classify = |error: io::Error| match error.kind() {
                io::ErrorKind::NotFound => InspectFailure::Gone,
                _ => InspectFailure::Residual(format!("read status: {error}")),
            };
            assert!(matches!(
                classify(io::Error::from_raw_os_error(libc::ENOENT)),
                InspectFailure::Gone
            ));
            for errno in [libc::EACCES, libc::EIO] {
                assert!(matches!(
                    classify(io::Error::from_raw_os_error(errno)),
                    InspectFailure::Residual(_)
                ));
            }
        }

        #[test]
        fn proc_stat_parser_preserves_identity_start_and_classifies_zombie() {
            let executing = b"42 (server name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987";
            let zombie = b"42 (server name) Z 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987";
            assert_eq!(
                parse_execution_state_and_start_time(executing),
                Some((ExecutionState::Executing, 987))
            );
            assert_eq!(
                parse_execution_state_and_start_time(zombie),
                Some((ExecutionState::Zombie, 987))
            );
        }

        #[test]
        fn zombie_snapshot_skips_environ_while_executing_failure_remains_residual() {
            let uid = rustix::process::geteuid().as_raw();
            let status = format!("Name:\\ttest\\nUid:\\t{uid}\\t{uid}\\t{uid}\\t{uid}\\n");
            let zombie = b"42 (server name) Z 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987";
            let snapshot = match inspect_linux_with(
                42,
                || Ok(status.as_bytes().to_vec()),
                || Ok(zombie.to_vec()),
                || panic!("zombie inspection must not read environ"),
            ) {
                Ok(snapshot) => snapshot,
                Err(_) => panic!("zombie stat did not produce a snapshot"),
            };
            assert_eq!(snapshot.execution_state, ExecutionState::Zombie);
            assert!(snapshot.environment.is_empty());

            let executing = b"42 (server name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987";
            assert!(matches!(
                inspect_linux_with(
                    42,
                    || Ok(status.as_bytes().to_vec()),
                    || Ok(executing.to_vec()),
                    || Err(InspectFailure::Residual("environ denied".into())),
                ),
                Err(InspectFailure::Residual(error)) if error == "environ denied"
            ));
        }
    }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
mod macos {
    use std::mem::{MaybeUninit, size_of};
    use std::ptr;

    use rustix::process::{Pid, Signal, kill_process};

    use super::*;

    const MAX_KERNEL_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    const KERNEL_READ_ATTEMPTS: usize = 2;
    const PROC_LIST_HEADROOM_BYTES: usize = 4096;

    enum BufferReadDecision {
        Use(usize),
        Retry,
    }

    pub(super) struct MacOsBackend;

    impl SweepBackend for MacOsBackend {
        fn list_pids(&self) -> std::result::Result<PidListing, String> {
            list_all_pids().map(|pids| PidListing {
                pids,
                residuals: Vec::new(),
            })
        }

        fn inspect(&self, pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
            inspect_macos_with(|| read_identity_observation(pid), || read_environment(pid))
        }

        fn kill_exact(
            &self,
            expected: &ProcessIdentity,
        ) -> std::result::Result<KillResult, String> {
            // macOS has no pidfd equivalent. Two identity rereads narrow, but
            // cannot eliminate, PID reuse between the final read and numeric
            // kill. That micro-window is an explicitly accepted product tail,
            // not a complete containment proof.
            if let Some(result) =
                validate_identity_rereads(expected, || read_identity(expected.pid))?
            {
                return Ok(result);
            }
            let Some(pid) = Pid::from_raw(expected.pid) else {
                return Ok(KillResult::IdentityChanged);
            };
            match kill_process(pid, Signal::KILL) {
                Ok(()) => Ok(KillResult::Killed),
                Err(rustix::io::Errno::SRCH) => Ok(KillResult::Gone),
                Err(error) => Err(format!("numeric SIGKILL: {error}")),
            }
        }
    }

    fn inspect_macos_with(
        read_identity: impl FnOnce() -> std::result::Result<
            (ProcessIdentity, ExecutionState),
            InspectFailure,
        >,
        read_environment: impl FnOnce() -> std::result::Result<Vec<Vec<u8>>, InspectFailure>,
    ) -> std::result::Result<ProcessSnapshot, InspectFailure> {
        let (identity, execution_state) = read_identity()?;
        if identity.uid != rustix::process::geteuid().as_raw() {
            return Err(InspectFailure::Gone);
        }
        let environment = if execution_state.is_absent() {
            Vec::new()
        } else {
            read_environment()?
        };
        Ok(ProcessSnapshot {
            identity,
            execution_state,
            environment,
        })
    }

    fn validate_identity_rereads(
        expected: &ProcessIdentity,
        mut read: impl FnMut() -> std::result::Result<ProcessIdentity, InspectFailure>,
    ) -> std::result::Result<Option<KillResult>, String> {
        for stage in ["identity reread", "final identity reread"] {
            match read() {
                Ok(identity) if identity == *expected => {}
                Ok(_) => return Ok(Some(KillResult::IdentityChanged)),
                Err(InspectFailure::Gone) => return Ok(Some(KillResult::Gone)),
                Err(InspectFailure::Residual(error)) => {
                    return Err(format!("{stage}: {error}"));
                }
            }
        }
        Ok(None)
    }

    fn list_all_pids() -> std::result::Result<Vec<i32>, String> {
        // libproc.h: PROC_UID_ONLY (4) restricts enumeration to effective UID.
        // Filtering at the enumeration boundary avoids treating protected
        // processes owned by other users as cleanup residuals.
        const PROC_UID_ONLY: u32 = 4;
        let uid = rustix::process::geteuid().as_raw();
        list_all_pids_with(|buffer, bytes| {
            // SAFETY: callers pass either the documented null sizing buffer or
            // a live writable allocation of exactly `bytes` bytes.
            unsafe { libc::proc_listpids(PROC_UID_ONLY, uid, buffer, bytes) }
        })
    }

    fn list_all_pids_with(
        mut call: impl FnMut(*mut libc::c_void, i32) -> i32,
    ) -> std::result::Result<Vec<i32>, String> {
        for _ in 0..KERNEL_READ_ATTEMPTS {
            // SAFETY: a null buffer with size zero is the documented sizing
            // call; the second call receives a writable i32 array and its exact
            // byte length.
            let required_bytes = call(ptr::null_mut(), 0);
            if required_bytes <= 0 {
                return Err(io::Error::last_os_error().to_string());
            }
            let required_bytes = usize::try_from(required_bytes)
                .map_err(|_| "negative macOS process list size".to_string())?;
            validate_kernel_buffer_size(required_bytes, "macOS process list")?;
            let capacity_bytes = required_bytes
                .saturating_add(PROC_LIST_HEADROOM_BYTES)
                .min(MAX_KERNEL_BUFFER_BYTES);
            let capacity = capacity_bytes.div_ceil(size_of::<i32>());
            let mut pids = vec![0_i32; capacity];
            let capacity_bytes = pids.len().saturating_mul(size_of::<i32>());
            let bytes = i32::try_from(capacity_bytes)
                .map_err(|_| "macOS process list is too large".to_string())?;
            let written = call(pids.as_mut_ptr().cast(), bytes);
            if written <= 0 {
                return Err(io::Error::last_os_error().to_string());
            }
            let written = usize::try_from(written)
                .map_err(|_| "negative macOS process list length".to_string())?;
            if written < capacity_bytes && written % size_of::<i32>() == 0 {
                pids.truncate(written / size_of::<i32>());
                pids.retain(|pid| *pid > 0);
                return Ok(pids);
            }
            if written > capacity_bytes {
                return Err("macOS process list exceeded its supplied buffer".into());
            }
        }
        Err("macOS process list remained exactly full after bounded retries".into())
    }

    fn validate_kernel_buffer_size(size: usize, stage: &str) -> std::result::Result<(), String> {
        if size == 0 {
            return Err(format!("{stage} reported an empty buffer"));
        }
        if size > MAX_KERNEL_BUFFER_BYTES {
            return Err(format!(
                "{stage} exceeds {MAX_KERNEL_BUFFER_BYTES} byte safety bound"
            ));
        }
        Ok(())
    }

    fn read_identity(pid: i32) -> std::result::Result<ProcessIdentity, InspectFailure> {
        read_identity_observation(pid).map(|(identity, _)| identity)
    }

    fn read_identity_observation(
        pid: i32,
    ) -> std::result::Result<(ProcessIdentity, ExecutionState), InspectFailure> {
        let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
        let size = i32::try_from(size_of::<libc::proc_bsdinfo>())
            .map_err(|_| InspectFailure::Residual("proc_bsdinfo size overflow".into()))?;
        // SAFETY: `info` points to writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        validate_proc_bsdinfo_read(written, size, io::Error::last_os_error())?;
        // SAFETY: proc_pidinfo reported that it initialized the full struct.
        let info = unsafe { info.assume_init() };
        let identity = ProcessIdentity {
            pid,
            uid: info.pbi_uid,
            start: (u128::from(info.pbi_start_tvsec) << 64) | u128::from(info.pbi_start_tvusec),
        };
        let execution_state = execution_state_from_bsd_status(info.pbi_status);
        Ok((identity, execution_state))
    }

    fn execution_state_from_bsd_status(status: u32) -> ExecutionState {
        if status == libc::SZOMB {
            ExecutionState::Zombie
        } else {
            ExecutionState::Executing
        }
    }

    fn validate_proc_bsdinfo_read(
        written: i32,
        expected: i32,
        error: io::Error,
    ) -> std::result::Result<(), InspectFailure> {
        if written == 0 {
            return match error.raw_os_error() {
                Some(libc::ESRCH) | Some(libc::ENOENT) => Err(InspectFailure::Gone),
                _ => Err(InspectFailure::Residual(format!(
                    "PROC_PIDTBSDINFO: {error}"
                ))),
            };
        }
        if written != expected {
            return Err(InspectFailure::Residual(format!(
                "short PROC_PIDTBSDINFO read: {written}/{expected}"
            )));
        }
        Ok(())
    }

    fn read_environment(pid: i32) -> std::result::Result<Vec<Vec<u8>>, InspectFailure> {
        for attempt in 0..KERNEL_READ_ATTEMPTS {
            let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
            let mut size = 0_usize;
            // SAFETY: the sizing call writes only `size`; `mib` has the exact
            // three-element KERN_PROCARGS2 shape.
            let size_result = unsafe {
                libc::sysctl(
                    mib.as_mut_ptr(),
                    3,
                    ptr::null_mut(),
                    &mut size,
                    ptr::null_mut(),
                    0,
                )
            };
            if size_result != 0 {
                return classify_sysctl_error("KERN_PROCARGS2 sizing");
            }
            validate_kernel_buffer_size(size, "KERN_PROCARGS2")
                .map_err(InspectFailure::Residual)?;
            let capacity = size;
            let mut buffer = vec![0_u8; capacity];
            // SAFETY: `buffer` is writable for `capacity` bytes and sysctl
            // updates `size` with the initialized prefix.
            let read_result = unsafe {
                libc::sysctl(
                    mib.as_mut_ptr(),
                    3,
                    buffer.as_mut_ptr().cast(),
                    &mut size,
                    ptr::null_mut(),
                    0,
                )
            };
            let result = if read_result == 0 {
                Ok(size)
            } else {
                Err(io::Error::last_os_error())
            };
            let used = match decide_buffer_read(attempt, capacity, result)? {
                BufferReadDecision::Use(used) => used,
                BufferReadDecision::Retry => continue,
            };
            buffer.truncate(used);
            return parse_procargs_environment(&buffer).map_err(|error| {
                InspectFailure::Residual(format!("KERN_PROCARGS2 parse: {error}"))
            });
        }
        Err(InspectFailure::Residual(
            "KERN_PROCARGS2 bounded retry exhausted".into(),
        ))
    }

    fn classify_sysctl_error<T>(stage: &str) -> std::result::Result<T, InspectFailure> {
        classify_sysctl_error_with(stage, io::Error::last_os_error())
    }

    fn classify_sysctl_error_with<T>(
        stage: &str,
        error: io::Error,
    ) -> std::result::Result<T, InspectFailure> {
        Err(classify_sysctl_io(stage, error))
    }

    fn classify_sysctl_io(stage: &str, error: io::Error) -> InspectFailure {
        match error.raw_os_error() {
            Some(libc::ESRCH) | Some(libc::ENOENT) => InspectFailure::Gone,
            _ => InspectFailure::Residual(format!("{stage}: {error}")),
        }
    }

    fn decide_buffer_read(
        attempt: usize,
        capacity: usize,
        result: io::Result<usize>,
    ) -> std::result::Result<BufferReadDecision, InspectFailure> {
        match result {
            Ok(used) if used <= capacity => Ok(BufferReadDecision::Use(used)),
            Ok(_) if attempt + 1 != KERNEL_READ_ATTEMPTS => Ok(BufferReadDecision::Retry),
            Ok(_) => Err(InspectFailure::Residual(
                "KERN_PROCARGS2 grew after bounded retries".into(),
            )),
            Err(error)
                if attempt + 1 != KERNEL_READ_ATTEMPTS
                    && matches!(error.raw_os_error(), Some(libc::ENOMEM)) =>
            {
                Ok(BufferReadDecision::Retry)
            }
            Err(error) => Err(classify_sysctl_io("KERN_PROCARGS2 read", error)),
        }
    }

    fn parse_procargs_environment(buffer: &[u8]) -> std::result::Result<Vec<Vec<u8>>, String> {
        let argc_bytes: [u8; 4] = buffer
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| "missing argc".to_string())?;
        let argc = usize::try_from(i32::from_ne_bytes(argc_bytes))
            .map_err(|_| "negative argc".to_string())?;
        let mut cursor = 4;
        read_c_string(buffer, &mut cursor).ok_or_else(|| "missing executable path".to_string())?;
        while buffer.get(cursor) == Some(&0) {
            cursor += 1;
        }
        for _ in 0..argc {
            read_c_string(buffer, &mut cursor).ok_or_else(|| "truncated argv".to_string())?;
        }
        while buffer.get(cursor) == Some(&0) {
            cursor += 1;
        }
        let mut environment = Vec::new();
        while cursor < buffer.len() {
            let entry = read_c_string(buffer, &mut cursor)
                .ok_or_else(|| "unterminated environment".to_string())?;
            if entry.is_empty() {
                break;
            }
            environment.push(entry.to_vec());
        }
        Ok(environment)
    }

    fn read_c_string<'a>(buffer: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
        let start = *cursor;
        let relative_end = buffer.get(start..)?.iter().position(|byte| *byte == 0)?;
        let end = start + relative_end;
        *cursor = end + 1;
        buffer.get(start..end)
    }

    #[cfg(test)]
    mod tests {
        use std::collections::VecDeque;

        use super::*;

        #[test]
        fn procargs_parser_preserves_non_utf8_exact_environment() {
            let mut bytes = 2_i32.to_ne_bytes().to_vec();
            bytes.extend_from_slice(b"/bin/server\0\0arg0\0--flag\0OWNER=ok\0RAW=\xff\0\0");
            assert_eq!(
                parse_procargs_environment(&bytes).unwrap(),
                vec![b"OWNER=ok".to_vec(), b"RAW=\xff".to_vec()]
            );
        }

        #[test]
        fn procargs_parser_rejects_truncated_argv() {
            let mut bytes = 2_i32.to_ne_bytes().to_vec();
            bytes.extend_from_slice(b"/bin/server\0\0arg0\0");
            assert_eq!(
                parse_procargs_environment(&bytes).unwrap_err(),
                "truncated argv"
            );
        }

        #[test]
        fn proc_bsdinfo_abi_and_error_results_are_typed() {
            let expected = i32::try_from(size_of::<libc::proc_bsdinfo>()).unwrap();
            assert!(
                validate_proc_bsdinfo_read(expected, expected, io::Error::other("unused")).is_ok()
            );
            assert!(matches!(
                validate_proc_bsdinfo_read(0, expected, io::Error::from_raw_os_error(libc::ESRCH)),
                Err(InspectFailure::Gone)
            ));
            assert!(matches!(
                validate_proc_bsdinfo_read(0, expected, io::Error::from_raw_os_error(libc::EPERM)),
                Err(InspectFailure::Residual(_))
            ));
            assert!(matches!(
                validate_proc_bsdinfo_read(expected - 1, expected, io::Error::other("unused")),
                Err(InspectFailure::Residual(_))
            ));
        }

        #[test]
        fn proc_bsdinfo_status_classifies_szomb_as_execution_absent() {
            assert_eq!(
                execution_state_from_bsd_status(libc::SZOMB),
                ExecutionState::Zombie
            );
            assert_eq!(
                execution_state_from_bsd_status(libc::SRUN),
                ExecutionState::Executing
            );
        }

        #[test]
        fn zombie_snapshot_skips_procargs_while_executing_failure_remains_residual() {
            let identity = ProcessIdentity {
                pid: 42,
                uid: rustix::process::geteuid().as_raw(),
                start: 9,
            };
            let snapshot = match inspect_macos_with(
                || Ok((identity.clone(), ExecutionState::Zombie)),
                || panic!("zombie inspection must not read KERN_PROCARGS2"),
            ) {
                Ok(snapshot) => snapshot,
                Err(_) => panic!("SZOMB observation did not produce a snapshot"),
            };
            assert_eq!(snapshot.execution_state, ExecutionState::Zombie);
            assert!(snapshot.environment.is_empty());

            assert!(matches!(
                inspect_macos_with(
                    || Ok((identity, ExecutionState::Executing)),
                    || Err(InspectFailure::Residual("procargs denied".into())),
                ),
                Err(InspectFailure::Residual(error)) if error == "procargs denied"
            ));
        }

        #[test]
        fn identity_rereads_require_both_reads_to_match() {
            let expected = ProcessIdentity {
                pid: 42,
                uid: 7,
                start: 9,
            };
            let mut matching = VecDeque::from([Ok(expected.clone()), Ok(expected.clone())]);
            assert!(
                validate_identity_rereads(&expected, || matching.pop_front().unwrap())
                    .unwrap()
                    .is_none()
            );

            let mut changed = expected.clone();
            changed.start += 1;
            let mut mismatch = VecDeque::from([Ok(expected.clone()), Ok(changed)]);
            assert!(matches!(
                validate_identity_rereads(&expected, || mismatch.pop_front().unwrap()),
                Ok(Some(KillResult::IdentityChanged))
            ));

            let mut residual = VecDeque::from([Err(InspectFailure::Residual("denied".into()))]);
            match validate_identity_rereads(&expected, || residual.pop_front().unwrap()) {
                Err(error) => assert_eq!(error, "identity reread: denied"),
                Ok(_) => panic!("residual identity read unexpectedly succeeded"),
            }
        }

        #[test]
        fn process_list_exact_full_retries_then_fails_closed() {
            let mut sizing = true;
            let result = list_all_pids_with(|_buffer, bytes| {
                if sizing {
                    sizing = false;
                    i32::try_from(size_of::<i32>()).unwrap()
                } else {
                    sizing = true;
                    bytes
                }
            });
            assert_eq!(
                result.unwrap_err(),
                "macOS process list remained exactly full after bounded retries"
            );
        }

        #[test]
        fn kernel_buffer_limit_accepts_max_and_rejects_max_plus_one_before_fill() {
            assert!(validate_kernel_buffer_size(MAX_KERNEL_BUFFER_BYTES, "test").is_ok());
            let mut fill_called = false;
            let result = list_all_pids_with(|_buffer, bytes| {
                if bytes == 0 {
                    i32::try_from(MAX_KERNEL_BUFFER_BYTES + 1).unwrap()
                } else {
                    fill_called = true;
                    0
                }
            });
            assert!(result.unwrap_err().contains("safety bound"));
            assert!(
                !fill_called,
                "oversized kernel report allocated a fill buffer"
            );
        }

        #[test]
        fn procargs_growth_enomem_and_io_errors_are_bounded_typed_residuals() {
            assert!(matches!(
                decide_buffer_read(0, 8, Err(io::Error::from_raw_os_error(libc::ENOMEM))),
                Ok(BufferReadDecision::Retry)
            ));
            assert!(matches!(
                decide_buffer_read(
                    KERNEL_READ_ATTEMPTS - 1,
                    8,
                    Err(io::Error::from_raw_os_error(libc::ENOMEM))
                ),
                Err(InspectFailure::Residual(_))
            ));
            assert!(matches!(
                decide_buffer_read(0, 8, Err(io::Error::from_raw_os_error(libc::EIO))),
                Err(InspectFailure::Residual(_))
            ));
            assert!(matches!(
                decide_buffer_read(0, 8, Ok(9)),
                Ok(BufferReadDecision::Retry)
            ));
            assert!(matches!(
                decide_buffer_read(KERNEL_READ_ATTEMPTS - 1, 8, Ok(9)),
                Err(InspectFailure::Residual(_))
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::process::{Child as StdChild, Command as StdCommand, Stdio};
    use std::sync::mpsc;
    use std::thread::JoinHandle;
    use std::time::Instant;

    use super::*;

    #[test]
    fn owner_marker_is_injective_for_raw_non_utf8_paths() {
        let raw = b"/tmp/cairn-\xff";
        let path = std::path::PathBuf::from(OsString::from_vec(raw.to_vec()));
        assert_eq!(
            owner_value_from_canonical_path(&path).unwrap(),
            format!("{OWNER_PREFIX}{}", hex::encode(raw))
        );
        assert_ne!(
            owner_value_from_raw(b"ab").unwrap(),
            owner_value_from_raw(b"a/b").unwrap()
        );
    }

    #[test]
    fn owner_marker_rejects_overlong_root() {
        let error = owner_value_from_raw(&vec![b'x'; MAX_CANONICAL_ROOT_BYTES + 1]).unwrap_err();
        assert!(error.to_string().contains("exceeds 4096 bytes"));
    }

    #[test]
    fn nonce_reader_requires_exact_sixteen_bytes() {
        let mut short = io::Cursor::new(vec![7_u8; STARTUP_NONCE_BYTES - 1]);
        assert!(read_nonce_from(&mut short).is_err());
        let mut exact = io::Cursor::new(vec![7_u8; STARTUP_NONCE_BYTES]);
        assert_eq!(
            read_nonce_from(&mut exact).unwrap(),
            [7_u8; STARTUP_NONCE_BYTES]
        );
    }

    #[test]
    fn family_is_unique_and_sequence_overflow_fails_closed() {
        let context = ProcessOwnerContext::new("owner".into(), [9; STARTUP_NONCE_BYTES]);
        let first = context.marker_for_spawn().unwrap();
        let second = context.marker_for_spawn().unwrap();
        assert_ne!(first.family(), second.family());
        assert!(first.family().ends_with(":0000000000000001"));
        assert!(second.family().ends_with(":0000000000000002"));

        let exhausted = ProcessOwnerContext::with_sequence_for_test(
            "owner".into(),
            [9; STARTUP_NONCE_BYTES],
            u64::MAX,
        );
        assert!(exhausted.marker_for_spawn().is_err());
    }

    #[derive(Default)]
    struct FakeBackend {
        lists: Mutex<VecDeque<Vec<i32>>>,
        snapshots: Mutex<std::collections::HashMap<i32, ProcessSnapshot>>,
        killed: Mutex<Vec<i32>>,
        kill_failure: Mutex<HashSet<i32>>,
        changed: Mutex<HashSet<i32>>,
        persistent: Mutex<HashSet<i32>>,
        zombie_after_kill: Mutex<HashSet<i32>>,
        replacement_after_kill: Mutex<std::collections::HashMap<i32, ProcessSnapshot>>,
    }

    impl SweepBackend for FakeBackend {
        fn list_pids(&self) -> std::result::Result<PidListing, String> {
            Ok(PidListing {
                pids: self.lists.lock().unwrap().pop_front().unwrap_or_default(),
                residuals: Vec::new(),
            })
        }

        fn inspect(&self, pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
            self.snapshots
                .lock()
                .unwrap()
                .get(&pid)
                .map(|snapshot| ProcessSnapshot {
                    identity: snapshot.identity.clone(),
                    execution_state: snapshot.execution_state,
                    environment: snapshot.environment.clone(),
                })
                .ok_or(InspectFailure::Gone)
        }

        fn kill_exact(
            &self,
            expected: &ProcessIdentity,
        ) -> std::result::Result<KillResult, String> {
            if self.changed.lock().unwrap().contains(&expected.pid) {
                return Ok(KillResult::IdentityChanged);
            }
            if self.kill_failure.lock().unwrap().contains(&expected.pid) {
                return Err("test-injected exact PID signal failure".into());
            }
            self.killed.lock().unwrap().push(expected.pid);
            if let Some(replacement) = self
                .replacement_after_kill
                .lock()
                .unwrap()
                .remove(&expected.pid)
            {
                self.snapshots
                    .lock()
                    .unwrap()
                    .insert(expected.pid, replacement);
            } else if self
                .zombie_after_kill
                .lock()
                .unwrap()
                .contains(&expected.pid)
            {
                if let Some(snapshot) = self.snapshots.lock().unwrap().get_mut(&expected.pid) {
                    snapshot.execution_state = ExecutionState::Zombie;
                }
            } else if !self.persistent.lock().unwrap().contains(&expected.pid) {
                self.snapshots.lock().unwrap().remove(&expected.pid);
            }
            Ok(KillResult::Killed)
        }
    }

    fn snapshot(pid: i32, entries: &[(&str, &str)]) -> ProcessSnapshot {
        ProcessSnapshot {
            identity: ProcessIdentity {
                pid,
                uid: 1,
                start: u128::try_from(pid).unwrap(),
            },
            execution_state: ExecutionState::Executing,
            environment: entries
                .iter()
                .map(|(key, value)| format!("{key}={value}").into_bytes())
                .collect(),
        }
    }

    #[test]
    fn family_sweep_kills_only_exact_owner_and_family_and_rescans() {
        let backend = FakeBackend::default();
        backend
            .lists
            .lock()
            .unwrap()
            .extend([vec![21, 22, 23, 24, 25], vec![26], vec![]]);
        backend.snapshots.lock().unwrap().extend([
            (21, snapshot(21, &[(OWNER_ENV, "o"), (FAMILY_ENV, "f")])),
            (22, snapshot(22, &[(OWNER_ENV, "o"), (FAMILY_ENV, "near")])),
            (23, snapshot(23, &[(OWNER_ENV, "o")])),
            (24, snapshot(24, &[(FAMILY_ENV, "f")])),
            (
                25,
                snapshot(25, &[(OWNER_ENV, "prefix-o"), (FAMILY_ENV, "f")]),
            ),
            (26, snapshot(26, &[(OWNER_ENV, "o"), (FAMILY_ENV, "f")])),
        ]);
        let report = sweep_with_backend(
            &backend,
            MarkerMatch::Family {
                owner: "o",
                family: "f",
            },
        );
        assert_eq!(*backend.killed.lock().unwrap(), vec![21, 26]);
        assert_eq!(report.signalled, 2);
        assert_eq!(report.confirmed_execution_absent, 2);
        assert_eq!(report.remaining, 0);
        assert_eq!(report.residual_count(), 0);
    }

    #[test]
    fn identity_change_prevents_kill() {
        let backend = FakeBackend::default();
        backend.lists.lock().unwrap().push_back(vec![31]);
        backend
            .snapshots
            .lock()
            .unwrap()
            .insert(31, snapshot(31, &[(OWNER_ENV, "o")]));
        backend.changed.lock().unwrap().insert(31);
        let report = sweep_with_backend(&backend, MarkerMatch::Owner("o"));
        assert!(backend.killed.lock().unwrap().is_empty());
        assert_eq!(report.matched, 1);
    }

    #[test]
    fn inspection_residual_is_diagnostic_and_does_not_block_new_family_admission() {
        struct ResidualBackend;

        impl SweepBackend for ResidualBackend {
            fn list_pids(&self) -> std::result::Result<PidListing, String> {
                Ok(PidListing {
                    pids: vec![41],
                    residuals: Vec::new(),
                })
            }

            fn inspect(&self, _pid: i32) -> std::result::Result<ProcessSnapshot, InspectFailure> {
                Err(InspectFailure::Residual("permission denied".into()))
            }

            fn kill_exact(
                &self,
                _expected: &ProcessIdentity,
            ) -> std::result::Result<KillResult, String> {
                unreachable!("uninspectable candidates cannot reach signalling")
            }
        }

        let report = sweep_with_backend(&ResidualBackend, MarkerMatch::Owner("owner"));
        assert_eq!(report.residual_count(), 1);
        let owner = ProcessOwnerContext::new("owner".into(), [5; STARTUP_NONCE_BYTES]);
        assert!(owner.marker_for_spawn().is_ok());
    }

    #[test]
    fn kill_failure_is_diagnostic_and_does_not_block_new_family_admission() {
        let backend = FakeBackend::default();
        backend.lists.lock().unwrap().push_back(vec![42]);
        backend
            .snapshots
            .lock()
            .unwrap()
            .insert(42, snapshot(42, &[(OWNER_ENV, "o"), (FAMILY_ENV, "f")]));
        backend.kill_failure.lock().unwrap().insert(42);

        let report = sweep_with_backend(
            &backend,
            MarkerMatch::Family {
                owner: "o",
                family: "f",
            },
        );
        assert_eq!(report.signalled, 0);
        assert_eq!(report.remaining, 0);
        assert_eq!(report.residual_count(), 1);
        assert!(report.residuals[0].contains("exact PID signal failure"));
        let owner = ProcessOwnerContext::new("owner".into(), [6; STARTUP_NONCE_BYTES]);
        assert!(owner.marker_for_spawn().is_ok());
    }

    #[test]
    fn accepted_signal_without_execution_absence_is_a_distinct_residual() {
        let backend = FakeBackend::default();
        backend.lists.lock().unwrap().push_back(vec![51]);
        backend
            .snapshots
            .lock()
            .unwrap()
            .insert(51, snapshot(51, &[(OWNER_ENV, "o")]));
        backend.persistent.lock().unwrap().insert(51);

        let report = sweep_with_backend(&backend, MarkerMatch::Owner("o"));
        assert_eq!(*backend.killed.lock().unwrap(), vec![51]);
        assert_eq!(report.signalled, 1);
        assert_eq!(report.confirmed_execution_absent, 0);
        assert_eq!(report.remaining, 1);
        assert_eq!(report.residual_count(), 1);
    }

    #[test]
    fn same_identity_zombie_after_signal_is_confirmed_execution_absent() {
        let backend = FakeBackend::default();
        backend.lists.lock().unwrap().push_back(vec![54]);
        backend
            .snapshots
            .lock()
            .unwrap()
            .insert(54, snapshot(54, &[(OWNER_ENV, "o")]));
        backend.zombie_after_kill.lock().unwrap().insert(54);

        let report = sweep_with_backend(&backend, MarkerMatch::Owner("o"));
        assert_eq!(*backend.killed.lock().unwrap(), vec![54]);
        assert_eq!(report.signalled, 1);
        assert_eq!(report.confirmed_execution_absent, 1);
        assert_eq!(report.remaining, 0);
        assert_eq!(report.residual_count(), 0);
    }

    #[test]
    fn gone_after_signal_confirms_absence_without_residual() {
        let backend = FakeBackend::default();
        backend.lists.lock().unwrap().push_back(vec![52]);
        backend
            .snapshots
            .lock()
            .unwrap()
            .insert(52, snapshot(52, &[(OWNER_ENV, "o")]));

        let report = sweep_with_backend(&backend, MarkerMatch::Owner("o"));
        assert_eq!(report.signalled, 1);
        assert_eq!(report.confirmed_execution_absent, 1);
        assert_eq!(report.remaining, 0);
        assert_eq!(report.residual_count(), 0);
    }

    #[test]
    fn pid_reuse_confirms_old_identity_without_signalling_new_process() {
        let backend = FakeBackend::default();
        backend.lists.lock().unwrap().push_back(vec![53]);
        backend
            .snapshots
            .lock()
            .unwrap()
            .insert(53, snapshot(53, &[(OWNER_ENV, "o")]));
        let mut replacement = snapshot(53, &[(OWNER_ENV, "o")]);
        replacement.identity.start += 1;
        backend
            .replacement_after_kill
            .lock()
            .unwrap()
            .insert(53, replacement);

        let report = sweep_with_backend(&backend, MarkerMatch::Owner("o"));
        assert_eq!(*backend.killed.lock().unwrap(), vec![53]);
        assert_eq!(report.signalled, 1);
        assert_eq!(report.confirmed_execution_absent, 1);
        assert_eq!(report.remaining, 0);
        assert_eq!(report.residual_count(), 0);
    }

    #[test]
    fn owner_context_is_published_only_after_startup_sweep_and_second_init_fails() {
        let cell = OnceLock::new();
        let context = Arc::new(ProcessOwnerContext::new(
            "owner-a".into(),
            [1; STARTUP_NONCE_BYTES],
        ));
        let swept = std::cell::Cell::new(false);
        let report = publish_after_sweep(&cell, context, |owner| {
            assert_eq!(owner, "owner-a");
            assert!(cell.get().is_none(), "spawn context published before sweep");
            swept.set(true);
            SweepReport {
                scope: "startup-owner",
                ..SweepReport::default()
            }
        })
        .unwrap();
        assert!(swept.get());
        assert_eq!(report.scope, "startup-owner");
        assert!(cell.get().is_some());

        let mismatch = Arc::new(ProcessOwnerContext::new(
            "owner-b".into(),
            [2; STARTUP_NONCE_BYTES],
        ));
        assert!(publish_after_sweep(&cell, mismatch, |_| unreachable!()).is_err());
    }

    #[derive(Debug, PartialEq, Eq)]
    enum ReapOutcome {
        Exited,
        CleanupRequested,
        DeadlineKill,
    }

    struct TaskChildReaper {
        cleanup: mpsc::Sender<()>,
        join: Option<JoinHandle<ReapOutcome>>,
    }

    impl TaskChildReaper {
        fn new(mut child: StdChild, deadline: Duration) -> Self {
            let (cleanup, cleanup_receiver) = mpsc::channel();
            let join = std::thread::spawn(move || {
                let deadline = Instant::now() + deadline;
                loop {
                    if child.try_wait().ok().flatten().is_some() {
                        return ReapOutcome::Exited;
                    }
                    if cleanup_receiver.try_recv().is_ok() {
                        let _ = child.kill();
                        let _ = child.wait();
                        return ReapOutcome::CleanupRequested;
                    }
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return ReapOutcome::DeadlineKill;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            Self {
                cleanup,
                join: Some(join),
            }
        }

        fn finish(mut self) -> ReapOutcome {
            self.join
                .take()
                .expect("task child reaper join exists")
                .join()
                .expect("task child reaper thread panicked")
        }

        fn cleanup(mut self) -> ReapOutcome {
            let _ = self.cleanup.send(());
            self.join
                .take()
                .expect("task child reaper join exists")
                .join()
                .expect("task child reaper thread panicked")
        }
    }

    impl Drop for TaskChildReaper {
        fn drop(&mut self) {
            let _ = self.cleanup.send(());
            if let Some(join) = self.join.take() {
                let _ = join.join();
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct MarkerReceipt {
        pid: i32,
        owner_present: bool,
        family_present: bool,
    }

    fn wait_for_marker_receipt(path: &Path) -> MarkerReceipt {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Ok(contents) = std::fs::read_to_string(path) {
                let mut fields = contents.trim().split('|');
                if let (Some(pid), Some(owner), Some(family), None) =
                    (fields.next(), fields.next(), fields.next(), fields.next())
                    && let (Ok(pid), Some(owner_present), Some(family_present)) = (
                        pid.parse(),
                        match owner {
                            "0" => Some(false),
                            "1" => Some(true),
                            _ => None,
                        },
                        match family {
                            "0" => Some(false),
                            "1" => Some(true),
                            _ => None,
                        },
                    )
                {
                    return MarkerReceipt {
                        pid,
                        owner_present,
                        family_present,
                    };
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "task-owned child did not publish marker receipt: {}",
            path.display()
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn actual_setsid_marked_child_is_killed_and_unmarked_same_binary_survives() {
        if StdCommand::new("python3")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_err()
        {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let script_path = temp.path().join("marker_exec.py");
        let marked_stage1_file = temp.path().join("marked.stage1");
        let marked_stage2_file = temp.path().join("marked.stage2");
        let unmarked_stage1_file = temp.path().join("unmarked.stage1");
        let unmarked_stage2_file = temp.path().join("unmarked.stage2");
        std::fs::write(
            &script_path,
            r#"import os
import sys
import time

owner_present = "CAIRN_LSP_OWNER" in os.environ
family_present = "CAIRN_LSP_FAMILY" in os.environ

def write_receipt(path):
    with open(path, "w") as receipt:
        receipt.write(f"{os.getpid()}|{int(owner_present)}|{int(family_present)}")
        receipt.flush()

if sys.argv[1] == "stage1":
    os.setsid()
    write_receipt(sys.argv[2])
    os.execvpe(
        sys.executable,
        [sys.executable, __file__, "stage2", sys.argv[2], sys.argv[3]],
        dict(os.environ),
    )

write_receipt(sys.argv[3])
time.sleep(300)
"#,
        )
        .unwrap();
        let marked_child = StdCommand::new("python3")
            .arg(&script_path)
            .arg("stage1")
            .arg(&marked_stage1_file)
            .arg(&marked_stage2_file)
            .env(OWNER_ENV, "task-owner")
            .env(FAMILY_ENV, "task-family")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let marked_handle_pid = i32::try_from(marked_child.id()).unwrap();
        let marked_reaper = TaskChildReaper::new(marked_child, Duration::from_secs(5));
        let unmarked_child = StdCommand::new("python3")
            .arg(&script_path)
            .arg("stage1")
            .arg(&unmarked_stage1_file)
            .arg(&unmarked_stage2_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let unmarked_handle_pid = i32::try_from(unmarked_child.id()).unwrap();
        let unmarked_reaper = TaskChildReaper::new(unmarked_child, Duration::from_secs(5));
        let marked_stage1 = wait_for_marker_receipt(&marked_stage1_file);
        let marked_stage2 = wait_for_marker_receipt(&marked_stage2_file);
        let unmarked_stage1 = wait_for_marker_receipt(&unmarked_stage1_file);
        let unmarked_stage2 = wait_for_marker_receipt(&unmarked_stage2_file);
        assert_eq!(marked_stage1.pid, marked_handle_pid);
        assert_eq!(marked_stage2.pid, marked_handle_pid);
        assert!(
            marked_stage1.owner_present && marked_stage1.family_present,
            "marked stage1 marker presence authority: pid={} owner_present={} family_present={}",
            marked_stage1.pid,
            marked_stage1.owner_present,
            marked_stage1.family_present
        );
        assert!(
            marked_stage2.owner_present && marked_stage2.family_present,
            "marked stage2 marker presence authority: pid={} owner_present={} family_present={}",
            marked_stage2.pid,
            marked_stage2.owner_present,
            marked_stage2.family_present
        );
        assert_eq!(unmarked_stage1.pid, unmarked_handle_pid);
        assert_eq!(unmarked_stage2.pid, unmarked_handle_pid);
        assert!(!unmarked_stage1.owner_present && !unmarked_stage1.family_present);
        assert!(!unmarked_stage2.owner_present && !unmarked_stage2.family_present);
        let marked_pid = marked_stage2.pid;
        let unmarked_pid = unmarked_stage2.pid;

        #[cfg(target_os = "macos")]
        let backend = super::macos::MacOsBackend;
        #[cfg(target_os = "linux")]
        let backend = super::linux::LinuxBackend;
        let listing = backend.list_pids().unwrap();
        assert!(
            listing.pids.contains(&marked_pid),
            "same-UID enumeration omitted marked task PID"
        );
        let marker_match = MarkerMatch::Family {
            owner: "task-owner",
            family: "task-family",
        };
        let first_marked = match backend.inspect(marked_pid) {
            Ok(snapshot) => snapshot,
            Err(_) => panic!("marked task-owned process could not be inspected"),
        };
        assert!(
            marker_match.matches(&first_marked.environment),
            "marked task-owned process did not expose both exact markers"
        );
        let second_marked = match backend.inspect(marked_pid) {
            Ok(snapshot) => snapshot,
            Err(_) => panic!("marked task-owned identity was not stable before sweep"),
        };
        assert_eq!(first_marked.identity, second_marked.identity);
        let unmarked_identity = match backend.inspect(unmarked_pid) {
            Ok(snapshot) => snapshot.identity,
            Err(_) => panic!("unmarked task-owned process could not be inspected"),
        };

        let report = run_sweep(marker_match);
        let counts = format!(
            "signalled={} confirmed_execution_absent={} remaining={} residuals={}",
            report.signalled,
            report.confirmed_execution_absent,
            report.remaining,
            report.residual_count()
        );
        assert_eq!(report.residual_count(), 0, "{counts}");
        assert!(report.signalled >= 1, "{counts}");
        assert!(report.confirmed_execution_absent >= 1, "{counts}");
        assert_eq!(report.remaining, 0, "{counts}");
        match backend.inspect(marked_pid) {
            Err(InspectFailure::Gone) => {}
            Ok(snapshot) if snapshot.identity != first_marked.identity => {}
            Ok(_) | Err(InspectFailure::Residual(_)) => {
                panic!("marked old identity remained after sweep; {counts}")
            }
        }
        let unmarked_after = match backend.inspect(unmarked_pid) {
            Ok(snapshot) => snapshot.identity,
            Err(_) => panic!("unmarked same-binary child did not survive sweep; {counts}"),
        };
        assert_eq!(unmarked_after, unmarked_identity, "{counts}");
        assert_eq!(marked_reaper.finish(), ReapOutcome::Exited, "{counts}");
        assert_eq!(
            unmarked_reaper.cleanup(),
            ReapOutcome::CleanupRequested,
            "{counts}"
        );
    }
}
