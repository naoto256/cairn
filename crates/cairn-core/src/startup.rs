//! Atomic daemon startup admission and ready-resource handoff.
//!
//! The socket listeners may accept connections before storage and background
//! managers are ready. This gate keeps that transport availability separate
//! from method readiness: status and shutdown remain available while every
//! other request receives a typed initialization response.
//!
//! State machine: `Initializing` → `Ready` → `ShuttingDown`, with a
//! direct `Initializing` → `ShuttingDown` edge when shutdown wins the
//! race to `publish_ready`. Every transition is one-way; `advance`
//! only moves the progress ordinal forward within `Initializing`.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use cairn_proto::control::{
    Ack, DaemonInitializationDetail, DaemonInitializationPhase, DaemonInitializationStatus,
    StatusReport,
};
use cairn_proto::jsonrpc::{
    Request, RequestId, error_code, error_response, ok_response, serialize_response,
};
use tokio::sync::Notify;

use crate::daemon::LineHandler;
use crate::jobs::JobManager;
use crate::lifecycle::RepoLifecycleManager;
use crate::reconcile::RepoReconcileManager;
use crate::watcher::WatchManager;
use crate::{Error, jsonrpc_errors};

/// Resources published atomically when daemon initialization completes.
///
/// The gate owns this bundle after publication. Teardown takes it back exactly
/// once through [`StartupGate::begin_shutdown`].
pub struct ReadyDaemon {
    /// Serves `cairn.sock` (data-plane / query) requests once ready.
    pub data_handler: Arc<dyn LineHandler>,
    /// Serves `control.sock` (management) requests once ready.
    pub control_handler: Arc<dyn LineHandler>,
    // The managers below are never dispatched to by the gate itself;
    // they ride through publication so teardown can stop background
    // work in order after taking ownership via `begin_shutdown`.
    pub job_manager: Arc<JobManager>,
    pub reconcile: Arc<RepoReconcileManager>,
    pub lifecycle: Arc<RepoLifecycleManager>,
    pub watch_manager: Arc<WatchManager>,
}

/// Tri-state lifecycle guarded by the [`StartupGate`] mutex.
enum StartupState {
    /// Sockets are bound, but resources are still being constructed.
    Initializing(DaemonInitializationStatus),
    /// Bundle published; requests dispatch to the real handlers.
    Ready(ReadyDaemon),
    /// Terminal. The bundle has been taken (or was never published).
    ShuttingDown,
}

/// Shared startup state for socket admission and resource ownership.
pub struct StartupGate {
    state: Mutex<StartupState>,
    shutdown: Arc<Notify>,
    version: &'static str,
    started_at: Instant,
}

impl StartupGate {
    #[must_use]
    pub fn new(shutdown: Arc<Notify>, version: &'static str) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StartupState::Initializing(
                DaemonInitializationStatus::initializing(
                    DaemonInitializationPhase::SocketBound,
                    Some(DaemonInitializationDetail::OpeningStorage),
                ),
            )),
            shutdown,
            version,
            started_at: Instant::now(),
        })
    }

    /// Advance startup progress without allowing ordinal regression.
    ///
    /// The `Ready` phase is rejected here: the ready transition must
    /// carry the resource bundle through [`StartupGate::publish_ready`]
    /// so readiness and resource visibility stay atomic.
    pub fn advance(
        &self,
        phase: DaemonInitializationPhase,
        detail: Option<DaemonInitializationDetail>,
    ) -> crate::Result<()> {
        if matches!(phase, DaemonInitializationPhase::Ready) {
            return Err(Error::InvalidArgument(
                "publish ready resources through StartupGate::publish_ready".into(),
            ));
        }
        let next = DaemonInitializationStatus::initializing(phase, detail);
        let mut state = self.state.lock().expect("startup gate poisoned");
        match &mut *state {
            StartupState::Initializing(current) => {
                if next.completed_phases < current.completed_phases {
                    return Err(Error::InvalidArgument(format!(
                        "startup phase cannot regress from {} to {}",
                        current.completed_phases, next.completed_phases
                    )));
                }
                *current = next;
                Ok(())
            }
            StartupState::Ready(_) => Err(Error::InvalidArgument(
                "startup progress cannot advance after ready publication".into(),
            )),
            // Shutdown owns the terminal transition. An initializer already
            // between cancellation checks may finish constructing resources;
            // publish_ready will return that bundle for partial cleanup.
            StartupState::ShuttingDown => Ok(()),
        }
    }

    #[must_use]
    pub fn status(&self) -> DaemonInitializationStatus {
        match &*self.state.lock().expect("startup gate poisoned") {
            StartupState::Initializing(status) => status.clone(),
            StartupState::Ready(_) => DaemonInitializationStatus::ready(),
            // Teardown is not a wire-visible state: report ready so a
            // late status poll does not claim initialization is still
            // in progress. The listeners stop accepting shortly after.
            StartupState::ShuttingDown => DaemonInitializationStatus::ready(),
        }
    }

    /// Atomically publish handlers and teardown-owned managers.
    ///
    /// If shutdown won the race, ownership is returned to the initializer for
    /// partial cleanup and the bundle is never visible to request dispatch.
    pub fn publish_ready(&self, resources: ReadyDaemon) -> std::result::Result<(), ReadyDaemon> {
        let mut state = self.state.lock().expect("startup gate poisoned");
        match &*state {
            StartupState::Initializing(_) => {
                *state = StartupState::Ready(resources);
                Ok(())
            }
            StartupState::Ready(_) | StartupState::ShuttingDown => Err(resources),
        }
    }

    /// Begin shutdown and take ready resources exactly once.
    #[must_use]
    pub fn begin_shutdown(&self) -> Option<ReadyDaemon> {
        let mut state = self.state.lock().expect("startup gate poisoned");
        match std::mem::replace(&mut *state, StartupState::ShuttingDown) {
            StartupState::Ready(resources) => Some(resources),
            StartupState::Initializing(_) | StartupState::ShuttingDown => None,
        }
    }

    /// Admission snapshot for `cairn.sock`. The handler `Arc` is
    /// cloned out so the gate mutex is never held across `handle`.
    fn data_admission(&self) -> Admission<Arc<dyn LineHandler>> {
        match &*self.state.lock().expect("startup gate poisoned") {
            StartupState::Initializing(status) => Admission::Initializing(status.clone()),
            StartupState::Ready(resources) => Admission::Ready(resources.data_handler.clone()),
            StartupState::ShuttingDown => Admission::ShuttingDown,
        }
    }

    /// Admission snapshot for `control.sock`; see [`Self::data_admission`].
    fn control_admission(&self) -> Admission<Arc<dyn LineHandler>> {
        match &*self.state.lock().expect("startup gate poisoned") {
            StartupState::Initializing(status) => Admission::Initializing(status.clone()),
            StartupState::Ready(resources) => Admission::Ready(resources.control_handler.clone()),
            StartupState::ShuttingDown => Admission::ShuttingDown,
        }
    }

    fn initializing_status_report(
        &self,
        initialization: DaemonInitializationStatus,
    ) -> StatusReport {
        StatusReport {
            daemon_version: self.version.to_string(),
            uptime_secs: self.started_at.elapsed().as_secs(),
            initialization,
            repos: Vec::new(),
        }
    }
}

/// Per-request routing decision derived from the gate state.
enum Admission<H> {
    Initializing(DaemonInitializationStatus),
    Ready(H),
    ShuttingDown,
}

/// Data-plane handler that rejects requests until ready publication.
pub struct StartupDataHandler {
    gate: Arc<StartupGate>,
}

impl StartupDataHandler {
    #[must_use]
    pub fn new(gate: Arc<StartupGate>) -> Self {
        Self { gate }
    }
}

#[async_trait::async_trait]
impl LineHandler for StartupDataHandler {
    async fn handle(&self, line: &str) -> Option<String> {
        match self.gate.data_admission() {
            Admission::Ready(handler) => handler.handle(line).await,
            Admission::Initializing(status) => Some(initializing_error(line, status)),
            // `None` makes the accept loop close the stream cleanly.
            Admission::ShuttingDown => None,
        }
    }
}

/// Control-plane handler that keeps status and shutdown available during init.
pub struct StartupControlHandler {
    gate: Arc<StartupGate>,
}

impl StartupControlHandler {
    #[must_use]
    pub fn new(gate: Arc<StartupGate>) -> Self {
        Self { gate }
    }
}

#[async_trait::async_trait]
impl LineHandler for StartupControlHandler {
    async fn handle(&self, line: &str) -> Option<String> {
        match self.gate.control_admission() {
            Admission::Ready(handler) => handler.handle(line).await,
            Admission::Initializing(status) => Some(self.handle_initializing(line, status)),
            // `None` makes the accept loop close the stream cleanly.
            Admission::ShuttingDown => None,
        }
    }
}

impl StartupControlHandler {
    /// Dispatch during initialization: `status` and `shutdown` are
    /// served locally; everything else gets the typed
    /// `DaemonInitializing` error carrying current progress.
    fn handle_initializing(
        &self,
        line: &str,
        initialization: DaemonInitializationStatus,
    ) -> String {
        let request = match parse_request(line) {
            Ok(request) => request,
            Err(response) => return response,
        };
        let response = match request.method.as_str() {
            "status" => ok_response(
                request.id,
                serde_json::to_value(self.gate.initializing_status_report(initialization)).unwrap(),
            ),
            "shutdown" => {
                self.gate.shutdown.notify_waiters();
                ok_response(request.id, serde_json::to_value(Ack::ok()).unwrap())
            }
            _ => jsonrpc_errors::error_from(
                request.id,
                &Error::DaemonInitializing { initialization },
            ),
        };
        serialize_response(&response)
    }
}

/// Typed `DaemonInitializing` error for a data-plane request that
/// arrived before ready publication.
fn initializing_error(line: &str, initialization: DaemonInitializationStatus) -> String {
    let request = match parse_request(line) {
        Ok(request) => request,
        Err(response) => return response,
    };
    serialize_response(&jsonrpc_errors::error_from(
        request.id,
        &Error::DaemonInitializing { initialization },
    ))
}

fn parse_request(line: &str) -> std::result::Result<Request, String> {
    // The request id is unrecoverable from a malformed envelope, so
    // the parse-error response carries a fixed placeholder id.
    serde_json::from_str(line).map_err(|err| {
        serialize_response(&error_response(
            RequestId::Number(0),
            error_code::PARSE_ERROR,
            format!("invalid JSON-RPC envelope: {err}"),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_proto::jsonrpc::Response;
    use serde_json::json;

    use crate::paths::CasDataDir;
    use crate::{ctl::CtlHandler, data_rpc::DataRpc};

    fn request(method: &str) -> String {
        json!({"jsonrpc": "2.0", "id": 7, "method": method, "params": null}).to_string()
    }

    fn ready_resources() -> (tempfile::TempDir, ReadyDaemon) {
        let data = tempfile::tempdir().unwrap();
        let cas = Arc::new(CasDataDir::with_root(data.path().to_path_buf()));
        cas.ensure().unwrap();
        let lifecycle = RepoLifecycleManager::new(cas.clone());
        let jobs = JobManager::with_lifecycle(cas.clone(), lifecycle.clone());
        let reconcile = RepoReconcileManager::new_with_lifecycle(
            cas.clone(),
            Some(jobs.clone()),
            lifecycle.clone(),
        );
        let watcher = Arc::new(WatchManager::with_reconcile(cas.clone(), reconcile.clone()));
        let shutdown = Arc::new(Notify::new());
        let data_handler = Arc::new(DataRpc::with_lifecycle(
            cas.clone(),
            Some(lifecycle.clone()),
        ));
        let control_handler = Arc::new(CtlHandler::with_full_context(
            cas,
            shutdown,
            "test-version",
            Some(watcher.clone()),
            Some(jobs.clone()),
            Some(reconcile.clone()),
            Some(lifecycle.clone()),
        ));
        (
            data,
            ReadyDaemon {
                data_handler,
                control_handler,
                job_manager: jobs,
                reconcile,
                lifecycle,
                watch_manager: watcher,
            },
        )
    }

    #[test]
    fn progress_is_monotonic_and_total_is_fixed() {
        let gate = StartupGate::new(Arc::new(Notify::new()), "test-version");
        gate.advance(
            DaemonInitializationPhase::WatcherBarrier,
            Some(DaemonInitializationDetail::ArmingRegisteredWatchers),
        )
        .unwrap();
        let status = gate.status();
        assert_eq!(status.completed_phases, 4);
        assert_eq!(status.total_phases, 7);

        let err = gate
            .advance(
                DaemonInitializationPhase::JobManager,
                Some(DaemonInitializationDetail::RestoringJobs),
            )
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
        assert_eq!(gate.status(), status);
    }

    #[tokio::test]
    async fn initializing_status_is_available_without_ready_resources() {
        let gate = StartupGate::new(Arc::new(Notify::new()), "test-version");
        gate.advance(
            DaemonInitializationPhase::WatcherBarrier,
            Some(DaemonInitializationDetail::ArmingRegisteredWatchers),
        )
        .unwrap();
        let handler = StartupControlHandler::new(gate);

        let response: Response =
            serde_json::from_str(&handler.handle(&request("status")).await.unwrap()).unwrap();
        let report: StatusReport = serde_json::from_value(response.result.unwrap()).unwrap();
        assert_eq!(report.daemon_version, "test-version");
        assert_eq!(report.initialization.completed_phases, 4);
        assert!(report.repos.is_empty());
    }

    #[tokio::test]
    async fn non_status_requests_receive_typed_initializing_error() {
        let gate = StartupGate::new(Arc::new(Notify::new()), "test-version");
        let data = StartupDataHandler::new(gate.clone());
        let control = StartupControlHandler::new(gate);

        for response in [
            data.handle(&request("find_symbols")).await.unwrap(),
            control.handle(&request("doctor")).await.unwrap(),
        ] {
            let response: Response = serde_json::from_str(&response).unwrap();
            let error = response.error.unwrap();
            assert_eq!(error.code, error_code::DAEMON_INITIALIZING);
            assert_eq!(error.data.unwrap()["initialization"]["total_phases"], 7);
        }
    }

    #[tokio::test]
    async fn shutdown_is_acknowledged_during_initialization() {
        let shutdown = Arc::new(Notify::new());
        let gate = StartupGate::new(shutdown.clone(), "test-version");
        let handler = StartupControlHandler::new(gate);
        let notified = shutdown.notified();

        let response: Response =
            serde_json::from_str(&handler.handle(&request("shutdown")).await.unwrap()).unwrap();
        assert_eq!(response.result.unwrap()["ok"], true);
        tokio::time::timeout(std::time::Duration::from_millis(50), notified)
            .await
            .expect("shutdown notification was not published");
    }

    #[tokio::test]
    async fn publish_before_shutdown_transfers_bundle_to_teardown_once() {
        let shutdown = Arc::new(Notify::new());
        let gate = StartupGate::new(shutdown, "test-version");
        let data_handler = StartupDataHandler::new(gate.clone());
        let (_data, resources) = ready_resources();

        assert!(gate.publish_ready(resources).is_ok());
        assert!(gate.status().is_ready());
        let response: Response = serde_json::from_str(
            &data_handler
                .handle(&request("definitely_missing"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(response.error.unwrap().code, error_code::METHOD_NOT_FOUND);

        assert!(gate.begin_shutdown().is_some());
        assert!(gate.begin_shutdown().is_none());
    }

    #[test]
    fn shutdown_before_publish_returns_bundle_for_partial_cleanup() {
        let gate = StartupGate::new(Arc::new(Notify::new()), "test-version");
        let (_data, resources) = ready_resources();

        assert!(gate.begin_shutdown().is_none());
        assert!(gate.publish_ready(resources).is_err());
    }
}
