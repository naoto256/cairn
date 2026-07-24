//! Shared mapping from core errors to JSON-RPC error envelopes.
//!
//! # Code assignment
//!
//! Client-input errors ([`Error::InvalidParams`],
//! [`Error::InvalidArgument`], [`Error::AnchorNotFound`]) map to the
//! standard [`error_code::INVALID_PARAMS`] (-32602). Typed lookup
//! failures use Cairn's implementation-defined codes in the
//! JSON-RPC 2.0 server-error range: [`error_code::REPO_NOT_FOUND`]
//! (-32001), [`error_code::FILE_NOT_INDEXED`] (-32002),
//! [`error_code::AMBIGUOUS_SOURCE`] (-32003),
//! [`error_code::SNAPSHOT_STALE`] (-32004), and
//! [`error_code::DAEMON_INITIALIZING`] (-32005). Every remaining
//! variant — I/O, SQLite, scan failure, repository unavailability,
//! store-not-found, job-manager shutdown, shutdown-deadline, LSP,
//! schema corruption — collapses to the standard
//! [`error_code::INTERNAL_ERROR`] (-32603) through the catch-all arm.
//!
//! # Message sanitization
//!
//! Only [`Error::Internal`] has its wire `message` replaced with a
//! fixed `"internal error"` string. Every other variant is currently
//! emitted unsanitized: its [`std::fmt::Display`] form is copied
//! into the JSON-RPC `message` verbatim. That includes potentially
//! sensitive detail — [`Error::StoreNotFound`] and [`Error::Io`]
//! interpolate absolute paths, [`Error::Lsp`] carries raw backend
//! text — and the wrapping code here does not filter it.
//!
//! # Structured `data`
//!
//! A subset of typed errors attach a structured `data` payload —
//! hints, diagnostics, completeness signals, or candidate lists —
//! so agent clients can retry or recover programmatically instead
//! of parsing prose.

use cairn_proto::jsonrpc::{
    RequestId, Response, error_code, error_response as jsonrpc_error_response,
};
use cairn_proto::{
    Completeness, Diagnostic, DiagnosticCode, DiagnosticSeverity, Hint, HintAction, HintCode,
};
use serde_json::json;

use crate::Error;

/// Build a JSON-RPC error [`Response`] for `err`, echoing `id`
/// verbatim. Chooses the wire code from the typed variant,
/// sanitizes only the [`Error::Internal`] message (see the module
/// docs), and attaches a structured `data` payload for the
/// error variants that carry hint / diagnostic content.
pub(crate) fn error_from(id: RequestId, err: &Error) -> Response {
    // Message: only `Internal(_)` is sanitized. Every other variant
    // returns its `Display` string as-is, so keep those messages
    // caller-facing.
    let msg = match err {
        Error::Internal(_) => "internal error".to_string(),
        _ => err.to_string(),
    };
    // Code: client-input errors → INVALID_PARAMS (-32602); each
    // typed lookup failure → its implementation-defined code in
    // the -32001..-32005 band; everything else falls through the
    // wildcard arm to INTERNAL_ERROR (-32603).
    let code = match err {
        Error::InvalidParams(_) | Error::InvalidArgument(_) | Error::AnchorNotFound { .. } => {
            error_code::INVALID_PARAMS
        }
        Error::RepoNotFound { .. } => error_code::REPO_NOT_FOUND,
        Error::FileNotIndexed { .. } => error_code::FILE_NOT_INDEXED,
        Error::AmbiguousSource { .. } => error_code::AMBIGUOUS_SOURCE,
        Error::SnapshotStale { .. } => error_code::SNAPSHOT_STALE,
        Error::DaemonInitializing { .. } => error_code::DAEMON_INITIALIZING,
        Error::Internal(_) => error_code::INTERNAL_ERROR,
        _ => error_code::INTERNAL_ERROR,
    };
    let mut response = jsonrpc_error_response(id, code, msg);
    // RepoNotFound: attach a single actionable hint pointing the
    // caller at `register_repo` for the missing alias.
    if let Error::RepoNotFound { alias } = err
        && let Some(error) = response.error.as_mut()
    {
        error.data = Some(json!({
            "hints": [Hint {
                code: HintCode::RepoNotRegistered,
                message: format!("No registered repo covers `{alias}`. Use `register_repo` to add it."),
                action: None,
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: Some(alias.clone()),
            }]
        }));
    }
    // FileNotIndexed: mark the response as partially truncated and
    // attach a wait-for-index diagnostic + hint so agents can retry
    // after reconciliation rather than treating this as a hard miss.
    if let Error::FileNotIndexed { repo, file, reason } = err
        && let Some(error) = response.error.as_mut()
    {
        error.data = Some(json!({
            "completeness": Completeness::partial_truncated(
                "file_not_indexed_or_snapshot_stale"
            ),
            "diagnostics": [Diagnostic {
                code: DiagnosticCode::FileNotIndexedOrSnapshotStale,
                severity: DiagnosticSeverity::Warning,
                message: "The requested file is absent from, or the current snapshot could not prove freshness for, this result.".into(),
                language: None,
                analyzer_id: None,
                repo: repo.clone(),
                file: Some(file.clone()),
                details: Some(json!({ "reason": reason })),
            }],
            "hints": [Hint {
                code: HintCode::FileNotIndexedOrSnapshotStale,
                message: "Wait for reconciliation or run `cairn ctl repo reindex <alias>` before retrying the file query.".into(),
                action: Some(HintAction::WaitForIndex),
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: repo.clone().or_else(|| Some(file.clone())),
            }],
            "repo": repo,
            "file": file,
            "reason": reason,
        }));
    }
    // SnapshotStale: same wait-for-index shape as FileNotIndexed but
    // for non-file lookups. `data.file` is deliberately absent — no
    // synthetic target is manufactured (see the regression test in
    // this module).
    if let Error::SnapshotStale { repo, reason } = err
        && let Some(error) = response.error.as_mut()
    {
        error.data = Some(json!({
            "completeness": Completeness::partial_truncated("snapshot_stale"),
            "diagnostics": [Diagnostic {
                code: DiagnosticCode::SnapshotStale,
                severity: DiagnosticSeverity::Warning,
                message: "The current snapshot could not prove freshness, so this empty lookup is not a confirmed miss.".into(),
                language: None,
                analyzer_id: None,
                repo: repo.clone(),
                file: None,
                details: Some(json!({ "reason": reason })),
            }],
            "hints": [Hint {
                code: HintCode::SnapshotStale,
                message: "Wait for reconciliation or run `cairn ctl repo reindex <alias>` before retrying the lookup.".into(),
                action: Some(HintAction::WaitForIndex),
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: repo.clone(),
            }],
            "repo": repo,
            "reason": reason,
        }));
    }
    // AmbiguousSource: enumerate the matching declarations so the
    // caller can narrow via `repo`, `file`, or `line`. The candidate
    // list is bounded upstream; `candidates_truncated` records
    // whether that bound was reached.
    if let Error::AmbiguousSource {
        qualified,
        candidates,
        candidates_truncated,
    } = err
        && let Some(error) = response.error.as_mut()
    {
        error.data = Some(json!({
            "qualified": qualified,
            "candidates": candidates,
            "candidates_truncated": candidates_truncated,
        }));
    }
    // DaemonInitializing: mirror the phase-progress snapshot into
    // `data` (state, phase, completed / total, detail) so callers
    // can render progress and decide when to retry instead of
    // polling blind.
    if let Error::DaemonInitializing { initialization } = err
        && let Some(error) = response.error.as_mut()
    {
        error.data = Some(json!({
            "initialization": initialization,
            "diagnostics": [Diagnostic {
                code: DiagnosticCode::DaemonInitializing,
                severity: DiagnosticSeverity::Info,
                message: "The daemon is still initializing and has not published its ready resources.".into(),
                language: None,
                analyzer_id: None,
                repo: None,
                file: None,
                details: Some(json!({
                    "phase": initialization.phase,
                    "completed_phases": initialization.completed_phases,
                    "total_phases": initialization.total_phases,
                    "detail": initialization.detail,
                })),
            }],
            "hints": [Hint {
                code: HintCode::DaemonNotReady,
                message: "Retry after `cairn ctl daemon status` reports ready.".into(),
                action: None,
                tool: None,
                params: None,
                drop_params: Vec::new(),
                target: None,
            }],
        }));
    }
    response
}

#[cfg(test)]
mod tests {
    use cairn_proto::SymbolKind;
    use cairn_proto::jsonrpc::{RequestId, error_code};
    use cairn_proto::methods::SymbolSourceCandidate;

    use super::error_from;
    use crate::Error;

    fn code_for(err: Error) -> i32 {
        error_from(RequestId::Number(1), &err).error.unwrap().code
    }

    fn message_for(err: Error) -> String {
        error_from(RequestId::Number(1), &err)
            .error
            .unwrap()
            .message
    }

    #[test]
    fn maps_typed_caller_errors_to_jsonrpc_codes() {
        assert_eq!(
            code_for(Error::InvalidParams("bad shape".into())),
            error_code::INVALID_PARAMS
        );
        assert_eq!(
            code_for(Error::InvalidArgument("bad argument".into())),
            error_code::INVALID_PARAMS
        );
        assert_eq!(
            code_for(Error::AnchorNotFound {
                name: "HEAD".into()
            }),
            error_code::INVALID_PARAMS
        );
        assert_eq!(
            code_for(Error::RepoNotFound {
                alias: "demo".into()
            }),
            error_code::REPO_NOT_FOUND
        );
    }

    #[test]
    fn maps_internal_errors_to_sanitized_jsonrpc_response() {
        let resp = error_from(
            RequestId::Number(1),
            &Error::Internal("task panicked: /private/repo secret".into()),
        );
        let error = resp.error.unwrap();

        assert_eq!(error.code, error_code::INTERNAL_ERROR);
        assert_eq!(error.message, "internal error");
        assert!(!error.message.contains("/private/repo"));
    }

    #[test]
    fn preserves_invalid_argument_message_for_client_errors() {
        assert_eq!(
            message_for(Error::InvalidArgument("missing `repo`".into())),
            "invalid argument: missing `repo`"
        );
    }

    #[test]
    fn repo_not_found_error_includes_repo_not_registered_hint() {
        let resp = error_from(
            RequestId::Number(1),
            &Error::RepoNotFound {
                alias: "/tmp/missing".into(),
            },
        );
        let error = resp.error.unwrap();
        let hints = error.data.unwrap()["hints"].as_array().unwrap().clone();
        assert_eq!(hints[0]["code"], "repo_not_registered");
        assert!(hints[0]["action"].is_null() || hints[0].get("action").is_none());
    }

    #[test]
    fn file_not_indexed_error_has_typed_code_and_structured_recovery_data() {
        let response = error_from(
            RequestId::Number(1),
            &Error::FileNotIndexed {
                repo: Some("demo".into()),
                file: "src/new.rs".into(),
                reason: "source_blob_mismatch".into(),
            },
        );
        let error = response.error.unwrap();

        assert_eq!(error.code, error_code::FILE_NOT_INDEXED);
        assert!(error.message.contains("source_blob_mismatch"));
        let data = error.data.unwrap();
        assert_eq!(data["reason"], "source_blob_mismatch");
        assert_eq!(
            data["completeness"]["reason"],
            "file_not_indexed_or_snapshot_stale"
        );
        assert_eq!(
            data["diagnostics"][0]["code"],
            "file_not_indexed_or_snapshot_stale"
        );
        assert_eq!(
            data["hints"][0]["code"],
            "file_not_indexed_or_snapshot_stale"
        );
    }

    #[test]
    fn snapshot_stale_error_has_no_synthetic_file_target() {
        let response = error_from(
            RequestId::Number(1),
            &Error::SnapshotStale {
                repo: Some("demo".into()),
                reason: "reconcile_generation_gap".into(),
            },
        );
        let error = response.error.unwrap();

        assert_eq!(error.code, error_code::SNAPSHOT_STALE);
        let data = error.data.unwrap();
        assert_eq!(data["completeness"]["reason"], "snapshot_stale");
        assert_eq!(data["diagnostics"][0]["code"], "snapshot_stale");
        assert_eq!(data["hints"][0]["code"], "snapshot_stale");
        assert!(data.get("file").is_none());
        assert!(!data.to_string().contains("<unspecified>"));
    }

    #[test]
    fn ambiguous_source_error_has_typed_code_and_bounded_candidates() {
        let response = error_from(
            RequestId::Number(1),
            &Error::AmbiguousSource {
                qualified: "crate::same".into(),
                candidates: vec![SymbolSourceCandidate {
                    repo: "demo".into(),
                    branch: "tentative/1".into(),
                    file: "src/lib.rs".into(),
                    line_start: 7,
                    line_end: 9,
                    kind: SymbolKind::Function,
                }],
                candidates_truncated: true,
            },
        );
        let error = response.error.unwrap();

        assert_eq!(error.code, error_code::AMBIGUOUS_SOURCE);
        let data = error.data.unwrap();
        assert_eq!(data["qualified"], "crate::same");
        assert_eq!(data["candidates"][0]["file"], "src/lib.rs");
        assert_eq!(data["candidates"][0]["line_start"], 7);
        assert_eq!(data["candidates_truncated"], true);
        assert!(!data.to_string().contains("blob_sha"));
    }

    #[test]
    fn daemon_initializing_error_has_typed_code_and_closed_progress_data() {
        use cairn_proto::control::{
            DaemonInitializationDetail, DaemonInitializationPhase, DaemonInitializationStatus,
        };

        let response = error_from(
            RequestId::Number(1),
            &Error::DaemonInitializing {
                initialization: DaemonInitializationStatus::initializing(
                    DaemonInitializationPhase::WatcherBarrier,
                    Some(DaemonInitializationDetail::ArmingRegisteredWatchers),
                ),
            },
        );
        let error = response.error.unwrap();

        assert_eq!(error.code, error_code::DAEMON_INITIALIZING);
        let data = error.data.unwrap();
        assert_eq!(data["initialization"]["state"], "initializing");
        assert_eq!(data["initialization"]["phase"], "watcher_barrier");
        assert_eq!(data["initialization"]["completed_phases"], 4);
        assert_eq!(data["initialization"]["total_phases"], 7);
        assert_eq!(data["diagnostics"][0]["code"], "daemon_initializing");
        assert_eq!(data["hints"][0]["code"], "daemon_not_ready");
        assert!(!data.to_string().contains('/'));
    }
}
