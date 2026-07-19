//! Shared mapping from core errors to JSON-RPC error envelopes.

use cairn_proto::jsonrpc::{
    RequestId, Response, error_code, error_response as jsonrpc_error_response,
};
use cairn_proto::{
    Completeness, Diagnostic, DiagnosticCode, DiagnosticSeverity, Hint, HintAction, HintCode,
};
use serde_json::json;

use crate::Error;

pub(crate) fn error_from(id: RequestId, err: &Error) -> Response {
    let msg = match err {
        Error::Internal(_) => "internal error".to_string(),
        _ => err.to_string(),
    };
    let code = match err {
        Error::InvalidParams(_) | Error::InvalidArgument(_) | Error::AnchorNotFound { .. } => {
            error_code::INVALID_PARAMS
        }
        Error::RepoNotFound { .. } => error_code::REPO_NOT_FOUND,
        Error::FileNotIndexed { .. } => error_code::FILE_NOT_INDEXED,
        Error::SnapshotStale { .. } => error_code::SNAPSHOT_STALE,
        Error::Internal(_) => error_code::INTERNAL_ERROR,
        _ => error_code::INTERNAL_ERROR,
    };
    let mut response = jsonrpc_error_response(id, code, msg);
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
    response
}

#[cfg(test)]
mod tests {
    use cairn_proto::jsonrpc::{RequestId, error_code};

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
                reason: "file_not_indexed".into(),
            },
        );
        let error = response.error.unwrap();

        assert_eq!(error.code, error_code::FILE_NOT_INDEXED);
        let data = error.data.unwrap();
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
}
