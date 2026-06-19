//! Shared mapping from core errors to JSON-RPC error envelopes.

use cairn_proto::jsonrpc::{
    RequestId, Response, error_code, error_response as jsonrpc_error_response,
};
use cairn_proto::{Hint, HintCode};
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
}
