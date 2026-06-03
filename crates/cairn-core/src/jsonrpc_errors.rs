//! Shared mapping from core errors to JSON-RPC error envelopes.

use cairn_proto::jsonrpc::{
    RequestId, Response, error_code, error_response as jsonrpc_error_response,
};

use crate::Error;

pub(crate) fn error_from(id: RequestId, err: &Error) -> Response {
    let msg = err.to_string();
    let code = match err {
        Error::InvalidParams(_) | Error::AnchorNotFound { .. } => error_code::INVALID_PARAMS,
        Error::RepoNotFound { .. } => error_code::REPO_NOT_FOUND,
        _ => error_code::INTERNAL_ERROR,
    };
    jsonrpc_error_response(id, code, msg)
}

#[cfg(test)]
mod tests {
    use cairn_proto::jsonrpc::{RequestId, error_code};

    use super::error_from;
    use crate::Error;

    fn code_for(err: Error) -> i32 {
        error_from(RequestId::Number(1), &err).error.unwrap().code
    }

    #[test]
    fn maps_typed_caller_errors_to_jsonrpc_codes() {
        assert_eq!(
            code_for(Error::InvalidParams("bad shape".into())),
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
}
