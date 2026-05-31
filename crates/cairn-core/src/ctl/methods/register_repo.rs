//! `register_repo` — add a repository, do the initial full index,
//! and attach the live watcher.

use cairn_proto::control::Ack;
use cairn_proto::methods::RegisterRepoArgs;
use linkme::distributed_slice;
use serde_json::Value;
use tracing::{info, warn};

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::{Error, Result};

struct RegisterRepo;

#[async_trait::async_trait]
impl ControlMethod for RegisterRepo {
    fn name(&self) -> &'static str {
        "register_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: RegisterRepoArgs = parse_params(params)?;
        let path = std::path::PathBuf::from(&args.path);
        let registered = ctx
            .indexer
            .register_repo(&args.alias, &path)
            .await
            .map_err(|e| Error::InvalidArgument(format!("register_repo: {e}")))?;
        ctx.indexer
            .full_index(&args.alias)
            .await
            .map_err(|e| Error::InvalidArgument(format!("full_index: {e}")))?;
        if let Err(e) = ctx.watcher.start(&args.alias, &registered.root).await {
            // Indexing already succeeded; report watcher failure as
            // a warning rather than rolling back, because subsequent
            // explicit `reindex_repo` calls still work.
            warn!(alias = %args.alias, error = %e, "watcher failed to start");
        }
        info!(alias = %args.alias, "register_repo complete");
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(RegisterRepo);
