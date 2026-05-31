//! `reindex_repo` — force a full rebuild of a registered repo's
//! snapshot DBs.

use cairn_proto::control::Ack;
use cairn_proto::methods::ReindexArgs;
use linkme::distributed_slice;
use serde_json::Value;
use tracing::info;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx, parse_params};
use crate::Result;

struct ReindexRepo;

#[async_trait::async_trait]
impl ControlMethod for ReindexRepo {
    fn name(&self) -> &'static str {
        "reindex_repo"
    }

    async fn dispatch(&self, ctx: &CtlCtx, params: Value) -> Result<Value> {
        let args: ReindexArgs = parse_params(params)?;
        let stats = ctx.indexer.full_index(&args.alias).await?;
        info!(alias = %args.alias, ?stats, "reindex_repo complete");
        Ok(serde_json::to_value(Ack::with_alias(args.alias)).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(ReindexRepo);
