//! `shutdown` — cleanly notify the daemon to stop accepting requests.

use cairn_proto::control::Ack;
use linkme::distributed_slice;
use serde_json::Value;
use tracing::info;

use super::super::{CONTROL_METHODS, ControlMethod, CtlCtx};
use crate::Result;

struct Shutdown;

#[async_trait::async_trait]
impl ControlMethod for Shutdown {
    fn name(&self) -> &'static str {
        "shutdown"
    }

    async fn dispatch(&self, ctx: &CtlCtx, _params: Value) -> Result<Value> {
        info!("shutdown requested via control socket");
        ctx.shutdown.notify_waiters();
        Ok(serde_json::to_value(Ack::ok()).unwrap())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(CONTROL_METHODS)]
static REGISTER: fn() -> Box<dyn ControlMethod> = || Box::new(Shutdown);
