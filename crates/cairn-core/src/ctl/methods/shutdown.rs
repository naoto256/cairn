//! `shutdown` — cleanly notify the daemon to stop accepting requests.
//!
//! The handler signals the shared shutdown `Notify` and returns an
//! `Ack::ok()` immediately; the actual teardown is driven by
//! whichever task in `main` (or the daemon runtime) is
//! `.notified().await`ing on it, which may already be advancing by
//! the time the reply is written — the ack and teardown are
//! concurrent, not strictly ordered. `notify_waiters()` only wakes
//! already-registered waiters, so the daemon is responsible for
//! subscribing before it opens the control socket.

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
