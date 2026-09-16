//! Sample capability — inline `ping` op.

use serde::{Deserialize, Serialize};
use trembita::{CapError, CapOp, OpCtx, Route};

#[derive(Default)]
pub struct PingState;

#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pong {
    pub echo: String,
}

impl trembita::CapRequest for Ping {
    const GROUP: &'static str = "app";
    const OP: &'static str = "ping";
    type Reply = Pong;
}

fn ping_run(msg: Ping, _ctx: OpCtx<'_>, _state: &mut PingState) -> Result<Pong, CapError> {
    Ok(Pong { echo: msg.msg })
}

#[must_use]
pub fn ping_op() -> CapOp<PingState> {
    CapOp::new("ping", ping_run).routes([Route::Inline, Route::InlineFire])
}
