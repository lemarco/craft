//! Read line count (inline ask — e.g. HTTP `/chat` stats).

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, CapError, OpCtx};

use super::State;
use crate::domain::chat;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LineCount {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LineCountAck {
    pub lines: u64,
}

#[cap_handler(group = "chat")]
async fn line_count(
    _msg: LineCount,
    _ctx: OpCtx<'_>,
    state: &mut State,
) -> Result<LineCountAck, CapError> {
    Ok(LineCountAck {
        lines: chat::line_count(&state.history),
    })
}
