//! Append a chat line ([`Route::Session`] / WebSocket fire path).

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, CapError, OpCtx};

use super::State;
use crate::debug;
use crate::domain::chat;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AppendAck {
    pub lines: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Append {
    pub text: String,
}

#[cap_handler(group = "chat")]
async fn append(msg: Append, _ctx: OpCtx<'_>, state: &mut State) -> Result<AppendAck, CapError> {
    let node = std::env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into());
    let lines = chat::append_line(&mut state.history, msg.text.clone());
    debug::chat_message(&msg.text);
    println!("[chat node {node}] {}", msg.text);
    Ok(AppendAck { lines })
}
