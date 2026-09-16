//! Chat — sticky session append on group `chat`.

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

use crate::debug;

#[derive(Default)]
struct State {
    history: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AppendAck {
    pub lines: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Append {
    pub text: String,
}

#[cap_handler(group = "chat")]
fn append(msg: Append, state: &mut State) -> Result<AppendAck, CapError> {
    let node = std::env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into());
    state.history.push(msg.text.clone());
    debug::chat_message(&msg.text);
    println!("[chat node {node}] {}", msg.text);
    Ok(AppendAck {
        lines: state.history.len() as u64,
    })
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<State>::for_cap::<Append>().per_node(),
        append_register,
    ))
}
