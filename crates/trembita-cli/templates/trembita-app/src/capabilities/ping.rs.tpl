//! Sample capability — inline `ping` op.

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest};

#[derive(Default)]
struct PingState;

#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pong {
    pub echo: String,
}

#[cap_handler(group = "app")]
async fn ping(msg: Ping, _state: &mut PingState) -> Result<Pong, CapError> {
    Ok(Pong { echo: msg.msg })
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<PingState>::for_cap::<Ping>().instances(1),
        ping_register,
    ))
}
