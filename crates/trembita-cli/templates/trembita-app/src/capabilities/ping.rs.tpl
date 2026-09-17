//! Sample capability — inline `ping` op.

use serde::{Deserialize, Serialize};
use trembita::{cap_handler, cap_register_chain, CapError, CapGroup, CapManifest, OpCtx};

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
async fn ping(msg: Ping, ctx: OpCtx<'_>, _state: &mut PingState) -> Result<Pong, CapError> {
    let _service = ctx
        .deps::<crate::deps::AppDeps>()
        .map(|d| d.service_name)
        .unwrap_or("unknown");
    Ok(Pong { echo: msg.msg })
}

#[must_use]
pub fn manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<PingState>::for_cap::<Ping>(),
        ping_register,
    ))
}
