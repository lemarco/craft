//! Group-scoped peer transport for multi-Raft (ADR 031).

use std::sync::Arc;

use craft_proto::{GroupPeerEnvelope, RaftRpc};

use crate::route::Route;
use crate::transport::{Body, BoxFuture, Transport, TransportError};
use crate::wire::{decode_body, encode_body};

/// Wraps a [`Transport`] so outbound `/peer/wire` bodies carry a
/// [`GroupPeerEnvelope`].
#[derive(Clone)]
pub struct GroupTransport {
    /// Raft group this transport sends on behalf of.
    pub group: u32,
    inner: Arc<dyn Transport>,
}

impl GroupTransport {
    /// Scope outbound peer RPCs to `group` over `inner`.
    #[must_use]
    pub fn new(group: u32, inner: Arc<dyn Transport>) -> Self {
        Self { group, inner }
    }
}

impl Transport for GroupTransport {
    fn send(
        &self,
        peer: craft_proto::NodeId,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        let inner = Arc::clone(&self.inner);
        let group = self.group;
        Box::pin(async move {
            let body = if route == Route::PeerWire {
                let rpc: RaftRpc = decode_body(&body)?;
                encode_body(&GroupPeerEnvelope { group, rpc })?
            } else {
                body
            };
            inner.send(peer, route, body).await
        })
    }
}
