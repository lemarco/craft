//! The per-node request router: a single [`RequestHandler`] that fans the fixed
//! `/raft/v1/*` routes out to the sub-handlers a fully-wired node runs
//! (consensus/client, actor control, actor delivery, and directory sync) and
//! owns the address plane (`/cluster/peers`, discovery).
//!
//! `craft-net` attaches exactly one handler per node, and each sub-handler only
//! serves the routes it owns (erroring on the rest). The facade owns the glue
//! that stitches them into one node, so users never assemble a router by hand.

use std::net::SocketAddr;
use std::sync::Arc;

use craft_net::transport::{Body, BoxFuture, RequestHandler};
use craft_net::{QuicTransport, Route, TransportError, decode_body, encode_body};
use craft_proto::{JoinRequest, NodeId, PeerBook, PeerEntry, ScaleRequest};

use craft_actor::{ClusterControl, ClusterMessaging, DirectorySync};

use crate::multi_raft::GroupMigratePort;

/// The address plane: learn peer addresses at runtime and snapshot the current
/// address book (discovery). Backed by the live [`QuicTransport`] directory in
/// production; a no-op over the in-memory [`LocalNetwork`](craft_net::LocalNetwork),
/// where nodes are addressed by id and have no socket addresses to gossip.
pub(crate) trait PeerSource: Send + Sync + 'static {
    /// Record (or update) how to reach `id`. Unparseable addresses are ignored.
    fn learn(&self, id: NodeId, addr: &str);
    /// A snapshot of currently known `(node, addr)` pairs for gossip.
    fn book(&self) -> PeerBook;
}

/// [`PeerSource`] over the live QUIC transport's mutable peer directory.
pub(crate) struct QuicPeers(pub Arc<QuicTransport>);

impl PeerSource for QuicPeers {
    fn learn(&self, id: NodeId, addr: &str) {
        if let Ok(addr) = addr.parse::<SocketAddr>() {
            self.0.learn_peer(id, addr);
        }
    }

    fn book(&self) -> PeerBook {
        PeerBook {
            entries: self
                .0
                .peers()
                .iter()
                .map(|(node, addr)| PeerEntry {
                    node,
                    addr: addr.to_string(),
                })
                .collect(),
        }
    }
}

/// [`PeerSource`] for transports without socket addresses (the in-memory
/// [`LocalNetwork`](craft_net::LocalNetwork)): there is nothing to learn or
/// gossip, so both operations are inert.
pub(crate) struct NoPeers;

impl PeerSource for NoPeers {
    fn learn(&self, _id: NodeId, _addr: &str) {}
    fn book(&self) -> PeerBook {
        PeerBook::default()
    }
}

/// Routes an inbound request to the sub-handler that owns it (wire-transport route
/// table). One of these is attached per node.
pub(crate) struct NodeRouter {
    service: Arc<dyn RequestHandler>,
    control: Arc<ClusterControl>,
    messaging: Arc<ClusterMessaging>,
    directory_sync: Arc<DirectorySync>,
    peers: Arc<dyn PeerSource>,
    group_migrate: Option<Arc<dyn GroupMigratePort>>,
}

impl NodeRouter {
    pub(crate) fn new(
        service: Arc<dyn RequestHandler>,
        control: Arc<ClusterControl>,
        messaging: Arc<ClusterMessaging>,
        directory_sync: Arc<DirectorySync>,
        peers: Arc<dyn PeerSource>,
        group_migrate: Option<Arc<dyn GroupMigratePort>>,
    ) -> Self {
        Self {
            service,
            control,
            messaging,
            directory_sync,
            peers,
            group_migrate,
        }
    }
}

impl RequestHandler for NodeRouter {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        match route {
            // Consensus, client API, and cluster join are served by the runtime.
            // A join carries the joiner's advertise address; record it here so
            // this node (leader or the forwarding follower) can dial the newcomer
            // before the address gossip converges (discovery, join-rpc).
            Route::ClusterJoin => {
                if let Ok(request) = decode_body::<JoinRequest>(&body) {
                    self.peers.learn(request.node_id, &request.advertise_addr);
                }
                self.service.handle(route, body)
            }
            Route::ClusterLeave => self.service.handle(route, body),
            Route::ClusterCatalogAdd => self.service.handle(route, body),
            Route::PeerWire | Route::ClientWire => self.service.handle(route, body),
            Route::ClusterGroupMigrate => {
                let Some(handler) = self.group_migrate.as_ref() else {
                    return Box::pin(async move {
                        Err(TransportError::Io(
                            "multi-raft group migration is not enabled".into(),
                        ))
                    });
                };
                let handler = Arc::clone(handler);
                Box::pin(async move {
                    let request: craft_proto::GroupMigrateRequest = decode_body(&body)?;
                    let reply = handler.handle_group_migrate(request).await;
                    Ok(encode_body(&reply)?)
                })
            }
            // Address-book anti-entropy: hand back what we know (discovery).
            Route::ClusterPeers => {
                let book = self.peers.book();
                Box::pin(async move { Ok(encode_body(&book)?) })
            }
            // Remote spawn, migration, and scale-down stop are the control plane.
            Route::ActorSpawn | Route::ActorMigrate | Route::ActorStop => {
                self.control.handle(route, body)
            }
            // A follower forwarded a cluster-wide scale here; execute it via
            // the control plane against the requester's observed voter set
            // (supervisor-leader). Async because it drives remote spawns.
            Route::ActorScale => {
                let control = Arc::clone(&self.control);
                Box::pin(async move {
                    let request: ScaleRequest = decode_body(&body)?;
                    let reply = control.handle_scale(&request).await;
                    Ok(encode_body(&reply)?)
                })
            }
            // Cross-node actor message delivery.
            Route::ActorDeliver => self.messaging.handle(route, body),
            // Directory publish / anti-entropy.
            Route::ActorRegister => self.directory_sync.handle(route, body),
        }
    }
}
