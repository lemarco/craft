//! The per-node request router: a single [`RequestHandler`] that fans the fixed
//! `/raft/v1/*` routes out to the four sub-handlers a fully-wired node runs
//! (consensus/client, actor control, actor delivery, and directory sync).
//!
//! `craft-net` attaches exactly one handler per node, and each sub-handler only
//! serves the routes it owns (erroring on the rest). The facade owns the glue
//! that stitches them into one node, so users never assemble a router by hand.

use std::sync::Arc;

use craft_core::StateMachine;
use craft_net::transport::{Body, BoxFuture};
use craft_net::{RequestHandler, Route, TransportError};

use craft_actor::{ClusterControl, ClusterMessaging, DirectorySync, NodeService};

/// Routes an inbound request to the sub-handler that owns it (ADR 010 route
/// table). One of these is attached per node.
pub(crate) struct NodeRouter<M: StateMachine> {
    service: NodeService<M>,
    control: Arc<ClusterControl>,
    messaging: Arc<ClusterMessaging>,
    directory_sync: Arc<DirectorySync>,
}

impl<M: StateMachine> NodeRouter<M> {
    pub(crate) fn new(
        service: NodeService<M>,
        control: Arc<ClusterControl>,
        messaging: Arc<ClusterMessaging>,
        directory_sync: Arc<DirectorySync>,
    ) -> Self {
        Self {
            service,
            control,
            messaging,
            directory_sync,
        }
    }
}

impl<M: StateMachine> RequestHandler for NodeRouter<M> {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        match route {
            // Consensus, client API, and cluster join are served by the runtime.
            Route::PeerWire | Route::ClientWire | Route::ClusterJoin => {
                self.service.handle(route, body)
            }
            // Remote spawn and migration are the control plane.
            Route::ActorSpawn | Route::ActorMigrate => self.control.handle(route, body),
            // Cross-node actor message delivery.
            Route::ActorDeliver => self.messaging.handle(route, body),
            // Directory publish / anti-entropy.
            Route::ActorRegister => self.directory_sync.handle(route, body),
        }
    }
}
