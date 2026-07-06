//! HTTP/3 route table (`docs/protocol.md`, ADR 010).
//!
//! Every craft RPC is a `POST` to a fixed path under `/raft/v1`. A single QUIC
//! listener serves them all; the path selects the handler, and the
//! [`TrafficClass`] groups routes so consensus traffic can use a **dedicated
//! QUIC connection** separate from client/actor traffic (ADR 027 R2), which
//! keeps heartbeats from being head-of-line blocked behind bulk payloads.

/// Common prefix for every versioned route.
pub const API_PREFIX: &str = "/raft/v1";

/// Inter-node Raft RPC (`RaftRpc` in, `RaftRpcReply` out).
pub const PEER_WIRE_PATH: &str = "/raft/v1/peer/wire";
/// Client API (`ClientRequest` in, `ClientResponse` out).
pub const CLIENT_WIRE_PATH: &str = "/raft/v1/client/wire";
/// Cluster join handshake (ADR 017).
pub const CLUSTER_JOIN_PATH: &str = "/raft/v1/cluster/join";
/// Peer-address book exchange for address propagation (ADR 007).
pub const CLUSTER_PEERS_PATH: &str = "/raft/v1/cluster/peers";
/// Deliver a message / ask to a remote actor mailbox (ADR 013).
pub const ACTOR_DELIVER_PATH: &str = "/raft/v1/actor/deliver";
/// Remote spawn / placement (ADR 013).
pub const ACTOR_SPAWN_PATH: &str = "/raft/v1/actor/spawn";
/// Snapshot transfer + respawn on a target node (ADR 013).
pub const ACTOR_MIGRATE_PATH: &str = "/raft/v1/actor/migrate";
/// Directory publish / revoke (ADR 013).
pub const ACTOR_REGISTER_PATH: &str = "/raft/v1/actor/register";

/// The connection class a route belongs to. Peer consensus traffic is isolated
/// onto its own QUIC connection from everything else (ADR 027 R2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrafficClass {
    /// Raft consensus RPC — latency-sensitive, isolated connection.
    Peer,
    /// External client API.
    Client,
    /// Cluster membership / join control plane.
    Cluster,
    /// Cross-node actor messaging and lifecycle.
    Actor,
}

/// A recognised HTTP/3 endpoint. Used by the server to dispatch an incoming
/// request and by the client to address an outgoing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    /// [`PEER_WIRE_PATH`].
    PeerWire,
    /// [`CLIENT_WIRE_PATH`].
    ClientWire,
    /// [`CLUSTER_JOIN_PATH`].
    ClusterJoin,
    /// [`CLUSTER_PEERS_PATH`].
    ClusterPeers,
    /// [`ACTOR_DELIVER_PATH`].
    ActorDeliver,
    /// [`ACTOR_SPAWN_PATH`].
    ActorSpawn,
    /// [`ACTOR_MIGRATE_PATH`].
    ActorMigrate,
    /// [`ACTOR_REGISTER_PATH`].
    ActorRegister,
}

impl Route {
    /// Every route, in a stable order (handy for building a router or tests).
    pub const ALL: [Route; 8] = [
        Route::PeerWire,
        Route::ClientWire,
        Route::ClusterJoin,
        Route::ClusterPeers,
        Route::ActorDeliver,
        Route::ActorSpawn,
        Route::ActorMigrate,
        Route::ActorRegister,
    ];

    /// The request path for this route.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Route::PeerWire => PEER_WIRE_PATH,
            Route::ClientWire => CLIENT_WIRE_PATH,
            Route::ClusterJoin => CLUSTER_JOIN_PATH,
            Route::ClusterPeers => CLUSTER_PEERS_PATH,
            Route::ActorDeliver => ACTOR_DELIVER_PATH,
            Route::ActorSpawn => ACTOR_SPAWN_PATH,
            Route::ActorMigrate => ACTOR_MIGRATE_PATH,
            Route::ActorRegister => ACTOR_REGISTER_PATH,
        }
    }

    /// The HTTP method. Every craft route is a `POST`.
    #[must_use]
    pub const fn method(self) -> &'static str {
        "POST"
    }

    /// Which [`TrafficClass`] (and therefore QUIC connection) this route uses.
    #[must_use]
    pub const fn traffic_class(self) -> TrafficClass {
        match self {
            Route::PeerWire => TrafficClass::Peer,
            Route::ClientWire => TrafficClass::Client,
            Route::ClusterJoin | Route::ClusterPeers => TrafficClass::Cluster,
            Route::ActorDeliver
            | Route::ActorSpawn
            | Route::ActorMigrate
            | Route::ActorRegister => TrafficClass::Actor,
        }
    }

    /// Resolve a request path to a [`Route`], or `None` if unrecognised.
    #[must_use]
    pub fn from_path(path: &str) -> Option<Route> {
        Route::ALL.into_iter().find(|route| route.path() == path)
    }
}
