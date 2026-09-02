//! HTTP/3 route table (`docs/protocol.md`, wire-transport).
//!
//! Every crafty RPC is a `POST` to a fixed path under `/raft/v1`. A single QUIC
//! listener serves them all; the path selects the handler, and the
//! [`TrafficClass`] groups routes so consensus traffic can use a **dedicated
//! QUIC connection** separate from client/actor traffic (future-work-and-risks R2), which
//! keeps heartbeats from being head-of-line blocked behind bulk payloads.

/// Common prefix for every versioned route.
pub const API_PREFIX: &str = "/raft/v1";

/// Inter-node Raft RPC (`RaftRpc` in, `RaftRpcReply` out).
pub const PEER_WIRE_PATH: &str = "/raft/v1/peer/wire";
/// Client API (`ClientRequest` in, `ClientResponse` out).
pub const CLIENT_WIRE_PATH: &str = "/raft/v1/client/wire";
/// Cluster join handshake (join-rpc).
pub const CLUSTER_JOIN_PATH: &str = "/raft/v1/cluster/join";
/// Cluster leave handshake (symmetric to join; removes a node from group 0).
pub const CLUSTER_LEAVE_PATH: &str = "/raft/v1/cluster/leave";
/// Peer-address book exchange for address propagation (discovery).
pub const CLUSTER_PEERS_PATH: &str = "/raft/v1/cluster/peers";
/// Deliver a message / ask to a remote actor mailbox (cross-node-actors).
pub const ACTOR_DELIVER_PATH: &str = "/raft/v1/actor/deliver";
/// Remote spawn / placement (cross-node-actors).
pub const ACTOR_SPAWN_PATH: &str = "/raft/v1/actor/spawn";
/// Forward a cluster-wide scale to the leader (cross-node-actors, supervisor-leader).
pub const ACTOR_SCALE_PATH: &str = "/raft/v1/actor/scale";
/// Snapshot transfer + respawn on a target node (cross-node-actors).
pub const ACTOR_MIGRATE_PATH: &str = "/raft/v1/actor/migrate";
/// Stop a group on a target node for a planned scale-down / removal (cross-node-actors, supervisor-leader).
pub const ACTOR_STOP_PATH: &str = "/raft/v1/actor/stop";
/// Directory publish / revoke (cross-node-actors).
pub const ACTOR_REGISTER_PATH: &str = "/raft/v1/actor/register";
/// Cross-node Raft group migration (write-sharding-multi-raft).
pub const CLUSTER_GROUP_MIGRATE_PATH: &str = "/raft/v1/cluster/group/migrate";
/// Dynamic multi-Raft catalog expansion (dynamic catalog / stable shards).
pub const CLUSTER_CATALOG_ADD_PATH: &str = "/raft/v1/cluster/catalog/add";
/// Enqueue a job on the leader queue service ([job-queue](../../../docs/decisions/job-queue.md)).
pub const QUEUE_ENQUEUE_PATH: &str = "/raft/v1/queue/enqueue";
/// Enqueue many jobs in one leader transaction.
pub const QUEUE_ENQUEUE_BATCH_PATH: &str = "/raft/v1/queue/enqueue-batch";
/// Lease jobs from a queue stream.
pub const QUEUE_LEASE_PATH: &str = "/raft/v1/queue/lease";
/// Acknowledge a leased job.
pub const QUEUE_ACK_PATH: &str = "/raft/v1/queue/ack";
/// Acknowledge many leased jobs in one leader transaction.
pub const QUEUE_ACK_BATCH_PATH: &str = "/raft/v1/queue/ack-batch";
/// Return a leased job to pending.
pub const QUEUE_NACK_PATH: &str = "/raft/v1/queue/nack";
/// Extend a live lease (worker heartbeat during long handlers).
pub const QUEUE_EXTEND_LEASE_PATH: &str = "/raft/v1/queue/extend-lease";
/// Queue depth gauges for autoscale / observability.
pub const QUEUE_METRICS_PATH: &str = "/raft/v1/queue/metrics";
/// Lookup job metadata by id.
pub const QUEUE_JOB_STATUS_PATH: &str = "/raft/v1/queue/job-status";
/// Requeue a dead-letter job.
pub const QUEUE_REQUEUE_DEAD_LETTER_PATH: &str = "/raft/v1/queue/requeue-dead-letter";
/// List jobs in a stream with filters (admin / ops).
pub const QUEUE_LIST_JOBS_PATH: &str = "/raft/v1/queue/list-jobs";
/// Requeue many dead-letter jobs in one leader transaction.
pub const QUEUE_REQUEUE_DEAD_LETTER_BATCH_PATH: &str = "/raft/v1/queue/requeue-dead-letter-batch";
/// Leader → follower replication of queue mutations (failover durability).
pub const QUEUE_REPLICATE_PATH: &str = "/raft/v1/queue/replicate";
/// Publish one event on a durable topic ([event-topics](../../../docs/decisions/event-topics.md)).
pub const TOPIC_PUBLISH_PATH: &str = "/raft/v1/topic/publish";
/// Lease events for a named subscription.
pub const TOPIC_LEASE_PATH: &str = "/raft/v1/topic/lease";
/// Acknowledge a leased event on a subscription.
pub const TOPIC_ACK_PATH: &str = "/raft/v1/topic/ack";
/// Negative-acknowledge a leased event on a subscription.
pub const TOPIC_NACK_PATH: &str = "/raft/v1/topic/nack";
/// Topic depth and subscription lag gauges.
pub const TOPIC_METRICS_PATH: &str = "/raft/v1/topic/metrics";
/// Leader → follower replication of topic mutations.
pub const TOPIC_REPLICATE_PATH: &str = "/raft/v1/topic/replicate";
/// Set an actor workflow key on the store leader ([actor-state-store](../../../docs/decisions/actor-state-store.md)).
pub const ACTOR_STORE_SET_PATH: &str = "/raft/v1/actor-store/set";
/// Delete an actor workflow key on the store leader.
pub const ACTOR_STORE_DELETE_PATH: &str = "/raft/v1/actor-store/delete";
/// Compare-and-set on the store leader.
pub const ACTOR_STORE_COMPARE_AND_SET_PATH: &str = "/raft/v1/actor-store/compare-and-set";
/// Leader → follower replication of actor-store mutations.
pub const ACTOR_STORE_REPLICATE_PATH: &str = "/raft/v1/actor-store/replicate";

/// The connection class a route belongs to. Peer consensus traffic is isolated
/// onto its own QUIC connection from everything else (future-work-and-risks R2).
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
    /// [`CLUSTER_LEAVE_PATH`].
    ClusterLeave,
    /// [`CLUSTER_PEERS_PATH`].
    ClusterPeers,
    /// [`ACTOR_DELIVER_PATH`].
    ActorDeliver,
    /// [`ACTOR_SPAWN_PATH`].
    ActorSpawn,
    /// [`ACTOR_SCALE_PATH`].
    ActorScale,
    /// [`ACTOR_MIGRATE_PATH`].
    ActorMigrate,
    /// [`ACTOR_STOP_PATH`].
    ActorStop,
    /// [`ACTOR_REGISTER_PATH`].
    ActorRegister,
    /// [`CLUSTER_GROUP_MIGRATE_PATH`].
    ClusterGroupMigrate,
    /// [`CLUSTER_CATALOG_ADD_PATH`].
    ClusterCatalogAdd,
    /// [`QUEUE_ENQUEUE_PATH`].
    QueueEnqueue,
    /// [`QUEUE_ENQUEUE_BATCH_PATH`].
    QueueEnqueueBatch,
    /// [`QUEUE_LEASE_PATH`].
    QueueLease,
    /// [`QUEUE_ACK_PATH`].
    QueueAck,
    /// [`QUEUE_ACK_BATCH_PATH`].
    QueueAckBatch,
    /// [`QUEUE_NACK_PATH`].
    QueueNack,
    /// [`QUEUE_EXTEND_LEASE_PATH`].
    QueueExtendLease,
    /// [`QUEUE_METRICS_PATH`].
    QueueMetrics,
    /// [`QUEUE_JOB_STATUS_PATH`].
    QueueJobStatus,
    /// [`QUEUE_LIST_JOBS_PATH`].
    QueueListJobs,
    /// [`QUEUE_REQUEUE_DEAD_LETTER_PATH`].
    QueueRequeueDeadLetter,
    /// [`QUEUE_REQUEUE_DEAD_LETTER_BATCH_PATH`].
    QueueRequeueDeadLetterBatch,
    /// [`QUEUE_REPLICATE_PATH`].
    QueueReplicate,
    /// [`TOPIC_PUBLISH_PATH`].
    TopicPublish,
    /// [`TOPIC_LEASE_PATH`].
    TopicLease,
    /// [`TOPIC_ACK_PATH`].
    TopicAck,
    /// [`TOPIC_NACK_PATH`].
    TopicNack,
    /// [`TOPIC_METRICS_PATH`].
    TopicMetrics,
    /// [`TOPIC_REPLICATE_PATH`].
    TopicReplicate,
    /// [`ACTOR_STORE_SET_PATH`].
    ActorStoreSet,
    /// [`ACTOR_STORE_DELETE_PATH`].
    ActorStoreDelete,
    /// [`ACTOR_STORE_COMPARE_AND_SET_PATH`].
    ActorStoreCompareAndSet,
    /// [`ACTOR_STORE_REPLICATE_PATH`].
    ActorStoreReplicate,
}

impl Route {
    /// Every route, in a stable order (handy for building a router or tests).
    pub const ALL: [Route; 36] = [
        Route::PeerWire,
        Route::ClientWire,
        Route::ClusterJoin,
        Route::ClusterLeave,
        Route::ClusterPeers,
        Route::ActorDeliver,
        Route::ActorSpawn,
        Route::ActorScale,
        Route::ActorMigrate,
        Route::ActorStop,
        Route::ActorRegister,
        Route::ClusterGroupMigrate,
        Route::ClusterCatalogAdd,
        Route::QueueEnqueue,
        Route::QueueEnqueueBatch,
        Route::QueueLease,
        Route::QueueAck,
        Route::QueueAckBatch,
        Route::QueueNack,
        Route::QueueExtendLease,
        Route::QueueMetrics,
        Route::QueueJobStatus,
        Route::QueueListJobs,
        Route::QueueRequeueDeadLetter,
        Route::QueueRequeueDeadLetterBatch,
        Route::QueueReplicate,
        Route::TopicPublish,
        Route::TopicLease,
        Route::TopicAck,
        Route::TopicNack,
        Route::TopicMetrics,
        Route::TopicReplicate,
        Route::ActorStoreSet,
        Route::ActorStoreDelete,
        Route::ActorStoreCompareAndSet,
        Route::ActorStoreReplicate,
    ];

    /// The request path for this route.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Route::PeerWire => PEER_WIRE_PATH,
            Route::ClientWire => CLIENT_WIRE_PATH,
            Route::ClusterJoin => CLUSTER_JOIN_PATH,
            Route::ClusterLeave => CLUSTER_LEAVE_PATH,
            Route::ClusterPeers => CLUSTER_PEERS_PATH,
            Route::ActorDeliver => ACTOR_DELIVER_PATH,
            Route::ActorSpawn => ACTOR_SPAWN_PATH,
            Route::ActorScale => ACTOR_SCALE_PATH,
            Route::ActorMigrate => ACTOR_MIGRATE_PATH,
            Route::ActorStop => ACTOR_STOP_PATH,
            Route::ActorRegister => ACTOR_REGISTER_PATH,
            Route::ClusterGroupMigrate => CLUSTER_GROUP_MIGRATE_PATH,
            Route::ClusterCatalogAdd => CLUSTER_CATALOG_ADD_PATH,
            Route::QueueEnqueue => QUEUE_ENQUEUE_PATH,
            Route::QueueEnqueueBatch => QUEUE_ENQUEUE_BATCH_PATH,
            Route::QueueLease => QUEUE_LEASE_PATH,
            Route::QueueAck => QUEUE_ACK_PATH,
            Route::QueueAckBatch => QUEUE_ACK_BATCH_PATH,
            Route::QueueNack => QUEUE_NACK_PATH,
            Route::QueueExtendLease => QUEUE_EXTEND_LEASE_PATH,
            Route::QueueMetrics => QUEUE_METRICS_PATH,
            Route::QueueJobStatus => QUEUE_JOB_STATUS_PATH,
            Route::QueueListJobs => QUEUE_LIST_JOBS_PATH,
            Route::QueueRequeueDeadLetter => QUEUE_REQUEUE_DEAD_LETTER_PATH,
            Route::QueueRequeueDeadLetterBatch => QUEUE_REQUEUE_DEAD_LETTER_BATCH_PATH,
            Route::QueueReplicate => QUEUE_REPLICATE_PATH,
            Route::TopicPublish => TOPIC_PUBLISH_PATH,
            Route::TopicLease => TOPIC_LEASE_PATH,
            Route::TopicAck => TOPIC_ACK_PATH,
            Route::TopicNack => TOPIC_NACK_PATH,
            Route::TopicMetrics => TOPIC_METRICS_PATH,
            Route::TopicReplicate => TOPIC_REPLICATE_PATH,
            Route::ActorStoreSet => ACTOR_STORE_SET_PATH,
            Route::ActorStoreDelete => ACTOR_STORE_DELETE_PATH,
            Route::ActorStoreCompareAndSet => ACTOR_STORE_COMPARE_AND_SET_PATH,
            Route::ActorStoreReplicate => ACTOR_STORE_REPLICATE_PATH,
        }
    }

    /// The HTTP method. Every crafty route is a `POST`.
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
            Route::ClusterJoin
            | Route::ClusterLeave
            | Route::ClusterPeers
            | Route::ClusterGroupMigrate
            | Route::ClusterCatalogAdd => TrafficClass::Cluster,
            Route::ActorDeliver
            | Route::ActorSpawn
            | Route::ActorScale
            | Route::ActorMigrate
            | Route::ActorStop
            | Route::ActorRegister
            | Route::QueueEnqueue
            | Route::QueueEnqueueBatch
            | Route::QueueLease
            | Route::QueueAck
            | Route::QueueAckBatch
            | Route::QueueNack
            | Route::QueueExtendLease
            | Route::QueueMetrics
            | Route::QueueJobStatus
            | Route::QueueListJobs
            | Route::QueueRequeueDeadLetter
            | Route::QueueRequeueDeadLetterBatch
            | Route::QueueReplicate
            | Route::TopicPublish
            | Route::TopicLease
            | Route::TopicAck
            | Route::TopicNack
            | Route::TopicMetrics
            | Route::TopicReplicate
            | Route::ActorStoreSet
            | Route::ActorStoreDelete
            | Route::ActorStoreCompareAndSet
            | Route::ActorStoreReplicate => TrafficClass::Actor,
        }
    }

    /// Resolve a request path to a [`Route`], or `None` if unrecognised.
    #[must_use]
    pub fn from_path(path: &str) -> Option<Route> {
        Route::ALL.into_iter().find(|route| route.path() == path)
    }
}
