//! Queue replication must fan out only to reachable voters, not every live member.

use std::sync::Arc;
use std::time::Duration;

use trembita_jobs::{JobQueue, QueueService, RedbJobQueue, WorkerId};
use trembita_net::transport::{Body, BoxFuture};
use trembita_net::{
    LocalNetwork, LocalTransport, RequestHandler, Route, Transport, TransportError,
    send_queue_enqueue, send_queue_lease,
};
use trembita_proto::{NodeId, QueueEnqueueRequest, QueueLeaseRequest};
use trembita_runtime::ClusterState;

struct MockState {
    leader: bool,
    leader_id: NodeId,
    live: Vec<NodeId>,
    reachable: Vec<NodeId>,
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        self.leader
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.live.clone()
    }

    fn leader_id(&self) -> Option<NodeId> {
        Some(self.leader_id)
    }

    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.reachable.clone()
    }
}

struct QueueHandler(Arc<QueueService>);

impl RequestHandler for QueueHandler {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        self.0.handle_request(route, body)
    }
}

#[tokio::test]
async fn enqueue_replicates_only_to_reachable_voters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let net = LocalNetwork::new();

    let leader_state: Arc<dyn ClusterState> = Arc::new(MockState {
        leader: true,
        leader_id: NodeId(1),
        live: vec![NodeId(1), NodeId(2), NodeId(3)],
        reachable: vec![NodeId(1), NodeId(2)],
    });
    let follower_state: Arc<dyn ClusterState> = Arc::new(MockState {
        leader: false,
        leader_id: NodeId(1),
        live: vec![NodeId(1), NodeId(2), NodeId(3)],
        reachable: vec![NodeId(1), NodeId(2), NodeId(3)],
    });

    let leader_transport: Arc<dyn Transport> =
        Arc::new(LocalTransport::new(net.clone(), NodeId(1)));
    let leader_service = Arc::new(QueueService::new(
        NodeId(1),
        Arc::clone(&leader_state),
        Arc::clone(&leader_transport),
    ));
    let leader_queue = Arc::new(
        RedbJobQueue::open(dir.path().join("leader.redb"), Duration::from_secs(60))
            .expect("open leader queue"),
    );
    leader_service.register_redb_stream("jobs", &leader_queue, 0);

    let follower_transport: Arc<dyn Transport> =
        Arc::new(LocalTransport::new(net.clone(), NodeId(2)));
    let follower_service = Arc::new(QueueService::new(
        NodeId(2),
        follower_state,
        follower_transport,
    ));
    let follower_queue = Arc::new(
        RedbJobQueue::open(dir.path().join("follower.redb"), Duration::from_secs(60))
            .expect("open follower queue"),
    );
    follower_service.register_redb_stream("jobs", &follower_queue, 0);

    net.attach(
        NodeId(1),
        Arc::new(QueueHandler(Arc::clone(&leader_service))),
    );
    net.attach(NodeId(2), Arc::new(QueueHandler(follower_service)));

    let reply = send_queue_enqueue(
        leader_transport.as_ref(),
        NodeId(1),
        &QueueEnqueueRequest {
            stream: "jobs".into(),
            payload: b"payload".to_vec(),
            priority: 0,
            not_before_ms: 0,
            shard_key: None,
            dedup_key: None,
            max_attempts: 0,
        },
    )
    .await
    .expect("enqueue rpc");
    assert!(reply.error.is_none(), "enqueue failed: {:?}", reply.error);
    assert_eq!(leader_queue.metrics().await.expect("metrics").pending, 1);
    assert_eq!(
        follower_queue.metrics().await.expect("metrics").pending,
        1,
        "reachable follower should receive replication"
    );
}

#[tokio::test]
async fn lease_succeeds_when_unreachable_voter_is_excluded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let net = LocalNetwork::new();

    let leader_state: Arc<dyn ClusterState> = Arc::new(MockState {
        leader: true,
        leader_id: NodeId(1),
        live: vec![NodeId(1), NodeId(2), NodeId(3)],
        reachable: vec![NodeId(1), NodeId(2)],
    });
    let follower_state: Arc<dyn ClusterState> = Arc::new(MockState {
        leader: false,
        leader_id: NodeId(1),
        live: vec![NodeId(1), NodeId(2), NodeId(3)],
        reachable: vec![NodeId(1), NodeId(2), NodeId(3)],
    });

    let leader_transport: Arc<dyn Transport> =
        Arc::new(LocalTransport::new(net.clone(), NodeId(1)));
    let leader_service = Arc::new(QueueService::new(
        NodeId(1),
        Arc::clone(&leader_state),
        Arc::clone(&leader_transport),
    ));
    let leader_queue = Arc::new(
        RedbJobQueue::open(dir.path().join("leader.redb"), Duration::from_secs(60))
            .expect("open leader queue"),
    );
    leader_service.register_redb_stream("jobs", &leader_queue, 0);

    let follower_transport: Arc<dyn Transport> =
        Arc::new(LocalTransport::new(net.clone(), NodeId(2)));
    let follower_service = Arc::new(QueueService::new(
        NodeId(2),
        follower_state,
        follower_transport,
    ));
    let follower_queue = Arc::new(
        RedbJobQueue::open(dir.path().join("follower.redb"), Duration::from_secs(60))
            .expect("open follower queue"),
    );
    follower_service.register_redb_stream("jobs", &follower_queue, 0);

    net.attach(
        NodeId(1),
        Arc::new(QueueHandler(Arc::clone(&leader_service))),
    );
    net.attach(NodeId(2), Arc::new(QueueHandler(follower_service)));

    assert!(
        send_queue_enqueue(
            leader_transport.as_ref(),
            NodeId(1),
            &QueueEnqueueRequest {
                stream: "jobs".into(),
                payload: b"work".to_vec(),
                priority: 0,
                not_before_ms: 0,
                shard_key: None,
                dedup_key: None,
                max_attempts: 0,
            },
        )
        .await
        .expect("enqueue rpc")
        .error
        .is_none(),
        "enqueue should succeed when dead voter is excluded from reachability"
    );

    let reply = send_queue_lease(
        leader_transport.as_ref(),
        NodeId(1),
        &QueueLeaseRequest {
            stream: "jobs".into(),
            worker_node: 1,
            worker_instance: 1,
            max: 1,
        },
    )
    .await
    .expect("lease rpc");
    assert!(reply.error.is_none(), "lease failed: {:?}", reply.error);
    assert_eq!(reply.jobs.len(), 1);
    assert_eq!(reply.jobs[0].payload, b"work");
    assert_eq!(leader_queue.metrics().await.expect("metrics").leased, 1);

    let worker = WorkerId {
        node: NodeId(1),
        instance: 1,
    };
    let follower_leased = follower_queue
        .lease(worker, 1)
        .await
        .expect("follower lease");
    assert!(
        follower_leased.is_empty(),
        "replicated lease should prevent double lease on follower"
    );
}
