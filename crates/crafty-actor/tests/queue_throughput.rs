//! [`QueueService`] batch + prefetch over [`LocalNetwork`](crafty_net::LocalNetwork).

use std::sync::Arc;
use std::time::Duration;

use crafty_actor::{ClusterState, DEFAULT_QUEUE_PREFETCH, JobQueue, QueueService, RedbJobQueue};
use crafty_net::transport::{Body, BoxFuture};
use crafty_net::{
    LocalNetwork, LocalTransport, RequestHandler, Route, Transport, TransportError,
    send_queue_ack_batch, send_queue_enqueue_batch, send_queue_lease,
};
use crafty_proto::{
    NodeId, QueueAckBatchRequest, QueueBatchEnqueueJob, QueueEnqueueBatchRequest, QueueLeaseRequest,
};

struct MockState {
    leader: bool,
    nodes: Vec<NodeId>,
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        self.leader
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }

    fn leader_id(&self) -> Option<NodeId> {
        self.leader.then_some(NodeId(1))
    }

    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }
}

struct QueueHandler(Arc<QueueService>);

impl RequestHandler for QueueHandler {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        self.0.handle_request(route, body)
    }
}

fn batch_job(payload: &[u8], priority: u8) -> QueueBatchEnqueueJob {
    QueueBatchEnqueueJob {
        payload: payload.to_vec(),
        priority,
        not_before_ms: 0,
        shard_key: None,
        dedup_key: None,
        max_attempts: 0,
    }
}

#[tokio::test]
async fn queue_service_batch_prefetch_priority_and_ack_eviction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let net = LocalNetwork::new();
    let state: Arc<dyn ClusterState> = Arc::new(MockState {
        leader: true,
        nodes: vec![NodeId(1)],
    });
    let transport: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), NodeId(1)));
    let service = Arc::new(QueueService::new(
        NodeId(1),
        Arc::clone(&state),
        Arc::clone(&transport),
    ));
    let queue = Arc::new(
        RedbJobQueue::open(dir.path().join("jobs.redb"), Duration::from_secs(60))
            .expect("open queue"),
    );
    service.register_redb_stream("jobs", &queue, DEFAULT_QUEUE_PREFETCH);
    net.attach(NodeId(1), Arc::new(QueueHandler(Arc::clone(&service))));

    let client = LocalTransport::new(net, NodeId(1));
    let enqueue = send_queue_enqueue_batch(
        &client,
        NodeId(1),
        &QueueEnqueueBatchRequest {
            stream: "jobs".into(),
            jobs: vec![batch_job(b"low", 0), batch_job(b"high", 9)],
        },
    )
    .await
    .expect("batch enqueue");
    assert!(enqueue.error.is_none(), "{:?}", enqueue.error);
    assert_eq!(enqueue.job_ids.len(), 2);

    let worker = crafty_actor::WorkerId {
        node: NodeId(1),
        instance: 0,
    };
    let first = send_queue_lease(
        &client,
        NodeId(1),
        &QueueLeaseRequest {
            stream: "jobs".into(),
            worker_node: worker.node.0,
            worker_instance: worker.instance,
            max: 1,
        },
    )
    .await
    .expect("lease");
    assert!(first.error.is_none());
    assert_eq!(first.jobs.len(), 1);
    assert_eq!(first.jobs[0].payload, b"high");

    let second = send_queue_lease(
        &client,
        NodeId(1),
        &QueueLeaseRequest {
            stream: "jobs".into(),
            worker_node: worker.node.0,
            worker_instance: worker.instance,
            max: 1,
        },
    )
    .await
    .expect("lease");
    assert_eq!(second.jobs.len(), 1);
    assert_eq!(second.jobs[0].payload, b"low");

    let lease_ids = [first.jobs[0].lease_id, second.jobs[0].lease_id];
    let ack = send_queue_ack_batch(
        &client,
        NodeId(1),
        &QueueAckBatchRequest {
            stream: "jobs".into(),
            worker_node: worker.node.0,
            worker_instance: worker.instance,
            lease_ids: lease_ids.to_vec(),
        },
    )
    .await
    .expect("ack batch");
    assert!(ack.error.is_none(), "{:?}", ack.error);

    let idle = send_queue_lease(
        &client,
        NodeId(1),
        &QueueLeaseRequest {
            stream: "jobs".into(),
            worker_node: worker.node.0,
            worker_instance: worker.instance,
            max: 4,
        },
    )
    .await
    .expect("lease idle");
    assert!(idle.jobs.is_empty());
    assert_eq!(queue.metrics().await.expect("metrics").pending, 0);
}
