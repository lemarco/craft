//! [`ScheduleSource`] reconcile, dynamic updates, and leader replication.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita_actor::{
    BoxFuture, ClusterState, DEFAULT_QUEUE_PREFETCH, QueueService, RecurringJob, RedbJobQueue,
    ScheduleError, SchedulePoll, ScheduleSource, StaticScheduleSource,
};
use trembita_net::transport::{Body, BoxFuture as NetBoxFuture};
use trembita_net::{
    LocalNetwork, LocalTransport, RequestHandler, Route, Transport, TransportError,
};
use trembita_proto::NodeId;

struct MockState {
    node_id: NodeId,
    leader: Arc<std::sync::Mutex<NodeId>>,
    nodes: Vec<NodeId>,
}

impl MockState {
    fn with_leader(
        node_id: NodeId,
        leader: Arc<std::sync::Mutex<NodeId>>,
        nodes: Vec<NodeId>,
    ) -> Self {
        Self {
            node_id,
            leader,
            nodes,
        }
    }
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        *self.leader.lock().unwrap() == self.node_id
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }

    fn leader_id(&self) -> Option<NodeId> {
        Some(*self.leader.lock().unwrap())
    }

    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }
}

struct QueueHandler(Arc<QueueService>);

impl RequestHandler for QueueHandler {
    fn handle(
        &self,
        route: Route,
        body: Body,
    ) -> NetBoxFuture<'static, Result<Body, TransportError>> {
        self.0.handle_request(route, body)
    }
}

struct MutableSource {
    version: AtomicUsize,
}

impl ScheduleSource for MutableSource {
    fn schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringJob>, ScheduleError>> {
        let v = self.version.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(match v {
                0 => vec![RecurringJob::new("v0", "0 9 * * *", b"zero")],
                _ => vec![
                    RecurringJob::new("v0", "0 9 * * *", b"zero"),
                    RecurringJob::new("v1", "0 10 * * *", b"one"),
                ],
            })
        })
    }
}

#[allow(clippy::type_complexity)]
fn setup_two_node_cluster(
    dir: &tempfile::TempDir,
) -> (
    Arc<std::sync::Mutex<NodeId>>,
    Arc<QueueService>,
    Arc<RedbJobQueue>,
    Arc<QueueService>,
    Arc<RedbJobQueue>,
) {
    let net = LocalNetwork::new();
    let leader = Arc::new(std::sync::Mutex::new(NodeId(1)));
    let nodes = vec![NodeId(1), NodeId(2)];

    let transport1: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), NodeId(1)));
    let state1 = Arc::new(MockState::with_leader(
        NodeId(1),
        Arc::clone(&leader),
        nodes.clone(),
    ));
    let service1 = Arc::new(QueueService::new(
        NodeId(1),
        state1 as Arc<dyn ClusterState>,
        Arc::clone(&transport1),
    ));
    let queue1 = Arc::new(
        RedbJobQueue::open(dir.path().join("n1.redb"), Duration::from_secs(60)).expect("open"),
    );
    service1.register_redb_stream("jobs", &queue1, DEFAULT_QUEUE_PREFETCH);

    let transport2: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), NodeId(2)));
    let state2 = Arc::new(MockState::with_leader(
        NodeId(2),
        Arc::clone(&leader),
        nodes.clone(),
    ));
    let service2 = Arc::new(QueueService::new(
        NodeId(2),
        state2 as Arc<dyn ClusterState>,
        Arc::clone(&transport2),
    ));
    let queue2 = Arc::new(
        RedbJobQueue::open(dir.path().join("n2.redb"), Duration::from_secs(60)).expect("open"),
    );
    service2.register_redb_stream("jobs", &queue2, DEFAULT_QUEUE_PREFETCH);

    net.attach(NodeId(1), Arc::new(QueueHandler(Arc::clone(&service1))));
    net.attach(NodeId(2), Arc::new(QueueHandler(Arc::clone(&service2))));

    (leader, service1, queue1, service2, queue2)
}

#[tokio::test]
async fn schedule_source_updates_between_polls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_leader, service1, queue1, _service2, _queue2) = setup_two_node_cluster(&dir);

    let source = Arc::new(MutableSource {
        version: AtomicUsize::new(0),
    });
    service1.register_schedule_source("jobs", source.clone(), Duration::from_millis(10));

    service1.poll_schedule_sources().await.expect("poll v0");
    assert_eq!(queue1.list_schedules().unwrap().len(), 1);

    source.version.store(1, Ordering::SeqCst);
    let desired = source.schedules().await.unwrap();
    service1
        .reconcile_schedules("jobs", &desired)
        .await
        .expect("reconcile v1");
    let names: Vec<_> = queue1
        .list_schedules()
        .unwrap()
        .into_iter()
        .map(|j| j.name)
        .collect();
    assert_eq!(names, vec!["v0".to_string(), "v1".to_string()]);
}

#[tokio::test]
async fn schedules_replicate_and_survive_leader_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (leader, service1, _queue1, service2, queue2) = setup_two_node_cluster(&dir);

    let source = Arc::new(StaticScheduleSource::new(vec![RecurringJob::new(
        "daily",
        "0 9 * * *",
        b"tick",
    )]));
    service1.register_schedule_source("jobs", source, SchedulePoll::secs(60).duration());

    service1.poll_schedule_sources().await.expect("leader poll");
    assert_eq!(queue2.list_schedules().unwrap().len(), 1);

    *leader.lock().unwrap() = NodeId(2);
    assert!(service2.poll_schedule_sources().await.is_ok());
    let names: Vec<_> = queue2
        .list_schedules()
        .unwrap()
        .into_iter()
        .map(|j| j.name)
        .collect();
    assert_eq!(names, vec!["daily".to_string()]);
}
