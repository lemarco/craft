//! Durable topic replication and leader failover.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use trembita_actor::{
    ClusterEventTopic, ClusterState, EventTopic, RedbEventTopic, SubscriptionStart, TopicService,
    TopicSubscriptionDef, WorkerId,
};
use trembita_net::transport::{Body, BoxFuture as NetBoxFuture, RequestHandler};
use trembita_net::{LocalNetwork, LocalTransport, Route, Transport, TransportError};
use trembita_proto::NodeId;

struct MockState {
    node_id: NodeId,
    leader: Arc<Mutex<NodeId>>,
    nodes: Vec<NodeId>,
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

struct TopicHandler(Arc<TopicService>);

impl RequestHandler for TopicHandler {
    fn handle(
        &self,
        route: Route,
        body: Body,
    ) -> NetBoxFuture<'static, Result<Body, TransportError>> {
        self.0.handle_request(route, body)
    }
}

#[tokio::test]
async fn topic_survives_leader_failover() {
    let dir = tempfile::tempdir().expect("tempdir");
    let net = LocalNetwork::new();
    let leader = Arc::new(Mutex::new(NodeId(1)));
    let nodes = vec![NodeId(1), NodeId(2)];

    let subs = [TopicSubscriptionDef {
        name: "analytics".into(),
        start: SubscriptionStart::Earliest,
        max_attempts: 0,
    }];

    let transport1: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), NodeId(1)));
    let state1 = Arc::new(MockState {
        node_id: NodeId(1),
        leader: Arc::clone(&leader),
        nodes: nodes.clone(),
    });
    let service1 = Arc::new(TopicService::new(
        NodeId(1),
        state1.clone() as Arc<dyn ClusterState>,
        Arc::clone(&transport1),
    ));
    let topic1 = Arc::new(
        RedbEventTopic::open(dir.path().join("n1.redb"), Duration::from_secs(60)).expect("open"),
    );
    service1.register_redb_topic("events", &topic1);

    let transport2: Arc<dyn Transport> = Arc::new(LocalTransport::new(net.clone(), NodeId(2)));
    let state2 = Arc::new(MockState {
        node_id: NodeId(2),
        leader: Arc::clone(&leader),
        nodes: nodes.clone(),
    });
    let service2 = Arc::new(TopicService::new(
        NodeId(2),
        state2.clone() as Arc<dyn ClusterState>,
        Arc::clone(&transport2),
    ));
    let topic2 = Arc::new(
        RedbEventTopic::open(dir.path().join("n2.redb"), Duration::from_secs(60)).expect("open"),
    );
    service2.register_redb_topic("events", &topic2);

    net.attach(NodeId(1), Arc::new(TopicHandler(Arc::clone(&service1))));
    net.attach(NodeId(2), Arc::new(TopicHandler(Arc::clone(&service2))));

    service1
        .bootstrap_subscriptions("events", &subs)
        .await
        .expect("bootstrap");

    let client = ClusterEventTopic::new(
        "events",
        NodeId(1),
        state1.clone() as Arc<dyn ClusterState>,
        transport1,
    );

    client.publish(b"before-failover").await.expect("publish");

    *leader.lock().unwrap() = NodeId(2);
    service2
        .bootstrap_subscriptions("events", &subs)
        .await
        .expect("bootstrap");

    let client2 = ClusterEventTopic::new(
        "events",
        NodeId(2),
        state2.clone() as Arc<dyn ClusterState>,
        transport2,
    );
    client2.publish(b"after-failover").await.expect("publish");

    let worker = WorkerId {
        node: NodeId(2),
        instance: 0,
    };
    let events = topic2.lease("analytics", worker, 10).await.expect("lease");
    assert_eq!(events.len(), 2);
}
