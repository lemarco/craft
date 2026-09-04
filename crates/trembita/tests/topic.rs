//! Event topic facade integration: publish, subscriptions, retention.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::sync::Arc;
use std::time::Duration;

use trembita::cluster::TrembitaCluster;
use trembita::core::StateMachine;
use trembita::net::LocalNetwork;
use trembita::proto::{LogIndex, NodeId};
use trembita::{TopicOpts, TrembitaApp, TrembitaConfigure};
use trembita_events::{SubscriptionStart, TopicSubscriptionDef};
use trembita_test_support::{advance, await_trembita_leader, boot_local_app};

#[derive(Default)]
struct Empty;

impl StateMachine for Empty {
    type Command = ();
    type Query = ();
    type Response = ();
    type Error = std::convert::Infallible;

    fn apply(&mut self, _index: LogIndex, _command: &()) -> Result<(), Self::Error> {
        Ok(())
    }
    fn query(&self, _query: &()) -> Result<(), Self::Error> {
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn trembita_app_publishes_and_tracks_topic_metrics() {
    let base = std::env::temp_dir().join(format!(
        "trembita-topic-facade-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .topics([TopicOpts::topic("orders.events").subscription_defs([
                    TopicSubscriptionDef {
                        name: "analytics".into(),
                        start: SubscriptionStart::Earliest,
                        max_attempts: 0,
                    },
                ])])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    for _ in 0..200 {
        if app.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(app.is_leader().await);

    app.publish("orders.events", b"order-created")
        .await
        .expect("publish");

    let topic = app.event_topic("orders.events").unwrap();
    for _ in 0..200 {
        let metrics = topic.metrics().await.unwrap();
        if metrics.event_count >= 1 {
            assert_eq!(metrics.event_count, 1);
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("topic metrics never showed published event");
}

#[tokio::test(start_paused = true)]
async fn topic_replicate_rejects_non_leader_caller() {
    use trembita::net::{LocalTransport, send_topic_replicate};
    use trembita_proto::{ProductWireError, TopicReplicateOp, TopicReplicateRequest};

    let base = std::env::temp_dir().join(format!(
        "trembita-topic-auth-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);

    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for id in ids {
        clusters.push(Arc::new(
            TrembitaCluster::builder(id, Empty)
                .members(ids)
                .data_dir(base.join(format!("node-{}", id.0)))
                .event_topic("orders.events", Duration::from_secs(30))
                .tick_period(Duration::from_millis(5))
                .start_local(&net)
                .await,
        ));
    }

    let _leader = await_trembita_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;

    let follower = LocalTransport::new(net.clone(), NodeId(2));
    let reply = send_topic_replicate(
        &follower,
        NodeId(3),
        &TopicReplicateRequest {
            topic: "orders.events".into(),
            leader_id: NodeId(3).0,
            ops: vec![TopicReplicateOp::Publish {
                event_id: 1,
                payload: b"x".to_vec(),
                published_at_ms: 1,
                next_event_id: 2,
            }],
        },
    )
    .await
    .expect("wire round trip");
    let err = reply.error.expect("replicate should fail");
    assert!(
        matches!(err, ProductWireError::ReplicateNotLeader),
        "unexpected: {err}"
    );

    for c in clusters {
        c.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}
