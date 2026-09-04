//! Integration tests for durable actor workflow store (B-01).

use std::sync::Arc;
use std::time::Duration;

use trembita::NodeId;
use trembita::cluster::TrembitaCluster;
use trembita::core::StateMachine;
use trembita::net::LocalNetwork;
use trembita::proto::LogIndex;
use trembita_test_support::{advance, await_trembita_leader};

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

async fn await_store_leader(
    clusters: &[Arc<TrembitaCluster<Empty>>],
) -> Arc<TrembitaCluster<Empty>> {
    for _ in 0..500 {
        for c in clusters {
            if !c.is_leader().await {
                continue;
            }
            let store = c.actor_state_store().expect("auto durable store");
            if store.set("_probe", b"1", None).await.is_ok() {
                let _ = store.delete("_probe").await;
                return Arc::clone(c);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("store leader not ready");
}

#[tokio::test(start_paused = true)]
async fn durable_actor_store_replicates_to_voters() {
    let base = std::env::temp_dir().join(format!(
        "trembita-store-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();

    for id in ids {
        let cluster = Arc::new(
            TrembitaCluster::builder(id, Empty)
                .members(ids)
                .data_dir(base.join(format!("node-{}", id.0)))
                .tick_period(Duration::from_millis(5))
                .start_local(&net)
                .await,
        );
        clusters.push(cluster);
    }

    let leader = await_trembita_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;

    let store = leader.actor_state_store().expect("auto durable store");
    store.set("order:1", b"done", None).await.unwrap();

    advance(Duration::from_millis(100)).await;

    for c in &clusters {
        let local = c.actor_state_store().expect("store on every node");
        assert_eq!(
            local.get("order:1").await.unwrap(),
            Some(b"done".to_vec()),
            "node {:?} should see replicated key",
            c.node_id()
        );
    }

    for c in clusters {
        c.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn store_replicate_rejects_non_leader_caller() {
    use trembita::net::{LocalTransport, send_store_replicate};
    use trembita_proto::{StoreReplicateOp, StoreReplicateRequest};

    let base = std::env::temp_dir().join(format!(
        "trembita-store-auth-{}",
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
                .start_local(&net)
                .await,
        ));
    }

    let _leader = await_trembita_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;

    let follower = LocalTransport::new(net.clone(), NodeId(2));
    let reply = send_store_replicate(
        &follower,
        NodeId(3),
        &StoreReplicateRequest {
            ops: vec![StoreReplicateOp::Set {
                key: "k".into(),
                value: b"v".to_vec(),
                expires_at_ms: 0,
            }],
            leader_id: NodeId(3).0,
        },
    )
    .await
    .expect("wire round trip");
    let err = reply.error.expect("replicate should fail");
    assert!(
        matches!(err, trembita_proto::ProductWireError::ReplicateNotLeader),
        "unexpected: {err}"
    );

    for c in clusters {
        c.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn ttl_keys_expire_on_cluster_store() {
    let base = std::env::temp_dir().join(format!(
        "trembita-store-ttl-{}",
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
                .tick_period(Duration::from_millis(5))
                .start_local(&net)
                .await,
        ));
    }

    let leader = await_store_leader(&clusters).await;
    let store = leader.actor_state_store().expect("auto durable store");
    store
        .set("session:1", b"open", Some(Duration::from_secs(1)))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1100)).await;

    for c in &clusters {
        let local = c.actor_state_store().expect("store on every node");
        assert_eq!(
            local.get("session:1").await.unwrap(),
            None,
            "node {:?} should not see expired key",
            c.node_id()
        );
    }

    for c in clusters {
        c.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}
