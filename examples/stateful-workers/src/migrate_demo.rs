//! # Actor migration demo (separate from main HTTP showcase)
//!
//! | Mode | Command |
//! |------|---------|
//! | Local (fast) | `cargo run --release -- migrate-demo` |
//! | QUIC cluster | `./cluster.sh 1-migrate` + `./cluster.sh 2-migrate`, then `./cluster.sh migrate-run` |

use std::time::Duration;

use crafty::core::{Config, StateMachine};
use crafty::net::LocalNetwork;
use crafty::proto::{ActorId, LogIndex};
use crafty::{CraftyCluster, NodeId};

use crate::migrate_counter::{CounterMsg, StatefulCounter};

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

async fn wait_leader(clusters: &[CraftyCluster<Empty>]) {
    for _ in 0..300 {
        for c in clusters {
            if c.is_leader().await {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader");
}

pub async fn run_local() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::temp_dir().join("crafty-showcase-stateful-migrate");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base)?;

    let ids = [NodeId(1), NodeId(2)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();

    for &id in &ids {
        let data_dir = base.join(format!("node-{}", id.0));
        std::fs::create_dir_all(&data_dir)?;
        clusters.push(
            CraftyCluster::builder(id, Empty)
                .members(ids)
                .raft_config(Config {
                    election_timeout_min: 5,
                    election_timeout_max: 10,
                    heartbeat_interval: 2,
                    seed: 9,
                    ..Default::default()
                })
                .data_dir(&data_dir)
                .register_actor::<StatefulCounter>()
                .tick_period(Duration::from_millis(10))
                .start_local(&net)
                .await,
        );
    }

    wait_leader(&clusters).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let node1 = &clusters[0];
    let node2 = &clusters[1];

    println!("=== migration demo: 2-node LocalNetwork ===");
    tracing::debug!(target: "showcase", "migration demo starting");

    node1
        .control()
        .spawn_remote::<StatefulCounter>(NodeId(1), "counter", 0)
        .await?;
    let counter = node1.registry().get::<StatefulCounter>("counter").unwrap();
    for _ in 0..3 {
        counter.send(CounterMsg::Inc)?;
    }

    let source = ActorId {
        node: NodeId(1),
        name: "counter".into(),
        instance: 0,
        generation: 0,
    };

    let migrated = node1
        .control()
        .migrate::<StatefulCounter>(source, NodeId(2), 0, Duration::from_secs(5))
        .await?;
    println!(
        "migrated → node {} generation {}",
        migrated.node.0, migrated.generation
    );

    node2
        .registry()
        .get::<StatefulCounter>("counter")
        .unwrap()
        .send(CounterMsg::Inc)?;
    println!("post-migration inc on node 2 (expect [counter] → 4)");

    for c in clusters {
        c.shutdown();
    }
    Ok(())
}
