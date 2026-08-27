//! Elastic membership: a new node joins a **running** cluster over live QUIC +
//! mTLS knowing only a single seed address — no static, cluster-wide peer map
//! (ADR 007/017). The joiner fetches the peer-address book from the seed, the
//! leader commits a membership change adding it, and addresses propagate both
//! ways over `/cluster/peers` so every node can reach the newcomer.
//!
//! This is the "spin up another VPS and point it at the cluster" workflow, run
//! here as four nodes in one process (each on its own UDP socket) so it needs no
//! external certs — a real dev CA is minted in-process.
//!
//! Run with: `cargo run -p craft --example vps_join --features dev-certs`

use std::net::SocketAddr;
use std::time::Duration;

use craft::core::{Config, StateMachine};
use craft::net::tls::ClusterCa;
use craft::proto::LogIndex;
use craft::{CraftCluster, NodeId, PeerDirectory, Security};

/// A trivial replicated counter — the point of this example is membership, not
/// the state machine.
#[derive(Default)]
struct Counter(u64);

impl StateMachine for Counter {
    type Command = u64;
    type Query = ();
    type Response = u64;
    type Error = std::convert::Infallible;

    fn apply(&mut self, _index: LogIndex, add: &u64) -> Result<u64, Self::Error> {
        self.0 += *add;
        Ok(self.0)
    }
    fn query(&self, _query: &()) -> Result<u64, Self::Error> {
        Ok(self.0)
    }
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_le_bytes().to_vec())
    }
    fn restore(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = u64::from_le_bytes(bytes.try_into().unwrap_or_default());
        Ok(())
    }
}

fn raft_config() -> Config {
    Config {
        election_timeout_min: 5,
        election_timeout_max: 10,
        heartbeat_interval: 2,
        seed: 7,
    }
}

/// Grab a currently-free localhost UDP address for a QUIC listener.
fn free_udp() -> SocketAddr {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 6)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One shared dev CA so every node mutually authenticates over mTLS.
    let ca = ClusterCa::generate()?;
    let security = |id| -> Result<Security, Box<dyn std::error::Error>> {
        Ok(Security::new(ca.issue_node(id)?, ca.root_store()?))
    };

    // --- Bring up an initial 3-node cluster --------------------------------
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let addrs: Vec<SocketAddr> = ids.iter().map(|_| free_udp()).collect();
    let peers: PeerDirectory = ids.iter().copied().zip(addrs.iter().copied()).collect();

    let mut clusters = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let cluster = CraftCluster::builder(id, Counter::default())
            .members(ids)
            .allow_join(true) // accept newcomers (ADR 017)
            .raft_config(raft_config())
            .tick_period(Duration::from_millis(10))
            .start_quic(security(id)?, addrs[i], peers.clone())
            .await?;
        clusters.push(cluster);
    }
    println!("started a 3-node QUIC cluster on {addrs:?}");

    // Find the leader and write something so the joiner has state to catch up on.
    let leader = await_leader(&clusters).await;
    let total = clusters[leader].handle().propose(41).await?;
    println!("leader is node {}; counter = {total}", leader + 1);

    // --- Spin up a 4th "VPS" that joins knowing only the seed --------------
    let joiner_id = NodeId(4);
    let joiner_addr = free_udp();
    let seed_id = ids[leader]; // any member works; use the leader here
    let seed_only: PeerDirectory = [(seed_id, addrs[leader])].into_iter().collect();

    println!("joining node 4 via seed {seed_id:?} at {}…", addrs[leader]);
    let joiner = CraftCluster::builder(joiner_id, Counter::default())
        .members(ids) // the *current* voters; node 4 is not one yet
        .allow_join(true)
        .raft_config(raft_config())
        .tick_period(Duration::from_millis(10))
        .join(seed_id, addrs[leader])
        .start_quic(security(joiner_id)?, joiner_addr, seed_only)
        .await?;

    // The join blocks until the membership change commits; confirm node 4 is now
    // a voter and has replicated the pre-join state.
    for _ in 0..1000 {
        if let Some(status) = joiner.status().await
            && status.voters.contains(&joiner_id)
            && status.last_applied >= LogIndex(1)
        {
            println!(
                "node 4 joined: voters = {:?}, applied up to index {}",
                status.voters, status.last_applied.0
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        joiner.status().await.unwrap().voters.contains(&joiner_id),
        "node 4 should be a voter after joining"
    );

    // A fresh proposal now replicates to all four nodes.
    if let Some(l) = first_leader(&clusters).await {
        clusters[l].handle().propose(1).await?;
    }
    println!("elastic join complete ✓");

    joiner.shutdown();
    for c in &clusters {
        c.shutdown();
    }
    Ok(())
}

/// Index of a node that currently believes it is the leader, if any.
async fn first_leader(clusters: &[CraftCluster<Counter>]) -> Option<usize> {
    for (i, c) in clusters.iter().enumerate() {
        if c.is_leader().await {
            return Some(i);
        }
    }
    None
}

/// Block (up to ~10s) until some node is the leader, returning its index.
async fn await_leader(clusters: &[CraftCluster<Counter>]) -> usize {
    for _ in 0..1000 {
        if let Some(i) = first_leader(clusters).await {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader elected");
}
