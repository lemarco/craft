//! End-to-end test of the **live QUIC + mTLS** transport via
//! [`TrembitaClusterBuilder::start_quic`]: a real 3-node cluster, each on its own
//! UDP socket, mutually authenticated by a shared dev cluster CA, elects a
//! leader and replicates proposals/queries over HTTP/3.

use std::net::SocketAddr;
use std::sync::Arc;

use trembita::NodeId;
use trembita::cluster::TrembitaCluster;
use trembita::cluster::{PeerDirectory, Security};
use trembita::net::tls::ClusterCa;
use trembita::proto::LogIndex;
use trembita_test_support::{
    Cmd, Kv, Qry, Resp, TICK_PERIOD, advance, await_trembita_leader, fast_raft_config, free_udp,
};

#[tokio::test(start_paused = true)]
async fn quic_cluster_elects_leader_and_replicates() {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let addrs: Vec<SocketAddr> = ids.iter().map(|_| free_udp()).collect();

    // One shared dev CA so every node trusts every peer (mTLS).
    let ca = ClusterCa::generate().expect("dev CA");
    let peers: PeerDirectory = ids.iter().copied().zip(addrs.iter().copied()).collect();

    let mut clusters = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        // `Security::new` is always available; the dev CA supplies the material
        // (equivalent to the feature-gated `Security::dev` helper).
        let security = Security::new(
            ca.issue_node(id).expect("issue node cert"),
            ca.root_store().expect("trust root"),
        );
        let cluster = TrembitaCluster::builder(id, Kv::default())
            .members(ids)
            .raft_config(fast_raft_config())
            .tick_period(TICK_PERIOD)
            .start_quic(security, addrs[i], peers.clone())
            .await
            .expect("start quic node");
        clusters.push(Arc::new(cluster));
    }

    let leader = await_trembita_leader(&clusters).await;

    // Write through the leader and read it back linearizably.
    let resp = leader
        .handle()
        .propose(Cmd::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect("propose over quic");
    assert_eq!(resp, Resp::Set { previous: None });

    let resp = leader
        .handle()
        .query(Qry::Get { key: "k".into() })
        .await
        .expect("query over quic");
    assert_eq!(resp, Resp::Value(Some("v".into())));

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn a_new_node_dynamically_joins_over_quic() {
    // Backlog E5 / discovery: a fourth node joins a live 3-node QUIC cluster
    // knowing ONLY the seed's address. It fetches the peer-address book from the
    // seed, is added by the leader via a membership change, and both directions
    // learn each other's addresses over `/cluster/peers`.
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let addrs: Vec<SocketAddr> = ids.iter().map(|_| free_udp()).collect();
    let ca = ClusterCa::generate().expect("dev CA");
    let peers: PeerDirectory = ids.iter().copied().zip(addrs.iter().copied()).collect();

    let mut clusters = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        let security = Security::new(
            ca.issue_node(id).expect("issue node cert"),
            ca.root_store().expect("trust root"),
        );
        let cluster = TrembitaCluster::builder(id, Kv::default())
            .members(ids)
            .allow_join(true)
            .raft_config(fast_raft_config())
            .tick_period(TICK_PERIOD)
            .start_quic(security, addrs[i], peers.clone())
            .await
            .expect("start quic node");
        clusters.push(Arc::new(cluster));
    }

    let leader = await_trembita_leader(&clusters).await;
    leader
        .handle()
        .propose(Cmd::Set {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect("seed proposal");

    // Bring up node 4 knowing only node 1's address as a seed — no static entry
    // for nodes 2 and 3. `members` is the *current* voter set (without node 4),
    // so it starts as a non-voting follower and never disrupts the election.
    let joiner_id = NodeId(4);
    let joiner_addr = free_udp();
    let seed_only: PeerDirectory = [(NodeId(1), addrs[0])].into_iter().collect();
    let security = Security::new(
        ca.issue_node(joiner_id).expect("issue joiner cert"),
        ca.root_store().expect("trust root"),
    );
    let joiner = TrembitaCluster::builder(joiner_id, Kv::default())
        .members(ids)
        .allow_join(true)
        .raft_config(fast_raft_config())
        .tick_period(TICK_PERIOD)
        .join(NodeId(1), addrs[0])
        .start_quic(security, joiner_addr, seed_only)
        .await
        .expect("dynamic join over quic");
    let joiner = Arc::new(joiner);

    // The join committed a membership change adding node 4, and it caught up to
    // the pre-join state.
    let mut join_complete = false;
    for _ in 0..1000 {
        if let Some(status) = joiner.status().await
            && status.voters.contains(&joiner_id)
            && status.last_applied >= LogIndex(1)
        {
            join_complete = true;
            break;
        }
        advance(TICK_PERIOD).await;
    }
    assert!(join_complete, "node 4 did not become a voter and catch up");

    // A fresh proposal now replicates to the enlarged cluster, node 4 included.
    let leader = await_trembita_leader(&clusters).await;
    leader
        .handle()
        .propose(Cmd::Set {
            key: "k2".into(),
            value: "v2".into(),
        })
        .await
        .expect("post-join proposal");
    let target = leader.status().await.expect("leader status").commit_index;

    let mut caught_up = false;
    for _ in 0..1000 {
        if let Some(status) = joiner.status().await
            && status.last_applied >= target
        {
            caught_up = true;
            break;
        }
        advance(TICK_PERIOD).await;
    }
    assert!(caught_up, "node 4 did not replicate the post-join proposal");

    joiner.shutdown();
    for c in &clusters {
        c.shutdown();
    }
}
