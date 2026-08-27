//! Facade persistence: `CraftClusterBuilder::data_dir` survives stop → restart.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use craft::CraftCluster;
use craft::net::LocalNetwork;
use craft::proto::NodeId;
use craft::storage::{GroupRedbLayout, LogStore, SnapshotStore};
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, TrackedKv, await_craft_leader,
    fast_raft_config_with_seed, wait_for_craft_leader, wait_for_craft_stopped,
    wait_for_group_leaders,
};

fn node_data_dir(base: &Path, id: NodeId) -> PathBuf {
    base.join(format!("node-{}", id.0))
}

async fn spawn_durable_node<M>(
    net: &LocalNetwork,
    id: NodeId,
    members: [NodeId; 3],
    data_dir: PathBuf,
    machine: M,
) -> CraftCluster<M>
where
    M: craft::core::StateMachine + Send + Sync + Default + 'static,
{
    CraftCluster::builder(id, machine)
        .members(members)
        .raft_config(fast_raft_config_with_seed(11))
        .tick_period(TICK_PERIOD)
        .data_dir(data_dir)
        .start_local(net)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_data_dir_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let node_id = NodeId(1);

    {
        let cluster = CraftCluster::builder(node_id, KvMachine::default())
            .members([node_id])
            .raft_config(fast_raft_config_with_seed(3))
            .tick_period(TICK_PERIOD)
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_craft_leader(&cluster).await;
        let resp = cluster
            .handle()
            .propose(KvCommand::Set {
                key: "persist".into(),
                value: "facade".into(),
            })
            .await
            .expect("propose before restart");
        assert_eq!(resp, KvResponse::Set { previous: None });
        wait_for_craft_stopped(&cluster).await;
    }

    net.detach(node_id);

    {
        let cluster = CraftCluster::builder(node_id, KvMachine::default())
            .members([node_id])
            .raft_config(fast_raft_config_with_seed(3))
            .tick_period(TICK_PERIOD)
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_craft_leader(&cluster).await;
        let got = cluster
            .handle()
            .query(KvQuery::Get {
                key: "persist".into(),
            })
            .await
            .expect("query after restart");
        assert_eq!(
            got,
            KvResponse::Value(Some("facade".into())),
            "state machine must replay the recovered log after restart"
        );
        cluster.shutdown();
    }

    assert!(
        data_dir.join("group-0.redb").is_file(),
        "durable storage file should exist under data_dir"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_raft_data_dir_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let node_id = NodeId(1);

    {
        let cluster = CraftCluster::builder(node_id, KvMachine::default())
            .members([node_id])
            .raft_config(fast_raft_config_with_seed(3))
            .tick_period(TICK_PERIOD)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_group_leaders(&cluster).await;

        let resp = cluster.group_handles()[0]
            .propose(KvCommand::Set {
                key: "g0".into(),
                value: "survives".into(),
            })
            .await
            .expect("propose group 0");
        assert_eq!(resp, KvResponse::Set { previous: None });

        wait_for_craft_stopped(&cluster).await;
    }

    net.detach(node_id);

    {
        let cluster = CraftCluster::builder(node_id, KvMachine::default())
            .members([node_id])
            .raft_config(fast_raft_config_with_seed(3))
            .tick_period(TICK_PERIOD)
            .raft_machines([KvMachine::default(), KvMachine::default()])
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_group_leaders(&cluster).await;

        let got = cluster.group_handles()[0]
            .query(KvQuery::Get { key: "g0".into() })
            .await
            .expect("query group 0 after restart");
        assert_eq!(
            got,
            KvResponse::Value(Some("survives".into())),
            "multi-raft group 0 must recover from redb after restart"
        );
        cluster.shutdown();
    }

    assert!(data_dir.join("group-0.redb").is_file());
    assert!(data_dir.join("group-1.redb").is_file());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compacted_snapshot_survives_facade_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let node_id = NodeId(1);

    {
        let cluster = CraftCluster::builder(node_id, TrackedKv::default())
            .members([node_id])
            .raft_config(fast_raft_config_with_seed(5))
            .tick_period(TICK_PERIOD)
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_craft_leader(&cluster).await;

        for (key, value) in [("a", "1"), ("b", "2"), ("a", "3")] {
            cluster
                .handle()
                .propose(KvCommand::Set {
                    key: key.into(),
                    value: value.into(),
                })
                .await
                .expect("pre-compact write");
        }

        assert!(
            cluster.handle().compact().await.expect("compact"),
            "applied state should compact"
        );

        cluster
            .handle()
            .propose(KvCommand::Set {
                key: "c".into(),
                value: "4".into(),
            })
            .await
            .expect("post-compact write");

        wait_for_craft_stopped(&cluster).await;
    }

    {
        let store = GroupRedbLayout::new(&data_dir)
            .open_group(0)
            .expect("open redb after shutdown");
        assert!(
            store.load_snapshot().expect("load snapshot").is_some(),
            "compact must persist a snapshot under data_dir"
        );
        assert!(
            store.first_index().expect("first index").0 > 1,
            "compacted log prefix must be purged from redb"
        );
    }

    net.detach(node_id);

    {
        let cluster = CraftCluster::builder(node_id, TrackedKv::default())
            .members([node_id])
            .raft_config(fast_raft_config_with_seed(5))
            .tick_period(TICK_PERIOD)
            .data_dir(&data_dir)
            .start_local(&net)
            .await;

        wait_for_craft_leader(&cluster).await;

        for (key, value) in [("a", "3"), ("b", "2"), ("c", "4")] {
            let got = cluster
                .handle()
                .query(KvQuery::Get { key: key.into() })
                .await
                .expect("query after snapshot restart");
            assert_eq!(
                got,
                KvResponse::Value(Some(value.into())),
                "key {key} must survive restart via snapshot + log suffix"
            );
        }

        cluster.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_majority_survives_one_member_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let members = [NodeId(1), NodeId(2), NodeId(3)];

    let mut clusters: Vec<Arc<CraftCluster<KvMachine>>> = Vec::new();
    for &id in &members {
        clusters.push(Arc::new(
            spawn_durable_node(
                &net,
                id,
                members,
                node_data_dir(&base, id),
                KvMachine::default(),
            )
            .await,
        ));
    }

    let leader = await_craft_leader(&clusters).await;
    leader
        .handle()
        .propose(KvCommand::Set {
            key: "before".into(),
            value: "restart".into(),
        })
        .await
        .expect("initial replicate");

    let follower = {
        let mut picked = None;
        for cluster in &clusters {
            if !cluster.is_leader().await {
                picked = Some(Arc::clone(cluster));
                break;
            }
        }
        picked.expect("cluster should have a follower")
    };
    let follower_id = follower.handle().id();

    wait_for_craft_stopped(follower.as_ref()).await;
    net.detach(follower_id);
    clusters.retain(|c| c.handle().id() != follower_id);

    let leader = await_craft_leader(&clusters).await;
    leader
        .handle()
        .propose(KvCommand::Set {
            key: "while-down".into(),
            value: "majority".into(),
        })
        .await
        .expect("two-node majority must still accept writes");

    let restarted = Arc::new(
        spawn_durable_node(
            &net,
            follower_id,
            members,
            node_data_dir(&base, follower_id),
            KvMachine::default(),
        )
        .await,
    );
    clusters.push(Arc::clone(&restarted));

    let leader = await_craft_leader(&clusters).await;
    let target = leader
        .status()
        .await
        .expect("leader status after restart")
        .commit_index;

    for _ in 0..500 {
        if let Some(status) = restarted.status().await
            && status.last_applied >= target
        {
            break;
        }
        tokio::time::sleep(TICK_PERIOD).await;
    }

    let status = restarted.status().await.expect("follower status");
    assert!(
        status.last_applied >= target,
        "restarted follower must catch up to commit_index {}",
        target.0
    );

    let pre = restarted
        .handle()
        .local_query(KvQuery::Get {
            key: "before".into(),
        })
        .await
        .expect("follower local read before");
    let post = restarted
        .handle()
        .local_query(KvQuery::Get {
            key: "while-down".into(),
        })
        .await
        .expect("follower local read while-down");
    assert_eq!(pre, KvResponse::Value(Some("restart".into())));
    assert_eq!(post, KvResponse::Value(Some("majority".into())));

    for cluster in clusters {
        cluster.shutdown();
    }
}
