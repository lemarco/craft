//! Facade persistence: `CraftClusterBuilder::data_dir` survives stop → restart.

use craft::CraftCluster;
use craft::net::LocalNetwork;
use craft::proto::NodeId;
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, fast_raft_config_with_seed,
    wait_for_craft_leader, wait_for_craft_stopped, wait_for_group_leaders,
};

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
