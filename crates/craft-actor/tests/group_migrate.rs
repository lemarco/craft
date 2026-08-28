//! Cross-node Raft group migration helpers (write-sharding-multi-raft).

use std::sync::Arc;
use std::time::Duration;

use craft_actor::craft_net::LocalNetwork;
use craft_actor::craft_proto::NodeId;
use craft_actor::{RuntimeConfig, spawn_raft_group, spawn_raft_group_from_bundle};
use craft_test_support::{
    KvCommand, KvMachine, KvQuery, KvResponse, TICK_PERIOD, fast_raft_config_with_seed,
    wait_for_node_leader,
};

#[tokio::test(start_paused = true)]
async fn export_and_respawn_from_bundle_preserves_state() {
    let net = LocalNetwork::new();
    let ids = [NodeId(1)];
    let runtime = RuntimeConfig {
        tick_period: TICK_PERIOD,
        allow_join: false,
        allow_leave: false,
        ..RuntimeConfig::default()
    };
    let raft = fast_raft_config_with_seed(3);
    let (_service_a, handle_a) = spawn_raft_group(
        NodeId(1),
        &ids,
        0,
        raft.clone(),
        &runtime,
        KvMachine::default(),
        Arc::new(net.clone()),
        Duration::from_secs(2),
        None,
    )
    .expect("spawn source group");

    wait_for_node_leader(&handle_a).await;
    let resp = handle_a
        .propose(KvCommand::Set {
            key: "migrated".into(),
            value: "yes".into(),
        })
        .await
        .expect("propose");
    assert_eq!(resp, KvResponse::Set { previous: None });

    let bundle = handle_a.export_migration().await.expect("export bundle");
    handle_a.shutdown();

    let (_service_b, handle_b) = spawn_raft_group_from_bundle::<KvMachine>(
        NodeId(1),
        &ids,
        0,
        raft,
        &runtime,
        Arc::new(net),
        Duration::from_secs(2),
        None,
        &bundle,
    )
    .expect("spawn from bundle");

    wait_for_node_leader(&handle_b).await;
    let got = handle_b
        .query(KvQuery::Get {
            key: "migrated".into(),
        })
        .await
        .expect("query restored state");
    assert_eq!(got, KvResponse::Value(Some("yes".into())));
}
