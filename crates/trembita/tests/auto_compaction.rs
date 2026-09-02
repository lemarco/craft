//! Automatic Raft log compaction via [`TrembitaClusterBuilder::auto_compaction`].

use trembita::cluster::TrembitaCluster;
use trembita::core::CompactionPolicy;
use trembita::net::LocalNetwork;
use trembita::proto::NodeId;
use trembita::storage::{GroupRedbLayout, LogStore, SnapshotStore};
use trembita_test_support::{
    KvCommand, KvMachine, TICK_PERIOD, eventually_async_default, fast_raft_config_with_seed,
    wait_for_trembita_leader, wait_for_trembita_stopped,
};

#[tokio::test(start_paused = true)]
async fn auto_compaction_persists_snapshot_under_data_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let net = LocalNetwork::new();
    let node_id = NodeId(1);

    let cluster = TrembitaCluster::builder(node_id, KvMachine::default())
        .members([node_id])
        .raft_config(fast_raft_config_with_seed(4))
        .tick_period(TICK_PERIOD)
        .data_dir(&data_dir)
        .auto_compaction(CompactionPolicy::entries(3))
        .start_local(&net)
        .await;

    wait_for_trembita_leader(&cluster).await;

    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
        cluster
            .handle()
            .propose(KvCommand::Set {
                key: key.into(),
                value: value.into(),
            })
            .await
            .expect("write");
    }

    eventually_async_default("auto-compaction persisted snapshot", || async {
        !cluster.handle().compact().await.expect("compact probe")
    })
    .await;

    wait_for_trembita_stopped(&cluster).await;

    let store = GroupRedbLayout::new(&data_dir)
        .open_group(0)
        .expect("open redb");
    assert!(
        store.load_snapshot().expect("load snapshot").is_some(),
        "auto-compact must persist a snapshot"
    );
    assert!(
        store.first_index().expect("first index").0 > 1,
        "compacted prefix must be purged from storage"
    );
}
