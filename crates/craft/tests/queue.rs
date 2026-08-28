//! Job queue wire integration: follower enqueue, autoscale from depth.

use std::sync::Arc;
use std::time::Duration;

use craft::AutoscalePolicy;
use craft::CraftCluster;
use craft::actor::{ConfigCodecError, UserActor, WorkerId};
use craft::net::LocalNetwork;
use craft::proto::{self, NodeId};
use craft_test_support::{
    KvMachine, TICK_PERIOD, advance, assert_eq, await_craft_leader, eventually_default,
    fast_raft_config_with_seed,
};

#[derive(Debug)]
struct WorkerErr;
impl std::fmt::Display for WorkerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}
impl std::error::Error for WorkerErr {}

struct Worker;

impl UserActor for Worker {
    type Config = u32;
    type Message = ();
    type Error = WorkerErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }
}

async fn spawn_queue_cluster(
    dir: &tempfile::TempDir,
    autoscale: bool,
) -> (LocalNetwork, Vec<Arc<CraftCluster<KvMachine>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let policy = AutoscalePolicy {
        worker_group: "w".into(),
        target_pending_per_worker: 1,
        min_workers: 1,
        max_workers: 3,
        cooldown: Duration::from_millis(30),
        poll_interval: Duration::from_millis(10),
    };
    let mut clusters = Vec::new();
    for &id in &ids {
        let data_dir = dir.path().join(format!("node-{}", id.0));
        let queue_path = data_dir.join("queue-jobs.redb");
        let mut builder = CraftCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(10))
            .directory_publish_period(Duration::from_millis(10))
            .data_dir(&data_dir)
            .job_queue_at("jobs", queue_path, Duration::from_secs(60))
            .manage::<Worker>("w", 1, 0);
        if autoscale {
            builder = builder.job_queue_autoscale::<Worker>("jobs", policy.clone(), 0);
        }
        clusters.push(Arc::new(builder.start_local(&net).await));
    }
    (net, clusters)
}

#[tokio::test(start_paused = true)]
async fn follower_enqueue_lease_ack_through_leader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_queue_cluster(&dir, false).await;

    let _leader = await_craft_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let mut follower = None;
    for cluster in &clusters {
        if !cluster.is_leader().await {
            follower = Some(Arc::clone(cluster));
            break;
        }
    }
    let follower = follower.expect("follower");
    let queue = follower.job_queue("jobs").expect("queue client");
    let job_id = queue.enqueue(b"hello").await.expect("enqueue");

    let worker = WorkerId {
        node: follower.node_id(),
        instance: 1,
    };
    let jobs = queue.lease(worker, 1).await.expect("lease");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, job_id);
    assert_eq!(jobs[0].payload, b"hello");
    queue.ack(worker, jobs[0].lease_id).await.expect("ack");

    let metrics = queue.metrics().await.expect("metrics");
    assert_eq!(metrics.pending, 0);
    assert_eq!(metrics.leased, 0);
}

#[tokio::test(start_paused = true)]
async fn queue_depth_autoscale_scales_worker_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_queue_cluster(&dir, true).await;

    let leader = await_craft_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    for i in 0..6u64 {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
    }

    let directory = leader.directory().clone();
    eventually_default("autoscale workers to node count", move || {
        directory.lookup("w").len() >= 3
    })
    .await;

    for c in &clusters {
        c.shutdown();
    }
}
