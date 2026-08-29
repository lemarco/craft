//! Job queue wire integration: replication, failover, autoscale.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crafty::AutoscalePolicy;
use crafty::CraftyCluster;
use crafty::EnqueueOptions;
use crafty::JobQueue;
use crafty::MembershipAutoscalePolicy;
use crafty::RedbJobQueue;
use crafty::actor::{ConfigCodecError, UserActor, WorkerId};
use crafty::net::LocalNetwork;
use crafty::net::send_join_request;
use crafty::proto::{self, NodeId};
use crafty::proto::{JoinRequest, JoinResponse, PROTOCOL_VERSION};
use crafty_test_support::{
    KvMachine, TICK_PERIOD, advance, assert_eq, await_crafty_leader, eventually_async_default,
    eventually_default, fast_raft_config_with_seed, wait_for_crafty_stopped,
};
use std::sync::atomic::{AtomicBool, Ordering};

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

    fn handle(
        &mut self,
        _msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }
}

async fn spawn_queue_cluster_n(
    dir: &tempfile::TempDir,
    node_ids: &[NodeId],
    autoscale: bool,
) -> (LocalNetwork, Vec<Arc<CraftyCluster<KvMachine>>>) {
    let net = LocalNetwork::new();
    let policy = AutoscalePolicy {
        worker_group: "w".into(),
        target_pending_per_worker: 1,
        min_workers: 1,
        max_workers: node_ids.len(),
        cooldown: Duration::from_millis(30),
        poll_interval: Duration::from_millis(10),
    };
    let mut clusters = Vec::new();
    for &id in node_ids {
        let data_dir = dir.path().join(format!("node-{}", id.0));
        let queue_path = data_dir.join("queue-jobs.redb");
        let mut builder = CraftyCluster::builder(id, KvMachine::default())
            .members(node_ids.to_vec())
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(10))
            .directory_publish_period(Duration::from_millis(10))
            .data_dir(&data_dir)
            .job_queue_at("jobs", queue_path, Duration::from_secs(60))
            .manage::<Worker>("w", 1, 0);
        if autoscale {
            builder = builder.job_queue_autoscale::<Worker>("jobs", &policy, 0);
        }
        clusters.push(Arc::new(builder.start_local(&net).await));
    }
    (net, clusters)
}

async fn spawn_queue_cluster(
    dir: &tempfile::TempDir,
    autoscale: bool,
) -> (LocalNetwork, Vec<Arc<CraftyCluster<KvMachine>>>) {
    spawn_queue_cluster_n(dir, &[NodeId(1), NodeId(2), NodeId(3)], autoscale).await
}

async fn spawn_sharded_queue_cluster(
    dir: &tempfile::TempDir,
    shards: usize,
) -> (LocalNetwork, Vec<Arc<CraftyCluster<KvMachine>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let data_dir = dir.path().join(format!("node-{}", id.0));
        let builder = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(10))
            .directory_publish_period(Duration::from_millis(10))
            .data_dir(&data_dir)
            .job_queue_sharded("jobs", shards, Duration::from_secs(60))
            .manage::<Worker>("w", 1, 0);
        clusters.push(Arc::new(builder.start_local(&net).await));
    }
    (net, clusters)
}

fn queue_path(dir: &tempfile::TempDir, id: NodeId) -> std::path::PathBuf {
    dir.path()
        .join(format!("node-{}", id.0))
        .join("queue-jobs.redb")
}

async fn local_pending(path: &Path) -> u64 {
    RedbJobQueue::open(path, Duration::from_secs(60))
        .expect("open local queue")
        .metrics()
        .await
        .expect("metrics")
        .pending
}

fn shutdown_queue_cluster(net: &LocalNetwork, clusters: Vec<Arc<CraftyCluster<KvMachine>>>) {
    for c in &clusters {
        c.shutdown();
    }
    drop(clusters);
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let _ = net.detach(id);
    }
}

#[tokio::test(start_paused = true)]
async fn batch_enqueue_and_ack_through_cluster_client() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let ids = leader
        .enqueue_batch("jobs", &[b"j1", b"j2", b"j3"])
        .await
        .expect("batch enqueue");
    assert_eq!(ids.len(), 3);

    let worker = WorkerId {
        node: leader.node_id(),
        instance: 1,
    };
    let queue = leader.job_queue("jobs").expect("queue");
    let leased = queue.lease(worker, 8).await.expect("lease");
    assert_eq!(leased.len(), 3);
    let lease_ids: Vec<_> = leased.iter().map(|j| j.lease_id).collect();
    queue
        .ack_batch(worker, &lease_ids)
        .await
        .expect("ack batch");

    let metrics = queue.metrics().await.expect("metrics");
    assert_eq!(metrics.pending, 0);
    assert_eq!(metrics.leased, 0);

    drop(queue);
    drop(leader);
    shutdown_queue_cluster(&net, clusters);
}

#[tokio::test(start_paused = true)]
async fn prefetch_does_not_resurrect_jobs_after_batch_ack() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    leader
        .enqueue_batch("jobs", &[b"a", b"b"])
        .await
        .expect("batch enqueue");

    let worker = WorkerId {
        node: leader.node_id(),
        instance: 1,
    };
    let queue = leader.job_queue("jobs").expect("queue");
    let leased = queue.lease(worker, 4).await.expect("lease");
    assert_eq!(leased.len(), 2);

    let lease_ids: Vec<_> = leased.iter().map(|j| j.lease_id).collect();
    queue
        .ack_batch(worker, &lease_ids)
        .await
        .expect("ack batch");

    let again = queue.lease(worker, 4).await.expect("lease again");
    assert!(again.is_empty());
    assert_eq!(queue.metrics().await.expect("metrics").pending, 0);
}

#[tokio::test(start_paused = true)]
async fn enqueue_replicates_to_every_voter_redb() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    for i in 0..4u64 {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
    }
    drop(leader);
    drop(queue);

    shutdown_queue_cluster(&net, clusters);

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        assert_eq!(local_pending(&queue_path(&dir, id)).await, 4);
    }
}

#[tokio::test(start_paused = true)]
async fn follower_local_redb_holds_replicated_backlog_for_failover() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    for i in 0..5u64 {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
    }

    let follower_id = clusters
        .iter()
        .find(|c| c.node_id() != leader.node_id())
        .map(|c| c.node_id())
        .expect("follower");
    drop(leader);
    drop(queue);

    shutdown_queue_cluster(&net, clusters);

    let local =
        RedbJobQueue::open(queue_path(&dir, follower_id), Duration::from_secs(60)).expect("open");
    assert_eq!(local.metrics().await.expect("metrics").pending, 5);

    let worker = WorkerId {
        node: follower_id,
        instance: 1,
    };
    let leased = local.lease(worker, 5).await.expect("lease");
    assert_eq!(leased.len(), 5);
}

#[tokio::test(start_paused = true)]
async fn follower_enqueue_lease_ack_through_leader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_queue_cluster(&dir, false).await;

    let _leader = await_crafty_leader(&clusters).await;
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

    let leader = await_crafty_leader(&clusters).await;
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

#[tokio::test(start_paused = true)]
async fn sharded_queue_enqueues_and_leases_across_shards() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_sharded_queue_cluster(&dir, 4).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    for i in 0..8u64 {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
    }

    let worker = WorkerId {
        node: leader.node_id(),
        instance: 1,
    };
    let leased = queue.lease(worker, 8).await.expect("lease");
    assert_eq!(leased.len(), 8);
    assert_eq!(queue.metrics().await.expect("metrics").pending, 0);
}

#[tokio::test(start_paused = true)]
async fn priority_enqueue_through_wire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    queue.enqueue(b"low").await.expect("enqueue");
    queue
        .enqueue_opts(b"high", EnqueueOptions::priority(9))
        .await
        .expect("enqueue");

    let worker = WorkerId {
        node: leader.node_id(),
        instance: 1,
    };
    let first = queue.lease(worker, 1).await.expect("lease");
    assert_eq!(first[0].payload, b"high");
}

#[tokio::test(start_paused = true)]
async fn backlog_survives_live_leader_failover() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ids = [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
    let (net, clusters) = spawn_queue_cluster_n(&dir, &ids, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(300)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    for i in 0..5u64 {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
    }

    let old_leader_id = leader.node_id();
    wait_for_crafty_stopped(leader.as_ref()).await;
    drop(leader);
    let _ = net.detach(old_leader_id);

    let survivors: Vec<_> = clusters
        .into_iter()
        .filter(|c| c.node_id() != old_leader_id)
        .collect();

    let new_leader = await_crafty_leader(&survivors).await;
    advance(Duration::from_millis(300)).await;

    let queue = new_leader.job_queue("jobs").expect("queue");
    assert_eq!(queue.metrics().await.expect("metrics").pending, 5);

    let worker = WorkerId {
        node: new_leader.node_id(),
        instance: 1,
    };
    let leased = queue.lease(worker, 5).await.expect("lease");
    assert_eq!(leased.len(), 5);
}

#[tokio::test(start_paused = true)]
async fn enqueue_dedup_key_is_idempotent_over_wire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    let id1 = queue
        .enqueue_opts(b"v1", EnqueueOptions::dedup_key("payment-42"))
        .await
        .expect("enqueue");
    let id2 = queue
        .enqueue_opts(b"v2", EnqueueOptions::dedup_key("payment-42"))
        .await
        .expect("retry");
    assert_eq!(id1, id2);
    assert_eq!(queue.metrics().await.expect("metrics").pending, 1);
}

#[tokio::test(start_paused = true)]
async fn membership_autoscale_invokes_join_hook() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let join_hook_called = Arc::new(AtomicBool::new(false));
    let joiner: Arc<Mutex<Option<Arc<CraftyCluster<KvMachine>>>>> = Arc::new(Mutex::new(None));

    let policy = MembershipAutoscalePolicy {
        pending_per_node_threshold: 2,
        max_nodes: 4,
        cooldown: Duration::from_millis(20),
        poll_interval: Duration::from_millis(10),
    };

    let mut clusters = Vec::new();
    for &id in &ids {
        let net = net.clone();
        let dir = dir.path().to_path_buf();
        let join_hook_called = Arc::clone(&join_hook_called);
        let joiner_store = Arc::clone(&joiner);
        let net_for_hook = net.clone();
        let dir_for_hook = dir.clone();
        let join_hook = move || {
            let net = net_for_hook.clone();
            let dir = dir_for_hook.clone();
            let join_hook_called = Arc::clone(&join_hook_called);
            let joiner_store = Arc::clone(&joiner_store);
            Box::pin(async move {
                if join_hook_called.swap(true, Ordering::SeqCst) {
                    return Ok(());
                }
                let joiner_id = NodeId(4);
                let data_dir = dir.join(format!("node-{}", joiner_id.0));
                std::fs::create_dir_all(&data_dir).expect("datadir");
                let cluster = Arc::new(
                    CraftyCluster::builder(joiner_id, KvMachine::default())
                        .members(ids)
                        .raft_config(fast_raft_config_with_seed(11))
                        .tick_period(TICK_PERIOD)
                        .reconcile_period(Duration::from_millis(10))
                        .directory_publish_period(Duration::from_millis(10))
                        .allow_join(true)
                        .data_dir(&data_dir)
                        .job_queue_at(
                            "jobs",
                            data_dir.join("queue-jobs.redb"),
                            Duration::from_secs(60),
                        )
                        .start_local(&net)
                        .await,
                );
                *joiner_store.lock().expect("poisoned") = Some(Arc::clone(&cluster));
                let response = send_join_request(
                    &net,
                    NodeId(1),
                    &JoinRequest {
                        protocol_version: PROTOCOL_VERSION,
                        node_id: joiner_id,
                        advertise_addr: "node4.local:7443".to_string(),
                    },
                )
                .await
                .expect("join rpc");
                assert!(
                    matches!(response, JoinResponse::Accepted { .. }),
                    "join rejected: {response:?}"
                );
                Ok(())
            })
                as crafty::actor::BoxFuture<'static, Result<(), crafty::actor::ClusterScaleError>>
        };
        let data_dir = dir.join(format!("node-{}", id.0));
        let queue_path = data_dir.join("queue-jobs.redb");
        let builder = CraftyCluster::builder(id, KvMachine::default())
            .members(ids)
            .raft_config(fast_raft_config_with_seed(11))
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(10))
            .directory_publish_period(Duration::from_millis(10))
            .allow_join(true)
            .data_dir(&data_dir)
            .job_queue_at("jobs", queue_path, Duration::from_secs(60))
            .manage::<Worker>("w", 1, 0)
            .job_queue_membership_autoscale("jobs", &policy, join_hook);
        clusters.push(Arc::new(builder.start_local(&net).await));
    }

    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(200)).await;

    let queue = leader.job_queue("jobs").expect("queue");
    for i in 0..12u64 {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
    }

    eventually_async_default("membership autoscale join hook", || {
        let join_hook_called = join_hook_called.load(Ordering::SeqCst);
        async move { join_hook_called }
    })
    .await;

    assert!(joiner.lock().expect("poisoned").is_some());
    assert!(net.is_reachable(NodeId(4)));
}

#[tokio::test(start_paused = true)]
async fn queue_replicate_rejects_non_leader_caller() {
    use crafty::net::{LocalTransport, send_queue_replicate};
    use crafty_proto::{QueueReplicateOp, QueueReplicateRequest};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let (net, clusters) = spawn_queue_cluster(&dir, false).await;
    let _leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;

    let follower = Arc::new(LocalTransport::new(net.clone(), NodeId(2)));
    let reply = send_queue_replicate(
        follower.as_ref(),
        NodeId(3),
        &QueueReplicateRequest {
            stream: "jobs".into(),
            ops: vec![QueueReplicateOp::Enqueue {
                job_id: 99,
                payload: b"x".to_vec(),
                enqueued_at_ms: 1,
                next_job_id: 100,
                priority: 0,
                not_before_ms: 1,
                dedup_key: None,
                attempts: 0,
                max_attempts: 0,
            }],
        },
    )
    .await
    .expect("wire round trip");
    let err = reply.error.expect("replicate should fail");
    assert!(err.contains("not raft leader"), "unexpected error: {err}");
}

/// Matches `crafty-actor::redb_queue::COMPACT_EVERY_ACKS`.
const COMPACT_EVERY_ACKS: u64 = 64;

#[tokio::test(start_paused = true)]
async fn redb_queue_compacts_after_many_acks_through_wire() {
    let dir = tempfile::tempdir().unwrap();
    let (_net, clusters) = spawn_queue_cluster(&dir, false).await;
    let leader = await_crafty_leader(&clusters).await;
    advance(Duration::from_millis(50)).await;

    let queue = leader.job_queue("jobs").expect("queue client");
    let worker = WorkerId {
        node: leader.node_id(),
        instance: 1,
    };

    for i in 0..COMPACT_EVERY_ACKS {
        queue
            .enqueue(format!("job-{i}").as_bytes())
            .await
            .expect("enqueue");
        let leased = queue.lease(worker, 1).await.expect("lease");
        queue.ack(worker, leased[0].lease_id).await.expect("ack");
    }

    let metrics = queue.metrics().await.expect("metrics");
    assert_eq!(metrics.pending, 0);
    assert_eq!(metrics.leased, 0);
}
