//! Interactive demo for [`scripts/dev-3node.sh`](../../../scripts/dev-3node.sh):
//!
//! * **fast** (`demo`) — smoke Raft + job queue in a few seconds.
//! * **watch** — ~2+ minute staged scenario for the live dashboard.
//!
//! Run: `./scripts/dev-3node.sh demo` or `./scripts/dev-3node.sh watch`.
#![allow(missing_docs)] // publish = false — local dev binary

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use trembita_client::{Client, RemoteClient, RetryPolicy};
use trembita_net::load_pem_material;
use trembita_net::{
    CertPaths, PeerDirectory, QuicTransport, Transport, client_config, client_endpoint,
    send_queue_ack, send_queue_enqueue, send_queue_lease, send_queue_metrics,
};
use trembita_proto::{
    NodeId, QueueAckRequest, QueueEnqueueRequest, QueueLeaseRequest, QueueLeasedJobWire,
    QueueMetricsReply, QueueMetricsRequest, decode, encode,
};

struct DemoConfig {
    propose_via: NodeId,
    query_via: NodeId,
    worker_peer: NodeId,
    worker_node: u64,
    worker_instance: u32,
    stream: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueueSnapshot {
    pending: u64,
    leased: u64,
}

fn env(key: &str) -> Result<String, String> {
    env::var(key).map_err(|_| format!("missing env {key}"))
}

fn parse_peers(raw: &str) -> Result<PeerDirectory, String> {
    let mut map = PeerDirectory::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| format!("bad peer entry {entry:?} (want id@host:port)"))?;
        let id = id
            .parse::<u64>()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e| format!("bad addr in {entry:?}: {e}"))?;
        map.insert(NodeId(id), addr);
    }
    if map.is_empty() {
        return Err("TREMBITA_PEERS is empty".into());
    }
    Ok(map)
}

fn node_from_env(key: &str) -> Result<NodeId, String> {
    Ok(NodeId(
        env(key)?
            .parse()
            .map_err(|_| format!("{key} must be u64"))?,
    ))
}

fn demo_config() -> DemoConfig {
    let worker_peer = node_from_env("TREMBITA_DEMO_WORKER_PEER").unwrap_or(NodeId(2));
    DemoConfig {
        propose_via: node_from_env("TREMBITA_DEMO_PROPOSE_NODE").unwrap_or(NodeId(1)),
        query_via: node_from_env("TREMBITA_DEMO_QUERY_NODE").unwrap_or(NodeId(3)),
        worker_peer,
        worker_node: env("TREMBITA_DEMO_WORKER_NODE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(worker_peer.0),
        worker_instance: env("TREMBITA_DEMO_WORKER_INSTANCE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1),
        stream: env::var("TREMBITA_JOB_QUEUE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "jobs".into()),
    }
}

fn decode_u64(bytes: &[u8]) -> Result<u64, String> {
    decode(bytes).map_err(|e| format!("decode response: {e}"))
}

fn demo_command() -> Result<Vec<u8>, String> {
    encode(&Vec::<u8>::new()).map_err(|e| format!("encode command: {e}"))
}

fn demo_query() -> Result<Vec<u8>, String> {
    encode(&()).map_err(|e| format!("encode query: {e}"))
}

fn watch_pause_secs() -> u64 {
    env::var("TREMBITA_DEV_WATCH_PAUSE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

async fn pause(label: &str, secs: u64) {
    println!();
    println!("⏸  {label} ({secs}s — watch the dashboard)");
    tokio::time::sleep(Duration::from_secs(secs)).await;
}

async fn wait_cluster_ready(client: &RemoteClient) -> Result<(), String> {
    let q = demo_query()?;
    for attempt in 0..90 {
        if client.query(q.clone()).await.is_ok() {
            return Ok(());
        }
        if attempt == 89 {
            return Err("cluster did not become queryable within 90s".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn wait_queue_ready(
    transport: &dyn Transport,
    peer: NodeId,
    stream: &str,
) -> Result<(), String> {
    for attempt in 0..90 {
        let reply = send_queue_metrics(
            transport,
            peer,
            &QueueMetricsRequest {
                stream: stream.into(),
            },
        )
        .await;
        if reply.is_ok() {
            return Ok(());
        }
        if attempt == 89 {
            return Err(format!(
                "queue not ready after 90s on {peer:?}: {}",
                reply.unwrap_err()
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn enqueue_via_leader(
    transport: &dyn Transport,
    stream: &str,
    payload: Vec<u8>,
) -> Result<(NodeId, u64), String> {
    let mut last_err = String::from("no cluster peers");
    for peer in [NodeId(1), NodeId(2), NodeId(3)] {
        let reply = send_queue_enqueue(
            transport,
            peer,
            &QueueEnqueueRequest {
                stream: stream.into(),
                payload: payload.clone(),
                priority: 0,
                not_before_ms: 0,
                shard_key: None,
                dedup_key: None,
                max_attempts: 0,
            },
        )
        .await;
        match reply {
            Ok(r) => {
                if let Some(err) = r.error {
                    if err.contains("not raft leader") {
                        last_err = err;
                        continue;
                    }
                    return Err(err);
                }
                let job_id = r
                    .job_id
                    .ok_or_else(|| String::from("enqueue returned no job_id"))?;
                return Ok((peer, job_id));
            }
            Err(e) => last_err = format!("enqueue on {peer:?}: {e}"),
        }
    }
    Err(last_err)
}

async fn queue_metrics(transport: &dyn Transport, stream: &str) -> Result<QueueSnapshot, String> {
    let mut last_err = String::from("no cluster peers");
    for peer in [NodeId(1), NodeId(2), NodeId(3)] {
        match send_queue_metrics(
            transport,
            peer,
            &QueueMetricsRequest {
                stream: stream.into(),
            },
        )
        .await
        {
            Ok(QueueMetricsReply {
                pending,
                leased,
                error: None,
                ..
            }) => {
                return Ok(QueueSnapshot { pending, leased });
            }
            Ok(QueueMetricsReply {
                error: Some(err), ..
            }) => last_err = err,
            Err(e) => last_err = format!("metrics on {peer:?}: {e}"),
        }
    }
    Err(last_err)
}

async fn print_queue(transport: &dyn Transport, stream: &str) -> Result<(), String> {
    let m = queue_metrics(transport, stream).await?;
    println!(
        "  queue `{stream}` → pending={}, leased={}",
        m.pending, m.leased
    );
    Ok(())
}

async fn lease_jobs(
    transport: &dyn Transport,
    cfg: &DemoConfig,
    max: usize,
) -> Result<Vec<QueueLeasedJobWire>, String> {
    let lease = send_queue_lease(
        transport,
        cfg.worker_peer,
        &QueueLeaseRequest {
            stream: cfg.stream.clone(),
            worker_node: cfg.worker_node,
            worker_instance: cfg.worker_instance,
            max,
        },
    )
    .await
    .map_err(|e| format!("lease: {e}"))?;
    if let Some(err) = lease.error {
        return Err(err);
    }
    Ok(lease.jobs)
}

async fn ack_job(transport: &dyn Transport, cfg: &DemoConfig, lease_id: u64) -> Result<(), String> {
    let reply = send_queue_ack(
        transport,
        cfg.worker_peer,
        &QueueAckRequest {
            stream: cfg.stream.clone(),
            worker_node: cfg.worker_node,
            worker_instance: cfg.worker_instance,
            lease_id,
        },
    )
    .await
    .map_err(|e| format!("ack: {e}"))?;
    if let Some(err) = reply.error {
        return Err(err);
    }
    Ok(())
}

async fn raft_demo(
    transport: Arc<dyn Transport>,
    cfg: &DemoConfig,
    count: u32,
) -> Result<(), String> {
    println!("--- Raft (Demo state machine) ---");
    let propose_client =
        RemoteClient::new(Arc::clone(&transport), [cfg.propose_via]).with_retry(RetryPolicy {
            max_attempts: 8,
            attempt_timeout: Duration::from_secs(10),
            backoff: Duration::from_millis(200),
        });
    let query_client =
        RemoteClient::new(Arc::clone(&transport), [cfg.query_via]).with_retry(RetryPolicy {
            max_attempts: 8,
            attempt_timeout: Duration::from_secs(10),
            backoff: Duration::from_millis(200),
        });

    wait_cluster_ready(&propose_client).await?;

    let cmd = demo_command()?;
    for i in 1..=count {
        propose_client
            .propose(cmd.clone())
            .await
            .map_err(|e| format!("propose #{i} via {:?}: {e}", cfg.propose_via))?;
        println!(
            "  propose #{i} via node {} OK — dashboard: commit index ↑",
            cfg.propose_via.0
        );
    }

    let counter = decode_u64(
        &query_client
            .query(demo_query()?)
            .await
            .map_err(|e| format!("query via {:?}: {e}", cfg.query_via))?,
    )?;
    println!(
        "  linearizable read via node {} → counter={counter}",
        cfg.query_via.0
    );
    Ok(())
}

async fn queue_demo(transport: Arc<dyn Transport>, cfg: &DemoConfig) -> Result<(), String> {
    println!("--- Job queue ({}) ---", cfg.stream);
    wait_queue_ready(transport.as_ref(), NodeId(1), &cfg.stream).await?;

    for i in 0..3u64 {
        let label = format!("demo-job-{i}");
        let (via, job_id) =
            enqueue_via_leader(transport.as_ref(), &cfg.stream, label.clone().into_bytes()).await?;
        println!(
            "  enqueued {label} -> job_id={job_id} (via leader on node {})",
            via.0
        );
    }

    print_queue(transport.as_ref(), &cfg.stream).await?;

    let jobs = lease_jobs(transport.as_ref(), cfg, 3).await?;
    println!(
        "  leased {} job(s) on node {}",
        jobs.len(),
        cfg.worker_peer.0
    );

    for job in jobs {
        ack_job(transport.as_ref(), cfg, job.lease_id).await?;
    }
    println!("  acked all leased jobs");

    print_queue(transport.as_ref(), &cfg.stream).await?;
    let m = queue_metrics(transport.as_ref(), &cfg.stream).await?;
    if m.pending != 0 {
        return Err(format!("expected pending=0 after ack, got {}", m.pending));
    }
    Ok(())
}

async fn watch_demo(transport: Arc<dyn Transport>, cfg: &DemoConfig) -> Result<(), String> {
    let step = watch_pause_secs();
    let long = step + 2;

    println!(
        "=== trembita watch demo (~{}s) ===",
        (5 * step) + (6 * long) + 60
    );
    println!("Open dashboard now: http://127.0.0.1:9080/dashboard");
    println!("Watch: Cluster.commit_index, Job queues pending/leased, Event feed");
    pause("setup — open dashboard in browser", 8).await;

    println!();
    println!("▶ Phase 1 — producer submits work (Raft counter)");
    for i in 1..=5u32 {
        let propose_client = RemoteClient::new(Arc::clone(&transport), [cfg.propose_via])
            .with_retry(RetryPolicy {
                max_attempts: 8,
                attempt_timeout: Duration::from_secs(10),
                backoff: Duration::from_millis(200),
            });
        if i == 1 {
            wait_cluster_ready(&propose_client).await?;
        }
        propose_client
            .propose(demo_command()?)
            .await
            .map_err(|e| format!("propose #{i}: {e}"))?;
        println!("  Raft propose #{i} — Cluster panel: commit index should tick up");
        pause("watch commit index on dashboard", step).await;
    }

    pause("Raft phase done — note leader/term in Cluster panel", long).await;

    println!();
    println!(
        "▶ Phase 2 — enqueue background jobs (stream `{}`)",
        cfg.stream
    );
    wait_queue_ready(transport.as_ref(), NodeId(1), &cfg.stream).await?;

    for i in 1..=6u32 {
        let label = format!("email-batch-{i}");
        let (_, job_id) =
            enqueue_via_leader(transport.as_ref(), &cfg.stream, label.clone().into_bytes()).await?;
        print_queue(transport.as_ref(), &cfg.stream).await?;
        println!("  enqueued {label} (job_id={job_id}) — pending should be ≥ {i}");
        pause("watch Job queues → pending increase", step).await;
    }

    pause("backlog built — pending should be ~6", long).await;

    println!();
    println!(
        "▶ Phase 3 — worker on node {} leases jobs",
        cfg.worker_peer.0
    );
    let batch1 = lease_jobs(transport.as_ref(), cfg, 2).await?;
    print_queue(transport.as_ref(), &cfg.stream).await?;
    println!("  leased {} jobs — pending ↓, leased ↑", batch1.len());
    pause("watch leased=2, pending=4", long).await;

    if let Some(job) = batch1.first() {
        ack_job(transport.as_ref(), cfg, job.lease_id).await?;
        print_queue(transport.as_ref(), &cfg.stream).await?;
        println!("  acked 1 job — leased ↓");
        pause("watch leased drop to 1", step).await;
    }

    let batch2 = lease_jobs(transport.as_ref(), cfg, 2).await?;
    print_queue(transport.as_ref(), &cfg.stream).await?;
    println!("  leased {} more jobs", batch2.len());
    pause("watch leased grow again", step).await;

    let in_flight: Vec<_> = batch1.into_iter().skip(1).chain(batch2).collect();
    let total = in_flight.len();
    for (n, job) in in_flight.iter().enumerate() {
        ack_job(transport.as_ref(), cfg, job.lease_id).await?;
        print_queue(transport.as_ref(), &cfg.stream).await?;
        println!("  ack job {} / {total}", n + 1);
        pause("watch queue drain", step).await;
    }

    let tail = lease_jobs(transport.as_ref(), cfg, 4).await?;
    print_queue(transport.as_ref(), &cfg.stream).await?;
    println!("  final lease: {} jobs", tail.len());
    pause("last jobs in flight — leased > 0", step).await;

    for job in tail {
        ack_job(transport.as_ref(), cfg, job.lease_id).await?;
        print_queue(transport.as_ref(), &cfg.stream).await?;
        pause("draining final jobs", step.min(6)).await;
    }

    let m = queue_metrics(transport.as_ref(), &cfg.stream).await?;
    println!();
    println!(
        "✓ watch demo complete — pending={}, leased={}",
        m.pending, m.leased
    );
    pause("queue empty — Event feed may still show cluster events", 8).await;
    Ok(())
}

async fn run(mode: &str, transport: Arc<dyn Transport>, cfg: &DemoConfig) -> Result<(), String> {
    if mode == "watch" {
        watch_demo(transport, cfg).await
    } else {
        raft_demo(Arc::clone(&transport), cfg, 3).await?;
        queue_demo(transport, cfg).await?;
        println!();
        println!("demo OK — dashboard: pending=0 on stream `{}`", cfg.stream);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = env::args()
        .nth(1)
        .or_else(|| env::var("TREMBITA_DEV_MODE").ok())
        .unwrap_or_else(|| "fast".into());

    let node_id = node_from_env("TREMBITA_NODE_ID")?;
    let peers = parse_peers(&env("TREMBITA_PEERS")?)?;
    let paths = CertPaths::new(
        env("TREMBITA_NODE_CERT")?,
        env("TREMBITA_NODE_KEY")?,
        env("TREMBITA_CA_CERT")?,
    );
    let cfg = demo_config();

    let material = load_pem_material(node_id, &paths)?;
    let client_cfg = client_config(&material.identity, material.roots)?;
    let endpoint = client_endpoint("0.0.0.0:0".parse()?)?;
    let transport: Arc<dyn Transport> = Arc::new(QuicTransport::new(endpoint, client_cfg, peers));

    println!("trembita dev client (node {}, mode={mode})", node_id.0);
    run(&mode, transport, &cfg).await?;
    Ok(())
}
