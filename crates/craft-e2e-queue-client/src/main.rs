//! QUIC job-queue E2E client (`e2e/queue.sh`).
//!
//! Phases (via `CRAFT_E2E_QUEUE_PHASE`):
//! - `before_failover` — enqueue five jobs, lease/ack two on a follower worker id
//! - `after_failover` — assert three pending remain, drain and ack the rest

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use craft_net::load_pem_material;
use craft_net::{
    CertPaths, PeerDirectory, QuicTransport, client_config, client_endpoint, send_queue_ack,
    send_queue_enqueue, send_queue_lease, send_queue_metrics,
};
use craft_proto::{
    NodeId, QueueAckRequest, QueueEnqueueRequest, QueueLeaseRequest, QueueMetricsRequest,
};

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
        return Err("CRAFT_PEERS is empty".into());
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

fn queue_stream() -> String {
    env::var("CRAFT_JOB_QUEUE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "jobs".into())
}

async fn metrics_pending(
    transport: &QuicTransport,
    peer: NodeId,
    stream: &str,
) -> Result<u64, String> {
    let reply = send_queue_metrics(
        transport,
        peer,
        &QueueMetricsRequest {
            stream: stream.into(),
        },
    )
    .await
    .map_err(|e| format!("metrics on {peer:?}: {e}"))?;
    if let Some(err) = reply.error {
        return Err(err);
    }
    Ok(reply.pending)
}

async fn wait_queue_ready(
    transport: &QuicTransport,
    peer: NodeId,
    stream: &str,
) -> Result<(), String> {
    for attempt in 0..90 {
        match metrics_pending(transport, peer, stream).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt == 89 => return Err(format!("queue not ready after 90s: {e}")),
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn before_failover(
    transport: Arc<QuicTransport>,
    stream: &str,
    submit: NodeId,
    worker_peer: NodeId,
    worker_node: u64,
    worker_instance: u32,
) -> Result<(), String> {
    wait_queue_ready(transport.as_ref(), submit, stream).await?;

    for i in 0..5u64 {
        let payload = format!("e2e-job-{i}");
        let reply = send_queue_enqueue(
            transport.as_ref(),
            submit,
            &QueueEnqueueRequest {
                stream: stream.into(),
                payload: payload.into_bytes(),
                priority: 0,
                not_before_ms: 0,
                shard_key: None,
                dedup_key: None,
            },
        )
        .await
        .map_err(|e| format!("enqueue: {e}"))?;
        if let Some(err) = reply.error {
            return Err(err);
        }
        reply
            .job_id
            .ok_or_else(|| String::from("enqueue returned no job_id"))?;
    }

    let lease = send_queue_lease(
        transport.as_ref(),
        worker_peer,
        &QueueLeaseRequest {
            stream: stream.into(),
            worker_node,
            worker_instance,
            max: 2,
        },
    )
    .await
    .map_err(|e| format!("lease: {e}"))?;
    if let Some(err) = lease.error {
        return Err(err);
    }
    if lease.jobs.len() != 2 {
        return Err(format!(
            "expected to lease 2 jobs on follower, got {}",
            lease.jobs.len()
        ));
    }

    for job in lease.jobs {
        let reply = send_queue_ack(
            transport.as_ref(),
            worker_peer,
            &QueueAckRequest {
                stream: stream.into(),
                worker_node,
                worker_instance,
                lease_id: job.lease_id,
            },
        )
        .await
        .map_err(|e| format!("ack: {e}"))?;
        if let Some(err) = reply.error {
            return Err(err);
        }
    }

    let pending = metrics_pending(transport.as_ref(), submit, stream).await?;
    if pending != 3 {
        return Err(format!("expected pending=3 before failover, got {pending}"));
    }
    println!("QUEUE BEFORE FAILOVER OK (pending={pending})");
    Ok(())
}

async fn after_failover(
    transport: Arc<QuicTransport>,
    stream: &str,
    contact: NodeId,
    worker_node: u64,
    worker_instance: u32,
) -> Result<(), String> {
    wait_queue_ready(transport.as_ref(), contact, stream).await?;

    let pending = metrics_pending(transport.as_ref(), contact, stream).await?;
    if pending != 3 {
        return Err(format!("expected pending=3 after failover, got {pending}"));
    }

    let lease = send_queue_lease(
        transport.as_ref(),
        contact,
        &QueueLeaseRequest {
            stream: stream.into(),
            worker_node,
            worker_instance,
            max: 5,
        },
    )
    .await
    .map_err(|e| format!("lease after failover: {e}"))?;
    if let Some(err) = lease.error {
        return Err(err);
    }
    if lease.jobs.len() != 3 {
        return Err(format!(
            "expected 3 jobs after failover, leased {}",
            lease.jobs.len()
        ));
    }

    for job in lease.jobs {
        let reply = send_queue_ack(
            transport.as_ref(),
            contact,
            &QueueAckRequest {
                stream: stream.into(),
                worker_node,
                worker_instance,
                lease_id: job.lease_id,
            },
        )
        .await
        .map_err(|e| format!("ack after failover: {e}"))?;
        if let Some(err) = reply.error {
            return Err(err);
        }
    }

    let pending = metrics_pending(transport.as_ref(), contact, stream).await?;
    if pending != 0 {
        return Err(format!("expected pending=0 after drain, got {pending}"));
    }
    println!("QUEUE AFTER FAILOVER OK (pending=0)");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = node_from_env("CRAFT_NODE_ID")?;
    let peers = parse_peers(&env("CRAFT_PEERS")?)?;
    let paths = CertPaths::new(
        env("CRAFT_NODE_CERT")?,
        env("CRAFT_NODE_KEY")?,
        env("CRAFT_CA_CERT")?,
    );
    let phase = env("CRAFT_E2E_QUEUE_PHASE")?;
    let stream = queue_stream();

    let submit = node_from_env("CRAFT_QUEUE_SUBMIT_NODE").unwrap_or(NodeId(1));
    let worker_peer = node_from_env("CRAFT_QUEUE_WORKER_PEER").unwrap_or(NodeId(2));
    let worker_node = env("CRAFT_QUEUE_WORKER_NODE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(worker_peer.0);
    let worker_instance = env("CRAFT_QUEUE_WORKER_INSTANCE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let material = load_pem_material(node_id, &paths)?;
    let client_cfg = client_config(&material.identity, material.roots)?;
    let endpoint = client_endpoint("0.0.0.0:0".parse()?)?;
    let transport = Arc::new(QuicTransport::new(endpoint, client_cfg, peers));

    match phase.as_str() {
        "before_failover" => {
            before_failover(
                transport,
                &stream,
                submit,
                worker_peer,
                worker_node,
                worker_instance,
            )
            .await?;
        }
        "after_failover" => {
            let contact = node_from_env("CRAFT_QUEUE_CONTACT_NODE").unwrap_or(submit);
            after_failover(transport, &stream, contact, worker_node, worker_instance).await?;
        }
        other => return Err(format!("unknown CRAFT_E2E_QUEUE_PHASE {other:?}").into()),
    }

    Ok(())
}
