//! # Background jobs showcase (messaging **tier C**)
//!
//! Demonstrates the Sidekiq-style pipeline (see `docs/scenarios/background-jobs.md`):
//!
//! ```text
//!  Client                Gateway (any node)              Queue (Raft leader)           Workers
//!    |  POST /jobs/emails        |                              |                         |
//!    | ------------------------> |  enqueue (forward if follower)|                         |
//!    | <---------------- 202 ----|                              |                         |
//!    |                           |                              |  lease / ack            |
//!    |                           |                              | ----------------------> |
//! ```
//!
//! ## Two run modes
//!
//! | Mode | How | Use when |
//! |------|-----|----------|
//! | **Local** | `cargo run --release` | Fast iteration, single laptop |
//! | **Cluster** | `./cluster.sh 1\|2\|3` | Real multi-VPS shape, QUIC/mTLS |
//!
//! Cluster: set `CRAFTY_PEERS` via [`cluster.sh`](../../background-jobs/cluster.sh).
//! Every node runs gateway + `#[consumer]` — same binary, homogeneous cluster.
//!
//! ## Debug logs
//!
//! `RUST_LOG=showcase=debug` — lines use `target: "showcase"`.

mod debug;

use std::env;
use std::sync::Arc;
use std::time::Duration;

use crafty::net::LocalNetwork;
use crafty::{
    ConsumerOpts, CraftyApp, CraftyAppBuilder, NodeId, ReadyOpts, app_config_from_env, consumer,
};
use crafty_showcase_common::{cluster_mode, data_dir, display_addr};

/// Global counter only for demo logging — not part of crafty API.
static HANDLED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Job stream name — must match `#[consumer("…")]` and gateway path `/jobs/{stream}`.
const STREAM: &str = "emails";
const DATA_DIR_NAME: &str = "crafty-showcase-background-jobs";

#[consumer("emails")]
#[allow(clippy::unused_async)]
async fn send_email(payload: &[u8]) -> Result<(), ()> {
    let n = HANDLED.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    let node = env::var("CRAFTY_NODE_ID").unwrap_or_else(|_| "?".into());
    let preview = String::from_utf8_lossy(payload);
    debug::worker_job(0, payload.len(), preview.trim());
    println!("[worker node {node}] email #{n} — {preview}");
    Ok(())
}

fn worker_count() -> u32 {
    env::var("CRAFTY_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1)
}

async fn wait_ready(app: &CraftyApp) {
    let ready = app
        .wait_until_ready(ReadyOpts::default().with_queue(STREAM))
        .await;
    if ready {
        debug::cluster_ready();
    } else {
        tracing::warn!(target: "showcase", showcase = debug::NAME, "cluster not ready after 60s");
        eprintln!("warn: no leader / queue yet — start nodes 2+3, then retry (enqueue may 503)");
    }
}

async fn start_local() -> Result<Arc<CraftyApp>, Box<dyn std::error::Error>> {
    let data_dir = data_dir(DATA_DIR_NAME);
    std::fs::create_dir_all(&data_dir)?;
    let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8090".into())
        .parse()?;
    let net = LocalNetwork::new();
    Ok(
        CraftyApp::builder(NodeId(1))
            .data_dir(&data_dir)
            .job_stream(STREAM, Duration::from_secs(300))
            .members([NodeId(1)])
            .tick_period(Duration::from_millis(10))
            .admin_addr("127.0.0.1:9080".parse()?)
            .gateway_addr(gateway)
            .gateway_jobs_api(true)
            .gateway_actors_api(false)
            .start_local_shared(&net)
            .await,
    )
}

fn spawn_consumers(
    app: &Arc<CraftyApp>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let workers = worker_count();
    (0..workers)
        .map(|instance| {
            app.spawn_consumer(
                SendEmailConsumer,
                ConsumerOpts {
                    instance,
                    batch: 4,
                    idle_sleep: Duration::from_millis(50),
                    ..ConsumerOpts::default()
                },
                stop_rx.clone(),
            )
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();

    let cluster = cluster_mode();
    let app = if cluster {
        let cfg = app_config_from_env().map_err(|e| format!("config: {e}"))?;
        let app = CraftyAppBuilder::from_config(&cfg)
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .start_quic_shared(cfg.security, cfg.listen, cfg.peers)
            .await
            .map_err(|e| format!("start: {e}"))?;
        wait_ready(&app).await;
        app
    } else {
        start_local().await?
    };

    let node_id = app.cluster().node_id().0;
    debug::startup(if cluster { "quic" } else { "local" }, node_id, &data_dir(DATA_DIR_NAME));

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer_handles = spawn_consumers(&app, stop_rx);

    print_banner(cluster, node_id);

    tokio::signal::ctrl_c().await?;
    debug::shutdown(consumer_handles.len());
    stop_tx.send(true)?;
    for handle in consumer_handles {
        let _ = handle.await;
    }
    app.cluster().shutdown();
    Ok(())
}

fn print_banner(cluster: bool, node_id: u64) {
    println!("crafty showcase · background jobs (tier C)");
    if cluster {
        println!("  mode     QUIC cluster (node {node_id})");
        println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_default());
        if env::var("CRAFTY_GATEWAY").is_ok_and(|g| g != "-") {
            let gw = env::var("CRAFTY_GATEWAY").unwrap_or_default();
            println!("  gateway  http://{}/jobs/{STREAM}", display_addr(&gw));
        }
        if let Ok(admin) = env::var("CRAFTY_ADMIN") {
            if admin != "-" {
                println!("  admin    http://{}/dashboard", display_addr(&admin));
            }
        }
        println!(
            "  worker   node {node_id} instance 0..{}",
            worker_count().saturating_sub(1)
        );
    } else {
        let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
            .unwrap_or_else(|_| "127.0.0.1:8090".into())
            .parse()
            .expect("gateway");
        println!("  mode     local (single process)");
        println!("  gateway  http://{gateway}/jobs/{STREAM}");
        println!("  admin    http://127.0.0.1:9080/dashboard");
        println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    }
    println!("  trigger  ./trigger.sh <payload>");
    println!("  debug    RUST_LOG=showcase=debug");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
