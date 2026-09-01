//! # Background jobs showcase (durable job queue)
//!
//! Every run mode is a QUIC cluster member: solo `cargo run` is a one-node seed
//! (`CRAFTY_ALLOW_JOIN=1`); `./cluster.sh` adds nodes via dynamic join.

mod debug;

use std::collections::HashMap;
use std::env;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crafty::{CraftyApp, CraftyConfigure, GatewayOpts, JobOpts, RunOpts, consumer};
use crafty_showcase_common::{data_dir, display_addr};

static HANDLED: AtomicUsize = AtomicUsize::new(0);
/// Times the *real* side effect ran. Stays at one per key even under redelivery.
static SENT: AtomicUsize = AtomicUsize::new(0);

/// Idempotency markers, keyed by job key.
///
/// Process-local on purpose — this is a single-binary showcase. In production
/// this is an [`ActorStateStore`] (see `examples/stateful-workers/`) or a shared
/// store (`crates/crafty-store-redis/examples/idempotent_worker.rs`) so the
/// marker survives a crash and is visible to whichever node redelivers.
static MARKERS: Mutex<Option<HashMap<String, Marker>>> = Mutex::new(None);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Marker {
    /// Side effect finished and was durably recorded — safe to ack.
    Done,
}

const STREAM: &str = "emails";
const DATA_DIR_NAME: &str = "crafty-showcase-background-jobs";

/// Business key for the job. Derived from the event, not from the payload bytes.
fn job_key(payload: &[u8]) -> String {
    let text = String::from_utf8_lossy(payload);
    text.trim().to_string()
}

/// `1` (default) makes the first delivery of each key fail *after* the marker is
/// written — the crash-before-ack window that at-least-once always leaves open.
fn simulate_redelivery() -> bool {
    env::var("CRAFTY_SIMULATE_REDELIVERY").as_deref() != Ok("0")
}

/// Effectively-once handler — see `docs/scenarios/background-jobs.md`.
///
/// Order matters: check marker → side effect → mark done → ack. Marking before
/// the ack is what makes the redelivery below safe.
#[consumer("emails")]
#[allow(clippy::unused_async)]
async fn send_email(payload: &[u8]) -> Result<(), ()> {
    let delivery = HANDLED.fetch_add(1, Ordering::SeqCst) + 1;
    let key = job_key(payload);
    debug::worker_job(0, payload.len(), &key);

    // 1. Already finished? This is a redelivery — ack it without repeating work.
    {
        let guard = MARKERS.lock().expect("poisoned");
        if guard
            .as_ref()
            .is_some_and(|m| m.get(&key) == Some(&Marker::Done))
        {
            println!(
                "[worker] delivery #{delivery} — {key}: duplicate, side effect already applied (skipping)"
            );
            return Ok(());
        }
    }

    // 2. The side effect itself.
    let sent = SENT.fetch_add(1, Ordering::SeqCst) + 1;
    println!("[worker] delivery #{delivery} — {key}: sending email (side effects so far: {sent})");

    // 3. Record it durably *before* acking.
    MARKERS
        .lock()
        .expect("poisoned")
        .get_or_insert_with(HashMap::new)
        .insert(key.clone(), Marker::Done);

    // 4. Crash before the ack lands: the queue redelivers, step 1 catches it.
    if simulate_redelivery() && delivery == 1 {
        println!("[worker] delivery #{delivery} — {key}: crashing before ack (expect redelivery)");
        return Err(());
    }

    Ok(())
}

fn worker_count() -> u32 {
    env::var("CRAFTY_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1)
}

fn server_builder() -> crafty::CraftyAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8090".into())
        .parse()
        .expect("gateway");
    CraftyApp::builder()
        .data_dir(dir)
        .jobs([JobOpts::new(STREAM)
            .lease(Duration::from_secs(300))
            .consumer(&SendEmailConsumer)
            .instances(worker_count())
            .batch(4)
            .idle_sleep(Duration::from_millis(50))
            // Applies to HTTP enqueues, which cannot pass per-job options.
            .default_max_attempts(5)
            .http_enqueue(true)])
        .configure(CraftyConfigure {
            tick_period: Duration::from_millis(10),
            reconcile_period: Duration::from_millis(20),
            admin_addr: Some("127.0.0.1:9080".parse().expect("admin")),
            ..CraftyConfigure::default()
        })
        .gateway(GatewayOpts::new(gateway))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    server_builder()
        .run(RunOpts::default().with_wait_queue(STREAM))
        .await?;
    debug::shutdown(worker_count() as usize);
    Ok(())
}

fn print_banner() {
    println!("crafty showcase · background jobs (background jobs)");
    println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
    if env::var("CRAFTY_GATEWAY").is_ok_and(|g| g != "-") {
        let gw = env::var("CRAFTY_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8090".into());
        println!("  gateway  http://{}/jobs/{STREAM}", display_addr(&gw));
    }
    if let Ok(admin) = env::var("CRAFTY_ADMIN") {
        if admin != "-" {
            println!("  admin    http://{}/dashboard", display_addr(&admin));
        }
    }
    if env::var("CRAFTY_JOIN_SEEDS").is_ok() {
        println!("  join     via CRAFTY_JOIN_SEEDS");
    } else {
        println!("  role     seed (CRAFTY_ALLOW_JOIN when unset)");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    println!("  trigger  ./trigger.sh <payload>");
    println!("  dedup    ./trigger-idempotent.sh <payload>");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
