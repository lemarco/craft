//! {{PROJECT_NAME}} — crafty product app (always a QUIC cluster member).

use std::sync::Arc;
use std::time::Duration;

use crafty::{
    CraftyApp, CraftyConfigure, GatewayBearerIdentity, GatewayOpts, IdempotencyOpts, InMemoryStore,
    JobOpts, RunOpts, consumer,
};

const STREAM: &str = "jobs";

#[consumer("jobs")]
async fn handle_job(payload: &[u8]) -> Result<(), String> {
    let preview = String::from_utf8_lossy(payload);
    tracing::info!(target: "app", "job: {preview}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crafty::init_tracing();

    let idem_store = Arc::new(InMemoryStore::new());

    CraftyApp::builder()
        .data_dir("/tmp/{{PROJECT_NAME}}")
        .jobs([JobOpts::new(STREAM)
            .lease(Duration::from_secs(300))
            .default_max_attempts(5)
            .idempotency(IdempotencyOpts::by_dedup_key(
                Arc::clone(&idem_store),
                "job:",
            ))
            .consumer(&HandleJobConsumer)
            .http_enqueue(true)])
        .gateway(
            GatewayOpts::new("127.0.0.1:8090".parse()?)
                .with_jobs_api(true)
                .identity(GatewayBearerIdentity::from_env())
                .protect_product_apis(true),
        )
        .configure(CraftyConfigure {
            admin_addr: Some("127.0.0.1:8080".parse()?),
            ..CraftyConfigure::default()
        })
        .run(RunOpts::default().with_wait_queue(STREAM))
        .await?;

    Ok(())
}
