//! Sample job consumer — replace with your domain handlers.

use trembita::consumer;

/// Default job stream for the scaffold.
pub const STREAM: &str = "jobs";

#[consumer("jobs")]
async fn handle_sample(payload: &[u8]) -> Result<(), String> {
    let preview = String::from_utf8_lossy(payload);
    tracing::info!(target: "app", stream = STREAM, "job: {preview}");
    Ok(())
}
