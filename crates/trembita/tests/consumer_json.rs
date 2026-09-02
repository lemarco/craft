//! B-14f: `#[consumer_json]` deserializes JSON payloads before the handler runs.

use trembita::{JobConsumer, JobContext, consumer_json};
use trembita_jobs::{JobId, LeaseId};

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
struct WelcomeEmail {
    to: String,
}

#[consumer_json("emails", WelcomeEmail)]
#[allow(clippy::unused_async)] // macro requires `async fn`; handler is sync body
async fn send_welcome(job: WelcomeEmail) -> Result<(), String> {
    if job.to.is_empty() {
        return Err("missing recipient".into());
    }
    Ok(())
}

#[tokio::test]
async fn consumer_json_decodes_payload() {
    assert_eq!(SendWelcomeConsumer::STREAM, "emails");
    let ctx = JobContext::new(JobId(1), LeaseId(1), "emails", 1, None);
    SendWelcomeConsumer::handle_job(br#"{"to":"user@example.com"}"#, ctx)
        .await
        .expect("valid json");
}

#[tokio::test]
async fn consumer_json_rejects_invalid_json() {
    let ctx = JobContext::new(JobId(1), LeaseId(1), "emails", 1, None);
    let err = SendWelcomeConsumer::handle_job(b"not-json", ctx)
        .await
        .expect_err("invalid json");
    assert!(err.contains("invalid job json"));
}
