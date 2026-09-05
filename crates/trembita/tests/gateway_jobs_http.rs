//! B-14c: HTTP jobs through the product gateway (batch + auth + metadata).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use http::StatusCode;
use trembita::{
    ConsumerOpts, GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError, QueueOpts,
    TrembitaApp, TrembitaConfigure, consumer,
};
use trembita_jobs::JobLifecycle;
use trembita_test_support::{
    advance, boot_local_app, spawn_test_gateway, wait_for_trembita_app_leader,
};

static SIDE_EFFECTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct BearerSecret;

impl GatewayIdentity for BearerSecret {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, req: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        match req.bearer_token() {
            Some("secret") => Ok("alice".into()),
            _ => Err(IdentityError::Unauthorized),
        }
    }
}

#[consumer("gateway-jobs")]
#[allow(clippy::unused_async)]
async fn handle_job(_payload: &[u8]) -> Result<(), ()> {
    SIDE_EFFECTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

fn gateway_config() -> trembita::GatewayConfig {
    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .with_jobs_api(true)
        .identity(BearerSecret)
        .protect_product_apis(true)
        .build_config()
}

#[tokio::test(start_paused = true)]
async fn gateway_jobs_batch_and_job_status_metadata() {
    SIDE_EFFECTS.store(0, std::sync::atomic::Ordering::SeqCst);
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-jobs-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .queue([
                    QueueOpts::new("gateway-jobs", Duration::from_secs(60)).default_max_attempts(3)
                ])
                .consumer(HandleJobConsumer, ConsumerOpts::default())
                .gateway(GatewayOpts::new("127.0.0.1:0".parse().unwrap()).with_jobs_api(true))
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let addr = spawn_test_gateway(&app, gateway_config()).await;
    let client = reqwest::Client::new();
    let url = |path: &str| format!("http://{addr}{path}");

    let unauth = client
        .post(url("/jobs/gateway-jobs/batch"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .body(r#"{"jobs":[{"payload":"a","dedup":"inv-1","max_attempts":2}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let batch = client
        .post(url("/jobs/gateway-jobs/batch"))
        .header("Host", "127.0.0.1")
        .header("content-type", "application/json")
        .header("authorization", "Bearer secret")
        .header("x-trembita-user", "alice")
        .body(r#"{"jobs":[{"payload":"a","dedup":"inv-1","max_attempts":2}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), StatusCode::ACCEPTED);

    let job_id = app
        .enqueue_opts(
            "gateway-jobs",
            b"lookup",
            trembita::cluster::EnqueueOptions::dedup_key("lookup-key"),
        )
        .await
        .expect("enqueue");
    advance(Duration::from_millis(300)).await;

    let get = client
        .get(url(&format!("/jobs/gateway-jobs/{}", job_id.0)))
        .header("Host", "127.0.0.1")
        .header("authorization", "Bearer secret")
        .header("x-trembita-user", "alice")
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);

    let status = app
        .job_status("gateway-jobs", job_id)
        .await
        .expect("status")
        .expect("job");
    assert!(matches!(
        status.lifecycle,
        JobLifecycle::Pending | JobLifecycle::Leased
    ));
}

#[tokio::test(start_paused = true)]
async fn gateway_rate_limit_returns_429() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-rate-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .queue([
                    QueueOpts::new("gateway-jobs", Duration::from_secs(60)).default_max_attempts(3)
                ])
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                        .with_jobs_api(true)
                        .identity(BearerSecret)
                        .protect_product_apis(true)
                        .rate_limit_per_sec(1),
                )
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;

    let addr = spawn_test_gateway(
        &app,
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .with_jobs_api(true)
            .identity(BearerSecret)
            .protect_product_apis(true)
            .rate_limit_per_sec(1)
            .build_config(),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/jobs/gateway-jobs");

    let ok = client
        .get(&url)
        .header("Host", "127.0.0.1")
        .header("authorization", "Bearer secret")
        .header("x-trembita-user", "alice")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);

    let limited = client
        .get(&url)
        .header("Host", "127.0.0.1")
        .header("authorization", "Bearer secret")
        .header("x-trembita-user", "alice")
        .send()
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}
