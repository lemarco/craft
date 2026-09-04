//! B-14c: HTTP jobs through the product gateway router (batch + auth + metadata).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;
use trembita::cluster::build_gateway_router;
use trembita::{
    ConsumerOpts, GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError, QueueOpts,
    TrembitaApp, TrembitaConfigure, consumer,
};
use trembita_jobs::JobLifecycle;
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

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
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                        .with_jobs_api(true)
                        .identity(BearerSecret)
                        .protect_product_apis(true),
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
    advance(Duration::from_millis(200)).await;

    let router = build_gateway_router(
        &app,
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .with_jobs_api(true)
            .identity(BearerSecret)
            .protect_product_apis(true)
            .build_config(),
    )
    .expect("gateway config");

    let unauth = Request::builder()
        .method("POST")
        .uri("/jobs/gateway-jobs/batch")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"jobs":[{"payload":"a","dedup":"inv-1","max_attempts":2}]}"#,
        ))
        .unwrap();
    let resp = router.clone().oneshot(unauth).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let batch = Request::builder()
        .method("POST")
        .uri("/jobs/gateway-jobs/batch")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer secret")
        .header("x-trembita-user", "alice")
        .body(Body::from(
            r#"{"jobs":[{"payload":"a","dedup":"inv-1","max_attempts":2}]}"#,
        ))
        .unwrap();
    let resp = router.clone().oneshot(batch).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let job_id = app
        .enqueue_opts(
            "gateway-jobs",
            b"lookup",
            trembita::cluster::EnqueueOptions::dedup_key("lookup-key"),
        )
        .await
        .expect("enqueue");
    advance(Duration::from_millis(300)).await;

    let get = Request::builder()
        .method("GET")
        .uri(format!("/jobs/gateway-jobs/{}", job_id.0))
        .header(header::AUTHORIZATION, "Bearer secret")
        .header("x-trembita-user", "alice")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(get).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

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

    let router = build_gateway_router(
        &app,
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .with_jobs_api(true)
            .identity(BearerSecret)
            .protect_product_apis(true)
            .rate_limit_per_sec(1)
            .build_config(),
    )
    .expect("gateway config");

    let ok = Request::builder()
        .method("GET")
        .uri("/jobs/gateway-jobs")
        .header(header::AUTHORIZATION, "Bearer secret")
        .header("x-trembita-user", "alice")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(ok).await.unwrap().status(),
        StatusCode::OK
    );

    let limited = Request::builder()
        .method("GET")
        .uri("/jobs/gateway-jobs")
        .header(header::AUTHORIZATION, "Bearer secret")
        .header("x-trembita-user", "alice")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.oneshot(limited).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}
