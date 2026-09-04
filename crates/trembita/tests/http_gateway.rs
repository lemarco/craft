//! [`TrembitaApp`] gateway integration (HTTP + WebSocket product surface).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;
use trembita::cluster::build_gateway_router;
use trembita::{
    GatewayIdentity, GatewayOpts, GatewayRequest, IdentityError, QueueOpts, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

struct TestGatewayIdentity;

impl GatewayIdentity for TestGatewayIdentity {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, _: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        Ok("test".into())
    }
}

#[tokio::test(start_paused = true)]
async fn gateway_serves_jobs_api_on_configured_addr() {
    let base = std::env::temp_dir().join(format!(
        "trembita-http-gateway-{}",
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
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                        .with_jobs_api(true)
                        .identity(TestGatewayIdentity),
                )
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
            .with_actors_api(true)
            .identity(TestGatewayIdentity)
            .build_config(),
    )
    .expect("gateway config");

    let req = Request::builder()
        .method("POST")
        .uri("/jobs/jobs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"payload":"via-gateway"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
