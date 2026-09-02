//! [`CraftyApp`] gateway integration (HTTP + WebSocket product surface).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use crafty::cluster::build_gateway_router;
use crafty::{CraftyApp, CraftyConfigure, GatewayOpts, QueueOpts};
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_app_leader};
use tower::ServiceExt;

#[tokio::test(start_paused = true)]
async fn gateway_serves_jobs_api_on_configured_addr() {
    let base = std::env::temp_dir().join(format!(
        "crafty-http-gateway-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            CraftyApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
                .configure(CraftyConfigure {
                    tick_period: Duration::from_millis(5),
                    ..CraftyConfigure::default()
                })
                .gateway(GatewayOpts::new("127.0.0.1:0".parse().unwrap()).with_jobs_api(true))
        },
        None,
    )
    .await;

    wait_for_crafty_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let router = build_gateway_router(
        Arc::clone(&app),
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .with_jobs_api(true)
            .with_actors_api(true)
            .build_config(),
    );

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
