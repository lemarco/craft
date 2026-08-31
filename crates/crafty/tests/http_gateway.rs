//! [`CraftyApp`] gateway integration (HTTP + WebSocket product surface).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use crafty::{CraftyApp, CraftyConfigure, GatewayConfig, GatewayOpts, build_gateway_router};
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_leader};
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
        CraftyApp::builder()
            .data_dir(&base)
            .queue("jobs", Duration::from_secs(60))
            .configure(CraftyConfigure {
                tick_period: Duration::from_millis(5),
                ..CraftyConfigure::default()
            })
            .gateway("127.0.0.1:0".parse().unwrap(), GatewayOpts::default()),
        None,
    )
    .await;

    wait_for_crafty_leader(app.cluster()).await;
    advance(Duration::from_millis(200)).await;

    let router = build_gateway_router(
        Arc::clone(&app),
        GatewayConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            jobs_api: true,
            actors_api: true,
            workflows_api: false,
            routes: None,
        },
    );

    let req = Request::builder()
        .method("POST")
        .uri("/jobs/jobs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"payload":"via-gateway"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    app.cluster().shutdown();
    let _ = std::fs::remove_dir_all(base);
}
