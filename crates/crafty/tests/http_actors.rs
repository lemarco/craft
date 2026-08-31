//! HTTP actor ask / cast routes (B-04f).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use crafty::{CraftyApp, CraftyConfigure};
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_leader};
use tower::ServiceExt;

#[tokio::test(start_paused = true)]
async fn http_ask_returns_503_when_group_has_no_workers() {
    let app = boot_local_app(
        CraftyApp::builder().configure(CraftyConfigure {
            tick_period: Duration::from_millis(5),
            ..CraftyConfigure::default()
        }),
        None,
    )
    .await;

    wait_for_crafty_leader(app.cluster()).await;
    advance(Duration::from_millis(200)).await;

    let api = CraftyApp::actors_api(Arc::clone(&app));
    let router = api.router().with_state(Arc::new(api.into_state()));

    let req = Request::builder()
        .method("POST")
        .uri("/actors/missing/ask")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"payload":"ping"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    app.cluster().shutdown();
}
