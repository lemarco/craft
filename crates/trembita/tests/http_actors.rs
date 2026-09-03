//! HTTP actor ask / cast routes (B-04f).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;
use trembita::{TrembitaApp, TrembitaConfigure};
use trembita_test_support::{advance, boot_local_app, wait_for_trembita_app_leader};

#[tokio::test(start_paused = true)]
async fn http_ask_returns_503_when_group_has_no_workers() {
    let app = boot_local_app(
        || {
            TrembitaApp::builder().configure(TrembitaConfigure {
                tick_period: Duration::from_millis(5),
                ..TrembitaConfigure::default()
            })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let api = TrembitaApp::actors_api(Arc::clone(&app));
    let router = api.router().with_state(Arc::new(api.into_state()));

    let req = Request::builder()
        .method("POST")
        .uri("/actors/missing/ask")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"payload":"ping"}"#))
        .unwrap();

    let resp = router.oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    app.shutdown();
}
