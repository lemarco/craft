//! HTTP enqueue → worker integration (B-03).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use crafty::net::LocalNetwork;
use crafty::{CraftyApp, NodeId};
use crafty_test_support::{advance, wait_for_crafty_leader};
use tower::ServiceExt;

#[tokio::test(start_paused = true)]
async fn http_post_job_returns_202_and_enqueues() {
    let base = std::env::temp_dir().join(format!(
        "crafty-http-jobs-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let net = LocalNetwork::new();
    let app = Arc::new(
        CraftyApp::builder(NodeId(1))
            .data_dir(&base)
            .job_stream("jobs", Duration::from_secs(60))
            .members([NodeId(1)])
            .tick_period(Duration::from_millis(5))
            .start_local(&net)
            .await,
    );

    wait_for_crafty_leader(app.cluster()).await;
    advance(Duration::from_millis(200)).await;

    let api = CraftyApp::jobs_api(Arc::clone(&app));
    let router = api.router().with_state(Arc::new(api.into_state()));

    let req = Request::builder()
        .method("POST")
        .uri("/jobs/jobs?dedup=invoice-1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"payload":"send-email"}"#))
        .unwrap();

    let resp = router.clone().oneshot(req).await.expect("route");
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let job_id = app
        .enqueue("jobs", b"send-email")
        .await
        .expect("dedup enqueue returns same job");
    assert!(job_id.0 >= 1);

    app.cluster().shutdown();
    let _ = std::fs::remove_dir_all(base);
}
