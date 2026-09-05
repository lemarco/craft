//! HTTP actor ask / cast routes (B-04f).

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode, header};
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
    let table = api.route_table();
    let mut headers = http::HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    let resp = table
        .dispatch(
            &Method::POST,
            "/actors/missing/ask",
            HashMap::new(),
            headers,
            Bytes::from(r#"{"payload":"ping"}"#),
        )
        .await
        .expect("dispatch");
    assert_eq!(resp.status_code(), StatusCode::SERVICE_UNAVAILABLE);

    app.shutdown();
}
