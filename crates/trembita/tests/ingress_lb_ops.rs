//! B-30 — product gateway ops routes used by edge load balancers.

#![allow(clippy::large_futures)]

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use trembita::{DefaultGatewayApis, TrembitaApp, TrembitaConfigure, TrembitaGatewayState};
use trembita_http::{HttpError, ResponseBody};
use trembita_test_facade::{boot_local_app, gateway_ops_config, spawn_test_gateway};
use trembita_test_support::advance;

async fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .expect("GET");
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

#[tokio::test(start_paused = true)]
async fn default_product_routes_ops_only_exposes_lb_paths() {
    let base = std::env::temp_dir().join(format!(
        "trembita-ingress-product-routes-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder().configure(
                TrembitaConfigure::default()
                    .with_local_gateway_apis()
                    .with_data_dir(&base),
            )
        },
        None,
    )
    .await;

    let state = TrembitaGatewayState::new(std::sync::Arc::clone(&app));
    let table = TrembitaApp::default_product_routes(&state, DefaultGatewayApis::ops_only());
    let health = table
        .dispatch_open(
            &Method::GET,
            "/health",
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        )
        .await
        .expect("health");
    assert_eq!(health.status_code(), StatusCode::OK);

    let ready = table
        .dispatch_open(
            &Method::GET,
            "/ready",
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        )
        .await
        .expect("ready");
    assert!(
        ready.status_code() == StatusCode::OK
            || ready.status_code() == StatusCode::SERVICE_UNAVAILABLE
    );

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn default_product_routes_without_ops_omits_lb_paths() {
    let app = boot_local_app(
        || TrembitaApp::builder().configure(TrembitaConfigure::default()),
        None,
    )
    .await;
    let state = TrembitaGatewayState::new(std::sync::Arc::clone(&app));
    let table = TrembitaApp::default_product_routes(
        &state,
        DefaultGatewayApis {
            ops: false,
            ..DefaultGatewayApis::default()
        },
    );
    let health = table
        .dispatch_open(
            &Method::GET,
            "/health",
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        )
        .await
        .expect_err("ops disabled => no /health route");
    assert!(matches!(health, HttpError::NotFound));
    app.shutdown();
}

#[tokio::test(start_paused = true)]
async fn gateway_ops_config_serves_ready_for_lb_http_check() {
    let base = std::env::temp_dir().join(format!(
        "trembita-ingress-gateway-ops-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder().configure(
                TrembitaConfigure::default()
                    .with_local_gateway_apis()
                    .with_data_dir(&base)
                    .with_tick_period(Duration::from_millis(5)),
            )
        },
        None,
    )
    .await;

    let addr = spawn_test_gateway(&app, gateway_ops_config()).await;
    let (health_status, health_body) = http_get(addr, "/health").await;
    assert_eq!(health_status, 200);
    assert!(health_body.contains("ok"));

    let mut ready_status = 0_u16;
    for _ in 0..400 {
        let (s, body) = http_get(addr, "/ready").await;
        ready_status = s;
        if s == 200 {
            assert!(body.contains("member"));
            break;
        }
        advance(Duration::from_millis(5)).await;
    }
    assert_eq!(ready_status, 200);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn b35_ready_json_includes_join_phase_after_boot() {
    let base = std::env::temp_dir().join(format!(
        "trembita-b35-ready-phase-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder().configure(
                TrembitaConfigure::default()
                    .with_local_gateway_apis()
                    .with_data_dir(&base)
                    .with_tick_period(Duration::from_millis(5)),
            )
        },
        None,
    )
    .await;

    let addr = spawn_test_gateway(&app, gateway_ops_config()).await;
    let mut body = String::new();
    for _ in 0..400 {
        let (status, b) = http_get(addr, "/ready").await;
        body = b;
        if status == 200 {
            break;
        }
        advance(Duration::from_millis(5)).await;
    }
    let json: serde_json::Value = serde_json::from_str(&body).expect("ready json");
    assert_eq!(
        json["join_phase"].as_str(),
        Some("pool_ready"),
        "seed voter should reach pool_ready: {json}"
    );

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn b35_product_ops_exposes_join_status_route() {
    let app = boot_local_app(
        || TrembitaApp::builder().configure(TrembitaConfigure::default().with_local_gateway_apis()),
        None,
    )
    .await;
    let state = TrembitaGatewayState::new(std::sync::Arc::clone(&app));
    let table = TrembitaApp::default_product_routes(&state, DefaultGatewayApis::ops_only());
    let resp = table
        .dispatch_open(
            &Method::GET,
            "/introspect/join-status",
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        )
        .await
        .expect("join-status route");
    assert_eq!(resp.status_code(), StatusCode::OK);
    let json = match resp.body() {
        ResponseBody::Json(v) => v.clone(),
        ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("json"),
        other => panic!("unexpected body {other:?}"),
    };
    assert!(json.get("phase").is_some());
    assert!(json.get("log_caught_up").is_some());

    app.shutdown();
}

#[tokio::test(start_paused = true)]
async fn ops_route_table_health_json_shape() {
    let app = boot_local_app(
        || TrembitaApp::builder().configure(TrembitaConfigure::default()),
        None,
    )
    .await;
    let table = app.ops_api().route_table();
    let resp = table
        .dispatch_open(
            &Method::GET,
            "/health",
            HashMap::new(),
            http::HeaderMap::new(),
            Bytes::new(),
        )
        .await
        .expect("health dispatch");
    let json = match resp.body() {
        ResponseBody::Json(v) => v.clone(),
        ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("json"),
        other => panic!("unexpected body {other:?}"),
    };
    assert_eq!(json["status"], "ok");
    app.shutdown();
}
