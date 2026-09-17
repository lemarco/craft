//! B-36 — R3 directory visibility on ops HTTP.

use std::path::PathBuf;
use std::sync::Arc;

use http::StatusCode;
use trembita::{
    AppManifest, CapError, CapGroup, CapManifest, CapOp, OpCtx, ReadyOpts, Route, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_facade::boot_local_app;

fn temp_data_dir(prefix: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "trembita-{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("tempdir");
    base
}

#[derive(Default)]
struct Marker;

#[derive(Default, serde::Deserialize)]
struct Ping;

fn ping_run(_msg: Ping, _ctx: OpCtx<'_>, _state: &mut Marker) -> Result<(), CapError> {
    Ok(())
}

fn ping_group() -> CapGroup<Marker> {
    CapGroup::<Marker>::with_state("r3_ping")
        .op(CapOp::new("ping", ping_run).routes([Route::Inline]))
}

#[tokio::test]
async fn b36_introspect_directory_r3_reports_ryw_defaults() {
    let base = temp_data_dir("b36-r3-defaults");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure::default().with_data_dir(&base))
                .manifest(AppManifest::new().capabilities(CapManifest::new().group(ping_group())))
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let snap = app.directory_r3_snapshot();
    assert_eq!(snap.directory_policy, "read_your_writes");
    assert_eq!(snap.directory_retry_max_attempts, 8);
    assert_eq!(snap.directory_retry_backoff_ms, 25);
    assert!(!snap.directory_retry_boost_active);
    assert!(snap.deliver_no_target_totals.is_empty());

    let state = trembita::TrembitaGatewayState::new(Arc::clone(&app));
    let table =
        TrembitaApp::default_product_routes(&state, trembita::DefaultGatewayApis::ops_only());

    let resp = table
        .dispatch_open(
            &http::Method::GET,
            "/introspect/directory-r3",
            Default::default(),
            Default::default(),
            bytes::Bytes::new(),
        )
        .await
        .expect("route");
    assert_eq!(resp.status_code(), StatusCode::OK);
    let body = match resp.body() {
        trembita_http::ResponseBody::Json(v) => v.clone(),
        trembita_http::ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("json"),
        other => panic!("expected json, got {other:?}"),
    };
    assert_eq!(
        body,
        serde_json::to_value(&snap).expect("snapshot json"),
        "HTTP body must match directory_r3_snapshot()"
    );

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b36_product_ops_only_exposes_directory_r3_route() {
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .manifest(AppManifest::new().capabilities(CapManifest::new().group(ping_group())))
        },
        Some(ReadyOpts::default()),
    )
    .await;
    let state = trembita::TrembitaGatewayState::new(Arc::clone(&app));
    let table =
        TrembitaApp::default_product_routes(&state, trembita::DefaultGatewayApis::ops_only());
    let resp = table
        .dispatch_open(
            &http::Method::GET,
            "/introspect/directory-r3",
            Default::default(),
            Default::default(),
            bytes::Bytes::new(),
        )
        .await
        .expect("directory-r3");
    assert_eq!(resp.status_code(), StatusCode::OK);
    let json = match resp.body() {
        trembita_http::ResponseBody::Json(v) => v.clone(),
        trembita_http::ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("json"),
        other => panic!("expected json, got {other:?}"),
    };
    assert_eq!(json["directory_policy"], "read_your_writes");
    assert!(json.get("merge_lag_epochs").is_some());
    assert!(json.get("local_directory_epoch").is_some());

    app.shutdown();
}
