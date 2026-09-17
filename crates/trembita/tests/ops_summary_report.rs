//! B-43 — aggregated ops cockpit on `GET /introspect/ops-summary`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use http::{Method, StatusCode};
use trembita::{
    AppManifest, CapError, CapGroup, CapManifest, CapOp, CoordinationGrowthPreset, OpCtx,
    QueueOpts, ReadyOpts, Route, TrembitaApp, TrembitaConfigure,
};
use trembita_http::{HttpError, ResponseBody, RouteTable};
use trembita_test_facade::boot_local_app;

#[derive(Default)]
struct Marker;

#[derive(Default, serde::Deserialize)]
struct Ping;

fn ping_run(_msg: Ping, _ctx: OpCtx<'_>, _state: &mut Marker) -> Result<(), CapError> {
    Ok(())
}

fn ping_group() -> CapGroup<Marker> {
    CapGroup::<Marker>::with_state("ops_ping")
        .op(CapOp::new("ping", ping_run).routes([Route::Inline]))
}

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

fn product_ops_table(app: &Arc<TrembitaApp>) -> RouteTable {
    let state = trembita::TrembitaGatewayState::new(Arc::clone(app));
    TrembitaApp::default_product_routes(&state, trembita::DefaultGatewayApis::ops_only())
}

async fn dispatch_json(table: &RouteTable, method: &Method, path: &str) -> serde_json::Value {
    let resp = table
        .dispatch_open(
            method,
            path,
            HashMap::default(),
            http::HeaderMap::default(),
            bytes::Bytes::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("dispatch {method} {path}: {e:?}"));
    assert_eq!(resp.status_code(), StatusCode::OK, "{method} {path}");
    match resp.body() {
        ResponseBody::Json(v) => v.clone(),
        ResponseBody::Bytes(b) => serde_json::from_slice(b).expect("json"),
        other => panic!("expected json at {path}, got {other:?}"),
    }
}

fn queue_depth_hints_from_queues_view(queues: &serde_json::Value) -> serde_json::Value {
    let hints: Vec<serde_json::Value> = queues["streams"]
        .as_array()
        .expect("streams array")
        .iter()
        .map(|s| {
            serde_json::json!({
                "stream": s["stream"],
                "pending": s["pending"],
                "leased": s["leased"],
                "oldest_pending_age_ms": s["oldest_pending_age_ms"],
            })
        })
        .collect();
    serde_json::Value::Array(hints)
}

#[tokio::test]
async fn b43_introspect_ops_summary_aggregates_join_scale_r3_and_preset() {
    let base = temp_data_dir("b43-ops-summary");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::JobsBacklog),
                )
                .manifest(
                    AppManifest::new()
                        .capabilities(CapManifest::new().group(ping_group()))
                        .queue([QueueOpts::new("app.jobs", Duration::from_secs(60)).sharded(2)]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    assert_eq!(
        app.coordination_growth_preset(),
        Some(CoordinationGrowthPreset::JobsBacklog)
    );

    let live = app.ops_summary().await;
    assert_eq!(live.join.phase, trembita_dashboard::JoinPhase::PoolReady);
    assert_eq!(live.product_scale.capability_groups[0].group, "ops_ping");
    assert_eq!(live.directory_r3.directory_policy, "read_your_writes");
    assert_eq!(
        live.coordination_profile.preset.as_deref(),
        Some("jobs_backlog")
    );
    assert_eq!(
        live.coordination_profile.env_job_queue_auto_shard,
        Some(true)
    );

    let table = product_ops_table(&app);
    let body = dispatch_json(&table, &Method::GET, "/introspect/ops-summary").await;

    assert_eq!(body["join"]["phase"], "pool_ready");
    assert_eq!(
        body["product_scale"]["capability_groups"][0]["group"],
        "ops_ping"
    );
    assert_eq!(body["directory_r3"]["directory_policy"], "read_your_writes");
    assert_eq!(body["coordination_profile"]["preset"], "jobs_backlog");
    assert!(body.get("queue_depths").is_some());

    let expected = serde_json::to_value(&live).expect("live json");
    assert_eq!(body, expected, "HTTP body must match ops_summary()");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b43_product_ops_exposes_ops_summary_route() {
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_coordination_growth_preset(CoordinationGrowthPreset::Standard),
                )
                .manifest(AppManifest::new().capabilities(CapManifest::new().group(ping_group())))
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let table = product_ops_table(&app);
    dispatch_json(&table, &Method::GET, "/introspect/ops-summary").await;

    app.shutdown();
}

#[tokio::test]
async fn b43_ops_summary_nested_routes_match_dedicated_introspect_endpoints() {
    let base = temp_data_dir("b43-nested-routes");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure::default().with_data_dir(&base))
                .manifest(
                    AppManifest::new()
                        .capabilities(CapManifest::new().group(ping_group()))
                        .queue([QueueOpts::new("imports", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let table = product_ops_table(&app);
    let summary = dispatch_json(&table, &Method::GET, "/introspect/ops-summary").await;
    let product_scale = dispatch_json(&table, &Method::GET, "/introspect/product-scale").await;
    let directory_r3 = dispatch_json(&table, &Method::GET, "/introspect/directory-r3").await;
    let join_status = dispatch_json(&table, &Method::GET, "/introspect/join-status").await;
    let queues = dispatch_json(&table, &Method::GET, "/introspect/queues").await;

    assert_eq!(
        summary["product_scale"], product_scale,
        "product_scale section must match /introspect/product-scale"
    );
    assert_eq!(
        summary["directory_r3"], directory_r3,
        "directory_r3 section must match /introspect/directory-r3"
    );
    assert_eq!(
        summary["join"], join_status,
        "join section must match /introspect/join-status"
    );
    assert_eq!(
        summary["queue_depths"],
        queue_depth_hints_from_queues_view(&queues),
        "queue_depths must mirror /introspect/queues depth fields"
    );

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b43_ops_summary_without_growth_preset_omits_profile_fields() {
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .manifest(AppManifest::new().capabilities(CapManifest::new().group(ping_group())))
        },
        Some(ReadyOpts::default()),
    )
    .await;

    assert_eq!(app.coordination_growth_preset(), None);
    let summary = app.ops_summary().await;
    assert_eq!(summary.coordination_profile, Default::default());

    let table = product_ops_table(&app);
    let json = dispatch_json(&table, &Method::GET, "/introspect/ops-summary").await;
    assert_eq!(json["coordination_profile"], serde_json::json!({}));

    app.shutdown();
}

#[tokio::test]
async fn b43_write_sharding_ops_summary_reflects_multi_raft_in_product_scale() {
    let base = temp_data_dir("b43-write-sharding");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::WriteSharding)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("meta", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let summary = app.ops_summary().await;
    assert_eq!(
        summary.coordination_profile.preset.as_deref(),
        Some("write_sharding")
    );
    assert_eq!(
        summary.coordination_profile.env_job_queue_auto_shard,
        Some(false)
    );
    assert_eq!(
        summary.product_scale.coordination.coordination_raft_groups,
        2
    );
    assert_eq!(
        summary.product_scale.coordination.coordination_shard_count,
        Some(64)
    );
    assert_eq!(summary.product_scale.job_queues[0].mode, "standard");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b43_full_preset_ops_summary_includes_auto_shard_profile_hints() {
    let base = temp_data_dir("b43-full-preset");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_growth_preset(CoordinationGrowthPreset::Full)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .manifest(
                    AppManifest::new().queue([QueueOpts::new("jobs", Duration::from_secs(30))]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let summary = app.ops_summary().await;
    assert_eq!(summary.coordination_profile.preset.as_deref(), Some("full"));
    assert_eq!(
        summary.coordination_profile.env_job_queue_auto_shard,
        Some(true)
    );
    assert!(summary.coordination_profile.auto_shard_max_shards.is_some());
    assert!(
        summary
            .coordination_profile
            .auto_shard_pending_threshold
            .is_some()
    );
    assert_eq!(summary.product_scale.job_queues[0].mode, "auto_shard");
    assert_eq!(
        summary.product_scale.coordination.coordination_raft_groups,
        2
    );

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b43_ops_summary_http_get_only() {
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .manifest(AppManifest::new().capabilities(CapManifest::new().group(ping_group())))
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let table = product_ops_table(&app);
    let err = table
        .dispatch_open(
            &Method::POST,
            "/introspect/ops-summary",
            HashMap::default(),
            http::HeaderMap::default(),
            bytes::Bytes::new(),
        )
        .await;
    assert!(matches!(err, Err(HttpError::NotFound)));

    app.shutdown();
}
