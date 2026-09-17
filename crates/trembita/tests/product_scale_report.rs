//! B-33 — boot scale plan exposed on ops HTTP.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use trembita::{
    AppManifest, CapError, CapGroup, CapManifest, CapOp, OpCtx, ReadyOpts, Route, TrembitaApp,
    TrembitaConfigure,
};
use trembita_test_facade::boot_local_app;

#[derive(Default)]
struct Marker;

#[derive(Default, serde::Deserialize)]
struct Ping;

fn ping_run(_msg: Ping, _ctx: OpCtx<'_>, _state: &mut Marker) -> Result<(), CapError> {
    Ok(())
}

fn ping_group() -> CapGroup<Marker> {
    CapGroup::<Marker>::with_state("scale_ping")
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

#[tokio::test]
async fn introspect_product_scale_lists_capability_and_coordination() {
    let base = temp_data_dir("b33-scale-report");
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(&base)
                        .with_coordination_raft_groups(2)
                        .with_coordination_shard_count(32),
                )
                .manifest(
                    AppManifest::new()
                        .capabilities(CapManifest::new().group(ping_group()))
                        .queue([
                            trembita::QueueOpts::new("app.jobs", Duration::from_secs(60))
                                .sharded(3),
                        ]),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let plan = app.scale_plan();
    assert_eq!(plan.capability_groups.len(), 1);
    assert_eq!(plan.capability_groups[0].group, "scale_ping");
    assert_eq!(plan.capability_groups[0].hosts, "PerNode");
    assert_eq!(plan.job_queues.len(), 1);
    assert_eq!(plan.job_queues[0].mode, "sharded(3)");
    assert_eq!(plan.coordination.coordination_raft_groups, 2);
    assert_eq!(plan.coordination.coordination_shard_count, Some(32));

    let state = trembita::TrembitaGatewayState::new(Arc::clone(&app));
    let table =
        TrembitaApp::default_product_routes(&state, trembita::DefaultGatewayApis::ops_only());

    let resp = table
        .dispatch_open(
            &http::Method::GET,
            "/introspect/product-scale",
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
    assert_eq!(body["capability_groups"][0]["group"], "scale_ping");
    assert_eq!(body["coordination"]["coordination_raft_groups"], 2);

    let expected = serde_json::to_value(plan).expect("plan json");
    assert_eq!(body, expected, "HTTP body must match in-memory scale_plan");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b33_introspect_reports_fixed_instances_and_shared_ram_defaults() {
    let base = temp_data_dir("b33-fixed-labels");

    #[derive(Default)]
    struct Ram {
        n: u64,
    }

    #[derive(Default, serde::Deserialize)]
    struct Touch;

    fn touch_run(_: Touch, _ctx: OpCtx<'_>, state: &mut Ram) -> Result<(), CapError> {
        state.n += 1;
        Ok(())
    }

    let fixed_group = || {
        CapGroup::<Marker>::with_state("scale_fixed")
            .instances(2)
            .op(CapOp::new("ping", ping_run).routes([Route::Inline]))
    };
    let ram_group = || {
        CapGroup::<Ram>::with_state("scale_ram")
            .op(CapOp::new("touch", touch_run).routes([Route::Inline]))
    };

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure::default().with_data_dir(&base))
                .manifest(
                    AppManifest::new()
                        .capabilities(CapManifest::new().group(fixed_group()).group(ram_group())),
                )
        },
        Some(ReadyOpts::default()),
    )
    .await;

    let plan = app.scale_plan();
    assert_eq!(plan.capability_groups.len(), 2);
    let hosts: Vec<_> = plan
        .capability_groups
        .iter()
        .map(|g| (g.group.as_str(), g.hosts.as_str()))
        .collect();
    assert!(hosts.contains(&("scale_fixed", "Fixed(2)")));
    assert!(hosts.contains(&("scale_ram", "Fixed(1)")));

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
