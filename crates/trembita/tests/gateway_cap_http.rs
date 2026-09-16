//! Gateway capability adapters — `cap_invoke` JSON round-trip.

#![allow(clippy::large_futures)]

use std::time::Duration;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita::{
    AppManifest, CapError, CapGroup, CapManifest, CapOp, CapRequest, Gateway, GatewayOpts, OpCtx,
    ProductRoutes, Route, TrembitaApp, TrembitaConfigure, cap_invoke,
};
use trembita_test_support::{
    advance, boot_local_app, eventually_default, spawn_test_gateway, wait_for_trembita_app_leader,
};

#[derive(Default)]
struct MathState {
    sum: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Add {
    n: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Sum {
    total: u64,
}

impl CapRequest for Add {
    const GROUP: &'static str = "math";
    const OP: &'static str = "add";
    const QUEUE_STREAM: &'static str = "math.add";
    const EVENT_TOPIC: &'static str = "math.add";
    const EVENT_SUBSCRIPTION: &'static str = "math.add.cap";
    type Reply = Sum;
}

fn add_run(msg: Add, _ctx: OpCtx<'_>, state: &mut MathState) -> Result<Sum, CapError> {
    state.sum += msg.n;
    Ok(Sum { total: state.sum })
}

#[tokio::test(start_paused = true)]
async fn gateway_cap_invoke_inline_json() {
    let caps = CapManifest::new().group(
        CapGroup::with_state("math")
            .instances(1)
            .op(CapOp::new("add", add_run).routes([Route::Inline])),
    );
    let manifest = AppManifest::new().capabilities(caps);

    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-cap-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    reconcile_period: Duration::from_millis(20),
                    directory_publish_period: Duration::from_millis(20),
                    ..TrembitaConfigure::default()
                })
                .manifest(manifest)
                .gateway(
                    GatewayOpts::new("127.0.0.1:0".parse().expect("addr")).surfaces(|state| {
                        Gateway::new(false).dev_fallback(
                            ProductRoutes::new()
                                .post("/math/add", cap_invoke::<Add>(state, Route::Inline))
                                .build(),
                        )
                    }),
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(500)).await;
    eventually_default("math capability host in directory", || {
        !app.cluster_ref("math").is_empty()
    })
    .await;

    let config = GatewayOpts::new("127.0.0.1:0".parse().expect("addr"))
        .surfaces(|state| {
            Gateway::new(false).dev_fallback(
                ProductRoutes::new()
                    .post("/math/add", cap_invoke::<Add>(state, Route::Inline))
                    .build(),
            )
        })
        .build_config();
    let addr = spawn_test_gateway(&app, config).await;

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/math/add");
    let resp = client
        .post(url)
        .json(&Add { n: 7 })
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), StatusCode::OK);
    let sum: Sum = resp.json().await.expect("json");
    assert_eq!(sum, Sum { total: 7 });

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
