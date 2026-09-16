//! Capability routes (B-21).

#![allow(clippy::large_futures)]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use trembita::{
    AppManifest, CapError, CapGroup, CapManifest, CapOp, CapRequest, CapVia, CapWire, OpCtx, Route,
    TrembitaApp, TrembitaConfigure,
};
use trembita_proto::encode;
use trembita_test_facade::{boot_local_app_with_consumers, wait_for_trembita_app_leader};
use trembita_test_support::{advance, eventually_default};

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

#[derive(Debug, Serialize, Deserialize, Default)]
struct Total {}

impl CapRequest for Add {
    const GROUP: &'static str = "math";
    const OP: &'static str = "add";
    const QUEUE_STREAM: &'static str = "math.add";
    const EVENT_TOPIC: &'static str = "math.add";
    const EVENT_SUBSCRIPTION: &'static str = "math.add.cap";
    type Reply = Sum;
}

impl CapRequest for Total {
    const GROUP: &'static str = "math";
    const OP: &'static str = "total";
    const QUEUE_STREAM: &'static str = "math.total";
    const EVENT_TOPIC: &'static str = "math.total";
    const EVENT_SUBSCRIPTION: &'static str = "math.total.cap";
    type Reply = Sum;
}

fn add_run(msg: Add, _ctx: OpCtx<'_>, state: &mut MathState) -> Result<Sum, CapError> {
    state.sum += msg.n;
    Ok(Sum { total: state.sum })
}

fn total_run(_msg: Total, _ctx: OpCtx<'_>, state: &mut MathState) -> Result<Sum, CapError> {
    Ok(Sum { total: state.sum })
}

fn math_manifest() -> AppManifest {
    let caps = CapManifest::new().group(
        CapGroup::with_state("math")
            .instances(1)
            .queue_stream("cap-math")
            .op(CapOp::new("add", add_run).routes([
                Route::Inline,
                Route::Queued,
                Route::QueuedWait,
            ]))
            .op(CapOp::new("total", total_run).routes([Route::Inline])),
    );
    AppManifest::new().capabilities(caps)
}

async fn boot_math_app(base: &std::path::Path) -> trembita::TestBoot {
    boot_local_app_with_consumers(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure {
                    data_dir: Some(base.into()),
                    without_ops: false,
                    without_jobs_api: false,
                    without_schedules_api: false,
                    without_workflows_api: false,
                    without_topics_api: false,
                    tick_period: Duration::from_millis(5),
                    reconcile_period: Duration::from_millis(20),
                    directory_publish_period: Duration::from_millis(20),
                    ..TrembitaConfigure::default()
                })
                .manifest(math_manifest())
        },
        None,
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn capability_inline_ask_round_trip() {
    let base = std::env::temp_dir().join(format!(
        "trembita-capability-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let boot = boot_math_app(&base).await;
    let app = &boot.app;

    wait_for_trembita_app_leader(app).await;
    advance(Duration::from_millis(500)).await;
    eventually_default("math capability host in directory", || {
        !app.cluster_ref("math").is_empty()
    })
    .await;

    let reply = Add { n: 3 }
        .via(app)
        .route(Route::Inline)
        .await
        .expect("inline");
    assert_eq!(reply, Sum { total: 3 });

    let reply = Add { n: 4 }
        .via(app)
        .route(Route::Inline)
        .await
        .expect("inline");
    assert_eq!(reply, Sum { total: 7 });

    let reply = Total::default()
        .via(app)
        .route(Route::Inline)
        .await
        .expect("total");
    assert_eq!(reply, Sum { total: 7 });

    boot.shutdown().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn capability_queued_job_runs_via_bridge() {
    let base = std::env::temp_dir().join(format!(
        "trembita-capability-q-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let boot = boot_math_app(&base).await;
    let app = &boot.app;

    wait_for_trembita_app_leader(app).await;
    advance(Duration::from_millis(500)).await;

    Add { n: 5 }.via(app).enqueue().await.expect("enqueue");
    advance(Duration::from_millis(800)).await;

    let reply = Total::default()
        .via(app)
        .route(Route::Inline)
        .await
        .expect("read");
    assert_eq!(reply, Sum { total: 5 });

    boot.shutdown().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn capability_enqueue_dedup_key_collapses() {
    let base = std::env::temp_dir().join(format!(
        "trembita-capability-dedup-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let boot = boot_math_app(&base).await;
    let app = &boot.app;

    wait_for_trembita_app_leader(app).await;
    advance(Duration::from_millis(500)).await;

    let first = Add { n: 1 }
        .via(app)
        .dedup_key("invoice-1")
        .enqueue()
        .await
        .expect("enqueue");
    let second = Add { n: 99 }
        .via(app)
        .dedup_key("invoice-1")
        .enqueue()
        .await
        .expect("dedup enqueue");
    assert_eq!(first.job_id, second.job_id);

    boot.shutdown().await;
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn capability_queued_wait_returns_reply() {
    let base = std::env::temp_dir().join(format!(
        "trembita-capability-qw-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let boot = boot_math_app(&base).await;
    let app = &boot.app;

    wait_for_trembita_app_leader(app).await;
    advance(Duration::from_millis(500)).await;

    let reply = Add { n: 9 }
        .via(app)
        .wait_timeout(Duration::from_secs(5))
        .queued_wait()
        .await
        .expect("queued wait");
    assert_eq!(reply, Sum { total: 9 });

    boot.shutdown().await;
    let _ = std::fs::remove_dir_all(base);
}

fn math_event_manifest() -> AppManifest {
    let caps = CapManifest::new().group(
        CapGroup::with_state("math")
            .instances(1)
            .event_ingress("math.events", "cap-handler")
            .op(CapOp::new("add", add_run).routes([Route::Inline, Route::Event]))
            .op(CapOp::new("total", total_run).routes([Route::Inline])),
    );
    AppManifest::new().capabilities(caps)
}

#[tokio::test(start_paused = true)]
async fn capability_event_subscription_runs_inline() {
    let base = std::env::temp_dir().join(format!(
        "trembita-capability-ev-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let boot = boot_local_app_with_consumers(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure {
                    data_dir: Some(base.to_path_buf()),
                    without_ops: false,
                    without_jobs_api: false,
                    without_schedules_api: false,
                    without_workflows_api: false,
                    without_topics_api: false,
                    tick_period: Duration::from_millis(5),
                    reconcile_period: Duration::from_millis(20),
                    directory_publish_period: Duration::from_millis(20),
                    ..TrembitaConfigure::default()
                })
                .manifest(math_event_manifest())
        },
        None,
    )
    .await;
    let app = &boot.app;

    wait_for_trembita_app_leader(app).await;
    advance(Duration::from_millis(500)).await;

    Add { n: 11 }
        .via(app)
        .publish_event()
        .await
        .expect("publish_event");
    advance(Duration::from_millis(800)).await;

    let reply = Total::default()
        .via(app)
        .route(Route::Inline)
        .await
        .expect("read");
    assert_eq!(reply, Sum { total: 11 });

    let frame = encode(&CapWire {
        op: Add::OP.to_string(),
        payload: encode(&Add { n: 1 }).expect("encode"),
        ingress: None,
    })
    .expect("wire");
    app.publish("math.events", &frame)
        .await
        .expect("raw publish");
    advance(Duration::from_millis(800)).await;
    let reply = Total::default()
        .via(app)
        .route(Route::Inline)
        .await
        .expect("read");
    assert_eq!(reply, Sum { total: 12 });

    boot.shutdown().await;
    let _ = std::fs::remove_dir_all(base);
}
