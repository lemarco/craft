//! B-28 — automatic [`CapGroup`](crate::capability::CapGroup) host scale at manifest apply.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::app::{AppManifest, EmptyStateMachine, TrembitaApp, TrembitaAppBuilder};
use crate::app_opts::RunOpts;
use crate::cap_register_chain;
use crate::capability::{CapError, CapGroup, CapManifest, CapOp, CapRequest, CapVia, OpCtx, Route};
use crate::integration::support::wait_for_group_leaders;
use crate::net::LocalNetwork;
use crate::proto::NodeId;
use trembita_assembly::TrembitaClusterBuilder;
use trembita_test_support::{
    TICK_PERIOD, advance, eventually_async_default, fast_raft_config_with_seed,
};

fn temp_base(prefix: &str) -> PathBuf {
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

fn app_builder(
    id: NodeId,
    members: [NodeId; 3],
    base: &Path,
    caps: CapManifest,
) -> TrembitaAppBuilder {
    let mut builder = TrembitaApp::builder();
    {
        let inner = builder.inner_mut();
        *inner = TrembitaClusterBuilder::new(id, EmptyStateMachine)
            .members(members)
            .data_dir(base.join(format!("node-{}", id.0)))
            .raft_config(fast_raft_config_with_seed(88))
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(15))
            .directory_publish_period(Duration::from_millis(15))
            .shard_count(64);
    }
    builder.manifest(AppManifest::new().capabilities(caps))
}

async fn boot_three_node_cluster(
    net: &LocalNetwork,
    members: [NodeId; 3],
    base: &Path,
    caps: impl Fn() -> CapManifest,
) -> Vec<Arc<TrembitaApp>> {
    let mut apps = Vec::new();
    for &id in &members {
        let mut opts = RunOpts::local();
        opts.local_net = Some(net.clone());
        let app = app_builder(id, members, base, caps())
            .boot_for_test(opts)
            .await
            .expect("boot cap scale app");
        apps.push(app);
    }
    apps
}

async fn find_leader_app(apps: &[Arc<TrembitaApp>]) -> Arc<TrembitaApp> {
    for _ in 0..200 {
        for app in apps {
            if app.cluster().is_leader().await {
                return Arc::clone(app);
            }
        }
        advance(TICK_PERIOD).await;
    }
    panic!("no leader");
}

async fn wait_pool_size(app: &TrembitaApp, group: &str, expected: usize) {
    let label = format!("{group} directory pool size {expected}");
    eventually_async_default(&label, || async {
        app.cluster_ref(group).len() == expected
    })
    .await;
}

// --- marker state → auto PerNode ---

#[derive(Default)]
struct MarkerState;

#[derive(serde::Serialize, serde::Deserialize)]
struct Ping {
    n: u64,
}

impl CapRequest for Ping {
    const GROUP: &'static str = "scale_ping";
    const OP: &'static str = "ping";
    const QUEUE_STREAM: &'static str = "scale_ping.ping";
    const EVENT_TOPIC: &'static str = "scale_ping.ping";
    const EVENT_SUBSCRIPTION: &'static str = "scale_ping.ping.cap";
    type Reply = PingAck;
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct PingAck {
    n: u64,
}

async fn ping_run(
    msg: Ping,
    _ctx: OpCtx<'_>,
    _state: &mut MarkerState,
) -> Result<PingAck, CapError> {
    Ok(PingAck { n: msg.n })
}

fn ping_register(group: CapGroup<MarkerState>) -> CapGroup<MarkerState> {
    group.op(
        CapOp::for_request_async(|req, ctx, state| Box::pin(ping_run(req, ctx, state)))
            .routes([Route::Inline]),
    )
}

fn marker_auto_manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<MarkerState>::for_cap::<Ping>(),
        ping_register,
    ))
}

#[tokio::test(start_paused = true)]
async fn auto_scale_marker_state_spawns_one_host_per_node() {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let base = temp_base("cap-scale-per-node");
    let net = LocalNetwork::new();
    let apps = boot_three_node_cluster(&net, members, &base, marker_auto_manifest).await;
    let leader = find_leader_app(&apps).await;
    wait_for_group_leaders(leader.cluster()).await;
    wait_pool_size(&leader, "scale_ping", 3).await;

    let plan = leader.scale_plan();
    assert_eq!(plan.capability_groups.len(), 1);
    assert_eq!(plan.capability_groups[0].group, "scale_ping");
    assert_eq!(plan.capability_groups[0].hosts, "PerNode");

    let nodes = leader.cluster_ref("scale_ping").nodes();
    assert_eq!(nodes, members.to_vec());

    let ack = Ping { n: 7 }
        .via(apps[1].as_ref())
        .route(Route::Inline)
        .await
        .expect("inline on auto-scaled pool");
    assert_eq!(ack, PingAck { n: 7 });

    for app in &apps {
        app.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn explicit_instances_pins_cluster_wide_pool_on_marker_state() {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let base = temp_base("cap-scale-fixed-two");
    let net = LocalNetwork::new();
    let apps = boot_three_node_cluster(&net, members, &base, || {
        CapManifest::new().group(cap_register_chain!(
            CapGroup::<MarkerState>::for_cap::<Ping>().instances(2),
            ping_register,
        ))
    })
    .await;
    let leader = find_leader_app(&apps).await;
    wait_for_group_leaders(leader.cluster()).await;
    wait_pool_size(&leader, "scale_ping", 2).await;

    let plan = leader.scale_plan();
    assert_eq!(plan.capability_groups[0].hosts, "Fixed(2)");

    for app in &apps {
        app.shutdown();
    }
}

// --- shared RAM → auto Fixed(1) ---

#[derive(Default)]
struct MathState {
    sum: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Add {
    n: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Sum {
    total: u64,
}

impl CapRequest for Add {
    const GROUP: &'static str = "scale_math";
    const OP: &'static str = "add";
    const QUEUE_STREAM: &'static str = "scale_math.add";
    const EVENT_TOPIC: &'static str = "scale_math.add";
    const EVENT_SUBSCRIPTION: &'static str = "scale_math.add.cap";
    type Reply = Sum;
}

async fn add_run(msg: Add, _ctx: OpCtx<'_>, state: &mut MathState) -> Result<Sum, CapError> {
    state.sum += msg.n;
    Ok(Sum { total: state.sum })
}

fn math_register(group: CapGroup<MathState>) -> CapGroup<MathState> {
    group.op(CapOp::for_request_async(|req, ctx, state| {
        Box::pin(add_run(req, ctx, state))
    }))
}

fn ram_auto_manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<MathState>::for_cap::<Add>(),
        math_register,
    ))
}

#[tokio::test(start_paused = true)]
async fn auto_scale_shared_ram_state_spawns_single_cluster_host() {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let base = temp_base("cap-scale-fixed-one");
    let net = LocalNetwork::new();
    let apps = boot_three_node_cluster(&net, members, &base, ram_auto_manifest).await;
    let leader = find_leader_app(&apps).await;
    wait_for_group_leaders(leader.cluster()).await;
    wait_pool_size(&leader, "scale_math", 1).await;

    assert_eq!(leader.scale_plan().capability_groups[0].hosts, "Fixed(1)");

    let sum1 = Add { n: 1 }
        .via(apps[0].as_ref())
        .route(Route::Inline)
        .await
        .expect("add from node 1");
    assert_eq!(sum1, Sum { total: 1 });

    let sum2 = Add { n: 2 }
        .via(apps[2].as_ref())
        .route(Route::Inline)
        .await
        .expect("add from node 3");
    assert_eq!(sum2, Sum { total: 3 }, "single host holds shared RAM state");

    for app in &apps {
        app.shutdown();
    }
}

// --- Route::Session without .per_node() → auto Fixed(1) ---

async fn session_run(
    msg: Ping,
    _ctx: OpCtx<'_>,
    _state: &mut MarkerState,
) -> Result<PingAck, CapError> {
    Ok(PingAck { n: msg.n })
}

fn session_register(group: CapGroup<MarkerState>) -> CapGroup<MarkerState> {
    group.op(
        CapOp::for_request_async(|req, ctx, state| Box::pin(session_run(req, ctx, state)))
            .routes([Route::Session]),
    )
}

#[tokio::test(start_paused = true)]
async fn auto_scale_session_route_defaults_to_single_host() {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let base = temp_base("cap-scale-session");
    let net = LocalNetwork::new();
    let apps = boot_three_node_cluster(&net, members, &base, || {
        CapManifest::new().group(cap_register_chain!(
            CapGroup::<MarkerState>::with_state("scale_session"),
            session_register,
        ))
    })
    .await;
    let leader = find_leader_app(&apps).await;
    wait_for_group_leaders(leader.cluster()).await;
    wait_pool_size(&leader, "scale_session", 1).await;

    for app in &apps {
        app.shutdown();
    }
}
