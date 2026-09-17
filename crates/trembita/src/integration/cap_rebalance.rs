//! Capability inline delivery survives multi-Raft catalog rebalance (R3 + directory RYW).

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

#[derive(Default)]
struct PingState;

#[derive(serde::Serialize, serde::Deserialize)]
struct Ping {
    id: u64,
}

impl CapRequest for Ping {
    const GROUP: &'static str = "ping";
    const OP: &'static str = "ping";
    const QUEUE_STREAM: &'static str = "ping.ping";
    const EVENT_TOPIC: &'static str = "ping.ping";
    const EVENT_SUBSCRIPTION: &'static str = "ping.ping.cap";
    type Reply = PingAck;

    fn cap_key(&self) -> Option<String> {
        Some(self.id.to_string())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct PingAck {
    ok: bool,
}

async fn ping_cap(
    _msg: Ping,
    _ctx: OpCtx<'_>,
    _state: &mut PingState,
) -> Result<PingAck, CapError> {
    Ok(PingAck { ok: true })
}

#[allow(missing_docs)]
fn ping_cap_register(group: CapGroup<PingState>) -> CapGroup<PingState> {
    group.op(
        CapOp::for_request_async(|req, ctx, state| Box::pin(ping_cap(req, ctx, state)))
            .key_cap::<Ping>(),
    )
}

fn ping_manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<PingState>::for_cap::<Ping>().per_node(),
        ping_cap_register,
    ))
}

fn temp_base() -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "trembita-cap-rebalance-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("tempdir");
    base
}

fn app_builder(id: NodeId, members: [NodeId; 3], base: &Path) -> TrembitaAppBuilder {
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
            .shard_count(64)
            .raft_machines([EmptyStateMachine, EmptyStateMachine]);
    }
    builder.manifest(AppManifest::new().capabilities(ping_manifest()))
}

async fn boot_node(
    net: &LocalNetwork,
    id: NodeId,
    members: [NodeId; 3],
    base: &Path,
) -> Arc<TrembitaApp> {
    let mut opts = RunOpts::local();
    opts.local_net = Some(net.clone());
    app_builder(id, members, base)
        .boot_for_test(opts)
        .await
        .expect("boot cap app")
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

#[tokio::test(start_paused = true)]
async fn capability_inline_survives_raft_group_rebalance() {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let base = temp_base();
    let net = LocalNetwork::new();
    let mut apps = Vec::new();
    for &id in &members {
        apps.push(boot_node(&net, id, members, &base).await);
    }

    let leader = find_leader_app(&apps).await;
    wait_for_group_leaders(leader.cluster()).await;

    eventually_async_default("ping hosts in directory", || async {
        apps.iter().all(|app| !app.cluster_ref("ping").is_empty())
    })
    .await;

    assert_eq!(leader.cluster().raft_groups(), 2);

    let leader_add = {
        let leader = Arc::clone(&leader);
        async move {
            leader.add_raft_groups(1).await.expect("add raft group");
        }
    };
    let invoke_during = async {
        for round in 0..32 {
            for (i, app) in apps.iter().enumerate() {
                let ack = Ping {
                    id: round * 10 + i as u64,
                }
                .via(app.as_ref())
                .route(Route::Inline)
                .await
                .expect("cap inline during rebalance");
                assert_eq!(ack, PingAck { ok: true });
            }
            advance(TICK_PERIOD).await;
        }
    };
    tokio::join!(leader_add, invoke_during);

    for round in 0..8 {
        Ping { id: 900 + round }
            .via(&apps[0])
            .route(Route::Inline)
            .await
            .expect("cap after rebalance");
        advance(TICK_PERIOD).await;
    }

    for app in &apps {
        app.shutdown();
    }
}
