//! Cluster wait helpers using in-crate [`TrembitaCluster`](crate::cluster::TrembitaCluster) types.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;

use crate::Gateway;
use crate::ReadyOpts;
use crate::RunOpts;
use crate::TrembitaApp;
use crate::TrembitaAppBuilder;
use crate::TrembitaGatewayState;
use crate::cluster::{TrembitaCluster, cluster_ops_route_table};
use crate::core::{Role, StateMachine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use trembita_test_support::{POLL_STEP, advance};

/// Minimal HTTP/1.1 `GET` returning `(status_code, body)` — LB health-check style.
pub(crate) async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect ops http");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("send req");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read resp");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// Dev-fallback gateway with ops routes (health, metrics, dashboard, introspect).
#[must_use]
pub fn gateway_ops_surfaces(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(state.app.ops_api().route_table())
}

/// Bind an ephemeral port and serve ops routes for `cluster` until shutdown.
pub async fn spawn_cluster_ops_gateway<M>(cluster: &TrembitaCluster<M>) -> SocketAddr
where
    M: StateMachine + Send + Sync + 'static,
{
    let gateway = Gateway::new(false).dev_fallback(cluster_ops_route_table(cluster));
    let service = gateway.build_service().expect("gateway service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway test listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let service = service.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let hyper_service = TowerToHyperService::new(service);
                let _ = http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    addr
}

/// Boot a local [`TrembitaApp`] for in-crate integration tests.
pub async fn boot_local_app(
    build: impl FnOnce() -> TrembitaAppBuilder,
    wait_ready: Option<ReadyOpts>,
) -> Arc<TrembitaApp> {
    let mut opts = RunOpts::local();
    opts.wait_ready = wait_ready;
    build().boot_for_test(opts).await.expect("boot_local_app")
}

pub async fn await_trembita_leader<M>(
    clusters: &[Arc<TrembitaCluster<M>>],
) -> Arc<TrembitaCluster<M>>
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..1000 {
        for c in clusters {
            if c.is_leader().await {
                return Arc::clone(c);
            }
        }
        advance(POLL_STEP).await;
    }
    panic!("no leader elected");
}

pub async fn wait_for_trembita_leader<M>(cluster: &TrembitaCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        if cluster.is_leader().await {
            return;
        }
        advance(POLL_STEP).await;
    }
    panic!("cluster failed to elect a leader");
}

pub async fn wait_for_trembita_stopped<M>(cluster: &TrembitaCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    cluster.shutdown_and_wait().await;
}

pub async fn wait_for_each_group_cluster_leader<M>(
    clusters: &[Arc<TrembitaCluster<M>>],
    group_count: u32,
) where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..1000 {
        let mut ready = true;
        'groups: for g in 0..group_count {
            for c in clusters {
                let Some(handle) = c.group_handles().get(g as usize) else {
                    continue;
                };
                if let Some(status) = handle.status().await
                    && status.role == Role::Leader
                {
                    continue 'groups;
                }
            }
            ready = false;
            break;
        }
        if ready {
            return;
        }
        advance(POLL_STEP).await;
    }
    panic!("not all raft groups elected a leader across the cluster");
}

pub async fn wait_for_group_leaders<M>(cluster: &TrembitaCluster<M>)
where
    M: StateMachine + Send + Sync + 'static,
{
    for _ in 0..500 {
        let mut leaders = 0usize;
        for handle in cluster.group_handles() {
            if let Some(status) = handle.status().await
                && status.role == Role::Leader
            {
                leaders += 1;
            }
        }
        if leaders == cluster.raft_groups() as usize {
            return;
        }
        advance(POLL_STEP).await;
    }
    panic!("not all raft groups elected a leader");
}
