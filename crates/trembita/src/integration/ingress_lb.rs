//! B-30 — ingress/LB health-check contract on ops HTTP (`GET /health`, `GET /ready`).

use std::sync::Arc;
use std::time::Duration;

use super::{await_trembita_leader, boot_local_app, gateway_ops_surfaces, http_get};
use crate::GatewayOpts;
use crate::NodeId;
use crate::TrembitaApp;
use crate::TrembitaConfigure;
use crate::cluster::TrembitaCluster;
use crate::net::LocalNetwork;
use trembita_assembly::builder::TrembitaClusterBuilder;
use trembita_test_support::{Kv, TICK_PERIOD, advance, fast_raft_config};

async fn spawn_three_node_cluster() -> (LocalNetwork, Vec<Arc<TrembitaCluster<Kv>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut clusters = Vec::new();
    for &id in &ids {
        let builder = TrembitaClusterBuilder::new(id, Kv::default())
            .members(ids)
            .raft_config(fast_raft_config())
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20));
        clusters.push(Arc::new(builder.start_local(&net).await));
    }
    (net, clusters)
}

async fn await_ready_on_addr(addr: std::net::SocketAddr) -> (u16, String) {
    let mut last = (0, String::new());
    for _ in 0..500 {
        last = http_get(addr, "/ready").await;
        if last.0 == 200 {
            return last;
        }
        advance(TICK_PERIOD).await;
    }
    last
}

#[tokio::test(start_paused = true)]
async fn lb_each_backend_exposes_health_and_ready_after_join() {
    let (_net, clusters) = spawn_three_node_cluster().await;
    let _leader = await_trembita_leader(&clusters).await;

    for cluster in &clusters {
        let addr = super::spawn_cluster_ops_gateway(cluster.as_ref()).await;
        let (health_status, health_body) = http_get(addr, "/health").await;
        assert_eq!(health_status, 200, "health body: {health_body}");
        assert!(
            health_body.contains("\"status\":\"ok\""),
            "health json: {health_body}"
        );

        let (ready_status, ready_body) = await_ready_on_addr(addr).await;
        assert_eq!(ready_status, 200, "ready body: {ready_body}");
        assert!(
            ready_body.contains("\"member\":true"),
            "ready json: {ready_body}"
        );
        assert!(
            !ready_body.contains("\"draining\":true"),
            "pool should not include draining backends: {ready_body}"
        );
    }

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn lb_liveness_health_stays_ok_while_readiness_warms_up() {
    let (_net, clusters) = spawn_three_node_cluster().await;
    let follower = Arc::clone(&clusters[1]);
    let addr = super::spawn_cluster_ops_gateway(follower.as_ref()).await;

    let (health_status, _) = http_get(addr, "/health").await;
    assert_eq!(health_status, 200);

    let _ = await_trembita_leader(&clusters).await;
    let (ready_status, ready_body) = await_ready_on_addr(addr).await;
    assert_eq!(ready_status, 200, "{ready_body}");

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn lb_concurrent_ready_polls_match_single_get() {
    let (_net, clusters) = spawn_three_node_cluster().await;
    let _leader = await_trembita_leader(&clusters).await;
    let addr = super::spawn_cluster_ops_gateway(clusters[0].as_ref()).await;
    let _ = await_ready_on_addr(addr).await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        handles.push(tokio::spawn(
            async move { http_get(addr, "/ready").await.0 },
        ));
    }
    for h in handles {
        assert_eq!(h.await.expect("join poll"), 200);
    }

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn lb_metrics_endpoint_available_on_same_tcp_bind_as_health() {
    let (_net, clusters) = spawn_three_node_cluster().await;
    let _leader = await_trembita_leader(&clusters).await;
    let addr = super::spawn_cluster_ops_gateway(clusters[2].as_ref()).await;
    let _ = await_ready_on_addr(addr).await;

    let (status, body) = http_get(addr, "/metrics").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("# TYPE") || body.is_empty(),
        "prometheus text or empty families: {body}"
    );

    for c in &clusters {
        c.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn trembita_app_ops_gateway_serves_lb_paths_on_unified_listener() {
    use std::sync::Arc;

    use crate::spawn_gateway;

    let base = std::env::temp_dir().join(format!(
        "trembita-ingress-lb-{}",
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

    let ops_addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr");
    let config = GatewayOpts::new(ops_addr)
        .surfaces(gateway_ops_surfaces)
        .build_config();
    let _handle = spawn_gateway(Arc::clone(&app), config)
        .await
        .expect("spawn ops gateway");
    let addr = ops_addr;

    let (health_status, health_body) = http_get(addr, "/health").await;
    assert_eq!(health_status, 200, "{health_body}");

    let mut ready = (0, String::new());
    for _ in 0..500 {
        ready = http_get(addr, "/ready").await;
        if ready.0 == 200 {
            break;
        }
        advance(TICK_PERIOD).await;
    }
    assert_eq!(ready.0, 200, "ready: {}", ready.1);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
