//! DNS seed resolution for Kubernetes-style headless services.

use crafty::discovery::{Seed, dedupe_seeds, resolve_dns_seeds};
use crafty::proto::NodeId;

#[tokio::test]
async fn resolve_dns_seeds_finds_localhost_statefulset_names() {
    // `.localhost` names resolve to loopback on modern resolvers (RFC 6761).
    let seeds = resolve_dns_seeds("crafty", "localhost", 2, 8080)
        .await
        .expect("crafty-0.localhost should resolve");
    assert!(
        seeds.iter().any(|s| s.node_id == NodeId(1)),
        "ordinal 0 maps to NodeId(1): {seeds:?}"
    );
    for seed in &seeds {
        assert_eq!(seed.addr.port(), 8080);
    }
}

#[test]
fn dedupe_seeds_drops_self_and_preserves_order() {
    let addr: std::net::SocketAddr = "127.0.0.1:7443".parse().unwrap();
    let seeds = [
        Seed::new(NodeId(2), addr),
        Seed::new(NodeId(3), addr),
        Seed::new(NodeId(2), addr),
        Seed::new(NodeId(1), addr),
    ];
    let out = dedupe_seeds(seeds, NodeId(1));
    assert_eq!(
        out,
        vec![Seed::new(NodeId(2), addr), Seed::new(NodeId(3), addr)]
    );
}
