//! End-to-end mTLS tests over a real loopback `quinn` handshake (security).
//!
//! These prove the config builders wire mutual authentication correctly: a peer
//! with a CA-signed identity connects and both ends observe the other's
//! certificate, while a peer whose certificate was issued by a *different* CA is
//! rejected during the handshake.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use craft_net::proto::NodeId;
use craft_net::tls::{ClusterCa, client_config, node_server_name, server_config};

/// Start a one-shot server endpoint that accepts a single connection and
/// reports whether the connected peer presented a certificate.
fn spawn_server(
    server_cfg: quinn::ServerConfig,
) -> (SocketAddr, tokio::task::JoinHandle<Result<bool, String>>) {
    let endpoint = quinn::Endpoint::server(server_cfg, (Ipv4Addr::LOCALHOST, 0).into())
        .expect("bind server endpoint");
    let addr = endpoint.local_addr().expect("server addr");
    let handle = tokio::spawn(async move {
        let incoming = endpoint.accept().await.ok_or("no incoming connection")?;
        let conn = incoming.await.map_err(|e| e.to_string())?;
        let saw_client_cert = conn.peer_identity().is_some();
        // Hold the endpoint until the client observes the handshake result.
        conn.closed().await;
        Ok(saw_client_cert)
    });
    (addr, handle)
}

#[tokio::test]
async fn mutual_tls_handshake_succeeds_between_ca_signed_peers() {
    let ca = ClusterCa::generate().unwrap();
    let server_id = ca.issue_node(NodeId(2)).unwrap();
    let client_id = ca.issue_node(NodeId(1)).unwrap();

    let server_cfg = server_config(&server_id, ca.root_store().unwrap()).unwrap();
    let client_cfg = client_config(&client_id, ca.root_store().unwrap()).unwrap();

    let (server_addr, server) = spawn_server(server_cfg);

    let client = quinn::Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let conn = client
        .connect_with(client_cfg, server_addr, &node_server_name(NodeId(2)))
        .unwrap()
        .await
        .expect("client should complete the mTLS handshake");

    // The client authenticated the server (mutual auth).
    assert!(conn.peer_identity().is_some(), "client saw no server cert");

    conn.close(0u32.into(), b"done");

    let saw_client_cert = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server task timed out")
        .expect("server task panicked")
        .expect("server connection failed");
    assert!(saw_client_cert, "server saw no client cert");
}

#[tokio::test]
async fn a_client_signed_by_a_foreign_ca_is_rejected() {
    let cluster_ca = ClusterCa::generate().unwrap();
    let rogue_ca = ClusterCa::generate().unwrap();

    let server_id = cluster_ca.issue_node(NodeId(2)).unwrap();
    // The rogue client trusts the real cluster CA (so it accepts the server),
    // but its own certificate is signed by a CA the server does not trust.
    let rogue_id = rogue_ca.issue_node(NodeId(1)).unwrap();

    let server_cfg = server_config(&server_id, cluster_ca.root_store().unwrap()).unwrap();
    let rogue_cfg = client_config(&rogue_id, cluster_ca.root_store().unwrap()).unwrap();

    let (server_addr, server) = spawn_server(server_cfg);

    let client = quinn::Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let attempt = client
        .connect_with(rogue_cfg, server_addr, &node_server_name(NodeId(2)))
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), attempt).await;
    match result {
        // Rejected outright during the handshake — expected.
        Ok(Err(_)) => {}
        // QUIC's 1-RTT client may optimistically finish its side before the
        // server's rejection arrives; the connection must then close promptly
        // (the server aborts with a TLS alert) rather than stay usable.
        Ok(Ok(conn)) => {
            let closed = tokio::time::timeout(Duration::from_secs(5), conn.closed()).await;
            assert!(
                closed.is_ok(),
                "foreign-CA connection stayed open instead of being rejected"
            );
        }
        Err(_) => panic!("handshake neither completed nor failed within timeout"),
    }

    // The server side must have failed rather than accepting the peer.
    let server_result = tokio::time::timeout(Duration::from_secs(5), server).await;
    if let Ok(Ok(Ok(saw_cert))) = server_result {
        assert!(!saw_cert, "server must not accept a foreign-CA client");
    }
}

#[tokio::test]
async fn wrong_server_name_is_rejected() {
    let ca = ClusterCa::generate().unwrap();
    let server_id = ca.issue_node(NodeId(2)).unwrap();
    let client_id = ca.issue_node(NodeId(1)).unwrap();

    let server_cfg = server_config(&server_id, ca.root_store().unwrap()).unwrap();
    let client_cfg = client_config(&client_id, ca.root_store().unwrap()).unwrap();

    let (server_addr, _server) = spawn_server(server_cfg);

    let client = quinn::Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    // Dial the server certificate issued for node 2 using the wrong name.
    let attempt = client
        .connect_with(client_cfg, server_addr, "craft-node-999")
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), attempt).await;
    assert!(
        matches!(result, Ok(Err(_))),
        "server-name mismatch must fail the handshake, got {result:?}"
    );
}
