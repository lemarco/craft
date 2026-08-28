//! Integration tests for the admin HTTP server: every route is exercised over
//! a real TCP socket against a fake [`Observer`], including the SSE feed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crafty_dashboard::{
    ActorView, AdminServer, AdminTlsPaths, BoxFuture, ClusterView, EventBus, Metrics, NodeSummary,
    NodeView, Observer, RaftGroupsView, Readiness, admin_tls_config,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Fake {
    ready: bool,
}

impl Observer for Fake {
    fn readiness(&self) -> BoxFuture<'_, Readiness> {
        let ready = self.ready;
        Box::pin(async move {
            Readiness {
                node_id: 2,
                role: "follower".into(),
                member: ready,
                draining: false,
                workers: vec!["orders".into()],
                reason: (!ready).then(|| "joining".into()),
            }
        })
    }

    fn cluster(&self) -> BoxFuture<'_, ClusterView> {
        Box::pin(async move {
            ClusterView {
                leader: Some(1),
                term: 4,
                commit_index: 12,
                nodes: vec![
                    NodeSummary {
                        id: 1,
                        role: "leader".into(),
                        member: true,
                    },
                    NodeSummary {
                        id: 2,
                        role: "follower".into(),
                        member: true,
                    },
                ],
            }
        })
    }

    fn raft_groups(&self) -> BoxFuture<'_, RaftGroupsView> {
        Box::pin(async move {
            RaftGroupsView {
                shard_count: 64,
                shard_routing: "stable_virtual".into(),
                catalog_size: 2,
                catalog_version: 1,
                replication_factor: 3,
                learner_factor: 1,
                hosted_groups: vec![0, 1],
                groups: vec![],
            }
        })
    }

    fn actors(&self) -> BoxFuture<'_, Vec<ActorView>> {
        Box::pin(async move {
            vec![ActorView {
                id: "orders/0".into(),
                node: 2,
                actor_type: "OrderWorker".into(),
                mailbox_depth: 3,
                uptime_secs: 42,
                generation: 1,
            }]
        })
    }

    fn actor(&self, id: &str) -> BoxFuture<'_, Option<ActorView>> {
        let id = id.to_owned();
        Box::pin(async move {
            (id == "orders/0").then(|| ActorView {
                id,
                node: 2,
                actor_type: "OrderWorker".into(),
                mailbox_depth: 3,
                uptime_secs: 42,
                generation: 1,
            })
        })
    }

    fn node(&self, id: u64) -> BoxFuture<'_, Option<NodeView>> {
        Box::pin(async move {
            (id == 2).then(|| NodeView {
                id: 2,
                workers: vec!["orders".into()],
                cpus: 8,
                store_healthy: true,
            })
        })
    }
}

async fn spawn(ready: bool) -> (SocketAddr, Metrics, EventBus) {
    let metrics = Metrics::new();
    metrics.incr("crafty_client_requests_total", "Client requests.", &[], 7.0);
    let events = EventBus::new(16);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = AdminServer::new(Arc::new(Fake { ready }), metrics.clone(), events.clone());
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    (addr, metrics, events)
}

/// Send a `GET path` with `Connection: close` and return `(status_code, body)`.
async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: admin\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

async fn https_get(
    addr: SocketAddr,
    path: &str,
    trust_anchor: &rustls::pki_types::CertificateDer<'static>,
) -> (u16, String) {
    use std::sync::Arc;

    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, RootCertStore};
    use tokio_rustls::TlsConnector;

    let mut roots = RootCertStore::empty();
    roots.add(trust_anchor.clone()).unwrap();
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, stream).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    tls.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    tls.read_to_string(&mut raw).await.unwrap();
    let status = raw
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn mint_admin_tls() -> (
    tempfile::TempDir,
    AdminTlsPaths,
    rustls::pki_types::CertificateDer<'static>,
) {
    let dir = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let der = rustls_pemfile::certs(&mut cert.cert.pem().as_bytes())
        .next()
        .expect("cert pem")
        .expect("parse cert");
    let paths = AdminTlsPaths {
        cert: cert_path,
        key: key_path,
    };
    (dir, paths, der)
}

#[tokio::test]
async fn admin_serves_https_when_tls_configured() {
    let (_dir, paths, trust) = mint_admin_tls();
    let tls = admin_tls_config(&paths).expect("tls config");
    let metrics = Metrics::new();
    let events = EventBus::new(16);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = AdminServer::new(Arc::new(Fake { ready: true }), metrics, events);
    tokio::spawn(async move {
        let _ = server.serve_tls(listener, tls).await;
    });

    let (status, body) = https_get(addr, "/health", &trust).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"status\":\"ok\""));

    let (status, body) = https_get(addr, "/introspect/raft-groups", &trust).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"shard_count\":64"));
}

#[tokio::test]
async fn health_is_always_ok() {
    let (addr, _m, _e) = spawn(true).await;
    let (status, body) = get(addr, "/health").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"status\":\"ok\""));
}

#[tokio::test]
async fn ready_reflects_membership() {
    let (addr, _m, _e) = spawn(true).await;
    let (status, body) = get(addr, "/ready").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"member\":true"));

    let (addr, _m, _e) = spawn(false).await;
    let (status, body) = get(addr, "/ready").await;
    assert_eq!(status, 503);
    assert!(body.contains("\"reason\":\"joining\""));
}

#[tokio::test]
async fn metrics_render_prometheus_text() {
    let (addr, _m, _e) = spawn(true).await;
    let (status, body) = get(addr, "/metrics").await;
    assert_eq!(status, 200);
    assert!(body.contains("# TYPE crafty_client_requests_total counter"));
    assert!(body.contains("crafty_client_requests_total 7"));
}

#[tokio::test]
async fn introspection_routes_return_json() {
    let (addr, _m, _e) = spawn(true).await;

    let (status, body) = get(addr, "/introspect/cluster").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"leader\":1") && body.contains("\"term\":4"));

    let (status, body) = get(addr, "/introspect/raft-groups").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"shard_count\":64")
            && body.contains("\"catalog_size\":2")
            && body.contains("\"catalog_version\":1")
    );
    assert!(body.contains("\"shard_routing\":\"stable_virtual\""));

    let (status, body) = get(addr, "/introspect/actors").await;
    assert_eq!(status, 200);
    assert!(body.contains("OrderWorker"));

    let (status, body) = get(addr, "/introspect/actors/orders/0").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"id\":\"orders/0\""));

    let (status, _) = get(addr, "/introspect/actors/missing").await;
    assert_eq!(status, 404);

    let (status, body) = get(addr, "/introspect/node/2").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"cpus\":8"));

    let (status, _) = get(addr, "/introspect/node/99").await;
    assert_eq!(status, 404);

    let (status, _) = get(addr, "/introspect/node/notanumber").await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn dashboard_serves_html() {
    let (addr, _m, _e) = spawn(true).await;
    let (status, body) = get(addr, "/dashboard").await;
    assert_eq!(status, 200);
    assert!(body.contains("<title>crafty · dashboard</title>"));
}

#[tokio::test]
async fn unknown_route_is_404() {
    let (addr, _m, _e) = spawn(true).await;
    let (status, _) = get(addr, "/nope").await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn sse_feed_streams_emitted_events() {
    let (addr, _m, events) = spawn(true).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = "GET /dashboard/events HTTP/1.1\r\nHost: admin\r\n\r\n";
    stream.write_all(req.as_bytes()).await.unwrap();

    // Give the handler a moment to subscribe, then emit an event.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = events.emit(crafty_dashboard::CraftyEvent::LeaderChanged { term: 5, leader: 3 });

    // Read a chunk and look for the SSE data line for our event.
    let mut buf = vec![0u8; 4096];
    let mut seen = String::new();
    for _ in 0..10 {
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("sse read timeout")
            .expect("sse read");
        if n == 0 {
            break;
        }
        seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        if seen.contains("leader_changed") {
            break;
        }
    }
    assert!(seen.contains("text/event-stream") || seen.contains(": connected"));
    assert!(seen.contains("\"event\":\"leader_changed\""), "got: {seen}");
}
