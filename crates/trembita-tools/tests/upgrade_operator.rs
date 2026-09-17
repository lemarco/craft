//! B-54 — `trembita-ops upgrade` HTTP operator against a mock gateway.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use trembita_tools::ops::upgrade::{RunUpgradeOpts, UpgradeManifest, run_upgrade};

async fn spawn_mock_upgrade_gateway(fleet_after: usize) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let polls = Arc::new(AtomicUsize::new(0));
    let polls_c = Arc::clone(&polls);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let polls = Arc::clone(&polls_c);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let (status, body) = if req.contains("GET /ready") {
                    ("200 OK", r#"{"ok":true}"#)
                } else if req.contains("GET /introspect/ops-summary") {
                    ("200 OK", r#"{"join":{"phase":"pool_ready"}}"#)
                } else if req.contains("POST /cluster/upgrade/desired") {
                    ("202 Accepted", "")
                } else if req.contains("GET /cluster/upgrade") {
                    let n = polls.fetch_add(1, Ordering::SeqCst);
                    if n + 1 >= fleet_after {
                        (
                            "200 OK",
                            r#"{"desired":{"app_version":"2.0.0","url":"file:///x","sha256_hex":"00"},"granted":null,"completed":[1,2,3],"pending":[],"fleet_ready":true,"aborted":null}"#,
                        )
                    } else {
                        (
                            "200 OK",
                            r#"{"desired":{"app_version":"2.0.0","url":"file:///x","sha256_hex":"00"},"granted":1,"completed":[1],"pending":[2,3],"fleet_ready":false,"aborted":null}"#,
                        )
                    }
                } else {
                    ("404 Not Found", "not found")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn b54_run_upgrade_happy_path_mock_gateway() {
    let gateway = spawn_mock_upgrade_gateway(2).await;
    let opts = RunUpgradeOpts {
        gateway,
        manifest: UpgradeManifest {
            app_version: "2.0.0".into(),
            url: "file:///tmp/artifact.bin".into(),
            sha256_hex: "00".repeat(64),
        },
        bearer_token: None,
        timeout: std::time::Duration::from_secs(30),
        poll_interval: std::time::Duration::from_millis(50),
        preflight: true,
        skip_post_smoke: false,
    };
    run_upgrade(opts).await.expect("upgrade run");
}

#[tokio::test]
async fn b54_run_upgrade_fails_when_upgrade_route_missing() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            let _ = stream.write_all(response.as_bytes()).await;
        }
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let response =
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    let opts = RunUpgradeOpts {
        gateway: format!("http://{addr}"),
        manifest: UpgradeManifest {
            app_version: "1.0.0".into(),
            url: "file:///x".into(),
            sha256_hex: "ab".repeat(64),
        },
        bearer_token: None,
        timeout: std::time::Duration::from_secs(5),
        poll_interval: std::time::Duration::from_millis(10),
        preflight: true,
        skip_post_smoke: true,
    };
    let err = run_upgrade(opts).await.expect_err("must fail preflight");
    let msg = err.to_string();
    assert!(msg.contains("404") || msg.contains("UpgradeApi"), "{msg}");
}
