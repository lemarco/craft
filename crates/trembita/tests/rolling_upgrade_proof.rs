//! B-49 — rolling upgrade proof (coordinator + HTTP + session/job continuity during peer restart).

#![allow(clippy::large_futures)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use http::StatusCode;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use trembita::cluster::TrembitaCluster;
use trembita::net::LocalNetwork;
use trembita::proto::NodeId;
use trembita::upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeMachine, UpgradeOpts, UpgradeQuery, UpgradeResponse,
    spawn_upgrade_coordinator, upgrade_api,
};
use trembita::{
    AppManifest, ClusterSessionSecret, CookieConfig, Gateway, GatewayOpts, JobOpts, RouteTable,
    TrembitaApp, TrembitaConfigure, cluster_session_gate, consumer,
};
use trembita_http::{Gateway as HttpGateway, RequestCtx, Response};
use trembita_test_facade::{await_trembita_leader, spawn_test_gateway};
use trembita_test_support::{
    TICK_PERIOD, advance, eventually_async_default, fast_raft_config_with_seed,
};

static B49_JOBS_DONE: AtomicU32 = AtomicU32::new(0);

#[consumer("jobs")]
async fn b49_job_consumer(_payload: &[u8]) -> Result<(), ()> {
    B49_JOBS_DONE.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn temp_base(prefix: &str) -> std::path::PathBuf {
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

async fn upgrade_view(cluster: &TrembitaCluster<UpgradeMachine>) -> trembita::UpgradeView {
    let members = cluster.members().to_vec();
    let UpgradeResponse::View(view) = cluster
        .handle()
        .query(UpgradeQuery::View { members })
        .await
        .expect("query")
    else {
        panic!("expected view");
    };
    view
}

async fn http_raw(
    addr: SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, Vec<(String, String)>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if !body.is_empty() {
        req.push_str(&format!(
            "Content-Length: {}\r\nContent-Type: application/json\r\n",
            body.len()
        ));
    }
    for (k, v) in extra_headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let header_block = text.split("\r\n\r\n").next().unwrap_or("");
    let mut headers = Vec::new();
    for line in header_block.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let response_body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, response_body, headers)
}

async fn spawn_upgrade_gateway(cluster: Arc<TrembitaCluster<UpgradeMachine>>) -> SocketAddr {
    let api = upgrade_api(cluster);
    let gateway = HttpGateway::new(false).dev_fallback(api.route_table());
    let service = gateway.build_service().expect("gateway service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let service = service.clone();
            tokio::spawn(async move {
                use hyper::server::conn::http1;
                use hyper_util::rt::TokioIo;
                use hyper_util::service::TowerToHyperService;
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

async fn start_three_node_upgrade_clusters(
    net: &LocalNetwork,
    tmp: &TempDir,
    bytes: &[u8],
) -> (String, Vec<Arc<TrembitaCluster<UpgradeMachine>>>) {
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let artifact_path = tmp.path().join("artifact.bin");
    std::fs::write(&artifact_path, bytes).expect("write artifact");
    let sha256_hex = hex::encode(Sha256::digest(bytes));
    let mut clusters = Vec::new();
    for &id in &ids {
        let install = tmp.path().join(format!("node-{}", id.0));
        let opts = UpgradeOpts {
            install_dir: install.clone(),
            current_link: install.join("current"),
            tick_period: Duration::from_millis(50),
            dry_run: true,
        };
        let cluster = Arc::new(
            trembita_assembly::builder::TrembitaClusterBuilder::new(id, UpgradeMachine::default())
                .members(ids)
                .raft_config(fast_raft_config_with_seed(49))
                .tick_period(TICK_PERIOD)
                .allow_leave(true)
                .start_local(net)
                .await,
        );
        let _coordinator = spawn_upgrade_coordinator(Arc::clone(&cluster), opts);
        clusters.push(cluster);
    }
    (sha256_hex, clusters)
}

#[tokio::test(start_paused = true)]
async fn b49_coordinator_dry_run_two_version_manifest() {
    let net = LocalNetwork::new();
    let tmp = TempDir::new().expect("tempdir");
    let (sha256_hex, clusters) =
        start_three_node_upgrade_clusters(&net, &tmp, b"trembita b49 rolling artifact v2").await;

    let leader = await_trembita_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;

    let artifact_path = tmp.path().join("artifact.bin");
    leader
        .handle()
        .propose(UpgradeCommand::SetDesired(ArtifactManifest {
            app_version: "2.0.0".into(),
            url: format!("file://{}", artifact_path.display()),
            sha256_hex,
            min_protocol: None,
        }))
        .await
        .expect("set desired");

    let leader_poll = Arc::clone(&leader);
    eventually_async_default("b49 fleet dry-run complete", move || {
        let leader = Arc::clone(&leader_poll);
        async move {
            for _ in 0..30 {
                advance(Duration::from_millis(100)).await;
            }
            let view = upgrade_view(leader.as_ref()).await;
            view.fleet_ready
                && view.completed.len() == 3
                && view
                    .desired
                    .as_ref()
                    .is_some_and(|d| d.app_version == "2.0.0")
        }
    })
    .await;

    for cluster in clusters {
        cluster.shutdown();
    }
}

#[tokio::test(start_paused = true)]
async fn b49_http_upgrade_routes_set_desired_and_poll_fleet_ready() {
    let net = LocalNetwork::new();
    let tmp = TempDir::new().expect("tempdir");
    let (sha256_hex, clusters) =
        start_three_node_upgrade_clusters(&net, &tmp, b"b49 http route artifact").await;

    let leader = await_trembita_leader(&clusters).await;
    advance(Duration::from_millis(100)).await;
    let addr = spawn_upgrade_gateway(Arc::clone(&leader)).await;

    let artifact_path = tmp.path().join("artifact.bin");
    let post_body = serde_json::json!({
        "app_version": "2.1.0",
        "url": format!("file://{}", artifact_path.display()),
        "sha256_hex": sha256_hex,
    });
    let (post_status, _, _) = http_raw(
        addr,
        "POST",
        "/cluster/upgrade/desired",
        &[],
        &post_body.to_string(),
    )
    .await;
    assert_eq!(post_status, 202);

    let leader_poll = Arc::clone(&leader);
    eventually_async_default("b49 http poll fleet_ready", move || {
        let leader = Arc::clone(&leader_poll);
        async move {
            for _ in 0..20 {
                advance(Duration::from_millis(100)).await;
            }
            let (status, body, _) = http_raw(addr, "GET", "/cluster/upgrade", &[], "").await;
            if status != 200 {
                return false;
            }
            let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            v.get("fleet_ready").and_then(serde_json::Value::as_bool) == Some(true)
                && upgrade_view(leader.as_ref()).await.completed.len() == 3
        }
    })
    .await;

    for cluster in clusters {
        cluster.shutdown();
    }
}

fn three_node_app_builder(
    id: NodeId,
    members: [NodeId; 3],
    base: &Path,
) -> trembita::TrembitaAppBuilder {
    let mut builder = TrembitaApp::builder();
    {
        let inner = builder.inner_mut();
        *inner = trembita_assembly::builder::TrembitaClusterBuilder::new(
            id,
            trembita::cluster::EmptyStateMachine,
        )
        .members(members)
        .data_dir(base.join(format!("node-{}", id.0)))
        .raft_config(fast_raft_config_with_seed(49))
        .tick_period(TICK_PERIOD)
        .reconcile_period(Duration::from_millis(15));
    }
    builder.manifest(AppManifest::new())
}

async fn boot_three_node_apps(base: &Path) -> (LocalNetwork, Vec<Arc<TrembitaApp>>) {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    let mut apps = Vec::new();
    for &id in &members {
        let opts = trembita::RunOpts::local()
            .with_local_net(net.clone())
            .with_wait_ready(trembita::ReadyOpts::default());
        let app = three_node_app_builder(id, members, base)
            .boot_for_test(opts)
            .await
            .expect("boot");
        apps.push(app);
    }
    (net, apps)
}

fn session_gateway(secret: ClusterSessionSecret) -> GatewayOpts {
    let cookie = CookieConfig {
        name: "sess".into(),
        ..CookieConfig::from_env("TEST", "sess")
    };
    let gate = cluster_session_gate(secret.clone(), cookie.clone());
    let cookie_name = cookie.name.clone();
    GatewayOpts::new("127.0.0.1:0".parse().unwrap()).surfaces(move |_state| {
        let routes = RouteTable::new()
            .get("/login", {
                let gate = gate.clone();
                let secret = secret.clone();
                move |ctx: RequestCtx| {
                    let gate = gate.clone();
                    let secret = secret.clone();
                    async move {
                        let user = ctx.query_param("user").unwrap_or("roll").to_string();
                        let token = secret
                            .issue(&user, Duration::from_secs(3600))
                            .map_err(|e| trembita_http::HttpError::Internal(e.to_string()))?;
                        let mut resp = Response::text(StatusCode::OK, user.clone());
                        gate.set_session_cookie(&mut resp, &token)?;
                        Ok(resp)
                    }
                }
            })
            .get_session("/me", {
                let secret = secret.clone();
                move |ctx: RequestCtx| {
                    let secret = secret.clone();
                    let cookie_name = cookie_name.clone();
                    async move {
                        let user = trembita::session_user_from_cookie(
                            &cookie_name,
                            ctx.headers(),
                            &secret,
                        )?;
                        Ok(Response::text(StatusCode::OK, user))
                    }
                }
            });
        Gateway::new(false)
            .dev_fallback_session(gate.clone())
            .dev_fallback(routes)
    })
}

/// Shared cluster session cookie works on a peer gateway while the fleet is up (B-34/B-49).
#[tokio::test(start_paused = true)]
async fn b49_cluster_session_cookie_valid_on_peer_during_fleet_up() {
    let secret = ClusterSessionSecret::from_bytes(b"b49-session-secret!!").unwrap();
    let base = temp_base("b49-session");
    let (_net, apps) = boot_three_node_apps(&base).await;
    advance(TICK_PERIOD).await;

    let addr_a = spawn_test_gateway(&apps[0], session_gateway(secret.clone()).build_config()).await;
    let addr_b = spawn_test_gateway(&apps[1], session_gateway(secret).build_config()).await;

    let (login_status, _, headers) = http_raw(addr_a, "GET", "/login?user=rolling", &[], "").await;
    assert_eq!(login_status, 200);
    let set_cookie = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("set-cookie"))
        .map(|(_, v)| v.clone())
        .expect("Set-Cookie");
    let cookie_pair = set_cookie.split(';').next().expect("cookie pair");

    let (me_status, body, _) = http_raw(addr_b, "GET", "/me", &[("Cookie", cookie_pair)], "").await;
    assert_eq!(me_status, 200);
    assert_eq!(body.trim(), "rolling");

    for app in apps {
        app.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

async fn leader_app(apps: &[Arc<TrembitaApp>]) -> Arc<TrembitaApp> {
    for _ in 0..200 {
        for app in apps {
            if app.is_leader().await {
                return Arc::clone(app);
            }
        }
        advance(TICK_PERIOD).await;
    }
    panic!("no leader app");
}

async fn boot_three_node_apps_with_jobs(
    base: &Path,
) -> (LocalNetwork, Vec<Arc<TrembitaApp>>, Vec<trembita::TestBoot>) {
    let members = [NodeId(1), NodeId(2), NodeId(3)];
    let net = LocalNetwork::new();
    B49_JOBS_DONE.store(0, Ordering::SeqCst);
    let mut boots = Vec::new();
    for &id in &members {
        let mut builder = TrembitaApp::builder();
        {
            let inner = builder.inner_mut();
            *inner = trembita_assembly::builder::TrembitaClusterBuilder::new(
                id,
                trembita::cluster::EmptyStateMachine,
            )
            .members(members)
            .data_dir(base.join(format!("node-{}", id.0)))
            .raft_config(fast_raft_config_with_seed(49))
            .tick_period(TICK_PERIOD)
            .reconcile_period(Duration::from_millis(15));
        }
        builder = builder
            .configure(TrembitaConfigure::default().with_tick_period(Duration::from_millis(5)))
            .manifest(
                AppManifest::new().jobs([JobOpts::new("jobs")
                    .lease(Duration::from_secs(60))
                    .consumer(&B49JobConsumerConsumer)]),
            );
        let opts = trembita::RunOpts::local()
            .with_local_net(net.clone())
            .with_wait_ready(trembita::ReadyOpts::default());
        let boot = builder
            .boot_for_test_with_consumers(opts)
            .await
            .expect("boot with consumers");
        boots.push(boot);
    }
    let apps: Vec<Arc<TrembitaApp>> = boots.iter().map(|b| Arc::clone(&b.app)).collect();
    (net, apps, boots)
}

#[tokio::test(start_paused = true)]
async fn b49_queued_job_completes_while_one_node_stopped() {
    let base = temp_base("b49-jobs");
    let (_net, apps, mut boots) = boot_three_node_apps_with_jobs(&base).await;
    advance(TICK_PERIOD).await;

    let leader = leader_app(&apps).await;
    leader
        .enqueue("jobs", b"rolling-upgrade-proof")
        .await
        .expect("enqueue");

    boots[1].app.shutdown();
    advance(Duration::from_millis(500)).await;

    eventually_async_default("b49 job processed with one node down", || async {
        B49_JOBS_DONE.load(Ordering::SeqCst) >= 1
    })
    .await;

    boots.remove(1);
    for boot in boots {
        boot.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(base);
}
