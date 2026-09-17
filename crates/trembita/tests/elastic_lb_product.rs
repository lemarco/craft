//! B-34 — product LB pool + cluster session + PerNode cap (in-process; docker in `e2e/elastic_lb.sh`).

#![allow(clippy::large_futures)]

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use trembita::{
    AppManifest, CapError, CapGroup, CapManifest, CapOp, CapRequest, ClusterSessionSecret,
    CookieConfig, GatewayOpts, OpCtx, ReadyOpts, Route, RouteTable, TrembitaApp,
    cap_register_chain, cluster_session_gate,
};
use trembita_http::{Gateway, RequestCtx, Response};
use trembita_proto::NodeId;
use trembita_test_facade::spawn_test_gateway;
use trembita_test_support::{
    TICK_PERIOD, advance, eventually_async_default, fast_raft_config_with_seed,
};

#[derive(Default)]
struct Marker;

#[derive(Default, serde::Deserialize, serde::Serialize)]
struct WhoAmI;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct NodeReply {
    node_id: u64,
}

impl CapRequest for WhoAmI {
    const GROUP: &'static str = "e2e_pool";
    const OP: &'static str = "whoami";
    const QUEUE_STREAM: &'static str = "e2e_pool.whoami";
    const EVENT_TOPIC: &'static str = "e2e_pool.whoami";
    const EVENT_SUBSCRIPTION: &'static str = "e2e_pool.whoami.cap";
    type Reply = NodeReply;
}

async fn whoami_run(
    _msg: WhoAmI,
    ctx: OpCtx<'_>,
    _state: &mut Marker,
) -> Result<NodeReply, CapError> {
    let app = ctx.app().ok_or_else(|| CapError::domain("missing app"))?;
    Ok(NodeReply {
        node_id: app.node_id().0,
    })
}

fn whoami_register(group: CapGroup<Marker>) -> CapGroup<Marker> {
    group.op(
        CapOp::for_request_async(|req, ctx, state| Box::pin(whoami_run(req, ctx, state)))
            .routes([Route::Inline]),
    )
}

fn cap_manifest() -> CapManifest {
    CapManifest::new().group(cap_register_chain!(
        CapGroup::<Marker>::for_cap::<WhoAmI>(),
        whoami_register,
    ))
}

async fn http_raw(
    addr: SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
) -> (u16, String, Vec<(String, String)>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in extra_headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
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
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body, headers)
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

fn app_builder(id: NodeId, members: [NodeId; 4], base: &Path) -> trembita::TrembitaAppBuilder {
    let mut builder = TrembitaApp::builder();
    {
        let inner = builder.inner_mut();
        *inner = trembita_assembly::builder::TrembitaClusterBuilder::new(
            id,
            trembita::cluster::EmptyStateMachine,
        )
        .members(members)
        .data_dir(base.join(format!("node-{}", id.0)))
        .raft_config(fast_raft_config_with_seed(34))
        .tick_period(TICK_PERIOD)
        .reconcile_period(Duration::from_millis(15))
        .directory_publish_period(Duration::from_millis(15))
        .shard_count(64);
    }
    builder.manifest(AppManifest::new().capabilities(cap_manifest()))
}

async fn boot_four_node_apps(base: &Path) -> Vec<Arc<TrembitaApp>> {
    let members = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let net = trembita::net::LocalNetwork::new();
    let mut apps = Vec::new();
    for &id in &members {
        let opts = trembita::RunOpts::local()
            .with_local_net(net.clone())
            .with_wait_ready(ReadyOpts::default());
        let app = app_builder(id, members, base)
            .boot_for_test(opts)
            .await
            .expect("boot");
        apps.push(app);
    }
    apps
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
                        let user = ctx.query_param("user").unwrap_or("anon").to_string();
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

async fn await_leader(apps: &[Arc<TrembitaApp>]) {
    for _ in 0..200 {
        for app in apps {
            if app.cluster().is_leader().await {
                return;
            }
        }
        advance(TICK_PERIOD).await;
    }
    panic!("no leader");
}

async fn leader_app(apps: &[Arc<TrembitaApp>]) -> Arc<TrembitaApp> {
    await_leader(apps).await;
    for app in apps {
        if app.cluster().is_leader().await {
            return Arc::clone(app);
        }
    }
    Arc::clone(&apps[0])
}

fn ready_node_id(body: &str) -> u64 {
    let v: serde_json::Value = serde_json::from_str(body).expect("ready json");
    v["node_id"].as_u64().expect("node_id")
}

#[tokio::test(start_paused = true)]
async fn b34_lb_pool_distinct_ready_on_four_nodes() {
    let base = temp_base("b34-lb");
    let apps = boot_four_node_apps(&base).await;
    await_leader(&apps).await;

    let mut addrs = Vec::new();
    for app in &apps {
        let addr = spawn_test_gateway(
            app,
            GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                .surfaces(|state| {
                    Gateway::new(false).dev_fallback(state.app.ops_api().route_table())
                })
                .build_config(),
        )
        .await;
        addrs.push(addr);
    }

    let mut seen = HashSet::new();
    for i in 0..32 {
        let addr = addrs[i % addrs.len()];
        let (status, body, _) = http_raw(addr, "GET", "/ready", &[]).await;
        assert_eq!(status, 200, "{body}");
        seen.insert(ready_node_id(&body));
    }
    assert_eq!(seen.len(), 4, "expected four backends in simulated LB pool");

    for app in &apps {
        app.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn b34_cluster_session_cookie_valid_on_peer_gateway() {
    let secret = ClusterSessionSecret::from_bytes(b"e2e-elastic-secret-16b").unwrap();
    let config_a = session_gateway(secret.clone()).build_config();
    let config_b = session_gateway(secret).build_config();

    let base = temp_base("b34-session");
    let apps = boot_four_node_apps(&base).await;
    await_leader(&apps).await;
    let addr_a = spawn_test_gateway(&apps[0], config_a).await;
    let addr_b = spawn_test_gateway(&apps[1], config_b).await;

    let (status, _, headers) = http_raw(addr_a, "GET", "/login?user=lbproof", &[]).await;
    assert_eq!(status, 200);
    let set_cookie = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("set-cookie"))
        .map(|(_, v)| v.clone())
        .expect("Set-Cookie");
    let cookie_pair = set_cookie.split(';').next().expect("cookie pair");

    let (me_status, body, _) = http_raw(addr_b, "GET", "/me", &[("Cookie", cookie_pair)]).await;
    assert_eq!(me_status, 200);
    assert_eq!(body.trim(), "lbproof");

    for app in &apps {
        app.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn b34_per_node_cap_directory_spans_four_hosts() {
    let base = temp_base("b34-cap");
    let apps = boot_four_node_apps(&base).await;
    let leader = leader_app(&apps).await;

    eventually_async_default("e2e_pool pool size 4", || async {
        leader.cluster_ref("e2e_pool").len() == 4
    })
    .await;

    let nodes = leader.cluster_ref("e2e_pool").nodes();
    assert_eq!(
        nodes.len(),
        4,
        "PerNode pool should span four cluster members"
    );
    let expected: HashSet<_> = (1..=4).map(NodeId).collect();
    assert_eq!(
        nodes.iter().copied().collect::<HashSet<_>>(),
        expected,
        "directory should list every cluster member as a cap host"
    );

    let plan = leader.scale_plan();
    assert_eq!(plan.capability_groups.len(), 1);
    assert_eq!(plan.capability_groups[0].group, "e2e_pool");
    assert_eq!(plan.capability_groups[0].hosts, "PerNode");

    for app in &apps {
        app.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn b34_each_backend_ready_node_id_is_unique() {
    let base = temp_base("b34-ready-ids");
    let apps = boot_four_node_apps(&base).await;
    await_leader(&apps).await;

    let mut ids = HashSet::new();
    for app in &apps {
        let addr = spawn_test_gateway(
            app,
            GatewayOpts::new("127.0.0.1:0".parse().unwrap())
                .surfaces(|state| {
                    Gateway::new(false).dev_fallback(state.app.ops_api().route_table())
                })
                .build_config(),
        )
        .await;
        let (status, body, _) = http_raw(addr, "GET", "/ready", &[]).await;
        assert_eq!(status, 200, "{body}");
        ids.insert(ready_node_id(&body));
    }
    assert_eq!(
        ids.len(),
        4,
        "each of four processes must expose its own node_id on /ready for LB registration"
    );

    for app in &apps {
        app.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn b34_cluster_session_rejects_peer_cookie_when_secret_differs() {
    let secret_a = ClusterSessionSecret::from_bytes(b"e2e-elastic-secret-16b").unwrap();
    let secret_b = ClusterSessionSecret::from_bytes(b"other-secret-16bytes!!").unwrap();
    let config_a = session_gateway(secret_a).build_config();
    let config_b = session_gateway(secret_b).build_config();

    let base = temp_base("b34-session-mismatch");
    let apps = boot_four_node_apps(&base).await;
    await_leader(&apps).await;
    let addr_a = spawn_test_gateway(&apps[0], config_a).await;
    let addr_b = spawn_test_gateway(&apps[1], config_b).await;

    let (status, _, headers) = http_raw(addr_a, "GET", "/login?user=lbproof", &[]).await;
    assert_eq!(status, 200);
    let set_cookie = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("set-cookie"))
        .map(|(_, v)| v.clone())
        .expect("Set-Cookie");
    let cookie_pair = set_cookie.split(';').next().expect("cookie pair");

    let (me_status, _, _) = http_raw(addr_b, "GET", "/me", &[("Cookie", cookie_pair)]).await;
    assert_ne!(
        me_status, 200,
        "peer gateway must reject cookies signed with a different cluster secret"
    );

    for app in &apps {
        app.shutdown();
    }
    let _ = std::fs::remove_dir_all(base);
}
