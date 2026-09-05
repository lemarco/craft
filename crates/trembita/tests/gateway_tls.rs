//! Gateway HTTPS / WSS when [`GatewayOpts::tls`] is configured.

#![allow(clippy::large_futures)] // boot_echo_app future grows with product builder surface

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use http::StatusCode;
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::Connector;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
use trembita::{
    ActorGroupOpts, GatewayOpts, SessionHandle, TrembitaApp, TrembitaConfigure,
    TrembitaGatewayState, spawn_gateway,
};
use trembita_http::{
    Gateway, Response, RouteTable, UpgradeStream, accept_websocket, routing_to_http_response,
};
use trembita_runtime::{UserActor, actor};
use trembita_test_support::{
    advance, boot_local_app, eventually_default, wait_for_trembita_app_leader,
};

struct FixedToken;

impl trembita::GatewayIdentity for FixedToken {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(
        &self,
        req: &trembita::GatewayRequest<'_>,
    ) -> Result<String, trembita::IdentityError> {
        let user = req
            .query("user")
            .ok_or(trembita::IdentityError::Unauthorized)?;
        let token = req
            .query("token")
            .ok_or(trembita::IdentityError::Unauthorized)?;
        if token == "secret" {
            Ok(user)
        } else {
            Err(trembita::IdentityError::Unauthorized)
        }
    }
}

#[derive(Debug)]
struct EchoErr;
impl std::fmt::Display for EchoErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("echo")
    }
}
impl std::error::Error for EchoErr {}

struct EchoWorker;

#[actor]
impl UserActor for EchoWorker {
    type Config = u32;
    type Message = String;
    type Error = EchoErr;

    fn start(_seed: Self::Config) -> Result<Self, EchoErr> {
        Ok(Self)
    }

    fn handle(
        &mut self,
        _msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), EchoErr>> + Send {
        std::future::ready(Ok(()))
    }
}

async fn handle_socket(
    stream: UpgradeStream,
    state: TrembitaGatewayState,
    mut handle: SessionHandle,
) {
    let _conn = state.track_connection();
    let mut ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg {
            let payload = trembita::proto::encode(&text.to_string()).expect("encode");
            if handle.cast(payload).await.is_ok() {
                let _ = ws.send(WsMessage::Text(format!("ok: {text}").into())).await;
            }
        }
    }
}

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    let ws_state = state.clone();
    Gateway::new(false).dev_fallback(
        RouteTable::new()
            .get("/ping", |_| async { Ok(Response::status(StatusCode::OK)) })
            .websocket("/ws", move |req| {
                let st = ws_state.clone();
                Box::pin(async move {
                    match st
                        .open_actor_session_parts(
                            "echo",
                            req.method(),
                            req.uri(),
                            req.headers(),
                            Some(Duration::from_secs(60)),
                        )
                        .await
                    {
                        Ok(handle) => accept_websocket(req, move |stream| {
                            let st = st.clone();
                            async move { handle_socket(stream, st, handle).await }
                        }),
                        Err(err) => routing_to_http_response(err.into_http_response()),
                    }
                })
            }),
    )
}

fn mint_gateway_tls_files() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    CertificateDer<'static>,
) {
    let dir = tempfile::tempdir().unwrap();
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let der = rustls_pemfile::certs(&mut cert.cert.pem().as_bytes())
        .next()
        .expect("cert pem")
        .expect("parse cert");
    (dir, cert_path, key_path, der)
}

async fn https_get(
    addr: std::net::SocketAddr,
    path: &str,
    trust_anchor: &CertificateDer<'static>,
) -> (u16, String) {
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, RootCertStore};
    use tokio_rustls::TlsConnector;

    let mut roots = RootCertStore::empty();
    roots.add(trust_anchor.clone()).unwrap();
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TcpStream::connect(addr).await.expect("connect gateway tls");
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector
        .connect(server_name, stream)
        .await
        .expect("tls handshake");
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    tls.write_all(req.as_bytes()).await.expect("send req");
    let mut raw = Vec::new();
    tls.read_to_end(&mut raw).await.expect("read resp");
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

fn rustls_connector(trust_anchor: &CertificateDer<'static>) -> Connector {
    use rustls::{ClientConfig, RootCertStore};

    let mut roots = RootCertStore::empty();
    roots.add(trust_anchor.clone()).unwrap();
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Connector::Rustls(Arc::new(config))
}

async fn boot_echo_app(base: &std::path::Path) -> Arc<TrembitaApp> {
    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(base)
                .actors::<EchoWorker>("echo", ActorGroupOpts::new(0))
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    reconcile_period: Duration::from_millis(20),
                    directory_publish_period: Duration::from_millis(20),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;
    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(500)).await;
    eventually_default("echo worker in directory", || {
        !app.cluster_ref("echo").is_empty()
    })
    .await;
    app
}

fn free_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test(start_paused = true)]
async fn gateway_serves_https_when_tls_configured() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-https-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_echo_app(&base).await;
    let (_dir, cert_path, key_path, trust) = mint_gateway_tls_files();
    let addr = free_port();

    let config = GatewayOpts::new(addr)
        .tls(cert_path, key_path)
        .surfaces(gateway_surfaces)
        .build_config();
    let _handle = spawn_gateway(Arc::clone(&app), config)
        .await
        .expect("spawn gateway tls");

    let (status, _body) = https_get(addr, "/ping", &trust).await;
    assert_eq!(status, 200);

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(start_paused = true)]
async fn websocket_gateway_wss_casts_to_worker() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-wss-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_echo_app(&base).await;
    let (_dir, cert_path, key_path, trust) = mint_gateway_tls_files();
    let addr = free_port();

    let config = GatewayOpts::new(addr)
        .identity(FixedToken)
        .tls(cert_path, key_path)
        .surfaces(gateway_surfaces)
        .build_config();
    let _handle = spawn_gateway(Arc::clone(&app), config)
        .await
        .expect("spawn gateway tls");

    let url = format!("wss://{addr}/ws?user=alice&token=secret");
    let connector = rustls_connector(&trust);
    let (mut ws, _) = connect_async_tls_with_config(url, None, false, Some(connector))
        .await
        .expect("wss connect");
    ws.send(WsMessage::Text("hello".into())).await.unwrap();
    let reply = ws.next().await.expect("frame").expect("ok");
    assert_eq!(reply.into_text().unwrap(), "ok: hello");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
