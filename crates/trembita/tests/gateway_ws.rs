//! WebSocket gateway end-to-end: identity → session → cast.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
use trembita::{
    ActorGroupOpts, GatewayOpts, SessionHandle, TrembitaApp, TrembitaConfigure,
    TrembitaGatewayState,
};
use trembita_http::{
    Gateway, RouteTable, UpgradeStream, accept_websocket, routing_to_http_response,
};
use trembita_runtime::{UserActor, actor};
use trembita_test_support::{
    advance, boot_local_app, eventually_default, spawn_test_gateway, wait_for_trembita_app_leader,
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
    let ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
    let (mut ws, _) = ws.split();
    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg {
            let payload = trembita::proto::encode(&text).expect("encode");
            if handle.cast(payload).await.is_ok() {
                let _ = ws.send(WsMessage::Text(format!("ok: {text}").into())).await;
            }
        }
    }
}

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    let st = state;
    Gateway::new(false).dev_fallback(RouteTable::new().websocket("/ws", move |req| {
        let st = st.clone();
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
                Err(err) => routing_to_http_response(
                    err.into_http_response()
                        .finalize()
                        .unwrap_or_else(|e| trembita_http::Response::text(e.status(), e.message())),
                ),
            }
        })
    }))
}

#[tokio::test(start_paused = true)]
async fn websocket_gateway_casts_to_worker() {
    let base = std::env::temp_dir().join(format!(
        "trembita-gateway-ws-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
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

    let addr = spawn_test_gateway(
        &app,
        GatewayOpts::new("127.0.0.1:0".parse().unwrap())
            .identity(FixedToken)
            .surfaces(gateway_surfaces)
            .build_config(),
    )
    .await;

    let url = format!("ws://127.0.0.1:{}/ws?user=alice&token=secret", addr.port());
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");
    ws.send(WsMessage::Text("hello".into())).await.unwrap();
    let reply = ws.next().await.expect("frame").expect("ok");
    assert_eq!(reply.into_text().unwrap(), "ok: hello");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
