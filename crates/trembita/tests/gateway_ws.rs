//! WebSocket gateway end-to-end: identity → session → cast.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, Method, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use trembita::cluster::build_gateway_router;
use trembita::{
    ActorGroupOpts, GatewayOpts, SessionHandle, TrembitaApp, TrembitaConfigure,
    TrembitaGatewayState,
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

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<TrembitaGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let handle = match state
        .open_actor_session_parts(
            "echo",
            &method,
            &uri,
            &headers,
            Some(Duration::from_secs(60)),
        )
        .await
    {
        Ok(h) => h,
        Err(err) => return err.into_response(),
    };
    let session_key = handle.session_key().to_string();
    ws.on_upgrade(move |socket| async move {
        handle_socket(socket, state, session_key, handle).await;
    })
    .into_response()
}

async fn handle_socket(
    mut socket: WebSocket,
    state: TrembitaGatewayState,
    _session_key: String,
    mut handle: SessionHandle,
) {
    let _conn = state.track_connection();
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            let payload = trembita::proto::encode(&text.to_string()).expect("encode");
            if handle.cast(payload).await.is_ok() {
                let _ = socket
                    .send(Message::Text(format!("ok: {text}").into()))
                    .await;
            }
        }
    }
}

fn gateway_routes(state: TrembitaGatewayState) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
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

    let config = GatewayOpts::new("127.0.0.1:0".parse().unwrap())
        .identity(FixedToken)
        .routes(gateway_routes)
        .build_config();

    let router = build_gateway_router(Arc::clone(&app), config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let url = format!("ws://{addr}/ws?user=alice&token=secret");
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("ws connect");
    ws.send(WsMessage::Text("hello".into())).await.unwrap();
    let reply = ws.next().await.expect("frame").expect("ok");
    assert_eq!(reply.into_text().unwrap(), "ok: hello");

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
