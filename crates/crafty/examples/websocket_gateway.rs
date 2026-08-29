//! WebSocket gateway + sticky [`ActorSession`] chat workers (B-04).
//!
//! **Production split:** run the same binary with `GATEWAY=1` on edge VPS (HTTP/WS only)
//! and `GATEWAY=0` on worker VPS (Raft + actors). This example runs both roles in one
//! process for local demo.
//!
//! Optional auth: set `GATEWAY_TOKEN` and pass `?token=…` on connect.
//!
//! Run: `cargo run -p crafty --example websocket_gateway --features http-jobs`

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use crafty::actor::{UserActor, remote_actor};
use crafty::net::LocalNetwork;
use crafty::{CraftyApp, CraftyGatewayState, NodeId};
use crafty_actor::CastError;

const SESSION_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, serde::Deserialize)]
struct ConnectQuery {
    user: String,
    /// Optional shared secret when `GATEWAY_TOKEN` is set.
    token: Option<String>,
}

#[derive(Debug)]
struct ChatErr;
impl std::fmt::Display for ChatErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("chat worker error")
    }
}
impl std::error::Error for ChatErr {}

/// Chat worker — in production checkpoint message history to [`ActorStateStore`]
/// after each handle so gateway reconnect + session migration can recover state.
struct ChatWorker {
    history: Mutex<Vec<String>>,
}

#[remote_actor]
impl UserActor for ChatWorker {
    type Config = u32;
    type Message = String;
    type Error = ChatErr;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self {
            history: Mutex::new(Vec::new()),
        })
    }

    fn handle(
        &mut self,
        msg: Self::Message,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        self.history.lock().unwrap().push(msg);
        std::future::ready(Ok(()))
    }
}

fn gateway_token_ok(token: Option<&str>) -> bool {
    match std::env::var("GATEWAY_TOKEN") {
        Ok(expected) if !expected.is_empty() => token == Some(expected.as_str()),
        _ => true,
    }
}

fn open_chat_session(app: &CraftyApp, user: &str) -> Option<crafty_actor::ActorSession> {
    let key = user.to_string();
    app.session("chat", &key, Some(SESSION_TTL))
}

fn session_recoverable(err: &CastError) -> bool {
    matches!(err, CastError::NoTarget(_))
        || format!("{err}").contains("NoTarget")
        || format!("{err}").contains("expired")
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<ConnectQuery>,
    State(state): State<CraftyGatewayState>,
) -> Response {
    if !gateway_token_ok(query.token.as_deref()) {
        return (StatusCode::UNAUTHORIZED, "invalid gateway token").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state, query.user))
        .into_response()
}

async fn handle_socket(mut socket: WebSocket, state: CraftyGatewayState, user: String) {
    let mut session = open_chat_session(state.app.as_ref(), &user);
    if session.is_none() {
        let _ = socket
            .send(Message::Text("no chat worker available".into()))
            .await;
        return;
    }

    let _ = socket
        .send(Message::Text(format!("session open for {user}").into()))
        .await;

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            let text = text.to_string();
            let payload = crafty::proto::encode(&text).expect("encode chat msg");
            match cast_with_reconnect(&state, &user, &mut session, payload).await {
                Ok(()) => {
                    let _ = socket
                        .send(Message::Text(format!("ok: {text}").into()))
                        .await;
                }
                Err(e) => {
                    let _ = socket
                        .send(Message::Text(format!("session error: {e}").into()))
                        .await;
                }
            }
        }
    }
}

async fn cast_with_reconnect(
    state: &CraftyGatewayState,
    user: &str,
    session: &mut Option<crafty_actor::ActorSession>,
    payload: Vec<u8>,
) -> Result<(), CastError> {
    for attempt in 0..2 {
        let Some(active) = session.as_ref() else {
            *session = open_chat_session(state.app.as_ref(), user);
            if session.is_none() {
                return Err(CastError::NoTarget("chat".into()));
            }
            continue;
        };
        match state.app.cast_session(active, payload.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt == 0 && session_recoverable(&e) => {
                *session = open_chat_session(state.app.as_ref(), user);
                if session.is_none() {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
    Err(CastError::NoTarget("chat".into()))
}

fn gateway_routes(app: Arc<CraftyApp>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(CraftyGatewayState { app })
}

#[tokio::main]
async fn main() {
    let gateway_mode = std::env::var("GATEWAY").ok().as_deref() == Some("1");

    let net = LocalNetwork::new();
    let builder = CraftyApp::builder(NodeId(1))
        .members([NodeId(1)])
        .tick_period(Duration::from_millis(10))
        .reconcile_period(Duration::from_millis(20))
        .directory_publish_period(Duration::from_millis(20))
        .manage_auto::<ChatWorker>("chat", 0)
        .gateway_addr("127.0.0.1:3000".parse().expect("gateway addr"))
        .gateway_jobs_api(false)
        .http_routes(gateway_routes);

    if gateway_mode {
        println!("gateway mode: WS on :3000 (workers run elsewhere in production)");
    } else {
        println!("worker mode: cluster + local WS on :3000 for demo");
    }

    let app = builder.start_local_shared(&net).await;
    println!("WebSocket gateway listening on ws://127.0.0.1:3000/ws?user=alice");

    tokio::signal::ctrl_c().await.expect("ctrl-c");
    app.cluster().shutdown();
}
