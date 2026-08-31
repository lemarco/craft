//! # Real-time sessions showcase (messaging **tier B** + sticky sessions)
//!
//! ```text
//!  Browser / websocat       WS Gateway (any node)       ActorSession            ChatWorker
//!       |    ws://…/ws?user=alice  |                            |                         |
//!       | ----------------------> | open session(user=alice)   |                         |
//!       |                         | --------------------------> | cast(message) --------> |
//!       | <---------------- ok ---|                            |                         |
//! ```
//!
//! **ActorSession** pins a user id to a concrete worker instance so consecutive
//! WebSocket messages hit the same in-memory `ChatWorker` (live chat, game room).
//!
//! Cluster mode: three **identical** nodes — each runs WS gateway + chat workers
//! (same binary on every VPS; connect to any node's WebSocket URL).
//!
//! ## Debug logs
//!
//! `RUST_LOG=showcase=debug` — WebSocket + session events on `target: "showcase"`.

mod debug;

use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use crafty::actor::{UserActor, remote_actor};
use crafty::{ActorGroupOpts, CraftyApp, CraftyAppBuilder, CraftyConfigure, CraftyGatewayState, GatewayOpts, ReadyOpts, RunOpts};
use crafty_actor::CastError;
use crafty_showcase_common::{data_dir, display_addr};

const DATA_DIR_NAME: &str = "crafty-showcase-realtime";

/// How long the directory keeps `(user → worker)` mapping without traffic.
const SESSION_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, serde::Deserialize)]
struct ConnectQuery {
    user: String,
    /// Optional shared secret — set `GATEWAY_TOKEN` on server to require `?token=…`.
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

/// In-memory chat history per worker instance — **hot state**, lost on crash.
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
        let node = env::var("CRAFTY_NODE_ID").unwrap_or_else(|_| "?".into());
        self.history.lock().unwrap().push(msg.clone());
        crate::debug::chat_message(&msg);
        println!("[chat node {node}] {msg}");
        std::future::ready(Ok(()))
    }
}

fn gateway_token_ok(token: Option<&str>) -> bool {
    match env::var("GATEWAY_TOKEN") {
        Ok(expected) if !expected.is_empty() => token == Some(expected.as_str()),
        _ => true,
    }
}

fn open_chat_session(app: &CraftyApp, user: &str) -> Option<crafty_actor::ActorSession> {
    app.session_str("chat", user, Some(SESSION_TTL))
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
    let token_ok = gateway_token_ok(query.token.as_deref());
    debug::ws_connect(&query.user, token_ok);
    if !token_ok {
        return (StatusCode::UNAUTHORIZED, "invalid gateway token").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state, query.user))
        .into_response()
}

async fn handle_socket(mut socket: WebSocket, state: CraftyGatewayState, user: String) {
    let mut session = open_chat_session(state.app.as_ref(), &user);
    debug::session_open(&user, session.is_some());
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
                    debug::ws_message(&user, &text, true);
                    let _ = socket
                        .send(Message::Text(format!("ok: {text}").into()))
                        .await;
                }
                Err(e) => {
                    debug::ws_message(&user, &text, false);
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
                debug::session_reconnect(user, attempt + 1);
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

fn base_builder() -> CraftyAppBuilder {
    CraftyApp::builder()
        .actors::<ChatWorker>("chat", ActorGroupOpts::new(0))
        .configure(CraftyConfigure {
            tick_period: Duration::from_millis(10),
            reconcile_period: Duration::from_millis(20),
            directory_publish_period: Duration::from_millis(20),
            ..CraftyConfigure::default()
        })
        .http_routes(gateway_routes)
}

fn server_builder() -> crafty::CraftyAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8294".into())
        .parse()
        .expect("gateway");
    base_builder()
        .data_dir(dir)
        .configure(CraftyConfigure {
            admin_addr: Some("127.0.0.1:9380".parse().expect("admin")),
            ..CraftyConfigure::default()
        })
        .gateway(gateway, GatewayOpts { jobs_api: false, ..Default::default() })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    server_builder()
        .run(RunOpts::default().with_wait_ready(ReadyOpts::default()))
        .await?;
    debug::shutdown();
    Ok(())
}

fn print_banner() {
    println!("crafty showcase · real-time sessions (tier B)");
    println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
    if env::var("CRAFTY_GATEWAY").is_ok_and(|g| g != "-") {
        let gw = env::var("CRAFTY_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8294".into());
        println!("  websocket ws://{}/ws?user=alice", display_addr(&gw));
    }
    if let Ok(admin) = env::var("CRAFTY_ADMIN") {
        if admin != "-" {
            println!("  admin    http://{}/dashboard", display_addr(&admin));
        }
    }
    if env::var("CRAFTY_JOIN_SEEDS").is_ok() {
        println!("  join     via CRAFTY_JOIN_SEEDS");
    } else {
        println!("  role     seed");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    println!("  trigger  ./trigger.sh alice hello");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
