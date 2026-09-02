//! # Real-time sessions showcase (sticky actor sessions + WebSocket gateway)
//!
//! WebSocket **and** authenticated HTTP on the same gateway identity.

mod debug;
mod gateway_http;

use std::env;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use crafty::actor::{UserActor, actor};
use crafty::{
    ActorGroupOpts, CraftyApp, CraftyConfigure, CraftyGatewayState, GatewayBearerIdentity,
    GatewayOpts, ReadyOpts, RunOpts,
};
use crafty_showcase_common::{data_dir, display_addr};

const DATA_DIR_NAME: &str = "crafty-showcase-realtime";
const SESSION_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug)]
struct ChatErr;
impl std::fmt::Display for ChatErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("chat worker error")
    }
}
impl std::error::Error for ChatErr {}

struct ChatWorker {
    history: Mutex<Vec<String>>,
}

#[actor]
impl UserActor for ChatWorker {
    type Config = u32;
    type Message = String;
    type Error = ChatErr;

    fn start(_seed: Self::Config) -> Result<Self, ChatErr> {
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

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<CraftyGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let handle = match state
        .open_actor_session_parts("chat", &method, &uri, &headers, Some(SESSION_TTL))
        .await
    {
        Ok(h) => h,
        Err(err) => return err.into_response(),
    };
    let session_key = handle.session_key().to_string();
    debug::ws_connect(&session_key, true);

    ws.on_upgrade(move |socket| async move {
        handle_socket(socket, state, session_key, handle).await;
    })
    .into_response()
}

async fn handle_socket(
    mut socket: WebSocket,
    state: CraftyGatewayState,
    session_key: String,
    mut handle: crafty::SessionHandle,
) {
    let _conn = state.track_connection();
    debug::session_open(&session_key, true);
    let _ = socket
        .send(Message::Text(format!("session open for {session_key}").into()))
        .await;

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            let text = text.to_string();
            let payload = crafty::proto::encode(&text).expect("encode chat msg");
            match handle.cast(payload).await {
                Ok(()) => {
                    debug::ws_message(&session_key, &text, true);
                    let _ = socket
                        .send(Message::Text(format!("ok: {text}").into()))
                        .await;
                }
                Err(e) => {
                    debug::ws_message(&session_key, &text, false);
                    let _ = socket
                        .send(Message::Text(format!("session error: {e}").into()))
                        .await;
                }
            }
        }
    }
    let _ = state;
}

fn gateway_routes(state: CraftyGatewayState) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/chat", post(gateway_http::post_chat))
        .route("/me", get(gateway_http::get_me))
        .with_state(state)
}

fn server_builder() -> crafty::CraftyAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8294".into())
        .parse()
        .expect("gateway");
    CraftyApp::builder()
        .actors::<ChatWorker>("chat", ActorGroupOpts::new(0))
        .configure(CraftyConfigure {
            tick_period: Duration::from_millis(10),
            reconcile_period: Duration::from_millis(20),
            directory_publish_period: Duration::from_millis(20),
            ..CraftyConfigure::default()
        })
        .data_dir(dir)
        .configure(CraftyConfigure {
            admin_addr: Some("127.0.0.1:9380".parse().expect("admin")),
            ..CraftyConfigure::default()
        })
        .gateway(
            GatewayOpts::new(gateway)
                .identity(GatewayBearerIdentity::from_env())
                .protect_product_apis(true)
                .routes(gateway_routes),
        )
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
    println!("crafty showcase · real-time sessions (stateful actors)");
    println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
    if env::var("CRAFTY_GATEWAY").is_ok_and(|g| g != "-") {
        let gw = env::var("CRAFTY_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8294".into());
        let host = display_addr(&gw);
        println!("  websocket ws://{host}/ws?user=alice");
        println!("  http chat POST http://{host}/chat  (Bearer + X-Crafty-User or ?user=)");
        println!("  http me    GET  http://{host}/me?user=alice");
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
    println!("  http     ./trigger-http.sh alice hello");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
