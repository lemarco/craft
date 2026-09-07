//! # Real-time sessions showcase (sticky actor sessions + WebSocket gateway)
//!
//! Demonstrates **login → `Set-Cookie` → session-protected HTTP** plus WebSocket on the same gateway.

mod debug;
mod gateway_session;

use std::env;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
use trembita::runtime::{UserActor, actor};
use trembita::{
    ActorGroupOpts, CookieConfig, Gateway, GatewayOpts, ReadyOpts, RequestCtx, RouteTable, RunOpts,
    TrembitaApp, TrembitaConfigure, TrembitaGatewayState, accept_websocket, routing_to_http_response,
};
use trembita_tools::showcase_common::{data_dir, display_addr};

use gateway_session::{SessionStore, session_gate};

const DATA_DIR_NAME: &str = "trembita-showcase-realtime";
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
        let node = env::var("TREMBITA_NODE_ID").unwrap_or_else(|_| "?".into());
        self.history.lock().unwrap().push(msg.clone());
        crate::debug::chat_message(&msg);
        println!("[chat node {node}] {msg}");
        std::future::ready(Ok(()))
    }
}

async fn handle_socket(
    stream: trembita::UpgradeStream,
    state: TrembitaGatewayState,
    session_key: String,
    mut handle: trembita::SessionHandle,
) {
    let _conn = state.track_connection();
    debug::session_open(&session_key, true);
    let mut ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
    let _ = ws
        .send(WsMessage::Text(format!("session open for {session_key}").into()))
        .await;

    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg {
            let text = text.to_string();
            let payload = trembita::proto::encode(&text).expect("encode chat msg");
            match handle.cast(payload).await {
                Ok(()) => {
                    debug::ws_message(&session_key, &text, true);
                    let _ = ws
                        .send(WsMessage::Text(format!("ok: {text}").into()))
                        .await;
                }
                Err(e) => {
                    debug::ws_message(&session_key, &text, false);
                    let _ = ws
                        .send(WsMessage::Text(format!("session error: {e}").into()))
                        .await;
                }
            }
        }
    }
}

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    let store = SessionStore::new();
    let gate = session_gate(store.clone());
    let login_state = state.clone();
    let login_gate = gate.clone();
    let login_store = store.clone();
    let chat_state = state.clone();
    let ws_state = state;

    Gateway::new(false)
        .dev_fallback_session(gate)
        .dev_fallback(
            RouteTable::new()
                .websocket("/ws", move |req| {
                    let st = ws_state.clone();
                    Box::pin(async move {
                        let handle = match st
                            .open_actor_session_parts(
                                "chat",
                                req.method(),
                                req.uri(),
                                req.headers(),
                                Some(SESSION_TTL),
                            )
                            .await
                        {
                            Ok(h) => h,
                            Err(err) => {
                                return routing_to_http_response(&err.into_http_response());
                            }
                        };
                        let session_key = handle.session_key().to_string();
                        debug::ws_connect(&session_key, true);
                        accept_websocket(req, move |stream| {
                            let st = st.clone();
                            async move { handle_socket(stream, st, session_key, handle).await }
                        })
                    })
                })
                .post_identity("/login", move |ctx: RequestCtx| {
                    let st = login_state.clone();
                    let g = login_gate.clone();
                    let s = login_store.clone();
                    async move { gateway_session::post_login(st, g, s, ctx).await }
                })
                .post_session("/chat", move |ctx: RequestCtx| {
                    let st = chat_state.clone();
                    async move { gateway_session::post_chat(st, ctx).await }
                })
                .get_session("/me", move |ctx: RequestCtx| {
                    async move { gateway_session::get_me(ctx).await }
                })
                .merge(state.app.ops_api().route_table()),
        )
}

fn server_builder() -> trembita::TrembitaAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway: std::net::SocketAddr = env::var("TREMBITA_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8294".into())
        .parse()
        .expect("gateway");
    TrembitaApp::builder()
        .actors::<ChatWorker>("chat", ActorGroupOpts::new(0))
        .configure(TrembitaConfigure {
            tick_period: Duration::from_millis(10),
            reconcile_period: Duration::from_millis(20),
            directory_publish_period: Duration::from_millis(20),
            ..TrembitaConfigure::default()
        })
        .data_dir(dir)
        .gateway(
            GatewayOpts::new(gateway)
                .identity(trembita::GatewayBearerIdentity::from_env())
                .surfaces(gateway_surfaces),
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
    println!("trembita showcase · real-time sessions (stateful actors)");
    println!("  listen   {}", env::var("TREMBITA_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
    if env::var("TREMBITA_GATEWAY").is_ok_and(|g| g != "-") {
        let gw = env::var("TREMBITA_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8294".into());
        let host = display_addr(&gw);
        println!("  websocket ws://{host}/ws?user=alice  (Bearer identity)");
        println!("  login     POST http://{host}/login  (Bearer + X-Trembita-User → Set-Cookie)");
        println!("  chat      POST http://{host}/chat   (session cookie)");
        println!("  me        GET  http://{host}/me      (session cookie)");
        println!("  ops       http://{host}/dashboard");
        let _ = CookieConfig::from_env("REALTIME", "sess");
    }
    if env::var("TREMBITA_JOIN_SEEDS").is_ok() {
        println!("  join     via TREMBITA_JOIN_SEEDS");
    } else {
        println!("  role     seed");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    println!("  trigger  ./trigger.sh alice hello");
    println!("  session  ./trigger-http.sh alice hello");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
