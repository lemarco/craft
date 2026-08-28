//! WebSocket gateway + sticky [`ActorSession`] chat workers (B-04).
//!
//! **Production split:** run the same binary with `GATEWAY=1` on edge VPS (HTTP/WS only)
//! and `GATEWAY=0` on worker VPS (Raft + actors). This example runs both roles in one
//! process for local demo.
//!
//! Run: `cargo run -p crafty --example websocket_gateway --features http-jobs`

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use crafty::actor::{UserActor, remote_actor};
use crafty::net::LocalNetwork;
use crafty::{CraftyApp, NodeId};

#[derive(Debug, serde::Deserialize)]
struct ConnectQuery {
    user: String,
}

#[derive(Clone)]
struct GatewayState {
    app: Arc<CraftyApp>,
}

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

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<ConnectQuery>,
    State(state): State<GatewayState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, query.user))
}

async fn handle_socket(mut socket: WebSocket, state: GatewayState, user: String) {
    let session = state
        .app
        .session_keyed("chat", &user, Some(Duration::from_secs(3600)));
    let Some(session) = session else {
        let _ = socket
            .send(Message::Text("no chat worker available".into()))
            .await;
        return;
    };

    let _ = socket
        .send(Message::Text(format!("session open for {user}").into()))
        .await;

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg {
            let text = text.to_string();
            let payload = crafty::proto::encode(&text).expect("encode chat msg");
            match state
                .app
                .cluster()
                .messaging()
                .cast_session(&session, payload)
                .await
            {
                Ok(()) => {
                    let _ = socket
                        .send(Message::Text(format!("ok: {text}").into()))
                        .await;
                }
                Err(e) => {
                    let _ = socket
                        .send(Message::Text(format!("session error: {e}").into()))
                        .await;
                    if format!("{e}").contains("NoTarget") {
                        let _ = socket
                            .send(Message::Text("reconnect to open a new session".into()))
                            .await;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let gateway_mode = std::env::var("GATEWAY").ok().as_deref() == Some("1");

    let net = LocalNetwork::new();
    let app = Arc::new(
        CraftyApp::builder(NodeId(1))
            .members([NodeId(1)])
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20))
            .directory_publish_period(Duration::from_millis(20))
            .manage_auto::<ChatWorker>("chat", 0)
            .start_local(&net)
            .await,
    );

    if gateway_mode {
        println!("gateway mode: WS on :3000 (workers run elsewhere in production)");
    } else {
        println!("worker mode: cluster + local WS on :3000 for demo");
    }

    let gateway = GatewayState {
        app: Arc::clone(&app),
    };
    let router = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(gateway);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind gateway");
    println!("WebSocket gateway listening on ws://127.0.0.1:3000/ws?user=alice");

    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });

    tokio::signal::ctrl_c().await.expect("ctrl-c");
    app.cluster().shutdown();
}
