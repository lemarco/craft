//! # Real-time sessions showcase (capability + sticky sessions + WebSocket)
//!
//! Demonstrates **login → `Set-Cookie` → session-protected HTTP** plus WebSocket on the same gateway.
//! Chat lines use [`capabilities::chat`] (`Route::Session`) — not app `UserActor` code.

mod capabilities;
mod debug;
mod domain;
mod gateway_session;

use std::env;
use std::time::Duration;

use trembita::{
    AppManifest, AuthMode, CookieConfig, Gateway,
    GatewayOpts, RequestCtx, RouteTable, TrembitaApp, TrembitaConfigure, TrembitaGatewayState,
    WsMessage,
    futures_util::{SinkExt, StreamExt}, mount_sticky_websocket, server_stream,
};
use trembita_tools::showcase_common::{
    data_dir, display_addr, http_bind_display, http_bind_from_env, http_disabled,
};

use capabilities::chat::Append;
use gateway_session::{SessionStore, session_gate};

const DATA_DIR_NAME: &str = "trembita-showcase-realtime";
const SESSION_TTL: Duration = Duration::from_secs(3600);

async fn handle_sticky_ws(sticky: trembita::StickyWs) {
    let session_key = sticky.session_key.clone();
    debug::ws_connect(&session_key, true);
    let mut ws = server_stream(sticky.stream).await;
    let _ = ws
        .send(WsMessage::Text(format!("session open for {session_key}").into()))
        .await;

    let mut handle = sticky.handle;
    let log_key = session_key.clone();
    while let Some(Ok(msg)) = ws.next().await {
        if let WsMessage::Text(text) = msg {
            let text = text.to_string();
            match handle
                .fire_cap(Append {
                    text: text.clone(),
                })
                .await
            {
                Ok(()) => {
                    debug::ws_message(&log_key, &text, true);
                    if ws
                        .send(WsMessage::Text(format!("ok: {text}").into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => {
                    debug::ws_message(&log_key, &text, false);
                    if ws
                        .send(WsMessage::Text(format!("session error").into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
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
    let ops = state.app.ops_api().route_table();
    let ws_state = state;

    let mut table = RouteTable::new();
    table = mount_sticky_websocket::<Append>(
        table,
        "/ws",
        AuthMode::Identity,
        ws_state,
        Some(SESSION_TTL),
        |sticky| Box::pin(handle_sticky_ws(sticky)),
    );

    Gateway::new(false)
        .dev_fallback_session(gate)
        .dev_fallback(
            table
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
                .merge(ops),
        )
}

fn server_builder() -> trembita::TrembitaAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway = http_bind_from_env("127.0.0.1:8290");
    TrembitaApp::builder()
        .manifest(AppManifest::new().capabilities(capabilities::chat::manifest()))
        .configure(
            TrembitaConfigure::default()
                .with_local_gateway_apis()
                .with_data_dir(dir)
                .with_tick_period(Duration::from_millis(10))
                .with_reconcile_period(Duration::from_millis(20))
                .with_directory_publish_period(Duration::from_millis(20)),
        )
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
        .run()
        .await?;
    debug::shutdown();
    Ok(())
}

fn print_banner() {
    println!("trembita showcase · real-time sessions (capability + sticky session)");
    println!("  listen   {}", env::var("TREMBITA_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
    if !http_disabled() {
        let host = display_addr(&http_bind_display("127.0.0.1:8290"));
        println!("  http      http://{host}  (product routes + ops on one listener)");
        println!("  websocket ws://{host}/ws?user=alice  (Bearer identity)");
        println!("  login     POST http://{host}/login  (Bearer + X-Trembita-User → Set-Cookie)");
        println!("  chat      POST http://{host}/chat   (session cookie)");
        println!("  me        GET  http://{host}/me      (session cookie)");
        println!("  ops       http://{host}/dashboard  /health  /metrics");
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
