//! Raw WebSocket upgrade — echo server with no sticky session or adapters.

use std::time::Duration;

use trembita::{
    AuthMode, Gateway, GatewayOpts, ReadyOpts, RouteTable, TrembitaApp, TrembitaConfigure,
    TrembitaGatewayState, mount_raw_websocket, run_text_loop, server_stream,
};
use trembita_tools::showcase_common::{data_dir, http_bind_from_env};

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    let table = mount_raw_websocket(
        RouteTable::new(),
        "/ws",
        AuthMode::Open,
        state,
        |raw| {
            Box::pin(async move {
                let ws = server_stream(raw.stream).await;
                run_text_loop(ws, |text| async move { Some(format!("echo: {text}")) }).await;
            })
        },
    );
    Gateway::new(false).dev_fallback(table)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = data_dir("trembita-showcase-ws-minimal");
    let _ = std::fs::create_dir_all(&dir);
    let gateway = http_bind_from_env("127.0.0.1:8390");
    println!("ws-minimal · ws://{gateway}/ws");
    TrembitaApp::builder()
        .configure(TrembitaConfigure {
            tick_period: Duration::from_millis(10),
            ..TrembitaConfigure::default()
        })
        .data_dir(dir)
        .gateway(GatewayOpts::new(gateway).surfaces(gateway_surfaces))
        .run()
        .await?;
    Ok(())
}
