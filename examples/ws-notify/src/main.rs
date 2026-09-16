//! Private notification WebSocket — identity auth, per-user push hub.

use std::time::Duration;

use trembita::{
    GatewayBearerIdentity, GatewayOpts, TrembitaApp, TrembitaConfigure, WsMount,
    WsNotifyHub,
};
use trembita_tools::showcase_common::{data_dir, http_bind_from_env};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hub = WsNotifyHub::new();
    let hub_demo = hub.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            hub_demo.publish_to("alice", "demo notification");
        }
    });

    let dir = data_dir("trembita-showcase-ws-notify");
    let _ = std::fs::create_dir_all(&dir);
    let gateway = http_bind_from_env("127.0.0.1:8392");
    println!("ws-notify · ws://{gateway}/ws/notify?user=alice  (Bearer token)");

    TrembitaApp::builder()
        .configure(
            TrembitaConfigure::default()
                .with_local_gateway_apis()
                .with_data_dir(dir)
                .with_tick_period(Duration::from_millis(10)),
        )
        .gateway(
            GatewayOpts::new(gateway)
                .identity(GatewayBearerIdentity::from_env())
                .ws(WsMount::notify("/ws/notify", hub)),
        )
        .run()
        .await?;
    Ok(())
}
