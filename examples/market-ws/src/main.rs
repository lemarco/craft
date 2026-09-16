//! Public market data WebSocket — open auth, topic subscribe, broadcast hub.

use std::time::Duration;

use trembita::{
    GatewayOpts, TrembitaApp, TrembitaConfigure, WsBroadcastHub, WsMount,
    WsSubscribeCmd,
};
use trembita_tools::showcase_common::{data_dir, http_bind_from_env};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hub = WsBroadcastHub::new();
    let hub_task = hub.clone();
    tokio::spawn(async move {
        let mut tick = 0_u64;
        loop {
            tick += 1;
            hub_task.publish("BTC", format!("{{\"symbol\":\"BTC\",\"px\":{tick}}}"));
            hub_task.publish("ETH", format!("{{\"symbol\":\"ETH\",\"px\":{}}}", tick * 2));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let dir = data_dir("trembita-showcase-market-ws");
    let _ = std::fs::create_dir_all(&dir);
    let gateway = http_bind_from_env("127.0.0.1:8391");
    println!("market-ws · ws://{gateway}/ws/tickers");
    println!("  subscribe: {}", serde_json::to_string(&WsSubscribeCmd::Subscribe {
        topic: "BTC".into(),
    })?);

    TrembitaApp::builder()
        .configure(
            TrembitaConfigure::default()
                .with_local_gateway_apis()
                .with_data_dir(dir)
                .with_tick_period(Duration::from_millis(10)),
        )
        .gateway(GatewayOpts::new(gateway).ws(WsMount::broadcast("/ws/tickers", hub)))
        .run()
        .await?;
    Ok(())
}
