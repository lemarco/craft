//! {{PROJECT_NAME}} — crafty product app (always a QUIC cluster member).

use std::time::Duration;

use crafty::{CraftyApp, CraftyConfigure, GatewayOpts, RunOpts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crafty::init_tracing();

    // TODO: register workers / consumers before `.run`

    #[cfg(feature = "http-jobs")]
    {
        CraftyApp::builder()
            .data_dir("/tmp/{{PROJECT_NAME}}")
            .queue("jobs", Duration::from_secs(300))
            .gateway("127.0.0.1:8090".parse()?, GatewayOpts::default())
            .configure(CraftyConfigure {
                admin_addr: Some("127.0.0.1:8080".parse()?),
                ..CraftyConfigure::default()
            })
            .run(RunOpts::default())
            .await?;
        return Ok(());
    }

    #[cfg(not(feature = "http-jobs"))]
    {
        CraftyApp::builder()
            .data_dir("/tmp/{{PROJECT_NAME}}")
            .run(RunOpts::default())
            .await?;
    }

    Ok(())
}
