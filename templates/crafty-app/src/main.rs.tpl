//! {{PROJECT_NAME}} — crafty product app starter (no Redis required).

use crafty::CraftyApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crafty::init_tracing();

    // TODO: register workers — use CraftyApp::builder(...).manage_auto before start in production

    #[cfg(feature = "http-jobs")]
    {
        let app = CraftyApp::start_from_env_shared().await?;
        return CraftyApp::run_until_shutdown_shared(app).await;
    }

    #[cfg(not(feature = "http-jobs"))]
    {
        let app = CraftyApp::start_from_env().await?;
        app.run_until_shutdown().await
    }
}
