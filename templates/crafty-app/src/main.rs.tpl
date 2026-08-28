//! {{PROJECT_NAME}} — crafty product app starter (no Redis required).

use crafty::CraftyApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = CraftyApp::start_from_env().await?;

    // TODO: register workers — use CraftyApp::builder(...).manage_auto before start in production
    // TODO: optional HTTP jobs — `http-jobs` feature + `CraftyApp::jobs_api`

    app.run_until_shutdown().await
}
