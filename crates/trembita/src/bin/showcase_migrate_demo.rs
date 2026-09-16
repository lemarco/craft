//! Workspace migration demo binary.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::workspace_showcase::migrate_demo::run().await
}
