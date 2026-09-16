//! Workspace self-update showcase binary.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::workspace_showcase::self_update::run().await
}
