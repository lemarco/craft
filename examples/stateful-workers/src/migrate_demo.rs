//! # Advanced: in-memory actor RAM migration (not capability migration)
//!
//! Runs the workspace binary `showcase-migrate-demo` (internal cluster builder).

pub async fn run_local() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Run: cargo run -p trembita --bin showcase-migrate-demo");
    let status = std::process::Command::new("cargo")
        .args(["run", "-p", "trembita", "--bin", "showcase-migrate-demo"])
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(format!("showcase-migrate-demo exited with {status}").into())
}
