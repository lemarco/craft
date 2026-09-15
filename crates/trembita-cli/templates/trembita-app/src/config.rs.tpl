//! Typed configuration from `TREMBITA_*` ([`trembita::env::AppConfig`]).

pub use trembita::env::{AppConfig, app_config_from_env};

/// Load from environment.
///
/// # Errors
/// Invalid or missing required `TREMBITA_*` variables.
pub fn from_env() -> Result<AppConfig, Box<dyn std::error::Error>> {
    app_config_from_env()
}
