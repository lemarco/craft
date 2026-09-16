//! App-injected ports for capability handlers ([`trembita::OpCtx::deps`]).

/// Shared dependencies registered with [`.cap_deps`](trembita::TrembitaAppBuilder::cap_deps).
#[derive(Clone, Debug)]
pub struct AppDeps {
    /// Crate / service name (stable for logs and metrics).
    pub service_name: &'static str,
}

impl AppDeps {
    /// Default deps for a new scaffold project.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            service_name: env!("CARGO_PKG_NAME"),
        }
    }
}
