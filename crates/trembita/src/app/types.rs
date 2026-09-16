/// Minimal state machine for actor-only / queue-only applications.
pub use trembita_assembly::EmptyStateMachine;

/// A worker instance registered in the cluster directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    /// Hosting cluster node.
    pub node: u64,
    /// Worker actor instance id on that node.
    pub instance: u32,
}

/// Job/actor registration toggles on [`super::TrembitaAppBuilder`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrembitaAppRegistrationFlags {
    pub(crate) jobs: bool,
    pub(crate) actors: bool,
}

/// Gateway built-in API toggles on [`super::TrembitaAppBuilder`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrembitaAppGatewayApiFlags {
    pub(crate) jobs: bool,
    pub(crate) actors: bool,
}

/// Opt-out flags for registration-driven product HTTP on the default gateway.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GatewayProductApiExclusions {
    pub(crate) jobs: bool,
    pub(crate) schedules: bool,
    pub(crate) actors: bool,
    pub(crate) workflows: bool,
    pub(crate) topics: bool,
}
