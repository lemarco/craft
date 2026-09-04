use std::convert::Infallible;

use trembita_core::StateMachine;
use trembita_proto::LogIndex;

/// A worker instance registered in the cluster directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    /// Hosting cluster node.
    pub node: u64,
    /// Worker actor instance id on that node.
    pub instance: u32,
}

/// Minimal state machine for actor-only / queue-only applications.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyStateMachine;

impl StateMachine for EmptyStateMachine {
    type Command = ();
    type Query = ();
    type Response = ();
    type Error = Infallible;

    fn apply(&mut self, _index: LogIndex, _command: &()) -> Result<(), Self::Error> {
        Ok(())
    }

    fn query(&self, _query: &()) -> Result<(), Self::Error> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
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
