//! Product-oriented re-exports — typical `use trembita::prelude::*` import set.

pub use crate::actor_group::ActorGroupOpts;
pub use crate::app::{ShutdownOpts, TrembitaApp, TrembitaAppBuilder, journal_workflow};
pub use crate::app_opts::RunOpts;
pub use crate::builder::StartError;
pub use crate::configure::TrembitaConfigure;
pub use crate::consumer::{ConsumerGroup, ConsumerOpts, IdempotencyOpts, JobConsumer};
pub use crate::cron_opts::CronOpts;
pub use crate::gateway::{
    ExtractedIdentity, GatewayBearerIdentity, GatewayIdentity, GatewayOpts, GatewayRequest,
    GatewayTokenIdentity, IdentityError, IdentityTypeError, OpenActorSessionError, SessionHandle,
    SessionKey, TrembitaGatewayState,
};
pub use crate::job_opts::JobOpts;
pub use crate::queue_opts::QueueOpts;
pub use crate::ready::ReadyOpts;
pub use crate::worker_opts::{WorkerGroup, WorkerOpts, WorkerScale};
pub use crate::workflow::{WorkflowBuildError, WorkflowBuilder};
pub use crate::workflow_opts::WorkflowOpts;
pub use crate::workload::WorkloadRuntime;
pub use trembita_dashboard::init_tracing;
pub use trembita_jobs::WorkloadOpts;
pub use trembita_macros::consumer;
pub use trembita_proto::NodeId;
