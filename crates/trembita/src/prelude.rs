//! Product-oriented re-exports — typical `use trembita::prelude::*` import set.

#[cfg(feature = "http-jobs")]
pub use crate::AuthMode;
#[cfg(feature = "http-jobs")]
pub use crate::ProductRoutes;
pub use crate::actor_group::ActorGroupOpts;
pub use crate::app::{
    AppManifest, DefaultGatewayApis, JobsPreset, RealtimePreset, ShutdownOpts, TopicsPreset,
    TrembitaApp, TrembitaAppBuilder, journal_workflow,
};
pub use crate::app_opts::RunOpts;
pub use crate::builder::StartError;
pub use crate::configure::TrembitaConfigure;
pub use crate::consumer::{ConsumerGroup, ConsumerOpts, IdempotencyOpts, JobConsumer};
pub use crate::cron_opts::CronOpts;
pub use crate::env_config::ProductEnv;
pub use crate::gateway::{
    ExtractedIdentity, GatewayBearerIdentity, GatewayIdentity, GatewayOpts, GatewayRequest,
    GatewayTokenIdentity, IdentityError, IdentityTypeError, OpenActorSessionError, SessionHandle,
    SessionKey, TrembitaGatewayState,
};
pub use crate::job_opts::JobOpts;
pub use crate::queue_opts::QueueOpts;
pub use crate::ready::ReadyOpts;
pub use crate::scheduled_workflow_opts::ScheduledWorkflowOpts;
pub use crate::work_trigger::{DispatchOutcome, dispatch_work_trigger};
pub use crate::worker_opts::{WorkerGroup, WorkerOpts, WorkerScale};
pub use crate::workflow::{WorkflowBuildError, WorkflowBuilder};
pub use crate::workflow_opts::WorkflowOpts;
pub use crate::workload::WorkloadRuntime;
pub use crate::{
    CallBuilder, CapCallOpts, CapEnqueueOutcome, CapError, CapGroup, CapManifest, CapOp,
    CapRequest, CapVia, OpCtx, Route, deliver_queued, enqueue, fire, invoke, publish_event,
};
#[cfg(feature = "http-jobs")]
pub use crate::{cap_enqueue, cap_fire, cap_invoke};
pub use trembita_dashboard::init_tracing;
pub use trembita_jobs::{WorkTrigger, WorkloadOpts};
pub use trembita_macros::{cap_handler, cap_request, consumer};
pub use trembita_proto::NodeId;
