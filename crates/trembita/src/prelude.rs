//! Product-oriented re-exports — typical `use trembita::prelude::*` import set.
//!
//! Legacy worker types ([`WorkerOpts`](crate::WorkerOpts), [`WorkerGroup`](crate::WorkerGroup),
//! [`OpenWorkerSessionError`](crate::OpenWorkerSessionError)) live on the crate root, not in the prelude.

#[cfg(feature = "http-jobs")]
pub use crate::AuthMode;
#[cfg(feature = "http-jobs")]
pub use crate::ProductRoutes;
pub use crate::app::{
    AppManifest, DefaultGatewayApis, JobsPreset, ShutdownOpts, TopicsPreset, TrembitaApp,
    TrembitaAppBuilder, journal_workflow,
};
pub use crate::app_opts::RunOpts;
pub use crate::configure::TrembitaConfigure;
pub use crate::consumer::{ConsumerGroup, ConsumerOpts, IdempotencyOpts, JobConsumer};
pub use crate::cron_opts::CronOpts;
pub use crate::gateway::{
    ExtractedIdentity, GatewayBearerIdentity, GatewayIdentity, GatewayOpts, GatewayRequest,
    GatewayTokenIdentity, IdentityError, IdentityTypeError, SessionHandle, SessionKey,
    TrembitaGatewayState,
};
pub use crate::job_opts::JobOpts;
pub use crate::queue_opts::QueueOpts;
pub use crate::scheduled_workflow_opts::ScheduledWorkflowOpts;
pub use crate::work_trigger::{DispatchOutcome, dispatch_work_trigger};
pub use crate::workflow::{WorkflowBuildError, WorkflowBuilder};
pub use crate::workflow_opts::WorkflowOpts;
pub use crate::{
    CallBuilder, CapCallOpts, CapEnqueueOutcome, CapError, CapGroup, CapManifest, CapOp,
    CapRequest, CapVia, OpCtx, Route, deliver_queued, enqueue, fire, invoke, publish_event,
};
#[cfg(feature = "http-jobs")]
pub use crate::{cap_enqueue, cap_fire, cap_invoke};
pub use trembita_assembly::workload::WorkloadRuntime;
pub use trembita_assembly::{ProductEnv, ReadyOpts, StartError};
pub use trembita_dashboard::init_tracing;
pub use trembita_jobs::{WorkTrigger, WorkloadOpts};
pub use trembita_macros::{cap_handler, cap_request, consumer};
pub use trembita_proto::NodeId;
