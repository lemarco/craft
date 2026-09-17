//! [`TrembitaApp`] — product-facing entry point over [`TrembitaCluster`](trembita_assembly::cluster_handle::TrembitaCluster)
//! ([product-scenarios](../../../docs/decisions/product-scenarios.md)).

mod builder;
pub(crate) mod capability_wiring;
#[cfg(feature = "http-jobs")]
mod gateway;
#[cfg(feature = "http-jobs")]
mod gateway_defaults;
mod manifest;
mod manifest_presets;
pub(crate) mod run_hint;
pub(crate) use run_hint::ManifestRunHint;

#[cfg(feature = "http-jobs")]
pub use gateway::DefaultGatewayApis;
mod directory_r3;
mod ops_summary;
#[cfg(feature = "otlp-metrics")]
mod otlp_metrics;
#[cfg(feature = "otlp-metrics")]
pub use otlp_metrics::install_otlp_metrics;
mod runtime;
mod scale_plan;
mod shutdown;
#[doc(hidden)]
mod test_boot;
mod types;
mod workflow;

pub use builder::TrembitaAppBuilder;
pub use manifest::{AppManifest, JobsPreset, RealtimePreset, ScheduleSourceOpts, TopicsPreset};
pub use ops_summary::OpsSummary;
pub use runtime::TrembitaApp;
pub use scale_plan::ProductScalePlan;
pub use shutdown::ShutdownOpts;
#[doc(hidden)]
pub use test_boot::TestBoot;
pub use types::{EmptyStateMachine, WorkerInfo};
pub use workflow::journal_workflow;
