//! Project scaffolding for trembita product apps.

mod add;
mod doctor;
mod features;
mod markers;
mod new;
mod project;
mod registry;
mod render;
mod template;
mod templates;

pub use add::{
    AddActorOpts, AddConsumerOpts, AddError, AddHttpSurfaceOpts, AddStaticSiteOpts, AddTopicOpts,
    AddWorkflowOpts, AddWsSurfaceOpts, StaticSiteSource, add_actor, add_consumer, add_http_surface,
    add_jobs_routes, add_ops_routes, add_static_site, add_topic, add_workflow, add_ws_surface,
};
pub use doctor::{DoctorFixReport, DoctorReport, Level, doctor_fix, run_doctor};
pub use features::{AppFeature, parse_feature_list};
pub use markers::{AppRsPatch, PatchError};
pub use new::{NewProjectOpts, default_output};
pub use project::{ProjectError, TrembitaProject};
pub use render::{ScaffoldError, scaffold_project};
pub use template::{AppTemplate, resolve_scaffold_features};
