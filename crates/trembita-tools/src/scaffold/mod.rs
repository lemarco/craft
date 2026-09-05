//! Project scaffolding for trembita product apps.

mod add;
mod doctor;
mod features;
mod markers;
mod new;
mod project;
mod render;

pub use add::{
    AddActorOpts, AddConsumerOpts, AddError, AddTopicOpts, add_actor, add_consumer, add_topic,
};
pub use doctor::{DoctorReport, Level, run_doctor};
pub use features::{AppFeature, parse_feature_list};
pub use markers::{AppRsPatch, PatchError};
pub use new::{NewProjectOpts, default_output};
pub use project::{ProjectError, TrembitaProject};
pub use render::{ScaffoldError, scaffold_project};
