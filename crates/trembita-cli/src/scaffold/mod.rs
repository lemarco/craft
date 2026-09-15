//! Project scaffolding for trembita product apps.

mod doctor;
mod features;
mod markers;
mod new;
mod project;
mod render;
mod template;

pub use doctor::{DoctorReport, Level, run_doctor};
pub use features::{AppFeature, parse_feature_list};
pub use new::{NewProjectOpts, default_output};
pub use project::{ProjectError, TrembitaProject};
pub use render::{ScaffoldError, scaffold_project};
pub use template::{AppTemplate, resolve_scaffold_features};
