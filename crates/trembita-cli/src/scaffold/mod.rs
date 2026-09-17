//! Project scaffolding for trembita product apps.

mod add;
mod doctor;
mod features;
mod markers;
mod new;
mod project;
mod render;
mod template;

pub use add::{AddKind, run_add};
pub use doctor::{DoctorReport, Finding, Level, run_doctor, run_doctor_fix, run_explain_scale};
pub use features::{AppFeature, parse_feature_list};
pub use new::{NewProjectOpts, default_output};
pub use project::{ProjectError, TrembitaProject};
pub use render::{ScaffoldError, scaffold_project};
pub use template::{AppTemplate, resolve_scaffold_features};
