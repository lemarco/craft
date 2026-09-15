//! Trembita framework CLI library — scaffold generators and project doctor.

#![allow(missing_docs)]

pub mod dev;
pub mod scaffold;

pub use dev::{
    DevError, all_showcases, dev_setup, dev_status, dev_stop, dev_trigger, dev_up,
    find_showcase, list_showcases, showcase_ids, workspace_root,
};
pub use scaffold::{
    AddActorOpts, AddConsumerOpts, AddError, AddHttpSurfaceOpts, AddStaticSiteOpts, AddTopicOpts,
    AppFeature, AppRsPatch, DoctorFixReport, DoctorReport, Level, NewProjectOpts, PatchError,
    ProjectError, ScaffoldError, StaticSiteSource, TrembitaProject, add_actor, add_consumer,
    add_http_surface, add_jobs_routes, add_ops_routes, add_static_site, add_topic, default_output,
    doctor_fix, parse_feature_list, run_doctor, scaffold_project,
};
