//! Trembita framework CLI library — scaffold generators and project doctor.

#![allow(missing_docs)]

pub mod dev;
pub mod scaffold;

pub use dev::{
    DevError, all_showcases, dev_http, dev_setup, dev_status, dev_stop, dev_trigger, dev_up,
    find_showcase, list_showcases, showcase_ids, workspace_root,
};
pub use scaffold::{
    AddActorOpts, AddConsumerOpts, AddError, AddHttpSurfaceOpts, AddStaticSiteOpts, AddTopicOpts,
    AddWorkflowOpts, AddWsSurfaceOpts, AppFeature, AppRsPatch, AppTemplate, DoctorFixReport,
    DoctorReport, Level, NewProjectOpts, PatchError, ProjectError, ScaffoldError, StaticSiteSource,
    TrembitaProject, add_actor, add_consumer, add_http_surface, add_jobs_routes, add_ops_routes,
    add_static_site, add_topic, add_workflow, add_ws_surface, default_output, doctor_fix,
    parse_feature_list, resolve_scaffold_features, run_doctor, scaffold_project,
};
