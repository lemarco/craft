//! Trembita framework CLI library — `trembita new` scaffold and read-only `doctor`.

#![allow(missing_docs)]

pub mod scaffold;

/// Local showcase clusters — **debug builds only** (omitted from `--release` / crates.io install).
#[cfg(debug_assertions)]
pub mod dev;

pub use scaffold::{
    AddKind, AppFeature, AppTemplate, DoctorReport, Level, NewProjectOpts, ProjectError,
    ScaffoldError, TrembitaProject, default_output, parse_feature_list, resolve_scaffold_features,
    run_add, run_doctor, run_doctor_fix, run_explain_scale, scaffold_project,
};

#[cfg(debug_assertions)]
pub use dev::local_cluster::{
    DEFAULT_CLUSTER_UP_SHOWCASE, LOCAL_CLUSTER_ELASTIC_NODES, LOCAL_CLUSTER_GATEWAY_SESSION_SECRET,
    LOCAL_CLUSTER_GATEWAY_TOKEN, LOCAL_CLUSTER_LB_PORT,
};
#[cfg(debug_assertions)]
pub use dev::{
    DevError, all_showcases, dev_cluster_lb_down, dev_cluster_lb_up, dev_cluster_up, dev_http,
    dev_setup, dev_status, dev_stop, dev_trigger, dev_up, find_showcase, list_showcases,
    showcase_ids, workspace_root,
};
