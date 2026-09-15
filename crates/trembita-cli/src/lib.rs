//! Trembita framework CLI library — `trembita new` scaffold and read-only `doctor`.

#![allow(missing_docs)]

pub mod scaffold;

/// Local showcase clusters — **debug builds only** (omitted from `--release` / crates.io install).
#[cfg(debug_assertions)]
pub mod dev;

pub use scaffold::{
    AppFeature, AppTemplate, DoctorReport, Level, NewProjectOpts, ProjectError, ScaffoldError,
    TrembitaProject, default_output, parse_feature_list, resolve_scaffold_features, run_doctor,
    scaffold_project,
};

#[cfg(debug_assertions)]
pub use dev::{
    DevError, all_showcases, dev_http, dev_setup, dev_status, dev_stop, dev_trigger, dev_up,
    find_showcase, list_showcases, showcase_ids, workspace_root,
};
