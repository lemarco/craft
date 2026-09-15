//! Local development — showcase clusters without bash/compose as the primary path.

mod cluster;
mod http;
mod showcases;
mod trigger;
mod workspace;

pub use showcases::{
    Showcase, all as all_showcases, find as find_showcase, id_list as showcase_ids,
};
pub use workspace::workspace_root;

use std::path::PathBuf;

use thiserror::Error;

/// `trembita dev` failure.
#[derive(Debug, Error)]
pub enum DevError {
    /// Showcase name not in [showcases::all].
    #[error("unknown showcase {name:?}; known: {known}")]
    UnknownShowcase {
        /// User input.
        name: String,
        /// Comma-separated ids.
        known: String,
    },
    /// Could not find repo root.
    #[error("not inside a trembita checkout — set TREMBITA_ROOT or run from the repo")]
    NoWorkspace,
    /// `TREMBITA_ROOT` is not a workspace.
    #[error("TREMBITA_ROOT is not a trembita workspace: {}", _0.display())]
    NotWorkspace(PathBuf),
    /// Run `trembita dev setup` first.
    #[error("cluster not set up — run: trembita dev setup --showcase <name>")]
    SetupRequired,
    /// Release binary missing.
    #[error("release binary not found at {} — run: trembita dev setup --showcase …", _0.display())]
    BinaryMissing(PathBuf),
    /// Node count out of range.
    #[error("--nodes must be 1..=8 (got {0})")]
    InvalidNodes(u32),
    /// Script missing.
    #[error("missing script {}", _0.display())]
    MissingScript(PathBuf),
    /// Subprocess failed.
    #[error("{0}")]
    CommandFailed(String),
    /// I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// `trembita dev list`
pub fn list_showcases() {
    eprintln!("Showcases:");
    for s in all_showcases() {
        eprintln!("  {:<18} port {}  examples/{}/", s.id, s.base_port, s.dir);
    }
}

/// `trembita dev setup --showcase …`
pub fn dev_setup(showcase_id: &str) -> Result<(), DevError> {
    let showcase = resolve(showcase_id)?;
    let root = workspace_root()?;
    cluster::setup(showcase, &root)
}

/// `trembita dev up --showcase … --nodes N`
pub fn dev_up(showcase_id: &str, nodes: u32, run_setup: bool) -> Result<(), DevError> {
    let showcase = resolve(showcase_id)?;
    let root = workspace_root()?;
    if run_setup {
        cluster::setup(showcase, &root)?;
    }
    cluster::up(showcase, &root, nodes)
}

/// `trembita dev stop --showcase …`
pub fn dev_stop(showcase_id: &str) -> Result<(), DevError> {
    let showcase = resolve(showcase_id)?;
    cluster::stop(showcase)
}

/// `trembita dev status --showcase …`
pub fn dev_status(showcase_id: &str) -> Result<(), DevError> {
    let showcase = resolve(showcase_id)?;
    let root = workspace_root()?;
    cluster::status(showcase, &root)
}

/// `trembita dev trigger <showcase> -- …`
pub fn dev_trigger(showcase_id: &str, args: &[String]) -> Result<(), DevError> {
    let showcase = resolve(showcase_id)?;
    let root = workspace_root()?;
    trigger::run(showcase, &root, args)
}

/// `trembita dev http --showcase … -- job|topic|workflow …`
pub fn dev_http(
    showcase_id: Option<&str>,
    gateway: Option<&str>,
    args: &[String],
) -> Result<(), DevError> {
    let showcase = match (showcase_id, gateway) {
        (Some(id), _) => resolve(id)?,
        (None, Some(_)) => {
            return Err(DevError::CommandFailed(
                "pass --showcase when using built-in http helpers".into(),
            ));
        }
        (None, None) => {
            return Err(DevError::CommandFailed(
                "pass --showcase <id> (or use trembita dev trigger <showcase> -- job …)".into(),
            ));
        }
    };
    let root = workspace_root()?;
    http::run(showcase, &root, gateway, args)
}

fn resolve(id: &str) -> Result<&'static Showcase, DevError> {
    find_showcase(id).ok_or_else(|| DevError::UnknownShowcase {
        name: id.to_string(),
        known: showcase_ids(),
    })
}
