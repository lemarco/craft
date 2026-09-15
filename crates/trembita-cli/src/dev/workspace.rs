//! Locate trembita repository root.

use std::path::{Path, PathBuf};

use super::DevError;

/// Root of the trembita checkout (`TREMBITA_ROOT` or walk from cwd / current exe).
pub fn workspace_root() -> Result<PathBuf, DevError> {
    if let Ok(raw) = std::env::var("TREMBITA_ROOT") {
        let path = PathBuf::from(raw);
        if is_workspace(&path) {
            return Ok(path);
        }
        return Err(DevError::NotWorkspace(path));
    }
    let mut dir = std::env::current_dir().map_err(DevError::Io)?;
    if let Some(found) = walk_up(&dir) {
        return Ok(found);
    }
    if let Ok(exe) = std::env::current_exe() {
        dir = exe;
        if dir.pop()
            && let Some(found) = walk_up(&dir)
        {
            return Ok(found);
        }
    }
    Err(DevError::NoWorkspace)
}

fn walk_up(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        if is_workspace(&dir) {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn is_workspace(root: &Path) -> bool {
    root.join("dev/cluster-common.sh").is_file()
        && root.join("examples/background-jobs/Cargo.toml").is_file()
}
