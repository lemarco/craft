//! Locate a trembita product app on disk.

use std::path::{Path, PathBuf};

/// A scaffolded trembita product app.
#[derive(Debug, Clone)]
pub struct TrembitaProject {
    /// Crate root (contains `Cargo.toml`).
    pub root: PathBuf,
}

/// Project discovery failure.
#[derive(Debug, thiserror::Error)]
pub enum ProjectError {
    /// No trembita app found walking up from the start path.
    #[error(
        "not a trembita product app (missing src/app.rs or trembita in Cargo.toml); run from project root or pass --path"
    )]
    NotFound,
    /// I/O error.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

impl TrembitaProject {
    /// Find a product app starting at `from` (file or directory), walking parents.
    pub fn discover(from: &Path) -> Result<Self, ProjectError> {
        let start = if from.is_file() {
            from.parent()
                .map(Path::to_path_buf)
                .ok_or(ProjectError::NotFound)?
        } else {
            from.to_path_buf()
        };

        let mut dir = start;
        loop {
            if Self::looks_like_product_app(&dir) {
                return Ok(Self { root: dir });
            }
            if !dir.pop() {
                break;
            }
        }
        Err(ProjectError::NotFound)
    }

    fn looks_like_product_app(root: &Path) -> bool {
        let app_rs = root.join("src/app.rs");
        let cargo = root.join("Cargo.toml");
        if !app_rs.is_file() || !cargo.is_file() {
            return false;
        }
        std::fs::read_to_string(cargo).is_ok_and(|c| c.contains("trembita"))
    }

    /// `src/app.rs`
    #[must_use]
    pub fn app_rs(&self) -> PathBuf {
        self.root.join("src/app.rs")
    }

    /// `src/main.rs`
    #[must_use]
    pub fn main_rs(&self) -> PathBuf {
        self.root.join("src/main.rs")
    }

    /// `src/consumers/`
    #[must_use]
    pub fn consumers_dir(&self) -> PathBuf {
        self.root.join("src/consumers")
    }

    /// `src/actors/`
    #[must_use]
    pub fn actors_dir(&self) -> PathBuf {
        self.root.join("src/actors")
    }

    /// `src/domain/`
    #[must_use]
    pub fn domain_dir(&self) -> PathBuf {
        self.root.join("src/domain")
    }

    /// `src/http/`
    #[must_use]
    pub fn http_dir(&self) -> PathBuf {
        self.root.join("src/http")
    }

    /// `Cargo.toml`
    #[must_use]
    pub fn cargo_toml(&self) -> PathBuf {
        self.root.join("Cargo.toml")
    }
}
