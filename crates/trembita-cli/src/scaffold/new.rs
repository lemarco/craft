//! Options for `trembita new`.

use std::path::{Path, PathBuf};

use super::features::AppFeature;
use super::template::AppTemplate;

/// Inputs for scaffolding a new product app.
#[derive(Debug, Clone)]
pub struct NewProjectOpts {
    /// Crate / directory name (`my-service`).
    pub name: String,
    /// Parent directory for the project folder.
    pub output: PathBuf,
    /// Enabled app features.
    pub features: Vec<AppFeature>,
    /// Preset scenario (`--template`), when set without `--features`.
    pub template: Option<AppTemplate>,
    /// crates.io version pin when not using a path dependency.
    pub trembita_version: String,
    /// Optional path to a local trembita checkout.
    pub trembita_path: Option<PathBuf>,
}

impl NewProjectOpts {
    /// `Cargo.toml` line for the `trembita` dependency.
    #[must_use]
    pub fn trembita_dependency_line(&self) -> String {
        let mut feats = vec!["dev-certs"];
        if self.features.contains(&AppFeature::Gateway)
            || self.features.contains(&AppFeature::Workflows)
        {
            feats.push("http-jobs");
        }
        if self.features.contains(&AppFeature::ExternalBacklog) {
            feats.push("external-backlog");
        }
        if self.features.contains(&AppFeature::DomainOutbox) {
            feats.push("domain-outbox");
        }
        let feat_list = feats
            .iter()
            .map(|f| format!("\"{f}\""))
            .collect::<Vec<_>>()
            .join(", ");

        if let Some(path) = &self.trembita_path {
            let path = path.display();
            format!("trembita = {{ path = \"{path}/crates/trembita\", features = [{feat_list}] }}")
        } else {
            format!(
                "trembita = {{ version = \"{}\", features = [{feat_list}] }}",
                self.trembita_version
            )
        }
    }

    /// Optional `trembita-runtime` dependency line (telemetry feature).
    #[must_use]
    pub fn trembita_runtime_dependency_line(&self) -> Option<String> {
        if !self.features.contains(&AppFeature::Telemetry) {
            return None;
        }
        if let Some(path) = &self.trembita_path {
            Some(format!(
                "trembita-runtime = {{ path = \"{}/crates/trembita-runtime\", features = [\"otlp\"] }}",
                path.display()
            ))
        } else {
            Some(format!(
                "trembita-runtime = {{ version = \"{}\", features = [\"otlp\"] }}",
                self.trembita_version
            ))
        }
    }

    /// Optional adapter dependency lines (none — adapters are enabled via `trembita` features).
    #[must_use]
    pub fn optional_adapter_lines(&self) -> String {
        String::new()
    }
}

/// Resolve output directory (default: cwd).
#[must_use]
pub fn default_output(cwd: &Path) -> PathBuf {
    cwd.to_path_buf()
}
