//! Patch product capability registrations in `manifest.rs` or legacy `app.rs`.

use std::path::PathBuf;

use super::markers::{AppRsPatch, PatchError, names};
use super::project::TrembitaProject;

/// Capability registry file editor (`src/manifest.rs` when present, else `src/app.rs`).
pub struct CapabilityRegistry {
    path: PathBuf,
    patch: AppRsPatch,
    manifest_layout: bool,
}

impl CapabilityRegistry {
    /// Load the registry file for `project`.
    pub fn load(project: &TrembitaProject) -> Result<Self, PatchError> {
        let manifest_layout = project.has_manifest();
        let path = if manifest_layout {
            project.manifest_rs()
        } else {
            project.app_rs()
        };
        Ok(Self {
            patch: AppRsPatch::load(&path)?,
            path,
            manifest_layout,
        })
    }

    /// Mutable patch accessor.
    pub fn patch_mut(&mut self) -> &mut AppRsPatch {
        &mut self.patch
    }

    /// Write changes back to disk.
    pub fn save(self) -> Result<(), PatchError> {
        self.patch.save(&self.path)
    }

    /// Insert a job registration line or bootstrap a `.jobs([…])` block.
    pub fn register_job_line(&mut self, job_line: &str) -> Result<(), PatchError> {
        if self.patch.has_marker(names::JOBS) {
            return self
                .patch
                .insert_before_end(names::JOBS, &format!("    {job_line}"));
        }
        if self.patch.contains(".jobs(") {
            return Err(PatchError::MissingMarker {
                marker: names::JOBS.into(),
            });
        }
        let block = if self.manifest_layout {
            format!(
                r".jobs([
                // trembita:jobs
                {job_line}
                // trembita:jobs-end
            ])
            "
            )
        } else {
            format!(
                r"            .jobs([
                // trembita:jobs
                {job_line}
                // trembita:jobs-end
            ])
"
            )
        };
        if self.manifest_layout {
            self.patch
                .insert_block_before_anchor("AppManifest::new()", &block)
        } else {
            self.patch.insert_block_before_configure(&block)
        }
    }

    /// Insert a topic registration line or bootstrap a `.topics([…])` block.
    pub fn register_topic_line(&mut self, topic_line: &str) -> Result<(), PatchError> {
        if self.patch.has_marker(names::TOPICS) {
            return self
                .patch
                .insert_before_end(names::TOPICS, &format!("    {topic_line}"));
        }
        if self.patch.contains(".topics(") {
            return Err(PatchError::MissingMarker {
                marker: names::TOPICS.into(),
            });
        }
        let block = if self.manifest_layout {
            format!(
                r".topics([
                // trembita:topics
                {topic_line}
                // trembita:topics-end
            ])
            "
            )
        } else {
            format!(
                r"            .topics([
                // trembita:topics
                {topic_line}
                // trembita:topics-end
            ])
"
            )
        };
        if self.manifest_layout {
            self.patch
                .insert_block_before_anchor("AppManifest::new()", &block)
        } else {
            self.patch.insert_block_before_configure(&block)
        }
    }

    /// Insert a worker registration line or bootstrap `.workers(workers!(…))`.
    pub fn register_worker_line(&mut self, worker_line: &str) -> Result<(), PatchError> {
        if self.patch.has_marker(names::WORKERS) {
            return self
                .patch
                .insert_before_end(names::WORKERS, &format!("    {worker_line}"));
        }
        if self.patch.contains(".workers(") {
            return Err(PatchError::MissingMarker {
                marker: names::WORKERS.into(),
            });
        }
        let block = if self.manifest_layout {
            format!(
                r".workers(workers!(
                // trembita:workers
                {worker_line}
                // trembita:workers-end
            ))
            "
            )
        } else {
            format!(
                r"            .workers(workers!(
                // trembita:workers
                {worker_line}
                // trembita:workers-end
            ))
"
            )
        };
        if self.manifest_layout {
            self.patch
                .insert_block_before_anchor("AppManifest::new()", &block)
        } else {
            self.patch.insert_block_before_configure(&block)
        }
    }
}
