//! Patch product capability registrations in `src/manifest.rs`.

use std::path::PathBuf;

use super::markers::{AppRsPatch, PatchError, names};
use super::project::TrembitaProject;

/// Capability registry editor (`src/manifest.rs`).
pub struct CapabilityRegistry {
    path: PathBuf,
    patch: AppRsPatch,
}

impl CapabilityRegistry {
    /// Load `src/manifest.rs`.
    pub fn load(project: &TrembitaProject) -> Result<Self, PatchError> {
        let path = project.manifest_rs();
        Ok(Self {
            patch: AppRsPatch::load(&path)?,
            path,
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
        self.patch.insert_block_before_anchor(
            "AppManifest::new()",
            &format!(
                r".jobs([
                // trembita:jobs
                {job_line}
                // trembita:jobs-end
            ])
            "
            ),
        )
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
        self.patch.insert_block_before_anchor(
            "AppManifest::new()",
            &format!(
                r".topics([
                // trembita:topics
                {topic_line}
                // trembita:topics-end
            ])
            "
            ),
        )
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
        self.patch.insert_block_before_anchor(
            "AppManifest::new()",
            &format!(
                r".workers(workers!(
                // trembita:workers
                {worker_line}
                // trembita:workers-end
            ))
            "
            ),
        )
    }
}
