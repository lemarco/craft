//! Run-time defaults derived from product registration (jobs, workers).

/// Readiness hints captured from [`.manifest`](super::TrembitaAppBuilder::manifest) / [`.jobs`](super::TrembitaAppBuilder::jobs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ManifestRunHint {
    pub(crate) job_streams: Vec<String>,
    pub(crate) has_workers: bool,
}

impl ManifestRunHint {
    pub(crate) fn record_job_stream(&mut self, stream: &str) {
        if !self.job_streams.iter().any(|s| s == stream) {
            self.job_streams.push(stream.to_string());
        }
    }

    pub(crate) fn merge_manifest_jobs(&mut self, streams: Vec<&str>) {
        for stream in streams {
            self.record_job_stream(stream);
        }
    }

    pub(crate) fn apply_manifest(&mut self, job_streams: Vec<&str>, has_workers: bool) {
        self.job_streams = job_streams.into_iter().map(str::to_string).collect();
        self.has_workers = has_workers;
    }
}
