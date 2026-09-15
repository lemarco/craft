//! Wire ↔ domain conversions for [`super::QueueService`] handlers.

use trembita_proto::{
    QueueBatchEnqueueJob, QueueEnqueueRequest, QueueJobLifecycleWire, QueueJobStatusReply,
    StreamName, worker_id_from_wire,
};

use crate::{EnqueueOptions, JobId, JobLifecycle, JobListFilter, JobStatus, WorkerId};

pub(crate) fn stream_key(stream: &StreamName) -> &str {
    stream.as_str()
}

pub(crate) fn worker_from_wire(node: trembita_proto::NodeId, instance: u32) -> WorkerId {
    worker_id_from_wire(node.0, instance)
}

pub(crate) fn job_status_to_reply(job_id: JobId, status: Option<JobStatus>) -> QueueJobStatusReply {
    match status {
        None => QueueJobStatusReply {
            found: false,
            job_id,
            lifecycle: None,
            payload_len: 0,
            priority: trembita_proto::JobPriority(0),
            leased_worker_node: None,
            leased_worker_instance: None,
            attempts: 0,
            max_attempts: trembita_proto::MaxAttempts(0),
            dedup_key: None,
            error: None,
        },
        Some(s) => QueueJobStatusReply {
            found: true,
            job_id,
            lifecycle: Some(match s.lifecycle {
                JobLifecycle::Pending => QueueJobLifecycleWire::Pending,
                JobLifecycle::Leased => QueueJobLifecycleWire::Leased,
                JobLifecycle::Delayed => QueueJobLifecycleWire::Delayed,
                JobLifecycle::DeadLetter => QueueJobLifecycleWire::DeadLetter,
            }),
            payload_len: s.payload_len,
            priority: s.priority,
            leased_worker_node: s.leased_by.map(|w| w.node),
            leased_worker_instance: s.leased_by.map(|w| w.instance),
            attempts: s.attempts,
            max_attempts: s.max_attempts,
            dedup_key: s
                .dedup_key
                .as_ref()
                .and_then(|k| trembita_proto::DedupKey::try_new(k.clone()).ok()),
            error: None,
        },
    }
}

pub(crate) fn job_status_to_list_entry(status: JobStatus) -> trembita_proto::QueueJobListEntryWire {
    trembita_proto::QueueJobListEntryWire {
        job_id: status.job_id,
        lifecycle: match status.lifecycle {
            JobLifecycle::Pending => QueueJobLifecycleWire::Pending,
            JobLifecycle::Leased => QueueJobLifecycleWire::Leased,
            JobLifecycle::Delayed => QueueJobLifecycleWire::Delayed,
            JobLifecycle::DeadLetter => QueueJobLifecycleWire::DeadLetter,
        },
        payload_len: status.payload_len,
        priority: status.priority,
        leased_worker_node: status.leased_by.map(|w| w.node),
        leased_worker_instance: status.leased_by.map(|w| w.instance),
        attempts: status.attempts,
        max_attempts: status.max_attempts,
        dedup_key: status
            .dedup_key
            .and_then(|k| trembita_proto::DedupKey::try_new(k).ok()),
    }
}

pub(crate) fn filter_from_list_request(
    request: &trembita_proto::QueueListJobsRequest,
) -> JobListFilter {
    JobListFilter {
        lifecycle: request.lifecycle.map(|l| match l {
            QueueJobLifecycleWire::Pending => JobLifecycle::Pending,
            QueueJobLifecycleWire::Leased => JobLifecycle::Leased,
            QueueJobLifecycleWire::Delayed => JobLifecycle::Delayed,
            QueueJobLifecycleWire::DeadLetter => JobLifecycle::DeadLetter,
        }),
        min_attempts: request.min_attempts,
        dedup_key: request.dedup_key.as_ref().map(|k| k.as_bytes().to_vec()),
        limit: Some(request.limit as usize),
        after_job_id: (request.after_job_id.0 != 0).then_some(request.after_job_id),
    }
}

pub(crate) fn enqueue_options_from_request(request: &QueueEnqueueRequest) -> EnqueueOptions {
    EnqueueOptions {
        priority: request.priority,
        not_before_ms: (request.not_before_ms.0 != 0).then_some(request.not_before_ms.0),
        shard_key: request.shard_key.clone(),
        dedup_key: request.dedup_key.as_ref().map(|k| k.as_bytes().to_vec()),
        max_attempts: Some(request.max_attempts),
    }
}

pub(crate) fn enqueue_options_from_batch_job(job: &QueueBatchEnqueueJob) -> EnqueueOptions {
    EnqueueOptions {
        priority: job.priority,
        not_before_ms: (job.not_before_ms.0 != 0).then_some(job.not_before_ms.0),
        shard_key: job.shard_key.clone(),
        dedup_key: job.dedup_key.as_ref().map(|k| k.as_bytes().to_vec()),
        max_attempts: Some(job.max_attempts),
    }
}

pub(crate) fn shard_stream_name(base: &str, shard: usize) -> String {
    StreamName::try_sharded(base, shard)
        .map(|s| s.into())
        .unwrap_or_else(|_| format!("{base}~{shard}"))
}
