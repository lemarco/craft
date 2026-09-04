use std::sync::Arc;

use trembita_net::{send_queue_job_status, send_queue_list_jobs, send_queue_metrics};
use trembita_proto::{
    QueueJobStatusReply, QueueJobStatusRequest, QueueListJobsReply, QueueListJobsRequest,
    QueueMetricsReply, QueueMetricsRequest,
};

use super::super::wire::{filter_from_list_request, job_status_to_list_entry, job_status_to_reply};
use crate::JobId;
use crate::JobQueue;

use super::super::QueueService;

impl QueueService {
    pub(in crate::queue_service) async fn handle_metrics(
        &self,
        request: QueueMetricsRequest,
    ) -> QueueMetricsReply {
        if self.state.is_leader() {
            let metrics = if let Some(sharded) = self.sharded_stream(&request.stream) {
                sharded.metrics().await
            } else {
                match self.local_stream(&request.stream) {
                    Err(e) => {
                        return QueueMetricsReply {
                            pending: 0,
                            leased: 0,
                            dead_letter: 0,
                            oldest_pending_age_ms: 0,
                            redelivered: 0,
                            error: Some(e),
                        };
                    }
                    Ok(queue) => queue.metrics().await,
                }
            };
            match metrics {
                Ok(m) => QueueMetricsReply {
                    pending: m.pending,
                    leased: m.leased,
                    dead_letter: m.dead_letter,
                    oldest_pending_age_ms: u64::try_from(m.oldest_pending_age.as_millis())
                        .unwrap_or(u64::MAX),
                    redelivered: m.redelivered,
                    error: None,
                },
                Err(e) => QueueMetricsReply {
                    pending: 0,
                    leased: 0,
                    dead_letter: 0,
                    oldest_pending_age_ms: 0,
                    redelivered: 0,
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_metrics(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueMetricsReply {
                    pending: 0,
                    leased: 0,
                    dead_letter: 0,
                    oldest_pending_age_ms: 0,
                    redelivered: 0,
                    error: Some(e),
                },
            }
        }
    }

    pub(in crate::queue_service) async fn handle_job_status(
        &self,
        request: QueueJobStatusRequest,
    ) -> QueueJobStatusReply {
        if self.state.is_leader() {
            let status = if let Some(sharded) = self.sharded_stream(&request.stream) {
                sharded.job_status(JobId(request.job_id)).await
            } else {
                match self.local_stream(&request.stream) {
                    Err(e) => {
                        return QueueJobStatusReply {
                            found: false,
                            job_id: request.job_id,
                            lifecycle: None,
                            payload_len: 0,
                            priority: 0,
                            leased_worker_node: None,
                            leased_worker_instance: None,
                            attempts: 0,
                            max_attempts: 0,
                            dedup_key: None,
                            error: Some(e),
                        };
                    }
                    Ok(queue) => queue.job_status(JobId(request.job_id)).await,
                }
            };
            match status {
                Ok(s) => job_status_to_reply(request.job_id, s),
                Err(e) => QueueJobStatusReply {
                    found: false,
                    job_id: request.job_id,
                    lifecycle: None,
                    payload_len: 0,
                    priority: 0,
                    leased_worker_node: None,
                    leased_worker_instance: None,
                    attempts: 0,
                    max_attempts: 0,
                    dedup_key: None,
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let job_id = request.job_id;
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_job_status(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueJobStatusReply {
                    found: false,
                    job_id,
                    lifecycle: None,
                    payload_len: 0,
                    priority: 0,
                    leased_worker_node: None,
                    leased_worker_instance: None,
                    attempts: 0,
                    max_attempts: 0,
                    dedup_key: None,
                    error: Some(e),
                },
            }
        }
    }

    pub(in crate::queue_service) async fn handle_list_jobs(
        &self,
        request: QueueListJobsRequest,
    ) -> QueueListJobsReply {
        if self.state.is_leader() {
            let filter = filter_from_list_request(&request);
            let page = if let Some(sharded) = self.sharded_stream(&request.stream) {
                sharded.list_jobs(filter).await
            } else {
                match self.local_stream(&request.stream) {
                    Err(e) => {
                        return QueueListJobsReply {
                            jobs: Vec::new(),
                            has_more: false,
                            error: Some(e),
                        };
                    }
                    Ok(queue) => queue.list_jobs(filter).await,
                }
            };
            match page {
                Ok(page) => QueueListJobsReply {
                    jobs: page
                        .jobs
                        .into_iter()
                        .map(job_status_to_list_entry)
                        .collect(),
                    has_more: page.has_more,
                    error: None,
                },
                Err(e) => QueueListJobsReply {
                    jobs: Vec::new(),
                    has_more: false,
                    error: Some(e.to_string()),
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_list_jobs(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueListJobsReply {
                    jobs: Vec::new(),
                    has_more: false,
                    error: Some(e),
                },
            }
        }
    }
}
