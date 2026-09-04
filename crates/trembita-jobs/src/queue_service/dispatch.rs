//! Transport dispatch: decode wire requests and encode replies.

use std::sync::Arc;

use trembita_net::transport::{Body, BoxFuture, TransportError};
use trembita_net::{Route, decode_body, encode_body};
use trembita_proto::{
    NodeId, QueueAckBatchRequest, QueueAckRequest, QueueEnqueueBatchRequest, QueueEnqueueRequest,
    QueueExtendLeaseRequest, QueueJobStatusRequest, QueueLeaseRequest, QueueListJobsRequest,
    QueueMetricsRequest, QueueNackRequest, QueueReplicateRequest,
    QueueRequeueDeadLetterBatchRequest,
};

use super::QueueService;

impl QueueService {
    /// Wire entry point when the service is held in an [`Arc`].
    pub fn handle_request(
        self: &Arc<Self>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        self.handle_request_from(None, route, body)
    }

    /// Like [`handle_request`](Self::handle_request) with authenticated caller identity.
    pub fn handle_request_from(
        self: &Arc<Self>,
        from: Option<NodeId>,
        route: Route,
        body: Body,
    ) -> BoxFuture<'static, Result<Body, TransportError>> {
        let service = Arc::clone(self);
        match route {
            Route::QueueEnqueue => Box::pin(async move {
                let request: QueueEnqueueRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_enqueue(request).await)?)
            }),
            Route::QueueEnqueueBatch => Box::pin(async move {
                let request: QueueEnqueueBatchRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_enqueue_batch(request).await)?)
            }),
            Route::QueueLease => Box::pin(async move {
                let request: QueueLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_lease(request).await)?)
            }),
            Route::QueueAck => Box::pin(async move {
                let request: QueueAckRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack(request).await)?)
            }),
            Route::QueueAckBatch => Box::pin(async move {
                let request: QueueAckBatchRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack_batch(request).await)?)
            }),
            Route::QueueNack => Box::pin(async move {
                let request: QueueNackRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_nack(request).await)?)
            }),
            Route::QueueExtendLease => Box::pin(async move {
                let request: QueueExtendLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_extend_lease(request).await)?)
            }),
            Route::QueueMetrics => Box::pin(async move {
                let request: QueueMetricsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_metrics(request).await)?)
            }),
            Route::QueueJobStatus => Box::pin(async move {
                let request: QueueJobStatusRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_job_status(request).await)?)
            }),
            Route::QueueListJobs => Box::pin(async move {
                let request: QueueListJobsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_list_jobs(request).await)?)
            }),
            Route::QueueRequeueDeadLetter => Box::pin(async move {
                let request: trembita_proto::QueueRequeueDeadLetterRequest = decode_body(&body)?;
                Ok(encode_body(
                    &service.handle_requeue_dead_letter(request).await,
                )?)
            }),
            Route::QueueRequeueDeadLetterBatch => Box::pin(async move {
                let request: QueueRequeueDeadLetterBatchRequest = decode_body(&body)?;
                Ok(encode_body(
                    &service.handle_requeue_dead_letter_batch(request).await,
                )?)
            }),
            Route::QueueReplicate => Box::pin(async move {
                let request: QueueReplicateRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_replicate(from, request).await)?)
            }),
            other => Box::pin(async move {
                Err(TransportError::Io(format!(
                    "queue handler received unexpected route {other:?}"
                )))
            }),
        }
    }
}
