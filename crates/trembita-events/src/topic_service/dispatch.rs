use std::sync::Arc;

use trembita_net::transport::{Body, BoxFuture, TransportError};
use trembita_net::{Route, decode_body, encode_body};
use trembita_proto::{
    NodeId, TopicAckRequest, TopicLeaseRequest, TopicMetricsRequest, TopicNackRequest,
    TopicPublishRequest, TopicReplicateRequest,
};

use super::TopicService;

impl TopicService {
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
            Route::TopicPublish => Box::pin(async move {
                let request: TopicPublishRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_publish(request).await)?)
            }),
            Route::TopicLease => Box::pin(async move {
                let request: TopicLeaseRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_lease(request).await)?)
            }),
            Route::TopicAck => Box::pin(async move {
                let request: TopicAckRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_ack(request).await)?)
            }),
            Route::TopicNack => Box::pin(async move {
                let request: TopicNackRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_nack(request).await)?)
            }),
            Route::TopicMetrics => Box::pin(async move {
                let request: TopicMetricsRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_metrics(request).await)?)
            }),
            Route::TopicReplicate => Box::pin(async move {
                let request: TopicReplicateRequest = decode_body(&body)?;
                Ok(encode_body(&service.handle_replicate(from, request).await)?)
            }),
            other => Box::pin(async move {
                Err(TransportError::Io(format!(
                    "topic handler received unexpected route {other:?}"
                )))
            }),
        }
    }
}
