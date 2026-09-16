//! Route table for event topic publish and metrics.

use std::sync::Arc;

use http::StatusCode;

use crate::TopicsApiState;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};
use crate::topic_types::{PublishAccepted, TopicsApiError};

/// Route table for topic product routes.
#[must_use]
pub fn route_table(state: Arc<TopicsApiState>) -> RouteTable {
    let publish_state = Arc::clone(&state);
    let metrics_state = state;
    RouteTable::new()
        .post("/topics/{name}/publish", move |ctx| {
            let state = Arc::clone(&publish_state);
            async move { post_publish(state, ctx).await }
        })
        .get("/topics/{name}", move |ctx| {
            let state = Arc::clone(&metrics_state);
            async move { get_metrics(state, ctx).await }
        })
}

async fn post_publish(state: Arc<TopicsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_publish_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_publish_inner(
    state: &TopicsApiState,
    ctx: RequestCtx,
) -> Result<Response, TopicsApiError> {
    let name = ctx
        .params()
        .get("name")
        .ok_or_else(|| TopicsApiError::BadRequest("missing topic name".into()))?
        .to_string();
    let (payload, _) = crate::routes::parse_enqueue_body(ctx.headers(), ctx.body())
        .map_err(|e| TopicsApiError::BadRequest(e.to_string()))?;
    let event_id = (state.publish)(name, payload)
        .await
        .map_err(map_topic_err)?;
    let json = serde_json::to_value(PublishAccepted { event_id })
        .map_err(|e| TopicsApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::ACCEPTED, json))
}

async fn get_metrics(state: Arc<TopicsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_metrics_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_metrics_inner(
    state: &TopicsApiState,
    ctx: RequestCtx,
) -> Result<Response, TopicsApiError> {
    let name = ctx
        .params()
        .get("name")
        .ok_or_else(|| TopicsApiError::BadRequest("missing topic name".into()))?
        .to_string();
    let metrics = (state.metrics)(name).await.map_err(map_topic_err)?;
    let json = serde_json::to_value(metrics)
        .map_err(|e| TopicsApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::OK, json))
}

fn map_topic_err(err: String) -> TopicsApiError {
    if err.contains("not found") || err.contains("unknown topic") {
        TopicsApiError::NotFound(err)
    } else {
        TopicsApiError::Failed(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Method;
    use std::collections::HashMap;
    use std::future;

    use crate::topic_types::TopicMetricsResponse;

    fn test_state() -> Arc<TopicsApiState> {
        Arc::new(TopicsApiState {
            publish: Arc::new(|name, _payload| {
                Box::pin(future::ready(if name == "orders" {
                    Ok(42)
                } else {
                    Err("unknown topic orders.events".into())
                }))
            }),
            metrics: Arc::new(|name| {
                Box::pin(future::ready(if name == "orders" {
                    Ok(TopicMetricsResponse {
                        event_count: 1,
                        head: 42,
                        compact_head: 0,
                        oldest_event_age_secs: 0,
                        subscriptions: vec![],
                    })
                } else {
                    Err("topic not found: missing".into())
                }))
            }),
        })
    }

    #[tokio::test]
    async fn post_publish_returns_accepted() {
        let table = route_table(test_state());
        let resp = table
            .dispatch_open(
                &Method::POST,
                "/topics/orders/publish",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::from(r#"{"payload":"evt"}"#),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn get_metrics_returns_ok() {
        let table = route_table(test_state());
        let resp = table
            .dispatch_open(
                &Method::GET,
                "/topics/orders",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::new(),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
    }
}
