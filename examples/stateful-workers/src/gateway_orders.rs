//! Authenticated order submit — custom HTTP beside built-in `/actors/*` API.

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use trembita::TrembitaGatewayState;
use serde::{Deserialize, Serialize};

use crate::debug;

#[derive(Deserialize)]
pub struct SubmitOrder {
    /// Order id to process idempotently on the sticky worker.
    pub order_id: u64,
}

#[derive(Serialize)]
pub struct SubmitAck {
    pub ok: bool,
    pub order_id: u64,
    pub tenant: String,
}

pub fn routes(state: TrembitaGatewayState) -> Router {
    Router::new()
        .route("/orders/submit", post(submit_order))
        .with_state(state)
}

/// `POST /orders/submit?user=<tenant>&token=…` — sticky cast to `orders` group.
pub async fn submit_order(
    State(state): State<TrembitaGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Json(body): Json<SubmitOrder>,
) -> Response {
    let mut handle = match state
        .open_actor_session_parts("orders", &method, &uri, &headers, None)
        .await
    {
        Ok(h) => h,
        Err(err) => return err.into_response(),
    };
    let tenant = handle.session_key().to_string();
    let payload = format!(r#"{{"payload":"{}"}}"#, body.order_id);
    let bytes = match trembita::proto::encode(&payload) {
        Ok(b) => b,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match handle.cast(bytes).await {
        Ok(()) => {
            debug::order_submit(body.order_id, &tenant, true);
            Json(SubmitAck {
                ok: true,
                order_id: body.order_id,
                tenant,
            })
            .into_response()
        }
        Err(e) => {
            debug::order_submit(body.order_id, &tenant, false);
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}
