//! Authenticated order submit — custom HTTP beside built-in `/actors/*` API.

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita::{Gateway, HttpError, RequestCtx, Response, RouteTable, TrembitaGatewayState};

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

fn ctx_uri(ctx: &RequestCtx) -> http::Uri {
    let query = ctx.query();
    if query.is_empty() {
        ctx.path().parse().expect("path uri")
    } else {
        let qs = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{qs}", ctx.path()).parse().expect("path+query uri")
    }
}

pub fn order_routes(state: TrembitaGatewayState) -> RouteTable {
    RouteTable::new().post("/orders/submit", move |ctx: RequestCtx| {
        let st = state.clone();
        async move { submit_order(st, ctx).await }
    })
}

pub fn surfaces(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(order_routes(state))
}

/// `POST /orders/submit?user=<tenant>&token=…` — sticky cast to `orders` group.
async fn submit_order(
    state: TrembitaGatewayState,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let uri = ctx_uri(&ctx);
    let mut handle = state
        .open_actor_session_parts("orders", ctx.method(), &uri, ctx.headers(), None)
        .await
        .map_err(|err| match err {
            trembita::OpenActorSessionError::Identity(e) => HttpError::Unauthorized(e.to_string()),
            trembita::OpenActorSessionError::NoWorker(e) => HttpError::Internal(e.to_string()),
        })?;
    let body: SubmitOrder = ctx.json()?;
    let tenant = handle.session_key().to_string();
    let payload = format!(r#"{{"payload":"{}"}}"#, body.order_id);
    let bytes = trembita::proto::encode(&payload).map_err(|e| HttpError::Internal(e.to_string()))?;
    match handle.cast(bytes).await {
        Ok(()) => {
            debug::order_submit(body.order_id, &tenant, true);
            Ok(Response::json(
                StatusCode::OK,
                serde_json::to_value(SubmitAck {
                    ok: true,
                    order_id: body.order_id,
                    tenant,
                })
                .expect("json"),
            ))
        }
        Err(e) => {
            debug::order_submit(body.order_id, &tenant, false);
            Ok(Response::text(StatusCode::BAD_GATEWAY, e.to_string()))
        }
    }
}
