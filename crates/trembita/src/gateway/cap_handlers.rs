//! Gateway handlers that deliver [`CapRequest`](crate::capability::CapRequest) ops from HTTP JSON.

use std::marker::PhantomData;

use http::StatusCode;
use serde::Serialize;
use trembita_http::{Handler, HttpError, RequestCtx, Response};

use crate::capability::{CapEnqueueOutcome, CapError, CapRequest, CapVia, Route};
use crate::gateway::{IdentityError, OpenActorSessionError, TrembitaGatewayState};

/// `POST` body → capability fire ([`Route::InlineFire`]), **`202 Accepted`** (no body).
#[must_use]
pub fn cap_fire<Req: CapRequest + Sync>(state: TrembitaGatewayState) -> CapFireHandler<Req> {
    CapFireHandler {
        state,
        _marker: PhantomData,
    }
}

/// `POST` body → capability call; JSON response when `route` is [`Route::Inline`].
#[must_use]
pub fn cap_invoke<Req>(state: TrembitaGatewayState, route: Route) -> CapInvokeHandler<Req>
where
    Req: CapRequest + Sync,
    Req::Reply: Serialize,
{
    CapInvokeHandler {
        state,
        route,
        _marker: PhantomData,
    }
}

/// `POST` body → [`Route::Queued`] enqueue; **`202 Accepted`** with stream + job id.
#[must_use]
pub fn cap_enqueue<Req: CapRequest + Sync>(state: TrembitaGatewayState) -> CapEnqueueHandler<Req> {
    CapEnqueueHandler {
        state,
        _marker: PhantomData,
    }
}

/// Handler adapter for [`cap_fire`].
#[derive(Clone)]
pub struct CapFireHandler<Req: CapRequest + Sync> {
    state: TrembitaGatewayState,
    _marker: PhantomData<fn(Req)>,
}

impl<Req: CapRequest + Sync> Handler for CapFireHandler<Req> {
    fn handle(
        &self,
        ctx: RequestCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, HttpError>> + Send>>
    {
        let state = self.state.clone();
        Box::pin(async move { handle_fire::<Req>(&state, ctx).await })
    }
}

/// Handler adapter for [`cap_invoke`].
#[derive(Clone)]
pub struct CapInvokeHandler<Req: CapRequest + Sync> {
    state: TrembitaGatewayState,
    route: Route,
    _marker: PhantomData<fn(Req)>,
}

impl<Req> Handler for CapInvokeHandler<Req>
where
    Req: CapRequest + Sync,
    Req::Reply: Serialize,
{
    fn handle(
        &self,
        ctx: RequestCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, HttpError>> + Send>>
    {
        let state = self.state.clone();
        let route = self.route;
        Box::pin(async move { handle_invoke::<Req>(&state, ctx, route).await })
    }
}

/// Handler adapter for [`cap_enqueue`].
#[derive(Clone)]
pub struct CapEnqueueHandler<Req: CapRequest + Sync> {
    state: TrembitaGatewayState,
    _marker: PhantomData<fn(Req)>,
}

impl<Req: CapRequest + Sync> Handler for CapEnqueueHandler<Req> {
    fn handle(
        &self,
        ctx: RequestCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, HttpError>> + Send>>
    {
        let state = self.state.clone();
        Box::pin(async move { handle_enqueue::<Req>(&state, ctx).await })
    }
}

async fn handle_fire<Req: CapRequest + Sync>(
    state: &TrembitaGatewayState,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    open_group_session_if_identity::<Req>(state, &ctx).await?;
    let req: Req = ctx.json()?;
    req.via(state.app.as_ref())
        .fire()
        .await
        .map_err(map_cap_error)?;
    Ok(Response::status(StatusCode::ACCEPTED))
}

async fn handle_invoke<Req>(
    state: &TrembitaGatewayState,
    ctx: RequestCtx,
    route: Route,
) -> Result<Response, HttpError>
where
    Req: CapRequest + Sync,
    Req::Reply: Serialize,
{
    open_group_session_if_identity::<Req>(state, &ctx).await?;
    let req: Req = ctx.json()?;
    match route {
        Route::Inline => {
            let reply = req
                .via(state.app.as_ref())
                .route(Route::Inline)
                .await
                .map_err(map_cap_error)?;
            let json = serde_json::to_value(reply)
                .map_err(|e| HttpError::Internal(format!("encode reply: {e}")))?;
            Ok(Response::json(StatusCode::OK, json))
        }
        Route::InlineFire => {
            req.via(state.app.as_ref())
                .fire()
                .await
                .map_err(map_cap_error)?;
            Ok(Response::status(StatusCode::ACCEPTED))
        }
        Route::Queued => {
            let mut call = req.via(state.app.as_ref());
            if let Some(dedup) = ctx
                .query()
                .iter()
                .find_map(|(k, v)| (k == "dedup").then_some(v))
            {
                call = call.dedup_key(dedup.as_bytes());
            }
            let outcome = call.enqueue().await.map_err(map_cap_error)?;
            Ok(enqueue_response(outcome))
        }
        Route::QueuedWait => {
            let reply = req
                .via(state.app.as_ref())
                .wait_timeout(std::time::Duration::from_secs(30))
                .queued_wait()
                .await
                .map_err(map_cap_error)?;
            let json = serde_json::to_value(reply)
                .map_err(|e| HttpError::Internal(format!("encode reply: {e}")))?;
            Ok(Response::json(StatusCode::OK, json))
        }
        Route::Scheduled | Route::Session | Route::Event => Err(HttpError::BadRequest(format!(
            "cap_invoke: route {route} is not supported from HTTP — use in-process CallBuilder"
        ))),
    }
}

async fn handle_enqueue<Req: CapRequest + Sync>(
    state: &TrembitaGatewayState,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    open_group_session_if_identity::<Req>(state, &ctx).await?;
    let req: Req = ctx.json()?;
    let mut call = req.via(state.app.as_ref());
    if let Some(dedup) = ctx
        .query()
        .iter()
        .find_map(|(k, v)| (k == "dedup").then_some(v))
    {
        call = call.dedup_key(dedup.as_bytes());
    }
    let outcome = call.enqueue().await.map_err(map_cap_error)?;
    Ok(enqueue_response(outcome))
}

fn enqueue_response(outcome: CapEnqueueOutcome) -> Response {
    #[derive(Serialize)]
    struct Body {
        stream: &'static str,
        job_id: u64,
    }
    let json = serde_json::to_value(Body {
        stream: outcome.stream,
        job_id: outcome.job_id.0,
    })
    .expect("enqueue json");
    Response::json(StatusCode::ACCEPTED, json)
}

async fn open_group_session_if_identity<Req: CapRequest>(
    state: &TrembitaGatewayState,
    ctx: &RequestCtx,
) -> Result<(), HttpError> {
    let uri = request_uri(ctx);
    match state
        .open_actor_session_parts(Req::GROUP, ctx.method(), &uri, ctx.headers(), None)
        .await
    {
        Ok(_) => Ok(()),
        Err(OpenActorSessionError::Identity(IdentityError::NotConfigured)) => Ok(()),
        Err(OpenActorSessionError::Identity(e)) => Err(HttpError::Unauthorized(e.to_string())),
        Err(OpenActorSessionError::NoWorker(e)) => Err(HttpError::Internal(e.to_string())),
    }
}

fn request_uri(ctx: &RequestCtx) -> http::Uri {
    let query = ctx.query();
    if query.is_empty() {
        ctx.path().parse().expect("path uri")
    } else {
        let qs = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{qs}", ctx.path())
            .parse()
            .expect("path+query uri")
    }
}

fn map_cap_error(err: CapError) -> HttpError {
    HttpError::Internal(err.to_string())
}
