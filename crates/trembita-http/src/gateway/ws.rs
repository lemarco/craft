//! Hyper WebSocket upgrade helpers (0.4.0).

use std::convert::Infallible;
use std::future::Future;

use bytes::Bytes;
use http::header::{
    CONNECTION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use http::{Request, Response as HttpResponse, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use tungstenite::handshake::derive_accept_key;

use super::UpgradeStream;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Accept a WebSocket upgrade and invoke `on_connected` on the upgraded stream.
pub fn accept_websocket<F, Fut>(
    mut req: Request<Incoming>,
    on_connected: F,
) -> HttpResponse<BoxBody>
where
    F: FnOnce(UpgradeStream) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let key = req.headers().get(SEC_WEBSOCKET_KEY).cloned();
    let accept = key.as_ref().map(|k| derive_accept_key(k.as_bytes()));
    let ver = req.version();

    tokio::spawn(async move {
        if let Ok(upgraded) = hyper::upgrade::on(&mut req).await {
            on_connected(TokioIo::new(upgraded)).await;
        }
    });

    let mut res = HttpResponse::new(
        Full::new(Bytes::new())
            .map_err(|never| match never {})
            .boxed(),
    );
    *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    *res.version_mut() = ver;
    res.headers_mut()
        .append(CONNECTION, http::HeaderValue::from_static("Upgrade"));
    res.headers_mut()
        .append(UPGRADE, http::HeaderValue::from_static("websocket"));
    res.headers_mut()
        .append(SEC_WEBSOCKET_VERSION, http::HeaderValue::from_static("13"));
    if let Some(accept) = accept
        && let Ok(value) = http::HeaderValue::from_str(&accept)
    {
        res.headers_mut().append(SEC_WEBSOCKET_ACCEPT, value);
    }
    res
}
