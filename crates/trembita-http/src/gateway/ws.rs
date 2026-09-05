//! Hyper WebSocket upgrade helpers (0.4.0).

use std::convert::Infallible;
use std::future::Future;

use bytes::Bytes;
use http::{Request, Response as HttpResponse};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;

use super::UpgradeStream;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Accept a WebSocket upgrade and invoke `on_connected` on the upgraded stream.
///
/// Returns the `101 Switching Protocols` response hyper builds via [`hyper::upgrade::on`].
pub fn accept_websocket<F, Fut>(
    mut req: Request<Incoming>,
    on_connected: F,
) -> HttpResponse<BoxBody>
where
    F: FnOnce(UpgradeStream) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (response, upgrade) = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        if let Ok(upgraded) = upgrade.await {
            on_connected(TokioIo::new(upgraded)).await;
        }
    });
    let (parts, body) = response.into_parts();
    HttpResponse::from_parts(
        parts,
        body.map_err(|never| match never {}).boxed(),
    )
}
