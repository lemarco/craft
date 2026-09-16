//! Typed invocation — [`CapRequest`] and [`Route`] adapters.

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use trembita_events::EventId;
use trembita_jobs::{EnqueueOptions, JobId};
use trembita_proto::{self as proto, encode};

use crate::TrembitaApp;

use super::error::CapError;
use super::route::Route;
use super::wire::{CapQueued, CapWire};

/// Request type for a registered capability op (implement on the request struct).
pub trait CapRequest: Serialize + DeserializeOwned + Send + Sized + Sync + 'static {
    /// Capability group name (matches [`super::CapGroup`]).
    const GROUP: &'static str;
    /// Operation name (matches [`super::CapOp::new`]).
    const OP: &'static str;
    /// Default job stream `{GROUP}.{OP}` ([`super::CapGroup::default_queue_for`]).
    const QUEUE_STREAM: &'static str;
    /// Default event topic `{GROUP}.{OP}` ([`super::CapGroup::default_event_ingress_for`]).
    const EVENT_TOPIC: &'static str;
    /// Subscription id paired with [`Self::EVENT_TOPIC`].
    const EVENT_SUBSCRIPTION: &'static str;
    /// Response type for this op.
    type Reply: DeserializeOwned + Send;
    /// Optional stable key for keyed inline routing (override when not using `.key()` in manifest).
    fn cap_key(&self) -> Option<String> {
        None
    }
}

/// Job accepted on [`Route::Queued`] ([`CallBuilder::enqueue`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapEnqueueOutcome {
    /// Durable stream name.
    pub stream: &'static str,
    /// Assigned job id.
    pub job_id: JobId,
}

/// Per-call modifiers for [`CallBuilder::route`].
#[derive(Debug, Clone, Default)]
pub struct CapCallOpts {
    /// Sticky session routing key ([`Route::Session`]).
    pub session_key: Option<String>,
    /// Absolute unix millis for [`Route::Scheduled`].
    pub run_at_ms: Option<u64>,
    /// Max wait for [`Route::QueuedWait`] (default 30s).
    pub wait_timeout: Option<Duration>,
    /// Enqueue dedup key (overrides [`CapRequest::cap_key`] when set).
    pub dedup_key: Option<Vec<u8>>,
}

/// Call-site builder: `MyReq { .. }.via(&app).route(Route::Inline).await`.
pub struct CallBuilder<'a, Req: CapRequest> {
    app: &'a TrembitaApp,
    req: Req,
    opts: CapCallOpts,
}

/// Extension for starting a capability call.
pub trait CapVia: CapRequest + Sized {
    /// Begin invocation on `app`.
    fn via(self, app: &TrembitaApp) -> CallBuilder<'_, Self>;
}

impl<Req: CapRequest> CapVia for Req {
    fn via(self, app: &TrembitaApp) -> CallBuilder<'_, Self> {
        CallBuilder {
            app,
            req: self,
            opts: CapCallOpts::default(),
        }
    }
}

impl<Req: CapRequest> CallBuilder<'_, Req> {
    /// Sticky session key ([`Route::Session`]).
    #[must_use]
    pub fn session_key(mut self, key: impl Into<String>) -> Self {
        self.opts.session_key = Some(key.into());
        self
    }

    /// Run at wall time ([`Route::Scheduled`]).
    #[must_use]
    pub fn run_at_ms(mut self, ms: u64) -> Self {
        self.opts.run_at_ms = Some(ms);
        self
    }

    /// Timeout for [`Route::QueuedWait`] (default 30s).
    #[must_use]
    pub fn wait_timeout(mut self, timeout: Duration) -> Self {
        self.opts.wait_timeout = Some(timeout);
        self
    }

    /// Client idempotency token for [`Route::Queued`] / [`Route::QueuedWait`] / [`Route::Scheduled`].
    #[must_use]
    pub fn dedup_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.opts.dedup_key = Some(key.into());
        self
    }

    /// Run the op using `route`.
    ///
    /// # Errors
    /// Returns [`CapError`] when the op is not registered, the route is disabled, or delivery fails.
    pub async fn route(self, route: Route) -> Result<Req::Reply, CapError> {
        dispatch(self.app, self.req, route, self.opts).await
    }

    /// Fire-and-forget inline delivery ([`Route::InlineFire`]).
    pub async fn fire(self) -> Result<(), CapError> {
        fire(self.app, self.req).await
    }

    /// Enqueue on the group's queue stream ([`Route::Queued`]).
    pub async fn enqueue(self) -> Result<CapEnqueueOutcome, CapError> {
        enqueue_job(self.app, self.req, false, &self.opts).await
    }

    /// Enqueue and await the handler reply ([`Route::QueuedWait`]).
    pub async fn queued_wait(self) -> Result<Req::Reply, CapError> {
        self.route(Route::QueuedWait).await
    }

    /// Schedule on the group's queue stream ([`Route::Scheduled`]).
    pub async fn schedule(self) -> Result<CapEnqueueOutcome, CapError> {
        let run_at_ms = self.opts.run_at_ms.ok_or_else(|| CapError::MissingOption {
            detail: "CallBuilder::run_at_ms required for schedule()".into(),
        })?;
        scheduled_enqueue(self.app, self.req, run_at_ms, &self.opts).await
    }

    /// Publish to the group's [`super::CapGroup::event_ingress`] topic ([`Route::Event`]).
    pub async fn publish_event(self) -> Result<EventId, CapError> {
        publish_event(self.app, self.req).await
    }
}

/// Invoke a capability op and await a reply ([`Route::Inline`] only).
pub async fn invoke<Req: CapRequest>(
    app: &TrembitaApp,
    req: Req,
    route: Route,
) -> Result<Req::Reply, CapError> {
    dispatch(app, req, route, CapCallOpts::default()).await
}

async fn dispatch<Req: CapRequest>(
    app: &TrembitaApp,
    req: Req,
    route: Route,
    opts: CapCallOpts,
) -> Result<Req::Reply, CapError> {
    match route {
        Route::Inline => {
            let reply_bytes = deliver_inline(app, &req).await?;
            proto::decode(&reply_bytes).map_err(CapError::codec)
        }
        Route::InlineFire | Route::Queued | Route::Scheduled => Err(CapError::UnsupportedRoute {
            route,
            op: Req::OP.to_string(),
        }),
        Route::QueuedWait => {
            let outcome = enqueue_job(app, req, true, &opts).await?;
            let timeout = opts.wait_timeout.unwrap_or(Duration::from_secs(30));
            let bytes = app
                .cap_runtime()
                .wait_store()
                .wait_for(outcome.job_id, timeout)
                .await
                .ok_or(CapError::WaitTimeout {
                    job_id: outcome.job_id,
                })?;
            proto::decode(&bytes).map_err(CapError::codec)
        }
        Route::Session => {
            let key = opts.session_key.or_else(|| req.cap_key()).ok_or_else(|| {
                CapError::MissingOption {
                    detail:
                        "Route::Session requires CallBuilder::session_key or CapRequest::cap_key"
                            .into(),
                }
            })?;
            let reply_bytes = deliver_session(app, &req, &key).await?;
            proto::decode(&reply_bytes).map_err(CapError::codec)
        }
        Route::Event => Err(CapError::UnsupportedRoute {
            route,
            op: Req::OP.to_string(),
        }),
    }
}

/// Publish a [`CapWire`] frame on the group's event topic ([`Route::Event`]).
pub async fn publish_event<Req: CapRequest>(
    app: &TrembitaApp,
    req: Req,
) -> Result<EventId, CapError> {
    let binding = binding(app, Req::GROUP, Req::OP)?;
    ensure_route(&binding, Route::Event, Req::OP)?;
    let topic = binding.event_topic.ok_or_else(|| CapError::MissingOption {
        detail: format!(
            "CapGroup {} has no .event_ingress(...) for op {}",
            Req::GROUP,
            Req::OP
        ),
    })?;
    let body = encode(&req).map_err(CapError::codec)?;
    let wire = CapWire {
        op: Req::OP.to_string(),
        payload: body,
    };
    let frame = encode(&wire).map_err(CapError::codec)?;
    app.publish(topic, &frame)
        .await
        .map_err(|e| CapError::Deliver(e.to_string()))
}

/// Fire-and-forget cast to the capability group.
pub async fn fire<Req: CapRequest>(app: &TrembitaApp, req: Req) -> Result<(), CapError> {
    let binding = binding(app, Req::GROUP, Req::OP)?;
    ensure_route(&binding, Route::InlineFire, Req::OP)?;
    let body = encode(&req).map_err(CapError::codec)?;
    deliver_fire(app, Req::GROUP, &binding, &body, req.cap_key()).await
}

/// Enqueue the op on the configured queue stream.
pub async fn enqueue<Req: CapRequest>(
    app: &TrembitaApp,
    req: Req,
) -> Result<CapEnqueueOutcome, CapError> {
    enqueue_job(app, req, false, &CapCallOpts::default()).await
}

fn enqueue_options<Req: CapRequest>(req: &Req, opts: &CapCallOpts) -> EnqueueOptions {
    if let Some(key) = opts
        .dedup_key
        .as_ref()
        .cloned()
        .or_else(|| req.cap_key().map(|k| k.into_bytes()))
    {
        EnqueueOptions::dedup_key(key)
    } else {
        EnqueueOptions::default()
    }
}

async fn enqueue_job<Req: CapRequest>(
    app: &TrembitaApp,
    req: Req,
    wait: bool,
    call_opts: &CapCallOpts,
) -> Result<CapEnqueueOutcome, CapError> {
    let binding = binding(app, Req::GROUP, Req::OP)?;
    let route = if wait {
        Route::QueuedWait
    } else {
        Route::Queued
    };
    ensure_route(&binding, route, Req::OP)?;
    let stream = binding
        .queue_stream
        .ok_or_else(|| CapError::MissingQueueStream {
            group: Req::GROUP.to_string(),
        })?;
    let body = encode(&req).map_err(CapError::codec)?;
    let queued = CapQueued {
        group: Req::GROUP.to_string(),
        op: Req::OP.to_string(),
        body,
        wait,
    };
    let payload = encode(&queued).map_err(CapError::codec)?;
    let job_id = app
        .enqueue_opts(stream, &payload, enqueue_options(&req, call_opts))
        .await
        .map_err(|e| CapError::Deliver(e.to_string()))?;
    Ok(CapEnqueueOutcome { stream, job_id })
}

async fn scheduled_enqueue<Req: CapRequest>(
    app: &TrembitaApp,
    req: Req,
    run_at_ms: u64,
    call_opts: &CapCallOpts,
) -> Result<CapEnqueueOutcome, CapError> {
    let binding = binding(app, Req::GROUP, Req::OP)?;
    ensure_route(&binding, Route::Scheduled, Req::OP)?;
    let stream = binding
        .queue_stream
        .ok_or_else(|| CapError::MissingQueueStream {
            group: Req::GROUP.to_string(),
        })?;
    let body = encode(&req).map_err(CapError::codec)?;
    let queued = CapQueued {
        group: Req::GROUP.to_string(),
        op: Req::OP.to_string(),
        body,
        wait: false,
    };
    let payload = encode(&queued).map_err(CapError::codec)?;
    let mut opts = enqueue_options(&req, call_opts);
    opts.not_before_ms = Some(run_at_ms);
    let job_id = app
        .enqueue_opts(stream, &payload, opts)
        .await
        .map_err(|e| CapError::Deliver(e.to_string()))?;
    Ok(CapEnqueueOutcome { stream, job_id })
}

fn binding(
    app: &TrembitaApp,
    group: &str,
    op: &str,
) -> Result<super::runtime::OpBinding, CapError> {
    app.cap_runtime()
        .binding(group, op)
        .cloned()
        .ok_or_else(|| CapError::NotRegistered {
            group: group.to_string(),
            op: op.to_string(),
        })
}

fn ensure_route(
    binding: &super::runtime::OpBinding,
    route: Route,
    op: &str,
) -> Result<(), CapError> {
    // Empty `routes` on registration = all delivery modes allowed; call site picks `Route`.
    if binding.routes.is_empty() || binding.routes.contains(&route) {
        Ok(())
    } else {
        Err(CapError::UnsupportedRoute {
            route,
            op: op.to_string(),
        })
    }
}

/// Postcard frame for session cast/ask to a [`CapHost`](super::host::CapHost) (internal wire).
pub(crate) fn cap_wire_bytes<Req: CapRequest>(req: &Req) -> Result<Vec<u8>, CapError> {
    let body = encode(req).map_err(CapError::codec)?;
    encode(&CapWire {
        op: Req::OP.to_string(),
        payload: body,
    })
    .map_err(CapError::codec)
}

async fn deliver_inline<Req: CapRequest>(
    app: &TrembitaApp,
    req: &Req,
) -> Result<Vec<u8>, CapError> {
    let binding = binding(app, Req::GROUP, Req::OP)?;
    ensure_route(&binding, Route::Inline, Req::OP)?;
    let body = encode(req).map_err(CapError::codec)?;
    let routing_key = binding
        .key
        .as_ref()
        .and_then(|k| k(&body))
        .or_else(|| req.cap_key());
    let bytes = cap_wire_bytes(req)?;
    if let Some(key) = routing_key {
        app.cluster()
            .messaging()
            .ask_keyed(Req::GROUP, &key, bytes)
            .await
            .map_err(|e| CapError::Deliver(e.to_string()))
    } else {
        app.cluster()
            .messaging()
            .ask(Req::GROUP, bytes)
            .await
            .map_err(|e| CapError::Deliver(e.to_string()))
    }
}

async fn deliver_session<Req: CapRequest>(
    app: &TrembitaApp,
    req: &Req,
    session_key: &str,
) -> Result<Vec<u8>, CapError> {
    let binding = binding(app, Req::GROUP, Req::OP)?;
    ensure_route(&binding, Route::Session, Req::OP)?;
    let session = app
        .session_str(Req::GROUP, session_key, None)
        .ok_or_else(|| {
            CapError::Deliver(format!(
                "no session for group {} key {session_key}",
                Req::GROUP
            ))
        })?;
    let bytes = cap_wire_bytes(req)?;
    app.ask_session(&session, bytes)
        .await
        .map_err(|e| CapError::Deliver(e.to_string()))
}

async fn deliver_fire(
    app: &TrembitaApp,
    group: &str,
    binding: &super::runtime::OpBinding,
    body: &[u8],
    cap_key: Option<String>,
) -> Result<(), CapError> {
    let routing_key = binding.key.as_ref().and_then(|k| k(body)).or(cap_key);
    let wire = CapWire {
        op: binding.op.to_string(),
        payload: body.to_vec(),
    };
    let bytes = encode(&wire).map_err(CapError::codec)?;
    if let Some(key) = routing_key {
        app.cluster()
            .messaging()
            .cast_keyed(group, &key, bytes)
            .await
            .map_err(|e| CapError::Deliver(e.to_string()))
    } else {
        app.cluster()
            .messaging()
            .cast(group, bytes)
            .await
            .map_err(|e| CapError::Deliver(e.to_string()))
    }
}
