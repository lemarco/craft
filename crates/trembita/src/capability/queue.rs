//! Queue consumer that forwards capability jobs to inline ask.

use std::sync::Arc;
use std::time::Duration;

use trembita_jobs::{JobContext, JobId, WorkerId, run_queue_consumer};
use trembita_proto::{self as proto, encode};

use super::wire::{CapQueued, CapWire};
use crate::TrembitaApp;
use crate::consumer::{DeliveryGuardError, IdempotencyOpts, run_delivery_with_idempotency};

/// Spawn the queue→ask bridge for a capability group stream.
pub(crate) fn spawn_bridge(
    app: Arc<TrembitaApp>,
    _group: String,
    stream: &'static str,
    stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let idempotency = app
        .actor_state_store()
        .map(|store| IdempotencyOpts::by_dedup_key(store, format!("cap:{stream}:")));
    tokio::spawn(async move {
        let queue = app
            .job_queue(stream)
            .expect("capability queue stream must be registered");
        let worker = WorkerId {
            node: app.node_id(),
            instance: 0,
        };
        run_queue_consumer(
            queue,
            worker,
            1,
            Duration::from_millis(10),
            stop,
            move |job| {
                let app = Arc::clone(&app);
                let payload = job.payload.clone();
                let job_id = job.job_id;
                let lease_id = job.lease_id;
                let attempts = job.attempts;
                let dedup_key = job.dedup_key.clone();
                let idempotency = idempotency.clone();
                async move {
                    let dedup_ref = dedup_key.as_deref();
                    let ctx = JobContext::new(job_id, lease_id, stream, attempts, dedup_ref);
                    let deliver = || {
                        let app = Arc::clone(&app);
                        let payload = payload.clone();
                        async move {
                            deliver_queued_job(&app, &payload, job_id)
                                .await
                                .map_err(|_| ())
                        }
                    };
                    match run_delivery_with_idempotency(
                        idempotency.as_ref(),
                        &payload,
                        ctx,
                        deliver,
                    )
                    .await
                    {
                        Ok(()) => Ok(()),
                        Err(DeliveryGuardError::Handler) => Err(()),
                        Err(DeliveryGuardError::Store(_)) => Err(()),
                        Err(DeliveryGuardError::Contended) => Err(()),
                    }
                }
            },
            None,
            1,
        )
        .await;
    })
}

/// Process one queued capability payload (tests without job id).
pub async fn deliver_queued(
    app: &TrembitaApp,
    _hint: &str,
    payload: &[u8],
) -> Result<(), super::CapError> {
    let job: CapQueued = proto::decode(payload).map_err(super::CapError::codec)?;
    run_queued_job(app, &job, None).await?;
    Ok(())
}

/// Process one queued job from the bridge consumer.
pub async fn deliver_queued_job(
    app: &TrembitaApp,
    payload: &[u8],
    job_id: JobId,
) -> Result<(), super::CapError> {
    let job: CapQueued = proto::decode(payload).map_err(super::CapError::codec)?;
    run_queued_job(app, &job, Some(job_id)).await?;
    Ok(())
}

async fn run_queued_job(
    app: &TrembitaApp,
    job: &CapQueued,
    job_id: Option<JobId>,
) -> Result<(), super::CapError> {
    let wire = CapWire {
        op: job.op.clone(),
        payload: job.body.clone(),
        ingress: None,
    };
    let bytes = encode(&wire).map_err(super::CapError::codec)?;
    let Some(binding) = app.cap_runtime().binding(&job.group, &wire.op) else {
        return Err(super::CapError::NotRegistered {
            group: job.group.clone(),
            op: wire.op,
        });
    };
    let reply = if let Some(key_fn) = &binding.key {
        if let Some(key) = key_fn(&wire.payload) {
            app.cluster()
                .messaging()
                .ask_keyed(&job.group, &key, bytes)
                .await
                .map_err(|e| super::CapError::Deliver(e.to_string()))?
        } else {
            app.cluster()
                .messaging()
                .ask(&job.group, bytes)
                .await
                .map_err(|e| super::CapError::Deliver(e.to_string()))?
        }
    } else {
        app.cluster()
            .messaging()
            .ask(&job.group, bytes)
            .await
            .map_err(|e| super::CapError::Deliver(e.to_string()))?
    };

    if job.wait
        && let Some(id) = job_id
    {
        app.cap_runtime().wait_store().store(id, reply);
    }
    Ok(())
}
