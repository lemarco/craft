//! Cross-shard keyed propose helpers (best-effort keyed batch, not atomic).

use crate::{ClientError, KeyedClient};

/// One keyed write in a [`propose_keyed_batch`].
#[derive(Debug, Clone)]
pub struct KeyedBatchStep {
    /// Shard routing key.
    pub key: Vec<u8>,
    /// Application-encoded command bytes.
    pub payload: Vec<u8>,
}

/// Why a multi-shard batch stopped early.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    /// Step `step` failed after `completed` prior steps succeeded.
    #[error("step {step} failed after {completed} successful step(s): {source}")]
    Partial {
        /// Index of the failing step.
        step: usize,
        /// Number of steps that completed successfully.
        completed: usize,
        /// The underlying client error.
        #[source]
        source: ClientError,
    },
}

/// Propose each step to its owning Raft group in order. **Not atomic** across
/// groups — if a later step fails, earlier writes remain committed (saga-style
/// callers must compensate explicitly).
///
/// # Errors
/// Returns [`BatchError::Partial`] on the first failing step.
pub async fn propose_keyed_batch<C: KeyedClient>(
    client: &C,
    steps: &[KeyedBatchStep],
) -> Result<Vec<Vec<u8>>, BatchError> {
    let mut out = Vec::with_capacity(steps.len());
    for (step, item) in steps.iter().enumerate() {
        match client
            .propose_keyed(item.key.clone(), item.payload.clone())
            .await
        {
            Ok(bytes) => out.push(bytes),
            Err(source) => {
                return Err(BatchError::Partial {
                    step,
                    completed: step,
                    source,
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crafty_net::{Route, Transport, TransportError, encode_body};
    use crafty_proto::{ClientResponse, NodeId};

    use super::*;
    use crate::{RemoteClient, RetryPolicy};

    struct ScriptTransport {
        ok: Mutex<u32>,
    }

    impl Transport for ScriptTransport {
        fn send(
            &self,
            _peer: NodeId,
            _route: Route,
            _body: crafty_net::transport::Body,
        ) -> crafty_net::transport::BoxFuture<
            'static,
            Result<crafty_net::transport::Body, TransportError>,
        > {
            let remaining = {
                let mut n = self.ok.lock().expect("lock");
                if *n == 0 {
                    return Box::pin(async move { Err(TransportError::Unreachable(NodeId(1))) });
                }
                *n -= 1;
                *n
            };
            let _ = remaining;
            Box::pin(async move {
                encode_body(&ClientResponse::Ok(b"ok".to_vec())).map_err(TransportError::Wire)
            })
        }
    }

    #[tokio::test]
    async fn batch_stops_on_first_failure_and_reports_partial_progress() {
        let client =
            RemoteClient::new(Arc::new(ScriptTransport { ok: Mutex::new(1) }), [NodeId(1)])
                .with_retry(RetryPolicy {
                    max_attempts: 1,
                    ..RetryPolicy::default()
                });
        let err = propose_keyed_batch(
            &client,
            &[
                KeyedBatchStep {
                    key: b"a".to_vec(),
                    payload: vec![1],
                },
                KeyedBatchStep {
                    key: b"b".to_vec(),
                    payload: vec![2],
                },
            ],
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            BatchError::Partial {
                step: 1,
                completed: 1,
                ..
            }
        ));
    }
}
