use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::oneshot;
use trembita_net::transport::BoxFuture;

use super::actor::UserActor;
use super::errors::{
    DeliverError, DrainOutcome, MessageDecodeError, MigrationError, SnapshotError,
};
use super::pool::PoolInner;
use super::reply::{WireReply, WireReplyPort};

/// Type-erased lifecycle handle so the registry can stop / inspect a group
/// without knowing its actor type.
pub(super) trait GroupLifecycle: Send + Sync {
    fn instance_count(&self) -> usize;
    fn instance_ids(&self) -> Vec<u32>;
    fn type_name(&self) -> &'static str;
    fn migratable(&self) -> bool;
    fn signal_stop(&self);
    /// Runtime counters `(instances, messages, handle_nanos, mailbox_depth)` for
    /// metrics sampling (Track H).
    fn runtime_stats(&self) -> (usize, u64, u64, i64);
    /// Per-instance uptime and mailbox depth for Observer introspection.
    fn instance_introspection(&self) -> Vec<(u32, u64, i64)>;
    /// Gracefully drain and stop the group with `timeout` (E12, drain-timeout).
    fn drain(self: Arc<Self>, timeout: Duration) -> BoxFuture<'static, DrainOutcome>;
    /// Per-group graceful-drain override ([drain-timeout]).
    fn set_drain_timeout(&self, timeout: Option<Duration>);
    fn drain_timeout(&self) -> Option<Duration>;
    /// Capture a migration snapshot from instance `instance` (E12).
    fn snapshot(
        self: Arc<Self>,
        instance: u32,
    ) -> BoxFuture<'static, Result<Vec<u8>, SnapshotError>>;
}

impl<A: UserActor> GroupLifecycle for PoolInner<A> {
    fn instance_count(&self) -> usize {
        self.len()
    }
    fn instance_ids(&self) -> Vec<u32> {
        PoolInner::instance_ids(self)
    }
    fn type_name(&self) -> &'static str {
        std::any::type_name::<A>()
    }
    fn migratable(&self) -> bool {
        A::MIGRATABLE
    }
    fn signal_stop(&self) {
        PoolInner::signal_stop(self);
    }
    fn runtime_stats(&self) -> (usize, u64, u64, i64) {
        PoolInner::runtime_stats(self)
    }
    fn instance_introspection(&self) -> Vec<(u32, u64, i64)> {
        PoolInner::instance_introspection(self)
    }
    fn drain(self: Arc<Self>, timeout: Duration) -> BoxFuture<'static, DrainOutcome> {
        Box::pin(async move { PoolInner::drain(&self, timeout).await })
    }
    fn set_drain_timeout(&self, timeout: Option<Duration>) {
        PoolInner::set_drain_timeout(self, timeout);
    }
    fn drain_timeout(&self) -> Option<Duration> {
        PoolInner::drain_timeout(self)
    }
    fn snapshot(
        self: Arc<Self>,
        instance: u32,
    ) -> BoxFuture<'static, Result<Vec<u8>, SnapshotError>> {
        Box::pin(async move { PoolInner::snapshot_instance(&self, instance).await })
    }
}

/// Type-erased byte ingress so the registry can deliver a cross-node
/// [`ActorEnvelope`](trembita_proto::ActorEnvelope) payload to a group without
/// knowing its actor type (E8): the payload is decoded via
/// [`UserActor::decode_message`] and routed to the selected instance.
pub(super) trait WireIngress: Send + Sync {
    fn deliver(&self, instance: u32, payload: &[u8]) -> Result<(), DeliverError>;
    /// Deliver a cross-node **ask**: decode via [`UserActor::decode_ask`] with a
    /// wire reply port and return the channel the encoded reply arrives on.
    fn deliver_ask(
        &self,
        instance: u32,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<WireReply>, DeliverError>;
}

impl<A: UserActor> WireIngress for PoolInner<A> {
    fn deliver(&self, instance: u32, payload: &[u8]) -> Result<(), DeliverError> {
        let msg = A::decode_message(payload)?;
        self.send_to_instance(instance, msg)
    }

    fn deliver_ask(
        &self,
        instance: u32,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<WireReply>, DeliverError> {
        let (tx, rx) = oneshot::channel();
        let msg = A::decode_ask(payload, WireReplyPort::new(tx))?;
        self.send_to_instance(instance, msg)?;
        Ok(rx)
    }
}
