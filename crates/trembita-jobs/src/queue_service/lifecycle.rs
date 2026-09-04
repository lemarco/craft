//! Lifecycle hooks and backlog settle emission for queue handlers.

use std::sync::Arc;

use trembita_proto::QueueReplicateOp;

use super::wire::shard_stream_name;
use crate::backlog_settle_outbox::push_backlog_settle;
use crate::external_backlog::{
    BacklogSettleEvent, BacklogSettleOutcome, emit_backlog_settle_for_terminal_ops,
};
use crate::queue_lifecycle::QueueLifecycleEvent;
use crate::{JobQueue, LeaseId, ShardedReplication};

use super::QueueService;

impl QueueService {
    pub(super) fn emit_lifecycle(&self, event: QueueLifecycleEvent) {
        if let Some(hook) = &self.lifecycle_hook {
            hook(event);
        }
    }

    pub(super) fn emit_enqueued(&self, stream: &str, job_id: u64) {
        self.emit_lifecycle(QueueLifecycleEvent::Enqueued {
            stream: stream.to_owned(),
            job_id,
        });
    }

    pub(super) fn emit_leased(
        &self,
        stream: &str,
        job_id: u64,
        lease_id: u64,
        worker_node: u64,
        worker_instance: u32,
        attempts: u32,
    ) {
        self.emit_lifecycle(QueueLifecycleEvent::Leased {
            stream: stream.to_owned(),
            job_id,
            lease_id,
            worker_node,
            worker_instance,
            attempts,
        });
    }

    pub(super) fn emit_acked(&self, stream: &str, lease_id: u64, worker_node: u64) {
        self.emit_lifecycle(QueueLifecycleEvent::Acked {
            stream: stream.to_owned(),
            lease_id,
            worker_node,
        });
    }

    pub(super) fn emit_backlog_settle(
        &self,
        stream: &str,
        dedup_key: Option<Vec<u8>>,
        outcome: BacklogSettleOutcome,
    ) {
        push_backlog_settle(
            self.backlog_settle_outbox.as_deref(),
            BacklogSettleEvent {
                stream: stream.to_owned(),
                dedup_key,
                outcome,
            },
        );
    }

    pub(super) async fn emit_backlog_settle_for_terminal_ops(
        &self,
        stream: &str,
        queue: &dyn JobQueue,
        ops: &[QueueReplicateOp],
        error: &str,
    ) {
        emit_backlog_settle_for_terminal_ops(
            stream,
            queue,
            self.backlog_settle_outbox.as_deref(),
            ops,
            error,
        )
        .await;
    }

    pub(super) async fn emit_backlog_settle_for_sharded_reps(
        &self,
        base: &str,
        reps: &[ShardedReplication],
        error: &str,
    ) {
        for rep in reps {
            let stream = shard_stream_name(base, rep.shard);
            if let Ok(queue) = self.local_stream(&stream) {
                self.emit_backlog_settle_for_terminal_ops(&stream, queue.as_ref(), &rep.ops, error)
                    .await;
            }
        }
    }

    pub(super) async fn peek_lease_meta(
        &self,
        stream: &str,
        lease_id: LeaseId,
    ) -> (Option<Vec<u8>>, u32) {
        match self.local_stream(stream) {
            Ok(queue) => queue.peek_lease_meta(lease_id).await.unwrap_or((None, 0)),
            Err(_) => (None, 0),
        }
    }
}
