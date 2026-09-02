//! Queue lifecycle hooks for observability (dashboard event feed).

/// A queue mutation that completed on the Raft leader (after replication).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueLifecycleEvent {
    /// A job was enqueued.
    Enqueued {
        /// Stream name.
        stream: String,
        /// Assigned job id.
        job_id: u64,
    },
    /// Jobs were leased to a worker.
    Leased {
        /// Stream name.
        stream: String,
        /// Leased job id.
        job_id: u64,
        /// Lease token for ack/nack.
        lease_id: u64,
        /// Worker node id.
        worker_node: u64,
        /// Worker instance id on that node.
        worker_instance: u32,
        /// Delivery attempts including this one (`1` on first delivery).
        ///
        /// `> 1` means the job was redelivered — an idempotency smell worth
        /// surfacing ([background-jobs](../../../docs/scenarios/background-jobs.md#delivery-semantics)).
        attempts: u32,
    },
    /// A lease was acknowledged (job completed).
    Acked {
        /// Stream name.
        stream: String,
        /// Acknowledged lease id.
        lease_id: u64,
        /// Worker node id.
        worker_node: u64,
    },
}
