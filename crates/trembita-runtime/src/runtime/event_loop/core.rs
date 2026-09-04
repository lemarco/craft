use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::mpsc;
use trembita_core::StateMachine;
use trembita_net::{Transport, send_peer_rpc};
use trembita_proto::{NodeId, RaftRpc};

use crate::RaftDriver;

use super::super::types::Envelope;
use super::Runtime;

impl<M: StateMachine> Runtime<M> {
    pub(in crate::runtime) fn new(
        driver: RaftDriver<M>,
        transport: Arc<dyn Transport>,
        config: &super::super::types::RuntimeConfig,
        self_tx: mpsc::UnboundedSender<Envelope<M>>,
        voter_replacement_grace_ticks: u64,
    ) -> Self {
        Self {
            driver,
            transport,
            self_tx,
            allow_join: config.allow_join,
            allow_voter_join: config.allow_voter_join,
            voter_replacement: config.voter_replacement,
            voter_replacement_grace_ticks,
            voter_unreachable_since: BTreeMap::new(),
            replacement_tick: 0,
            allow_leave: config.allow_leave,
            pending_proposals: HashMap::new(),
            pending_queries: HashMap::new(),
            pending_read_confirms: HashMap::new(),
            pending_joins: HashMap::new(),
            pending_leaves: HashMap::new(),
            pending_catalog_adds: HashMap::new(),
            pending_saga_journals: HashMap::new(),
            pending_two_phase_journals: HashMap::new(),
            pending_queue_autoscale_policies: HashMap::new(),
            pending_two_phase_prepares: HashMap::new(),
            pending_two_phase_aborts: HashMap::new(),
            pending_two_phase_commits: HashMap::new(),
            catalog_snapshot: config.catalog_snapshot.clone(),
            on_catalog_applied: config.on_catalog_applied.clone(),
            on_saga_journal_applied: config.on_saga_journal_applied.clone(),
            on_two_phase_journal_applied: config.on_two_phase_journal_applied.clone(),
            on_queue_autoscale_policy_applied: config.on_queue_autoscale_policy_applied.clone(),
            on_two_phase_gc_aborted: config.on_two_phase_gc_aborted.clone(),
            next_read_id: 0,
            cross_shard_2pc: config.cross_shard_2pc,
            durable_cross_shard_2pc: config.durable_cross_shard_2pc,
            two_phase_prepare_timeout: config.two_phase_prepare_timeout,
            tick_period: config.tick_period,
            two_phase_tick: 0,
            two_phase_prepares: crate::two_phase::PrepareStore::default(),
            compaction: config.compaction.clone(),
        }
    }

    pub(in crate::runtime) fn run_background(
        mut self,
        mut rx: mpsc::UnboundedReceiver<Envelope<M>>,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.tick_period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut shutdown_done = None;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.two_phase_tick = self.two_phase_tick.saturating_add(1);
                        self.replacement_tick = self.replacement_tick.saturating_add(1);
                        match self.driver.tick() {
                            Ok(step) => { let _ = self.settle(step); }
                            Err(_) => break,
                        }
                        self.maybe_replace_unreachable_voter();
                        if self.maybe_gc_two_phase_prepares().is_err() {
                            break;
                        }
                    }
                    maybe = rx.recv() => {
                        let Some(env) = maybe else { break };
                        if let Envelope::Shutdown { done } = env {
                            shutdown_done = done;
                            break;
                        }
                        match self.on_envelope(env) {
                            Ok(true) => {}
                            Ok(false) | Err(_) => break,
                        }
                    }
                }
            }
            drop(self);
            if let Some(done) = shutdown_done {
                let _ = done.send(());
            }
            // Pending responders drop here, so blocked clients observe `Stopped`.
        });
    }

    /// Dispatch one outbound request RPC; feed its reply back into the mailbox.
    pub(in crate::runtime::event_loop) fn dispatch_send(&self, peer: NodeId, rpc: RaftRpc) {
        let transport = Arc::clone(&self.transport);
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            if let Ok(reply) = send_peer_rpc(&*transport, peer, &rpc).await {
                let _ = tx.send(Envelope::Reply { from: peer, reply });
            }
            // On transport error the peer is unreachable for now; the next
            // heartbeat/election round will retry. Nothing to feed back.
        });
    }
}
