use tokio::sync::oneshot;
use trembita_core::{Command as _, StateMachine};

use crate::DriverError;

use super::super::types::ClientError;
use super::Runtime;

impl<M: StateMachine> Runtime<M> {
    pub(in crate::runtime::event_loop) fn on_two_phase_prepare(
        &mut self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        command: Vec<u8>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) {
        if !self.cross_shard_2pc {
            let _ = respond.send(Err(ClientError::Driver(
                "cross-shard 2PC is disabled on this group".to_string(),
            )));
            return;
        }
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return;
        }
        if M::Command::from_bytes(&command).is_err() {
            let _ = respond.send(Err(ClientError::Driver(
                "decode command for 2PC prepare failed".to_string(),
            )));
            return;
        }
        if self.durable_cross_shard_2pc {
            let prepared_at_ms = crate::two_phase::unix_now_ms();
            let journal_cmd = trembita_proto::TwoPhasePrepareCommand {
                tx_id,
                route_key,
                command,
                prepared_at_ms,
            };
            match self.driver.propose_two_phase_prepare(journal_cmd) {
                Ok(Ok((index, step))) => {
                    self.pending_two_phase_prepares.insert(index, respond);
                    let _ = self.settle(step);
                }
                Ok(Err(trembita_core::CatalogProposeError::NotLeader { leader })) => {
                    let _ = respond.send(Err(ClientError::NotLeader { leader }));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            }
            return;
        }
        match self
            .two_phase_prepares
            .prepare(tx_id, route_key, command, self.two_phase_tick)
        {
            Ok(()) => {
                let _ = respond.send(Ok(()));
            }
            Err(e) => {
                let _ = respond.send(Err(ClientError::Driver(e.to_string())));
            }
        }
    }

    pub(in crate::runtime::event_loop) fn on_two_phase_commit(
        &mut self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        respond: oneshot::Sender<Result<M::Response, ClientError>>,
    ) {
        if !self.cross_shard_2pc {
            let _ = respond.send(Err(ClientError::Driver(
                "cross-shard 2PC is disabled on this group".to_string(),
            )));
            return;
        }
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return;
        }
        let Some(bytes) = self.two_phase_prepares.get(&tx_id, &route_key).cloned() else {
            let _ = respond.send(Err(ClientError::Driver(
                "no prepared command for transaction key".to_string(),
            )));
            return;
        };
        let command = match M::Command::from_bytes(&bytes) {
            Ok(c) => c,
            Err(e) => {
                let _ = respond.send(Err(ClientError::Driver(format!(
                    "decode prepared command: {e}"
                ))));
                return;
            }
        };
        match self.driver.propose(&command) {
            Ok((index, step)) => {
                self.pending_two_phase_commits
                    .insert(index, (tx_id, route_key, respond));
                let _ = self.settle(step);
            }
            Err(DriverError::NotLeader { leader }) => {
                let _ = respond.send(Err(ClientError::NotLeader { leader }));
            }
            Err(e) => {
                let _ = respond.send(Err(ClientError::Driver(e.to_string())));
            }
        }
    }

    pub(in crate::runtime::event_loop) fn on_two_phase_abort(
        &mut self,
        tx_id: Vec<u8>,
        route_key: Vec<u8>,
        respond: oneshot::Sender<Result<(), ClientError>>,
    ) {
        if !self.cross_shard_2pc {
            let _ = respond.send(Err(ClientError::Driver(
                "cross-shard 2PC is disabled on this group".to_string(),
            )));
            return;
        }
        if !self.driver.is_leader() {
            let _ = respond.send(Err(ClientError::NotLeader {
                leader: self.driver.node().leader_id(),
            }));
            return;
        }
        if self.durable_cross_shard_2pc {
            let journal_cmd = trembita_proto::TwoPhaseAbortCommand { tx_id, route_key };
            match self.driver.propose_two_phase_abort(journal_cmd) {
                Ok(Ok((index, step))) => {
                    self.pending_two_phase_aborts.insert(index, respond);
                    let _ = self.settle(step);
                }
                Ok(Err(trembita_core::CatalogProposeError::NotLeader { leader })) => {
                    let _ = respond.send(Err(ClientError::NotLeader { leader }));
                }
                Err(e) => {
                    let _ = respond.send(Err(ClientError::Driver(e.to_string())));
                }
            }
            return;
        }
        let _ = self.two_phase_prepares.abort(&tx_id, &route_key);
        let _ = respond.send(Ok(()));
    }

    /// Abort prepares that exceeded [`two_phase_prepare_timeout`] (leader-only).
    pub(in crate::runtime::event_loop) fn maybe_gc_two_phase_prepares(
        &mut self,
    ) -> Result<(), DriverError> {
        let Some(timeout) = self.two_phase_prepare_timeout else {
            return Ok(());
        };
        if !self.cross_shard_2pc || !self.driver.is_leader() {
            return Ok(());
        }
        let tick_period_ms = u64::try_from(
            self.tick_period
                .as_millis()
                .max(1)
                .min(u128::from(u64::MAX)),
        )
        .unwrap_or(u64::MAX);
        let timeout_ms =
            u64::try_from(timeout.as_millis().max(1).min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
        let timeout_ticks = timeout_ms.div_ceil(tick_period_ms).max(1);
        let expired = self
            .two_phase_prepares
            .expired_ticks(self.two_phase_tick, timeout_ticks);
        for (tx_id, route_key) in expired {
            if self.durable_cross_shard_2pc {
                let journal_cmd = trembita_proto::TwoPhaseAbortCommand { tx_id, route_key };
                match self.driver.propose_two_phase_abort(journal_cmd)? {
                    Ok((_, step)) => {
                        let _ = self.settle(step);
                        if let Some(hook) = &self.on_two_phase_gc_aborted {
                            hook();
                        }
                    }
                    Err(trembita_core::CatalogProposeError::NotLeader { .. }) => break,
                }
            } else {
                let _ = self.two_phase_prepares.abort(&tx_id, &route_key);
                if let Some(hook) = &self.on_two_phase_gc_aborted {
                    hook();
                }
            }
        }
        Ok(())
    }
}
