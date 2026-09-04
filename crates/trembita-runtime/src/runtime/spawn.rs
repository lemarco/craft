use std::sync::Arc;

use tokio::sync::mpsc;
use trembita_core::{StateMachine, voter_replacement_grace_ticks};
use trembita_net::Transport;

use crate::RaftDriver;

use super::event_loop::Runtime;
use super::handle::NodeHandle;
use super::types::{Envelope, RuntimeConfig};

/// Spawn a node runtime around `driver`, driving it over `transport`, and
/// return a [`NodeHandle`] for clients and the request handler.
///
/// The returned handle can be cloned freely; the node stops when
/// [`NodeHandle::shutdown`] is called or a fatal driver error occurs.
pub fn spawn<M>(
    driver: RaftDriver<M>,
    transport: Arc<dyn Transport>,
    config: &RuntimeConfig,
) -> NodeHandle<M>
where
    M: StateMachine,
{
    let id = driver.node().id();
    let voter_replacement_grace_ticks = config.voter_replacement_grace_ticks.unwrap_or_else(|| {
        voter_replacement_grace_ticks(driver.node().reachability_window_ticks())
    });
    let (tx, rx) = mpsc::unbounded_channel::<Envelope<M>>();
    let runtime = Runtime::new(
        driver,
        transport,
        config,
        tx.clone(),
        voter_replacement_grace_ticks,
    );
    runtime.run_background(rx);

    NodeHandle { id, tx }
}
