//! Leader-only periodic tasks — [`LeaderSession`] term gating and [`run_leader_loop`].
//!
//! See [leader-task ADR](https://github.com/trembita/trembita/blob/main/docs/decisions/leader-task.md).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{self, MissedTickBehavior};

use crate::supervisor::ClusterState;

/// Outcome of gating one iteration against [`ClusterState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderGate {
    /// Not leader — skip body.
    Idle,
    /// Leader — run body; `first_in_term` is true on the first gate after acquiring
    /// leadership (including process start while already leader).
    Active {
        /// First tick since this node became leader.
        first_in_term: bool,
    },
}

impl LeaderGate {
    /// Whether this node should run leader-only work this tick.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active { .. })
    }

    /// Whether leadership was just acquired (one shot per term).
    #[must_use]
    pub const fn first_in_term(self) -> bool {
        matches!(
            self,
            Self::Active {
                first_in_term: true
            }
        )
    }
}

/// Tracks leadership **terms** for periodic tasks (pure, no I/O).
#[derive(Debug, Clone, Default)]
pub struct LeaderSession {
    was_leader: bool,
}

impl LeaderSession {
    /// Start a fresh session (not leader).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Compare against `state` and update internal term tracking.
    #[must_use]
    pub fn gate(&mut self, state: &dyn ClusterState) -> LeaderGate {
        let is_leader = state.is_leader();
        let first_in_term = is_leader && !self.was_leader;
        self.was_leader = is_leader;
        if is_leader {
            LeaderGate::Active { first_in_term }
        } else {
            LeaderGate::Idle
        }
    }
}

/// Tunables for [`run_leader_loop`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderLoopOpts {
    /// Wall-clock period between ticks (after the previous tick completes).
    pub period: Duration,
    /// When true and the node is leader at loop start, invoke `tick` once before
    /// the first interval wait.
    pub run_on_acquire: bool,
}

impl LeaderLoopOpts {
    /// Periodic leader task with default options (`run_on_acquire = false`).
    #[must_use]
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            run_on_acquire: false,
        }
    }

    /// Invoke `tick` immediately when the loop starts while already leader.
    #[must_use]
    pub fn run_on_acquire(mut self) -> Self {
        self.run_on_acquire = true;
        self
    }
}

impl Default for LeaderLoopOpts {
    fn default() -> Self {
        Self::new(Duration::from_secs(1))
    }
}

/// Periodic loop: wait → gate on [`ClusterState`] → invoke `tick` when [`LeaderGate::is_active`].
///
/// Exits when `stop` is set to `true`. A dropped stop sender does **not** stop the
/// loop (background tasks in the node builder keep running until aborted on shutdown).
pub async fn run_leader_loop<F, Fut>(
    state: Arc<dyn ClusterState>,
    opts: LeaderLoopOpts,
    mut stop: watch::Receiver<bool>,
    mut tick: F,
) where
    F: FnMut(LeaderGate) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut session = LeaderSession::new();
    let mut interval = time::interval(opts.period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    if opts.run_on_acquire {
        let gate = session.gate(state.as_ref());
        if gate.is_active() {
            tick(gate).await;
        }
    }

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            changed = stop.changed() => {
                if changed.is_ok() && *stop.borrow() {
                    break;
                }
            }
        }
        if *stop.borrow() {
            break;
        }
        let gate = session.gate(state.as_ref());
        if gate.is_active() {
            tick(gate).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use trembita_proto::NodeId;

    use super::*;

    struct MockState {
        leader: bool,
    }

    impl ClusterState for MockState {
        fn is_leader(&self) -> bool {
            self.leader
        }

        fn live_nodes(&self) -> Vec<NodeId> {
            vec![NodeId(1)]
        }
    }

    #[test]
    fn gate_transitions() {
        let mut state = MockState { leader: false };
        let mut session = LeaderSession::new();

        assert_eq!(session.gate(&state), LeaderGate::Idle);

        state.leader = true;
        assert_eq!(
            session.gate(&state),
            LeaderGate::Active {
                first_in_term: true
            }
        );
        assert_eq!(
            session.gate(&state),
            LeaderGate::Active {
                first_in_term: false
            }
        );

        state.leader = false;
        assert_eq!(session.gate(&state), LeaderGate::Idle);

        state.leader = true;
        assert_eq!(
            session.gate(&state),
            LeaderGate::Active {
                first_in_term: true
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn loop_ticks_only_while_leader() {
        let leader = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state: Arc<dyn ClusterState> = Arc::new(AtomicLeader(Arc::clone(&leader)));
        let (stop_tx, stop_rx) = watch::channel(false);
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ticks_task = Arc::clone(&ticks);
        let state_task = Arc::clone(&state);

        let handle = tokio::spawn(async move {
            run_leader_loop(
                state_task,
                LeaderLoopOpts::new(Duration::from_millis(100)),
                stop_rx,
                move |_| {
                    let ticks_task = Arc::clone(&ticks_task);
                    async move {
                        ticks_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                },
            )
            .await;
        });

        tokio::time::advance(Duration::from_millis(250)).await;
        assert_eq!(ticks.load(std::sync::atomic::Ordering::SeqCst), 0);

        leader.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(250)).await;
        assert!(ticks.load(std::sync::atomic::Ordering::SeqCst) >= 1);

        leader.store(false, std::sync::atomic::Ordering::SeqCst);
        let before = ticks.load(std::sync::atomic::Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(250)).await;
        assert_eq!(ticks.load(std::sync::atomic::Ordering::SeqCst), before);

        let _ = stop_tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_stop_sender_does_not_exit_loop() {
        let state: Arc<dyn ClusterState> = Arc::new(MutexMock::new(true));
        let (stop_tx, stop_rx) = watch::channel(false);
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ticks_task = Arc::clone(&ticks);
        let state_task = Arc::clone(&state);

        let handle = tokio::spawn(async move {
            run_leader_loop(
                state_task,
                LeaderLoopOpts::new(Duration::from_millis(50)),
                stop_rx,
                move |_| {
                    let ticks_task = Arc::clone(&ticks_task);
                    async move {
                        ticks_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                },
            )
            .await;
        });

        drop(stop_tx);
        tokio::time::advance(Duration::from_millis(120)).await;
        assert!(ticks.load(std::sync::atomic::Ordering::SeqCst) >= 1);

        let _ = handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn run_on_acquire_ticks_before_first_interval() {
        let state: Arc<dyn ClusterState> = Arc::new(MutexMock::new(true));
        let (stop_tx, stop_rx) = watch::channel(false);
        let first = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_task = Arc::clone(&first);

        let handle = tokio::spawn(async move {
            run_leader_loop(
                state,
                LeaderLoopOpts::new(Duration::from_secs(60)).run_on_acquire(),
                stop_rx,
                move |gate| {
                    let first_task = Arc::clone(&first_task);
                    async move {
                        if gate.first_in_term() {
                            first_task.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                },
            )
            .await;
        });

        tokio::task::yield_now().await;
        assert!(first.load(std::sync::atomic::Ordering::SeqCst));

        let _ = stop_tx.send(true);
        let _ = handle.await;
    }

    struct AtomicLeader(Arc<std::sync::atomic::AtomicBool>);

    impl ClusterState for AtomicLeader {
        fn is_leader(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn live_nodes(&self) -> Vec<NodeId> {
            vec![NodeId(1)]
        }
    }

    struct MutexMock {
        leader: std::sync::Mutex<bool>,
    }

    impl MutexMock {
        fn new(leader: bool) -> Self {
            Self {
                leader: std::sync::Mutex::new(leader),
            }
        }
    }

    impl ClusterState for MutexMock {
        fn is_leader(&self) -> bool {
            *self.leader.lock().unwrap()
        }

        fn live_nodes(&self) -> Vec<NodeId> {
            vec![NodeId(1)]
        }
    }
}
