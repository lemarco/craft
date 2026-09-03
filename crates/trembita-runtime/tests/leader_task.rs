//! Driver-level tests for [`LeaderSession`] and [`run_leader_loop`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use trembita_proto::NodeId;
use trembita_runtime::{ClusterState, LeaderLoopOpts, LeaderSession, run_leader_loop};

struct AtomicLeader(Arc<AtomicBool>);

impl ClusterState for AtomicLeader {
    fn is_leader(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        vec![NodeId(1)]
    }
}

#[test]
fn session_first_in_term_after_step_down_and_re_elect() {
    let leader = Arc::new(AtomicBool::new(false));
    let state = AtomicLeader(Arc::clone(&leader));
    let mut session = LeaderSession::new();

    assert!(!session.gate(&state).is_active());

    leader.store(true, Ordering::SeqCst);
    assert!(session.gate(&state).first_in_term());
    assert!(!session.gate(&state).first_in_term());

    leader.store(false, Ordering::SeqCst);
    assert!(!session.gate(&state).is_active());

    leader.store(true, Ordering::SeqCst);
    assert!(session.gate(&state).first_in_term());
}

#[tokio::test(start_paused = true)]
async fn run_leader_loop_stops_after_step_down_within_one_period() {
    let leader = Arc::new(AtomicBool::new(true));
    let state: Arc<dyn ClusterState> = Arc::new(AtomicLeader(Arc::clone(&leader)));
    let (stop_tx, stop_rx) = watch::channel(false);
    let ticks = Arc::new(AtomicUsize::new(0));
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
                    ticks_task.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(150)).await;
    assert!(ticks.load(Ordering::SeqCst) >= 1);

    leader.store(false, Ordering::SeqCst);
    let at_step_down = ticks.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_millis(250)).await;
    assert_eq!(ticks.load(Ordering::SeqCst), at_step_down);

    let _ = stop_tx.send(true);
    let _ = handle.await;
}
