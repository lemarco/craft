//! Topic leader retention loop wired at node assemble time.

use std::sync::Arc;

use trembita_events::TopicService;
use trembita_runtime::LeaderGate;

use super::types::TopicStreamSpec;

pub(super) struct TopicLeaderLoop {
    bootstrapped: bool,
    service: Arc<TopicService>,
    specs: Vec<TopicStreamSpec>,
}

impl TopicLeaderLoop {
    pub(super) fn new(service: Arc<TopicService>, specs: Vec<TopicStreamSpec>) -> Self {
        Self {
            bootstrapped: false,
            service,
            specs,
        }
    }

    pub(super) async fn tick(&mut self, gate: LeaderGate) {
        if gate.first_in_term() && !self.bootstrapped {
            for spec in &self.specs {
                if !spec.subscriptions.is_empty() {
                    let _ = self
                        .service
                        .bootstrap_subscriptions(&spec.name, &spec.subscriptions)
                        .await;
                }
            }
            self.bootstrapped = true;
        }
        let _ = self.service.enforce_retention_all().await;
    }
}
