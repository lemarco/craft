//! Leader-gated topic wire service ([event-topics](../../../docs/decisions/event-topics.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success.

mod cluster_topic;
mod dispatch;
mod handlers;
mod replication;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use trembita_net::transport::Transport;
use trembita_proto::NodeId;
use trembita_runtime::ClusterState;

use crate::{EventTopic, RedbEventTopic, TopicSubscriptionDef};

pub use cluster_topic::ClusterEventTopic;

/// Serves `/raft/v1/topic/*` on the leader; followers transparently forward.
pub struct TopicService {
    pub(super) node_id: NodeId,
    pub(super) topics: Mutex<HashMap<String, Arc<dyn EventTopic>>>,
    pub(super) redb_topics: Mutex<HashMap<String, Arc<RedbEventTopic>>>,
    pub(super) state: Arc<dyn ClusterState>,
    pub(super) transport: Arc<dyn Transport>,
}

impl TopicService {
    /// Empty service; register topics before accepting traffic.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            topics: Mutex::new(HashMap::new()),
            redb_topics: Mutex::new(HashMap::new()),
            state,
            transport,
        }
    }

    /// Register a local redb-backed topic.
    ///
    /// Call [`Self::bootstrap_subscriptions`] after all topics are registered.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_redb_topic(&self, name: impl Into<String>, topic: &Arc<RedbEventTopic>) {
        let name = name.into();
        self.topics
            .lock()
            .expect("poisoned")
            .insert(name.clone(), Arc::clone(topic) as Arc<dyn EventTopic>);
        self.redb_topics
            .lock()
            .expect("poisoned")
            .insert(name, Arc::clone(topic));
    }

    /// Register subscriptions on an already-open topic (leader boot path).
    ///
    /// # Errors
    /// Propagates topic failures as strings.
    pub async fn bootstrap_subscriptions(
        &self,
        name: &str,
        subscriptions: &[TopicSubscriptionDef],
    ) -> Result<(), String> {
        if subscriptions.is_empty() {
            return Ok(());
        }
        let topic = self.local_topic(name).map_err(|e| e.to_string())?;
        let ops = topic
            .register_subscriptions(subscriptions)
            .await
            .map_err(|e| e.to_string())?;
        self.replicate_ops(name, &ops)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Enforce retention on every registered redb topic (leader-only ticker).
    ///
    /// # Errors
    ///
    /// Returns an error string when topic lookup, retention, or replication fails.
    ///
    /// # Panics
    ///
    /// Panics if the redb topic registry mutex is poisoned.
    pub async fn enforce_retention_all(&self) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let names: Vec<String> = self
            .redb_topics
            .lock()
            .expect("poisoned")
            .keys()
            .cloned()
            .collect();
        for name in names {
            let topic = self.local_topic(&name).map_err(|e| e.to_string())?;
            let ops = topic
                .enforce_retention_replicated()
                .await
                .map_err(|e| e.to_string())?;
            self.replicate_ops(&name, &ops)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
